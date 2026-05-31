"""
rag/langgraph_chatbot.py
─────────────────────────────────────────────────────────────────────────────
MOSDAC AI Help Bot — LangGraph RAG Pipeline (Groq / llama-3.3-70b)

Fixes applied:
  LG-1  generate_node now includes full conversation history in LLM call
  LG-2  stream_response uses graph.update_state (not full graph.invoke)
  LG-3  AIMessage imported for history reconstruction in generate_node
  LG-4  SQLite DB path uses project root (not CWD) — consistent across OS

Graph:
  START → classify → retrieve → [mission/payload] → enrich → generate → END
                                → [FAQ/general   ] ─────────────────────→
─────────────────────────────────────────────────────────────────────────────
"""

import operator
import os
import sqlite3
from pathlib import Path
from typing import Annotated, List, TypedDict

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, StateGraph

from rag.prompt_builder import SYSTEM_PROMPT, build_context_block, format_sources
from rag.retriever import QueryType, Retriever
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)

# ── DB path — always next to this file's project root ────────────────────────
# FIX LG-4: use absolute path so DB is consistent wherever the app is started
_PROJECT_ROOT = Path(__file__).parent.parent
_DB_PATH      = _PROJECT_ROOT / "mosdac.db"

connection   = sqlite3.connect(database=str(_DB_PATH), check_same_thread=False)
checkpointer = SqliteSaver(conn=connection)

_cursor = connection.cursor()
_cursor.execute("""
    CREATE TABLE IF NOT EXISTS thread_metadata (
        thread_id  TEXT PRIMARY KEY,
        title      TEXT,
        created_at TEXT
    )
""")
connection.commit()


# ── Retriever registry ────────────────────────────────────────────────────────
_RETRIEVER_REGISTRY: dict = {}


def register_retriever(key: str, retriever: Retriever) -> None:
    """Register once at startup: register_retriever('default', retriever)"""
    _RETRIEVER_REGISTRY[key] = retriever
    log.info(f"Retriever registered: key='{key}'")


def _get_retriever(key: str = "default") -> Retriever:
    if key not in _RETRIEVER_REGISTRY:
        raise RuntimeError(
            f"Retriever '{key}' not registered. "
            "Call register_retriever(key, retriever) at startup."
        )
    return _RETRIEVER_REGISTRY[key]


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH STATE
# ─────────────────────────────────────────────────────────────────────────────

class MOSDACState(TypedDict):
    query:         str
    session_id:    str
    retriever_key: str
    query_type:    str
    chunks:        List[dict]
    context_text:  str
    sources:       str
    response:      str
    # operator.add accumulates messages across turns automatically
    messages: Annotated[List[dict], operator.add]


# ─────────────────────────────────────────────────────────────────────────────
# NODES
# ─────────────────────────────────────────────────────────────────────────────

def classify_node(state: MOSDACState) -> dict:
    """Node 1 — classify query intent."""
    retriever = _get_retriever(state.get("retriever_key", "default"))
    q_type    = retriever._classify_query(state["query"])
    log.debug(f"[classify] {q_type!r} ← '{state['query'][:60]}'")
    return {"query_type": q_type}


def retrieve_node(state: MOSDACState) -> dict:
    """Node 2 — FAISS semantic search + keyword fallback."""
    retriever = _get_retriever(state.get("retriever_key", "default"))
    chunks    = retriever.retrieve(state["query"])
    if not chunks:
        chunks = retriever.keyword_search(state["query"], top_k=4)
    log.debug(f"[retrieve] {len(chunks)} chunks")
    return {"chunks": chunks}


def enrich_node(state: MOSDACState) -> dict:
    """Node 3 — build context + KG graph enrichment for mission/payload."""
    retriever = _get_retriever(state.get("retriever_key", "default"))
    chunks    = state.get("chunks", [])
    q_type    = state.get("query_type", QueryType.GENERAL)

    if q_type in (QueryType.MISSION, QueryType.PAYLOAD) and retriever.graph:
        chunks = _add_graph_neighbours(chunks, retriever)

    context_text = build_context_block(chunks) if chunks else "No relevant context found."
    sources      = format_sources(chunks)
    log.debug(f"[enrich] context={len(context_text)} chars")
    return {"context_text": context_text, "sources": sources, "chunks": chunks}


def generate_node(state: MOSDACState) -> dict:
    """
    Node 4 — call Groq LLM with full conversation history.

    FIX LG-1: history is now included in the LLM call.
    FIX LG-3: AIMessage used to reconstruct assistant turns.
    """
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        answer = "⚠️ GROQ_API_KEY not set. Check your .env or settings."
        return {
            "response": answer,
            "messages": [
                {"role": "user",      "content": state["query"]},
                {"role": "assistant", "content": answer},
            ],
        }

    history      = state.get("messages", [])[-20:]   # last 10 turns
    context      = state.get("context_text", "No relevant context found.")
    query        = state["query"]

    # Build LangChain message list: system + history + current query
    lc_messages: list = [SystemMessage(content=SYSTEM_PROMPT)]
    for m in history:
        if m.get("role") == "user":
            lc_messages.append(HumanMessage(content=m["content"]))
        else:
            lc_messages.append(AIMessage(content=m["content"]))
    lc_messages.append(
        HumanMessage(content=f"<context>\n{context}\n</context>\n\nQuestion: {query}")
    )

    try:
        llm    = ChatGroq(model="llama-3.3-70b-versatile", groq_api_key=api_key, temperature=0.1)
        answer = llm.invoke(lc_messages).content
    except Exception as exc:
        log.error(f"[generate] Groq error: {exc}")
        answer = f"⚠️ Groq API error: {exc}"

    log.debug(f"[generate] {len(answer)} chars")
    return {
        "response": answer,
        "messages": [
            {"role": "user",      "content": query},
            {"role": "assistant", "content": answer},
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING
# ─────────────────────────────────────────────────────────────────────────────

def _routing(state: MOSDACState) -> str:
    q_type = state.get("query_type", QueryType.GENERAL)
    return "enrich" if q_type in (QueryType.MISSION, QueryType.PAYLOAD) else "generate"


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_graph():
    """Compile the MOSDAC LangGraph RAG pipeline with SqliteSaver."""
    g = StateGraph(MOSDACState)

    g.add_node("classify", classify_node)
    g.add_node("retrieve", retrieve_node)
    g.add_node("enrich",   enrich_node)
    g.add_node("generate", generate_node)

    g.set_entry_point("classify")
    g.add_edge("classify", "retrieve")
    g.add_conditional_edges(
        "retrieve", _routing,
        {"enrich": "enrich", "generate": "generate"},
    )
    g.add_edge("enrich",   "generate")
    g.add_edge("generate", END)

    graph = g.compile(checkpointer=checkpointer)
    log.info("LangGraph pipeline compiled  (classify→retrieve→[enrich]→generate)")
    return graph


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING HELPER  (called by Streamlit)
# ─────────────────────────────────────────────────────────────────────────────

def stream_response(
    query:         str,
    session_id:    str,
    graph,
    retriever_key: str = "default",
):
    """
    Generator: yields text tokens from Groq, then sources sentinel.

    FIX LG-2: uses graph.update_state (not graph.invoke) for memory update —
              avoids re-running the full pipeline for every memory save.
    """
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        yield "⚠️ GROQ_API_KEY not set. Add it in Settings or your .env file."
        return

    config    = {"configurable": {"thread_id": session_id}}
    retriever = _get_retriever(retriever_key)

    # ── Step 1: Retrieval pipeline ────────────────────────────────────────
    q_type = retriever._classify_query(query)
    chunks = retriever.retrieve(query)
    if not chunks:
        chunks = retriever.keyword_search(query, top_k=4)
    if q_type in (QueryType.MISSION, QueryType.PAYLOAD) and retriever.graph:
        chunks = _add_graph_neighbours(chunks, retriever)

    context_text = build_context_block(chunks) if chunks else "No relevant context found."
    sources      = format_sources(chunks)

    # ── Step 2: Load history from checkpoint ─────────────────────────────
    history = []
    try:
        checkpoint = graph.checkpointer.get(config)
        if checkpoint:
            history = checkpoint.get("channel_values", {}).get("messages", [])[-20:]
    except Exception:
        pass

    # ── Step 3: Build LangChain messages with history ─────────────────────
    lc_messages: list = [SystemMessage(content=SYSTEM_PROMPT)]
    for m in history:
        if m.get("role") == "user":
            lc_messages.append(HumanMessage(content=m["content"]))
        else:
            lc_messages.append(AIMessage(content=m["content"]))
    lc_messages.append(
        HumanMessage(content=f"<context>\n{context_text}\n</context>\n\nQuestion: {query}")
    )

    # ── Step 4: Stream Groq ───────────────────────────────────────────────
    full_answer: list = []
    try:
        llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            groq_api_key=api_key,
            temperature=0.1,
            streaming=True,
        )
        for chunk in llm.stream(lc_messages):
            if chunk.content:
                full_answer.append(chunk.content)
                yield chunk.content
    except Exception as exc:
        err = f"⚠️ Error: {exc}"
        yield err
        full_answer = [err]

    # Yield sources as end-of-stream sentinel
    if sources:
        yield f"\n\n__SOURCES__\n{sources}"

    # ── Step 5: Save Q+A to LangGraph memory (FIX LG-2) ──────────────────
    # graph.update_state directly appends to messages via operator.add reducer.
    # No need to re-run classify/retrieve/generate.
    answer = "".join(full_answer)
    try:
        graph.update_state(
            config=config,
            values={
                "messages": [
                    {"role": "user",      "content": query},
                    {"role": "assistant", "content": answer},
                ]
            },
        )
    except Exception as exc:
        log.debug(f"[stream] Memory update skipped: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# KG GRAPH ENRICHMENT HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _add_graph_neighbours(chunks: List[dict], retriever, max_extra: int = 3) -> List[dict]:
    """Add payload/section chunks for matched missions from the KG graph."""
    if not retriever.graph or not retriever.store._metadata:
        return chunks

    mission_slugs = {
        c.get("mission_slug")
        for c in chunks[:3]
        if c.get("mission_slug")
    }
    if not mission_slugs:
        return chunks

    seen  = {c.get("chunk_id") for c in chunks}
    extra = []
    for chunk in retriever.store._metadata:
        if chunk.get("mission_slug") not in mission_slugs:
            continue
        if chunk.get("chunk_id") in seen:
            continue
        if chunk.get("node_type") in ("Payload", "MissionSection"):
            extra.append(chunk)
            seen.add(chunk.get("chunk_id"))
            if len(extra) >= max_extra:
                break

    return chunks + extra


# ─────────────────────────────────────────────────────────────────────────────
# TITLE GENERATOR  (called once per new conversation)
# ─────────────────────────────────────────────────────────────────────────────

def generate_chat_title(user_message: str) -> str:
    """Generate a short 2-3 word title from the first user message."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return user_message[:25]
    try:
        llm = ChatGroq(model="llama-3.3-70b-versatile", groq_api_key=api_key, temperature=0.1)
        prompt = (
            f"Generate a short 2 or max 3 word chat title for: '{user_message}'. "
            "Reply with ONLY the title, no punctuation or explanation."
        )
        return llm.invoke([HumanMessage(content=prompt)]).content.strip()[:30]
    except Exception:
        return user_message[:25]


# ─────────────────────────────────────────────────────────────────────────────
# SQLITE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def save_thread_metadata(thread_id: str, title: str, label: str) -> None:
    _cursor.execute(
        "INSERT OR IGNORE INTO thread_metadata (thread_id, title, created_at) VALUES (?, ?, ?)",
        (str(thread_id), title, label),
    )
    connection.commit()


def retrieve_all_threads() -> list:
    rows = _cursor.execute(
        "SELECT thread_id, title, created_at FROM thread_metadata ORDER BY created_at"
    ).fetchall()
    return [{"id": r[0], "title": r[1], "label": r[2]} for r in rows]


