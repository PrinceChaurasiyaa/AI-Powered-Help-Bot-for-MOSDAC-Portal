"""
streamlit_app.py — MOSDAC AI Help Bot
"""


import streamlit as st

st.set_page_config(
    page_title="MOSDAC AI Help Bot",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"Get help": None, "Report a bug": None, "About": None},
)

# ==========Imports ==================
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from rag.langgraph_chatbot import (
    build_graph,
    generate_chat_title,
    retrieve_all_threads,
    save_thread_metadata,
)

# ========== CSS ===============================

st.markdown("""
<style>

/* ── Welcome screen ──────────────────────────────────────────── */
.welcome-title {
    font-size: 28px;
    font-weight: 600;
    color: #ececec;
    text-align: center;
    margin: 40px 0 24px;
}
.suggestion-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;           
    margin-top: 12px;
}
.suggestion-card {
    
    border: 1px solid #3a3a3a;   
    border-radius: 12px;
    padding: 14px 16px;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
    font-size: 16px;
}
.suggestion-card:hover {
    background: #383838;
    border-color: #4a4a4a;
}
.suggestion-card .card-title { font-weight: 600; color: #ececec; }
.suggestion-card .card-sub   { color: #8e8ea0; font-size: 12px; margin-top: 3px; }

/* ── Source block (FIX ST-6) ─────────────────────────────────── */
.source-block {
    background: #2a2a2a;
    border: 1px solid #3a3a3a;
    border-radius: 8px;
    padding: 8px 14px;
    margin-top: 10px;
    font-size: 11.5px;
    color: #8e8ea0;
    font-family: 'Courier New', monospace;
    line-height: 1.7;
}
.source-label {
    font-family: system-ui, sans-serif;
    font-size: 11px;
    font-weight: 700;
    color: #19c37d;
    margin-bottom: 4px;
    letter-spacing: 0.5px;
}

/* ── Status pill (FIX ST-6) ──────────────────────────────────── */
.status-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 12px;
    color: #8e8ea0;
}
.status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #19c37d;
    flex-shrink: 0;
}
.status-dot.loading {
    background: #fbbf24;
    animation: blink 1s step-end infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0} }

/* ── Bottom note ─────────────────────────────────────────────── */
.bottom-note {
    text-align: center;
    font-size: 12px;
    color: #6b6b7b;
    padding: 12px 0 24px;
    max-width: 720px;
    margin: 0 auto;
}

/* ── Chat messages ───────────────────────────────────────────── */
.stChatMessage { background: transparent !important; border: none !important; }

/* ── Input bar ───────────────────────────────────────────────── */
.stChatInputContainer > div {
    background: #2f2f2f !important;
    border: 1px solid #3a3a3a !important;
    border-radius: 14px !important;
}
.stChatInputContainer textarea {
    background: transparent !important;
    color: #ececec !important;
}
.stChatInputContainer textarea::placeholder { color: #6b6b7b !important; }

/* ── Scrollbar ───────────────────────────────────────────────── */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-thumb { background: #3a3a3a; border-radius: 3px; }

</style>
""", unsafe_allow_html=True)



#  ==================== UTILITIES ==================================


def chat_label() -> str:
    return datetime.now().strftime("%d %b %Y, %H:%M:%S")


def _load_api_key() -> str:
    key = os.environ.get("GROQ_API_KEY", "")
    if key:
        return key
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GROQ_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    try:
        return st.secrets.get("GROQ_API_KEY", "")
    except Exception:
        return ""


 
# CACHED PIPELINE LOADING
 

@st.cache_resource(show_spinner=False)
def load_pipeline():
    from config import KG_GRAPH_JSON, RAG_INDEX_FILE
    from rag.embedder import Embedder
    from rag.langgraph_chatbot import register_retriever
    from rag.retriever import Retriever
    from rag.vector_store import VectorStore

    if not RAG_INDEX_FILE.exists():
        return None, None, None, (
            f"FAISS index not found at `{RAG_INDEX_FILE}`.\n\n"
            "Build it first:\n```\npython chatbot_main.py --mode build\n```"
        )
    try:
        embedder = Embedder()
        embedder.load()

        vector_store = VectorStore()
        vector_store.load()

        graph_kg = None
        try:
            from knowledge_graph.graph.graph_store import GraphStore
            from config import KG_GRAPH_JSON
            if KG_GRAPH_JSON.exists():
                graph_kg = GraphStore().load_json()
        except Exception:
            pass

        retriever = Retriever(vector_store=vector_store, embedder=embedder, graph=graph_kg)
        register_retriever("default", retriever)

        lg_graph = build_graph()
        return retriever, lg_graph, embedder, None

    except Exception as exc:
        return None, None, None, str(exc)


 
# ================= SESSION STATE ==================================
 

def init_session():
   
    defaults = {
        "conv_id":       str(uuid.uuid4()),   
        "conversations": {},
        "turn_count":    0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    if "chat_threads" not in st.session_state:
        st.session_state["chat_threads"] = retrieve_all_threads()

    if not st.session_state["conversations"]:
        _new_conversation()


def _new_conversation() -> str:
    conv_id = str(uuid.uuid4())
    st.session_state["conversations"][conv_id] = {"title": "New chat", "messages": []}
    st.session_state["conv_id"]    = conv_id
    st.session_state["turn_count"] = 0
    return conv_id


def _current_messages() -> list:
    cid = st.session_state.get("conv_id")
    if cid and cid in st.session_state["conversations"]:
        return st.session_state["conversations"][cid]["messages"]
    return []


def _add_message(role: str, content: str, sources: str = "") -> None:
    """
    Append a message.

    """
    cid = st.session_state.get("conv_id")
    if not cid or cid not in st.session_state["conversations"]:
        return

    st.session_state["conversations"][cid]["messages"].append(
        {"role": role, "content": content, "sources": sources}
    )

    conv = st.session_state["conversations"][cid]
    # Only title-generate for the very first user message
    if role == "user" and conv["title"] == "New chat":
        try:
            title = generate_chat_title(content)
        except Exception:
            title = content[:25]
        conv["title"] = title
        _add_thread(cid, title)


def _add_thread(thread_id: str, title: str) -> None:
    """Register thread in sidebar list + SQLite (idempotent)."""
    for t in st.session_state["chat_threads"]:
        if t["id"] == thread_id:
            return   # already registered
    label = chat_label()
    st.session_state["chat_threads"].append(
        {"id": thread_id, "title": title, "label": label}
    )
    save_thread_metadata(thread_id, title, label)


def load_conversation(thread_id: str) -> list:
    """
    Load messages from LangGraph checkpoint.
    """
    workflow = st.session_state.get("graph")
    if not workflow:
        return []
    try:
        state = workflow.get_state(config={"configurable": {"thread_id": thread_id}})
        # messages are stored as plain dicts {"role": ..., "content": ...}
        return state.values.get("messages", [])
    except Exception as exc:
        log.warning(f"load_conversation failed for {thread_id}: {exc}")
        return []


 
# SIDEBAR
 

def render_sidebar(retriever, error_msg: str) -> None:
    with st.sidebar:
        st.markdown("# SatGPT")
        st.caption("Space Applications Centre · ISRO")
        st.caption("Ask. Explore. Discover Satellite Data ")
        st.markdown("---")

        if st.button("  New chat", use_container_width=True):
            _new_conversation()
            st.rerun()

        # Status pill
        st.markdown("<br>", unsafe_allow_html=True)
        if error_msg:
            st.markdown(
                '<div class="status-pill"><div class="status-dot loading"></div> Index not loaded</div>',
                unsafe_allow_html=True,
            )
        else:
            n = 0
            try:
                n = retriever.store.size if retriever else 0
            except Exception:
                pass
            st.markdown(
                f'<div class="status-pill"><div class="status-dot"></div> {n:,} vectors ready</div>',
                unsafe_allow_html=True,
            )

        # Chat history
        st.markdown("<br>", unsafe_allow_html=True)
        if st.session_state["chat_threads"]:
            st.markdown("### Chats History")

        for thread in st.session_state["chat_threads"][::-1]:
            label       = thread.get("label", "")
            short_label = label[:6] + ", " + label[12:18] if len(label) >= 18 else label
            btn_label   = f"{thread['title']} · {short_label}"

            if st.button(btn_label, key=f"thread_{thread['id']}", use_container_width=True):
                raw_msgs = load_conversation(thread["id"])

                # Convert plain dicts → local format
                temp_messages = []
                for msg in raw_msgs:
                    # FIX ST-10: use msg["role"] directly (plain dict, no isinstance needed)
                    temp_messages.append({
                        "role":    msg.get("role", "user"),
                        "content": msg.get("content", ""),
                        "sources": "",
                    })

                cid = thread["id"]
                if cid not in st.session_state["conversations"]:
                    st.session_state["conversations"][cid] = {
                        "title":    thread["title"],
                        "messages": [],
                    }
                st.session_state["conversations"][cid]["messages"] = temp_messages
                st.session_state["conv_id"] = cid
                st.rerun()

        # Settings expander
        # st.markdown("---")
        # with st.expander(" Settings"):
        #     api_input = st.text_input(
        #         "Groq API Key", value=_load_api_key(), type="password"
        #     )
        #     if api_input:
        #         os.environ["GROQ_API_KEY"] = api_input
        #     st.caption("Or set GROQ_API_KEY in your .env file")


 
# ======================== CHAT AREA COMPONENTS ==============================
 

SUGGESTIONS = [
    {"title": "What are INSAT-3DR payloads?",        "sub": "Sensors, channels, resolution"},
    {"title": "How do I download data from MOSDAC?", "sub": "Registration & SFTP guide"},
    {"title": "Tell me about ocean surface currents", "sub": "Open data product details"},
    {"title": "What is the INSAT-3D ATBD document?", "sub": "Algorithm technical basis"},
]


def render_welcome() -> None:
    st.markdown('<div class="welcome-title">How can I help you today?</div>', unsafe_allow_html=True)
    cols = st.columns(2)
    for i, s in enumerate(SUGGESTIONS):
        with cols[i % 2]:
            st.markdown(
                f'<div class="suggestion-card">'
                f'<div class="card-title">{s["title"]}</div>'
                f'<div class="card-sub">{s["sub"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


def render_messages() -> None:
    for msg in _current_messages():
        avatar = "🛰️" if msg["role"] == "assistant" else "👤"
        with st.chat_message(msg["role"], avatar=avatar):
            st.markdown(msg["content"])
            if msg.get("sources"):
                st.markdown(
                    f'<div class="source-block">'
                    f'<div class="source-label">📎 Sources</div>'
                    f'{msg["sources"].replace(chr(10), "<br>")}'
                    f'</div>',
                    unsafe_allow_html=True,
                )


def render_error(error_msg: str) -> None:
    st.markdown(
        '<div style="max-width:600px;margin:60px auto;padding:24px;'
        'background:#2f2f2f;border:1px solid #3a3a3a;border-radius:12px;">'
        '<div style="font-size:18px;font-weight:700;color:#ececec;margin-bottom:12px;">'
        ' RAG Index Not Found</div>',
        unsafe_allow_html=True,
    )
    st.error(error_msg)


 
# =================  STREAMING DISPLAY =========================================
 

def stream_and_display(query: str, retriever, lg_graph) -> tuple:
    """Stream Groq response token-by-token. Returns (answer, sources)."""
    from rag.langgraph_chatbot import stream_response

    if not _load_api_key():
        st.error("GROQ_API_KEY not set. Add it in Settings or your .env file.")
        return "", ""

    conv_id = st.session_state["conv_id"]

    with st.chat_message("assistant", avatar="🛰️"):
        container      = st.empty()
        full_tokens:   list = []
        sources_buf:   list = []
        in_sources:    bool = False

        for token in stream_response(
            query=query, session_id=conv_id, graph=lg_graph, retriever_key="default"
        ):
            if "__SOURCES__" in token:
                in_sources = True
                parts = token.split("__SOURCES__", 1)
                if parts[0]:
                    full_tokens.append(parts[0])
                if len(parts) > 1:
                    sources_buf.append(parts[1])
                container.markdown("".join(full_tokens) + "▌")
                continue

            if in_sources:
                sources_buf.append(token)
            else:
                full_tokens.append(token)
                container.markdown("".join(full_tokens) + "▌")

        full_answer = "".join(full_tokens).strip()
        sources     = "".join(sources_buf).strip()

        container.markdown(full_answer)

        if sources:
            st.markdown(
                f'<div class="source-block">'
                f'<div class="source-label">📎 Sources</div>'
                f'{sources.replace(chr(10), "<br>")}'
                f'</div>',
                unsafe_allow_html=True,
            )

    return full_answer, sources


 
# MAIN
 

import logging
log = logging.getLogger(__name__)


def main() -> None:
    
    st.markdown(
        '<h2 style="text-align:center;margin-bottom:2px;"> MOSDAC Help Desk</h2>'
        '<p style="text-align:center;color:#8e8ea0;font-size:13px;margin-bottom:0;">'
        'Meteorological &amp; Oceanographic Satellite Data Archival Centre · ISRO</p>',
        unsafe_allow_html=True,
    )

    init_session()

    with st.spinner("Loading MOSDAC RAG pipeline…"):
        retriever, lg_graph, embedder, error_msg = load_pipeline()

    # Store graph in session for load_conversation
    st.session_state["graph"] = lg_graph
    

    render_sidebar(retriever, error_msg)

    if error_msg:
        render_error(error_msg)
        st.stop()

    messages = _current_messages()
    if not messages:
        render_welcome()
    else:
        render_messages()

    st.markdown(
        '<div class="bottom-note">'
        'MOSDAC AI Help Bot may make mistakes. Verify at '
        '<a href="https://mosdac.gov.in" target="_blank" style="color:#6b6b7b;">mosdac.gov.in</a>'
        '</div>',
        unsafe_allow_html=True,
    )

    if prompt := st.chat_input("Ask about MOSDAC missions, data, or portal help…"):
        _add_message("user", prompt)

        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        answer, sources = stream_and_display(prompt, retriever, lg_graph)

        if answer:
            _add_message("assistant", answer, sources)
            st.session_state["turn_count"] += 1

        st.rerun()


if __name__ == "__main__":
    main()