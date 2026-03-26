"""
rag/chatbot.py
─────────────────────────────────────────────────────────────────────────────
The MOSDAC chatbot engine — combines retrieval with Claude generation.

Architecture:
  User query
       ↓
  Retriever (FAISS search + graph enrichment)
       ↓
  Retrieved chunks (top-k)
       ↓
  PromptBuilder (formats context + history)
       ↓
  Claude API (generates answer)
       ↓
  Response (answer + sources)

The Chatbot class manages:
  • Multi-turn conversation history (up to RAG_HISTORY_MAX_TURNS)
  • API key loading from environment or .env file
  • Streaming and non-streaming response modes
  • Graceful fallback when no context is found
─────────────────────────────────────────────────────────────────────────────
"""

import os
from typing import Generator, List, Optional
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage


from config import (
    RAG_CLAUDE_MODEL,
    RAG_HISTORY_MAX_TURNS,
    RAG_MAX_TOKENS,
    RAG_TEMPERATURE,
)
from rag.prompt_builder import (
    SYSTEM_PROMPT,
    build_messages,
    build_no_context_message,
    format_sources,
)
from rag.retriever import Retriever
from utils.logger import get_logger

log = get_logger(__name__)


class ChatResponse:
    """Container for a single chatbot response."""

    def __init__(
        self,
        answer:   str,
        sources:  str,
        chunks:   List[dict],
        query:    str,
        fallback: bool = False,
    ):
        self.answer   = answer
        self.sources  = sources
        self.chunks   = chunks
        self.query    = query
        self.fallback = fallback   # True if no context was found

    def __str__(self) -> str:
        text = self.answer
        if self.sources:
            text += f"\n\n📎 Sources:\n{self.sources}"
        return text


class MOSDACChatbot:
    """
    MOSDAC AI Help Bot — RAG-powered chatbot using Claude.

    Usage:
        bot = MOSDACChatbot(retriever, api_key="sk-ant-...")
        response = bot.ask("What are INSAT-3D payloads?")
        print(response)

        # Streaming
        for token in bot.ask_stream("How do I download data?"):
            print(token, end="", flush=True)
    """

    def __init__(
        self,
        retriever:  Retriever,
        api_key:    Optional[str] = None,
        model:      str           = RAG_CLAUDE_MODEL,
        max_tokens: int           = RAG_MAX_TOKENS,
        temperature: float        = RAG_TEMPERATURE,
    ):
        self.retriever   = retriever
        self.model       = model
        self._llm = ChatOllama(model="mistral:7b")
        self.max_tokens  = max_tokens
        self.temperature = temperature
        self._history:   List[dict] = []
        #self._client     = None

        # Resolve API key
        # self._api_key = (
        #     api_key
        #     or os.environ.get("ANTHROPIC_API_KEY")
        #     or self._load_from_env_file()
        # )

    # ── API client ─────────────────────────────────────────────

    # def _get_client(self):
    #     """Lazy-init the Anthropic client."""
    #     if self._client is not None:
    #         return self._client

    #     try:
    #         import anthropic
    #     except ImportError:
    #         raise ImportError(
    #             "anthropic not installed.\n"
    #             "Run: pip install anthropic"
    #         )

    #     if not self._api_key:
    #         raise ValueError(
    #             "No Anthropic API key found.\n"
    #             "Set ANTHROPIC_API_KEY environment variable, "
    #             "or create a .env file with:\n"
    #             "  ANTHROPIC_API_KEY=sk-ant-..."
    #         )

    #     self._client = anthropic.Anthropic(api_key=self._api_key)
    #     return self._client

    def _load_from_env_file(self) -> Optional[str]:
        """Try to load API key from .env file in project root."""
        env_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), ".env"
        )
        if not os.path.exists(env_path):
            return None
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("ANTHROPIC_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        return None

    # ── Main ask method ────────────────────────────────────────

    def ask(self, query: str) -> ChatResponse:
        """
        Answer a question using RAG.

        1. Retrieve relevant chunks
        2. Build prompt with context
        3. Call Claude API
        4. Update history
        5. Return ChatResponse
        """
        query = query.strip()
        if not query:
            return ChatResponse(
                answer="Please ask a question.",
                sources="", chunks=[], query=query
            )

        # ── Step 1: Retrieve ──────────────────────────────────
        chunks = self.retriever.retrieve(query)
        fallback = False

        if not chunks:
            log.warning(f"No chunks retrieved for: {query[:80]}")
            # Try keyword search as fallback
            chunks = self.retriever.keyword_search(query, top_k=3)
            if not chunks:
                fallback = True

        # ── Step 2: Build messages ────────────────────────────
        history = self._get_trimmed_history()
        if fallback:
            messages = build_no_context_message(query, history)
        else:
            messages = build_messages(query, chunks, history)

        # ── Step 3: Call Claude ───────────────────────────────
        #answer = self._call_claude(messages)
        answer = self._call_ollama(messages)

        # ── Step 4: Update history ────────────────────────────
        # Store clean version in history (without the context block)
        self._history.append({"role": "user",      "content": query})
        self._history.append({"role": "assistant",  "content": answer})
        self._trim_history()

        # ── Step 5: Build response ────────────────────────────
        sources = format_sources(chunks)
        return ChatResponse(
            answer=answer,
            sources=sources,
            chunks=chunks,
            query=query,
            fallback=fallback,
        )

    # def ask_stream(self, query: str) -> Generator[str, None, None]:
    #     """
    #     Streaming version of ask().
    #     Yields text tokens as they arrive from Claude.
    #     After the stream completes, history is updated.

    #     Usage:
    #         for token in bot.ask_stream("What is MOSDAC?"):
    #             print(token, end="", flush=True)
    #     """
    #     query  = query.strip()
    #     chunks = self.retriever.retrieve(query)
    #     fallback = not bool(chunks)

    #     if fallback:
    #         chunks = self.retriever.keyword_search(query, top_k=3)

    #     history  = self._get_trimmed_history()
    #     messages = (
    #         build_no_context_message(query, history)
    #         if (fallback and not chunks)
    #         else build_messages(query, chunks, history)
    #     )

    #     #client     = self._get_client()
    #     full_text  = []
    #     lc_messages = []

    #     for m in messages:
    #         if m["role"] == "system":
    #             lc_messages.append(SystemMessage(content=m["content"]))
    #         elif m["role"] == "user":
    #             lc_messages.append(HumanMessage(content=m["content"]))
    #         elif m["role"] == "assistant":
    #             lc_messages.append(HumanMessage(content=m["content"]))

    #     for chunk in self._llm.stream(lc_messages):
    #         full_text.append(chunk.content)
    #         yield chunk.content

    #     answer = "".join(full_text)

    #     answer = "".join(full_text)
    #     self._history.append({"role": "user",      "content": query})
    #     self._history.append({"role": "assistant",  "content": answer})
    #     self._trim_history()

    def ask_stream(self, query: str):
        query  = query.strip()
        chunks = self.retriever.retrieve(query)
        fallback = not bool(chunks)

        if fallback:
            chunks = self.retriever.keyword_search(query, top_k=3)

        history  = self._get_trimmed_history()
        messages = (
            build_no_context_message(query, history)
            if (fallback and not chunks)
            else build_messages(query, chunks, history)
        )

        # Convert to LangChain format
        lc_messages = []
        for m in messages:
            if m["role"] == "system":
                lc_messages.append(SystemMessage(content=m["content"]))
            elif m["role"] == "user":
                lc_messages.append(HumanMessage(content=m["content"]))
            elif m["role"] == "assistant":
                lc_messages.append(HumanMessage(content=m["content"]))

        full_text = []

        # Streaming from Ollama
        for chunk in self._llm.stream(lc_messages):
            full_text.append(chunk.content)
            yield chunk.content

        answer = "".join(full_text)

        # Update history
        self._history.append({"role": "user", "content": query})
        self._history.append({"role": "assistant", "content": answer})
        self._trim_history()

    # ── Claude API call ────────────────────────────────────────

    # def _call_claude(self, messages: List[dict]) -> str:
    #     """Non-streaming Claude API call."""
    #     client = self._get_client()
    #     try:
    #         response = client.messages.create(
    #             model=self.model,
    #             max_tokens=self.max_tokens,
    #             temperature=self.temperature,
    #             system=SYSTEM_PROMPT,
    #             messages=messages,
    #         )
    #         return response.content[0].text
    #     except Exception as exc:
    #         log.error(f"Claude API error: {exc}")
    #         return (
    #             "I encountered an error connecting to the AI service. "
    #             f"Error: {exc}\n\n"
    #             "Please check your ANTHROPIC_API_KEY and try again."
    #         )
    
    def _call_ollama(self, messages):
        lc_messages = []

        for m in messages:
            if m["role"] == "system":
                lc_messages.append(SystemMessage(content=m["content"]))
            elif m["role"] == "user":
                lc_messages.append(HumanMessage(content=m["content"]))
            elif m["role"] == "assistant":
                lc_messages.append(HumanMessage(content=m["content"]))

        response = self._llm.invoke(lc_messages)
        return response.content

    # ── History management ─────────────────────────────────────

    def _get_trimmed_history(self) -> List[dict]:
        """Return history with clean (query-only) user turns."""
        return self._history[-RAG_HISTORY_MAX_TURNS * 2:]

    def _trim_history(self) -> None:
        """Keep only the last N turns."""
        max_items = RAG_HISTORY_MAX_TURNS * 2  # user + assistant per turn
        if len(self._history) > max_items:
            self._history = self._history[-max_items:]

    def clear_history(self) -> None:
        """Reset conversation history."""
        self._history = []
        log.info("Conversation history cleared.")

    @property
    def turn_count(self) -> int:
        """Number of conversation turns so far."""
        return len(self._history) // 2
