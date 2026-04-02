"""
chatbot_main.py
─────────────────────────────────────────────────────────────────────────────
MOSDAC AI Help Bot — Phase 3: RAG Pipeline & Conversational Chatbot

Pipeline:
  Step 1  ChunkLoader    — loads + cleans text_chunks.jsonl (Phase 2 output)
  Step 2  Embedder       — embeds all chunks with sentence-transformers
  Step 3  VectorStore    — builds FAISS index, saves to output/rag/
  Step 4  Chatbot        — interactive CLI loop using Claude + RAG

Run:
    python chatbot_main.py                     # Full build + interactive chat
    python chatbot_main.py --mode build        # Build index only (no chat)
    python chatbot_main.py --mode chat         # Chat using existing index
    python chatbot_main.py --mode ask "query"  # Single question, no loop
    python chatbot_main.py --mode report       # Print index stats

API Key:
    Set ANTHROPIC_API_KEY environment variable, OR
    Create a .env file in this directory:
        ANTHROPIC_API_KEY=sk-ant-api03-...
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from config import (
    KG_TEXT_CHUNKS,
    RAG_BUILD_REPORT,
    RAG_DIR,
    RAG_EMBEDDING_MODEL,
    RAG_INDEX_FILE,
    RAG_METADATA_FILE,
    RAG_TOP_K,
)
from knowledge_graph.graph.graph_store import GraphStore
from rag.chatbot import MOSDACChatbot
from rag.chunk_loader import ChunkLoader
from rag.embedder import Embedder
from rag.retriever import Retriever
from rag.vector_store import VectorStore
from utils.logger import get_logger

log     = get_logger(__name__)
console = Console()


# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

class RAGOrchestrator:
    """Coordinates the full Phase 3 RAG pipeline."""

    def __init__(self):
        self.start_time  = time.time()
        self.embedder    = Embedder()
        self.vector_store = VectorStore()

    # ── Full build + chat ─────────────────────────────────────

    def run_full(self) -> None:
        """Build index then launch interactive chat."""
        self._build_index()
        self._launch_chat()

    # ── Build only ────────────────────────────────────────────

    def run_build_only(self) -> None:
        """Build the FAISS index from text_chunks.jsonl."""
        self._build_index()
        self._print_build_summary()

    # ── Chat only ─────────────────────────────────────────────

    def run_chat_only(self) -> None:
        """Launch interactive chat using existing index."""
        if not RAG_INDEX_FILE.exists():
            console.print(
                "[bold red]ERROR:[/bold red] Index not found.\n"
                "Run:  python chatbot_main.py --mode build"
            )
            sys.exit(1)
        self.vector_store.load()
        self._launch_chat()

    # ── Single question ───────────────────────────────────────

    def run_ask(self, question: str) -> None:
        """Answer a single question and exit."""
        if not RAG_INDEX_FILE.exists():
            console.print(
                "[bold red]ERROR:[/bold red] Index not found.\n"
                "Run:  python chatbot_main.py --mode build"
            )
            sys.exit(1)

        self.vector_store.load()
        bot = self._make_chatbot()

        console.print(f"\n[bold cyan]Q:[/bold cyan] {question}\n")
        response = bot.ask(question)
        console.print(Markdown(response.answer))
        if response.sources:
            console.print(f"\n[dim]Sources:\n{response.sources}[/dim]")

    # ── Report ────────────────────────────────────────────────

    def run_report(self) -> None:
        """Print stats from existing index."""
        if not RAG_INDEX_FILE.exists():
            console.print("[yellow]No index found. Run --mode build first.[/yellow]")
            return

        self.vector_store.load()

        # Count types from metadata
        type_counts = Counter(
            c.get("node_type", "?")
            for c in self.vector_store._metadata
        )

        t = Table(title="RAG Index Stats", header_style="bold cyan")
        t.add_column("Node Type", style="dim")
        t.add_column("Chunks",    justify="right", style="bold yellow")
        for ntype, cnt in sorted(type_counts.items()):
            t.add_row(ntype, str(cnt))
        t.add_row("[bold]TOTAL[/bold]",
                  f"[bold]{self.vector_store.size}[/bold]")
        console.print(t)

        console.print(f"\n[dim]Index:[/dim]    {RAG_INDEX_FILE}")
        console.print(f"[dim]Metadata:[/dim]  {RAG_METADATA_FILE}")
        console.print(f"[dim]Model:[/dim]     {RAG_EMBEDDING_MODEL}")

    # ── Build index ───────────────────────────────────────────

    def _build_index(self) -> None:
        """Full index build pipeline."""

        console.print(Panel(
            "[bold cyan]Step 1 of 3:[/bold cyan] Loading + cleaning chunks",
            expand=False,
        ))
        loader = ChunkLoader()
        chunks = loader.load_all()

        stats = loader.stats()
        console.print(
            f"  Loaded [bold]{stats['total_chunks']}[/bold] chunks "
            f"({stats['total_chars']:,} chars, "
            f"avg {stats['avg_chars']} chars/chunk)"
        )
        type_table = Table(header_style="bold cyan", show_header=True)
        type_table.add_column("Type")
        type_table.add_column("Chunks", justify="right", style="yellow")
        for ntype, cnt in sorted(stats["by_type"].items()):
            type_table.add_row(ntype, str(cnt))
        console.print(type_table)

        console.print(Panel(
            "[bold cyan]Step 2 of 3:[/bold cyan] Embedding chunks "
            f"(model: {RAG_EMBEDDING_MODEL})",
            expand=False,
        ))
        self.embedder.load()
        texts   = [c["text"] for c in chunks]
        vectors = self.embedder.embed(texts)

        console.print(Panel(
            "[bold cyan]Step 3 of 3:[/bold cyan] Building FAISS index",
            expand=False,
        ))
        self.vector_store.build(vectors, chunks)
        self.vector_store.save()

        # Write build report
        elapsed = time.time() - self.start_time
        report = {
            "phase":          "Phase 3 — RAG Index",
            "built_at":       datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "total_chunks":   len(chunks),
            "embedding_model": RAG_EMBEDDING_MODEL,
            "index_size":     self.vector_store.size,
            "by_type":        stats["by_type"],
            "output_files": {
                "faiss_index": str(RAG_INDEX_FILE),
                "metadata":    str(RAG_METADATA_FILE),
            },
        }
        RAG_BUILD_REPORT.parent.mkdir(parents=True, exist_ok=True)
        with open(RAG_BUILD_REPORT, "w") as f:
            json.dump(report, f, indent=2)

        console.print(
            f"\n[bold green]✓ Index built:[/bold green] "
            f"{self.vector_store.size} vectors | "
            f"elapsed {elapsed:.1f}s"
        )
        console.print(f"[dim]Saved to: {RAG_DIR}[/dim]")

    # ── Launch interactive chat ───────────────────────────────

    def _launch_chat(self) -> None:
        """Start the interactive chat loop."""
        bot = self._make_chatbot()

        console.print()
        console.print(Panel(
            "[bold green]MOSDAC Help Bot ready![/bold green]\n"
            "[dim]Type your question. Commands: /clear /sources /quit[/dim]",
            expand=False,
        ))
        console.print()

        show_sources = True   # Toggle with /sources command

        while True:
            try:
                query = Prompt.ask("[bold cyan]You[/bold cyan]")
            except (KeyboardInterrupt, EOFError):
                console.print("\n[yellow]Goodbye![/yellow]")
                break

            query = query.strip()
            if not query:
                continue

            # ── Commands ──────────────────────────────────────
            if query.lower() in ("/quit", "/exit", "quit", "exit", "q"):
                console.print("[yellow]Goodbye![/yellow]")
                break

            if query.lower() == "/clear":
                bot.clear_history()
                console.print("[dim]Conversation history cleared.[/dim]")
                continue

            if query.lower() == "/sources":
                show_sources = not show_sources
                state = "ON" if show_sources else "OFF"
                console.print(f"[dim]Source display: {state}[/dim]")
                continue

            if query.lower() == "/help":
                console.print(
                    "[dim]Commands:\n"
                    "  /clear   — clear conversation history\n"
                    "  /sources — toggle source display\n"
                    "  /quit    — exit[/dim]"
                )
                continue

            # ── Stream answer ──────────────────────────────────
            console.print("\n[bold green]Bot:[/bold green]", end=" ")
            full_answer = []
            final_chunks = []

            try:
                # Use streaming for responsive output
                for token in bot.ask_stream(query):
                    console.print(token, end="", markup=False)
                    full_answer.append(token)

                console.print()   # newline after streamed answer

                # Get sources from the last retrieval
                from rag.retriever import Retriever
                chunks = bot.retriever.retrieve.__wrapped__ if hasattr(
                    bot.retriever.retrieve, "__wrapped__"
                ) else None

                # Re-retrieve for source display (cheap, already cached)
                if show_sources:
                    from rag.prompt_builder import format_sources
                    # Re-run retrieve to get chunks (fast, index already loaded)
                    display_chunks = bot.retriever.retrieve(query)
                    sources = format_sources(display_chunks)
                    if sources:
                        console.print(
                            f"\n[dim]📎 Sources:\n{sources}[/dim]"
                        )

            except Exception as exc:
                console.print(f"\n[red]Error: {exc}[/red]")
                log.error(f"Chat error: {exc}", exc_info=True)

            console.print()   # blank line between turns

    # ── Helpers ───────────────────────────────────────────────

    def _make_chatbot(self) -> MOSDACChatbot:
        """Create a chatbot instance with optional KG graph."""
        # Load KG graph if available (for graph-enriched retrieval)
        graph = None
        try:
            from config import KG_GRAPH_JSON
            if KG_GRAPH_JSON.exists():
                graph = GraphStore().load_json()
                log.info("KG graph loaded for graph-enriched retrieval")
        except Exception as exc:
            log.debug(f"KG graph not loaded (optional): {exc}")

        retriever = Retriever(
            vector_store=self.vector_store,
            embedder=self.embedder,
            graph=graph,
        )
        return MOSDACChatbot(retriever=retriever)

    def _print_build_summary(self) -> None:
        elapsed = time.time() - self.start_time
        console.print(Panel(
            f"[bold green]Phase 3 RAG Index Complete[/bold green]\n"
            f"Elapsed: {elapsed:.1f}s | Vectors: {self.vector_store.size}",
            expand=False,
        ))

        ft = Table(title="Output Files", header_style="bold cyan")
        ft.add_column("File",    style="dim")
        ft.add_column("Size",    justify="right")
        ft.add_column("Purpose")
        for path, purpose in [
            (RAG_INDEX_FILE,    "FAISS vector index"),
            (RAG_METADATA_FILE, "Chunk metadata (JSON)"),
            (RAG_BUILD_REPORT,  "Build report"),
        ]:
            size = f"{path.stat().st_size / 1024:.0f} KB" if path.exists() else "—"
            ft.add_row(path.name, size, purpose)
        console.print(ft)
        console.print(f"\n[dim]To start chatting: python chatbot_main.py --mode chat[/dim]")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MOSDAC AI Help Bot — Phase 3: RAG Pipeline & Chatbot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python chatbot_main.py                          # Full build + chat
  python chatbot_main.py --mode build             # Build index only
  python chatbot_main.py --mode chat              # Chat (index must exist)
  python chatbot_main.py --mode ask "What is INSAT-3D?"
  python chatbot_main.py --mode report            # Show index stats

API Key setup:
  export ANTHROPIC_API_KEY=sk-ant-...   (Linux/Mac)
  $env:ANTHROPIC_API_KEY="sk-ant-..."   (Windows PowerShell)
  OR create a .env file:
    ANTHROPIC_API_KEY=sk-ant-api03-...
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "build", "chat", "ask", "report"],
        default="full",
    )
    parser.add_argument(
        "question",
        nargs="?",
        default=None,
        help="Question for --mode ask",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    console.print(Panel(
        "[bold blue]MOSDAC AI Help Bot — Phase 3: RAG Chatbot[/bold blue]\n"
        "[dim]sentence-transformers + FAISS | mosdac.gov.in[/dim]",
        expand=False,
    ))

    # Pre-flight check
    if not KG_TEXT_CHUNKS.exists() and args.mode in ("full", "build"):
        console.print(
            f"[bold red]ERROR:[/bold red] text_chunks.jsonl not found at:\n"
            f"  {KG_TEXT_CHUNKS}\n\n"
            f"Run Phase 2 first:  python kg_main.py"
        )
        sys.exit(1)

    orchestrator = RAGOrchestrator()
    try:
        if args.mode == "full":
            orchestrator.run_full()
        elif args.mode == "build":
            orchestrator.run_build_only()
        elif args.mode == "chat":
            orchestrator.run_chat_only()
        elif args.mode == "ask":
            q = args.question
            if not q:
                console.print(
                    "[red]Provide a question:[/red] "
                    'python chatbot_main.py --mode ask "Your question here"'
                )
                sys.exit(1)
            orchestrator.run_ask(q)
        elif args.mode == "report":
            orchestrator.run_report()

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")
        sys.exit(0)
    except Exception as exc:
        log.critical(f"Fatal: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
