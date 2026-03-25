"""
kg_main.py
─────────────────────────────────────────────────────────────────────────────
MOSDAC AI Help Bot — Phase 2: Knowledge Graph Construction
Reads from the Phase 1 SQLite database and builds the knowledge graph.

Pipeline:
  Step 1  KnowledgeGraphBuilder   — runs all entity extractors
            ├── MissionExtractor      Mission + MissionSection nodes
            ├── PayloadExtractor      Payload nodes (from tables)
            ├── OpenDataExtractor     OpenDataProduct nodes
            ├── FAQEntityBuilder      FAQ nodes
            └── DocumentEntityBuilder Document nodes
  Step 2  GraphStore               — saves JSON + GraphML
  Step 3  TextChunker              — produces text_chunks.jsonl for RAG
  Step 4  Neo4jExporter            — optional Cypher file for Neo4j

Run:
    python kg_main.py                    # Full build (recommended)
    python kg_main.py --mode build       # Build and save graph
    python kg_main.py --mode chunk       # Re-chunk from existing graph JSON
    python kg_main.py --mode neo4j       # Re-export Cypher from existing JSON
    python kg_main.py --mode report      # Print stats from existing graph JSON
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import networkx as nx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config import (
    DB_PATH,
    KG_CYPHER_FILE,
    KG_DIR,
    KG_GRAPH_GRAPHML,
    KG_GRAPH_JSON,
    KG_REPORT_FILE,
    KG_TEXT_CHUNKS,
    OUTPUT_DIR,
)
from knowledge_graph.graph.graph_builder import KnowledgeGraphBuilder
from knowledge_graph.graph.graph_store import GraphStore
from knowledge_graph.graph.neo4j_exporter import Neo4jExporter
from knowledge_graph.graph.text_chunker import TextChunker
from storage.data_store import DataStore
from utils.logger import get_logger

log     = get_logger(__name__)
console = Console()


# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

class KGOrchestrator:
    """Coordinates the full Phase 2 Knowledge Graph pipeline."""

    def __init__(self):
        self.store      = DataStore()
        self.start_time = time.time()
        self.store_io   = GraphStore()

    # ── Full build ────────────────────────────────────────────

    def run_full_build(self) -> None:
        """
        Full pipeline:
          1. Build graph from Phase 1 DB
          2. Save JSON + GraphML
          3. Generate RAG text chunks
          4. Export Neo4j Cypher
          5. Print summary + write report JSON
        """
        console.print(Panel(
            "[bold cyan]Step 1 of 4:[/bold cyan] Building Knowledge Graph "
            "from Phase 1 database",
            expand=False,
        ))
        G = self._build_graph()

        console.print(Panel(
            "[bold cyan]Step 2 of 4:[/bold cyan] Saving graph to disk",
            expand=False,
        ))
        self._save_graph(G)

        console.print(Panel(
            "[bold cyan]Step 3 of 4:[/bold cyan] Generating RAG text chunks",
            expand=False,
        ))
        self._chunk_text(G)

        console.print(Panel(
            "[bold cyan]Step 4 of 4:[/bold cyan] Exporting Neo4j Cypher",
            expand=False,
        ))
        self._export_neo4j(G)

        self._print_summary(G)

    # ── Partial modes ─────────────────────────────────────────

    def run_build_only(self) -> None:
        """Build and save graph without chunking or Cypher."""
        G = self._build_graph()
        self._save_graph(G)
        self._print_summary(G)

    def run_chunk_only(self) -> None:
        """Re-generate text chunks from the existing saved graph."""
        G = self._load_existing()
        if G is None:
            return
        self._chunk_text(G)
        log.info("Chunking complete.")

    def run_neo4j_only(self) -> None:
        """Re-generate Cypher from the existing saved graph."""
        G = self._load_existing()
        if G is None:
            return
        self._export_neo4j(G)
        log.info("Cypher export complete.")

    def run_report_only(self) -> None:
        """Print stats from existing saved graph."""
        G = self._load_existing()
        if G is None:
            return
        self._print_summary(G)

    # ── Internal steps ────────────────────────────────────────

    def _build_graph(self) -> nx.DiGraph:
        builder = KnowledgeGraphBuilder(self.store)
        return builder.build()

    def _save_graph(self, G: nx.DiGraph) -> None:
        self.store_io.save(G)

    def _chunk_text(self, G: nx.DiGraph) -> None:
        chunker = TextChunker(G, self.store)
        n = chunker.chunk_all()
        log.info(f"Text chunks: {n}")

    def _export_neo4j(self, G: nx.DiGraph) -> None:
        exporter = Neo4jExporter()
        n = exporter.export(G)
        log.info(f"Cypher statements: {n}")

    def _load_existing(self) -> nx.DiGraph | None:
        """Load the graph built by a previous run."""
        if not KG_GRAPH_JSON.exists():
            log.error(
                f"No graph found at {KG_GRAPH_JSON}. "
                "Run: python kg_main.py --mode build"
            )
            return None
        return self.store_io.load_json()

    # ── Summary ───────────────────────────────────────────────

    def _print_summary(self, G: nx.DiGraph) -> None:
        elapsed = time.time() - self.start_time

        # Node type breakdown
        type_counts = Counter(
            G.nodes[n].get("node_type", "?") for n in G.nodes
        )
        # Edge type breakdown
        rel_counts = Counter(
            G.edges[e].get("relation_type", "?") for e in G.edges
        )

        console.print()
        console.print(Panel(
            f"[bold green]MOSDAC Phase 2 Complete[/bold green]\n"
            f"Elapsed: {elapsed:.1f}s | "
            f"Nodes: {G.number_of_nodes()} | "
            f"Edges: {G.number_of_edges()}",
            expand=False,
        ))

        # Node table
        nt = Table(title="Knowledge Graph — Node Types", header_style="bold cyan")
        nt.add_column("Node Type",  style="dim")
        nt.add_column("Count",      justify="right", style="bold yellow")
        for ntype, count in sorted(type_counts.items()):
            nt.add_row(ntype, str(count))
        nt.add_row("[bold]TOTAL[/bold]", f"[bold]{G.number_of_nodes()}[/bold]")
        console.print(nt)

        # Edge table
        et = Table(title="Knowledge Graph — Relationships", header_style="bold cyan")
        et.add_column("Relationship",  style="dim")
        et.add_column("Count",         justify="right", style="bold yellow")
        for rel, count in sorted(rel_counts.items()):
            et.add_row(rel, str(count))
        et.add_row("[bold]TOTAL[/bold]", f"[bold]{G.number_of_edges()}[/bold]")
        console.print(et)

        # Output files table
        ft = Table(title="Output Files", header_style="bold cyan")
        ft.add_column("File",    style="dim")
        ft.add_column("Size",    justify="right")
        ft.add_column("Purpose")
        for path, purpose in [
            (KG_GRAPH_JSON,    "Primary KG (JSON)"),
            (KG_GRAPH_GRAPHML, "Visualisation (GraphML)"),
            (KG_TEXT_CHUNKS,   "RAG chunks (JSONL)"),
            (KG_CYPHER_FILE,   "Neo4j import (Cypher)"),
        ]:
            if path.exists():
                size = f"{path.stat().st_size / 1024:.1f} KB"
            else:
                size = "—"
            ft.add_row(path.name, size, purpose)
        console.print(ft)

        console.print(f"\n[dim]Outputs:[/dim] {KG_DIR}")

        # Write JSON report
        chunk_count = 0
        if KG_TEXT_CHUNKS.exists():
            with open(KG_TEXT_CHUNKS, encoding="utf-8") as f:
                chunk_count = sum(1 for _ in f)

        report = {
            "phase":               "Phase 2 — Knowledge Graph",
            "built_at":            datetime.utcnow().isoformat() + "Z",
            "elapsed_seconds":     round(elapsed, 1),
            "nodes_total":         G.number_of_nodes(),
            "edges_total":         G.number_of_edges(),
            "node_type_breakdown": dict(type_counts),
            "edge_type_breakdown": dict(rel_counts),
            "rag_chunks_total":    chunk_count,
            "output_files": {
                "graph_json":    str(KG_GRAPH_JSON),
                "graph_graphml": str(KG_GRAPH_GRAPHML),
                "text_chunks":   str(KG_TEXT_CHUNKS),
                "cypher":        str(KG_CYPHER_FILE),
                "report":        str(KG_REPORT_FILE),
            },
        }
        KG_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(KG_REPORT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        log.info(f"KG report → {KG_REPORT_FILE}")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MOSDAC AI Help Bot — Phase 2: Knowledge Graph Construction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kg_main.py                      # Full build (recommended)
  python kg_main.py --mode build         # Build + save graph only
  python kg_main.py --mode chunk         # Re-chunk from saved graph
  python kg_main.py --mode neo4j         # Re-export Cypher from saved graph
  python kg_main.py --mode report        # Print stats from saved graph
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "build", "chunk", "neo4j", "report"],
        default="full",
        help="Which step(s) to run (default: full)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    console.print(Panel(
        "[bold blue]MOSDAC AI Help Bot — Phase 2: Knowledge Graph[/bold blue]\n"
        "[dim]Reads Phase 1 DB → builds graph → generates RAG chunks[/dim]",
        expand=False,
    ))

    # Pre-flight check: Phase 1 DB must exist
    if not DB_PATH.exists():
        console.print(
            f"[bold red]ERROR:[/bold red] Phase 1 database not found at:\n"
            f"  {DB_PATH}\n\n"
            f"Run Phase 1 first:  python main.py"
        )
        sys.exit(1)

    orchestrator = KGOrchestrator()
    try:
        if args.mode == "full":
            orchestrator.run_full_build()
        elif args.mode == "build":
            orchestrator.run_build_only()
        elif args.mode == "chunk":
            orchestrator.run_chunk_only()
        elif args.mode == "neo4j":
            orchestrator.run_neo4j_only()
        elif args.mode == "report":
            orchestrator.run_report_only()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")
        sys.exit(0)
    except Exception as exc:
        log.critical(f"Fatal: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
