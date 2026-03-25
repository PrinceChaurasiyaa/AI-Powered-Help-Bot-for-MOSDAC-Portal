"""
knowledge_graph/graph/graph_builder.py
─────────────────────────────────────────────────────────────────────────────
Central orchestrator for Phase 2 Knowledge Graph construction.

Calls every entity extractor, collects all KGNodes and KGEdges,
and builds a NetworkX DiGraph with the full MOSDAC knowledge graph.

The graph can then be:
  • Saved to disk (GraphML, JSON)  by graph_store.py
  • Chunked for RAG               by text_chunker.py
  • Exported to Neo4j Cypher      by neo4j_exporter.py

Entity extractors:
  1. MissionExtractor       → Mission + MissionSection nodes
  2. PayloadExtractor        → Payload nodes + HAS_PAYLOAD edges
  3. OpenDataExtractor       → OpenDataProduct nodes
  4. FAQEntityBuilder        → FAQ nodes
  5. DocumentEntityBuilder   → Document nodes + DOCUMENTED_BY edges
─────────────────────────────────────────────────────────────────────────────
"""

import time
from typing import Dict, List, Optional, Tuple

import networkx as nx

from knowledge_graph.entities.base import KGEdge, KGNode
from knowledge_graph.entities.document_entity_builder import DocumentEntityBuilder
from knowledge_graph.entities.faq_entity_builder import FAQEntityBuilder
from knowledge_graph.entities.mission_extractor import MissionExtractor
from knowledge_graph.entities.open_data_extractor import OpenDataExtractor
from knowledge_graph.entities.payload_extractor import PayloadExtractor
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)


class KnowledgeGraphBuilder:
    """
    Builds the MOSDAC Knowledge Graph as a NetworkX DiGraph.

    Usage:
        store   = DataStore()
        builder = KnowledgeGraphBuilder(store)
        G       = builder.build()
        # G is a NetworkX DiGraph with node/edge attributes
    """

    def __init__(self, store: DataStore):
        self.store = store
        self._all_nodes: List[KGNode] = []
        self._all_edges: List[KGEdge] = []

    # ── Public API ────────────────────────────────────────────

    def build(self) -> nx.DiGraph:
        """
        Run all extractors, assemble the graph, return NetworkX DiGraph.

        Steps:
          1. Extract all entities (nodes)
          2. Extract all relationships (edges)
          3. Validate edge endpoints exist
          4. Build NetworkX graph
        """
        t0 = time.time()
        log.info("=" * 60)
        log.info("Knowledge Graph Builder — starting extraction")
        log.info("=" * 60)

        # ── Step 1: Run all extractors ────────────────────────
        self._run_extractors()

        log.info(
            f"Extraction complete: "
            f"{len(self._all_nodes)} nodes, "
            f"{len(self._all_edges)} edges"
        )

        # ── Step 2: Deduplicate nodes ─────────────────────────
        unique_nodes = self._deduplicate_nodes(self._all_nodes)
        log.info(f"After dedup: {len(unique_nodes)} unique nodes")

        # ── Step 3: Build node index ──────────────────────────
        node_index: Dict[str, KGNode] = {n.node_id: n for n in unique_nodes}

        # ── Step 4: Validate and filter edges ─────────────────
        valid_edges = self._validate_edges(self._all_edges, node_index)
        log.info(f"Valid edges: {len(valid_edges)} / {len(self._all_edges)}")

        # ── Step 5: Build NetworkX DiGraph ────────────────────
        G = self._build_networkx(unique_nodes, valid_edges)

        elapsed = time.time() - t0
        log.info(
            f"Graph built in {elapsed:.1f}s — "
            f"{G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
        )
        self._log_graph_stats(G)
        return G

    # ── Extractor orchestration ───────────────────────────────

    def _run_extractors(self) -> None:
        """Call all entity extractors and collect nodes + edges."""

        extractors = [
            ("Missions",       MissionExtractor(self.store)),
            ("Payloads",       PayloadExtractor(self.store)),
            ("Open Data",      OpenDataExtractor(self.store)),
            ("FAQs",           FAQEntityBuilder(self.store)),
            ("Documents",      DocumentEntityBuilder(self.store)),
        ]

        for name, extractor in extractors:
            log.info(f"  [{name}] running extractor…")
            try:
                nodes, edges = extractor.extract()
                self._all_nodes.extend(nodes)
                self._all_edges.extend(edges)
                log.info(f"  [{name}] → {len(nodes)} nodes, {len(edges)} edges")
            except Exception as exc:
                log.error(f"  [{name}] extractor failed: {exc}", exc_info=True)

    # ── Graph assembly ────────────────────────────────────────

    def _deduplicate_nodes(self, nodes: List[KGNode]) -> List[KGNode]:
        """
        Remove duplicate node_ids.
        When the same node_id appears twice, keep the one with longer text
        (more content = more useful for RAG).
        """
        seen: Dict[str, KGNode] = {}
        for node in nodes:
            existing = seen.get(node.node_id)
            if existing is None or len(node.text) > len(existing.text):
                seen[node.node_id] = node
        return list(seen.values())

    def _validate_edges(
        self,
        edges: List[KGEdge],
        node_index: Dict[str, KGNode],
    ) -> List[KGEdge]:
        """
        Remove edges whose source or target does not exist in the node index.
        Logs a warning for each dropped edge.
        """
        valid = []
        dropped = 0
        for edge in edges:
            if edge.source_id not in node_index:
                log.debug(
                    f"  Edge dropped — source not found: "
                    f"{edge.source_id} →[{edge.relation_type}]→ {edge.target_id}"
                )
                dropped += 1
                continue
            if edge.target_id not in node_index:
                log.debug(
                    f"  Edge dropped — target not found: "
                    f"{edge.source_id} →[{edge.relation_type}]→ {edge.target_id}"
                )
                dropped += 1
                continue
            valid.append(edge)

        if dropped:
            log.warning(f"  Dropped {dropped} edges with missing endpoints")
        return valid

    def _build_networkx(
        self,
        nodes: List[KGNode],
        edges: List[KGEdge],
    ) -> nx.DiGraph:
        """Assemble a NetworkX DiGraph from KGNode + KGEdge lists."""
        G = nx.DiGraph()
        G.graph["name"]        = "MOSDAC Knowledge Graph"
        G.graph["phase"]       = "Phase 2"
        G.graph["description"] = (
            "Knowledge graph of MOSDAC satellite missions, payloads, "
            "open data products, FAQs and documents"
        )

        # Add nodes
        for node in nodes:
            G.add_node(
                node.node_id,
                **node.flat_attrs,
                text=node.text,
            )

        # Add edges
        for edge in edges:
            G.add_edge(
                edge.source_id,
                edge.target_id,
                relation_type=edge.relation_type,
                **{k: str(v) for k, v in edge.attributes.items()},
            )

        return G

    # ── Helpers ───────────────────────────────────────────────

    def _log_graph_stats(self, G: nx.DiGraph) -> None:
        """Log a breakdown of node types and edge relation types."""
        from collections import Counter

        type_counts    = Counter(
            G.nodes[n].get("node_type", "?") for n in G.nodes
        )
        rel_counts     = Counter(
            G.edges[e].get("relation_type", "?") for e in G.edges
        )

        log.info("Graph node type breakdown:")
        for ntype, count in sorted(type_counts.items()):
            log.info(f"   {ntype:22s}: {count}")

        log.info("Graph edge relation type breakdown:")
        for rel, count in sorted(rel_counts.items()):
            log.info(f"   {rel:22s}: {count}")

    # ── Accessors (for graph_store / text_chunker) ────────────

    def get_nodes(self) -> List[KGNode]:
        return self._all_nodes

    def get_edges(self) -> List[KGEdge]:
        return self._all_edges
