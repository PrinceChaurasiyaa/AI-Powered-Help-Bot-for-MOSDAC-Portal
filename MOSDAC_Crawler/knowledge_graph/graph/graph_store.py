"""
knowledge_graph/graph/graph_store.py
─────────────────────────────────────────────────────────────────────────────
Saves and loads the MOSDAC Knowledge Graph in multiple formats.

Formats:
  1. JSON (node-link format)   — primary format, used by Phase 3 RAG
  2. GraphML                   — for visualisation in Gephi / yEd / Neo4j
  3. Adjacency summary         — lightweight text summary

All paths are configured in config.py (KG_GRAPH_JSON, KG_GRAPH_GRAPHML).
─────────────────────────────────────────────────────────────────────────────
"""

import json
from pathlib import Path
from typing import Any, Dict

import networkx as nx
from networkx.readwrite import json_graph

from config import KG_GRAPH_GRAPHML, KG_GRAPH_JSON
from utils.logger import get_logger

log = get_logger(__name__)


class GraphStore:
    """
    Saves and loads the MOSDAC Knowledge Graph.

    Usage:
        store = GraphStore()
        store.save(G)
        G2 = store.load_json()
    """

    def __init__(
        self,
        json_path:    Path = KG_GRAPH_JSON,
        graphml_path: Path = KG_GRAPH_GRAPHML,
    ):
        self.json_path    = json_path
        self.graphml_path = graphml_path

    # ── Save ─────────────────────────────────────────────────

    def save(self, G: nx.DiGraph) -> None:
        """Save the graph to all configured formats."""
        self._save_json(G)
        self._save_graphml(G)
        log.info(
            f"Graph saved — "
            f"JSON: {self.json_path.name}, "
            f"GraphML: {self.graphml_path.name}"
        )

    def _save_json(self, G: nx.DiGraph) -> None:
        """
        Save as NetworkX node-link JSON.

        Format:
        {
          "graph": { "name": "...", "phase": "2" },
          "nodes": [ { "id": "mission:insat-3d", "node_type": "Mission", ... } ],
          "links": [ { "source": "...", "target": "...", "relation_type": "..." } ]
        }
        """
        data = json_graph.node_link_data(G)

        # Ensure all values are JSON-serialisable (convert non-str to str)
        data = _make_serialisable(data)

        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        size_kb = self.json_path.stat().st_size / 1024
        log.info(f"  JSON saved: {self.json_path} ({size_kb:.1f} KB)")

    def _save_graphml(self, G: nx.DiGraph) -> None:
        """
        Save as GraphML (XML-based, readable by Gephi, yEd, Neo4j).
        GraphML requires all attribute values to be strings.
        """
        # GraphML doesn't allow None values — convert to empty string
        H = nx.DiGraph()
        H.graph.update(G.graph)

        for node_id, attrs in G.nodes(data=True):
            clean = {k: (str(v) if v is not None else "") for k, v in attrs.items()}
            # Truncate long text fields (GraphML doesn't compress well)
            if "text" in clean and len(clean["text"]) > 500:
                clean["text"] = clean["text"][:500] + "…"
            H.add_node(node_id, **clean)

        for src, tgt, attrs in G.edges(data=True):
            clean = {k: (str(v) if v is not None else "") for k, v in attrs.items()}
            H.add_edge(src, tgt, **clean)

        self.graphml_path.parent.mkdir(parents=True, exist_ok=True)
        nx.write_graphml(H, str(self.graphml_path), encoding="utf-8")

        size_kb = self.graphml_path.stat().st_size / 1024
        log.info(f"  GraphML saved: {self.graphml_path} ({size_kb:.1f} KB)")

    # ── Load ─────────────────────────────────────────────────

    def load_json(self) -> nx.DiGraph:
        """Load graph from JSON file."""
        if not self.json_path.exists():
            raise FileNotFoundError(
                f"Graph JSON not found: {self.json_path}. "
                "Run kg_main.py build first."
            )
        with open(self.json_path, encoding="utf-8") as f:
            data = json.load(f)
        G = json_graph.node_link_graph(data, directed=True)
        log.info(
            f"Graph loaded from JSON: "
            f"{G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
        )
        return G

    def load_graphml(self) -> nx.DiGraph:
        """Load graph from GraphML file."""
        if not self.graphml_path.exists():
            raise FileNotFoundError(
                f"Graph GraphML not found: {self.graphml_path}"
            )
        G = nx.read_graphml(str(self.graphml_path))
        log.info(
            f"Graph loaded from GraphML: "
            f"{G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
        )
        return G

    # ── Query helpers (used by Phase 3 chatbot) ───────────────

    def get_node(self, G: nx.DiGraph, node_id: str) -> Dict[str, Any]:
        """Return all attributes of a node by ID."""
        if node_id not in G:
            return {}
        return dict(G.nodes[node_id])

    def get_neighbours(
        self,
        G: nx.DiGraph,
        node_id: str,
        relation_type: str = None,
    ) -> list:
        """
        Return list of (neighbour_id, edge_attrs) for all outgoing edges.
        Optionally filter by relation_type.
        """
        results = []
        for _, target, attrs in G.out_edges(node_id, data=True):
            if relation_type and attrs.get("relation_type") != relation_type:
                continue
            results.append((target, attrs))
        return results

    def find_nodes_by_type(
        self, G: nx.DiGraph, node_type: str
    ) -> list:
        """Return all node IDs of a given type."""
        return [
            n for n, attrs in G.nodes(data=True)
            if attrs.get("node_type") == node_type
        ]


# ── Serialisation helper ──────────────────────────────────────

def _make_serialisable(obj: Any) -> Any:
    """Recursively convert any non-JSON-safe values to strings."""
    if isinstance(obj, dict):
        return {k: _make_serialisable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_serialisable(i) for i in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)
