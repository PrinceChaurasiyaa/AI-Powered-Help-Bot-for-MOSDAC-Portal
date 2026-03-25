"""
knowledge_graph/entities/base.py
─────────────────────────────────────────────────────────────────────────────
Core dataclasses for Knowledge Graph nodes and edges.

Every entity type in the MOSDAC KG is a KGNode.
Every relationship between two entities is a KGEdge.

Node types used in Phase 2:
  Mission          — A satellite mission (INSAT-3D, OCEANSAT-3 …)
  MissionSection   — A sub-page of a mission (payloads, spacecraft …)
  Payload          — A sensor/instrument carried by a Mission
  OpenDataProduct  — A freely downloadable derived data product
  FAQ              — A question-answer pair from the FAQ page
  Document         — A PDF or other document (ATBD, handbook …)

Edge (relationship) types:
  HAS_SECTION      Mission → MissionSection
  HAS_PAYLOAD      Mission → Payload
  DOCUMENTED_BY    Mission → Document
  PRODUCES         Payload → OpenDataProduct  (when known)
  PART_OF          MissionSection → Mission  (reverse navigation)
  RELATED_TO       Generic weak relationship
─────────────────────────────────────────────────────────────────────────────
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ── Valid node types ────────────────────────────────────────────
NODE_TYPES = {
    "Mission",
    "MissionSection",
    "Payload",
    "OpenDataProduct",
    "FAQ",
    "Document",
    "Organization",
}

# ── Valid edge (relationship) types ─────────────────────────────
EDGE_TYPES = {
    "HAS_SECTION",
    "HAS_PAYLOAD",
    "DOCUMENTED_BY",
    "PRODUCES",
    "PART_OF",
    "RELATED_TO",
    "CATEGORY",
}


@dataclass
class KGNode:
    """
    A node in the Knowledge Graph.

    node_id   : Unique identifier  e.g. 'mission:insat-3d'
    node_type : One of NODE_TYPES  e.g. 'Mission'
    label     : Human-readable name e.g. 'INSAT-3D'
    source_url: URL this entity was extracted from (for traceability)
    attributes: Domain-specific attributes (orbit_type, channels, etc.)
    text       : Full text content used for RAG retrieval
    """
    node_id:    str
    node_type:  str
    label:      str
    source_url: str                        = ""
    attributes: Dict[str, Any]            = field(default_factory=dict)
    text:       str                        = ""

    def __post_init__(self):
        if self.node_type not in NODE_TYPES:
            raise ValueError(
                f"Unknown node_type '{self.node_type}'. "
                f"Valid: {sorted(NODE_TYPES)}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to plain dict (for JSON export)."""
        return {
            "node_id":    self.node_id,
            "node_type":  self.node_type,
            "label":      self.label,
            "source_url": self.source_url,
            "attributes": self.attributes,
            "text":       self.text,
        }

    @property
    def flat_attrs(self) -> Dict[str, str]:
        """
        Return attributes as flat str→str dict suitable for GraphML.
        GraphML requires all attribute values to be scalars.
        """
        flat = {
            "node_type":  self.node_type,
            "label":      self.label,
            "source_url": self.source_url,
            "text_len":   str(len(self.text)),
        }
        for k, v in self.attributes.items():
            if isinstance(v, (str, int, float, bool)):
                flat[k] = str(v)
            elif isinstance(v, list):
                flat[k] = "; ".join(str(i) for i in v)
        return flat


@dataclass
class KGEdge:
    """
    A directed edge (relationship) in the Knowledge Graph.

    source_id     : node_id of the source node
    target_id     : node_id of the target node
    relation_type : One of EDGE_TYPES
    attributes    : Optional metadata (confidence, weight …)
    """
    source_id:     str
    target_id:     str
    relation_type: str
    attributes:    Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.relation_type not in EDGE_TYPES:
            raise ValueError(
                f"Unknown relation_type '{self.relation_type}'. "
                f"Valid: {sorted(EDGE_TYPES)}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id":     self.source_id,
            "target_id":     self.target_id,
            "relation_type": self.relation_type,
            "attributes":    self.attributes,
        }
