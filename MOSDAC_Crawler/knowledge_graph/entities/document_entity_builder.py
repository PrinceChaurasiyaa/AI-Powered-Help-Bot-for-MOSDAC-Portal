"""
knowledge_graph/entities/document_entity_builder.py
─────────────────────────────────────────────────────────────────────────────
Builds Document KGNodes from the documents table.

Documents are the richest data source — 29 PDFs including ATBDs (Algorithm
Theoretical Basis Documents), product specifications, handbooks, and
calibration reports.  Each gets a KGNode that links to its source mission
where the filename contains a recognisable mission keyword.

Source:  DB documents WHERE extraction_ok = 1

Nodes produced:
  Document  node_id = 'doc:{url_hash}'

Edges produced:
  Mission -[DOCUMENTED_BY]→ Document  (when mission inferred from filename)
─────────────────────────────────────────────────────────────────────────────
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import KNOWN_MISSIONS
from knowledge_graph.entities.base import KGEdge, KGNode
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)

# Document type classification from filename keywords
DOC_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "ATBD":        ["atbd", "algorithm", "theoretical"],
    "Handbook":    ["handbook", "manual", "guide"],
    "Product Spec":["product", "specification", "products"],
    "Report":      ["report", "validation", "calibration"],
    "Newsletter":  ["newsletter"],
    "API Manual":  ["api", "download"],
    "Announcement":["announcement", "eruption", "cyclone", "seasonal"],
    "Data Policy": ["guidelines", "policy"],
}

# Mission keyword → slug (for linking documents to missions)
DOC_MISSION_KEYWORDS: Dict[str, str] = {
    "insat3d":        "insat-3d",
    "insat-3d":       "insat-3d",
    "insat3dr":       "insat-3dr",
    "insat-3dr":      "insat-3dr",
    "insat3ds":       "insat-3ds",
    "insat-3ds":      "insat-3ds",
    "insat3s":        "insat-3ds",
    "kalpana":        "kalpana-1",
    "megha":          "megha-tropiques",
    "mt_atbd":        "megha-tropiques",
    "saphir":         "megha-tropiques",
    "madras":         "megha-tropiques",
    "scarab":         "megha-tropiques",
    "saral":          "saral-altika",
    "altika":         "saral-altika",
    "salp":           "saral-altika",
    "oceansat":       "oceansat-2",
    "ocm":            "oceansat-2",
    "scatsat":        "scatsat-1",
    "scat":           "scatsat-1",
    "atbd_scat":      "scatsat-1",
}


class DocumentEntityBuilder:
    """
    Wraps downloaded PDF documents as KGNodes.

    Usage:
        builder = DocumentEntityBuilder(store)
        nodes, edges = builder.extract()
    """

    def __init__(self, store: DataStore):
        self.store = store

    def extract(self) -> Tuple[List[KGNode], List[KGEdge]]:
        """Return Document nodes and Mission-DOCUMENTED_BY-Document edges."""
        docs  = self.store.get_all_documents(min_chars=0)
        nodes: List[KGNode] = []
        edges: List[KGEdge] = []

        for doc in docs:
            node = self._build_node(doc)
            nodes.append(node)

            # Try to link to a mission
            mission_slug = self._infer_mission(
                doc.get("filename", ""), doc.get("url", "")
            )
            if mission_slug:
                edges.append(KGEdge(
                    source_id     = f"mission:{mission_slug}",
                    target_id     = node.node_id,
                    relation_type = "DOCUMENTED_BY",
                    attributes    = {"filename": doc.get("filename", "")},
                ))

        log.info(
            f"DocumentEntityBuilder: {len(nodes)} document nodes, "
            f"{len(edges)} mission links"
        )
        return nodes, edges

    def _build_node(self, doc: dict) -> KGNode:
        filename     = doc.get("filename", "unknown.pdf")
        url          = doc.get("url", "")
        url_hash     = doc.get("url_hash", "")
        page_count   = doc.get("page_count") or 0
        file_size_kb = doc.get("file_size_kb") or 0.0
        raw_text     = doc.get("extracted_text") or ""

        doc_type  = self._classify_doc_type(filename)
        label     = self._make_label(filename, doc_type)
        text      = self._build_text(label, filename, raw_text)

        attrs = {
            "filename":     filename,
            "file_type":    doc.get("file_type", ".pdf"),
            "doc_type":     doc_type,
            "page_count":   str(page_count),
            "file_size_kb": f"{file_size_kb:.1f}",
            "has_text":     str(len(raw_text) > 50),
            "char_count":   str(len(raw_text)),
        }
        if doc.get("source_page_url"):
            attrs["linked_from"] = doc["source_page_url"]

        return KGNode(
            node_id    = f"doc:{url_hash}",
            node_type  = "Document",
            label      = label,
            source_url = url,
            attributes = attrs,
            text       = text,
        )

    def _classify_doc_type(self, filename: str) -> str:
        fname_lower = filename.lower()
        for doc_type, keywords in DOC_TYPE_KEYWORDS.items():
            if any(kw in fname_lower for kw in keywords):
                return doc_type
        return "Document"

    def _make_label(self, filename: str, doc_type: str) -> str:
        """Create a clean human-readable label from filename."""
        stem = Path(filename).stem
        # Remove URL encoding artifacts
        stem = re.sub(r"%20", " ", stem)
        stem = re.sub(r"[_\-]+", " ", stem)
        stem = re.sub(r"\s+", " ", stem).strip()
        return f"{stem} ({doc_type})"[:120]

    def _infer_mission(self, filename: str, url: str) -> Optional[str]:
        """Return mission slug if this document clearly belongs to a mission."""
        text = (filename + " " + url).lower()
        for keyword, slug in DOC_MISSION_KEYWORDS.items():
            if keyword in text:
                return slug
        return None

    def _build_text(
        self, label: str, filename: str, raw_text: str
    ) -> str:
        """
        Build RAG-ready text for this document.
        Uses first 1200 chars of extracted PDF text.
        """
        if raw_text and len(raw_text) > 50:
            snippet = raw_text[:1200].replace("\n\n", " ").replace("\n", " ")
            return f"{label}. Document: {filename}. Content: {snippet}"
        return f"{label}. Source document: {filename}. (Full text available in documents table.)"
