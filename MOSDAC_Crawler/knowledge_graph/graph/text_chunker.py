"""
knowledge_graph/graph/text_chunker.py
─────────────────────────────────────────────────────────────────────────────
Produces RAG-ready text chunks from the Knowledge Graph.

Every node in the KG has a `text` field.  For Phase 3 (chatbot with RAG),
we need chunks that are:
  • Small enough to fit in a retrieval context window
  • Large enough to be self-contained and meaningful
  • Tagged with metadata so the chatbot knows what it retrieved

Output: text_chunks.jsonl  (one JSON object per line)

Each chunk record:
{
  "chunk_id":    "mission:insat-3d:0",
  "node_id":     "mission:insat-3d",
  "node_type":   "Mission",
  "label":       "INSAT-3D",
  "source_url":  "https://www.mosdac.gov.in/insat-3d",
  "chunk_index": 0,
  "total_chunks": 2,
  "text":        "INSAT-3D is a multipurpose geostationary spacecraft …"
}

Documents (PDFs) also get chunked from their extracted_text field directly,
since they often contain far more text than the KG node stores.
─────────────────────────────────────────────────────────────────────────────
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import networkx as nx

from config import (
    KG_CHUNK_MAX_CHARS,
    KG_CHUNK_OVERLAP,
    KG_TEXT_CHUNKS,
)
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)


class TextChunker:
    """
    Generates JSONL text chunks from a built Knowledge Graph.

    Usage:
        chunker = TextChunker(G, store)
        n = chunker.chunk_all()   # writes text_chunks.jsonl, returns count
    """

    def __init__(
        self,
        G: nx.DiGraph,
        store: DataStore,
        output_path: Path = KG_TEXT_CHUNKS,
        max_chars: int     = KG_CHUNK_MAX_CHARS,
        overlap: int       = KG_CHUNK_OVERLAP,
    ):
        self.G           = G
        self.store       = store
        self.output_path = output_path
        self.max_chars   = max_chars
        self.overlap     = overlap

    # ── Public API ────────────────────────────────────────────

    def chunk_all(self) -> int:
        """
        Generate all chunks from the KG and write to JSONL.
        Returns total chunk count.
        """
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        total = 0

        with open(self.output_path, "w", encoding="utf-8") as f:

            # ── Chunks from KG nodes ──────────────────────────
            for node_id, attrs in self.G.nodes(data=True):
                text = attrs.get("text", "")
                if not text or len(text.strip()) < 20:
                    continue

                node_type  = attrs.get("node_type", "Unknown")
                label      = attrs.get("label", node_id)
                source_url = attrs.get("source_url", "")

                # Enrich FAQ chunks with Q+A format
                if node_type == "FAQ":
                    text = self._enrich_faq_text(attrs)

                chunks = self._split_text(text)
                for i, chunk in enumerate(chunks):
                    record = {
                        "chunk_id":     f"{node_id}:{i}",
                        "node_id":      node_id,
                        "node_type":    node_type,
                        "label":        label,
                        "source_url":   source_url,
                        "chunk_index":  i,
                        "total_chunks": len(chunks),
                        "text":         chunk,
                        "char_count":   len(chunk),
                    }
                    # Add key attributes to the chunk record for filtering
                    for key in ["mission_slug", "section_type", "category",
                                "topic", "doc_type", "orbit_type"]:
                        val = attrs.get(key, "")
                        if val:
                            record[key] = val

                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    total += 1

            # ── Extra chunks from full PDF text ───────────────
            # KG nodes store truncated text; here we chunk the full PDF text
            pdf_total = self._chunk_documents(f)
            total += pdf_total

        size_kb = self.output_path.stat().st_size / 1024
        log.info(
            f"TextChunker: {total} chunks written to "
            f"{self.output_path.name} ({size_kb:.1f} KB)"
        )
        return total

    # ── Document chunking (full PDF text) ─────────────────────

    def _chunk_documents(self, file_handle) -> int:
        """
        Chunk full PDF extracted text — much richer than KG node text.
        Only chunks PDFs with >200 chars of extracted text.
        """
        docs  = self.store.get_all_documents(min_chars=200)
        total = 0

        for doc in docs:
            text      = doc.get("extracted_text", "") or ""
            filename  = doc.get("filename", "")
            url       = doc.get("url", "")
            url_hash  = doc.get("url_hash", "")

            if len(text.strip()) < 200:
                continue

            # Check if this doc already has a KG node
            node_id = f"doc:{url_hash}"
            node_label = filename

            chunks = self._split_text(text)
            for i, chunk in enumerate(chunks):
                record = {
                    "chunk_id":     f"{node_id}:full:{i}",
                    "node_id":      node_id,
                    "node_type":    "Document",
                    "label":        node_label,
                    "source_url":   url,
                    "chunk_index":  i,
                    "total_chunks": len(chunks),
                    "text":         chunk,
                    "char_count":   len(chunk),
                    "doc_source":   "pdf_full_text",
                }
                file_handle.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
                total += 1

        log.info(f"  PDF full-text chunks: {total} from {len(docs)} documents")
        return total

    # ── Text splitting ─────────────────────────────────────────

    def _split_text(self, text: str) -> List[str]:
        """
        Split text into overlapping chunks of max_chars characters.

        Strategy:
          - Try to split on paragraph boundaries first (\n\n)
          - Then on sentence boundaries (. / ? / !)
          - Fall back to hard character splits
        """
        text = text.strip()
        if not text:
            return []

        if len(text) <= self.max_chars:
            return [text]

        chunks = []
        start  = 0

        while start < len(text):
            end = start + self.max_chars

            if end >= len(text):
                chunks.append(text[start:].strip())
                break

            # Try paragraph break
            para_break = text.rfind("\n\n", start, end)
            if para_break > start + self.max_chars // 2:
                chunks.append(text[start:para_break].strip())
                start = para_break - self.overlap
                continue

            # Try sentence break
            for sep in (". ", "? ", "! ", "\n"):
                sent_break = text.rfind(sep, start, end)
                if sent_break > start + self.max_chars // 2:
                    chunks.append(text[start:sent_break + 1].strip())
                    start = sent_break + 1 - self.overlap
                    break
            else:
                # Hard split
                chunks.append(text[start:end].strip())
                start = end - self.overlap

        return [c for c in chunks if len(c.strip()) > 20]

    # ── FAQ text enrichment ───────────────────────────────────

    def _enrich_faq_text(self, attrs: Dict[str, Any]) -> str:
        """Format FAQ text as Q+A for clear retrieval."""
        question = attrs.get("question", attrs.get("label", ""))
        answer   = attrs.get("answer", attrs.get("text", ""))
        if question and answer:
            return f"Question: {question}\nAnswer: {answer}"
        return attrs.get("text", "")
