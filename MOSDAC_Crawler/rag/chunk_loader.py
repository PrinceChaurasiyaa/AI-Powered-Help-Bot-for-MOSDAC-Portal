"""
Loads and cleans chunks from text_chunks.jsonl (Phase 2 output).

Key responsibility:
  Strips Drupal PHP/JS artifacts that leaked into OpenDataProduct chunks
  (e.g. "?php drupal_add_library('system', 'ui.accordion')…").
  These hurt embedding quality and confuse the LLM.

Each loaded chunk is a plain dict with at minimum:
  chunk_id, node_id, node_type, label, source_url, text, char_count
plus optional: orbit_type, mission_slug, section_type, category, topic,
               doc_type, chunk_index, total_chunks
"""

import json
import re
from pathlib import Path
from typing import Iterator, List

from config import RAG_CHUNKS_FILE, RAG_NOISE_PATTERNS
from utils.logger import get_logger
log = get_logger(__name__)

# PHP/JS artifacts that appear in some OpenDataProduct chunks
_DRUPAL_NOISE_RE = re.compile(
    r"\?php.*?(?=\n|The|Data|Sea|Soil|Wave|River|Ocean|GPS|Cloud|Bayesian|METEOSAT)",
    re.DOTALL | re.IGNORECASE,
)

# Strip any remaining JS inline snippets
_JS_SNIPPET_RE = re.compile(
    r"(?:jQuery|drupal_add_|heightStyle|collapsible)[^;]*;?",
    re.IGNORECASE,
)

class ChunkLoader:
    """
    Loads, cleans, and validates chunks from text_chunks.jsonl.
    """
    
    def __init__(self, path: Path = RAG_CHUNKS_FILE):
        self.path = path
    
    # ========================= Public API =====================================

    def load_all(self) -> List[dict]:
        """Load all chunks into memory. Returns list of cleaned chunk dicts."""
        chunks = list(self.stream())
        log.info(f"ChunkLoader: loaded {len(chunks)} chunks from {self.path.name}")
        return chunks
    
    def stream(self) -> Iterator[dict]:
        """Yield cleaned chunks one by one (memory-efficient for large files)."""
        if not self.path.exists():
            raise FileNotFoundError(
                f"text_chunks.jsonl not found at {self.path}.\n"
                "Run phase 2 first: python kg_main.py"
            )

        with open(self.path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError as e:
                    log.warning(f"      Skipping malformed JSON at line {line_no}: {e}")
                    continue
                cleaned = self._clean(chunk)
                if cleaned:
                    yield cleaned
    
    #==============================Cleaning =============================================
    def _clean(self, chunk: dict) -> dict | None:
        """
        Clean a single chunk dict.
        Returns None if the chunk should be discarded after cleaning.
        """
        text = chunk.get("text", "")

        #Strip Drupal PHP/JS noise
        text = self._strip_drupal_noise(text)

        #Strip any remaining noise patterns
        for pattern in RAG_NOISE_PATTERNS:
            if pattern in text:
                text = text.replace(pattern, "")
        
        # Normalise whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        # Skip if too short after cleaning
        if len(text) < 30:
            return None
        
        # ── Skip chunks that are pure Hindi text (not useful for En chatbot)
        # If >50% chars are non-ASCII, skip
        non_ascii = sum(1 for c in text if ord(c) > 127)
        if non_ascii > len(text) * 0.5:
            return None
        
        chunk['text'] = text
        chunk['char_count'] = len(text)
        return chunk
    
    def _strip_drupal_noise(self, text: str) -> str:
        """Remove PHP/JS artifacts introduced by Drupal template rendering."""
        
        # Try the targeted regex first
        cleaned = _DRUPAL_NOISE_RE.sub("", text)

        # Fallback: strip any remaining JS snippets
        cleaned = _JS_SNIPPET_RE.sub("", cleaned)

        # Clean up any orphaned '?' at the start
        cleaned = re.sub(r"^\s*\?+\s*", "", cleaned)

        return cleaned.strip()
    
    def stats(self) -> dict:
        """Return a breakdown of chunk counts by node_type."""
        from collections import Counter
        type_counts = Counter()
        total = 0
        total_chars = 0

        for chunk in self.stream():
            type_counts[chunk.get("node_type", "Unknown")] += 1
            total += 1
            total_chars += chunk.get("char_count", 0)
        
        return {
            "total_chunks": total,
            "total_chars": total_chars,
            "avg_chars": total_chars // max(total, 1),
            "by_type": dict(type_counts),
        }