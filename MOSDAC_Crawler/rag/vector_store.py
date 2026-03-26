"""
rag/vector_store.py
─────────────────────────────────────────────────────────────────────────────
FAISS-based vector store for semantic chunk retrieval.

Storage layout (output/rag/):
  faiss.index          — binary FAISS index (IVFFlat or Flat depending on size)
  chunk_metadata.json  — list of chunk dicts (parallel to index rows)

Why FAISS:
  • File-based, no server required
  • Handles 2,281 chunks easily in seconds
  • Cosine similarity via L2-normalised inner product (IndexFlatIP)
  • Rebuilds in <5s locally after adding new chunks

Index type selection:
  ≤ 10,000 chunks → IndexFlatIP  (exact search, ~3.5 MB)
  >  10,000 chunks → IndexIVFFlat (approximate, faster at scale)
─────────────────────────────────────────────────────────────────────────────
"""

import json
import time
from pathlib import Path
from typing import List, Tuple

import numpy as np

from config import (
    RAG_EMBEDDING_DIM,
    RAG_INDEX_FILE,
    RAG_METADATA_FILE,
    RAG_MIN_SCORE,
    RAG_TOP_K,
)
from utils.logger import get_logger

log = get_logger(__name__)

# Threshold for switching from exact to approximate index
_IVF_THRESHOLD = 10_000


class VectorStore:
    """
    Builds and searches a FAISS vector index over text chunks.

    Usage — build:
        store = VectorStore()
        store.build(vectors, chunk_metadata)   # builds + saves
        store.save()

    Usage — query:
        store = VectorStore()
        store.load()
        results = store.search(query_vec, top_k=6)
        # results: list of (chunk_dict, score) ordered by score desc
    """

    def __init__(
        self,
        index_path:    Path = RAG_INDEX_FILE,
        metadata_path: Path = RAG_METADATA_FILE,
        dim: int             = RAG_EMBEDDING_DIM,
    ):
        self.index_path    = index_path
        self.metadata_path = metadata_path
        self.dim           = dim
        self._index        = None
        self._metadata: List[dict] = []

    # ── Build ─────────────────────────────────────────────────

    def build(
        self,
        vectors:  np.ndarray,
        metadata: List[dict],
    ) -> None:
        """
        Build FAISS index from embeddings.

        vectors  : float32 ndarray shape (N, dim), L2-normalised
        metadata : list of N chunk dicts (parallel to vectors)
        """
        try:
            import faiss
        except ImportError:
            raise ImportError(
                "faiss-cpu not installed.\n"
                "Run: pip install faiss-cpu"
            )

        assert vectors.shape[0] == len(metadata), (
            f"Vector count ({vectors.shape[0]}) ≠ metadata count ({len(metadata)})"
        )
        assert vectors.shape[1] == self.dim, (
            f"Vector dim ({vectors.shape[1]}) ≠ expected ({self.dim})"
        )

        n = vectors.shape[0]
        log.info(f"Building FAISS index: {n} vectors, dim={self.dim}")
        t0 = time.time()

        if n <= _IVF_THRESHOLD:
            # Exact inner-product search (cosine sim because vectors are L2-normalised)
            index = faiss.IndexFlatIP(self.dim)
        else:
            # Approximate IVF for large collections
            nlist = min(256, n // 10)
            quantiser = faiss.IndexFlatIP(self.dim)
            index = faiss.IndexIVFFlat(
                quantiser, self.dim, nlist, faiss.METRIC_INNER_PRODUCT
            )
            index.train(vectors)
            index.nprobe = 32   # number of clusters to probe at query time

        index.add(vectors)

        self._index    = index
        self._metadata = metadata

        elapsed = time.time() - t0
        log.info(
            f"FAISS index built in {elapsed:.2f}s — "
            f"{index.ntotal} vectors, "
            f"type={'Flat' if n <= _IVF_THRESHOLD else 'IVFFlat'}"
        )

    def save(self) -> None:
        """Persist the index and metadata to disk."""
        try:
            import faiss
        except ImportError:
            raise ImportError("faiss-cpu not installed.")

        if self._index is None:
            raise RuntimeError("No index to save. Call build() first.")

        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self._index, str(self.index_path))
        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(self._metadata, f, ensure_ascii=False)

        idx_kb  = self.index_path.stat().st_size / 1024
        meta_kb = self.metadata_path.stat().st_size / 1024
        log.info(
            f"VectorStore saved — "
            f"index: {self.index_path.name} ({idx_kb:.0f} KB), "
            f"metadata: {self.metadata_path.name} ({meta_kb:.0f} KB)"
        )

    # ── Load ─────────────────────────────────────────────────

    def load(self) -> None:
        """Load index + metadata from disk."""
        try:
            import faiss
        except ImportError:
            raise ImportError("faiss-cpu not installed.")

        if not self.index_path.exists():
            raise FileNotFoundError(
                f"FAISS index not found at {self.index_path}.\n"
                "Run Phase 3 build first:  python chatbot_main.py --mode build"
            )

        t0 = time.time()
        self._index = faiss.read_index(str(self.index_path))
        with open(self.metadata_path, encoding="utf-8") as f:
            self._metadata = json.load(f)

        log.info(
            f"VectorStore loaded in {time.time()-t0:.2f}s — "
            f"{self._index.ntotal} vectors"
        )

    # ── Search ────────────────────────────────────────────────

    def search(
        self,
        query_vector: np.ndarray,
        top_k: int        = RAG_TOP_K,
        min_score: float  = RAG_MIN_SCORE,
        filter_type: str  = None,   # filter by node_type if provided
    ) -> List[Tuple[dict, float]]:
        """
        Search for the top_k most similar chunks.

        query_vector : float32 ndarray shape (1, dim) or (dim,)
        top_k        : number of results to return
        min_score    : minimum cosine similarity (0–1)
        filter_type  : optional node_type filter (post-retrieval)

        Returns list of (chunk_dict, score) sorted by score descending.
        """
        if self._index is None:
            raise RuntimeError("Index not loaded. Call load() or build() first.")

        vec = query_vector.reshape(1, self.dim).astype(np.float32)

        # Retrieve more than top_k to allow for filtering + dedup
        k = min(top_k * 4, self._index.ntotal)
        scores, indices = self._index.search(vec, k)

        results = []
        seen_node_ids = set()

        for score, idx in zip(scores[0], indices[0]):
            if idx < 0 or idx >= len(self._metadata):
                continue
            if float(score) < min_score:
                continue

            chunk = self._metadata[idx]

            # Optional node_type filter
            if filter_type and chunk.get("node_type") != filter_type:
                continue

            # Deduplicate by node_id (keep highest-scoring chunk per node)
            node_id = chunk.get("node_id", "")
            if node_id in seen_node_ids:
                continue
            seen_node_ids.add(node_id)

            results.append((chunk, float(score)))
            if len(results) >= top_k:
                break

        return results

    # ── Info ─────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """Number of vectors in the index."""
        return self._index.ntotal if self._index else 0

    def is_loaded(self) -> bool:
        return self._index is not None
