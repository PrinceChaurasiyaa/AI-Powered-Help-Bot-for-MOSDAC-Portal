"""Generates dense vector embeddings for text chunks using sentence-transformers.
"""

import time
from pathlib import Path
from typing import List
import numpy as np

from config import RAG_EMBEDDING_BATCH, RAG_EMBEDDING_DIM, RAG_EMBEDDING_MODEL
from utils.logger import get_logger

log = get_logger(__name__)

class Embedder:
    """
    Wraps sentence-transformers to embed text strings into dense vectors.
    """

    def __init__(
        self,
        model_name: str = RAG_EMBEDDING_MODEL,
        batch_size: str = RAG_EMBEDDING_BATCH,
        dim: int = RAG_EMBEDDING_DIM,
    ): 
        self.model_name = model_name
        self.batch_size = batch_size
        self.dim = dim
        self._model = None
    
    def load(self) -> None:
        """"""
        if self._model is not None:
            return
        
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed.\n"
                "Run: pip install sentence-transformers"
            )
        
        log.info(f"Loading embedding model: {self.model_name}")
        t0 = time.time()
        self._model = SentenceTransformer(self.model_name)
        log.info(f"Model loaded in {time.time() - t0:.1f}s")
    
    def embed(
            self,
            texts: List[str],
            show_progress: bool = True,
    ) -> np.ndarray:
        """
        Embed a list of texts. Returns float32 ndarray of shape (N, dim).
        Processes in batches to avoid memory issues.
        """
        if self._model is None:
            self.load()

        if not texts:
            return np.zeros((0, self.dim), dtype=np.float32)
        
        log.info(
            f"Embedding {len(texts)} texts "
            f"(batch={self.batch_size}, model={self.model_name})…"
        )

        t0 = time.time()

        vectors = self._model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        elapsed = time.time() - t0
        log.info(
            f"Embedding complete: {len(texts)} texts in {elapsed:.1f}s "
            f"({len(texts)/elapsed:.0f} texts/sec), "
            f"shape={vectors.shape}"
        )

        return vectors.astype(np.float32)
    
    def embed_query(self, query: str) -> np.ndarray:
        return self.embed([query], show_progress=False)
    
    def embed_in_batches(
        self,
        texts: List[str],
    ):
        """
        Generator that yields (batch_vectors, start_idx) for large datasets.
        Useful when you want to track progress or save incrementally.
        """
        if self._model is None:
            self.load()

        for start in range(0, len(texts), self.batch_size):
            batch = texts[start : start + self.batch_size]
            vecs  = self._model.encode(
                batch,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            ).astype(np.float32)
            yield vecs, start