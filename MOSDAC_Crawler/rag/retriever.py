"""
rag/retriever.py
─────────────────────────────────────────────────────────────────────────────
Retrieves relevant chunks for a user query.

Two-stage retrieval:
  1. Dense retrieval   — FAISS semantic search (fast, approximate)
  2. Graph enrichment  — if a Mission is retrieved, also fetch its payload
                         and section chunks from the KG for fuller context

Also handles:
  • Query classification  — detects FAQ, mission, product, document questions
  • Fallback              — keyword search on metadata if FAISS returns nothing
─────────────────────────────────────────────────────────────────────────────
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import networkx as nx

from config import (
    KNOWN_MISSIONS,
    RAG_CONTEXT_MAX_CHARS,
    RAG_MIN_SCORE,
    RAG_PRIORITY_TYPES,
    RAG_TOP_K,
)
from rag.embedder import Embedder
from rag.vector_store import VectorStore
from utils.logger import get_logger

log = get_logger(__name__)


class QueryType:
    FAQ         = "faq"
    MISSION     = "mission"
    PAYLOAD     = "payload"
    PRODUCT     = "open_data"
    DOCUMENT    = "document"
    GENERAL     = "general"


class Retriever:
    """
    Retrieves the most relevant chunks for a user query.

    Usage:
        retriever = Retriever(store, embedder, G)
        retriever.load()
        chunks = retriever.retrieve("What are INSAT-3D payloads?")
    """

    def __init__(
        self,
        vector_store: VectorStore,
        embedder:     Embedder,
        graph:        Optional[nx.DiGraph] = None,
    ):
        self.store   = vector_store
        self.embedder = embedder
        self.graph   = graph   # Optional: KG for graph-based enrichment

    # ── Public API ────────────────────────────────────────────

    def retrieve(
        self,
        query: str,
        top_k: int = RAG_TOP_K,
    ) -> List[dict]:
        """
        Retrieve the top_k most relevant chunks for a query.

        Pipeline:
          1. Classify query type
          2. FAISS semantic search
          3. Graph enrichment (if mission query)
          4. Re-rank and trim to context budget
        """
        query = query.strip()
        if not query:
            return []

        # ── Step 1: Classify query ────────────────────────────
        q_type   = self._classify_query(query)
        log.debug(f"Query type: {q_type} | Query: {query[:80]}")

        # ── Step 2: Embed query ───────────────────────────────
        q_vec = self.embedder.embed_query(query)

        # ── Step 3: FAISS retrieval ───────────────────────────
        raw_results: List[Tuple[dict, float]] = self.store.search(
            q_vec, top_k=top_k * 2, min_score=RAG_MIN_SCORE
        )

        # ── Step 4: Graph enrichment ──────────────────────────
        if self.graph and q_type in (QueryType.MISSION, QueryType.PAYLOAD):
            raw_results = self._enrich_with_graph(query, raw_results, top_k)

        # ── Step 5: Re-rank by priority type ──────────────────
        ranked = self._rerank(raw_results, q_type)

        # ── Step 6: Trim to context budget ────────────────────
        final = self._trim_to_budget(ranked)

        log.info(
            f"Retrieved {len(final)} chunks for query "
            f"[type={q_type}]: {query[:60]}…"
        )
        return final

    # ── Query classification ──────────────────────────────────

    def _classify_query(self, query: str) -> str:
        """
        Classify query intent. Priority order:
          1. Open data (very specific product names — unambiguous)
          2. FAQ (how-to, account, portal usage)
          3. Document (ATBD, handbook, report requests)
          4. Payload (sensor specs within a mission)
          5. Mission (general satellite mission questions)
          6. General (fallback)
        """
        q = query.lower()

        # ── 1. Open data — specific product keywords ──────────────────
        if any(kw in q for kw in [
            "soil moisture", "ocean current", "surface current",
            "sea ice", "river discharge", "inland water",
            "water height", "cloud properties", "water vapour",
            "gsmap", "ocean subsurface", "coastal product",
            "wave energy", "renewable energy", "bayesian rainfall",
            "open data", "free data", "salinity product",
            "oceanic eddy", "eddies detection",
        ]):
            return QueryType.PRODUCT

        # ── 2. FAQ — account, portal how-to, data ordering ────────────
        if any(kw in q for kw in [
            "how to", "how do i", "how can i",
            "register", "login", "password", "account",
            "sign up", "standing order", "download", "sftp", "ftp",
            "near real time", "nrt", "in-situ", "aws", "insitu",
            "order data", "get data", "access data",
            "what is mosdac",
        ]):
            return QueryType.FAQ

        # ── 3. Document — ATBD, handbook, report lookups ──────────────
        if any(kw in q for kw in [
            "atbd", "handbook", "manual",
            "user guide", "calibration report", "validation report",
            "where is the", "where can i find",
        ]):
            return QueryType.DOCUMENT

        # ── 4. Payload / sensor specs (within a mission context) ───────
        payload_kws = [
            "payload", "sensor", "instrument", "imager", "sounder",
            "channel", "band", "resolution", "swath", "saphir",
            "spatial resolution", "ground resolution", "spectral",
            "wavelength", "detector", "radiometer", "madras", "scarab",
        ]
        mission_keywords = list(KNOWN_MISSIONS.keys()) + [
            "insat", "oceansat", "kalpana", "megha", "saral",
            "scatsat", "satellite", "mission", "spacecraft",
            "geostationary", "sun-synchronous",
            "imager", "sounder", "saphir", "altika", "ocm",
            "scatterometer",
        ]
        has_mission = any(kw in q for kw in mission_keywords)
        has_payload = any(kw in q for kw in payload_kws)

        if has_payload:
            return QueryType.PAYLOAD

        # ── 5. Mission — general satellite mission question ────────────
        if has_mission:
            return QueryType.MISSION

        # ── 6. Broader open data / report (without specific names) ────
        if any(kw in q for kw in [
            "open data", "free data", "rainfall", "eddies",
            "ocean product", "atmosphere product", "land product",
        ]):
            return QueryType.PRODUCT

        if any(kw in q for kw in [
            "report", "document", "calibration", "validation",
        ]):
            return QueryType.DOCUMENT

        return QueryType.GENERAL

    # ── Graph enrichment ──────────────────────────────────────

    def _enrich_with_graph(
        self,
        query: str,
        results: List[Tuple[dict, float]],
        top_k: int,
    ) -> List[Tuple[dict, float]]:
        """
        If a Mission node was retrieved, also add its payload chunks
        and section chunks from the metadata index by node_id.
        """
        if not self.graph:
            return results

        # Find mission slugs in top results
        mission_slugs = set()
        for chunk, _ in results[:3]:
            slug = chunk.get("mission_slug", "")
            if slug:
                mission_slugs.add(slug)
            elif chunk.get("node_type") == "Mission":
                nid = chunk.get("node_id", "")
                if nid.startswith("mission:"):
                    mission_slugs.add(nid.replace("mission:", ""))

        if not mission_slugs:
            return results

        # Get all metadata to search for related chunks
        all_meta = self.store._metadata

        # Find payload and section chunks for matched missions
        extra: List[Tuple[dict, float]] = []
        for chunk in all_meta:
            chunk_slug = chunk.get("mission_slug", "")
            if chunk_slug not in mission_slugs:
                continue
            # Don't re-add what's already in results
            already_in = any(
                c.get("chunk_id") == chunk.get("chunk_id")
                for c, _ in results
            )
            if not already_in:
                extra.append((chunk, 0.5))   # fixed lower score for enriched

        # Add top payload/section extras (limit to avoid overflow)
        payload_chunks = [
            (c, s) for c, s in extra
            if c.get("node_type") in ("Payload", "MissionSection")
        ][:top_k]

        return results + payload_chunks

    # ── Re-ranking ────────────────────────────────────────────

    def _rerank(
        self,
        results: List[Tuple[dict, float]],
        q_type: str,
    ) -> List[Tuple[dict, float]]:
        """
        Re-rank results:
          1. Boost FAQ chunks for FAQ queries
          2. Boost MissionSection/Payload for mission queries
          3. Secondary sort by score
        """
        # Priority map: node_type → boost
        if q_type == QueryType.FAQ:
            boosts = {"FAQ": 1.0, "MissionSection": 0.3, "Mission": 0.2}
        elif q_type in (QueryType.MISSION, QueryType.PAYLOAD):
            boosts = {
                "MissionSection": 0.5, "Payload": 0.4,
                "Mission": 0.3, "Document": 0.2,
            }
        elif q_type == QueryType.PRODUCT:
            boosts = {"OpenDataProduct": 0.5, "Document": 0.3}
        elif q_type == QueryType.DOCUMENT:
            boosts = {"Document": 0.5, "MissionSection": 0.2}
        else:
            boosts = {}

        def rank_key(item):
            chunk, score = item
            ntype = chunk.get("node_type", "")
            boost = boosts.get(ntype, 0.0)
            return -(score + boost)

        return sorted(results, key=rank_key)

    # ── Context budget ────────────────────────────────────────

    def _trim_to_budget(
        self,
        results: List[Tuple[dict, float]],
        max_chars: int = RAG_CONTEXT_MAX_CHARS,
    ) -> List[dict]:
        """
        Keep chunks until the total character budget is reached.
        Always includes at least 1 chunk even if it exceeds budget.
        """
        kept  = []
        total = 0

        for chunk, score in results:
            text = chunk.get("text", "")
            if not text:
                continue
            if total > 0 and total + len(text) > max_chars:
                break
            kept.append(chunk)
            total += len(text)

        return kept

    # ── Fallback keyword search ───────────────────────────────

    def keyword_search(
        self,
        query: str,
        top_k: int = RAG_TOP_K,
    ) -> List[dict]:
        """
        Keyword-based fallback search over chunk text + metadata.
        Used when FAISS retrieval returns nothing or low-confidence results.
        """
        query_lower = query.lower()
        keywords = set(re.findall(r'\b\w{3,}\b', query_lower))

        scored: List[Tuple[dict, int]] = []
        for chunk in self.store._metadata:
            text   = (chunk.get("text", "") + " " + chunk.get("label", "")).lower()
            hits   = sum(1 for kw in keywords if kw in text)
            if hits > 0:
                scored.append((chunk, hits))

        scored.sort(key=lambda x: -x[1])
        return [c for c, _ in scored[:top_k]]
