"""
knowledge_graph/entities/faq_entity_builder.py
─────────────────────────────────────────────────────────────────────────────
Builds FAQ KGNodes directly from the faqs table.

The 17 FAQs from the MOSDAC FAQ page are already perfectly structured —
no further extraction needed.  This module simply wraps each DB row as
a KGNode with a clean text field for RAG retrieval.

Nodes produced:
  FAQ   node_id = 'faq:{id}'
─────────────────────────────────────────────────────────────────────────────
"""

from typing import List, Tuple

from knowledge_graph.entities.base import KGEdge, KGNode
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)

# Category → topic mapping (FAQ categories inferred from question content)
TOPIC_KEYWORDS = {
    "Registration":  ["register", "signup", "sign up", "account", "email verification"],
    "Login":         ["password", "login", "credentials", "locked", "username"],
    "Data Access":   ["download", "data", "catalog", "sftp", "order", "request", "near real time"],
    "AWS":           ["aws", "automatic weather", "in-situ", "insitu"],
    "General":       ["what is mosdac", "mosdac"],
}


class FAQEntityBuilder:
    """
    Wraps FAQ rows from the database as KGNodes.

    Usage:
        builder = FAQEntityBuilder(store)
        nodes, edges = builder.extract()
    """

    def __init__(self, store: DataStore):
        self.store = store

    def extract(self) -> Tuple[List[KGNode], List[KGEdge]]:
        """Return one KGNode per FAQ row."""
        faqs  = self.store.get_all_faqs()
        nodes: List[KGNode] = []

        for faq in faqs:
            node = self._build_node(faq)
            nodes.append(node)

        log.info(f"FAQEntityBuilder: {len(nodes)} FAQ nodes")
        return nodes, []

    def _build_node(self, faq: dict) -> KGNode:
        faq_id   = faq["id"]
        question = faq.get("question", "").strip()
        answer   = faq.get("answer", "").strip()
        category = faq.get("category", "") or self._infer_topic(question)

        # Text for RAG: Q+A together
        text = f"Q: {question}\nA: {answer}"

        return KGNode(
            node_id    = f"faq:{faq_id}",
            node_type  = "FAQ",
            label      = question[:80] + ("…" if len(question) > 80 else ""),
            source_url = faq.get("source_url", ""),
            attributes = {
                "question": question,
                "answer":   answer,
                "topic":    category,
            },
            text = text,
        )

    def _infer_topic(self, question: str) -> str:
        """Assign a topic category based on question keywords."""
        q_lower = question.lower()
        for topic, keywords in TOPIC_KEYWORDS.items():
            if any(kw in q_lower for kw in keywords):
                return topic
        return "General"
