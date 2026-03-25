"""
knowledge_graph/entities/mission_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts Mission and MissionSection nodes from the Phase 1 database.

Sources used:
  1. config.KNOWN_MISSIONS             — authoritative mission list
  2. config.MISSION_ORBIT_TYPES        — orbit enrichment
  3. config.MISSION_FAMILIES           — family grouping (INSAT, OCEANSAT…)
  4. DB: mission_hierarchy table        — section structure
  5. DB: pages (page_type=mission)      — landing page content
  6. DB: pages (page_type=mission_section) — sub-page content

Nodes produced:
  Mission        node_id = 'mission:{slug}'
  MissionSection node_id = 'section:{url_slug}'

Edges produced:
  Mission -[HAS_SECTION]→ MissionSection
  MissionSection -[PART_OF]→ Mission
─────────────────────────────────────────────────────────────────────────────
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from config import (
    KNOWN_MISSIONS,
    MISSION_FAMILIES,
    MISSION_ORBIT_TYPES,
    MISSION_SUBPAGE_TYPES,
)
from knowledge_graph.entities.base import KGEdge, KGNode
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)


class MissionExtractor:
    """
    Builds Mission and MissionSection KG nodes from the crawled database.

    Usage:
        extractor = MissionExtractor(store)
        nodes, edges = extractor.extract()
    """

    def __init__(self, store: DataStore):
        self.store = store

    # ── Public API ────────────────────────────────────────────

    def extract(self) -> Tuple[List[KGNode], List[KGEdge]]:
        """
        Return all Mission + MissionSection nodes and their edges.
        """
        nodes: List[KGNode] = []
        edges: List[KGEdge] = []

        # Load source data from DB once
        mission_hierarchy = self.store.get_mission_hierarchy()
        all_pages         = self.store.get_all_pages(
            page_types=["mission", "mission_section"]
        )

        # Index pages by URL for fast lookup
        pages_by_url: Dict[str, dict] = {p["url"]: p for p in all_pages}

        # Build one Mission node per known mission
        for slug, name in KNOWN_MISSIONS.items():
            mission_node, mission_edges = self._build_mission_node(
                slug, name, pages_by_url, mission_hierarchy
            )
            nodes.append(mission_node)

            # Build sub-page nodes for this mission
            section_rows = [
                r for r in mission_hierarchy
                if r["mission_slug"] == slug
                and r["section_type"] != "landing"
            ]

            for row in section_rows:
                section_node = self._build_section_node(row, pages_by_url)
                if section_node:
                    nodes.append(section_node)

                    # HAS_SECTION: Mission → Section
                    edges.append(KGEdge(
                        source_id=mission_node.node_id,
                        target_id=section_node.node_id,
                        relation_type="HAS_SECTION",
                        attributes={"section_type": row["section_type"]},
                    ))
                    # PART_OF: Section → Mission (reverse, for lookup)
                    edges.append(KGEdge(
                        source_id=section_node.node_id,
                        target_id=mission_node.node_id,
                        relation_type="PART_OF",
                    ))

            edges.extend(mission_edges)

        log.info(
            f"MissionExtractor: {len([n for n in nodes if n.node_type == 'Mission'])} missions, "
            f"{len([n for n in nodes if n.node_type == 'MissionSection'])} sections"
        )
        return nodes, edges

    # ── Mission node ──────────────────────────────────────────

    def _build_mission_node(
        self,
        slug: str,
        name: str,
        pages_by_url: Dict[str, dict],
        hierarchy: List[dict],
    ) -> Tuple[KGNode, List[KGEdge]]:
        """Build a Mission KGNode from config + page content."""

        from config import BASE_URL
        landing_url  = f"{BASE_URL}/{slug}"
        landing_page = pages_by_url.get(landing_url, {})

        # Gather all section content for the text field
        section_texts = []
        section_urls  = [
            r["section_url"] for r in hierarchy
            if r["mission_slug"] == slug
        ]
        for sec_url in section_urls:
            page = pages_by_url.get(sec_url, {})
            text = (page.get("content_text") or "").strip()
            if text and len(text) > 20:
                section_type = self._section_type_from_url(sec_url, slug)
                section_texts.append(f"[{section_type.upper()}]\n{text}")

        # Build combined text: landing + all sections
        landing_text = (landing_page.get("content_text") or "").strip()
        all_text = landing_text
        if section_texts:
            all_text = landing_text + "\n\n" + "\n\n".join(section_texts)

        # Extract structured attributes from text
        attrs = self._extract_mission_attributes(slug, name, all_text)

        node = KGNode(
            node_id    = f"mission:{slug}",
            node_type  = "Mission",
            label      = name,
            source_url = landing_url,
            attributes = attrs,
            text       = all_text[:3000],   # capped for KG storage
        )
        return node, []

    def _extract_mission_attributes(
        self, slug: str, name: str, text: str
    ) -> Dict:
        """
        Extract structured attributes from mission page text using rules.
        No ML — regex patterns tuned to MOSDAC page content style.
        """
        text_lower = text.lower()

        attrs = {
            "slug":          slug,
            "display_name":  name,
            "orbit_type":    MISSION_ORBIT_TYPES.get(slug, "Unknown"),
            "mission_family": MISSION_FAMILIES.get(slug, "Unknown"),
            "agency":        "ISRO / SAC",
        }

        # Launch year — "launched in YYYY" or "launched on ... YYYY"
        year_match = re.search(
            r"launch(?:ed)?\s+(?:in|on)[^0-9]*(\d{4})", text_lower
        )
        if year_match:
            attrs["launch_year"] = year_match.group(1)

        # Orbit altitude — "NNN km" near orbit keywords
        alt_match = re.search(
            r"(\d{3,5})\s*km\b", text_lower
        )
        if alt_match:
            attrs["altitude_km"] = alt_match.group(1)

        # Mission type keywords
        if "geostationary" in text_lower or "geosynch" in text_lower:
            attrs["orbit_class"] = "GEO"
        elif "sun-synchronous" in text_lower or "polar" in text_lower:
            attrs["orbit_class"] = "LEO-SSO"
        elif "inclined" in text_lower:
            attrs["orbit_class"] = "Inclined LEO"

        # Operational status hint
        if "operational" in text_lower:
            attrs["status"] = "Operational"
        elif "decommission" in text_lower or "retired" in text_lower:
            attrs["status"] = "Retired"

        # Primary application keywords
        applications = []
        for kw in ["meteorology", "meteorological", "weather"]:
            if kw in text_lower:
                applications.append("Meteorology")
                break
        for kw in ["ocean", "oceanography"]:
            if kw in text_lower:
                applications.append("Oceanography")
                break
        for kw in ["altimetry", "altimeter"]:
            if kw in text_lower:
                applications.append("Altimetry")
                break
        for kw in ["scatterometer", "wind vector", "wind speed"]:
            if kw in text_lower:
                applications.append("Scatterometry")
                break
        if applications:
            attrs["applications"] = "; ".join(dict.fromkeys(applications))

        return attrs

    # ── Section node ──────────────────────────────────────────

    def _build_section_node(
        self,
        row: dict,
        pages_by_url: Dict[str, dict],
    ) -> Optional[KGNode]:
        """Build a MissionSection KGNode from a mission_hierarchy row."""

        sec_url  = row.get("section_url", "")
        sec_type = row.get("section_type", "other")
        slug     = row.get("mission_slug", "")
        name     = row.get("mission_name", "")

        # section node_id uses the URL slug tail
        url_path  = urlparse(sec_url).path.strip("/")
        node_id   = f"section:{url_path}"

        page = pages_by_url.get(sec_url, {})
        text = (page.get("content_text") or "").strip()
        word_count = page.get("word_count", 0)

        # Skip empty sections (not crawled or login-walled)
        if word_count < 5 and not text:
            log.debug(f"  Skipping empty section: {sec_url}")
            return None

        label = f"{name} — {sec_type.capitalize()}"

        attrs = {
            "mission_slug":  slug,
            "mission_name":  name,
            "section_type":  sec_type,
            "word_count":    word_count,
            "crawled":       bool(page),
        }

        return KGNode(
            node_id    = node_id,
            node_type  = "MissionSection",
            label      = label,
            source_url = sec_url,
            attributes = attrs,
            text       = text,
        )

    # ── Helpers ───────────────────────────────────────────────

    def _section_type_from_url(self, url: str, mission_slug: str) -> str:
        """Infer section type from URL path."""
        url_lower = url.lower()
        for sec in MISSION_SUBPAGE_TYPES:
            if sec in url_lower:
                return sec
        return "section"
