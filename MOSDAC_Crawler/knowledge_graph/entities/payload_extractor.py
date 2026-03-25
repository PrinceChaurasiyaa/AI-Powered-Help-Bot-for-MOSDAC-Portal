"""
knowledge_graph/entities/payload_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts Payload (sensor/instrument) nodes from tables crawled on
mission payload pages (e.g. /insat-3d-payloads, /oceansat-3-payloads).

Source:  DB extracted_tables WHERE source_url LIKE '%-payloads%'
         OR source_url LIKE '%-spacecraft%' (some missions put specs there)

The table extractor saved raw tables as JSON arrays.  This module
interprets table columns to build structured Payload entities.

Column name normalisation handles the variety seen across missions:
  "Payload" / "Sensor" / "Instrument" / "Name"          → payload_name
  "Type" / "Payload Type" / "Sensor Type"               → payload_type
  "No. of Channels" / "Channels" / "Bands"              → channels
  "Spectral Range" / "Wavelength" / "Spectral Bands"    → spectral_range
  "Spatial Resolution" / "Resolution" / "Ground Res."   → resolution
  "Swath" / "Swath Width"                               → swath
  "Revisit Time" / "Repeat Cycle"                       → revisit_time

Nodes produced:
  Payload   node_id = 'payload:{mission_slug}:{sanitised_name}'

Edges produced:
  Mission -[HAS_PAYLOAD]→ Payload   (returned for graph_builder to add)
─────────────────────────────────────────────────────────────────────────────
"""

import re
from typing import Dict, List, Optional, Tuple

from config import KNOWN_MISSIONS, MISSION_SUBPAGE_SLUG_OVERRIDES
from knowledge_graph.entities.base import KGEdge, KGNode
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)

# ── Column header synonym map ─────────────────────────────────
# Maps lowercase column keywords → canonical attribute name
COLUMN_SYNONYMS: Dict[str, str] = {
    # Payload name
    "payload":              "payload_name",
    "instrument":           "payload_name",
    "sensor":               "payload_name",
    "name":                 "payload_name",

    # Type
    "type":                 "payload_type",
    "payload type":         "payload_type",
    "sensor type":          "payload_type",
    "instrument type":      "payload_type",

    # Channels / bands
    "channels":             "channels",
    "no. of channels":      "channels",
    "number of channels":   "channels",
    "bands":                "channels",
    "no. of bands":         "channels",

    # Spectral range
    "spectral range":       "spectral_range",
    "spectral bands":       "spectral_range",
    "wavelength":           "spectral_range",
    "wavelength range":     "spectral_range",
    "band":                 "spectral_range",

    # Spatial resolution
    "spatial resolution":   "resolution",
    "resolution":           "resolution",
    "ground resolution":    "resolution",
    "pixel size":           "resolution",
    "gsd":                  "resolution",

    # Swath
    "swath":                "swath",
    "swath width":          "swath",
    "coverage":             "swath",

    # Revisit / repeat
    "revisit time":         "revisit_time",
    "repeat cycle":         "revisit_time",
    "repeat":               "revisit_time",

    # Orbit / altitude
    "altitude":             "altitude",
    "orbit":                "orbit",
    "inclination":          "inclination",
}


def _normalise_header(col: str) -> str:
    """Map a raw column header string to a canonical attribute name."""
    col_lower = col.strip().lower()
    # Exact match first
    if col_lower in COLUMN_SYNONYMS:
        return COLUMN_SYNONYMS[col_lower]
    # Partial match
    for kw, canon in COLUMN_SYNONYMS.items():
        if kw in col_lower:
            return canon
    # Fall back to snake_case of original
    return re.sub(r"\W+", "_", col_lower).strip("_")


def _sanitise_id(name: str) -> str:
    """Convert a payload name to a safe node_id fragment."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


class PayloadExtractor:
    """
    Builds Payload KGNodes from extracted table data.

    Usage:
        extractor = PayloadExtractor(store)
        nodes, edges = extractor.extract()
    """

    def __init__(self, store: DataStore):
        self.store = store

    # ── Public API ────────────────────────────────────────────

    def extract(self) -> Tuple[List[KGNode], List[KGEdge]]:
        """
        Scan all tables from payload/spacecraft pages and build Payload nodes.
        Returns (nodes, edges) where each edge is Mission→[HAS_PAYLOAD]→Payload.
        """
        nodes: List[KGNode] = []
        edges: List[KGEdge] = []

        all_tables = self.store.get_all_tables()
        log.info(f"PayloadExtractor: scanning {len(all_tables)} tables")

        # Filter to payload-related source pages only
        payload_tables = [
            t for t in all_tables
            if self._is_payload_source(t["source_url"])
        ]
        log.info(f"  → {len(payload_tables)} payload/spacecraft tables found")

        # Group by source URL → parse each table
        seen_ids = set()
        for table in payload_tables:
            mission_slug = self._slug_from_url(table["source_url"])
            if not mission_slug:
                continue

            mission_name = KNOWN_MISSIONS.get(mission_slug, mission_slug)
            table_nodes  = self._parse_payload_table(
                table, mission_slug, mission_name
            )

            for node in table_nodes:
                if node.node_id in seen_ids:
                    continue
                seen_ids.add(node.node_id)
                nodes.append(node)

                edges.append(KGEdge(
                    source_id     = f"mission:{mission_slug}",
                    target_id     = node.node_id,
                    relation_type = "HAS_PAYLOAD",
                    attributes    = {
                        "source_url": table["source_url"],
                        "table_idx":  table["table_index"],
                    },
                ))

        log.info(f"PayloadExtractor: {len(nodes)} payload nodes, {len(edges)} edges")
        return nodes, edges

    # ── Table parsing ─────────────────────────────────────────

    def _parse_payload_table(
        self,
        table: dict,
        mission_slug: str,
        mission_name: str,
    ) -> List[KGNode]:
        """
        Parse one extracted table into Payload KGNodes.

        Strategy:
          1. Normalise column headers to canonical attribute names.
          2. Each data row becomes one Payload node IF it has a name.
          3. Alternatively, if the table has 2 columns (key/value style),
             treat the whole table as one payload's specs.
        """
        headers = table.get("headers", [])
        rows    = table.get("rows", [])
        if not headers or not rows:
            return []

        # Normalise headers
        norm_headers = [_normalise_header(h) for h in headers]

        # ── Key/Value style table (2 columns: Attribute | Value) ──────
        if len(headers) == 2:
            return self._parse_kv_table(
                norm_headers, rows, mission_slug, mission_name,
                table["source_url"]
            )

        # ── Multi-row table: each row = one payload ────────────────────
        return self._parse_row_table(
            norm_headers, rows, mission_slug, mission_name,
            table["source_url"]
        )

    def _parse_row_table(
        self,
        norm_headers: List[str],
        rows: List[List[str]],
        mission_slug: str,
        mission_name: str,
        source_url: str,
    ) -> List[KGNode]:
        """Each row = one payload instrument."""
        nodes = []
        name_idx = None
        if "payload_name" in norm_headers:
            name_idx = norm_headers.index("payload_name")

        for row_idx, row in enumerate(rows):
            if not any(c.strip() for c in row):
                continue

            # Build attribute dict from row
            attrs: Dict[str, str] = {}
            for col_i, canon in enumerate(norm_headers):
                if col_i < len(row) and row[col_i].strip():
                    attrs[canon] = row[col_i].strip()

            # Determine payload name
            if name_idx is not None and name_idx < len(row):
                raw_name = row[name_idx].strip()
            else:
                # Use first non-empty cell as name
                raw_name = next(
                    (c.strip() for c in row if c.strip()), f"Payload-{row_idx+1}"
                )

            if not raw_name or len(raw_name) > 100:
                continue

            attrs["mission_slug"] = mission_slug
            attrs["mission_name"] = mission_name

            node_id = f"payload:{mission_slug}:{_sanitise_id(raw_name)}"
            text    = self._attrs_to_text(raw_name, mission_name, attrs)

            nodes.append(KGNode(
                node_id    = node_id,
                node_type  = "Payload",
                label      = raw_name,
                source_url = source_url,
                attributes = attrs,
                text       = text,
            ))

        return nodes

    def _parse_kv_table(
        self,
        norm_headers: List[str],
        rows: List[List[str]],
        mission_slug: str,
        mission_name: str,
        source_url: str,
    ) -> List[KGNode]:
        """
        2-column key/value table → one Payload node.
        e.g.  Attribute | Value
              Channels   | 19
              Resolution | 1 km
        """
        attrs: Dict[str, str] = {"mission_slug": mission_slug}
        payload_name = f"{mission_name} Payload"

        for row in rows:
            if len(row) < 2:
                continue
            key   = _normalise_header(row[0])
            value = row[1].strip()
            if key and value:
                attrs[key] = value
                if key == "payload_name":
                    payload_name = value

        if len(attrs) <= 1:   # Only mission_slug — nothing useful
            return []

        node_id = f"payload:{mission_slug}:{_sanitise_id(payload_name)}"
        text    = self._attrs_to_text(payload_name, mission_name, attrs)

        return [KGNode(
            node_id    = node_id,
            node_type  = "Payload",
            label      = payload_name,
            source_url = source_url,
            attributes = attrs,
            text       = text,
        )]

    # ── Helpers ───────────────────────────────────────────────

    def _is_payload_source(self, url: str) -> bool:
        """True if the URL is a payload or spacecraft page."""
        url_lower = url.lower()
        return "-payloads" in url_lower or "-spacecraft" in url_lower

    def _slug_from_url(self, url: str) -> Optional[str]:
        """
        Extract mission slug from a payload page URL.
        e.g. .../insat-3d-payloads → 'insat-3d'
             .../insat-3s-payloads → 'insat-3ds'  (via overrides)
        """
        url_lower = url.lower()

        # Check override slugs first (e.g. insat-3s → insat-3ds)
        for url_slug, landing_slug in MISSION_SUBPAGE_SLUG_OVERRIDES.items():
            if f"/{url_slug}-" in url_lower:
                return landing_slug

        # Check standard known slugs
        for slug in KNOWN_MISSIONS:
            if f"/{slug}-" in url_lower:
                return slug

        return None

    def _attrs_to_text(
        self, name: str, mission: str, attrs: Dict[str, str]
    ) -> str:
        """Build a human-readable text description of a payload."""
        lines = [f"{name} is a payload/instrument on the {mission} satellite."]
        attr_labels = {
            "payload_type":  "Type",
            "channels":      "Number of channels/bands",
            "spectral_range":"Spectral range",
            "resolution":    "Spatial resolution",
            "swath":         "Swath width",
            "revisit_time":  "Revisit time",
            "altitude":      "Orbit altitude",
        }
        for key, label in attr_labels.items():
            val = attrs.get(key, "")
            if val and val not in (name, mission):
                lines.append(f"{label}: {val}")
        return " ".join(lines)
