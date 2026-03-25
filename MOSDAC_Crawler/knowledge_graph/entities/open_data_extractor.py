"""
knowledge_graph/entities/open_data_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts OpenDataProduct nodes from pages with page_type='open_data'.

MOSDAC's Open Data products are freely downloadable derived datasets
covering Atmosphere, Land, and Ocean domains.

Source:  DB pages WHERE page_type = 'open_data'
Config:  OPEN_DATA_CATEGORIES — maps keywords to category

Nodes produced:
  OpenDataProduct  node_id = 'opendata:{url_slug}'

Attributes extracted from page content text:
  category         atmosphere | ocean | land
  description      first meaningful paragraph from page text
  data_format      e.g. NetCDF, HDF5 — regex matched from text
  spatial_res      spatial resolution if mentioned
  temporal_res     temporal resolution / frequency
  coverage         geographic coverage
  algorithm        algorithm name if mentioned (e.g. Bayesian, GSMaP)
─────────────────────────────────────────────────────────────────────────────
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from config import OPEN_DATA_CATEGORIES
from knowledge_graph.entities.base import KGEdge, KGNode
from storage.data_store import DataStore
from utils.logger import get_logger

log = get_logger(__name__)

# URL slug → friendly product name overrides (where URL slug is cryptic)
PRODUCT_NAME_OVERRIDES: Dict[str, str] = {
    "bayesian-based-mt-saphir-rainfall": "Bayesian MT-SAPHIR Rainfall",
    "gps-derived-integrated-water-vapour": "GPS-derived Integrated Water Vapour",
    "gsmap-isro-rain":                   "GSMaP ISRO Rain",
    "meteosat8-cloud-properties":        "METEOSAT-8 Cloud Properties",
    "3d-volumetric-terls-dwrproduct":    "3D Volumetric TERLS DWR Product",
    "inland-water-height":               "Inland Water Height",
    "river-discharge":                   "River Discharge",
    "soil-moisture-0":                   "Soil Moisture",
    "global-ocean-surface-current":      "Global Ocean Surface Current",
    "high-resolution-sea-surface-salinity": "High Resolution Sea Surface Salinity",
    "indian-mainland-coastal-product":   "Indian Mainland Coastal Product",
    "ocean-subsurface":                  "Ocean Subsurface Fields",
    "oceanic-eddies-detection":          "Oceanic Eddies Detection",
    "sea-ice-occurrence-probability":    "Sea Ice Occurrence Probability",
    "wave-based-renewable-energy":       "Wave-based Renewable Energy",
}


class OpenDataExtractor:
    """
    Builds OpenDataProduct KGNodes from crawled open-data pages.

    Usage:
        extractor = OpenDataExtractor(store)
        nodes, edges = extractor.extract()
    """

    def __init__(self, store: DataStore):
        self.store = store

    # ── Public API ────────────────────────────────────────────

    def extract(self) -> Tuple[List[KGNode], List[KGEdge]]:
        """Return OpenDataProduct nodes (no edges at this stage)."""
        pages = self.store.get_all_pages(page_types=["open_data"])
        nodes: List[KGNode] = []

        for page in pages:
            node = self._build_node(page)
            if node:
                nodes.append(node)

        log.info(f"OpenDataExtractor: {len(nodes)} open data product nodes")
        return nodes, []   # Edges added in graph_builder if needed

    # ── Node building ─────────────────────────────────────────

    def _build_node(self, page: dict) -> Optional[KGNode]:
        url   = page.get("url", "")
        text  = (page.get("content_text") or "").strip()
        wc    = page.get("word_count", 0)

        if wc < 20 or not text:
            log.debug(f"  Skipping low-content open_data page: {url}")
            return None

        url_slug = urlparse(url).path.strip("/").split("/")[-1]
        node_id  = f"opendata:{url_slug}"
        label    = PRODUCT_NAME_OVERRIDES.get(url_slug, _slug_to_label(url_slug))
        category = self._infer_category(url_slug, text)
        attrs    = self._extract_attrs(text, url_slug, category)

        return KGNode(
            node_id    = node_id,
            node_type  = "OpenDataProduct",
            label      = label,
            source_url = url,
            attributes = attrs,
            text       = self._build_text(label, category, text),
        )

    # ── Attribute extraction ──────────────────────────────────

    def _infer_category(self, slug: str, text: str) -> str:
        """Infer Atmosphere / Ocean / Land from URL slug and text."""
        slug_lower = slug.lower()
        text_lower = text.lower()[:500]

        for category, keywords in OPEN_DATA_CATEGORIES.items():
            if any(kw in slug_lower for kw in keywords):
                return category.capitalize()

        # Fallback: check text
        ocean_kws = ["ocean", "sea", "marine", "coastal", "wave", "salinity"]
        atmo_kws  = ["rain", "cloud", "atmosphere", "vapour", "humidity"]
        land_kws  = ["soil", "river", "inland", "land", "discharge"]

        if any(kw in text_lower for kw in ocean_kws):
            return "Ocean"
        if any(kw in text_lower for kw in atmo_kws):
            return "Atmosphere"
        if any(kw in text_lower for kw in land_kws):
            return "Land"

        return "Unknown"

    def _extract_attrs(
        self, text: str, slug: str, category: str
    ) -> Dict[str, str]:
        """Rule-based attribute extraction from page text."""
        text_lower = text.lower()
        attrs: Dict[str, str] = {"category": category, "slug": slug}

        # Data format — NetCDF, HDF5, GeoTIFF, CSV
        for fmt in ["netcdf", "hdf5", "hdf4", "geotiff", "csv", "ascii", "binary"]:
            if fmt in text_lower:
                attrs["data_format"] = fmt.upper()
                break

        # Spatial resolution — "N km" or "N m" near resolution keyword
        res_match = re.search(
            r"(?:spatial\s+)?resolution\s*[:\-–]?\s*([0-9.]+\s*(?:km|m)\b)",
            text_lower,
        )
        if res_match:
            attrs["spatial_resolution"] = res_match.group(1).strip()

        # Temporal resolution / frequency
        temp_match = re.search(
            r"(?:temporal\s+resolution|frequency|temporal\s+sampling)\s*[:\-–]?\s*([^\n.]{3,40})",
            text_lower,
        )
        if temp_match:
            attrs["temporal_resolution"] = temp_match.group(1).strip()[:80]

        # Coverage
        for cov in [
            "global", "indian ocean", "bay of bengal", "arabian sea",
            "india", "south asia", "indian subcontinent",
        ]:
            if cov in text_lower:
                attrs["coverage"] = cov.title()
                break

        # Algorithm name
        for algo in [
            "bayesian", "gsmap", "empirical", "rtofs", "hycom",
            "altimetry", "neural network", "random forest",
        ]:
            if algo in text_lower:
                attrs["algorithm"] = algo.title()
                break

        # Access URL (if any open-data download link mentioned)
        url_match = re.search(
            r"https?://(?:www\.)?mosdac\.gov\.in/opendata/\S+", text
        )
        if url_match:
            attrs["download_url"] = url_match.group(0)

        return attrs

    def _build_text(self, label: str, category: str, raw_text: str) -> str:
        """Build a clean, RAG-friendly description for this product."""
        # Use first 800 chars of page text as description base
        desc = raw_text[:800].replace("\n\n", " ").replace("\n", " ")
        return f"{label} ({category}). {desc}"


# ── Helpers ────────────────────────────────────────────────────

def _slug_to_label(slug: str) -> str:
    """Convert URL slug to title-cased label."""
    return slug.replace("-", " ").replace("_", " ").title()
