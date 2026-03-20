"""
extractors/meta_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts structured metadata from HTML <head> and ARIA attributes.

What is extracted
─────────────────
  meta (standard)   — description, keywords, author, robots, viewport …
  og (OpenGraph)    — og:title, og:description, og:image, og:type …
  twitter           — twitter:card, twitter:title, twitter:description …
  schema (JSON-LD)  — Structured data blocks (Google-readable schema.org)
  aria              — aria-label, aria-describedby values on all elements
  alt               — img alt text (valuable for satellite imagery pages)

All extracted values are stored as (meta_type, key, value) triples.
─────────────────────────────────────────────────────────────────────────────
"""

import json
import re
from typing import List, Tuple

from bs4 import BeautifulSoup, Tag

from utils.helpers import clean_text
from utils.logger import get_logger

log = get_logger(__name__)


class MetaExtractor:
    """
    Extracts meta tags, OpenGraph data, JSON-LD schema, and ARIA labels.

    Usage:
        extractor = MetaExtractor()
        for meta_type, key, value in extractor.extract(soup, url):
            store.save_meta(url, meta_type, key, value)
    """

    def extract(
        self, soup: BeautifulSoup, url: str
    ) -> List[Tuple[str, str, str]]:
        """
        Run all extraction strategies.
        Returns list of (meta_type, key, value) tuples.
        """
        results = []
        results.extend(self._extract_standard_meta(soup))
        results.extend(self._extract_opengraph(soup))
        results.extend(self._extract_twitter_card(soup))
        results.extend(self._extract_json_ld(soup))
        results.extend(self._extract_aria(soup))
        results.extend(self._extract_img_alt(soup))

        # Deduplicate
        seen = set()
        unique = []
        for item in results:
            key = (item[0], item[1], item[2][:100])
            if key not in seen:
                seen.add(key)
                unique.append(item)

        log.debug(f"MetaExtractor: {len(unique)} meta items at {url}")
        return unique

    # ── Standard <meta> tags ─────────────────────────────────

    def _extract_standard_meta(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Extract standard HTML meta tags:
          <meta name="description" content="...">
          <meta http-equiv="..." content="...">
        """
        results = []
        for tag in soup.find_all("meta"):
            name    = tag.get("name")    or tag.get("http-equiv") or ""
            content = tag.get("content") or ""
            if name and content:
                results.append(("meta", name.lower().strip(), clean_text(content)[:1000]))
        return results

    # ── OpenGraph ─────────────────────────────────────────────

    def _extract_opengraph(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Extract OpenGraph protocol meta tags:
          <meta property="og:title" content="...">
        """
        results = []
        for tag in soup.find_all("meta", property=re.compile(r"^og:", re.I)):
            prop    = tag.get("property", "").lower().strip()
            content = clean_text(tag.get("content", ""))
            if prop and content:
                results.append(("og", prop, content[:1000]))
        return results

    # ── Twitter Card ─────────────────────────────────────────

    def _extract_twitter_card(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Extract Twitter card meta tags:
          <meta name="twitter:title" content="...">
        """
        results = []
        for tag in soup.find_all("meta", attrs={"name": re.compile(r"^twitter:", re.I)}):
            name    = tag.get("name", "").lower().strip()
            content = clean_text(tag.get("content", ""))
            if name and content:
                results.append(("twitter", name, content[:1000]))
        return results

    # ── JSON-LD Structured Data ───────────────────────────────

    def _extract_json_ld(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Parse <script type="application/ld+json"> blocks.
        These contain rich structured data (Article, FAQPage, Dataset …)
        which is especially valuable for a knowledge graph.
        """
        results = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string or ""
                data = json.loads(raw.strip())
                # Flatten key top-level fields
                schema_type = data.get("@type", "unknown")
                results.append(("schema", "@type", str(schema_type)))

                important_keys = [
                    "name", "description", "url", "headline",
                    "datePublished", "dateModified", "author",
                    "publisher", "keywords", "about",
                ]
                for k in important_keys:
                    v = data.get(k)
                    if v:
                        val = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                        results.append(("schema", k, val[:2000]))

                # For FAQPage schema — extract Q&A pairs too
                if schema_type == "FAQPage":
                    for item in data.get("mainEntity", []):
                        q = item.get("name", "")
                        a = item.get("acceptedAnswer", {}).get("text", "")
                        if q and a:
                            results.append(("schema_faq", q[:500], a[:2000]))

            except (json.JSONDecodeError, AttributeError, TypeError) as exc:
                log.debug(f"JSON-LD parse error: {exc}")
                continue

        return results

    # ── ARIA Labels ───────────────────────────────────────────

    def _extract_aria(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Extract ARIA accessibility attributes:
          aria-label, aria-describedby, role, title

        These are valuable because web developers use them to describe
        interactive elements that may not have visible text labels.
        """
        results = []
        aria_attrs = ["aria-label", "aria-describedby", "aria-description",
                      "title", "role"]

        for tag in soup.find_all(True):
            for attr in aria_attrs:
                value = tag.get(attr, "")
                if value and isinstance(value, str) and len(value) > 2:
                    element_id = tag.get("id", tag.name or "element")
                    key = f"{attr}:{element_id}"[:200]
                    results.append(("aria", key, clean_text(value)[:500]))

        return results

    # ── Image Alt Text ────────────────────────────────────────

    def _extract_img_alt(
        self, soup: BeautifulSoup
    ) -> List[Tuple[str, str, str]]:
        """
        Extract alt text from all images.
        On a satellite data portal, alt text often describes what the
        image shows (e.g., "INSAT-3D Sea Surface Temperature Product").
        """
        results = []
        for img in soup.find_all("img"):
            alt = img.get("alt", "").strip()
            src = img.get("src", "")
            if alt and len(alt) > 3:
                results.append(("alt", src[-100:], clean_text(alt)[:300]))
        return results
