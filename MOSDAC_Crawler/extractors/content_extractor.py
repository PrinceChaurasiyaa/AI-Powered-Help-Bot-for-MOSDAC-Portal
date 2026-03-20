"""
Extracts clean text from MOSDAC pages (Drupal 7 CMS).

MOSDAC-specific adaptations (from real HTML analysis)
  • Targets div.region-content / div#content (Drupal content region)
  • Strips Drupal chrome: nav menus, superfish, footer, sticky bar,
    lang selector, contrast switcher, quicktabs widget chrome
  • Strips div#site-map list navigation items (they're just link lists)
  • Extracts Drupal Views row content (div.views-row)
  • Preserves figcaption text (service thumbnails have useful descriptions)
  • Extracts Announcements block (title + date + PDF link)
  • Page classification uses MOSDAC-specific domain vocabulary
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, NavigableString, Tag

from config import (
    DOCUMENT_KEYWORDS,
    DRUPAL_BOILERPLATE_CLASSES,
    DRUPAL_BOILERPLATE_IDS,
    DRUPAL_CONTENT_SELECTORS,
    DRUPAL_MISSION_NODE_TYPE,
    FAQ_KEYWORDS,
    GEOSPATIAL_KEYWORDS,
    KNOWN_MISSIONS,
    MISSION_CATALOG_SLUGS,
    MISSION_KEYWORDS,
    MISSION_SIDEBAR_BLOCK_PREFIX,
    MISSION_SIDEBAR_BLOCK_SUFFIX,
    MISSION_SUBPAGE_SLUG_OVERRIDES,
    MISSION_SUBPAGE_TYPES,
    PRODUCT_KEYWORDS,
)
from utils.helpers import clean_text, content_hash, detect_language, url_hash
from utils.logger import get_logger

log = get_logger(__name__)

HEADING_MAP = {
    "h1": "# ", "h2": "## ", "h3": "### ",
    "h4": "#### ", "h5": "##### ", "h6": "###### ",
}

# Tags whose entire content is discarded during text walk
SKIP_TAGS = {
    "script", "style", "noscript", "iframe",
    "form",       # Search/login forms — no content value
    "button",
    "svg",
}


class ContentExtractor:
    """
    Extracts clean structured text from MOSDAC Drupal 7 pages.

    Usage:
        extractor = ContentExtractor()
        data = extractor.extract(soup, url, depth=0, status_code=200)
    """

    def _extract(
            self,
            soup: BeautifulSoup,
            url: str,
            depth: int = 0,
            status_code: int = 200,
    ) -> Dict[str, Any]:
        """Main pipeline. Returns dict for DataStore.save_page()."""

        title = self._extract_title(soup)
        main_block = self._find_main_block(soup)
        

        self._strip_drupal_chrome(main_block)
        self._strip_quicktabs_chrome(main_block)

        content_text = self._extract_text(main_block)

        # Also extract the Announcements block if on homepage
        announcements = self._extract_announcements(soup)
        if announcements:
            content_text = content_text + "\n\n" + announcements

        content_text = clean_text(content_text)
        c_hash       = content_hash(content_text)
        uid          = url_hash(url)
        word_count   = len(content_text.split())
        language     = detect_language(content_text)
        page_type    = self._classify_page(url, title, content_text)

        # ── Mission-specific enrichment ──────────────────────
        mission_ctx = self._extract_mission_context(soup, url)

        result = {
            "url":          url,
            "url_hash":     uid,
            "title":        title,
            "content_text": content_text,
            "content_html": str(main_block)[:50_000],
            "content_hash": c_hash,
            "page_type":    page_type,
            "depth":        depth,
            "links_found":  len(soup.find_all("a", href=True)),
            "crawled_at":   datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "status_code":  status_code,
            "language":     language,
            "word_count":   word_count,
        }
        result.update(mission_ctx)
        return result

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """
        MOSDAC has meta name="title" (non-standard) in addition to <title>.
        Priority: meta[name=title] → <title> → og:title → h1
        """
        mt = soup.find("meta", attrs={"name": "title"})
        if mt and mt.get("content"):
            return clean_text(mt["content"])[:300]
        
        title_tag = soup.find("title")
        if title_tag and title_tag.get_text(strip=True):
            t = clean_text(title_tag.get_text(strip=True))

            if " | " in t:
                t = t.split(" | ")[0]
            return t[: 300]
        
        h1 = soup.find("h1", class_="title") or soup.find("h1")
        if h1:
            return clean_text(h1.get_text(strip = True))[:300]
        
        return "Untitled"
        

    def _find_main_block(self, soup: BeautifulSoup) -> Tag:
        """
        Locate the primary content region using MOSDAC Drupal 7 selectors.
        Falls back to <body> if nothing matches.
        """
        for tag, attrs in DRUPAL_CONTENT_SELECTORS:
            el = soup.find(tag, attrs)
            if el:
                return el
        
        return soup.find("body") or soup


    def _strip_drupal_chrome(self, block: Tag) -> None:
        """
        Remove Drupal UI elements that are not content:
          - Navigation menus (superfish, secondary-menu)
          - Footer blocks
          - Accessibility toolbar (sticky bar)
          - Language selector, contrast switcher
        """
        if not isinstance(block, Tag):
            return
        
        for el_id in DRUPAL_BOILERPLATE_IDS:
            for el in block.find_all(id=el_id):
                el.decompose()
        
        boilerplate_class_patterns = list(DRUPAL_BOILERPLATE_CLASSES) + [
            "sf-menu",
            "superfish",
            "quicktabs-tabs",      # Tab navigation bar (not content)
            "element-invisible",   # Screen-reader-only text (labels)
            "rdf-meta",            # RDF metadata spans
            "contextual-links",    # Drupal admin links
        ]
        for el in block.find_all(True):
            if not isinstance(el, Tag) or el.attrs is None:   # ← ADD THIS LINE
                continue
            classes = " ".join(el.get("class", [])).lower()
            el_id   = (el.get("id") or "").lower()
            combined = classes + " " + el_id
            if any(p in combined for p in boilerplate_class_patterns):
                el.decompose()

        # Strip <nav> blocks entirely
        for nav in block.find_all("nav"):
            nav.decompose()

    def _strip_quicktabs_chrome(self, block: Tag) -> None:
        """
        MOSDAC uses Drupal Quicktabs heavily on the homepage.
        The HIDDEN tabs (quicktabs-hide) have no rendered content.
        Keep only the ACTIVE tab content; remove hidden tabpages.

        Also remove the quicktabs tab navigation ul (just tab labels).
        """
        if not isinstance(block, Tag):
            return

        # Remove hidden quicktab pages
        for el in block.find_all("div", class_="quicktabs-hide"):
            el.decompose()

        # Remove quicktabs tab navigation (not content)
        for el in block.find_all("ul", class_="quicktabs-tabs"):
            el.decompose()
    
    def _extract_announcements(self, soup: BeautifulSoup) -> str:
        """
        Extract the Announcements block from the homepage.
        This block (div.view-announcement-view) contains:
          - Document title (linked to PDF)
          - File size
          - Publication date
        Returns a structured text block.
        """
        block = soup.find("div", class_="view-announcement-view")
        if not block:
            return ""

        lines = ["## Announcements"]
        for row in block.find_all("div", class_="views-row"):
            title_span = row.find("span", class_="field-content")
            date_span  = row.find("span", class_="views-field-created")
            size_span  = row.find("span", class_="views-field-filesize")

            title = clean_text(title_span.get_text()) if title_span else ""
            date  = clean_text(date_span.get_text())  if date_span  else ""
            size  = clean_text(size_span.get_text())  if size_span  else ""

            if title:
                line = f"• {title}"
                if date:
                    line += f" ({date})"
                if size:
                    line += f" [{size}]"
                lines.append(line)

        return "\n".join(lines) if len(lines) > 1 else ""
    
    def _extract_text(self, block) -> str:
        """
        Recursive DOM walk — produces clean structured text.
        """
        if not block:
            return ""
        lines = []
        self._walk(block, lines)
        return "\n".join(lines)
    
    def _walk(self, node, lines: list) -> None:
        """Recursive DOM walk — produces clean structured text."""
        if isinstance(node, NavigableString):
            text = str(node).strip()
            if text:
                lines.append(text)
            return

        if not isinstance(node, Tag):
            return

        tag = node.name.lower() if node.name else ""

        if tag in SKIP_TAGS:
            return

        if tag in HEADING_MAP:
            text = node.get_text(separator=" ", strip=True)
            if text:
                lines.append(f"\n{HEADING_MAP[tag]}{text}\n")
            return

        if tag == "p":
            text = node.get_text(separator=" ", strip=True)
            if text:
                lines.append(text)
                lines.append("")
            return

        if tag == "figcaption":
            # MOSDAC service tiles have figcaption with title + description
            text = node.get_text(separator=" ", strip=True)
            if text:
                lines.append(f"Service: {text}")
            return

        if tag in ("li", "dt", "dd"):
            text = node.get_text(separator=" ", strip=True)
            if text:
                lines.append(f"• {text}")
            return

        if tag == "br":
            lines.append("")
            return

        if tag == "table":
            return  # TableExtractor handles tables

        if tag == "a":
            text = node.get_text(separator=" ", strip=True)
            if text:
                lines.append(text)
            return

        # Recurse into block-level elements
        for child in node.children:
            self._walk(child, lines)

        if tag in {"div", "section", "article", "main", "aside",
                   "blockquote", "pre", "td", "th"}:
            lines.append("")
        
    # ── Page classification ───────────────────────────────────

    # Build a flat lookup of ALL URL slugs that belong to missions, including
    # overridden sub-page slugs. Pre-computed at class definition time.
    # e.g. { 'insat-3d': 'insat-3d', 'insat-3ds': 'insat-3s', ... }
    _SLUG_TO_MISSION: dict = {}   # populated in __init_subclass__ below
    
    @classmethod
    def _build_slug_map(cls) -> dict:
        """
        Build a mapping: url_slug_fragment → landing_slug

        MISSION_SUBPAGE_SLUG_OVERRIDES format:  { url_slug: landing_slug }
        e.g.  "insat-3s"  → "insat-3ds"   (INSAT-3DS sub-pages use insat-3s)
              "saral"     → "saral-altika" (/saral-references)
              "oceansat3" → "oceansat-3"   (/oceansat3-references, no hyphen)

        Result entries:
          landing slug  → itself         (every landing page)
          url slug      → landing slug   (override sub-page URLs)
        """
        result = {}
        # Every landing slug maps to itself
        for slug in KNOWN_MISSIONS:
            result[slug] = slug
        # Override entries: url_slug (key) → landing_slug (value)
        # Iterate directly — don't try to look them up via landing slug
        for url_slug, landing_slug in MISSION_SUBPAGE_SLUG_OVERRIDES.items():
            result[url_slug] = landing_slug
        return result

    def _classify_page(self, url: str, title: str, text: str) -> str:
        """
        Classify page type using MOSDAC domain vocabulary.
        Types: faq | mission | mission_section | product | document
               | open_data | geospatial | general

        Handles real-world slug mismatches discovered from actual crawl logs:
          - INSAT-3DS sub-pages use /insat-3s-* (not /insat-3ds-*)
          - SARAL-AltiKa references use /saral-references
          - OCEANSAT-3 references use /oceansat3-references
        """
        combined  = (url + " " + title + " " + text[:600]).lower()
        url_lower = url.lower()

        if any(kw in combined for kw in FAQ_KEYWORDS):
            return "faq"

        # Build slug map once (cached on the class after first call)
        if not ContentExtractor._SLUG_TO_MISSION:
            ContentExtractor._SLUG_TO_MISSION = self._build_slug_map()

        slug_map = ContentExtractor._SLUG_TO_MISSION

        # Check every known URL slug fragment (landing + sub-page overrides)
        for url_slug, landing_slug in slug_map.items():
            if f"/{url_slug}" not in url_lower:
                continue

            # Matched a mission URL fragment — now determine section type.
            # Strip the matched slug from the URL to get the tail.
            # e.g. "/insat-3s-payloads" → tail = "payloads"
            tail = url_lower.split(f"/{url_slug}")[-1].lstrip("/").lstrip("-")

            if tail and any(sec in tail for sec in MISSION_SUBPAGE_TYPES):
                return "mission_section"
            if not tail:
                return "mission"           # pure landing page
            # tail exists but no section keyword → still a mission page
            return "mission"

        # Open data product pages
        open_data_slugs = [
            "rainfall", "water-vapour", "gsmap", "cloud-properties",
            "soil-moisture", "river-discharge", "inland-water",
            "ocean-surface", "sea-surface", "salinity", "coastal-product",
            "ocean-subsurface", "eddies", "sea-ice", "wave-based",
            "volumetric", "dwrproduct",
        ]
        if any(slug in url_lower for slug in open_data_slugs):
            return "open_data"

        # For keyword checks, use only title + content — NOT the URL.
        # The URL always contains 'mosdac.gov.in' so MISSION_KEYWORDS would
        # match every single page on the site if we included the URL.
        title_and_text = (title + " " + text[:600]).lower()
        if any(kw in title_and_text for kw in MISSION_KEYWORDS):
            return "mission"
        if any(kw in title_and_text for kw in PRODUCT_KEYWORDS):
            return "product"
        if any(kw in title_and_text for kw in DOCUMENT_KEYWORDS):
            return "document"
        if any(kw in title_and_text for kw in GEOSPATIAL_KEYWORDS):
            return "geospatial"

        return "general"

    # ── Mission context extraction ────────────────────────────

    def _extract_mission_context(
        self, soup: BeautifulSoup, url: str
    ) -> Dict[str, Any]:
        """
        For mission and mission_section pages, extract structured context:
          - which mission this page belongs to
          - which section type (landing | introduction | payloads | …)
          - the Drupal node type from <body class="...">
          - sub-page links discovered from the sidebar navigation block

        Returns a dict with keys:
          mission_slug, mission_name, mission_section, drupal_node_type,
          mission_subpage_urls

        All keys default to None / [] if the page is not a mission page.
        """
        ctx: Dict[str, Any] = {
            "mission_slug":         None,
            "mission_name":         None,
            "mission_section":      None,   # landing|introduction|payloads|…
            "drupal_node_type":     None,
            "mission_subpage_urls": [],
        }

        url_lower = url.lower()

        # ── 1. Identify mission slug ──────────────────────────
        # Use the same slug map as _classify_page so we correctly resolve
        # missions where sub-page URLs use a different slug than the landing.
        # e.g. /insat-3s-payloads → landing slug = 'insat-3ds'
        if not ContentExtractor._SLUG_TO_MISSION:
            ContentExtractor._SLUG_TO_MISSION = self._build_slug_map()

        matched_landing_slug = None
        matched_url_slug     = None
        for url_slug, landing_slug in ContentExtractor._SLUG_TO_MISSION.items():
            if f"/{url_slug}" in url_lower:
                matched_landing_slug = landing_slug
                matched_url_slug     = url_slug
                break

        if matched_landing_slug is None:
            return ctx   # Not a mission page

        ctx["mission_slug"] = matched_landing_slug
        ctx["mission_name"] = KNOWN_MISSIONS[matched_landing_slug]

        # ── 2. Identify section type ──────────────────────────
        # Split on the ACTUAL URL slug (may differ from landing slug)
        tail = url_lower.split(f"/{matched_url_slug}")[-1].lstrip("/").lstrip("-")
        if not tail:
            ctx["mission_section"] = "landing"
        else:
            found_section = "other"
            for sec in MISSION_SUBPAGE_TYPES:
                if sec in tail:
                    found_section = sec
                    break
            ctx["mission_section"] = found_section

        # ── 3. Drupal node type (from <body class="…"> ) ──────
        body = soup.find("body")
        if body:
            body_classes = " ".join(body.get("class", []))
            if DRUPAL_MISSION_NODE_TYPE in body_classes:
                ctx["drupal_node_type"] = DRUPAL_MISSION_NODE_TYPE
            else:
                # Extract node-type-* class if present
                for cls in body.get("class", []):
                    if cls.startswith("node-type-"):
                        ctx["drupal_node_type"] = cls
                        break

        # ── 4. Discover sub-page URLs from sidebar nav ────────
        # Only do this on landing pages to avoid redundant work
        if ctx["mission_section"] == "landing":
            subpage_urls = self._extract_sidebar_subpage_urls(
                soup, matched_landing_slug
            )
            ctx["mission_subpage_urls"] = subpage_urls
            if subpage_urls:
                log.debug(
                    f"Mission {matched_landing_slug}: "
                    f"found {len(subpage_urls)} sidebar sub-page links"
                )

        return ctx

    def _extract_sidebar_subpage_urls(
        self, soup: BeautifulSoup, mission_slug: str
    ) -> List[str]:
        """
        Find the mission sidebar nav block and return sub-page hrefs.

        Block ID pattern: block-menu-menu-{slug}-menu
        Returns absolute URLs (only public, non-/internal/ ones).
        """
        from config import BASE_URL  # avoid circular at module level

        block_id = (
            MISSION_SIDEBAR_BLOCK_PREFIX
            + mission_slug
            + MISSION_SIDEBAR_BLOCK_SUFFIX
        )
        block = soup.find(id=block_id)
        if not block:
            return []

        landing_path = f"/{mission_slug}"
        urls = []
        for a in block.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href == "#":
                continue
            # Skip the landing page link and /internal/ links
            if href.endswith(landing_path):
                continue
            if "/internal/" in href:
                continue
            # Build absolute URL
            if href.startswith("http"):
                full = href
            elif href.startswith("/"):
                full = BASE_URL + href
            else:
                continue
            urls.append(full)

        return urls
