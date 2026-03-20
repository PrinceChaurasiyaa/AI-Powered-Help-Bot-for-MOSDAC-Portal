"""
This file discovers all MOSDAC URLs and seeds them into the crawler queue so 
the scraper can collect data for your AI help bot.
Parses the MOSDAC /sitemap HTML page to extract ALL public URLs.
"""

from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    ANNOUNCEMENTS_BLOCK_CLASS,
    BASE_URL,
    DEFAULT_HEADERS,
    EXCLUDED_URL_PATTERNS,
    REQUEST_TIMEOUT,
    SITEMAP_URL,
    SUPPORTED_DOC_TYPES,
    KNOWN_MISSIONS,
    MISSION_SIDEBAR_BLOCK_PREFIX,
    MISSION_SIDEBAR_BLOCK_SUFFIX,
    MISSION_SUBPAGE_TYPES,
    DRUPAL_MISSION_NODE_TYPE,
)

from storage.data_store import DataStore

from utils.helpers import (
    is_allowed_url,
    is_document_url,
    normalise_url,
    url_hash,
)

from utils.logger import get_logger
log = get_logger(__name__)

class MOSDACsitemap:
    """
    Fetches and parses the MOSDAC /sitemap page to seed the crawl queue.
    Usage:
        store  = DataStore()
        seeder = MOSDACsitemap(store)
        pages, docs = seeder.seed_all()
        print(f"Seeded {pages} page URLs and {docs} document URLs")
    """

    def __init__(self, store: DataStore):
        self.store = store
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    # ============================ Public API ======================================
    def seed_all(self, seed_mission_subpages: bool = True) -> Tuple[int, int]:
        """
        Fetch sitemap + homepage, extract all URLs, populate queue.
        Pass seed_mission_subpages=True (default) to also visit each
        known mission landing page and discover sub-pages from the
        sidebar navigation (Introduction, Objectives, SpaceCraft,
        Payloads, Documents).  These URLs are NOT in /sitemap.
        Returns (page_count, doc_count). 
        """

        page_urls: List[str] = []
        doc_urls:  List[str] = []

        # ------------------------ Step 1: Parse /site-map ------------------------------------------
        log.info(f"Fetching sitemap: {SITEMAP_URL}")
        sitmap_html = self._fetch(SITEMAP_URL)
        if sitmap_html:
            p, d = self._parse_sitemap(sitmap_html)
            page_urls.extend(p)
            doc_urls.extend(d)
            log.info(f"Sitemap: {len(p)} page URLs, {len(d)} doc URLs found")

        # ------------------------ Step 2: Homepage - harvest Announcements PDFs --------------------
        log.info(f"Fetching homepage for Announcements: {BASE_URL}")
        home_html = self._fetch(BASE_URL)
        if home_html:
            announcement_docs = self._parse_announcements(home_html)
            doc_urls.extend(announcement_docs)
            log.info(f"Announcements: {len(announcement_docs)} PDF links found")


        # ------------------------ Step 3: Enque Everything -----------------------------------------
        page_count = 0
        for url in set(page_urls):
            uid = url_hash(url)
            if not self.store.is_url_seen(url):
                added = self.store.enqueue_url(url, uid, depth=0)
                if added:
                    page_count += 1
                    log.debug(f"    [page] {url}")
        
        doc_count = 0
        for url in set(doc_urls):
            uid = url_hash(url)
            if not self.store.is_url_seen(url):
                added = self.store.enqueue_url(url, uid, depth=0)
                if added:
                    doc_count += 1
                    log.debug(f"    [doc] {url}")
        
        # ── Step 4: Mission sub-page discovery ────────────────
        mission_page_count = 0
        if seed_mission_subpages:
            mission_page_count = self.seed_mission_subpages()
            total_pages = page_count + mission_page_count
        
        log.info(
            f"Queue seeded: {total_pages} page URLs total "
            f"({page_count} sitemap + {mission_page_count} mission sub-pages) "
            f"+ {doc_count} document URLs"
        )
        return total_pages, doc_count

    # =============================== Mission sub-page discovery ============================================

    def seed_mission_subpages(self) -> int:
        """
        For each known mission, visit its landing page and extract
        sub-page links from the sidebar navigation block.

        Structure (confirmed from real INSAT-3D HTML):
          <nav id="block-menu-menu-insat-3d-menu" role="navigation">
            <ul class="menu clearfix">
              <li class="expanded active-trail">       ← "Missions"
                <ul>
                  <li class="expanded active-trail">  ← "INSAT-3D"
                    <a href="/insat-3d">INSAT-3D</a>
                    <ul>
                      <li><a href="/insat-3d-introduction">Introduction</a></li>
                      <li><a href="/insat-3d-objectives">Objectives</a></li>
                      <li><a href="/insat-3d-spacecraft">SpaceCraft</a></li>
                      <li><a href="/insat-3d-payloads">Payloads</a></li>
                      <li><a href="/internal/catalog-insat3d">Data Products</a></li>
                      <li><a href="/insat-3d-references">Documents</a></li>
                    </ul>
                  </li>
                </ul>
              </li>
            </ul>
          </nav>

        Saves each sub-page to mission_hierarchy table.
        Adds public sub-page URLs (not /internal/) to crawl_queue.
        Returns count of new URLs seeded.
        """

        log.info("====Mission sub-page discovery pass =====================")
        total_seeded = 0

        for slug, name in KNOWN_MISSIONS.items():
            mission_url = f"{BASE_URL}/{slug}"
            log.info(f"     Scanning mission: {name} -> {mission_url}")

            # 1. Register the landing page itself
            self.store.save_mission_subpage(
                mission_slug=slug,
                mission_name=name,
                mission_url=mission_url,
                section_type="landing",
                section_title=name,
                section_url=mission_url,
            )

            # 2. Fetch the mission landing page
            html = self._fetch(mission_url)
            if not html:
                log.warning(f"     Could not fetch {mission_url}")
                continue

            # 3. Find the sidebar navigation block
            soup = BeautifulSoup(html, "lxml")
            sidebar_block = self._find_mission_sidebar(soup, slug)
            if not sidebar_block:
                log.warning(
                    f"  Sidebar nav not found for {slug} - "
                    f"trying fallback link extraction"
                )
                subpages = self._fallback_subpage_links(html, slug)
            else:
                subpages = self._extract_subpage_links(sidebar_block, slug)
            
            if not subpages:
                log.warning(f"  No sub-pages found for {name}")
                continue

            log.info(f"  Found {len(subpages)} sub-pages for {name}:")
            for section_type, title, url in subpages:
                log.info(f"     [{section_type:14s}] {title} -> {url}")

                self.store.save_mission_subpage(
                    mission_slug=slug,
                    mission_name=name,
                    mission_url=mission_url,
                    section_type=section_type,
                    section_title=title,
                    section_url=url,
                )

                if not self._is_excluded(url):
                    uid = url_hash(url)
                    added = self.store.enqueue_url(
                        url, uid, depth=1, parent_url=mission_url
                    )
                    if added: 
                        total_seeded +=1
                else:
                    log.debug(f"       [skip /internal] {url}")
        
        log.info(f"Mission sub-page seeding done - {total_seeded} new URLs queued")
        return total_seeded
    
    def _find_mission_sidebar(
            self, soup: BeautifulSoup, mission_slug: str
    ):
        """Locate the mission-specific sidebar navigation block.

        ID pattern: block-menu-menu-{slug}-menu
        e.g.  block-menu-menu-insat-3d-menu
              block-menu-menu-oceansat-3-menu"""
        
        block_id = (
            MISSION_SIDEBAR_BLOCK_PREFIX
            + mission_slug 
            + MISSION_SIDEBAR_BLOCK_SUFFIX
        )
        block = soup.find(id=block_id)
        if block:
            return block
        
        megha_tropiques = MISSION_SIDEBAR_BLOCK_PREFIX + "mt" + MISSION_SIDEBAR_BLOCK_SUFFIX
        megha = soup.find(id=megha_tropiques)
        if megha:
            return megha
        
        saral_altika = MISSION_SIDEBAR_BLOCK_PREFIX + "saral" + MISSION_SIDEBAR_BLOCK_SUFFIX
        saral = soup.find(id=saral_altika)
        if saral:
            return saral
        
        # for tag in soup.find_all(id=True):
        #     if tag["id"].startswith(prefix):
        #         return tag
        
        return None
    
    def _extract_subpage_links(
            self, sidebar_block, mission_slug: str
    ) -> List[Tuple[str, str, str]]:
        """Walk the sidebar nav block and return sub-page tuples:
          (section_type, title, full_url)

        The third-level <ul> inside the block contains the sub-page links.
        Skips href="#" (accordion toggles) and the landing page itself."""

        results = []
        landing_path = f"/{mission_slug}"

        for a in sidebar_block.find_all("a", href=True):
            href = a["href"].strip()
            title = a.get_text(strip=True)

            if not href or href == "#" or not title:
                continue

            if href.endswith(landing_path) or href == landing_path:
                continue

            full_url = normalise_url(href, base=BASE_URL)
            if not full_url:
                continue

            section_type = self._classify_section(href, title)
            results.append((section_type, title, full_url))
        
        return results
    
    def _fallback_subpage_links(
            self, html: str, mission_slug: str
    ) -> List[Tuple[str, str, str]]:
        """If sidebar block not found, look for links matching
        /{mission_slug}-{section} pattern anywhere on the page."""

        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            title = a.get_text(strip=True)

            if not href or href == "#" or href in seen:
                continue

            if not href.startswith(f"/{mission_slug}-"):
                continue

            full_url = normalise_url(href, base=BASE_URL)
            if not full_url:
                continue

            section_type = self._classify_section(href, title)
            results.append((section_type, title, full_url))
            seen.add(href)

        return results
    
    def _classify_section(self, href: str, title: str) -> str:
        """Map href/title to a canonical section_type string."""

        href_lower = href.lower()
        title_lower = title.lower()

        if "introduction" in href_lower or "introduction" in title_lower:
            return "introduction"
        if "objective" in href_lower or "objective" in title_lower:
            return "objective"
        if "spacecraft" in href_lower or "spacecraft" in title_lower:
            return "spacecraft"
        if "payload" in href_lower or "payload" in title_lower:
            return "payload"
        if "reference" in href_lower or "document" in title_lower:
            return "reference"
        if "catalog" in href_lower or "data product" in title_lower:
            return "catalog"
        
        # Anything else — store as 'other' with original title
        return "other"
    
        
    # ================================ Sitemap parsing  =====================================================
    def _parse_sitemap(self, html: str) -> Tuple[List[str], List[str]]:
        """
        Parse MOSDAC /sitemap HTML.
 
        Structure (from real HTML):
          div#site-map > div.site-map-menus
            > div.site-map-box
              > div.content
                > ul.site-map-menu (nested lists)
                  > li > a href="..."
 
        Returns (page_urls, doc_urls).
        """
        soup = BeautifulSoup(html, "lxml")
        page_urls = []
        doc_urls = []

        # Find all sitemap menu links
        sitemap_div = soup.find("div", id="site-map")
        if not sitemap_div:
            log.warning("div#site-map not found - falling back to all links")
            sitemap_div = soup
        
        for a in sitemap_div.find_all("a", href=True):
            # <a href="/insat-3dr" style="font-size: 20px;">INSAT-3DR</a>
            href = a["href"].strip()

            # Skip non-links (Drupal accordion: onclick="return false;")
            if not href or href == "#":
                #<a href="#" onclick="return false;" title="" class="nolink" tabindex="0" style="font-size: 20px;">Catalog</a>
                continue
            
            full_url = normalise_url(href, base=BASE_URL)
            if not full_url:
                # None is treated as False
                continue

            if not is_allowed_url(full_url):
                continue

            if self._is_excluded(full_url):
                log.debug(f"    [skip] {full_url}")
                continue

            if is_document_url(full_url):
                doc_urls.append(full_url)
            else:
                page_urls.append(full_url)


        return page_urls, doc_urls



    # ================================ Announcement parsing ==================================================
    def _parse_announcements(self, html: str) -> List[str]:
        """
        Extract PDF links from the Announcements block on the homepage.
 
        Structure (from real HTML):
          div.view-announcement-view
            div.views-row
              span.views-field-title-field
                span.field-content
                  a href="https://www.mosdac.gov.in/sites/default/files/docs/..."
                    Document Title
              span.views-field-filesize ...
              span.views-field-created ...  (date)
        """

        soup = BeautifulSoup(html, "lxml")
        block = soup.find("div", class_=ANNOUNCEMENTS_BLOCK_CLASS)
        if not block:
            return []
        
        doc_urls = []
        for a in block.find_all("a", href=True):
            href = a["href"].strip()
            full_url = normalise_url(href, base=BASE_URL)
            if full_url and is_document_url(full_url):
                title = a.get_text(strip=True)
                log.info(f" [announcement PDF] {title[:60]} -> {full_url}")
                doc_urls.append(full_url)
        

        return doc_urls



    # ================================ Helpers ===============================================================
    def _fetch(self, url: str) -> str:
        """Fetch URL, return HTML string or empty string on failure."""
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        
        except Exception as exc:
            log.error(f"Failed to fetch {url}: {exc}")
            return ""

    def _is_excluded(self, url: str) -> bool:
        """True if URL matches any exclusion pattern."""
        lower = url.lower()
        for pattern in EXCLUDED_URL_PATTERNS:
            if pattern in lower:
                return True
        return False
        
