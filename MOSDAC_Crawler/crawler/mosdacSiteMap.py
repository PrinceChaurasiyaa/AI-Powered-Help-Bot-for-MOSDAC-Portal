"""
This file discovers all MOSDAC URLs and seeds them into the crawler queue so 
the scraper can collect data for your AI help bot.
Parses the MOSDAC /sitemap HTML page to extract ALL public URLs.
"""

from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..config import (
    ANNOUNCEMENTS_BLOCK_CLASS,
    BASE_URL,
    DEFAULT_HEADERS,
    EXCLUDED_URL_PATTERNS,
    REQUEST_TIMEOUT,
    SITEMAP_URL,
    SUPPORTED_DOC_TYPES
)

from ..utils.helpers import (
    is_allowed_url,
    is_document_url,
    normalize_url,

)

from ..utils.logger import get_logger

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

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    # ============================ Public API ======================================
    def seed_all(self) -> Tuple[int, int]:
        """
        Fetch sitemap + homepage, extract all URLs, populate queue.
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
            # uid = url_hash(url)
            page_count += 1
            log.debug(f"    [page] {url}")
        
        doc_count = 0
        for url in set(doc_urls):
            doc_count += 1
            log.debug(f"    [doc] {url}")
        
        log.info(
            f"Queue seeded: {page_count} page URLs + "
            f"{doc_count} document URLs"
        )
        return page_count, doc_count

        
        
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
            
            full_url = normalize_url(href, base=BASE_URL)
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
            full_url = normalize_url(href, base=BASE_URL)
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
        
