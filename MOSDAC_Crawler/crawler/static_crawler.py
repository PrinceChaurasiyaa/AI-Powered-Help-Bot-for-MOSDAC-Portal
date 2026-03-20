"""
crawler/static_crawler.py
─────────────────────────────────────────────────────────────────────────────
Crawls MOSDAC static (Drupal-rendered) HTML pages.

MOSDAC-specific behaviour
──────────────────────────
  • Skips ALL /internal/ URLs (login-required — confirmed from real HTML)
  • Skips /quicktabs/ajax/ AJAX endpoints
  • Harvests PDFs from div.view-announcement-view on each page
  • Uses MOSDAC Drupal 7 content selectors
  • Treats /faq-page as FAQ source (not /faq — verified from sitemap HTML)
  • Respects 2s polite delay (government server)
─────────────────────────────────────────────────────────────────────────────
"""

import time
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    DOWNLOAD_DELAY,
    EXCLUDED_URL_PATTERNS,
    MAX_DEPTH,
    MAX_PAGES,
    MOSDAC_DOC_PATH_PREFIX,
    REQUEST_TIMEOUT,
    SUPPORTED_DOC_TYPES,
)
from storage.data_store import DataStore
from utils.helpers import (
    can_fetch,
    clean_text,
    content_hash,
    detect_language,
    is_allowed_url,
    is_document_url,
    normalise_url,
    safe_filename,
    url_hash,
)
from utils.logger import get_logger

log = get_logger(__name__)


class StaticCrawler:
    """
    Requests + BeautifulSoup crawler for MOSDAC public pages.

    Usage:
        store   = DataStore()
        crawler = StaticCrawler(store)
        crawler._process_queue()   # Call after seeder has seeded queue
    """

    def __init__(self, store: DataStore):
        self.store           = store
        self.session         = self._build_session()
        self._pages_crawled  = 0

    # ── Session setup ─────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://",  HTTPAdapter(max_retries=retry))
        return session

    # ── Public entry point ────────────────────────────────────

    def crawl(self, start_url: str, depth: int = 0) -> None:
        """Seed a single URL and process the queue."""
        norm = normalise_url(start_url)
        if norm:
            self.store.enqueue_url(norm, url_hash(norm), depth=depth)
        self._process_queue()

    # ── Queue processing ──────────────────────────────────────

    def _process_queue(self) -> None:
        """Drain the crawl queue with polite delays."""
        while self._pages_crawled < MAX_PAGES:
            batch = self.store.get_next_pending(batch_size=5)
            if not batch:
                log.info("Crawl queue empty — finished.")
                break

            for item in batch:
                if self._pages_crawled >= MAX_PAGES:
                    log.warning(f"MAX_PAGES={MAX_PAGES} reached. Stopping.")
                    return

                url   = item["url"]
                depth = item["depth"]

                # ── MOSDAC guard: skip /internal/ entirely ────
                if "/internal/" in url.lower():
                    self.store.mark_url_skipped(url, "login-required /internal/")
                    continue

                # ── Skip Drupal AJAX endpoints ────────────────
                if "/quicktabs/ajax/" in url or "/ajax/" in url:
                    self.store.mark_url_skipped(url, "AJAX endpoint")
                    continue

                # Depth limit
                if depth > MAX_DEPTH:
                    self.store.mark_url_skipped(url, "exceeded max depth")
                    continue

                # Robots.txt check
                if not can_fetch(url):
                    self.store.mark_url_skipped(url, "blocked by robots.txt")
                    continue

                # Document — download and parse
                if is_document_url(url):
                    self._handle_document(url)
                    self.store.mark_url_visited(url)
                    time.sleep(DOWNLOAD_DELAY)
                    continue

                # Normal HTML page
                self._crawl_page(url, depth)
                time.sleep(DOWNLOAD_DELAY)

    # ── Page crawl ────────────────────────────────────────────

    def _crawl_page(self, url: str, depth: int) -> None:
        """Fetch, extract, enqueue children for one HTML page."""
        log.info(f"[{self._pages_crawled + 1}] Crawling (depth={depth}): {url}")

        html, status_code = self._fetch(url)
        if html is None:
            self.store.mark_url_failed(url, f"HTTP {status_code}")
            return

        self.store.mark_url_visited(url)
        self._pages_crawled += 1

        soup = BeautifulSoup(html, "lxml")

        # ── Extract content ───────────────────────────────────
        from extractors.content_extractor import ContentExtractor
        from extractors.faq_extractor     import FAQExtractor
        from extractors.meta_extractor    import MetaExtractor
        from extractors.table_extractor   import TableExtractor

        ce        = ContentExtractor()
        page_data = ce._extract(soup, url, depth, status_code)

        # Deduplication by content hash
        if self.store.is_content_duplicate(page_data["content_hash"]):
            log.debug(f"Duplicate content — skipping save: {url}")
            self._enqueue_links(soup, url, depth)  # Still enqueue links
            return

        self.store.save_page(page_data)

        # FAQs — MOSDAC FAQ page is at /faq-page
        faq_extractor = FAQExtractor()
        faqs = faq_extractor.extract(soup, url)
        for q, a, cat in faqs:
            self.store.save_faq(url, q, a, cat)
        if faqs:
            log.info(f"  ↳ {len(faqs)} FAQs")

        # Tables
        table_extractor = TableExtractor()
        tables = table_extractor.extract(soup, url)
        for idx, (headers, rows, caption) in enumerate(tables):
            self.store.save_table(url, idx, headers, rows, caption)
        if tables:
            log.info(f"  ↳ {len(tables)} tables")

        # Meta / ARIA / OpenGraph
        meta_extractor = MetaExtractor()
        for m_type, key, value in meta_extractor.extract(soup, url):
            self.store.save_meta(url, m_type, key, value)

        # MOSDAC-specific: harvest announcement PDFs from any page that has them
        self._harvest_announcement_pdfs(soup, url, depth)

        # Enqueue child links
        n_enqueued = self._enqueue_links(soup, url, depth)
        log.info(
            f"  ↳ words={page_data['word_count']} | "
            f"type={page_data['page_type']} | "
            f"new_links={n_enqueued}"
        )

    # ── Link extraction ───────────────────────────────────────

    def _enqueue_links(
        self, soup: BeautifulSoup, base_url: str, depth: int
    ) -> int:
        """Extract + validate all links; enqueue unseen ones. Returns count."""
        enqueued  = 0
        seen: Set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            raw = a_tag["href"].strip()

            # Skip Drupal nolink accordion items
            if raw == "#" or "return false" in a_tag.get("onclick", ""):
                continue

            full = normalise_url(raw, base=base_url)
            if not full or full in seen:
                continue
            seen.add(full)

            # MOSDAC-specific: hard block /internal/
            if "/internal/" in full.lower():
                continue

            if not is_allowed_url(full):
                continue

            if self._is_excluded(full):
                continue

            if not self.store.is_url_seen(full):
                uid   = url_hash(full)
                added = self.store.enqueue_url(
                    full, uid, depth=depth + 1, parent_url=base_url
                )
                if added:
                    enqueued += 1

        return enqueued

    # ── Announcement PDF harvesting ───────────────────────────

    def _harvest_announcement_pdfs(
        self, soup: BeautifulSoup, source_url: str, depth: int
    ) -> None:
        """
        MOSDAC-specific: find and queue PDF links in the announcements block.
        These PDFs are goldmines of satellite product documentation.
        """
        from config import ANNOUNCEMENTS_BLOCK_CLASS
        block = soup.find("div", class_=ANNOUNCEMENTS_BLOCK_CLASS)
        if not block:
            return

        for a in block.find_all("a", href=True):
            href     = a["href"].strip()
            full_url = normalise_url(href, base=BASE_URL)
            if full_url and is_document_url(full_url):
                if not self.store.is_url_seen(full_url):
                    uid = url_hash(full_url)
                    self.store.enqueue_url(
                        full_url, uid, depth=depth + 1, parent_url=source_url
                    )
                    log.debug(f"  [PDF queued] {full_url}")

    # ── Document handling ─────────────────────────────────────

    def _handle_document(self, url: str) -> None:
        """Download and parse a document URL."""
        try:
            from crawler.document_parser import DocumentParser
            parser = DocumentParser(self.store)
            parser.download_and_parse(url, source_page_url="")
        except Exception as exc:
            log.error(f"Document handling failed for {url}: {exc}")

    # ── HTTP fetch ────────────────────────────────────────────

    def _fetch(self, url: str) -> Tuple[Optional[str], int]:
        """Return (html, status_code) or (None, status_code) on error."""
        try:
            resp = self.session.get(
                url, timeout=REQUEST_TIMEOUT, allow_redirects=True
            )
            if resp.status_code == 200:
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text, 200
            log.warning(f"HTTP {resp.status_code}: {url}")
            return None, resp.status_code
        except requests.exceptions.RequestException as exc:
            log.error(f"Request failed: {url} — {exc}")
            return None, 0

    # ── Exclusion helper ──────────────────────────────────────

    def _is_excluded(self, url: str) -> bool:
        lower = url.lower()
        for pattern in EXCLUDED_URL_PATTERNS:
            if pattern in lower:
                return True
        return False

    @property
    def pages_crawled(self) -> int:
        return self._pages_crawled
