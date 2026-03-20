"""
crawler/dynamic_crawler.py
─────────────────────────────────────────────────────────────────────────────
Playwright-based crawler for JavaScript-rendered pages.

When to use
───────────
  • Pages that show a spinner / skeleton before content appears
  • SPA (Single Page Application) routes
  • Infinite-scroll product catalogues
  • Interactive map / viewer pages

How it works
────────────
  1. Launch a headless Chromium browser via Playwright
  2. Navigate to the URL and wait for network activity to settle
  3. Auto-scroll the page to trigger lazy-loaded content
  4. Extract the final rendered HTML
  5. Pass to the same extractors as StaticCrawler
─────────────────────────────────────────────────────────────────────────────
"""

from typing import Optional, Tuple

from config import (
    BROWSER_WAIT_MS,
    JS_SCROLL_PAUSE_MS,
    USE_HEADLESS_BROWSER,
)
from storage.data_store import DataStore
from utils.helpers import content_hash, url_hash
from utils.logger import get_logger

log = get_logger(__name__)


class DynamicCrawler:
    """
    Playwright-powered crawler.

    Usage:
        store = DataStore()
        dyn   = DynamicCrawler(store)
        dyn.crawl_page("https://www.mosdac.gov.in/catalog", depth=1)

    Playwright must be installed separately:
        pip install playwright
        playwright install chromium
    """

    def __init__(self, store: DataStore):
        self.store = store
        self._playwright = None
        self._browser    = None

    # ── Lifecycle ─────────────────────────────────────────────

    def _start(self):
        """Launch Playwright + Chromium (call once per session)."""
        try:
            from playwright.sync_api import sync_playwright
            self._pw_ctx  = sync_playwright().__enter__()
            self._browser = self._pw_ctx.chromium.launch(
                headless=USE_HEADLESS_BROWSER,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
            log.info("Playwright Chromium browser launched")
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed.\n"
                "Run:  pip install playwright && playwright install chromium"
            )

    def _stop(self):
        """Close browser and Playwright context."""
        if self._browser:
            self._browser.close()
        if hasattr(self, "_pw_ctx") and self._pw_ctx:
            self._pw_ctx.__exit__(None, None, None)
        log.info("Playwright browser closed")

    # ── Public API ────────────────────────────────────────────

    def crawl_page(self, url: str, depth: int = 0) -> None:
        """
        Navigate to `url`, wait for JS to render, extract content.
        """
        self._start()
        try:
            html, status_code = self._fetch_rendered(url)
            if html:
                self._process_html(html, url, depth, status_code)
        finally:
            self._stop()

    # ── Internal ─────────────────────────────────────────────

    def _fetch_rendered(self, url: str) -> Tuple[Optional[str], int]:
        """
        Open a new browser page, navigate, scroll, and return final HTML.
        """
        page = self._browser.new_page()

        # Block heavy assets we don't need (images, fonts, media)
        page.route(
            "**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,mp4,mp3}",
            lambda route: route.abort(),
        )

        try:
            response = page.goto(
                url,
                wait_until="networkidle",   # Wait until network is quiet
                timeout=30_000,             # 30 seconds
            )
            status_code = response.status if response else 0

            # Extra wait for any post-load JS
            page.wait_for_timeout(BROWSER_WAIT_MS)

            # Auto-scroll to load lazy content
            self._auto_scroll(page)

            html = page.content()
            log.info(f"[Playwright] Rendered: {url} (status={status_code})")
            return html, status_code

        except Exception as exc:
            log.error(f"[Playwright] Failed to render {url}: {exc}")
            return None, 0
        finally:
            page.close()

    def _auto_scroll(self, page) -> None:
        """
        Scroll the page in steps to trigger lazy-loaded content.
        Stops when we reach the bottom or after 10 scrolls.
        """
        try:
            total_height = page.evaluate("document.body.scrollHeight")
            viewport_h   = page.viewport_size["height"] if page.viewport_size else 800
            current_y    = 0
            scrolls      = 0

            while current_y < total_height and scrolls < 10:
                page.evaluate(f"window.scrollTo(0, {current_y + viewport_h})")
                page.wait_for_timeout(JS_SCROLL_PAUSE_MS)
                current_y    += viewport_h
                scrolls      += 1
                # Re-check height (content may have loaded more)
                total_height = page.evaluate("document.body.scrollHeight")

            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            log.debug(f"Auto-scroll: {scrolls} scroll steps performed")
        except Exception as exc:
            log.debug(f"Auto-scroll error (non-fatal): {exc}")

    def _process_html(self, html: str, url: str,
                      depth: int, status_code: int) -> None:
        """Run extractors on the rendered HTML and persist results."""
        from bs4 import BeautifulSoup

        from extractors.content_extractor import ContentExtractor
        from extractors.faq_extractor     import FAQExtractor
        from extractors.meta_extractor    import MetaExtractor
        from extractors.table_extractor   import TableExtractor

        soup = BeautifulSoup(html, "lxml")

        # Content
        extractor = ContentExtractor()
        page_data = extractor.extract(soup, url, depth, status_code)

        c_hash = page_data["content_hash"]
        if self.store.is_content_duplicate(c_hash):
            log.debug(f"[Playwright] Duplicate — skipping: {url}")
            return

        self.store.save_page(page_data)

        # FAQs
        faq_extractor = FAQExtractor()
        for q, a, cat in faq_extractor.extract(soup, url):
            self.store.save_faq(url, q, a, cat)

        # Tables
        table_extractor = TableExtractor()
        for idx, (headers, rows, caption) in enumerate(
            table_extractor.extract(soup, url)
        ):
            self.store.save_table(url, idx, headers, rows, caption)

        # Meta
        meta_extractor = MetaExtractor()
        for m_type, key, value in meta_extractor.extract(soup, url):
            self.store.save_meta(url, m_type, key, value)

        # Enqueue links found in the rendered page
        uid = url_hash(url)
        from utils.helpers import is_allowed_url, normalise_url

        for tag in soup.find_all("a", href=True):
            link = normalise_url(tag["href"], base=url)
            if link and is_allowed_url(link) and not self.store.is_url_seen(link):
                self.store.enqueue_url(url_hash(link), url_hash(link), depth=depth + 1)

        log.info(f"[Playwright] Saved: {url} | words={page_data.get('word_count', 0)}")
