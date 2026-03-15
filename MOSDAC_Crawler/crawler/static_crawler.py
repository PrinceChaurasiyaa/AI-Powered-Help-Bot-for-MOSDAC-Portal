from MOSDAC_Crawler.extractors.faq_extractor import FAQExtractor
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup, Tag
from ...MOSDAC_Crawler.utils.logger import get_logger
from config import (
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT
)

log = get_logger(__name__)

class StaticCrawler:

    def __init__ (self):
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)

        # Retry adapter
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,                             # Controls delay between retries. sleep = backoff_factor * (2 ** retry_number)
            status_forcelist=[429, 500, 502, 503, 504],     # Retry only when these HTTP errors occur
            allowed_methods=['GET'],                        # Retry Only GET requests
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def _crawl_page(self, url:str, depth: int) -> None:
        user_agent = ""
        URL = "https://www.mosdac.gov.in/faq-page"

        html, status_code = self._fetch(url)

        # if html is None:
        #     self.store.mark_url_failed(url)

        soup = BeautifulSoup(html, "lxml")

        from ...MOSDAC_Crawler.extractors.faq_extractor import FAQExtractor

        # FAQs
        faq_extractor = FAQExtractor()
        faq = faq_extractor.extract(soup, URL)

        if faq:
            log.info(f" {len(faq)} FAQs extracted")

    def _fetch(self, url: str) -> Tuple[Optional[str], int]:
        """
        Return (html_string, status_code) or (None, status_code) on failure.
        """
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if response.status_code == 200:
                response.encoding = response.apparent_encoding or "utf-8"
                return response.text, response.status_code
            
            else:
                log.warning(f"HTTP {response.status_code} for {url}")
                return None, response.status_code
        except requests.exceptions.RequestException as exc:
            log.error(f"Request failed for {url}: {exc}")
            return None, 0