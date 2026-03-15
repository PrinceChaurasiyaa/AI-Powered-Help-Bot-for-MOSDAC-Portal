"""
config.py
─────────────────────────────────────────────────────────────────────────────
Central configuration for the MOSDAC Crawler.
All tunable parameters live here — change only this file to adapt the
crawler to a different portal.
─────────────────────────────────────────────────────────────────────────────
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 1. TARGET PORTAL
# ─────────────────────────────────────────────────────────────
BASE_URL            = "https://www.mosdac.gov.in"
ALLOWED_DOMAINS     = ["mosdac.gov.in"]          # Stay inside these domains
START_URLS = [
    "https://www.mosdac.gov.in",
    "https://www.mosdac.gov.in/faq",
    "https://www.mosdac.gov.in/catalog",
    "https://www.mosdac.gov.in/gallery",
    "https://www.mosdac.gov.in/about",
    "https://www.mosdac.gov.in/contact",
]

# URL patterns to skip (login walls, logout, CDN assets, etc.)
EXCLUDED_URL_PATTERNS = [
    "/login", "/logout", "/register", "/cart",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".css", ".js", ".woff", ".ttf", ".mp4", ".avi",
    "javascript:", "mailto:", "tel:",
    "#",                           # In-page anchors
]

# ─────────────────────────────────────────────────────────────
# 2. CRAWLER BEHAVIOUR
# ─────────────────────────────────────────────────────────────
MAX_DEPTH           = 5           # How many link-hops from start URLs
MAX_PAGES           = 2000        # Hard ceiling on pages crawled
CONCURRENT_REQUESTS = 8           # Parallel requests (be polite)
DOWNLOAD_DELAY      = 1.5         # Seconds between requests per domain
REQUEST_TIMEOUT     = 30          # Seconds before a request times out
MAX_RETRIES         = 3           # Retry failed requests N times
RESPECT_ROBOTS_TXT  = True        # Honour robots.txt

# ─────────────────────────────────────────────────────────────
# 3. DYNAMIC PAGE SETTINGS (Playwright)
# ─────────────────────────────────────────────────────────────
USE_HEADLESS_BROWSER = True       # Set False to watch the browser
BROWSER_WAIT_MS      = 3000       # ms to wait for JS to render
JS_SCROLL_PAUSE_MS   = 1000       # ms between scroll steps (lazy-load)

# Pages that NEED JavaScript rendering (detected by pattern)
JS_REQUIRED_PATTERNS = [
    "/catalog",
    "/gallery",
    "/viewer",
    "/map",
    "/dashboard",
]

# ─────────────────────────────────────────────────────────────
# 4. DOCUMENT DOWNLOAD SETTINGS
# ─────────────────────────────────────────────────────────────
DOWNLOAD_DOCUMENTS  = True
SUPPORTED_DOC_TYPES = [".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv"]
MAX_DOC_SIZE_MB     = 50          # Skip files larger than this
MAX_DOCS_TOTAL      = 500         # Total document download ceiling

# ─────────────────────────────────────────────────────────────
# 5. EXTRACTION FLAGS
# ─────────────────────────────────────────────────────────────
EXTRACT_FAQS        = True
EXTRACT_TABLES      = True
EXTRACT_META_TAGS   = True
EXTRACT_ARIA_LABELS = True
EXTRACT_STRUCTURED_DATA = True    # JSON-LD, microdata, OpenGraph
ENABLE_OCR          = False       # Enable for scanned PDFs (slow)

# ─────────────────────────────────────────────────────────────
# 6. FILE PATHS
# ─────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
OUTPUT_DIR      = BASE_DIR / "output"
RAW_HTML_DIR    = OUTPUT_DIR / "raw_html"
DOCUMENTS_DIR   = OUTPUT_DIR / "documents"
DATA_DIR        = OUTPUT_DIR / "data"
DB_PATH         = DATA_DIR / "mosdac_crawler.db"
CRAWL_LOG_PATH  = DATA_DIR / "crawl.log"
TRAINIG_CRAWL_LOG_PATH = DATA_DIR / "training_crawl.log"
VISITED_URLS_FILE = DATA_DIR / "visited_urls.txt"

# Ensure all directories exist
for _dir in [OUTPUT_DIR, RAW_HTML_DIR, DOCUMENTS_DIR, DATA_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# 7. REQUEST HEADERS
# ─────────────────────────────────────────────────────────────
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────────────────────
# 8. CONTENT CLASSIFICATION KEYWORDS
# ─────────────────────────────────────────────────────────────
FAQ_KEYWORDS        = ["faq", "frequently asked", "question", "help", "support"]
PRODUCT_KEYWORDS    = ["product", "catalog", "catalogue", "satellite", "data"]
MISSION_KEYWORDS    = ["mission", "insat", "oceansat", "megha", "kalpana", "resourcesat"]
DOCUMENT_KEYWORDS   = ["manual", "guide", "specification", "report", "document"]
