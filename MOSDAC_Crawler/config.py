

"""
config.py
─────────────────────────────────────────────────────────────────────────────
Central configuration for the MOSDAC Crawler.
Tuned to the REAL MOSDAC portal structure (Drupal 7 CMS).
Analysed from live HTML of homepage + /sitemap — March 2026.
─────────────────────────────────────────────────────────────────────────────
"""

from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 1. TARGET PORTAL
# ─────────────────────────────────────────────────────────────
BASE_URL        = "https://www.mosdac.gov.in"
ALLOWED_DOMAINS = ["mosdac.gov.in"]

# Sitemap page — parsed first to auto-discover all public URLs
SITEMAP_URL = "https://www.mosdac.gov.in/sitemap"

# Explicit seed URLs (confirmed public, no-login, from real /sitemap HTML)
START_URLS = [
    # Core
    "https://www.mosdac.gov.in",
    "https://www.mosdac.gov.in/sitemap",
    "https://www.mosdac.gov.in/faq-page",       # Real URL — not /faq
    "https://www.mosdac.gov.in/help",
    "https://www.mosdac.gov.in/about-us",
    "https://www.mosdac.gov.in/contact-us",
    "https://www.mosdac.gov.in/atlases",
    "https://www.mosdac.gov.in/tools",
    "https://www.mosdac.gov.in/downloadapi-manual",
    "https://www.mosdac.gov.in/announcements",

    # Satellite Missions (all public, no login required)
    "https://www.mosdac.gov.in/insat-3dr",
    "https://www.mosdac.gov.in/insat-3d",
    "https://www.mosdac.gov.in/insat-3a",
    "https://www.mosdac.gov.in/insat-3ds",
    "https://www.mosdac.gov.in/kalpana-1",
    "https://www.mosdac.gov.in/megha-tropiques",
    "https://www.mosdac.gov.in/saral-altika",
    "https://www.mosdac.gov.in/oceansat-2",
    "https://www.mosdac.gov.in/oceansat-3",
    "https://www.mosdac.gov.in/scatsat-1",

    # Open Data — Atmosphere
    "https://www.mosdac.gov.in/bayesian-based-mt-saphir-rainfall",
    "https://www.mosdac.gov.in/gps-derived-integrated-water-vapour",
    "https://www.mosdac.gov.in/gsmap-isro-rain",
    "https://www.mosdac.gov.in/meteosat8-cloud-properties",

    # Open Data — Land
    "https://www.mosdac.gov.in/3d-volumetric-terls-dwrproduct",
    "https://www.mosdac.gov.in/inland-water-height",
    "https://www.mosdac.gov.in/river-discharge",
    "https://www.mosdac.gov.in/soil-moisture-0",

    # Open Data — Ocean
    "https://www.mosdac.gov.in/global-ocean-surface-current",
    "https://www.mosdac.gov.in/high-resolution-sea-surface-salinity",
    "https://www.mosdac.gov.in/indian-mainland-coastal-product",
    "https://www.mosdac.gov.in/ocean-subsurface",
    "https://www.mosdac.gov.in/oceanic-eddies-detection",
    "https://www.mosdac.gov.in/sea-ice-occurrence-probability",
    "https://www.mosdac.gov.in/wave-based-renewable-energy",

    # Reports
    "https://www.mosdac.gov.in/insitu",
    "https://www.mosdac.gov.in/calibration-reports",
    "https://www.mosdac.gov.in/validation-reports",
    "https://www.mosdac.gov.in/data-quality",
    "https://www.mosdac.gov.in/weather-reports",
    "https://www.mosdac.gov.in/rss-feed",

    # Policies
    "https://www.mosdac.gov.in/copyright-policy",
    "https://www.mosdac.gov.in/data-access-policy",
    "https://www.mosdac.gov.in/hyperlink-policy",
    "https://www.mosdac.gov.in/privacy-policy",
    "https://www.mosdac.gov.in/website-policies",
    "https://www.mosdac.gov.in/terms-conditions",
]

# ─────────────────────────────────────────────────────────────
# 2. URL EXCLUSION RULES  (MOSDAC-specific, from real HTML)
# ─────────────────────────────────────────────────────────────
# CRITICAL: ALL /internal/ URLs require login — must skip entirely
EXCLUDED_URL_PATTERNS = [
    "/internal/",            # Covers catalog, gallery, uops, registration, etc.

    # Drupal AJAX endpoints — not crawlable HTML pages
    "/quicktabs/ajax/",
    "/ajax/",
    "/apios/",               # OpenSearch descriptor XML
    "/matomo/",              # Analytics server
    "/visit/",               # Piwik analytics

    # MOSDAC-specific non-text content
    "gallery/index.html",    # JS-only image gallery widget
    "?language=hi",          # Hindi language variant (duplicate content)

    # Auth links
    "javascript:",
    "mailto:",
    "tel:",
    "#",                     # In-page anchors

    # Static assets (no text content)
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".css", ".js", ".woff", ".woff2", ".ttf",
    ".mp4", ".avi", ".mp3",
    ".xml",
    ".zip", ".tar", ".jar",
]

# ─────────────────────────────────────────────────────────────
# 3. DOCUMENT DOWNLOAD
# ─────────────────────────────────────────────────────────────
# MOSDAC stores docs at:  /sites/default/files/docs/
# The Announcements block on the homepage has direct PDF links with titles.
DOWNLOAD_DOCUMENTS      = True
SUPPORTED_DOC_TYPES     = [".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv"]
MOSDAC_DOC_PATH_PREFIX  = "/sites/default/files/docs/"
MAX_DOC_SIZE_MB         = 50
MAX_DOCS_TOTAL          = 500

# ─────────────────────────────────────────────────────────────
# 4. CRAWLER BEHAVIOUR
# ─────────────────────────────────────────────────────────────
MAX_DEPTH           = 4     # MOSDAC is shallow — 4 hops covers all public pages
MAX_PAGES           = 500   # Approx 200-300 public pages on the portal
CONCURRENT_REQUESTS = 4     # Polite to a government server
DOWNLOAD_DELAY      = 2.0   # 2 seconds between requests
REQUEST_TIMEOUT     = 30
MAX_RETRIES         = 3
RESPECT_ROBOTS_TXT  = True

# ─────────────────────────────────────────────────────────────
# 5. DYNAMIC PAGE SETTINGS (Playwright — rarely needed)
# ─────────────────────────────────────────────────────────────
USE_HEADLESS_BROWSER = True
BROWSER_WAIT_MS      = 4000   # Extra time for Drupal quicktabs
JS_SCROLL_PAUSE_MS   = 1000

# MOSDAC public pages do NOT require JS rendering.
# Quicktabs AJAX is skipped by URL exclusion rule above.
JS_REQUIRED_PATTERNS = []

# ─────────────────────────────────────────────────────────────
# 6. DRUPAL 7 SELECTORS (mapped from real MOSDAC HTML)
# ─────────────────────────────────────────────────────────────
# Priority-ordered list of (tag, attrs) to find the main content block
DRUPAL_CONTENT_SELECTORS = [
    ("div", {"id": "site-map"}),                      # /sitemap page only
    ("div", {"class": "region region-content"}),      # Standard Drupal pages
    ("div", {"id": "content"}),                       # Content column fallback
    ("div", {"class": "view-content"}),               # Drupal Views listing
    ("article", {"role": "article"}),                 # Node article pages
    ("div", {"class": "node"}),                       # Generic Drupal node
]

# Drupal UI block IDs to strip before text extraction
DRUPAL_BOILERPLATE_IDS = {
    "sticky",                           # Accessibility bar (text resize, contrast)
    "secondary-menu",                   # SignUp / Login / Logout links
    "block-superfish-1",                # Superfish main navigation
    "footer-wrapper",
    "footer",
    "footer-columns",
    "back-top",
    "block-search-form",                # Search widget in footer
    "block-lang-dropdown-language-content",
    "block-high-contrast-high-contrast-switcher",
    "block-text-resize-0",
    "block-block-12",                   # "Skip to main Content" link
    "block-block-11",                   # "Ver 3.0; Last reviewed..." text
    "block-block-10",                   # STQC certificate logo
    "block-social-media-links-social-media-links",
    "smart",                            # Empty placeholder div
}

DRUPAL_BOILERPLATE_CLASSES = {
    "sf-menu",               # Superfish navigation list
    "breadcrumb",
    "region-sticky",
    "lang_dropdown_form",
    "high_contrast_switcher",
    "social-media-links",
    "views-icons-footer",
}

# ─────────────────────────────────────────────────────────────
# 7. ANNOUNCEMENTS PDF HARVESTING
# ─────────────────────────────────────────────────────────────
# div.view-announcement-view on homepage has direct PDF links with:
#   - Document title (as <a> text)
#   - File size
#   - Publication date
# These are the most important documents to extract.
ANNOUNCEMENTS_BLOCK_CLASS = "view-announcement-view"

# ─────────────────────────────────────────────────────────────
# 8. EXTRACTION FLAGS
# ─────────────────────────────────────────────────────────────
EXTRACT_FAQS            = True
EXTRACT_TABLES          = True
EXTRACT_META_TAGS       = True
EXTRACT_ARIA_LABELS     = True
EXTRACT_STRUCTURED_DATA = True   # JSON-LD, OpenGraph, microdata
ENABLE_OCR              = False  # Enable for scanned PDFs (requires Tesseract)

# ─────────────────────────────────────────────────────────────
# 9. FILE PATHS
# ─────────────────────────────────────────────────────────────
BASE_DIR          = Path(__file__).parent
OUTPUT_DIR        = BASE_DIR / "output"
RAW_HTML_DIR      = OUTPUT_DIR / "raw_html"
DOCUMENTS_DIR     = OUTPUT_DIR / "documents"
DATA_DIR          = OUTPUT_DIR / "data"
DB_PATH           = DATA_DIR / "mosdac_crawler.db"
CRAWL_LOG_PATH    = DATA_DIR / "crawl.log"
VISITED_URLS_FILE = DATA_DIR / "visited_urls.txt"
TRAINIG_CRAWL_LOG_PATH = DATA_DIR / "training_crawl.log"

for _dir in [OUTPUT_DIR, RAW_HTML_DIR, DOCUMENTS_DIR, DATA_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# 10. REQUEST HEADERS
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
    "Referer": "https://www.mosdac.gov.in/",
}

# ─────────────────────────────────────────────────────────────
# 11. KNOWN SATELLITE MISSIONS  (verified from real nav HTML, March 2026)
# ─────────────────────────────────────────────────────────────
# slug → display name, as they appear in the Superfish navigation menu
KNOWN_MISSIONS: dict = {
    "insat-3dr":      "INSAT-3DR",
    "insat-3d":       "INSAT-3D",
    "insat-3a":       "INSAT-3A",
    "insat-3ds":      "INSAT-3DS",
    "kalpana-1":      "KALPANA-1",
    "megha-tropiques": "MeghaTropiques",
    "saral-altika":   "SARAL-AltiKa",
    "oceansat-2":     "OCEANSAT-2",
    "oceansat-3":     "OCEANSAT-3",
    "scatsat-1":      "SCATSAT-1",
}

# Sub-page section types that exist under each mission landing page.
# Discovered from INSAT-3D sidebar nav: block-menu-menu-{mission}-menu
# Pattern: /{mission-slug}-{section}
# Note: 'data-products' links to /internal/catalog-{name} — LOGIN WALL, skip.
MISSION_SUBPAGE_TYPES: list = [
    "introduction",   # Overview / about the mission
    "objectives",     # Mission goals
    "spacecraft",     # Bus / platform specs
    "payloads",       # Sensors / instruments
    "references",     # Documents / papers (labelled "Documents" in menu)
]

# ── URL slug overrides (verified from REAL crawl logs, March 2026) ──────────
# Some missions use a different URL slug for their sub-pages than the landing
# page slug.  e.g. landing page is /insat-3ds but sub-pages are /insat-3s-*
# These are discovered by actually running the crawler and observing log output.
#
# Format:  { landing_slug: sub_page_url_slug }
# Only entries that DIFFER from the landing slug are listed here.
MISSION_SUBPAGE_SLUG_OVERRIDES: dict = {
    # Landing /insat-3ds  →  sub-pages /insat-3s-introduction etc.
    "insat-3s":   "insat-3ds",
    # /saral-references (references page has shorter slug, not saral-altika-references)
    "saral":      "saral-altika",
    # /oceansat3-references (no hyphen in this one page only)
    "oceansat3":  "oceansat-3",
}

# ── Known catalog slugs (for /internal/catalog-* recognition) ───────────────
# Maps landing slug → catalog slug used in the /internal/ URL.
# Verified from real crawl logs.
MISSION_CATALOG_SLUGS: dict = {
    "insat-3dr":       "insat3dr",
    "insat-3d":        "insat3d",
    "insat-3a":        "insat3a",
    "insat-3ds":       "insat3s",          # note: "3s" not "3ds"
    "kalpana-1":       "kalpana1",
    "megha-tropiques": "meghatropiques",
    "saral-altika":    "saral",
    "oceansat-2":      "oceansat2",
    "oceansat-3":      "oceansat3",
    "scatsat-1":       "scatsat",
}

# Drupal content type for mission/satellite pages: node-type-satellite
# Detected from <body class="... node-type-satellite ...">
DRUPAL_MISSION_NODE_TYPE = "node-type-satellite"

# Sidebar block ID pattern for mission sub-navigation
# e.g. block-menu-menu-insat-3d-menu, block-menu-menu-oceansat-3-menu
MISSION_SIDEBAR_BLOCK_PREFIX = "block-menu-menu-"
MISSION_SIDEBAR_BLOCK_SUFFIX = "-menu"

# ─────────────────────────────────────────────────────────────
# 12. CONTENT CLASSIFICATION — MOSDAC domain vocabulary
# ─────────────────────────────────────────────────────────────
FAQ_KEYWORDS = [
    "faq", "frequently asked", "question", "help", "support",
    "how to", "how do i", "what is", "where can",
]
PRODUCT_KEYWORDS = [
    "product", "catalog", "catalogue", "data product",
    "sst", "ndvi", "olr", "rainfall", "wind vector",
    "netcdf", "hdf5", "geotiff", "level-1", "level-2", "level-3",
]
MISSION_KEYWORDS = [
    "insat", "insat-3d", "insat-3dr", "insat-3ds", "insat-3a",
    "oceansat", "kalpana", "megha-tropiques", "saral", "altika",
    "scatsat", "imager", "sounder", "saphir", "madras",
    "isro", "sac", "mosdac",
]
DOCUMENT_KEYWORDS = [
    "manual", "guide", "specification", "report", "document",
    "algorithm", "atbd", "product specification", "user guide",
    "validation report", "calibration",
]
GEOSPATIAL_KEYWORDS = [
    "bay of bengal", "arabian sea", "indian ocean",
    "latitude", "longitude", "spatial resolution", "coverage",
    "swath", "projection", "coordinate", "geospatial",
]