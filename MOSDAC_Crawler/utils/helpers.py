"""
Shared utility functions:
   URL normalisation & validation
   Content-hash based deduplication
   Safe filename generation
   Robots.txt parsing
   Retry decorator
"""

import hashlib
import re
import time
import urllib.robotparser
from functools import wraps
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse

from config import (
    ALLOWED_DOMAINS,
    BASE_URL,
    EXCLUDED_URL_PATTERNS,
    MAX_RETRIES,
    RESPECT_ROBOTS_TXT,
    SUPPORTED_DOC_TYPES,
    JS_REQUIRED_PATTERNS
)
from utils.logger import get_logger

log = get_logger(__name__)

# ================================= URL Utils ===========================

def normalise_url(url: str, base: str = BASE_URL) -> Optional[str]:
    """
     Resolve relative URLs against base
     Strip fragments (#section)
     Remove trailing slashes
     Lowercase scheme + host
    Returns None if URL should be skipped.
    """

    try:
        full = urljoin(base, url.strip())

        parsed = urlparse(full)

        if parsed.scheme not in ("http", "https"):
            return None
        
        clean = parsed._replace(fragment="")

        normalised = urlunparse(clean).rstrip("/")

        return normalised
    
    except Exception:
        return None
    
def is_allowed_url(url: str) -> bool:
    """
    Returns True only if the URL:
      1. Belongs to an allowed domain
      2. Does not match any excluded pattern
    """

    if not url:
        return False
    
    parsed = urlparse(url)

    # Domain Check
    domain_ok = any(parsed.netloc.endswith(d) for d in ALLOWED_DOMAINS)

    if not domain_ok:
        return False
    
    # Exclusion pattern check
    lower = url.lower()
    for pattern in EXCLUDED_URL_PATTERNS:
        if pattern in lower:
            return False
    
    return True

def is_document_url(url: str) -> bool:
    """True if the URL points to a downloadable document."""

    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SUPPORTED_DOC_TYPES)

def need_javasripts(url: str) -> bool:

    lower = url.lower()
    return any(pat in lower for pat in JS_REQUIRED_PATTERNS)


# ─────────────────────────────────────────────────────────────
# ROBOTS.TXT
# ─────────────────────────────────────────────────────────────

_robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}


def can_fetch(url: str, user_agent: str = "*") -> bool:
    """
    Check robots.txt for the domain of `url`.
    Returns True if crawling is allowed (or RESPECT_ROBOTS_TXT is off).
    """
    if not RESPECT_ROBOTS_TXT:
        return True

    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    if robots_url not in _robots_cache:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
            _robots_cache[robots_url] = rp
            log.debug(f"Loaded robots.txt from {robots_url}")
        except Exception as exc:
            log.warning(f"Could not fetch robots.txt from {robots_url}: {exc}")
            # Assume allowed if we can't read it
            return True

    return _robots_cache[robots_url].can_fetch(user_agent, url)


# =================================DEDUPLICATION ==========================================================

def content_hash(text: str) -> str:
    """SHA-256 hash of text content — used for deduplication."""
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def url_hash(url: str) -> str:
    """MD5 hash of normalised URL — short unique key for DB storage."""
    return hashlib.md5(url.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────
# FILE UTILITIES
# ─────────────────────────────────────────────────────────────

def safe_filename(url: str, extension: str = ".html") -> str:
    """
    Convert a URL to a safe filesystem filename.
    e.g. https://www.mosdac.gov.in/faq → mosdac_gov_in_faq.html
    """
    parsed = urlparse(url)
    name = (parsed.netloc + parsed.path).replace("/", "_").replace(".", "_")
    name = re.sub(r"[^\w\-]", "", name)[:200]  # Safe chars only, max 200 chars
    return name + extension


def file_size_mb(path: Path) -> float:
    """Return file size in megabytes."""
    return path.stat().st_size / (1024 * 1024)


# ─────────────────────────────────────────────────────────────
# RETRY DECORATOR
# ─────────────────────────────────────────────────────────────

def retry(max_attempts: int = MAX_RETRIES, delay: float = 2.0, backoff: float = 2.0):
    """
    Decorator that retries a function on exception.

    Usage:
        @retry(max_attempts=3, delay=2.0)
        def fetch(url): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            wait = delay
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    attempts += 1
                    if attempts >= max_attempts:
                        log.error(
                            f"{func.__name__} failed after {max_attempts} "
                            f"attempts: {exc}"
                        )
                        raise
                    log.warning(
                        f"{func.__name__} attempt {attempts} failed: {exc}. "
                        f"Retrying in {wait:.1f}s …"
                    )
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────
# TEXT CLEANING
# ─────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """
    Light normalisation applied to ALL extracted text:
      • Collapse whitespace
      • Strip BOM characters
      • Remove control characters
      • Normalise newlines
    """
    if not text:
        return ""
    # Remove BOM
    text = text.lstrip("\ufeff")
    # Remove null bytes and control chars except newlines/tabs
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    # Collapse multiple spaces
    text = re.sub(r" +", " ", text)
    # Collapse more than 2 consecutive newlines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def detect_language(text: str) -> str:
    """
    Very lightweight language hint based on character set.
    Returns 'en' for English, 'other' otherwise.
    Extend with langdetect library if multilingual support needed.
    """
    ascii_ratio = sum(1 for c in text if ord(c) < 128) / max(len(text), 1)
    return "en" if ascii_ratio > 0.85 else "other"
