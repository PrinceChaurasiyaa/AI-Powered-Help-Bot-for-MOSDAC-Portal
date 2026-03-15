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

from ..config import (
    ALLOWED_DOMAINS,
    BASE_URL,
    EXCLUDED_URL_PATTERNS,
    MAX_RETRIES,
    RESPECT_ROBOTS_TXT,
    SUPPORTED_DOC_TYPES,
    JS_REQUIRED_PATTERNS
)
from ..utils.logger import get_logger

log = get_logger(__name__)

# ================================= URL Utils ===========================

def normalize_url(url: str, base: str = BASE_URL) -> Optional[str]:
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
