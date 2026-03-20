"""
SQLite-backed storage for all crawled content.

Tables
──────
  pages        — one row per crawled HTML page
  documents    — one row per downloaded document (PDF/DOCX/XLSX)
  faqs         — extracted Q&A pairs
  tables       — extracted HTML / document tables (stored as JSON)
  meta_data    — meta tags and ARIA labels per page
  crawl_queue  — URL frontier (pending / visited / failed)

All writes go through DataStore.  Other modules only import this class.
"""

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import DB_PATH
from utils.logger import get_logger

log = get_logger(__name__)

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;          -- Better concurrent access
PRAGMA foreign_keys = ON;

-- ── Pages ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    NOT NULL UNIQUE,
    url_hash        TEXT    NOT NULL,
    title           TEXT,
    content_text    TEXT,            -- Clean plain text
    content_html    TEXT,            -- Raw HTML (optional, large)
    content_hash    TEXT,            -- SHA-256 for dedup
    page_type       TEXT,            -- 'faq'|'product'|'mission'|'general'
    depth           INTEGER DEFAULT 0,
    links_found     INTEGER DEFAULT 0,
    crawled_at      TEXT,
    status_code     INTEGER,
    language        TEXT DEFAULT 'en',
    word_count      INTEGER DEFAULT 0
);

-- ── Documents ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    NOT NULL UNIQUE,
    url_hash        TEXT    NOT NULL,
    filename        TEXT,
    file_type       TEXT,            -- 'pdf'|'docx'|'xlsx'|'csv'
    local_path      TEXT,            -- Where we saved it on disk
    extracted_text  TEXT,
    content_hash    TEXT,
    page_count      INTEGER,
    file_size_kb    REAL,
    source_page_url TEXT,            -- Which page linked to this doc
    downloaded_at   TEXT,
    extraction_ok   INTEGER DEFAULT 1
);

-- ── FAQs ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS faqs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT,
    question        TEXT    NOT NULL,
    answer          TEXT    NOT NULL,
    category        TEXT,
    confidence      REAL DEFAULT 1.0,
    extracted_at    TEXT
);

-- ── Tables ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extracted_tables (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT,
    table_index     INTEGER,         -- nth table on the page
    headers         TEXT,            -- JSON array
    rows            TEXT,            -- JSON array of arrays
    caption         TEXT,
    row_count       INTEGER,
    col_count       INTEGER,
    extracted_at    TEXT
);

-- ── Meta Data ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT,
    meta_type       TEXT,            -- 'meta'|'og'|'schema'|'aria'
    key             TEXT,
    value           TEXT,
    extracted_at    TEXT
);

-- ── Mission Hierarchy ─────────────────────────────────────
-- Stores the parent-child structure of mission pages.
-- Each satellite mission (INSAT-3D, OCEANSAT-3 …) has a landing
-- page plus sub-pages (Introduction, Objectives, SpaceCraft,
-- Payloads, References/Documents).  These sub-pages are only
-- discoverable from the sidebar navigation (block-menu-menu-*-menu),
-- NOT from /sitemap — hence they need a dedicated table.
CREATE TABLE IF NOT EXISTS mission_hierarchy (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mission_slug    TEXT    NOT NULL,   -- e.g. 'insat-3d'
    mission_name    TEXT    NOT NULL,   -- e.g. 'INSAT-3D'
    mission_url     TEXT    NOT NULL,   -- e.g. 'https://…/insat-3d'
    section_type    TEXT,              -- 'introduction'|'objectives'|
                                       --   'spacecraft'|'payloads'|
                                       --   'references'|'landing'
    section_title   TEXT,              -- Menu label text
    section_url     TEXT    NOT NULL UNIQUE,
    crawl_status    TEXT    DEFAULT 'pending',  -- pending|done|skipped
    discovered_at   TEXT
);

-- Index for fast mission lookups
CREATE INDEX IF NOT EXISTS idx_mission_hierarchy_slug
    ON mission_hierarchy (mission_slug);

-- ── Crawl Queue ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crawl_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    NOT NULL UNIQUE,
    url_hash        TEXT    NOT NULL,
    status          TEXT    DEFAULT 'pending',   -- pending|visited|failed|skipped
    depth           INTEGER DEFAULT 0,
    parent_url      TEXT,
    added_at        TEXT,
    processed_at    TEXT,
    error_msg       TEXT
);

-- ── Indexes ────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_pages_url_hash      ON pages(url_hash);
CREATE INDEX IF NOT EXISTS idx_pages_content_hash  ON pages(content_hash);
CREATE INDEX IF NOT EXISTS idx_pages_page_type     ON pages(page_type);
CREATE INDEX IF NOT EXISTS idx_queue_status        ON crawl_queue(status);
CREATE INDEX IF NOT EXISTS idx_docs_file_type      ON documents(file_type);
"""


class DataStore:
    def __init__(self, db_path: Path=DB_PATH):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
        log.info(f"DataStore initialized -> {db_path}")
    
    # =============Connection Management========================
    @property
    def conn(self) -> sqlite3.Connection:
        """Return a per-thread connection (creates if needed)."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30,
            )
            self._local.conn.row_factory = sqlite3.Row     # Rows behave like dictionaries.
        return self._local.conn

    def _init_db(self):
        """Create all tables if they don't exist."""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
        log.debug("DataBase schema initialized")
    
    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    # ============= CRAWL QUEUE ================================

    def enqueue_url(self, url: str, url_hash: str, depth: int = 0,
                    parent_url: str = "") -> bool:
        """
        Add a URL to the crawl queue.
        Returns True if inserted, False if already exists.
        """

        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO crawl_queue
                   (url, url_hash, status, depth, parent_url, added_at)
                   VALUES (?, ?, 'pending', ?, ?, ?)""",
                (url, url_hash, depth, parent_url, _now()),
            )
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.Error as exc:
            log.error(f"enqueue_url failed for {url}: {exc}")
            return False
    
    def get_next_pending(self, batch_size: int = 10) -> List[Dict]:
        """
        Fetch next N pending URLs ordered by depth (BFS).
        """
        rows = self.conn.execute(
            """SELECT url, url_hash, depth, parent_url
               FROM crawl_queue
               WHERE status = 'pending'
               ORDER BY depth ASC, id ASC
               LIMIT ?""",
            (batch_size,),
        ).fetchall()
        return [dict(r) for r in rows]
    
    def mark_url_visited(self, url: str):
        self.conn.execute(
            """UPDATE crawl_queue
               SET status = 'visited', processed_at = ?
               WHERE url = ?""",
            (_now(), url),
        )
        self.conn.commit()
    
    def mark_url_failed(self, url: str, error_msg: str=""):
        self.conn.execute(
            """UPDATE crawl_queue
               SET status = 'failed', processed_at = ?, error_msg = ?
               WHERE url = ?""",
            (_now(), error_msg[:500], url)
        )
        self.conn.commit()
    
    def mark_url_skipped(self, url: str, reason: str = ""):
        self.conn.execute(
            """UPDATE crawl_queue
                SET status = "skipped", processed_at = ?, error_msg = ?
            """,
            (_now(), reason[:200], url),
        )
        self.conn.commit()

    def is_url_seen(self, url: str) -> bool:
        """True if URL is already in queue (any status)."""
        row = self.conn.execute(
            "SELECT 1 FROM crawl_queue WHERE url = ?",
            (url,)
        ).fetchone()
        return row is not None

    def queue_stats(self) -> Dict[str, int]:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM crawl_queue GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}


    # =============PAGES =======================================

    def save_page(self, data: Dict[str, Any]):
        """Insert or replace a crawled page record.
        Required keys: url, url_hash, content_hash
        """
        self.conn.execute(
            """INSERT OR REPLACE INTO pages
               (url, url_hash, title, content_text, content_html,
                content_hash, page_type, depth, links_found,
                crawled_at, status_code, language, word_count)
               VALUES
               (:url, :url_hash, :title, :content_text, :content_html,
                :content_hash, :page_type, :depth, :links_found,
                :crawled_at, :status_code, :language, :word_count)""",
            {
                "url":          data.get("url", ""),
                "url_hash":     data.get("url_hash", ""),
                "title":        data.get("title", ""),
                "content_text": data.get("content_text", ""),
                "content_html": data.get("content_html", ""),
                "content_hash": data.get("content_hash", ""),
                "page_type":    data.get("page_type", "general"),
                "depth":        data.get("depth", 0),
                "links_found":  data.get("links_found", 0),
                "crawled_at":   data.get("crawled_at", _now()),
                "status_code":  data.get("status_code", 200),
                "language":     data.get("language", "en"),
                "word_count":   data.get("word_count", 0),
            },
        )
        self.conn.commit()

    def is_content_duplicate(self, content_hash: str) -> bool:
        """True if we've already stored a page with this content hash."""
        row = self.conn.execute(
            "SELECT 1 FROM pages WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        return row is not None

    # ============ MISSION HIERARCHY ===========================

    def save_mission_subpage(
            self,
            mission_slug: str,
            mission_name: str,
            mission_url: str,
            section_type: str,
            section_title: str,
            section_url: str,
    ) -> bool:
        """
        Insert a mission sub-page record.

        Ignores duplicate section_url (UNIQUE constraint).
        Returns True if a new row was inserted.

        section_type is one of:
          'landing'       — the mission root page itself
          'introduction'  — /insat-3d-introduction
          'objectives'    — /insat-3d-objectives
          'spacecraft'    — /insat-3d-spacecraft
          'payloads'      — /insat-3d-payloads
          'references'    — /insat-3d-references  (Documents)
        """

        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO mission_hierarchy
                   (mission_slug, mission_name, mission_url,
                    section_type, section_title, section_url,
                    crawl_status, discovered_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
                   (
                       mission_slug, mission_name, mission_url,
                       section_type, section_title, section_url,
                       _now(),
                   ),
            )
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.Error as exc:
            log.error(f"save_mission_subpage failed for {section_url}: {exc}")
            return False
    
    def mark_mission_section_done(self, section_url: str):
        """Mark a mission sub-page as crawled."""
        try:
            self.conn.execute(
                "UPDATE mission_hierarchy SET crawl_status='done' WHERE section_url=?",
                (section_url,),
            )
            self.conn.commit()
        
        except sqlite3.Error as exc:
            log.error(f"mark_mission_section_done failed: {exc}")

    def get_mission_hierarchy(self, mission_slug: str = "") -> List[Dict]:
        """
        Return all rows from mission_hierarchy.
        If mission_slug is given, filter to that mission only.
        """

        if mission_slug:
            rows = self.conn.execute(
                """SELECT * FROM mission_hierarchy
                   WHERE mission_slug = ?
                   ORDER BY mission_slug, section_type""",
                   (mission_slug,),
            ).fetchall()
        
        else:
            rows = self.conn.execute(
                """SELECT * FROM mission_hierarchy
                   ORDER BY mission_slug, section_type"""
            ).fetchall()
        return [dict(r) for r in rows]
    
    def get_mission_stats(self) -> Dict[str, Any]:
        """
        Summary stats for mission_hierarchy table.
        Returns {mission_slug: {total, done, pending}} dict.
        """
        rows = self.conn.execute(
            """SELECT mission_slug, mission_name,
                      COUNT(*) as total,
                      SUM(CASE WHEN crawl_status='done' THEN 1 ELSE 0 END) as done
               FROM mission_hierarchy
               GROUP BY mission_slug
               ORDER BY mission_slug"""
        ).fetchall()
        return {
            r['mission_slug']: {
                "name":    r["mission_name"],
                "total":   r["total"],
                "done":    r["done"],
                "pending": r["total"] - r["done"],
            }
            for r in rows
        }
            

    # ============ DOCUMENTS ===================================

    def save_document(self, data: Dict[str, Any]):
        self.conn.execute(
            """INSERT OR REPLACE INTO documents
               (url, url_hash, filename, file_type, local_path,
                extracted_text, content_hash, page_count,
                file_size_kb, source_page_url, downloaded_at, extraction_ok)
               VALUES
               (:url, :url_hash, :filename, :file_type, :local_path,
                :extracted_text, :content_hash, :page_count,
                :file_size_kb, :source_page_url, :downloaded_at, :extraction_ok)""",
            {
                "url":             data.get("url", ""),
                "url_hash":        data.get("url_hash", ""),
                "filename":        data.get("filename", ""),
                "file_type":       data.get("file_type", ""),
                "local_path":      data.get("local_path", ""),
                "extracted_text":  data.get("extracted_text", ""),
                "content_hash":    data.get("content_hash", ""),
                "page_count":      data.get("page_count", 0),
                "file_size_kb":    data.get("file_size_kb", 0.0),
                "source_page_url": data.get("source_page_url", ""),
                "downloaded_at":   data.get("downloaded_at", _now()),
                "extraction_ok":   int(data.get("extraction_ok", True)),
            },
        )
        self.conn.commit()

    # ============ FAQs ========================================

    def save_faq(self, source_url:str, question: str, answer: str,
                 category: str = "", confidence: float = 1.0):
        self.conn.execute(
            """INSERT INTO faqs
               (source_url, question, answer, category, confidence, extracted_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_url, question, answer, category, confidence, _now())
        )
        self.conn.commit()

    # ============ TABLES ======================================
    def save_table(self, source_url: str, table_index: int,
                   headers: List[str], rows: List[List[str]],
                   caption: str = ""):
        self.conn.execute(
            """INSERT INTO extracted_tables
               (source_url, table_index, headers, rows,
                caption, row_count, col_count, extracted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                source_url,
                table_index,
                json.dumps(headers),
                json.dumps(rows),
                caption,
                len(rows),
                len(headers),
                _now(),
            ),
        )
        self.conn.commit()

    # ============ META DATA ===================================
    def save_meta(self, source_url: str, meta_type: str,
                  key: str, value: str):
        self.conn.execute(
            """INSERT INTO meta_data
               (source_url, meta_type, key, value, extracted_at)
               VALUES (?, ?, ?, ?, ?)""",
            (source_url, meta_type, key, value, _now()),
        )
        self.conn.commit()

    # ============ REPORTING ===================================
    def summary(self) -> Dict[str, Any]:
        """Quick summary of what's been collected so far."""
        pages     = self.conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        docs      = self.conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        faqs      = self.conn.execute("SELECT COUNT(*) FROM faqs").fetchone()[0]
        tables    = self.conn.execute("SELECT COUNT(*) FROM extracted_tables").fetchone()[0]
        queue     = self.queue_stats()
        return {
            "pages_crawled":  pages,
            "documents":      docs,
            "faqs_extracted": faqs,
            "tables":         tables,
            "queue":          queue,
        }

def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"