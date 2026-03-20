"""
utils/exporter.py
─────────────────────────────────────────────────────────────────────────────
Exports crawled data from SQLite to JSON and CSV formats,
ready for Phase 2 (Knowledge Graph Construction).

Exports
───────
  pages.json              — all crawled page text content
  faqs.json               — all extracted Q&A pairs
  tables.json             — all extracted tables
  meta.json               — all meta/ARIA data
  documents.json          — all parsed document content
  mission_hierarchy.json  — satellite missions + sub-page structure
  pages.csv               — flat CSV of pages (for quick inspection)
  faqs.csv                — flat CSV of FAQs
  mission_hierarchy.csv   — flat CSV of mission tree
─────────────────────────────────────────────────────────────────────────────
"""

import csv
import json
import sqlite3
from pathlib import Path

from config import DATA_DIR, DB_PATH
from utils.logger import get_logger

log = get_logger(__name__)


class DataExporter:
    """
    Reads from the SQLite database and exports to JSON/CSV.

    Usage:
        exporter = DataExporter()
        exporter.export_all()
    """

    def __init__(self, db_path: Path = DB_PATH, output_dir: Path = DATA_DIR):
        self.db_path    = db_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(self) -> None:
        """Run all exports."""
        log.info("Starting data export …")
        self._export_pages()
        self._export_faqs()
        self._export_tables()
        self._export_meta()
        self._export_documents()
        self._export_mission_hierarchy()
        log.info(f"All exports saved to: {self.output_dir}")

    # ── Internal ─────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _export_pages(self) -> None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT url, title, content_text, page_type,
                          depth, word_count, crawled_at, language
                   FROM pages ORDER BY id"""
            ).fetchall()

        records = [dict(r) for r in rows]

        # JSON
        self._write_json("pages.json", records)

        # CSV
        fields = ["url", "title", "page_type", "depth", "word_count",
                  "crawled_at", "language"]
        self._write_csv("pages.csv", records, fields)

        log.info(f"Pages exported: {len(records)}")

    def _export_faqs(self) -> None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT source_url, question, answer, category, extracted_at
                   FROM faqs ORDER BY id"""
            ).fetchall()

        records = [dict(r) for r in rows]
        self._write_json("faqs.json", records)
        self._write_csv("faqs.csv", records,
                        ["source_url", "question", "answer", "category"])
        log.info(f"FAQs exported: {len(records)}")

    def _export_tables(self) -> None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT source_url, table_index, headers, rows,
                          caption, row_count, col_count
                   FROM extracted_tables ORDER BY id"""
            ).fetchall()

        records = []
        for r in rows:
            rec = dict(r)
            rec["headers"] = json.loads(rec["headers"] or "[]")
            rec["rows"]    = json.loads(rec["rows"]    or "[]")
            records.append(rec)

        self._write_json("tables.json", records)
        log.info(f"Tables exported: {len(records)}")

    def _export_meta(self) -> None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT source_url, meta_type, key, value, extracted_at
                   FROM meta_data ORDER BY id"""
            ).fetchall()

        records = [dict(r) for r in rows]
        self._write_json("meta.json", records)
        log.info(f"Meta items exported: {len(records)}")

    def _export_documents(self) -> None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT url, filename, file_type, extracted_text,
                          page_count, file_size_kb, source_page_url, downloaded_at
                   FROM documents ORDER BY id"""
            ).fetchall()

        records = [dict(r) for r in rows]
        self._write_json("documents.json", records)
        self._write_csv("documents.csv", records,
                        ["url", "filename", "file_type", "page_count",
                         "file_size_kb", "downloaded_at"])
        log.info(f"Documents exported: {len(records)}")

    def _export_mission_hierarchy(self) -> None:
        """
        Export mission_hierarchy table to JSON and CSV.

        JSON groups by mission for easy Phase 2 ingestion:
        {
          "insat-3d": {
            "name": "INSAT-3D",
            "landing_url": "https://…/insat-3d",
            "sections": [
              {"section_type": "introduction",
               "section_title": "Introduction",
               "section_url": "https://…/insat-3d-introduction",
               "crawl_status": "done"},
              …
            ]
          },
          …
        }
        """
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    """SELECT mission_slug, mission_name, mission_url,
                              section_type, section_title, section_url,
                              crawl_status, discovered_at
                       FROM mission_hierarchy
                       ORDER BY mission_slug,
                                CASE section_type
                                    WHEN 'landing'      THEN 0
                                    WHEN 'introduction' THEN 1
                                    WHEN 'objectives'   THEN 2
                                    WHEN 'spacecraft'   THEN 3
                                    WHEN 'payloads'     THEN 4
                                    WHEN 'references'   THEN 5
                                    ELSE 9
                                END"""
                ).fetchall()
        except Exception as exc:
            log.warning(f"mission_hierarchy table not found: {exc}")
            return

        flat_records = [dict(r) for r in rows]

        # ── Build grouped JSON ────────────────────────────────
        grouped: dict = {}
        for rec in flat_records:
            slug = rec["mission_slug"]
            if slug not in grouped:
                grouped[slug] = {
                    "name":        rec["mission_name"],
                    "landing_url": rec["mission_url"],
                    "sections":    [],
                }
            if rec["section_type"] != "landing":
                grouped[slug]["sections"].append({
                    "section_type":  rec["section_type"],
                    "section_title": rec["section_title"],
                    "section_url":   rec["section_url"],
                    "crawl_status":  rec["crawl_status"],
                    "discovered_at": rec["discovered_at"],
                })

        self._write_json("mission_hierarchy.json", grouped)
        self._write_csv(
            "mission_hierarchy.csv",
            flat_records,
            ["mission_slug", "mission_name", "section_type",
             "section_title", "section_url", "crawl_status", "discovered_at"],
        )
        log.info(
            f"Mission hierarchy exported: "
            f"{len(grouped)} missions, "
            f"{len(flat_records)} total rows"
        )

    # ── File writers ─────────────────────────────────────────

    def _write_json(self, filename: str, data) -> None:
        path = self.output_dir / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.debug(f"Written: {path}")

    def _write_csv(self, filename: str, data: list, fields: list) -> None:
        path = self.output_dir / filename
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
        log.debug(f"Written: {path}")


# ── CLI shortcut ─────────────────────────────────────────────

if __name__ == "__main__":
    exporter = DataExporter()
    exporter.export_all()
    print("Export complete.")

