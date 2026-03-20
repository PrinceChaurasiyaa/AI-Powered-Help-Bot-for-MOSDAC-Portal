"""
extractors/table_extractor.py
─────────────────────────────────────────────────────────────────────────────
Extracts HTML tables from BeautifulSoup-parsed pages.

For each <table>:
  • Detects header row (from <thead> or first <tr> with <th> cells)
  • Extracts data rows as lists of strings
  • Captures table caption from <caption> tag
  • Skips layout/navigation tables (1-column tables, tiny tables)
─────────────────────────────────────────────────────────────────────────────
"""

from typing import List, Tuple

from bs4 import BeautifulSoup, Tag

from utils.helpers import clean_text
from utils.logger import get_logger

log = get_logger(__name__)

# Minimum rows/cols to be considered a data table (not a layout table)
MIN_ROWS = 2
MIN_COLS = 2


class TableExtractor:
    """
    Extracts data tables from HTML.

    Usage:
        extractor = TableExtractor()
        tables = extractor.extract(soup, url)
        for headers, rows, caption in tables:
            store.save_table(url, idx, headers, rows, caption)
    """

    def extract(
        self, soup: BeautifulSoup, url: str
    ) -> List[Tuple[List[str], List[List[str]], str]]:
        """
        Returns a list of (headers, rows, caption) tuples.
          headers: list of column header strings
          rows:    list of rows, each row is a list of cell strings
          caption: table caption text (may be empty)
        """
        results = []

        for table in soup.find_all("table"):
            try:
                result = self._parse_table(table)
                if result is None:
                    continue
                headers, rows, caption = result
                # Filter out layout / tiny tables
                if len(rows) < MIN_ROWS or len(headers) < MIN_COLS:
                    continue
                results.append((headers, rows, caption))
            except Exception as exc:
                log.debug(f"Table parse error: {exc}")
                continue

        log.debug(f"TableExtractor: {len(results)} data tables found at {url}")
        return results

    # ── Internal ─────────────────────────────────────────────

    def _parse_table(
        self, table: Tag
    ) -> Tuple[List[str], List[List[str]], str] | None:
        """
        Parse a single <table> element.
        Returns (headers, rows, caption) or None if table is unusable.
        """
        # ── Caption ───────────────────────────────────────────
        caption_tag = table.find("caption")
        caption     = clean_text(caption_tag.get_text()) if caption_tag else ""

        # ── Header row ────────────────────────────────────────
        headers: List[str] = []

        # Priority 1: <thead>
        thead = table.find("thead")
        if thead:
            header_row = thead.find("tr")
            if header_row:
                headers = self._extract_cells(header_row, cell_tags=["th", "td"])

        # Priority 2: First row with <th> cells
        if not headers:
            first_row = table.find("tr")
            if first_row:
                th_cells = first_row.find_all("th")
                if th_cells:
                    headers = [clean_text(c.get_text(separator=" ")) for c in th_cells]

        # ── Data rows ─────────────────────────────────────────
        tbody   = table.find("tbody") or table
        all_trs = tbody.find_all("tr")
        rows: List[List[str]] = []

        for tr in all_trs:
            # Skip the header row (already extracted)
            cells = self._extract_cells(tr)
            if not cells:
                continue
            # Skip if this row IS the header row
            if cells == headers:
                continue
            # Skip completely empty rows
            if not any(c.strip() for c in cells):
                continue
            rows.append(cells)

        # Infer headers from first data row if still missing
        if not headers and rows:
            headers = [f"Column {i+1}" for i in range(len(rows[0]))]

        if not rows:
            return None

        # Normalise row widths to match header count
        col_count = len(headers) if headers else max(len(r) for r in rows)
        rows = [self._pad_row(row, col_count) for row in rows]

        return headers, rows, caption

    def _extract_cells(
        self, tr: Tag, cell_tags: List[str] = ["td", "th"]
    ) -> List[str]:
        """Extract text from all cells in a <tr>."""
        cells = []
        for cell in tr.find_all(cell_tags):
            # Handle colspan — repeat cell text for each spanned column
            colspan = int(cell.get("colspan", 1))
            text    = clean_text(cell.get_text(separator=" "))
            cells.extend([text] * colspan)
        return cells

    def _pad_row(self, row: List[str], target_len: int) -> List[str]:
        """Pad a row with empty strings or truncate to target_len."""
        if len(row) < target_len:
            return row + [""] * (target_len - len(row))
        return row[:target_len]
