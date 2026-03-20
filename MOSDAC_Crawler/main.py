"""
main.py
─────────────────────────────────────────────────────────────────────────────
MOSDAC AI Help Bot — Phase 1: Data Ingestion
Orchestrator for the full crawl pipeline.

Crawl strategy for MOSDAC (Drupal 7, real HTML verified March 2026)
────────────────────────────────────────────────────────────────────
  Step 1  MOSDACsitemapSeeder   parses /sitemap HTML → seeds all public URLs
                                also harvests PDF links from Announcements block
  Step 2  StaticCrawler         processes queue → extracts content from each page
            ├── ContentExtractor  Drupal-aware text extraction
            ├── FAQExtractor      detects Q&A pairs
            ├── TableExtractor    data tables
            ├── MetaExtractor     MOSDAC meta tags (title/description/abstract/keywords)
            └── DocumentParser    downloads PDFs from /sites/default/files/docs/
  Step 3  DataExporter          writes JSON/CSV outputs for Phase 2

Run:
    python main.py                    # Full crawl (recommended)
    python main.py --mode seed        # Seed queue only (inspect before crawling)
    python main.py --mode crawl       # Crawl only (queue must already be seeded)
    python main.py --mode report      # Print DB stats (no crawling)
    python main.py --url <url>        # Crawl a single URL
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import sys
import time
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config import (
    BASE_URL,
    DB_PATH,
    MAX_PAGES,
    OUTPUT_DIR,
    SITEMAP_URL,
    START_URLS,
)
from crawler.mosdacSiteMap import MOSDACsitemap
from crawler.static_crawler import StaticCrawler
from storage.data_store import DataStore
from utils.helpers import is_document_url, normalise_url, url_hash
from utils.logger import get_logger

log     = get_logger(__name__)
console = Console()


# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

class CrawlOrchestrator:
    """Coordinates seeding → crawling → exporting for MOSDAC."""

    def __init__(self):
        self.store      = DataStore()
        self.start_time = time.time()

    # ── Full crawl (recommended) ──────────────────────────────

    def run_full_crawl(self) -> None:
        """
        Full pipeline:
          1. Parse /sitemap to discover all public URLs
          2. Seed the crawl queue from sitemap + START_URLS fallback
          3. Run StaticCrawler to process every queued URL
          4. Print summary + export JSON/CSV
        """
        console.print(Panel(
            "[bold cyan]Step 1 of 3:[/bold cyan] Seeding queue from MOSDAC /sitemap",
            expand=False,
        ))
        self._seed_queue()

        console.print(Panel(
            "[bold cyan]Step 2 of 3:[/bold cyan] Crawling all queued pages",
            expand=False,
        ))
        self._run_crawler()

        console.print(Panel(
            "[bold cyan]Step 3 of 3:[/bold cyan] Exporting data",
            expand=False,
        ))
        self._export()
        self._print_summary()

    # ── Seed only ─────────────────────────────────────────────

    def run_seed_only(self) -> None:
        """Seed the queue without crawling. Inspect DB before proceeding."""
        self._seed_queue()
        self._print_summary()

    # ── Crawl only (queue already seeded) ─────────────────────

    def run_crawl_only(self) -> None:
        """Process an already-seeded queue."""
        pending = self.store.queue_stats().get("pending", 0)
        if pending == 0:
            log.warning("Queue is empty. Run --mode seed first.")
            return
        log.info(f"Resuming crawl — {pending} URLs pending")
        self._run_crawler()
        self._export()
        self._print_summary()

    # ── Single URL ────────────────────────────────────────────

    def run_single_url(self, url: str) -> None:
        """Crawl exactly one URL. Useful for debugging a specific page."""
        norm = normalise_url(url)
        if not norm:
            log.error(f"Invalid URL: {url}")
            return

        log.info(f"Single URL mode: {norm}")
        uid = url_hash(norm)
        self.store.enqueue_url(norm, uid, depth=0)

        if is_document_url(norm):
            from crawler.document_parser import DocumentParser
            DocumentParser(self.store).download_and_parse(norm)
        else:
            StaticCrawler(self.store)._crawl_page(norm, depth=0)

        self._print_summary()

    def print_report_only(self) -> None:
        self._print_summary()

    # ── Internal helpers ──────────────────────────────────────

    def _seed_queue(self) -> None:
        """Use MOSDACsitemapSeeder then fall back to START_URLS."""
        seeder = MOSDACsitemap(self.store)
        pages, docs = seeder.seed_all()

        if pages == 0 and docs == 0:
            # Fallback: seed from static START_URLS list in config
            log.warning("Sitemap seeder returned 0 URLs — using START_URLS fallback")
            for url in START_URLS:
                norm = normalise_url(url)
                if norm:
                    uid = url_hash(norm)
                    self.store.enqueue_url(norm, uid, depth=0)
            log.info(f"Seeded {len(START_URLS)} URLs from START_URLS config")

    def _run_crawler(self) -> None:
        crawler = StaticCrawler(self.store)
        crawler._process_queue()

    def _export(self) -> None:
        from utils.exporter import DataExporter
        DataExporter().export_all()

    def _print_summary(self) -> None:
        elapsed = time.time() - self.start_time
        summary = self.store.summary()

        console.print()
        console.print(Panel(
            f"[bold green]MOSDAC Phase 1 Complete[/bold green]\n"
            f"Elapsed: {elapsed:.1f}s",
            expand=False,
        ))

        t = Table(title="Extraction Summary", header_style="bold cyan")
        t.add_column("Metric",    style="dim")
        t.add_column("Count",     justify="right", style="bold yellow")
        t.add_row("Pages crawled",    str(summary["pages_crawled"]))
        t.add_row("Documents (PDFs)", str(summary["documents"]))
        t.add_row("FAQs extracted",   str(summary["faqs_extracted"]))
        t.add_row("Tables extracted", str(summary["tables"]))

        q = summary.get("queue", {})
        t.add_row("Queue: pending",  str(q.get("pending",  0)))
        t.add_row("Queue: visited",  str(q.get("visited",  0)))
        t.add_row("Queue: failed",   str(q.get("failed",   0)))
        t.add_row("Queue: skipped",  str(q.get("skipped",  0)))
        console.print(t)
        console.print(f"\n[dim]Database:[/dim] {DB_PATH}")
        console.print(f"[dim]Outputs: [/dim] {OUTPUT_DIR / 'data'}")

        # Write JSON report
        import json
        report = {
            "crawl_finished_at": datetime.utcnow().isoformat() + "Z",
            "elapsed_seconds":   round(elapsed, 1),
            **summary,
        }
        report_path = OUTPUT_DIR / "data" / "crawl_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        log.info(f"JSON report → {report_path}")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MOSDAC Portal Crawler — Phase 1 Data Ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                            # Full crawl (seed + crawl + export)
  python main.py --mode seed                # Discover URLs only
  python main.py --mode crawl               # Process existing queue
  python main.py --mode report              # Show DB stats only
  python main.py --url https://mosdac.gov.in/insat-3d   # Single page test
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "seed", "crawl", "report"],
        default="full",
    )
    parser.add_argument("--url", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    console.print(Panel(
        "[bold blue]MOSDAC AI Help Bot — Phase 1: Data Ingestion[/bold blue]\n"
        "[dim]Drupal 7 portal crawler | mosdac.gov.in[/dim]",
        expand=False,
    ))

    orchestrator = CrawlOrchestrator()
    try:
        if args.url:
            orchestrator.run_single_url(args.url)
        elif args.mode == "full":
            orchestrator.run_full_crawl()
        elif args.mode == "seed":
            orchestrator.run_seed_only()
        elif args.mode == "crawl":
            orchestrator.run_crawl_only()
        elif args.mode == "report":
            orchestrator.print_report_only()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. Partial results saved.[/yellow]")
        orchestrator._print_summary()
        sys.exit(0)
    except Exception as exc:
        log.critical(f"Fatal: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
