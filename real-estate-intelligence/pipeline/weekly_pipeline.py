"""
Weekly pipeline orchestrator.

Steps:
  1. Scrape PropertyFinder
  2. Scrape Bayut
  3. Scrape Dubizzle
  4. Download government datasets
  5. Clean and validate data
  6. Run analytics
  7. Generate weekly market report

Schedule: run every Sunday at 03:00 AM via cron or GitHub Actions.

Usage:
    python -m pipeline.weekly_pipeline
    python -m pipeline.weekly_pipeline --dry-run
    python -m pipeline.weekly_pipeline --areas "Dubai Marina" "Business Bay"
    python -m pipeline.weekly_pipeline --max-pages 5
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root so we can import sibling packages regardless of cwd
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers import PropertyFinderScraper, BayutScraper, DubizzleScraper
from datasets.government_downloader import GovernmentDataDownloader
from pipeline.cleaner import clean_and_deduplicate
from analysis.analytics import run_analytics
from analysis.report_generator import generate_report

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weekly_pipeline")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_AREAS = [
    "Dubai Marina",
    "Downtown Dubai",
    "Palm Jumeirah",
    "Business Bay",
    "Jumeirah Beach Residence",
]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
GOV_DIR = PROJECT_ROOT / "datasets" / "government"
REPORTS_DIR = PROJECT_ROOT / "reports"


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step_scrape_propertyfinder(areas: list[str], max_pages: int) -> list[dict]:
    logger.info("=" * 60)
    logger.info("STEP 1: Scraping PropertyFinder")
    scraper = PropertyFinderScraper(output_dir=RAW_DIR)
    listings = scraper.scrape_all(areas, max_pages=max_pages)
    logger.info("PropertyFinder: %d listings collected", len(listings))
    return listings


def step_scrape_bayut(areas: list[str], max_pages: int) -> list[dict]:
    logger.info("=" * 60)
    logger.info("STEP 2: Scraping Bayut")
    scraper = BayutScraper(output_dir=RAW_DIR)
    listings = scraper.scrape_all(areas, max_pages=max_pages)
    logger.info("Bayut: %d listings collected", len(listings))
    return listings


def step_scrape_dubizzle(areas: list[str], max_pages: int) -> list[dict]:
    logger.info("=" * 60)
    logger.info("STEP 3: Scraping Dubizzle")
    scraper = DubizzleScraper(output_dir=RAW_DIR)
    listings = scraper.scrape_all(areas, max_pages=max_pages)
    logger.info("Dubizzle: %d listings collected", len(listings))
    return listings


def step_download_government_data() -> list[Path]:
    logger.info("=" * 60)
    logger.info("STEP 4: Downloading government datasets")
    downloader = GovernmentDataDownloader(output_dir=GOV_DIR)
    files = downloader.download_all()
    logger.info("Government datasets: %d files downloaded", len(files))
    return files


def step_clean_data(today: date) -> list[dict]:
    logger.info("=" * 60)
    logger.info("STEP 5: Cleaning and validating data")
    output_path = PROCESSED_DIR / f"listings_cleaned_{today.isoformat()}.jsonl"
    records = clean_and_deduplicate(RAW_DIR, output_path)
    logger.info("Cleaned records: %d", len(records))
    return records


def step_analytics(today: date) -> dict:
    logger.info("=" * 60)
    logger.info("STEP 6: Running analytics")
    analytics = run_analytics(PROCESSED_DIR, today=today)
    # Save analytics JSON
    analytics_path = PROCESSED_DIR / f"analytics_{today.isoformat()}.json"
    analytics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(analytics_path, "w") as fh:
        json.dump(analytics, fh, indent=2)
    logger.info("Analytics saved → %s", analytics_path)
    return analytics


def step_generate_report(analytics: dict) -> Path:
    logger.info("=" * 60)
    logger.info("STEP 7: Generating weekly market report")
    report_path = generate_report(analytics, REPORTS_DIR)
    logger.info("Report → %s", report_path)
    return report_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(areas: list[str], max_pages: int, dry_run: bool = False):
    today = date.today()
    logger.info("Dubai Real Estate Intelligence Pipeline — %s", today.isoformat())
    logger.info("Areas: %s", ", ".join(areas))
    logger.info("Max pages per area: %d", max_pages)
    if dry_run:
        logger.info("DRY RUN mode — scrapers will not make real requests")
        return

    # Ensure directories exist
    for d in [RAW_DIR, PROCESSED_DIR, GOV_DIR, REPORTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # --- Scraping ---
    step_scrape_propertyfinder(areas, max_pages)
    step_scrape_bayut(areas, max_pages)
    step_scrape_dubizzle(areas, max_pages)

    # --- Government data ---
    step_download_government_data()

    # --- Processing ---
    step_clean_data(today)

    # --- Analytics + Report ---
    analytics = step_analytics(today)
    if analytics:
        report = step_generate_report(analytics)
        logger.info("=" * 60)
        logger.info("Pipeline complete. Report: %s", report)
    else:
        logger.warning("No analytics produced — no report generated.")


def main():
    parser = argparse.ArgumentParser(
        description="Dubai Real Estate Intelligence — Weekly Pipeline"
    )
    parser.add_argument(
        "--areas",
        nargs="+",
        default=DEFAULT_AREAS,
        help="Areas to scrape (default: all 5 V1 areas)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Max search result pages per area per scraper (default: 20)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without making requests",
    )
    args = parser.parse_args()
    run(areas=args.areas, max_pages=args.max_pages, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
