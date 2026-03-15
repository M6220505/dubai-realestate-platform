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
from datasets.local_real_data_loader import load_available
from pipeline.cleaner import clean_and_deduplicate
from analysis.analytics import run_analytics
from analysis.report_generator import generate_report
from analysis.rent_market_analysis import compute_rent_stats
from analysis.investment_ranking import build_ranking

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
OUTPUTS_DIR = PROJECT_ROOT / "outputs"


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


def step_government_csv_analytics() -> None:
    """
    Step 8: Load locally stored Dubai Open Data CSVs and produce:
      - outputs/rent_area_rankings.csv
      - outputs/investment_rankings.csv

    Skipped gracefully when the CSV files are not present (CI / dry-run).
    """
    logger.info("=" * 60)
    logger.info("STEP 8: Government CSV analytics (local datasets)")

    data = load_available(DATA_DIR)

    if not data:
        logger.warning("No local government CSVs found in %s — skipping step 8", DATA_DIR)
        return

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Rental market rankings ---
    if "rent_contracts" in data:
        rent_df = data["rent_contracts"]
        logger.info("Computing rent market stats (%d rows)…", len(rent_df))
        rent_summary = compute_rent_stats(rent_df)
        out = OUTPUTS_DIR / "rent_area_rankings.csv"
        rent_summary.to_csv(out, index=False)
        logger.info("Rent rankings saved → %s (%d areas)", out, len(rent_summary))
    else:
        rent_summary = None

    # --- Investment rankings ---
    if rent_summary is not None and len(data) >= 2:
        transactions_df = data.get("transactions_agg")
        permits_df = data.get("building_permits")
        sale_index_df = data.get("sale_index")

        # Provide empty DataFrames for any missing datasets so ranking still runs
        import pandas as pd
        empty = pd.DataFrame()
        rankings = build_ranking(
            rent_df=rent_summary,
            transactions_df=transactions_df if transactions_df is not None else empty,
            permits_df=permits_df if permits_df is not None else empty,
            sale_index_df=sale_index_df if sale_index_df is not None else empty,
        )
        out = OUTPUTS_DIR / "investment_rankings.csv"
        rankings.to_csv(out, index=False)
        logger.info("Investment rankings saved → %s (%d areas ranked)", out, len(rankings))
    else:
        logger.info("Skipping investment ranking — insufficient datasets loaded")


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
    for d in [RAW_DIR, PROCESSED_DIR, GOV_DIR, REPORTS_DIR, OUTPUTS_DIR]:
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

    # --- Government CSV analytics (runs independently of scraping) ---
    step_government_csv_analytics()


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
