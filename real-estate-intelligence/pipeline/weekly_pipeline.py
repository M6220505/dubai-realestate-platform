<<<<<<< HEAD
# pipeline/weekly_pipeline.py
"""
Weekly Pipeline — AI Real Estate Intelligence Platform (Dubai v1)
Schedule: Every Sunday 03:00 AM GST (UTC+4)

Usage:
    python pipeline/weekly_pipeline.py

Cron (add to crontab):
    0 3 * * 0 cd /path/to/real-estate-intelligence && python pipeline/weekly_pipeline.py >> logs/cron.log 2>&1
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    DATA_RAW_DIR, DATA_PROCESSED_DIR, LOGS_DIR,
    TARGET_AREAS, ALERT_FAILURE_THRESHOLD
)
from scrapers.scraper_propertyfinder import PropertyFinderScraper
from scrapers.scraper_bayut import BayutScraper
from scrapers.scraper_dubizzle import DubizzleScraper
from datasets.data_dubai_loader import GovernmentDataLoader
from analysis.market_summary import MarketSummary


# ── Logging setup ────────────────────────────────────────────
def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"pipeline_{today}.log"

    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger("pipeline")


# ── Pipeline ─────────────────────────────────────────────────
class WeeklyPipeline:

    def __init__(self):
        self.logger     = logging.getLogger("pipeline")
        self.start_time = datetime.now(timezone.utc)
        self.today      = self.start_time.strftime("%Y-%m-%d")
        self.week_label = self.start_time.strftime("%Y-W%W")
        self.results    = {}

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(f"  Pipeline START — {self.week_label}")
        self.logger.info("=" * 60)

        all_listings = []
        scraper_statuses = {}

        # ── Step 1–3: Run scrapers ────────────────────────────
        scrapers = [
            ("propertyfinder", PropertyFinderScraper),
            ("bayut",          BayutScraper),
            ("dubizzle",       DubizzleScraper),
        ]

        for name, ScraperClass in scrapers:
            self.logger.info(f"\n[Step] Running {name} scraper...")
            try:
                scraper  = ScraperClass()
                listings = scraper.run()
                all_listings.extend(listings)
                scraper_statuses[name] = {
                    "status":  "success",
                    "count":   len(listings),
                    "summary": scraper.summary(),
                }
                self.logger.info(f"[Step] {name}: ✓ {len(listings)} listings")
            except Exception as e:
                self.logger.error(f"[Step] {name}: ✗ FAILED — {e}")
                scraper_statuses[name] = {"status": "failed", "error": str(e)}

        # ── Step 4: Government data ───────────────────────────
        self.logger.info("\n[Step] Loading government data...")
        gov_result = {"status": "skipped", "records": []}
        try:
            loader     = GovernmentDataLoader()
            gov_result = loader.run()
            self.logger.info(f"[Step] Gov data: {gov_result['status']} (tier {gov_result.get('tier')})")
        except Exception as e:
            self.logger.error(f"[Step] Gov data failed: {e}")

        # ── Step 5: Validate ─────────────────────────────────
        self.logger.info("\n[Step] Validating listings...")
        valid, rejected = self._validate_all(all_listings)
        self.logger.info(f"[Step] Validation: {len(valid)} valid, {len(rejected)} rejected")

        # ── Step 6: Deduplicate ──────────────────────────────
        self.logger.info("\n[Step] Deduplicating...")
        deduped = self._deduplicate(valid)
        self.logger.info(f"[Step] After dedup: {len(deduped)} unique listings")

        # ── Step 7: Merge & save ─────────────────────────────
        self.logger.info("\n[Step] Saving merged dataset...")
        self._save_weekly(deduped)

        # ── Step 8: Market summary ───────────────────────────
        self.logger.info("\n[Step] Generating market summary...")
        try:
            summary = MarketSummary(deduped, self.week_label)
            summary.generate()
        except Exception as e:
            self.logger.error(f"[Step] Summary failed: {e}")

        # ── Step 9: Pipeline log ─────────────────────────────
        failed_count = sum(1 for s in scraper_statuses.values() if s["status"] == "failed")
        total_count  = len(scraper_statuses)
        pipeline_ok  = (failed_count / total_count) < ALERT_FAILURE_THRESHOLD

        status = "success" if pipeline_ok else "partial"
        if failed_count == total_count:
            status = "failed"

        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        self._save_pipeline_log(status, scraper_statuses, gov_result, len(valid), len(rejected), duration)

        # Alert check
        if not pipeline_ok:
            self.logger.warning(f"⚠️  ALERT: {failed_count}/{total_count} scrapers failed this run!")

        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"  Pipeline {status.upper()} — {duration:.1f}s")
        self.logger.info(f"  Listings: {len(deduped)} | Rejected: {len(rejected)}")
        self.logger.info(f"{'='*60}\n")

    # ── Validation ───────────────────────────────────────────
    def _validate_all(self, listings: list[dict]):
        from config.settings import MIN_PRICE_AED, TARGET_AREAS
        valid, rejected = [], []
        for l in listings:
            reasons = []
            if not l.get("price"):                     reasons.append("missing_price")
            elif l["price"] == 0:                      reasons.append("zero_price")
            elif l["price"] < MIN_PRICE_AED:           reasons.append("unrealistic_price")
            if not l.get("area"):                      reasons.append("missing_area")
            elif l["area"] not in TARGET_AREAS:        reasons.append("wrong_area")
            if not l.get("url"):                       reasons.append("missing_url")
            elif not l["url"].startswith("http"):      reasons.append("invalid_url")
            if not l.get("source"):                    reasons.append("missing_source")

            if reasons:
                l["rejected"]        = True
                l["rejection_reasons"] = reasons
                rejected.append(l)
            else:
                valid.append(l)
        return valid, rejected

    # ── Deduplication ────────────────────────────────────────
    def _deduplicate(self, listings: list[dict]) -> list[dict]:
        seen    = {}
        deduped = []
        for l in listings:
            url = l.get("url")
            if not url:
                deduped.append(l)
                continue
            if url in seen:
                # Mark as duplicate, keep most recently scraped
                existing = seen[url]
                if l.get("scraped_at", "") > existing.get("scraped_at", ""):
                    seen[url]["duplicate"] = True
                    seen[url] = l
                else:
                    l["duplicate"] = True
            else:
                seen[url] = l
                deduped.append(l)
        return deduped

    # ── Save helpers ─────────────────────────────────────────
    def _save_weekly(self, listings: list[dict]):
        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        path = DATA_PROCESSED_DIR / f"weekly_{self.week_label}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        self.logger.info(f"[Step] Saved {len(listings)} listings → {path}")

    def _save_pipeline_log(self, status, scrapers, gov, valid_count, rejected_count, duration):
        log = {
            "week":           self.week_label,
            "date":           self.today,
            "status":         status,
            "duration_sec":   round(duration, 2),
            "scrapers":       scrapers,
            "gov_data":       {"status": gov["status"], "tier": gov.get("tier")},
            "listings_valid": valid_count,
            "listings_rejected": rejected_count,
        }
        path = LOGS_DIR / f"pipeline_{self.today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)


# ── Entry point ──────────────────────────────────────────────
if __name__ == "__main__":
    logger = setup_logging()
    try:
        pipeline = WeeklyPipeline()
        pipeline.run()
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)
        sys.exit(1)
=======
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
>>>>>>> fc0932babea4050510c0efdd24e21c142329beec
