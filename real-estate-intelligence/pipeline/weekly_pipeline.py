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
