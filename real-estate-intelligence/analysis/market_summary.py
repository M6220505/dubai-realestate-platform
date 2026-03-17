# analysis/market_summary.py
"""
Generates weekly market summary per area from merged listings.
"""
import json
import logging
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DATA_PROCESSED_DIR, TARGET_AREAS

logger = logging.getLogger(__name__)


class MarketSummary:

    def __init__(self, listings: list[dict], week_label: str):
        self.listings   = listings
        self.week_label = week_label
        self.generated  = datetime.now(timezone.utc).isoformat()

    def generate(self) -> dict:
        """Generate full summary document."""
        areas_summary = []
        for area in TARGET_AREAS:
            area_listings = [l for l in self.listings if l.get("area") == area]
            if area_listings:
                areas_summary.append(self._summarize_area(area, area_listings))
            else:
                logger.warning(f"[Summary] No listings found for {area}")

        total    = len(self.listings)
        rejected = sum(1 for l in self.listings if l.get("rejected"))
        quality  = round((total - rejected) / total, 4) if total > 0 else 0

        summary = {
            "week":             self.week_label,
            "generated_at":     self.generated,
            "total_listings":   total,
            "data_quality_score": quality,
            "pipeline_status":  "success" if areas_summary else "partial",
            "areas":            areas_summary,
            "records":          self.listings,
        }

        self._save(summary)
        return summary

    def _summarize_area(self, area: str, listings: list[dict]) -> dict:
        prices      = [l["price"] for l in listings if l.get("price")]
        psf_values  = [l["price_per_sqft"] for l in listings if l.get("price_per_sqft")]

        sources = {}
        for l in listings:
            src = l.get("source", "unknown")
            sources[src] = sources.get(src, 0) + 1

        return {
            "area":              area,
            "listings_count":    len(listings),
            "avg_price_aed":     round(statistics.mean(prices)) if prices else None,
            "median_price_aed":  round(statistics.median(prices)) if prices else None,
            "min_price_aed":     min(prices) if prices else None,
            "max_price_aed":     max(prices) if prices else None,
            "avg_price_per_sqft": round(statistics.mean(psf_values), 2) if psf_values else None,
            "data_quality_score": self._quality_score(listings),
            "sources":           sources,
        }

    def _quality_score(self, listings: list[dict]) -> float:
        if not listings:
            return 0.0
        required = ["title", "area", "price", "url", "source", "scraped_at"]
        scores = []
        for l in listings:
            filled = sum(1 for f in required if l.get(f))
            scores.append(filled / len(required))
        return round(statistics.mean(scores), 4)

    def _save(self, summary: dict):
        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        path = DATA_PROCESSED_DIR / f"summary_{self.week_label}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"[Summary] Saved → {path}")
        self._print_table(summary)

    def _print_table(self, summary: dict):
        print(f"\n{'='*60}")
        print(f"  Weekly Market Summary — {summary['week']}")
        print(f"{'='*60}")
        print(f"  Total listings : {summary['total_listings']}")
        print(f"  Quality score  : {summary['data_quality_score']}")
        print(f"{'─'*60}")
        for area in summary["areas"]:
            print(f"\n  📍 {area['area']}")
            print(f"     Listings  : {area['listings_count']}")
            if area['avg_price_aed']:
                print(f"     Avg price : AED {area['avg_price_aed']:,}")
            if area['median_price_aed']:
                print(f"     Median    : AED {area['median_price_aed']:,}")
            if area['avg_price_per_sqft']:
                print(f"     Avg/sqft  : AED {area['avg_price_per_sqft']:,}")
        print(f"\n{'='*60}\n")
