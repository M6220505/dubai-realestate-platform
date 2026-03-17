# scrapers/base_scraper.py
import json
import random
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    DATA_RAW_DIR, TARGET_AREAS, AREA_SLUGS,
    SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX,
    SCRAPER_MAX_RETRIES, SCRAPER_TIMEOUT,
    SCRAPER_HEADLESS, MIN_PRICE_AED, USER_AGENTS
)

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all property scrapers."""

    SOURCE_NAME: str = ""

    def __init__(self):
        self.results = []
        self.errors  = []
        self.today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Public API ───────────────────────────────────────────
    def run(self) -> list[dict]:
        """Run scraper for all target areas. Returns list of listings."""
        logger.info(f"[{self.SOURCE_NAME}] Starting scrape for {len(TARGET_AREAS)} areas")
        for area in TARGET_AREAS:
            try:
                area_results = self._scrape_area(area)
                valid = [r for r in area_results if self._validate(r)]
                self.results.extend(valid)
                logger.info(f"[{self.SOURCE_NAME}] {area}: {len(valid)}/{len(area_results)} valid listings")
            except Exception as e:
                logger.error(f"[{self.SOURCE_NAME}] Failed on {area}: {e}")
                self.errors.append({"area": area, "error": str(e)})
        self._save()
        return self.results

    # ── Abstract methods ─────────────────────────────────────
    @abstractmethod
    def _scrape_area(self, area: str) -> list[dict]:
        """Scrape listings for a single area."""
        pass

    @abstractmethod
    def _parse_listing(self, raw: dict) -> dict:
        """Parse a raw listing card into the standard schema."""
        pass

    # ── Helpers ──────────────────────────────────────────────
    def _random_delay(self):
        time.sleep(random.uniform(SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX))

    def _random_agent(self) -> str:
        return random.choice(USER_AGENTS)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _normalize_area(self, raw_area: str) -> Optional[str]:
        for area in TARGET_AREAS:
            if area.lower() in raw_area.lower() or raw_area.lower() in area.lower():
                return area
        return None

    def _validate(self, listing: dict) -> bool:
        """Return True if listing passes all validation rules."""
        if not listing.get("price"):              return False
        if listing["price"] == 0:                 return False
        if listing["price"] < MIN_PRICE_AED:      return False
        if not listing.get("area"):               return False
        if listing["area"] not in TARGET_AREAS:   return False
        if not listing.get("url"):                return False
        if not listing["url"].startswith("http"): return False
        if not listing.get("source"):             return False
        return True

    def _save(self):
        DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = DATA_RAW_DIR / f"{self.SOURCE_NAME}_{self.today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        logger.info(f"[{self.SOURCE_NAME}] Saved {len(self.results)} listings → {path}")

    def summary(self) -> dict:
        return {
            "source":   self.SOURCE_NAME,
            "date":     self.today,
            "total":    len(self.results),
            "errors":   len(self.errors),
            "by_area":  {
                area: sum(1 for r in self.results if r.get("area") == area)
                for area in TARGET_AREAS
            }
        }
