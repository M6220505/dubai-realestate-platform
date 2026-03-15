"""
Base scraper class with shared utilities: rate limiting, user-agent rotation,
robots.txt respect, and structured output.
"""

import time
import random
import logging
import json
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Safe delays between requests (seconds)
MIN_DELAY = 3.0
MAX_DELAY = 7.0


def make_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def random_delay():
    """Sleep for a random duration to be polite to servers."""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    logger.debug("Sleeping %.1fs", delay)
    time.sleep(delay)


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


class BaseScraper(ABC):
    """Abstract base scraper. Subclass and implement `scrape_area`."""

    SOURCE_NAME: str = ""
    BASE_URL: str = ""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = make_session()
        self._robots: dict[str, RobotFileParser] = {}

    # ------------------------------------------------------------------
    # robots.txt helpers
    # ------------------------------------------------------------------

    def _get_robots(self, base_url: str) -> RobotFileParser:
        if base_url not in self._robots:
            rp = RobotFileParser()
            robots_url = base_url.rstrip("/") + "/robots.txt"
            try:
                rp.set_url(robots_url)
                rp.read()
            except Exception:
                pass  # if robots.txt unavailable, allow by default
            self._robots[base_url] = rp
        return self._robots[base_url]

    def can_fetch(self, url: str) -> bool:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        rp = self._get_robots(base)
        return rp.can_fetch("*", url)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        if not self.can_fetch(url):
            logger.warning("robots.txt disallows: %s", url)
            return None
        headers = kwargs.pop("headers", {})
        headers.setdefault("User-Agent", get_random_user_agent())
        headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        try:
            resp = self.session.get(url, headers=headers, timeout=20, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            logger.error("GET %s failed: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def save_listings(self, listings: list[dict], area_slug: str) -> Path:
        today = date.today().isoformat()
        filename = self.output_dir / f"{self.SOURCE_NAME}_{area_slug}_{today}.jsonl"
        with open(filename, "w", encoding="utf-8") as fh:
            for record in listings:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("Saved %d listings → %s", len(listings), filename)
        return filename

    @staticmethod
    def clean_price(raw: str) -> Optional[int]:
        """Parse 'AED 2,100,000' → 2100000. Returns None if unparseable."""
        if not raw:
            return None
        digits = "".join(c for c in raw if c.isdigit())
        return int(digits) if digits else None

    @staticmethod
    def clean_int(raw: str) -> Optional[int]:
        if not raw:
            return None
        digits = "".join(c for c in raw if c.isdigit())
        return int(digits) if digits else None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape_area(self, area: str, max_pages: int = 10) -> list[dict]:
        """Scrape listings for a single area. Return list of record dicts."""

    def scrape_all(self, areas: list[str], max_pages: int = 10) -> list[dict]:
        all_listings: list[dict] = []
        for area in areas:
            logger.info("[%s] Scraping area: %s", self.SOURCE_NAME, area)
            listings = self.scrape_area(area, max_pages=max_pages)
            all_listings.extend(listings)
            area_slug = area.lower().replace(" ", "_")
            self.save_listings(listings, area_slug)
            random_delay()
        return all_listings
