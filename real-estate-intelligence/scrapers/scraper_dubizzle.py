# scrapers/scraper_dubizzle.py
import re
import logging
from typing import Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scrapers.base_scraper import BaseScraper
from config.settings import AREA_SLUGS, SCRAPER_TIMEOUT, SCRAPER_HEADLESS

logger = logging.getLogger(__name__)


class DubizzleScraper(BaseScraper):
    SOURCE_NAME = "dubizzle"
    BASE_URL    = "https://dubai.dubizzle.com/property/for-sale"

    def _scrape_area(self, area: str) -> list[dict]:
        from playwright.sync_api import sync_playwright

        slug     = AREA_SLUGS[area]
        listings = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=SCRAPER_HEADLESS)
            context = browser.new_context(
                user_agent  = self._random_agent(),
                viewport    = {"width": 1440, "height": 900},
                locale      = "en-US",
                timezone_id = "Asia/Dubai",
            )
            page = context.new_page()

            page_num = 1
            while True:
                url = f"{self.BASE_URL}/?location={slug}&page={page_num}"
                logger.info(f"[Dubizzle] Scraping {area} page {page_num}")

                retries = 0
                while retries < 3:
                    try:
                        page.goto(url, timeout=SCRAPER_TIMEOUT, wait_until="networkidle")
                        page.wait_for_timeout(2000)
                        break
                    except Exception as e:
                        retries += 1
                        if retries == 3:
                            logger.error(f"[Dubizzle] Gave up on {url}: {e}")
                            browser.close()
                            return listings

                cards = page.query_selector_all("[data-testid='listing-card']")
                if not cards:
                    cards = page.query_selector_all(".listing-card")
                if not cards:
                    logger.info(f"[Dubizzle] No cards on page {page_num} for {area}")
                    break

                for card in cards:
                    try:
                        raw    = self._extract_card(card, area)
                        parsed = self._parse_listing(raw)
                        if parsed:
                            listings.append(parsed)
                    except Exception as e:
                        logger.warning(f"[Dubizzle] Card error: {e}")

                self._random_delay()
                page_num += 1
                if page_num > 10:
                    break

            browser.close()
        return listings

    def _extract_card(self, card, area: str) -> dict:
        def safe_text(selector):
            el = card.query_selector(selector)
            return el.inner_text().strip() if el else None

        def safe_attr(selector, attr):
            el = card.query_selector(selector)
            return el.get_attribute(attr) if el else None

        href = safe_attr("a", "href")
        url  = f"https://dubai.dubizzle.com{href}" if href and href.startswith("/") else href

        return {
            "title":     safe_text("h2") or safe_text("[class*='title']"),
            "area_raw":  area,
            "price_raw": safe_text("[class*='price']") or safe_text("strong"),
            "beds_raw":  safe_text("[class*='bedroom']") or safe_text("[class*='bed']"),
            "size_raw":  safe_text("[class*='size']") or safe_text("[class*='area']"),
            "type_raw":  safe_text("[class*='type']") or safe_text("[class*='category']"),
            "date_raw":  safe_text("[class*='date']") or safe_text("time"),
            "url":       url,
        }

    def _parse_listing(self, raw: dict) -> Optional[dict]:
        try:
            price = self._parse_price(raw.get("price_raw"))
            beds  = self._parse_int(raw.get("beds_raw"))
            size  = self._parse_float(raw.get("size_raw"))
            area  = self._normalize_area(raw.get("area_raw", ""))

            return {
                "title":          raw.get("title"),
                "area":           area,
                "price":          price,
                "bedrooms":       beds,
                "size_sqft":      size,
                "price_per_sqft": round(price / size, 2) if price and size else None,
                "property_type":  raw.get("type_raw"),
                "listing_date":   raw.get("date_raw"),
                "source":         self.SOURCE_NAME,
                "url":            raw.get("url"),
                "scraped_at":     self._now_iso(),
            }
        except Exception as e:
            logger.warning(f"[Dubizzle] Parse failed: {e}")
            return None

    def _parse_price(self, raw):
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_int(self, raw):
        if not raw:
            return None
        match = re.search(r"\d+", raw)
        return int(match.group()) if match else None

    def _parse_float(self, raw):
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return float(cleaned)
        except ValueError:
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = DubizzleScraper()
    scraper.run()
    print(scraper.summary())
