# scrapers/scraper_bayut.py
import re
import logging
from typing import Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scrapers.base_scraper import BaseScraper
from config.settings import AREA_SLUGS, SCRAPER_TIMEOUT, SCRAPER_HEADLESS

logger = logging.getLogger(__name__)


class BayutScraper(BaseScraper):
    SOURCE_NAME = "bayut"
    BASE_URL    = "https://www.bayut.com/for-sale/property"

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
                url = f"{self.BASE_URL}/{slug}/"
                if page_num > 1:
                    url += f"?page={page_num}"

                logger.info(f"[Bayut] Scraping {area} page {page_num}")

                retries = 0
                while retries < 3:
                    try:
                        page.goto(url, timeout=SCRAPER_TIMEOUT, wait_until="domcontentloaded")
                        page.wait_for_timeout(2500)
                        break
                    except Exception as e:
                        retries += 1
                        if retries == 3:
                            logger.error(f"[Bayut] Gave up on {url}: {e}")
                            browser.close()
                            return listings

                # Bayut uses article tags for listing cards
                cards = page.query_selector_all("article[class*='property']")
                if not cards:
                    cards = page.query_selector_all("[data-testid='listing-card']")
                if not cards:
                    logger.info(f"[Bayut] No cards on page {page_num} for {area}")
                    break

                for card in cards:
                    try:
                        raw    = self._extract_card(card, area)
                        parsed = self._parse_listing(raw)
                        if parsed:
                            listings.append(parsed)
                    except Exception as e:
                        logger.warning(f"[Bayut] Card error: {e}")

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

        href  = safe_attr("a", "href")
        url   = f"https://www.bayut.com{href}" if href and href.startswith("/") else href

        return {
            "title":     safe_text("h2") or safe_text("[class*='title']"),
            "area_raw":  area,
            "price_raw": safe_text("[aria-label*='Price']") or safe_text("[class*='price']"),
            "beds_raw":  safe_text("[aria-label*='Beds']") or safe_text("[class*='beds']"),
            "size_raw":  safe_text("[aria-label*='Area']") or safe_text("[class*='area']"),
            "type_raw":  safe_text("[class*='type']") or safe_text("[class*='category']"),
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
                "listing_date":   None,
                "source":         self.SOURCE_NAME,
                "url":            raw.get("url"),
                "scraped_at":     self._now_iso(),
            }
        except Exception as e:
            logger.warning(f"[Bayut] Parse failed: {e}")
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
    scraper = BayutScraper()
    scraper.run()
    print(scraper.summary())
