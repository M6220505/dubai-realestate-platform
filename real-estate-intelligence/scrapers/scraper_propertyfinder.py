# scrapers/scraper_propertyfinder.py
import re
import logging
from typing import Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scrapers.base_scraper import BaseScraper
from config.settings import AREA_SLUGS, SCRAPER_TIMEOUT, SCRAPER_HEADLESS

logger = logging.getLogger(__name__)


class PropertyFinderScraper(BaseScraper):
    SOURCE_NAME = "propertyfinder"
    BASE_URL    = "https://www.propertyfinder.ae/en/search"

    def _scrape_area(self, area: str) -> list[dict]:
        """Scrape all listing pages for a given area."""
        from playwright.sync_api import sync_playwright

        slug     = AREA_SLUGS[area]
        listings = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=SCRAPER_HEADLESS)
            context = browser.new_context(
                user_agent    = self._random_agent(),
                viewport      = {"width": 1440, "height": 900},
                locale        = "en-US",
                timezone_id   = "Asia/Dubai",
            )
            page = context.new_page()

            page_num = 1
            while True:
                url = f"{self.BASE_URL}?c=1&l=1&q={slug}&page={page_num}"
                logger.info(f"[PF] Scraping {area} page {page_num}: {url}")

                retries = 0
                while retries < 3:
                    try:
                        page.goto(url, timeout=SCRAPER_TIMEOUT, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                        break
                    except Exception as e:
                        retries += 1
                        logger.warning(f"[PF] Retry {retries}/3 for {url}: {e}")
                        if retries == 3:
                            logger.error(f"[PF] Giving up on {url}")
                            browser.close()
                            return listings

                # Extract listing cards
                cards = page.query_selector_all("[data-testid='property-card']")
                if not cards:
                    # Try alternate selector
                    cards = page.query_selector_all(".property-card")

                if not cards:
                    logger.info(f"[PF] No more cards on page {page_num} for {area}")
                    break

                for card in cards:
                    try:
                        raw = self._extract_card(card, area, page)
                        parsed = self._parse_listing(raw)
                        if parsed:
                            listings.append(parsed)
                    except Exception as e:
                        logger.warning(f"[PF] Card parse error: {e}")

                self._random_delay()
                page_num += 1

                # Safety limit: max 10 pages per area
                if page_num > 10:
                    break

            browser.close()

        return listings

    def _extract_card(self, card, area: str, page) -> dict:
        """Extract raw data from a listing card element."""
        def safe_text(selector):
            el = card.query_selector(selector)
            return el.inner_text().strip() if el else None

        def safe_attr(selector, attr):
            el = card.query_selector(selector)
            return el.get_attribute(attr) if el else None

        href  = safe_attr("a", "href")
        url   = f"https://www.propertyfinder.ae{href}" if href and href.startswith("/") else href

        price_raw = safe_text("[data-testid='property-card-price']") or safe_text(".price")
        beds_raw  = safe_text("[data-testid='property-card-spec-bedroom']") or safe_text(".beds")
        size_raw  = safe_text("[data-testid='property-card-spec-area']") or safe_text(".size")
        title_raw = safe_text("[data-testid='property-card-title']") or safe_text("h2")
        type_raw  = safe_text("[data-testid='property-card-type']") or safe_text(".type")

        return {
            "title":    title_raw,
            "area_raw": area,
            "price_raw": price_raw,
            "beds_raw":  beds_raw,
            "size_raw":  size_raw,
            "type_raw":  type_raw,
            "url":       url,
        }

    def _parse_listing(self, raw: dict) -> Optional[dict]:
        """Convert raw extracted data to standard schema."""
        try:
            price = self._parse_price(raw.get("price_raw"))
            beds  = self._parse_int(raw.get("beds_raw"))
            size  = self._parse_float(raw.get("size_raw"))
            area  = self._normalize_area(raw.get("area_raw", ""))

            return {
                "title":         raw.get("title"),
                "area":          area,
                "price":         price,
                "bedrooms":      beds,
                "size_sqft":     size,
                "price_per_sqft": round(price / size, 2) if price and size else None,
                "property_type": raw.get("type_raw"),
                "listing_date":  None,
                "source":        self.SOURCE_NAME,
                "url":           raw.get("url"),
                "scraped_at":    self._now_iso(),
            }
        except Exception as e:
            logger.warning(f"[PF] Parse failed: {e} | raw={raw}")
            return None

    # ── Parsers ──────────────────────────────────────────────
    def _parse_price(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_int(self, raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        match = re.search(r"\d+", raw)
        return int(match.group()) if match else None

    def _parse_float(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return float(cleaned)
        except ValueError:
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = PropertyFinderScraper()
    results = scraper.run()
    print(scraper.summary())
