"""
PropertyFinder.ae scraper.

Uses requests + BeautifulSoup. Falls back to Playwright for JS-heavy pages.
Respects robots.txt, rotates user-agents, and applies random delays.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, random_delay

logger = logging.getLogger(__name__)

# Area slug mapping for PropertyFinder search URLs
AREA_SLUGS = {
    "Dubai Marina": "dubai-marina",
    "Downtown Dubai": "downtown-dubai",
    "Palm Jumeirah": "palm-jumeirah",
    "Business Bay": "business-bay",
    "Jumeirah Beach Residence": "jumeirah-beach-residence",
}


class PropertyFinderScraper(BaseScraper):
    SOURCE_NAME = "PropertyFinder"
    BASE_URL = "https://www.propertyfinder.ae"

    # Search URL template: sale listings, ordered by newest
    SEARCH_URL = (
        "https://www.propertyfinder.ae/en/search?"
        "c=1"           # category: residential
        "&fu=0"         # for sale
        "&rp=y"         # results per page = default
        "&l={area_slug}"
        "&page={page}"
    )

    def scrape_area(self, area: str, max_pages: int = 10) -> list[dict]:
        area_slug = AREA_SLUGS.get(area)
        if not area_slug:
            logger.warning("No URL slug for area '%s' – skipping", area)
            return []

        listings: list[dict] = []
        for page in range(1, max_pages + 1):
            url = self.SEARCH_URL.format(area_slug=area_slug, page=page)
            logger.info("[PropertyFinder] %s page %d → %s", area, page, url)

            resp = self.get(url)
            if resp is None:
                break

            page_listings = self._parse_listings_page(resp.text, area)
            if not page_listings:
                logger.info("[PropertyFinder] No more listings on page %d", page)
                break

            listings.extend(page_listings)
            logger.info("[PropertyFinder] Page %d: +%d listings (total %d)",
                        page, len(page_listings), len(listings))
            random_delay()

        return listings

    def _parse_listings_page(self, html: str, area: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[dict] = []

        # PropertyFinder uses article cards; selectors may drift with redesigns
        cards = soup.select(
            "article.card, "
            "[data-testid='property-card'], "
            ".property-card, "
            ".card-list__item"
        )

        if not cards:
            # Try generic fallback: any element with a price
            cards = soup.select("[class*='card']")

        for card in cards:
            record = self._parse_card(card, area)
            if record:
                records.append(record)

        return records

    def _parse_card(self, card, area: str) -> Optional[dict]:
        today = date.today().isoformat()

        # --- title ---
        title_el = (
            card.select_one("[data-testid='property-name']")
            or card.select_one(".card-list__property-name")
            or card.select_one("h2")
            or card.select_one("h3")
        )
        title = title_el.get_text(strip=True) if title_el else None

        # --- price ---
        price_el = (
            card.select_one("[data-testid='property-card-price']")
            or card.select_one(".card-list__price")
            or card.select_one("[class*='price']")
        )
        raw_price = price_el.get_text(strip=True) if price_el else None
        price = self.clean_price(raw_price)

        # Reject records missing mandatory fields
        if not price or price == 0:
            return None
        if not area:
            return None

        # --- property type ---
        type_el = (
            card.select_one("[data-testid='property-type']")
            or card.select_one(".card-list__property-type")
        )
        property_type = type_el.get_text(strip=True) if type_el else None

        # --- bedrooms ---
        bed_el = (
            card.select_one("[data-testid='property-beds']")
            or card.select_one("[class*='bed']")
        )
        bedrooms = self.clean_int(bed_el.get_text(strip=True)) if bed_el else None

        # --- bathrooms ---
        bath_el = (
            card.select_one("[data-testid='property-baths']")
            or card.select_one("[class*='bath']")
        )
        bathrooms = self.clean_int(bath_el.get_text(strip=True)) if bath_el else None

        # --- size ---
        size_el = (
            card.select_one("[data-testid='property-size']")
            or card.select_one("[class*='size']")
            or card.select_one("[class*='area']")
        )
        size_sqft = self.clean_int(size_el.get_text(strip=True)) if size_el else None

        # --- price per sqft ---
        price_per_sqft: Optional[int] = None
        if price and size_sqft and size_sqft > 0:
            price_per_sqft = round(price / size_sqft)

        # --- URL ---
        link_el = card.select_one("a[href]")
        listing_url: Optional[str] = None
        if link_el:
            href = link_el["href"]
            listing_url = href if href.startswith("http") else self.BASE_URL + href

        # --- building name (optional) ---
        building_el = card.select_one("[class*='building'], [class*='project']")
        building_name = building_el.get_text(strip=True) if building_el else None

        record: dict = {
            "title": title,
            "area": area,
            "property_type": property_type,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "size_sqft": size_sqft,
            "price": price,
            "price_per_sqft": price_per_sqft,
            "building_name": building_name,
            "developer": None,
            "floor_number": None,
            "source": self.SOURCE_NAME,
            "url": listing_url,
            "scraped_at": today,
        }
        # Strip None values to keep records lean but present as None for schema
        return record
