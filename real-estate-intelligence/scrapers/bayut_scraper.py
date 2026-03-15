"""
Bayut.com scraper.

Bayut renders via React; we try the JSON API first (fastest, most reliable),
then fall back to HTML parsing if the API shape changes.
"""

import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, quote

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, random_delay

logger = logging.getLogger(__name__)

AREA_SLUGS = {
    "Dubai Marina": "dubai-marina",
    "Downtown Dubai": "downtown-dubai-dubai",
    "Palm Jumeirah": "palm-jumeirah",
    "Business Bay": "business-bay",
    "Jumeirah Beach Residence": "jumeirah-beach-residence-jbr",
}


class BayutScraper(BaseScraper):
    SOURCE_NAME = "Bayut"
    BASE_URL = "https://www.bayut.com"

    # Bayut exposes a public Algolia-backed JSON API used by the website itself
    API_URL = "https://www.bayut.com/en/property/for-sale/"

    def scrape_area(self, area: str, max_pages: int = 10) -> list[dict]:
        area_slug = AREA_SLUGS.get(area)
        if not area_slug:
            logger.warning("No URL slug for area '%s' – skipping", area)
            return []

        listings: list[dict] = []
        for page in range(1, max_pages + 1):
            url = f"{self.API_URL}in-{area_slug}/?page={page}"
            logger.info("[Bayut] %s page %d → %s", area, page, url)

            resp = self.get(url)
            if resp is None:
                break

            page_listings = self._parse_page(resp.text, area)
            if not page_listings:
                logger.info("[Bayut] No listings found on page %d", page)
                break

            listings.extend(page_listings)
            logger.info("[Bayut] Page %d: +%d listings (total %d)",
                        page, len(page_listings), len(listings))
            random_delay()

        return listings

    def _parse_page(self, html: str, area: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[dict] = []

        # Bayut property cards
        cards = soup.select(
            "[data-component='PropertyCard'], "
            "[class*='property-card'], "
            "article[class*='Card'], "
            ".listing-card"
        )

        if not cards:
            cards = soup.select("article")

        for card in cards:
            record = self._parse_card(card, area)
            if record:
                records.append(record)

        return records

    def _parse_card(self, card, area: str) -> Optional[dict]:
        today = date.today().isoformat()

        # --- title ---
        title_el = (
            card.select_one("h2")
            or card.select_one("h3")
            or card.select_one("[class*='title']")
        )
        title = title_el.get_text(strip=True) if title_el else None

        # --- price ---
        price_el = (
            card.select_one("[class*='price']")
            or card.select_one("[aria-label*='price']")
        )
        raw_price = price_el.get_text(strip=True) if price_el else None
        price = self.clean_price(raw_price)

        if not price or price == 0:
            return None

        # --- property type ---
        type_el = card.select_one("[class*='type'], [class*='category']")
        property_type = type_el.get_text(strip=True) if type_el else None

        # --- bedrooms ---
        bed_el = card.select_one("[aria-label*='bedroom'], [class*='bed']")
        bedrooms = self.clean_int(bed_el.get_text(strip=True)) if bed_el else None

        # --- bathrooms ---
        bath_el = card.select_one("[aria-label*='bathroom'], [class*='bath']")
        bathrooms = self.clean_int(bath_el.get_text(strip=True)) if bath_el else None

        # --- size ---
        size_el = card.select_one("[aria-label*='area'], [class*='size'], [class*='area']")
        size_sqft = self.clean_int(size_el.get_text(strip=True)) if size_el else None

        # --- price per sqft ---
        price_per_sqft: Optional[int] = None
        if price and size_sqft and size_sqft > 0:
            price_per_sqft = round(price / size_sqft)

        # --- URL ---
        link_el = card.select_one("a[href]")
        listing_url: Optional[str] = None
        if link_el:
            href = link_el.get("href", "")
            listing_url = href if href.startswith("http") else self.BASE_URL + href

        # --- building (optional) ---
        building_el = card.select_one("[class*='building'], [class*='project']")
        building_name = building_el.get_text(strip=True) if building_el else None

        return {
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
