import re, json, time, random, logging
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

# ── الحل: URLs محددة لكل منطقة بدل ?q= العام ────────────────
AREA_URLS = {
    "Dubai Marina": [
        "https://www.propertyfinder.ae/en/buy/apartments-for-sale-in-dubai-marina.html",
        "https://www.propertyfinder.ae/en/buy/penthouses-for-sale-in-dubai-marina.html",
    ],
    "Downtown Dubai": [
        "https://www.propertyfinder.ae/en/buy/apartments-for-sale-in-downtown-dubai.html",
    ],
    "Palm Jumeirah": [
        "https://www.propertyfinder.ae/en/buy/apartments-for-sale-in-palm-jumeirah.html",
        "https://www.propertyfinder.ae/en/buy/villas-for-sale-in-palm-jumeirah.html",
    ],
    "Business Bay": [
        "https://www.propertyfinder.ae/en/buy/apartments-for-sale-in-business-bay.html",
    ],
    "Jumeirah Beach Residence": [
        "https://www.propertyfinder.ae/en/buy/apartments-for-sale-in-jumeirah-beach-residence.html",
    ],
}

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

MIN_PRICE = 100_000

class PropertyFinderScraperV2:
    SOURCE = "propertyfinder"

    def __init__(self):
        self.results = []
        self.today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        Path("data/raw").mkdir(parents=True, exist_ok=True)

    def run(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        for area, urls in AREA_URLS.items():
            area_listings = []
            for base_url in urls:
                for page in range(1, 6):  # max 5 pages
                    url = base_url if page == 1 else base_url.replace(".html", f"-p{page}.html")
                    listings = self._scrape_page(url, area)
                    if not listings:
                        break
                    area_listings.extend(listings)
                    logger.info(f"  {area} p{page}: +{len(listings)} → total {len(area_listings)}")
                    time.sleep(random.uniform(2, 4))

            self.results.extend(area_listings)
            logger.info(f"✓ {area}: {len(area_listings)} listings")
            time.sleep(random.uniform(3, 6))

        self._save()
        return self.results

    def _scrape_page(self, url, area):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Run: pip install playwright && playwright install chromium")
            return []

        listings = []
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-blink-features=AutomationControlled","--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(
                user_agent   = random.choice(USER_AGENTS),
                viewport     = {"width": 1440, "height": 900},
                locale       = "en-US",
                timezone_id  = "Asia/Dubai",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
            )
            ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page = ctx.new_page()

            try:
                logger.info(f"  Fetching: {url}")
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(random.randint(2500, 4000))

                # scroll to load lazy cards
                page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5)")
                page.wait_for_timeout(1000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1000)

                # try multiple card selectors
                cards = (
                    page.query_selector_all("[data-testid='property-card']") or
                    page.query_selector_all("div[class*='property-card']") or
                    page.query_selector_all("article[class*='card']") or
                    page.query_selector_all("[class*='PropertyCard']")
                )

                if not cards:
                    logger.warning(f"  No cards found on {url}")
                    browser.close()
                    return []

                for card in cards:
                    parsed = self._parse(card, area)
                    if parsed:
                        listings.append(parsed)

            except Exception as e:
                logger.warning(f"  Page error {url}: {e}")
            finally:
                browser.close()

        return listings

    def _parse(self, card, area):
        def txt(*sels):
            for s in sels:
                el = card.query_selector(s)
                if el:
                    t = el.inner_text().strip()
                    if t: return t
            return None

        def attr(sel, a):
            el = card.query_selector(sel)
            return el.get_attribute(a) if el else None

        try:
            title    = txt("h2","[data-testid='property-card-title']","[class*='title']")
            price_r  = txt("[data-testid='property-card-price']","[class*='price']","strong")
            beds_r   = txt("[data-testid='property-card-spec-bedroom']","[aria-label*='bed']","[class*='bed']")
            size_r   = txt("[data-testid='property-card-spec-area']","[aria-label*='sqft']","[class*='area']")
            type_r   = txt("[data-testid='property-card-type']","[class*='type']","[class*='category']")
            href     = attr("a","href")
            url      = f"https://www.propertyfinder.ae{href}" if href and href.startswith("/") else href

            price = self._num(price_r)
            size  = self._num(size_r)

            if not price or price < MIN_PRICE: return None
            if not url or not url.startswith("http"): return None

            return {
                "title":          title,
                "area":           area,
                "price":          price,
                "bedrooms":       self._int(beds_r),
                "size_sqft":      size,
                "price_per_sqft": round(price/size, 1) if price and size and size > 0 else None,
                "property_type":  type_r,
                "listing_date":   None,
                "source":         self.SOURCE,
                "url":            url,
                "scraped_at":     datetime.now(timezone.utc).isoformat(),
                "duplicate":      False,
            }
        except Exception as e:
            logger.debug(f"  Parse error: {e}")
            return None

    def _num(self, raw):
        if not raw: return None
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",",""))
        try: return float(cleaned) if cleaned else None
        except: return None

    def _int(self, raw):
        if not raw: return None
        m = re.search(r"\d+", raw)
        return int(m.group()) if m else None

    def _save(self):
        # deduplicate by URL
        seen, deduped = set(), []
        for r in self.results:
            if r.get("url") and r["url"] not in seen:
                seen.add(r["url"]); deduped.append(r)
            elif r.get("url"):
                r["duplicate"] = True; deduped.append(r)
        self.results = deduped

        path = f"data/raw/propertyfinder_{self.today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        # summary
        by_area = {}
        for a in AREA_URLS:
            al = [r for r in self.results if r["area"]==a and not r.get("duplicate")]
            prices = [r["price_per_sqft"] for r in al if r.get("price_per_sqft")]
            by_area[a] = {
                "count": len(al),
                "avg_psf": round(sum(prices)/len(prices)) if prices else None
            }

        print(f"\n{'='*50}")
        print(f"  PropertyFinder Scrape Complete")
        print(f"  Total: {len([r for r in self.results if not r.get('duplicate')])} unique listings")
        print(f"{'='*50}")
        for area, stats in by_area.items():
            psf = f"AED {stats['avg_psf']:,}/sqft" if stats["avg_psf"] else "no psf data"
            print(f"  {area:<30} {stats['count']:>3} listings · {psf}")
        print(f"{'='*50}")
        print(f"  Saved → {path}\n")


if __name__ == "__main__":
    s = PropertyFinderScraperV2()
    s.run()
