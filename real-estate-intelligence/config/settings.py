# config/settings.py
# AI Real Estate Intelligence Platform - Dubai v1
# Configuration file

from pathlib import Path

# ── Project Root ────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# ── Directories ─────────────────────────────────────────────
DATA_RAW_DIR       = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR           = BASE_DIR / "logs"

# ── Target Areas (V1) ────────────────────────────────────────
TARGET_AREAS = [
    "Dubai Marina",
    "Downtown Dubai",
    "Palm Jumeirah",
    "Business Bay",
    "Jumeirah Beach Residence",
]

# Area slug mapping for URL construction
AREA_SLUGS = {
    "Dubai Marina":              "dubai-marina",
    "Downtown Dubai":            "downtown-dubai",
    "Palm Jumeirah":             "palm-jumeirah",
    "Business Bay":              "business-bay",
    "Jumeirah Beach Residence":  "jumeirah-beach-residence",
}

# ── Scraper Settings ─────────────────────────────────────────
SCRAPER_DELAY_MIN   = 2      # seconds
SCRAPER_DELAY_MAX   = 5      # seconds
SCRAPER_MAX_RETRIES = 3
SCRAPER_TIMEOUT     = 30000  # milliseconds (Playwright)
SCRAPER_HEADLESS    = True   # set False for debugging

# ── Validation Rules ─────────────────────────────────────────
MIN_PRICE_AED = 100_000

# ── User Agents ────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# ── Pipeline Schedule ────────────────────────────────────────
PIPELINE_SCHEDULE = "0 3 * * 0"  # Sunday 03:00 AM (cron format, GST = UTC+4)

# ── Alert Threshold ──────────────────────────────────────────
ALERT_FAILURE_THRESHOLD = 0.5  # Alert if > 50% of scrapers fail

# ── Source URLs ──────────────────────────────────────────────
SOURCES = {
    "propertyfinder": {
        "base_url": "https://www.propertyfinder.ae/en/search",
        "params":   "c=1&l=1&q={slug}",
    },
    "bayut": {
        "base_url": "https://www.bayut.com/for-sale/property/{slug}/",
    },
    "dubizzle": {
        "base_url": "https://dubai.dubizzle.com/property/for-sale/",
        "area_param": "{slug}",
    },
}

# ── Government Sources ────────────────────────────────────────
GOV_SOURCES = {
    "data_dubai":  "https://data.dubai/",
    "dld":         "https://dubailand.gov.ae",
    "dsc":         "https://www.dsc.gov.ae",
}
