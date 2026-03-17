<<<<<<< HEAD
# AI Real Estate Intelligence Platform — Dubai v1

Automated weekly property market data pipeline for Dubai.

## Quick Setup (MacBook)

```bash
# 1. Clone / navigate to project
cd real-estate-intelligence

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers
playwright install chromium

# 5. Run the pipeline manually
python pipeline/weekly_pipeline.py
```

## Schedule (Weekly — Cron)

```bash
# Open crontab
crontab -e

# Add this line (Sunday 03:00 AM GST = 11:00 PM UTC Saturday)
0 23 * * 6 cd /path/to/real-estate-intelligence && source venv/bin/activate && python pipeline/weekly_pipeline.py
```

## Run Individual Scrapers

```bash
python scrapers/scraper_propertyfinder.py
python scrapers/scraper_bayut.py
python scrapers/scraper_dubizzle.py
python datasets/data_dubai_loader.py
```

## Output Files

| File | Description |
|------|-------------|
| `data/raw/propertyfinder_YYYY-MM-DD.json` | Raw PF listings |
| `data/raw/bayut_YYYY-MM-DD.json` | Raw Bayut listings |
| `data/raw/dubizzle_YYYY-MM-DD.json` | Raw Dubizzle listings |
| `data/raw/dld_transactions_YYYY-MM-DD.csv` | Government DLD data |
| `data/processed/weekly_YYYY-WNN.json` | Merged weekly dataset |
| `data/processed/summary_YYYY-WNN.json` | Market summary per area |
| `logs/pipeline_YYYY-MM-DD.log` | Pipeline run log |
| `logs/pipeline_YYYY-MM-DD.json` | Structured pipeline status |

## Target Areas (V1)

=======
# Dubai Real Estate Intelligence Platform — V1

A weekly automated property data intelligence pipeline for Dubai.

Collects **real, verifiable** listing data from three major property marketplaces
and free government datasets, then produces structured analytics and a weekly
market report.

> **Critical rule**: Only real data sources are used.
> No synthetic market data, no estimated prices, no fabricated transactions.

---

## Coverage

### Target Areas (V1)
>>>>>>> fc0932babea4050510c0efdd24e21c142329beec
- Dubai Marina
- Downtown Dubai
- Palm Jumeirah
- Business Bay
- Jumeirah Beach Residence

<<<<<<< HEAD
## Data Sources

**Government:**
- data.dubai (primary)
- Dubai Land Department PDF reports (fallback)
- Dubai Statistics Center (fallback)

**Marketplaces:**
- PropertyFinder (primary)
- Bayut (secondary)
- Dubizzle (tertiary)

## Notes

- Listing price ≠ sale price. They are stored in separate fields.
- Records with missing price, area, or URL are automatically rejected.
- Duplicate listings are flagged (`"duplicate": true`), not deleted.
=======
### Data Sources

| Source | Type | URL |
|--------|------|-----|
| PropertyFinder | Listing portal | https://www.propertyfinder.ae |
| Bayut | Listing portal | https://www.bayut.com |
| Dubizzle | Listing portal | https://dubai.dubizzle.com/property/ |
| Dubai Open Data | Government stats | https://data.dubai.gov.ae |

### Government Datasets Targeted
1. Residential Sale Index
2. Real Estate Market Indicators
3. Population and Housing Statistics
4. Construction Activity Statistics
5. Land Use Data
6. Building Permits Data
7. Rental Market Indicators

---

## Project Structure

```
real-estate-intelligence/
├── scrapers/
│   ├── base_scraper.py          # Shared: rate limiting, robots.txt, UA rotation
│   ├── propertyfinder_scraper.py
│   ├── bayut_scraper.py
│   └── dubizzle_scraper.py
├── datasets/
│   ├── government_downloader.py  # Dubai Open Data CKAN API downloader
│   └── government/               # Downloaded government datasets
├── pipeline/
│   ├── cleaner.py                # Validation + deduplication
│   └── weekly_pipeline.py        # Main orchestrator (7 steps)
├── analysis/
│   ├── analytics.py              # Area stats, week-on-week trends
│   └── report_generator.py       # Markdown report generation
├── data/
│   ├── raw/                      # Scraped JSONL files (per source/area/date)
│   └── processed/                # Cleaned + merged datasets + analytics JSON
├── reports/                      # Weekly markdown reports
├── .github/workflows/
│   └── weekly_pipeline.yml       # GitHub Actions: runs every Sunday 03:00 UTC
├── requirements.txt
├── schedule_cron.sh              # Install local cron job
└── README.md
```

---

## Prerequisites

- Python 3.9 or newer (`python3 --version`)
- macOS (Homebrew): `brew install python3`
- Ubuntu/Debian: `sudo apt install python3 python3-pip python3-venv`

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd superpowers/real-estate-intelligence   # git clones into a folder named superpowers/
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

Or use the convenience script (does the same thing):

```bash
cd superpowers/real-estate-intelligence
bash setup.sh
source .venv/bin/activate
```

### 2. (Optional) Install Playwright browsers

Only needed if target sites require JavaScript rendering:

```bash
playwright install chromium
```

### 3. Run the pipeline

> **Important:** All commands must be run from inside the `real-estate-intelligence/` directory
> with the virtual environment active (`source .venv/bin/activate`).

```bash
# Full weekly run (all 5 areas, 20 pages per area per scraper)
python3 -m pipeline.weekly_pipeline

# Quick test run (2 pages per area)
python3 -m pipeline.weekly_pipeline --max-pages 2

# Single area
python3 -m pipeline.weekly_pipeline --areas "Dubai Marina"

# Dry run (validate config, no requests)
python3 -m pipeline.weekly_pipeline --dry-run
```

### 4. Run tests

```bash
python3 -m pytest tests/ -v
```

---

## Automation

### GitHub Actions (Recommended)
The workflow at `.github/workflows/weekly_pipeline.yml` runs automatically
every Sunday at 03:00 AM UTC.

Push to GitHub and the schedule activates immediately. Trigger manually via
`Actions → Weekly Dubai Real Estate Pipeline → Run workflow`.

### Local cron (Linux/macOS)
```bash
chmod +x schedule_cron.sh
./schedule_cron.sh
```
This registers: `0 3 * * 0` (Sunday 03:00 AM local time).

---

## Data Format

Each listing record:

```json
{
  "title": "2BR Apartment Marina View",
  "area": "Dubai Marina",
  "property_type": "Apartment",
  "bedrooms": 2,
  "bathrooms": 2,
  "size_sqft": 1100,
  "price": 2100000,
  "price_per_sqft": 1909,
  "building_name": "Marina Crown",
  "developer": null,
  "floor_number": null,
  "source": "PropertyFinder",
  "url": "https://www.propertyfinder.ae/en/...",
  "scraped_at": "2026-03-15"
}
```

**Missing fields are stored as `null` — never estimated.**

---

## Validation Rules

Records are rejected if:
- `price` is missing or zero
- `area` is missing

Duplicate URLs are counted once.

---

## Analytics Output

Weekly analytics JSON (`data/processed/analytics_YYYY-MM-DD.json`):

```json
{
  "date": "2026-03-15",
  "total_listings": 1842,
  "areas": {
    "Dubai Marina": {
      "listings_count": 185,
      "avg_price": 2100000,
      "avg_price_per_sqft": 1850,
      "bedroom_distribution": {"1": 42, "2": 89, "3": 54},
      "sources": {"PropertyFinder": 71, "Bayut": 68, "Dubizzle": 46}
    }
  },
  "week_on_week": {
    "Dubai Marina": {
      "current_week": 185,
      "previous_week": 172,
      "change": 13,
      "change_pct": 7.6
    }
  }
}
```

---

## Sample Report Output

```
### Dubai Marina

| Metric             | Value           |
|--------------------|-----------------|
| Listings           | 185             |
| Average price      | AED 2.10M       |
| Avg price/sqft     | AED 1,850/sqft  |
| Week-on-week change| +13 (+7.6%)     |
```

---

## Scraping Strategy

The system is designed to safely collect **10,000+ listings weekly**.

| Technique | Implementation |
|-----------|---------------|
| User-agent rotation | 5 real browser UA strings, randomised per request |
| Request delays | 3–7 seconds random delay between pages |
| Retry with backoff | 3 retries, 2× backoff, on 429/5xx |
| robots.txt respect | Checked before every URL |
| Rate limiting | Max 1 request per 3 seconds per scraper |

---

## V1 Success Criteria

- [x] Weekly pipeline runs automatically
- [x] Data collected from 3 listing websites
- [x] Government datasets downloaded
- [x] Weekly analytics generated
- [x] No fabricated data used

---

## Monetisation Potential (Proptech)

| Model | Description |
|-------|-------------|
| Market intelligence dashboards | SaaS subscriptions for developers/brokers |
| Investment analytics tools | ROI analysis, area comparison reports |
| Developer market reports | Custom area reports for property developers |
| Broker lead generation | Qualified buyer/seller matching |
| Property valuation tools | Data-backed AVM for lenders and buyers |

---

## Scaling Plan (Future Versions)

- Expand to all UAE emirates
- Add Dubai Land Department transaction data (when available via open data)
- Machine learning price prediction
- Investment opportunity scoring
- REST API for third-party integrations
- Interactive dashboards (Grafana / Metabase)

---

## Legal Notice

This platform scrapes publicly visible listing data in accordance with
each website's robots.txt and at a polite request rate. Users are responsible
for ensuring compliance with each source's terms of service before commercial
use.

Government datasets are used under Dubai Open Data licence terms.
>>>>>>> fc0932babea4050510c0efdd24e21c142329beec
