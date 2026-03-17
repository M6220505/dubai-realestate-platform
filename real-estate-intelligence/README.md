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

- Dubai Marina
- Downtown Dubai
- Palm Jumeirah
- Business Bay
- Jumeirah Beach Residence

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
