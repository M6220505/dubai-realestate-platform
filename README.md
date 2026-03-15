# Dubai Real Estate Intelligence Platform

Automated data pipeline for collecting, processing, and analyzing Dubai property listings from major portals (PropertyFinder, Bayut, Dubizzle).

## Features

- Automated web scraping
- Weekly data pipelines
- Area-level market analysis
- Price trend insights
  



## Data Pipeline Architecture

1. Property portals (PropertyFinder, Bayut, Dubizzle)
2. Playwright scrapers collect listing data
3. Raw listings are stored as JSON/CSV
4. Pandas pipeline cleans and normalizes the data
5. PostgreSQL stores structured records
6. Analytics layer calculates area trends and price per sqft


## Tech Stack

- Python
- Playwright
- Pandas
- PostgreSQL



## Project Structure

real-estate-intelligence/
├── pipeline/
├── scrapers/
├── analytics/
└── tests/



## Setup

cd real-estate-intelligence
pip install -r requirements.txt
playwright install
python -m pipeline.weekly_pipeline --dry-run


## Example Output

Average price per sqft by area
| Area | Avg Price/sqft |
|-----|-----|
| Dubai Marina | 1,850 AED |
| Downtown Dubai | 2,450 AED |
| JVC | 1,050 AED |

## Sample Data

Example output datasets are provided in:

data-samples/

- sample_listings.csv
- area_price_summary.csv
