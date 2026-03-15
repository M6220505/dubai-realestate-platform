# Dubai Real Estate Intelligence Platform

Automated data pipeline for collecting, processing, and analyzing Dubai property listings from major portals (PropertyFinder, Bayut, Dubizzle).

## Features
- Automated web scraping
- Weekly data pipelines
- Area-level market analysis
- Price trend insights

## Tech Stack
- Python
- Playwright
- Pandas
- PostgreSQL

## Project Structure
real-estate-intelligence/
  pipeline/
  scrapers/
  analytics/
  tests/

## Setup
pip install -r requirements.txt
playwright install
python pipeline/run_pipeline.py


## Example Output

Average price per sqft by area

| Area | Avg Price/sqft |
|-----|-----|
| Dubai Marina | 1,850 AED |
| Downtown Dubai | 2,450 AED |
| JVC | 1,050 AED |

