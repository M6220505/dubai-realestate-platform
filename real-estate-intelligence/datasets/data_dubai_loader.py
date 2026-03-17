# datasets/data_dubai_loader.py
"""
Government data loader — 4-tier fallback strategy:
1. data.dubai API / CSV
2. DLD monthly PDF reports (pdfplumber)
3. Dubai Statistics Center
4. Log no_data and skip
"""
import csv
import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DATA_RAW_DIR, GOV_SOURCES

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "DubaiIntel/1.0 Research Bot (non-commercial)"}
TIMEOUT = 20


class GovernmentDataLoader:

    def __init__(self):
        self.today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        """Try each tier in order. Return status dict."""
        logger.info("[GOV] Starting government data fetch")

        # Tier 1: data.dubai
        result = self._try_data_dubai()
        if result:
            return {"status": "success", "tier": 1, "source": "data.dubai", "records": result}

        # Tier 2: DLD PDF reports
        result = self._try_dld_pdf()
        if result:
            return {"status": "success", "tier": 2, "source": "dld_pdf", "records": result}

        # Tier 3: DSC
        result = self._try_dsc()
        if result:
            return {"status": "success", "tier": 3, "source": "dsc", "records": result}

        # Tier 4: No data
        logger.warning("[GOV] All government data sources unavailable this week.")
        self._log_no_data()
        return {"status": "no_data", "tier": 4, "source": None, "records": []}

    # ── Tier 1: data.dubai ───────────────────────────────────
    def _try_data_dubai(self):
        try:
            logger.info("[GOV] Tier 1: Trying data.dubai portal...")
            # Try known dataset endpoints
            endpoints = [
                "https://data.dubai.gov.ae/api/3/action/datastore_search?resource_id=real-estate-transactions&limit=1000",
                "https://data.dubai.gov.ae/dataset/real-estate-transactions",
            ]
            for url in endpoints:
                try:
                    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
                    if resp.status_code == 200:
                        data = resp.json()
                        records = data.get("result", {}).get("records", [])
                        if records:
                            logger.info(f"[GOV] Tier 1 success: {len(records)} records")
                            self._save_csv(records, "dld_transactions")
                            return records
                except Exception as e:
                    logger.warning(f"[GOV] Tier 1 endpoint failed ({url}): {e}")
                    continue
        except Exception as e:
            logger.warning(f"[GOV] Tier 1 failed: {e}")
        return None

    # ── Tier 2: DLD PDF ──────────────────────────────────────
    def _try_dld_pdf(self):
        try:
            import pdfplumber
            logger.info("[GOV] Tier 2: Trying DLD PDF reports...")

            pdf_urls = [
                "https://dubailand.gov.ae/media/reports/real-estate-market-report.pdf",
                "https://dubailand.gov.ae/en/open-data/real-estate-reports/",
            ]

            for url in pdf_urls:
                try:
                    resp = requests.get(url, headers=HEADERS, timeout=30)
                    if resp.status_code == 200 and b"%PDF" in resp.content[:10]:
                        pdf_path = DATA_RAW_DIR / f"dld_report_{self.today}.pdf"
                        pdf_path.write_bytes(resp.content)

                        records = []
                        with pdfplumber.open(pdf_path) as pdf:
                            for page in pdf.pages:
                                tables = page.extract_tables()
                                for table in tables:
                                    if table and len(table) > 1:
                                        headers = table[0]
                                        for row in table[1:]:
                                            if row:
                                                records.append(dict(zip(headers, row)))

                        if records:
                            logger.info(f"[GOV] Tier 2 success: {len(records)} rows from PDF")
                            self._save_csv(records, "dld_transactions")
                            return records
                except Exception as e:
                    logger.warning(f"[GOV] Tier 2 URL failed: {e}")
                    continue
        except ImportError:
            logger.warning("[GOV] pdfplumber not installed — skipping Tier 2")
        except Exception as e:
            logger.warning(f"[GOV] Tier 2 failed: {e}")
        return None

    # ── Tier 3: DSC ──────────────────────────────────────────
    def _try_dsc(self):
        try:
            logger.info("[GOV] Tier 3: Trying Dubai Statistics Center...")
            url = "https://www.dsc.gov.ae/en-us/Themes/Pages/Real-Estate.aspx"
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                logger.info("[GOV] Tier 3: DSC page accessible but structured data not yet parsed")
                # Future: parse DSC datasets
        except Exception as e:
            logger.warning(f"[GOV] Tier 3 failed: {e}")
        return None

    # ── Helpers ──────────────────────────────────────────────
    def _save_csv(self, records: list[dict], name: str):
        if not records:
            return
        path = DATA_RAW_DIR / f"{name}_{self.today}.csv"
        keys = records[0].keys() if records else []
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"[GOV] Saved {len(records)} records → {path}")

    def _log_no_data(self):
        path = DATA_RAW_DIR / f"gov_no_data_{self.today}.json"
        with open(path, "w") as f:
            json.dump({
                "status": "no_data",
                "date":   self.today,
                "reason": "All government data tiers unavailable"
            }, f, indent=2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    loader = GovernmentDataLoader()
    result = loader.run()
    print(f"Status: {result['status']} | Tier: {result['tier']} | Records: {len(result['records'])}")
