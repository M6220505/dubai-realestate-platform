"""
Dubai Open Data Portal downloader.

Downloads free government datasets from data.dubai.gov.ae (Dubai Data and
Statistics Establishment / Dubai Land Department).

The portal uses a CKAN-compatible API. We query the catalogue, find relevant
real-estate datasets, and download them to datasets/government/.

Real data only — nothing is estimated or fabricated.
"""

import logging
import json
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Dubai Open Data portal (CKAN-based)
PORTAL_BASE = "https://www.dubaipulse.gov.ae"
CKAN_API = f"{PORTAL_BASE}/api/3/action"

# Alternative direct portal
DATA_DUBAI_BASE = "https://data.dubai.gov.ae"
DATA_DUBAI_CKAN = f"{DATA_DUBAI_BASE}/api/3/action"

# Keywords to search for relevant datasets
SEARCH_KEYWORDS = [
    "real estate",
    "residential sale index",
    "property statistics",
    "rental market",
    "building permits",
    "land use",
    "population housing",
    "construction activity",
    "market indicators",
]

TARGET_DATASETS = [
    "Residential Sale Index",
    "Real Estate Market Indicators",
    "Population and Housing Statistics",
    "Construction Activity Statistics",
    "Land Use Data",
    "Building Permits Data",
    "Rental Market Indicators",
]


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


class GovernmentDataDownloader:
    """
    Downloads Dubai government open datasets related to real estate.
    Tries multiple portal endpoints to maximise dataset coverage.
    """

    PORTALS = [
        {"name": "Dubai Pulse", "ckan": DATA_DUBAI_CKAN},
        {"name": "Dubai Open Data", "ckan": f"https://opendata.dubai.gov.ae/api/3/action"},
    ]

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = _make_session()
        self.manifest: list[dict] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def download_all(self) -> list[Path]:
        """
        Search all portals for real-estate datasets and download them.
        Returns list of paths to downloaded files.
        """
        downloaded: list[Path] = []

        for keyword in SEARCH_KEYWORDS:
            results = self._search_datasets(keyword)
            for dataset_meta in results:
                files = self._download_dataset(dataset_meta)
                downloaded.extend(files)

        # Save manifest
        manifest_path = self.output_dir / f"manifest_{date.today().isoformat()}.json"
        with open(manifest_path, "w") as fh:
            json.dump(self.manifest, fh, indent=2)
        logger.info("Manifest saved → %s", manifest_path)

        return downloaded

    # ------------------------------------------------------------------
    # CKAN search
    # ------------------------------------------------------------------

    def _search_datasets(self, keyword: str) -> list[dict]:
        """Query CKAN package_search endpoint."""
        results: list[dict] = []
        for portal in self.PORTALS:
            url = f"{portal['ckan']}/package_search"
            params = {"q": keyword, "rows": 20}
            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                packages = data.get("result", {}).get("results", [])
                for pkg in packages:
                    pkg["_portal"] = portal["name"]
                    pkg["_portal_ckan"] = portal["ckan"]
                results.extend(packages)
                logger.debug("[%s] keyword '%s' → %d datasets", portal["name"], keyword, len(packages))
            except Exception as exc:
                logger.warning("[%s] Search failed for '%s': %s", portal["name"], keyword, exc)
        return results

    # ------------------------------------------------------------------
    # Download a single dataset (all resources)
    # ------------------------------------------------------------------

    def _download_dataset(self, dataset_meta: dict) -> list[Path]:
        downloaded: list[Path] = []
        name = dataset_meta.get("title") or dataset_meta.get("name", "unknown")
        resources = dataset_meta.get("resources", [])

        for resource in resources:
            fmt = (resource.get("format") or "").upper()
            if fmt not in ("CSV", "XLSX", "XLS", "JSON", ""):
                continue  # skip shapefiles, PDFs, etc. for now

            url = resource.get("url")
            if not url:
                continue

            path = self._fetch_resource(url, name, fmt)
            if path:
                downloaded.append(path)
                self.manifest.append({
                    "dataset": name,
                    "resource_url": url,
                    "format": fmt,
                    "local_path": str(path),
                    "downloaded_at": date.today().isoformat(),
                })

        return downloaded

    def _fetch_resource(self, url: str, dataset_name: str, fmt: str) -> Optional[Path]:
        """Download a single resource file."""
        safe_name = "".join(c if c.isalnum() else "_" for c in dataset_name)[:60]
        ext = fmt.lower() if fmt else "dat"
        filename = self.output_dir / f"{safe_name}_{date.today().isoformat()}.{ext}"

        # Skip if already downloaded today
        if filename.exists():
            logger.debug("Already exists: %s", filename)
            return filename

        try:
            logger.info("Downloading: %s → %s", url, filename.name)
            resp = self.session.get(url, timeout=60, stream=True)
            resp.raise_for_status()
            with open(filename, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)
            logger.info("Saved %s (%.1f KB)", filename.name, filename.stat().st_size / 1024)
            return filename
        except Exception as exc:
            logger.error("Failed to download %s: %s", url, exc)
            return None
