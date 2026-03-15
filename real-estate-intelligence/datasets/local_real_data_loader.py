"""
Loader for locally stored Dubai Open Data CSV exports.

Expected files in <project_root>/data/:
    Rent_Contracts_2026-03-15.csv
    Building_Permits_2026-03-15.csv
    Residential_Sale_Index_2026-03-15.csv
    Real_Estate_Transactions_2026-03-15.csv

Drop newer exports into data/ and update FILES below to point to them.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

# Map logical name → filename.  Update filenames when new exports are downloaded.
FILES = {
    "rent_contracts": "Rent_Contracts_2026-03-15.csv",
    "building_permits": "Building_Permits_2026-03-15.csv",
    "sale_index": "Residential_Sale_Index_2026-03-15.csv",
    "transactions_agg": "Real_Estate_Transactions_2026-03-15.csv",
}


def load_all(data_dir: Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """
    Load all four government CSV exports.

    Returns a dict keyed by logical name.
    Raises FileNotFoundError if any expected file is missing.
    """
    datasets: dict[str, pd.DataFrame] = {}
    for key, filename in FILES.items():
        path = data_dir / filename
        if not path.exists():
            raise FileNotFoundError(
                f"Missing dataset '{key}': expected at {path}\n"
                f"Download it from the Dubai Open Data portal and place it in {data_dir}"
            )
        logger.info("Loading %s → %s", key, path.name)
        datasets[key] = pd.read_csv(path, low_memory=False)
        logger.info("  %d rows loaded", len(datasets[key]))
    return datasets


def load_available(data_dir: Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """
    Like load_all() but silently skips missing files instead of raising.
    Useful in CI / dry-run contexts where real CSVs are not present.
    """
    datasets: dict[str, pd.DataFrame] = {}
    for key, filename in FILES.items():
        path = data_dir / filename
        if path.exists():
            logger.info("Loading %s → %s", key, path.name)
            datasets[key] = pd.read_csv(path, low_memory=False)
            logger.info("  %d rows loaded", len(datasets[key]))
        else:
            logger.warning("Skipping %s — file not found: %s", key, path)
    return datasets
