"""
Rental market analytics derived from Dubai Land Department rent contract data.

Input DataFrame columns expected (from Rent_Contracts CSV):
    contract_id, area_name_en, annual_amount, actual_area, property_type_en, ...

All computations are derived from real data only — no estimates injected.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Minimum records per area to be included in summary (filters noise)
MIN_CONTRACTS = 10


def compute_rent_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-area rental statistics.

    Returns a DataFrame sorted by rent_per_sqm descending with columns:
        area_name_en, contracts, avg_rent, median_rent, min_rent, max_rent,
        avg_area_sqm, rent_per_sqm
    """
    df = df.copy()

    # Keep only rows with valid financials
    df = df[df["annual_amount"].notna() & (df["annual_amount"] > 0)]
    df = df[df["actual_area"].notna() & (df["actual_area"] > 0)]

    df["rent_per_sqm"] = df["annual_amount"] / df["actual_area"]

    summary = (
        df.groupby("area_name_en")
        .agg(
            contracts=("contract_id", "count"),
            avg_rent=("annual_amount", "mean"),
            median_rent=("annual_amount", "median"),
            min_rent=("annual_amount", "min"),
            max_rent=("annual_amount", "max"),
            avg_area_sqm=("actual_area", "mean"),
            rent_per_sqm=("rent_per_sqm", "mean"),
        )
        .reset_index()
    )

    # Remove low-volume areas that skew rankings
    summary = summary[summary["contracts"] >= MIN_CONTRACTS]

    # Round for readability
    for col in ["avg_rent", "median_rent", "min_rent", "max_rent", "avg_area_sqm", "rent_per_sqm"]:
        summary[col] = summary[col].round(0)

    summary = summary.sort_values("rent_per_sqm", ascending=False).reset_index(drop=True)

    logger.info(
        "Rent stats computed: %d areas, %d total contracts",
        len(summary),
        int(summary["contracts"].sum()),
    )
    return summary


def compute_property_type_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns contracts and average rent broken down by property type.
    """
    df = df[df["annual_amount"].notna() & (df["annual_amount"] > 0)]

    if "property_type_en" not in df.columns:
        logger.warning("Column 'property_type_en' not found — skipping type breakdown")
        return pd.DataFrame()

    breakdown = (
        df.groupby("property_type_en")
        .agg(
            contracts=("contract_id", "count"),
            avg_rent=("annual_amount", "mean"),
            median_rent=("annual_amount", "median"),
        )
        .reset_index()
        .sort_values("contracts", ascending=False)
        .reset_index(drop=True)
    )

    breakdown["avg_rent"] = breakdown["avg_rent"].round(0)
    breakdown["median_rent"] = breakdown["median_rent"].round(0)

    return breakdown
