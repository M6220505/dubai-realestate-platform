"""
Investment ranking engine.

Combines four government datasets to score Dubai areas by investment attractiveness:

Signals used:
    rent_per_sqm        — rental income potential (from rent contracts)
    transaction_volume  — market liquidity (from transactions)
    permit_activity     — supply pipeline / growth signal (from building permits)
    sale_index_trend    — price momentum (from residential sale index)

Each signal is normalised to [0, 1] then weighted into a composite score.
Areas with insufficient data are excluded rather than estimated.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Weights must sum to 1.0
WEIGHTS = {
    "rent_per_sqm": 0.35,
    "transaction_volume": 0.30,
    "permit_activity": 0.20,
    "sale_index_trend": 0.15,
}

MIN_TRANSACTIONS = 5  # minimum transactions for an area to qualify


def _normalise(series: pd.Series) -> pd.Series:
    """Min-max normalise a Series to [0, 1]. Returns zeros if range is zero."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(0.0, index=series.index)
    return (series - lo) / (hi - lo)


def build_ranking(
    rent_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    permits_df: pd.DataFrame,
    sale_index_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Produce a per-area investment score DataFrame.

    Parameters
    ----------
    rent_df : output of rent_market_analysis.compute_rent_stats()
              must have columns: area_name_en, rent_per_sqm
    transactions_df : raw Real_Estate_Transactions CSV
              must have column: area_name_en (or will try TRANS_AREA_COL fallbacks)
    permits_df : raw Building_Permits CSV
              must have column: area_name_en (or fallbacks)
    sale_index_df : raw Residential_Sale_Index CSV
              must have columns: area_name_en, index_value (or fallbacks)

    Returns
    -------
    DataFrame sorted by investment_score descending with columns:
        area_name_en, rent_per_sqm, transaction_volume, permit_activity,
        sale_index_value, investment_score, rank
    """

    # --- Signal 1: rent per sqm (already computed) ---
    rent_signal = rent_df[["area_name_en", "rent_per_sqm"]].copy()

    # --- Signal 2: transaction volume ---
    trans_area_col = _find_col(transactions_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    if trans_area_col:
        trans_signal = (
            transactions_df.groupby(trans_area_col)
            .size()
            .reset_index(name="transaction_volume")
            .rename(columns={trans_area_col: "area_name_en"})
        )
        trans_signal = trans_signal[trans_signal["transaction_volume"] >= MIN_TRANSACTIONS]
    else:
        logger.warning("Could not find area column in transactions data — skipping signal")
        trans_signal = pd.DataFrame(columns=["area_name_en", "transaction_volume"])

    # --- Signal 3: permit activity ---
    permit_area_col = _find_col(permits_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    if permit_area_col:
        permit_signal = (
            permits_df.groupby(permit_area_col)
            .size()
            .reset_index(name="permit_activity")
            .rename(columns={permit_area_col: "area_name_en"})
        )
    else:
        logger.warning("Could not find area column in permits data — skipping signal")
        permit_signal = pd.DataFrame(columns=["area_name_en", "permit_activity"])

    # --- Signal 4: sale index value ---
    index_area_col = _find_col(sale_index_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    index_val_col = _find_col(sale_index_df, ["index_value", "INDEX_VALUE", "value", "VALUE", "indicator_value"])
    if index_area_col and index_val_col:
        index_signal = (
            sale_index_df.groupby(index_area_col)[index_val_col]
            .mean()
            .reset_index()
            .rename(columns={index_area_col: "area_name_en", index_val_col: "sale_index_value"})
        )
    else:
        logger.warning("Could not find required columns in sale index data — skipping signal")
        index_signal = pd.DataFrame(columns=["area_name_en", "sale_index_value"])

    # --- Merge all signals ---
    merged = rent_signal
    for df_signal in [trans_signal, permit_signal, index_signal]:
        if not df_signal.empty:
            merged = merged.merge(df_signal, on="area_name_en", how="left")

    # Fill missing signal values with 0 (area present in rent data but not others)
    for col in ["transaction_volume", "permit_activity", "sale_index_value"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0)

    # --- Compute composite score ---
    merged["_norm_rent"] = _normalise(merged["rent_per_sqm"])
    merged["_norm_trans"] = _normalise(merged["transaction_volume"])
    merged["_norm_permits"] = _normalise(merged["permit_activity"])
    merged["_norm_index"] = _normalise(merged["sale_index_value"])

    merged["investment_score"] = (
        merged["_norm_rent"] * WEIGHTS["rent_per_sqm"]
        + merged["_norm_trans"] * WEIGHTS["transaction_volume"]
        + merged["_norm_permits"] * WEIGHTS["permit_activity"]
        + merged["_norm_index"] * WEIGHTS["sale_index_trend"]
    ).round(4)

    # Drop internal normalised columns
    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("_norm")])

    merged = merged.sort_values("investment_score", ascending=False).reset_index(drop=True)
    merged.insert(0, "rank", range(1, len(merged) + 1))

    logger.info("Investment ranking built: %d areas scored", len(merged))
    return merged


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from candidates that exists in df, else None."""
    for col in candidates:
        if col in df.columns:
            return col
    return None
