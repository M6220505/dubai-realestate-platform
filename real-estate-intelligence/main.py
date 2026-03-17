"""
Dubai Real Estate Intelligence — Single-file runner
====================================================
يجمع هذا الملف كل منطق المشروع في مكان واحد.

المتطلبات:
    pip install pandas streamlit plotly

الاستخدام:
    # تشغيل التحليلات وإنتاج ملفات الإخراج
    python main.py

    # تشغيل لوحة التحكم التفاعلية (بعد تشغيل التحليلات أولاً)
    python main.py --dashboard

    # تحديد مسار مختلف للبيانات
    python main.py --data-dir /path/to/csv/files

ضع ملفات CSV في مجلد data/ بجانب هذا الملف:
    Rent_Contracts_2026-03-15.csv
    Building_Permits_2026-03-15.csv
    Residential_Sale_Index_2026-03-15.csv
    Real_Estate_Transactions_2026-03-15.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dubai-intel")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
REPORTS_DIR = BASE_DIR / "reports"

# CSV filenames — update if you download newer exports
CSV_FILES = {
    "rent_contracts":   "Rent_Contracts_2026-03-15.csv",
    "building_permits": "Building_Permits_2026-03-15.csv",
    "sale_index":       "Residential_Sale_Index_2026-03-15.csv",
    "transactions_agg": "Real_Estate_Transactions_2026-03-15.csv",
}

# Investment signal weights (must sum to 1.0)
WEIGHTS = {
    "rent_per_sqm":     0.35,
    "transaction_volume": 0.30,
    "permit_activity":  0.20,
    "sale_index_trend": 0.15,
}

MIN_CONTRACTS = 10   # minimum contracts for an area to appear in rent rankings
MIN_TRANSACTIONS = 5 # minimum transactions for investment ranking


# ===========================================================================
# 1. DATA LOADER
# ===========================================================================

def load_available(data_dir: Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Load whichever CSV files exist. Warns and skips missing ones."""
    datasets: dict[str, pd.DataFrame] = {}
    for key, filename in CSV_FILES.items():
        path = data_dir / filename
        if path.exists():
            logger.info("Loading %-20s ← %s", key, path.name)
            datasets[key] = pd.read_csv(path, low_memory=False)
            logger.info("  %d rows", len(datasets[key]))
        else:
            logger.warning("Missing: %s  (expected at %s)", filename, path)
    return datasets


# ===========================================================================
# 2. RENT MARKET ANALYSIS
# ===========================================================================

def compute_rent_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-area rental statistics from DLD rent contracts.

    Required columns: contract_id, area_name_en, annual_amount, actual_area
    """
    df = df.copy()
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

    summary = summary[summary["contracts"] >= MIN_CONTRACTS]
    for col in ["avg_rent", "median_rent", "min_rent", "max_rent", "avg_area_sqm", "rent_per_sqm"]:
        summary[col] = summary[col].round(0)

    summary = summary.sort_values("rent_per_sqm", ascending=False).reset_index(drop=True)
    logger.info("Rent stats: %d areas, %d total contracts", len(summary), int(summary["contracts"].sum()))
    return summary


def compute_property_type_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Contracts and average rent by property type."""
    df = df[df["annual_amount"].notna() & (df["annual_amount"] > 0)]
    if "property_type_en" not in df.columns:
        return pd.DataFrame()
    breakdown = (
        df.groupby("property_type_en")
        .agg(contracts=("contract_id", "count"), avg_rent=("annual_amount", "mean"), median_rent=("annual_amount", "median"))
        .reset_index()
        .sort_values("contracts", ascending=False)
        .reset_index(drop=True)
    )
    breakdown["avg_rent"] = breakdown["avg_rent"].round(0)
    breakdown["median_rent"] = breakdown["median_rent"].round(0)
    return breakdown


# ===========================================================================
# 3. INVESTMENT RANKING ENGINE
# ===========================================================================

def _normalise(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(0.0, index=series.index)
    return (series - lo) / (hi - lo)


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def build_investment_ranking(
    rent_summary: pd.DataFrame,
    transactions_df: pd.DataFrame,
    permits_df: pd.DataFrame,
    sale_index_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Composite investment score combining four signals.
    Returns DataFrame sorted by investment_score descending with a rank column.
    """
    # Signal 1: rent per sqm (already in rent_summary)
    merged = rent_summary[["area_name_en", "rent_per_sqm"]].copy()

    # Signal 2: transaction volume
    trans_col = _find_col(transactions_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    if trans_col and not transactions_df.empty:
        trans_signal = (
            transactions_df.groupby(trans_col).size()
            .reset_index(name="transaction_volume")
            .rename(columns={trans_col: "area_name_en"})
        )
        trans_signal = trans_signal[trans_signal["transaction_volume"] >= MIN_TRANSACTIONS]
        merged = merged.merge(trans_signal, on="area_name_en", how="left")
    else:
        merged["transaction_volume"] = 0.0

    # Signal 3: permit activity
    permit_col = _find_col(permits_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    if permit_col and not permits_df.empty:
        permit_signal = (
            permits_df.groupby(permit_col).size()
            .reset_index(name="permit_activity")
            .rename(columns={permit_col: "area_name_en"})
        )
        merged = merged.merge(permit_signal, on="area_name_en", how="left")
    else:
        merged["permit_activity"] = 0.0

    # Signal 4: sale index
    index_area = _find_col(sale_index_df, ["area_name_en", "AREA_EN", "area_en", "AREA"])
    index_val  = _find_col(sale_index_df, ["index_value", "INDEX_VALUE", "value", "VALUE", "indicator_value"])
    if index_area and index_val and not sale_index_df.empty:
        index_signal = (
            sale_index_df.groupby(index_area)[index_val].mean()
            .reset_index()
            .rename(columns={index_area: "area_name_en", index_val: "sale_index_value"})
        )
        merged = merged.merge(index_signal, on="area_name_en", how="left")
    else:
        merged["sale_index_value"] = 0.0

    for col in ["transaction_volume", "permit_activity", "sale_index_value"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0)

    merged["investment_score"] = (
        _normalise(merged["rent_per_sqm"])     * WEIGHTS["rent_per_sqm"]
        + _normalise(merged["transaction_volume"]) * WEIGHTS["transaction_volume"]
        + _normalise(merged["permit_activity"])    * WEIGHTS["permit_activity"]
        + _normalise(merged["sale_index_value"])   * WEIGHTS["sale_index_trend"]
    ).round(4)

    merged = merged.sort_values("investment_score", ascending=False).reset_index(drop=True)
    merged.insert(0, "rank", range(1, len(merged) + 1))
    logger.info("Investment ranking: %d areas scored", len(merged))
    return merged


# ===========================================================================
# 4. MARKET REPORT (Markdown)
# ===========================================================================

def _fmt(value, suffix="") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    if isinstance(value, (int, float)):
        if value >= 1_000_000:
            return f"AED {value/1_000_000:.2f}M{suffix}"
        if value >= 1_000:
            return f"AED {value/1_000:.0f}K{suffix}"
        return f"AED {value:,.0f}{suffix}"
    return str(value)


def generate_markdown_report(
    rent_summary: pd.DataFrame,
    rankings: pd.DataFrame,
    report_date: str,
    reports_dir: Path,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    out = reports_dir / f"market_report_{report_date}.md"

    top_rent = rent_summary.head(20)
    top_rank = rankings.head(20)

    lines = [
        "# Dubai Real Estate Intelligence Report",
        f"**Date: {report_date}**",
        "",
        "> Source: Dubai Land Department — Dubai Open Data Portal",
        "> All figures derived from real DLD records. No estimated values.",
        "",
        "---",
        "",
        "## Top 20 Areas by Rent per sqm",
        "",
        "| Area | Contracts | Avg Rent (AED) | Median Rent | Rent/sqm |",
        "|------|-----------|---------------|-------------|----------|",
    ]
    for _, row in top_rent.iterrows():
        lines.append(
            f"| {row['area_name_en']} | {int(row['contracts']):,} | "
            f"{_fmt(row['avg_rent'])} | {_fmt(row['median_rent'])} | "
            f"AED {int(row['rent_per_sqm']):,}/sqm |"
        )

    lines += [
        "",
        "---",
        "",
        "## Top 20 Investment Rankings",
        "",
        f"Weights: Rent yield {WEIGHTS['rent_per_sqm']*100:.0f}% · "
        f"Transactions {WEIGHTS['transaction_volume']*100:.0f}% · "
        f"Permits {WEIGHTS['permit_activity']*100:.0f}% · "
        f"Sale index {WEIGHTS['sale_index_trend']*100:.0f}%",
        "",
        "| Rank | Area | Score | Rent/sqm | Transactions | Permits |",
        "|------|------|-------|----------|--------------|---------|",
    ]
    for _, row in top_rank.iterrows():
        lines.append(
            f"| {int(row['rank'])} | {row['area_name_en']} | {row['investment_score']:.4f} | "
            f"AED {int(row['rent_per_sqm']):,} | "
            f"{int(row.get('transaction_volume', 0)):,} | "
            f"{int(row.get('permit_activity', 0)):,} |"
        )

    lines += [
        "",
        "---",
        "*Generated by Dubai Real Estate Intelligence Platform*",
    ]

    out.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report → %s", out)
    return out


# ===========================================================================
# 5. PIPELINE ORCHESTRATOR
# ===========================================================================

def run_analytics(data_dir: Path = DATA_DIR) -> None:
    """Load CSVs, run all analytics, write outputs."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    data = load_available(data_dir)
    if not data:
        logger.error("No CSV files found in %s", data_dir)
        logger.error("Download files from https://data.dubai.gov.ae and place them in data/")
        sys.exit(1)

    from datetime import date
    today = date.today().isoformat()

    rent_summary = None
    rankings = None

    # --- Rent analysis ---
    if "rent_contracts" in data:
        logger.info("-" * 50)
        logger.info("Running rental market analysis…")
        rent_summary = compute_rent_stats(data["rent_contracts"])
        out = OUTPUTS_DIR / "rent_area_rankings.csv"
        rent_summary.to_csv(out, index=False)
        logger.info("Saved → %s", out)

        # Property type breakdown
        if "property_type_en" in data["rent_contracts"].columns:
            ptype = compute_property_type_breakdown(data["rent_contracts"])
            if not ptype.empty:
                ptype.to_csv(OUTPUTS_DIR / "rent_by_property_type.csv", index=False)

    # --- Investment ranking ---
    if rent_summary is not None:
        logger.info("-" * 50)
        logger.info("Building investment rankings…")
        empty = pd.DataFrame()
        rankings = build_investment_ranking(
            rent_summary=rent_summary,
            transactions_df=data.get("transactions_agg", empty),
            permits_df=data.get("building_permits", empty),
            sale_index_df=data.get("sale_index", empty),
        )
        out = OUTPUTS_DIR / "investment_rankings.csv"
        rankings.to_csv(out, index=False)
        logger.info("Saved → %s", out)

    # --- Markdown report ---
    if rent_summary is not None and rankings is not None:
        logger.info("-" * 50)
        logger.info("Generating markdown report…")
        generate_markdown_report(rent_summary, rankings, today, REPORTS_DIR)

    # --- Summary printout ---
    logger.info("=" * 50)
    logger.info("DONE. Files written to outputs/")
    if rent_summary is not None:
        logger.info("")
        logger.info("TOP 10 AREAS BY RENT/SQM:")
        logger.info("%-35s %10s %12s", "Area", "Contracts", "AED/sqm")
        logger.info("-" * 60)
        for _, row in rent_summary.head(10).iterrows():
            logger.info("%-35s %10,d %12,d", row["area_name_en"], int(row["contracts"]), int(row["rent_per_sqm"]))

    if rankings is not None:
        logger.info("")
        logger.info("TOP 10 INVESTMENT RANKINGS:")
        logger.info("%-4s %-35s %8s", "Rank", "Area", "Score")
        logger.info("-" * 50)
        for _, row in rankings.head(10).iterrows():
            logger.info("%-4d %-35s %8.4f", int(row["rank"]), row["area_name_en"], row["investment_score"])


# ===========================================================================
# 6. STREAMLIT DASHBOARD (inline)
# ===========================================================================

DASHBOARD_CODE = '''
"""Dubai Real Estate Intelligence — Streamlit Dashboard"""
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"
RENT_FILE    = OUTPUTS_DIR / "rent_area_rankings.csv"
RANK_FILE    = OUTPUTS_DIR / "investment_rankings.csv"

st.set_page_config(page_title="Dubai RE Intelligence", page_icon="🏙️", layout="wide")
st.title("Dubai Real Estate Intelligence")
st.caption("Dubai Land Department · Open Data Portal")

def load(path, label):
    if not path.exists():
        st.error(f"**{label}** not found. Run `python main.py` first.")
        return None
    return pd.read_csv(path)

# ── Investment Rankings ──────────────────────────────────────────────────────
st.header("Investment Rankings")
rankings = load(RANK_FILE, "investment_rankings.csv")
if rankings is not None:
    c1, c2 = st.columns([1, 3])
    n = c1.slider("Top N areas", 5, min(50, len(rankings)), 15)
    c2.metric("Total areas ranked", len(rankings))
    top = rankings.head(n)
    cols = [c for c in ["rank","area_name_en","investment_score","rent_per_sqm","transaction_volume","permit_activity"] if c in top.columns]
    st.dataframe(top[cols], hide_index=True, use_container_width=True)
    st.plotly_chart(
        px.bar(top, x="area_name_en", y="investment_score", color="investment_score",
               color_continuous_scale="Viridis",
               title=f"Top {n} Areas — Investment Score",
               labels={"area_name_en":"Area","investment_score":"Score"})
        .update_layout(xaxis_tickangle=-45),
        use_container_width=True,
    )
    if "transaction_volume" in top.columns:
        st.plotly_chart(
            px.scatter(top, x="transaction_volume", y="rent_per_sqm",
                       size="investment_score", color="investment_score",
                       hover_name="area_name_en", color_continuous_scale="RdYlGn",
                       title="Rent Yield vs Market Liquidity",
                       labels={"transaction_volume":"Transactions","rent_per_sqm":"Rent/sqm (AED)"})
            .update_layout(xaxis_tickangle=-45),
            use_container_width=True,
        )

st.divider()

# ── Rental Market ────────────────────────────────────────────────────────────
st.header("Rental Market")
rent = load(RENT_FILE, "rent_area_rankings.csv")
if rent is not None:
    n2 = st.slider("Top N areas (rent)", 5, min(50, len(rent)), 20)
    c1, c2, c3 = st.columns(3)
    c1.metric("Areas tracked", len(rent))
    c2.metric("Total contracts", f"{int(rent['contracts'].sum()):,}")
    c3.metric("Highest rent/sqm", f"AED {int(rent['rent_per_sqm'].max()):,}", delta=rent.iloc[0]["area_name_en"])
    top_r = rent.head(n2)
    st.dataframe(top_r[["area_name_en","contracts","avg_rent","median_rent","rent_per_sqm","avg_area_sqm"]], hide_index=True, use_container_width=True)
    st.plotly_chart(
        px.bar(top_r, x="area_name_en", y="rent_per_sqm", color="rent_per_sqm",
               color_continuous_scale="Blues", title=f"Rent per sqm — Top {n2} Areas",
               labels={"area_name_en":"Area","rent_per_sqm":"AED/sqm"})
        .update_layout(xaxis_tickangle=-45),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(top_r, x="area_name_en", y="median_rent",
               title=f"Median Annual Rent — Top {n2} Areas",
               labels={"area_name_en":"Area","median_rent":"Median Rent (AED)"})
        .update_layout(xaxis_tickangle=-45),
        use_container_width=True,
    )

st.divider()
st.caption("Data: Dubai Land Department — Dubai Open Data Portal · Updated weekly")
'''


def launch_dashboard() -> None:
    """Write a temporary dashboard file and launch streamlit."""
    dash_file = BASE_DIR / "_dashboard_tmp.py"
    dash_file.write_text(DASHBOARD_CODE, encoding="utf-8")
    logger.info("Launching dashboard at http://localhost:8501 …")
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(dash_file),
             "--server.headless", "true"],
            check=True,
        )
    finally:
        dash_file.unlink(missing_ok=True)


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dubai Real Estate Intelligence — analyse DLD open-data CSV exports"
    )
    parser.add_argument(
        "--dashboard", action="store_true",
        help="Launch Streamlit dashboard (run analytics first)",
    )
    parser.add_argument(
        "--data-dir", default=str(DATA_DIR),
        help=f"Path to folder containing CSV files (default: {DATA_DIR})",
    )
    args = parser.parse_args()

    if args.dashboard:
        launch_dashboard()
    else:
        run_analytics(data_dir=Path(args.data_dir))


if __name__ == "__main__":
    main()
