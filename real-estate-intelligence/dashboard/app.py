"""
Dubai Real Estate Intelligence Dashboard

Run from the real-estate-intelligence directory:
    streamlit run dashboard/app.py

Requires outputs/ to be populated by the pipeline first:
    python -m pipeline.weekly_pipeline
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUTS_DIR = Path(__file__).resolve().parents[1] / "outputs"
RANKINGS_FILE = OUTPUTS_DIR / "investment_rankings.csv"
RENT_FILE = OUTPUTS_DIR / "rent_area_rankings.csv"

st.set_page_config(
    page_title="Dubai Real Estate Intelligence",
    page_icon="🏙️",
    layout="wide",
)

st.title("Dubai Real Estate Intelligence")
st.caption("Powered by Dubai Land Department open data")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_csv(path: Path, label: str) -> pd.DataFrame | None:
    if not path.exists():
        st.warning(
            f"**{label}** not found at `{path}`.\n\n"
            "Run the pipeline first: `python -m pipeline.weekly_pipeline`"
        )
        return None
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Investment Rankings
# ---------------------------------------------------------------------------
st.header("Investment Rankings")

rankings = load_csv(RANKINGS_FILE, "investment_rankings.csv")

if rankings is not None:
    col1, col2 = st.columns([1, 2])

    with col1:
        top_n = st.slider("Show top N areas", min_value=5, max_value=min(50, len(rankings)), value=15)

    with col2:
        st.metric("Total areas ranked", len(rankings))

    top = rankings.head(top_n)

    st.dataframe(
        top[["rank", "area_name_en", "investment_score", "rent_per_sqm",
             "transaction_volume", "permit_activity"]],
        use_container_width=True,
        hide_index=True,
    )

    fig = px.bar(
        top,
        x="area_name_en",
        y="investment_score",
        color="investment_score",
        color_continuous_scale="Viridis",
        title=f"Top {top_n} Areas by Investment Score",
        labels={"area_name_en": "Area", "investment_score": "Score"},
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # Score breakdown scatter
    if "transaction_volume" in rankings.columns and "rent_per_sqm" in rankings.columns:
        st.subheader("Rent Yield vs Market Liquidity")
        fig2 = px.scatter(
            top,
            x="transaction_volume",
            y="rent_per_sqm",
            size="investment_score",
            color="investment_score",
            hover_name="area_name_en",
            color_continuous_scale="RdYlGn",
            title="Rent per sqm vs Transaction Volume (bubble size = investment score)",
            labels={
                "transaction_volume": "Transaction Volume",
                "rent_per_sqm": "Avg Rent / sqm (AED)",
            },
        )
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Rental Market
# ---------------------------------------------------------------------------
st.header("Rental Market")

rent = load_csv(RENT_FILE, "rent_area_rankings.csv")

if rent is not None:
    top_rent_n = st.slider("Show top N areas (rent)", min_value=5, max_value=min(50, len(rent)), value=20)
    top_rent = rent.head(top_rent_n)

    col_r1, col_r2, col_r3 = st.columns(3)
    col_r1.metric("Areas tracked", len(rent))
    col_r2.metric("Total contracts", f"{int(rent['contracts'].sum()):,}")
    col_r3.metric(
        "Highest rent/sqm",
        f"AED {int(rent['rent_per_sqm'].max()):,}",
        delta=rent.iloc[0]["area_name_en"],
    )

    st.dataframe(
        top_rent[["area_name_en", "contracts", "avg_rent", "median_rent", "rent_per_sqm", "avg_area_sqm"]],
        use_container_width=True,
        hide_index=True,
    )

    fig3 = px.bar(
        top_rent,
        x="area_name_en",
        y="rent_per_sqm",
        color="rent_per_sqm",
        color_continuous_scale="Blues",
        title=f"Average Rent per sqm — Top {top_rent_n} Areas",
        labels={"area_name_en": "Area", "rent_per_sqm": "AED / sqm"},
    )
    fig3.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig3, use_container_width=True)

    fig4 = px.bar(
        top_rent,
        x="area_name_en",
        y="median_rent",
        title=f"Median Annual Rent — Top {top_rent_n} Areas",
        labels={"area_name_en": "Area", "median_rent": "Median Annual Rent (AED)"},
    )
    fig4.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.caption("Data source: Dubai Land Department via Dubai Open Data Portal · Updated weekly")
