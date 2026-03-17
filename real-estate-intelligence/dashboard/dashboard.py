import streamlit as st
import pandas as pd

st.title("Dubai Real Estate Intelligence")

df = pd.read_json("data/processed/area_metrics.json")

st.dataframe(df)
st.bar_chart(df.set_index("area")["score"])
