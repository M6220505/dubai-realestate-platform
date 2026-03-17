import pandas as pd
import json

data = [
    {"area": "Dubai Marina", "price": 1500000, "rent": 95000, "growth": 12.5, "transactions": 320},
    {"area": "Business Bay", "price": 1300000, "rent": 88000, "growth": 15.2, "transactions": 410},
    {"area": "Dubai Hills", "price": 1800000, "rent": 110000, "growth": 18.1, "transactions": 210},
    {"area": "JVC", "price": 900000, "rent": 65000, "growth": 10.4, "transactions": 500},
]

df = pd.DataFrame(data)

df["yield"] = df["rent"] / df["price"] * 100

df["score"] = (
    df["yield"] * 0.4 +
    df["growth"] * 0.4 +
    (df["transactions"] / df["transactions"].max()) * 100 * 0.2
)

df.to_json("data/processed/area_metrics.json", orient="records", indent=2)

print("✅ area_metrics.json created")
