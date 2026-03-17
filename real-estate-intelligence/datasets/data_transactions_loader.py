"""
Dubai Real Estate Transactions Data Loader
Processes quarterly transaction data from Dubai Land Department.
"""
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "raw" / "Real_Estate_Transactions_2026-03-15---b69fbb0f-e0f4-4007-b87a-6992e32adb43.csv"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "dld_transactions.json"


def load_transactions() -> list:
    """Load Dubai real estate transactions."""
    if not DATA_FILE.exists():
        print(f"Transactions data not found: {DATA_FILE}")
        return []
    
    transactions = []
    
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Only process "Value" rows, not "Number" rows
                desc = row.get("Description", "").strip()
                if desc != "Value":
                    continue
                
                year = int(row.get("Year", 0))
                quarter_num = int(row.get("Quarter_Number", 0))
                trans_type = row.get("Type", "").strip()  # Units, Building, Land, Villa
                category = row.get("Title", "").strip()  # Sales, Mortgages, Other
                
                # Parse value - handle both string and numeric
                val = row.get("Value", "0")
                if isinstance(val, str):
                    val = val.replace(",", "").strip()
                try:
                    value = float(val) if val else 0
                except:
                    value = 0
                
                # Skip rows with no valid year or value
                if not year or year < 2000 or year > 2030:
                    continue
                if value <= 0:
                    continue
                    
                transactions.append({
                    "year": year,
                    "quarter": quarter_num,
                    "type": trans_type,
                    "category": category,
                    "value": value
                })
            except Exception as e:
                continue
    
    return transactions


def analyze_transactions(transactions: list) -> dict:
    """Analyze transaction trends."""
    # Group by year and category
    by_year = defaultdict(lambda: {"Sales": 0, "Mortgages": 0, "Other": 0})
    
    for t in transactions:
        year = t["year"]
        cat = t["category"]
        if cat in by_year[year]:
            by_year[year][cat] += t["value"]
    
    # Calculate annual totals
    annual = {}
    for year, cats in sorted(by_year.items()):
        total = sum(cats.values())
        annual[year] = {
            "Sales": cats["Sales"],
            "Mortgages": cats["Mortgages"],
            "Other": cats["Other"],
            "Total": total
        }
    
    return annual


def main():
    """Main loader."""
    print("Loading Dubai real estate transactions...")
    
    transactions = load_transactions()
    
    if not transactions:
        print("No transactions loaded")
        return
    
    print(f"Loaded {len(transactions)} transaction records")
    
    # Analyze
    annual = analyze_transactions(transactions)
    
    # Print summary
    print("\n=== Annual Transaction Volume (AED) ===")
    print(f"{'Year':<8} {'Sales':>20} {'Mortgages':>20} {'Total':>20}")
    print("-" * 70)
    
    for year, data in sorted(annual.items()):
        print(f"{year:<8} {data['Sales']:>20,.0f} {data['Mortgages']:>20,.0f} {data['Total']:>20,.0f}")
    
    # Calculate growth
    years = sorted(annual.keys())
    if len(years) >= 2:
        latest = annual[years[-1]]["Total"]
        previous = annual[years[-2]]["Total"]
        if previous > 0:
            growth = ((latest - previous) / previous) * 100
            print(f"\nYoY Growth: {growth:+.1f}%")
    
    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump({
            "metadata": {
                "source": "Dubai Land Department",
                "dataset": "Real Estate Transactions",
                "load_date": datetime.now().isoformat(),
                "record_count": len(transactions)
            },
            "annual_transactions": annual
        }, f, indent=2)
    
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
