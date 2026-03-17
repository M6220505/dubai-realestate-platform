"""
Dubai Rent Contracts Data Loader
Processes actual rental transaction data from Dubai Land Department.
"""
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "raw" / "Rent_Contracts_2026-03-15---08a8ef2a-6114-4712-9463-da2ea89bf363.csv"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "dubai_rent_contracts.json"


def load_rent_contracts() -> dict:
    """Load and process Dubai rent contracts."""
    if not DATA_FILE.exists():
        print(f"Rent data not found: {DATA_FILE}")
        return {}
    
    contracts = []
    
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Parse contract amount
                amount = float(row.get("contract_amount", 0)) if row.get("contract_amount") else 0
                annual = float(row.get("annual_amount", 0)) if row.get("annual_amount") else amount
                
                # Parse area
                area = row.get("area_name_en", "").strip()
                
                # Property type
                prop_type = row.get("ejari_property_type_en", "").strip()
                
                # Contract type
                contract_type = row.get("contract_reg_type_en", "").strip()
                
                # Dates
                start_date = row.get("contract_start_date", "")
                end_date = row.get("contract_end_date", "")
                
                # Skip if critical data missing
                if not area or amount <= 0:
                    continue
                
                contract = {
                    "area": area,
                    "property_type": prop_type,
                    "contract_type": contract_type,
                    "contract_amount": amount,
                    "annual_amount": annual,
                    "start_date": start_date,
                    "end_date": end_date,
                    "nearest_metro": row.get("nearest_metro_en", ""),
                    "nearest_mall": row.get("nearest_mall_en", ""),
                }
                contracts.append(contract)
            except Exception as e:
                continue
    
    return contracts


def analyze_rent_by_area(contracts: list) -> dict:
    """Analyze rent prices by area."""
    by_area = defaultdict(lambda: {"rents": [], "count": 0})
    
    for c in contracts:
        area = c.get("area", "Unknown")
        by_area[area]["rents"].append(c.get("annual_amount", 0))
        by_area[area]["count"] += 1
    
    # Calculate stats
    results = {}
    for area, data in by_area.items():
        rents = data["rents"]
        if rents:
            results[area] = {
                "count": data["count"],
                "avg_rent": sum(rents) / len(rents),
                "min_rent": min(rents),
                "max_rent": max(rents),
            }
    
    return results


def main():
    """Main loader."""
    print("Loading Dubai rent contracts...")
    
    contracts = load_rent_contracts()
    
    if not contracts:
        print("No contracts loaded")
        return
    
    print(f"Loaded {len(contracts)} rent contracts")
    
    # Analyze by area
    by_area = analyze_rent_by_area(contracts)
    
    # Sort by average rent
    sorted_areas = sorted(by_area.items(), key=lambda x: x[1]["avg_rent"], reverse=True)
    
    print("\n=== Top 10 Areas by Avg Rent ===")
    for i, (area, data) in enumerate(sorted_areas[:10]):
        print(f"{i+1}. {area}: AED {data['avg_rent']:,.0f}/yr ({data['count']} contracts)")
    
    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump({
            "metadata": {
                "source": "Dubai Land Department",
                "dataset": "Rent Contracts",
                "load_date": datetime.now().isoformat(),
                "record_count": len(contracts)
            },
            "by_area": by_area,
            "contracts": contracts[:100]  # Save sample
        }, f, indent=2)
    
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
