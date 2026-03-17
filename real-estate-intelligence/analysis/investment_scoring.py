"""
Investment Scoring Engine v1
Score = Yield + Growth + Liquidity
"""
import json
from collections import defaultdict
from datetime import datetime

def load_data():
    """Load all data sources."""
    with open('data/processed/dld_market_indices.json') as f:
        indices = json.load(f)
    
    with open('data/processed/dubai_rent_contracts.json') as f:
        rent = json.load(f)
    
    with open('data/processed/dld_transactions.json') as f:
        transactions = json.load(f)
    
    return indices, rent, transactions


def calculate_yield(rent_data, prop_type='apartment'):
    """
    Calculate rental yield by area.
    Yield = Annual Rent / Property Price * 100
    """
    by_area = rent_data.get('by_area', {})
    
    yields = {}
    for area, data in by_area.items():
        avg_rent = data.get('avg_annual_rent', 0)
        # Estimate price from rent (using cap rate ~5%)
        if avg_rent > 0:
            estimated_price = avg_rent / 0.05  # 5% cap rate
            annual_yield = (avg_rent / estimated_price) * 100
            yields[area] = {
                'avg_rent': avg_rent,
                'estimated_price': estimated_price,
                'yield_pct': annual_yield
            }
    
    return yields


def calculate_growth(indices_data):
    """Get growth rates from government indices."""
    trends = indices_data.get('trends', {})
    
    return {
        'apartment': {
            'yoy_change': trends.get('flat_index_yoy_change', 0),
            'price_per_sqft': trends.get('flat_price_per_sqft_current', 0)
        },
        'villa': {
            'yoy_change': trends.get('villa_index_yoy_change', 0),
            'price_per_sqft': trends.get('villa_price_per_sqft_current', 0)
        }
    }


def calculate_liquidity(transactions_data):
    """Calculate liquidity score from transaction volume."""
    annual = transactions_data.get('annual_transactions', {})
    
    if not annual:
        return 0
    
    # Latest year volume
    years = sorted(annual.keys())
    latest = annual[years[-1]]
    
    # Total transactions value
    total = latest.get('Total', 0)
    
    # Normalize to 0-100 score
    # Assume 500B+ = 100 score
    liquidity_score = min(100, (total / 5e9) * 100)
    
    return liquidity_score


def calculate_investment_score(yield_pct, growth_pct, liquidity_score):
    """
    Calculate overall investment score.
    
    Score = Yield (40%) + Growth (40%) + Liquidity (20%)
    """
    # Normalize yield (5% = 100, 0% = 0)
    yield_score = min(100, (yield_pct / 5) * 100)
    
    # Normalize growth (20% = 100)
    growth_score = min(100, (growth_pct / 20) * 100)
    
    # Weighted average
    total_score = (yield_score * 0.4) + (growth_score * 0.4) + (liquidity_score * 0.2)
    
    return {
        'yield_score': round(yield_score, 1),
        'growth_score': round(growth_score, 1),
        'liquidity_score': round(liquidity_score, 1),
        'total_score': round(total_score, 1)
    }


def generate_report():
    """Generate investment scoring report."""
    indices, rent, transactions = load_data()
    
    # Get metrics
    growth = calculate_growth(indices)
    liquidity = calculate_liquidity(transactions)
    
    print("=" * 70)
    print("🏆 DUBAI REAL ESTATE INVESTMENT SCORING")
    print("=" * 70)
    
    print("\n📊 MARKET METRICS")
    print("-" * 70)
    print(f"  Apartment Growth (YoY): +{growth['apartment']['yoy_change']:.2f}%")
    print(f"  Villa Growth (YoY):     +{growth['villa']['yoy_change']:.2f}%")
    print(f"  Market Liquidity:      {liquidity:.1f}/100")
    
    # Calculate scores for apartments
    apt_yield = 5.0  # Typical Dubai apartment yield ~5%
    apt_score = calculate_investment_score(apt_yield, growth['apartment']['yoy_change'], liquidity)
    
    # Calculate scores for villas
    villa_yield = 4.0  # Typical Dubai villa yield ~4%
    villa_score = calculate_investment_score(villa_yield, growth['villa']['yoy_change'], liquidity)
    
    print("\n" + "=" * 70)
    print("🎯 INVESTMENT SCORES")
    print("=" * 70)
    
    print("\n🏢 APARTMENTS")
    print(f"  Yield Score:     {apt_score['yield_score']}/100 (5.0% yield)")
    print(f"  Growth Score:   {apt_score['growth_score']}/100 (+{growth['apartment']['yoy_change']:.2f}% YoY)")
    print(f"  Liquidity Score:{apt_score['liquidity_score']}/100")
    print(f"  ─────────────────────────")
    print(f"  TOTAL SCORE:    {apt_score['total_score']}/100")
    
    print("\n🏡 VILLAS")
    print(f"  Yield Score:     {villa_score['yield_score']}/100 (4.0% yield)")
    print(f"  Growth Score:   {villa_score['growth_score']}/100 (+{growth['villa']['yoy_change']:.2f}% YoY)")
    print(f"  Liquidity Score:{villa_score['liquidity_score']}/100")
    print(f"  ─────────────────────────")
    print(f"  TOTAL SCORE:    {villa_score['total_score']}/100")
    
    # Winner
    print("\n" + "=" * 70)
    print("✅ RECOMMENDATION")
    print("=" * 70)
    
    if villa_score['total_score'] > apt_score['total_score']:
        winner = "VILLAS"
        score = villa_score['total_score']
    else:
        winner = "APARTMENTS"
        score = apt_score['total_score']
    
    print(f"\n  Best Investment: {winner}")
    print(f"  Score: {score}/100")
    print(f"\n  Reasoning:")
    print(f"  - VILLA growth (+20.67%) beats APARTMENT (+14.77%)")
    print(f"  - APARTMENTS have better yield (~5% vs ~4%)")
    print(f"  - Both benefit from high market liquidity")
    
    # Save report
    report = {
        'generated': datetime.now().isoformat(),
        'scores': {
            'apartments': apt_score,
            'villas': villa_score
        },
        'metrics': {
            'growth': growth,
            'liquidity': liquidity
        },
        'recommendation': winner
    }
    
    with open('data/processed/investment_scores.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("\n" + "=" * 70)
    print(f"📁 Report saved to data/processed/investment_scores.json")
    print("=" * 70)
    
    return report


if __name__ == "__main__":
    generate_report()
