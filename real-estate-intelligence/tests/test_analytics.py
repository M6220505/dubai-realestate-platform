"""Tests for analysis/analytics.py"""

import json
import tempfile
from pathlib import Path
from datetime import date

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.analytics import compute_area_stats, compute_week_on_week, load_jsonl


LISTINGS = [
    {"area": "Dubai Marina", "price": 2000000, "price_per_sqft": 1800, "bedrooms": 2, "source": "PropertyFinder"},
    {"area": "Dubai Marina", "price": 2200000, "price_per_sqft": 1900, "bedrooms": 3, "source": "Bayut"},
    {"area": "Business Bay", "price": 1500000, "price_per_sqft": 1400, "bedrooms": 1, "source": "Dubizzle"},
]


class TestComputeAreaStats:
    def test_counts_per_area(self):
        stats = compute_area_stats(LISTINGS)
        assert stats["Dubai Marina"]["listings_count"] == 2
        assert stats["Business Bay"]["listings_count"] == 1

    def test_avg_price(self):
        stats = compute_area_stats(LISTINGS)
        assert stats["Dubai Marina"]["avg_price"] == 2100000

    def test_avg_price_per_sqft(self):
        stats = compute_area_stats(LISTINGS)
        assert stats["Dubai Marina"]["avg_price_per_sqft"] == 1850

    def test_bedroom_distribution(self):
        stats = compute_area_stats(LISTINGS)
        dist = stats["Dubai Marina"]["bedroom_distribution"]
        assert dist["2"] == 1
        assert dist["3"] == 1

    def test_sources(self):
        stats = compute_area_stats(LISTINGS)
        assert stats["Dubai Marina"]["sources"]["PropertyFinder"] == 1
        assert stats["Dubai Marina"]["sources"]["Bayut"] == 1

    def test_skips_missing_price(self):
        records = [
            {"area": "Dubai Marina", "price": None, "price_per_sqft": None, "bedrooms": 2, "source": "X"},
            {"area": "Dubai Marina", "price": 2000000, "price_per_sqft": 1800, "bedrooms": 2, "source": "X"},
        ]
        stats = compute_area_stats(records)
        # avg_price should only use the valid record
        assert stats["Dubai Marina"]["avg_price"] == 2000000


class TestWeekOnWeek:
    def test_increase(self, tmp_path):
        prev_file = tmp_path / "prev.jsonl"
        prev_records = [{"area": "Dubai Marina"}, {"area": "Dubai Marina"}]
        with open(prev_file, "w") as fh:
            for r in prev_records:
                fh.write(json.dumps(r) + "\n")

        current = [{"area": "Dubai Marina"} for _ in range(5)]
        wow = compute_week_on_week(current, prev_file)
        assert wow["Dubai Marina"]["current_week"] == 5
        assert wow["Dubai Marina"]["previous_week"] == 2
        assert wow["Dubai Marina"]["change"] == 3
        assert wow["Dubai Marina"]["change_pct"] == 150.0

    def test_no_previous_data(self, tmp_path):
        current = [{"area": "Business Bay"} for _ in range(3)]
        wow = compute_week_on_week(current, tmp_path / "nonexistent.jsonl")
        assert wow["Business Bay"]["previous_week"] == 0
        assert wow["Business Bay"]["change_pct"] is None
