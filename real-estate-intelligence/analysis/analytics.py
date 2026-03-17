"""
Analytics engine.

Computes from the cleaned weekly dataset:
  - listings count per area
  - average listing price per area
  - average price/sqft per area
  - bedroom distribution per area
  - new listings this week vs previous week

All values are derived from real collected data.
No estimates are injected.
"""

import json
import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import mean
from typing import Optional

logger = logging.getLogger(__name__)


def load_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def compute_area_stats(records: list[dict]) -> dict[str, dict]:
    """
    Returns a dict keyed by area with:
      listings_count, avg_price, median_price, avg_price_per_sqft,
      bedroom_distribution, sources
    """
    by_area: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        area = r.get("area")
        if area:
            by_area[area].append(r)

    stats: dict[str, dict] = {}
    for area, listings in by_area.items():
        prices = [r["price"] for r in listings if isinstance(r.get("price"), (int, float))]
        ppsf = [r["price_per_sqft"] for r in listings
                if isinstance(r.get("price_per_sqft"), (int, float)) and r["price_per_sqft"] > 0]

        # Bedroom distribution
        bed_dist: dict[str, int] = defaultdict(int)
        for r in listings:
            beds = r.get("bedrooms")
            key = str(beds) if beds is not None else "unknown"
            bed_dist[key] += 1

        # Sources breakdown
        source_counts: dict[str, int] = defaultdict(int)
        for r in listings:
            source_counts[r.get("source", "unknown")] += 1

        stats[area] = {
            "listings_count": len(listings),
            "avg_price": round(mean(prices)) if prices else None,
            "min_price": min(prices) if prices else None,
            "max_price": max(prices) if prices else None,
            "avg_price_per_sqft": round(mean(ppsf)) if ppsf else None,
            "bedroom_distribution": dict(sorted(bed_dist.items())),
            "sources": dict(source_counts),
        }

    return stats


def compute_week_on_week(
    current_records: list[dict],
    previous_path: Optional[Path],
) -> dict[str, dict]:
    """
    Compare current week listings count per area to the previous week.
    Returns dict with area → {current, previous, change, change_pct}.
    """
    current_counts: dict[str, int] = defaultdict(int)
    for r in current_records:
        area = r.get("area")
        if area:
            current_counts[area] += 1

    prev_counts: dict[str, int] = defaultdict(int)
    if previous_path and previous_path.exists():
        for r in load_jsonl(previous_path):
            area = r.get("area")
            if area:
                prev_counts[area] += 1

    wow: dict[str, dict] = {}
    all_areas = set(list(current_counts.keys()) + list(prev_counts.keys()))
    for area in all_areas:
        curr = current_counts.get(area, 0)
        prev = prev_counts.get(area, 0)
        change = curr - prev
        change_pct = round((change / prev * 100), 1) if prev > 0 else None
        wow[area] = {
            "current_week": curr,
            "previous_week": prev,
            "change": change,
            "change_pct": change_pct,
        }

    return wow


def run_analytics(
    processed_dir: Path,
    today: Optional[date] = None,
) -> dict:
    """
    Main entry point: load this week's cleaned data, compute all stats,
    and return a structured analytics dict.
    """
    today = today or date.today()
    last_week = today - timedelta(weeks=1)

    current_path = processed_dir / f"listings_cleaned_{today.isoformat()}.jsonl"
    previous_path = processed_dir / f"listings_cleaned_{last_week.isoformat()}.jsonl"

    logger.info("Loading current data: %s", current_path)
    current_records = load_jsonl(current_path)

    if not current_records:
        logger.warning("No records found in %s", current_path)
        return {}

    logger.info("Loaded %d records", len(current_records))

    area_stats = compute_area_stats(current_records)
    wow_stats = compute_week_on_week(current_records, previous_path)

    return {
        "date": today.isoformat(),
        "total_listings": len(current_records),
        "areas": area_stats,
        "week_on_week": wow_stats,
    }
