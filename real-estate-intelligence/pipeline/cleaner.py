"""
Data cleaner and validator.

Rules (from spec):
  - Reject if price is missing or zero
  - Reject if area is missing
  - Never create estimated values
  - De-duplicate by URL
"""

import json
import logging
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("price", "area")


def iter_raw_records(raw_dir: Path) -> Iterator[dict]:
    """Yield every record from all .jsonl files in raw_dir."""
    for jsonl_file in sorted(raw_dir.glob("*.jsonl")):
        with open(jsonl_file, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning("Bad JSON in %s: %s", jsonl_file.name, exc)


def validate(record: dict) -> bool:
    """Return True if record passes all validation rules."""
    for field in REQUIRED_FIELDS:
        val = record.get(field)
        if val is None or val == "" or val == 0:
            return False
    # Price must be a positive integer
    try:
        if int(record["price"]) <= 0:
            return False
    except (TypeError, ValueError):
        return False
    return True


def clean_and_deduplicate(raw_dir: Path, output_path: Path) -> list[dict]:
    """
    Load all raw records, validate, deduplicate by URL, and write to output_path.
    Returns the cleaned list.
    """
    seen_urls: set[str] = set()
    valid: list[dict] = []
    total = rejected = duplicates = 0

    for record in iter_raw_records(raw_dir):
        total += 1

        if not validate(record):
            rejected += 1
            continue

        url = record.get("url") or ""
        if url and url in seen_urls:
            duplicates += 1
            continue
        if url:
            seen_urls.add(url)

        valid.append(record)

    logger.info(
        "Cleaning: %d total → %d valid, %d rejected, %d duplicates",
        total, len(valid), rejected, duplicates,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        for record in valid:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info("Cleaned data saved → %s", output_path)
    return valid
