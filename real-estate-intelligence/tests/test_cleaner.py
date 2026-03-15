"""Tests for pipeline/cleaner.py"""

import json
import tempfile
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.cleaner import validate, clean_and_deduplicate


VALID_RECORD = {
    "title": "2BR Apartment",
    "area": "Dubai Marina",
    "property_type": "Apartment",
    "bedrooms": 2,
    "bathrooms": 2,
    "size_sqft": 1100,
    "price": 2100000,
    "price_per_sqft": 1909,
    "source": "PropertyFinder",
    "url": "https://www.propertyfinder.ae/en/listing/123",
    "scraped_at": "2026-03-15",
}


class TestValidate:
    def test_valid_record(self):
        assert validate(VALID_RECORD) is True

    def test_missing_price(self):
        r = {**VALID_RECORD, "price": None}
        assert validate(r) is False

    def test_zero_price(self):
        r = {**VALID_RECORD, "price": 0}
        assert validate(r) is False

    def test_missing_area(self):
        r = {**VALID_RECORD, "area": None}
        assert validate(r) is False

    def test_empty_area(self):
        r = {**VALID_RECORD, "area": ""}
        assert validate(r) is False

    def test_negative_price(self):
        r = {**VALID_RECORD, "price": -100}
        assert validate(r) is False


class TestCleanAndDeduplicate:
    def _write_jsonl(self, path: Path, records: list[dict]):
        with open(path, "w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

    def test_basic_cleaning(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        self._write_jsonl(raw_dir / "test.jsonl", [VALID_RECORD])
        output = tmp_path / "processed" / "out.jsonl"
        result = clean_and_deduplicate(raw_dir, output)
        assert len(result) == 1
        assert result[0]["price"] == 2100000

    def test_rejects_invalid(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bad = {**VALID_RECORD, "price": 0}
        self._write_jsonl(raw_dir / "test.jsonl", [bad, VALID_RECORD])
        output = tmp_path / "processed" / "out.jsonl"
        result = clean_and_deduplicate(raw_dir, output)
        assert len(result) == 1

    def test_deduplication_by_url(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        self._write_jsonl(raw_dir / "test.jsonl", [VALID_RECORD, VALID_RECORD])
        output = tmp_path / "processed" / "out.jsonl"
        result = clean_and_deduplicate(raw_dir, output)
        assert len(result) == 1

    def test_allows_no_url_duplicates(self, tmp_path):
        """Records without URL are not deduplicated against each other."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        r1 = {**VALID_RECORD, "url": None}
        r2 = {**VALID_RECORD, "url": None}
        self._write_jsonl(raw_dir / "test.jsonl", [r1, r2])
        output = tmp_path / "processed" / "out.jsonl"
        result = clean_and_deduplicate(raw_dir, output)
        assert len(result) == 2
