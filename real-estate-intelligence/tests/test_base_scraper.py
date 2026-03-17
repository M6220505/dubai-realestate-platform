"""Tests for scrapers/base_scraper.py"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers.base_scraper import BaseScraper


class TestCleanPrice:
    def test_aed_format(self):
        assert BaseScraper.clean_price("AED 2,100,000") == 2100000

    def test_plain_digits(self):
        assert BaseScraper.clean_price("2100000") == 2100000

    def test_with_spaces(self):
        assert BaseScraper.clean_price("2 100 000") == 2100000

    def test_empty(self):
        assert BaseScraper.clean_price("") is None

    def test_none(self):
        assert BaseScraper.clean_price(None) is None

    def test_non_numeric(self):
        assert BaseScraper.clean_price("Price on request") is None


class TestCleanInt:
    def test_plain(self):
        assert BaseScraper.clean_int("2") == 2

    def test_with_text(self):
        assert BaseScraper.clean_int("2 Beds") == 2

    def test_none(self):
        assert BaseScraper.clean_int(None) is None

    def test_empty(self):
        assert BaseScraper.clean_int("") is None
