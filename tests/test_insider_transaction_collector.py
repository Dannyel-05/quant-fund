"""Tests for InsiderTransactionCollector."""
import os
import sqlite3
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from data.collectors.insider_transaction_collector import (
    InsiderTransactionCollector,
    _is_ceo_cfo,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


class TestIsCeoCfo:
    def test_ceo_detected(self):
        assert _is_ceo_cfo("Chief Executive Officer") is True

    def test_cfo_detected(self):
        assert _is_ceo_cfo("Chief Financial Officer") is True

    def test_director_not_ceo(self):
        assert _is_ceo_cfo("Director") is False

    def test_vp_not_ceo(self):
        assert _is_ceo_cfo("VP of Engineering") is False


class TestInsiderTransactionCollector:
    def test_ensure_table_created(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        con = sqlite3.connect(tmp_db)
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "insider_transactions" in tables

    def test_store_and_retrieve(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        txs = [{
            "ticker": "AAPL",
            "insider_name": "Tim Cook",
            "title": "Chief Executive Officer",
            "transaction_date": "2026-01-15",
            "shares": 10000.0,
            "price_per_share": 200.0,
            "transaction_type": "P",
            "value_usd": 2_000_000.0,
            "is_ceo_cfo": 1,
            "filing_url": "https://example.com/form4",
        }]
        stored = c._store_transactions(txs)
        assert stored == 1

    def test_duplicate_not_stored_twice(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        tx = [{
            "ticker": "AAPL", "insider_name": "Tim Cook", "title": "CEO",
            "transaction_date": "2026-01-15", "shares": 1000.0,
            "price_per_share": 200.0, "transaction_type": "P",
            "value_usd": 200_000.0, "is_ceo_cfo": 1, "filing_url": "url",
        }]
        c._store_transactions(tx)
        c._store_transactions(tx)  # duplicate
        con = sqlite3.connect(tmp_db)
        count = con.execute("SELECT COUNT(*) FROM insider_transactions WHERE ticker='AAPL'").fetchone()[0]
        con.close()
        assert count == 1

    def test_generate_insider_signal_empty(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        score = c.generate_insider_signal("AAPL")
        assert score == 0.0

    def test_generate_insider_signal_cluster_buy(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        # Insert 3 insiders buying
        for i, name in enumerate(["Alice", "Bob", "Charlie"]):
            c._store_transactions([{
                "ticker": "TEST", "insider_name": name, "title": "VP",
                "transaction_date": "2026-04-01", "shares": 5000.0,
                "price_per_share": 50.0, "transaction_type": "P",
                "value_usd": 250_000.0, "is_ceo_cfo": 0,
                "filing_url": f"url{i}",
            }])
        score = c.generate_insider_signal("TEST")
        assert score >= 0.8   # cluster buy → 0.8+ floor

    def test_ceo_purchase_weighted_higher(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        c._store_transactions([{
            "ticker": "CEO_STOCK", "insider_name": "John CEO", "title": "Chief Executive Officer",
            "transaction_date": "2026-04-01", "shares": 1000.0,
            "price_per_share": 100.0, "transaction_type": "P",
            "value_usd": 100_000.0, "is_ceo_cfo": 1, "filing_url": "url1",
        }])
        score_ceo = c.generate_insider_signal("CEO_STOCK")
        c._store_transactions([{
            "ticker": "VP_STOCK", "insider_name": "Jane VP", "title": "VP",
            "transaction_date": "2026-04-01", "shares": 1000.0,
            "price_per_share": 100.0, "transaction_type": "P",
            "value_usd": 100_000.0, "is_ceo_cfo": 0, "filing_url": "url2",
        }])
        score_vp = c.generate_insider_signal("VP_STOCK")
        assert score_ceo >= score_vp  # CEO weighed higher

    def test_should_boost_signal(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        # No insider data → no boost
        boosted = c.should_boost_signal("EMPTY", 0.5)
        assert boosted == 0.5

    def test_status_returns_dict(self, tmp_db):
        c = InsiderTransactionCollector(db_path=tmp_db)
        s = c.status()
        assert "total_rows" in s
        assert s["total_rows"] == 0
