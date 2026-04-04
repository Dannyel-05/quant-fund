"""Tests for JobPostingsCollector (mocked RSS/API calls)."""
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from data.collectors.job_postings_collector import JobPostingsCollector, _categorise_title


@pytest.fixture
def collector(tmp_path):
    return JobPostingsCollector(config={}, db_path=str(tmp_path / "test.db"))


class TestCategoriseTitle:
    def test_engineer(self):
        assert _categorise_title("Software Engineer") == "engineering"

    def test_sales(self):
        assert _categorise_title("Account Executive, EMEA") == "sales"

    def test_admin(self):
        assert _categorise_title("HR Coordinator") == "admin"

    def test_other(self):
        assert _categorise_title("Mystery Role") == "other"


class TestDB:
    def test_table_created(self, tmp_path):
        c = JobPostingsCollector(db_path=str(tmp_path / "t.db"))
        con = sqlite3.connect(str(tmp_path / "t.db"))
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "job_postings" in tables

    def test_status_empty(self, collector):
        s = collector.status()
        assert s["total_rows"] == 0


class TestStore:
    def test_store_new_row(self, collector):
        counts = {"total": 10, "engineering": 4, "sales": 3, "admin": 2, "other": 1}
        stored = collector._store("AAPL", "Apple Inc", counts, "indeed")
        assert stored == 1

    def test_duplicate_not_stored(self, collector):
        counts = {"total": 5, "engineering": 2, "sales": 1, "admin": 1, "other": 1}
        collector._store("MSFT", "Microsoft", counts, "indeed")
        stored2 = collector._store("MSFT", "Microsoft", counts, "indeed")
        assert stored2 == 0  # OR IGNORE


class TestGrowthRate:
    def test_no_data_returns_zero(self, collector):
        assert collector.calculate_growth_rate("EMPTY") == 0.0

    def test_growth_rate_positive(self, collector):
        import datetime
        # Seed 2 rows
        con = sqlite3.connect(collector._db_path)
        today = datetime.date.today().isoformat()
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,source) VALUES (?,?,?,?,?)",
                    ("TEST", "TestCo", yesterday, 100, "indeed"))
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,source) VALUES (?,?,?,?,?)",
                    ("TEST", "TestCo", today, 130, "indeed"))
        con.commit(); con.close()
        gr = collector.calculate_growth_rate("TEST")
        assert gr == pytest.approx(0.30, rel=1e-3)


class TestJobGrowthSignal:
    def test_no_data_zero(self, collector):
        assert collector.job_growth_signal("NONE") == 0.0

    def test_job_cuts_negative(self, collector):
        import datetime
        con = sqlite3.connect(collector._db_path)
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        today     = datetime.date.today().isoformat()
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("CUTS", "CutsCo", yesterday, 200, 80, 50, "indeed"))
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("CUTS", "CutsCo", today, 100, 30, 20, "indeed"))  # -50% cuts
        con.commit(); con.close()
        signal = collector.job_growth_signal("CUTS")
        assert signal < 0.0

    def test_engineering_hiring_positive(self, collector):
        import datetime
        con = sqlite3.connect(collector._db_path)
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        today     = datetime.date.today().isoformat()
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("HIRE", "HireCo", yesterday, 100, 50, 20, "indeed"))
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("HIRE", "HireCo", today, 140, 70, 25, "indeed"))  # +40% eng
        con.commit(); con.close()
        signal = collector.job_growth_signal("HIRE")
        assert signal > 0.0

    def test_signal_clamped(self, collector):
        import datetime
        con = sqlite3.connect(collector._db_path)
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        today     = datetime.date.today().isoformat()
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("MAX", "MaxCo", yesterday, 10, 5, 3, "indeed"))
        con.execute("INSERT INTO job_postings (ticker,company,collection_date,total_postings,engineering_count,sales_count,source) VALUES (?,?,?,?,?,?,?)",
                    ("MAX", "MaxCo", today, 1000, 500, 300, "indeed"))
        con.commit(); con.close()
        signal = collector.job_growth_signal("MAX")
        assert -1.0 <= signal <= 1.0
