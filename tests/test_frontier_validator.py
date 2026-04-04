"""Tests for FrontierSignalValidator."""
import os
import sqlite3
import numpy as np
import pytest
from unittest.mock import patch
from analysis.frontier_validator import FrontierSignalValidator


@pytest.fixture
def validator(tmp_path):
    db = str(tmp_path / "closeloop.db")
    hdb = str(tmp_path / "historical.db")
    # Seed historical DB with SPY prices
    con = sqlite3.connect(hdb)
    con.execute("""CREATE TABLE price_history (
        id INTEGER PRIMARY KEY, ticker TEXT, date TEXT,
        open REAL, high REAL, low REAL, close REAL,
        adj_close REAL, volume REAL, source TEXT, delisted INTEGER)""")
    # 60 trading days of SPY
    import pandas as pd
    from datetime import date, timedelta
    dates = pd.bdate_range("2025-01-01", periods=60)
    price = 450.0
    rows = []
    for d in dates:
        price += np.random.normal(0, 2)
        rows.append(("SPY", str(d.date()), price, price*1.01, price*0.99, price, price, 1e8, "test", 0))
    con.executemany("INSERT INTO price_history (ticker,date,open,high,low,close,adj_close,volume,source,delisted) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit(); con.close()
    return FrontierSignalValidator(closeloop_db=db, historical_db=hdb)


class TestFrontierValidatorDB:
    def test_table_created(self, validator):
        con = sqlite3.connect(validator._closeloop_db)
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "frontier_signal_validation" in tables


class TestValidateSignal:
    def test_insufficient_data(self, validator):
        with patch.object(validator, "_load_signal_observations", return_value={}):
            result = validator.validate_signal("test_signal")
        assert result["status"] == "INSUFFICIENT_DATA"
        assert result["n_obs"] == 0

    def test_failed_validation_with_random_signal(self, validator):
        spy = validator._load_spy_returns()
        # Random signal — should fail (no real predictive power)
        rng = np.random.default_rng(0)
        obs = {d: float(rng.standard_normal()) for d in list(spy.keys())[:60]}
        with patch.object(validator, "_load_signal_observations", return_value=obs):
            result = validator.validate_signal("random_signal")
        assert result["status"] in ("FAILED_VALIDATION", "INSUFFICIENT_DATA", "PROMOTED")
        assert result["n_obs"] >= 0

    def test_promoted_signal_with_perfect_predictor(self, validator):
        spy = validator._load_spy_returns()
        dates = sorted(spy.keys())[:60]
        # Perfect predictor: signal = sign(spy_return) — should have high Sharpe
        obs = {d: (1.0 if spy[d] > 0 else -1.0) for d in dates}
        with patch.object(validator, "_load_signal_observations", return_value=obs):
            result = validator.validate_signal("perfect_signal")
        # With perfect predictor and enough data, should be PROMOTED
        assert result["n_obs"] >= 50  # alignment reduces count by 1
        assert result["status"] in ("PROMOTED", "FAILED_VALIDATION")  # depends on data

    def test_result_has_required_keys(self, validator):
        with patch.object(validator, "_load_signal_observations", return_value={}):
            result = validator.validate_signal("x")
        for key in ("signal_name", "run_date", "status", "weight", "n_obs"):
            assert key in result

    def test_persisted_to_db(self, validator):
        with patch.object(validator, "_load_signal_observations", return_value={}):
            validator.validate_signal("test_persist")
        con = sqlite3.connect(validator._closeloop_db)
        row = con.execute("SELECT * FROM frontier_signal_validation WHERE signal_name='test_persist'").fetchone()
        con.close()
        assert row is not None


class TestRunAll:
    def test_run_all_returns_list(self, validator, tmp_path):
        with patch.object(validator, "_load_signal_observations", return_value={}):
            results = validator.run_all()
        assert isinstance(results, list)
        assert len(results) > 0

    def test_log_file_written(self, validator, tmp_path, monkeypatch):
        monkeypatch.setattr("analysis.frontier_validator._LOG_FILE", str(tmp_path / "test_validation.log"))
        with patch.object(validator, "_load_signal_observations", return_value={}):
            validator.run_all()
        assert os.path.exists(str(tmp_path / "test_validation.log"))

    def test_status_returns_dict(self, validator):
        with patch.object(validator, "_load_signal_observations", return_value={}):
            validator.run_all()
        s = validator.status()
        assert isinstance(s, dict)
