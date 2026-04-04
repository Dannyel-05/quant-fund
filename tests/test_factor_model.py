"""Tests for FactorModelAnalyser (mocked factor download, no live HTTP)."""
import sqlite3
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from analysis.factor_model import FactorModelAnalyser, _FACTORS


def make_factor_df(n=200):
    idx = pd.bdate_range("2024-01-01", periods=n)
    rng = np.random.default_rng(42)
    data = {f: rng.normal(0, 0.01, n) for f in _FACTORS}
    data["RF"] = 0.0001
    return pd.DataFrame(data, index=idx)


def make_returns(n=200):
    idx = pd.bdate_range("2024-01-01", periods=n)
    rng = np.random.default_rng(1)
    return pd.Series(rng.normal(0.0005, 0.015, n), index=idx)


@pytest.fixture
def analyser(tmp_path):
    db = str(tmp_path / "closeloop.db")
    return FactorModelAnalyser(
        historical_db=str(tmp_path / "hist.db"),
        closeloop_db=db,
    )


class TestFactorModelDB:
    def test_table_created(self, analyser, tmp_path):
        con = sqlite3.connect(str(tmp_path / "closeloop.db"))
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "factor_exposures" in tables


class TestComputeFactorLoadings:
    def test_returns_dict_with_keys(self, analyser):
        factors = make_factor_df()
        with patch.object(analyser, "_get_factors", return_value=factors):
            result = analyser.compute_factor_loadings("AAPL", make_returns())
        assert "alpha" in result
        assert "r_squared" in result
        assert "n_obs" in result

    def test_insufficient_data_returns_zero_alpha(self, analyser):
        factors = make_factor_df()
        with patch.object(analyser, "_get_factors", return_value=factors):
            short_returns = make_returns(n=10)  # < min_obs=60
            result = analyser.compute_factor_loadings("AAPL", short_returns)
        assert result["alpha"] == 0.0

    def test_empty_factors_graceful(self, analyser):
        with patch.object(analyser, "_get_factors", return_value=pd.DataFrame()):
            result = analyser.compute_factor_loadings("AAPL", make_returns())
        assert result["alpha"] == 0.0

    def test_r_squared_in_range(self, analyser):
        factors = make_factor_df()
        with patch.object(analyser, "_get_factors", return_value=factors):
            result = analyser.compute_factor_loadings("AAPL", make_returns())
        if result["n_obs"] >= 60:
            assert 0.0 <= result["r_squared"] <= 1.0


class TestPortfolioExposure:
    def test_equal_weight_two_tickers(self, analyser):
        factors = make_factor_df()
        dummy = {"alpha": 0.001, "beta_smb": 0.3, "beta_hml": -0.1,
                 "beta_rmw": 0.2, "beta_cma": 0.1, "beta_mom": 0.4,
                 "beta_mkt_rf": 1.0, "r_squared": 0.5, "n_obs": 100}
        with patch.object(analyser, "compute_factor_loadings", return_value=dummy):
            tickers_returns = {"AAPL": make_returns(), "MSFT": make_returns()}
            exp = analyser.portfolio_factor_exposure(tickers_returns)
        assert "beta_smb" in exp or "alpha" in exp

    def test_empty_portfolio(self, analyser):
        exp = analyser.portfolio_factor_exposure({})
        assert exp == {}


class TestNeutraliseRecommendation:
    def test_bear_high_smb_warns(self, analyser):
        exp = {"beta_smb": 0.7, "beta_hml": 0.1, "beta_mom": 0.1,
               "alpha": 0.001, "beta_mkt_rf": 1.0}
        recs = analyser.neutralise_recommendation(exp, "BEAR")
        assert any("small-cap" in r.lower() for r in recs)

    def test_bull_low_mom_warns(self, analyser):
        exp = {"beta_smb": 0.1, "beta_hml": 0.0, "beta_mom": -0.5,
               "alpha": 0.002, "beta_mkt_rf": 0.9}
        recs = analyser.neutralise_recommendation(exp, "BULL")
        assert any("momentum" in r.lower() for r in recs)

    def test_neutral_no_issues_returns_ok_message(self, analyser):
        exp = {"beta_smb": 0.1, "beta_hml": 0.1, "beta_mom": 0.2,
               "alpha": 0.001, "beta_mkt_rf": 1.0}
        recs = analyser.neutralise_recommendation(exp, "NEUTRAL")
        assert len(recs) > 0

    def test_negative_alpha_warns(self, analyser):
        exp = {"beta_smb": 0.1, "beta_hml": 0.1, "beta_mom": 0.1,
               "alpha": -0.05, "beta_mkt_rf": 1.0}
        recs = analyser.neutralise_recommendation(exp, "NEUTRAL")
        assert any("alpha" in r.lower() for r in recs)


class TestStatus:
    def test_status_returns_dict(self, analyser):
        s = analyser.status()
        assert "rows" in s
        assert s["rows"] == 0
