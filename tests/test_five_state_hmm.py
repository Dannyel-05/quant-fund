"""Tests for FiveStateHMM in analysis/mathematical_signals.py."""
import numpy as np
import pandas as pd
import pytest

from analysis.mathematical_signals import FiveStateHMM, HMM_AVAILABLE


def make_price_df(n: int = 300, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 0.5, n))
    close = np.maximum(close, 1.0)
    idx = pd.bdate_range("2022-01-01", periods=n)
    return pd.DataFrame(
        {
            "Open":   close * 0.999,
            "High":   close * 1.01,
            "Low":    close * 0.99,
            "Close":  close,
            "Volume": rng.integers(100_000, 1_000_000, n).astype(float),
        },
        index=idx,
    )


@pytest.mark.skipif(not HMM_AVAILABLE, reason="hmmlearn not installed")
class TestFiveStateHMM:
    def test_fit_returns_true(self):
        hmm = FiveStateHMM()
        df = make_price_df(300)
        assert hmm.fit(df) is True

    def test_state_map_has_five_labels(self):
        hmm = FiveStateHMM()
        df = make_price_df(300)
        hmm.fit(df)
        labels = set(hmm._state_map.values())
        expected = {"CRISIS", "BEAR", "NEUTRAL", "BULL", "EUPHORIA"}
        assert labels == expected

    def test_get_current_label_valid(self):
        hmm = FiveStateHMM()
        df = make_price_df(300)
        hmm.fit(df)
        label = hmm.get_current_label()
        assert label in FiveStateHMM.STATE_LABELS

    def test_regime_weights_bull(self):
        hmm = FiveStateHMM()
        w = hmm.get_regime_weights("BULL")
        assert w.get("momentum") == 2.0
        assert w.get("mean_reversion") == 0.5

    def test_regime_weights_crisis(self):
        hmm = FiveStateHMM()
        w = hmm.get_regime_weights("CRISIS")
        assert w.get("all_longs") == 0.0

    def test_regime_weights_neutral_empty(self):
        hmm = FiveStateHMM()
        w = hmm.get_regime_weights("NEUTRAL")
        assert w == {}

    def test_compare_aic_bic_keys(self):
        hmm = FiveStateHMM()
        df = make_price_df(300)
        result = hmm.compare_aic_bic(df)
        assert "aic_3state" in result
        assert "bic_3state" in result
        assert "aic_5state" in result
        assert "bic_5state" in result
        assert result["preferred"] in ("3state", "5state")

    def test_partial_fit(self):
        hmm = FiveStateHMM()
        df = make_price_df(400)
        result = hmm.partial_fit(df, lookback_days=252)
        assert result is True

    def test_log_transition_matrix_no_crash(self):
        hmm = FiveStateHMM()
        df = make_price_df(300)
        hmm.fit(df)
        hmm.log_transition_matrix()  # should not raise

    def test_insufficient_data_returns_false(self):
        hmm = FiveStateHMM()
        df = make_price_df(50)  # less than MIN_HISTORY_DAYS=252
        assert hmm.fit(df) is False


@pytest.mark.skipif(HMM_AVAILABLE, reason="only relevant when hmmlearn missing")
def test_fit_without_hmmlearn():
    hmm = FiveStateHMM()
    df = make_price_df(300)
    assert hmm.fit(df) is False
    assert hmm.get_current_label() == "NEUTRAL"
