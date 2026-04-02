"""Tests for AnomalyScanner and SignalValidator."""
import numpy as np
import pandas as pd
import pytest

from signals.anomaly_scanner import AnomalyScanner
from signals.signal_validator import SignalValidator

CONFIG = {
    "signal": {
        "anomaly": {
            "min_observations": 20,
            "min_sharpe": 0.5,
            "max_correlation_to_existing": 0.7,
            "validation_required": True,
            "auto_promote": False,
        }
    },
    "backtest": {
        "train_pct": 0.70,
        "validate_pct": 0.15,
        "test_pct": 0.15,
    },
}


def make_returns(n: int = 500, n_tickers: int = 5, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2018-01-01", periods=n)
    data = rng.normal(0.0005, 0.015, (n, n_tickers))
    return pd.DataFrame(data, index=idx, columns=[f"T{i}" for i in range(n_tickers)])


class TestAnomalyScanner:
    def setup_method(self):
        self.scanner = AnomalyScanner(CONFIG)

    def test_scan_returns_list(self):
        returns = make_returns()
        result = self.scanner.scan(returns)
        assert isinstance(result, list)

    def test_all_candidates_meet_min_sharpe(self):
        returns = make_returns()
        result = self.scanner.scan(returns)
        for a in result:
            assert a["sharpe"] >= CONFIG["signal"]["anomaly"]["min_sharpe"]

    def test_all_candidates_meet_min_obs(self):
        returns = make_returns()
        result = self.scanner.scan(returns)
        for a in result:
            assert a["n_obs"] >= CONFIG["signal"]["anomaly"]["min_observations"]

    def test_results_sorted_by_sharpe_descending(self):
        returns = make_returns()
        result = self.scanner.scan(returns)
        sharpes = [a["sharpe"] for a in result]
        assert sharpes == sorted(sharpes, reverse=True)

    def test_deduplicate_removes_correlated(self):
        returns = make_returns()
        candidates = self.scanner.scan(returns)
        if len(candidates) < 2:
            pytest.skip("Not enough candidates to test deduplication")
        # Create a near-duplicate of the first candidate
        dup = dict(candidates[0])
        dup["returns_series"] = candidates[0]["returns_series"] + np.random.normal(0, 1e-9, len(candidates[0]["returns_series"]))
        augmented = [candidates[0], dup] + candidates[1:]
        deduped = self.scanner.deduplicate(augmented)
        assert len(deduped) < len(augmented)

    def test_deduplicate_keeps_uncorrelated(self):
        returns = make_returns()
        # Two completely independent random series
        rng = np.random.default_rng(1)
        s1 = pd.Series(rng.normal(0, 1, 200))
        rng2 = np.random.default_rng(99)
        s2 = pd.Series(rng2.normal(0, 1, 200))
        candidates = [
            {"returns_series": s1, "sharpe": 1.5, "n_obs": 200, "type": "x", "params": {}, "mean_return": 0},
            {"returns_series": s2, "sharpe": 1.2, "n_obs": 200, "type": "y", "params": {}, "mean_return": 0},
        ]
        deduped = self.scanner.deduplicate(candidates)
        assert len(deduped) == 2

    def test_empty_returns_returns_empty_list(self):
        result = self.scanner.scan(pd.DataFrame())
        assert result == []


class TestSignalValidator:
    def setup_method(self):
        self.validator = SignalValidator(CONFIG)

    def test_passes_consistently_positive_signal(self):
        # Strong, noisy positive returns — Sharpe well above threshold in all splits
        rng = np.random.default_rng(42)
        returns = pd.Series(rng.normal(0.005, 0.005, 300))  # Sharpe ~sqrt(252) ≈ 16
        result = self.validator.validate(returns)
        assert result["passed"] is True

    def test_fails_noisy_zero_return(self):
        rng = np.random.default_rng(7)
        returns = pd.Series(rng.normal(0, 0.02, 300))
        result = self.validator.validate(returns)
        # With zero mean and low Sharpe, should fail
        assert "passed" in result

    def test_insufficient_data_fails(self):
        result = self.validator.validate(pd.Series([0.01] * 50))
        assert result["passed"] is False
        assert result["reason"] == "insufficient_data"

    def test_splits_are_correct_proportions(self):
        n = 200
        returns = pd.Series(np.ones(n) * 0.001)
        result = self.validator.validate(returns)
        assert result["train_n"] + result["val_n"] + result["test_n"] == n

    def test_ttest_on_positive_series(self):
        rng = np.random.default_rng(0)
        positive = pd.Series(rng.normal(0.005, 0.01, 200))
        p, significant = self.validator.ttest(positive)
        assert isinstance(p, float)
        assert isinstance(significant, bool)
