"""
Out-of-sample validation for candidate signals.
Uses the same train/val/test split fractions as the backtest config.
"""
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class SignalValidator:
    def __init__(self, config: dict):
        bt = config["backtest"]
        self.train_pct = bt["train_pct"]
        self.val_pct = bt["validate_pct"]
        self.min_sharpe = config["signal"]["anomaly"]["min_sharpe"]

    def validate(self, signal_returns: pd.Series) -> Dict:
        """
        Split returns into train / val / test.
        A signal passes if OOS Sharpe >= 50% of the in-sample min_sharpe threshold.
        """
        r = signal_returns.dropna()
        n = len(r)
        if n < 100:
            return {"passed": False, "reason": "insufficient_data", "n": n}

        i1 = int(n * self.train_pct)
        i2 = int(n * (self.train_pct + self.val_pct))

        train = r.iloc[:i1]
        val = r.iloc[i1:i2]
        test = r.iloc[i2:]

        sharpes = {
            "train": self._sharpe(train),
            "val": self._sharpe(val),
            "test": self._sharpe(test),
        }

        threshold = self.min_sharpe * 0.5
        passed = sharpes["val"] >= threshold and sharpes["test"] >= threshold

        result = {
            "passed": passed,
            "train_sharpe": sharpes["train"],
            "val_sharpe": sharpes["val"],
            "test_sharpe": sharpes["test"],
            "train_n": len(train),
            "val_n": len(val),
            "test_n": len(test),
        }
        if not passed:
            result["reason"] = "oos_sharpe_below_threshold"

        logger.info(
            "Validation: train=%.2f val=%.2f test=%.2f passed=%s",
            sharpes["train"],
            sharpes["val"],
            sharpes["test"],
            passed,
        )
        return result

    def ttest(self, returns: pd.Series, alpha: float = 0.05) -> Tuple[float, bool]:
        """One-sample t-test: is the mean return significantly > 0?"""
        r = returns.dropna()
        t_stat, p = stats.ttest_1samp(r, 0)
        return p, bool(p < alpha and t_stat > 0)

    @staticmethod
    def _sharpe(returns: pd.Series, ann: float = 252) -> float:
        r = returns.dropna()
        if len(r) < 5 or r.std() == 0:
            return 0.0
        return float((r.mean() / r.std()) * np.sqrt(ann))
