"""
Bootstrap Monte Carlo simulator for strategy robustness testing.

Resamples the observed trade-return distribution to produce a distribution
of possible equity outcomes, max drawdowns, and Sharpe ratios.
"""
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_CI = (0.05, 0.25, 0.50, 0.75, 0.95)


class MonteCarloSimulator:
    def __init__(self, config: dict):
        self.n_sims = config["backtest"]["monte_carlo_simulations"]
        self.initial_capital = config["backtest"]["initial_capital"]

    def run(
        self,
        trade_returns: pd.Series,
        confidence_levels: Tuple[float, ...] = _DEFAULT_CI,
    ) -> Dict:
        r = trade_returns.dropna()
        if len(r) < 10:
            logger.warning("Too few trades (%d) for Monte Carlo", len(r))
            return {}

        paths = self._bootstrap(r)  # (n_sims, n_trades)
        final = paths[:, -1] * self.initial_capital
        mdd = self._max_drawdowns(paths)
        sharpe = self._sharpes(paths)

        def pct(arr: np.ndarray) -> Dict:
            return {str(int(p * 100)): float(np.percentile(arr, p * 100)) for p in confidence_levels}

        return {
            "n_simulations": self.n_sims,
            "n_trades": len(r),
            "final_value": {
                "mean": float(final.mean()),
                "std": float(final.std()),
                "percentiles": pct(final),
            },
            "max_drawdown": {
                "mean": float(mdd.mean()),
                "percentiles": pct(mdd),
            },
            "sharpe": {
                "mean": float(sharpe.mean()),
                "percentiles": pct(sharpe),
            },
            "prob_profit": float((final > self.initial_capital).mean()),
            "prob_ruin": float((final < self.initial_capital * 0.5).mean()),
        }

    def _bootstrap(self, returns: pd.Series) -> np.ndarray:
        samples = np.random.choice(returns.values, size=(self.n_sims, len(returns)), replace=True)
        return np.cumprod(1 + samples, axis=1)

    def _max_drawdowns(self, paths: np.ndarray) -> np.ndarray:
        result = np.empty(self.n_sims)
        for i, path in enumerate(paths):
            peak = np.maximum.accumulate(path)
            result[i] = ((path - peak) / peak).min()
        return result

    def _sharpes(self, paths: np.ndarray) -> np.ndarray:
        log_ret = np.diff(np.log(paths), axis=1)
        mu = log_ret.mean(axis=1)
        sig = log_ret.std(axis=1)
        return np.where(sig > 0, mu / sig * np.sqrt(252), 0.0)
