"""
Scans a returns matrix for recurring statistical anomalies.

Anomaly types:
  - calendar_dow   : day-of-week effect
  - calendar_month : month-of-year effect
  - momentum       : past-N-day return predicts forward-M-day return
  - mean_reversion : contrary short-term signal
"""
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class AnomalyScanner:
    def __init__(self, config: dict):
        cfg = config["signal"]["anomaly"]
        self.min_obs = cfg["min_observations"]       # 50
        self.min_sharpe = cfg["min_sharpe"]          # 1.0
        self.max_corr = cfg["max_correlation_to_existing"]  # 0.3

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, returns: pd.DataFrame) -> List[Dict]:
        """
        Scan a returns DataFrame (columns=tickers, index=dates).
        Returns list of anomaly dicts filtered by min_obs and min_sharpe.
        """
        if returns.empty or not hasattr(returns.index, "dayofweek"):
            return []

        anomalies: List[Dict] = []
        anomalies.extend(self._calendar_dow(returns))
        anomalies.extend(self._calendar_month(returns))
        anomalies.extend(self._momentum(returns))
        anomalies.extend(self._mean_reversion(returns))

        anomalies = [a for a in anomalies if a["n_obs"] >= self.min_obs]
        anomalies = [a for a in anomalies if a["sharpe"] >= self.min_sharpe]
        anomalies.sort(key=lambda x: x["sharpe"], reverse=True)

        logger.info(f"AnomalyScanner: {len(anomalies)} candidates after filters")
        return anomalies

    def deduplicate(
        self,
        anomalies: List[Dict],
        existing_series: Optional[List[pd.Series]] = None,
    ) -> List[Dict]:
        """Remove anomalies that are too correlated to each other or to existing signals."""
        kept: List[Dict] = []
        pool: List[pd.Series] = list(existing_series or [])

        for anomaly in anomalies:
            new_s = anomaly["returns_series"]
            correlated = False
            for s in pool:
                aligned = new_s.align(s, join="inner")
                if len(aligned[0]) < 20:
                    continue
                if abs(aligned[0].corr(aligned[1])) > self.max_corr:
                    correlated = True
                    break
            if not correlated:
                kept.append(anomaly)
                pool.append(new_s)

        logger.info(f"AnomalyScanner: {len(kept)}/{len(anomalies)} after deduplication")
        return kept

    # ------------------------------------------------------------------
    # Scanners
    # ------------------------------------------------------------------

    def _calendar_dow(self, returns: pd.DataFrame) -> List[Dict]:
        market = returns.mean(axis=1)
        results = []
        for dow in range(5):
            subset = market[market.index.dayofweek == dow]
            if len(subset) < self.min_obs:
                continue
            sharpe = self._sharpe(subset)
            if sharpe >= self.min_sharpe:
                results.append(self._record("calendar_dow", {"day_of_week": dow}, subset, sharpe))
        return results

    def _calendar_month(self, returns: pd.DataFrame) -> List[Dict]:
        market = returns.mean(axis=1)
        results = []
        for month in range(1, 13):
            subset = market[market.index.month == month]
            if len(subset) < self.min_obs:
                continue
            sharpe = self._sharpe(subset)
            if sharpe >= self.min_sharpe:
                results.append(self._record("calendar_month", {"month": month}, subset, sharpe))
        return results

    def _momentum(self, returns: pd.DataFrame) -> List[Dict]:
        results = []
        for lookback in [5, 10, 20, 60]:
            for forward in [1, 5, 10]:
                all_ret = self._cross_sectional_signal(returns, lookback, forward, sign=1)
                if all_ret is None or len(all_ret) < self.min_obs:
                    continue
                sharpe = self._sharpe(all_ret)
                if sharpe >= self.min_sharpe:
                    results.append(
                        self._record(
                            "momentum",
                            {"lookback": lookback, "forward": forward},
                            all_ret,
                            sharpe,
                        )
                    )
        return results

    def _mean_reversion(self, returns: pd.DataFrame) -> List[Dict]:
        results = []
        for lookback in [1, 3, 5]:
            for forward in [1, 3, 5]:
                all_ret = self._cross_sectional_signal(returns, lookback, forward, sign=-1)
                if all_ret is None or len(all_ret) < self.min_obs:
                    continue
                sharpe = self._sharpe(all_ret)
                if sharpe >= self.min_sharpe:
                    results.append(
                        self._record(
                            "mean_reversion",
                            {"lookback": lookback, "forward": forward},
                            all_ret,
                            sharpe,
                        )
                    )
        return results

    def _cross_sectional_signal(
        self,
        returns: pd.DataFrame,
        lookback: int,
        forward: int,
        sign: int,
    ) -> Optional[pd.Series]:
        parts = []
        for ticker in returns.columns:
            r = returns[ticker].dropna()
            past = r.rolling(lookback).sum().shift(1)
            fwd = r.rolling(forward).sum().shift(-forward)
            valid = past.dropna().index.intersection(fwd.dropna().index)
            if len(valid) < self.min_obs:
                continue
            sig = sign * np.sign(past[valid])
            parts.append(sig * fwd[valid])
        if not parts:
            return None
        return pd.concat(parts).dropna()

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _record(
        atype: str, params: dict, series: pd.Series, sharpe: float
    ) -> Dict:
        return {
            "type": atype,
            "params": params,
            "sharpe": sharpe,
            "n_obs": len(series),
            "mean_return": float(series.mean()),
            "returns_series": series,
        }

    @staticmethod
    def _sharpe(returns: pd.Series, ann: float = 252) -> float:
        r = returns.dropna()
        if len(r) < 10 or r.std() == 0:
            return 0.0
        return float((r.mean() / r.std()) * np.sqrt(ann))
