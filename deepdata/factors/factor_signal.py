"""FactorSignal — generates alpha signals from factor mispricings."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FactorSignal:
    """Generates alpha signals from factor mispricings."""

    # Thresholds for "high" factor loading (cross-sectionally normalised -1 to +1)
    HIGH_THRESHOLD = 0.5
    LOW_THRESHOLD = -0.5
    MOMENTUM_HIGH = 0.5
    VOL_LOW = -0.3   # negative = low vol

    def __init__(self, config: dict):
        self.config = config or {}
        self.high_threshold = self.config.get("high_factor_threshold", self.HIGH_THRESHOLD)
        self.factor_momentum_lookback = self.config.get("factor_momentum_lookback", 63)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        exposure_matrix: "pd.DataFrame",
        factor_returns: "pd.DataFrame",
    ) -> list:
        """
        Find stocks with unusual factor combinations = potential mispricings.
        Return CollectorResult list.
        """
        if not HAS_PANDAS or exposure_matrix is None or exposure_matrix.empty:
            return []
        try:
            mispricings = self.find_mispricings(exposure_matrix)
            factor_momentum = self.calc_factor_momentum(factor_returns, self.factor_momentum_lookback)
            results = self.generate_collector_results(mispricings)
            return results
        except Exception as exc:
            logger.warning("FactorSignal.generate failed: %s", exc)
            return []

    def find_mispricings(self, exposure_matrix: "pd.DataFrame") -> list:
        """
        Find:
        - Stocks high on QUALITY + VALUE simultaneously (rare)
        - Stocks with HIGH MOMENTUM + LOW VOLATILITY (unusual)
        - Stocks with HIGH EARNINGS_QUALITY + LOW SHORT_INTEREST
        """
        if not HAS_PANDAS or exposure_matrix is None or exposure_matrix.empty:
            return []
        signals = []
        try:
            df = exposure_matrix.copy()
            cols = df.columns.tolist()

            for ticker in df.index:
                row = df.loc[ticker]

                # 1. Quality + Value
                q = row.get("quality", 0.0) if "quality" in cols else 0.0
                v = row.get("value", 0.0) if "value" in cols else 0.0
                if float(q) > self.high_threshold and float(v) > self.high_threshold:
                    combo_score = (float(q) + float(v)) / 2.0
                    signals.append({
                        "ticker": ticker,
                        "reason": "high_quality_high_value",
                        "factor_combo": {"quality": float(q), "value": float(v)},
                        "signal_strength": combo_score,
                    })

                # 2. High Momentum + Low Volatility
                m = row.get("momentum", 0.0) if "momentum" in cols else 0.0
                vol = row.get("volatility", 0.0) if "volatility" in cols else 0.0
                if float(m) > self.MOMENTUM_HIGH and float(vol) > self.high_threshold:
                    # vol factor loading > threshold means low realised vol (since -vol)
                    combo_score = (float(m) + float(vol)) / 2.0
                    signals.append({
                        "ticker": ticker,
                        "reason": "high_momentum_low_volatility",
                        "factor_combo": {"momentum": float(m), "volatility": float(vol)},
                        "signal_strength": combo_score,
                    })

                # 3. High Earnings Quality + Low Short Interest (proxy: positive altdata)
                eq = row.get("earnings_quality", 0.0) if "earnings_quality" in cols else 0.0
                ad = row.get("altdata", 0.0) if "altdata" in cols else 0.0
                if float(eq) > self.high_threshold and float(ad) > 0:
                    combo_score = (float(eq) + float(ad)) / 2.0
                    signals.append({
                        "ticker": ticker,
                        "reason": "high_earnings_quality_low_short_interest",
                        "factor_combo": {"earnings_quality": float(eq), "altdata": float(ad)},
                        "signal_strength": combo_score,
                    })

        except Exception as exc:
            logger.warning("find_mispricings failed: %s", exc)

        # Sort by signal strength descending
        signals.sort(key=lambda x: x.get("signal_strength", 0.0), reverse=True)
        return signals

    def calc_factor_momentum(
        self,
        factor_returns: "pd.DataFrame",
        lookback: int = 63,
    ) -> dict:
        """Which factors are trending? Returns {factor_name: momentum_score}."""
        if not HAS_PANDAS or factor_returns is None or factor_returns.empty:
            return {}
        try:
            tail = factor_returns.tail(lookback)
            momentum = {}
            for col in tail.columns:
                cumret = float((1 + tail[col].fillna(0)).prod() - 1)
                momentum[col] = cumret
            return momentum
        except Exception as exc:
            logger.warning("calc_factor_momentum failed: %s", exc)
            return {}

    def generate_collector_results(self, signals: list, market: str = "us") -> list:
        """Convert factor mispricings to CollectorResult format."""
        if not signals:
            return []
        results = []
        for s in signals:
            try:
                result = {
                    "source": "factor_signal",
                    "ticker": s.get("ticker", ""),
                    "market": market,
                    "data_type": "factor_mispricing",
                    "value": float(s.get("signal_strength", 0.0)),
                    "raw_data": s,
                    "timestamp": _now_iso(),
                    "quality_score": min(1.0, float(s.get("signal_strength", 0.0))),
                }
                results.append(result)
            except Exception as exc:
                logger.warning("generate_collector_results item failed: %s", exc)
        return results
