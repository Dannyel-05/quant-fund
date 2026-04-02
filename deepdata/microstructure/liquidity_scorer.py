"""LiquidityScorer — ensures signals are actually tradeable."""

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LiquidityScorer:
    """Ensures signals are actually tradeable by scoring liquidity."""

    # Square-root market impact coefficient
    IMPACT_K = 0.1
    # Fraction of ADV that is the hard cap
    ADV_CAP_FRACTION = 0.10

    def __init__(self, config: dict):
        self.config = config or {}
        self.adv_window = self.config.get("adv_window_days", 30)
        self.default_slippage = self.config.get("default_slippage_pct", 0.001)
        self.intraday_days = self.config.get("intraday_days", 5)

    # ------------------------------------------------------------------
    # Main scoring entry point
    # ------------------------------------------------------------------

    def score(
        self,
        ticker: str,
        price_data,
        proposed_position_size: float,
        expected_edge_pct: float,
    ) -> dict:
        """
        Returns a full liquidity assessment dict.
        """
        empty = {
            "ticker": ticker,
            "avg_daily_volume": 0.0,
            "spread_pct": 0.0,
            "market_impact_pct": 0.0,
            "total_cost_pct": 0.0,
            "feasibility": "REJECT",
            "recommended_size": 0.0,
            "illiquidity_premium": 0.0,
        }
        try:
            adv = self._get_avg_daily_volume(ticker, price_data)
            spread_pct = self._get_spread_pct(ticker, price_data)
            slippage_pct = self.default_slippage
            market_impact_pct = self.calc_market_impact(proposed_position_size, adv)
            total_cost_pct = self.calc_total_cost(spread_pct, slippage_pct, market_impact_pct)
            feasibility = self.check_feasibility(total_cost_pct, expected_edge_pct)

            if feasibility == "FEASIBLE":
                recommended_size = proposed_position_size
            elif feasibility == "REDUCE_SIZE":
                recommended_size = self.calc_optimal_size(
                    proposed_position_size, adv, expected_edge_pct, slippage_pct
                )
            else:
                recommended_size = 0.0

            illiquidity_premium = 0.0  # populated externally via track_illiquidity_premium

            return {
                "ticker": ticker,
                "avg_daily_volume": adv,
                "spread_pct": spread_pct,
                "market_impact_pct": market_impact_pct,
                "total_cost_pct": total_cost_pct,
                "feasibility": feasibility,
                "recommended_size": recommended_size,
                "illiquidity_premium": illiquidity_premium,
            }
        except Exception as exc:
            logger.warning("LiquidityScorer.score failed for %s: %s", ticker, exc)
            return empty

    # ------------------------------------------------------------------
    # Core calculations
    # ------------------------------------------------------------------

    def calc_market_impact(self, position_size: float, avg_daily_volume: float) -> float:
        """Square root model: impact = k * sqrt(position_size / avg_daily_volume). k=0.1"""
        if avg_daily_volume <= 0 or position_size <= 0:
            return 0.0
        try:
            ratio = position_size / avg_daily_volume
            return self.IMPACT_K * math.sqrt(ratio)
        except Exception as exc:
            logger.warning("calc_market_impact error: %s", exc)
            return 0.0

    def calc_total_cost(
        self,
        spread_pct: float,
        slippage_pct: float,
        market_impact_pct: float,
    ) -> float:
        """total = spread_pct/2 + slippage_pct + market_impact_pct"""
        try:
            return (spread_pct / 2.0) + slippage_pct + market_impact_pct
        except Exception as exc:
            logger.warning("calc_total_cost error: %s", exc)
            return 0.0

    def check_feasibility(self, total_cost_pct: float, expected_edge_pct: float) -> str:
        """
        total > edge: REJECT
        total > 0.5 * edge: REDUCE_SIZE
        else: FEASIBLE
        """
        if expected_edge_pct <= 0:
            return "REJECT"
        if total_cost_pct > expected_edge_pct:
            return "REJECT"
        if total_cost_pct > 0.5 * expected_edge_pct:
            return "REDUCE_SIZE"
        return "FEASIBLE"

    def calc_optimal_size(
        self,
        position_size: float,
        avg_daily_volume: float,
        edge_pct: float,
        slippage_pct: float,
    ) -> float:
        """Find max position size where total_cost < edge. Binary search."""
        if avg_daily_volume <= 0 or edge_pct <= 0 or position_size <= 0:
            return 0.0
        try:
            # We need: spread/2 + slippage + k*sqrt(x/adv) < edge
            # Solve for max x
            spread_pct = self._default_spread()
            budget = edge_pct - (spread_pct / 2.0) - slippage_pct
            if budget <= 0:
                return 0.0
            # k * sqrt(x / adv) = budget  => x = adv * (budget/k)^2
            x_max = avg_daily_volume * (budget / self.IMPACT_K) ** 2
            result = min(position_size, x_max)
            return max(0.0, result)
        except Exception as exc:
            logger.warning("calc_optimal_size error: %s", exc)
            return 0.0

    def track_illiquidity_premium(
        self,
        ticker: str,
        returns_history: list,
        spread_history: list,
    ) -> float:
        """Historical excess return for illiquid stocks. Return premium or 0.0."""
        if not returns_history or not spread_history or len(returns_history) < 10:
            return 0.0
        try:
            returns = np.array([float(r) for r in returns_history if r is not None], dtype=float)
            spreads = np.array([float(s) for s in spread_history if s is not None], dtype=float)
            n = min(len(returns), len(spreads))
            if n < 5:
                return 0.0
            returns = returns[:n]
            spreads = spreads[:n]
            median_spread = float(np.median(spreads))
            high_spread_mask = spreads > median_spread
            low_spread_mask = ~high_spread_mask
            if high_spread_mask.sum() == 0 or low_spread_mask.sum() == 0:
                return 0.0
            premium = float(np.mean(returns[high_spread_mask]) - np.mean(returns[low_spread_mask]))
            return max(premium, 0.0)
        except Exception as exc:
            logger.warning("track_illiquidity_premium failed for %s: %s", ticker, exc)
            return 0.0

    def generate_vwap_schedule(
        self,
        ticker: str,
        total_size: float,
        execution_window_minutes: int,
    ) -> list:
        """
        VWAP execution: split order proportional to historical intraday volume profile.
        Returns list of {minute_offset, order_size_pct}.
        """
        if total_size <= 0 or execution_window_minutes <= 0:
            return []
        try:
            profile = self._get_intraday_profile(ticker)
            pct_by_hour = profile.get("pct_volume_by_hour", {})

            if not pct_by_hour:
                # Uniform schedule fallback
                slices = max(1, execution_window_minutes // 5)
                uniform_pct = 1.0 / slices
                return [
                    {"minute_offset": i * 5, "order_size_pct": uniform_pct}
                    for i in range(slices)
                ]

            # Map execution window minutes to hours and distribute
            schedule = []
            minutes_per_step = max(1, execution_window_minutes // max(len(pct_by_hour), 1))
            hours_sorted = sorted(pct_by_hour.keys())
            total_pct = sum(pct_by_hour.values())
            if total_pct <= 0:
                total_pct = 1.0

            for i, hour in enumerate(hours_sorted):
                if i * minutes_per_step >= execution_window_minutes:
                    break
                frac = pct_by_hour[hour] / total_pct
                schedule.append(
                    {
                        "minute_offset": i * minutes_per_step,
                        "order_size_pct": frac,
                    }
                )
            # Normalise so sum = 1
            total = sum(s["order_size_pct"] for s in schedule)
            if total > 0:
                for s in schedule:
                    s["order_size_pct"] /= total
            return schedule
        except Exception as exc:
            logger.warning("generate_vwap_schedule failed for %s: %s", ticker, exc)
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _default_spread(self) -> float:
        return self.config.get("default_spread_pct", 0.002)

    def _get_avg_daily_volume(self, ticker: str, price_data) -> float:
        """Extract ADV from price_data or fall back to yfinance."""
        if price_data is not None:
            try:
                if HAS_PANDAS and isinstance(price_data, pd.DataFrame):
                    if "Volume" in price_data.columns:
                        return float(price_data["Volume"].tail(self.adv_window).mean())
            except Exception:
                pass
        if HAS_YF:
            try:
                t = yf.Ticker(ticker)
                hist = t.history(period=f"{self.adv_window}d")
                if HAS_PANDAS and not hist.empty and "Volume" in hist.columns:
                    return float(hist["Volume"].mean())
            except Exception as exc:
                logger.warning("ADV fetch failed for %s: %s", ticker, exc)
        return 0.0

    def _get_spread_pct(self, ticker: str, price_data) -> float:
        """Get spread estimate from price_data or fallback."""
        if price_data is not None:
            try:
                if HAS_PANDAS and isinstance(price_data, pd.DataFrame):
                    if all(c in price_data.columns for c in ("High", "Low", "Close")):
                        last = price_data.iloc[-1]
                        c = float(last["Close"])
                        if c > 0:
                            return float((last["High"] - last["Low"]) / c)
            except Exception:
                pass
        return self._default_spread()

    def _get_intraday_profile(self, ticker: str) -> dict:
        """Fetch intraday volume profile via spread_monitor helper."""
        try:
            from deepdata.microstructure.spread_monitor import SpreadMonitor
            sm = SpreadMonitor(self.config)
            return sm.calc_intraday_volume_profile(ticker)
        except Exception as exc:
            logger.warning("intraday profile fetch failed for %s: %s", ticker, exc)
            return {"pct_volume_by_hour": {}, "vwap_approximation": 0.0}
