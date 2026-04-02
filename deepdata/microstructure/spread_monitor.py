"""SpreadMonitor — tracks bid-ask spreads and market microstructure."""

import logging
import time
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


class SpreadMonitor:
    """Tracks bid-ask spreads and market microstructure."""

    def __init__(self, config: dict):
        self.config = config or {}
        self.zscore_window = self.config.get("zscore_window", 20)
        self.liquidity_shock_z = self.config.get("liquidity_shock_z", 2.0)
        self.intraday_days = self.config.get("intraday_days", 5)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def monitor(self, tickers: list, market: str = "us") -> list:
        """Return CollectorResult list with spread data per ticker."""
        if not tickers:
            return []
        results = []
        for ticker in tickers:
            try:
                spread_data = self.get_spread_data(ticker)
                quality_score = 1.0 if spread_data.get("bid") and spread_data.get("ask") else 0.5
                result = {
                    "source": "spread_monitor",
                    "ticker": ticker,
                    "market": market,
                    "data_type": "microstructure_spread",
                    "value": spread_data.get("spread_pct", 0.0),
                    "raw_data": spread_data,
                    "timestamp": _now_iso(),
                    "quality_score": quality_score,
                }
                results.append(result)
            except Exception as exc:
                logger.warning("spread_monitor.monitor failed for %s: %s", ticker, exc)
        return results

    def get_spread_data(self, ticker: str) -> dict:
        """
        Use yfinance Ticker.fast_info for bid/ask.
        Fallback: use (high - low) / close as daily spread approximation.
        Returns: {bid, ask, spread_abs, spread_pct, mid_price, timestamp}
        """
        result = {
            "bid": None,
            "ask": None,
            "spread_abs": None,
            "spread_pct": None,
            "mid_price": None,
            "timestamp": _now_iso(),
        }
        if not HAS_YF:
            logger.warning("yfinance not available; cannot get spread data for %s", ticker)
            return result

        try:
            t = yf.Ticker(ticker)
            fi = t.fast_info
            bid = getattr(fi, "bid", None)
            ask = getattr(fi, "ask", None)
            if bid and ask and bid > 0 and ask > 0 and ask > bid:
                mid = (bid + ask) / 2.0
                spread_abs = ask - bid
                spread_pct = spread_abs / mid if mid > 0 else 0.0
                result.update(
                    bid=bid,
                    ask=ask,
                    spread_abs=spread_abs,
                    spread_pct=spread_pct,
                    mid_price=mid,
                )
                return result
        except Exception as exc:
            logger.warning("fast_info bid/ask failed for %s: %s", ticker, exc)

        # Fallback: daily (high - low) / close
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if HAS_PANDAS and not hist.empty:
                last = hist.iloc[-1]
                high = float(last.get("High", 0))
                low = float(last.get("Low", 0))
                close = float(last.get("Close", 1))
                if close > 0 and high >= low:
                    spread_abs = high - low
                    spread_pct = spread_abs / close
                    mid = close
                    result.update(
                        bid=low,
                        ask=high,
                        spread_abs=spread_abs,
                        spread_pct=spread_pct,
                        mid_price=mid,
                    )
        except Exception as exc:
            logger.warning("Fallback spread calc failed for %s: %s", ticker, exc)

        return result

    def calc_spread_zscore(
        self,
        ticker: str,
        current_spread_pct: float,
        history: list,
    ) -> float:
        """Z-score of current spread vs 20-day history."""
        if not history or len(history) < 2:
            return 0.0
        try:
            arr = np.array([float(x) for x in history if x is not None], dtype=float)
            if len(arr) < 2:
                return 0.0
            mean = float(np.mean(arr))
            std = float(np.std(arr, ddof=1))
            if std == 0:
                return 0.0
            return (current_spread_pct - mean) / std
        except Exception as exc:
            logger.warning("calc_spread_zscore failed for %s: %s", ticker, exc)
            return 0.0

    def detect_liquidity_events(self, ticker: str, spread_history: list) -> list:
        """Flag days where spread > mean + 2*std. Liquidity shock events."""
        if not spread_history or len(spread_history) < 3:
            return []
        try:
            arr = np.array([float(x) for x in spread_history if x is not None], dtype=float)
            mean = float(np.mean(arr))
            std = float(np.std(arr, ddof=1))
            threshold = mean + self.liquidity_shock_z * std
            events = []
            for i, val in enumerate(arr):
                if val > threshold:
                    events.append(
                        {
                            "day_index": i,
                            "spread_pct": float(val),
                            "threshold": threshold,
                            "zscore": (val - mean) / std if std > 0 else 0.0,
                            "event_type": "liquidity_shock",
                        }
                    )
            return events
        except Exception as exc:
            logger.warning("detect_liquidity_events failed for %s: %s", ticker, exc)
            return []

    def calc_intraday_volume_profile(self, ticker: str) -> dict:
        """
        Use yfinance 1-minute data (last 5 days) to estimate intraday volume profile.
        Returns: {pct_volume_by_hour: dict, vwap_approximation: float}
        """
        empty = {"pct_volume_by_hour": {}, "vwap_approximation": 0.0}
        if not HAS_YF or not HAS_PANDAS:
            logger.warning("yfinance/pandas not available for intraday profile %s", ticker)
            return empty
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=f"{self.intraday_days}d", interval="1m")
            if hist is None or hist.empty:
                return empty

            # Compute VWAP approximation
            price_mid = (hist["High"] + hist["Low"]) / 2.0
            volume = hist["Volume"].replace(0, np.nan)
            total_vol = volume.sum()
            if total_vol == 0 or total_vol != total_vol:
                return empty

            vwap = float((price_mid * volume).sum() / total_vol)

            # Volume profile by hour
            hist_idx = hist.copy()
            try:
                hist_idx.index = pd.to_datetime(hist_idx.index)
                hist_idx["hour"] = hist_idx.index.hour
            except Exception:
                hist_idx["hour"] = 0

            hour_vol = hist_idx.groupby("hour")["Volume"].sum()
            pct_by_hour = {}
            for hour, vol in hour_vol.items():
                pct_by_hour[int(hour)] = float(vol / total_vol) if total_vol > 0 else 0.0

            return {"pct_volume_by_hour": pct_by_hour, "vwap_approximation": vwap}
        except Exception as exc:
            logger.warning("calc_intraday_volume_profile failed for %s: %s", ticker, exc)
            return empty
