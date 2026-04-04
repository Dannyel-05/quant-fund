"""
TechnicalIndicatorCalculator — RSI, MACD, Bollinger Bands, ATR.

All methods accept a pandas DataFrame with columns: open, high, low, close, volume
(case-insensitive).  Returns a dict of indicator values for the most recent bar.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _col(df: pd.DataFrame, *names: str) -> pd.Series:
    """Return first matching column (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for name in names:
        if name.lower() in cols_lower:
            return df[cols_lower[name.lower()]]
    raise KeyError(f"None of {names} found in DataFrame columns: {list(df.columns)}")


class TechnicalIndicatorCalculator:
    """
    Stateless calculator for common technical indicators.
    """

    # ── RSI ───────────────────────────────────────────────────────────────────

    @staticmethod
    def rsi(df: pd.DataFrame, period: int = 14) -> Optional[float]:
        """Return latest RSI (0–100) or None."""
        try:
            close = _col(df, "close", "Close", "Adj Close")
            delta = close.diff().dropna()
            gain  = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
            loss  = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
            rs    = gain / loss
            rsi   = 100.0 - (100.0 / (1.0 + rs))
            return round(float(rsi.iloc[-1]), 2)
        except Exception as exc:
            logger.debug("RSI error: %s", exc)
            return None

    # ── MACD ──────────────────────────────────────────────────────────────────

    @staticmethod
    def macd(
        df: pd.DataFrame,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> dict:
        """
        Return {"macd": float, "signal": float, "histogram": float} or empty dict.
        Positive histogram → bullish momentum.
        """
        try:
            close     = _col(df, "close", "Close", "Adj Close")
            ema_fast  = close.ewm(span=fast,   adjust=False).mean()
            ema_slow  = close.ewm(span=slow,   adjust=False).mean()
            macd_line = ema_fast - ema_slow
            sig_line  = macd_line.ewm(span=signal, adjust=False).mean()
            hist      = macd_line - sig_line
            return {
                "macd":      round(float(macd_line.iloc[-1]), 6),
                "signal":    round(float(sig_line.iloc[-1]), 6),
                "histogram": round(float(hist.iloc[-1]), 6),
            }
        except Exception as exc:
            logger.debug("MACD error: %s", exc)
            return {}

    # ── Bollinger Bands ───────────────────────────────────────────────────────

    @staticmethod
    def bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> dict:
        """
        Return {upper, middle, lower, pct_b, bandwidth} or empty dict.
        pct_b = (close - lower) / (upper - lower); 0=lower, 1=upper.
        """
        try:
            close  = _col(df, "close", "Close", "Adj Close")
            sma    = close.rolling(period).mean()
            std    = close.rolling(period).std()
            upper  = sma + std_dev * std
            lower  = sma - std_dev * std

            last_close = float(close.iloc[-1])
            u = float(upper.iloc[-1])
            m = float(sma.iloc[-1])
            lo= float(lower.iloc[-1])

            pct_b     = (last_close - lo) / (u - lo) if (u - lo) > 1e-9 else 0.5
            bandwidth = (u - lo) / m if m > 1e-9 else 0.0

            return {
                "upper":      round(u, 4),
                "middle":     round(m, 4),
                "lower":      round(lo, 4),
                "pct_b":      round(pct_b, 4),
                "bandwidth":  round(bandwidth, 4),
            }
        except Exception as exc:
            logger.debug("Bollinger Bands error: %s", exc)
            return {}

    # ── ATR ───────────────────────────────────────────────────────────────────

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
        """Return latest Average True Range or None."""
        try:
            high  = _col(df, "high",  "High")
            low   = _col(df, "low",   "Low")
            close = _col(df, "close", "Close", "Adj Close")

            prev_close = close.shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low  - prev_close).abs(),
            ], axis=1).max(axis=1)

            atr = tr.ewm(com=period - 1, adjust=False).mean()
            return round(float(atr.iloc[-1]), 6)
        except Exception as exc:
            logger.debug("ATR error: %s", exc)
            return None

    # ── Convenience: all indicators ───────────────────────────────────────────

    @classmethod
    def compute_all(cls, df: pd.DataFrame) -> dict:
        """
        Compute RSI, MACD, Bollinger Bands, ATR in one call.
        Returns a flat dict of all indicator values.
        """
        result = {}

        rsi_val = cls.rsi(df)
        if rsi_val is not None:
            result["rsi"] = rsi_val

        macd_dict = cls.macd(df)
        result.update(macd_dict)

        bb_dict = cls.bollinger_bands(df)
        result.update({f"bb_{k}": v for k, v in bb_dict.items()})

        atr_val = cls.atr(df)
        if atr_val is not None:
            result["atr"] = atr_val

        return result
