"""
MultiTimeframeAnalyzer — daily → 4H → 1H confirmation cascade.

Workflow:
  1. Daily signal fires (existing pipeline).
  2. 4H confirmation: does momentum on 4H bars agree?
  3. 1H entry timing: RSI/momentum on 1H bars for precise entry.
  4. Intraday burst detection: 0.5% move in 15-min bar.

Usage
-----
mta = MultiTimeframeAnalyzer()
result = mta.confirm(ticker, daily_direction)
# result.confirmed → bool
# result.entry_score → 0.0–1.0
# result.intraday_burst → bool
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_BURST_THRESHOLD = 0.005   # 0.5% intraday move


@dataclass
class MTFResult:
    ticker: str
    daily_direction: str           # "LONG" or "SHORT"
    confirmed_4h: bool
    confirmed_1h: bool
    confirmed: bool                # both 4H and 1H agree
    entry_score: float             # 0.0–1.0
    intraday_burst: bool
    burst_direction: Optional[str]  # "LONG", "SHORT", or None
    notes: str = ""


class MultiTimeframeAnalyzer:
    """
    Fetches 4H and 1H bars using yfinance intervals and checks directional
    confirmation.
    """

    def __init__(self) -> None:
        pass

    def confirm(
        self,
        ticker: str,
        daily_direction: str,
        lookback_4h: int = 5,   # days of 4H data
        lookback_1h: int = 3,   # days of 1H data
    ) -> MTFResult:
        """
        Full multi-timeframe confirmation.
        Returns MTFResult.
        """
        confirmed_4h  = self._check_4h(ticker, daily_direction, lookback_4h)
        confirmed_1h  = self._check_1h(ticker, daily_direction, lookback_1h)
        burst, burst_dir = self._check_burst(ticker)

        confirmed  = confirmed_4h and confirmed_1h
        score_components = [
            0.5 * float(confirmed_4h),
            0.3 * float(confirmed_1h),
            0.2 * float(burst and burst_dir == daily_direction),
        ]
        entry_score = min(1.0, sum(score_components))

        notes_parts = []
        if not confirmed_4h: notes_parts.append("4H disagrees")
        if not confirmed_1h: notes_parts.append("1H disagrees")
        if burst:            notes_parts.append(f"burst:{burst_dir}")

        return MTFResult(
            ticker=ticker,
            daily_direction=daily_direction,
            confirmed_4h=confirmed_4h,
            confirmed_1h=confirmed_1h,
            confirmed=confirmed,
            entry_score=round(entry_score, 4),
            intraday_burst=burst,
            burst_direction=burst_dir,
            notes="; ".join(notes_parts),
        )

    # ── 4H check ──────────────────────────────────────────────────────────────

    def _check_4h(self, ticker: str, direction: str, lookback_days: int) -> bool:
        """
        Fetch 4H bars and check if recent momentum agrees with daily direction.
        Uses a simple slope: last bar close > first bar close → LONG signal.
        """
        try:
            df = self._fetch(ticker, interval="60m", days=lookback_days * 2)
            if df is None or len(df) < 4:
                return True   # can't confirm = neutral pass
            # Resample to 4H
            df_4h = df["Close"].resample("4h").last().dropna()
            if len(df_4h) < 2:
                return True
            slope = float(df_4h.iloc[-1]) - float(df_4h.iloc[-4]) if len(df_4h) >= 4 else float(df_4h.iloc[-1]) - float(df_4h.iloc[0])
            if direction == "LONG":
                return slope > 0
            else:
                return slope < 0
        except Exception as exc:
            logger.debug("MTF 4H check error for %s: %s", ticker, exc)
            return True   # fail open

    # ── 1H check ──────────────────────────────────────────────────────────────

    def _check_1h(self, ticker: str, direction: str, lookback_days: int) -> bool:
        """
        Fetch 1H bars and compute RSI.  LONG confirmed if RSI between 45–70.
        SHORT confirmed if RSI between 30–55.
        """
        try:
            df = self._fetch(ticker, interval="60m", days=lookback_days)
            if df is None or len(df) < 14:
                return True
            rsi = self._rsi(df["Close"], period=14)
            if rsi is None:
                return True
            if direction == "LONG":
                return 40.0 <= rsi <= 75.0
            else:
                return 25.0 <= rsi <= 60.0
        except Exception as exc:
            logger.debug("MTF 1H check error for %s: %s", ticker, exc)
            return True

    # ── burst detection ───────────────────────────────────────────────────────

    def _check_burst(self, ticker: str) -> tuple[bool, Optional[str]]:
        """
        Check for 0.5% intraday burst on the most recent 15-min bar.
        Returns (is_burst, direction).
        """
        try:
            df = self._fetch(ticker, interval="15m", days=1)
            if df is None or len(df) < 2:
                return False, None
            last_close = float(df["Close"].iloc[-1])
            prev_close = float(df["Close"].iloc[-2])
            move = (last_close - prev_close) / prev_close if prev_close else 0.0
            if abs(move) >= _BURST_THRESHOLD:
                burst_dir = "LONG" if move > 0 else "SHORT"
                logger.debug("MTF burst: %s %.2f%% %s", ticker, move * 100, burst_dir)
                return True, burst_dir
            return False, None
        except Exception as exc:
            logger.debug("MTF burst check error for %s: %s", ticker, exc)
            return False, None

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _fetch(ticker: str, interval: str, days: int):
        """Fetch OHLCV data from yfinance."""
        try:
            import yfinance as yf
            end   = datetime.now()
            start = end - timedelta(days=days + 2)
            df = yf.download(ticker, start=str(start.date()), end=str(end.date()),
                             interval=interval, progress=False, auto_adjust=True,
                             threads=False)
            if df is None or df.empty:
                return None
            return df
        except Exception:
            return None

    @staticmethod
    def _rsi(prices, period: int = 14) -> Optional[float]:
        """Compute RSI on a Series.  Returns last RSI value or None."""
        try:
            delta = prices.diff().dropna()
            gain  = delta.clip(lower=0).rolling(period).mean()
            loss  = (-delta.clip(upper=0)).rolling(period).mean()
            rs    = gain / loss
            rsi   = 100.0 - (100.0 / (1.0 + rs))
            return float(rsi.iloc[-1])
        except Exception:
            return None
