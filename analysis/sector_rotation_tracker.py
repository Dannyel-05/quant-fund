"""
SectorRotationTracker — scores 11 US GICS sectors + UK proxy.

Fetches ETF prices one at a time (RAM-safe, 2s sleep between calls)
and computes a momentum score 0–100 for each sector.

US sector ETFs:
  XLK, XLV, XLF, XLC, XLY, XLP, XLI, XLB, XLE, XLU, XLRE

UK proxy:
  ISF.L (iShares Core FTSE 100)

Usage
-----
srt = SectorRotationTracker(config)
scores = srt.compute()   # dict: sector_name → score 0-100
top3  = srt.top_sectors(n=3)
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_US_SECTORS: dict[str, str] = {
    "Technology":          "XLK",
    "Healthcare":          "XLV",
    "Financials":          "XLF",
    "Communication":       "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples":    "XLP",
    "Industrials":         "XLI",
    "Materials":           "XLB",
    "Energy":              "XLE",
    "Utilities":           "XLU",
    "Real Estate":         "XLRE",
}

_UK_PROXY: dict[str, str] = {
    "UK FTSE 100": "ISF.L",
}

_ALL_SECTORS = {**_US_SECTORS, **_UK_PROXY}

_LOOKBACK_DAYS = 20   # momentum window
_ETF_SLEEP     = 2.0  # seconds between yfinance calls (RAM-safe)


def _fetch_momentum(ticker: str, lookback_days: int = _LOOKBACK_DAYS) -> Optional[float]:
    """
    Fetch ETF price and compute simple momentum: (close_today / close_N_days_ago) - 1.
    Returns None on any error.
    """
    try:
        import yfinance as yf
        end   = date.today()
        start = end - timedelta(days=lookback_days + 10)  # buffer for weekends
        df = yf.download(ticker, start=str(start), end=str(end),
                         progress=False, auto_adjust=True, threads=False)
        if df is None or len(df) < 2:
            return None
        closes = df["Close"].dropna()
        if len(closes) < 2:
            return None
        momentum = float(closes.iloc[-1]) / float(closes.iloc[0]) - 1.0
        return momentum
    except Exception as exc:
        logger.debug("SectorRotation: fetch error for %s: %s", ticker, exc)
        return None


def _momentum_to_score(momentum: float, min_mom: float, max_mom: float) -> float:
    """Normalise momentum to 0–100 within the observed range."""
    span = max_mom - min_mom
    if span < 1e-9:
        return 50.0
    return max(0.0, min(100.0, (momentum - min_mom) / span * 100.0))


class SectorRotationTracker:
    """
    Fetches sector ETF prices and scores them 0–100.
    """

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}
        self._last_scores: dict[str, float] = {}
        self._last_computed: Optional[date] = None

    def compute(self, force: bool = False) -> dict[str, float]:
        """
        Compute sector scores.  Cached for the trading day unless force=True.
        Returns dict mapping sector_name → score (0–100).
        """
        today = date.today()
        if not force and self._last_computed == today and self._last_scores:
            return self._last_scores

        raw: dict[str, Optional[float]] = {}

        for sector_name, etf in _ALL_SECTORS.items():
            logger.debug("SectorRotation: fetching %s (%s)", sector_name, etf)
            mom = _fetch_momentum(etf)
            raw[sector_name] = mom
            time.sleep(_ETF_SLEEP)  # RAM-safe / rate-limit-safe

        # Filter to valid values
        valid = {k: v for k, v in raw.items() if v is not None}
        if not valid:
            logger.warning("SectorRotation: no valid ETF data fetched — returning neutral 50")
            return {k: 50.0 for k in _ALL_SECTORS}

        min_mom = min(valid.values())
        max_mom = max(valid.values())

        scores: dict[str, float] = {}
        for name in _ALL_SECTORS:
            if name in valid:
                scores[name] = round(_momentum_to_score(valid[name], min_mom, max_mom), 1)
            else:
                scores[name] = 50.0  # neutral when data missing

        self._last_scores = scores
        self._last_computed = today
        logger.info("SectorRotation: scores computed — top: %s",
                    sorted(scores.items(), key=lambda x: -x[1])[:3])
        return scores

    def top_sectors(self, n: int = 3) -> list[tuple[str, float]]:
        """Return top-N sectors sorted by score descending."""
        scores = self._last_scores or self.compute()
        return sorted(scores.items(), key=lambda x: -x[1])[:n]

    def bottom_sectors(self, n: int = 3) -> list[tuple[str, float]]:
        """Return bottom-N sectors sorted by score ascending."""
        scores = self._last_scores or self.compute()
        return sorted(scores.items(), key=lambda x: x[1])[:n]

    def score_for(self, sector_name: str) -> Optional[float]:
        """Return score for a specific sector name."""
        scores = self._last_scores or {}
        return scores.get(sector_name)

    def is_sector_hot(self, sector_name: str, threshold: float = 70.0) -> bool:
        """Return True if sector score exceeds threshold."""
        score = self.score_for(sector_name)
        return score is not None and score >= threshold

    def is_sector_cold(self, sector_name: str, threshold: float = 30.0) -> bool:
        """Return True if sector score is below threshold."""
        score = self.score_for(sector_name)
        return score is not None and score <= threshold
