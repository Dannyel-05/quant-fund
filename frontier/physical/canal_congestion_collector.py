"""
Canal Congestion Index Collector — Freight Rate Proxy Signal.

Measures shipping and freight congestion using publicly available freight
indices as a proxy for global trade flow bottlenecks.

Note on data availability
--------------------------
Direct real-time canal congestion data (e.g. Suez Canal, Panama Canal,
Bosphorus transit queues) is not freely available via public API.  The
Panama Canal Authority and Suez Canal Authority do not offer free
machine-readable vessel position feeds.  AIS (Automatic Identification
System) data aggregators charge commercial rates.

Proxy methodology
-----------------
Freight rate indices — particularly the Baltic Dry Index (BDI) — are the
market's own real-time estimate of shipping congestion and demand.  When
canals congest, alternate routing increases voyage distances and vessel
demand, driving freight rates upward.  The BDI has historically spiked
during major congestion events (Suez blockage March 2021, Panama drought
2023).

Ticker priority:
  1. BDRY  — Breakwave Daily Shipping ETF (most direct BDI proxy, US-listed)
  2. BOAT  — SonicShares Global Shipping ETF
  3. BSEP  — Breakwave Tanker Shipping ETF (tanker focus)

Economic hypothesis
-------------------
A high (positive) z-score in freight rates signals supply chain stress:
higher logistics costs reduce margins for importers, particularly
consumer goods and commodity-dependent manufacturers.  Elevated congestion
typically leads CPI import components by 2–6 weeks (Cerdeiro et al. 2021,
IMF Working Paper).  For small/mid-cap manufacturers and importers, this
is a margin headwind that the market tends to under-price in the immediate
term, consistent with a PEAD-style delayed reaction.

Data source
-----------
yfinance (Yahoo Finance) — free, no API key required.
"""

import logging
from datetime import datetime, timezone

import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

_TICKER_PRIORITY = ["BDRY", "BOAT", "BSEP"]
_ROLLING_WINDOW = 20
_ZSCORE_CLAMP = 3.0


class CanalCongestionCollector:
    """
    Collects freight ETF price data from yfinance as a canal/shipping
    congestion proxy and computes a 20-day rolling z-score.

    Proxy for canal/shipping congestion using freight indices.
    """

    def _fetch_ticker(self, ticker: str) -> tuple:
        """
        Download the last 60 trading days of close prices for a ticker.

        Returns (ticker_str, pd.Series) or raises on failure.
        """
        data = yf.download(ticker, period="60d", progress=False, auto_adjust=True)
        if data.empty:
            raise ValueError(f"No data returned for ticker {ticker}.")
        closes = data["Close"].dropna()
        if len(closes) < _ROLLING_WINDOW + 1:
            raise ValueError(
                f"Insufficient data for {ticker}: {len(closes)} rows "
                f"(need {_ROLLING_WINDOW + 1})."
            )
        return ticker, closes

    def _compute_zscore(self, closes) -> float:
        """
        Compute z-score of latest close vs. 20-day rolling mean and std.

        Uses the most recent value vs. the preceding _ROLLING_WINDOW values.
        """
        values = closes.values.flatten()
        latest = float(values[-1])
        window = values[-(  _ROLLING_WINDOW + 1):-1]

        mean = float(np.mean(window))
        std = float(np.std(window, ddof=1))

        if std < 1e-6:
            logger.warning("Near-zero std in freight data; z-score set to 0.")
            return 0.0

        zscore = (latest - mean) / std
        return float(np.clip(zscore, -_ZSCORE_CLAMP, _ZSCORE_CLAMP))

    def collect(self) -> dict:
        """
        Fetch freight ETF data and compute the congestion z-score.

        Tries tickers in priority order (BDRY → BOAT → BSEP) and uses the
        first that returns sufficient history.

        Returns
        -------
        dict with keys:
            signal_name   : "canal_congestion_index"
            value         : float — z-score clamped to [-3, 3]
            raw_data      : dict — ticker used, latest price, rolling stats
            quality_score : float 0.0–1.0 (decays down the ticker priority list)
            timestamp     : ISO-8601 UTC string
            source        : ticker symbol used
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        # Quality scores: highest for the most direct proxy
        quality_by_rank = {0: 1.0, 1: 0.8, 2: 0.7}

        last_exc = None
        for rank, ticker in enumerate(_TICKER_PRIORITY):
            try:
                used_ticker, closes = self._fetch_ticker(ticker)
                logger.debug(
                    "Using ticker %s (%d rows) for canal congestion proxy.",
                    used_ticker, len(closes),
                )

                zscore = self._compute_zscore(closes)
                values = closes.values.flatten()
                window = values[-(  _ROLLING_WINDOW + 1):-1]

                raw_data = {
                    "ticker": used_ticker,
                    "latest_price": float(values[-1]),
                    "rolling_mean": float(np.mean(window)),
                    "rolling_std": float(np.std(window, ddof=1)),
                    "data_points": len(closes),
                    "tickers_tried": _TICKER_PRIORITY[:rank + 1],
                }

                return {
                    "signal_name": "canal_congestion_index",
                    "value": zscore,
                    "raw_data": raw_data,
                    "quality_score": quality_by_rank.get(rank, 0.6),
                    "timestamp": timestamp,
                    "source": used_ticker,
                }

            except Exception as exc:
                logger.warning("Ticker %s failed: %s", ticker, exc)
                last_exc = exc
                continue

        # All tickers failed
        logger.warning(
            "CanalCongestionCollector.collect: all tickers failed. Last error: %s",
            last_exc,
        )
        return {
            "signal_name": "canal_congestion_index",
            "value": 0.0,
            "raw_data": {"error": str(last_exc), "tickers_tried": _TICKER_PRIORITY},
            "quality_score": 0.0,
            "timestamp": timestamp,
            "source": "none",
        }
