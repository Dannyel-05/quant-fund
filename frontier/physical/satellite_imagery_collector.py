"""
Satellite Imagery Activity Proxy Collector — Retail Volume Anomaly Signal.

Estimates ground-level commercial/retail activity from publicly observable
market data as a proxy for the parking lot occupancy and foot traffic signals
derived from commercial satellite imagery services.

Note on data availability
--------------------------
True commercial satellite imagery for trading purposes requires subscriptions
to providers such as RS Metrics (parking lot car counts), Orbital Insight
(multi-site foot traffic), or Planet Labs (daily imaging of target sites).
These services charge commercial rates typically starting at $50,000+/year.
No free API for this data exists as of 2026.

Proxy methodology
-----------------
Retail ETF trading volume is a high-frequency, freely available proxy for
aggregate consumer-sector activity intensity.  When foot traffic at retail
locations surges (e.g. Black Friday, back-to-school), institutional investors
trade retail ETFs more actively, elevating volume.  Conversely, demand shocks
(recessions, pandemic closures) suppress both physical foot traffic AND ETF
volume simultaneously.

XRT (SPDR S&P Retail ETF) is used as the primary proxy:
  - Tracks the S&P Retail Select Industry Index (broad US retail)
  - High liquidity (~$3–10B daily notional), minimal tracking error
  - Volume closely correlated with sector-specific news flow and
    consumer spending data releases

The signal is INVERTED: high volume (high activity) → low "activity drop" value.
A value near +1.0 means volume is significantly below average (activity drop).
A value near -1.0 means volume is significantly above average (activity surge).

Economic hypothesis
-------------------
Abnormal retail ETF volume predicts short-term momentum and mean-reversion
in consumer discretionary stocks (consistent with Gervais, Kaniel & Mingelgrin
2001: high-volume stocks earn positive abnormal returns over the next week).
The inverted framing aligns with satellite imagery convention where an
"activity drop" signal triggers defensive positioning in affected retail names.

Data source
-----------
yfinance (Yahoo Finance) — free, no API key required.
Tickers: XRT (primary), XLY (fallback — Consumer Discretionary Select SPDR).
"""

import logging
from datetime import datetime, timezone

import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

_DEFAULT_TICKERS = ["XRT"]
_FALLBACK_TICKER = "XLY"
_ROLLING_WINDOW = 20
_ZSCORE_CLAMP = 3.0


class SatelliteImageryCollector:
    """
    Collects retail ETF volume data as a satellite imagery / foot traffic proxy.

    True satellite imagery requires commercial vendor (RS Metrics, Orbital
    Insight).  This is a retail volume proxy using XRT or XLY.

    The returned value is the INVERTED z-score:
      positive → below-average volume (potential activity drop)
      negative → above-average volume (activity surge)
    """

    def _fetch_volume(self, ticker: str) -> tuple:
        """
        Download the last 60 trading days of volume for a ticker.

        Returns (ticker_str, volume_series) or raises on failure.
        """
        data = yf.download(ticker, period="60d", progress=False, auto_adjust=True)
        if data.empty:
            raise ValueError(f"No data returned for ticker {ticker}.")
        volume = data["Volume"].dropna()
        if len(volume) < _ROLLING_WINDOW + 1:
            raise ValueError(
                f"Insufficient volume data for {ticker}: {len(volume)} rows "
                f"(need {_ROLLING_WINDOW + 1})."
            )
        return ticker, volume

    def _compute_volume_zscore(self, volume) -> float:
        """
        Compute z-score of the latest day's volume vs. the prior 20-day window.
        """
        values = volume.values.flatten().astype(float)
        latest = values[-1]
        window = values[-(  _ROLLING_WINDOW + 1):-1]

        mean = float(np.mean(window))
        std = float(np.std(window, ddof=1))

        if std < 1e-6:
            logger.warning("Near-zero std in volume data; z-score set to 0.")
            return 0.0

        zscore = (latest - mean) / std
        return float(np.clip(zscore, -_ZSCORE_CLAMP, _ZSCORE_CLAMP))

    def collect(self, tickers: list = None) -> dict:
        """
        Fetch retail ETF volume and compute the inverted activity z-score.

        Parameters
        ----------
        tickers : list of str, optional
            Ticker symbols to try in order.  Defaults to ["XRT"].
            If all provided tickers fail, falls back to XLY.

        Returns
        -------
        dict with keys:
            signal_name   : "satellite_activity_drop"
            value         : float — (-1 * volume_zscore), clamped to [-3, 3]
                            Positive = below-average volume (activity drop)
                            Negative = above-average volume (activity surge)
            raw_data      : dict — ticker used, volume stats
            quality_score : float 0.0–1.0
            timestamp     : ISO-8601 UTC string
            source        : ticker symbol used
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        if tickers is None:
            tickers = list(_DEFAULT_TICKERS)

        # Append fallback if not already in list
        if _FALLBACK_TICKER not in tickers:
            tickers = tickers + [_FALLBACK_TICKER]

        # Quality degrades for fallback ticker
        quality_by_rank = {0: 1.0, 1: 0.8}

        last_exc = None
        for rank, ticker in enumerate(tickers):
            try:
                used_ticker, volume = self._fetch_volume(ticker)
                logger.debug(
                    "Using ticker %s (%d rows) for satellite imagery proxy.",
                    used_ticker, len(volume),
                )

                vol_zscore = self._compute_volume_zscore(volume)
                # Invert: high volume = low activity drop score
                activity_drop = -1.0 * vol_zscore

                values = volume.values.flatten().astype(float)
                window = values[-(  _ROLLING_WINDOW + 1):-1]

                raw_data = {
                    "ticker": used_ticker,
                    "latest_volume": float(values[-1]),
                    "rolling_mean_volume": float(np.mean(window)),
                    "rolling_std_volume": float(np.std(window, ddof=1)),
                    "volume_zscore": vol_zscore,
                    "data_points": len(volume),
                    "tickers_tried": tickers[:rank + 1],
                }

                return {
                    "signal_name": "satellite_activity_drop",
                    "value": float(activity_drop),
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
            "SatelliteImageryCollector.collect: all tickers failed. Last error: %s",
            last_exc,
        )
        return {
            "signal_name": "satellite_activity_drop",
            "value": 0.0,
            "raw_data": {"error": str(last_exc), "tickers_tried": tickers},
            "quality_score": 0.0,
            "timestamp": timestamp,
            "source": "none",
        }
