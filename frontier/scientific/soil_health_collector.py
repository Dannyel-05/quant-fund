"""
Soil Health Collector — soil_health_degradation Signal.

Proxy-based signal measuring agricultural / food-system stress derived
from the price behaviour of the Invesco DB Agriculture Fund (DBA ETF).

Economic hypothesis
-------------------
Global soil health is the ultimate constraint on agricultural output.
Sustained soil degradation — caused by intensive monoculture, loss of
organic matter, erosion, and desertification — reduces crop yields and
elevates food-system fragility.  While no free real-time global soil
health index exists, agricultural commodity ETF prices embed market
expectations of supply constraints, weather anomalies, and input cost
pressures that are themselves downstream consequences of soil degradation.

Specifically:
  - A sustained FALL in DBA price below its 60-day mean signals that
    agricultural supply is currently perceived as adequate (or demand is
    weak), implying lower near-term food-system stress.
  - A sustained RISE above the 60-day mean signals supply pressure,
    weather-driven shortfalls, or input cost spikes — conditions
    correlated with soil and food-system stress.

This signal is INVERTED relative to price: a negative z-score (falling
ag prices) is interpreted as latent food-system stress building up
without market recognition yet, whereas elevated prices reflect stress
that is already priced.

Relevant investment hypotheses:
  1. Agri-input companies (fertilisers, seeds) — demand rises during
     stress periods.
  2. Food retail margin compression (costs rise before prices can be passed
     through to consumers).
  3. Emerging-market sovereign risk: food-import-dependent countries face
     balance-of-payments pressure when food prices spike.

Proxy note
----------
This is a PROXY signal.  A dedicated soil-health index from FAO or
SoilGrids would improve accuracy but has no free programmatic API.
The DBA z-score captures related economic dynamics but is not a direct
measurement of soil health.

Data source
-----------
yfinance (free, no key required): DBA ETF daily OHLCV
"""

import logging
from datetime import datetime, timezone

import numpy as np

logger = logging.getLogger(__name__)

_ETF_TICKER = "DBA"
_LOOKBACK_DAYS = 90      # fetch window — need >60 trading days
_ZSCORE_WINDOW = 60      # rolling window for z-score computation
_NORMALIZATION_FACTOR = 3.0  # divide |zscore| by this to map to [0,1]
_SOURCE = "yfinance:DBA (Invesco DB Agriculture Fund)"


class SoilHealthCollector:
    """
    Uses the DBA agricultural ETF as a proxy for global food-system and
    soil health stress.

    A falling DBA price relative to its 60-day mean produces a negative
    z-score.  The degradation signal is: max(0, -zscore) / 3, which is
    positive when ag prices are depressed (latent stress) and zero when
    prices are at or above their rolling average.
    """

    def _fetch_dba_prices(self) -> "pd.Series":
        """
        Download DBA closing prices.  Imports yfinance inside the method
        to allow graceful ImportError handling.

        Returns a pandas Series of closing prices, or an empty Series on error.
        """
        try:
            import yfinance as yf
            import pandas as pd

            ticker = yf.Ticker(_ETF_TICKER)
            hist = ticker.history(period=f"{_LOOKBACK_DAYS}d")
            if hist.empty or "Close" not in hist.columns:
                logger.warning("SoilHealthCollector: DBA history is empty.")
                return pd.Series(dtype=float)
            return hist["Close"].dropna()
        except ImportError:
            logger.warning("SoilHealthCollector: yfinance not installed.")
        except Exception as exc:
            logger.warning("SoilHealthCollector: error fetching DBA prices: %s", exc)
        try:
            import pandas as pd
            return pd.Series(dtype=float)
        except ImportError:
            return []

    def collect(self) -> dict:
        """
        Compute the soil health degradation proxy from DBA price z-score.

        The 60-day z-score of DBA is computed.  The degradation signal is
        the inverted negative region: max(0, -zscore) / 3, clamped to [0, 1].

        Returns
        -------
        dict with keys:
            signal_name   : "soil_health_degradation"
            value         : float in [0, 1]
            raw_data      : dict — DBA price stats and z-score
            quality_score : 1.0 if sufficient price data, 0.3 if insufficient
            timestamp     : ISO-8601 UTC string
            source        : data source description
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        prices = self._fetch_dba_prices()

        # Determine whether we have a pandas Series or a plain list
        try:
            n_prices = len(prices)
        except Exception:
            n_prices = 0

        if n_prices < _ZSCORE_WINDOW:
            logger.warning(
                "SoilHealthCollector: only %d price observations, need %d; "
                "returning zero signal.",
                n_prices,
                _ZSCORE_WINDOW,
            )
            return {
                "signal_name": "soil_health_degradation",
                "value": 0.0,
                "raw_data": {
                    "ticker": _ETF_TICKER,
                    "observations": n_prices,
                    "zscore_window": _ZSCORE_WINDOW,
                    "note": "Insufficient data for z-score calculation.",
                },
                "quality_score": 0.3,
                "timestamp": timestamp,
                "source": _SOURCE,
            }

        try:
            # Compute rolling 60-day z-score using the last ZSCORE_WINDOW prices
            window_prices = list(prices)[-_ZSCORE_WINDOW:]
            arr = np.array(window_prices, dtype=float)
            mean_price = float(np.mean(arr))
            std_price = float(np.std(arr))

            latest_price = float(arr[-1])

            if std_price < 1e-9:
                zscore = 0.0
            else:
                zscore = (latest_price - mean_price) / std_price

            # Degradation is highest when DBA is falling (negative z-score)
            # max(0, -zscore) / 3, clamped to [0, 1]
            degradation = min(1.0, max(0.0, -zscore / _NORMALIZATION_FACTOR))

            raw_data = {
                "ticker": _ETF_TICKER,
                "observations": n_prices,
                "zscore_window": _ZSCORE_WINDOW,
                "latest_price": round(latest_price, 4),
                "window_mean": round(mean_price, 4),
                "window_std": round(std_price, 4),
                "zscore": round(zscore, 4),
                "note": (
                    "Proxy using agricultural ETF as soil health / "
                    "food system stress indicator."
                ),
            }

            return {
                "signal_name": "soil_health_degradation",
                "value": float(degradation),
                "raw_data": raw_data,
                "quality_score": 1.0,
                "timestamp": timestamp,
                "source": _SOURCE,
            }

        except Exception as exc:
            logger.warning("SoilHealthCollector: z-score computation failed: %s", exc)
            return {
                "signal_name": "soil_health_degradation",
                "value": 0.0,
                "raw_data": {"ticker": _ETF_TICKER, "error": str(exc)},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": _SOURCE,
            }
