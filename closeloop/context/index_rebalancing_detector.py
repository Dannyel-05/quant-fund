"""
Index rebalancing detector.

Estimates probability that a ticker will be added to or removed from
major indices (Russell 2000, S&P 600) based on market cap proximity to
index thresholds. Generates rebalancing signals.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Russell 2000: approximate market cap range (USD)
_RUSSELL_2000_MIN = 300_000_000     # $300M
_RUSSELL_2000_MAX = 10_000_000_000  # $10B (approximate upper cutoff)

# S&P 600 SmallCap: approximate range
_SP600_MIN = 300_000_000
_SP600_MAX = 6_000_000_000

# Proximity threshold for rebalancing signal (within 20% of boundary)
_PROXIMITY_PCT = 0.20

# Russell rebalancing: last Friday of June
_RUSSELL_REBALANCE_MONTH = 6
_RUSSELL_REBALANCE_DAY_RANGE = (24, 30)  # last week of June


class IndexRebalancingDetector:
    """
    Detects proximity to index inclusion/exclusion thresholds.
    Generates directional signals for anticipated rebalancing flows.
    """

    def __init__(self, store=None):
        self.store = store

    def detect(self, ticker: str, market: str = "us") -> Dict:
        """
        Returns:
            inclusion_probability : float [0, 1] — probability of imminent inclusion
            exclusion_probability : float [0, 1] — probability of imminent exclusion
            nearest_index         : str — which index boundary is closest
            signal_direction      : int — +1 inclusion buy, -1 exclusion sell, 0 neutral
            market_cap            : float or None
            notes                 : str
        """
        if market != "us":
            return self._neutral(ticker, "Non-US market not modelled")

        info = self._fetch_info(ticker)
        if not info:
            return self._neutral(ticker, "Could not fetch ticker info")

        cap = info.get("market_cap")
        if not cap or cap <= 0:
            return self._neutral(ticker, "Market cap unavailable")

        r2k_inc, r2k_exc, r2k_note = self._check_index(
            cap, _RUSSELL_2000_MIN, _RUSSELL_2000_MAX, "Russell 2000"
        )
        sp6_inc, sp6_exc, sp6_note = self._check_index(
            cap, _SP600_MIN, _SP600_MAX, "S&P 600"
        )

        # Take the higher of the two index signals
        inc_prob = max(r2k_inc, sp6_inc)
        exc_prob = max(r2k_exc, sp6_exc)
        nearest = r2k_note if max(r2k_inc, r2k_exc) >= max(sp6_inc, sp6_exc) else sp6_note

        direction = 0
        if inc_prob >= 0.40:
            direction = 1
        elif exc_prob >= 0.40:
            direction = -1

        return {
            "ticker": ticker,
            "inclusion_probability": round(inc_prob, 3),
            "exclusion_probability": round(exc_prob, 3),
            "nearest_index": nearest,
            "signal_direction": direction,
            "market_cap": cap,
            "notes": f"{r2k_note} | {sp6_note}",
        }

    def batch_detect(self, tickers: List[str], market: str = "us") -> List[Dict]:
        """Run detection for a list of tickers."""
        results = []
        for ticker in tickers:
            try:
                results.append(self.detect(ticker, market))
            except Exception as exc:
                logger.warning("IndexRebalancingDetector.batch(%s): %s", ticker, exc)
        return results

    def _check_index(
        self, cap: float, min_cap: float, max_cap: float, index_name: str
    ) -> tuple:
        """Returns (inclusion_prob, exclusion_prob, note)."""
        lower_zone = min_cap * (1 + _PROXIMITY_PCT)   # near lower boundary from inside
        upper_zone = max_cap * (1 - _PROXIMITY_PCT)   # near upper boundary from inside
        below_zone = min_cap * (1 - _PROXIMITY_PCT)   # near lower boundary from outside
        above_zone = max_cap * (1 + _PROXIMITY_PCT)   # near upper boundary from outside

        # Inside index range
        if min_cap <= cap <= max_cap:
            if cap <= lower_zone:
                # Near lower boundary — exclusion risk
                proximity = 1 - (cap - min_cap) / (min_cap * _PROXIMITY_PCT)
                return 0.0, max(0.0, min(0.8, proximity * 0.7)), f"{index_name}:exclusion_risk"
            elif cap >= upper_zone:
                # Near upper boundary — graduation (exits Russell 2000)
                proximity = (cap - upper_zone) / (max_cap * _PROXIMITY_PCT)
                return 0.0, max(0.0, min(0.6, proximity * 0.5)), f"{index_name}:graduation_risk"
            return 0.0, 0.0, f"{index_name}:safely_inside"

        # Below index — potential inclusion
        if below_zone <= cap < min_cap:
            proximity = (cap - below_zone) / (min_cap * _PROXIMITY_PCT)
            return max(0.0, min(0.7, proximity * 0.6)), 0.0, f"{index_name}:inclusion_candidate"

        # Above index — already graduated or excluded
        if max_cap < cap <= above_zone:
            return 0.0, 0.0, f"{index_name}:above_range"

        return 0.0, 0.0, f"{index_name}:out_of_range"

    def _fetch_info(self, ticker: str) -> Optional[Dict]:
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info or {}
            return {"market_cap": info.get("marketCap")}
        except Exception as exc:
            logger.debug("IndexRebalancingDetector.fetch(%s): %s", ticker, exc)
            return None

    def _neutral(self, ticker: str, note: str) -> Dict:
        return {
            "ticker": ticker,
            "inclusion_probability": 0.0,
            "exclusion_probability": 0.0,
            "nearest_index": "N/A",
            "signal_direction": 0,
            "market_cap": None,
            "notes": note,
        }
