"""
Market impact model: square root model + VWAP scheduling.

impact = η * σ * √(Q / V)
η = 0.15 (US), 0.20 (UK)
σ = 30-day realised volatility
Q = order quantity (shares)
V = average daily volume
"""
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_ETA = {"us": 0.15, "uk": 0.20}
_VWAP_SLICES = [
    (("09:30", "10:00"), 0.15),
    (("10:00", "11:30"), 0.20),
    (("11:30", "14:00"), 0.15),
    (("14:00", "15:00"), 0.15),
    (("15:00", "16:00"), 0.20),
    (("16:00", "16:00"), 0.15),
]


class MarketImpactModel:
    """
    Estimates market impact cost and returns VWAP schedule.
    """

    def estimate_impact(
        self,
        ticker: str,
        quantity: float,
        price: float,
        avg_daily_volume: float,
        realised_vol: float,
        market: str = "us",
    ) -> Dict:
        """
        Returns impact dict with keys:
            impact_pct   : estimated market impact as fraction of price
            impact_usd   : estimated $ cost
            participation: Q/V participation rate
        """
        try:
            eta = _ETA.get(market, 0.15)
            if avg_daily_volume <= 0 or price <= 0:
                return self._zero()
            participation = quantity / avg_daily_volume
            impact_pct = eta * realised_vol * (participation ** 0.5)
            impact_usd = impact_pct * quantity * price
            return {
                "impact_pct": round(impact_pct, 6),
                "impact_usd": round(impact_usd, 2),
                "participation": round(participation, 4),
                "eta": eta,
                "realised_vol": realised_vol,
            }
        except Exception as exc:
            logger.warning("MarketImpactModel.estimate_impact: %s", exc)
            return self._zero()

    def vwap_schedule(
        self,
        ticker: str,
        total_quantity: float,
        market: str = "us",
    ) -> list:
        """
        Returns a list of dicts with {window_start, window_end, quantity}
        representing the VWAP slices for the order.
        """
        schedule = []
        remaining = total_quantity
        for (start, end), fraction in _VWAP_SLICES:
            qty = round(total_quantity * fraction, 2)
            qty = min(qty, remaining)
            schedule.append({
                "window_start": start,
                "window_end": end,
                "quantity": qty,
                "fraction": fraction,
            })
            remaining -= qty
            if remaining <= 0:
                break
        return schedule

    def fetch_vol_and_adv(self, ticker: str) -> Dict:
        """
        Fetch 30-day realised vol and average daily volume via yfinance.
        Returns {"realised_vol": float, "avg_daily_volume": float} or defaults.
        """
        try:
            import numpy as np
            import yfinance as yf
            t = yf.Ticker(ticker)
            hist = t.history(period="30d")
            if hist.empty:
                return {"realised_vol": 0.02, "avg_daily_volume": 1_000_000}
            closes = hist["Close"].dropna()
            rets = closes.pct_change().dropna()
            vol = float(rets.std() * (252 ** 0.5)) if len(rets) > 1 else 0.02
            adv = float(hist["Volume"].mean()) if "Volume" in hist else 1_000_000
            return {"realised_vol": max(vol, 0.005), "avg_daily_volume": max(adv, 1)}
        except Exception as exc:
            logger.warning("MarketImpactModel.fetch_vol_and_adv(%s): %s", ticker, exc)
            return {"realised_vol": 0.02, "avg_daily_volume": 1_000_000}

    def _zero(self) -> Dict:
        return {"impact_pct": 0.0, "impact_usd": 0.0, "participation": 0.0}
