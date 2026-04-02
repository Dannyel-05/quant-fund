import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)

# Sector -> ticker mapping (fallback)
SECTOR_TICKERS: Dict[str, List[str]] = {
    "mining":       ["RIO", "GLEN", "AAL", "FCX", "BHP", "VALE"],
    "retail":       ["M", "KSS", "JWN", "NEXT", "MKS", "WMT", "TGT", "AMZN"],
    "agriculture":  ["ADM", "BG", "DE", "NTR"],
    "steel":        ["X", "NUE", "STLD", "MT"],
    "energy":       ["XOM", "CVX", "BP", "SHEL", "COP"],
    "consumer_goods": ["PG", "UL", "KMB", "CL"],
    "transport":    ["FDX", "UPS", "JBHT", "XPO"],
}

# Reverse map: ticker -> sector
_TICKER_TO_SECTOR: Dict[str, str] = {}
for _sector, _tickers in SECTOR_TICKERS.items():
    for _t in _tickers:
        _TICKER_TO_SECTOR[_t.upper()] = _sector


def _ticker_sector(ticker: str, provided_sector: Optional[str] = None) -> Optional[str]:
    if provided_sector:
        return provided_sector.lower()
    return _TICKER_TO_SECTOR.get(ticker.upper())


def _zscore(series: np.ndarray) -> float:
    """Return z-score of the last element vs the rest of the series."""
    if len(series) < 2:
        return 0.0
    history = series[:-1]
    mu = history.mean()
    sigma = history.std()
    if sigma < 1e-8:
        return 0.0
    return float((series[-1] - mu) / sigma)


def _rate_of_change(series: np.ndarray, lookback: int = 20) -> float:
    """(current - N periods ago) / N periods ago"""
    if len(series) < lookback + 1:
        return 0.0
    past = series[-(lookback + 1)]
    current = series[-1]
    if abs(past) < 1e-8:
        return 0.0
    return float((current - past) / abs(past))


def _acceleration(series: np.ndarray, lookback: int = 20) -> float:
    """Difference between current RoC and previous RoC."""
    if len(series) < lookback * 2 + 1:
        return 0.0
    roc_current = _rate_of_change(series, lookback)
    roc_prev = _rate_of_change(series[:-lookback], lookback)
    return roc_current - roc_prev


def _fetch_yfinance_close(symbol: str, days: int = 365) -> np.ndarray:
    """Fetch closing prices for a yfinance symbol. Returns empty array on error."""
    try:
        import yfinance as yf
        end = datetime.now()
        start = end - timedelta(days=days)
        df = yf.download(symbol, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("No data returned for symbol %s", symbol)
            return np.array([])
        closes = df["Close"].dropna().values.flatten()
        return closes.astype(float)
    except Exception as exc:
        logger.warning("Failed to fetch %s from yfinance: %s", symbol, exc)
        return np.array([])


def _bdi_sector_impact(z: float, roc: float) -> Dict[str, float]:
    """Return per-sector impact scores based on BDI z-score and rate-of-change."""
    impacts: Dict[str, float] = {}
    if z > 1.0:
        impacts["mining"] = 0.4
        impacts["agriculture"] = 0.4
        impacts["steel"] = 0.4
        impacts["retail"] = -0.2
    elif z < -1.0:
        impacts["mining"] = -0.3
        impacts["retail"] = 0.2
    else:
        # Mild signal — scale linearly
        impacts["mining"] = 0.4 * (z / 1.0) if z > 0 else -0.3 * (abs(z) / 1.0)
        impacts["retail"] = -0.2 * (z / 1.0) if z > 0 else 0.2 * (abs(z) / 1.0)
        impacts["agriculture"] = 0.4 * (z / 1.0) if z > 0 else 0.0
        impacts["steel"] = 0.4 * (z / 1.0) if z > 0 else 0.0
    return impacts


def _container_sector_impact(z: float) -> Dict[str, float]:
    """Return per-sector impact from container shipping rates."""
    impacts: Dict[str, float] = {}
    if z > 1.0:
        impacts["retail"] = -0.4
        impacts["consumer_goods"] = -0.4
    elif z < -1.0:
        impacts["retail"] = 0.3
        impacts["consumer_goods"] = 0.3
    else:
        impacts["retail"] = -0.4 * (z / 1.0) if z > 0 else 0.3 * (abs(z) / 1.0)
        impacts["consumer_goods"] = impacts["retail"]
    return impacts


LAG_DISCOUNT = 0.7  # 90-day forward-looking discount


class ShippingCollector:
    """
    Collect shipping index data (BDI + container proxy) and compute
    ShippingPressureScore per ticker.
    """

    BDI_SYMBOL = "^BDI"
    CONTAINER_FALLBACK_SYMBOL = "^GSPC"

    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        """
        Returns list of dicts conforming to the altdata interface:
        {source, ticker, market, data_type, value (-1 to +1),
         raw_data (dict), timestamp (ISO str), quality_score (0-1)}
        """
        timestamp = datetime.now().isoformat()
        results: List[dict] = []

        # --- Fetch BDI ---
        bdi_metrics = self._compute_index_metrics(self.BDI_SYMBOL, label="BDI")
        container_metrics = self._compute_index_metrics(
            self.CONTAINER_FALLBACK_SYMBOL, label="Container"
        )

        # Build combined sector impact maps
        bdi_impacts = _bdi_sector_impact(
            bdi_metrics.get("z_score_52w", 0.0),
            bdi_metrics.get("rate_of_change_4w", 0.0),
        )
        container_impacts = _container_sector_impact(
            container_metrics.get("z_score_52w", 0.0)
        )

        quality_score = self._overall_quality(bdi_metrics, container_metrics)

        for ticker in tickers:
            try:
                sector = _ticker_sector(
                    ticker, self.config.get("sectors", {}).get(ticker)
                )
                score = self._compute_shipping_pressure(
                    ticker, sector, bdi_impacts, container_impacts
                )
                # Apply 90-day lag discount
                value = max(-1.0, min(1.0, score * LAG_DISCOUNT))

                raw: dict = {
                    "sector": sector,
                    "bdi": bdi_metrics,
                    "container": container_metrics,
                    "bdi_sector_impacts": bdi_impacts,
                    "container_sector_impacts": container_impacts,
                    "raw_score": score,
                    "lag_discount": LAG_DISCOUNT,
                }
                results.append({
                    "source": "shipping",
                    "ticker": ticker,
                    "market": market,
                    "data_type": "shipping_pressure",
                    "value": value,
                    "raw_data": raw,
                    "timestamp": timestamp,
                    "quality_score": quality_score,
                })
            except Exception as exc:
                logger.error("ShippingCollector error for ticker %s: %s", ticker, exc)
                results.append(self._empty_result(ticker, market, timestamp, str(exc)))

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_index_metrics(self, symbol: str, label: str) -> dict:
        """Fetch price series for symbol and compute analytics."""
        series = _fetch_yfinance_close(symbol, days=365)
        if len(series) < 30:
            logger.warning("%s (%s): insufficient data (%d points)", label, symbol, len(series))
            return {"symbol": symbol, "label": label, "error": "insufficient_data",
                    "z_score_52w": 0.0, "rate_of_change_4w": 0.0, "acceleration": 0.0,
                    "current": None, "ma_4w": None}

        current = float(series[-1])
        # 4-week MA (approx 20 trading days)
        ma_4w = float(series[-20:].mean()) if len(series) >= 20 else current
        # 52-week z-score
        z_52w = _zscore(series)
        # Rate of change vs 4 weeks ago
        roc_4w = _rate_of_change(series, lookback=20)
        # Acceleration
        accel = _acceleration(series, lookback=20)

        return {
            "symbol": symbol,
            "label": label,
            "current": current,
            "ma_4w": ma_4w,
            "z_score_52w": z_52w,
            "rate_of_change_4w": roc_4w,
            "acceleration": accel,
            "data_points": len(series),
        }

    def _compute_shipping_pressure(
        self,
        ticker: str,
        sector: Optional[str],
        bdi_impacts: Dict[str, float],
        container_impacts: Dict[str, float],
    ) -> float:
        """Combine BDI and container impacts for a given ticker/sector."""
        if not sector:
            return 0.0

        bdi_impact = bdi_impacts.get(sector, 0.0)
        container_impact = container_impacts.get(sector, 0.0)
        # Simple average of both signals
        return (bdi_impact + container_impact) / 2.0

    def _overall_quality(self, bdi: dict, container: dict) -> float:
        """Heuristic quality score based on data availability."""
        bdi_ok = "error" not in bdi
        container_ok = "error" not in container
        if bdi_ok and container_ok:
            return 0.85
        if bdi_ok or container_ok:
            return 0.5
        return 0.1

    def _empty_result(self, ticker: str, market: str, timestamp: str, error: str) -> dict:
        return {
            "source": "shipping",
            "ticker": ticker,
            "market": market,
            "data_type": "shipping_pressure",
            "value": 0.0,
            "raw_data": {"error": error},
            "timestamp": timestamp,
            "quality_score": 0.0,
        }
