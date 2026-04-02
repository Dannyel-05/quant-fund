"""
HQ Traffic Monitor — Corporate Headquarters Foot Traffic Index.

Economic Hypothesis
-------------------
Physical office occupancy is a real-time proxy for corporate operational
intensity and employee engagement.  Companies with high HQ foot traffic tend
to have more coordinated decision-making, faster execution cycles, and stronger
cultural cohesion.  Conversely, companies that have shifted to remote-first
working may exhibit lower near-term productivity but higher talent retention
in certain sectors.

The signal is particularly relevant for:
- Financial services and law firms (high face-time culture)
- Technology companies (where remote work varies widely)
- Real estate exposure (office REIT sector performance)

Proxy using office REIT performance and WFH trend data.

No free, real-time corporate HQ foot traffic API exists.  This collector
constructs a composite proxy signal from two complementary free sources:

1. Office REIT Z-score: The 20-day rolling price z-score of VNQ (Vanguard
   Real Estate ETF), used as a broad office demand proxy.  Rising VNQ with
   positive z-score implies increasing office utilisation sentiment.

2. WFH Anti-signal: Google Trends interest for "work from home" normalised
   0-1.  High WFH search interest is an anti-signal to physical office
   occupancy — workers searching for WFH tips are not commuting.

Combined: hq_traffic = office_reit_zscore × (1 - wfh_signal)

A positive hq_traffic score implies rising office occupancy sentiment.
A negative score implies declining occupancy (remote shift or economic stress).

Signal: hq_traffic_index (real-valued, centred near zero under normal conditions).
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_VNQ_TICKER = "VNQ"
_WFH_KEYWORDS = ["work from home"]
_ZSCORE_WINDOW = 20
_SLEEP_SECONDS = 45.0


def _fetch_vnq_zscore() -> tuple:
    """
    Fetch VNQ closing prices via yfinance and compute the 20-day z-score
    of the most recent close.

    Returns (zscore, raw_prices_list, error_msg).
    """
    try:
        import yfinance as yf  # type: ignore

        ticker = yf.Ticker(_VNQ_TICKER)
        hist = ticker.history(period="3mo")

        if hist is None or hist.empty or "Close" not in hist.columns:
            return 0.0, [], "yfinance returned empty history"

        closes = hist["Close"].dropna().tolist()
        if len(closes) < _ZSCORE_WINDOW:
            return 0.0, closes, f"Insufficient data: {len(closes)} days"

        window = closes[-_ZSCORE_WINDOW:]
        mean = sum(window) / len(window)
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std = variance ** 0.5

        if std == 0:
            return 0.0, closes, "Zero std — prices flat"

        latest = closes[-1]
        zscore = (latest - mean) / std
        return float(zscore), closes[-_ZSCORE_WINDOW:], ""

    except Exception as exc:
        logger.warning("VNQ yfinance fetch failed: %s", exc)
        return 0.0, [], str(exc)


def _fetch_wfh_signal() -> tuple:
    """
    Fetch Google Trends interest for "work from home" via pytrends.

    Returns (normalised_signal 0-1, latest_value, error_msg).
    """
    try:
        from pytrends.request import TrendReq  # type: ignore

        logger.debug(
            "Sleeping %.1f seconds before pytrends WFH call.", _SLEEP_SECONDS
        )
        time.sleep(_SLEEP_SECONDS)

        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload(_WFH_KEYWORDS, timeframe="today 3-m")
        iot = pytrends.interest_over_time()

        if iot is None or iot.empty:
            raise ValueError("pytrends returned empty DataFrame")

        kw = _WFH_KEYWORDS[0]
        if kw not in iot.columns:
            raise ValueError(f"Keyword '{kw}' not in pytrends response")

        latest_val = float(iot[kw].iloc[-1])
        normalised = latest_val / 100.0
        return normalised, latest_val, ""

    except Exception as exc:
        logger.warning("WFH pytrends fetch failed: %s", exc)
        return 0.5, 50.0, str(exc)  # neutral fallback


class HQTrafficMonitor:
    """
    Estimate corporate HQ foot traffic using a composite proxy signal
    derived from office REIT performance (VNQ) and remote-work sentiment
    (Google Trends "work from home").

    Proxy using office REIT performance and WFH trend data.
    """

    def collect(self, tickers: Optional[list] = None) -> dict:
        """
        Compute the HQ Traffic Index as:
            hq_traffic = office_reit_zscore × (1 − wfh_signal)

        Parameters
        ----------
        tickers : not used in this collector; accepted for API consistency.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        office_reit_zscore, vnq_prices, vnq_error = _fetch_vnq_zscore()
        wfh_signal, wfh_raw, wfh_error = _fetch_wfh_signal()

        hq_traffic = office_reit_zscore * (1.0 - wfh_signal)

        # Quality score: 1.0 if both sources succeeded, 0.5 if one failed,
        # 0.0 if both failed
        sources_ok = sum([vnq_error == "", wfh_error == ""])
        quality_score = sources_ok / 2.0

        raw_data = {
            "office_reit_ticker": _VNQ_TICKER,
            "office_reit_zscore": office_reit_zscore,
            "vnq_window_prices": vnq_prices,
            "vnq_error": vnq_error,
            "wfh_signal_normalised": wfh_signal,
            "wfh_raw_value": wfh_raw,
            "wfh_error": wfh_error,
            "formula": "hq_traffic = office_reit_zscore * (1 - wfh_signal)",
        }

        return {
            "signal_name": "hq_traffic_index",
            "value": hq_traffic,
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": "yfinance_vnq+google_trends_wfh",
        }
