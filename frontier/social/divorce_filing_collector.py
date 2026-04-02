"""
Divorce Filing Collector — Divorce Lead Indicator (DLI).

Economic Hypothesis
-------------------
Household dissolution is a leading indicator for specific consumer expenditure
categories (Kumar & Page 2014).  When one household splits into two, predictable
spending occurs: duplicate furniture, two rental properties, legal and financial
advisory fees, and consumer electronics.  Conversely, luxury spending declines.

These effects lead actual consumer spending by 3–18 months depending on sector:
- Legal services: immediate (1–3 months)
- Home furnishings / consumer electronics: 3–9 months
- Rental housing: 6–18 months
- Luxury goods: negative correlation, 3–9 months

The DLI is a standardised z-score of the current divorce rate relative to a
regional baseline, making it comparable across time and geographies.

Sector sensitivities are defined in SECTOR_SENSITIVITIES (derived_formulas.py).
DLI > +1.5 implies an above-average filing surge → overweight beneficiary sectors.
DLI < -1.5 implies a filing drought → underweight those sectors.

No real-time government divorce data is freely available.  This collector uses
Google Trends search interest for divorce-related legal terms as a high-frequency
proxy.  Fallback: a seasonal model capturing the documented January/February and
August peaks in US divorce filings.

Signal: DLI (standardised z-score).
"""

import logging
import math
import time
from datetime import datetime, timezone

from frontier.equations.derived_formulas import calc_dli, SECTOR_SENSITIVITIES

logger = logging.getLogger(__name__)

_KEYWORDS = ["divorce lawyer", "divorce attorney", "legal separation"]
_BASELINE_MEAN = 0.5
_BASELINE_STD = 0.15
_SLEEP_SECONDS = 45.0

# Months with documented above-average US divorce filing rates
_HIGH_SEASON_MONTHS = {1, 2, 8}


def _seasonal_rate() -> float:
    """
    Seasonal proxy for divorce filing rate.

    January/February and August are documented US peak months.
    Returns a value in [0, 1] representing the normalised rate.
    """
    month = datetime.now().month
    if month in _HIGH_SEASON_MONTHS:
        return 0.65
    # Off-peak: gentle sinusoidal decline, floor at 0.35
    angle = 2 * math.pi * (month - 1) / 12
    return 0.5 + 0.15 * math.cos(angle - math.pi * (1 / 6))


class DivorceFilingCollector:
    """
    Estimate divorce filing rates using Google Trends as a proxy and compute
    the Divorce Lead Indicator (DLI) with associated sector sensitivities.
    """

    def collect(self) -> dict:
        """
        Fetch Google Trends interest for divorce-related keywords, compute
        the normalised rate, and return the DLI standardised score.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        pytrends_succeeded = False
        keyword_values: dict = {}
        rate: float = _BASELINE_MEAN
        source_method = "seasonal_fallback"
        error_msg: str = ""

        try:
            from pytrends.request import TrendReq  # type: ignore

            logger.debug(
                "Sleeping %.1f seconds before pytrends call.", _SLEEP_SECONDS
            )
            time.sleep(_SLEEP_SECONDS)

            pytrends = TrendReq(hl="en-US", tz=360)
            pytrends.build_payload(_KEYWORDS, timeframe="today 3-m")
            iot = pytrends.interest_over_time()

            if iot is None or iot.empty:
                raise ValueError("pytrends returned empty DataFrame")

            for kw in _KEYWORDS:
                if kw in iot.columns:
                    keyword_values[kw] = float(iot[kw].iloc[-1])
                else:
                    keyword_values[kw] = 50.0

            rate = sum(keyword_values.values()) / len(keyword_values) / 100.0
            pytrends_succeeded = True
            source_method = "pytrends"

        except Exception as exc:
            error_msg = str(exc)
            logger.warning(
                "DivorceFilingCollector pytrends failed, using seasonal fallback: %s",
                exc,
            )
            rate = _seasonal_rate()

        dli = calc_dli(
            divorce_rate=rate,
            baseline_mean=_BASELINE_MEAN,
            baseline_std=_BASELINE_STD,
        )

        # Compute sector impacts for reference (months_ahead=6, income_weight=1.0)
        sector_impacts = {
            sector: round(
                dli
                * sensitivity
                * 1.0
                * math.exp(-0.1 * 6),
                4,
            )
            for sector, sensitivity in SECTOR_SENSITIVITIES.items()
        }

        raw_data = {
            "rate": rate,
            "baseline_mean": _BASELINE_MEAN,
            "baseline_std": _BASELINE_STD,
            "dli": dli,
            "source_method": source_method,
            "keyword_values": keyword_values,
            "sector_sensitivities": SECTOR_SENSITIVITIES,
            "sector_impacts_6m": sector_impacts,
        }
        if error_msg:
            raw_data["error"] = error_msg

        quality_score = 1.0 if pytrends_succeeded else 0.0

        return {
            "signal_name": "divorce_anomaly",
            "value": dli,
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": "google_trends_pytrends" if pytrends_succeeded else "seasonal_model",
        }
