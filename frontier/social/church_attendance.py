"""
Church Attendance Collector — Religious Participation as a Risk-Sentiment Proxy.

Economic Hypothesis
-------------------
Religious participation is a documented counter-cyclical behavior: church
attendance rises during periods of economic stress, unemployment, and
uncertainty (Bentzen 2019, "Acts of God: Religiosity and Natural Disasters").
Elevated religiosity correlates with increased risk-aversion (Kumar et al.
2011, "Religion and Stock Price Crash Risk").

A rising church-attendance signal therefore implies:
1. Elevated population-level risk aversion → demand suppression for equities
2. Potential leading indicator for consumer-spending slowdowns in luxury and
   discretionary sectors
3. Counter-signal for safe-haven assets and defensive sectors

Because no free real-time attendance API exists, Google Trends search interest
for "church near me" and "sunday service" is used as a high-frequency proxy.
Both terms are strongly correlated with actual Sunday attendance intent.

Fallback: a seasonal cosine model captures the well-documented winter peak
in attendance (Christmas, Easter) when pytrends is unavailable.

Signal: church_attendance_signal centred at 0.  Positive = above-average
attendance/interest, negative = below average.
"""

import logging
import math
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_KEYWORDS = ["church near me", "sunday service"]
_SLEEP_SECONDS = 45.0


def _seasonal_fallback() -> float:
    """
    Seasonal cosine proxy for church attendance.

    Peaks in January (month=1, aligned with winter religious observance).
    Amplitude ±0.2 centred at 0.
    """
    month = datetime.now().month
    return 0.2 * math.cos(2 * math.pi * (month - 1) / 12)


class ChurchAttendanceCollector:
    """
    Estimate church attendance as a risk-aversion proxy using Google Trends.

    Primary source: pytrends interest for "church near me" and "sunday service".
    Fallback: seasonal cosine model when pytrends is unavailable or fails.
    """

    def collect(self) -> dict:
        """
        Fetch Google Trends interest for church-related keywords, compute a
        normalised attendance signal centred at zero.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()

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

            latest_values = {}
            for kw in _KEYWORDS:
                if kw in iot.columns:
                    latest_values[kw] = float(iot[kw].iloc[-1])
                else:
                    latest_values[kw] = 50.0

            mean_interest = sum(latest_values.values()) / len(latest_values)
            # Centre at 0: (latest / 100) - 0.5
            signal = (mean_interest / 100.0) - 0.5

            raw_data = {
                "keyword_latest_values": latest_values,
                "mean_interest": mean_interest,
                "source_method": "pytrends",
            }

            return {
                "signal_name": "church_attendance_signal",
                "value": signal,
                "raw_data": raw_data,
                "quality_score": 1.0,
                "timestamp": timestamp,
                "source": "google_trends_pytrends",
            }

        except Exception as exc:
            logger.warning(
                "ChurchAttendanceCollector pytrends failed, using seasonal fallback: %s",
                exc,
            )
            signal = _seasonal_fallback()

            raw_data = {
                "error": str(exc),
                "fallback_method": "seasonal_cosine",
                "month": datetime.now().month,
                "source_method": "seasonal_fallback",
            }

            return {
                "signal_name": "church_attendance_signal",
                "value": signal,
                "raw_data": raw_data,
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": "seasonal_cosine_model",
            }
