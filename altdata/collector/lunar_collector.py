import logging
import math
from datetime import date, datetime, timedelta
from typing import Dict, List

import ephem

logger = logging.getLogger(__name__)


class LunarCollector:
    """
    Compute lunar cycle metrics for each requested ticker.
    Uses the ephem library — pure Python, no API calls required.
    Pre-computes a date range on startup via precompute_range().
    """

    def __init__(self, config: dict):
        self.config = config
        self._cache: Dict[str, dict] = {}  # date_str -> lunar_data dict

    def get_lunar_data(self, for_date: date) -> dict:
        """Compute lunar metrics for a given date. Cached."""
        key = str(for_date)
        if key in self._cache:
            return self._cache[key]

        try:
            d = ephem.Date(for_date.strftime("%Y/%m/%d"))
            moon = ephem.Moon(d)

            # Phase: illumination fraction 0.0-1.0
            phase_raw = moon.phase / 100.0

            # Days to/from full moon
            next_full = ephem.next_full_moon(d)
            prev_full = ephem.previous_full_moon(d)
            days_to_full = float(next_full - d)
            days_since_full = float(d - prev_full)
            days_from_full = min(days_to_full, days_since_full)

            # Days to/from new moon
            next_new = ephem.next_new_moon(d)
            prev_new = ephem.previous_new_moon(d)
            days_to_new = float(next_new - d)
            days_from_new = min(days_to_new, float(d - prev_new))

            # Moon distance in AU (perigee when small)
            moon_distance = float(moon.earth_distance)

            # Phase encoding: cyclical sin/cos
            phase_angle = phase_raw * 2 * math.pi
            phase_sin = math.sin(phase_angle)
            phase_cos = math.cos(phase_angle)

            # LunarSignal: academic literature suggests slight positive at new moon
            # cos peaks at 0 (new moon) and troughs at pi (full moon)
            lunar_signal = math.cos(phase_angle)
            # Scale to [-0.1, +0.1] — effect is ~3-4 bp/day in literature
            lunar_signal_scaled = lunar_signal * 0.1

            result = {
                "phase_illumination": phase_raw,
                "phase_sin": phase_sin,
                "phase_cos": phase_cos,
                "days_to_full": days_to_full,
                "days_from_full": days_from_full,
                "days_from_new": days_from_new,
                "moon_distance_au": moon_distance,
                "lunar_signal": lunar_signal_scaled,
                "is_full_moon": days_from_full < 1.0,
                "is_new_moon": days_from_new < 1.0,
            }
        except Exception as exc:
            logger.error("LunarCollector: ephem computation failed for %s: %s", for_date, exc)
            result = {
                "phase_illumination": 0.5,
                "phase_sin": 0.0,
                "phase_cos": 1.0,
                "days_to_full": 7.0,
                "days_from_full": 7.0,
                "days_from_new": 7.0,
                "moon_distance_au": 0.00257,
                "lunar_signal": 0.0,
                "is_full_moon": False,
                "is_new_moon": False,
                "error": str(exc),
            }

        self._cache[key] = result
        return result

    def precompute_range(self, start: date, end: date) -> None:
        """Pre-compute for entire date range — call once at startup."""
        current = start
        while current <= end:
            self.get_lunar_data(current)
            current += timedelta(days=1)
        logger.info(
            "LunarCollector: pre-computed %d dates (%s to %s)",
            (end - start).days + 1,
            start,
            end,
        )

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        """Return today's lunar data for all tickers (same value for all)."""
        today = date.today()
        data = self.get_lunar_data(today)
        timestamp = datetime.now().isoformat()
        results: List[dict] = []

        for ticker in tickers:
            results.append({
                "source": "lunar",
                "ticker": ticker,
                "market": market,
                "data_type": "lunar_cycle",
                "value": float(data["lunar_signal"]),
                "raw_data": dict(data),
                "timestamp": timestamp,
                "quality_score": 0.3,  # speculative signal
            })

        return results
