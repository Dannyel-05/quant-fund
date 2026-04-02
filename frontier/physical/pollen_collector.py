"""
Pollen Stress Index Collector.

Measures ambient airborne pollen concentration across major pollen types as a
proxy for seasonal biological stress on the human population.

Economic hypothesis
-------------------
High pollen counts cause allergic rhinitis in roughly 30% of the population
(WHO estimates).  Symptoms include impaired sleep, reduced cognitive performance,
and elevated fatigue — all of which have been linked to suboptimal financial
decision-making (Lam 2001; Parker & Tavassoli 2000).  Seasonal allergy peaks
(spring birch/alder, summer grass, autumn ragweed) may systematically depress
investor risk appetite and attention quality in affected regions.

This is a speculative signal with no peer-reviewed financial literature to date.
It is classified as a biological stressor proxy, similar in mechanism to the
SAD / daylight hypothesis (Kamstra, Kramer & Levi 2003).

Pollen types covered
--------------------
alder, birch, grass, mugwort, olive, ragweed — the six types provided by
the Open-Meteo Air Quality API.  Each has a distinct seasonal peak that
varies by latitude and climate zone.

Data source
-----------
Open-Meteo Air Quality API: https://air-quality-api.open-meteo.com/v1/air-quality
Free, no API key required.  Forecast resolution: hourly.
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
_POLLEN_TYPES = [
    "alder_pollen",
    "birch_pollen",
    "grass_pollen",
    "mugwort_pollen",
    "olive_pollen",
    "ragweed_pollen",
]
_NORMALISATION_FACTOR = 1000.0


class PollenCollector:
    """
    Collects hourly pollen concentration data from Open-Meteo Air Quality API.

    Pollen values are measured in grains/m³.  The six types are summed for
    the current forecast hour and normalised by 1000 to produce an index
    that is typically in [0, 1] during moderate seasons and may exceed 1.0
    during peak bloom events.
    """

    def _fetch_pollen(self, lat: float, lon: float) -> dict:
        """Fetch hourly pollen forecast from Open-Meteo Air Quality API."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(_POLLEN_TYPES),
            "forecast_days": 1,
        }
        resp = requests.get(_AIR_QUALITY_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _current_hour_index(self, time_strings: list) -> int:
        """
        Find the index in the hourly time array closest to the current UTC hour.

        Open-Meteo returns times as ISO-8601 strings without timezone suffix,
        e.g. '2024-05-01T14:00'.  We match on the current UTC hour.
        """
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00")
        for i, t in enumerate(time_strings):
            if str(t).startswith(now_str):
                return i
        # Fallback: use the last available hour
        return len(time_strings) - 1

    def collect(self, lat: float = 40.7128, lon: float = -74.0060) -> dict:
        """
        Fetch pollen data for the given location and compute the stress index.

        Parameters
        ----------
        lat : float
            Latitude (default: New York City, 40.7128° N)
        lon : float
            Longitude (default: New York City, -74.0060° W)

        Returns
        -------
        dict with keys:
            signal_name   : "pollen_stress_index"
            value         : float — sum of all pollen types / 1000.0
            raw_data      : dict — per-type pollen values for the current hour
            quality_score : float 0.0–1.0
            timestamp     : ISO-8601 UTC string
            source        : Open-Meteo Air Quality API URL
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            data = self._fetch_pollen(lat, lon)
            hourly = data.get("hourly", {})

            time_list = hourly.get("time", [])
            if not time_list:
                raise ValueError("No hourly time data returned from Open-Meteo.")

            hour_idx = self._current_hour_index(time_list)
            logger.debug(
                "Using hour index %d / %d (time: %s)",
                hour_idx, len(time_list) - 1, time_list[hour_idx],
            )

            pollen_values = {}
            total_pollen = 0.0
            missing_types = 0

            for pollen_type in _POLLEN_TYPES:
                series = hourly.get(pollen_type, [])
                if series and hour_idx < len(series) and series[hour_idx] is not None:
                    val = float(series[hour_idx])
                    pollen_values[pollen_type] = val
                    total_pollen += val
                else:
                    pollen_values[pollen_type] = None
                    missing_types += 1
                    logger.debug("No data for %s at hour index %d.", pollen_type, hour_idx)

            normalised = total_pollen / _NORMALISATION_FACTOR

            # Quality degrades proportionally with missing pollen types
            quality_score = 1.0 - (missing_types / len(_POLLEN_TYPES))

            raw_data = {
                "pollen_by_type": pollen_values,
                "total_grains_per_m3": total_pollen,
                "hour_used": time_list[hour_idx] if hour_idx < len(time_list) else None,
                "latitude": lat,
                "longitude": lon,
            }

            return {
                "signal_name": "pollen_stress_index",
                "value": float(normalised),
                "raw_data": raw_data,
                "quality_score": float(quality_score),
                "timestamp": timestamp,
                "source": _AIR_QUALITY_URL,
            }

        except Exception as exc:
            logger.warning("PollenCollector.collect failed: %s", exc)
            return {
                "signal_name": "pollen_stress_index",
                "value": 0.0,
                "raw_data": {},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": _AIR_QUALITY_URL,
            }
