"""
Schumann Resonance Proxy Collector — Solar Wind Deviation Signal.

Schumann resonances are standing electromagnetic waves in the cavity between
the Earth's surface and the ionosphere, with a fundamental frequency near
7.83 Hz.  Some studies suggest correlations between Schumann resonance
intensity and human brainwave activity (alpha/theta bands), which could
theoretically influence aggregate investor mood and risk appetite.

Note on data availability
--------------------------
True real-time Schumann resonance data requires a network of ground-based
magnetometer stations (e.g. the Global Coherence Monitoring Network run
by HeartMath Institute).  No free, publicly accessible API exists for this
data as of 2026.

Proxy methodology
-----------------
Solar wind speed modulates the magnetosphere and ionospheric electron
density, which in turn affects the Q-factor (sharpness) of Schumann
resonance peaks.  High solar wind speed (> 600 km/s) compresses the
magnetosphere and is associated with elevated Schumann resonance amplitude
(Nickolaenko & Hayakawa 2002).  The deviation from the 400 km/s baseline
(nominal quiet-sun solar wind) is used as a proxy.

Economic hypothesis
-------------------
Elevated Schumann resonance amplitude has been correlated with changes in
human autonomic nervous system responses (McCraty et al. 2017).  If this
modulates investor cognitive bias, periods of high solar wind / resonance
anomaly may correspond to elevated aggregate risk aversion or impaired
decision quality.

Data source
-----------
NOAA Real-Time Solar Wind (RTSW): https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json
Free, no API key required.
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_RTSW_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"
_BASELINE_SOLAR_WIND_KMS = 400.0


class SchumannCollector:
    """
    Collects NOAA real-time solar wind data as a Schumann resonance proxy.

    True Schumann data requires a ground station network.  This collector
    uses solar wind speed deviation from the 400 km/s baseline as a proxy
    for ionospheric perturbation of Schumann resonance amplitude.
    """

    def _fetch_solar_wind(self) -> list:
        """Fetch the 1-minute resolution RTSW solar wind JSON from NOAA."""
        resp = requests.get(_RTSW_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _extract_speed(self, records: list) -> float:
        """
        Extract the most recent valid solar wind proton speed (km/s).

        NOAA RTSW JSON: list of dicts with key 'proton_speed' (may be null).
        Iterates from the end to find the latest non-null reading.
        """
        for record in reversed(records):
            if not isinstance(record, dict):
                continue
            speed = record.get("proton_speed")
            if speed is not None:
                try:
                    return float(speed)
                except (ValueError, TypeError):
                    continue
        raise ValueError("No valid proton_speed found in RTSW records.")

    def collect(self) -> dict:
        """
        Fetch solar wind speed and compute the Schumann resonance proxy deviation.

        Deviation = (speed - 400) / 400, clamped to [-1, 1].

        Positive deviation: solar wind faster than baseline → compressed
        magnetosphere → elevated resonance proxy → potential investor stress.
        Negative deviation: slower solar wind → quieter ionosphere.

        Returns
        -------
        dict with keys:
            signal_name   : "schumann_deviation"
            value         : float in [-1, 1]
            raw_data      : dict — latest solar wind reading and metadata
            quality_score : float 0.0–1.0
            timestamp     : ISO-8601 UTC string
            source        : "noaa_solar_wind_proxy"
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            records = self._fetch_solar_wind()
            logger.debug("Fetched %d RTSW records.", len(records))

            speed_kms = self._extract_speed(records)
            logger.debug("Latest solar wind proton speed: %.1f km/s", speed_kms)

            deviation = (speed_kms - _BASELINE_SOLAR_WIND_KMS) / _BASELINE_SOLAR_WIND_KMS
            deviation = max(-1.0, min(1.0, deviation))

            # Quality: 1.0 if speed is plausible (200–1000 km/s), else reduced
            quality_score = 1.0 if 200.0 <= speed_kms <= 1000.0 else 0.5

            raw_data = {
                "proton_speed_kms": speed_kms,
                "baseline_kms": _BASELINE_SOLAR_WIND_KMS,
                "records_fetched": len(records),
            }

            return {
                "signal_name": "schumann_deviation",
                "value": float(deviation),
                "raw_data": raw_data,
                "quality_score": quality_score,
                "timestamp": timestamp,
                "source": "noaa_solar_wind_proxy",
            }

        except Exception as exc:
            logger.warning("SchumannCollector.collect failed: %s", exc)
            return {
                "signal_name": "schumann_deviation",
                "value": 0.0,
                "raw_data": {},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": "noaa_solar_wind_proxy",
            }
