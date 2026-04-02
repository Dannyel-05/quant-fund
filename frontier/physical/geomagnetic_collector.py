"""
Geomagnetic Activity Collector — GRAI Signal.

Measures the planetary K-index (Kp), a 3-hourly index (0–9) of global
geomagnetic disturbance published by NOAA's Space Weather Prediction Center.

Economic hypothesis
-------------------
Geomagnetic storms suppress melatonin and alter serotonin synthesis
(Burch et al. 1998), elevating investor risk-aversion.  Krivelyova &
Robotti (2003) found high geomagnetic activity precedes below-average
equity returns by 1–4 days.  The GRAI (Geomagnetic Risk Aversion Index)
converts raw Kp readings into an actionable position-size multiplier by
weighting for geographic proximity to poles, trading-session overlap,
storm decay, and ambient market volatility.

Data source
-----------
Primary  : https://services.swpc.noaa.gov/json/planetary_k_index_1m.json
Fallback : https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json
Both are freely available from NOAA — no API key required.
"""

import logging
from datetime import datetime, timezone

import requests
import yfinance as yf

from frontier.equations.derived_formulas import calc_grai

logger = logging.getLogger(__name__)

_PRIMARY_URL = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"
_FALLBACK_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

_HISTORICAL_VIX_MEAN = 20.0
_DEFAULT_HOURS_SINCE_PEAK = 6.0


class GeomagneticCollector:
    """
    Collects NOAA planetary K-index data and computes the GRAI signal.

    The collector attempts the 1-minute-resolution endpoint first, then
    falls back to the coarser 3-hourly product endpoint.  The last three
    readings are averaged and passed to calc_grai.
    """

    def _fetch_kp_primary(self) -> list:
        """Return raw JSON list from the 1-minute K-index endpoint."""
        resp = requests.get(_PRIMARY_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _fetch_kp_fallback(self) -> list:
        """Return raw JSON list from the 3-hourly K-index endpoint."""
        resp = requests.get(_FALLBACK_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _parse_kp_value(self, record: dict | list) -> float:
        """
        Extract a numeric Kp value from a single record.

        NOAA's JSON schema varies between endpoints:
        - 1-minute endpoint: list of dicts with key 'kp_index'
        - 3-hourly endpoint: list of lists where index 1 is the Kp string
        """
        if isinstance(record, dict):
            # 1-minute endpoint: {"time_tag": "...", "kp_index": 2.33, ...}
            return float(record.get("kp_index", 0.0))
        if isinstance(record, list) and len(record) >= 2:
            # 3-hourly endpoint: ["2024-01-01 00:00:00", "2.00", ...]
            try:
                return float(record[1])
            except (ValueError, TypeError):
                return 0.0
        return 0.0

    def _get_vix(self) -> float:
        """Fetch the latest VIX close from yfinance; fall back to mean."""
        try:
            vix_data = yf.download("^VIX", period="2d", progress=False)["Close"]
            if vix_data.empty:
                logger.warning("VIX download returned empty data; using mean.")
                return _HISTORICAL_VIX_MEAN
            return float(vix_data.iloc[-1])
        except Exception as exc:
            logger.warning("VIX fetch failed: %s — using historical mean.", exc)
            return _HISTORICAL_VIX_MEAN

    def collect(self) -> dict:
        """
        Fetch NOAA Kp data, compute GRAI, and return the standard signal dict.

        Returns
        -------
        dict with keys:
            signal_name   : "grai"
            value         : float — GRAI score (0.0 on error)
            raw_data      : dict — raw Kp readings used
            quality_score : float 0.0–1.0 (0.0 on error)
            timestamp     : ISO-8601 UTC string
            source        : data source URL used
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            raw_records = None
            source = _PRIMARY_URL
            quality_score = 1.0

            # Attempt primary endpoint
            try:
                raw_records = self._fetch_kp_primary()
                logger.debug("Fetched %d records from primary Kp endpoint.", len(raw_records))
            except Exception as primary_exc:
                logger.warning("Primary Kp endpoint failed: %s — trying fallback.", primary_exc)
                source = _FALLBACK_URL
                quality_score = 0.7  # fallback data is coarser
                raw_records = self._fetch_kp_fallback()
                logger.debug("Fetched %d records from fallback Kp endpoint.", len(raw_records))

            if not raw_records:
                raise ValueError("No Kp records returned from either endpoint.")

            # Use last 3 readings (skip header row if present for fallback)
            usable = [r for r in raw_records if not (isinstance(r, list) and str(r[0]).startswith(":"))]
            last_three = usable[-3:] if len(usable) >= 3 else usable
            kp_values = [self._parse_kp_value(r) for r in last_three]
            avg_kp = sum(kp_values) / len(kp_values) if kp_values else 0.0

            kp_readings = {"global": avg_kp}
            geographic_weights = {"global": 1.0}
            session_overlaps = {"global": 1.0}

            current_vix = self._get_vix()

            grai_value = calc_grai(
                kp_readings=kp_readings,
                geographic_weights=geographic_weights,
                session_overlaps=session_overlaps,
                hours_since_storm_peak=_DEFAULT_HOURS_SINCE_PEAK,
                current_vix=current_vix,
                historical_vix_mean=_HISTORICAL_VIX_MEAN,
            )

            raw_data = {
                "kp_readings": kp_values,
                "avg_kp": avg_kp,
                "vix_used": current_vix,
                "records_fetched": len(raw_records),
            }

            return {
                "signal_name": "grai",
                "value": float(grai_value),
                "raw_data": raw_data,
                "quality_score": quality_score,
                "timestamp": timestamp,
                "source": source,
            }

        except Exception as exc:
            logger.warning("GeomagneticCollector.collect failed: %s", exc)
            return {
                "signal_name": "grai",
                "value": 0.0,
                "raw_data": {},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": _PRIMARY_URL,
            }
