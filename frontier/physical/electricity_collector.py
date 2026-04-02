"""
Electricity Demand Anomaly Collector — UK National Grid ESO.

Measures real-time electricity consumption against its rolling mean and
flags statistically unusual demand levels using a z-score.

Economic hypothesis
-------------------
Electricity demand is one of the most real-time proxies for aggregate
economic activity.  Unlike GDP (quarterly), PMI (monthly), or even weekly
jobless claims, grid-level demand is measured every 30 minutes.  Demand
anomalies can signal:

  - Positive z-score: elevated industrial/commercial activity, cold weather
    surge, or data centre load growth → bullish short-term signal for the UK
    economy and energy-intensive sectors.
  - Negative z-score: demand destruction from economic slowdown, mild weather,
    or major disruption (e.g. pandemic lockdowns) → bearish leading indicator.

The effect is most pronounced for UK mid-cap industrials, utilities, and
energy companies.  It acts as a high-frequency complement to official
economic statistics, consistent with the "nowcasting" literature (Castle &
Hendry 2010).

Data source
-----------
UK National Grid ESO Open Data API (CKAN):
https://api.nationalgrideso.com/api/3/action/datastore_search
Resource ID: 177f6fa4-ae49-4182-81ea-0c6b35f26ca6
Free, no API key required.
"""

import logging
from datetime import datetime, timezone

import numpy as np
import requests

logger = logging.getLogger(__name__)

_ESO_API_URL = "https://api.nationalgrideso.com/api/3/action/datastore_search"
_RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"
_RECORDS_TO_FETCH = 48
_ZSCORE_CLAMP = 3.0


class ElectricityCollector:
    """
    Collects UK electricity demand data from National Grid ESO and computes
    a demand anomaly z-score relative to the rolling window mean.
    """

    def _fetch_demand(self) -> list:
        """Fetch the last N demand records from the National Grid ESO CKAN API."""
        params = {
            "resource_id": _RESOURCE_ID,
            "limit": _RECORDS_TO_FETCH,
            "sort": "_id desc",
        }
        resp = requests.get(_ESO_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("success"):
            raise ValueError(f"ESO API returned success=False: {payload.get('error')}")
        return payload["result"]["records"]

    def _extract_demand_values(self, records: list) -> list:
        """
        Parse demand MW values from ESO records.

        ESO records contain various field names depending on the dataset version.
        We attempt common field names in priority order.
        """
        candidate_fields = ["ENGLAND_WALES_DEMAND", "ND", "TSD", "TRANSMISSION_SYSTEM_DEMAND"]
        values = []
        for record in records:
            for field in candidate_fields:
                raw = record.get(field)
                if raw is not None:
                    try:
                        values.append(float(raw))
                        break
                    except (ValueError, TypeError):
                        continue
        return values

    def _compute_zscore(self, values: list) -> float:
        """
        Compute z-score of the latest value relative to the rolling window.

        Latest value (index 0, since sorted desc) vs. mean/std of the
        remaining N-1 values.  Clamped to [-3, 3].
        """
        if len(values) < 2:
            raise ValueError("Insufficient demand records to compute z-score.")

        latest = values[0]
        historical = values[1:]
        mean = float(np.mean(historical))
        std = float(np.std(historical, ddof=1))

        if std < 1e-6:
            logger.warning("Near-zero std in demand data; z-score set to 0.")
            return 0.0

        zscore = (latest - mean) / std
        return float(np.clip(zscore, -_ZSCORE_CLAMP, _ZSCORE_CLAMP))

    def collect(self) -> dict:
        """
        Fetch UK electricity demand data and return the anomaly z-score.

        Returns
        -------
        dict with keys:
            signal_name   : "electricity_anomaly"
            value         : float — z-score clamped to [-3, 3]
            raw_data      : dict — demand values and statistics
            quality_score : 1.0 if API succeeded, 0.3 if fallback used
            timestamp     : ISO-8601 UTC string
            source        : ESO API URL or "fallback_hardcoded"
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            records = self._fetch_demand()
            logger.debug("Fetched %d ESO demand records.", len(records))

            demand_values = self._extract_demand_values(records)
            if not demand_values:
                raise ValueError("No parseable demand values in ESO records.")

            logger.debug(
                "Parsed %d demand values. Latest: %.0f MW",
                len(demand_values), demand_values[0],
            )

            zscore = self._compute_zscore(demand_values)

            raw_data = {
                "latest_mw": demand_values[0],
                "rolling_mean_mw": float(np.mean(demand_values[1:])) if len(demand_values) > 1 else None,
                "rolling_std_mw": float(np.std(demand_values[1:], ddof=1)) if len(demand_values) > 2 else None,
                "records_fetched": len(records),
                "values_parsed": len(demand_values),
            }

            return {
                "signal_name": "electricity_anomaly",
                "value": zscore,
                "raw_data": raw_data,
                "quality_score": 1.0,
                "timestamp": timestamp,
                "source": _ESO_API_URL,
            }

        except Exception as exc:
            logger.warning(
                "ElectricityCollector.collect failed: %s — using fallback value.", exc
            )
            return {
                "signal_name": "electricity_anomaly",
                "value": 0.0,
                "raw_data": {"error": str(exc)},
                "quality_score": 0.3,
                "timestamp": timestamp,
                "source": "fallback_hardcoded",
            }
