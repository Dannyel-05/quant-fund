"""
Building Permit Collector — building_permit_inflection Signal.

Measures the deviation of current US residential building permit issuance
from its 12-month average as a leading indicator for housing sector momentum.

Economic hypothesis
-------------------
US residential building permits (PERMIT series) are a leading economic
indicator published by the US Census Bureau.  They measure new residential
construction authorised before a single nail is struck, making them 3–9
months ahead of housing starts, which are themselves 6–12 months ahead of
housing completions.

The series therefore leads the following investable outcomes:
  1. Homebuilder revenue and margins (XHB, ITB ETFs, individual builders).
  2. Building materials demand: lumber, concrete, copper, insulation.
  3. Mortgage origination volume and bank fee income.
  4. Home furnishings and appliance demand (6–18 month lag from permit).
  5. Residential REIT supply pressure: new completions ~12m after permit.

The inflection signal — (latest - 12m_avg) / 12m_avg — captures turning
points:
  - Positive inflection: permit activity above trend → housing cycle
    accelerating → bullish homebuilders and materials.
  - Negative inflection: permits below trend → cycle decelerating → reduce
    homebuilder exposure, potential short in high-inventory markets.

This signal has a well-documented lead time; use with a 3–6 month holding
period for optimal positioning.

Data source
-----------
FRED (Federal Reserve Bank of St. Louis) CSV endpoint (free, no key):
https://fred.stlouisfed.org/graph/fredgraph.csv?id=PERMIT
Returns monthly data for US New Privately-Owned Housing Units Authorised
in Permit-Issuing Places (thousands of units, seasonally adjusted annual rate).
"""

import logging
from datetime import datetime, timezone
from io import StringIO

import requests

logger = logging.getLogger(__name__)

_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PERMIT"
_LOOKBACK_MONTHS = 24
_TREND_MONTHS = 12
_REQUEST_TIMEOUT = 15
_SOURCE = "FRED:PERMIT (US Building Permits, Census/BEA)"


class BuildingPermitCollector:
    """
    Downloads the FRED PERMIT series and computes a permit-inflection
    signal as the percentage deviation of the latest reading from its
    trailing 12-month mean.
    """

    def _fetch_fred_csv(self) -> str:
        """
        Download the FRED PERMIT CSV.

        Returns the raw CSV text, or an empty string on error.
        """
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; QuantFundBuildingPermitBot/1.0)"
                )
            }
            resp = requests.get(_FRED_URL, headers=headers, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.Timeout:
            logger.warning("BuildingPermitCollector: FRED request timed out.")
        except requests.exceptions.RequestException as exc:
            logger.warning("BuildingPermitCollector: network error: %s", exc)
        except Exception as exc:
            logger.warning("BuildingPermitCollector: unexpected error: %s", exc)
        return ""

    def _parse_csv(self, csv_text: str) -> list[dict]:
        """
        Parse FRED CSV into a list of {date: str, value: float} dicts.

        FRED format: header row "DATE,PERMIT", then date rows.
        Missing values are represented as "." and are skipped.
        """
        records = []
        if not csv_text:
            return records

        lines = csv_text.strip().splitlines()
        for line in lines[1:]:  # skip header
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            date_str = parts[0].strip()
            val_str = parts[1].strip()
            if val_str in (".", "", "NA"):
                continue
            try:
                value = float(val_str)
                records.append({"date": date_str, "value": value})
            except ValueError:
                continue

        return records

    def collect(self) -> dict:
        """
        Fetch FRED building permit data and return the inflection signal.

        Uses the last 24 months of data.  The inflection is:
            (latest_value - mean_of_last_12_months) / mean_of_last_12_months
        Clamped to [-1, 1].

        Returns
        -------
        dict with keys:
            signal_name   : "building_permit_inflection"
            value         : float in [-1, 1]
            raw_data      : dict — recent permit data, trend stats
            quality_score : 1.0 if FRED data obtained, 0.0 on error
            timestamp     : ISO-8601 UTC string
            source        : FRED URL
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        csv_text = self._fetch_fred_csv()
        quality_score = 1.0 if csv_text else 0.0

        if not csv_text:
            return {
                "signal_name": "building_permit_inflection",
                "value": 0.0,
                "raw_data": {"error": "Failed to fetch FRED PERMIT data."},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": _SOURCE,
            }

        records = self._parse_csv(csv_text)

        if len(records) < _TREND_MONTHS + 1:
            logger.warning(
                "BuildingPermitCollector: only %d records, need %d+.",
                len(records),
                _TREND_MONTHS + 1,
            )
            return {
                "signal_name": "building_permit_inflection",
                "value": 0.0,
                "raw_data": {
                    "records_available": len(records),
                    "error": "Insufficient data for trend calculation.",
                },
                "quality_score": 0.3,
                "timestamp": timestamp,
                "source": _SOURCE,
            }

        # Use most recent 24 months (or all available if fewer)
        recent_records = records[-_LOOKBACK_MONTHS:]

        latest = recent_records[-1]["value"]
        last_12 = [r["value"] for r in recent_records[-_TREND_MONTHS:]]

        twelve_month_avg = sum(last_12) / len(last_12)

        if twelve_month_avg == 0:
            inflection = 0.0
        else:
            inflection = (latest - twelve_month_avg) / twelve_month_avg

        # Clamp to [-1, 1]
        inflection = max(-1.0, min(1.0, inflection))

        # Month-over-month change
        if len(recent_records) >= 2:
            prev = recent_records[-2]["value"]
            mom_change = (latest - prev) / prev if prev != 0 else 0.0
        else:
            mom_change = 0.0

        raw_data = {
            "latest_date": recent_records[-1]["date"],
            "latest_value_thousands": latest,
            "twelve_month_avg": round(twelve_month_avg, 1),
            "mom_change_pct": round(mom_change * 100, 2),
            "inflection_pre_clamp": round(
                (latest - twelve_month_avg) / twelve_month_avg
                if twelve_month_avg != 0
                else 0.0,
                4,
            ),
            "records_used": len(recent_records),
            "series": "PERMIT — US New Privately-Owned Housing Units Authorised (SAAR, thousands)",
        }

        return {
            "signal_name": "building_permit_inflection",
            "value": float(inflection),
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": _SOURCE,
        }
