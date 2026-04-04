"""
STEP 4 — Geographic Intelligence Collector

Collects weather, air quality, earthquake, and regional economic data for
all US and UK locations. Data is stored in:
  - output/permanent_archive.db  (raw_weather_data, raw_geopolitical_events,
                                   raw_macro_data tables)
  - output/historical_db.db       (weather_data table)

Sources used:
  - Open-Meteo Historical Archive (free, no key)
  - OpenWeatherMap current + forecast (api_keys.openweathermap)
  - Open-Meteo Air Quality / Pollen (free, no key)
  - WAQI Air Quality (api_keys.waqi)
  - USGS Earthquake feed (free)
  - FRED Regional Economic Indicators (api_keys.fred)
  - EIA Electricity Data (api_keys.eia)

Usage:
    from data.collectors.geographic_intelligence import GeographicIntelligence
    gi = GeographicIntelligence(config)
    summary = gi.collect()
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "settings.yaml"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"
_HIST_DB = _ROOT / "output" / "historical_db.db"

# ── Location definitions ───────────────────────────────────────────────────────
US_LOCATIONS: Dict[str, Tuple[float, float]] = {
    "new_york":    (40.7128, -74.0060),
    "los_angeles": (34.0522, -118.2437),
    "chicago":     (41.8781, -87.6298),
    "houston":     (29.7604, -95.3698),
    "dallas":      (32.7767, -96.7970),
    "atlanta":     (33.7490, -84.3880),
    "seattle":     (47.6062, -122.3321),
    "miami":       (25.7617, -80.1918),
    "denver":      (39.7392, -104.9903),
    "minneapolis": (44.9778, -93.2650),
    "detroit":     (42.3314, -83.0458),
    "boston":      (42.3601, -71.0589),
    "phoenix":     (33.4484, -112.0740),
}

UK_LOCATIONS: Dict[str, Tuple[float, float]] = {
    "london":      (51.5074, -0.1278),
    "manchester":  (53.4808, -2.2426),
    "birmingham":  (52.4862, -1.8904),
    "edinburgh":   (55.9533, -3.1883),
    "cardiff":     (51.4816, -3.1791),
    "leeds":       (53.8008, -1.5491),
}

ALL_LOCATIONS: Dict[str, Tuple[float, float]] = {**US_LOCATIONS, **UK_LOCATIONS}

# City name mapping for OWM queries
_CITY_OWM_NAMES: Dict[str, str] = {
    "new_york":    "New York",
    "los_angeles": "Los Angeles",
    "chicago":     "Chicago",
    "houston":     "Houston",
    "dallas":      "Dallas",
    "atlanta":     "Atlanta",
    "seattle":     "Seattle",
    "miami":       "Miami",
    "denver":      "Denver",
    "minneapolis": "Minneapolis",
    "detroit":     "Detroit",
    "boston":      "Boston",
    "phoenix":     "Phoenix",
    "london":      "London",
    "manchester":  "Manchester",
    "birmingham":  "Birmingham",
    "edinburgh":   "Edinburgh",
    "cardiff":     "Cardiff",
    "leeds":       "Leeds",
}

# WAQI city slugs
_WAQI_CITIES: List[str] = [
    "london", "chicago", "los-angeles", "new-york", "houston", "seattle"
]

# FRED regional series
_FRED_REGIONAL_SERIES: Dict[str, str] = {
    "PHILFRBSURPM":  "Philadelphia Fed Manufacturing Survey",
    "GAUTHMPNSA":    "Empire State Manufacturing — Authority",
    "CHIPMINDX":     "Chicago PMI",
    "DALLMFG":       "Dallas Fed Manufacturing Index",
    "KCFSI":         "Kansas City Financial Stress Index",
}

# Open-Meteo weather variables to request
_WEATHER_VARS = (
    "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
    "precipitation_sum,rain_sum,snowfall_sum,"
    "windspeed_10m_max,weathercode,sunshine_duration"
)

# Only variables guaranteed to exist in Open-Meteo historical archive
_HIST_WEATHER_VARS = (
    "temperature_2m_max,temperature_2m_min,"
    "precipitation_sum,windspeed_10m_max,weathercode,snowfall_sum"
)

# Pollen variables
_POLLEN_VARS = (
    "alder_pollen,birch_pollen,grass_pollen,"
    "mugwort_pollen,olive_pollen,ragweed_pollen"
)

# ── Config loader ─────────────────────────────────────────────────────────────

def _load_config() -> Dict[str, Any]:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:
        logger.warning("Could not load settings.yaml: %s", exc)
        return {}


# ── Database helpers ──────────────────────────────────────────────────────────

def _conn(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def _init_perm_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_weather_data (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            location             TEXT    NOT NULL,
            country              TEXT,
            latitude             REAL,
            longitude            REAL,
            date                 TEXT    NOT NULL,
            source               TEXT    NOT NULL,
            temp_max             REAL,
            temp_min             REAL,
            temp_mean            REAL,
            precipitation        REAL,
            rain                 REAL,
            snowfall             REAL,
            windspeed_max        REAL,
            weathercode          INTEGER,
            sunshine_duration    REAL,
            temperature_anomaly  REAL,
            precip_anomaly       REAL,
            windspeed_zscore     REAL,
            weather_risk_score   REAL,
            is_extreme           INTEGER DEFAULT 0,
            pollen_total         REAL,
            aqi                  REAL,
            pm25                 REAL,
            raw_json             TEXT,
            collected_at         TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS raw_macro_data (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            series_name   TEXT    NOT NULL,
            series_id     TEXT,
            date          TEXT    NOT NULL,
            value         REAL,
            source        TEXT,
            raw_json      TEXT,
            collected_at  TEXT    DEFAULT (datetime('now')),
            UNIQUE(series_name, date)
        );

        CREATE TABLE IF NOT EXISTS raw_geopolitical_events (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            source           TEXT    NOT NULL,
            event_date       TEXT    NOT NULL,
            event_type       TEXT,
            title            TEXT,
            description      TEXT,
            url              TEXT,
            goldstein_scale  REAL,
            magnitude        REAL,
            latitude         REAL,
            longitude        REAL,
            location         TEXT,
            severity         TEXT,
            affected_sectors TEXT,
            affected_regions TEXT,
            raw_json         TEXT,
            collected_at     TEXT    DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


def _init_hist_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            location             TEXT    NOT NULL,
            country              TEXT,
            date                 TEXT    NOT NULL,
            source               TEXT    NOT NULL,
            temp_max             REAL,
            temp_min             REAL,
            temp_mean            REAL,
            precipitation        REAL,
            snowfall             REAL,
            windspeed_max        REAL,
            weathercode          INTEGER,
            temperature_anomaly  REAL,
            precip_anomaly       REAL,
            weather_risk_score   REAL,
            is_extreme           INTEGER DEFAULT 0,
            collected_at         TEXT    DEFAULT (datetime('now')),
            UNIQUE(location, date, source)
        );
    """)
    conn.commit()


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str, params: Optional[Dict] = None, timeout: int = 30,
         retries: int = 2) -> Optional[Dict]:
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logger.warning("GET %s failed: %s", url, exc)
    return None


# ── Statistics helpers ────────────────────────────────────────────────────────

def _zscore(value: Optional[float], mean: float, std: float) -> Optional[float]:
    if value is None or std == 0:
        return None
    return (value - mean) / std


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 1.0
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / len(values)
    return variance ** 0.5 or 1.0


# ── Main class ────────────────────────────────────────────────────────────────

class GeographicIntelligence:
    """
    Collects weather, air quality, pollen, earthquake, and regional economic
    data for all configured US and UK locations.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or _load_config()
        api_keys = self.config.get("api_keys", {})
        self._owm_key   = api_keys.get("openweathermap", "")
        self._waqi_key  = api_keys.get("waqi", "")
        self._fred_key  = api_keys.get("fred", "")
        self._eia_key   = api_keys.get("eia", "")

        # Cache of per-location 10-yr baselines: {loc: {var: (mean, std)}}
        self._baselines: Dict[str, Dict[str, Tuple[float, float]]] = {}

    # ── DB connections (lazy) ─────────────────────────────────────────────────

    def _perm_conn(self) -> sqlite3.Connection:
        conn = _conn(_PERM_DB)
        _init_perm_db(conn)
        return conn

    def _hist_conn(self) -> sqlite3.Connection:
        conn = _conn(_HIST_DB)
        _init_hist_db(conn)
        return conn

    # ── Baseline computation ──────────────────────────────────────────────────

    def _compute_baseline(self, location: str) -> Dict[str, Tuple[float, float]]:
        """
        Compute 10-yr baseline (2010-2020) statistics from permanent DB.
        Returns dict: {'temp_max': (mean, std), 'precipitation': (mean, std), ...}
        """
        if location in self._baselines:
            return self._baselines[location]

        result: Dict[str, Tuple[float, float]] = {}
        try:
            with self._perm_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT temp_max, temp_min, temp_mean, precipitation, windspeed_max
                    FROM raw_weather_data
                    WHERE location = ?
                      AND source = 'OPEN_METEO_HIST'
                      AND date >= '2010-01-01'
                      AND date <  '2021-01-01'
                    """,
                    (location,)
                ).fetchall()

            if rows:
                cols = ["temp_max", "temp_min", "temp_mean",
                        "precipitation", "windspeed_max"]
                for i, col in enumerate(cols):
                    vals = [r[i] for r in rows if r[i] is not None]
                    if vals:
                        result[col] = (_mean(vals), _std(vals))
        except Exception as exc:
            logger.debug("Baseline computation failed for %s: %s", location, exc)

        self._baselines[location] = result
        return result

    # ── WeatherRiskScore ──────────────────────────────────────────────────────

    def get_weather_risk(self, location: str) -> Optional[float]:
        """
        Returns WeatherRiskScore for a location: -1 to +1.
        Positive = unusually bad weather.
        Combines temperature anomaly, precipitation anomaly, windspeed z-score.
        """
        try:
            with self._perm_conn() as conn:
                row = conn.execute(
                    """
                    SELECT temperature_anomaly, precip_anomaly, windspeed_zscore
                    FROM raw_weather_data
                    WHERE location = ? AND source IN ('OPEN_METEO_HIST','OPEN_METEO_RECENT')
                    ORDER BY date DESC LIMIT 1
                    """,
                    (location,)
                ).fetchone()

            if not row:
                return None

            t_anom = row[0] or 0.0
            p_anom = row[1] or 0.0
            w_z    = row[2] or 0.0

            # Combine: temperature deviation (abs) + positive precip anomaly + wind z
            risk = (abs(t_anom) * 0.4 + max(p_anom, 0.0) * 0.35 + max(w_z, 0.0) * 0.25)
            # Normalise roughly to -1..+1 (assuming anomaly rarely exceeds 4 std devs)
            risk_norm = min(max(risk / 4.0, -1.0), 1.0)
            return round(risk_norm, 4)
        except Exception as exc:
            logger.warning("get_weather_risk(%s) failed: %s", location, exc)
            return None

    def _compute_risk_score(
        self,
        temp_anom: Optional[float],
        precip_anom: Optional[float],
        wind_z: Optional[float],
    ) -> float:
        t = abs(temp_anom or 0.0)
        p = max(precip_anom or 0.0, 0.0)
        w = max(wind_z or 0.0, 0.0)
        raw = t * 0.4 + p * 0.35 + w * 0.25
        return round(min(max(raw / 4.0, -1.0), 1.0), 4)

    # ── Open-Meteo Historical Weather ─────────────────────────────────────────

    def collect_historical(self, start_date: str = "2010-01-01") -> Dict[str, int]:
        """
        Backfills all historical weather data from start_date to today.
        Fetches in a single request per location (Open-Meteo supports full range).
        Returns {location: rows_inserted}.
        """
        end_date = datetime.now().strftime("%Y-%m-%d")
        summary: Dict[str, int] = {}

        for location, (lat, lon) in ALL_LOCATIONS.items():
            country = "UK" if location in UK_LOCATIONS else "US"
            logger.info("Fetching historical weather for %s (%s to %s)", location, start_date, end_date)
            inserted = self._fetch_and_store_meteo(
                location, country, lat, lon, start_date, end_date,
                source="OPEN_METEO_HIST"
            )
            summary[location] = inserted
            # Invalidate cached baseline so it recomputes
            self._baselines.pop(location, None)
            time.sleep(0.3)  # be polite to free API

        logger.info("Historical collection complete: %s", summary)
        return summary

    def collect_weather(self, days_back: int = 7) -> Dict[str, int]:
        """
        Collects recent weather (last N days) for all locations.
        Returns {location: rows_inserted}.
        """
        end_date   = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        summary: Dict[str, int] = {}

        for location, (lat, lon) in ALL_LOCATIONS.items():
            country = "UK" if location in UK_LOCATIONS else "US"
            inserted = self._fetch_and_store_meteo(
                location, country, lat, lon, start_date, end_date,
                source="OPEN_METEO_RECENT"
            )
            summary[location] = inserted
            time.sleep(0.2)

        logger.info("Recent weather collection complete: %s", summary)
        return summary

    def _fetch_and_store_meteo(
        self,
        location: str,
        country: str,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        source: str,
    ) -> int:
        """Fetch from Open-Meteo archive and store in both DBs."""
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": start_date,
            "end_date":   end_date,
            "daily":      _HIST_WEATHER_VARS,
            "timezone":   "auto",
        }
        data = _get(url, params=params, timeout=60)
        if not data or "daily" not in data:
            logger.warning("No data from Open-Meteo for %s", location)
            return 0

        daily   = data["daily"]
        dates   = daily.get("time", [])
        t_max   = daily.get("temperature_2m_max", [None] * len(dates))
        t_min   = daily.get("temperature_2m_min", [None] * len(dates))
        precip  = daily.get("precipitation_sum", [None] * len(dates))
        rain    = daily.get("rain_sum", [None] * len(dates))
        snow    = daily.get("snowfall_sum", [None] * len(dates))
        wind    = daily.get("windspeed_10m_max", [None] * len(dates))
        wcode   = daily.get("weathercode", [None] * len(dates))
        sun     = daily.get("sunshine_duration", [None] * len(dates))

        # Compute baseline for anomaly calculations
        baseline = self._compute_baseline(location)

        inserted = 0
        perm_rows = []
        hist_rows = []

        for i, date in enumerate(dates):
            tm = t_max[i]
            ti = t_min[i]
            t_mean = None
            if tm is not None and ti is not None:
                t_mean = (tm + ti) / 2.0

            pr = precip[i]
            wi = wind[i]

            # Anomalies vs 10-yr baseline
            t_anom = None
            p_anom = None
            w_z    = None

            if "temp_max" in baseline and tm is not None:
                bm, bs = baseline["temp_max"]
                t_anom = _zscore(tm, bm, bs)
            if "precipitation" in baseline and pr is not None:
                bm, bs = baseline["precipitation"]
                p_anom = _zscore(pr, bm, bs)
            if "windspeed_max" in baseline and wi is not None:
                bm, bs = baseline["windspeed_max"]
                w_z = _zscore(wi, bm, bs)

            risk = self._compute_risk_score(t_anom, p_anom, w_z)
            extreme = 1 if any(
                abs(v or 0.0) > 2.0 for v in [t_anom, p_anom, w_z]
            ) else 0

            perm_rows.append((
                location, country, lat, lon, date, source,
                tm, ti, t_mean, pr, rain[i] if i < len(rain) else None,
                snow[i] if i < len(snow) else None,
                wi,
                wcode[i] if i < len(wcode) else None,
                sun[i] if i < len(sun) else None,
                t_anom, p_anom, w_z, risk, extreme,
                None, None, None,  # pollen_total, aqi, pm25
                None,              # raw_json
            ))

            hist_rows.append((
                location, country, date, source,
                tm, ti, t_mean, pr,
                snow[i] if i < len(snow) else None,
                wi,
                wcode[i] if i < len(wcode) else None,
                t_anom, p_anom, risk, extreme,
            ))

        try:
            with self._perm_conn() as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO raw_weather_data
                      (location, country, latitude, longitude, date, source,
                       temp_max, temp_min, temp_mean,
                       precipitation, rain, snowfall,
                       windspeed_max, weathercode, sunshine_duration,
                       temperature_anomaly, precip_anomaly, windspeed_zscore,
                       weather_risk_score, is_extreme,
                       pollen_total, aqi, pm25, raw_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    perm_rows
                )
                conn.commit()
                inserted += len(perm_rows)
        except Exception as exc:
            logger.error("permanent_archive insert failed for %s: %s", location, exc)

        try:
            with self._hist_conn() as conn:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO weather_data
                      (location, country, date, source,
                       temp_max, temp_min, temp_mean, precipitation, snowfall,
                       windspeed_max, weathercode,
                       temperature_anomaly, precip_anomaly,
                       weather_risk_score, is_extreme)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    hist_rows
                )
                conn.commit()
        except Exception as exc:
            logger.error("historical_db insert failed for %s: %s", location, exc)

        return inserted

    # ── OpenWeatherMap current + forecast ─────────────────────────────────────

    def _collect_owm(self) -> int:
        """Collect current conditions + forecast from OpenWeatherMap."""
        if not self._owm_key:
            logger.warning("No OpenWeatherMap key configured — skipping OWM collection")
            return 0

        inserted = 0
        today = datetime.now().strftime("%Y-%m-%d")

        for loc_key, city_name in _CITY_OWM_NAMES.items():
            # Current conditions
            try:
                data = _get(
                    "https://api.openweathermap.org/data/2.5/weather",
                    params={"q": city_name, "appid": self._owm_key, "units": "metric"},
                )
                if data and data.get("cod") != 404:
                    main   = data.get("main", {})
                    wind   = data.get("wind", {})
                    rain   = data.get("rain", {}).get("1h", 0.0)
                    snow   = data.get("snow", {}).get("1h", 0.0)
                    wcode  = data.get("weather", [{}])[0].get("id")
                    country = "UK" if loc_key in UK_LOCATIONS else "US"
                    lat, lon = ALL_LOCATIONS.get(loc_key, (None, None))

                    with self._perm_conn() as conn:
                        conn.execute(
                            """
                            INSERT INTO raw_weather_data
                              (location, country, latitude, longitude, date, source,
                               temp_max, temp_min, temp_mean,
                               precipitation, rain, snowfall,
                               windspeed_max, weathercode, raw_json)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                loc_key, country, lat, lon, today, "OWM_CURRENT",
                                main.get("temp_max"), main.get("temp_min"),
                                main.get("temp"),
                                rain + snow, rain, snow,
                                wind.get("speed"), wcode,
                                json.dumps(data),
                            )
                        )
                        conn.commit()
                    inserted += 1
            except Exception as exc:
                logger.warning("OWM current failed for %s: %s", city_name, exc)

            # 5-day forecast (stored for reference — not critical)
            try:
                _get(
                    "https://api.openweathermap.org/data/2.5/forecast",
                    params={"q": city_name, "appid": self._owm_key, "units": "metric"},
                )
            except Exception:
                pass

            time.sleep(0.25)

        return inserted

    # ── Open-Meteo Air Quality / Pollen ───────────────────────────────────────

    def _collect_pollen(self) -> int:
        """Collect pollen forecasts for all locations."""
        inserted = 0
        today = datetime.now().strftime("%Y-%m-%d")

        for location, (lat, lon) in ALL_LOCATIONS.items():
            try:
                data = _get(
                    "https://air-quality-api.open-meteo.com/v1/air-quality",
                    params={
                        "latitude":      lat,
                        "longitude":     lon,
                        "hourly":        _POLLEN_VARS,
                        "forecast_days": 16,
                    },
                    timeout=30,
                )
                if not data or "hourly" not in data:
                    continue

                hourly = data["hourly"]
                times  = hourly.get("time", [])

                # Aggregate daily pollen totals
                daily_sums: Dict[str, float] = {}
                pollen_keys = [
                    "alder_pollen", "birch_pollen", "grass_pollen",
                    "mugwort_pollen", "olive_pollen", "ragweed_pollen",
                ]
                for t_str, *vals in zip(times, *[hourly.get(k, []) for k in pollen_keys]):
                    day = t_str[:10]
                    total = sum(v for v in vals if v is not None)
                    daily_sums[day] = daily_sums.get(day, 0.0) + total

                # Compute seasonal baseline (use all collected values)
                all_vals = list(daily_sums.values())
                baseline_mean = _mean(all_vals) if all_vals else 0.0
                baseline_std  = _std(all_vals) if all_vals else 1.0

                country = "UK" if location in UK_LOCATIONS else "US"

                with self._perm_conn() as conn:
                    for day, total in daily_sums.items():
                        pollen_z = _zscore(total, baseline_mean, baseline_std)
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO raw_weather_data
                              (location, country, latitude, longitude, date, source,
                               pollen_total, raw_json)
                            VALUES (?,?,?,?,?,?,?,?)
                            """,
                            (
                                location, country, lat, lon, day,
                                "OPEN_METEO_POLLEN",
                                pollen_z,
                                json.dumps({"day": day, "total": total}),
                            )
                        )
                        inserted += 1
                    conn.commit()

            except Exception as exc:
                logger.warning("Pollen collection failed for %s: %s", location, exc)

            time.sleep(0.2)

        return inserted

    def get_pollen_forecast(self, location: str) -> List[Dict[str, Any]]:
        """Returns 14-day pollen forecast for a location."""
        try:
            with self._perm_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT date, pollen_total
                    FROM raw_weather_data
                    WHERE location = ? AND source = 'OPEN_METEO_POLLEN'
                    ORDER BY date DESC LIMIT 14
                    """,
                    (location,)
                ).fetchall()
            return [{"date": r[0], "pollen_stress_index": r[1]} for r in rows]
        except Exception as exc:
            logger.warning("get_pollen_forecast(%s) failed: %s", location, exc)
            return []

    # ── WAQI Air Quality ──────────────────────────────────────────────────────

    def _collect_waqi(self) -> int:
        """Collect AQI readings from WAQI for key cities."""
        if not self._waqi_key:
            logger.warning("No WAQI key configured — skipping WAQI collection")
            return 0

        inserted = 0
        today = datetime.now().strftime("%Y-%m-%d")

        for city_slug in _WAQI_CITIES:
            # Map slug to location key
            loc_key = city_slug.replace("-", "_")
            if loc_key not in ALL_LOCATIONS:
                loc_key = city_slug  # may match directly

            try:
                data = _get(
                    f"https://api.waqi.info/feed/{city_slug}/",
                    params={"token": self._waqi_key},
                )
                if not data or data.get("status") != "ok":
                    continue

                d = data.get("data", {})
                aqi  = d.get("aqi")
                pm25 = None
                iaqi = d.get("iaqi", {})
                if "pm25" in iaqi:
                    pm25 = iaqi["pm25"].get("v")

                lat, lon = ALL_LOCATIONS.get(loc_key, (None, None))
                country = "UK" if loc_key in UK_LOCATIONS else "US"

                with self._perm_conn() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO raw_weather_data
                          (location, country, latitude, longitude, date, source,
                           aqi, pm25, raw_json)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            loc_key, country, lat, lon, today,
                            "WAQI",
                            aqi, pm25,
                            json.dumps(data),
                        )
                    )
                    conn.commit()
                inserted += 1

            except Exception as exc:
                logger.warning("WAQI failed for %s: %s", city_slug, exc)

            time.sleep(0.3)

        return inserted

    # ── USGS Earthquakes ──────────────────────────────────────────────────────

    def _collect_earthquakes(self) -> int:
        """Fetch and store earthquakes magnitude > 3.0 from USGS."""
        url  = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
        data = _get(url, timeout=30)
        if not data:
            return 0

        features = data.get("features", [])
        inserted = 0

        with self._perm_conn() as conn:
            for feat in features:
                try:
                    props = feat.get("properties", {})
                    mag   = props.get("mag", 0.0)
                    if mag is None or mag < 3.0:
                        continue

                    geom  = feat.get("geometry", {})
                    coords = geom.get("coordinates", [None, None, None])
                    lon   = coords[0]
                    lat   = coords[1]
                    place = props.get("place", "")
                    ts    = props.get("time", 0)
                    event_date = datetime.fromtimestamp(
                        ts / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d") if ts else datetime.now().strftime("%Y-%m-%d")

                    severity = "LOW"
                    if mag >= 6.5:
                        severity = "CRITICAL"
                    elif mag >= 5.5:
                        severity = "HIGH"
                    elif mag >= 4.5:
                        severity = "MEDIUM"

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO raw_geopolitical_events
                          (source, event_date, event_type, title, description,
                           magnitude, latitude, longitude, location, severity, raw_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            "USGS_EARTHQUAKE",
                            event_date,
                            "EARTHQUAKE",
                            f"M{mag:.1f} Earthquake — {place}",
                            f"Magnitude {mag:.1f} earthquake at {place}",
                            mag, lat, lon, place, severity,
                            json.dumps(props),
                        )
                    )
                    inserted += 1
                except Exception as exc:
                    logger.debug("Earthquake insert failed: %s", exc)

            conn.commit()

        logger.info("USGS: stored %d earthquakes (mag >= 3.0)", inserted)
        return inserted

    def get_earthquake_alerts(self, min_magnitude: float = 4.5) -> List[Dict[str, Any]]:
        """Returns list of significant recent earthquakes."""
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        try:
            with self._perm_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT event_date, title, magnitude, location, severity
                    FROM raw_geopolitical_events
                    WHERE source = 'USGS_EARTHQUAKE'
                      AND event_type = 'EARTHQUAKE'
                      AND magnitude >= ?
                      AND event_date >= ?
                    ORDER BY magnitude DESC
                    """,
                    (min_magnitude, cutoff)
                ).fetchall()
            return [
                {
                    "date":      r[0],
                    "title":     r[1],
                    "magnitude": r[2],
                    "location":  r[3],
                    "severity":  r[4],
                }
                for r in rows
            ]
        except Exception as exc:
            logger.warning("get_earthquake_alerts failed: %s", exc)
            return []

    # ── FRED Regional Economic Indicators ─────────────────────────────────────

    def _collect_fred(self) -> int:
        """Fetch all available history for regional FRED series."""
        if not self._fred_key:
            logger.warning("No FRED key configured — skipping FRED regional collection")
            return 0

        inserted = 0
        for series_id, series_name in _FRED_REGIONAL_SERIES.items():
            try:
                data = _get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={
                        "series_id":         series_id,
                        "api_key":           self._fred_key,
                        "file_type":         "json",
                        "observation_start": "2000-01-01",
                        "observation_end":   datetime.now().strftime("%Y-%m-%d"),
                    },
                    timeout=30,
                )
                if not data or "observations" not in data:
                    logger.warning("FRED: no data for series %s", series_id)
                    continue

                obs_rows = []
                for obs in data["observations"]:
                    raw_val = obs.get("value", ".")
                    if raw_val == ".":
                        continue
                    try:
                        val = float(raw_val)
                    except ValueError:
                        continue
                    obs_rows.append((
                        series_name, series_id, obs["date"],
                        val, "FRED", json.dumps(obs),
                    ))

                with self._perm_conn() as conn:
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO raw_macro_data
                          (series_name, series_id, date, value, source, raw_json)
                        VALUES (?,?,?,?,?,?)
                        """,
                        obs_rows
                    )
                    conn.commit()
                inserted += len(obs_rows)
                logger.info("FRED %s: inserted %d observations", series_id, len(obs_rows))

            except Exception as exc:
                logger.warning("FRED %s failed: %s", series_id, exc)

            time.sleep(0.5)

        return inserted

    # ── EIA Electricity Data ──────────────────────────────────────────────────

    def _collect_eia(self) -> int:
        """Fetch regional electricity demand data from EIA."""
        if not self._eia_key:
            logger.warning("No EIA key configured — skipping EIA collection")
            return 0

        inserted = 0
        try:
            data = _get(
                "https://api.eia.gov/v2/electricity/rto/region-data/data/",
                params={
                    "api_key":         self._eia_key,
                    "frequency":       "daily",
                    "data[0]":         "value",
                    "facets[type][]":  "D",
                    "length":          365,
                },
                timeout=30,
            )
            if not data:
                return 0

            response_data = data.get("response", {}).get("data", [])
            rows = []
            for item in response_data:
                region     = item.get("respondent", "UNKNOWN")
                series_name = f"EIA_ELECTRICITY_{region}"
                date        = item.get("period", "")
                value       = item.get("value")
                if not date or value is None:
                    continue
                rows.append((
                    series_name, None, date,
                    float(value), "EIA", json.dumps(item),
                ))

            with self._perm_conn() as conn:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO raw_macro_data
                      (series_name, series_id, date, value, source, raw_json)
                    VALUES (?,?,?,?,?,?)
                    """,
                    rows
                )
                conn.commit()
            inserted = len(rows)
            logger.info("EIA Electricity: inserted %d records", inserted)

        except Exception as exc:
            logger.warning("EIA electricity collection failed: %s", exc)

        return inserted

    # ── Extreme events ────────────────────────────────────────────────────────

    def get_extreme_events(self, threshold: float = 2.0) -> List[Dict[str, Any]]:
        """
        Returns list of current extreme weather events where any anomaly
        exceeds the given threshold (in std deviations).
        """
        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            with self._perm_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT location, date,
                           temperature_anomaly, precip_anomaly, windspeed_zscore,
                           weather_risk_score
                    FROM raw_weather_data
                    WHERE is_extreme = 1
                      AND date >= ?
                      AND source IN ('OPEN_METEO_HIST','OPEN_METEO_RECENT','OWM_CURRENT')
                    ORDER BY date DESC
                    """,
                    (cutoff,)
                ).fetchall()
            events = []
            for r in rows:
                loc, date, t_anom, p_anom, w_z, risk = r
                anoms = {
                    "temperature_anomaly": t_anom,
                    "precip_anomaly": p_anom,
                    "windspeed_zscore": w_z,
                }
                active = {k: v for k, v in anoms.items()
                          if v is not None and abs(v) > threshold}
                if active:
                    events.append({
                        "location":          loc,
                        "date":              date,
                        "anomalies":         active,
                        "weather_risk_score": risk,
                    })
            return events
        except Exception as exc:
            logger.warning("get_extreme_events failed: %s", exc)
            return []

    # ── Master collect ────────────────────────────────────────────────────────

    def collect(self) -> Dict[str, Any]:
        """
        Runs all collections. Returns summary dict.
        """
        logger.info("=== GeographicIntelligence.collect() START ===")
        summary: Dict[str, Any] = {
            "timestamp":          datetime.now().isoformat(),
            "weather_recent":     0,
            "owm_current":        0,
            "pollen":             0,
            "waqi_aqi":           0,
            "earthquakes":        0,
            "fred_regional":      0,
            "eia_electricity":    0,
            "errors":             [],
        }

        # 1. Recent weather (last 7 days) from Open-Meteo
        try:
            result = self.collect_weather(days_back=7)
            summary["weather_recent"] = sum(result.values())
        except Exception as exc:
            logger.error("collect_weather failed: %s", exc)
            summary["errors"].append(f"weather_recent: {exc}")

        # 2. OWM current conditions
        try:
            summary["owm_current"] = self._collect_owm()
        except Exception as exc:
            logger.error("OWM collection failed: %s", exc)
            summary["errors"].append(f"owm_current: {exc}")

        # 3. Pollen forecast
        try:
            summary["pollen"] = self._collect_pollen()
        except Exception as exc:
            logger.error("Pollen collection failed: %s", exc)
            summary["errors"].append(f"pollen: {exc}")

        # 4. WAQI air quality
        try:
            summary["waqi_aqi"] = self._collect_waqi()
        except Exception as exc:
            logger.error("WAQI collection failed: %s", exc)
            summary["errors"].append(f"waqi: {exc}")

        # 5. Earthquakes
        try:
            summary["earthquakes"] = self._collect_earthquakes()
        except Exception as exc:
            logger.error("Earthquake collection failed: %s", exc)
            summary["errors"].append(f"earthquakes: {exc}")

        # 6. FRED regional
        try:
            summary["fred_regional"] = self._collect_fred()
        except Exception as exc:
            logger.error("FRED regional collection failed: %s", exc)
            summary["errors"].append(f"fred_regional: {exc}")

        # 7. EIA electricity
        try:
            summary["eia_electricity"] = self._collect_eia()
        except Exception as exc:
            logger.error("EIA electricity collection failed: %s", exc)
            summary["errors"].append(f"eia_electricity: {exc}")

        logger.info("=== GeographicIntelligence.collect() DONE: %s ===", summary)
        return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    gi = GeographicIntelligence()
    if len(sys.argv) > 1 and sys.argv[1] == "historical":
        print("Running full historical backfill (2010-01-01 to today)...")
        result = gi.collect_historical()
        print(f"Done: {result}")
    else:
        print("Running standard collection...")
        summary = gi.collect()
        print(json.dumps(summary, indent=2))
