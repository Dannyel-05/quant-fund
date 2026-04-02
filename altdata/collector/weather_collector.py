import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")
OWM_BASE = "https://api.openweathermap.org/data/2.5"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

CACHE_TTL_HOURS = 6
SEASONAL_CACHE_TTL_DAYS = 30

# City size proxy for weighting (max = 1.0)
CITY_WEIGHT: Dict[str, float] = {
    "new york":   1.0,
    "new york city": 1.0,
    "london":     0.9,
    "chicago":    0.8,
    "los angeles": 0.75,
    "tokyo":      0.7,
    "paris":      0.65,
    "frankfurt":  0.6,
    "hong kong":  0.6,
    "shanghai":   0.6,
    "sydney":     0.5,
    "toronto":    0.5,
}

# Sector weather impact rules
# Each sector maps to a dict of condition -> impact value
SECTOR_WEATHER: Dict[str, Dict[str, float]] = {
    "retail":   {"cold_snap": -0.3, "heatwave": -0.2, "severe": -0.5, "mild": 0.1},
    "energy":   {"cold_snap": 0.4, "heatwave": 0.3, "mild": -0.2},
    "transport":{"severe": -0.4, "clear": 0.1},
    "agriculture": {"severe": -0.4, "mild": 0.1, "cold_snap": -0.2},
    "consumer_goods": {"cold_snap": -0.2, "heatwave": -0.1, "severe": -0.3},
}

# yfinance sector string -> internal sector key
YFINANCE_SECTOR_MAP: Dict[str, str] = {
    "consumer cyclical": "retail",
    "consumer defensive": "consumer_goods",
    "energy": "energy",
    "industrials": "transport",
    "utilities": "energy",
    "basic materials": "agriculture",
    "technology": None,
    "healthcare": None,
    "financial services": None,
    "communication services": None,
    "real estate": None,
}


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json_cache(path: str) -> Optional[dict]:
    try:
        if os.path.exists(path):
            with open(path, "r") as fh:
                return json.load(fh)
    except Exception as exc:
        logger.warning("Cache load failed %s: %s", path, exc)
    return None


def _save_json_cache(path: str, data: dict):
    try:
        _ensure_cache_dir()
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2)
    except Exception as exc:
        logger.warning("Cache save failed %s: %s", path, exc)


def _is_cache_fresh(path: str, ttl_hours: float) -> bool:
    if not os.path.exists(path):
        return False
    age_hours = (time.time() - os.path.getmtime(path)) / 3600.0
    return age_hours < ttl_hours


def _weather_cache_path(city: str) -> str:
    today = date.today().isoformat()
    safe_city = city.replace(" ", "_").lower()
    return os.path.join(CACHE_DIR, f"weather_{safe_city}_{today}.json")


def _seasonal_cache_path(city: str) -> str:
    safe_city = city.replace(" ", "_").lower()
    return os.path.join(CACHE_DIR, f"seasonal_baseline_{safe_city}.json")


def _get_ticker_sector(ticker: str) -> Optional[str]:
    """Return internal sector key from yfinance info."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        yfin_sector = (info.get("sector") or "").lower()
        mapped = YFINANCE_SECTOR_MAP.get(yfin_sector)
        return mapped
    except Exception as exc:
        logger.warning("yfinance sector lookup failed for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Open-Meteo helpers (no API key required)
# ---------------------------------------------------------------------------

def _fetch_openmeteo_current(lat: float, lon: float) -> Optional[dict]:
    """
    Fetch current weather from Open-Meteo free forecast API (no API key).
    Returns dict with temp_c, rain_mm, wind_ms or None on failure.
    """
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,precipitation,wind_speed_10m",
            "wind_speed_unit": "ms",
            "timezone": "auto",
            "forecast_days": 1,
        }
        resp = requests.get(OPEN_METEO_FORECAST, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        current = payload.get("current", {})
        return {
            "temp_c":   float(current.get("temperature_2m", 15.0) or 15.0),
            "rain_mm":  float(current.get("precipitation",  0.0)  or 0.0),
            "wind_ms":  float(current.get("wind_speed_10m", 0.0)  or 0.0),
        }
    except Exception as exc:
        logger.warning("Open-Meteo current fetch failed (%.2f, %.2f): %s", lat, lon, exc)
        return None


# ---------------------------------------------------------------------------
# OpenWeatherMap helpers
# ---------------------------------------------------------------------------

def _fetch_current_weather(city: str, api_key: str) -> Optional[dict]:
    """Fetch current weather from OWM."""
    cache_path = _weather_cache_path(city)
    if _is_cache_fresh(cache_path, CACHE_TTL_HOURS):
        cached = _load_json_cache(cache_path)
        if cached and "current" in cached:
            return cached["current"]

    try:
        url = f"{OWM_BASE}/weather"
        params = {"q": city, "appid": api_key, "units": "metric"}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _save_or_merge_weather_cache(cache_path, "current", data)
        return data
    except Exception as exc:
        logger.warning("OWM current weather failed for '%s': %s", city, exc)
        return None


def _fetch_forecast(city: str, api_key: str) -> Optional[dict]:
    """Fetch 5-day/3-hour forecast from OWM."""
    cache_path = _weather_cache_path(city)
    if _is_cache_fresh(cache_path, CACHE_TTL_HOURS):
        cached = _load_json_cache(cache_path)
        if cached and "forecast" in cached:
            return cached["forecast"]

    try:
        url = f"{OWM_BASE}/forecast"
        params = {"q": city, "appid": api_key, "units": "metric"}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _save_or_merge_weather_cache(cache_path, "forecast", data)
        return data
    except Exception as exc:
        logger.warning("OWM forecast failed for '%s': %s", city, exc)
        return None


def _save_or_merge_weather_cache(cache_path: str, key: str, value):
    existing = _load_json_cache(cache_path) or {}
    existing[key] = value
    _save_json_cache(cache_path, existing)


# ---------------------------------------------------------------------------
# Open-Meteo seasonal baseline
# ---------------------------------------------------------------------------

def _fetch_seasonal_baseline(
    city: str, lat: float, lon: float
) -> Dict[int, float]:
    """
    Return dict {month (1-12): avg_temperature_C} computed from
    5-year historical data via Open-Meteo archive API.
    Cached for SEASONAL_CACHE_TTL_DAYS days.
    """
    cache_path = _seasonal_cache_path(city)
    if _is_cache_fresh(cache_path, SEASONAL_CACHE_TTL_DAYS * 24):
        cached = _load_json_cache(cache_path)
        if cached:
            return {int(k): v for k, v in cached.items()}

    try:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=5 * 365)
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": "temperature_2m_mean",
            "timezone": "UTC",
        }
        resp = requests.get(OPEN_METEO_ARCHIVE, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        dates = payload.get("daily", {}).get("time", [])
        temps = payload.get("daily", {}).get("temperature_2m_mean", [])

        if not dates or not temps:
            return {}

        monthly: Dict[int, List[float]] = {m: [] for m in range(1, 13)}
        for d_str, t in zip(dates, temps):
            if t is None:
                continue
            month = int(d_str[5:7])
            monthly[month].append(float(t))

        baseline = {}
        for m, vals in monthly.items():
            if vals:
                baseline[m] = sum(vals) / len(vals)

        _save_json_cache(cache_path, baseline)
        return baseline

    except Exception as exc:
        logger.warning("Open-Meteo seasonal baseline failed for '%s': %s", city, exc)
        return {}


# ---------------------------------------------------------------------------
# Weather condition classification
# ---------------------------------------------------------------------------

def _classify_weather(
    temp_c: float,
    seasonal_temp: float,
    rain_mm: float,
    wind_ms: float,
) -> str:
    """
    Return one of: 'cold_snap', 'heatwave', 'severe', 'mild', 'clear'
    """
    deviation = temp_c - seasonal_temp
    extreme_rain = rain_mm > 20.0
    extreme_wind = wind_ms > 15.0

    if extreme_rain or extreme_wind:
        return "severe"
    if deviation < -5.0:
        return "cold_snap"
    if deviation > 5.0:
        return "heatwave"
    if -2.0 <= deviation <= 2.0:
        return "mild"
    return "clear"


def _extract_current_conditions(owm_data: dict) -> Tuple[float, float, float]:
    """Extract (temp_c, rain_mm_1h, wind_ms) from OWM current weather payload."""
    try:
        temp = float(owm_data.get("main", {}).get("temp", 15.0))
        rain = float(owm_data.get("rain", {}).get("1h", 0.0))
        wind = float(owm_data.get("wind", {}).get("speed", 0.0))
        return temp, rain, wind
    except Exception:
        return 15.0, 0.0, 0.0


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

class WeatherCollector:
    """
    Collect current weather conditions for tracked cities and compute
    WeatherRiskScore per ticker based on sector sensitivity.
    """

    def __init__(self, config: dict):
        self.config = config
        _ensure_cache_dir()
        # City coordinates for Open-Meteo seasonal baseline
        self._city_coords: Dict[str, Tuple[float, float]] = {
            "new york":      (40.71, -74.01),
            "new york city": (40.71, -74.01),
            "london":        (51.51, -0.13),
            "chicago":       (41.88, -87.63),
            "los angeles":   (34.05, -118.24),
            "tokyo":         (35.68, 139.69),
            "paris":         (48.86, 2.35),
            "frankfurt":     (50.11, 8.68),
            "hong kong":     (22.33, 114.17),
            "shanghai":      (31.23, 121.47),
            "sydney":        (-33.87, 151.21),
            "toronto":       (43.65, -79.38),
        }

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        timestamp = datetime.now().isoformat()
        api_key = self.config.get("openweathermap_api_key")

        cities: List[str] = self.config.get("weather_cities", ["New York", "London", "Chicago"])

        # Collect per-city weather once
        city_conditions: Dict[str, dict] = {}
        for city in cities:
            city_conditions[city] = self._collect_city(city, api_key)

        # Drop cities that returned hard errors
        city_conditions = {k: v for k, v in city_conditions.items() if "error" not in v}

        if not city_conditions:
            logger.warning("WeatherCollector: no city data obtained")
            return []

        results: List[dict] = []
        for ticker in tickers:
            try:
                result = self._score_ticker(ticker, market, timestamp, city_conditions)
                results.append(result)
            except Exception as exc:
                logger.error("WeatherCollector error for %s: %s", ticker, exc)
                results.append(self._empty_result(ticker, market, timestamp, str(exc)))

        return results

    def _collect_city(self, city: str, api_key: Optional[str]) -> dict:
        city_lower = city.lower()
        coords = self._city_coords.get(city_lower)

        # ── Seasonal baseline (Open-Meteo archive, no key needed) ─────────
        seasonal_avg: Optional[float] = None
        if coords:
            baseline = _fetch_seasonal_baseline(city, coords[0], coords[1])
            month = date.today().month
            seasonal_avg = baseline.get(month)

        # ── Current conditions ─────────────────────────────────────────────
        temp_c = rain_mm = wind_ms = None

        if api_key:
            # Prefer OWM when key is available
            current = _fetch_current_weather(city, api_key)
            forecast = _fetch_forecast(city, api_key)
            has_forecast = forecast is not None
            if current:
                temp_c, rain_mm, wind_ms = _extract_current_conditions(current)

        if temp_c is None and coords:
            # Fallback: Open-Meteo forecast (no key required)
            om = _fetch_openmeteo_current(coords[0], coords[1])
            if om:
                temp_c  = om["temp_c"]
                rain_mm = om["rain_mm"]
                wind_ms = om["wind_ms"]
                has_forecast = True
            else:
                has_forecast = False

        if temp_c is None:
            return {"error": "no_current_data", "city": city}

        if seasonal_avg is None:
            seasonal_avg = 12.0  # neutral fallback

        deviation = temp_c - seasonal_avg
        condition = _classify_weather(temp_c, seasonal_avg, rain_mm or 0.0, wind_ms or 0.0)

        return {
            "city":           city,
            "temp_c":         temp_c,
            "rain_mm":        rain_mm or 0.0,
            "wind_ms":        wind_ms or 0.0,
            "seasonal_avg_c": seasonal_avg,
            "deviation_c":    deviation,
            "condition":      condition,
            "weight":         CITY_WEIGHT.get(city_lower, 0.4),
            "has_forecast":   has_forecast,
            "source":         "owm" if api_key and temp_c is not None else "open_meteo",
        }

    def _score_ticker(
        self,
        ticker: str,
        market: str,
        timestamp: str,
        city_conditions: Dict[str, dict],
    ) -> dict:
        sector = _get_ticker_sector(ticker)
        sector_map = SECTOR_WEATHER.get(sector) if sector else None

        if not sector_map:
            # Unknown sector: no weather signal
            return {
                "source": "weather",
                "ticker": ticker,
                "market": market,
                "data_type": "weather_impact",
                "value": 0.0,
                "raw_data": {
                    "sector": sector,
                    "note": "sector_not_mapped",
                    "city_conditions": city_conditions,
                },
                "timestamp": timestamp,
                "quality_score": 0.3,
            }

        weighted_impact = 0.0
        total_weight = 0.0
        city_impacts: Dict[str, float] = {}

        for city, cdata in city_conditions.items():
            if "error" in cdata:
                continue
            condition = cdata.get("condition", "clear")
            weight = cdata.get("weight", 0.4)
            impact = sector_map.get(condition, 0.0)
            weighted_impact += impact * weight
            total_weight += weight
            city_impacts[city] = impact

        if total_weight > 0:
            final_score = weighted_impact / total_weight
        else:
            final_score = 0.0

        final_score = max(-1.0, min(1.0, final_score))
        quality = 0.7 if total_weight > 0 else 0.2

        raw: dict = {
            "sector": sector,
            "city_impacts": city_impacts,
            "city_conditions": city_conditions,
            "weighted_impact": weighted_impact,
            "total_weight": total_weight,
        }

        return {
            "source": "weather",
            "ticker": ticker,
            "market": market,
            "data_type": "weather_impact",
            "value": final_score,
            "raw_data": raw,
            "timestamp": timestamp,
            "quality_score": quality,
        }

    def _empty_result(self, ticker: str, market: str, timestamp: str, error: str) -> dict:
        return {
            "source": "weather",
            "ticker": ticker,
            "market": market,
            "data_type": "weather_impact",
            "value": 0.0,
            "raw_data": {"error": error},
            "timestamp": timestamp,
            "quality_score": 0.0,
        }
