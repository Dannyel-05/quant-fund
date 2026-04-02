import json
import logging
import math
import os
import random
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")
COMPANY_NAME_CACHE = os.path.join(CACHE_DIR, "company_names.json")
CACHE_TTL_HOURS = 12

BREAKOUT_KEYWORDS = {"recall", "merger", "acquisition", "fraud", "buyout", "sec", "lawsuit"}

# Max tickers per pytrends batch
BATCH_SIZE = 5


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json_cache(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r") as fh:
                return json.load(fh)
    except Exception as exc:
        logger.warning("Failed to load cache %s: %s", path, exc)
    return {}


def _save_json_cache(path: str, data: dict):
    try:
        _ensure_cache_dir()
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2)
    except Exception as exc:
        logger.warning("Failed to save cache %s: %s", path, exc)


def _gtrends_cache_path(ticker: str) -> str:
    today = date.today().isoformat()
    return os.path.join(CACHE_DIR, f"gtrends_{ticker}_{today}.json")


def _is_cache_fresh(path: str) -> bool:
    if not os.path.exists(path):
        return False
    mtime = os.path.getmtime(path)
    age_hours = (time.time() - mtime) / 3600.0
    return age_hours < CACHE_TTL_HOURS


def _get_company_name(ticker: str) -> str:
    cache = _load_json_cache(COMPANY_NAME_CACHE)
    if ticker in cache:
        return cache[ticker]
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        name = info.get("longName") or info.get("shortName") or ticker
    except Exception as exc:
        logger.warning("yfinance name lookup failed for %s: %s", ticker, exc)
        name = ticker
    cache[ticker] = name
    _save_json_cache(COMPANY_NAME_CACHE, cache)
    return name


def _linear_slope(values: List[float]) -> float:
    """Compute slope of linear regression over the values list."""
    if len(values) < 2:
        return 0.0
    n = len(values)
    x = np.arange(n, dtype=float)
    y = np.array(values, dtype=float)
    x_mean, y_mean = x.mean(), y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom < 1e-8:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def _compute_google_interest_score(df_col: "pd.Series") -> Tuple[float, float, dict]:
    """
    Compute GoogleInterestScore from a pytrends interest Series.
    Returns (score, quality_score, metrics_dict).
    """
    import pandas as pd

    if df_col is None or len(df_col) == 0:
        return 0.0, 0.3, {"note": "empty_result"}

    values = df_col.dropna().tolist()
    if not values:
        return 0.0, 0.3, {"note": "all_nan"}

    values_arr = np.array(values, dtype=float)
    n = len(values_arr)

    # 4-week / 12-week moving averages (weekly data: 4 vs 12 points)
    ma_4w = float(values_arr[-4:].mean()) if n >= 4 else float(values_arr.mean())
    ma_12w = float(values_arr[-12:].mean()) if n >= 12 else float(values_arr.mean())

    # z-score of last value vs 90-day history
    history = values_arr[:-1] if n > 1 else values_arr
    mu = history.mean()
    sigma = history.std() if history.std() > 1e-8 else 1.0
    last_val = float(values_arr[-1])
    z_score = float((last_val - mu) / sigma)

    # Trend direction: slope over last 4 weeks
    trend_direction = _linear_slope(values_arr[-4:].tolist() if n >= 4 else values_arr.tolist())

    direction_factor = 1.0 if trend_direction >= 0 else 0.7
    google_score = float(math.tanh(z_score / 2.0) * direction_factor)
    google_score = max(-1.0, min(1.0, google_score))

    quality = min(0.9, 0.4 + n * 0.005)

    metrics = {
        "last_value": last_val,
        "ma_4w": ma_4w,
        "ma_12w": ma_12w,
        "z_score": z_score,
        "trend_direction": trend_direction,
        "n_points": n,
    }
    return google_score, quality, metrics


def _check_related_breakout(related: dict) -> Optional[str]:
    """Scan related queries for breakout terms. Returns found keyword or None."""
    if not related:
        return None
    for query_type in ("top", "rising"):
        queries = related.get(query_type)
        if queries is None or not hasattr(queries, "iterrows"):
            continue
        try:
            for _, row in queries.iterrows():
                q = str(row.get("query", "")).lower()
                for kw in BREAKOUT_KEYWORDS:
                    if kw in q:
                        return kw
        except Exception:
            continue
    return None


class GoogleTrendsCollector:
    """
    Collect Google Trends interest data via pytrends.
    Applies strict rate limiting: 45-90 second sleep between any pytrends call.
    Results cached for 12 hours.
    """

    def __init__(self, config: dict):
        self.config = config
        _ensure_cache_dir()
        self._pytrends = None
        self._last_call_time: float = 0.0
        # Track which tickers need weekly related queries
        self._last_related_fetch: Dict[str, str] = _load_json_cache(
            os.path.join(CACHE_DIR, "gtrends_related_dates.json")
        )

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        timestamp = datetime.now().isoformat()
        geo = self.config.get("google_trends_geo", "US")
        results: List[dict] = []

        # Process in batches of BATCH_SIZE
        batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

        for batch in batches:
            for ticker in batch:
                try:
                    result = self._collect_single(ticker, market, timestamp, geo)
                    results.append(result)
                except Exception as exc:
                    logger.error("GoogleTrendsCollector error for %s: %s", ticker, exc)
                    results.append(self._empty_result(ticker, market, timestamp, str(exc)))

        return results

    def _collect_single(
        self, ticker: str, market: str, timestamp: str, geo: str
    ) -> dict:
        cache_path = _gtrends_cache_path(ticker)

        # Serve from cache if fresh
        if _is_cache_fresh(cache_path):
            cached = _load_json_cache(cache_path)
            if cached:
                logger.debug("GoogleTrends cache hit for %s", ticker)
                return cached

        company_name = _get_company_name(ticker)
        kw_list = [company_name]

        interest_df = self._fetch_interest(kw_list, geo, timeframe="today 3-m")
        if interest_df is None:
            # Rate limit hit — skip this ticker
            result = self._empty_result(ticker, market, timestamp, "rate_limited")
            _save_json_cache(cache_path, result)
            return result

        # Extract series for the keyword
        series = None
        if interest_df is not None and not interest_df.empty:
            col = company_name if company_name in interest_df.columns else interest_df.columns[0]
            series = interest_df[col]

        if series is None or len(series) == 0:
            result = self._empty_result(ticker, market, timestamp, "empty_response")
            result["quality_score"] = 0.3
            _save_json_cache(cache_path, result)
            return result

        score, quality, metrics = _compute_google_interest_score(series)

        raw: dict = {
            "company_name": company_name,
            "geo": geo,
            **metrics,
        }

        # Related queries — run weekly to conserve rate limit budget
        today_str = date.today().isoformat()
        last_related = self._last_related_fetch.get(ticker, "")
        should_fetch_related = (
            not last_related
            or (datetime.fromisoformat(today_str) - datetime.fromisoformat(last_related)).days >= 7
        )

        if should_fetch_related:
            related = self._fetch_related_queries(kw_list, geo)
            if related is not None:
                breakout_kw = _check_related_breakout(related.get(company_name, {}))
                if breakout_kw:
                    raw["BREAKOUT_QUERY"] = breakout_kw
                    quality = 1.0
                    logger.info("BREAKOUT_QUERY detected for %s: '%s'", ticker, breakout_kw)
                self._last_related_fetch[ticker] = today_str
                _save_json_cache(
                    os.path.join(CACHE_DIR, "gtrends_related_dates.json"),
                    self._last_related_fetch,
                )

        result = {
            "source": "google_trends",
            "ticker": ticker,
            "market": market,
            "data_type": "google_interest",
            "value": score,
            "raw_data": raw,
            "timestamp": timestamp,
            "quality_score": quality,
        }
        _save_json_cache(cache_path, result)
        return result

    # ------------------------------------------------------------------
    # pytrends wrappers with rate limiting
    # ------------------------------------------------------------------

    def _rate_limit_sleep(self):
        """Sleep between 45-90 seconds since last pytrends call."""
        elapsed = time.time() - self._last_call_time
        min_wait = random.uniform(45, 90)
        remaining = min_wait - elapsed
        if remaining > 0:
            logger.info("Google Trends rate limit: sleeping %.1f seconds", remaining)
            time.sleep(remaining)
        self._last_call_time = time.time()

    def _get_pytrends(self):
        if self._pytrends is None:
            from pytrends.request import TrendReq
            self._pytrends = TrendReq(hl="en-US", tz=0)
        return self._pytrends

    def _fetch_interest(
        self, kw_list: List[str], geo: str, timeframe: str = "today 3-m"
    ) -> Optional["pd.DataFrame"]:
        """Fetch interest_over_time. Returns None on 429 (after 5-min wait+skip)."""
        self._rate_limit_sleep()
        try:
            pt = self._get_pytrends()
            pt.build_payload(kw_list, timeframe=timeframe, geo=geo)
            df = pt.interest_over_time()
            return df
        except Exception as exc:
            exc_str = str(exc)
            if "429" in exc_str or "Too Many Requests" in exc_str:
                logger.warning("Google Trends 429 — waiting 5 minutes then skipping.")
                time.sleep(300)
                return None
            logger.warning("pytrends interest fetch failed: %s", exc)
            return None

    def _fetch_related_queries(
        self, kw_list: List[str], geo: str
    ) -> Optional[dict]:
        """Fetch related_queries. Returns None on failure."""
        self._rate_limit_sleep()
        try:
            pt = self._get_pytrends()
            pt.build_payload(kw_list, timeframe="today 3-m", geo=geo)
            return pt.related_queries()
        except Exception as exc:
            exc_str = str(exc)
            if "429" in exc_str or "Too Many Requests" in exc_str:
                logger.warning("Google Trends 429 on related queries — waiting 5 minutes.")
                time.sleep(300)
                return None
            logger.warning("pytrends related_queries failed: %s", exc)
            return None

    def _empty_result(self, ticker: str, market: str, timestamp: str, error: str) -> dict:
        return {
            "source": "google_trends",
            "ticker": ticker,
            "market": market,
            "data_type": "google_interest",
            "value": 0.0,
            "raw_data": {"error": error},
            "timestamp": timestamp,
            "quality_score": 0.3,
        }
