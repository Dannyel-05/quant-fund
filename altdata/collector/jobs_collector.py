import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")
COMPANY_NAME_CACHE = os.path.join(CACHE_DIR, "company_names.json")

BLS_BASE = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
BLS_SERIES = {
    "job_openings": "JTS000000000000000JOL",
    "unemployment_rate": "LNS14000000",
}

# Role classification keywords
ENGINEERING_ROLES = ["engineer", "developer", "software", "data scientist", "product manager",
                      "architect", "devops", "machine learning", "ml engineer", "platform"]
SALES_ROLES = ["sales", "account executive", "business development", "revenue", "growth"]
FINANCE_ROLES = ["finance", "financial analyst", "controller", "accounting", "treasurer"]
LEGAL_ROLES = ["legal", "counsel", "attorney", "compliance", "regulatory"]
OPERATIONS_ROLES = ["operations", "supply chain", "logistics", "manufacturing", "plant"]
CSUITE_PATTERNS = [
    r"\bCEO\b", r"\bCFO\b", r"\bCTO\b", r"\bCOO\b", r"\bCHRO\b",
    r"chief executive", r"chief financial", r"chief technology",
    r"chief operating", r"managing director", r"chairman",
]


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


def _get_company_name(ticker: str) -> str:
    """Fetch company long name from yfinance; cache results."""
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


def _classify_title(title: str) -> List[str]:
    """Return list of role categories matched."""
    title_lower = title.lower()
    cats = []
    if any(k in title_lower for k in ENGINEERING_ROLES):
        cats.append("engineering")
    if any(k in title_lower for k in SALES_ROLES):
        cats.append("sales")
    if any(k in title_lower for k in FINANCE_ROLES):
        cats.append("finance")
    if any(k in title_lower for k in LEGAL_ROLES):
        cats.append("legal")
    if any(k in title_lower for k in OPERATIONS_ROLES):
        cats.append("operations")
    return cats


def _is_csuite(title: str) -> bool:
    for pattern in CSUITE_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return True
    return False


# ---------------------------------------------------------------------------
# BLS data helpers
# ---------------------------------------------------------------------------

def _fetch_bls_series(series_id: str) -> List[dict]:
    """Fetch BLS public API v1 for a series. Returns list of {year, period, value}."""
    url = f"{BLS_BASE}{series_id}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") != "REQUEST_SUCCEEDED":
            logger.warning("BLS API non-success for %s: %s", series_id, payload.get("message"))
            return []
        series = payload.get("Results", {}).get("series", [])
        if not series:
            return []
        return series[0].get("data", [])
    except Exception as exc:
        logger.warning("BLS API fetch failed for %s: %s", series_id, exc)
        return []


def _bls_monthly_values(data: List[dict]) -> List[float]:
    """Sort BLS data chronologically and return float values."""
    try:
        sorted_data = sorted(data, key=lambda x: (int(x["year"]), x["period"]))
        return [float(x["value"]) for x in sorted_data if x.get("value") not in (None, "-")]
    except Exception as exc:
        logger.warning("BLS data parse error: %s", exc)
        return []


def _compute_mom_and_zscore(values: List[float]) -> Tuple[float, float]:
    """Compute month-over-month change and z-score of last value."""
    if len(values) < 2:
        return 0.0, 0.0
    import numpy as np
    arr = np.array(values)
    mom = float((arr[-1] - arr[-2]) / abs(arr[-2])) if abs(arr[-2]) > 1e-8 else 0.0
    if len(arr) < 3:
        return mom, 0.0
    history = arr[:-1]
    mu, sigma = history.mean(), history.std()
    if sigma < 1e-8:
        return mom, 0.0
    zscore = float((arr[-1] - mu) / sigma)
    return mom, zscore


# ---------------------------------------------------------------------------
# Indeed RSS helpers
# ---------------------------------------------------------------------------

def _fetch_indeed_postings(company_name: str) -> List[dict]:
    """Fetch Indeed RSS for company. Returns list of {title, published_parsed}."""
    try:
        import feedparser
        url = f"https://www.indeed.com/rss?q={requests.utils.quote(company_name)}&sort=date"
        feed = feedparser.parse(url)
        return list(feed.entries)
    except Exception as exc:
        logger.warning("Indeed RSS fetch failed for '%s': %s", company_name, exc)
        return []


def _compute_hiring_momentum(
    entries: List[dict],
) -> Tuple[float, Dict[str, int], bool]:
    """
    Compute HiringMomentumScore from Indeed entries.
    Returns (score, role_counts, has_csuite).
    """
    import numpy as np

    now = datetime.now(tz=timezone.utc)
    seven_days_ago = now - timedelta(days=7)
    twenty_eight_days_ago = now - timedelta(days=28)

    recent_titles: List[str] = []
    weekly_buckets: List[int] = [0, 0, 0, 0]  # weeks 0-3 (most recent = 0)

    for entry in entries:
        try:
            import time as _time
            ts = entry.get("published_parsed")
            if ts is None:
                continue
            pub = datetime.fromtimestamp(_time.mktime(ts), tz=timezone.utc)
            age = (now - pub).total_seconds()
            if age < 0:
                continue
            week_idx = int(age // (7 * 86400))
            if week_idx < 4:
                weekly_buckets[week_idx] += 1
            if pub >= seven_days_ago:
                recent_titles.append(entry.get("title", ""))
        except Exception:
            continue

    recent_count = weekly_buckets[0]
    past_4w = weekly_buckets[1:4]
    avg_4w = float(np.mean(past_4w)) if past_4w else 0.0
    std_4w = float(np.std(past_4w)) if len(past_4w) > 1 else 1.0

    score = (recent_count - avg_4w) / max(std_4w, 1.0)

    role_counts: Dict[str, int] = {}
    has_csuite = False
    for title in recent_titles:
        for cat in _classify_title(title):
            role_counts[cat] = role_counts.get(cat, 0) + 1
        if _is_csuite(title):
            has_csuite = True

    return score, role_counts, has_csuite


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

class JobsCollector:
    """
    Collect labour market signals:
    - BLS macro series (job openings, unemployment)
    - Indeed RSS per-company hiring momentum
    - Reed UK (graceful fallback — requires API key)
    """

    def __init__(self, config: dict):
        self.config = config
        _ensure_cache_dir()

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        timestamp = datetime.now().isoformat()
        results: List[dict] = []

        # Fetch BLS macro once (not per-ticker)
        bls_data = self._fetch_bls_data()

        for ticker in tickers:
            try:
                result = self._collect_ticker(ticker, market, timestamp, bls_data)
                results.append(result)
            except Exception as exc:
                logger.error("JobsCollector error for %s: %s", ticker, exc)
                results.append(self._empty_result(ticker, market, timestamp, str(exc)))

        return results

    def _fetch_bls_data(self) -> dict:
        bls_out = {}
        for label, series_id in BLS_SERIES.items():
            raw = _fetch_bls_series(series_id)
            values = _bls_monthly_values(raw)
            mom, zscore = _compute_mom_and_zscore(values)
            bls_out[label] = {
                "series_id": series_id,
                "latest_value": values[-1] if values else None,
                "mom_change": mom,
                "zscore": zscore,
                "n_periods": len(values),
            }
        return bls_out

    def _collect_ticker(
        self, ticker: str, market: str, timestamp: str, bls_data: dict
    ) -> dict:
        company_name = _get_company_name(ticker)
        entries = _fetch_indeed_postings(company_name)
        momentum_score, role_counts, has_csuite = _compute_hiring_momentum(entries)

        # --- Translate HiringMomentumScore to value in [-1, +1] ---
        value = 0.0
        growth_roles = role_counts.get("engineering", 0) + role_counts.get("sales", 0)
        stress_roles = role_counts.get("finance", 0) + role_counts.get("legal", 0)

        if momentum_score > 1.5 and growth_roles > 0:
            value = 0.5
        elif momentum_score < -1.5:
            value = -0.4
        else:
            # Linear scale between thresholds
            value = max(-0.4, min(0.5, momentum_score * 0.2))

        # C-suite adjustment
        if has_csuite:
            if stress_roles > growth_roles:
                value -= 0.3
            else:
                value += 0.3
        value = max(-1.0, min(1.0, value))

        # UK Reed: graceful fallback
        reed_data: dict = {}
        if market == "uk":
            reed_data = self._try_reed(company_name)

        # Quality: depends on number of postings found
        quality_score = min(0.9, 0.3 + len(entries) * 0.02)

        raw: dict = {
            "company_name": company_name,
            "total_postings_scraped": len(entries),
            "momentum_score": momentum_score,
            "role_counts": role_counts,
            "has_csuite_posting": has_csuite,
            "bls_macro": bls_data,
            "reed": reed_data,
        }

        return {
            "source": "jobs",
            "ticker": ticker,
            "market": market,
            "data_type": "hiring_momentum",
            "value": value,
            "raw_data": raw,
            "timestamp": timestamp,
            "quality_score": quality_score,
        }

    def _try_reed(self, company_name: str) -> dict:
        """Reed UK requires an API key; return empty gracefully."""
        api_key = self.config.get("reed_api_key")
        if not api_key:
            logger.info("Reed UK API key not configured — skipping Reed data.")
            return {"error": "no_api_key", "note": "Reed requires API key"}
        try:
            url = "https://www.reed.co.uk/api/1.0/search"
            resp = requests.get(
                url,
                params={"keywords": company_name, "resultsToTake": 100},
                auth=(api_key, ""),
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("Reed API call failed for '%s': %s", company_name, exc)
            return {"error": str(exc)}

    def _empty_result(self, ticker: str, market: str, timestamp: str, error: str) -> dict:
        return {
            "source": "jobs",
            "ticker": ticker,
            "market": market,
            "data_type": "hiring_momentum",
            "value": 0.0,
            "raw_data": {"error": error},
            "timestamp": timestamp,
            "quality_score": 0.0,
        }
