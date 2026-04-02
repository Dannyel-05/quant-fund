import json
import logging
import math
import os
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")
WIKI_TITLE_CACHE = os.path.join(CACHE_DIR, "wiki_titles.json")

PAGEVIEW_BASE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}"
)
REVISION_API = (
    "https://en.wikipedia.org/w/api.php"
)
HEADERS = {"User-Agent": "quant-fund-altdata/1.0 (research; contact@example.com)"}


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
    """Resolve long company name from yfinance (for Wikipedia title lookup)."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception as exc:
        logger.warning("yfinance name lookup failed for %s: %s", ticker, exc)
        return ticker


def _resolve_wiki_title(ticker: str, title_cache: dict) -> Optional[str]:
    """
    Try to find a valid Wikipedia article title for the ticker.
    Search order: cache -> ticker itself -> company name.
    """
    if ticker in title_cache:
        return title_cache[ticker]

    candidates = [ticker]
    company_name = _get_company_name(ticker)
    if company_name and company_name != ticker:
        candidates.append(company_name)

    for candidate in candidates:
        encoded = urllib.parse.quote(candidate, safe="")
        url = f"https://en.wikipedia.org/wiki/{encoded}"
        try:
            resp = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                # Use the final URL's last path segment as title
                final_title = urllib.parse.unquote(resp.url.rsplit("/", 1)[-1])
                title_cache[ticker] = final_title
                return final_title
        except Exception as exc:
            logger.warning("Wikipedia title check failed for '%s': %s", candidate, exc)

    logger.info("No Wikipedia article found for ticker %s", ticker)
    title_cache[ticker] = None
    return None


def _fetch_pageviews(title: str, start: datetime, end: datetime) -> List[int]:
    """Fetch daily pageview counts for a Wikipedia article."""
    encoded = urllib.parse.quote(title, safe="")
    url = PAGEVIEW_BASE.format(
        article=encoded,
        start=start.strftime("%Y%m%d"),
        end=end.strftime("%Y%m%d"),
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            logger.info("No pageview data for '%s'", title)
            return []
        resp.raise_for_status()
        items = resp.json().get("items", [])
        return [int(item["views"]) for item in items if "views" in item]
    except Exception as exc:
        logger.warning("Pageview fetch failed for '%s': %s", title, exc)
        return []


def _fetch_recent_edits(title: str, days: int = 30) -> List[str]:
    """Fetch revision timestamps for the last `days` days."""
    params = {
        "action": "query",
        "titles": title,
        "prop": "revisions",
        "rvprop": "timestamp",
        "rvlimit": "100",
        "format": "json",
        "rvstart": (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rvdir": "newer",
    }
    try:
        resp = requests.get(REVISION_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        pages = resp.json().get("query", {}).get("pages", {})
        for page_data in pages.values():
            revisions = page_data.get("revisions", [])
            return [r["timestamp"] for r in revisions if "timestamp" in r]
        return []
    except Exception as exc:
        logger.warning("Revision fetch failed for '%s': %s", title, exc)
        return []


def _pageview_stats(views: List[int]) -> Tuple[float, float, float, float]:
    """Return (last_7d_avg, last_30d_avg, last_30d_std, zscore)."""
    import numpy as np

    if not views:
        return 0.0, 0.0, 1.0, 0.0

    arr = np.array(views, dtype=float)
    last_7 = arr[-7:] if len(arr) >= 7 else arr
    avg_7d = float(last_7.mean())
    avg_30d = float(arr.mean())
    std_30d = float(arr.std()) if len(arr) > 1 else 1.0

    if std_30d < 1.0:
        std_30d = 1.0
    zscore = float((avg_7d - avg_30d) / std_30d)
    return avg_7d, avg_30d, std_30d, zscore


def _edit_stats(
    timestamps: List[str],
) -> Tuple[float, float, float]:
    """Return (recent_7d_edits, baseline_daily_avg, edit_zscore)."""
    import numpy as np

    if not timestamps:
        return 0.0, 0.0, 0.0

    now = datetime.utcnow()
    cutoff_7d = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)

    recent_count = 0
    weekly_buckets: List[int] = [0] * 4  # 4 weeks

    for ts_str in timestamps:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).replace(tzinfo=None)
            if ts < cutoff_30d:
                continue
            age_days = (now - ts).total_seconds() / 86400
            week_idx = int(age_days // 7)
            if 0 <= week_idx < 4:
                weekly_buckets[week_idx] += 1
            if ts >= cutoff_7d:
                recent_count += 1
        except Exception:
            continue

    baseline = weekly_buckets[1:]  # weeks 1-3 as baseline
    baseline_avg = float(np.mean(baseline)) if baseline else 0.0
    baseline_std = float(np.std(baseline)) if len(baseline) > 1 else 0.1

    edit_zscore = float((recent_count - baseline_avg) / max(baseline_std, 0.1))
    return float(recent_count), baseline_avg, edit_zscore


class WikipediaCollector:
    """
    Collect Wikipedia page-view and edit-frequency signals as
    proxy for public interest in a company.
    """

    def __init__(self, config: dict):
        self.config = config
        _ensure_cache_dir()
        self._title_cache: dict = _load_json_cache(WIKI_TITLE_CACHE)

    def collect(self, tickers: List[str], market: str = "us") -> List[dict]:
        timestamp = datetime.now().isoformat()
        results: List[dict] = []

        for ticker in tickers:
            try:
                result = self._collect_ticker(ticker, market, timestamp)
                results.append(result)
            except Exception as exc:
                logger.error("WikipediaCollector error for %s: %s", ticker, exc)
                results.append(self._empty_result(ticker, market, timestamp, str(exc)))

        # Persist updated title cache
        _save_json_cache(WIKI_TITLE_CACHE, self._title_cache)
        return results

    def _collect_ticker(self, ticker: str, market: str, timestamp: str) -> dict:
        title = _resolve_wiki_title(ticker, self._title_cache)
        if not title:
            return self._empty_result(ticker, market, timestamp, "no_wiki_article")

        now = datetime.utcnow()
        start_30d = now - timedelta(days=30)

        views = _fetch_pageviews(title, start_30d, now)
        avg_7d, avg_30d, std_30d, pageview_zscore = _pageview_stats(views)

        edit_timestamps = _fetch_recent_edits(title, days=30)
        recent_edits, baseline_edits, edit_zscore = _edit_stats(edit_timestamps)

        # WikipediaMomentumScore via tanh
        combined_raw = pageview_zscore * 0.6 + edit_zscore * 0.4
        combined_score = float(math.tanh(combined_raw / 3.0))

        # EDIT_SURGE flag
        edit_surge = edit_zscore > 3.0
        quality_score = 1.0 if edit_surge else min(0.8, 0.3 + len(views) * 0.015)

        raw: dict = {
            "wiki_title": title,
            "pageview_7d_avg": avg_7d,
            "pageview_30d_avg": avg_30d,
            "pageview_30d_std": std_30d,
            "pageview_zscore": pageview_zscore,
            "recent_edits_7d": recent_edits,
            "baseline_edits_weekly_avg": baseline_edits,
            "edit_zscore": edit_zscore,
            "combined_raw": combined_raw,
            "days_of_views": len(views),
        }
        if edit_surge:
            raw["EDIT_SURGE"] = True

        return {
            "source": "wikipedia",
            "ticker": ticker,
            "market": market,
            "data_type": "wikipedia_momentum",
            "value": max(-1.0, min(1.0, combined_score)),
            "raw_data": raw,
            "timestamp": timestamp,
            "quality_score": quality_score,
        }

    def _empty_result(self, ticker: str, market: str, timestamp: str, error: str) -> dict:
        return {
            "source": "wikipedia",
            "ticker": ticker,
            "market": market,
            "data_type": "wikipedia_momentum",
            "value": 0.0,
            "raw_data": {"error": error},
            "timestamp": timestamp,
            "quality_score": 0.0,
        }
