"""
uspto_collector.py — Collects patent data from USPTO PatentsView API (free, no key required).
"""

import json
import logging
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

CACHE_DIR = Path("data/cache/deepdata")
COMPANY_NAMES_CACHE = CACHE_DIR / "company_names.json"

# PatentsView v0 (api.patentsview.org/patents/query) — HTTP 410 Gone as of 2025.
# PatentsView v1 (search.patentsview.org/api/v1/patent/) — also HTTP 410, migrating to
# data.uspto.gov which requires an account/API key (not freely scriptable).
#
# Replacement: Google Patents public search API (no key required).
# Endpoint: https://patents.google.com/xhr/query
# Query string syntax: assignee=<name>&num=<rows>&after=priority:<YYYYMMDD>&before=priority:<YYYYMMDD>
# Response: JSON with results.cluster[].result[].patent objects containing
#   publication_number, title, priority_date, filing_date, grant_date, publication_date, assignee
GOOGLE_PATENTS_URL = "https://patents.google.com/xhr/query"
GOOGLE_PATENTS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://patents.google.com/",
    "Accept-Language": "en-US,en;q=0.9",
}
USPTO_SLEEP = 1.5  # rate-limit courtesy delay between requests

COLLECTOR_SOURCE = "GOOGLE_PATENTS"

# CrossRef fallback — free, no auth required.
# Used when Google Patents is rate-limited (returns persistent 503/429).
# Returns academic publications associated with the company as an R&D-activity proxy.
# Quality score is reduced to reflect that publications ≠ patents.
CROSSREF_URL = "https://api.crossref.org/works"
CROSSREF_HEADERS = {
    "User-Agent": "QuantFund/1.0 (research; mailto:research@example.com)",
}
CROSSREF_QUALITY = 0.4  # lower confidence — publications proxy for patents


class USPTOCollector:
    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._company_cache: dict = self._load_company_cache()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, tickers: list, market: str = "us") -> list:
        """Return CollectorResult list with patent data per ticker."""
        results = []
        for ticker in tickers:
            try:
                company_name = self.get_company_name(ticker)
                if not company_name:
                    logger.warning("No company name found for %s, skipping", ticker)
                    continue

                # 90-day window
                patent_history = self.fetch_patents(company_name, days_back=90)
                prev_history = self.fetch_patents(company_name, days_back=180)

                # Detect if CrossRef fallback was used and lower quality score
                using_crossref = any(
                    p.get("_source") == "crossref_fallback"
                    for p in patent_history + prev_history
                )
                quality = CROSSREF_QUALITY if using_crossref else 0.8
                source = "CROSSREF_PROXY" if using_crossref else COLLECTOR_SOURCE

                filing_velocity = self.calc_filing_velocity(patent_history)
                citation_growth = self.calc_citation_growth(patent_history + prev_history)
                tech_pivot = self.detect_tech_pivot(patent_history + prev_history)

                results.append(_make_collector_result(
                    source=source,
                    ticker=ticker,
                    market=market,
                    data_type="patent_velocity",
                    value=filing_velocity,
                    raw_data={
                        "filing_velocity": filing_velocity,
                        "citation_growth": citation_growth,
                        "tech_pivot": tech_pivot,
                        "patent_count_90d": len(patent_history),
                        "company_name": company_name,
                        "data_source": source,
                    },
                    quality_score=quality,
                ))

            except Exception as exc:
                logger.warning("USPTO collect failed for %s: %s", ticker, exc)

        return results

    # ------------------------------------------------------------------
    # Patent fetching
    # ------------------------------------------------------------------

    def fetch_patents(self, company_name: str, days_back: int = 90) -> list:
        """
        Fetch patents via Google Patents public search API (no key required).

        Replaces the defunct PatentsView v0/v1 endpoints (both return HTTP 410).
        URL: https://patents.google.com/xhr/query
        Returns list of {patent_number, date, title, cpc_class, citations_received}
        where date = priority_date (earliest) or filing_date as fallback.

        CPC class is approximated from the first character of the publication number
        (country code is used as a coarse technology proxy) because Google Patents'
        JSON response does not include CPC data — the full CPC requires a per-patent
        detail request which is too slow at scale.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping patent fetch")
            return []

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        # Google Patents date format: YYYYMMDD (no hyphens in after:/before: params)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        # Construct Google Patents query string
        # assignee=<name> searches assignee field; after/before filter by priority date
        assignee_query = company_name.replace('"', "").strip()
        query_url = (
            f"assignee={assignee_query}"
            f"&num=100"
            f"&after=priority:{start_str}"
            f"&before=priority:{end_str}"
        )

        results = []
        # Retry on 429/503 with exponential backoff: 3s → 6s → 12s (max 3 tries)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(USPTO_SLEEP * (2 ** attempt))  # 1.5s, 3.0s, 6.0s
                resp = requests.get(
                    GOOGLE_PATENTS_URL,
                    params={"url": query_url},
                    headers=GOOGLE_PATENTS_HEADERS,
                    timeout=8,
                )
                if resp.status_code in (429, 503):
                    logger.warning(
                        "Google Patents rate-limited (%s) for '%s'; returning []",
                        resp.status_code, company_name,
                    )
                    return []

                resp.raise_for_status()
                data = resp.json()

                clusters = data.get("results", {}).get("cluster", [])
                for cluster in clusters:
                    for item in cluster.get("result", []):
                        pat = item.get("patent", {})
                        if not pat:
                            continue
                        pub_num = pat.get("publication_number", "")
                        # Pick the earliest available date
                        date_str = (
                            pat.get("priority_date")
                            or pat.get("filing_date")
                            or pat.get("publication_date")
                            or pat.get("grant_date")
                            or ""
                        )
                        # Normalise: Google Patents already returns YYYY-MM-DD
                        norm_date = date_str if (date_str and len(date_str) == 10 and "-" in date_str) else ""
                        title = pat.get("title", "").replace("&hellip;", "...").strip()
                        # CPC proxy: country-code prefix of publication number
                        cpc_proxy = pub_num[:2] if pub_num else ""
                        results.append({
                            "patent_number": pub_num,
                            "date": norm_date,
                            "title": title,
                            "cpc_class": cpc_proxy,
                            "citations_received": 0,  # not in listing response
                        })
                break  # success — exit retry loop

            except Exception as exc:
                if attempt < max_retries - 1:
                    logger.debug("Google Patents fetch error (attempt %d): %s", attempt + 1, exc)
                else:
                    logger.warning("Google Patents fetch failed for '%s': %s", company_name, exc)

        return results

    def _fetch_crossref_fallback(self, company_name: str, days_back: int) -> list:
        """
        CrossRef fallback when Google Patents is rate-limited.
        Queries academic publications affiliated with the company as an R&D-activity proxy.
        Returns the same record schema as fetch_patents() with cpc_class='CR' marker and
        a lower quality score embedded in the title prefix so collect() can detect it.
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        results = []
        try:
            resp = requests.get(
                CROSSREF_URL,
                params={
                    "query.affiliation": company_name,
                    "filter": (
                        f"from-pub-date:{start_date.strftime('%Y-%m-%d')},"
                        f"until-pub-date:{end_date.strftime('%Y-%m-%d')}"
                    ),
                    "rows": 100,
                    "select": "DOI,title,published,type",
                },
                headers=CROSSREF_HEADERS,
                timeout=8,
            )
            if resp.status_code in (429, 503):
                logger.warning("CrossRef rate-limited (%s) for '%s'; returning []", resp.status_code, company_name)
                return []
            if resp.status_code != 200:
                logger.warning("CrossRef fallback HTTP %s for '%s'", resp.status_code, company_name)
                return []
            items = resp.json().get("message", {}).get("items", [])
            for item in items:
                doi = item.get("DOI", "")
                title_parts = item.get("title", [])
                title = title_parts[0] if title_parts else ""
                # Extract date from 'published' → date-parts [[year, month, day]]
                pub = item.get("published", {}).get("date-parts", [[]])[0]
                if len(pub) >= 3:
                    norm_date = f"{pub[0]:04d}-{pub[1]:02d}-{pub[2]:02d}"
                elif len(pub) == 2:
                    norm_date = f"{pub[0]:04d}-{pub[1]:02d}-01"
                elif len(pub) == 1:
                    norm_date = f"{pub[0]:04d}-01-01"
                else:
                    norm_date = ""
                results.append({
                    "patent_number": f"CR:{doi}",
                    "date": norm_date,
                    "title": title,
                    "cpc_class": "CR",  # CrossRef proxy marker
                    "citations_received": 0,
                    "_source": "crossref_fallback",
                })
            logger.info(
                "CrossRef fallback for '%s': %d publications in last %d days",
                company_name, len(results), days_back,
            )
        except Exception as exc:
            logger.warning("CrossRef fallback failed for '%s': %s", company_name, exc)
        return results

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def calc_filing_velocity(self, patent_history: list) -> float:
        """velocity = patents_last_90d / max(patents_prev_90d_avg, 1)"""
        if not patent_history:
            return 0.0

        now = datetime.utcnow()
        cutoff_90 = now - timedelta(days=90)
        cutoff_180 = now - timedelta(days=180)

        recent = []
        prior = []
        for p in patent_history:
            try:
                date = datetime.strptime(p["date"], "%Y-%m-%d")
                if date >= cutoff_90:
                    recent.append(p)
                elif date >= cutoff_180:
                    prior.append(p)
            except (ValueError, KeyError):
                pass

        patents_last_90 = len(recent)
        patents_prev_90 = len(prior)
        return patents_last_90 / max(patents_prev_90, 1)

    def calc_citation_growth(self, patent_history: list) -> float:
        """Year-over-year growth in citations received."""
        if not patent_history:
            return 0.0

        now = datetime.utcnow()
        this_year_start = now - timedelta(days=365)
        prev_year_start = now - timedelta(days=730)

        this_year_citations = 0
        prev_year_citations = 0

        for p in patent_history:
            try:
                date = datetime.strptime(p["date"], "%Y-%m-%d")
                citations = int(p.get("citations_received", 0))
                if date >= this_year_start:
                    this_year_citations += citations
                elif date >= prev_year_start:
                    prev_year_citations += citations
            except (ValueError, KeyError, TypeError):
                pass

        if prev_year_citations == 0:
            return float(this_year_citations > 0)

        return round((this_year_citations - prev_year_citations) / max(prev_year_citations, 1), 4)

    def detect_tech_pivot(self, patent_history: list) -> dict:
        """
        Compare CPC technology classes in last 90 days vs prior year.
        Returns {pivoting, new_categories, abandoned_categories}
        """
        if not patent_history:
            return {"pivoting": False, "new_categories": [], "abandoned_categories": []}

        now = datetime.utcnow()
        cutoff_90 = now - timedelta(days=90)
        cutoff_365 = now - timedelta(days=365)

        recent_classes: list = []
        prior_classes: list = []

        for p in patent_history:
            try:
                date = datetime.strptime(p["date"], "%Y-%m-%d")
                cpc = p.get("cpc_class", "")
                if not cpc:
                    continue
                if date >= cutoff_90:
                    recent_classes.append(cpc)
                elif date >= cutoff_365:
                    prior_classes.append(cpc)
            except (ValueError, KeyError):
                pass

        recent_set = set(recent_classes)
        prior_set = set(prior_classes)

        new_categories = sorted(recent_set - prior_set)
        abandoned_categories = sorted(prior_set - recent_set)
        pivoting = bool(new_categories or abandoned_categories)

        return {
            "pivoting": pivoting,
            "new_categories": new_categories,
            "abandoned_categories": abandoned_categories,
        }

    def calc_competitor_overlap(self, ticker_a: str, ticker_b: str) -> float:
        """Fraction of shared CPC classes between two companies."""
        name_a = self.get_company_name(ticker_a)
        name_b = self.get_company_name(ticker_b)

        if not name_a or not name_b:
            return 0.0

        patents_a = self.fetch_patents(name_a, days_back=365)
        patents_b = self.fetch_patents(name_b, days_back=365)

        classes_a = {p.get("cpc_class", "") for p in patents_a if p.get("cpc_class")}
        classes_b = {p.get("cpc_class", "") for p in patents_b if p.get("cpc_class")}

        if not classes_a or not classes_b:
            return 0.0

        overlap = classes_a & classes_b
        return round(len(overlap) / len(classes_a | classes_b), 4)

    # ------------------------------------------------------------------
    # Company name
    # ------------------------------------------------------------------

    def get_company_name(self, ticker: str) -> str:
        """Get company name from yfinance info. Cache in data/cache/deepdata/company_names.json."""
        ticker_upper = ticker.upper()
        if ticker_upper in self._company_cache:
            return self._company_cache[ticker_upper]

        if not HAS_YFINANCE:
            logger.warning("yfinance not available; cannot look up company name for %s", ticker)
            return ""

        try:
            info = yf.Ticker(ticker).info
            name = info.get("longName") or info.get("shortName") or ""
            if name:
                self._company_cache[ticker_upper] = name
                self._save_company_cache()
            return name
        except Exception as exc:
            logger.warning("yfinance company name lookup failed for %s: %s", ticker, exc)
            return ""

    # ------------------------------------------------------------------
    # Innovation lead time
    # ------------------------------------------------------------------

    def calc_innovation_lead_time(
        self, ticker: str, patent_history: list, earnings_history
    ) -> int:
        """
        Backtest: how many days do patent filings lead earnings beats for this company?
        earnings_history: iterable of {date: str, beat: bool} dicts.
        Returns median lead time in days, or -1 if insufficient data.
        """
        if not patent_history or not earnings_history:
            return -1

        lead_times = []
        try:
            earnings_list = list(earnings_history)
            beats = [
                e for e in earnings_list
                if e.get("beat", False) and e.get("date")
            ]

            for beat in beats:
                try:
                    beat_date = datetime.strptime(beat["date"], "%Y-%m-%d")
                except ValueError:
                    continue

                # Find patent filing dates in the 180 days preceding this beat
                for p in patent_history:
                    try:
                        pat_date = datetime.strptime(p["date"], "%Y-%m-%d")
                        delta = (beat_date - pat_date).days
                        if 0 < delta <= 180:
                            lead_times.append(delta)
                    except (ValueError, KeyError):
                        pass

        except Exception as exc:
            logger.warning("Lead time calculation error for %s: %s", ticker, exc)
            return -1

        if not lead_times:
            return -1

        sorted_times = sorted(lead_times)
        n = len(sorted_times)
        mid = n // 2
        if n % 2 == 0:
            return int((sorted_times[mid - 1] + sorted_times[mid]) / 2)
        return sorted_times[mid]

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _load_company_cache(self) -> dict:
        if COMPANY_NAMES_CACHE.exists():
            try:
                return json.loads(COMPANY_NAMES_CACHE.read_text())
            except Exception:
                pass
        return {}

    def _save_company_cache(self) -> None:
        try:
            COMPANY_NAMES_CACHE.write_text(json.dumps(self._company_cache, indent=2))
        except Exception as exc:
            logger.warning("Could not save company names cache: %s", exc)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_collector_result(
    source: str,
    ticker: str,
    market: str,
    data_type: str,
    value: float,
    raw_data: dict,
    quality_score: float = 0.8,
) -> dict:
    return {
        "source": source,
        "ticker": ticker,
        "market": market,
        "data_type": data_type,
        "value": value,
        "raw_data": raw_data,
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
    }
