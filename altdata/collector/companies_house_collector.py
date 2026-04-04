"""
Companies House alternative data collector (UK equities only).

Uses the free Companies House REST API (requires a free API key).
Only processes tickers ending with '.L' (LSE-listed securities).
"""

import logging
import time
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.company-information.service.gov.uk"
_REQUEST_DELAY = 0.5  # polite delay between calls

# ── filing type mappings ──────────────────────────────────────────────────────

# TM01 = director termination (resignation)
# AP01 = director appointment
# AA   = annual accounts
_FILING_SCORES = {
    "TM01": ("director_resignation", -0.6),
    "AP01": ("director_appointment", 0.2),
}

_LATE_ACCOUNTS_SCORE = -0.5
_NEW_LARGE_SHAREHOLDER_SCORE = 0.1  # per new PSC

# ── helpers ───────────────────────────────────────────────────────────────────

def _make_session(api_key: str) -> requests.Session:
    """Return a requests Session with HTTP Basic auth (key as username, empty pw)."""
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({
        "User-Agent": "quant-fund/1.0",
        "Accept": "application/json",
    })
    return session


def _safe_get(session: requests.Session, url: str, params: dict = None) -> dict | list | None:
    """GET with error handling. Returns parsed JSON or None."""
    try:
        time.sleep(_REQUEST_DELAY)
        resp = session.get(url, params=params or {}, timeout=15)
        if resp.status_code == 429:
            logger.warning("companies_house: rate limited on %s", url)
            return None
        if resp.status_code == 404:
            logger.debug("companies_house: 404 for %s", url)
            return None
        if resp.status_code != 200:
            logger.warning("companies_house: HTTP %s for %s", resp.status_code, url)
            return None
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("companies_house: network error %s: %s", url, exc)
        return None
    except Exception as exc:
        logger.warning("companies_house: unexpected error %s: %s", url, exc)
        return None


def _search_company(session: requests.Session, company_name: str) -> str | None:
    """Return company_number for first matching result, or None."""
    url = f"{_BASE_URL}/search/companies"
    data = _safe_get(session, url, params={"q": company_name, "items_per_page": 5})
    if not data:
        return None
    items = data.get("items", [])
    if not items:
        return None
    return items[0].get("company_number")


def _filing_history(session: requests.Session, company_number: str) -> list:
    """Fetch recent filing history."""
    url = f"{_BASE_URL}/company/{company_number}/filing-history"
    params = {
        "category": "confirmation-statement,accounts,officers",
        "items_per_page": 20,
    }
    data = _safe_get(session, url, params=params)
    if not data:
        return []
    return data.get("items", [])


def _psc_list(session: requests.Session, company_number: str) -> list:
    """Fetch persons with significant control."""
    url = f"{_BASE_URL}/company/{company_number}/persons-with-significant-control"
    data = _safe_get(session, url)
    if not data:
        return []
    return data.get("items", [])


def _is_late_accounts(filing: dict) -> bool:
    """
    Return True if annual accounts were filed more than 9 months after period end.
    """
    if filing.get("type") != "AA":
        return False
    date_str = filing.get("date")  # filed date
    period_end_str = None
    # description_values sometimes has period end
    dv = filing.get("description_values") or {}
    period_end_str = dv.get("period_end_on") or dv.get("period_of_accounts", {}).get("end_on")

    if not date_str or not period_end_str:
        return False
    try:
        filed = datetime.fromisoformat(date_str)
        period_end = datetime.fromisoformat(period_end_str)
        return (filed - period_end).days > 274  # ~9 months
    except Exception:
        return False


def _c_suite_resignation(filing: dict) -> bool:
    """Return True if TM01 appears to be a C-suite officer."""
    if filing.get("type") != "TM01":
        return False
    desc = (filing.get("description") or "").lower()
    dv = filing.get("description_values") or {}
    officer_name = (dv.get("officer_name") or "").lower()
    # Companies House doesn't always give title, but we can check description
    c_suite_keywords = ["chief executive", "ceo", "chief financial", "cfo", "managing director"]
    return any(kw in desc or kw in officer_name for kw in c_suite_keywords)


def _ticker_to_company_name(ticker: str) -> str:
    """Strip '.L' suffix for Companies House search."""
    return ticker.removesuffix(".L")


def _compute_score(events: list, new_pscs: int) -> float:
    """
    Compute CompaniesHouseRiskScore on 0-100 scale, then normalise.

    Start at 50.
    Late filing: -20 each.
    Director departure: -15 each.
    New large shareholder: +10 each.
    """
    raw = 50
    for ev in events:
        ev_type = ev.get("event_type")
        if ev_type == "late_accounts":
            raw -= 20
        elif ev_type == "director_resignation":
            raw -= 15
        elif ev_type == "director_appointment":
            pass  # neutral in raw score
    raw += new_pscs * 10
    normalised = (raw - 50) / 50.0
    return max(-1.0, min(1.0, normalised))


# ── main collector ─────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect Companies House risk signals for UK tickers (ending with .L).

    Parameters
    ----------
    tickers : list of str
    market  : str
    config  : dict

    Returns
    -------
    list of result dicts with data_type="companies_house_risk"
    """
    if config is None:
        config = {}

    ch_cfg = (
        config
        .get("altdata", {})
        .get("collectors", {})
        .get("companies_house", {})
    )

    api_key: str = (config.get("api_keys") or {}).get("companies_house", "") or ch_cfg.get("api_key", "")

    if not api_key:
        logger.warning(
            "[companies_house_collector] No API key found.\n"
            "  Setup instructions:\n"
            "    1. Register at https://developer.company-information.service.gov.uk/\n"
            "    2. Create a new 'live' application and copy the API key.\n"
            "    3. Add to config: altdata.collectors.companies_house.api_key = '<key>'\n"
            "  (The key is free — no cost involved.)"
        )
        return []

    # Filter to UK tickers only
    uk_tickers = [t for t in tickers if t.endswith(".L")]
    if not uk_tickers:
        logger.info("companies_house_collector: no UK (.L) tickers in universe")
        return []

    session = _make_session(api_key)
    results: list = []
    now_iso = datetime.now().isoformat()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()

    for ticker in uk_tickers:
        company_name = _ticker_to_company_name(ticker)

        # ── lookup company number ─────────────────────────────────────────────
        try:
            company_number = _search_company(session, company_name)
        except Exception as exc:
            logger.warning("companies_house: search failed for %s: %s", ticker, exc)
            continue

        if not company_number:
            logger.info("companies_house: no company found for %s", ticker)
            continue

        # ── filing history ────────────────────────────────────────────────────
        events: list = []

        try:
            filings = _filing_history(session, company_number)
        except Exception as exc:
            logger.warning("companies_house: filing history failed for %s: %s", ticker, exc)
            filings = []

        for filing in filings:
            filing_date = filing.get("date", "")
            if filing_date < cutoff:
                continue  # outside our window

            ftype = filing.get("type", "")

            if ftype == "TM01":
                c_suite = _c_suite_resignation(filing)
                events.append({
                    "event_type": "director_resignation",
                    "c_suite": c_suite,
                    "date": filing_date,
                    "description": filing.get("description", ""),
                    "score_delta": -0.6 if c_suite else -0.3,
                })

            elif ftype == "AP01":
                events.append({
                    "event_type": "director_appointment",
                    "date": filing_date,
                    "description": filing.get("description", ""),
                    "score_delta": 0.2,
                })

            elif ftype == "AA":
                if _is_late_accounts(filing):
                    events.append({
                        "event_type": "late_accounts",
                        "date": filing_date,
                        "score_delta": _LATE_ACCOUNTS_SCORE,
                    })

        # ── PSC changes ───────────────────────────────────────────────────────
        new_pscs = 0
        try:
            pscs = _psc_list(session, company_number)
            for psc in pscs:
                notified = psc.get("notified_on", "")
                if notified >= cutoff:
                    new_pscs += 1
                    events.append({
                        "event_type": "new_psc",
                        "date": notified,
                        "name": psc.get("name", ""),
                        "score_delta": _NEW_LARGE_SHAREHOLDER_SCORE,
                    })
        except Exception as exc:
            logger.warning("companies_house: PSC fetch failed for %s: %s", ticker, exc)

        # ── score ─────────────────────────────────────────────────────────────
        normalised_score = _compute_score(events, new_pscs)

        quality = 0.5
        if events:
            quality += 0.2
        if len(events) > 3:
            quality += 0.1
        quality = min(quality, 1.0)

        results.append({
            "source": "companies_house",
            "ticker": ticker,
            "market": market,
            "data_type": "companies_house_risk",
            "value": round(normalised_score, 6),
            "raw_data": {
                "company_number": company_number,
                "company_name": company_name,
                "events": events,
                "new_psc_count": new_pscs,
            },
            "timestamp": now_iso,
            "quality_score": round(quality, 4),
        })

    logger.info("companies_house_collector: returned %d signals", len(results))
    return results
