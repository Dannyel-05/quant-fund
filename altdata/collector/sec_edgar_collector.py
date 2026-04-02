"""
SEC EDGAR alternative data collector.

Collects Form 4 (insider transactions) and Form 8-K (material events)
from the free SEC EDGAR full-text search API.  No API key required;
a descriptive User-Agent header is mandatory per SEC policy.
"""

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "quant-fund research@example.com",
    "Accept-Encoding": "gzip, deflate",
}

_EFTS_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_CIK_LOOKUP = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?company=&CIK={ticker}&type=&dateb=&owner=include"
    "&count=10&search_text=&action=getcompany&output=atom"
)

_CIK_CACHE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "cache", "edgar_cik.json"
)

_ET_TIMEZONE = ZoneInfo("America/New_York")
_REQUEST_DELAY = 2  # seconds between EDGAR calls

# ── CIK cache ─────────────────────────────────────────────────────────────────

_cik_cache: dict = {}
_cik_cache_loaded = False


def _load_cik_cache() -> None:
    global _cik_cache, _cik_cache_loaded
    if _cik_cache_loaded:
        return
    _cik_cache_loaded = True
    try:
        os.makedirs(os.path.dirname(_CIK_CACHE_FILE), exist_ok=True)
        if os.path.exists(_CIK_CACHE_FILE):
            with open(_CIK_CACHE_FILE, "r") as fh:
                _cik_cache = json.load(fh)
    except Exception as exc:
        logger.warning("edgar: could not load CIK cache: %s", exc)
        _cik_cache = {}


def _save_cik_cache() -> None:
    try:
        os.makedirs(os.path.dirname(_CIK_CACHE_FILE), exist_ok=True)
        with open(_CIK_CACHE_FILE, "w") as fh:
            json.dump(_cik_cache, fh)
    except Exception as exc:
        logger.warning("edgar: could not save CIK cache: %s", exc)


def _get_cik(session: requests.Session, ticker: str) -> str | None:
    """Return zero-padded 10-digit CIK for ticker, or None on failure."""
    _load_cik_cache()
    if ticker in _cik_cache:
        return _cik_cache[ticker]

    url = _CIK_LOOKUP.format(ticker=ticker)
    try:
        time.sleep(_REQUEST_DELAY)
        resp = session.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning("edgar CIK lookup HTTP %s for %s", resp.status_code, ticker)
            return None

        # The ATOM feed embeds the CIK as <company-info><cik>
        root = ET.fromstring(resp.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        # Try to find accession number in the entry id
        for entry in root.findall("atom:entry", ns):
            for id_el in entry.findall("atom:id", ns):
                id_text = id_el.text or ""
                # id looks like: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001318605&...
                m = re.search(r'CIK=(\d+)', id_text, re.IGNORECASE)
                if m:
                    cik = m.group(1).zfill(10)
                    _cik_cache[ticker] = cik
                    _save_cik_cache()
                    return cik
    except Exception as exc:
        logger.warning("edgar: CIK lookup failed for %s: %s", ticker, exc)

    return None


# ── filing search ─────────────────────────────────────────────────────────────

def _search_filings(session: requests.Session, ticker: str, form_type: str, days_back: int) -> list:
    """Return list of filing metadata dicts from EDGAR full-text search."""
    today = datetime.now(timezone.utc).date()
    start_dt = (today - timedelta(days=days_back)).isoformat()
    end_dt = today.isoformat()

    params = {
        "q": f'"{ticker}"',
        "forms": form_type,
        "dateRange": "custom",
        "startdt": start_dt,
        "enddt": end_dt,
    }

    try:
        time.sleep(_REQUEST_DELAY)
        resp = session.get(_EFTS_SEARCH, params=params, headers=_HEADERS, timeout=20)
        if resp.status_code != 200:
            logger.warning("edgar: search HTTP %s for %s/%s", resp.status_code, ticker, form_type)
            return []
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        return hits
    except Exception as exc:
        logger.warning("edgar: search failed for %s/%s: %s", ticker, form_type, exc)
        return []


# ── Form 4 parsing ─────────────────────────────────────────────────────────────

_ROLE_PATTERNS = {
    "CEO": re.compile(r'\bCEO\b|\bChief Executive\b', re.I),
    "CFO": re.compile(r'\bCFO\b|\bChief Financial\b', re.I),
    "Director": re.compile(r'\bDirector\b', re.I),
}


def _parse_form4_xml(xml_text: str) -> list:
    """
    Parse a Form 4 XML document.
    Returns list of transaction dicts.
    """
    transactions: list = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        logger.warning("edgar: Form 4 XML parse error: %s", exc)
        return []

    # Reporting owner
    owner_el = root.find(".//reportingOwner")
    owner_name = ""
    owner_title = ""
    is_10b51 = False

    if owner_el is not None:
        name_el = owner_el.find(".//rptOwnerName")
        if name_el is not None:
            owner_name = (name_el.text or "").strip()
        title_el = owner_el.find(".//officerTitle")
        if title_el is not None:
            owner_title = (title_el.text or "").strip()

    # Determine role
    role = "Other"
    for role_name, pattern in _ROLE_PATTERNS.items():
        if pattern.search(owner_title):
            role = role_name
            break

    # Non-derivative transactions
    for txn in root.findall(".//nonDerivativeTransaction"):
        txn_code_el = txn.find(".//transactionCode")
        shares_el = txn.find(".//transactionShares/value")
        price_el = txn.find(".//transactionPricePerShare/value")
        date_el = txn.find(".//transactionDate/value")

        txn_code = (txn_code_el.text or "").strip() if txn_code_el is not None else ""
        shares = 0.0
        price = 0.0
        date_str = ""

        try:
            shares = float(shares_el.text) if shares_el is not None else 0.0
        except (ValueError, TypeError):
            pass
        try:
            price = float(price_el.text) if price_el is not None else 0.0
        except (ValueError, TypeError):
            pass
        if date_el is not None:
            date_str = (date_el.text or "").strip()

        # Check 10b5-1 flag
        footnote_el = txn.find(".//transactionAmounts/transactionPricePerShare/footnoteId")
        # Also look in top-level footnotes
        for fn in root.findall(".//footnote"):
            fn_text = (fn.text or "").lower()
            if "10b5-1" in fn_text or "rule 10b5" in fn_text:
                is_10b51 = True

        transactions.append({
            "owner_name": owner_name,
            "owner_title": owner_title,
            "role": role,
            "transaction_code": txn_code,  # P=purchase, S=sale, A=award, etc.
            "shares": shares,
            "price_per_share": price,
            "value_usd": shares * price,
            "date": date_str,
            "is_10b51": is_10b51,
        })

    return transactions


def _score_insider_transactions(transactions: list, filing_date: str) -> float:
    """
    Apply scoring rules to a batch of Form 4 transactions.
    Returns InsiderSentimentScore in [-1, 1].
    """
    if not transactions:
        return 0.0

    purchases = [t for t in transactions if t["transaction_code"] == "P"]
    sales = [t for t in transactions if t["transaction_code"] == "S"]
    scheduled_sales = [t for t in sales if t["is_10b51"]]
    unscheduled_sales = [t for t in sales if not t["is_10b51"]]

    score = 0.0

    # Multiple insiders buying (same week) → +1.0
    if len(purchases) > 1:
        score = 1.0

    # Single large purchase (>$100k)
    elif purchases:
        large = [p for p in purchases if p["value_usd"] > 100_000]
        if large:
            score = 0.7
        else:
            score = 0.3

    # CEO sells entire position
    ceo_sales = [t for t in unscheduled_sales if t["role"] == "CEO" and not t["is_10b51"]]
    if ceo_sales:
        # Heuristic: very large sale by CEO
        if any(t["shares"] > 50_000 for t in ceo_sales):
            score = -1.0
        else:
            score = -0.6

    # Clustered unscheduled selling (>2 insiders, not 10b5-1)
    if len(unscheduled_sales) > 2 and score == 0.0:
        score = -0.8

    # Routine/scheduled selling
    if unscheduled_sales and len(scheduled_sales) >= len(unscheduled_sales):
        score = 0.0

    return max(-1.0, min(1.0, score))


# ── Form 8-K parsing ──────────────────────────────────────────────────────────

_8K_ITEMS = {
    "1.01": "material_agreement",
    "2.02": "earnings_results",
    "5.02": "director_changes",
    "7.01": "regulation_fd",
}

_AFTER_HOURS_START = 16  # 4pm ET
_AFTER_HOURS_END = 9     # 9am ET


def _is_after_hours(filed_at: str) -> bool:
    """Return True if filing time is between 4pm and 9am ET (after hours)."""
    if not filed_at:
        return False
    try:
        dt = datetime.fromisoformat(filed_at.replace("Z", "+00:00"))
        dt_et = dt.astimezone(_ET_TIMEZONE)
        hour = dt_et.hour
        return hour >= _AFTER_HOURS_START or hour < _AFTER_HOURS_END
    except Exception:
        return False


def _score_8k(hit: dict) -> tuple[float, dict]:
    """Return (score, extra_raw) for an 8-K hit."""
    source = hit.get("_source", {})
    file_date = source.get("file_date", "")
    period_of_report = source.get("period_of_report", "")
    form_type = source.get("form_type", "")
    entity_name = source.get("entity_name", "")

    # Detect item types from display_names / description
    description = (source.get("display_names") or source.get("file_date") or "")
    items_detected: list = []
    for item_code, item_label in _8K_ITEMS.items():
        if item_code in str(description):
            items_detected.append(item_label)

    after_hours = _is_after_hours(file_date)

    # Base score heuristic
    score = 0.0
    if "earnings_results" in items_detected:
        score = 0.3  # needs further analysis; flag as moderate positive
    if "director_changes" in items_detected:
        score = -0.3
    if after_hours:
        score = score * 1.5  # amplify significance

    score = max(-1.0, min(1.0, score))

    extra = {
        "items_detected": items_detected,
        "after_hours_flag": after_hours,
        "entity_name": entity_name,
        "file_date": file_date,
        "period_of_report": period_of_report,
    }
    return score, extra


# ── quality helpers ───────────────────────────────────────────────────────────

def _quality_from_transactions(transactions: list) -> float:
    if not transactions:
        return 0.3
    qs = 0.5
    if len(transactions) > 1:
        qs += 0.2
    if any(t["value_usd"] > 100_000 for t in transactions):
        qs += 0.2
    ceos = [t for t in transactions if t["role"] == "CEO"]
    if ceos:
        qs += 0.1
    return min(qs, 1.0)


# ── Form 4 XML fetch ──────────────────────────────────────────────────────────

def _fetch_form4_xml(session: requests.Session, hit: dict) -> str | None:
    """Try to fetch the actual Form 4 XML from a search hit."""
    source = hit.get("_source", {})
    # The EDGAR search index includes a file_date, accession_no, cik
    accession_no = source.get("accession_no", "")
    cik = source.get("entity_id", "") or source.get("file_num", "")

    if not accession_no:
        return None

    # Normalise accession number format
    acc_clean = accession_no.replace("-", "")
    # Typical URL: https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_nodash}/{acc_no}.txt
    # Try the primary document index
    index_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&type=4&dateb=&owner=include&count=5"
        f"&search_text=&accession={accession_no}"
    )

    # Simpler: construct direct filing URL
    if cik:
        cik_str = str(cik).lstrip("0")
        xml_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_str}"
            f"/{acc_clean}/{acc_clean}-index.htm"
        )
    else:
        return None

    try:
        time.sleep(_REQUEST_DELAY)
        resp = session.get(xml_url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        # Find the form4 XML link in the index HTML
        m = re.search(r'href="([^"]*form4[^"]*\.xml[^"]*)"', resp.text, re.I)
        if not m:
            # Try any .xml link
            m = re.search(r'href="([^"]*\.xml)"', resp.text, re.I)
        if m:
            xml_path = m.group(1)
            if not xml_path.startswith("http"):
                xml_path = "https://www.sec.gov" + xml_path
            time.sleep(_REQUEST_DELAY)
            xml_resp = session.get(xml_path, headers=_HEADERS, timeout=15)
            if xml_resp.status_code == 200:
                return xml_resp.text
    except Exception as exc:
        logger.warning("edgar: Form 4 XML fetch failed: %s", exc)

    return None


# ── main collector ─────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect SEC EDGAR insider sentiment (Form 4) and material event (Form 8-K)
    signals for the given tickers.

    Parameters
    ----------
    tickers : list of str
    market  : str
    config  : dict

    Returns
    -------
    list of result dicts with data_type in {"insider_sentiment", "material_event"}
    """
    if config is None:
        config = {}

    results: list = []
    session = requests.Session()
    session.headers.update(_HEADERS)

    now_iso = datetime.now().isoformat()

    for ticker in tickers:
        # ── Form 4 ────────────────────────────────────────────────────────────
        try:
            hits4 = _search_filings(session, ticker, "4", days_back=30)
            all_txns: list = []

            for hit in hits4[:10]:  # cap to avoid hammering EDGAR
                xml_text = _fetch_form4_xml(session, hit)
                if xml_text:
                    txns = _parse_form4_xml(xml_text)
                    all_txns.extend(txns)

            if all_txns:
                insider_score = _score_insider_transactions(all_txns, now_iso)
                qs = _quality_from_transactions(all_txns)

                results.append({
                    "source": "sec_edgar",
                    "ticker": ticker,
                    "market": market,
                    "data_type": "insider_sentiment",
                    "value": round(insider_score, 6),
                    "raw_data": {
                        "filing_count": len(hits4),
                        "transaction_count": len(all_txns),
                        "transactions": all_txns[:20],
                    },
                    "timestamp": now_iso,
                    "quality_score": round(qs, 4),
                })

                # Wire into InsiderAnalyser for sophisticated classification
                try:
                    from analysis.insider_analyser import InsiderAnalyser
                    ia = InsiderAnalyser()
                    classified = ia.analyse(
                        ticker=ticker,
                        transactions=all_txns,
                        price_change_30d=0.0,  # default; can be enriched later
                    )
                    # Upgrade the result with the max insider signal score
                    max_score = max(
                        (c.get("insider_signal_score") or 0 for c in classified),
                        default=0,
                    )
                    results[-1]["raw_data"]["insider_signal_score"] = max_score
                    results[-1]["raw_data"]["classified_transactions"] = [
                        {k: v for k, v in c.items() if k != "cluster_analysis"}
                        for c in classified if not c.get("is_noise")
                    ]
                    # Get cluster flags
                    cluster = ia.get_cluster_score(ticker, window_days=30)
                    results[-1]["raw_data"]["cluster"] = cluster
                except Exception as exc:
                    logger.debug("InsiderAnalyser wiring failed for %s: %s", ticker, exc)

        except Exception as exc:
            logger.warning("edgar: Form 4 collection failed for %s: %s", ticker, exc)

        # ── Form 8-K ──────────────────────────────────────────────────────────
        try:
            hits8k = _search_filings(session, ticker, "8-K", days_back=7)

            for hit in hits8k[:5]:
                score_8k, extra_raw = _score_8k(hit)
                source_info = hit.get("_source", {})

                results.append({
                    "source": "sec_edgar",
                    "ticker": ticker,
                    "market": market,
                    "data_type": "material_event",
                    "value": round(score_8k, 6),
                    "raw_data": {
                        "accession_no": source_info.get("accession_no"),
                        **extra_raw,
                    },
                    "timestamp": now_iso,
                    "quality_score": 0.6 + (0.2 if extra_raw.get("after_hours_flag") else 0.0),
                })
        except Exception as exc:
            logger.warning("edgar: Form 8-K collection failed for %s: %s", ticker, exc)

    logger.info("sec_edgar_collector: returned %d signals", len(results))
    return results


class SECEdgarCollector:
    """Class wrapper around the module-level collect() function."""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def collect(self, tickers: list, market: str = 'US') -> list:
        return collect(tickers, market, self.config)
