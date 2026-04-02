"""
Obituary Tracker — Knowledge Loss Score via SEC 8-K Item 5.02 Filings.

Economic Hypothesis
-------------------
Senior executive departures create an information asymmetry analogous to
Post-Earnings-Announcement Drift (PEAD): the market systematically
under-reacts to the loss of tacit knowledge embedded in key individuals.
This is especially pronounced in small- and mid-cap companies where single
executives bear disproportionate strategic and operational knowledge.

The SEC mandates that public companies disclose director and officer changes
in Form 8-K, Item 5.02, within four business days.  These filings are
publicly available via the EDGAR full-text search API at no cost and provide
a real-time, structured signal of human-capital events.

For each departure detected, the KnowledgeLossScore (KLS) is computed and
converted to an ExpectedDrift magnitude.  The aggregate signal is the mean
absolute expected drift across all recent departures.

Signal: mean |ExpectedDrift| across all Item 5.02 resignations in the last
30 days.  Higher values indicate elevated market-wide human-capital risk.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from frontier.equations.derived_formulas import (
    calc_expected_drift,
    calc_knowledge_loss_score,
)

logger = logging.getLogger(__name__)

_SEC_SEARCH_URL = (
    "https://efts.sec.gov/LATEST/search-index"
    "?q=%22Item+5.02%22+%22resigned%22"
    "&dateRange=custom&startdt={startdt}&enddt={enddt}"
    "&forms=8-K"
)
_SEC_HEADERS = {
    "User-Agent": "quant-fund-research-bot research@example.com",
    "Accept-Encoding": "gzip, deflate",
}

# Conservative defaults used when per-filing data is unavailable
_DEFAULT_TENURE_YEARS = 3.0
_DEFAULT_SUCCESSION_SIGNAL = 0.2   # low succession certainty
_DEFAULT_ANALYST_COVERAGE = 5
_DEFAULT_MARKET_CAP_M = 500.0      # mid-cap fallback


def _classify_role(text: str) -> str:
    """Infer executive role from filing text keywords."""
    text_lower = text.lower()
    if "chief executive" in text_lower or " ceo" in text_lower:
        return "ceo"
    if "chief financial" in text_lower or " cfo" in text_lower:
        return "cfo"
    if "chief operating" in text_lower or " coo" in text_lower:
        return "coo"
    if "chief technology" in text_lower or " cto" in text_lower:
        return "cto"
    if "chief scientist" in text_lower or "chief science" in text_lower:
        return "chief_scientist"
    if "founder" in text_lower:
        return "founder"
    if "director" in text_lower:
        return "director"
    return "other_csuite"


def _fetch_sec_filings(days_back: int = 30) -> list:
    """
    Query EDGAR full-text search for recent 8-K Item 5.02 resignation filings.

    Returns a list of filing hit dicts from the EDGAR API.
    """
    today = datetime.utcnow().date()
    start = today - timedelta(days=days_back)
    url = _SEC_SEARCH_URL.format(
        startdt=start.isoformat(),
        enddt=today.isoformat(),
    )
    try:
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        return hits
    except Exception as exc:
        logger.warning("SEC EDGAR fetch failed: %s", exc)
        return []


class ObituaryTracker:
    """
    Track executive departures via SEC 8-K Item 5.02 filings and compute
    an aggregate human-capital risk signal using the KnowledgeLossScore
    and ExpectedDrift formulas.
    """

    def collect(self, tickers: Optional[list] = None) -> dict:
        """
        Fetch recent SEC 8-K resignation filings, compute KLS and
        ExpectedDrift for each departure, and return an aggregate signal.

        Parameters
        ----------
        tickers : optional list of tickers to filter filings.
                  If None, all recent filings are used for the aggregate.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        hits = _fetch_sec_filings(days_back=30)
        n_filings = len(hits)
        logger.info("ObituaryTracker: fetched %d EDGAR 8-K hits", n_filings)

        departures = []
        for hit in hits:
            try:
                source_data = hit.get("_source", {})
                entity_name = source_data.get("entity_name", "Unknown")
                filing_date = source_data.get("file_date", "")
                text_snippet = (
                    source_data.get("period_of_report", "")
                    + " "
                    + source_data.get("display_date_filed", "")
                    + " "
                    + entity_name
                )

                # Attempt to extract role from available text fields
                description = source_data.get("description", "")
                role = _classify_role(description + " " + text_snippet)

                kls = calc_knowledge_loss_score(
                    role=role,
                    tenure_years=_DEFAULT_TENURE_YEARS,
                    succession_signal=_DEFAULT_SUCCESSION_SIGNAL,
                    analyst_coverage=_DEFAULT_ANALYST_COVERAGE,
                )
                drift = calc_expected_drift(
                    kls=kls,
                    market_cap_millions=_DEFAULT_MARKET_CAP_M,
                )

                departures.append(
                    {
                        "entity": entity_name,
                        "role": role,
                        "filing_date": filing_date,
                        "kls": kls,
                        "expected_drift": drift,
                    }
                )
            except Exception as exc:
                logger.debug("Skipping filing hit due to parse error: %s", exc)
                continue

        if departures:
            mean_abs_drift = sum(abs(d["expected_drift"]) for d in departures) / len(
                departures
            )
        else:
            mean_abs_drift = 0.0

        # Quality: 0 filings → 0.1 (endpoint worked but no data),
        # scales toward 1.0 with more filings (cap at 50 for full confidence)
        if n_filings == 0 and not hits:
            quality_score = 0.0  # fetch failed entirely
        else:
            quality_score = min(1.0, 0.1 + (n_filings / 50.0) * 0.9)

        raw_data = {
            "n_filings_fetched": n_filings,
            "n_departures_parsed": len(departures),
            "departures": departures[:20],  # cap raw payload size
            "mean_abs_expected_drift": mean_abs_drift,
        }

        return {
            "signal_name": "obituary_impact_score",
            "value": mean_abs_drift,
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": "sec_edgar_8k_item502",
        }
