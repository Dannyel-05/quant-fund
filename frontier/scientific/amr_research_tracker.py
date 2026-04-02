"""
AMR Research Tracker — amr_urgency Signal.

Measures the intensity of antimicrobial resistance (AMR) research
published on PubMed in the rolling 30-day window as a proxy for
biotech-sector urgency around drug-resistant infection.

Economic hypothesis
-------------------
Antimicrobial resistance is an escalating global health crisis: the
WHO classifies it as one of the top ten threats to global health.
A surge in AMR publications signals that the scientific community is
responding to a worsening real-world situation — new outbreaks, rising
mortality, or a near-miss pandemic-level event.

This research intensity is a leading indicator for:
  1. Biotech and pharma sector attention (capital flows into AMR-focused
     companies before clinical trial announcements).
  2. Government procurement and contract award velocity (BARDA, DTRA,
     UKRI funding cycles follow publication spikes by 6–18 months).
  3. Broader macro risk: sustained high AMR urgency correlates with
     supply-chain stress in agriculture (antibiotics used in livestock
     banned under pressure) and healthcare cost inflation.

Update frequency: daily.  Signal is most useful as a 30-day rolling
level rather than day-on-day noise.

Data source
-----------
NCBI PubMed E-utilities (free, no API key required):
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_SEARCH_TERM = "antimicrobial resistance[Title/Abstract]"
_BASELINE_PAPERS_PER_MONTH = 500
_REQUEST_TIMEOUT = 15


class AMRResearchTracker:
    """
    Queries PubMed for the count of AMR papers published in the last
    30 days and converts the count into a normalised urgency score.

    The baseline of 500 papers/month is derived from typical PubMed
    publication rates for this query circa 2022–2024.  Values above the
    baseline signal accelerating research urgency.
    """

    def _fetch_pubmed_count(self) -> tuple[int, dict]:
        """
        Call the PubMed ESearch endpoint and return (count, raw_json).

        Returns (0, {}) on any error.
        """
        params = {
            "db": "pubmed",
            "term": _SEARCH_TERM,
            "retmax": 1,          # We only need the Count, not the IDs
            "datetype": "pdat",   # Publication date
            "reldate": 30,        # Last 30 days
            "retmode": "json",
        }
        try:
            resp = requests.get(_ESEARCH_URL, params=params, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            count_str = data.get("esearchresult", {}).get("count", "0")
            return int(count_str), data
        except requests.exceptions.Timeout:
            logger.warning("AMRResearchTracker: PubMed request timed out.")
        except requests.exceptions.RequestException as exc:
            logger.warning("AMRResearchTracker: network error: %s", exc)
        except (ValueError, KeyError) as exc:
            logger.warning("AMRResearchTracker: response parsing error: %s", exc)
        except Exception as exc:
            logger.warning("AMRResearchTracker: unexpected error: %s", exc)
        return 0, {}

    def collect(self) -> dict:
        """
        Fetch PubMed AMR publication counts and return the signal dict.

        Returns
        -------
        dict with keys:
            signal_name   : "amr_urgency"
            value         : float in [0, 1] — normalised urgency score
            raw_data      : dict — raw count and query parameters
            quality_score : 1.0 if API returned data, 0.0 on error
            timestamp     : ISO-8601 UTC string
            source        : PubMed ESearch URL
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        count, raw_json = self._fetch_pubmed_count()
        quality_score = 1.0 if raw_json else 0.0

        if count == 0 and not raw_json:
            logger.warning("AMRResearchTracker: no data retrieved from PubMed.")

        # Normalise: papers / baseline, clamp to [0, 1]
        amr_urgency = min(1.0, count / _BASELINE_PAPERS_PER_MONTH)

        raw_data = {
            "pubmed_count_30d": count,
            "baseline": _BASELINE_PAPERS_PER_MONTH,
            "search_term": _SEARCH_TERM,
            "reldate_days": 30,
            "esearch_response": raw_json.get("esearchresult", {}),
        }

        return {
            "signal_name": "amr_urgency",
            "value": float(amr_urgency),
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": _ESEARCH_URL,
        }
