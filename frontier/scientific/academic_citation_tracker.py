"""
Academic Citation Tracker — citation_velocity Signal.

Measures the velocity of academic citations in finance-adjacent research
topics by querying the Semantic Scholar public API (free, no key required).

Economic hypothesis
-------------------
Academic research predicts future market anomalies: factors are discovered
in papers before they are fully exploited by capital.  A surge in citation
velocity in quantitative finance topics signals that practitioner adoption
is accelerating, which in turn compresses alpha in affected strategies.
Conversely, high citation velocity in emerging sub-fields (e.g. ML in
finance) identifies areas where institutional deployment is still early,
leaving opportunity windows of 12–36 months before crowding occurs.

The signal serves as a slow-moving regime indicator: update weekly or
monthly.  High velocity periods coincide with proliferation of systematic
strategies; low velocity periods (post-crash consolidation) mark windows
of reduced competition.

Data source
-----------
Semantic Scholar Graph API (free, no key):
https://api.semanticscholar.org/graph/v1/paper/search
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
_DEFAULT_TOPICS = [
    "machine learning finance",
    "quantitative trading",
    "market microstructure",
    "factor investing",
]
_PAPERS_PER_TOPIC = 10
_REQUEST_TIMEOUT = 15


class AcademicCitationTracker:
    """
    Queries the Semantic Scholar API to measure citation activity in
    quantitative-finance research topics and returns a normalised
    citation-velocity signal.

    The collector fetches the top-N most-cited papers for each topic,
    sums their citation counts, divides by total paper count to get a
    per-paper velocity proxy, then normalises to [0, 1] by dividing by
    a scaling constant of 1 000 and clamping.
    """

    def _fetch_topic_citations(self, topic: str) -> list[dict]:
        """
        Fetch top papers for a single topic sorted by citation count.

        Returns a list of paper dicts containing at least {'citationCount': int}.
        Returns an empty list on any network or parsing error.
        """
        params = {
            "query": topic,
            "fields": "title,citationCount,year",
            "sort": "citationCount:desc",
            "limit": _PAPERS_PER_TOPIC,
        }
        try:
            resp = requests.get(_SEARCH_URL, params=params, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except requests.exceptions.Timeout:
            logger.warning("Timeout fetching citations for topic: %s", topic)
        except requests.exceptions.RequestException as exc:
            logger.warning("Request error for topic '%s': %s", topic, exc)
        except Exception as exc:
            logger.warning("Unexpected error for topic '%s': %s", topic, exc)
        return []

    def collect(self, topics: Optional[list] = None) -> dict:
        """
        Collect citation data across all topics and return the signal dict.

        Parameters
        ----------
        topics : list of str, optional
            Research topics to query.  Defaults to _DEFAULT_TOPICS.

        Returns
        -------
        dict with keys:
            signal_name   : "academic_citation_velocity"
            value         : float in [0, 1] — normalised citation velocity
            raw_data      : dict — per-topic citation counts and paper counts
            quality_score : 1.0 if at least one topic returned data, else 0.0
            timestamp     : ISO-8601 UTC string
            source        : Semantic Scholar API URL
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        if topics is None:
            topics = _DEFAULT_TOPICS

        topic_results: dict[str, dict] = {}
        total_citations = 0
        total_papers = 0

        for topic in topics:
            papers = self._fetch_topic_citations(topic)
            if not papers:
                topic_results[topic] = {"papers_found": 0, "total_citations": 0}
                continue

            topic_citations = sum(
                int(p.get("citationCount") or 0) for p in papers
            )
            topic_results[topic] = {
                "papers_found": len(papers),
                "total_citations": topic_citations,
            }
            total_citations += topic_citations
            total_papers += len(papers)

        quality_score = 1.0 if total_papers > 0 else 0.0

        if total_papers > 0:
            citation_velocity = total_citations / total_papers
        else:
            logger.warning("AcademicCitationTracker: no papers retrieved across all topics.")
            citation_velocity = 0.0

        # Normalise: divide by 1 000, clamp to [0, 1]
        normalised_velocity = min(1.0, max(0.0, citation_velocity / 1000.0))

        raw_data = {
            "topics_queried": len(topics),
            "total_papers": total_papers,
            "total_citations": total_citations,
            "citation_velocity_raw": citation_velocity,
            "per_topic": topic_results,
        }

        return {
            "signal_name": "academic_citation_velocity",
            "value": float(normalised_velocity),
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": _SEARCH_URL,
        }
