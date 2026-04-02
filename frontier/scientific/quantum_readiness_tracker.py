"""
Quantum Readiness Tracker — qtpi Signal.

Measures the proximity of quantum computing to cryptographically-relevant
capability using a composite of ArXiv paper velocity, quantum ETF volume
anomaly as a patent-activity proxy, and hardcoded milestone achievements.

Economic hypothesis
-------------------
Quantum computing poses an existential threat to RSA-2048 and ECC
encryption, which underpin the security of banking systems, telecom
infrastructure, and government contractor networks.  The timeline to
"cryptographically relevant" quantum capability is uncertain but is
measurable in advance via observable leading indicators:

  1. ArXiv paper velocity in quant-ph: academic progress precedes
     engineering breakthroughs by 2–5 years.  A surge in quantum
     computing papers signals accelerating capability.

  2. Patent filing velocity (proxied via QTUM ETF volume anomaly):
     corporate patent filing bursts correlate with lab breakthroughs
     that precede public disclosure by 6–18 months.  The QTUM ETF
     tracks companies at the quantum computing frontier; abnormal
     volume suggests institutional positioning around patent news.

  3. Milestone achievements: discrete capability thresholds (e.g.,
     logical qubit error correction at scale) whose completion
     dramatically compresses the remaining timeline.

Portfolio implications:
  - Threatened sectors: encryption-dependent financial infrastructure,
    legacy cybersecurity vendors.
  - Opportunity sectors: quantum hardware companies, post-quantum
    cryptography software vendors (e.g., lattice-based crypto).

The QTPI is a slow-moving signal; update monthly.  Current milestone
baseline (2024): approximately 35% of milestones towards cryptographic
relevance have been achieved.

Data sources
------------
ArXiv API (free, no key): http://export.arxiv.org/api/query
yfinance (free, no key): QTUM ETF volume data
"""

import logging
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import requests

from frontier.equations.derived_formulas import calc_qtpi

logger = logging.getLogger(__name__)

_ARXIV_URL = (
    "http://export.arxiv.org/api/query"
    "?search_query=cat:quant-ph+AND+ti:quantum+computing"
    "&start=0&max_results=20&sortBy=submittedDate&sortOrder=descending"
)
_QTUM_TICKER = "QTUM"
_ACHIEVED_MILESTONES_FRACTION = 0.35  # ~35% of milestones achieved as of 2024
_REQUEST_TIMEOUT = 15

# Milestones known achieved as of knowledge cutoff
# Passed as a list of milestone name strings to calc_qtpi
_ACHIEVED_MILESTONE_NAMES = [
    "50_qubit_system",
    "quantum_advantage_demonstrated",
]


class QuantumReadinessTracker:
    """
    Collects quantum computing progress signals from ArXiv and market data,
    then computes the Quantum Threat Proximity Index (QTPI) via the shared
    derived-formula library.
    """

    def _fetch_arxiv_papers(self) -> list[dict]:
        """
        Fetch the latest 20 quant-ph + quantum computing papers from ArXiv.

        Returns a list of dicts with keys: title, submitted_date.
        Returns an empty list on any error.
        """
        try:
            resp = requests.get(_ARXIV_URL, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            papers = []
            for entry in entries:
                published_el = entry.find("atom:published", ns)
                title_el = entry.find("atom:title", ns)
                if published_el is None:
                    continue
                papers.append({
                    "title": title_el.text.strip() if title_el is not None else "",
                    "submitted_date": published_el.text.strip(),
                })
            return papers
        except requests.exceptions.Timeout:
            logger.warning("QuantumReadinessTracker: ArXiv request timed out.")
        except requests.exceptions.RequestException as exc:
            logger.warning("QuantumReadinessTracker: ArXiv network error: %s", exc)
        except ET.ParseError as exc:
            logger.warning("QuantumReadinessTracker: ArXiv XML parse error: %s", exc)
        except Exception as exc:
            logger.warning("QuantumReadinessTracker: ArXiv unexpected error: %s", exc)
        return []

    def _count_recent_papers(self, papers: list[dict], days: int = 30) -> int:
        """Count papers submitted within the last `days` days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        count = 0
        for p in papers:
            try:
                # ArXiv dates: "2024-01-15T12:00:00Z"
                date_str = p["submitted_date"].replace("Z", "+00:00")
                submitted = datetime.fromisoformat(date_str)
                if submitted.tzinfo is None:
                    submitted = submitted.replace(tzinfo=timezone.utc)
                if submitted >= cutoff:
                    count += 1
            except Exception:
                continue
        return count

    def _fetch_qtum_volume_anomaly(self) -> float:
        """
        Compute QTUM 5-day volume / 20-day avg volume - 1.0 as a patent
        activity proxy.

        Returns 0.0 on any error or insufficient data.
        """
        try:
            import yfinance as yf

            ticker = yf.Ticker(_QTUM_TICKER)
            hist = ticker.history(period="30d")
            if hist.empty or "Volume" not in hist.columns or len(hist) < 20:
                logger.warning(
                    "QuantumReadinessTracker: insufficient QTUM volume data."
                )
                return 0.0

            volume = hist["Volume"].dropna()
            if len(volume) < 20:
                return 0.0

            vol_5d = float(volume.iloc[-5:].mean()) if len(volume) >= 5 else float(volume.mean())
            vol_20d = float(volume.iloc[-20:].mean())

            if vol_20d < 1:
                return 0.0

            return float(vol_5d / vol_20d - 1.0)

        except ImportError:
            logger.warning("QuantumReadinessTracker: yfinance not installed.")
        except Exception as exc:
            logger.warning(
                "QuantumReadinessTracker: QTUM volume fetch error: %s", exc
            )
        return 0.0

    def collect(self) -> dict:
        """
        Collect quantum readiness signals and return the QTPI signal dict.

        ArXiv velocity is normalised as papers_per_month / 50 (50 papers in 30
        days = baseline normal activity).  Patent velocity is the QTUM volume
        anomaly (5d / 20d avg - 1).  Achieved milestones are hardcoded at the
        2024 level of ~35% completion.

        Returns
        -------
        dict with keys:
            signal_name   : "qtpi"
            value         : float — Quantum Threat Proximity Index
            raw_data      : dict — arxiv counts, volume anomaly, milestones
            quality_score : 1.0 if ArXiv data obtained, 0.5 if only proxy
            timestamp     : ISO-8601 UTC string
            source        : "arxiv + yfinance:QTUM"
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        papers = self._fetch_arxiv_papers()
        recent_count = self._count_recent_papers(papers, days=30)
        quality_score = 1.0 if papers else 0.5

        arxiv_velocity = recent_count / 50.0  # normalise: 50 papers/month = 1.0

        patent_velocity = self._fetch_qtum_volume_anomaly()

        qtpi_value = calc_qtpi(
            arxiv_velocity=arxiv_velocity,
            patent_velocity=patent_velocity,
            achieved_milestones=_ACHIEVED_MILESTONE_NAMES,
        )

        raw_data = {
            "arxiv_papers_fetched": len(papers),
            "arxiv_papers_last_30d": recent_count,
            "arxiv_velocity_normalised": round(arxiv_velocity, 4),
            "qtum_volume_anomaly": round(patent_velocity, 4),
            "achieved_milestones": _ACHIEVED_MILESTONE_NAMES,
            "achieved_milestones_fraction": _ACHIEVED_MILESTONES_FRACTION,
        }

        return {
            "signal_name": "qtpi",
            "value": float(qtpi_value),
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": "arxiv + yfinance:QTUM",
        }
