"""
AcademicFundamentalBridge — links academic research trends to company
fundamental outlooks.  Uses Semantic Scholar (free, no API key required) to
find relevant papers and computes a tailwind score based on citation velocity
and relevance, combined with a company's R&D efficiency ratio.
"""
import logging
import math
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False
    logger.warning("requests not available — AcademicFundamentalBridge paper search disabled")

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False
    logger.warning("yfinance not available — AcademicFundamentalBridge using fallback data")

try:
    import pandas as pd
    _PD_AVAILABLE = True
except ImportError:
    _PD_AVAILABLE = False


# ---------------------------------------------------------------------------
# Sector → academic field mapping
# ---------------------------------------------------------------------------

SECTOR_TO_FIELD: Dict[str, List[str]] = {
    "Technology": ["computer science", "machine learning", "semiconductors", "software"],
    "Biotechnology": ["biology", "pharmacology", "genomics", "drug discovery"],
    "Energy": ["energy storage", "renewable energy", "petroleum engineering"],
    "Financial Services": ["financial economics", "quantitative finance", "risk management"],
    "Consumer Discretionary": ["consumer behavior", "retail analytics"],
    "Industrials": ["manufacturing", "supply chain", "robotics", "automation"],
    "Healthcare": ["medicine", "medical devices", "clinical trials"],
    "Materials": ["materials science", "chemistry", "nanotechnology"],
}

_STOPWORDS = frozenset({
    "the", "and", "for", "are", "with", "that", "this", "its", "has", "have",
    "been", "from", "but", "our", "not", "also", "was", "were", "will", "can",
    "all", "any", "their", "they", "which", "who", "how", "into", "over",
    "more", "than", "such", "each", "other", "both", "about", "through",
    "including", "provide", "products", "services", "company", "companies",
    "business", "based", "operates", "operates", "provides", "inc", "corp",
    "ltd", "plc", "group",
})

_SEMANTIC_SCHOLAR_URL = "https://api.semanticscholar.org/graph/v1/paper/search"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _tokenise(text: str) -> List[str]:
    """Lowercase alphabetic tokens, filter stopwords and short tokens."""
    tokens = re.findall(r"[a-z]{3,}", text.lower())
    return [t for t in tokens if t not in _STOPWORDS]


def _jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union else 0.0


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class AcademicFundamentalBridge:
    """
    Bridges academic research momentum (via Semantic Scholar) with company
    fundamentals to produce a tailwind score and signal for quantitative use.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        cfg = (config or {}).get("closeloop", {}).get("context", {}).get("academic_bridge", {})
        self.relevance_threshold: float = cfg.get("relevance_threshold", 0.3)
        self.store = store
        self._session: Optional[object] = None

    def _get_session(self):
        """Lazily create a requests session."""
        if not _REQUESTS_AVAILABLE:
            return None
        if self._session is None:
            try:
                self._session = requests.Session()
                self._session.headers.update({"User-Agent": "quant-fund/1.0"})
            except Exception as exc:
                logger.warning("Could not create requests session: %s", exc)
                return None
        return self._session

    # ------------------------------------------------------------------
    # Keyword extraction
    # ------------------------------------------------------------------

    def get_company_keywords(self, ticker: str) -> List[str]:
        """
        Extract keywords from yfinance ticker info:
          - longBusinessSummary: tokenise, remove stopwords, top 10 words
          - sector + industry → SECTOR_TO_FIELD mapping

        Returns list of keyword strings.  Returns [] on failure.
        """
        try:
            if not _YF_AVAILABLE:
                return []
            info = yf.Ticker(ticker).info or {}

            word_freq: Dict[str, int] = {}
            summary = info.get("longBusinessSummary", "") or ""
            if summary:
                for token in _tokenise(summary):
                    word_freq[token] = word_freq.get(token, 0) + 1

            # Top 10 words from summary
            top_words = [w for w, _ in sorted(word_freq.items(), key=lambda x: -x[1])[:10]]

            sector = info.get("sector", "") or ""
            industry = info.get("industry", "") or ""
            field_keywords = SECTOR_TO_FIELD.get(sector, [])

            keywords = list(dict.fromkeys(top_words + field_keywords + [sector, industry]))
            keywords = [k for k in keywords if k]
            return keywords[:20]
        except Exception as exc:
            logger.error("get_company_keywords failed for %s: %s", ticker, exc)
            return []

    # ------------------------------------------------------------------
    # Paper search
    # ------------------------------------------------------------------

    def search_relevant_papers(
        self, keywords: List[str], max_results: int = 20
    ) -> List[Dict]:
        """
        Search Semantic Scholar (free, no key) for relevant recent papers.
        Query: top-3 keywords joined with space (SS uses boolean by default).
        Filter: published in last 24 months, citationCount > 0.

        Returns list of paper dicts.  Graceful empty list on any failure.
        """
        session = self._get_session()
        if session is None:
            return []
        if not keywords:
            return []

        try:
            query = " ".join(keywords[:3])
            cutoff_year = datetime.now(timezone.utc).year - 2

            params = {
                "query": query,
                "limit": min(max_results * 2, 100),
                "fields": "paperId,title,year,citationCount,fieldsOfStudy,externalIds",
            }
            resp = session.get(_SEMANTIC_SCHOLAR_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            papers = []
            for item in data.get("data", []):
                year = item.get("year") or 0
                if year < cutoff_year:
                    continue
                citations = item.get("citationCount") or 0
                if citations < 1:
                    continue
                papers.append({
                    "paperId": item.get("paperId", ""),
                    "title": item.get("title", ""),
                    "year": year,
                    "citationCount": citations,
                    "fieldsOfStudy": item.get("fieldsOfStudy") or [],
                    "externalIds": item.get("externalIds") or {},
                })
                if len(papers) >= max_results:
                    break

            logger.debug(
                "search_relevant_papers: query=%r → %d papers", query, len(papers)
            )
            return papers
        except Exception as exc:
            logger.error("search_relevant_papers failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Relevance scoring
    # ------------------------------------------------------------------

    def calc_relevance_score(
        self,
        paper: Dict,
        company_keywords: List[str],
        sector: str,
        months_since_pub: float,
    ) -> float:
        """
        RelevanceScore = keyword_overlap * field_match * recency_weight

          keyword_overlap = Jaccard(paper_title_words, company_keywords)
          field_match     = 1.0 if paper field in SECTOR_TO_FIELD[sector] else 0.3
          recency_weight  = exp(-0.1 * months_since_pub)

        Returns float [0, 1].
        """
        try:
            paper_words = set(_tokenise(paper.get("title", "")))
            company_set = set(w.lower() for w in company_keywords if w)
            keyword_overlap = _jaccard(paper_words, company_set)

            sector_fields = SECTOR_TO_FIELD.get(sector, [])
            paper_fields = [f.lower() for f in (paper.get("fieldsOfStudy") or [])]
            field_match = 1.0 if any(f in paper_fields for f in sector_fields) else 0.3

            recency_weight = math.exp(-0.1 * max(0.0, months_since_pub))

            score = keyword_overlap * field_match * recency_weight
            return round(min(1.0, max(0.0, score)), 4)
        except Exception as exc:
            logger.error("calc_relevance_score failed: %s", exc)
            return 0.0

    # ------------------------------------------------------------------
    # Tailwind score
    # ------------------------------------------------------------------

    def calc_tailwind_score(self, ticker: str) -> float:
        """
        tailwind = Σ citation_velocity(p) * relevance(p)  for relevant papers
          citation_velocity = citationCount / max(1, months_since_pub)

        Normalised 0-1 by dividing by the maximum observed value.
        Returns float [0, 1].  Returns 0.0 on failure.
        """
        try:
            keywords = self.get_company_keywords(ticker)
            if not keywords:
                return 0.0

            sector = ""
            if _YF_AVAILABLE:
                try:
                    sector = yf.Ticker(ticker).info.get("sector", "") or ""
                except Exception:
                    pass

            papers = self.search_relevant_papers(keywords)
            if not papers:
                return 0.0

            now_year = datetime.now(timezone.utc).year
            scores = []
            for p in papers:
                months_since = max(1.0, (now_year - (p.get("year") or now_year)) * 12)
                rel = self.calc_relevance_score(p, keywords, sector, months_since)
                if rel < self.relevance_threshold:
                    continue
                cit_vel = p.get("citationCount", 0) / months_since
                scores.append(cit_vel * rel)

            if not scores:
                return 0.0

            raw_sum = sum(scores)
            # Normalise: treat 100 as a strong absolute upper bound
            normalised = min(1.0, raw_sum / 100.0)
            return round(normalised, 4)
        except Exception as exc:
            logger.error("calc_tailwind_score failed for %s: %s", ticker, exc)
            return 0.0

    # ------------------------------------------------------------------
    # R&D efficiency
    # ------------------------------------------------------------------

    def rd_efficiency_ratio(self, ticker: str) -> Optional[float]:
        """
        RDE = revenue_growth_rate / rd_as_pct_revenue

        Fetch from yfinance quarterly_financials.
        Returns float or None if insufficient data.
        """
        if not _YF_AVAILABLE:
            return None
        try:
            t = yf.Ticker(ticker)
            fin = t.quarterly_financials
            if fin is None or fin.empty:
                return None

            # Normalise column access — columns are dates
            if not _PD_AVAILABLE:
                return None

            fin = fin.sort_index(axis=1)  # oldest → newest
            if fin.shape[1] < 2:
                return None

            def _row(name: str):
                """Case-insensitive row lookup."""
                for idx in fin.index:
                    if name.lower() in str(idx).lower():
                        return fin.loc[idx]
                return None

            rev_row = _row("Total Revenue") or _row("Revenue")
            rd_row = _row("Research And Development") or _row("Research Development")

            if rev_row is None:
                return None

            rev_vals = rev_row.dropna().tolist()
            if len(rev_vals) < 2:
                return None

            latest_rev = _safe_float(rev_vals[-1])
            prev_rev = _safe_float(rev_vals[-2])
            if prev_rev == 0:
                return None

            rev_growth = (latest_rev - prev_rev) / abs(prev_rev)

            if rd_row is None:
                return None

            rd_vals = rd_row.dropna().tolist()
            if not rd_vals:
                return None
            latest_rd = abs(_safe_float(rd_vals[-1]))
            if latest_rev == 0 or latest_rd == 0:
                return None

            rd_pct = latest_rd / latest_rev
            rde = rev_growth / rd_pct
            return round(rde, 4)
        except Exception as exc:
            logger.error("rd_efficiency_ratio failed for %s: %s", ticker, exc)
            return None

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def generate_signal(self, ticker: str) -> Optional[Dict]:
        """
        Generate a signal based on academic tailwind and R&D efficiency:
          tailwind > 0.7 AND rd_efficiency > 0 → strong LONG consideration
          tailwind < 0.3                        → technology obsolescence risk flag

        Returns signal dict or None.
        """
        try:
            tailwind = self.calc_tailwind_score(ticker)
            rde = self.rd_efficiency_ratio(ticker)

            # Median RDE approximation: 1.0 as neutral baseline
            median_rde = 1.0
            rde_ok = (rde is not None and rde > median_rde)

            if tailwind > 0.7 and rde_ok:
                direction = "LONG"
                confidence = round(tailwind * 0.8, 4)
                notes = "Strong academic tailwind + positive R&D efficiency"
            elif tailwind > 0.7:
                direction = "LONG"
                confidence = round(tailwind * 0.5, 4)
                notes = "Strong academic tailwind; R&D efficiency unconfirmed"
            elif tailwind < 0.3:
                direction = "RISK_FLAG"
                confidence = round((0.3 - tailwind) * 0.6, 4)
                notes = "Technology obsolescence risk — low academic tailwind"
            else:
                return None

            signal = {
                "ticker": ticker,
                "signal_direction": direction,
                "tailwind_score": tailwind,
                "rd_efficiency": rde,
                "confidence": confidence,
                "notes": notes,
                "signal_type": "ACADEMIC_FUNDAMENTAL_BRIDGE",
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            logger.info(
                "generate_signal(%s): %s (tailwind=%.3f, rde=%s)",
                ticker, direction, tailwind, rde,
            )
            return signal
        except Exception as exc:
            logger.error("generate_signal failed for %s: %s", ticker, exc)
            return None

    # ------------------------------------------------------------------
    # Outcome recording
    # ------------------------------------------------------------------

    def record_match(
        self,
        ticker: str,
        paper_id: str,
        paper_title: str,
        relevance_score: float,
        citation_velocity: float,
    ) -> None:
        """
        Store academic–company match in store.academic_company_matches.
        Silently no-ops when store is unavailable.
        """
        if self.store is None:
            return
        try:
            conn = self.store._conn()
            conn.execute("""
                INSERT INTO academic_company_matches
                    (ticker, paper_id, paper_title, relevance_score,
                     citation_velocity, matched_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                paper_id,
                paper_title,
                relevance_score,
                citation_velocity,
                datetime.now(timezone.utc).date().isoformat(),
            ))
            conn.commit()
            logger.debug("record_match: stored paper %s for %s", paper_id, ticker)
        except Exception as exc:
            logger.error("record_match failed for %s: %s", ticker, exc)
