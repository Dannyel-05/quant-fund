"""
innovation_scorer.py — Combines patent signals from USPTO and UK IPO into investment signals.
"""

import logging
import math
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

CACHE_DIR = Path("data/cache/deepdata")

# Weights for the innovation signal components
_W_VELOCITY = 0.35
_W_CITATION = 0.25
_W_COMPETITOR = 0.20
_W_LEAD_TIME = 0.10
_W_PIVOT = 0.10


class InnovationScorer:
    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Main scoring
    # ------------------------------------------------------------------

    def score(
        self, ticker: str, patent_data: list, competitor_tickers: list = None
    ) -> dict:
        """
        Returns comprehensive innovation score dict.
        patent_data: list of patent dicts from USPTOCollector or UKIPOCollector.
        """
        if not patent_data:
            return self._empty_result(ticker)

        # Aggregate raw metrics from patent_data list
        # patent_data may contain CollectorResult dicts or raw patent dicts
        filing_velocity = self._extract_metric(patent_data, "filing_velocity", default=1.0)
        citation_growth = self._extract_metric(patent_data, "citation_growth", default=0.0)
        tech_pivot_info = self._extract_raw(patent_data, "tech_pivot") or {}
        lead_time_days = self._extract_metric(patent_data, "lead_time_days", default=0)

        # Score each component on [0, 1]
        velocity_score = self._score_velocity(filing_velocity)
        citation_score = self._score_citation_growth(citation_growth)
        pivot_signal = self._score_tech_pivot(tech_pivot_info)

        # Competitor threat score
        competitor_threat_score = 0.0
        if competitor_tickers:
            try:
                from deepdata.patents.uspto_collector import USPTOCollector
                collector = USPTOCollector(self.config)
                overlaps = []
                for comp_ticker in (competitor_tickers or [])[:5]:
                    try:
                        overlap = collector.calc_competitor_overlap(ticker, comp_ticker)
                        overlaps.append(overlap)
                    except Exception as exc:
                        logger.warning("Competitor overlap failed %s vs %s: %s",
                                       ticker, comp_ticker, exc)
                competitor_threat_score = sum(overlaps) / len(overlaps) if overlaps else 0.0
            except Exception as exc:
                logger.warning("Competitor threat calculation failed: %s", exc)

        # Lead time score: normalise 0-180 days to 0-1
        lead_time_score = min(int(lead_time_days) / 180.0, 1.0) if lead_time_days and int(lead_time_days) > 0 else 0.5

        # Combined innovation score
        innovation_score = (
            velocity_score * _W_VELOCITY
            + citation_score * _W_CITATION
            + (1.0 - competitor_threat_score) * _W_COMPETITOR  # lower threat = better
            + lead_time_score * _W_LEAD_TIME
            + (velocity_score if pivot_signal == "ACCELERATING" else 0.0) * _W_PIVOT
        )
        innovation_score = max(0.0, min(1.0, innovation_score))

        signal = self.calc_innovation_signal(
            velocity=filing_velocity,
            citation_growth=citation_growth,
            pivot=tech_pivot_info,
            lead_time=int(lead_time_days) if lead_time_days else 0,
        )

        result = {
            "innovation_score": round(innovation_score, 4),
            "filing_velocity_score": round(velocity_score, 4),
            "citation_growth_score": round(citation_score, 4),
            "competitor_threat_score": round(competitor_threat_score, 4),
            "tech_pivot_signal": pivot_signal,
            "lead_time_days": int(lead_time_days) if lead_time_days else 0,
            "signal": round(signal, 4),
        }
        return result

    # ------------------------------------------------------------------
    # Signal calculation
    # ------------------------------------------------------------------

    def calc_innovation_signal(
        self,
        velocity: float,
        citation_growth: float,
        pivot: dict,
        lead_time: int,
    ) -> float:
        """Combine all patent metrics into final -1 to 1 signal."""

        # Velocity: >1.5 is strongly positive, <0.5 is negative
        if velocity > 1.5:
            vel_signal = min((velocity - 1.0) / 2.0, 1.0)
        elif velocity < 0.5:
            vel_signal = max((velocity - 1.0), -1.0)
        else:
            vel_signal = (velocity - 1.0)  # small positive/negative

        # Citation growth: directly maps to signal
        cit_signal = max(-1.0, min(1.0, citation_growth))

        # Pivot signal: new tech categories = positive, abandoned = slightly negative
        new_cats = len(pivot.get("new_categories", []))
        abandoned_cats = len(pivot.get("abandoned_categories", []))
        pivot_signal_val = min(new_cats * 0.1, 0.3) - min(abandoned_cats * 0.05, 0.15)

        # Lead time: positive lead time with reasonable window is a good sign
        lead_signal = 0.1 if 0 < lead_time <= 120 else 0.0

        # Weighted combination
        raw = (
            vel_signal * 0.45
            + cit_signal * 0.30
            + pivot_signal_val * 0.15
            + lead_signal * 0.10
        )
        return round(max(-1.0, min(1.0, raw)), 4)

    # ------------------------------------------------------------------
    # CollectorResult generation
    # ------------------------------------------------------------------

    def generate_collector_results(self, ticker: str, score_dict: dict) -> list:
        """Convert score dict to list of CollectorResult dicts for storage."""
        timestamp = datetime.utcnow().isoformat()
        results = []

        metric_map = {
            "innovation_score": "innovation_score",
            "filing_velocity_score": "patent_velocity_score",
            "citation_growth_score": "citation_growth_score",
            "competitor_threat_score": "competitor_threat_score",
            "signal": "innovation_signal",
        }

        for key, data_type in metric_map.items():
            value = score_dict.get(key, 0.0)
            results.append({
                "source": "INNOVATION_SCORER",
                "ticker": ticker,
                "market": "us" if not ticker.endswith(".L") else "uk",
                "data_type": data_type,
                "value": float(value),
                "raw_data": {
                    "full_score": score_dict,
                    "tech_pivot_signal": score_dict.get("tech_pivot_signal", "STABLE"),
                    "lead_time_days": score_dict.get("lead_time_days", 0),
                },
                "timestamp": timestamp,
                "quality_score": 0.75,
            })

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _empty_result(self, ticker: str) -> dict:
        return {
            "innovation_score": 0.0,
            "filing_velocity_score": 0.0,
            "citation_growth_score": 0.0,
            "competitor_threat_score": 0.0,
            "tech_pivot_signal": "STABLE",
            "lead_time_days": 0,
            "signal": 0.0,
        }

    def _score_velocity(self, velocity: float) -> float:
        """Map filing velocity ratio to [0, 1]."""
        if velocity <= 0:
            return 0.0
        # Log scale: velocity of 1 → 0.5; >2 → approaching 1; <0.5 → approaching 0
        try:
            score = 0.5 + 0.5 * math.tanh(velocity - 1.0)
        except Exception:
            score = 0.5
        return round(max(0.0, min(1.0, score)), 4)

    def _score_citation_growth(self, growth: float) -> float:
        """Map citation YoY growth to [0, 1]."""
        # Growth of 0 → 0.5; +1.0 → ~0.88; -1.0 → ~0.12
        try:
            score = 0.5 + 0.5 * math.tanh(growth)
        except Exception:
            score = 0.5
        return round(max(0.0, min(1.0, score)), 4)

    def _score_tech_pivot(self, pivot: dict) -> str:
        """Convert pivot dict to descriptive signal string."""
        if not pivot:
            return "STABLE"

        new_cats = len(pivot.get("new_categories", []))
        abandoned = len(pivot.get("abandoned_categories", []))
        is_pivoting = pivot.get("pivoting", False)

        if is_pivoting and new_cats > 2:
            return "PIVOTING"
        if new_cats > 0 and abandoned == 0:
            return "ACCELERATING"
        if abandoned > new_cats:
            return "DECELERATING"
        return "STABLE"

    def _extract_metric(self, patent_data: list, key: str, default=0.0):
        """Extract a named metric from patent_data list (CollectorResult or raw dict)."""
        for item in patent_data:
            # CollectorResult format
            raw = item.get("raw_data", {})
            if key in raw:
                return raw[key]
            # Direct key
            if key in item:
                return item[key]
        return default

    def _extract_raw(self, patent_data: list, key: str):
        """Extract nested raw value."""
        for item in patent_data:
            raw = item.get("raw_data", {})
            if key in raw:
                return raw[key]
        return None
