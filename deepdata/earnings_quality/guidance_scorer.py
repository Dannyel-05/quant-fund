"""
GuidanceScorer — scores forward guidance quality and management credibility.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")
GUIDANCE_HISTORY_FILE = CACHE_DIR / "guidance_history.json"

# Signal scores for different guidance types
GUIDANCE_SIGNAL_SCORES = {
    "RAISED": 0.8,
    "RAISED_ABOVE_CONSENSUS": 1.0,
    "MAINTAINED": 0.1,
    "LOWERED": -0.7,
    "LOWERED_BELOW_CONSENSUS": -1.0,
    "WITHDRAWN": -0.5,
    "ISSUED_POSITIVE": 0.6,
    "ISSUED_NEGATIVE": -0.6,
    "NO_GUIDANCE": 0.0,
    "BEAT_AND_RAISE": 1.0,
    "BEAT_AND_MAINTAIN": 0.4,
    "BEAT_AND_LOWER": -0.3,
    "MISS_AND_LOWER": -1.0,
    "MISS_AND_MAINTAIN": -0.4,
}

GUIDANCE_CREDIBILITY_THRESHOLDS = {
    "conservative": {"beat_rate": 0.70},   # Consistently beats own guidance
    "accurate": {"beat_rate": 0.50},        # ~50/50 beat/miss
    "optimistic": {"beat_rate": 0.30},      # Frequently misses own guidance
}


class GuidanceScorer:
    """
    Scores forward guidance quality and adjusts by management credibility.
    Conservative guiders (who routinely beat) have lower raw credibility
    because the market already discounts their conservative guidance.
    """

    def __init__(self, config: dict):
        self.config = config
        eq_config = config.get("deepdata", {}).get("earnings_quality", {})
        self.guidance_weight = eq_config.get("guidance_weight", 0.3)
        self.min_history_count = eq_config.get("min_guidance_history", 4)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._history: dict = self._load_history()

    def _load_history(self) -> dict:
        """Load guidance history from cache."""
        if GUIDANCE_HISTORY_FILE.exists():
            try:
                with open(GUIDANCE_HISTORY_FILE, "r") as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning("Failed to load guidance history: %s", exc)
        return {}

    def _save_history(self) -> None:
        """Persist guidance history to cache."""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(GUIDANCE_HISTORY_FILE, "w") as f:
                json.dump(self._history, f, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to save guidance history: %s", exc)

    def score(self, ticker: str, guidance_signal: str, credibility: float) -> dict:
        """
        Score a guidance signal adjusted by management credibility.

        Returns:
        {
          raw_score, credibility_adjusted, signal, weight
        }
        """
        raw_score = GUIDANCE_SIGNAL_SCORES.get(guidance_signal.upper(), 0.0)
        credibility_adjusted = raw_score * credibility

        signal = self._classify_signal(credibility_adjusted)

        return {
            "ticker": ticker,
            "guidance_signal": guidance_signal,
            "raw_score": round(raw_score, 4),
            "credibility_adjusted": round(credibility_adjusted, 4),
            "signal": signal,
            "weight": self.guidance_weight,
            "credibility": round(credibility, 4),
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

    def _classify_signal(self, score: float) -> str:
        """Convert continuous score to discrete signal."""
        if score >= 0.6:
            return "STRONG_BUY"
        elif score >= 0.2:
            return "BUY"
        elif score >= -0.2:
            return "NEUTRAL"
        elif score >= -0.6:
            return "SELL"
        else:
            return "STRONG_SELL"

    def update_credibility_history(
        self, ticker: str, guidance_type: str, actual_outcome: str
    ) -> None:
        """
        Track whether guidance type led to actual beat/miss.
        guidance_type: e.g. 'RAISED', 'MAINTAINED', 'LOWERED'
        actual_outcome: 'BEAT', 'MISS', 'IN_LINE'

        Store in data/cache/deepdata/guidance_history.json
        """
        if ticker not in self._history:
            self._history[ticker] = {"records": [], "stats": {}}

        record = {
            "guidance_type": guidance_type.upper(),
            "actual_outcome": actual_outcome.upper(),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        self._history[ticker]["records"].append(record)

        # Recompute stats
        records = self._history[ticker]["records"]
        total = len(records)
        beats = sum(1 for r in records if r.get("actual_outcome") == "BEAT")
        misses = sum(1 for r in records if r.get("actual_outcome") == "MISS")
        in_line = total - beats - misses

        # Accuracy when guidance was raised (should = BEAT)
        raised_records = [r for r in records if r.get("guidance_type") in ("RAISED", "BEAT_AND_RAISE")]
        raised_beat = sum(1 for r in raised_records if r.get("actual_outcome") == "BEAT")
        raised_accuracy = raised_beat / len(raised_records) if raised_records else 0.5

        self._history[ticker]["stats"] = {
            "total": total,
            "beats": beats,
            "misses": misses,
            "in_line": in_line,
            "beat_rate": beats / total if total > 0 else 0.0,
            "raised_guidance_accuracy": raised_accuracy,
        }

        self._save_history()
        logger.debug("Updated guidance history for %s: %d records", ticker, total)

    def get_management_credibility(self, ticker: str) -> float:
        """
        Based on historical guidance accuracy:
        Conservative guiders (consistently beat own guidance) = 0.7
        Accurate guiders = 1.0
        Optimistic guiders (miss own guidance) = 0.3
        """
        if ticker not in self._history:
            return 0.5  # Default: unknown track record

        stats = self._history[ticker].get("stats", {})
        total = stats.get("total", 0)

        if total < self.min_history_count:
            return 0.5  # Insufficient data

        beat_rate = stats.get("beat_rate", 0.5)

        # Conservative: always beats = they guide conservatively
        # Market discounts this, so credibility is moderate not high
        if beat_rate >= GUIDANCE_CREDIBILITY_THRESHOLDS["conservative"]["beat_rate"]:
            return 0.7

        # Accurate: guidance tracks actual results well
        if beat_rate >= GUIDANCE_CREDIBILITY_THRESHOLDS["accurate"]["beat_rate"]:
            return 1.0

        # Optimistic: routinely misses own guidance
        if beat_rate <= GUIDANCE_CREDIBILITY_THRESHOLDS["optimistic"]["beat_rate"]:
            return 0.3

        # Interpolate between optimistic (0.3) and accurate (1.0)
        # beat_rate in range [0.30, 0.50]
        slope = (1.0 - 0.3) / (0.50 - 0.30)
        credibility = 0.3 + slope * (beat_rate - 0.30)
        return round(max(0.1, min(1.0, credibility)), 4)
