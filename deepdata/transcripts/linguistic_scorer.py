"""
linguistic_scorer.py — Combines all transcript features into investment signals.
"""

import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class LinguisticScorer:
    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # Main scoring
    # ------------------------------------------------------------------

    def score(
        self,
        tone_analysis: dict,
        deflection_analysis: dict,
        guidance_analysis: dict,
    ) -> dict:
        """
        TranscriptBullishScore = weighted combination of all linguistic signals.
        Returns: {score, classification, components, signal_strength}
        """
        # Extract components (with safe defaults)
        hedge_ratio = float(tone_analysis.get("hedge_ratio", 0.5))
        forward_ratio = float(tone_analysis.get("forward_ratio", 0.0))
        passive_ratio = float(tone_analysis.get("passive_ratio", 0.5))
        we_ratio = float(tone_analysis.get("we_ratio", 0.5))
        tone_shift = float(tone_analysis.get("tone_shift", 0.0))
        deflection_score = float(deflection_analysis.get("deflection_score", 0.5))
        guidance_score_raw = float(guidance_analysis.get("guidance_score", 0.0))

        # Normalise guidance_score from [-0.7, 1.0] to [0, 1]
        guidance_sentiment = (guidance_score_raw + 0.7) / 1.7

        # Tone shift: large absolute shift is a bad sign (inconsistency)
        # Clamp to [0, 1] range for use in formula
        tone_shift_score_abs = min(abs(tone_shift), 1.0)

        # Core formula (weights sum to 1.0)
        raw_score = (
            (1.0 - hedge_ratio) * 0.20
            + forward_ratio * 0.15
            + (1.0 - passive_ratio) * 0.10
            + we_ratio * 0.10
            + (1.0 - deflection_score) * 0.20
            + (1.0 - tone_shift_score_abs) * 0.15
            + guidance_sentiment * 0.10
        )

        # Normalise to [0, 1]
        score = max(0.0, min(1.0, raw_score))

        classification = self.classify(score)

        # Signal strength: distance from midpoint 0.5
        signal_strength = abs(score - 0.5) * 2.0  # 0 = neutral, 1 = max conviction

        components = {
            "hedge_component": round((1.0 - hedge_ratio) * 0.20, 4),
            "forward_component": round(forward_ratio * 0.15, 4),
            "passive_component": round((1.0 - passive_ratio) * 0.10, 4),
            "we_component": round(we_ratio * 0.10, 4),
            "deflection_component": round((1.0 - deflection_score) * 0.20, 4),
            "tone_shift_component": round((1.0 - tone_shift_score_abs) * 0.15, 4),
            "guidance_component": round(guidance_sentiment * 0.10, 4),
        }

        return {
            "score": round(score, 4),
            "classification": classification,
            "components": components,
            "signal_strength": round(signal_strength, 4),
        }

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def classify(self, score: float) -> str:
        """score > 0.65 = BULLISH, < 0.35 = BEARISH, else NEUTRAL"""
        if score > 0.65:
            return "BULLISH"
        elif score < 0.35:
            return "BEARISH"
        return "NEUTRAL"

    # ------------------------------------------------------------------
    # PEAD modifier
    # ------------------------------------------------------------------

    def pead_modifier(self, transcript_score: float, pead_direction: int) -> float:
        """
        PEAD modifier based on transcript quality.
        pead_direction: +1 = positive surprise, -1 = negative surprise.
        Returns multiplier.
        """
        # Strong positive PEAD + bullish transcript
        if pead_direction == 1 and transcript_score > 0.65:
            return 1.3
        # Strong positive PEAD + bearish transcript (contradiction — caution)
        if pead_direction == 1 and transcript_score < 0.35:
            return 0.5
        # Negative PEAD + bearish transcript (confirm the negative)
        if pead_direction == -1 and transcript_score < 0.35:
            return 1.3
        # Negative PEAD + bullish transcript (possible reversal, muted)
        if pead_direction == -1 and transcript_score > 0.65:
            return 0.7
        # Weak PEAD + very bullish: flag for review, slight positive nudge
        if pead_direction == 0 and transcript_score > 0.75:
            logger.info("Weak PEAD + very bullish transcript flagged for review")
            return 1.1
        # Default: neutral multiplier
        return 1.0

    # ------------------------------------------------------------------
    # Trend tracking
    # ------------------------------------------------------------------

    def track_trend(self, ticker: str, current_score: float, history: list) -> dict:
        """
        Detect if management confidence trend is rising or falling over quarters.
        history: list of previous scores (oldest first).
        """
        if not history:
            return {
                "trend": "INSUFFICIENT_DATA",
                "direction": 0,
                "slope": 0.0,
                "quarters_tracked": 0,
            }

        all_scores = history + [current_score]
        n = len(all_scores)

        if n < 2:
            return {
                "trend": "INSUFFICIENT_DATA",
                "direction": 0,
                "slope": 0.0,
                "quarters_tracked": n,
            }

        # Simple linear regression slope
        if HAS_NUMPY:
            import numpy as np
            x = np.arange(n, dtype=float)
            y = np.array(all_scores, dtype=float)
            slope = float(np.polyfit(x, y, 1)[0])
        else:
            # Manual slope calculation
            x_mean = (n - 1) / 2.0
            y_mean = sum(all_scores) / n
            numerator = sum((i - x_mean) * (s - y_mean) for i, s in enumerate(all_scores))
            denominator = sum((i - x_mean) ** 2 for i in range(n)) or 1e-9
            slope = numerator / denominator

        if slope > 0.02:
            trend = "RISING"
            direction = 1
        elif slope < -0.02:
            trend = "FALLING"
            direction = -1
        else:
            trend = "STABLE"
            direction = 0

        return {
            "trend": trend,
            "direction": direction,
            "slope": round(slope, 5),
            "quarters_tracked": n,
        }

    # ------------------------------------------------------------------
    # Sector-relative score
    # ------------------------------------------------------------------

    def sector_relative_score(self, ticker_score: float, sector_scores: list) -> float:
        """Compare transcript score to sector average. Return z-score."""
        if not sector_scores:
            return 0.0

        n = len(sector_scores)
        mean = sum(sector_scores) / n

        if n < 2:
            return 0.0

        variance = sum((s - mean) ** 2 for s in sector_scores) / (n - 1)

        if HAS_NUMPY:
            import numpy as np
            std = float(np.sqrt(variance))
        else:
            std = variance ** 0.5

        if std < 1e-9:
            return 0.0

        return round((ticker_score - mean) / std, 4)
