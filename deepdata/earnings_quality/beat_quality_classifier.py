"""
BeatQualityClassifier — the main earnings quality entry point.
Combines revenue analysis, guidance scoring, and transcript sentiment.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from .revenue_analyser import RevenueAnalyser
from .guidance_scorer import GuidanceScorer

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

# Default guidance weight if not in config
DEFAULT_GUIDANCE_WEIGHT = 0.3

# Transcript modifier defaults
DEFAULT_TRANSCRIPT_MODIFIER = 1.0
TRANSCRIPT_BULLISH_THRESHOLD = 0.5
TRANSCRIPT_BEARISH_THRESHOLD = -0.5


class BeatQualityClassifier:
    """
    Main entry point for earnings quality classification.
    Combines revenue analysis + guidance + transcript to produce final PEAD multiplier.
    """

    def __init__(self, config: dict):
        self.config = config
        eq_config = config.get("deepdata", {}).get("earnings_quality", {})
        self.guidance_weight = eq_config.get("guidance_weight", DEFAULT_GUIDANCE_WEIGHT)
        self.transcript_weight = eq_config.get("transcript_weight", 0.2)
        self.revenue_analyser = RevenueAnalyser(config)
        self.guidance_scorer = GuidanceScorer(config)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def classify(
        self,
        ticker: str,
        earnings_data: dict,
        transcript_score: float = None,
    ) -> dict:
        """
        Full classification pipeline.

        1. Revenue analysis (what drove the beat)
        2. Guidance extraction (forward looking)
        3. Combine with transcript score if available

        Returns combined classification dict.
        """
        # Step 1: Revenue analysis
        revenue_analysis = {}
        try:
            revenue_analysis = self.revenue_analyser.analyse(ticker, earnings_data)
        except Exception as exc:
            logger.warning("Revenue analysis failed for %s: %s", ticker, exc)
            revenue_analysis = {
                "beat_quality": "ONE_OFF",
                "quality_score": 0.0,
                "pead_multiplier": 0.0,
                "revenue_beat_pct": 0.0,
                "eps_beat_pct": 0.0,
            }

        beat_quality = revenue_analysis.get("beat_quality", "ONE_OFF")
        quality_score = revenue_analysis.get("quality_score", 0.0)

        # Step 2: Guidance scoring
        guidance_signal = earnings_data.get("guidance_signal", "NO_GUIDANCE")
        guidance_score = 0.0
        guidance_signal_label = "NEUTRAL"

        try:
            management_credibility = self.guidance_scorer.get_management_credibility(ticker)
            guidance_result = self.guidance_scorer.score(
                ticker, guidance_signal, management_credibility
            )
            guidance_score = guidance_result.get("credibility_adjusted", 0.0)
            guidance_signal_label = guidance_result.get("signal", "NEUTRAL")
        except Exception as exc:
            logger.warning("Guidance scoring failed for %s: %s", ticker, exc)

        # Step 3: Transcript modifier
        transcript_modifier = self._calc_transcript_modifier(transcript_score)

        # Combine: final PEAD multiplier
        final_pead_multiplier = self.calc_final_multiplier(
            quality_score, guidance_score, transcript_modifier
        )

        # Overall signal strength: normalised confidence
        signal_strength = self._calc_signal_strength(
            quality_score, guidance_score, transcript_score
        )

        summary = self._make_summary(
            ticker, beat_quality, quality_score, guidance_signal_label,
            final_pead_multiplier
        )

        return {
            "ticker": ticker,
            "beat_quality": beat_quality,
            "quality_score": round(quality_score, 4),
            "guidance_signal": guidance_signal,
            "guidance_score": round(guidance_score, 4),
            "guidance_signal_label": guidance_signal_label,
            "transcript_modifier": round(transcript_modifier, 4),
            "final_pead_multiplier": round(final_pead_multiplier, 4),
            "signal_strength": round(signal_strength, 4),
            "classification_summary": summary,
            "revenue_beat_pct": revenue_analysis.get("revenue_beat_pct", 0.0),
            "eps_beat_pct": revenue_analysis.get("eps_beat_pct", 0.0),
            "margin_expansion": revenue_analysis.get("margin_expansion", 0.0),
            "non_recurring_contribution": revenue_analysis.get("non_recurring_contribution", 0.0),
            "tax_contribution": revenue_analysis.get("tax_contribution", 0.0),
            "tax_anomaly_detected": revenue_analysis.get("tax_anomaly_detected", False),
            "classified_at": datetime.now(timezone.utc).isoformat(),
        }

    def _calc_transcript_modifier(self, transcript_score: float = None) -> float:
        """
        Convert transcript sentiment score to a multiplier.
        None or 0.0 -> 1.0 (neutral, no modification)
        Positive -> up to 1.3 (bullish amplification)
        Negative -> down to 0.5 (bearish dampening)
        """
        if transcript_score is None:
            return DEFAULT_TRANSCRIPT_MODIFIER

        if transcript_score >= TRANSCRIPT_BULLISH_THRESHOLD:
            # Scale 0.5 -> 1.3 as score goes from threshold to 1.0
            scale = (transcript_score - TRANSCRIPT_BULLISH_THRESHOLD) / (
                1.0 - TRANSCRIPT_BULLISH_THRESHOLD
            )
            return round(1.0 + 0.3 * scale, 4)
        elif transcript_score <= TRANSCRIPT_BEARISH_THRESHOLD:
            # Scale 1.0 -> 0.5 as score goes from threshold to -1.0
            scale = (TRANSCRIPT_BEARISH_THRESHOLD - transcript_score) / (
                1.0 - abs(TRANSCRIPT_BEARISH_THRESHOLD)
            )
            return round(max(0.5, 1.0 - 0.5 * scale), 4)
        else:
            # Linear interpolation around neutral
            return round(1.0 + transcript_score * 0.3, 4)

    def calc_final_multiplier(
        self,
        quality_score: float,
        guidance_score: float,
        transcript_modifier: float,
    ) -> float:
        """
        final = quality_score * (1 + guidance_score * config_weight) * transcript_modifier
        Clamped to [0, 1.5].
        """
        raw = quality_score * (1.0 + guidance_score * self.guidance_weight) * transcript_modifier
        return round(max(0.0, min(1.5, raw)), 4)

    def _calc_signal_strength(
        self,
        quality_score: float,
        guidance_score: float,
        transcript_score: float = None,
    ) -> float:
        """
        Combined confidence 0-1.
        Weighted average of quality, guidance, transcript confidence.
        """
        components = [quality_score * 0.5]  # Quality is primary

        # Guidance contribution
        guidance_component = abs(guidance_score) * 0.3
        components.append(guidance_component)

        # Transcript contribution
        if transcript_score is not None:
            transcript_component = abs(transcript_score) * 0.2
            components.append(transcript_component)
        else:
            # Redistribute weight to quality and guidance
            components = [quality_score * 0.6, abs(guidance_score) * 0.4]

        signal_strength = sum(components)
        return round(max(0.0, min(1.0, signal_strength)), 4)

    def _make_summary(
        self,
        ticker: str,
        beat_quality: str,
        quality_score: float,
        guidance_signal_label: str,
        final_pead_multiplier: float,
    ) -> str:
        """Generate human-readable classification summary."""
        quality_descriptions = {
            "REVENUE_DRIVEN": "beat driven by strong top-line revenue growth",
            "MARGIN_DRIVEN": "beat driven by margin expansion",
            "COST_CUT": "beat driven by cost reductions despite weak revenue",
            "TAX_DRIVEN": "beat driven by unusually low effective tax rate",
            "ONE_OFF": "beat driven by non-recurring items",
        }

        quality_desc = quality_descriptions.get(beat_quality, "unknown beat driver")
        guidance_desc = guidance_signal_label.lower().replace("_", " ")
        multiplier_desc = (
            "PEAD suppressed" if final_pead_multiplier == 0.0
            else f"PEAD multiplier {final_pead_multiplier:.2f}x"
        )

        return (
            f"{ticker}: {beat_quality} ({quality_desc}). "
            f"Guidance: {guidance_desc}. "
            f"{multiplier_desc}."
        )

    def generate_collector_results(self, ticker: str, classification: dict) -> list:
        """Convert classification output to CollectorResult format for storage."""
        ts = datetime.now(timezone.utc).isoformat()
        results = []

        # Main earnings quality signal
        results.append({
            "source": "earnings_quality",
            "ticker": ticker,
            "market": "US",
            "data_type": "beat_quality",
            "value": classification.get("final_pead_multiplier", 0.0),
            "raw_data": {k: v for k, v in classification.items() if k != "ticker"},
            "timestamp": ts,
            "quality_score": classification.get("signal_strength", 0.0),
        })

        # Guidance signal
        guidance_score = classification.get("guidance_score", 0.0)
        if guidance_score != 0.0:
            results.append({
                "source": "earnings_quality",
                "ticker": ticker,
                "market": "US",
                "data_type": "guidance",
                "value": guidance_score,
                "raw_data": {
                    "guidance_signal": classification.get("guidance_signal"),
                    "guidance_signal_label": classification.get("guidance_signal_label"),
                    "guidance_score": guidance_score,
                },
                "timestamp": ts,
                "quality_score": min(1.0, abs(guidance_score)),
            })

        return results

    def should_suppress_pead(self, classification: dict) -> bool:
        """
        Return True if beat is ONE_OFF (quality_score == 0).
        PEAD should not fire for non-recurring earnings beats.
        """
        return (
            classification.get("beat_quality") == "ONE_OFF"
            or classification.get("quality_score", 0.0) == 0.0
            or classification.get("final_pead_multiplier", 0.0) == 0.0
        )
