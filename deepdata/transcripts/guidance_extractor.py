"""
guidance_extractor.py — Extracts and scores forward guidance from earnings transcripts.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")
CREDIBILITY_CACHE_FILE = CACHE_DIR / "guidance_credibility.json"

# ---------------------------------------------------------------------------
# Signal word lists
# ---------------------------------------------------------------------------

RAISED_SIGNALS = [
    "raised guidance", "increased outlook", "higher than expected",
    "raise our full year", "increasing our forecast", "above previous",
    "raise full-year", "increase guidance", "raising our outlook",
    "raising guidance", "increased our guidance",
]
MAINTAINED_SIGNALS = [
    "reaffirm", "maintain", "consistent with", "on track", "unchanged",
    "reiterating", "continue to expect", "maintain our guidance",
    "no change to", "in line with previous",
]
LOWERED_SIGNALS = [
    "lower our guidance", "reduce our outlook", "below previous",
    "challenging environment", "headwinds", "revising down", "below our prior",
    "lowering guidance", "reducing outlook", "revise lower",
    "below prior", "downward revision",
]
WITHDRAWN_SIGNALS = [
    "not providing guidance", "withdrawing guidance",
    "too uncertain", "visibility is limited", "not in a position to guide",
    "suspend guidance", "withdrawing our guidance", "no longer providing",
    "given the uncertainty",
]

GUIDANCE_SIGNAL_SCORES = {
    "RAISED": 1.0,
    "MAINTAINED": 0.0,
    "LOWERED": -0.7,
    "WITHDRAWN": -0.5,
    "NONE": 0.0,
}


class GuidanceExtractor:
    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._credibility_cache: dict = self._load_credibility_cache()

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def extract(self, transcript: dict) -> dict:
        """
        Returns guidance analysis dict with signal, score, numeric extractions,
        credibility and adjusted signal strength.
        """
        sections = transcript.get("sections", {})
        prepared_text = sections.get("prepared_remarks", "")
        full_text = prepared_text + "\n" + sections.get("qa_section", "")
        ticker = transcript.get("ticker", "")

        try:
            guidance_signal = self.classify_guidance(prepared_text)
        except Exception as exc:
            logger.warning("classify_guidance error: %s", exc)
            guidance_signal = "NONE"

        guidance_score = GUIDANCE_SIGNAL_SCORES.get(guidance_signal, 0.0)

        try:
            extracted_guidance = self.extract_numeric_guidance(full_text)
        except Exception as exc:
            logger.warning("extract_numeric_guidance error: %s", exc)
            extracted_guidance = []

        credibility = self.get_credibility(ticker) if ticker else 0.5
        adjusted_signal_strength = guidance_score * credibility

        return {
            "guidance_signal": guidance_signal,
            "guidance_score": guidance_score,
            "extracted_guidance": extracted_guidance,
            "management_credibility": credibility,
            "adjusted_signal_strength": round(adjusted_signal_strength, 4),
        }

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def classify_guidance(self, prepared_text: str) -> str:
        """Scan prepared remarks for guidance signal keywords. Return classification."""
        text_lower = prepared_text.lower()

        # Check most specific/strong signals first
        for signal in WITHDRAWN_SIGNALS:
            if signal in text_lower:
                return "WITHDRAWN"

        for signal in RAISED_SIGNALS:
            if signal in text_lower:
                return "RAISED"

        for signal in LOWERED_SIGNALS:
            if signal in text_lower:
                return "LOWERED"

        for signal in MAINTAINED_SIGNALS:
            if signal in text_lower:
                return "MAINTAINED"

        return "NONE"

    # ------------------------------------------------------------------
    # Numeric guidance extraction
    # ------------------------------------------------------------------

    def extract_numeric_guidance(self, text: str) -> list:
        """
        Extract specific guidance ranges using regex.
        Returns list of {metric, low, high, unit} dicts.
        """
        results = []

        # Revenue patterns: "$X to $Y billion/million" or "between X and Y"
        revenue_patterns = [
            re.compile(
                r"revenue\s+(?:of\s+)?\$?([\d,.]+)\s*(?:to|and|-)\s*\$?([\d,.]+)\s*(billion|million|B|M)\b",
                re.I,
            ),
            re.compile(
                r"revenue\s+between\s+\$?([\d,.]+)\s+and\s+\$?([\d,.]+)\s*(billion|million|B|M)\b",
                re.I,
            ),
            re.compile(
                r"revenue\s+(?:of\s+)?approximately\s+\$?([\d,.]+)\s*(billion|million|B|M)\b",
                re.I,
            ),
        ]

        for pat in revenue_patterns:
            for m in pat.finditer(text):
                groups = m.groups()
                if len(groups) >= 2:
                    low_str = groups[0].replace(",", "")
                    high_str = groups[1].replace(",", "") if groups[1] else groups[0].replace(",", "")
                    unit = groups[-1] if groups[-1] else ""
                    try:
                        results.append({
                            "metric": "Revenue",
                            "low": float(low_str),
                            "high": float(high_str),
                            "unit": unit,
                        })
                    except ValueError:
                        pass

        # EPS patterns: "$X.XX to $X.XX"
        eps_patterns = [
            re.compile(
                r"earnings\s+per\s+share\s+(?:of\s+)?\$?([\d.]+)\s*(?:to|and|-)\s*\$?([\d.]+)",
                re.I,
            ),
            re.compile(
                r"\bEPS\b\s+(?:of\s+)?\$?([\d.]+)\s*(?:to|and|-)\s*\$?([\d.]+)",
                re.I,
            ),
        ]

        for pat in eps_patterns:
            for m in pat.finditer(text):
                groups = m.groups()
                if len(groups) >= 2:
                    try:
                        results.append({
                            "metric": "EPS",
                            "low": float(groups[0]),
                            "high": float(groups[1]),
                            "unit": "USD",
                        })
                    except ValueError:
                        pass

        # Margin patterns: "approximately X%" or "X% to Y%"
        margin_patterns = [
            re.compile(
                r"(?:gross\s+)?margin\s+(?:of\s+)?approximately\s+([\d.]+)\s*%",
                re.I,
            ),
            re.compile(
                r"(?:gross\s+)?margin\s+(?:of\s+)?([\d.]+)\s*%\s*(?:to|-)\s*([\d.]+)\s*%",
                re.I,
            ),
        ]

        for pat in margin_patterns:
            for m in pat.finditer(text):
                groups = m.groups()
                try:
                    low = float(groups[0])
                    high = float(groups[1]) if len(groups) > 1 and groups[1] else low
                    metric_name = "Gross Margin" if "gross" in m.group(0).lower() else "Margin"
                    results.append({
                        "metric": metric_name,
                        "low": low,
                        "high": high,
                        "unit": "%",
                    })
                except (ValueError, IndexError):
                    pass

        return results

    # ------------------------------------------------------------------
    # Credibility tracking
    # ------------------------------------------------------------------

    def update_credibility(self, ticker: str, guidance: dict, actual_result: dict) -> None:
        """
        Store guidance vs actual and update beat/miss rates.
        guidance: {guidance_signal, extracted_guidance, ...}
        actual_result: {revenue, eps, ...}
        """
        if ticker not in self._credibility_cache:
            self._credibility_cache[ticker] = {
                "beat_own_guidance": 0,
                "miss_own_guidance": 0,
                "history": [],
            }

        record = {
            "guidance_signal": guidance.get("guidance_signal"),
            "extracted_guidance": guidance.get("extracted_guidance", []),
            "actual": actual_result,
        }

        # Simple heuristic: if RAISED and actual > previous, it's a beat
        signal = guidance.get("guidance_signal", "NONE")
        actual_eps = actual_result.get("eps")
        guidance_eps_list = [
            g for g in guidance.get("extracted_guidance", []) if g.get("metric") == "EPS"
        ]

        if guidance_eps_list and actual_eps is not None:
            try:
                guidance_high = guidance_eps_list[0].get("high", 0)
                if float(actual_eps) >= float(guidance_high):
                    self._credibility_cache[ticker]["beat_own_guidance"] += 1
                else:
                    self._credibility_cache[ticker]["miss_own_guidance"] += 1
            except (ValueError, TypeError):
                pass

        self._credibility_cache[ticker]["history"].append(record)
        self._save_credibility_cache()

    def get_credibility(self, ticker: str) -> float:
        """Return management credibility score 0-1. Default 0.5 if insufficient history."""
        data = self._credibility_cache.get(ticker, {})
        beats = data.get("beat_own_guidance", 0)
        misses = data.get("miss_own_guidance", 0)
        total = beats + misses

        if total < 3:
            return 0.5  # insufficient history

        return round(beats / total, 4)

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _load_credibility_cache(self) -> dict:
        if CREDIBILITY_CACHE_FILE.exists():
            try:
                return json.loads(CREDIBILITY_CACHE_FILE.read_text())
            except Exception:
                pass
        return {}

    def _save_credibility_cache(self) -> None:
        try:
            CREDIBILITY_CACHE_FILE.write_text(json.dumps(self._credibility_cache, indent=2))
        except Exception as exc:
            logger.warning("Could not save credibility cache: %s", exc)
