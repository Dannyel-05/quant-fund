"""
Unified Market Complexity Index (UMCI).

Combines signals from ALL modules — core, altdata, deepdata, frontier —
into one number measuring how 'strange' current market conditions are
across every observable dimension simultaneously.

Scale: 0–100.
Every reading is logged permanently regardless of what it triggers.
"""
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Strategy adjustment table
# ---------------------------------------------------------------------------

UMCI_LEVELS = [
    {
        "name": "LOW",
        "min": 0,
        "max": 30,
        "position_multiplier": 1.0,
        "preferred_strategy": "momentum",
        "message": "Normal market complexity — full signal activation",
        "frontier_active": True,
        "tier_min": 1,          # all tiers active
        "confidence_threshold": 0.60,
    },
    {
        "name": "MEDIUM",
        "min": 30,
        "max": 60,
        "position_multiplier": 0.85,
        "preferred_strategy": "balanced",
        "message": "Elevated complexity — moderate position reduction",
        "frontier_active": True,
        "tier_min": 1,
        "confidence_threshold": 0.65,
    },
    {
        "name": "HIGH",
        "min": 60,
        "max": 80,
        "position_multiplier": 0.65,
        "preferred_strategy": "mean_reversion",
        "message": "High complexity — significant position reduction, frontier signals suppressed",
        "frontier_active": False,
        "tier_min": 2,          # tier 3+ suppressed
        "confidence_threshold": 0.70,
        "alert": True,
    },
    {
        "name": "EXTREME",
        "min": 80,
        "max": 95,
        "position_multiplier": 0.30,
        "preferred_strategy": "defensive",
        "message": "Extreme complexity — near-defensive positioning",
        "frontier_active": False,
        "tier_min": 1,          # only tier-1
        "confidence_threshold": 0.80,
        "alert": True,
        "urgent": True,
    },
    {
        "name": "UNPRECEDENTED",
        "min": 95,
        "max": 101,
        "position_multiplier": 0.0,
        "preferred_strategy": "halt",
        "message": "UNPRECEDENTED — system has never seen conditions like this. Halting new positions.",
        "frontier_active": False,
        "tier_min": 999,        # nothing active
        "confidence_threshold": 1.0,
        "alert": True,
        "urgent": True,
        "halt": True,
    },
]


def get_umci_level(umci: float) -> Dict:
    """Return the UMCI level dict for a given score."""
    for level in UMCI_LEVELS:
        if level["min"] <= umci < level["max"]:
            return level
    return UMCI_LEVELS[-1]


# ---------------------------------------------------------------------------
# Dimension score calculators
# ---------------------------------------------------------------------------

def _safe_norm(value: float, history: List[float]) -> float:
    """Normalise value to 0–1 using historical range. Clamp at extremes."""
    if not history or len(history) < 5:
        return 0.5  # neutral when insufficient history
    mn, mx = min(history), max(history)
    if mx == mn:
        return 0.5
    return max(0.0, min(1.0, (value - mn) / (mx - mn)))


def calc_physical_complexity(
    grai: float,
    schumann_deviation: float,
    pollen_stress_index: float,
    electricity_anomaly: float,
    canal_congestion_zscore: float,
    history: Optional[Dict[str, List[float]]] = None,
) -> float:
    """
    PhysicalComplexity (PC) — combines geophysical and infrastructure signals.

    PC = normalise(
      GRAI × 0.35
      + schumann_deviation × 0.15
      + pollen_stress_index × 0.10
      + electricity_anomaly × 0.25
      + canal_congestion_zscore × 0.15
    )

    Weights reflect estimated economic significance:
    - GRAI (35%): geomagnetic effect has published evidence of market impact
    - Electricity anomaly (25%): direct economic proxy, real-time
    - Canal congestion (15%): supply chain leading indicator
    - Pollen (10%): seasonal, sector-specific, predictable
    - Schumann (15%): speculative but costless to include
    """
    h = history or {}
    raw = (
        grai * 0.35
        + schumann_deviation * 0.15
        + pollen_stress_index * 0.10
        + electricity_anomaly * 0.25
        + canal_congestion_zscore * 0.15
    )
    hist_vals = h.get("physical_complexity", [])
    return _safe_norm(raw, hist_vals) if hist_vals else min(1.0, max(0.0, raw / 10.0))


def calc_social_complexity(
    asi: float,
    scv_max: float,
    divorce_anomaly: float,
    obituary_impact: float,
    church_attendance_signal: float,
    history: Optional[Dict[str, List[float]]] = None,
) -> float:
    """
    SocialComplexity (SC) — captures human behavioural and demographic signals.

    SC = normalise(
      (1 − ASI) × 0.30    [attention saturation — higher = more complex]
      + SCV_max × 0.25    [social contagion spreading anywhere in universe]
      + divorce_anomaly × 0.15
      + obituary_impact × 0.15
      + church_attendance_signal × 0.15
    )

    Note: (1-ASI) because ASI near 0 means high saturation = high complexity.
    """
    h = history or {}
    raw = (
        (1.0 - asi) * 0.30
        + scv_max * 0.25
        + divorce_anomaly * 0.15
        + obituary_impact * 0.15
        + church_attendance_signal * 0.15
    )
    hist_vals = h.get("social_complexity", [])
    return _safe_norm(raw, hist_vals) if hist_vals else min(1.0, max(0.0, raw))


def calc_scientific_complexity(
    qtpi: float,
    amr_urgency: float,
    citation_velocity: float,
    soil_health_degradation: float,
    history: Optional[Dict[str, List[float]]] = None,
) -> float:
    """
    ScientificComplexity (SciC) — captures long-duration technological signals.

    SciC = normalise(
      QTPI × 0.40
      + amr_urgency × 0.25
      + academic_citation_velocity × 0.20
      + soil_health_degradation × 0.15
    )

    QTPI dominates (40%) because quantum breakthrough is the single largest
    structural risk to financial infrastructure.
    """
    h = history or {}
    raw = (
        qtpi * 0.40
        + amr_urgency * 0.25
        + citation_velocity * 0.20
        + soil_health_degradation * 0.15
    )
    hist_vals = h.get("scientific_complexity", [])
    return _safe_norm(raw, hist_vals) if hist_vals else min(1.0, max(0.0, raw))


def calc_financial_frontier_complexity(
    lpas_mean: float,
    option_overhang: float,
    building_permit_inflection: float,
    food_safety_risk: float,
    history: Optional[Dict[str, List[float]]] = None,
) -> float:
    """
    FinancialFrontierComplexity (FFC) — non-traditional financial signals.

    FFC = normalise(
      LPAS_universe_mean × 0.35
      + option_overhang × 0.30
      + building_permit_inflection × 0.20
      + food_safety_risk × 0.15
    )
    """
    h = history or {}
    raw = (
        lpas_mean * 0.35
        + option_overhang * 0.30
        + building_permit_inflection * 0.20
        + food_safety_risk * 0.15
    )
    hist_vals = h.get("ffc", [])
    return _safe_norm(raw, hist_vals) if hist_vals else min(1.0, max(0.0, raw / 5.0))


def calc_altdata_complexity(
    reddit_coordination: float,
    wikipedia_surge_count: float,
    shipping_pressure: float,
    insider_cluster: float,
    history: Optional[Dict[str, List[float]]] = None,
) -> float:
    """
    AltDataComplexity (ADC) — from existing altdata module.

    ADC = normalise(
      reddit_coordination × 0.25
      + wikipedia_surge_count × 0.20
      + shipping_pressure × 0.25
      + insider_cluster × 0.30
    )

    Insider cluster score has highest weight (30%) because coordinated
    insider buying/selling is the most reliable signal of pending material information.
    """
    h = history or {}
    raw = (
        reddit_coordination * 0.25
        + wikipedia_surge_count * 0.20
        + shipping_pressure * 0.25
        + insider_cluster * 0.30
    )
    hist_vals = h.get("altdata_complexity", [])
    return _safe_norm(raw, hist_vals) if hist_vals else min(1.0, max(0.0, raw))


# ---------------------------------------------------------------------------
# Main UMCI calculator
# ---------------------------------------------------------------------------

def calc_umci(
    physical_complexity: float,
    social_complexity: float,
    scientific_complexity: float,
    financial_complexity: float,
    altdata_complexity: float,
    config: Optional[Dict] = None,
) -> Tuple[float, Dict]:
    """
    Unified Market Complexity Index (UMCI).

    UMCI(t) = (
        PC  × physical_weight  +
        SC  × social_weight    +
        SciC × scientific_weight +
        FFC × financial_weight +
        ADC × altdata_weight
    ) × 100

    Default weights (from config):
      Physical  : 0.20
      Social    : 0.25
      Scientific: 0.15
      Financial : 0.25
      AltData   : 0.15

    Returns
    -------
    (umci_score: float, breakdown: dict)
      umci_score : 0–100
      breakdown  : per-dimension scores, dominant dimension, level name
    """
    cfg = (config or {}).get("frontier", {}).get("complexity_index", {})
    pw  = cfg.get("physical_weight",   0.20)
    sw  = cfg.get("social_weight",     0.25)
    scw = cfg.get("scientific_weight", 0.15)
    fw  = cfg.get("financial_weight",  0.25)
    aw  = cfg.get("altdata_weight",    0.15)

    raw = (
        physical_complexity   * pw  +
        social_complexity     * sw  +
        scientific_complexity * scw +
        financial_complexity  * fw  +
        altdata_complexity    * aw
    )
    umci = min(100.0, max(0.0, raw * 100.0))

    dimensions = {
        "physical":   physical_complexity,
        "social":     social_complexity,
        "scientific": scientific_complexity,
        "financial":  financial_complexity,
        "altdata":    altdata_complexity,
    }
    dominant = max(dimensions, key=dimensions.get)
    level = get_umci_level(umci)

    breakdown = {
        "umci": round(umci, 2),
        "level": level["name"],
        "dominant_dimension": dominant,
        "dimensions": {k: round(v, 4) for k, v in dimensions.items()},
        "position_multiplier": level["position_multiplier"],
        "preferred_strategy": level["preferred_strategy"],
        "message": level["message"],
        "frontier_signals_active": level["frontier_active"],
        "halt_new_positions": level.get("halt", False),
        "alert_required": level.get("alert", False),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return umci, breakdown


# ---------------------------------------------------------------------------
# UMCI History and snapshot logger
# ---------------------------------------------------------------------------

class UMCILogger:
    """Permanently logs every UMCI reading with full dimensional breakdown."""

    def __init__(self, log_path: str = "logs/umci_history.jsonl"):
        self._path = Path(log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, umci: float, breakdown: Dict) -> None:
        with open(self._path, "a") as f:
            f.write(json.dumps({"umci": umci, **breakdown}) + "\n")

    def get_history(self, n: int = 252) -> List[Dict]:
        if not self._path.exists():
            return []
        records = []
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
        return records[-n:]

    def last_comparable(self, current_umci: float, tolerance: float = 5.0) -> Optional[Dict]:
        """Find the last time UMCI was within tolerance of current value."""
        history = self.get_history(n=2000)
        for rec in reversed(history[:-1]):  # skip current
            if abs(rec.get("umci", 0) - current_umci) <= tolerance:
                return rec
        return None

    def generate_complexity_report(
        self,
        umci: float,
        breakdown: Dict,
        comparable: Optional[Dict] = None,
    ) -> str:
        """
        Complexity breakdown report in plain text.
        Includes: which dimension is highest, specific signals,
        historical comparison, and manual review recommendations.
        """
        lines = [
            "=" * 65,
            f"  UMCI COMPLEXITY REPORT   {breakdown.get('timestamp', '')}",
            "=" * 65,
            f"  Score   : {umci:.1f}/100  [{breakdown['level']}]",
            f"  Message : {breakdown['message']}",
            f"  Dominant: {breakdown['dominant_dimension'].upper()} dimension",
            "",
            "  Dimension breakdown:",
        ]
        for dim, score in breakdown.get("dimensions", {}).items():
            bar = "█" * int(score * 20)
            lines.append(f"    {dim:<12} {score:.3f}  {bar}")

        lines.append("")
        lines.append(f"  Strategy  : {breakdown['preferred_strategy'].upper()}")
        lines.append(f"  Position  : {breakdown['position_multiplier']:.0%} of normal")
        lines.append(f"  Frontier  : {'ACTIVE' if breakdown.get('frontier_signals_active') else 'SUPPRESSED'}")

        if comparable:
            lines.extend([
                "",
                f"  Last similar UMCI: {comparable.get('timestamp', 'unknown')}",
                f"  Score was        : {comparable.get('umci', '?'):.1f}",
            ])

        if breakdown.get("halt_new_positions"):
            lines.extend([
                "",
                "  *** HALT: No new positions until UMCI < 95 ***",
                "  *** Existing positions maintained. ***",
            ])

        lines.append("=" * 65)
        return "\n".join(lines)
