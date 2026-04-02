"""
Signal aggregator: combines all signal sources with full provenance tracking.

CONFLUENCE_THRESHOLDS:
  STRONG   > 0.70 → full Kelly
  MODERATE > 0.50 → 0.75x Kelly
  WEAK     > 0.30 → 0.50x Kelly
  NONE     ≤ 0.30 → 0.0 (skip)

Also provides MultiFrequencyAggregator for 4-horizon MFS scoring.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

CONFLUENCE_THRESHOLDS = {
    "STRONG":   0.70,
    "MODERATE": 0.50,
    "WEAK":     0.30,
    "NONE":     0.0,
}

CONFLUENCE_KELLY_MULTIPLIERS = {
    "STRONG":   1.00,
    "MODERATE": 0.75,
    "WEAK":     0.50,
    "NONE":     0.00,
}


class SignalAggregator:
    """
    Aggregates signals from PEAD, frontier, context, and altdata sources.
    Computes a confluence score and recommended Kelly multiplier.
    Maintains full provenance for post-trade attribution.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        self.store = store
        self.config = (config or {}).get("closeloop", {}).get("entry", {})

    def aggregate(
        self,
        ticker: str,
        pead_signal: Optional[Dict] = None,
        frontier_signals: Optional[List[Dict]] = None,
        context_signals: Optional[List[Dict]] = None,
        altdata_signals: Optional[List[Dict]] = None,
        intelligence_signals: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Aggregate all available signals for a ticker.

        Each signal dict should contain:
            signal_name  : str
            value        : float (typically -1 to +1)
            direction    : int (+1 long, -1 short, 0 neutral)
            strength     : float [0, 1]
            quality_score: float [0, 1]
            source       : str

        Returns:
            ticker               : str
            confluence_score     : float [0, 1]
            confluence_level     : str (STRONG/MODERATE/WEAK/NONE)
            kelly_multiplier     : float
            combined_direction   : int (+1/-1/0)
            signal_count         : int
            provenance           : list of signal contributions
            timestamp            : str
        """
        all_signals = []
        provenance = []

        # --- PEAD (primary trigger) ---
        if pead_signal:
            strength = min(abs(pead_signal.get("surprise_zscore", 1.0)) / 3.0, 1.0)
            direction = int(pead_signal.get("signal", 0))
            all_signals.append({
                "name": "pead",
                "strength": strength,
                "direction": direction,
                "quality": 0.9,
                "role": "primary_trigger",
            })
            provenance.append({
                "signal_name": "pead",
                "role": "primary_trigger",
                "strength": round(strength, 4),
                "direction": direction,
                "source": "EarningsCalendar",
            })

        # --- Frontier signals ---
        for sig in (frontier_signals or []):
            name = sig.get("signal_name", "frontier")
            strength = min(abs(sig.get("value", 0.0)), 1.0)
            direction = 1 if sig.get("value", 0) > 0 else (-1 if sig.get("value", 0) < 0 else 0)
            quality = sig.get("quality_score", 0.5)
            all_signals.append({
                "name": name,
                "strength": strength * quality,
                "direction": direction,
                "quality": quality,
                "role": "secondary",
            })
            provenance.append({
                "signal_name": name,
                "role": "secondary",
                "strength": round(strength, 4),
                "direction": direction,
                "source": sig.get("source", "frontier"),
            })

        # --- Context signals ---
        for sig in (context_signals or []):
            name = sig.get("signal_name", "context")
            strength = float(sig.get("strength", 0.3))
            direction = int(sig.get("direction", 0))
            all_signals.append({
                "name": name,
                "strength": strength,
                "direction": direction,
                "quality": sig.get("quality_score", 0.6),
                "role": "context",
            })
            provenance.append({
                "signal_name": name,
                "role": "context",
                "strength": round(strength, 4),
                "direction": direction,
                "source": sig.get("source", "context"),
            })

        # --- Altdata signals ---
        for sig in (altdata_signals or []):
            name = sig.get("signal_name", "altdata")
            strength = float(sig.get("strength", 0.2))
            direction = int(sig.get("direction", 0))
            all_signals.append({
                "name": name,
                "strength": strength,
                "direction": direction,
                "quality": sig.get("quality_score", 0.5),
                "role": "altdata",
            })
            provenance.append({
                "signal_name": name,
                "role": "altdata",
                "strength": round(strength, 4),
                "direction": direction,
                "source": sig.get("source", "altdata"),
            })

        # --- Intelligence signals (readthrough, pattern, composite) ---
        for sig in (intelligence_signals or []):
            name = sig.get("signal_name", "intelligence")
            raw_score = float(sig.get("value", 0.0))
            direction = 1 if raw_score > 0.05 else (-1 if raw_score < -0.05 else 0)
            strength  = float(sig.get("strength", min(abs(raw_score), 1.0)))
            all_signals.append({
                "name": name,
                "strength": strength,
                "direction": direction,
                "quality": sig.get("quality_score", 0.65),
                "role": "intelligence",
            })
            provenance.append({
                "signal_name": name,
                "role": "intelligence",
                "strength": round(strength, 4),
                "direction": direction,
                "value": round(raw_score, 4),
                "source": sig.get("source", "intelligence_engine"),
            })

        if not all_signals:
            return self._empty(ticker)

        # --- Compute confluence ---
        # Confluence = weighted agreement (same-direction signals)
        primary = next((s for s in all_signals if s["role"] == "primary_trigger"), None)
        primary_dir = primary["direction"] if primary else 0

        agreement_score = 0.0
        total_weight = 0.0

        for sig in all_signals:
            w = sig["strength"] * sig["quality"]
            agrees = (sig["direction"] == primary_dir) if primary_dir != 0 else True
            agreement_score += w * (1.0 if agrees else 0.0)
            total_weight += w

        confluence = agreement_score / total_weight if total_weight > 0 else 0.0

        # Determine level
        level = "NONE"
        for lvl in ["STRONG", "MODERATE", "WEAK"]:
            if confluence >= CONFLUENCE_THRESHOLDS[lvl]:
                level = lvl
                break

        kelly_mult = CONFLUENCE_KELLY_MULTIPLIERS[level]
        combined_dir = primary_dir if primary_dir != 0 else (
            1 if agreement_score > 0 else 0
        )

        return {
            "ticker": ticker,
            "confluence_score": round(confluence, 4),
            "confluence_level": level,
            "kelly_multiplier": kelly_mult,
            "combined_direction": combined_dir,
            "signal_count": len(all_signals),
            "active_signals": [s["name"] for s in all_signals],
            "provenance": provenance,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _empty(self, ticker: str) -> Dict:
        return {
            "ticker": ticker,
            "confluence_score": 0.0,
            "confluence_level": "NONE",
            "kelly_multiplier": 0.0,
            "combined_direction": 0,
            "signal_count": 0,
            "active_signals": [],
            "provenance": [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_multi_frequency_score(self, ticker: str, signals_dict: dict) -> dict:
        """Delegate to MultiFrequencyAggregator.compute_mfs."""
        return MultiFrequencyAggregator(self.config).compute_mfs(ticker, signals_dict)


# ---------------------------------------------------------------------------
# Multi-Frequency Aggregator
# ---------------------------------------------------------------------------


class MultiFrequencyAggregator:
    """
    Combines signals from 4 time horizons into a single Multi-Frequency Score (MFS).

    H1 Intraday    (0.20): HMM state, options flow, pre-market momentum
    H2 Short-term  (0.35): PEAD, pairs, momentum, analyst revision, calendar
    H3 Medium-term (0.30): Macro regime, sector rotation, narrative shift
    H4 Long-term   (0.15): Technology themes, thematic tailwinds

    All signal values are in [-1, +1] (0 = neutral, +1 = strong long, -1 = strong short).
    Missing keys default to 0.0 (neutral).
    """

    WEIGHTS = {"h1": 0.20, "h2": 0.35, "h3": 0.30, "h4": 0.15}

    def __init__(self, config=None):
        self.config = config or {}

    def compute_mfs(self, ticker: str, signals: dict) -> dict:
        """
        Compute Multi-Frequency Score for a ticker.

        Parameters
        ----------
        ticker  : str
        signals : dict with optional keys:
            h1 : {"hmm_signal": float, "options_flow": float, "momentum_1d": float}
            h2 : {"pead_signal": float, "pairs_signal": float,
                  "analyst_revision": float, "calendar_modifier": float}
            h3 : {"macro_regime": float, "sector_rotation": float,
                  "narrative_shift": float}
            h4 : {"tech_theme": float, "thematic_score": float}

        Returns
        -------
        dict with mfs, horizon scores, size_multiplier, conflict flag, label
        """

        def _h_score(h_dict: dict) -> float:
            vals = [float(v) for v in h_dict.values() if v is not None]
            return float(np.mean(vals)) if vals else 0.0

        h1 = _h_score(signals.get("h1", {}))
        h2 = _h_score(signals.get("h2", {}))
        h3 = _h_score(signals.get("h3", {}))
        h4 = _h_score(signals.get("h4", {}))

        mfs = (
            h1 * self.WEIGHTS["h1"]
            + h2 * self.WEIGHTS["h2"]
            + h3 * self.WEIGHTS["h3"]
            + h4 * self.WEIGHTS["h4"]
        )
        mfs = float(np.clip(mfs, -1.0, 1.0))

        # Base size multiplier from MFS magnitude
        if mfs > 0.7:
            size_mult = 1.50
        elif mfs > 0.5:
            size_mult = 1.00
        elif mfs > 0.3:
            size_mult = 0.50
        else:
            size_mult = 0.25

        # Conflict detection: H2 LONG but H3 negative (fighting macro)
        conflict = False
        if h2 > 0.3 and h3 < -0.2:
            size_mult *= 0.55
            conflict = True

        if mfs > 0.7:
            label = "MAX_CONVICTION"
        elif mfs > 0.5:
            label = "STANDARD"
        elif mfs > 0.3:
            label = "REDUCED"
        else:
            label = "MINIMAL"

        return {
            "ticker": ticker,
            "mfs": round(mfs, 4),
            "h1_score": round(h1, 4),
            "h2_score": round(h2, 4),
            "h3_score": round(h3, 4),
            "h4_score": round(h4, 4),
            "size_multiplier": round(size_mult, 4),
            "conflict_detected": conflict,
            "label": label,
            "computed_at": datetime.utcnow().isoformat(),
        }
