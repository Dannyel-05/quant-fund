"""
Frontier Signal Purity tracking — monitors how novel each signal remains over time.
If FSP declines, competitors are discovering and trading the same signal.
"""
import logging
from typing import Dict, List, Optional

import pandas as pd

from frontier.equations.derived_formulas import calc_fsp, calc_frontier_value_score

logger = logging.getLogger(__name__)


class SignalPurityTracker:
    """
    Tracks FSP for each frontier signal over time.
    Alerts when purity is declining (competitors entering).
    """

    def __init__(self, config: dict):
        self._history: Dict[str, List[Dict]] = {}

    def update(
        self,
        signal_name: str,
        signal_returns: pd.Series,
        factor_returns: Dict[str, pd.Series],
        validated_sharpe: float,
        evidence_tier: int,
        timestamp: Optional[str] = None,
    ) -> Dict:
        """
        Calculate and store current FSP for a signal.
        Returns dict with current FSP, FrontierValueScore, and trend.
        """
        from datetime import datetime, timezone
        ts = timestamp or datetime.now(timezone.utc).isoformat()

        fsp = calc_fsp(signal_returns, factor_returns)
        fvs = calc_frontier_value_score(fsp, validated_sharpe, evidence_tier)

        entry = {
            "timestamp": ts,
            "fsp": fsp,
            "frontier_value_score": fvs,
            "validated_sharpe": validated_sharpe,
            "evidence_tier": evidence_tier,
        }
        self._history.setdefault(signal_name, []).append(entry)

        # Trend: compare to 3 months ago
        hist = self._history[signal_name]
        trend = "stable"
        if len(hist) >= 4:
            older_fsp = sum(e["fsp"] for e in hist[-4:-1]) / 3
            if fsp < older_fsp - 0.05:
                trend = "declining"
                logger.warning(
                    f"Signal purity declining for '{signal_name}': "
                    f"{older_fsp:.3f} → {fsp:.3f}. "
                    "Competitors may be discovering this signal."
                )
            elif fsp > older_fsp + 0.05:
                trend = "increasing"

        return {
            "signal_name": signal_name,
            "fsp": fsp,
            "frontier_value_score": fvs,
            "purity_trend": trend,
            "alert": trend == "declining",
        }

    def get_purity_ranking(self) -> List[Dict]:
        """Return all tracked signals ranked by current FSP descending."""
        latest = []
        for name, hist in self._history.items():
            if hist:
                e = hist[-1]
                latest.append({"signal_name": name, **e})
        return sorted(latest, key=lambda x: x["fsp"], reverse=True)
