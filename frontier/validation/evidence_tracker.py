"""
Evidence Tracker — accumulates live accuracy data for frontier signals
and triggers automatic tier promotion when criteria are met.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Evidence grades based on validation test results
EVIDENCE_GRADES = {
    "A": "Published + replicated + OOS confirmed",
    "B": "Novel + strong OOS (Sharpe > 1.0)",
    "C": "Novel + moderate OOS (Sharpe 0.5–1.0)",
    "D": "Early data / watchlist only",
    "F": "Failed validation",
}


def assign_evidence_grade(
    has_published_paper: bool,
    replications: int,
    oos_sharpe: float,
    monte_carlo_pct: float,    # fraction of shuffles beaten (0–1)
    benjamini_pass: bool,
    fsp: float,
) -> str:
    """
    Assign an evidence grade A–F based on validation outcomes.

    Grade A: published paper + ≥2 replications + OOS Sharpe > 1.5 + MC top 1%
    Grade B: novel + OOS Sharpe > 1.0 + MC top 5% + BH pass + FSP > 0.5
    Grade C: novel + OOS Sharpe > 0.5 + MC top 10%
    Grade D: early data, Sharpe > 0
    Grade F: anything that fails
    """
    if oos_sharpe <= 0 or monte_carlo_pct < 0.90:
        return "F"
    if has_published_paper and replications >= 2 and oos_sharpe > 1.5 and monte_carlo_pct > 0.99:
        return "A"
    if oos_sharpe > 1.0 and monte_carlo_pct > 0.95 and benjamini_pass and fsp > 0.5:
        return "B"
    if oos_sharpe > 0.5 and monte_carlo_pct > 0.90:
        return "C"
    return "D"


def grade_to_tier(grade: str) -> int:
    """Map evidence grade to initial sizing tier."""
    return {"A": 1, "B": 2, "C": 3, "D": 4, "F": 5}.get(grade, 5)


class EvidenceTracker:
    """
    Accumulates live trade outcomes for a frontier signal and
    computes running accuracy, live Sharpe, and promotion eligibility.
    """

    def __init__(self, signal_name: str, initial_tier: int = 4):
        self.signal_name = signal_name
        self.current_tier = initial_tier
        self._records: List[Dict] = []

    def record_outcome(
        self,
        direction: int,     # +1 / -1
        entry_price: float,
        exit_price: float,
        hold_days: int,
        timestamp: Optional[str] = None,
    ) -> None:
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        pnl_pct = direction * (exit_price - entry_price) / entry_price
        self._records.append({
            "timestamp": ts,
            "direction": direction,
            "pnl_pct": pnl_pct,
            "hold_days": hold_days,
        })

    def live_stats(self) -> Dict:
        """Compute running accuracy, live Sharpe, win rate."""
        if not self._records:
            return {
                "n_trades": 0,
                "win_rate": 0.0,
                "live_sharpe": 0.0,
                "mean_pnl_pct": 0.0,
                "live_days": 0,
            }

        import numpy as np
        pnls = [r["pnl_pct"] for r in self._records]
        wins = sum(1 for p in pnls if p > 0)
        mean_pnl = float(np.mean(pnls))
        std_pnl = float(np.std(pnls)) if len(pnls) > 1 else 1e-9
        live_sharpe = (mean_pnl / std_pnl) * (252 ** 0.5) if std_pnl > 0 else 0.0

        # Estimate live days as sum of hold_days (proxy)
        live_days = sum(r["hold_days"] for r in self._records)

        return {
            "n_trades": len(self._records),
            "win_rate": round(wins / len(self._records), 3),
            "live_sharpe": round(live_sharpe, 3),
            "mean_pnl_pct": round(mean_pnl * 100, 3),
            "live_days": live_days,
        }

    def check_promotion(self, fsp: float = 0.7, validated_replications: int = 0) -> Dict:
        from frontier.sizing.frontier_sizer import check_tier_promotion

        stats = self.live_stats()
        result = check_tier_promotion(
            signal_name=self.signal_name,
            current_tier=self.current_tier,
            live_days=stats["live_days"],
            oos_sharpe=stats["live_sharpe"],
            fsp=fsp,
            validated_replications=validated_replications,
        )
        if result["promoted"]:
            old_tier = self.current_tier
            self.current_tier = result["new_tier"]
            logger.info(
                f"[EvidenceTracker] {self.signal_name}: Tier {old_tier} → {self.current_tier}"
            )
        return result
