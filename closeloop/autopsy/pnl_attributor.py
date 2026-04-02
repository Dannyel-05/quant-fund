"""
Decomposes trade P&L across all contributing signals using counterfactual simulation.
"""
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in pnl_attributor")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False
    logger.warning("numpy not available — Sharpe computed via pure Python")


def _std(values: List[float]) -> float:
    """Pure-Python standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance) if variance > 0 else 0.0


def _sharpe(pnl_series: List[float]) -> float:
    """Annualised Sharpe from daily/trade P&L series (zero risk-free rate)."""
    if not pnl_series:
        return 0.0
    if _HAS_NUMPY:
        arr = np.array(pnl_series, dtype=float)
        std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
        mean = float(np.mean(arr))
    else:
        mean = sum(pnl_series) / len(pnl_series)
        std = _std(pnl_series)
    if std == 0:
        return 0.0
    return (mean / std) * math.sqrt(252)


class PnLAttributor:
    """Decomposes P&L contributions per signal via counterfactual analysis."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = config or {}
        # In-memory scorecard: signal_name -> {pnl_series, wins, n}
        self._scorecard_cache: Dict[str, Dict] = {}

    # ------------------------------------------------------------------
    # Core attribution
    # ------------------------------------------------------------------

    def attribute(
        self,
        trade_id: int,
        closed_trade: dict,
        entry_context: dict,
    ) -> List[Dict]:
        """
        For each signal in entry_context['active_signals']:
          1. Calculate signal_strength_at_entry
          2. Estimate counterfactual_pnl without this signal
          3. attributed_pnl = actual_pnl - counterfactual_pnl
          4. was_signal_correct = (direction * actual_return) > 0

        Special attributions:
          - EntryTimer: entry_alpha = actual_pnl - immediate_open_pnl
          - ScaleIn tranches: per-tranche attribution logged separately
          - Peer influence: attributed if peer_influence_score > 0.3

        Returns list of attribution dicts and stores them in the closeloop_store.
        """
        actual_pnl = closed_trade.get("net_pnl", 0.0)
        direction = closed_trade.get("direction", 1)
        position_size = closed_trade.get("position_size", 1.0) or 1.0
        entry_price = closed_trade.get("entry_price", 1.0) or 1.0
        actual_return = actual_pnl / (entry_price * position_size) if entry_price else 0.0

        active_signals: List[dict] = entry_context.get("active_signals", [])
        # Support list-of-str or list-of-dict
        signal_dicts: List[dict] = []
        for s in active_signals:
            if isinstance(s, dict):
                signal_dicts.append(s)
            elif isinstance(s, str):
                signal_dicts.append({"name": s, "strength": 1.0, "role": "secondary"})

        attributions: List[Dict] = []

        for sig in signal_dicts:
            try:
                signal_name: str = sig.get("name", "unknown")
                strength: float = float(sig.get("strength", 1.0))
                role: str = sig.get("role", "secondary")
                source_module: str = sig.get("source_module", "")

                # --- Counterfactual logic ----------------------------------
                if role == "primary_trigger":
                    # Without this signal there would be no trade
                    counterfactual_pnl = 0.0
                elif role == "size_boost":
                    # Without this signal, position at base size (assume 80% of current)
                    base_fraction = 0.80
                    counterfactual_pnl = actual_pnl * base_fraction
                elif role == "early_exit":
                    # Signal triggered early exit; counterfactual = held to original plan
                    planned_hold_pnl = entry_context.get(
                        f"{signal_name}_planned_pnl", actual_pnl * 1.1
                    )
                    counterfactual_pnl = float(planned_hold_pnl)
                else:
                    # Generic secondary signal: proportional weight removal
                    # Estimate contribution as strength fraction of total attribution space
                    total_strength = sum(
                        float(s.get("strength", 1.0)) for s in signal_dicts
                    ) or 1.0
                    weight_fraction = strength / total_strength
                    # Counterfactual: without signal we'd have (1 - fraction) of the pnl
                    counterfactual_pnl = actual_pnl * (1.0 - weight_fraction)

                attributed_pnl = actual_pnl - counterfactual_pnl
                denominator = entry_price * position_size
                attributed_pnl_pct = attributed_pnl / denominator if denominator else 0.0
                was_correct = (direction * actual_return) > 0

                attr_record: Dict = {
                    "signal_name": signal_name,
                    "signal_source_module": source_module,
                    "signal_strength": strength,
                    "signal_direction": direction,
                    "attributed_pnl": attributed_pnl,
                    "attributed_pnl_pct": attributed_pnl_pct,
                    "was_correct": was_correct,
                    "counterfactual_pnl": counterfactual_pnl,
                    "role": role,
                }

                # ---- EntryTimer special attribution -------------------------
                if signal_name == "EntryTimer":
                    immediate_open_pnl = entry_context.get("immediate_open_pnl", actual_pnl)
                    entry_alpha = actual_pnl - float(immediate_open_pnl)
                    attr_record["entry_alpha"] = entry_alpha

                # ---- ScaleIn tranche attribution ----------------------------
                tranche = closed_trade.get("scale_in_tranche", 0)
                if tranche and tranche > 0:
                    attr_record["scale_in_tranche"] = tranche

                # ---- Peer influence ----------------------------------------
                peer_score = entry_context.get("peer_influence_score", 0.0) or 0.0
                if signal_name == "PeerInfluence" and peer_score > 0.3:
                    attr_record["peer_influence_score"] = peer_score

                attributions.append(attr_record)

                # Update in-memory scorecard cache
                self._update_cache(signal_name, attributed_pnl, was_correct)

            except Exception as exc:
                logger.warning("Attribution failed for signal %s: %s", sig, exc)

        # ---- Store attributions -------------------------------------------
        if attributions and self._store is not None:
            try:
                self._store.record_attribution(trade_id, attributions)
            except Exception as exc:
                logger.warning("Failed to store attributions for trade %s: %s", trade_id, exc)

        return attributions

    # ------------------------------------------------------------------
    # Scorecard
    # ------------------------------------------------------------------

    def _update_cache(self, signal_name: str, pnl: float, was_correct: bool) -> None:
        if signal_name not in self._scorecard_cache:
            self._scorecard_cache[signal_name] = {"pnl_series": [], "wins": 0, "n": 0}
        entry = self._scorecard_cache[signal_name]
        entry["pnl_series"].append(pnl)
        entry["n"] += 1
        if was_correct:
            entry["wins"] += 1

    def get_scorecard(self, signal_name: str) -> Dict:
        """
        Return running scorecard: total_pnl, win_rate, mean_pnl, sharpe, recommendation.

        Recommendation rules:
          n >= 10 and sharpe > 1.0 and win_rate > 0.55  -> INCREASE
          n >= 20 and sharpe < 0   and win_rate < 0.40  -> INVESTIGATE
          n >= 20 and sharpe < -0.5                     -> DECREASE
          else                                           -> MAINTAIN
        """
        # Prefer live store data if available
        store_data: Dict = {}
        if self._store is not None:
            try:
                store_data = self._store.get_signal_scorecard(signal_name) or {}
            except Exception as exc:
                logger.warning("get_signal_scorecard store call failed: %s", exc)

        cache = self._scorecard_cache.get(signal_name, {})
        pnl_series = cache.get("pnl_series", [])
        n = store_data.get("n_trades") or cache.get("n", 0)
        total_pnl = store_data.get("total_pnl") or (sum(pnl_series) if pnl_series else 0.0)
        mean_pnl = store_data.get("mean_pnl") or (total_pnl / n if n > 0 else 0.0)
        win_rate = store_data.get("win_rate") or (
            cache["wins"] / cache["n"] if cache.get("n", 0) > 0 else 0.0
        )
        sharpe = _sharpe(pnl_series)

        # Recommendation
        if n >= 10 and sharpe > 1.0 and win_rate > 0.55:
            recommendation = "INCREASE"
        elif n >= 20 and sharpe < 0 and win_rate < 0.40:
            recommendation = "INVESTIGATE"
        elif n >= 20 and sharpe < -0.5:
            recommendation = "DECREASE"
        else:
            recommendation = "MAINTAIN"

        return {
            "signal_name": signal_name,
            "n_trades": n,
            "total_pnl": total_pnl,
            "mean_pnl": mean_pnl,
            "win_rate": win_rate,
            "sharpe": sharpe,
            "recommendation": recommendation,
        }
