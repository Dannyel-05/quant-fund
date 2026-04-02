"""
Stress learner: learns which signals are fragile under stress conditions.

Learning mode: always on (records every trade outcome vs stress conditions).
Prediction mode: activates when n_trades >= 50.

Tracks which signals lose money during high-stress environments and
adjusts fragility scores accordingly.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_MIN_TRADES_FOR_PREDICTION = 50
_STRESS_VIX_THRESHOLD = 25.0
_FRAGILITY_THRESHOLD = 0.40  # signal fragility score above which we cap weights


class StressLearner:
    """
    Records trade outcomes under different stress conditions and learns
    which signals perform well or poorly under market stress.
    """

    def __init__(self, store=None, config=None):
        self.store = store
        self.config = config or {}
        self._n_trades = 0
        self._fragility_cache: Dict[str, float] = {}

    @property
    def prediction_mode_active(self) -> bool:
        return self._n_trades >= _MIN_TRADES_FOR_PREDICTION

    def record_outcome(
        self,
        trade_id: str,
        signal_names: List[str],
        net_pnl: float,
        entry_vix: float,
        exit_vix: Optional[float],
        drawdown_at_exit: float,
        umci_at_entry: Optional[float] = None,
    ) -> None:
        """
        Record a trade outcome alongside the stress conditions at entry/exit.
        Always called regardless of n_trades count (learning always on).
        """
        self._n_trades += 1
        is_stress = entry_vix >= _STRESS_VIX_THRESHOLD
        trade_return = net_pnl / max(abs(net_pnl), 1.0)  # normalised direction

        for signal in signal_names:
            # Update in-memory fragility cache
            current = self._fragility_cache.get(signal, 0.5)
            if is_stress and net_pnl < 0:
                # Stressed loss → increases fragility score
                new_score = min(1.0, current + 0.05)
            elif is_stress and net_pnl > 0:
                # Stressed win → decreases fragility score
                new_score = max(0.0, current - 0.03)
            else:
                # Unstressed → slight decay toward neutral
                new_score = current * 0.99 + 0.5 * 0.01
            self._fragility_cache[signal] = new_score

        # Persist to store
        if self.store:
            try:
                self.store.record_stress_outcome(
                    run_date=__import__("datetime").datetime.now().isoformat(),
                    scenario_name=f"vix_{entry_vix:.0f}",
                    weighted_stress_risk=entry_vix / 80.0,
                    crisis_fragile=is_stress,
                    top_scenario=",".join(signal_names[:3]),
                    conditions=str({
                        "vix": entry_vix,
                        "drawdown": drawdown_at_exit,
                        "umci": umci_at_entry,
                        "trade_id": trade_id,
                        "net_pnl": net_pnl,
                    }),
                )
            except Exception as exc:
                logger.debug("StressLearner.record_outcome store error: %s", exc)

    def predict_fragility(self, signal_name: str) -> Dict:
        """
        Predict how fragile a signal is under stress conditions.

        Returns:
            fragility_score : float [0, 1] (0=robust, 1=very fragile)
            prediction_mode : bool
            recommendation  : str
            n_trades_seen   : int
        """
        score = self._fragility_cache.get(signal_name, 0.5)

        if not self.prediction_mode_active:
            return {
                "signal_name": signal_name,
                "fragility_score": score,
                "prediction_mode": False,
                "recommendation": "INSUFFICIENT_DATA",
                "n_trades_seen": self._n_trades,
                "min_trades_needed": _MIN_TRADES_FOR_PREDICTION,
            }

        if score >= _FRAGILITY_THRESHOLD:
            recommendation = "CAP_WEIGHT"
        elif score >= 0.25:
            recommendation = "MONITOR"
        else:
            recommendation = "ROBUST"

        return {
            "signal_name": signal_name,
            "fragility_score": round(score, 4),
            "prediction_mode": True,
            "recommendation": recommendation,
            "n_trades_seen": self._n_trades,
        }

    def get_all_fragility_scores(self) -> Dict[str, float]:
        """Return fragility scores for all signals seen."""
        return dict(self._fragility_cache)

    def get_crisis_fragile_signals(self) -> List[str]:
        """Return list of signals above the fragility threshold."""
        return [
            s for s, score in self._fragility_cache.items()
            if score >= _FRAGILITY_THRESHOLD
        ]

    def update_signal_vulnerability(self, *args, **kwargs) -> None:
        """Stub — called by autopsy pipeline; no-op until stress data available."""
        pass

    def load_history_from_store(self) -> None:
        """Initialise from stored stress outcomes (called at startup)."""
        if not self.store:
            return
        try:
            rows = self.store.get_stress_outcomes(limit=500)
            for row in rows:
                conditions_str = row.get("conditions", "{}")
                try:
                    import json
                    cond = json.loads(conditions_str.replace("'", '"'))
                    vix = float(cond.get("vix", 20.0))
                    pnl = float(cond.get("net_pnl", 0.0))
                    signals = row.get("top_scenario", "").split(",")
                    for sig in signals:
                        sig = sig.strip()
                        if not sig:
                            continue
                        is_stress = vix >= _STRESS_VIX_THRESHOLD
                        current = self._fragility_cache.get(sig, 0.5)
                        if is_stress and pnl < 0:
                            self._fragility_cache[sig] = min(1.0, current + 0.03)
                        elif is_stress and pnl > 0:
                            self._fragility_cache[sig] = max(0.0, current - 0.02)
                    self._n_trades += 1
                except Exception:
                    continue
            logger.info("StressLearner: loaded %d historical stress outcomes", len(rows))
        except Exception as exc:
            logger.warning("StressLearner.load_history: %s", exc)
