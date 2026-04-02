"""
Soft-updates signal weights after every trade. Batch-optimises weekly.
Integrates stress fragility caps.
"""
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in weight_updater")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False
    logger.warning("numpy not available — batch_update will use fallback Sharpe method")

try:
    import cvxpy as cp
    _HAS_CVXPY = True
except ImportError:
    cp = None  # type: ignore
    _HAS_CVXPY = False
    logger.warning("cvxpy not available — batch_update will use proportional-Sharpe fallback")


DEFAULT_WEIGHT_BOUNDS = {"min": 0.05, "max": 3.0}


def _sharpe_from_series(pnl_series: List[float]) -> float:
    if len(pnl_series) < 2:
        return 0.0
    if _HAS_NUMPY:
        arr = np.array(pnl_series, dtype=float)
        std = float(np.std(arr, ddof=1))
        mean = float(np.mean(arr))
    else:
        n = len(pnl_series)
        mean = sum(pnl_series) / n
        variance = sum((v - mean) ** 2 for v in pnl_series) / (n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
    return (mean / std) * math.sqrt(252) if std > 0 else 0.0


class WeightUpdater:
    """Manages signal weight updates via soft per-trade updates and weekly batch optimisation."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._cfg = (config or {}).get("closeloop", {}).get("learning", {})
        self._soft_update_rate: float = self._cfg.get("soft_update_rate", 0.02)
        self._max_change: float = self._cfg.get("max_weight_change_per_update", 0.15)
        self._bounds: Dict = self._cfg.get("weight_bounds", DEFAULT_WEIGHT_BOUNDS)
        self._min_trades: int = self._cfg.get("min_trades_before_weight_update", 10)
        self._w_min: float = self._bounds.get("min", DEFAULT_WEIGHT_BOUNDS["min"])
        self._w_max: float = self._bounds.get("max", DEFAULT_WEIGHT_BOUNDS["max"])

    # ------------------------------------------------------------------
    # Soft per-trade update
    # ------------------------------------------------------------------

    def soft_update(self, attribution: List[Dict], entry_context: dict) -> List[Dict]:
        """
        For each signal in attribution:
          gradient = attributed_performance * soft_update_rate
          new_weight = clip(current * (1 + gradient), w_min, w_max)
          Max change per update enforced: abs(new-old)/old <= max_weight_change_per_update
          Skip if n_trades < min_trades_before_weight_update
          Log every change to store.weight_history

        Returns list of {signal_name, old_weight, new_weight, reason}
        """
        changes: List[Dict] = []

        for attr in attribution:
            signal_name: str = attr.get("signal_name", "unknown")
            attributed_pnl: float = attr.get("attributed_pnl", 0.0)

            try:
                # Check trade count gate
                n_trades = 0
                if self._store is not None:
                    try:
                        sc = self._store.get_signal_scorecard(signal_name)
                        n_trades = sc.get("n_trades", 0) if sc else 0
                    except Exception:
                        pass

                if n_trades < self._min_trades:
                    logger.debug(
                        "Skipping weight update for %s: only %d trades (min %d)",
                        signal_name, n_trades, self._min_trades,
                    )
                    continue

                # Current weight
                current_weight = 1.0
                if self._store is not None:
                    try:
                        current_weight = self._store.get_signal_weight(signal_name, default=1.0)
                    except Exception:
                        pass

                # Gradient step
                gradient = attributed_pnl * self._soft_update_rate
                raw_new = current_weight * (1.0 + gradient)

                # Enforce max change per update
                max_delta = current_weight * self._max_change
                if abs(raw_new - current_weight) > max_delta:
                    direction = 1.0 if raw_new > current_weight else -1.0
                    raw_new = current_weight + direction * max_delta

                # Clip to bounds
                new_weight = max(self._w_min, min(self._w_max, raw_new))

                if abs(new_weight - current_weight) < 1e-9:
                    continue

                reason = (
                    f"soft_update: attributed_pnl={attributed_pnl:.4f} "
                    f"gradient={gradient:.4f} n_trades={n_trades}"
                )

                # Persist
                if self._store is not None:
                    try:
                        self._store.set_signal_weight(
                            signal_name, new_weight, reason=reason,
                            n_trades=n_trades, sharpe=0.0, auto=True,
                        )
                    except Exception as exc:
                        logger.warning("Failed to persist weight for %s: %s", signal_name, exc)

                change_record = {
                    "signal_name": signal_name,
                    "old_weight": current_weight,
                    "new_weight": new_weight,
                    "reason": reason,
                }
                changes.append(change_record)
                logger.debug(
                    "WeightUpdater soft_update: %s %.4f -> %.4f",
                    signal_name, current_weight, new_weight,
                )

            except Exception as exc:
                logger.warning("soft_update failed for signal %s: %s", signal_name, exc)

        return changes

    # ------------------------------------------------------------------
    # Batch weekly optimisation
    # ------------------------------------------------------------------

    def batch_update(self, all_trades: List[Dict]) -> Dict:
        """
        Weekly portfolio optimisation.
        Tries cvxpy for mean-variance. Falls back to proportional Sharpe.
        Smooths: 70% current + 30% optimal.
        Notifies of changes > 20%.

        Returns {signal_name: new_weight} dict.
        """
        # Collect per-signal P&L series from trades
        signal_pnl: Dict[str, List[float]] = {}
        for trade in all_trades:
            try:
                import json
                signals_raw = trade.get("signals_at_entry") or "{}"
                if isinstance(signals_raw, str):
                    signals = json.loads(signals_raw)
                else:
                    signals = signals_raw
                net_pnl = trade.get("net_pnl", 0.0)
                for sig in signals:
                    if sig not in signal_pnl:
                        signal_pnl[sig] = []
                    signal_pnl[sig].append(float(net_pnl))
            except Exception:
                continue

        if not signal_pnl:
            logger.warning("batch_update: no signal P&L data available")
            return {}

        signal_names = list(signal_pnl.keys())
        sharpes = {s: _sharpe_from_series(signal_pnl[s]) for s in signal_names}

        # Current weights
        current_weights: Dict[str, float] = {}
        if self._store is not None:
            try:
                current_weights = self._store.get_all_weights()
            except Exception as exc:
                logger.warning("batch_update: failed to fetch current weights: %s", exc)

        optimal: Dict[str, float] = {}

        # ---- cvxpy optimisation ------------------------------------------
        if _HAS_CVXPY and _HAS_NUMPY and len(signal_names) >= 2:
            try:
                # Build per-signal return series (pad to same length)
                max_len = max(len(signal_pnl[s]) for s in signal_names)
                pnl_matrix = np.zeros((max_len, len(signal_names)))
                for j, s in enumerate(signal_names):
                    series = signal_pnl[s]
                    pnl_matrix[-len(series):, j] = series

                mu = pnl_matrix.mean(axis=0)
                cov = np.cov(pnl_matrix, rowvar=False)
                if cov.ndim == 0:
                    cov = np.array([[float(cov)]])

                risk_aversion = 2.0
                w = cp.Variable(len(signal_names))
                objective = cp.Maximize(
                    mu @ w - 0.5 * risk_aversion * cp.quad_form(w, cov)
                )
                constraints = [
                    w >= self._w_min,
                    w <= self._w_max,
                ]
                prob = cp.Problem(objective, constraints)
                prob.solve(solver=cp.ECOS, warm_start=True)

                if prob.status in ("optimal", "optimal_inaccurate") and w.value is not None:
                    for j, s in enumerate(signal_names):
                        optimal[s] = float(np.clip(w.value[j], self._w_min, self._w_max))
                    logger.info("batch_update: cvxpy optimisation succeeded")
                else:
                    logger.warning("batch_update: cvxpy status=%s, using Sharpe fallback", prob.status)
            except Exception as exc:
                logger.warning("batch_update: cvxpy failed (%s), using Sharpe fallback", exc)

        # ---- Fallback: proportional to Sharpe --------------------------------
        if not optimal:
            positive_sharpes = {s: max(0.0, sharpes[s]) for s in signal_names}
            total = sum(positive_sharpes.values()) or 1.0
            n_signals = len(signal_names)
            for s in signal_names:
                if total > 0:
                    # Scale so that mean weight = 1.0
                    optimal[s] = max(
                        self._w_min,
                        min(self._w_max, (positive_sharpes[s] / total) * n_signals),
                    )
                else:
                    optimal[s] = 1.0

        # ---- 70/30 smoothing ------------------------------------------------
        new_weights: Dict[str, float] = {}
        for s in signal_names:
            cur = current_weights.get(s, 1.0)
            blended = 0.70 * cur + 0.30 * optimal[s]
            blended = max(self._w_min, min(self._w_max, blended))
            new_weights[s] = blended

            # Notify significant changes
            change_pct = abs(blended - cur) / max(abs(cur), 1e-9) * 100
            if change_pct > 20.0:
                logger.warning(
                    "batch_update SIGNIFICANT CHANGE: %s %.4f -> %.4f (%.1f%%)",
                    s, cur, blended, change_pct,
                )

            # Persist
            if self._store is not None:
                try:
                    self._store.set_signal_weight(
                        s, blended,
                        reason=f"batch_update sharpe={sharpes[s]:.3f}",
                        n_trades=len(signal_pnl[s]),
                        sharpe=sharpes[s],
                        auto=True,
                    )
                except Exception as exc:
                    logger.warning("batch_update persist failed for %s: %s", s, exc)

        return new_weights

    # ------------------------------------------------------------------
    # Stress caps
    # ------------------------------------------------------------------

    def apply_stress_caps(
        self, vix_level: float, crisis_fragile_signals: List[str]
    ) -> None:
        """
        If vix_level > 25 AND signal in crisis_fragile_signals:
          cap weight at 0.5 * normal weight (log as STRESS_CAP_APPLIED)
        If correlation_regime == CRISIS:
          reduce all weights by 30%.
        """
        if self._store is None:
            logger.warning("apply_stress_caps: no store available, skipping")
            return

        try:
            all_weights = self._store.get_all_weights()
        except Exception as exc:
            logger.warning("apply_stress_caps: failed to fetch weights: %s", exc)
            return

        if vix_level > 25:
            for sig in crisis_fragile_signals:
                current = all_weights.get(sig)
                if current is None:
                    continue
                try:
                    capped = max(self._w_min, current * 0.5)
                    self._store.set_signal_weight(
                        sig, capped,
                        reason=f"STRESS_CAP_APPLIED vix={vix_level:.1f}",
                        auto=True,
                    )
                    logger.warning(
                        "STRESS_CAP_APPLIED: %s %.4f -> %.4f (vix=%.1f)",
                        sig, current, capped, vix_level,
                    )
                except Exception as exc:
                    logger.warning("apply_stress_caps failed for %s: %s", sig, exc)

    def apply_crisis_reduction(self) -> None:
        """Reduce all weights by 30% when correlation_regime == CRISIS."""
        if self._store is None:
            return
        try:
            all_weights = self._store.get_all_weights()
            for sig, current in all_weights.items():
                try:
                    reduced = max(self._w_min, current * 0.70)
                    self._store.set_signal_weight(
                        sig, reduced,
                        reason="CRISIS_REGIME_REDUCTION -30%",
                        auto=True,
                    )
                    logger.warning("CRISIS_REDUCTION: %s %.4f -> %.4f", sig, current, reduced)
                except Exception as exc:
                    logger.warning("apply_crisis_reduction failed for %s: %s", sig, exc)
        except Exception as exc:
            logger.warning("apply_crisis_reduction: failed to fetch weights: %s", exc)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def get_weight_summary(self) -> str:
        """Formatted table: signal | current_weight | change_since_start | n_trades"""
        if self._store is None:
            return "No store available.\n"

        try:
            rows = self._store._conn().execute("""
                SELECT sw.signal_name, sw.weight, sw.previous_weight, sw.n_trades_basis,
                       sw.sharpe_basis
                FROM signal_weights sw
                ORDER BY sw.weight DESC
            """).fetchall()
        except Exception as exc:
            logger.warning("get_weight_summary query failed: %s", exc)
            return f"Weight summary unavailable: {exc}\n"

        if not rows:
            return "No signal weights recorded yet.\n"

        header = f"{'Signal':<35} {'Weight':>8} {'Change':>9} {'N Trades':>10} {'Sharpe':>8}\n"
        sep = "-" * 74 + "\n"
        lines = [header, sep]
        for r in rows:
            cur = r["weight"] or 1.0
            prev = r["previous_weight"] or 1.0
            change = (cur - prev) / max(abs(prev), 1e-9) * 100
            lines.append(
                f"{r['signal_name']:<35} {cur:>8.4f} {change:>+8.1f}% "
                f"{(r['n_trades_basis'] or 0):>10} {(r['sharpe_basis'] or 0.0):>8.3f}"
            )
        lines.append("")
        return "\n".join(lines)
