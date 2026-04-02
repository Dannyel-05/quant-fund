"""
Sunday deep retrain: pulls 52 weeks of trades, optimises signal weights,
saves full report.
"""
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in batch_retrainer")

try:
    from closeloop.learning.weight_updater import WeightUpdater
except ImportError:
    WeightUpdater = None  # type: ignore
    logger.warning("WeightUpdater not importable in batch_retrainer")

try:
    from closeloop.learning.regime_tracker import RegimeTracker
except ImportError:
    RegimeTracker = None  # type: ignore
    logger.warning("RegimeTracker not importable in batch_retrainer")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False


def _sharpe(pnl_series: List[float]) -> float:
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


class BatchRetrainer:
    """Runs the full deep-retrain cycle, typically on Sunday evening."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = config or {}
        self._cl_cfg = self._config.get("closeloop", {})
        self._output_dir = Path(
            self._cl_cfg.get("retrain_output_dir", "output/weight_updates")
        )
        self._retrain_day = self._cl_cfg.get("batch_retrain_day", "Sunday")
        self._n_weeks = self._cl_cfg.get("retrain_lookback_weeks", 52)

        self._weight_updater: Optional[object] = None
        self._regime_tracker: Optional[object] = None

        if WeightUpdater is not None:
            try:
                self._weight_updater = WeightUpdater(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("WeightUpdater init failed in BatchRetrainer: %s", exc)

        if RegimeTracker is not None:
            try:
                self._regime_tracker = RegimeTracker(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("RegimeTracker init failed in BatchRetrainer: %s", exc)

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def should_run_today(self) -> bool:
        """Check if today is the configured batch_retrain_day (default Sunday)."""
        today_name = datetime.now(timezone.utc).strftime("%A")
        return today_name.lower() == self._retrain_day.lower()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> Dict:
        """
        1. Pull all closed trades from last 52 weeks
        2. Compute per-signal performance stats
        3. Compute entry timing alpha
        4. Compute peer influence alpha
        5. Compute analyst revision alpha
        6. Portfolio optimisation via WeightUpdater.batch_update()
        7. Smooth: 70% current + 30% optimal
        8. Save full report
        9. Notify of changes > 10%
        Returns {n_trades, signals_updated, report_path, weight_changes}
        """
        result: Dict = {
            "n_trades": 0,
            "signals_updated": 0,
            "report_path": None,
            "weight_changes": {},
        }

        # Step 1: Pull trades
        trades: List[Dict] = []
        try:
            if self._store is not None:
                # 52 weeks * 5 trading days ≈ 260; use a generous 500 to be safe
                n_lookback = self._n_weeks * 5
                trades = self._store.get_trades(n=n_lookback)
                result["n_trades"] = len(trades)
                logger.info("BatchRetrainer: pulled %d trades for retrain", len(trades))
            else:
                logger.warning("BatchRetrainer: no store available")
        except Exception as exc:
            logger.warning("BatchRetrainer step 1 (fetch trades) failed: %s", exc)

        # Step 2: Per-signal stats
        signal_stats: Dict[str, Dict] = {}
        try:
            signal_stats = self._compute_signal_stats(trades)
        except Exception as exc:
            logger.warning("BatchRetrainer step 2 (_compute_signal_stats) failed: %s", exc)

        # Step 3: Entry timing alpha
        entry_timing_alpha: float = 0.0
        try:
            entry_timing_alpha = self._compute_entry_timing_alpha()
        except Exception as exc:
            logger.warning("BatchRetrainer step 3 (entry timing alpha) failed: %s", exc)

        # Step 4: Peer influence alpha
        peer_alpha: Dict = {}
        try:
            peer_alpha = self._compute_peer_alpha()
        except Exception as exc:
            logger.warning("BatchRetrainer step 4 (peer influence alpha) failed: %s", exc)

        # Step 5: Analyst revision alpha
        analyst_alpha: Dict = {}
        try:
            analyst_alpha = self._compute_analyst_alpha()
        except Exception as exc:
            logger.warning("BatchRetrainer step 5 (analyst revision alpha) failed: %s", exc)

        # Step 6 & 7: Portfolio optimisation + smoothing via WeightUpdater
        weight_changes: Dict = {}
        try:
            if self._weight_updater is not None and trades:
                weight_changes = self._weight_updater.batch_update(trades)
                result["signals_updated"] = len(weight_changes)
            else:
                logger.warning("BatchRetrainer: WeightUpdater unavailable or no trades")
        except Exception as exc:
            logger.warning("BatchRetrainer step 6/7 (batch_update) failed: %s", exc)

        result["weight_changes"] = weight_changes

        # Step 8: Save report
        try:
            report_text = self._generate_report(
                signal_stats, weight_changes,
                entry_timing_alpha=entry_timing_alpha,
                peer_alpha=peer_alpha,
                analyst_alpha=analyst_alpha,
                n_trades=result["n_trades"],
            )
            report_path = self._save_report(report_text)
            result["report_path"] = report_path
        except Exception as exc:
            logger.warning("BatchRetrainer step 8 (report) failed: %s", exc)

        # Step 9: Notify significant changes (> 10%)
        try:
            if self._store is not None:
                current_weights = self._store.get_all_weights()
                for sig, new_w in weight_changes.items():
                    old_w = current_weights.get(sig, 1.0)
                    change_pct = abs(new_w - old_w) / max(abs(old_w), 1e-9) * 100
                    if change_pct > 10.0:
                        logger.warning(
                            "BatchRetrainer NOTIFY: %s weight changed %.4f -> %.4f (%.1f%%)",
                            sig, old_w, new_w, change_pct,
                        )
        except Exception as exc:
            logger.warning("BatchRetrainer step 9 (notifications) failed: %s", exc)

        logger.info("BatchRetrainer complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Signal stats
    # ------------------------------------------------------------------

    def _compute_signal_stats(self, trades: List[Dict]) -> Dict[str, Dict]:
        """Per-signal: n, sharpe, win_rate, mean_pnl, best_regime, worst_regime."""
        import json

        # Collect per-signal pnl series, indexed by regime
        signal_pnl: Dict[str, List[float]] = {}
        signal_wins: Dict[str, int] = {}
        signal_regime_pnl: Dict[str, Dict[str, List[float]]] = {}

        for trade in trades:
            try:
                signals_raw = trade.get("signals_at_entry") or "{}"
                if isinstance(signals_raw, str):
                    signals = json.loads(signals_raw)
                else:
                    signals = signals_raw
                net_pnl = float(trade.get("net_pnl") or 0.0)
                regime = trade.get("macro_regime") or "UNKNOWN"
                was_profitable = bool(trade.get("was_profitable"))

                for sig in signals:
                    if not isinstance(sig, str):
                        continue
                    if sig not in signal_pnl:
                        signal_pnl[sig] = []
                        signal_wins[sig] = 0
                        signal_regime_pnl[sig] = {}
                    signal_pnl[sig].append(net_pnl)
                    if was_profitable:
                        signal_wins[sig] += 1
                    if regime not in signal_regime_pnl[sig]:
                        signal_regime_pnl[sig][regime] = []
                    signal_regime_pnl[sig][regime].append(net_pnl)

            except Exception:
                continue

        stats: Dict[str, Dict] = {}
        for sig, pnl_list in signal_pnl.items():
            n = len(pnl_list)
            if n == 0:
                continue
            mean_pnl = sum(pnl_list) / n
            win_rate = signal_wins[sig] / n
            sharpe = _sharpe(pnl_list)

            # Best and worst regime by mean pnl
            regime_means = {
                r: sum(v) / len(v)
                for r, v in signal_regime_pnl[sig].items()
                if v
            }
            best_regime = max(regime_means, key=lambda r: regime_means[r]) if regime_means else "N/A"
            worst_regime = min(regime_means, key=lambda r: regime_means[r]) if regime_means else "N/A"

            stats[sig] = {
                "n": n,
                "mean_pnl": mean_pnl,
                "win_rate": win_rate,
                "sharpe": sharpe,
                "best_regime": best_regime,
                "worst_regime": worst_regime,
            }

        return stats

    # ------------------------------------------------------------------
    # Alpha computations
    # ------------------------------------------------------------------

    def _compute_entry_timing_alpha(self) -> float:
        """Average pnl_vs_immediate_entry from entry_timing_outcomes."""
        if self._store is None:
            return 0.0
        try:
            rows = self._store._conn().execute(
                "SELECT AVG(pnl_vs_immediate_entry) as avg FROM entry_timing_outcomes "
                "WHERE pnl_vs_immediate_entry IS NOT NULL"
            ).fetchone()
            return float(rows["avg"] or 0.0)
        except Exception as exc:
            logger.warning("_compute_entry_timing_alpha failed: %s", exc)
            return 0.0

    def _compute_peer_alpha(self) -> Dict:
        """Peer influence accuracy and mean PnL from peer_influence_outcomes."""
        if self._store is None:
            return {}
        try:
            rows = self._store._conn().execute("""
                SELECT COUNT(*) as n,
                       AVG(CASE WHEN was_correct=1 THEN 1.0 ELSE 0.0 END) as accuracy,
                       AVG(pnl) as mean_pnl
                FROM peer_influence_outcomes
            """).fetchone()
            return {
                "n": rows["n"] or 0,
                "accuracy": rows["accuracy"] or 0.0,
                "mean_pnl": rows["mean_pnl"] or 0.0,
            }
        except Exception as exc:
            logger.warning("_compute_peer_alpha failed: %s", exc)
            return {}

    def _compute_analyst_alpha(self) -> Dict:
        """Analyst revision performance from analyst_revision_outcomes."""
        if self._store is None:
            return {}
        try:
            rows = self._store._conn().execute("""
                SELECT COUNT(*) as n,
                       AVG(forward_return_5d) as avg_5d,
                       AVG(forward_return_20d) as avg_20d,
                       AVG(pnl_if_traded) as avg_pnl,
                       AVG(CASE WHEN pead_improved=1 THEN 1.0 ELSE 0.0 END) as pead_improvement_rate
                FROM analyst_revision_outcomes
            """).fetchone()
            return {
                "n": rows["n"] or 0,
                "avg_fwd_5d": rows["avg_5d"] or 0.0,
                "avg_fwd_20d": rows["avg_20d"] or 0.0,
                "avg_pnl": rows["avg_pnl"] or 0.0,
                "pead_improvement_rate": rows["pead_improvement_rate"] or 0.0,
            }
        except Exception as exc:
            logger.warning("_compute_analyst_alpha failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    def _generate_report(
        self,
        stats: Dict[str, Dict],
        weight_changes: Dict,
        entry_timing_alpha: float = 0.0,
        peer_alpha: Optional[Dict] = None,
        analyst_alpha: Optional[Dict] = None,
        n_trades: int = 0,
    ) -> str:
        """Full text report of the retrain run."""
        peer_alpha = peer_alpha or {}
        analyst_alpha = analyst_alpha or {}
        ts = datetime.now(timezone.utc).isoformat()

        lines = [
            "=" * 80,
            f"BATCH RETRAIN REPORT — {ts}",
            f"Trades analysed: {n_trades}",
            "=" * 80,
            "",
            "SIGNAL PERFORMANCE TABLE",
            "-" * 80,
            f"{'Signal':<35} {'N':>5} {'Win%':>7} {'MeanPnL':>10} {'Sharpe':>8} {'BestReg':<16} {'WorstReg':<16}",
            "-" * 80,
        ]

        for sig, s in sorted(stats.items(), key=lambda x: x[1].get("sharpe", 0), reverse=True):
            lines.append(
                f"{sig:<35} {s['n']:>5} {s['win_rate']*100:>6.1f}% "
                f"{s['mean_pnl']:>10.2f} {s['sharpe']:>8.3f} "
                f"{s['best_regime']:<16} {s['worst_regime']:<16}"
            )

        lines += [
            "",
            "ENTRY TIMING ALPHA",
            "-" * 80,
            f"  Average PnL vs immediate entry: {entry_timing_alpha:.4f}",
            f"  Interpretation: {'POSITIVE — entry timing adds value' if entry_timing_alpha > 0 else 'NEGATIVE — entry timing hurts'}",
            "",
            "PEER INFLUENCE PERFORMANCE",
            "-" * 80,
        ]
        if peer_alpha.get("n", 0) > 0:
            lines += [
                f"  N signals:   {peer_alpha['n']}",
                f"  Accuracy:    {peer_alpha['accuracy']*100:.1f}%",
                f"  Mean PnL:    {peer_alpha['mean_pnl']:.4f}",
            ]
        else:
            lines.append("  No peer influence outcomes recorded.")

        lines += [
            "",
            "ANALYST REVISION ALPHA",
            "-" * 80,
        ]
        if analyst_alpha.get("n", 0) > 0:
            lines += [
                f"  N records:              {analyst_alpha['n']}",
                f"  Avg 5d forward return:  {analyst_alpha['avg_fwd_5d']:.4f}",
                f"  Avg 20d forward return: {analyst_alpha['avg_fwd_20d']:.4f}",
                f"  Avg PnL if traded:      {analyst_alpha['avg_pnl']:.4f}",
                f"  PEAD improvement rate:  {analyst_alpha['pead_improvement_rate']*100:.1f}%",
            ]
        else:
            lines.append("  No analyst revision outcomes recorded.")

        lines += [
            "",
            "WEIGHT CHANGES",
            "-" * 80,
            f"{'Signal':<35} {'New Weight':>12} {'Note':<30}",
            "-" * 80,
        ]
        if weight_changes:
            for sig, new_w in sorted(weight_changes.items()):
                note = "Updated" if new_w != 1.0 else "Unchanged"
                lines.append(f"{sig:<35} {new_w:>12.4f} {note:<30}")
        else:
            lines.append("  No weight changes this run.")

        # Next retrain date (next Sunday or configured day)
        lines += [
            "",
            f"Next retrain: next {self._retrain_day}",
            "=" * 80,
            "",
        ]

        return "\n".join(lines)

    def _save_report(self, report_text: str) -> str:
        """Save report to output/weight_updates/ and return the path."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("Could not create output directory %s: %s", self._output_dir, exc)
        report_path = self._output_dir / f"retrain_{ts}.txt"
        try:
            report_path.write_text(report_text, encoding="utf-8")
            logger.info("BatchRetrainer report saved to %s", report_path)
        except Exception as exc:
            logger.warning("Failed to save retrain report: %s", exc)
        return str(report_path)
