"""
EntryLearner — learns optimal entry timing from actual trade outcomes.
EntryAlpha = ActualEntryPnL - ImmediateEntryPnL

Segments by signal_type, market, macro_regime, sector, entry_condition.
Adjusts extension_threshold_atr, pullback_threshold_pct, confirmation_days
based on observed entry alpha. All changes logged with statistical justification.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class EntryLearner:
    """
    Tracks entry timing outcomes and learns which entry conditions add value
    in which market segments. Updates entry parameters only when statistically
    justified (n >= min_entries).
    """

    def __init__(self, store=None, config=None):
        cfg = (config or {}).get("closeloop", {}).get("entry", {})
        self.min_entries = cfg.get("min_entries_before_learning", 20)
        self._store = store

    def record_outcome(self, trade_id: int, closed_trade: dict,
                       entry_context: dict) -> None:
        """
        Compute EntryAlpha = actual_entry_pnl - immediate_entry_pnl
        (immediate_entry_pnl stored in entry_context['immediate_entry_price'])
        Log to store.entry_timing_outcomes.
        Trigger parameter updates if n_entries >= min_entries.
        """
        try:
            actual_entry_price = closed_trade.get("entry_price")
            exit_price = closed_trade.get("exit_price")
            direction = closed_trade.get("direction", 1)
            immediate_entry_price = entry_context.get("immediate_entry_price")

            if actual_entry_price is None or exit_price is None:
                logger.warning(
                    f"record_outcome(trade_id={trade_id}): missing entry_price or exit_price"
                )
                return

            actual_entry_pnl = (exit_price - actual_entry_price) * direction
            if immediate_entry_price is not None and immediate_entry_price > 0:
                immediate_entry_pnl = (exit_price - immediate_entry_price) * direction
                entry_alpha = actual_entry_pnl - immediate_entry_pnl
            else:
                immediate_entry_pnl = None
                entry_alpha = 0.0
                logger.debug(
                    f"record_outcome(trade_id={trade_id}): no immediate_entry_price — entry_alpha set to 0"
                )

            outcome = {
                "trade_id": trade_id,
                "ticker": closed_trade.get("ticker"),
                "signal_type": entry_context.get("signal_type", "unknown"),
                "market": entry_context.get("market", "us"),
                "sector": entry_context.get("sector", "unknown"),
                "macro_regime": entry_context.get("macro_regime", "unknown"),
                "entry_condition": entry_context.get("entry_method", "unknown"),
                "entry_timing_score": entry_context.get("entry_timing_score"),
                "actual_entry_price": actual_entry_price,
                "immediate_entry_price": immediate_entry_price,
                "exit_price": exit_price,
                "actual_entry_pnl": actual_entry_pnl,
                "immediate_entry_pnl": immediate_entry_pnl,
                "entry_alpha": entry_alpha,
                "direction": direction,
            }

            if self._store is not None:
                try:
                    self._store.log_entry_timing_outcome(outcome)
                    logger.info(
                        f"record_outcome(trade_id={trade_id}): entry_alpha={entry_alpha:.5f} logged"
                    )
                except Exception as e:
                    logger.warning(f"record_outcome(trade_id={trade_id}): store log failed: {e}")
            else:
                logger.info(
                    f"record_outcome(trade_id={trade_id}): entry_alpha={entry_alpha:.5f} "
                    f"(no store — not persisted)"
                )

            # Trigger parameter update check
            self._maybe_update_parameters(outcome)

        except Exception as e:
            logger.warning(f"record_outcome(trade_id={trade_id}): unexpected error: {e}")

    def _maybe_update_parameters(self, latest_outcome: dict) -> None:
        """Internal: check if we have enough data and trigger update if so."""
        try:
            if self._store is None:
                return
            n = self._store.count_entry_timing_outcomes() if hasattr(self._store, "count_entry_timing_outcomes") else 0
            if n >= self.min_entries:
                logger.info(
                    f"_maybe_update_parameters: n={n} >= min_entries={self.min_entries} — triggering update"
                )
                self.update_parameters()
        except Exception as e:
            logger.debug(f"_maybe_update_parameters: {e}")

    def compute_entry_alpha_stats(self, segment_by: str = "signal_type") -> Dict:
        """
        Segment by: signal_type, market, macro_regime, sector, entry_condition
        Returns per-segment: mean_entry_alpha, win_rate, n_trades
        Rules derived:
          mean_entry_alpha > 0.004: timing adds value in this segment
          mean_entry_alpha < -0.002: timing costs money (return immediate entry)
        """
        try:
            if self._store is None:
                logger.warning("compute_entry_alpha_stats: no store available")
                return {}

            try:
                outcomes = self._store.get_entry_timing_outcomes()
            except Exception as e:
                logger.warning(f"compute_entry_alpha_stats: store fetch failed: {e}")
                return {}

            if not outcomes:
                logger.info("compute_entry_alpha_stats: no outcomes to analyse")
                return {}

            valid_segments = {
                "signal_type", "market", "macro_regime", "sector", "entry_condition"
            }
            if segment_by not in valid_segments:
                logger.warning(
                    f"compute_entry_alpha_stats: unknown segment '{segment_by}', defaulting to 'signal_type'"
                )
                segment_by = "signal_type"

            from collections import defaultdict
            buckets: Dict[str, List[float]] = defaultdict(list)

            for o in outcomes:
                key = o.get(segment_by, "unknown")
                ea = o.get("entry_alpha")
                if ea is not None:
                    buckets[key].append(float(ea))

            result = {}
            for segment_key, alphas in buckets.items():
                n = len(alphas)
                mean_ea = sum(alphas) / n
                wins = sum(1 for a in alphas if a > 0)
                win_rate = wins / n

                if mean_ea > 0.004:
                    verdict = "ADDS_VALUE"
                elif mean_ea < -0.002:
                    verdict = "COSTS_MONEY"
                else:
                    verdict = "NEUTRAL"

                result[segment_key] = {
                    "mean_entry_alpha": mean_ea,
                    "win_rate": win_rate,
                    "n_trades": n,
                    "verdict": verdict,
                }

                logger.debug(
                    f"compute_entry_alpha_stats[{segment_by}={segment_key}]: "
                    f"mean_ea={mean_ea:.5f}, win_rate={win_rate:.3f}, n={n}, verdict={verdict}"
                )

            return result

        except Exception as e:
            logger.warning(f"compute_entry_alpha_stats: unexpected error: {e}")
            return {}

    def update_parameters(self, config_path: str = "config/settings.yaml") -> Dict:
        """
        Only runs if n_entries >= min_entries.
        Adjust:
          extension_threshold_atr: toward value that maximised entry_alpha
          pullback_threshold_pct: calibrated from best observed entries
          confirmation_days: shorten if market moving fast (high VIX), extend if slow
        All changes logged with statistical justification.
        Returns dict of {param: {old, new, reason, n_basis}}
        Never adjusts without logging.
        """
        try:
            if self._store is None:
                logger.warning("update_parameters: no store available")
                return {}

            # Count outcomes
            try:
                n = self._store.count_entry_timing_outcomes() if hasattr(self._store, "count_entry_timing_outcomes") else 0
            except Exception as e:
                logger.warning(f"update_parameters: count failed: {e}")
                return {}

            if n < self.min_entries:
                logger.info(
                    f"update_parameters: only {n} entries, need {self.min_entries} — skipping"
                )
                return {}

            try:
                outcomes = self._store.get_entry_timing_outcomes()
            except Exception as e:
                logger.warning(f"update_parameters: store fetch failed: {e}")
                return {}

            if not outcomes:
                return {}

            changes = {}

            # --- extension_threshold_atr ---
            try:
                current_ext = self._store.get_config_param("extension_threshold_atr", 2.0)
                # Find entry_timing_score bucket that produced highest mean entry_alpha
                score_buckets: Dict[str, List[float]] = {}
                for o in outcomes:
                    score = o.get("entry_timing_score")
                    ea = o.get("entry_alpha")
                    if score is not None and ea is not None:
                        bucket = "high" if score >= 0.7 else ("mid" if score >= 0.4 else "low")
                        score_buckets.setdefault(bucket, []).append(float(ea))

                if score_buckets:
                    best_bucket = max(score_buckets, key=lambda k: sum(score_buckets[k]) / len(score_buckets[k]))
                    best_mean_ea = sum(score_buckets[best_bucket]) / len(score_buckets[best_bucket])
                    # If high-score entries do better, tighten threshold (require more confirmation)
                    if best_bucket == "high" and best_mean_ea > 0.004:
                        new_ext = round(current_ext * 0.95, 2)
                    elif best_bucket == "low" and best_mean_ea > 0.002:
                        new_ext = round(current_ext * 1.05, 2)
                    else:
                        new_ext = current_ext

                    if new_ext != current_ext:
                        changes["extension_threshold_atr"] = {
                            "old": current_ext,
                            "new": new_ext,
                            "reason": f"Best entry alpha in '{best_bucket}' score bucket (mean={best_mean_ea:.5f})",
                            "n_basis": n,
                        }
                        logger.info(
                            f"update_parameters: extension_threshold_atr {current_ext} -> {new_ext} "
                            f"(best_bucket={best_bucket}, mean_ea={best_mean_ea:.5f}, n={n})"
                        )
            except Exception as e:
                logger.warning(f"update_parameters: extension_threshold_atr calc failed: {e}")

            # --- pullback_threshold_pct ---
            try:
                current_pull = self._store.get_config_param("pullback_threshold_pct", 0.02)
                pullback_alphas = [
                    float(o["entry_alpha"]) for o in outcomes
                    if o.get("entry_condition") == "pullback_wait" and o.get("entry_alpha") is not None
                ]
                immediate_alphas = [
                    float(o["entry_alpha"]) for o in outcomes
                    if o.get("entry_condition") == "immediate" and o.get("entry_alpha") is not None
                ]

                if pullback_alphas and immediate_alphas:
                    mean_pb = sum(pullback_alphas) / len(pullback_alphas)
                    mean_im = sum(immediate_alphas) / len(immediate_alphas)
                    if mean_pb > mean_im + 0.002:
                        # Pullback strategy working — slightly increase threshold
                        new_pull = round(min(0.05, current_pull * 1.1), 4)
                    elif mean_im > mean_pb + 0.002:
                        # Immediate is better — reduce pullback wait
                        new_pull = round(max(0.005, current_pull * 0.9), 4)
                    else:
                        new_pull = current_pull

                    if new_pull != current_pull:
                        changes["pullback_threshold_pct"] = {
                            "old": current_pull,
                            "new": new_pull,
                            "reason": (
                                f"Pullback mean_ea={mean_pb:.5f} vs immediate mean_ea={mean_im:.5f}"
                            ),
                            "n_basis": len(pullback_alphas) + len(immediate_alphas),
                        }
                        logger.info(
                            f"update_parameters: pullback_threshold_pct {current_pull} -> {new_pull}"
                        )
            except Exception as e:
                logger.warning(f"update_parameters: pullback_threshold_pct calc failed: {e}")

            # --- confirmation_days ---
            try:
                current_conf = self._store.get_config_param("confirmation_days", 3)
                # Estimate market speed from VIX proxy: std of recent entry_alphas
                all_alphas = [
                    float(o["entry_alpha"]) for o in outcomes
                    if o.get("entry_alpha") is not None
                ]
                if len(all_alphas) >= 10:
                    mean_a = sum(all_alphas) / len(all_alphas)
                    variance = sum((a - mean_a) ** 2 for a in all_alphas) / len(all_alphas)
                    vol = variance ** 0.5
                    # High volatility = fast market = fewer confirmation days
                    if vol > 0.015 and current_conf > 2:
                        new_conf = current_conf - 1
                        reason = f"High market volatility (alpha_std={vol:.4f}) — faster confirmation"
                    elif vol < 0.005 and current_conf < 5:
                        new_conf = current_conf + 1
                        reason = f"Low market volatility (alpha_std={vol:.4f}) — slower confirmation"
                    else:
                        new_conf = current_conf
                        reason = ""

                    if new_conf != current_conf:
                        changes["confirmation_days"] = {
                            "old": current_conf,
                            "new": new_conf,
                            "reason": reason,
                            "n_basis": len(all_alphas),
                        }
                        logger.info(
                            f"update_parameters: confirmation_days {current_conf} -> {new_conf} ({reason})"
                        )
            except Exception as e:
                logger.warning(f"update_parameters: confirmation_days calc failed: {e}")

            # Persist changes to config
            if changes:
                try:
                    self._persist_parameter_changes(config_path, changes)
                except Exception as e:
                    logger.warning(f"update_parameters: persist failed: {e}")

            if not changes:
                logger.info(f"update_parameters: no parameter changes warranted (n={n})")

            return changes

        except Exception as e:
            logger.warning(f"update_parameters: unexpected error: {e}")
            return {}

    def _persist_parameter_changes(self, config_path: str, changes: Dict) -> None:
        """Write parameter changes to YAML config file."""
        try:
            import yaml  # type: ignore
            import os

            if not os.path.exists(config_path):
                logger.warning(f"_persist_parameter_changes: config not found at {config_path}")
                return

            with open(config_path, "r") as f:
                cfg = yaml.safe_load(f) or {}

            entry_cfg = cfg.setdefault("closeloop", {}).setdefault("entry", {})

            for param, change in changes.items():
                entry_cfg[param] = change["new"]
                logger.info(
                    f"_persist_parameter_changes: {param} updated to {change['new']} "
                    f"(was {change['old']}, reason: {change['reason']}, n={change['n_basis']})"
                )

            with open(config_path, "w") as f:
                yaml.dump(cfg, f, default_flow_style=False)

        except ImportError:
            logger.warning("_persist_parameter_changes: PyYAML not available — changes not persisted to file")
        except Exception as e:
            logger.warning(f"_persist_parameter_changes: {e}")

    def render_entry_performance(self) -> str:
        """
        Text table: segment | mean_entry_alpha | win_rate | n_trades | verdict
        Verdict: ADDS_VALUE / NEUTRAL / COSTS_MONEY
        """
        try:
            lines = []
            lines.append("=" * 75)
            lines.append("ENTRY TIMING PERFORMANCE")
            lines.append("=" * 75)

            all_stats = {}
            for segment in ["signal_type", "market", "sector", "macro_regime", "entry_condition"]:
                stats = self.compute_entry_alpha_stats(segment_by=segment)
                if stats:
                    all_stats[segment] = stats

            if not all_stats:
                lines.append("No entry timing data available yet.")
                lines.append("=" * 75)
                return "\n".join(lines)

            header = f"{'Segment':<22} {'Category':<20} {'Mean Alpha':>12} {'Win Rate':>10} {'N':>6} {'Verdict':<14}"
            lines.append(header)
            lines.append("-" * 75)

            for segment, stats in all_stats.items():
                for key, s in sorted(stats.items()):
                    verdict = s.get("verdict", "NEUTRAL")
                    mean_ea = s.get("mean_entry_alpha", 0.0)
                    win_rate = s.get("win_rate", 0.0)
                    n = s.get("n_trades", 0)

                    verdict_display = {
                        "ADDS_VALUE": "ADDS_VALUE",
                        "NEUTRAL": "NEUTRAL",
                        "COSTS_MONEY": "COSTS_MONEY",
                    }.get(verdict, verdict)

                    lines.append(
                        f"{segment:<22} {str(key):<20} {mean_ea:>+12.5f} {win_rate:>9.1%} {n:>6d} {verdict_display:<14}"
                    )
                lines.append("")

            lines.append("=" * 75)
            lines.append("Verdicts: ADDS_VALUE=mean_alpha>0.4% | NEUTRAL | COSTS_MONEY=mean_alpha<-0.2%")
            lines.append("=" * 75)

            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"render_entry_performance: unexpected error: {e}")
            return f"EntryLearner render error: {e}"
