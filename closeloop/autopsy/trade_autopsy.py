"""
Runs automatically every time a position closes. Captures all context, attributes P&L,
and triggers all downstream learning.
"""
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable — closeloop.storage not importable")

try:
    from closeloop.autopsy.pnl_attributor import PnLAttributor
except ImportError:
    PnLAttributor = None  # type: ignore
    logger.warning("PnLAttributor not importable")

try:
    from closeloop.learning.regime_tracker import RegimeTracker
except ImportError:
    RegimeTracker = None  # type: ignore
    logger.warning("RegimeTracker not importable")

try:
    from closeloop.autopsy.signal_interaction_ledger import SignalInteractionLedger
except ImportError:
    SignalInteractionLedger = None  # type: ignore
    logger.warning("SignalInteractionLedger not importable")

try:
    from closeloop.learning.weight_updater import WeightUpdater
except ImportError:
    WeightUpdater = None  # type: ignore
    logger.warning("WeightUpdater not importable")

try:
    from closeloop.autopsy.drawdown_forensics import DrawdownForensics
except ImportError:
    DrawdownForensics = None  # type: ignore
    logger.warning("DrawdownForensics not importable")

try:
    from closeloop.risk.benchmark_tracker import BenchmarkTracker
except ImportError:
    BenchmarkTracker = None  # type: ignore
    logger.warning("BenchmarkTracker not importable")

try:
    from closeloop.risk.tax_manager import TaxManager
except ImportError:
    TaxManager = None  # type: ignore
    logger.warning("TaxManager not importable")

try:
    from closeloop.entry.entry_learner import EntryLearner
except ImportError:
    EntryLearner = None  # type: ignore
    logger.warning("EntryLearner not importable")

try:
    from closeloop.stress.stress_learner import StressLearner
except ImportError:
    StressLearner = None  # type: ignore
    logger.warning("StressLearner not importable")


@dataclass
class AutopsyReport:
    trade_id: int
    ticker: str
    net_pnl: float
    pnl_pct: float
    was_profitable: bool
    macro_regime: str
    attribution: List[Dict] = field(default_factory=list)
    entry_alpha: float = 0.0
    weight_changes: List[Dict] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


class TradeAutopsy:
    """Orchestrates all post-trade analysis steps. One failing step never stops the others."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = config or {}

        if self._store is None:
            logger.warning(
                "TradeAutopsy initialised without a ClosedLoopStore — "
                "persistence steps will be skipped."
            )

        self._attributor: Optional[object] = None
        self._regime_tracker: Optional[object] = None
        self._interaction_ledger: Optional[object] = None
        self._weight_updater: Optional[object] = None
        self._drawdown_forensics: Optional[object] = None
        self._benchmark_tracker: Optional[object] = None
        self._tax_manager: Optional[object] = None
        self._entry_learner: Optional[object] = None
        self._stress_learner: Optional[object] = None

        if PnLAttributor is not None:
            try:
                self._attributor = PnLAttributor(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("PnLAttributor init failed: %s", exc)

        if RegimeTracker is not None:
            try:
                self._regime_tracker = RegimeTracker(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("RegimeTracker init failed: %s", exc)

        if SignalInteractionLedger is not None:
            try:
                self._interaction_ledger = SignalInteractionLedger(
                    store=self._store, config=self._config
                )
            except Exception as exc:
                logger.warning("SignalInteractionLedger init failed: %s", exc)

        if WeightUpdater is not None:
            try:
                self._weight_updater = WeightUpdater(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("WeightUpdater init failed: %s", exc)

        if DrawdownForensics is not None:
            try:
                self._drawdown_forensics = DrawdownForensics(
                    store=self._store, config=self._config
                )
            except Exception as exc:
                logger.warning("DrawdownForensics init failed: %s", exc)

        if BenchmarkTracker is not None:
            try:
                self._benchmark_tracker = BenchmarkTracker(
                    store=self._store, config=self._config
                )
            except Exception as exc:
                logger.warning("BenchmarkTracker init failed: %s", exc)

        if TaxManager is not None:
            try:
                self._tax_manager = TaxManager(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("TaxManager init failed: %s", exc)

        if EntryLearner is not None:
            try:
                self._entry_learner = EntryLearner(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("EntryLearner init failed: %s", exc)

        if StressLearner is not None:
            try:
                self._stress_learner = StressLearner(store=self._store, config=self._config)
            except Exception as exc:
                logger.warning("StressLearner init failed: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, closed_trade: dict, entry_context: dict) -> AutopsyReport:
        """
        1. record_trade in store
        2. run PnLAttributor.attribute()
        3. update RegimeTracker
        4. update SignalInteractionLedger
        5. soft_update WeightUpdater
        6. check DrawdownForensics
        7. update BenchmarkTracker
        8. record TaxManager disposal
        9. record EntryLearner outcome
        10. update StressLearner signal vulnerability
        11. record peer influence outcome if merger_spillover_flag or peer_influence_score > 0.3
        12. log autopsy complete

        Each step is wrapped in try/except — one failing must not stop others.
        """
        ticker = closed_trade.get("ticker", "UNKNOWN")
        net_pnl = closed_trade.get("net_pnl", 0.0)
        entry_price = closed_trade.get("entry_price", 1.0) or 1.0
        position_size = closed_trade.get("position_size", 1.0) or 1.0
        pnl_pct = net_pnl / (entry_price * position_size)
        was_profitable = net_pnl > 0
        macro_regime = entry_context.get("macro_regime", "UNKNOWN")

        report = AutopsyReport(
            trade_id=-1,
            ticker=ticker,
            net_pnl=net_pnl,
            pnl_pct=pnl_pct,
            was_profitable=was_profitable,
            macro_regime=macro_regime,
        )

        # ---- Step 1: Record trade ------------------------------------------
        trade_id = -1
        try:
            if self._store is not None:
                trade_id = self._store.record_trade(closed_trade, entry_context)
                report.trade_id = trade_id
                report.notes.append(f"Trade recorded with id={trade_id}")
            else:
                report.notes.append("store=None: trade not persisted")
        except Exception as exc:
            logger.warning("Step 1 record_trade failed for %s: %s", ticker, exc)
            report.notes.append(f"Step 1 failed: {exc}")

        # ---- Step 2: PnL attribution ----------------------------------------
        attribution: List[Dict] = []
        try:
            if self._attributor is not None and trade_id >= 0:
                attribution = self._attributor.attribute(
                    trade_id, closed_trade, entry_context
                )
                report.attribution = attribution
                report.notes.append(f"Attribution: {len(attribution)} signals attributed")
        except Exception as exc:
            logger.warning("Step 2 PnLAttributor.attribute failed for trade %s: %s", trade_id, exc)
            report.notes.append(f"Step 2 failed: {exc}")

        # ---- Step 3: RegimeTracker ------------------------------------------
        try:
            if self._regime_tracker is not None:
                self._regime_tracker.update(macro_regime, attribution)
                report.notes.append("RegimeTracker updated")
        except Exception as exc:
            logger.warning("Step 3 RegimeTracker.update failed: %s", exc)
            report.notes.append(f"Step 3 failed: {exc}")

        # ---- Step 4: SignalInteractionLedger -----------------------------------
        try:
            if self._interaction_ledger is not None:
                active_signals = list(entry_context.get("active_signals", []))
                if active_signals:
                    self._interaction_ledger.update(active_signals, closed_trade)
                    report.notes.append("SignalInteractionLedger updated")
        except Exception as exc:
            logger.warning("Step 4 SignalInteractionLedger.update failed: %s", exc)
            report.notes.append(f"Step 4 failed: {exc}")

        # ---- Step 5: WeightUpdater (soft update) ------------------------------
        try:
            if self._weight_updater is not None and attribution:
                weight_changes = self._weight_updater.soft_update(attribution, entry_context)
                report.weight_changes = weight_changes
                report.notes.append(f"WeightUpdater soft_update: {len(weight_changes)} changes")
        except Exception as exc:
            logger.warning("Step 5 WeightUpdater.soft_update failed: %s", exc)
            report.notes.append(f"Step 5 failed: {exc}")

        # ---- Step 6: DrawdownForensics -----------------------------------------
        try:
            if self._drawdown_forensics is not None:
                self._drawdown_forensics.check_and_record(closed_trade)
                report.notes.append("DrawdownForensics checked")
        except Exception as exc:
            logger.warning("Step 6 DrawdownForensics.check_and_record failed: %s", exc)
            report.notes.append(f"Step 6 failed: {exc}")

        # ---- Step 7: BenchmarkTracker -----------------------------------------
        try:
            if self._benchmark_tracker is not None:
                self._benchmark_tracker.update(closed_trade, entry_context)
                report.notes.append("BenchmarkTracker updated")
        except Exception as exc:
            logger.warning("Step 7 BenchmarkTracker.update failed: %s", exc)
            report.notes.append(f"Step 7 failed: {exc}")

        # ---- Step 8: TaxManager (disposal) ------------------------------------
        try:
            if self._tax_manager is not None and trade_id >= 0:
                self._tax_manager.record_disposal(trade_id, closed_trade)
                report.notes.append("TaxManager disposal recorded")
        except Exception as exc:
            logger.warning("Step 8 TaxManager.record_disposal failed: %s", exc)
            report.notes.append(f"Step 8 failed: {exc}")

        # ---- Step 9: EntryLearner outcome -------------------------------------
        try:
            if self._entry_learner is not None and trade_id >= 0:
                entry_alpha = self._entry_learner.record_outcome(
                    trade_id, closed_trade, entry_context
                )
                if entry_alpha is not None:
                    report.entry_alpha = float(entry_alpha)
                report.notes.append(f"EntryLearner outcome recorded; entry_alpha={report.entry_alpha:.4f}")
        except Exception as exc:
            logger.warning("Step 9 EntryLearner.record_outcome failed: %s", exc)
            report.notes.append(f"Step 9 failed: {exc}")

        # ---- Step 10: StressLearner signal vulnerability ----------------------
        try:
            if self._stress_learner is not None:
                self._stress_learner.update_signal_vulnerability(closed_trade, entry_context)
                report.notes.append("StressLearner vulnerability updated")
        except Exception as exc:
            logger.warning("Step 10 StressLearner.update_signal_vulnerability failed: %s", exc)
            report.notes.append(f"Step 10 failed: {exc}")

        # ---- Step 11: Peer influence outcome ----------------------------------
        try:
            peer_score = entry_context.get("peer_influence_score", 0.0) or 0.0
            merger_flag = bool(entry_context.get("merger_spillover_flag", False))
            if (merger_flag or peer_score > 0.3) and self._store is not None and trade_id >= 0:
                influence_outcome = {
                    "trigger_ticker": entry_context.get("peer_trigger_ticker", ""),
                    "trigger_event": entry_context.get("peer_trigger_event", ""),
                    "influenced_ticker": ticker,
                    "influence_type": "merger_spillover" if merger_flag else "peer_influence",
                    "predicted_direction": closed_trade.get("direction", 1),
                    "actual_direction": 1 if net_pnl > 0 else -1,
                    "predicted_magnitude": peer_score,
                    "actual_magnitude": abs(pnl_pct),
                    "lag_days": closed_trade.get("holding_days", 0),
                    "was_correct": was_profitable,
                    "pnl": net_pnl,
                }
                self._store.record_peer_influence(influence_outcome)
                report.notes.append(
                    f"Peer influence outcome recorded (score={peer_score:.2f}, merger={merger_flag})"
                )
        except Exception as exc:
            logger.warning("Step 11 peer influence record failed: %s", exc)
            report.notes.append(f"Step 11 failed: {exc}")

        # ---- Step 12: Log autopsy complete ------------------------------------
        try:
            if self._store is not None and trade_id >= 0:
                self._store.log_autopsy_complete(trade_id)
            logger.info(
                "Autopsy complete | trade_id=%s ticker=%s net_pnl=%.2f regime=%s",
                trade_id, ticker, net_pnl, macro_regime,
            )
            report.notes.append("Autopsy complete")
        except Exception as exc:
            logger.warning("Step 12 log_autopsy_complete failed: %s", exc)
            report.notes.append(f"Step 12 failed: {exc}")

        return report
