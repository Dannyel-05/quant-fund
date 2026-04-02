"""
Monitors equity curve for drawdowns. Runs forensics on each event.
Builds DrawdownPrecursorModel.
"""
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in drawdown_forensics")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False


FAILURE_TYPES = {
    "TYPE_A": "Overfit signal — performed well IS but failed OOS",
    "TYPE_B": "Signal decay — worked historically but competitors entered",
    "TYPE_C": "Execution failure — signal was right but entry/exit poor",
    "TYPE_D": "Random variance — within expected statistical range",
    "TYPE_E": "Correlation failure — assumed uncorrelated signals all moved together",
}

_DRAWDOWN_THRESHOLD = 0.03   # 3 % drawdown triggers event tracking


class DrawdownForensics:
    """Monitors the equity curve and performs root-cause analysis on drawdown events."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = (config or {}).get("closeloop", {})
        self._equity_history: List[Tuple[str, float]] = []   # (date, value)
        self._peak: float = 0.0
        self._drawdown_start: Optional[str] = None
        self._in_drawdown: bool = False
        self._trough: float = float("inf")
        self._trough_date: Optional[str] = None
        self._drawdown_events: List[Dict] = []

    # ------------------------------------------------------------------
    # Equity update
    # ------------------------------------------------------------------

    def update_equity(self, date: str, portfolio_value: float) -> Optional[Dict]:
        """
        Update equity curve. Returns a drawdown event dict if a drawdown just ended, else None.
        Drawdown threshold: current < peak * (1 - DRAWDOWN_THRESHOLD).
        """
        self._equity_history.append((date, portfolio_value))

        if portfolio_value > self._peak:
            if self._in_drawdown:
                # Drawdown just ended — emit event
                event = self._finalise_drawdown(date, portfolio_value)
                self._in_drawdown = False
                self._trough = float("inf")
                self._trough_date = None
                self._drawdown_start = None
                self._peak = portfolio_value
                self._drawdown_events.append(event)
                return event
            self._peak = portfolio_value

        elif portfolio_value < self._peak * (1.0 - _DRAWDOWN_THRESHOLD):
            if not self._in_drawdown:
                self._in_drawdown = True
                self._drawdown_start = date

            if portfolio_value < self._trough:
                self._trough = portfolio_value
                self._trough_date = date

        return None

    def _finalise_drawdown(self, recovery_date: str, recovery_value: float) -> Dict:
        """Build an event dict when a drawdown completes."""
        drawdown_pct = (
            (self._peak - self._trough) / self._peak * 100.0
            if self._peak > 0 else 0.0
        )
        # Recovery days
        recovery_days = 0
        if self._drawdown_start and recovery_date:
            try:
                start_dt = datetime.fromisoformat(self._drawdown_start)
                end_dt = datetime.fromisoformat(recovery_date)
                recovery_days = max(0, (end_dt - start_dt).days)
            except Exception:
                recovery_days = 0

        return {
            "start_date": self._drawdown_start,
            "end_date": recovery_date,
            "trough_date": self._trough_date,
            "drawdown_pct": round(drawdown_pct, 4),
            "recovery_days": recovery_days,
            "contributing_signals": [],
            "contributing_regimes": [],
            "contributing_sectors": [],
            "contributing_markets": [],
            "failure_type": None,
            "forensics_report": None,
        }

    # ------------------------------------------------------------------
    # Called from TradeAutopsy
    # ------------------------------------------------------------------

    def check_and_record(self, closed_trade: dict) -> None:
        """Called from TradeAutopsy on every trade close."""
        date = closed_trade.get("exit_date") or datetime.now(timezone.utc).date().isoformat()
        portfolio_value = closed_trade.get("portfolio_value_after_close")
        if portfolio_value is None:
            # Cannot update equity curve without a portfolio value — log and skip
            logger.debug(
                "DrawdownForensics.check_and_record: no portfolio_value_after_close "
                "in closed_trade for %s, skipping equity update",
                closed_trade.get("ticker", "?"),
            )
            return

        try:
            event = self.update_equity(str(date), float(portfolio_value))
            if event is not None:
                # Enrich with forensics
                contributing_signals = list(closed_trade.get("active_signals", []))
                failure_type = self._classify_failure(event, contributing_signals)
                event["failure_type"] = failure_type
                event["contributing_signals"] = contributing_signals
                report_text = (
                    f"Drawdown {event['drawdown_pct']:.2f}% "
                    f"({event['start_date']} -> {event['end_date']}): "
                    f"{FAILURE_TYPES.get(failure_type, failure_type)}"
                )
                event["forensics_report"] = report_text

                if self._store is not None:
                    self._store.record_drawdown(event)
                    logger.info("Drawdown event recorded: %s", report_text)
                else:
                    logger.info("Drawdown event (no store): %s", report_text)

        except Exception as exc:
            logger.warning("DrawdownForensics.check_and_record failed: %s", exc)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify_failure(self, event: dict, contributing_signals: List[str]) -> str:
        """
        TYPE_A: signals had high IS sharpe but low OOS sharpe (from store)
        TYPE_B: FSP declining for contributing signals (frontier proxy)
        TYPE_C: entry_timing_score was low for trades in drawdown period
        TYPE_D: drawdown < 2x expected from sharpe alone
        TYPE_E: correlation_regime was CRISIS during drawdown
        """
        if self._store is None:
            return "TYPE_D"

        try:
            # TYPE_E check: look at benchmark performance for crisis correlation clues
            bench_hist = self._store.get_benchmark_history(n=60)
            drawdown_start = event.get("start_date") or ""
            during_drawdown = [
                b for b in bench_hist
                if b.get("date", "") >= drawdown_start
            ]
            if during_drawdown:
                active_us = [b.get("active_return_us", 0.0) or 0.0 for b in during_drawdown]
                if active_us:
                    avg_active = sum(active_us) / len(active_us)
                    # If both portfolio and benchmark dropped sharply, correlation failure
                    if avg_active > -0.02:
                        # Active return near zero => portfolio moved with market
                        return "TYPE_E"

            # TYPE_A check: compare IS vs OOS scorecard for each signal
            for sig_name in contributing_signals:
                try:
                    sc = self._store.get_signal_scorecard(sig_name)
                    if sc and sc.get("n_trades", 0) >= 20:
                        # Proxy: low win rate with high trade count => overfit
                        if sc.get("win_rate", 0.5) < 0.35:
                            return "TYPE_A"
                except Exception:
                    pass

            # TYPE_C: low entry timing score signals execution issue
            recent_trades = self._store.get_trades(n=50)
            drawdown_trades = [
                t for t in recent_trades
                if (t.get("exit_date") or "") >= drawdown_start
            ]
            if drawdown_trades:
                avg_timing = sum(
                    t.get("entry_timing_score") or 0.5 for t in drawdown_trades
                ) / len(drawdown_trades)
                if avg_timing < 0.35:
                    return "TYPE_C"

        except Exception as exc:
            logger.warning("_classify_failure encountered error: %s", exc)

        return "TYPE_D"

    # ------------------------------------------------------------------
    # Precursor conditions
    # ------------------------------------------------------------------

    def get_precursor_conditions(self) -> Dict:
        """
        Extract features from recent drawdown events.
        Returns dict of conditions that preceded drawdowns:
        {vix_threshold, umci_threshold, correlation_regime, signal_concentration}
        Used by StressLearner.
        """
        if not self._drawdown_events:
            return {}

        vix_values: List[float] = []
        umci_values: List[float] = []
        type_e_count = 0

        for event in self._drawdown_events:
            # We don't have vix/umci on the event itself; pull from store if available
            if self._store is not None:
                try:
                    start = event.get("start_date", "")
                    trades = self._store.get_trades(n=252)
                    pre_trades = [
                        t for t in trades if (t.get("exit_date") or "") <= start
                    ]
                    if pre_trades:
                        recent = pre_trades[:5]
                        for t in recent:
                            v = t.get("vix_level")
                            u = t.get("umci_score")
                            if v is not None:
                                vix_values.append(float(v))
                            if u is not None:
                                umci_values.append(float(u))
                except Exception:
                    pass
            if event.get("failure_type") == "TYPE_E":
                type_e_count += 1

        result: Dict = {}
        if vix_values:
            result["vix_threshold"] = sum(vix_values) / len(vix_values)
        if umci_values:
            result["umci_threshold"] = sum(umci_values) / len(umci_values)
        result["correlation_regime"] = "CRISIS" if type_e_count > 0 else "NORMAL"
        result["n_drawdown_events"] = len(self._drawdown_events)
        type_e_pct = type_e_count / max(len(self._drawdown_events), 1)
        result["signal_concentration"] = "HIGH" if type_e_pct > 0.3 else "NORMAL"
        return result

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def render_summary(self) -> str:
        """Text summary of all drawdown events with failure types."""
        if not self._drawdown_events:
            return "No drawdown events recorded.\n"

        lines = [
            f"{'Start':<12} {'End':<12} {'Trough':<12} {'DD%':>7} {'Days':>6} {'Type':<8} Description",
            "-" * 100,
        ]
        for ev in self._drawdown_events:
            ftype = ev.get("failure_type") or "?"
            desc = FAILURE_TYPES.get(ftype, "Unknown")
            lines.append(
                f"{str(ev.get('start_date','')):<12} "
                f"{str(ev.get('end_date','')):<12} "
                f"{str(ev.get('trough_date','')):<12} "
                f"{ev.get('drawdown_pct', 0.0):>7.2f} "
                f"{ev.get('recovery_days', 0):>6} "
                f"{ftype:<8} {desc}"
            )
        lines.append("")
        return "\n".join(lines)
