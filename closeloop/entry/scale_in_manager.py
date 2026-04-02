"""
ScaleInManager — three-tranche position entry.
Never commit full position at once; confirm before adding size.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Tranche:
    number: int           # 1, 2, 3
    fraction: float       # 0.33, 0.33, 0.34
    entry_date: Optional[str] = None
    entry_price: Optional[float] = None
    status: str = "pending"  # pending, entered, aborted
    conditions_met: List[str] = field(default_factory=list)
    net_pnl: Optional[float] = None


@dataclass
class ScaleInPosition:
    ticker: str
    direction: int
    target_size: float
    tranches: List[Tranche] = field(default_factory=list)
    status: str = "active"   # active, complete, aborted
    abort_reason: Optional[str] = None
    days_since_t1: int = 0


ABORT_CONDITIONS = [
    "PEAD_ABORT",
    "contradicting_signal_high_confidence",
    "price_3x_atr_against",
    "earnings_revision_sharply_negative",
    "congressional_trade_against",
    "drawdown_halt",
]


class ScaleInManager:
    """
    Manages three-tranche position entries.
    Tranche 1 entered immediately on signal.
    Tranche 2 entered after momentum/volume confirmation.
    Tranche 3 entered after sustained confirmation, 4-7 days post T1.
    Positions can be aborted at any time on ABORT_CONDITIONS.
    """

    def __init__(self, store=None, config=None):
        cfg = (config or {}).get("closeloop", {}).get("entry", {}).get("scale_in", {})
        self.enabled = cfg.get("enabled", True)
        self.tranches_cfg = cfg.get("tranches", {"first": 0.33, "second": 0.33, "third": 0.34})
        self.confirmation_days = cfg.get("confirmation_days", 3)
        self.momentum_threshold = cfg.get("momentum_threshold", 0.01)
        self.abort_days = cfg.get("abort_if_no_confirmation_days", 10)
        self._active: Dict[str, ScaleInPosition] = {}
        self._store = store

    def open_position(self, ticker: str, direction: int, target_size: float,
                      entry_price: float, signal_context: dict = None) -> ScaleInPosition:
        """
        Open tranche 1 immediately. Set up tranches 2 and 3 as pending.
        If scale-in is disabled, marks all tranches as entered at once.
        Returns the new ScaleInPosition.
        """
        try:
            f1 = self.tranches_cfg.get("first", 0.33)
            f2 = self.tranches_cfg.get("second", 0.33)
            f3 = self.tranches_cfg.get("third", 0.34)

            now_str = datetime.now(timezone.utc).isoformat()

            t1 = Tranche(
                number=1,
                fraction=f1,
                entry_date=now_str,
                entry_price=entry_price,
                status="entered",
                conditions_met=["initial_signal"],
            )
            t2 = Tranche(number=2, fraction=f2, status="pending")
            t3 = Tranche(number=3, fraction=f3, status="pending")

            if not self.enabled:
                # Enter all tranches immediately when scale-in disabled
                t2.entry_date = now_str
                t2.entry_price = entry_price
                t2.status = "entered"
                t2.conditions_met = ["scale_in_disabled"]
                t3.entry_date = now_str
                t3.entry_price = entry_price
                t3.status = "entered"
                t3.conditions_met = ["scale_in_disabled"]
                logger.info(
                    f"open_position({ticker}): scale_in disabled — all tranches entered immediately"
                )

            pos = ScaleInPosition(
                ticker=ticker,
                direction=direction,
                target_size=target_size,
                tranches=[t1, t2, t3],
                status="active",
            )
            self._active[ticker] = pos

            logger.info(
                f"open_position({ticker}): T1 entered at {entry_price:.4f}, "
                f"size={target_size * f1:.2f}, direction={direction}"
            )

            if self._store is not None:
                try:
                    self._store.log_scale_in_event(ticker, "T1_ENTERED", {
                        "entry_price": entry_price,
                        "fraction": f1,
                        "signal_context": signal_context,
                    })
                except Exception as e:
                    logger.warning(f"open_position({ticker}): store log failed: {e}")

            return pos

        except Exception as e:
            logger.error(f"open_position({ticker}): unexpected error: {e}")
            # Return a minimal position to avoid crashing callers
            return ScaleInPosition(
                ticker=ticker,
                direction=direction,
                target_size=target_size,
                tranches=[],
                status="aborted",
                abort_reason=str(e),
            )

    def check_tranche_2(self, ticker: str, current_price: float,
                        t1_price: float, volume_ratio: float,
                        confluence_score: float) -> bool:
        """
        Enter tranche 2 if:
          price moved > momentum_threshold (default 1%) in signal direction from t1_price AND
          volume_ratio > 1.0 (elevated) AND
          no abort conditions triggered AND
          confluence_score still > 0.5
        Returns True if tranche 2 should be entered.
        """
        try:
            pos = self._active.get(ticker)
            if pos is None:
                logger.warning(f"check_tranche_2({ticker}): no active position found")
                return False

            if pos.status != "active":
                return False

            # Find tranche 2
            t2 = next((t for t in pos.tranches if t.number == 2), None)
            if t2 is None or t2.status != "pending":
                return False

            if t1_price <= 0:
                logger.warning(f"check_tranche_2({ticker}): t1_price <= 0")
                return False

            price_move = (current_price - t1_price) / t1_price * pos.direction
            momentum_ok = price_move > self.momentum_threshold
            volume_ok = volume_ratio > 1.0
            confluence_ok = confluence_score > 0.5

            should_enter = momentum_ok and volume_ok and confluence_ok

            logger.debug(
                f"check_tranche_2({ticker}): price_move={price_move:.4f} "
                f"(threshold={self.momentum_threshold}), "
                f"vol_ratio={volume_ratio:.2f}, confluence={confluence_score:.3f}, "
                f"enter={should_enter}"
            )

            if should_enter:
                now_str = datetime.now(timezone.utc).isoformat()
                t2.entry_date = now_str
                t2.entry_price = current_price
                t2.status = "entered"
                t2.conditions_met = ["momentum_confirmed", "volume_elevated", "confluence_ok"]

                logger.info(
                    f"check_tranche_2({ticker}): T2 entered at {current_price:.4f}"
                )

                if self._store is not None:
                    try:
                        self._store.log_scale_in_event(ticker, "T2_ENTERED", {
                            "entry_price": current_price,
                            "price_move": price_move,
                            "volume_ratio": volume_ratio,
                            "confluence_score": confluence_score,
                        })
                    except Exception as e:
                        logger.warning(f"check_tranche_2({ticker}): store log failed: {e}")

            return should_enter

        except Exception as e:
            logger.warning(f"check_tranche_2({ticker}): unexpected error: {e}")
            return False

    def check_tranche_3(self, ticker: str, current_price: float,
                        confluence_score: float, days_since_t1: int) -> bool:
        """
        Enter tranche 3 if:
          price continues in signal direction AND
          confluence_score > 0.5 AND
          no negative news / signal reversal AND
          4-7 days since tranche 1
        Returns True if tranche 3 should be entered.
        """
        try:
            pos = self._active.get(ticker)
            if pos is None:
                logger.warning(f"check_tranche_3({ticker}): no active position found")
                return False

            if pos.status != "active":
                return False

            t3 = next((t for t in pos.tranches if t.number == 3), None)
            if t3 is None or t3.status != "pending":
                return False

            # Check T2 has been entered — don't skip tranches
            t2 = next((t for t in pos.tranches if t.number == 2), None)
            if t2 is None or t2.status != "entered":
                logger.debug(f"check_tranche_3({ticker}): T2 not yet entered, skipping T3")
                return False

            timing_ok = 4 <= days_since_t1 <= 7
            confluence_ok = confluence_score > 0.5

            # Direction check: current_price vs T1 entry price
            t1 = next((t for t in pos.tranches if t.number == 1), None)
            if t1 is None or t1.entry_price is None:
                direction_ok = True  # Cannot verify, allow
            else:
                price_move = (current_price - t1.entry_price) / t1.entry_price * pos.direction
                direction_ok = price_move > 0

            should_enter = timing_ok and confluence_ok and direction_ok

            logger.debug(
                f"check_tranche_3({ticker}): timing_ok={timing_ok} (days={days_since_t1}), "
                f"confluence_ok={confluence_ok} ({confluence_score:.3f}), "
                f"direction_ok={direction_ok}, enter={should_enter}"
            )

            if should_enter:
                now_str = datetime.now(timezone.utc).isoformat()
                t3.entry_date = now_str
                t3.entry_price = current_price
                t3.status = "entered"
                t3.conditions_met = ["confluence_sustained", "direction_confirmed", "timing_window"]
                pos.status = "complete"

                logger.info(
                    f"check_tranche_3({ticker}): T3 entered at {current_price:.4f} — position complete"
                )

                if self._store is not None:
                    try:
                        self._store.log_scale_in_event(ticker, "T3_ENTERED", {
                            "entry_price": current_price,
                            "days_since_t1": days_since_t1,
                            "confluence_score": confluence_score,
                        })
                    except Exception as e:
                        logger.warning(f"check_tranche_3({ticker}): store log failed: {e}")

            return should_enter

        except Exception as e:
            logger.warning(f"check_tranche_3({ticker}): unexpected error: {e}")
            return False

    def check_abort(self, ticker: str, abort_reason: str) -> bool:
        """
        Check if any abort condition fires. If yes, close all tranches.
        Logs ABORT with reason. Returns True if aborted.
        """
        try:
            pos = self._active.get(ticker)
            if pos is None:
                return False

            if pos.status not in ("active", "complete"):
                return False

            # Validate abort reason is a known condition (or allow any string)
            is_known = abort_reason in ABORT_CONDITIONS
            if not is_known:
                logger.debug(
                    f"check_abort({ticker}): non-standard abort reason '{abort_reason}' — still aborting"
                )

            pos.status = "aborted"
            pos.abort_reason = abort_reason

            aborted_tranches = []
            for t in pos.tranches:
                if t.status == "pending":
                    t.status = "aborted"
                    aborted_tranches.append(t.number)

            logger.warning(
                f"ABORT {ticker}: reason={abort_reason}, "
                f"tranches_aborted={aborted_tranches}, "
                f"known_condition={is_known}"
            )

            if self._store is not None:
                try:
                    self._store.log_scale_in_event(ticker, "ABORT", {
                        "reason": abort_reason,
                        "tranches_aborted": aborted_tranches,
                    })
                except Exception as e:
                    logger.warning(f"check_abort({ticker}): store log failed: {e}")

            return True

        except Exception as e:
            logger.warning(f"check_abort({ticker}): unexpected error: {e}")
            return False

    def check_failed_confirmation(self, ticker: str) -> bool:
        """
        If no tranche 2 confirmation after abort_days: exit tranche 1.
        Logs as FAILED_CONFIRMATION.
        Returns True if position should be closed.
        """
        try:
            pos = self._active.get(ticker)
            if pos is None:
                return False

            if pos.status != "active":
                return False

            t2 = next((t for t in pos.tranches if t.number == 2), None)
            if t2 is not None and t2.status == "entered":
                # T2 already entered — no failed confirmation
                return False

            if pos.days_since_t1 >= self.abort_days:
                pos.status = "aborted"
                pos.abort_reason = "FAILED_CONFIRMATION"

                for t in pos.tranches:
                    if t.status == "pending":
                        t.status = "aborted"

                logger.warning(
                    f"FAILED_CONFIRMATION {ticker}: no T2 after {pos.days_since_t1} days "
                    f"(abort_days={self.abort_days}) — exiting T1"
                )

                if self._store is not None:
                    try:
                        self._store.log_scale_in_event(ticker, "FAILED_CONFIRMATION", {
                            "days_since_t1": pos.days_since_t1,
                            "abort_days": self.abort_days,
                        })
                    except Exception as e:
                        logger.warning(f"check_failed_confirmation({ticker}): store log failed: {e}")

                return True

            return False

        except Exception as e:
            logger.warning(f"check_failed_confirmation({ticker}): unexpected error: {e}")
            return False

    def close_position(self, ticker: str, exit_price: float, reason: str) -> Optional[ScaleInPosition]:
        """
        Close all active tranches, compute per-tranche P&L, log to store.
        Returns the closed ScaleInPosition, or None if not found.
        """
        try:
            pos = self._active.get(ticker)
            if pos is None:
                logger.warning(f"close_position({ticker}): no active position found")
                return None

            now_str = datetime.now(timezone.utc).isoformat()

            for t in pos.tranches:
                if t.status == "entered":
                    if t.entry_price is not None and t.entry_price > 0:
                        raw_pnl = (exit_price - t.entry_price) * pos.direction
                        t.net_pnl = raw_pnl * (pos.target_size * t.fraction)
                    else:
                        t.net_pnl = 0.0

            total_pnl = sum(t.net_pnl for t in pos.tranches if t.net_pnl is not None)

            pos.status = "aborted" if "abort" in reason.lower() or "fail" in reason.lower() else "complete"

            logger.info(
                f"close_position({ticker}): reason={reason}, exit={exit_price:.4f}, "
                f"total_pnl={total_pnl:.4f}"
            )

            if self._store is not None:
                try:
                    self._store.log_scale_in_event(ticker, "POSITION_CLOSED", {
                        "exit_price": exit_price,
                        "reason": reason,
                        "total_pnl": total_pnl,
                        "closed_at": now_str,
                        "tranches": [
                            {
                                "number": t.number,
                                "entry_price": t.entry_price,
                                "fraction": t.fraction,
                                "status": t.status,
                                "net_pnl": t.net_pnl,
                            }
                            for t in pos.tranches
                        ],
                    })
                except Exception as e:
                    logger.warning(f"close_position({ticker}): store log failed: {e}")

            del self._active[ticker]
            return pos

        except Exception as e:
            logger.warning(f"close_position({ticker}): unexpected error: {e}")
            return None

    def get_total_exposure(self, ticker: str) -> float:
        """Return total current notional across all entered tranches."""
        try:
            pos = self._active.get(ticker)
            if pos is None:
                return 0.0

            total = 0.0
            for t in pos.tranches:
                if t.status == "entered" and t.entry_price is not None:
                    total += pos.target_size * t.fraction

            return total

        except Exception as e:
            logger.warning(f"get_total_exposure({ticker}): unexpected error: {e}")
            return 0.0
