"""
Watchlist Manager — unified interface for managing the frontier signal pipeline.

The watchlist is the staging area for newly discovered signals before they
have been formally validated and promoted to live status.  This module
provides a single interface that keeps the FrontierStore (SQLite) and the
DiscoveryRegistry (JSONL audit log) in sync at all state transitions.

State transitions:
  PENDING → VALIDATING → LIVE
                       ↘ FAILED

Every state change is logged permanently in both the store and registry,
ensuring a full audit trail of every signal's lifecycle.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Status constants (mirror DiscoveryRegistry)
STATUS_WATCHLIST  = "watchlist"
STATUS_VALIDATING = "validating"
STATUS_LIVE       = "live"
STATUS_FAILED     = "failed"


class WatchlistManager:
    """
    Unified manager for the frontier signal watchlist.

    Keeps FrontierStore and DiscoveryRegistry in sync across all
    lifecycle state transitions.

    Parameters
    ----------
    store    : FrontierStore instance or None
    registry : DiscoveryRegistry instance or None
    """

    def __init__(self, store=None, registry=None):
        self._store = store
        self._registry = registry

    # ------------------------------------------------------------------
    # Add / register
    # ------------------------------------------------------------------

    def add(
        self,
        signal_name: str,
        description: str,
        source_signals: List[str],
        initial_corr: float,
        nonsense_score: float = 0.0,
        has_story: bool = False,
    ) -> Dict:
        """
        Add a new signal candidate to the watchlist.

        Writes to both store.upsert_watchlist() and registry.register().

        Parameters
        ----------
        signal_name    : unique identifier string
        description    : human-readable description of the signal
        source_signals : list of raw signal names that compose this signal
        initial_corr   : in-sample rolling correlation with returns
        nonsense_score : 0–1; 1 = no plausible economic mechanism
        has_story      : True if a coherent economic mechanism exists

        Returns
        -------
        dict : registry record for the newly added signal
        """
        now = datetime.now(timezone.utc).isoformat()

        # Write to persistent SQLite store
        if self._store is not None:
            try:
                self._store.upsert_watchlist({
                    "name": signal_name,
                    "description": description,
                    "formula": ", ".join(source_signals),
                    "correlation": initial_corr,
                    "validation_status": STATUS_WATCHLIST,
                    "notes": (
                        f"nonsense={nonsense_score:.2f}, "
                        f"story={'yes' if has_story else 'no'}"
                    ),
                    "discovered_at": now,
                })
            except Exception as exc:
                logger.warning(f"[WatchlistManager] store.upsert_watchlist failed: {exc}")

        # Write to JSONL audit registry
        record: Dict = {}
        if self._registry is not None:
            try:
                record = self._registry.register(
                    signal_name=signal_name,
                    description=description,
                    source_signals=source_signals,
                    initial_correlation=initial_corr,
                    discovery_method="auto",
                    nonsense_score=nonsense_score,
                    has_economic_story=has_story,
                )
            except Exception as exc:
                logger.warning(f"[WatchlistManager] registry.register failed: {exc}")
        else:
            # Return a minimal record when registry unavailable
            record = {
                "signal_name": signal_name,
                "description": description,
                "source_signals": source_signals,
                "initial_correlation": initial_corr,
                "nonsense_score": nonsense_score,
                "has_economic_story": has_story,
                "status": STATUS_WATCHLIST,
                "registered_at": now,
            }

        logger.info(
            f"[WatchlistManager] Added '{signal_name}' to watchlist "
            f"(corr={initial_corr:.4f}, nonsense={nonsense_score:.2f})"
        )
        return record

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def promote_to_validating(self, signal_name: str) -> None:
        """
        Advance a signal from watchlist to active validation.

        Updates status in both store and registry.

        Parameters
        ----------
        signal_name : signal to promote
        """
        if self._store is not None:
            try:
                self._store.upsert_watchlist({
                    "name": signal_name,
                    "validation_status": STATUS_VALIDATING,
                })
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] store update failed for "
                    f"promote_to_validating('{signal_name}'): {exc}"
                )

        if self._registry is not None:
            try:
                self._registry.update_status(
                    signal_name=signal_name,
                    new_status=STATUS_VALIDATING,
                    note="Promoted to active validation",
                )
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] registry update failed for "
                    f"promote_to_validating('{signal_name}'): {exc}"
                )

        logger.info(f"[WatchlistManager] '{signal_name}' → VALIDATING")

    def promote_to_live(
        self,
        signal_name: str,
        evidence_grade: str,
        tier: int,
        oos_sharpe: float,
    ) -> None:
        """
        Mark a signal as live after successful out-of-sample validation.

        Parameters
        ----------
        signal_name    : signal to promote
        evidence_grade : 'A', 'B', 'C', or 'D'
        tier           : sizing tier (1 = full size, 5 = minimal)
        oos_sharpe     : out-of-sample Sharpe ratio from validation
        """
        if self._store is not None:
            try:
                self._store.upsert_watchlist({
                    "name": signal_name,
                    "validation_status": STATUS_LIVE,
                    "sizing_tier": tier,
                    "notes": (
                        f"grade={evidence_grade}, tier={tier}, "
                        f"oos_sharpe={oos_sharpe:.3f}"
                    ),
                })
                self._store.update_evidence(
                    signal_name=signal_name,
                    sizing_tier=tier,
                    live_days=0,
                    live_sharpe=oos_sharpe,
                )
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] store update failed for "
                    f"promote_to_live('{signal_name}'): {exc}"
                )

        if self._registry is not None:
            try:
                self._registry.update_status(
                    signal_name=signal_name,
                    new_status=STATUS_LIVE,
                    evidence_grade=evidence_grade,
                    suggested_tier=tier,
                    oos_sharpe=oos_sharpe,
                    note=(
                        f"Promoted to LIVE: grade={evidence_grade}, "
                        f"tier={tier}, oos_sharpe={oos_sharpe:.3f}"
                    ),
                )
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] registry update failed for "
                    f"promote_to_live('{signal_name}'): {exc}"
                )

        logger.info(
            f"[WatchlistManager] '{signal_name}' → LIVE "
            f"(grade={evidence_grade}, tier={tier}, sharpe={oos_sharpe:.3f})"
        )

    def fail(self, signal_name: str, reason: str) -> None:
        """
        Mark a signal as failed.

        The failure record is preserved permanently — failed signals are
        evidence against data-mining bias for similar-looking candidates.

        Parameters
        ----------
        signal_name : signal to mark failed
        reason      : human-readable reason for failure
        """
        if self._store is not None:
            try:
                self._store.upsert_watchlist({
                    "name": signal_name,
                    "validation_status": STATUS_FAILED,
                    "notes": f"FAILED: {reason}",
                })
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] store update failed for "
                    f"fail('{signal_name}'): {exc}"
                )

        if self._registry is not None:
            try:
                self._registry.update_status(
                    signal_name=signal_name,
                    new_status=STATUS_FAILED,
                    note=f"FAILED: {reason}",
                )
            except Exception as exc:
                logger.warning(
                    f"[WatchlistManager] registry update failed for "
                    f"fail('{signal_name}'): {exc}"
                )

        logger.info(f"[WatchlistManager] '{signal_name}' → FAILED: {reason}")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_all(self, status: Optional[str] = None) -> List[Dict]:
        """
        Return all watchlist records, optionally filtered by status.

        Reads from the registry (the canonical in-memory source).
        Falls back to an empty list if registry is unavailable.

        Parameters
        ----------
        status : filter by status string (None = all records)

        Returns
        -------
        list of dicts, sorted by registration time descending
        """
        if self._registry is None:
            logger.debug("[WatchlistManager] get_all(): registry not configured")
            return []
        try:
            return self._registry.get_all(status=status)
        except Exception as exc:
            logger.warning(f"[WatchlistManager] get_all() failed: {exc}")
            return []

    def render_watchlist_table(self) -> str:
        """
        Return a formatted plain-text table of all watchlist entries.

        Columns: Signal | Status | Grade | Tier | OOS_Sharpe | Nonsense | Days_Live
        Rows sorted by OOS Sharpe descending (None treated as -inf).

        Returns
        -------
        str : formatted table
        """
        records = self.get_all()
        if not records:
            return "  (watchlist empty)\n"

        # Sort by OOS Sharpe descending
        def _sharpe_key(r: Dict) -> float:
            v = r.get("oos_sharpe")
            return float(v) if v is not None else float("-inf")

        records_sorted = sorted(records, key=_sharpe_key, reverse=True)

        # Header
        col_signal   = 30
        col_status   = 12
        col_grade    = 6
        col_tier     = 5
        col_sharpe   = 10
        col_nonsense = 9
        col_days     = 9

        header = (
            f"  {'Signal':<{col_signal}} "
            f"{'Status':<{col_status}} "
            f"{'Grade':<{col_grade}} "
            f"{'Tier':<{col_tier}} "
            f"{'OOS_Sharpe':<{col_sharpe}} "
            f"{'Nonsense':<{col_nonsense}} "
            f"{'Days_Live':<{col_days}}"
        )
        separator = "  " + "-" * (
            col_signal + col_status + col_grade + col_tier +
            col_sharpe + col_nonsense + col_days + 6
        )

        lines = [header, separator]
        for r in records_sorted:
            sig_name  = str(r.get("signal_name", ""))[:col_signal]
            status    = str(r.get("status", ""))[:col_status]
            grade     = str(r.get("evidence_grade", "-"))[:col_grade]
            tier      = str(r.get("evidence_tier", "-"))[:col_tier]
            sharpe_v  = r.get("oos_sharpe")
            sharpe_s  = f"{sharpe_v:.3f}" if sharpe_v is not None else "-"
            nonsense_v = r.get("nonsense_score")
            nonsense_s = f"{nonsense_v:.2f}" if nonsense_v is not None else "-"
            days_v    = r.get("live_days", 0)
            days_s    = str(days_v) if days_v is not None else "0"

            lines.append(
                f"  {sig_name:<{col_signal}} "
                f"{status:<{col_status}} "
                f"{grade:<{col_grade}} "
                f"{tier:<{col_tier}} "
                f"{sharpe_s:<{col_sharpe}} "
                f"{nonsense_s:<{col_nonsense}} "
                f"{days_s:<{col_days}}"
            )

        return "\n".join(lines) + "\n"
