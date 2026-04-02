import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class RollbackManager:
    """
    Monitors live model performance and triggers automatic rollback when
    configurable degradation thresholds are breached.
    """

    def __init__(self, config: dict, store, registry, notifier=None):
        self.config = config
        self.store = store
        self.registry = registry
        self.notifier = notifier

        vc = (
            config.get("altdata", {})
            .get("learning", {})
            .get("model_versioning", {})
        )
        self.auto_rollback: bool = vc.get("auto_rollback", True)
        self.rollback_threshold: float = vc.get("rollback_threshold", 0.1)
        self.window_days: int = vc.get("comparison_window_days", 14)

        self._consecutive_wrong: int = 0
        self._investigation_mode: bool = False
        self._investigation_until: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Monitoring
    # ------------------------------------------------------------------

    def check(self, model_name: str) -> Optional[str]:
        """
        Check if rollback is needed for model_name.

        Returns a rollback-reason string when a trigger fires, else None.

        Triggers:
          1. Live accuracy dropped >15 % below training accuracy.
          2. Live Sharpe dropped >rollback_threshold below previous version Sharpe.
          3. Three consecutive wrong signals.
        """
        current_acc = self.store.get_signal_accuracy(model_name, self.window_days)
        if current_acc is None:
            return None

        history = self.registry.get_history(model_name)
        if len(history) < 2:
            return None

        # Active version entry
        current_version = next(
            (v for v in reversed(history) if v.get("is_active")), None
        )
        if not current_version:
            return None

        training_acc = current_version.get("metrics", {}).get("accuracy", 0.75)

        # Trigger 1: accuracy dropped >15 % below training accuracy ----------
        if current_acc < training_acc * (1 - self.rollback_threshold * 1.5):
            return (
                f"live_accuracy_{current_acc:.3f}_below_training_{training_acc:.3f}"
            )

        # Trigger 2: live Sharpe vs previous model Sharpe --------------------
        current_sharpe = current_version.get("live_sharpe") or current_version.get(
            "metrics", {}
        ).get("sharpe", 0)
        prev_version_str = self.registry.get_previous_version(model_name)
        if prev_version_str:
            prev_entry = next(
                (v for v in history if v.get("version") == prev_version_str),
                None,
            )
            if prev_entry:
                prev_sharpe = prev_entry.get(
                    "live_sharpe"
                ) or prev_entry.get("metrics", {}).get("sharpe", 0)
                if prev_sharpe and prev_sharpe > 0:
                    if current_sharpe < prev_sharpe * (1 - self.rollback_threshold):
                        return (
                            f"live_sharpe_{current_sharpe:.2f}_below_prev_{prev_sharpe:.2f}"
                        )

        # Trigger 3: three consecutive wrong signals -------------------------
        if self._consecutive_wrong >= 3:
            return f"consecutive_wrong_signals_{self._consecutive_wrong}"

        return None

    def record_outcome(self, was_correct: bool):
        """Call this after each signal outcome is known to track streak."""
        if was_correct:
            self._consecutive_wrong = 0
        else:
            self._consecutive_wrong += 1
            logger.debug(
                "Consecutive wrong signals: %d", self._consecutive_wrong
            )

    # ------------------------------------------------------------------
    # Rollback execution
    # ------------------------------------------------------------------

    def execute_rollback(self, model_name: str, reason: str) -> bool:
        """
        Execute automatic rollback to the previous non-rolled-back version.

        Returns True on success, False if rollback is disabled or not possible.
        """
        if not self.auto_rollback:
            logger.warning(
                "Auto rollback disabled. Manual rollback needed for %s. Reason: %s",
                model_name,
                reason,
            )
            return False

        prev_version = self.registry.get_previous_version(model_name)
        if not prev_version:
            logger.error(
                "No previous version to roll back to for %s", model_name
            )
            return False

        # Mark current version as rolled back
        history = self.registry.get_history(model_name)
        current = next(
            (v for v in reversed(history) if v.get("is_active")), None
        )
        if current:
            self.registry.mark_rolled_back(
                model_name, current["version"], reason
            )

        # Activate previous version in history
        for v in self.registry._history.get(model_name, []):
            if v["version"] == prev_version:
                v["is_active"] = True
                v["reactivated_at"] = datetime.now().isoformat()
        self.registry._save_history()

        # Reset counters and enter investigation mode
        self._consecutive_wrong = 0
        self._investigation_mode = True
        self._investigation_until = datetime.now() + timedelta(days=7)

        msg = (
            f"Model rollback executed for {model_name}. "
            f"Reason: {reason}. Reverted to {prev_version}."
        )
        logger.warning(msg)

        if self.notifier:
            self.notifier.send("model_rollback", None, None, msg)

        return True

    def manual_rollback(self, model_name: str, to_version: str) -> bool:
        """
        Manually roll back to a specific version.

        Returns True on success.
        """
        logger.info(
            "Manual rollback requested for %s -> %s", model_name, to_version
        )
        return self.execute_rollback(
            model_name, f"manual_rollback_to_{to_version}"
        )

    # ------------------------------------------------------------------
    # Investigation mode helpers
    # ------------------------------------------------------------------

    @property
    def in_investigation_mode(self) -> bool:
        """True while the post-rollback investigation window is active."""
        if not self._investigation_mode:
            return False
        if self._investigation_until and datetime.now() > self._investigation_until:
            self._investigation_mode = False
            return False
        return True
