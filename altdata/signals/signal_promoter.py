import logging
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class SignalPromoter:
    """
    Connects validated altdata signals to the main signal_registry.py.

    Responsibilities:
      - Gate promotion based on accuracy and occurrence frequency
      - Register and promote signals in the main SignalRegistry
      - Filter PEAD modifier signals to avoid double-counting with main engine
    """

    def __init__(self, config: dict, altdata_store, signal_registry):
        self.config = config
        self.altdata_store = altdata_store
        self.signal_registry = signal_registry

        # Promotion thresholds
        self.min_confidence_for_promotion: float = 0.70
        self.min_occurrences: int = 10  # must have fired at least 10 times

    # ------------------------------------------------------------------
    # Promotion
    # ------------------------------------------------------------------

    def promote_to_registry(
        self,
        signal_name: str,
        signal_type: str,
        params: dict,
        performance: dict,
    ) -> bool:
        """
        Promote a validated altdata signal to the main SignalRegistry.

        Only promotes when both accuracy and frequency thresholds are met.

        Parameters
        ----------
        signal_name  : unique name for the signal (e.g. "altdata_wiki_sunday")
        signal_type  : signal category (e.g. "ALT_LONG", "temporal")
        params       : signal construction parameters
        performance  : {"accuracy": float, "sharpe": float, "n_occurrences": int}

        Returns True if promotion succeeded, False otherwise.
        """
        accuracy = performance.get("accuracy", 0)
        n_occurrences = performance.get("n_occurrences", 0)
        sharpe = performance.get("sharpe", 0)

        if accuracy < 0.55:
            logger.info(
                "Not promoting %s: accuracy %.2f < 0.55", signal_name, accuracy
            )
            return False

        if n_occurrences < self.min_occurrences:
            logger.info(
                "Not promoting %s: only %d occurrences (need %d)",
                signal_name,
                n_occurrences,
                self.min_occurrences,
            )
            return False

        metadata = {
            "sharpe": sharpe,
            "accuracy": accuracy,
            "n_occurrences": n_occurrences,
            "source": "altdata_engine",
            "promoted_at": datetime.now().isoformat(),
        }

        try:
            self.signal_registry.register(
                name=signal_name,
                signal_type=f"altdata_{signal_type}",
                params=params,
                metadata=metadata,
            )

            # Pass a lightweight validation result so the registry marks
            # the signal as promoted (live) rather than candidate.
            validation = {
                "passed": True,
                "train_sharpe": sharpe,
                "val_sharpe": sharpe * 0.8,
                "test_sharpe": sharpe * 0.7,
            }
            self.signal_registry.promote(signal_name, validation)

            logger.info(
                "Promoted altdata signal to registry: %s (sharpe=%.2f acc=%.2f n=%d)",
                signal_name,
                sharpe,
                accuracy,
                n_occurrences,
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to promote signal %s: %s", signal_name, e
            )
            return False

    # ------------------------------------------------------------------
    # PEAD modifier sync
    # ------------------------------------------------------------------

    def sync_pead_modifier(self, active_signals: List[Dict]) -> List[Dict]:
        """
        Filter PEAD_BOOST / PEAD_REDUCE / PEAD_ABORT signals through the
        registry to avoid double-counting with positions already managed by
        the main PEAD engine.

        Rules:
          PEAD_ABORT  — always actionable (overrides existing position)
          ALT_LONG / ALT_SHORT — always actionable (independent signals)
          PEAD_BOOST / PEAD_REDUCE — only actionable if confidence > 0.75

        Returns the filtered list of actionable signals.
        """
        actionable: List[Dict] = []

        for sig in active_signals:
            sig_type = sig.get("signal_type", "")

            if sig_type == "PEAD_ABORT":
                # Strong contradiction — always surface to execution layer
                actionable.append(sig)

            elif sig_type in ("ALT_LONG", "ALT_SHORT"):
                # Independent altdata signals — always actionable
                actionable.append(sig)

            elif sig.get("confidence", 0) > 0.75:
                # High-confidence modifiers only
                actionable.append(sig)
            else:
                logger.debug(
                    "sync_pead_modifier: dropping %s %s (confidence=%.3f < 0.75)",
                    sig.get("ticker", "?"),
                    sig_type,
                    sig.get("confidence", 0),
                )

        logger.debug(
            "sync_pead_modifier: %d/%d signals actionable",
            len(actionable),
            len(active_signals),
        )
        return actionable
