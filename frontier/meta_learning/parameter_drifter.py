"""
Parameter Drifter — Bayesian adaptive parameter management.

Maintains a set of published anchor values (sourced from academic literature
or expert calibration) and allows them to drift toward new empirical evidence
via a weighted Bayesian update.

Each update is clamped so a parameter can never stray more than 50%–200% of
its published anchor — this prevents runaway adaptation while allowing
meaningful learning from new data.

Every drift event is permanently logged to the FrontierStore (parameter_history
table) so the full adaptation history is auditable and can be replayed.

Design principle: anchors are ground truth until the market proves otherwise.
Drifting toward evidence is learning; abandoning anchors entirely is overfitting.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Published parameter anchors
# ---------------------------------------------------------------------------

PUBLISHED_ANCHORS: Dict[str, Dict] = {
    "grai_decay_lambda": {
        "value": 0.15,
        "source": "Krivelyova & Robotti 2003",
        "drift_rate": 0.01,
    },
    "grai_volatility_amplifier": {
        "value": 0.30,
        "source": "Estimated",
        "drift_rate": 0.02,
    },
    "kelly_base_fraction": {
        "value": 0.25,
        "source": "Kelly 1956",
        "drift_rate": 0.005,
    },
    "scv_beta": {
        "value": 0.30,
        "source": "SIR literature",
        "drift_rate": 0.02,
    },
    "scv_gamma": {
        "value": 0.10,
        "source": "SIR literature",
        "drift_rate": 0.01,
    },
    "fsp_min_threshold": {
        "value": 0.50,
        "source": "Defined",
        "drift_rate": 0.005,
    },
}


class ParameterDrifter:
    """
    Manages Bayesian adaptive drift of frontier model parameters.

    Parameters can drift from their published anchors in response to live
    market evidence, subject to hard bounds of 50%–200% of the anchor.

    Parameters
    ----------
    store : FrontierStore or None
        If provided, every drift event is logged via store.log_parameter_drift().
    anchors : dict or None
        Override the default PUBLISHED_ANCHORS (useful for testing).
    """

    def __init__(self, store=None, anchors: Optional[Dict] = None):
        self._store = store
        self._anchors: Dict[str, Dict] = dict(anchors or PUBLISHED_ANCHORS)
        # Current (drifted) values — start at anchor values
        self._current: Dict[str, float] = {
            name: meta["value"] for name, meta in self._anchors.items()
        }

    # ------------------------------------------------------------------
    # Core public methods
    # ------------------------------------------------------------------

    def drift(
        self,
        param_name: str,
        current_value: float,
        new_evidence_value: float,
        weight: float = 0.1,
    ) -> float:
        """
        Bayesian update: blend current value toward new evidence.

        new_value = (1 - weight) * current + weight * new_evidence_value

        The result is clamped to [anchor * 0.5, anchor * 2.0].

        Parameters
        ----------
        param_name         : parameter key (must exist in anchors)
        current_value      : current parameter value before update
        new_evidence_value : observed optimal value from new data
        weight             : learning rate (0–1, default 0.1 = 10% update)

        Returns
        -------
        float : new clamped parameter value
        """
        if param_name not in self._anchors:
            logger.warning(
                f"[ParameterDrifter] Unknown parameter '{param_name}' — "
                "no drift applied, returning current_value unchanged"
            )
            return current_value

        anchor = self._anchors[param_name]["value"]
        lo = anchor * 0.5
        hi = anchor * 2.0

        raw_new = (1.0 - weight) * current_value + weight * new_evidence_value
        clamped = max(lo, min(hi, raw_new))

        old_value = self._current.get(param_name, current_value)
        self._current[param_name] = clamped

        logger.info(
            f"[ParameterDrifter] '{param_name}': "
            f"{old_value:.5f} → {clamped:.5f} "
            f"(evidence={new_evidence_value:.5f}, weight={weight:.3f}, "
            f"anchor={anchor:.5f}, bounds=[{lo:.5f}, {hi:.5f}])"
        )

        # Persist drift event
        if self._store is not None:
            try:
                self._store.log_parameter_drift(
                    signal_name="parameter_drifter",
                    param_name=param_name,
                    published=anchor,
                    old_val=old_value,
                    new_val=clamped,
                    delta=clamped - old_value,
                    reason=f"Bayesian update: evidence={new_evidence_value:.5f}, w={weight:.3f}",
                )
            except Exception as exc:
                logger.warning(f"[ParameterDrifter] Failed to log drift to store: {exc}")

        return clamped

    def get_current(self, param_name: str) -> float:
        """
        Return the current (possibly drifted) value of a parameter.

        Falls back to the published anchor if no drift has been applied.

        Parameters
        ----------
        param_name : parameter key

        Returns
        -------
        float : current parameter value
        """
        if param_name not in self._anchors:
            logger.warning(f"[ParameterDrifter] Unknown parameter '{param_name}' — returning 0.0")
            return 0.0
        return self._current.get(param_name, self._anchors[param_name]["value"])

    def reset_to_anchor(self, param_name: str) -> float:
        """
        Reset a parameter to its published anchor value.

        Parameters
        ----------
        param_name : parameter key

        Returns
        -------
        float : the published anchor value
        """
        if param_name not in self._anchors:
            logger.warning(f"[ParameterDrifter] Unknown parameter '{param_name}' — cannot reset")
            return 0.0

        anchor_value = self._anchors[param_name]["value"]
        old_value = self._current.get(param_name, anchor_value)
        self._current[param_name] = anchor_value

        logger.info(
            f"[ParameterDrifter] '{param_name}' reset: "
            f"{old_value:.5f} → {anchor_value:.5f} (anchor)"
        )

        if self._store is not None:
            try:
                self._store.log_parameter_drift(
                    signal_name="parameter_drifter",
                    param_name=param_name,
                    published=anchor_value,
                    old_val=old_value,
                    new_val=anchor_value,
                    delta=anchor_value - old_value,
                    reason="Manual reset to published anchor",
                )
            except Exception as exc:
                logger.warning(f"[ParameterDrifter] Failed to log reset to store: {exc}")

        return anchor_value

    def summary(self) -> Dict:
        """
        Return a summary dict of all parameters with published vs current
        values and drift amounts.

        Returns
        -------
        dict : {param_name: {published, source, current, drift, drift_pct}}
        """
        result = {}
        for param_name, meta in self._anchors.items():
            published = meta["value"]
            current = self._current.get(param_name, published)
            drift = current - published
            drift_pct = (drift / published * 100.0) if published != 0 else 0.0
            result[param_name] = {
                "published": published,
                "source": meta.get("source", "unknown"),
                "current": round(current, 6),
                "drift": round(drift, 6),
                "drift_pct": round(drift_pct, 2),
                "anchor_lo": round(published * 0.5, 6),
                "anchor_hi": round(published * 2.0, 6),
                "drift_rate": meta.get("drift_rate", 0.01),
            }
        return result

    # ------------------------------------------------------------------
    # Bulk convenience
    # ------------------------------------------------------------------

    def drift_all(self, evidence: Dict[str, float], weight: float = 0.1) -> Dict[str, float]:
        """
        Apply drift to multiple parameters at once from an evidence dict.

        Parameters
        ----------
        evidence : {param_name: new_evidence_value}
        weight   : shared learning rate

        Returns
        -------
        dict : {param_name: new_value} for each updated parameter
        """
        updated = {}
        for param_name, evidence_value in evidence.items():
            if param_name in self._anchors:
                current = self.get_current(param_name)
                updated[param_name] = self.drift(param_name, current, evidence_value, weight)
        return updated

    def get_all_current(self) -> Dict[str, float]:
        """Return all current parameter values as a flat dict."""
        return {
            name: self._current.get(name, meta["value"])
            for name, meta in self._anchors.items()
        }
