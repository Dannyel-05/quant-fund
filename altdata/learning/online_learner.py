try:
    from river import ensemble, linear_model, drift, metrics, preprocessing
    RIVER_AVAILABLE = True
except ImportError:
    RIVER_AVAILABLE = False

import logging
import joblib
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class OnlineLearner:
    def __init__(self, config: dict, registry=None):
        self.config = config
        self.registry = registry
        cfg = (
            config.get("altdata", {})
            .get("learning", {})
            .get("online_learning", {})
        )
        self.min_samples = cfg.get("min_samples_before_signal", 100)
        self.n_trained: int = 0
        self.drift_events: List[dict] = []
        self._feature_importance: Dict[str, float] = {}
        self._recent_predictions: List[dict] = []  # for rollback monitoring

        if RIVER_AVAILABLE:
            self._init_models()
        else:
            logger.warning(
                "River not installed. Online learning disabled. pip install river"
            )
            self.arf = None
            self.lr = None

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _init_models(self):
        from river import ensemble, linear_model, preprocessing, drift

        self.scaler = preprocessing.StandardScaler()
        self.arf = ensemble.AdaptiveRandomForestClassifier(n_models=10, seed=42)
        self.lr = linear_model.LogisticRegression()
        self.drift_detector = drift.ADWIN()

        # Ensemble weights [arf_weight, lr_weight]
        self._weights = [0.7, 0.3]
        # Track recent accuracy per model for adaptive weighting
        self._recent_correct: Dict[str, List[float]] = {"arf": [], "lr": []}

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, features: Dict[str, float]) -> Tuple[int, float]:
        """
        Returns (direction, confidence).
        direction  : +1 or -1
        confidence : [0, 1]
        Returns (0, 0.0) if insufficient data or models not ready.
        """
        if not RIVER_AVAILABLE or self.arf is None:
            return 0, 0.0
        if self.n_trained < self.min_samples:
            return 0, 0.0

        try:
            scaled = self.scaler.transform_one(features)

            p_arf = self.arf.predict_proba_one(scaled)
            p_lr = self.lr.predict_proba_one(scaled)

            # Weighted ensemble probability for class 1 (positive direction)
            prob_pos = (
                self._weights[0] * p_arf.get(1, 0.5)
                + self._weights[1] * p_lr.get(1, 0.5)
            )

            direction = 1 if prob_pos > 0.5 else -1
            # Rescale distance from 0.5 to [0, 1]
            confidence = abs(prob_pos - 0.5) * 2

            return direction, float(confidence)
        except Exception as e:
            logger.error("Prediction error: %s", e)
            return 0, 0.0

    # ------------------------------------------------------------------
    # Learning
    # ------------------------------------------------------------------

    def learn(
        self,
        features: Dict[str, float],
        actual_direction: int,
        trade_return: float,
    ):
        """
        Update models with observed outcome.
        actual_direction : +1 or -1
        trade_return     : realised return (signed)
        """
        if not RIVER_AVAILABLE or self.arf is None:
            return

        try:
            label = 1 if actual_direction > 0 else 0

            # Update scaler and scale features for this step
            scaled = self.scaler.learn_one(features).transform_one(features)

            # Track drift via ADWIN on classification error
            arf_pred = self.arf.predict_one(scaled)
            error = 0.0 if (arf_pred == label) else 1.0
            self.drift_detector.update(error)
            if self.drift_detector.drift_detected:
                event = {
                    "timestamp": datetime.now().isoformat(),
                    "n_trained": self.n_trained,
                    "error": error,
                }
                self.drift_events.append(event)
                logger.warning(
                    "Concept drift detected at sample %d", self.n_trained
                )

            # Track per-model accuracy for ensemble weight update
            lr_pred = self.lr.predict_one(scaled)
            self._recent_correct["arf"].append(1.0 if arf_pred == label else 0.0)
            self._recent_correct["lr"].append(1.0 if lr_pred == label else 0.0)

            # Learn
            self.arf.learn_one(scaled, label)
            self.lr.learn_one(scaled, label)
            self.n_trained += 1

            # Update ensemble weights every 50 samples
            if self.n_trained % 50 == 0:
                self._update_ensemble_weights()

        except Exception as e:
            logger.error("Learn error: %s", e)

    # ------------------------------------------------------------------
    # Ensemble weight update
    # ------------------------------------------------------------------

    def _update_ensemble_weights(self):
        """Softmax of recent per-model accuracy to compute ensemble weights."""
        arf_acc = (
            np.mean(self._recent_correct["arf"][-100:])
            if self._recent_correct["arf"]
            else 0.5
        )
        lr_acc = (
            np.mean(self._recent_correct["lr"][-100:])
            if self._recent_correct["lr"]
            else 0.5
        )
        total = arf_acc + lr_acc + 1e-8
        self._weights = [arf_acc / total, lr_acc / total]
        logger.debug(
            "Ensemble weights updated: arf=%.3f lr=%.3f", self._weights[0], self._weights[1]
        )

    # ------------------------------------------------------------------
    # Feature importance
    # ------------------------------------------------------------------

    def get_feature_importance(self) -> Dict[str, float]:
        if not RIVER_AVAILABLE or self.arf is None:
            return {}
        try:
            importances = self.arf.feature_importances
            return {str(k): float(v) for k, v in enumerate(importances)}
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str):
        """Persist the online learner state to disk."""
        state = {
            "n_trained": self.n_trained,
            "weights": self._weights,
            "drift_events": self.drift_events,
        }
        payload: dict = {"state": state}
        if RIVER_AVAILABLE and self.arf is not None:
            payload["arf"] = self.arf
            payload["lr"] = self.lr
            payload["scaler"] = self.scaler
        joblib.dump(payload, path)
        logger.info("OnlineLearner saved to %s (n_trained=%d)", path, self.n_trained)

    def load(self, path: str):
        """Restore the online learner state from disk."""
        try:
            data = joblib.load(path)
            if RIVER_AVAILABLE:
                self.arf = data.get("arf", self.arf)
                self.lr = data.get("lr", self.lr)
                self.scaler = data.get("scaler", self.scaler)
            state = data.get("state", {})
            self.n_trained = state.get("n_trained", 0)
            self._weights = state.get("weights", [0.7, 0.3])
            self.drift_events = state.get("drift_events", [])
            logger.info(
                "OnlineLearner loaded from %s (n_trained=%d)", path, self.n_trained
            )
        except Exception as e:
            logger.error("Failed to load OnlineLearner: %s", e)
