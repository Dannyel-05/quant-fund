import logging
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class WeeklyRetrainer:
    def __init__(self, config: dict, store, registry):
        self.config = config
        self.store = store
        self.registry = registry
        cfg = (
            config.get("altdata", {})
            .get("learning", {})
            .get("weekly_retrain", {})
        )
        self.lookback_days = cfg.get("lookback_days", 365)
        self.val_split = cfg.get("validation_split", 0.2)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self, tickers: list, market: str) -> Dict:
        """
        Full retrain pipeline. Returns report dict.

        Steps:
          1. Load data from store
          2. Time-series train/val split (no shuffling)
          3. Feature selection (near-zero variance threshold)
          4. Standardise features
          5. Train candidate models (RF, GBM, Logistic)
          6. Build ensemble with Sharpe-weighted voting
          7. Compare to current live model; promote if >= 90 % of current Sharpe
          8. Persist via registry
        """
        logger.info(
            "Starting weekly retraining for %d tickers [%s]", len(tickers), market
        )

        # 1. Load data -------------------------------------------------
        X, y, _weights = self._load_training_data(tickers, market)
        if X is None or len(X) < 100:
            n = len(X) if X is not None else 0
            logger.warning("Insufficient training data (%d samples)", n)
            return {"status": "skipped", "reason": "insufficient_data"}

        # 2. Time-series train/val split (NO shuffling) ----------------
        n = len(X)
        val_start = int(n * (1 - self.val_split))
        X_train, y_train = X.iloc[:val_start], y.iloc[:val_start]
        X_val, y_val = X.iloc[val_start:], y.iloc[val_start:]

        # 3. Feature selection: drop near-zero variance ----------------
        from sklearn.feature_selection import VarianceThreshold

        selector = VarianceThreshold(threshold=0.01)
        X_train_sel = selector.fit_transform(X_train)
        X_val_sel = selector.transform(X_val)
        support = selector.get_support()
        feature_names = [
            X_train.columns[i]
            for i in range(len(X_train.columns))
            if support[i]
        ]

        # 4. Standardise -----------------------------------------------
        scaler = StandardScaler()
        X_train_sc = scaler.fit_transform(X_train_sel)
        X_val_sc = scaler.transform(X_val_sel)

        # 5. Train candidate models ------------------------------------
        models = {
            "random_forest": RandomForestClassifier(
                n_estimators=200, max_depth=8, random_state=42, n_jobs=-1
            ),
            "gradient_boost": GradientBoostingClassifier(
                n_estimators=100, max_depth=4, random_state=42
            ),
            "logistic": LogisticRegression(
                C=1.0, max_iter=500, random_state=42
            ),
        }

        trained_models: Dict = {}
        val_metrics: Dict = {}

        for name, model in models.items():
            try:
                model.fit(X_train_sc, y_train)
                preds = model.predict(X_val_sc)
                acc = accuracy_score(y_val, preds)

                val_returns = self._compute_signal_returns(preds, y_val)
                if val_returns.std() > 0:
                    sharpe = float(
                        val_returns.mean() / val_returns.std() * np.sqrt(252)
                    )
                else:
                    sharpe = 0.0

                trained_models[name] = model
                val_metrics[name] = {"accuracy": float(acc), "sharpe": sharpe}
                logger.info(
                    "  %s: acc=%.3f sharpe=%.2f", name, acc, sharpe
                )
            except Exception as e:
                logger.error("  %s failed: %s", name, e)

        if not trained_models:
            return {"status": "failed", "reason": "all_models_failed"}

        # 6. Ensemble weights by val Sharpe (softmax) ------------------
        sharpes = np.array([val_metrics[n]["sharpe"] for n in trained_models])
        sharpes_clipped = np.clip(sharpes, 0, None)
        ensemble_weights = sharpes_clipped / (sharpes_clipped.sum() + 1e-8)

        # 7. Ensemble validation Sharpe --------------------------------
        ensemble_preds = np.zeros(len(X_val_sc))
        for i, (name, model) in enumerate(trained_models.items()):
            proba = model.predict_proba(X_val_sc)[:, 1]
            ensemble_preds += ensemble_weights[i] * proba

        ensemble_dir = (ensemble_preds > 0.5).astype(int) * 2 - 1
        ens_returns = self._compute_signal_returns(ensemble_dir, y_val)
        if ens_returns.std() > 0:
            ens_sharpe = float(
                ens_returns.mean() / ens_returns.std() * np.sqrt(252)
            )
        else:
            ens_sharpe = 0.0

        logger.info("Ensemble validation Sharpe: %.2f", ens_sharpe)

        # 8. Compare to current model; promote if >= 90 % of current Sharpe
        current = self.registry.get_history("weekly_retrain")
        current_sharpe = (
            current[-1].get("metrics", {}).get("sharpe", 0) if current else 0
        )
        should_promote = ens_sharpe >= current_sharpe * 0.9

        # 9. Save via registry ----------------------------------------
        version = self.registry.save_model(
            "weekly_retrain",
            {
                "models": trained_models,
                "scaler": scaler,
                "selector": selector,
                "weights": ensemble_weights.tolist(),
                "features": feature_names,
            },
            {
                "sharpe": ens_sharpe,
                "n_samples": len(X),
                "trained_at": datetime.now().isoformat(),
            },
        )

        report = {
            "status": "promoted" if should_promote else "saved_not_promoted",
            "version": version,
            "ensemble_sharpe": ens_sharpe,
            "previous_sharpe": current_sharpe,
            "n_training_samples": len(X_train),
            "n_validation_samples": len(X_val),
            "model_metrics": val_metrics,
        }

        logger.info(
            "Weekly retrain complete: %s (sharpe=%.2f)",
            report["status"],
            ens_sharpe,
        )
        return report

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_training_data(self, tickers, market):
        """
        Load features and labels from store.
        Returns (X, y, weights) DataFrames, or (None, None, None) on failure.
        """
        end = datetime.now()
        start = end - timedelta(days=self.lookback_days)

        frames = []
        for ticker in tickers:
            try:
                df = self.store.get_features(
                    ticker,
                    start.isoformat(),
                    end.isoformat(),
                )
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception:
                continue

        if not frames:
            return None, None, None

        all_data = pd.concat(frames, ignore_index=True)

        # Target: 1 = positive / correct, 0 = negative / incorrect
        if "target" not in all_data.columns:
            if "return" in all_data.columns:
                all_data["target"] = (all_data["return"] > 0).astype(int)
            else:
                logger.warning(
                    "Neither 'target' nor 'return' column found in training data."
                )
                return None, None, None

        feature_cols = [
            c
            for c in all_data.columns
            if c not in ("ticker", "timestamp", "target", "return")
        ]
        X = all_data[feature_cols].fillna(0)
        y = all_data["target"]

        return X, y, None

    # ------------------------------------------------------------------
    # Return computation
    # ------------------------------------------------------------------

    def _compute_signal_returns(self, predictions, actuals) -> pd.Series:
        """
        Compute proxy returns from directional predictions.
        Correct prediction  -> +2 % return
        Incorrect prediction -> -2 % return
        """
        preds = pd.Series(predictions)
        acts = pd.Series(
            actuals.values if hasattr(actuals, "values") else actuals
        )
        correct = (preds == acts).astype(float)
        returns = correct * 0.02 - (1 - correct) * 0.02
        return returns
