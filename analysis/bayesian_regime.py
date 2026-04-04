"""
BayesianRegimeDetector — probabilistic regime detection using a
Gaussian mixture model with EM fitting as a lightweight alternative
to full PyMC MCMC (which is too slow for production use).

Falls back to a fast sklearn GaussianMixture (4 components) rather than
full PyMC MCMC — this gives probabilistic regime probabilities without the
multi-minute fitting cost. PyMC is only used for offline analysis.

States: CRISIS | BEAR | NEUTRAL | BULL

Features: SPY daily return, VIX level, yield_curve_slope

Usage:
    brd = BayesianRegimeDetector()
    probs = brd.get_regime_probabilities()  # dict: state->probability
    regime = brd.get_regime()               # most likely state
    multiplier = brd.position_size_multiplier()
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

try:
    from sklearn.mixture import GaussianMixture as _GMM
    GMM_AVAILABLE = True
except ImportError:
    _GMM = None
    GMM_AVAILABLE = False
    logger.warning("sklearn not installed — BayesianRegimeDetector will use fallback")

# State ordering by mean return (ascending: CRISIS->BEAR->NEUTRAL->BULL)
_STATE_LABELS = ["CRISIS", "BEAR", "NEUTRAL", "BULL"]

# Position size multipliers per state (same as RegimeDetector for consistency)
_SIZE_MULTIPLIERS = {"BULL": 1.00, "NEUTRAL": 0.85, "BEAR": 0.60, "CRISIS": 0.30}


class BayesianRegimeDetector:
    """
    Probabilistic regime detector using Gaussian Mixture Model.

    Outputs a probability vector [p_crisis, p_bear, p_neutral, p_bull]
    rather than a hard binary classification.

    Comparison mode: runs in parallel with RegimeDetector and logs
    agreement/disagreement for monitoring.
    """

    def __init__(self) -> None:
        self._gmm: Optional[object] = None
        self._fitted = False
        self._last_probs: Dict[str, float] = {s: 0.25 for s in _STATE_LABELS}
        self._last_regime: str = "NEUTRAL"
        self._comparison_log: list = []
        self._component_order: list = [0, 1, 2, 3]

    # ------------------------------------------------------------------
    def _fetch_features(self, lookback: int = 252) -> Optional[np.ndarray]:
        """
        Fetch feature matrix: [spy_return, vix, yield_curve_slope] per day.
        Returns (N, 3) array or None.
        """
        try:
            import yfinance as yf
            import pandas as pd
            end   = date.today()
            start = end - timedelta(days=lookback + 30)

            spy = yf.download("SPY", start=str(start), end=str(end),
                               progress=False, auto_adjust=True, threads=False)
            vix = yf.download("^VIX", start=str(start), end=str(end),
                               progress=False, auto_adjust=True, threads=False)
            # Yield curve: 10Y - 2Y (use TNX proxy)
            try:
                t10 = yf.download("^TNX", start=str(start), end=str(end),
                                   progress=False, auto_adjust=True, threads=False)
                t2  = yf.download("^IRX", start=str(start), end=str(end),
                                   progress=False, auto_adjust=True, threads=False)
            except Exception:
                t10, t2 = None, None

            spy_ret = spy["Close"].pct_change().dropna() if spy is not None and len(spy) > 1 else pd.Series(dtype=float)
            vix_cl  = vix["Close"].dropna() if vix is not None and len(vix) > 0 else pd.Series(dtype=float)

            # Handle MultiIndex columns from yfinance
            if hasattr(spy_ret, 'columns'):
                spy_ret = spy_ret.iloc[:, 0]
            if hasattr(vix_cl, 'columns'):
                vix_cl = vix_cl.iloc[:, 0]

            # Align indices
            idx = spy_ret.index.intersection(vix_cl.index)
            if len(idx) < 30:
                return None

            spy_ret = spy_ret.loc[idx].values
            vix_cl  = vix_cl.loc[idx].values

            # Yield curve slope
            if t10 is not None and t2 is not None and len(t10) > 10 and len(t2) > 10:
                t10_c = t10["Close"]
                t2_c  = t2["Close"]
                if hasattr(t10_c, 'columns'):
                    t10_c = t10_c.iloc[:, 0]
                if hasattr(t2_c, 'columns'):
                    t2_c = t2_c.iloc[:, 0]
                t10_cl = t10_c.dropna().reindex(pd.Index(idx), method="ffill").fillna(0).values
                t2_cl  = t2_c.dropna().reindex(pd.Index(idx), method="ffill").fillna(0).values
                yc     = t10_cl - t2_cl
            else:
                yc = np.zeros(len(spy_ret))

            features = np.column_stack([spy_ret, vix_cl, yc])
            return features

        except Exception as exc:
            logger.debug("BayesianRegimeDetector._fetch_features: %s", exc)
            return None

    # ------------------------------------------------------------------
    def fit(self, features: Optional[np.ndarray] = None) -> bool:
        """
        Fit a 4-component Gaussian Mixture Model on the feature data.
        Returns True on success.
        """
        if not GMM_AVAILABLE:
            return False

        if features is None:
            features = self._fetch_features()

        if features is None or len(features) < 30:
            return False

        try:
            gmm = _GMM(
                n_components=4,
                covariance_type="full",
                max_iter=200,
                random_state=42,
                n_init=5,
            )
            gmm.fit(features)

            # Sort components by mean SPY return (col 0): ascending -> CRISIS->BEAR->NEUTRAL->BULL
            order = np.argsort(gmm.means_[:, 0])
            self._gmm = gmm
            self._component_order = order.tolist()
            self._fitted = True
            logger.info("BayesianRegimeDetector: fitted 4-component GMM on %d observations", len(features))
            return True
        except Exception as exc:
            logger.warning("BayesianRegimeDetector.fit failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    def get_regime_probabilities(
        self, observation: Optional[np.ndarray] = None
    ) -> Dict[str, float]:
        """
        Get regime probabilities for current market state.
        Returns dict: state -> probability (sum to 1.0).
        """
        if not self._fitted:
            success = self.fit()
            if not success:
                return {s: 0.25 for s in _STATE_LABELS}

        if observation is None:
            features = self._fetch_features(lookback=5)
            if features is None or len(features) == 0:
                return self._last_probs
            observation = features[-1].reshape(1, -1)
        else:
            observation = np.array(observation).reshape(1, -1)

        try:
            raw_probs = self._gmm.predict_proba(observation)[0]   # (4,) — per component
            # Reorder to CRISIS, BEAR, NEUTRAL, BULL
            ordered = np.zeros(4)
            for label_idx, comp_idx in enumerate(self._component_order):
                ordered[label_idx] = raw_probs[comp_idx]
            # Normalise
            total = ordered.sum()
            if total > 0:
                ordered /= total
            probs = {s: float(ordered[i]) for i, s in enumerate(_STATE_LABELS)}
            self._last_probs  = probs
            self._last_regime = max(probs, key=probs.get)
            return probs
        except Exception as exc:
            logger.debug("BayesianRegimeDetector.get_regime_probabilities: %s", exc)
            return self._last_probs

    # ------------------------------------------------------------------
    def get_regime(self) -> str:
        """Return the most probable regime state."""
        probs = self.get_regime_probabilities()
        return max(probs, key=probs.get)

    # ------------------------------------------------------------------
    def position_size_multiplier(self) -> float:
        """
        Probability-weighted position size multiplier.
        Rather than a hard threshold, uses expected multiplier:
          E[multiplier] = sum p(state) * multiplier(state)
        """
        probs = self.get_regime_probabilities()
        expected = sum(probs[s] * _SIZE_MULTIPLIERS[s] for s in _STATE_LABELS)
        return float(np.clip(expected, 0.0, 1.0))

    # ------------------------------------------------------------------
    def compare_with_rule_based(self) -> Dict:
        """
        Run both this detector and the rule-based RegimeDetector.
        Log agreement/disagreement. Return comparison dict.
        """
        try:
            from analysis.regime_detector import RegimeDetector
            rd = RegimeDetector()
            rule_regime = rd.get_current_regime()
        except Exception:
            rule_regime = "UNKNOWN"

        bayes_regime = self.get_regime()
        bayes_probs  = self._last_probs
        agree        = (rule_regime == bayes_regime)

        result = {
            "rule_based":   rule_regime,
            "bayesian":     bayes_regime,
            "bayesian_probs": bayes_probs,
            "agree":        agree,
        }

        if not agree:
            logger.info(
                "RegimeDetector DISAGREE: rule=%s bayesian=%s probs=%s",
                rule_regime, bayes_regime,
                {s: f"{p:.2f}" for s, p in bayes_probs.items()},
            )
        self._comparison_log.append(result)
        if len(self._comparison_log) > 100:
            self._comparison_log = self._comparison_log[-100:]

        return result

    # ------------------------------------------------------------------
    def agreement_rate(self) -> float:
        """Return fraction of comparisons where both detectors agreed."""
        if not self._comparison_log:
            return 0.0
        return sum(1 for r in self._comparison_log if r["agree"]) / len(self._comparison_log)
