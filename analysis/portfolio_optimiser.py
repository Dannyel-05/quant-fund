"""
PortfolioOptimiser — minimum-variance portfolio construction using
Ledoit-Wolf covariance shrinkage.

Ledoit-Wolf shrinkage:
  Sigma_shrunk = (1-alpha) * Sigma_sample + alpha * mu * I
  where alpha is the analytically optimal shrinkage coefficient.

Provides:
  - get_minimum_variance_weights()  — minimum-variance portfolio weights
  - get_portfolio_var()             — expected portfolio variance
  - get_marginal_risk_contribution() — each position's marginal risk contribution
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from sklearn.covariance import LedoitWolf as _LW
    LEDOIT_WOLF_AVAILABLE = True
except ImportError:
    _LW = None
    LEDOIT_WOLF_AVAILABLE = False
    logger.warning("sklearn not installed — LedoitWolf unavailable, falling back to equal weights")

try:
    import scipy.optimize as _opt
    SCIPY_OPT_AVAILABLE = True
except ImportError:
    _opt = None
    SCIPY_OPT_AVAILABLE = False


class PortfolioOptimiser:
    """
    Ledoit-Wolf shrinkage covariance + minimum-variance portfolio construction.

    Usage:
        opt = PortfolioOptimiser()
        weights = opt.get_minimum_variance_weights(returns_df)  # dict ticker->weight
        var     = opt.get_portfolio_var(weights, returns_df)
        mrc     = opt.get_marginal_risk_contribution(weights, returns_df)
    """

    def __init__(self) -> None:
        self._lw = _LW(assume_centered=False) if LEDOIT_WOLF_AVAILABLE else None

    # ------------------------------------------------------------------
    def _shrunk_cov(self, returns_df: pd.DataFrame) -> np.ndarray:
        """
        Compute Ledoit-Wolf shrunk covariance matrix.
        Falls back to sample covariance if sklearn unavailable.
        """
        R = returns_df.dropna().values
        if len(R) < 5:
            return np.eye(returns_df.shape[1])
        if LEDOIT_WOLF_AVAILABLE:
            try:
                self._lw.fit(R)
                return self._lw.covariance_
            except Exception as exc:
                logger.debug("LedoitWolf fit failed: %s", exc)
        return np.cov(R.T)

    # ------------------------------------------------------------------
    def get_minimum_variance_weights(
        self,
        returns_df: pd.DataFrame,
        long_only: bool = True,
        max_weight: float = 0.30,
    ) -> Dict[str, float]:
        """
        Compute minimum-variance portfolio weights.

        Args:
            returns_df: DataFrame with tickers as columns, daily returns as rows.
            long_only:  If True, no negative weights (no shorts in portfolio).
            max_weight: Maximum weight per ticker (default 30%).

        Returns:
            dict of ticker -> weight (sum to 1.0).
        """
        tickers = list(returns_df.columns)
        n = len(tickers)

        if n == 0:
            return {}
        if n == 1:
            return {tickers[0]: 1.0}

        cov = self._shrunk_cov(returns_df)

        # Regularise to ensure positive-definiteness
        cov += np.eye(n) * 1e-8

        if not SCIPY_OPT_AVAILABLE:
            # Equal weights fallback
            w = 1.0 / n
            return {t: w for t in tickers}

        # Objective: minimise w^T Sigma w
        def portfolio_var(w):
            return float(w @ cov @ w)

        def portfolio_var_grad(w):
            return 2 * cov @ w

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bounds = [(0.0 if long_only else -max_weight, max_weight)] * n
        w0 = np.full(n, 1.0 / n)

        try:
            result = _opt.minimize(
                portfolio_var, w0,
                jac=portfolio_var_grad,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"ftol": 1e-9, "maxiter": 1000},
            )
            if result.success:
                weights = result.x
                # Clip tiny weights
                weights[weights < 1e-4] = 0.0
                total = weights.sum()
                if total > 0:
                    weights /= total
                return {t: float(w) for t, w in zip(tickers, weights)}
        except Exception as exc:
            logger.debug("MinVar optimisation failed: %s", exc)

        # Fallback: equal weights
        w = 1.0 / n
        return {t: w for t in tickers}

    # ------------------------------------------------------------------
    def get_portfolio_var(
        self,
        weights: Dict[str, float],
        returns_df: pd.DataFrame,
    ) -> float:
        """Expected annualised portfolio variance (assumes 252 trading days)."""
        tickers = [t for t in weights if t in returns_df.columns]
        if not tickers:
            return 0.0
        w = np.array([weights[t] for t in tickers])
        cov = self._shrunk_cov(returns_df[tickers])
        return float(w @ cov @ w) * 252

    # ------------------------------------------------------------------
    def get_marginal_risk_contribution(
        self,
        weights: Dict[str, float],
        returns_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        Marginal Risk Contribution = w_i * (Sigma*w)_i / (w^T Sigma w)
        Sums to 1.0.
        """
        tickers = [t for t in weights if t in returns_df.columns]
        if not tickers:
            return {}
        w = np.array([weights[t] for t in tickers])
        cov = self._shrunk_cov(returns_df[tickers])
        sigma_w    = cov @ w
        total_var  = float(w @ sigma_w)
        if total_var < 1e-12:
            return {t: 1.0 / len(tickers) for t in tickers}
        mrc = {t: float(w[i] * sigma_w[i] / total_var) for i, t in enumerate(tickers)}
        return mrc

    # ------------------------------------------------------------------
    def mahalanobis_outlier_score(
        self,
        observation: np.ndarray,
        returns_df: pd.DataFrame,
    ) -> float:
        """
        Mahalanobis distance of observation from the returns distribution.
        High score = outlier (unusual regime). Uses shrunk covariance.
        """
        R = returns_df.dropna().values
        if len(R) < 5:
            return 0.0
        cov = self._shrunk_cov(returns_df)
        mean = R.mean(axis=0)
        try:
            cov_inv = np.linalg.pinv(cov)
            diff    = observation - mean
            dist    = float(np.sqrt(diff @ cov_inv @ diff))
            return dist
        except Exception:
            return 0.0
