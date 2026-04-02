"""RiskDecomposer — decomposes portfolio risk by factor."""

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RiskDecomposer:
    """Decomposes portfolio risk by factor."""

    COV_WINDOW = 252

    def __init__(self, config: dict):
        self.config = config or {}
        self.concentration_threshold = self.config.get("concentration_threshold", 0.4)
        self.cov_window = self.config.get("cov_window", self.COV_WINDOW)

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def decompose(
        self,
        portfolio_weights: dict,
        factor_matrix: "pd.DataFrame",
        factor_cov_matrix: "pd.DataFrame" = None,
    ) -> dict:
        """
        Returns factor contribution breakdown.
        """
        empty = {
            "factor_contributions": {},
            "idiosyncratic_risk": 0.0,
            "total_risk": 0.0,
            "factor_tilts": {},
            "concentration_risk": "LOW",
        }
        if not portfolio_weights or factor_matrix is None:
            return empty
        if not HAS_PANDAS:
            logger.warning("pandas not available; risk decomposition unavailable")
            return empty

        try:
            tickers = [t for t in portfolio_weights if t in factor_matrix.index]
            if not tickers:
                return empty

            weights = np.array([portfolio_weights[t] for t in tickers], dtype=float)
            w_sum = weights.sum()
            if w_sum > 0:
                weights /= w_sum

            B = factor_matrix.loc[tickers].fillna(0.0).values  # (n_stocks, n_factors)
            factor_names = list(factor_matrix.columns)

            # Portfolio factor exposures: w^T * B  (1, n_factors)
            port_exposures = weights @ B  # shape: (n_factors,)

            # Factor covariance
            if factor_cov_matrix is not None:
                F = factor_cov_matrix.values
            else:
                # Identity scaled by average variance as fallback
                n_factors = len(factor_names)
                F = np.eye(n_factors) * 0.01

            # Factor variance: e^T * F * e
            factor_var_total = float(port_exposures @ F @ port_exposures)

            # Per-factor contribution
            factor_contributions = {}
            for i, fname in enumerate(factor_names):
                e_i = np.zeros(len(factor_names))
                e_i[i] = port_exposures[i]
                contrib = float(e_i @ F @ port_exposures)
                factor_contributions[fname] = contrib

            # Idiosyncratic risk (residual)
            # Approximate: total variance = factor_var + idio_var
            # For idio, we'd need residual variance per stock; here use a simple estimate
            total_factor_var = factor_var_total
            # Rough total variance from factor_matrix diagonal volatilities
            try:
                stock_vars = np.var(B, axis=1)  # per-stock variance across factors
                idio_var = float(np.dot(weights ** 2, stock_vars))
            except Exception:
                idio_var = total_factor_var * 0.3  # rough 30% idio

            total_var = total_factor_var + idio_var
            total_risk = math.sqrt(max(total_var, 0.0))
            idio_risk = math.sqrt(max(idio_var, 0.0))

            # Factor tilts: significant deviations
            factor_tilts = {}
            for fname, contrib in factor_contributions.items():
                if total_var > 0 and abs(contrib / total_var) > 0.15:
                    factor_tilts[fname] = {
                        "contribution_pct": contrib / total_var,
                        "exposure": float(port_exposures[factor_names.index(fname)]),
                    }

            concentration_risk = self.identify_concentration(factor_contributions, self.concentration_threshold)

            return {
                "factor_contributions": factor_contributions,
                "idiosyncratic_risk": idio_risk,
                "total_risk": total_risk,
                "factor_tilts": factor_tilts,
                "concentration_risk": concentration_risk,
            }
        except Exception as exc:
            logger.warning("RiskDecomposer.decompose failed: %s", exc)
            return empty

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def calc_factor_covariance(self, factor_returns: "pd.DataFrame") -> "pd.DataFrame":
        """Covariance matrix of factor returns. Use 252-day window."""
        if not HAS_PANDAS or factor_returns is None:
            return None
        try:
            tail = factor_returns.tail(self.cov_window)
            return tail.cov()
        except Exception as exc:
            logger.warning("calc_factor_covariance failed: %s", exc)
            return None

    def identify_concentration(self, factor_contributions: dict, threshold: float = 0.4) -> str:
        """Flag if any single factor contributes > threshold of total risk."""
        if not factor_contributions:
            return "LOW"
        try:
            total = sum(abs(v) for v in factor_contributions.values())
            if total == 0:
                return "LOW"
            for fname, contrib in factor_contributions.items():
                if abs(contrib) / total > threshold:
                    return "HIGH"
            # Check moderate: any factor > threshold/2
            for fname, contrib in factor_contributions.items():
                if abs(contrib) / total > threshold / 2:
                    return "MODERATE"
            return "LOW"
        except Exception as exc:
            logger.warning("identify_concentration error: %s", exc)
            return "LOW"

    def suggest_hedges(self, factor_tilts: dict) -> list:
        """Suggest offsetting positions to reduce factor concentration."""
        if not factor_tilts:
            return []
        suggestions = []
        try:
            HEDGE_MAP = {
                "momentum": "Consider short high-momentum ETF (e.g., MTUM short) or momentum-neutral rebalance",
                "value": "Consider long growth stocks or growth ETF to offset value tilt",
                "size": "Consider large-cap allocation to offset small-cap concentration",
                "volatility": "Consider VIX futures or low-vol ETF short to offset vol factor tilt",
                "quality": "Consider cyclical/low-quality basket long to offset quality concentration",
                "congressional": "Diversify away from politically-exposed names",
                "supply_chain": "Hedge via sector ETFs with lower supply-chain risk",
                "altdata": "Reduce positions with high altdata concentration",
                "earnings_quality": "Consider names with lower earnings quality to neutralise",
            }
            for fname, tilt_info in factor_tilts.items():
                hedge_text = HEDGE_MAP.get(fname, f"Reduce exposure to {fname} factor")
                suggestions.append(
                    {
                        "factor": fname,
                        "current_exposure": tilt_info.get("exposure", 0.0),
                        "contribution_pct": tilt_info.get("contribution_pct", 0.0),
                        "suggested_hedge": hedge_text,
                    }
                )
        except Exception as exc:
            logger.warning("suggest_hedges error: %s", exc)
        return suggestions
