"""ExecutionFeasibility — checks if a proposed trade is feasible before execution."""

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutionFeasibility:
    """Checks if a proposed trade is feasible before paper/live execution."""

    DEFAULT_WINDOW_MINUTES = 60
    DEFAULT_SLIPPAGE_PCT = 0.001

    def __init__(self, config: dict):
        self.config = config or {}
        self.default_window = self.config.get("execution_window_minutes", self.DEFAULT_WINDOW_MINUTES)
        self.max_adv_fraction = self.config.get("max_adv_fraction", 0.05)
        self.min_edge_margin = self.config.get("min_edge_margin", 1.2)  # edge must be 1.2x costs

    # ------------------------------------------------------------------
    # Main check
    # ------------------------------------------------------------------

    def check(
        self,
        ticker: str,
        signal: dict,
        price_data,
        account_value: float,
    ) -> dict:
        """
        Full feasibility check before paper/live trade.
        Returns feasibility assessment with execution plan.
        """
        empty = {
            "feasible": False,
            "adjusted_size": 0.0,
            "reasons": ["check_failed"],
            "execution_plan": {},
            "expected_slippage_pct": 0.0,
            "expected_market_impact_pct": 0.0,
        }
        if not signal or not ticker:
            return {**empty, "reasons": ["invalid_input"]}

        try:
            from deepdata.microstructure.liquidity_scorer import LiquidityScorer
            scorer = LiquidityScorer(self.config)
        except Exception as exc:
            logger.warning("LiquidityScorer import failed: %s", exc)
            scorer = None

        try:
            position_size_usd = signal.get("position_size_usd", account_value * 0.02)
            expected_edge_pct = signal.get("expected_edge_pct", 0.01)
            direction = signal.get("direction", 1)

            reasons = []
            slippage_pct = self.DEFAULT_SLIPPAGE_PCT

            # Get liquidity assessment
            liq_result = {}
            if scorer:
                liq_result = scorer.score(ticker, price_data, position_size_usd, expected_edge_pct)

            adv = liq_result.get("avg_daily_volume", 0.0)
            spread_pct = liq_result.get("spread_pct", 0.002)
            market_impact_pct = liq_result.get("market_impact_pct", 0.0)
            total_cost_pct = liq_result.get("total_cost_pct", spread_pct / 2 + slippage_pct)
            feasibility_label = liq_result.get("feasibility", "REJECT")

            # ADV size constraint
            adjusted_size = position_size_usd
            if adv > 0:
                max_size_usd = adv * self.max_adv_fraction
                if position_size_usd > max_size_usd:
                    adjusted_size = max_size_usd
                    reasons.append(f"size_reduced_adv_constraint: max={max_size_usd:.0f}")

            # Edge vs cost check
            edge_passes = self.validate_signal_edge(signal, {"total_cost_pct": total_cost_pct})
            if not edge_passes:
                reasons.append(f"edge_insufficient: cost={total_cost_pct:.4f} edge={expected_edge_pct:.4f}")
                feasibility_label = "REJECT"

            if feasibility_label == "REJECT":
                reasons.append("liquidity_scorer_reject")
                return {
                    "feasible": False,
                    "adjusted_size": 0.0,
                    "reasons": reasons,
                    "execution_plan": {},
                    "expected_slippage_pct": slippage_pct,
                    "expected_market_impact_pct": market_impact_pct,
                }

            if feasibility_label == "REDUCE_SIZE":
                recommended = liq_result.get("recommended_size", adjusted_size * 0.5)
                adjusted_size = min(adjusted_size, recommended)
                reasons.append(f"size_reduced_liquidity: new_size={adjusted_size:.0f}")

            # Build execution plan
            execution_plan = self.generate_execution_plan(
                ticker, direction, adjusted_size, self.default_window
            )

            return {
                "feasible": True,
                "adjusted_size": adjusted_size,
                "reasons": reasons,
                "execution_plan": execution_plan,
                "expected_slippage_pct": slippage_pct,
                "expected_market_impact_pct": market_impact_pct,
            }
        except Exception as exc:
            logger.warning("ExecutionFeasibility.check failed for %s: %s", ticker, exc)
            return empty

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def validate_signal_edge(self, signal: dict, costs: dict) -> bool:
        """Signal edge must exceed total execution costs."""
        try:
            edge = float(signal.get("expected_edge_pct", 0.0))
            total_cost = float(costs.get("total_cost_pct", 0.0))
            return edge > total_cost * self.min_edge_margin
        except Exception as exc:
            logger.warning("validate_signal_edge error: %s", exc)
            return False

    def generate_execution_plan(
        self,
        ticker: str,
        direction: int,
        size: float,
        window_minutes: int,
    ) -> dict:
        """
        VWAP execution plan: strategy, slices, limit_price, time_limit.
        """
        if size <= 0:
            return {}
        try:
            strategy = "VWAP"
            n_slices = max(1, window_minutes // 5)

            # Attempt VWAP schedule from liquidity scorer
            slices = []
            try:
                from deepdata.microstructure.liquidity_scorer import LiquidityScorer
                scorer = LiquidityScorer(self.config)
                slices = scorer.generate_vwap_schedule(ticker, size, window_minutes)
            except Exception:
                pass

            if not slices:
                # Uniform fallback
                pct = 1.0 / n_slices
                slices = [{"time_offset": i * 5, "size_pct": pct} for i in range(n_slices)]
            else:
                # Rename keys for consistency
                slices = [
                    {"time_offset": s.get("minute_offset", 0), "size_pct": s.get("order_size_pct", 0.0)}
                    for s in slices
                ]

            limit_price = self._get_limit_price(ticker, direction)

            return {
                "strategy": strategy,
                "slices": slices,
                "limit_price": limit_price,
                "time_limit_minutes": window_minutes,
            }
        except Exception as exc:
            logger.warning("generate_execution_plan failed for %s: %s", ticker, exc)
            return {
                "strategy": "MARKET",
                "slices": [],
                "limit_price": 0.0,
                "time_limit_minutes": window_minutes,
            }

    def _get_limit_price(self, ticker: str, direction: int) -> float:
        """Get approximate limit price from yfinance."""
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            fi = t.fast_info
            price = getattr(fi, "last_price", None) or getattr(fi, "previous_close", None)
            if price and price > 0:
                # Give 0.5% room for limit order
                slippage = 0.005 * direction
                return float(price * (1 + slippage))
        except Exception:
            pass
        return 0.0
