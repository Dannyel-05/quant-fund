"""
Risk manager: position sizing (Kelly), ATR stops, portfolio-level limits.
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class RiskManager:
    # Regime multipliers imported from fred_collector constants
    _REGIME_MULTIPLIERS = {
        0: {"long_multiplier": 1.2, "short_multiplier": 0.9},   # RISK_ON
        1: {"long_multiplier": 1.0, "short_multiplier": 1.0},   # GOLDILOCKS
        2: {"long_multiplier": 1.0, "short_multiplier": 1.0},   # STAGFLATION
        3: {"long_multiplier": 0.7, "short_multiplier": 1.2},   # RISK_OFF
        4: {"long_multiplier": 0.5, "short_multiplier": 0.5},   # RECESSION_RISK
    }

    def __init__(self, config: dict):
        self.config = config
        r = config["risk"]
        self.max_pos_pct = r["max_position_pct"]          # 0.05
        self.max_sector_pct = r["max_sector_exposure_pct"]  # 0.25
        self.max_market_pct = r["max_market_exposure_pct"]  # 0.60
        self.max_positions = r["max_total_positions"]       # 20
        self.halt_drawdown = r["max_drawdown_halt_pct"]     # 0.15
        self.kelly_fraction = r["kelly_fraction"]           # 0.5
        self.corr_limit = r["correlation_limit"]            # 0.75
        self.atr_mult = r["atr_stop_multiplier"]            # 2.0
        self._macro_regime: int = 0  # default RISK_ON; updated via set_macro_regime()
        self._frontier_store = None   # cached; avoids per-ticker DB re-init

    def set_macro_regime(self, regime: int) -> None:
        """Update the active macro regime (0=RISK_ON … 4=RECESSION_RISK)."""
        if regime in self._REGIME_MULTIPLIERS:
            self._macro_regime = regime
            logger.info("RiskManager: macro regime set to %d", regime)
        else:
            logger.warning("RiskManager: unknown regime %d — ignoring", regime)

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    def size_position(
        self,
        ticker: str,
        signal_strength: float,
        portfolio: Dict,
        price_data: pd.DataFrame,
        win_rate: float = 0.55,
        avg_win_loss: float = 1.5,
        direction: int = 1,
    ) -> float:
        """
        Returns position size as a fraction of portfolio value.
        Returns 0.0 if any risk limit blocks the trade.

        Applies macro regime multiplier (set via set_macro_regime()) and
        optionally caps size using LiquidityScorer if price_data is provided.
        """
        if not self.check_limits(portfolio):
            return 0.0

        # Apply UMCI complexity multiplier (fail-open)
        umci_multiplier = 1.0
        try:
            from frontier.storage.frontier_store import FrontierStore
            if self._frontier_store is None:
                self._frontier_store = FrontierStore(self.config)
            fs = self._frontier_store
            last_umci = fs.get_last_umci()
            if last_umci is not None:
                score = last_umci.get("umci", 50)
                if score < 30:
                    umci_multiplier = 1.0
                elif score < 60:
                    umci_multiplier = 0.85
                elif score < 80:
                    umci_multiplier = 0.65
                elif score < 95:
                    umci_multiplier = 0.30
                else:
                    umci_multiplier = 0.10
        except Exception:
            pass  # fail open

        # Half-Kelly scaled by |signal_strength| (z-score clipped to [0,1])
        strength = min(abs(signal_strength), 3.0) / 3.0
        size = self.kelly_size(win_rate, avg_win_loss) * strength

        # Hard cap
        size = min(size, self.max_pos_pct)

        # ── Macro regime multiplier ─────────────────────────────────────
        mults = self._REGIME_MULTIPLIERS.get(self._macro_regime, {"long_multiplier": 1.0, "short_multiplier": 1.0})
        regime_mult = mults["long_multiplier"] if direction >= 0 else mults["short_multiplier"]
        size = size * regime_mult

        # Sector concentration cap
        sector = portfolio.get("sectors", {}).get(ticker, "Unknown")
        current_sector = portfolio.get("sector_exposures", {}).get(sector, 0.0)
        size = min(size, max(0.0, self.max_sector_pct - current_sector))

        # Max-positions cap
        if len(portfolio.get("positions", {})) >= self.max_positions:
            logger.warning("Max positions reached; skipping %s", ticker)
            return 0.0

        # ── Liquidity cap (optional — requires price_data) ─────────────
        if price_data is not None and not price_data.empty and size > 0:
            try:
                from deepdata.microstructure.liquidity_scorer import LiquidityScorer
                capital_proxy = portfolio.get("capital", 1_000_000)
                proposed_usd = size * capital_proxy
                liquidity = LiquidityScorer().score(
                    ticker, price_data, proposed_usd, expected_edge_pct=0.005
                )
                feasibility = liquidity.get("feasibility", "FEASIBLE")
                if feasibility == "REJECT":
                    logger.debug("LiquidityScorer: REJECT %s", ticker)
                    return 0.0
                if feasibility == "REDUCE_SIZE":
                    recommended = liquidity.get("recommended_size", proposed_usd)
                    size = min(size, recommended / capital_proxy)
                    logger.debug(
                        "LiquidityScorer: REDUCE_SIZE %s → %.4f", ticker, size
                    )
            except Exception as exc:
                logger.debug("LiquidityScorer unavailable for %s: %s", ticker, exc)

        # Apply UMCI multiplier (computed above, fail-open)
        size = size * umci_multiplier

        return round(max(0.0, size), 4)

    def kelly_size(self, win_rate: float, win_loss_ratio: float) -> float:
        """Full Kelly f* = (b*p - q) / b, then multiply by kelly_fraction."""
        b = max(win_loss_ratio, 1e-6)
        p = np.clip(win_rate, 0, 1)
        q = 1 - p
        kelly = (b * p - q) / b
        return max(0.0, kelly * self.kelly_fraction)

    # ------------------------------------------------------------------
    # Stop losses
    # ------------------------------------------------------------------

    def atr_stop(self, price_data: pd.DataFrame, window: int = 14) -> Optional[float]:
        """Returns ATR-based stop distance in price units."""
        if len(price_data) < window + 1:
            return None
        h = price_data["high"]
        lo = price_data["low"]
        c = price_data["close"]
        tr = pd.concat(
            [h - lo, (h - c.shift()).abs(), (lo - c.shift()).abs()], axis=1
        ).max(axis=1)
        atr = tr.rolling(window).mean().iloc[-1]
        return float(atr * self.atr_mult)

    # ------------------------------------------------------------------
    # Portfolio checks
    # ------------------------------------------------------------------

    def check_limits(self, portfolio: Dict) -> bool:
        if portfolio.get("drawdown", 0.0) <= -self.halt_drawdown:
            logger.warning(
                "Drawdown halt: %.1f%%", portfolio.get("drawdown", 0) * 100
            )
            return False
        if abs(portfolio.get("net_exposure", 0.0)) >= self.max_market_pct:
            logger.warning(
                "Max market exposure reached: %.1f%%",
                portfolio.get("net_exposure", 0) * 100,
            )
            return False
        return True

    def correlation_ok(
        self,
        new_returns: pd.Series,
        portfolio_returns: Dict[str, pd.Series],
    ) -> bool:
        for ticker, existing in portfolio_returns.items():
            aligned = new_returns.align(existing, join="inner")
            if len(aligned[0]) < 20:
                continue
            if abs(aligned[0].corr(aligned[1])) > self.corr_limit:
                logger.debug(
                    "High correlation with existing position %s", ticker
                )
                return False
        return True

    def portfolio_stats(
        self, positions: Dict[str, float], prices: Dict[str, float]
    ) -> Dict:
        long_val = sum(v * prices.get(t, 0) for t, v in positions.items() if v > 0)
        short_val = sum(abs(v) * prices.get(t, 0) for t, v in positions.items() if v < 0)
        gross = long_val + short_val
        net = long_val - short_val
        return {
            "gross_exposure": gross,
            "net_exposure": net,
            "long_exposure": long_val,
            "short_exposure": short_val,
            "n_positions": len(positions),
        }
