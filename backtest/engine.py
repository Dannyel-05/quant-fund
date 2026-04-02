"""
Vectorized backtesting engine.

Handles US and UK cost models (commission, slippage, stamp duty, borrow).
Position sizing uses a fixed fraction of capital (max_position_pct from risk config).
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TieredCostModel:
    """
    Realistic tiered transaction cost model.
    Costs are applied per round trip (entry + exit = 2x one-way costs).
    """

    US_TIERS = {
        'small_cap': {   # $50M-$500M
            'market_cap_max': 500_000_000,
            'commission': 1.00,
            'bid_ask_spread_pct': 0.005,   # 0.5% round trip
            'slippage_base_pct': 0.003,    # 0.3%
            'short_borrow_daily': 0.0003,  # ~10% annual
        },
        'mid_cap': {     # $500M-$2B
            'market_cap_max': 2_000_000_000,
            'commission': 1.00,
            'bid_ask_spread_pct': 0.002,
            'slippage_base_pct': 0.001,
            'short_borrow_daily': 0.0001,
        },
    }

    UK_TIERS = {
        'small_cap': {   # £30M-£300M (AIM)
            'market_cap_max': 300_000_000,
            'commission': 3.00,
            'bid_ask_spread_pct': 0.008,   # wider AIM spreads
            'slippage_base_pct': 0.005,
            'short_borrow_daily': 0.0002,
            'stamp_duty_pct': 0.005,       # 0.5% on buys
        },
        'mid_cap': {     # £300M-£1.5B
            'market_cap_max': 1_500_000_000,
            'commission': 3.00,
            'bid_ask_spread_pct': 0.003,
            'slippage_base_pct': 0.002,
            'short_borrow_daily': 0.0002,
            'stamp_duty_pct': 0.005,
        },
    }

    def _select_tier(self, market: str, market_cap: float) -> dict:
        """Select the appropriate cost tier based on market and market_cap."""
        tiers = self.US_TIERS if market == 'us' else self.UK_TIERS
        for tier_name in ('small_cap', 'mid_cap'):
            tier = tiers[tier_name]
            if market_cap <= tier['market_cap_max']:
                return tier
        # Default to mid_cap if above all thresholds
        return tiers['mid_cap']

    def get_costs(
        self,
        ticker: str,
        market: str,
        market_cap: float,
        order_value: float,
        avg_daily_volume: float,
        price: float,
        is_short: bool = False,
    ) -> Optional[dict]:
        """
        Calculate all-in one-way cost for a trade.

        Parameters
        ----------
        ticker            : ticker symbol (for logging)
        market            : 'us' or 'uk'
        market_cap        : market capitalisation in local currency
        order_value       : notional value of the order
        avg_daily_volume  : average daily *share* volume
        price             : execution price per share
        is_short          : True if this is a short position

        Returns
        -------
        dict with keys: commission, spread_cost, slippage, stamp_duty,
                        total_one_way_pct, impact_mult
        Returns None if the trade is too large (order > 15% of ADV).
        """
        tier = self._select_tier(market, market_cap)

        # Market-impact scaling
        adv_value = price * avg_daily_volume if avg_daily_volume > 0 else order_value
        order_pct_adv = order_value / adv_value if adv_value > 0 else 0.0

        # Reject if order exceeds 15% of ADV
        if order_pct_adv > 0.15:
            logger.debug(
                "Trade rejected for %s: order is %.1f%% of ADV (>15%%)",
                ticker, order_pct_adv * 100,
            )
            return None

        # Market-impact multiplier
        if order_pct_adv > 0.05:
            impact_mult = 1 + (order_pct_adv - 0.05) * 10
        else:
            impact_mult = 1.0

        slippage_pct = tier['slippage_base_pct'] * impact_mult

        # Spread cost (one-way = half the round-trip spread)
        spread_cost = order_value * (tier['bid_ask_spread_pct'] / 2)

        # Commission (fixed per trade)
        commission = tier['commission']

        # Slippage (one-way)
        slippage = order_value * slippage_pct

        # Stamp duty: UK buys only
        stamp_duty = 0.0
        if not is_short and 'stamp_duty_pct' in tier:
            stamp_duty = order_value * tier['stamp_duty_pct']

        total_cost = commission + spread_cost + slippage + stamp_duty
        total_one_way_pct = total_cost / order_value if order_value > 0 else 0.0

        return {
            'commission': commission,
            'spread_cost': spread_cost,
            'slippage': slippage,
            'stamp_duty': stamp_duty,
            'total_one_way_pct': total_one_way_pct,
            'impact_mult': impact_mult,
            'order_pct_adv': order_pct_adv,
        }

    @classmethod
    def summarise_cost_assumptions(cls) -> str:
        """Return a formatted string describing all cost tiers."""
        lines = [
            "=" * 60,
            "TIERED TRANSACTION COST MODEL — ASSUMPTIONS",
            "=" * 60,
            "",
            "US TIERS",
            "-" * 40,
        ]
        for tier_name, t in cls.US_TIERS.items():
            lines += [
                f"  {tier_name.replace('_', ' ').title()} (cap <= ${t['market_cap_max']:,.0f}):",
                f"    Commission        : ${t['commission']:.2f} / trade",
                f"    Bid-ask spread    : {t['bid_ask_spread_pct']*100:.3f}% (round trip)",
                f"    Slippage (base)   : {t['slippage_base_pct']*100:.3f}%",
                f"    Short borrow/day  : {t['short_borrow_daily']*100:.4f}%",
                "",
            ]

        lines += ["UK TIERS", "-" * 40]
        for tier_name, t in cls.UK_TIERS.items():
            lines += [
                f"  {tier_name.replace('_', ' ').title()} (cap <= £{t['market_cap_max']:,.0f}):",
                f"    Commission        : £{t['commission']:.2f} / trade",
                f"    Bid-ask spread    : {t['bid_ask_spread_pct']*100:.3f}% (round trip)",
                f"    Slippage (base)   : {t['slippage_base_pct']*100:.3f}%",
                f"    Short borrow/day  : {t['short_borrow_daily']*100:.4f}%",
                f"    Stamp duty (buys) : {t.get('stamp_duty_pct', 0)*100:.2f}%",
                "",
            ]

        lines += [
            "Market-impact scaling:",
            "  order_pct_adv > 5%  → impact_mult = 1 + (pct_adv - 0.05) × 10",
            "  order_pct_adv > 15% → trade REJECTED",
            "=" * 60,
        ]
        return "\n".join(lines)


class BacktestEngine:
    def __init__(self, config: dict):
        self.config = config
        self.costs = {
            "us": config["costs"]["us"],
            "uk": config["costs"]["uk"],
        }
        self.max_pos_pct = config["risk"]["max_position_pct"]
        self.initial_capital = config["backtest"]["initial_capital"]
        self.stop_loss_pct  = config["backtest"].get("stop_loss_pct", 0.25)
        self.cost_model = TieredCostModel()

    def run(
        self,
        signals: pd.DataFrame,
        price_data: Dict[str, pd.DataFrame],
        market: str = "us",
        initial_capital: float = None,
    ) -> Dict:
        """
        signals: DataFrame with [ticker, signal, entry_date, exit_date]
        price_data: {ticker: OHLCV DataFrame}
        Returns: {trades, equity_curve, metrics, market}
        """
        capital = initial_capital or self.initial_capital
        if signals.empty:
            return self._empty(market)

        trades = self._simulate(signals, price_data, market, capital)
        if trades.empty:
            return self._empty(market)

        equity = self._build_equity(trades, capital)
        metrics = self._metrics(equity, trades)
        metrics["cost_analysis"] = self._cost_analysis(trades, equity)

        return {"trades": trades, "equity_curve": equity, "metrics": metrics, "market": market}

    # ------------------------------------------------------------------

    def _simulate(
        self,
        signals: pd.DataFrame,
        price_data: Dict[str, pd.DataFrame],
        market: str,
        capital: float,
    ) -> pd.DataFrame:
        fallback_costs = self.costs[market]
        records = []

        for _, row in signals.iterrows():
            ticker = row["ticker"]
            if ticker not in price_data or price_data[ticker].empty:
                continue

            prices = price_data[ticker]
            direction = int(row["signal"])
            entry_date = row["entry_date"]
            exit_date = row.get("exit_date")

            entry_price = self._exec_price_raw(prices, entry_date)
            if entry_price is None:
                continue

            exit_date = self._resolve_exit(prices, entry_date, exit_date)
            if exit_date is None:
                continue

            # Stop-loss: scan daily closes between entry and planned exit;
            # exit early if price moves > stop_loss_pct against the position.
            stop_triggered = False
            if self.stop_loss_pct > 0:
                window = prices.loc[
                    (prices.index > entry_date) & (prices.index <= exit_date), "close"
                ]
                for day_ts, day_close in window.items():
                    move = (day_close - entry_price) / entry_price * direction
                    if move < -self.stop_loss_pct:
                        exit_date = day_ts
                        stop_triggered = True
                        break

            exit_price = self._exec_price_raw(prices, exit_date)
            if exit_price is None:
                continue

            pos_value = capital * self.max_pos_pct
            shares = pos_value / entry_price

            # ---- Tiered cost model ----------------------------------------
            market_cap = row.get("market_cap", None)
            avg_daily_volume = row.get("avg_daily_volume", None)

            # Try tiered model when we have market_cap and volume data
            cost_detail = None
            if market_cap is not None and avg_daily_volume is not None:
                try:
                    market_cap = float(market_cap)
                    avg_daily_volume = float(avg_daily_volume)
                    cost_detail = self.cost_model.get_costs(
                        ticker=ticker,
                        market=market,
                        market_cap=market_cap,
                        order_value=pos_value,
                        avg_daily_volume=avg_daily_volume,
                        price=entry_price,
                        is_short=(direction == -1),
                    )
                    if cost_detail is None:
                        # Trade rejected — too large relative to ADV
                        logger.info("Trade rejected (ADV limit): %s", ticker)
                        continue
                except (TypeError, ValueError, ZeroDivisionError):
                    cost_detail = None

            if cost_detail is not None:
                commission = cost_detail['commission'] * 2          # round trip
                spread_cost = cost_detail['spread_cost'] * 2        # round trip
                slippage = cost_detail['slippage'] * 2              # round trip
                stamp_duty = cost_detail['stamp_duty']              # one-way (buy only)
            else:
                # Fallback to config flat costs
                commission = fallback_costs.get(
                    "commission_per_trade", fallback_costs.get("commission_per_trade_gbp", 0)
                ) * 2
                spread_cost = 0.0
                slippage = shares * entry_price * fallback_costs["slippage_pct"]
                stamp_duty = (
                    shares * entry_price * fallback_costs.get("stamp_duty_pct", 0)
                    if direction == 1 else 0
                )

            holding = max(len(prices.loc[entry_date:exit_date]) - 1, 0)

            # Short borrow cost
            if direction == -1:
                if cost_detail is not None:
                    tier = self.cost_model._select_tier(market, market_cap)
                    borrow_rate = tier['short_borrow_daily']
                else:
                    borrow_rate = fallback_costs["short_borrow_daily"]
                borrow = shares * entry_price * borrow_rate * holding
            else:
                borrow = 0.0

            gross = shares * (exit_price - entry_price) * direction
            net = gross - commission - spread_cost - slippage - stamp_duty - borrow

            records.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "shares": shares,
                    "gross_pnl": gross,
                    "net_pnl": net,
                    "return": net / pos_value,
                    "holding_days": holding,
                    "surprise_pct":  row.get("surprise_pct"),
                    "earnings_date": row.get("earnings_date"),
                    "stop_triggered": stop_triggered,
                    # Cost breakdown
                    "cost_commission": commission,
                    "cost_spread": spread_cost,
                    "cost_slippage": slippage,
                    "cost_stamp_duty": stamp_duty,
                    "cost_borrow": borrow,
                    "cost_total": commission + spread_cost + slippage + stamp_duty + borrow,
                    "used_tiered_costs": cost_detail is not None,
                }
            )

        return pd.DataFrame(records)

    def _exec_price_raw(
        self,
        prices: pd.DataFrame,
        date: pd.Timestamp,
    ) -> Optional[float]:
        """Return the open price on or after date (no slippage adjustment — handled separately)."""
        if date not in prices.index:
            future = prices.index[prices.index >= date]
            if len(future) == 0:
                return None
            date = future[0]
        return float(prices.loc[date, "open"])

    def _exec_price(
        self,
        prices: pd.DataFrame,
        date: pd.Timestamp,
        side: str,
        direction: int,
        costs: dict,
    ) -> Optional[float]:
        """Legacy slippage-adjusted price (kept for compatibility)."""
        raw = self._exec_price_raw(prices, date)
        if raw is None:
            return None
        slip = costs["slippage_pct"]
        if side == "entry":
            return raw * (1 + slip * direction)
        else:
            return raw * (1 - slip * direction)

    def _resolve_exit(
        self,
        prices: pd.DataFrame,
        entry: pd.Timestamp,
        exit_: Optional[pd.Timestamp],
    ) -> Optional[pd.Timestamp]:
        if exit_ is not None and exit_ in prices.index:
            return exit_
        # Nearest available date on or after exit_
        future = prices.index[prices.index > entry]
        if len(future) == 0:
            return None
        if exit_ is None:
            return future[-1]
        candidates = future[future >= exit_]
        return candidates[0] if len(candidates) > 0 else future[-1]

    def _build_equity(self, trades: pd.DataFrame, capital: float) -> pd.Series:
        pnl = trades.groupby("exit_date")["net_pnl"].sum().sort_index()
        seed = pd.Series([capital], index=[trades["entry_date"].min()])
        equity = pd.concat([seed, capital + pnl.cumsum()])
        equity.name = "equity"
        return equity

    def _metrics(self, equity: pd.Series, trades: pd.DataFrame) -> Dict:
        ret = equity.pct_change().dropna()
        if len(ret) < 2:
            return {}

        total_ret = equity.iloc[-1] / equity.iloc[0] - 1
        years = max((equity.index[-1] - equity.index[0]).days / 365.25, 1e-6)
        cagr = (1 + total_ret) ** (1 / years) - 1
        sharpe = float((ret.mean() / ret.std()) * np.sqrt(252)) if ret.std() > 0 else 0

        rolling_max = equity.expanding().max()
        drawdown = (equity - rolling_max) / rolling_max
        max_dd = float(drawdown.min())

        trade_ret = trades["return"]
        win_rate = float((trade_ret > 0).mean())

        return {
            "total_return": float(total_ret),
            "cagr": float(cagr),
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "calmar": abs(cagr / max_dd) if max_dd != 0 else 0,
            "win_rate": win_rate,
            "n_trades": len(trades),
            "volatility": float(ret.std() * np.sqrt(252)),
        }

    def _cost_analysis(self, trades: pd.DataFrame, equity: pd.Series) -> Dict:
        """Compute transaction cost breakdown across all trades."""
        if trades.empty:
            return {}

        total_gross = trades["gross_pnl"].sum()
        total_net = trades["net_pnl"].sum()
        total_costs = trades["cost_total"].sum() if "cost_total" in trades.columns else 0.0

        commission_total = trades["cost_commission"].sum() if "cost_commission" in trades.columns else 0.0
        spread_total = trades["cost_spread"].sum() if "cost_spread" in trades.columns else 0.0
        slippage_total = trades["cost_slippage"].sum() if "cost_slippage" in trades.columns else 0.0
        stamp_total = trades["cost_stamp_duty"].sum() if "cost_stamp_duty" in trades.columns else 0.0
        borrow_total = trades["cost_borrow"].sum() if "cost_borrow" in trades.columns else 0.0

        gross_ret = total_gross / equity.iloc[0] if equity.iloc[0] > 0 else 0.0
        cost_as_pct_gross = (total_costs / total_gross * 100) if total_gross > 0 else 0.0

        # Optimistic Sharpe: re-compute equity as if only flat commissions were charged
        # (use fallback commission only, no spread/slippage beyond legacy)
        tiered_mask = trades.get("used_tiered_costs", pd.Series(False, index=trades.index))
        optimistic_net = trades["net_pnl"].copy()
        if "cost_spread" in trades.columns and "cost_slippage" in trades.columns:
            # Add back spread + extra slippage for trades that used tiered model
            optimistic_net = optimistic_net + trades["cost_spread"].fillna(0)

        pnl_opt = optimistic_net.groupby(trades["exit_date"]).sum().sort_index()
        seed_val = equity.iloc[0]
        equity_opt = pd.concat([pd.Series([seed_val], index=[trades["entry_date"].min()]),
                                 seed_val + pnl_opt.cumsum()])
        ret_opt = equity_opt.pct_change().dropna()
        sharpe_opt = float((ret_opt.mean() / ret_opt.std()) * np.sqrt(252)) if ret_opt.std() > 0 else 0.0

        ret_real = equity.pct_change().dropna()
        sharpe_real = float((ret_real.mean() / ret_real.std()) * np.sqrt(252)) if ret_real.std() > 0 else 0.0

        return {
            "total_commissions": commission_total,
            "total_spread_costs": spread_total,
            "total_slippage": slippage_total,
            "total_stamp_duty": stamp_total,
            "total_borrow_costs": borrow_total,
            "total_all_costs": total_costs,
            "cost_as_pct_gross_returns": cost_as_pct_gross,
            "sharpe_optimistic": sharpe_opt,
            "sharpe_realistic": sharpe_real,
            "gross_pnl": total_gross,
            "net_pnl": total_net,
        }

    def _empty(self, market: str) -> Dict:
        return {
            "trades": pd.DataFrame(),
            "equity_curve": pd.Series(dtype=float),
            "metrics": {},
            "market": market,
        }
