"""Tests for BacktestEngine, WalkForwardAnalysis, MonteCarloSimulator."""
import numpy as np
import pandas as pd
import pytest

from backtest.engine import BacktestEngine
from backtest.monte_carlo import MonteCarloSimulator
from backtest.walk_forward import WalkForwardAnalysis

CONFIG = {
    "backtest": {
        "start_date": "2020-01-01",
        "end_date": "2023-12-31",
        "initial_capital": 100_000,
        "train_pct": 0.70,
        "validate_pct": 0.15,
        "test_pct": 0.15,
        "monte_carlo_simulations": 200,
        "benchmark_us": "^GSPC",
        "benchmark_uk": "^FTSE",
    },
    "costs": {
        "us": {
            "commission_per_trade": 1.0,
            "slippage_pct": 0.001,
            "short_borrow_daily": 0.0001,
            "stamp_duty_pct": 0.0,
        },
        "uk": {
            "commission_per_trade_gbp": 3.0,
            "stamp_duty_pct": 0.005,
            "slippage_pct": 0.002,
            "short_borrow_daily": 0.0002,
        },
    },
    "risk": {
        "max_position_pct": 0.05,
    },
}


def make_price_df(start: str = "2020-01-01", n: int = 500, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(start, periods=n)
    close = 50 + np.cumsum(rng.normal(0, 0.5, n))
    close = np.clip(close, 1, None)
    volume = rng.integers(300_000, 700_000, n).astype(float)
    return pd.DataFrame(
        {"open": close, "high": close * 1.01, "low": close * 0.99, "close": close, "volume": volume},
        index=idx,
    )


def make_signals(price_df: pd.DataFrame, ticker: str = "AAPL", n: int = 5) -> pd.DataFrame:
    idx = price_df.index
    records = []
    step = len(idx) // (n + 1)
    for i in range(1, n + 1):
        entry = idx[i * step]
        exit_ = idx[min(i * step + 20, len(idx) - 1)]
        records.append(
            {
                "ticker": ticker,
                "signal": 1,
                "entry_date": entry,
                "exit_date": exit_,
                "surprise_pct": 0.08,
            }
        )
    return pd.DataFrame(records)


class TestBacktestEngine:
    def setup_method(self):
        self.engine = BacktestEngine(CONFIG)

    def test_returns_expected_keys(self):
        prices = make_price_df()
        signals = make_signals(prices)
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        assert set(result.keys()) == {"trades", "equity_curve", "metrics", "market"}

    def test_empty_signals_returns_empty_results(self):
        result = self.engine.run(pd.DataFrame(), {}, market="us")
        assert result["trades"].empty
        assert result["equity_curve"].empty

    def test_equity_starts_at_initial_capital(self):
        prices = make_price_df()
        signals = make_signals(prices)
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        assert abs(result["equity_curve"].iloc[0] - CONFIG["backtest"]["initial_capital"]) < 1e-3

    def test_metrics_contain_required_fields(self):
        prices = make_price_df()
        signals = make_signals(prices)
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        for key in ("total_return", "sharpe", "max_drawdown", "win_rate", "n_trades"):
            assert key in result["metrics"]

    def test_max_drawdown_is_non_positive(self):
        prices = make_price_df()
        signals = make_signals(prices)
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        assert result["metrics"]["max_drawdown"] <= 0

    def test_short_signals_processed(self):
        prices = make_price_df()
        signals = make_signals(prices)
        signals["signal"] = -1  # all shorts
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        assert not result["trades"].empty
        assert (result["trades"]["direction"] == -1).all()

    def test_uk_market_applies_stamp_duty(self):
        prices = make_price_df()
        # Run same signals as US vs UK and compare costs
        signals = make_signals(prices, ticker="VOD.L")
        result_uk = self.engine.run(signals, {"VOD.L": prices}, market="uk")
        result_us = self.engine.run(signals, {"VOD.L": prices}, market="us")
        # UK should have stamp duty → lower total P&L for longs
        assert result_uk["trades"]["net_pnl"].sum() <= result_us["trades"]["net_pnl"].sum()

    def test_missing_ticker_skipped(self):
        prices = make_price_df()
        signals = make_signals(prices, ticker="MISSING")
        result = self.engine.run(signals, {"AAPL": prices}, market="us")
        assert result["trades"].empty


class TestMonteCarloSimulator:
    def setup_method(self):
        self.mc = MonteCarloSimulator(CONFIG)

    def test_returns_expected_structure(self):
        rng = np.random.default_rng(0)
        returns = pd.Series(rng.normal(0.002, 0.02, 100))
        result = self.mc.run(returns)
        for key in ("n_simulations", "final_value", "max_drawdown", "sharpe", "prob_profit", "prob_ruin"):
            assert key in result

    def test_prob_profit_in_unit_interval(self):
        rng = np.random.default_rng(1)
        returns = pd.Series(rng.normal(0.001, 0.02, 100))
        result = self.mc.run(returns)
        assert 0.0 <= result["prob_profit"] <= 1.0
        assert 0.0 <= result["prob_ruin"] <= 1.0

    def test_too_few_trades_returns_empty(self):
        result = self.mc.run(pd.Series([0.01, 0.02]))
        assert result == {}

    def test_percentiles_are_ordered(self):
        rng = np.random.default_rng(2)
        returns = pd.Series(rng.normal(0.001, 0.02, 150))
        result = self.mc.run(returns)
        pcts = result["final_value"]["percentiles"]
        vals = [pcts[k] for k in sorted(pcts, key=int)]
        assert vals == sorted(vals)


class TestWalkForwardAnalysis:
    def setup_method(self):
        self.engine = BacktestEngine(CONFIG)
        self.wf = WalkForwardAnalysis(CONFIG, self.engine)

    def test_returns_expected_keys(self):
        prices = {"AAPL": make_price_df(n=600)}

        def gen(pd_):
            return make_signals(prices["AAPL"], n=3)

        result = self.wf.run(gen, prices, market="us", n_windows=3)
        assert "mean_oos_sharpe" in result
        assert "n_windows" in result

    def test_insufficient_data_returns_empty(self):
        prices = {"AAPL": make_price_df(n=50)}

        def gen(pd_):
            return pd.DataFrame()

        result = self.wf.run(gen, prices, market="us", n_windows=2)
        assert result == {}
