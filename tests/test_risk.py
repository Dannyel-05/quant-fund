"""Tests for RiskManager and PaperBroker."""
import numpy as np
import pandas as pd
import pytest

from risk.manager import RiskManager
from execution.broker_interface import PaperBroker

CONFIG = {
    "risk": {
        "max_position_pct": 0.05,
        "max_sector_exposure_pct": 0.25,
        "max_market_exposure_pct": 0.60,
        "max_total_positions": 5,
        "max_drawdown_halt_pct": 0.15,
        "kelly_fraction": 0.5,
        "correlation_limit": 0.75,
        "atr_stop_multiplier": 2.0,
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
}

CLEAN_PORTFOLIO = {
    "positions": {},
    "drawdown": 0.0,
    "net_exposure": 0.0,
    "sectors": {},
    "sector_exposures": {},
}


def make_portfolio(**overrides) -> dict:
    p = dict(CLEAN_PORTFOLIO)
    p.update(overrides)
    return p


class TestRiskManager:
    def setup_method(self):
        self.rm = RiskManager(CONFIG)

    # --- Kelly ---

    def test_kelly_positive_edge(self):
        size = self.rm.kelly_size(win_rate=0.60, win_loss_ratio=1.5)
        assert size > 0

    def test_kelly_negative_edge_returns_zero(self):
        size = self.rm.kelly_size(win_rate=0.30, win_loss_ratio=0.5)
        assert size == 0.0

    def test_kelly_fraction_applied(self):
        full_kelly = self.rm.kelly_size(0.60, 1.5) / self.rm.kelly_fraction
        half_kelly = self.rm.kelly_size(0.60, 1.5)
        assert abs(half_kelly - full_kelly * self.rm.kelly_fraction) < 1e-9

    # --- Position sizing ---

    def test_size_position_within_max_pct(self):
        size = self.rm.size_position(
            "AAPL", 1.0, make_portfolio(), pd.DataFrame()
        )
        assert size <= CONFIG["risk"]["max_position_pct"]

    def test_size_position_zero_on_drawdown_halt(self):
        portfolio = make_portfolio(drawdown=-0.20)
        size = self.rm.size_position("AAPL", 1.0, portfolio, pd.DataFrame())
        assert size == 0.0

    def test_size_position_zero_on_max_exposure(self):
        portfolio = make_portfolio(net_exposure=0.65)
        size = self.rm.size_position("AAPL", 1.0, portfolio, pd.DataFrame())
        assert size == 0.0

    def test_size_position_zero_when_max_positions_reached(self):
        portfolio = make_portfolio(
            positions={f"T{i}": 100 for i in range(CONFIG["risk"]["max_total_positions"])}
        )
        size = self.rm.size_position("NEWT", 1.0, portfolio, pd.DataFrame())
        assert size == 0.0

    def test_size_position_capped_by_sector_limit(self):
        portfolio = make_portfolio(
            sectors={"AAPL": "Technology"},
            sector_exposures={"Technology": 0.23},
        )
        size = self.rm.size_position("AAPL", 3.0, portfolio, pd.DataFrame())
        assert size <= CONFIG["risk"]["max_sector_exposure_pct"] - 0.23 + 1e-6

    # --- ATR stop ---

    def test_atr_stop_returns_positive(self):
        n = 50
        idx = pd.bdate_range("2022-01-01", periods=n)
        rng = np.random.default_rng(0)
        close = 100 + np.cumsum(rng.normal(0, 0.5, n))
        df = pd.DataFrame(
            {"high": close * 1.01, "low": close * 0.99, "close": close},
            index=idx,
        )
        stop = self.rm.atr_stop(df)
        assert stop is not None
        assert stop > 0

    def test_atr_stop_insufficient_data_returns_none(self):
        df = pd.DataFrame({"high": [10.0], "low": [9.0], "close": [9.5]})
        assert self.rm.atr_stop(df) is None

    # --- Correlation ---

    def test_correlation_ok_with_uncorrelated(self):
        rng = np.random.default_rng(0)
        new_r = pd.Series(rng.normal(0, 1, 100))
        rng2 = np.random.default_rng(99)
        existing = {"X": pd.Series(rng2.normal(0, 1, 100))}
        assert self.rm.correlation_ok(new_r, existing) is True

    def test_correlation_rejected_with_correlated(self):
        rng = np.random.default_rng(0)
        base = pd.Series(rng.normal(0, 1, 100))
        near_copy = base + pd.Series(np.random.default_rng(1).normal(0, 0.01, 100))
        assert self.rm.correlation_ok(near_copy, {"X": base}) is False

    # --- Portfolio stats ---

    def test_portfolio_stats_net_exposure(self):
        stats = self.rm.portfolio_stats(
            {"AAPL": 10, "MSFT": -5},
            {"AAPL": 100.0, "MSFT": 200.0},
        )
        assert stats["long_exposure"] == 1000.0
        assert stats["short_exposure"] == 1000.0
        assert stats["net_exposure"] == 0.0


class TestPaperBroker:
    def setup_method(self):
        self.broker = PaperBroker(100_000, CONFIG)

    def test_buy_reduces_cash(self):
        cash_before = self.broker.get_cash()
        self.broker.place_order("AAPL", 100, "buy", fill_price=50.0)
        assert self.broker.get_cash() < cash_before

    def test_buy_creates_position(self):
        self.broker.place_order("AAPL", 100, "buy", fill_price=50.0)
        assert "AAPL" in self.broker.get_positions()
        assert self.broker.get_positions()["AAPL"] == pytest.approx(100, abs=1e-3)

    def test_sell_removes_position(self):
        self.broker.place_order("AAPL", 100, "buy", fill_price=50.0)
        self.broker.place_order("AAPL", 100, "sell", fill_price=55.0)
        assert "AAPL" not in self.broker.get_positions()

    def test_sell_insufficient_position_rejected(self):
        result = self.broker.place_order("AAPL", 999, "sell", fill_price=50.0)
        assert result["status"] == "rejected"

    def test_buy_insufficient_funds_rejected(self):
        result = self.broker.place_order("AAPL", 1_000_000, "buy", fill_price=1000.0)
        assert result["status"] == "rejected"

    def test_short_increases_cash(self):
        cash_before = self.broker.get_cash()
        self.broker.place_order("AAPL", 100, "short", fill_price=50.0)
        assert self.broker.get_cash() > cash_before

    def test_account_value_with_prices(self):
        self.broker.place_order("AAPL", 100, "buy", fill_price=50.0)
        # Price rose to 60
        value = self.broker.get_account_value({"AAPL": 60.0})
        assert value > self.broker.initial_capital

    def test_uk_stamp_duty_applied_on_buy(self):
        broker_uk = PaperBroker(100_000, CONFIG)
        broker_us = PaperBroker(100_000, CONFIG)
        broker_uk.place_order("VOD.L", 1000, "buy", fill_price=10.0)
        broker_us.place_order("AAPL", 1000, "buy", fill_price=10.0)
        # UK should cost more due to stamp duty
        assert broker_uk.get_cash() < broker_us.get_cash()

    def test_trade_log_records_fills(self):
        self.broker.place_order("AAPL", 50, "buy", fill_price=100.0)
        self.broker.place_order("AAPL", 50, "sell", fill_price=110.0)
        log = self.broker.get_trade_log()
        assert len(log) == 2
        assert set(log["direction"]) == {"buy", "sell"}
