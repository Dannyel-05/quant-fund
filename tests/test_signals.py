"""Tests for PEADSignal."""
import numpy as np
import pandas as pd
import pytest

from signals.pead_signal import PEADSignal

# Minimal config matching settings.yaml structure
CONFIG = {
    "signal": {
        "pead": {
            "earnings_surprise_threshold": 0.08,
            "volume_surge_multiplier": 1.5,
            "holding_period_days": 20,
            "lookback_days": 504,
            "zscore_window": 60,
        }
    }
}


def make_price_data(n: int = 200, base: float = 50.0) -> pd.DataFrame:
    idx = pd.bdate_range("2020-01-01", periods=n)
    rng = np.random.default_rng(42)
    close = base + np.cumsum(rng.normal(0, 0.5, n))
    volume = rng.integers(200_000, 600_000, n).astype(float)
    return pd.DataFrame(
        {"open": close, "high": close * 1.01, "low": close * 0.99, "close": close, "volume": volume},
        index=idx,
    )


def make_earnings(surprise_pcts: list, dates: list) -> pd.DataFrame:
    # surprisePercent stored as a FRACTION (e.g. 0.10 for 10% beat),
    # matching EarningsCalendar.get_earnings_surprise() output convention.
    return pd.DataFrame(
        {
            "epsActual": [1.0 * (1 + s) for s in surprise_pcts],
            "epsEstimate": [1.0] * len(surprise_pcts),
            "surprisePercent": [s for s in surprise_pcts],
        },
        index=pd.DatetimeIndex(dates),
    )


def _set_day1_up(prices: pd.DataFrame, earnings_idx: int) -> None:
    """Force day+1 close above earnings-day close for long confirmation."""
    t0 = prices.index[earnings_idx]
    d1 = prices.index[earnings_idx + 1]
    t0_close = float(prices.loc[t0, "close"])
    up = t0_close * 1.03
    for col in ["open", "high", "low", "close"]:
        prices.loc[d1, col] = up


def _set_day1_down(prices: pd.DataFrame, earnings_idx: int) -> None:
    """Force day+1 close below earnings-day close for short confirmation."""
    t0 = prices.index[earnings_idx]
    d1 = prices.index[earnings_idx + 1]
    t0_close = float(prices.loc[t0, "close"])
    down = t0_close * 0.97
    for col in ["open", "high", "low", "close"]:
        prices.loc[d1, col] = down


class TestPEADSignal:
    def setup_method(self):
        self.signal = PEADSignal(CONFIG)

    def test_positive_surprise_produces_long(self):
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        _set_day1_up(prices, 50)   # day+1 up → long confirmed → enter day+2
        earnings = make_earnings([0.10], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert not result.empty
        assert result.iloc[0]["signal"] == 1

    def test_negative_surprise_produces_short(self):
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        _set_day1_down(prices, 50)  # day+1 down → short confirmed → enter day+2
        earnings = make_earnings([-0.10], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert not result.empty
        assert result.iloc[0]["signal"] == -1

    def test_surprise_below_threshold_produces_no_signal(self):
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        earnings = make_earnings([0.04], [earnings_date])  # 4% < 8% threshold
        result = self.signal.generate("AAPL", prices, earnings)

        assert result.empty

    def test_no_volume_surge_produces_no_signal(self):
        prices = make_price_data()
        earnings_date = prices.index[50]
        # Normal volume — no surge
        earnings = make_earnings([0.20], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert result.empty

    def test_day1_against_signal_filters_trade(self):
        """Day+1 moving against signal direction should produce no signal."""
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        _set_day1_down(prices, 50)  # day+1 down → long NOT confirmed
        earnings = make_earnings([0.10], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert result.empty

    def test_exit_date_is_after_entry(self):
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        _set_day1_up(prices, 50)
        earnings = make_earnings([0.15], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert not result.empty
        assert result.iloc[0]["exit_date"] > result.iloc[0]["entry_date"]

    def test_entry_is_day2_after_earnings(self):
        """With day1 confirmed, entry should be day+2 (not day+1 or day+3)."""
        prices = make_price_data()
        earnings_date = prices.index[50]
        prices.loc[earnings_date, "volume"] *= 5
        _set_day1_up(prices, 50)
        earnings = make_earnings([0.10], [earnings_date])
        result = self.signal.generate("AAPL", prices, earnings)

        assert not result.empty
        expected_entry = prices.index[52]  # day+2 after index 50
        assert result.iloc[0]["entry_date"] == expected_entry

    def test_zscore_column_present(self):
        prices = make_price_data()
        dates = prices.index[20:120:10]
        for i, d in enumerate(dates):
            prices.loc[d, "volume"] *= 5
            # Ensure day+1 moves in signal direction for each event
            if 0.10 > 0:  # long signals
                _set_day1_up(prices, prices.index.get_loc(d))
            else:
                _set_day1_down(prices, prices.index.get_loc(d))
        surprises = [0.10 if i % 2 == 0 else -0.10 for i in range(len(dates))]
        # Set day1 direction correctly for each signal
        for i, d in enumerate(dates):
            pos = prices.index.get_loc(d)
            if surprises[i] > 0:
                _set_day1_up(prices, pos)
            else:
                _set_day1_down(prices, pos)
        earnings = make_earnings(surprises, dates)
        result = self.signal.generate("AAPL", prices, earnings)

        assert not result.empty
        assert "surprise_zscore" in result.columns

    def test_empty_inputs_return_empty(self):
        pead = PEADSignal(CONFIG)
        assert pead.generate("X", pd.DataFrame(), pd.DataFrame()).empty
        assert pead.generate("X", make_price_data(), pd.DataFrame()).empty
