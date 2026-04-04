"""Tests for regime-conditioned GapSignal."""
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock
from signals.gap_signal import GapSignal, MIN_GAP_PCT, MAX_GAP_PCT


def make_price_df(prev_close=100.0, today_open=103.0, n=25):
    """OHLCV df where last row's open = today_open, prev row's close = prev_close."""
    closes = np.linspace(95, prev_close, n)
    opens  = closes * 0.999
    df = pd.DataFrame({
        "open":   opens,
        "high":   closes * 1.005,
        "low":    closes * 0.995,
        "close":  closes,
        "volume": np.full(n, 500_000.0),
    })
    df.iloc[-1, df.columns.get_loc("open")] = today_open
    return df


class TestGapSignalSizeFilter:
    def test_gap_too_small_returns_empty(self):
        g = GapSignal({})
        df = make_price_df(100.0, 100.5)  # 0.5% gap — below 2%
        assert g.generate("AAPL", df) == []

    def test_gap_too_large_returns_empty(self):
        g = GapSignal({})
        df = make_price_df(100.0, 110.0)  # 10% gap — above 8%
        assert g.generate("AAPL", df) == []

    def test_gap_in_range_returns_signal(self):
        g = GapSignal({})
        with patch.object(g, "_get_regime", return_value="NEUTRAL"):
            df = make_price_df(100.0, 104.0)  # 4% gap — valid
            sigs = g.generate("AAPL", df)
        assert len(sigs) == 1

    def test_outside_open_window_skipped(self):
        g = GapSignal({})
        df = make_price_df(100.0, 104.0)
        sigs = g.generate("AAPL", df, within_open_window=False)
        assert sigs == []


class TestGapSignalRegimeGating:
    def _make_gap(self, regime, gap_pct=0.04):
        g = GapSignal({})
        prev_close = 100.0
        today_open = prev_close * (1 + gap_pct)
        df = make_price_df(prev_close, today_open)
        with patch.object(g, "_get_regime", return_value=regime):
            return g.generate("AAPL", df)

    def test_crisis_returns_empty(self):
        assert self._make_gap("CRISIS") == []

    def test_neutral_up_gap_fades_short(self):
        sigs = self._make_gap("NEUTRAL", gap_pct=0.04)
        assert len(sigs) == 1
        assert sigs[0]["direction"] == "SHORT"
        assert sigs[0]["gap_context"]["action"] == "GAP_FADE"

    def test_neutral_down_gap_fades_long(self):
        sigs = self._make_gap("NEUTRAL", gap_pct=-0.04)
        assert len(sigs) == 1
        assert sigs[0]["direction"] == "LONG"

    def test_bear_up_gap_fades_short(self):
        sigs = self._make_gap("BEAR", gap_pct=0.05)
        assert len(sigs) == 1
        assert sigs[0]["direction"] == "SHORT"

    def test_bull_up_gap_continuation_long(self):
        sigs = self._make_gap("BULL", gap_pct=0.04)
        assert len(sigs) == 1
        assert sigs[0]["direction"] == "LONG"
        assert sigs[0]["gap_context"]["action"] == "GAP_CONTINUATION"

    def test_bull_down_gap_still_fades_long(self):
        sigs = self._make_gap("BULL", gap_pct=-0.04)
        assert len(sigs) == 1
        assert sigs[0]["direction"] == "LONG"
        assert sigs[0]["gap_context"]["action"] == "GAP_FADE"

    def test_signal_has_gap_context(self):
        sigs = self._make_gap("NEUTRAL", gap_pct=0.04)
        ctx = sigs[0]["gap_context"]
        assert "regime" in ctx
        assert "gap_pct" in ctx
        assert "action" in ctx


class TestGapSignalVolumeFilter:
    def test_high_volume_gap_skipped(self):
        g = GapSignal({})
        df = make_price_df(100.0, 104.0)
        with patch.object(g, "_get_regime", return_value="NEUTRAL"):
            # opening_volume = 3x avg → should be skipped
            sigs = g.generate("AAPL", df, opening_volume=1_500_000.0)
        assert sigs == []

    def test_normal_volume_allowed(self):
        g = GapSignal({})
        df = make_price_df(100.0, 104.0)
        with patch.object(g, "_get_regime", return_value="NEUTRAL"):
            sigs = g.generate("AAPL", df, opening_volume=400_000.0)
        assert len(sigs) == 1


class TestGapSignalSector:
    def test_sector_aligned_up_gap_skipped(self):
        g = GapSignal({})
        g.update_sector_gap("XLK", 1)   # sector also up
        df = make_price_df(100.0, 104.0)
        with patch.object(g, "_get_regime", return_value="NEUTRAL"):
            sigs = g.generate("AAPL", df)
        assert sigs == []
