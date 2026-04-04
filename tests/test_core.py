"""
test_core.py — Integration tests for Phase 20 upgrade modules.

Run: venv/bin/pytest tests/test_core.py -v
"""
import sys
import os
import pytest

# Ensure project root on path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ── FeatureManager ────────────────────────────────────────────────────────────

class TestFeatureManager:
    def setup_method(self):
        from execution.feature_manager import FeatureManager
        FeatureManager.reset()

    def test_enabled_by_default(self):
        from execution.feature_manager import FeatureManager
        fm = FeatureManager.get({"feature_flags": {}})
        assert fm.is_enabled("some_new_feature") is True

    def test_disabled_via_config(self):
        from execution.feature_manager import FeatureManager
        fm = FeatureManager.get({"feature_flags": {"sector_rotation": False}})
        assert fm.is_enabled("sector_rotation") is False

    def test_disable_after_3_failures(self):
        from execution.feature_manager import FeatureManager
        fm = FeatureManager.get({"feature_flags": {}})

        def bad_fn():
            raise ValueError("boom")

        for _ in range(3):
            fm.wrap("test_feature", bad_fn, default=None)

        assert fm.is_enabled("test_feature") is False

    def test_wrap_returns_default_when_disabled(self):
        from execution.feature_manager import FeatureManager
        fm = FeatureManager.get({"feature_flags": {"feat_x": False}})
        result = fm.wrap("feat_x", lambda: 42, default="sentinel")
        assert result == "sentinel"

    def test_singleton(self):
        from execution.feature_manager import FeatureManager
        fm1 = FeatureManager.get({"feature_flags": {}})
        fm2 = FeatureManager.get()
        assert fm1 is fm2


# ── MarketCalendar ────────────────────────────────────────────────────────────

class TestMarketCalendar:
    def setup_method(self):
        from analysis.market_calendar import MarketCalendar
        self.mc = MarketCalendar()

    def test_new_years_day_not_trading(self):
        from datetime import date
        assert self.mc.is_trading_day("us", date(2026, 1, 1)) is False

    def test_regular_weekday_is_trading(self):
        from datetime import date
        assert self.mc.is_trading_day("us", date(2026, 1, 5)) is True

    def test_weekend_not_trading(self):
        from datetime import date
        assert self.mc.is_trading_day("us", date(2026, 1, 3)) is False  # Saturday

    def test_uk_easter_monday_not_trading(self):
        from datetime import date
        assert self.mc.is_trading_day("uk", date(2026, 4, 6)) is False

    def test_fomc_week_detection(self):
        from datetime import date
        assert self.mc.is_fomc_week(date(2026, 1, 29)) is True
        assert self.mc.is_fomc_week(date(2026, 2, 2)) is False

    def test_next_trading_day(self):
        from datetime import date
        # Christmas Day 2026 is Friday — next US trading day is Mon Dec 28
        nxt = self.mc.next_trading_day("us", date(2026, 12, 25))
        assert nxt == date(2026, 12, 28)


# ── TrailingStopManager ───────────────────────────────────────────────────────

class TestTrailingStopManager:
    def setup_method(self):
        from execution.trailing_stops import TrailingStopManager
        self.tm = TrailingStopManager()

    def test_add_and_exit(self):
        self.tm.add_position("AAPL", entry_price=100.0, current_price=100.0)
        assert not self.tm.should_exit("AAPL", 90.0)   # 10% drop, tier1 stop at 85
        assert self.tm.should_exit("AAPL", 84.0)       # below 15% trailing stop

    def test_stop_only_moves_up(self):
        self.tm.add_position("TSLA", entry_price=100.0, current_price=100.0)
        initial_stop = self.tm.stop_price("TSLA")
        self.tm.observe("TSLA", 120.0)   # new peak
        higher_stop = self.tm.stop_price("TSLA")
        assert higher_stop > initial_stop
        self.tm.observe("TSLA", 110.0)   # pullback — stop should NOT move down
        assert self.tm.stop_price("TSLA") == higher_stop

    def test_tier_escalation(self):
        self.tm.add_position("NVDA", entry_price=100.0, current_price=100.0)
        assert self.tm.tier("NVDA") == 1
        self.tm.observe("NVDA", 120.0)   # 20% gain → tier 2
        assert self.tm.tier("NVDA") == 2

    def test_bulk_init(self):
        positions = [
            {"ticker": "MSFT", "entry_price": 400.0, "current_price": 420.0},
            {"ticker": "GOOG", "entry_price": 200.0},
        ]
        added = self.tm.initialise_from_positions(positions)
        assert added == 2
        assert self.tm.has_position("MSFT")
        assert self.tm.has_position("GOOG")


# ── CoolingOffTracker ─────────────────────────────────────────────────────────

class TestCoolingOffTracker:
    def setup_method(self):
        from execution.cooling_off_tracker import StockCoolingOffTracker
        self.tracker = StockCoolingOffTracker(cooling_days=5)

    def test_losing_trade_imposes_cooling(self):
        from datetime import date
        self.tracker.register_exit("XYZ", date(2026, 1, 5), exit_price=10.0, pnl_pct=-0.05)
        assert self.tracker.is_cooling_off("XYZ", as_of=date(2026, 1, 6)) is True

    def test_winning_trade_no_cooling(self):
        from datetime import date
        self.tracker.register_exit("ABC", date(2026, 1, 5), exit_price=10.0, pnl_pct=0.10)
        assert self.tracker.is_cooling_off("ABC", as_of=date(2026, 1, 6)) is False

    def test_early_release_earnings_beat(self):
        from datetime import date
        self.tracker.register_exit("NVDA", date(2026, 1, 5), exit_price=100.0, pnl_pct=-0.03)
        released = self.tracker.check_early_release(
            "NVDA", earnings_beat_pct=0.08, as_of=date(2026, 1, 6)
        )
        assert released is True
        assert self.tracker.is_cooling_off("NVDA") is False

    def test_expires_after_period(self):
        from datetime import date
        self.tracker.register_exit("OLD", date(2026, 1, 1), exit_price=10.0, pnl_pct=-0.05)
        assert self.tracker.is_cooling_off("OLD", as_of=date(2026, 1, 10)) is False


# ── TechnicalIndicators ───────────────────────────────────────────────────────

class TestTechnicalIndicators:
    def _make_df(self, n: int = 50):
        import pandas as pd
        import numpy as np
        rng = np.random.default_rng(42)
        price = 100 + np.cumsum(rng.normal(0, 1, n))
        df = pd.DataFrame({
            "open":   price * 0.99,
            "high":   price * 1.01,
            "low":    price * 0.98,
            "close":  price,
            "volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        })
        return df

    def test_rsi_in_range(self):
        from analysis.technical_indicators import TechnicalIndicatorCalculator
        df = self._make_df()
        rsi = TechnicalIndicatorCalculator.rsi(df)
        assert rsi is not None
        assert 0 <= rsi <= 100

    def test_macd_has_keys(self):
        from analysis.technical_indicators import TechnicalIndicatorCalculator
        df = self._make_df()
        macd = TechnicalIndicatorCalculator.macd(df)
        assert "macd" in macd and "signal" in macd and "histogram" in macd

    def test_atr_positive(self):
        from analysis.technical_indicators import TechnicalIndicatorCalculator
        df = self._make_df()
        atr = TechnicalIndicatorCalculator.atr(df)
        assert atr is not None and atr > 0

    def test_bollinger_pct_b(self):
        from analysis.technical_indicators import TechnicalIndicatorCalculator
        df = self._make_df()
        bb = TechnicalIndicatorCalculator.bollinger_bands(df)
        assert "pct_b" in bb
        # pct_b can be < 0 or > 1 when price is outside bands, just check it's a float
        assert isinstance(bb["pct_b"], float)


# ── RegimeDetector (offline / no network) ────────────────────────────────────

class TestRegimeDetector:
    def test_classify_crisis_high_vix(self):
        from analysis.regime_detector import RegimeDetector
        rd = RegimeDetector()
        regime = rd._classify(spy_close=450.0, spy_ma200=480.0, vix_close=40.0, credit_spread_bps=None)
        assert regime == "CRISIS"

    def test_classify_bull(self):
        from analysis.regime_detector import RegimeDetector
        rd = RegimeDetector()
        regime = rd._classify(spy_close=520.0, spy_ma200=500.0, vix_close=15.0, credit_spread_bps=None)
        assert regime == "BULL"

    def test_classify_bear(self):
        from analysis.regime_detector import RegimeDetector
        rd = RegimeDetector()
        # SPY 470 vs MA200 500 = 94% (> 92% crisis threshold), VIX 28 > 25 → BEAR
        regime = rd._classify(spy_close=470.0, spy_ma200=500.0, vix_close=28.0, credit_spread_bps=None)
        assert regime == "BEAR"

    def test_position_size_multipliers(self):
        from analysis.regime_detector import RegimeDetector
        rd = RegimeDetector()
        rd._current_regime = "CRISIS"
        assert rd.position_size_multiplier() == 0.30
        rd._current_regime = "BULL"
        assert rd.position_size_multiplier() == 1.00


# ── AlpacaRateLimiter ────────────────────────────────────────────────────────

class TestAlpacaRateLimiter:
    def test_acquire_returns_true(self):
        from execution.alpaca_rate_limiter import AlpacaRateLimiter
        rl = AlpacaRateLimiter({"trading": (10, 60.0), "default": (100, 60.0)})
        assert rl.acquire("trading") is True

    def test_available_decreases(self):
        from execution.alpaca_rate_limiter import AlpacaRateLimiter
        rl = AlpacaRateLimiter({"trading": (10, 60.0), "default": (100, 60.0)})
        before = rl.available("trading")
        rl.acquire("trading")
        assert rl.available("trading") < before

    def test_unknown_endpoint_uses_default(self):
        from execution.alpaca_rate_limiter import AlpacaRateLimiter
        rl = AlpacaRateLimiter({"default": (50, 60.0)})
        assert rl.acquire("nonexistent_endpoint") is True


# ── SignalContradiction (via paper_trader static method) ──────────────────────

class TestSignalContradiction:
    def test_full_consensus_boost(self):
        from execution.paper_trader import PaperTrader
        signals = [
            {"direction": "LONG",  "score": 0.8, "signal_type": "A"},
            {"direction": "LONG",  "score": 0.6, "signal_type": "B"},
            {"direction": "LONG",  "score": 0.7, "signal_type": "C"},
        ]
        result = PaperTrader._apply_contradiction_scores(signals)
        # All LONG → contradiction_score should be 0.0 → score boosted
        assert result[0]["contradiction_score"] == 0.0
        assert result[0]["score"] > 0.8

    def test_high_contradiction_penalty(self):
        from execution.paper_trader import PaperTrader
        signals = [
            {"direction": "LONG",  "score": 1.0, "signal_type": "A"},
            {"direction": "SHORT", "score": 1.0, "signal_type": "B"},
        ]
        result = PaperTrader._apply_contradiction_scores(signals)
        assert result[0]["contradiction_score"] == 1.0
        assert result[0]["score"] < 1.0   # penalised


# ── NewsContextEnricher ───────────────────────────────────────────────────────

class TestNewsContextEnricher:
    def test_enriches_articles(self):
        from data.collectors.news_context_enricher import NewsContextEnricher
        enricher = NewsContextEnricher()
        articles = [
            {"title": "AAPL beats earnings with record revenue",
             "summary": "Apple reported strong quarterly results above expectations."},
        ]
        enriched = enricher.enrich("AAPL", articles)
        assert len(enriched) == 1
        assert "sentiment_score" in enriched[0]
        assert "financial_context_score" in enriched[0]
        assert "earnings" in enriched[0]["categories"]

    def test_negative_article_sentiment(self):
        from data.collectors.news_context_enricher import NewsContextEnricher
        enricher = NewsContextEnricher()
        articles = [
            {"title": "SEC fraud investigation causes crash and loss",
             "summary": "Company faces regulatory action and revenue decline."},
        ]
        enriched = enricher.enrich("XYZ", articles)
        assert enriched[0]["sentiment_score"] < 0


# ── MarketTimer (offline) ─────────────────────────────────────────────────────

class TestMarketTimer:
    def test_session_returns_string(self):
        from analysis.market_timer import MarketTimer
        mt = MarketTimer()
        session = mt.current_session("us")
        assert session in ("pre", "power_hour", "open", "close", "after", "closed")

    def test_minutes_to_open_is_int(self):
        from analysis.market_timer import MarketTimer
        mt = MarketTimer()
        mins = mt.minutes_to_open("us")
        assert mins is None or isinstance(mins, int)
