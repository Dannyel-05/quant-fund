"""Tests for OptionsFlowAnalyser (all yfinance calls mocked)."""
import pytest
from unittest.mock import patch, MagicMock
from analysis.options_flow_analyser import OptionsFlowAnalyser


@pytest.fixture
def oa():
    return OptionsFlowAnalyser()


class TestPCR:
    def test_high_pcr_bearish_score(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=2.0):
            with patch.object(oa, "get_iv_percentile", return_value=50.0):
                with patch.object(oa, "detect_unusual_activity", return_value=False):
                    score = oa.options_sentiment_score("AAPL")
        assert score < 0.0

    def test_low_pcr_bullish_score(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=0.2):
            with patch.object(oa, "get_iv_percentile", return_value=50.0):
                with patch.object(oa, "detect_unusual_activity", return_value=False):
                    score = oa.options_sentiment_score("AAPL")
        assert score > 0.0

    def test_no_pcr_neutral(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=None):
            with patch.object(oa, "get_iv_percentile", return_value=None):
                with patch.object(oa, "detect_unusual_activity", return_value=False):
                    score = oa.options_sentiment_score("AAPL")
        assert score == 0.0


class TestIVPercentile:
    def test_high_iv_adds_positive(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=1.0):  # neutral PCR
            with patch.object(oa, "get_iv_percentile", return_value=85.0):
                with patch.object(oa, "detect_unusual_activity", return_value=False):
                    score = oa.options_sentiment_score("AAPL")
        assert score > 0.0  # IV > 80 adds +0.1

    def test_low_iv_neutral(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=1.0):  # neutral PCR
            with patch.object(oa, "get_iv_percentile", return_value=15.0):
                with patch.object(oa, "detect_unusual_activity", return_value=False):
                    score = oa.options_sentiment_score("AAPL")
        assert score == 0.0  # IV < 20 is neutral


class TestUnusualActivity:
    def test_unusual_amplifies_bearish(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=2.0):
            with patch.object(oa, "get_iv_percentile", return_value=50.0):
                with patch.object(oa, "detect_unusual_activity", return_value=False) as no_ua:
                    score_normal = oa.options_sentiment_score("AAPL")
                with patch.object(oa, "detect_unusual_activity", return_value=True):
                    score_unusual = oa.options_sentiment_score("AAPL")
        assert score_unusual <= score_normal  # amplified bearish = more negative

    def test_score_clamped_to_minus_one(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=10.0):
            with patch.object(oa, "get_iv_percentile", return_value=None):
                with patch.object(oa, "detect_unusual_activity", return_value=True):
                    score = oa.options_sentiment_score("AAPL")
        assert score >= -1.0

    def test_score_clamped_to_plus_one(self, oa):
        with patch.object(oa, "get_put_call_ratio", return_value=0.0):
            with patch.object(oa, "get_iv_percentile", return_value=95.0):
                with patch.object(oa, "detect_unusual_activity", return_value=True):
                    score = oa.options_sentiment_score("AAPL")
        assert score <= 1.0


class TestPositionSizeAdjustment:
    def test_very_bearish_reduces_size(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=-0.8):
            adjusted = oa.position_size_adjustment("AAPL", 1000.0)
        assert adjusted < 1000.0
        assert adjusted >= 700.0  # max 30% reduction

    def test_neutral_unchanged(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=0.0):
            adjusted = oa.position_size_adjustment("AAPL", 1000.0)
        assert adjusted == 1000.0

    def test_mildly_bearish_unchanged(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=-0.3):
            adjusted = oa.position_size_adjustment("AAPL", 1000.0)
        assert adjusted == 1000.0  # only reduces if score < -0.5


class TestApplyToSignal:
    def test_bullish_options_boost_score(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=0.8):
            result = oa.apply_to_signal("AAPL", 0.5)
        assert result > 0.5

    def test_bearish_options_reduce_score(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=-0.8):
            result = oa.apply_to_signal("AAPL", 0.5)
        assert result < 0.5

    def test_output_clamped(self, oa):
        with patch.object(oa, "options_sentiment_score", return_value=1.0):
            result = oa.apply_to_signal("AAPL", 0.99)
        assert result <= 1.0


class TestStatus:
    def test_status_returns_dict(self, oa):
        s = oa.status()
        assert "thresholds" in s
        assert "pcr_bearish" in s["thresholds"]
