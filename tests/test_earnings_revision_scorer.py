"""Tests for EarningsRevisionScorer."""
import sqlite3
import pytest
from unittest.mock import patch, MagicMock

from analysis.earnings_revision_scorer import EarningsRevisionScorer


@pytest.fixture
def scorer(tmp_path):
    return EarningsRevisionScorer(db_path=str(tmp_path / "test.db"))


class TestEarningsRevisionScorerDB:
    def test_table_created(self, tmp_path):
        s = EarningsRevisionScorer(db_path=str(tmp_path / "t.db"))
        con = sqlite3.connect(str(tmp_path / "t.db"))
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "earnings_revisions" in tables

    def test_status_empty(self, scorer):
        st = scorer.status()
        assert st["total_rows"] == 0
        assert st["unique_tickers"] == 0


class TestRevisionScore:
    def test_no_data_returns_zero(self, scorer):
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value={}):
            with patch.object(scorer, "_fetch_simfin_estimates", return_value={}):
                assert scorer.get_revision_score("AAPL") == 0.0

    def test_positive_revision(self, scorer):
        data = {"current_estimate": 1.10, "prior_estimate": 1.00, "estimate_date": "2026-04-01"}
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value=data):
            score = scorer.get_revision_score("AAPL")
        assert score > 0.0
        assert score <= 0.30

    def test_negative_revision(self, scorer):
        data = {"current_estimate": 0.90, "prior_estimate": 1.00, "estimate_date": "2026-04-01"}
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value=data):
            score = scorer.get_revision_score("MSFT")
        assert score < 0.0
        assert score >= -0.30

    def test_small_revision_returns_zero(self, scorer):
        # 3% revision < 5% threshold → score = 0.0
        data = {"current_estimate": 1.03, "prior_estimate": 1.00, "estimate_date": "2026-04-01"}
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value=data):
            score = scorer.get_revision_score("GOOG")
        assert score == 0.0

    def test_caching(self, scorer):
        """Second call with same ticker today should use cached DB value."""
        data = {"current_estimate": 1.10, "prior_estimate": 1.00, "estimate_date": "2026-04-01"}
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value=data) as m:
            scorer.get_revision_score("TSLA")
        # Force today's date to match cached entry
        import sqlite3 as _sq, datetime
        con = _sq.connect(scorer._db_path)
        today = datetime.datetime.utcnow().date().isoformat()
        con.execute("UPDATE earnings_revisions SET estimate_date=? WHERE ticker='TSLA'", (today,))
        con.commit(); con.close()
        with patch.object(scorer, "_fetch_yfinance_estimates", return_value=data) as m2:
            scorer.get_revision_score("TSLA")
        # Should NOT call yfinance again (cached)
        m2.assert_not_called()


class TestAmplifyPead:
    def test_positive_revision_amplifies(self, scorer):
        with patch.object(scorer, "get_revision_score", return_value=0.20):
            result = scorer.amplify_pead_signal(0.5, "AAPL")
        assert result == pytest.approx(0.5 * 1.20, rel=1e-5)

    def test_negative_revision_reduces(self, scorer):
        with patch.object(scorer, "get_revision_score", return_value=-0.20):
            result = scorer.amplify_pead_signal(0.5, "AAPL")
        assert result == pytest.approx(0.5 * 0.80, rel=1e-5)

    def test_clamped_to_one(self, scorer):
        with patch.object(scorer, "get_revision_score", return_value=0.30):
            result = scorer.amplify_pead_signal(0.9, "X")
        assert result <= 1.0

    def test_clamped_to_minus_one(self, scorer):
        with patch.object(scorer, "get_revision_score", return_value=-0.30):
            result = scorer.amplify_pead_signal(-0.9, "X")
        assert result >= -1.0

    def test_zero_revision_unchanged(self, scorer):
        with patch.object(scorer, "get_revision_score", return_value=0.0):
            result = scorer.amplify_pead_signal(0.6, "X")
        assert result == pytest.approx(0.6, rel=1e-5)
