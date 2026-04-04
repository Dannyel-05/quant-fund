"""Tests for HealthDashboard."""
import json
import os
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from monitoring.health_dashboard import HealthDashboard


@pytest.fixture
def db(tmp_path):
    """Seed a minimal closeloop.db."""
    db_path = str(tmp_path / "closeloop.db")
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE trade_ledger (
        id INTEGER PRIMARY KEY, ticker TEXT, entry_date TEXT, exit_date TEXT,
        gross_pnl REAL DEFAULT 0, is_phantom INTEGER DEFAULT 0)""")
    con.execute("""CREATE TABLE signals_log (
        id INTEGER PRIMARY KEY, signal_type TEXT, timestamp TEXT, score REAL)""")
    con.execute("""CREATE TABLE cointegration_log (
        id INTEGER PRIMARY KEY, status TEXT)""")
    con.execute("INSERT INTO trade_ledger VALUES (1,'AAPL','2026-04-01','2026-04-02',150.0,0)")
    con.execute("INSERT INTO trade_ledger VALUES (2,'MSFT','2026-04-01',NULL,0,0)")
    con.execute("INSERT INTO cointegration_log VALUES (1,'valid')")
    con.commit(); con.close()
    return db_path


@pytest.fixture
def dashboard(db):
    return HealthDashboard(config={}, closeloop_db=db)


class TestGenerate:
    def test_returns_dict_with_keys(self, dashboard):
        metrics = dashboard.generate()
        for key in ("timestamp", "phase", "real_trade_count", "open_positions",
                    "today_pnl_usd", "pairs_active", "regime_state", "kalman_status"):
            assert key in metrics

    def test_real_trade_count(self, dashboard):
        metrics = dashboard.generate()
        assert metrics["real_trade_count"] == 1  # 1 closed non-phantom trade

    def test_open_positions(self, dashboard):
        metrics = dashboard.generate()
        assert metrics["open_positions"] == 1  # MSFT open

    def test_pairs_active(self, dashboard):
        metrics = dashboard.generate()
        assert metrics["pairs_active"] == 1

    def test_disk_metrics_present(self, dashboard):
        metrics = dashboard.generate()
        assert "disk_free_gb" in metrics

    def test_phase_phase1(self, dashboard):
        metrics = dashboard.generate()
        assert metrics["phase"] == "PHASE_1"


class TestWrite:
    def test_json_written(self, dashboard, tmp_path):
        with patch("monitoring.health_dashboard._DASHBOARD_JSON", str(tmp_path / "dashboard.json")):
            with patch("monitoring.health_dashboard._DASHBOARD_LOG", str(tmp_path / "dash.log")):
                dashboard.write()
        assert os.path.exists(str(tmp_path / "dashboard.json"))
        with open(str(tmp_path / "dashboard.json")) as fh:
            data = json.load(fh)
        assert "timestamp" in data

    def test_log_written(self, dashboard, tmp_path):
        with patch("monitoring.health_dashboard._DASHBOARD_JSON", str(tmp_path / "dashboard.json")):
            with patch("monitoring.health_dashboard._DASHBOARD_LOG", str(tmp_path / "dash.log")):
                dashboard.write()
        assert os.path.exists(str(tmp_path / "dash.log"))
        content = open(str(tmp_path / "dash.log")).read()
        assert "APOLLO" in content
        assert "Phase" in content


class TestDailyTelegram:
    def test_no_duplicate_send(self, dashboard, tmp_path):
        """If already sent today, should not call requests."""
        sentinel = str(tmp_path / "sentinel.txt")
        import datetime
        today = datetime.date.today().isoformat()
        with open(sentinel, "w") as fh:
            fh.write(today)
        with patch("monitoring.health_dashboard._DAILY_SENT_FILE", sentinel):
            with patch("requests.post") as mock_post:
                dashboard.send_daily_telegram()
        mock_post.assert_not_called()

    def test_sends_when_not_sent_today(self, dashboard, tmp_path):
        sentinel = str(tmp_path / "never_sent.txt")
        with patch("monitoring.health_dashboard._DAILY_SENT_FILE", sentinel):
            with patch.object(dashboard, "_config", {
                "notifications": {"telegram": {"bot_token": "tok", "chat_id": "cid"}}
            }):
                with patch("requests.post") as mock_post:
                    mock_post.return_value = MagicMock(ok=True)
                    dashboard.send_daily_telegram()
        mock_post.assert_called_once()


class TestBackgroundThread:
    def test_start_stop(self, dashboard):
        dashboard.start_background(interval_seconds=9999)
        assert dashboard._running is True
        assert dashboard._thread is not None
        assert dashboard._thread.is_alive()
        dashboard.stop()
        assert dashboard._running is False

    def test_double_start_noop(self, dashboard):
        dashboard.start_background(interval_seconds=9999)
        t = dashboard._thread
        dashboard.start_background(interval_seconds=9999)
        assert dashboard._thread is t  # same thread
        dashboard.stop()
