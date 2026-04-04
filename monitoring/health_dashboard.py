"""
HealthDashboard — live system metrics dashboard.

Writes every collection cycle (overwrites, not appends):
  logs/apollo_health_dashboard.log  — human-readable
  output/dashboard.json             — machine-readable

Sends daily Telegram summary at 09:00 UTC.
Start with start_background(interval_seconds=300).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import threading
import time
from datetime import datetime, date
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DASHBOARD_LOG  = "logs/apollo_health_dashboard.log"
_DASHBOARD_JSON = "output/dashboard.json"
_DAILY_SENT_FILE = "output/last_dashboard_telegram_date.txt"
_CLOSELOOP_DB   = "closeloop/storage/closeloop.db"


class HealthDashboard:
    """
    Aggregates live system metrics and writes dashboard files.
    """

    def __init__(
        self,
        config: Optional[Dict] = None,
        store=None,
        paper_trader=None,
        regime_detector=None,
        pairs_trader=None,
        closeloop_db: Optional[str] = None,
    ) -> None:
        self._config         = config or {}
        self._store          = store
        self._paper_trader   = paper_trader
        self._regime_detector = regime_detector
        self._pairs_trader   = pairs_trader
        self._db_path        = closeloop_db or _CLOSELOOP_DB
        self._running        = False
        self._thread: Optional[threading.Thread] = None

    # ── metric collection ─────────────────────────────────────────────────

    def generate(self) -> Dict[str, Any]:
        """Collect all metrics and return dashboard dict."""
        metrics: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Phase + trade count
        metrics.update(self._get_phase_metrics())

        # Open positions
        metrics["open_positions"] = self._get_open_positions()

        # Today PnL
        metrics["today_pnl_usd"] = self._get_today_pnl()

        # Active signals count
        metrics["active_signals_today"] = self._get_signal_count()

        # Pairs trading
        metrics["pairs_active"] = self._get_pairs_count()

        # Regime
        metrics["regime_state"] = self._get_regime()

        # Kalman status
        metrics["kalman_status"] = self._get_kalman_status()

        # Last Telegram msg id
        metrics["last_telegram_msg_id"] = self._get_last_telegram_id()

        # System resources
        metrics.update(self._get_system_resources())

        return metrics

    # ── sub-collectors ────────────────────────────────────────────────────

    def _get_phase_metrics(self) -> Dict[str, Any]:
        try:
            con = sqlite3.connect(self._db_path, timeout=5)
            real_trades = con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT MIN(rowid) FROM trade_ledger
                    WHERE exit_date IS NOT NULL AND gross_pnl != 0.0
                    AND (is_phantom=0 OR is_phantom IS NULL)
                    GROUP BY ticker, entry_date
                )
            """).fetchone()[0]
            con.close()
        except Exception:
            real_trades = 0

        # Determine phase from trade count
        phase = "PHASE_1"
        if real_trades >= 2000:
            phase = "PHASE_FREE"
        elif real_trades >= 1000:
            phase = "PHASE_5"
        elif real_trades >= 600:
            phase = "PHASE_4"
        elif real_trades >= 300:
            phase = "PHASE_3"
        elif real_trades >= 100:
            phase = "PHASE_2"

        return {"phase": phase, "real_trade_count": real_trades}

    def _get_open_positions(self) -> int:
        try:
            con = sqlite3.connect(self._db_path, timeout=5)
            n = con.execute(
                "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL"
            ).fetchone()[0]
            con.close()
            return n
        except Exception:
            return 0

    def _get_today_pnl(self) -> float:
        try:
            today = date.today().isoformat()
            con = sqlite3.connect(self._db_path, timeout=5)
            pnl = con.execute(
                "SELECT COALESCE(SUM(gross_pnl), 0) FROM trade_ledger WHERE date(exit_date)=?",
                (today,),
            ).fetchone()[0]
            con.close()
            return float(pnl or 0.0)
        except Exception:
            return 0.0

    def _get_signal_count(self) -> int:
        try:
            today = date.today().isoformat()
            con = sqlite3.connect(self._db_path, timeout=5)
            n = con.execute(
                "SELECT COUNT(*) FROM signals_log WHERE date(timestamp)=?", (today,)
            ).fetchone()[0]
            con.close()
            return n
        except Exception:
            return 0

    def _get_pairs_count(self) -> int:
        try:
            con = sqlite3.connect(self._db_path, timeout=5)
            n = con.execute(
                "SELECT COUNT(*) FROM cointegration_log WHERE status='valid'"
            ).fetchone()[0]
            con.close()
            return n
        except Exception:
            return 0

    def _get_regime(self) -> str:
        if self._regime_detector is not None:
            try:
                return self._regime_detector.detect()
            except Exception:
                pass
        return "UNKNOWN"

    def _get_kalman_status(self) -> str:
        try:
            import pykalman  # noqa: F401
            return "active"
        except ImportError:
            try:
                from analysis.mathematical_signals import KalmanSignalSmoother  # noqa: F401
                return "available (pykalman missing — graceful degradation)"
            except Exception:
                return "unavailable"

    def _get_last_telegram_id(self) -> Optional[int]:
        try:
            if os.path.exists("output/last_telegram_msg_id.txt"):
                with open("output/last_telegram_msg_id.txt") as fh:
                    return int(fh.read().strip())
        except Exception:
            pass
        return None

    def _get_system_resources(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        # Disk
        try:
            du = shutil.disk_usage("/")
            res["disk_used_gb"]  = round(du.used  / 1e9, 2)
            res["disk_free_gb"]  = round(du.free  / 1e9, 2)
            res["disk_total_gb"] = round(du.total / 1e9, 2)
        except Exception:
            pass

        # RAM
        try:
            with open("/proc/meminfo") as fh:
                info: Dict[str, int] = {}
                for line in fh:
                    parts = line.split()
                    if len(parts) >= 2:
                        info[parts[0].rstrip(":")] = int(parts[1])
            total_kb = info.get("MemTotal", 0)
            avail_kb = info.get("MemAvailable", 0)
            used_kb  = total_kb - avail_kb
            res["ram_used_gb"]  = round(used_kb  * 1024 / 1e9, 2)
            res["ram_total_gb"] = round(total_kb * 1024 / 1e9, 2)
        except Exception:
            pass

        # CPU (1-minute load average)
        try:
            with open("/proc/loadavg") as fh:
                load1 = float(fh.read().split()[0])
            res["cpu_load_1m"] = load1
        except Exception:
            try:
                import psutil
                res["cpu_pct"] = psutil.cpu_percent(interval=0)
            except Exception:
                pass

        return res

    # ── writing ───────────────────────────────────────────────────────────

    def write(self, metrics: Optional[Dict] = None) -> None:
        """Write dashboard log and JSON. Generates metrics if not provided."""
        if metrics is None:
            metrics = self.generate()

        os.makedirs("logs",   exist_ok=True)
        os.makedirs("output", exist_ok=True)

        # JSON
        try:
            with open(_DASHBOARD_JSON, "w") as fh:
                json.dump(metrics, fh, indent=2, default=str)
        except Exception as exc:
            logger.debug("write dashboard.json: %s", exc)

        # Text log
        try:
            lines = [
                "=" * 68,
                "APOLLO QUANT FUND — LIVE HEALTH DASHBOARD",
                f"Updated: {metrics.get('timestamp', 'unknown')}",
                "=" * 68,
                "",
                f"Phase:              {metrics.get('phase', '?')}",
                f"Real trades:        {metrics.get('real_trade_count', '?')}",
                f"Open positions:     {metrics.get('open_positions', '?')}",
                f"Today PnL:          ${metrics.get('today_pnl_usd', 0):,.2f}",
                f"Active signals:     {metrics.get('active_signals_today', '?')}",
                f"Regime:             {metrics.get('regime_state', '?')}",
                "",
                f"Pairs active:       {metrics.get('pairs_active', '?')}",
                f"Kalman filter:      {metrics.get('kalman_status', '?')}",
                f"Last Telegram ID:   {metrics.get('last_telegram_msg_id', 'N/A')}",
                "",
                "── System Resources ──",
                f"Disk free:          {metrics.get('disk_free_gb', '?')} GB",
                f"RAM used:           {metrics.get('ram_used_gb', '?')} / {metrics.get('ram_total_gb', '?')} GB",
                f"CPU load (1m):      {metrics.get('cpu_load_1m', metrics.get('cpu_pct', '?'))}",
                "",
                "=" * 68,
            ]
            with open(_DASHBOARD_LOG, "w") as fh:
                fh.write("\n".join(lines) + "\n")
        except Exception as exc:
            logger.debug("write dashboard log: %s", exc)

    # ── Telegram daily ────────────────────────────────────────────────────

    def send_daily_telegram(self, metrics: Optional[Dict] = None) -> None:
        """Send daily summary to Telegram. Guards against duplicate sends."""
        today_str = date.today().isoformat()
        try:
            if os.path.exists(_DAILY_SENT_FILE):
                with open(_DAILY_SENT_FILE) as fh:
                    last = fh.read().strip()
                if last == today_str:
                    return  # already sent today
        except Exception:
            pass

        if metrics is None:
            metrics = self.generate()

        text = (
            f"[HealthDashboard] Daily Summary — {today_str}\n"
            f"Phase: {metrics.get('phase','?')} | Trades: {metrics.get('real_trade_count','?')}\n"
            f"Open positions: {metrics.get('open_positions','?')}\n"
            f"Today PnL: ${metrics.get('today_pnl_usd',0):,.2f}\n"
            f"Regime: {metrics.get('regime_state','?')}\n"
            f"Pairs active: {metrics.get('pairs_active','?')}\n"
            f"RAM used: {metrics.get('ram_used_gb','?')} GB | "
            f"Disk free: {metrics.get('disk_free_gb','?')} GB"
        )
        try:
            import requests
            tg = self._config.get("notifications", {}).get("telegram", {})
            token   = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
            if token and chat_id:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=10,
                )
                if resp.ok:
                    try:
                        with open(_DAILY_SENT_FILE, "w") as fh:
                            fh.write(today_str)
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("send_daily_telegram: %s", exc)

    # ── background loop ───────────────────────────────────────────────────

    def start_background(self, interval_seconds: int = 300) -> None:
        """Start background metrics collection + writing thread."""
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop,
            args=(interval_seconds,),
            daemon=True,
            name="HealthDashboard",
        )
        self._thread.start()
        logger.info("HealthDashboard started (interval=%ds)", interval_seconds)

    def stop(self) -> None:
        self._running = False

    def _loop(self, interval: int) -> None:
        while self._running:
            try:
                metrics = self.generate()
                self.write(metrics)
                # Send daily at 09:00 UTC
                if datetime.utcnow().hour == 9:
                    self.send_daily_telegram(metrics)
            except Exception as exc:
                logger.exception("HealthDashboard loop error: %s", exc)
            time.sleep(interval)
