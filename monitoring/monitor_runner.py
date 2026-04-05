"""
MonitorRunner — orchestrates all background monitoring threads.

Single daemon thread that runs a 60-second heartbeat loop.
Handles scheduling of:
  - Daily health report (7am UK = 6am UTC in summer BST)
  - Weekly report (Sunday 6am UTC)
  - 6-hour self-diagnostics
  - 5-minute Telegram retry queue processing
  - Per-minute instant alert checks

Usage:
    from monitoring.monitor_runner import MonitorRunner
    runner = MonitorRunner(config, stream_worker=bot.stream_worker)
    runner.start()
"""
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from monitoring import alert_monitor, telegram_logger
from monitoring.health_reporter import send_daily_report, send_weekly_report
from monitoring.self_diagnostic import run_diagnostic

logger = logging.getLogger(__name__)

_UK_DAILY_REPORT_UTC_HOUR   = 6    # 6am UTC = 7am BST (Apr-Oct) / 6am GMT (Nov-Mar)
_UK_WEEKLY_REPORT_UTC_HOUR  = 6    # Sunday 6am UTC
_DIAGNOSTIC_INTERVAL_HOURS  = 6
_ALERT_CHECK_INTERVAL_SEC   = 60
_RETRY_INTERVAL_MIN         = 5
_QUIET_START_UTC             = 22   # quiet hours start (22:00 UTC)
_QUIET_END_UTC               = 7    # quiet hours end   (07:00 UTC)


def _is_quiet_hours(hour: int) -> bool:
    """Returns True during 22:00-07:00 UTC — only CRITICAL alerts during this window."""
    return hour >= _QUIET_START_UTC or hour < _QUIET_END_UTC


class MonitorRunner:
    """Background daemon thread for all monitoring tasks."""

    def __init__(self, config: dict, stream_worker=None):
        self._config        = config
        self._stream_worker = stream_worker
        self._stop          = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Scheduling state
        self._last_daily_report_date: Optional[str]  = None
        self._last_weekly_report_week: Optional[int] = None
        self._last_diagnostic_hour:    int            = -1
        self._last_retry_min:          int            = -1
        self._last_alert_ts:           float          = 0.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="monitor-runner", daemon=True,
        )
        self._thread.start()
        logger.info("MonitorRunner started")

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def set_stream_worker(self, worker) -> None:
        """Allow the trading bot to register the stream worker after init."""
        self._stream_worker = worker

    # ── main loop ─────────────────────────────────────────────────────────

    def _run(self) -> None:
        logger.info("MonitorRunner loop started")
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:
                logger.error("MonitorRunner tick error: %s", exc)
            self._stop.wait(timeout=_ALERT_CHECK_INTERVAL_SEC)
        logger.info("MonitorRunner loop exited")

    def _tick(self) -> None:
        now     = datetime.now(timezone.utc)
        today   = now.strftime("%Y-%m-%d")
        weekday = now.weekday()   # 0=Mon … 6=Sun

        # ── 1. Daily health report (6am UTC, once per day) ───────────────
        if (now.hour == _UK_DAILY_REPORT_UTC_HOUR
                and now.minute < 2
                and self._last_daily_report_date != today):
            self._last_daily_report_date = today
            self._spawn(send_daily_report, self._config)

        # ── 2. Weekly report (Sunday 6am UTC) ────────────────────────────
        week_num = now.isocalendar()[1]
        if (weekday == 6   # Sunday
                and now.hour == _UK_WEEKLY_REPORT_UTC_HOUR
                and now.minute < 2
                and self._last_weekly_report_week != week_num):
            self._last_weekly_report_week = week_num
            self._spawn(send_weekly_report, self._config)

        # ── 3. Self-diagnostic every 6 hours ─────────────────────────────
        diag_slot = (now.hour // _DIAGNOSTIC_INTERVAL_HOURS) * _DIAGNOSTIC_INTERVAL_HOURS
        if (now.minute < 2
                and diag_slot != self._last_diagnostic_hour):
            self._last_diagnostic_hour = diag_slot
            quiet = _is_quiet_hours(now.hour)
            self._spawn(run_diagnostic, self._config, quiet)

        # ── 4. Instant alert checks every 60s ────────────────────────────
        elapsed = time.monotonic() - self._last_alert_ts
        if elapsed >= _ALERT_CHECK_INTERVAL_SEC:
            self._last_alert_ts = time.monotonic()
            try:
                alert_monitor.run_all_checks(self._config, self._stream_worker)
            except Exception as exc:
                logger.debug("alert checks: %s", exc)

        # ── 5. Retry failed Telegram messages every 5 minutes ────────────
        if now.minute % _RETRY_INTERVAL_MIN == 0 and now.minute != self._last_retry_min:
            self._last_retry_min = now.minute
            self._process_retry_queue()

    def _process_retry_queue(self) -> None:
        items = telegram_logger.pop_retry_queue()
        if not items:
            return
        logger.info("Retrying %d failed Telegram messages", len(items))
        try:
            from altdata.notifications.notifier import Notifier
            n = Notifier(self._config)
        except Exception:
            # Re-queue everything if notifier fails to init
            for item in items:
                telegram_logger.queue_retry(item["type"], item["message"])
            return
        for item in items:
            try:
                delivered = n._send_telegram(item["message"])
                telegram_logger.log_message(
                    item["type"] + "_retry", item["message"], delivered
                )
                if not delivered:
                    telegram_logger.queue_retry(item["type"], item["message"])
            except Exception:
                telegram_logger.queue_retry(item["type"], item["message"])

    @staticmethod
    def _spawn(fn, *args) -> None:
        """Run *fn* in a short-lived daemon thread."""
        t = threading.Thread(target=fn, args=args, daemon=True)
        t.start()


# ── module-level start helper ──────────────────────────────────────────────

_runner: Optional[MonitorRunner] = None


def start_monitoring(config: dict, stream_worker=None) -> MonitorRunner:
    """
    Initialise and start the MonitorRunner singleton.
    Safe to call multiple times — only starts once.
    """
    global _runner
    if _runner is not None and _runner.is_alive():
        if stream_worker is not None:
            _runner.set_stream_worker(stream_worker)
        return _runner
    _runner = MonitorRunner(config, stream_worker)
    _runner.start()
    return _runner


def get_monitor_runner() -> Optional[MonitorRunner]:
    return _runner
