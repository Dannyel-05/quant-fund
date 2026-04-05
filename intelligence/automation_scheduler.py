"""
Daily Automation Scheduler
============================
Runs all data collection and signal generation on a fixed schedule.
Uses the 'schedule' library (already installed).

Schedule (all times UTC):
  06:00 — Data collection (altdata, macro, rates, commodities)
  07:00 — Morning intelligence (macro briefing, earnings calendar)
  08:15 — UK market scan
  14:45 — US market scan
  18:00 — Midday position check
  21:30 — EOD: close positions, equity curve, decay check
  Sunday 03:00 — Weekly deep work (retrain, pairs scan, stress test)
"""
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule
import yaml

logger = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parents[1]


def _load_config():
    return yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())


# ── Job functions ──────────────────────────────────────────────────────────────

def job_collect_data():
    """06:00 UTC — collect all alt and macro data."""
    logger.info("=== SCHEDULED: data collection ===")
    try:
        import subprocess
        subprocess.run([sys.executable, "main.py", "altdata", "collect"],
                       cwd=_ROOT, timeout=1800)
    except Exception as e:
        logger.error("Data collection failed: %s", e)


def job_update_prices():
    """06:30 UTC — update daily price history for full universe (last 5 days)."""
    logger.info("=== SCHEDULED: price history update ===")
    try:
        import subprocess
        from datetime import datetime, timedelta
        start = (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")
        subprocess.run(
            [sys.executable, "main.py", "historical", "collect",
             "--phases", "prices", "--start", start],
            cwd=_ROOT, timeout=1800
        )
    except Exception as e:
        logger.error("Price history update failed: %s", e)


def job_morning_intelligence():
    """07:00 UTC — morning macro briefing."""
    logger.info("=== SCHEDULED: morning intelligence ===")
    try:
        config = _load_config()
        from intelligence.daily_pipeline import DailyPipeline
        dp = DailyPipeline(config)
        dp.run_morning()
    except Exception as e:
        logger.error("Morning intelligence failed: %s", e)


def job_uk_scan():
    """08:15 UTC — UK market scan."""
    logger.info("=== SCHEDULED: UK market scan ===")
    try:
        from execution.paper_trader import PaperTrader
        config = _load_config()
        pt = PaperTrader(config)
        pt.run_scan(market='uk')
    except Exception as e:
        logger.error("UK scan failed: %s", e)


def job_us_scan():
    """14:45 UTC — US market scan."""
    logger.info("=== SCHEDULED: US market scan ===")
    try:
        from execution.paper_trader import PaperTrader
        config = _load_config()
        pt = PaperTrader(config)
        pt.run_scan(market='us')
    except Exception as e:
        logger.error("US scan failed: %s", e)


def job_midday_check():
    """18:00 UTC — midday position check."""
    logger.info("=== SCHEDULED: midday position check ===")
    try:
        from execution.paper_trader import PaperTrader
        config = _load_config()
        pt = PaperTrader(config)
        pt.check_open_positions()
    except Exception as e:
        logger.error("Midday check failed: %s", e)


def job_eod():
    """21:30 UTC — end of day + daily simulation."""
    logger.info("=== SCHEDULED: end of day ===")
    try:
        from execution.paper_trader import PaperTrader
        config = _load_config()
        pt = PaperTrader(config)
        pt.run_eod()
    except Exception as e:
        logger.error("EOD failed: %s", e)

    # Daily simulation — runs after EOD, non-blocking
    try:
        from simulations.sim_scheduler import run_daily_simulation
        run_daily_simulation()
    except Exception as e:
        logger.error("Daily simulation failed: %s", e)


def job_weekly():
    """Sunday 03:00 UTC — weekly deep work (signal weight optimisation)."""
    if datetime.utcnow().weekday() != 6:  # 6 = Sunday
        return
    logger.info("=== SCHEDULED: weekly deep work ===")
    try:
        from closeloop.learning.batch_retrainer import BatchRetrainer
        config = _load_config()
        BatchRetrainer(config).run()
    except Exception as e:
        logger.error("Weekly batch retrain failed: %s", e)


def job_retraining_monitor():
    """Every 6 hours — check ML model performance triggers (stays dormant until threshold met)."""
    logger.info("=== SCHEDULED: retraining monitor ===")
    try:
        from core.retraining_controller import RetrainingController
        RetrainingController().run_monitoring_cycle()
    except Exception as e:
        logger.error("Retraining monitor failed: %s", e)


def job_weekly_report():
    """Sunday 09:00 UTC — Apollo weekly report (8 sections via Telegram)."""
    if datetime.utcnow().weekday() != 6:  # 6 = Sunday
        return
    logger.info("=== SCHEDULED: weekly report ===")
    try:
        from monitoring.weekly_report import WeeklyReportGenerator
        weekly_reporter = WeeklyReportGenerator()
        weekly_reporter.send_weekly_report()
    except Exception as e:
        logger.error("Weekly report failed: %s", e)


# ── Scheduler ─────────────────────────────────────────────────────────────────

class AutomationScheduler:
    def __init__(self):
        self._running = False

    def setup(self):
        schedule.every().day.at("06:00").do(job_collect_data)
        schedule.every().day.at("06:30").do(job_update_prices)
        schedule.every().day.at("07:00").do(job_morning_intelligence)
        schedule.every().day.at("08:15").do(job_uk_scan)
        schedule.every().day.at("14:45").do(job_us_scan)
        schedule.every().day.at("18:00").do(job_midday_check)
        schedule.every().day.at("21:30").do(job_eod)
        schedule.every().day.at("03:00").do(job_weekly)
        schedule.every().day.at("09:00").do(job_weekly_report)
        schedule.every(6).hours.do(job_retraining_monitor)
        logger.info("Automation scheduler configured with 10 jobs")

    def run(self):
        self.setup()
        self._running = True

        def _stop(sig, frame):
            logger.info("Scheduler stopping...")
            self._running = False

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
        logger.info("Automation scheduler started. Press Ctrl+C to stop.")
        while self._running:
            schedule.run_pending()
            time.sleep(30)

    def status(self):
        self.setup()
        lines = ["AUTOMATION SCHEDULE (UTC)", "=" * 50]
        for job in schedule.jobs:
            lines.append(f"  {job.next_run.strftime('%H:%M')}  {job.job_func.__name__}")
        return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s")
    AutomationScheduler().run()
