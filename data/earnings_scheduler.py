"""
Daily earnings data scheduler.

Runs at 18:00 UK time (Europe/London) every weekday.
Collects earnings observations and upcoming calendar for the full universe.

Usage (blocking loop):
    python -m data.earnings_scheduler

Or from main.py:
    python main.py earnings schedule
"""
import logging
import signal
import sys
import time
from datetime import datetime

import schedule

logger = logging.getLogger(__name__)


def _uk_time_str() -> str:
    """Return current time as HH:MM string in Europe/London timezone."""
    try:
        import zoneinfo
        from datetime import timezone
        tz = zoneinfo.ZoneInfo("Europe/London")
        return datetime.now(tz=tz).strftime("%H:%M")
    except Exception:
        # Fallback: just use UTC (close enough for scheduling)
        return datetime.utcnow().strftime("%H:%M")


def _is_weekday() -> bool:
    return datetime.now().weekday() < 5  # 0=Mon, 4=Fri


def _run_daily_collection(config: dict, market: str = "us") -> None:
    if not _is_weekday():
        logger.info("Skipping daily earnings collection — weekend")
        return

    from data.earnings_collector import EarningsCollector
    from data.universe import UniverseManager

    logger.info("Daily earnings collection starting at %s UK", _uk_time_str())
    try:
        mgr = UniverseManager(config)
        tickers = mgr.get_tickers(market)

        collector = EarningsCollector(config)

        # Collect last 90 days of observations (catch any recent earnings)
        from datetime import timedelta
        start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        n_obs = collector.collect(tickers, market=market, start=start)

        # Update 30-day forward calendar
        n_cal = collector.collect_calendar(tickers, market=market, days_ahead=30)

        st = collector.db.status()
        logger.info(
            "Daily collection done: %d obs written, %d calendar entries | "
            "DB total=%d tickers=%d range=%s",
            n_obs, n_cal,
            st["total_observations"], st["tickers"], st["date_range"],
        )
    except Exception as e:
        logger.error("Daily earnings collection failed: %s", e, exc_info=True)


def run_scheduler(config: dict, market: str = "us", run_time: str = "18:00") -> None:
    """
    Block forever, running the daily collection at `run_time` UK time.
    Handles SIGINT/SIGTERM for clean shutdown.
    """
    logger.info("Earnings scheduler starting — will collect daily at %s UK", run_time)

    schedule.every().day.at(run_time).do(_run_daily_collection, config=config, market=market)

    # Graceful shutdown on Ctrl-C or SIGTERM
    def _shutdown(signum, frame):
        logger.info("Earnings scheduler shutting down (signal %s)", signum)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    import yaml
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    with open("config/settings.yaml") as f:
        cfg = yaml.safe_load(f)
    run_scheduler(cfg)
