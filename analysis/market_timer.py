"""
MarketTimer — pytz-based market open/close window checks for US and UK.

Handles:
  - NYSE: 09:30–16:00 ET (America/New_York)
  - LSE:  08:00–16:30 GMT/BST (Europe/London)
  - Pre-market / post-market windows
  - Power hours (first and last 30 minutes)

Usage
-----
timer = MarketTimer()
timer.is_open("us")              # bool
timer.is_power_hour("us")        # bool
timer.minutes_to_open("uk")      # int (negative if already open)
timer.current_session("us")      # "pre", "open", "power_hour", "close", "after"
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import pytz
    _PYTZ_OK = True
except ImportError:
    _PYTZ_OK = False
    logger.warning("MarketTimer: pytz not installed — using UTC fallback")


_MARKET_TZ = {
    "us": "America/New_York",
    "uk": "Europe/London",
}

_MARKET_HOURS = {
    "us": {
        "pre_open":    dtime(4,  0),
        "open":        dtime(9,  30),
        "power_start": dtime(9,  30),
        "power_end":   dtime(10,  0),
        "close":       dtime(16,  0),
        "close_power_start": dtime(15, 30),
        "after_close": dtime(20,  0),
    },
    "uk": {
        "pre_open":    dtime(7,  0),
        "open":        dtime(8,  0),
        "power_start": dtime(8,  0),
        "power_end":   dtime(8, 30),
        "close":       dtime(16, 30),
        "close_power_start": dtime(16,  0),
        "after_close": dtime(18,  0),
    },
}


def _local_time(market: str) -> Optional[datetime]:
    """Return current datetime in the market's local timezone."""
    tz_name = _MARKET_TZ.get(market, "UTC")
    if _PYTZ_OK:
        tz = pytz.timezone(tz_name)
        return datetime.now(tz)
    else:
        # UTC fallback — imprecise but won't crash
        return datetime.utcnow()


class MarketTimer:
    """
    Timezone-aware market timing checks.
    """

    def local_time(self, market: str) -> Optional[datetime]:
        """Return current local time for the market."""
        return _local_time(market)

    def is_open(self, market: str) -> bool:
        """True if the market is currently in its regular session."""
        dt = _local_time(market)
        if dt is None:
            return False
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        t = dt.time()
        # Skip weekends
        if dt.weekday() >= 5:
            return False
        return h["open"] <= t < h["close"]

    def is_pre_market(self, market: str) -> bool:
        dt = _local_time(market)
        if dt is None or dt.weekday() >= 5:
            return False
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        t = dt.time()
        return h["pre_open"] <= t < h["open"]

    def is_after_hours(self, market: str) -> bool:
        dt = _local_time(market)
        if dt is None or dt.weekday() >= 5:
            return False
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        t = dt.time()
        return h["close"] <= t < h["after_close"]

    def is_power_hour(self, market: str) -> bool:
        """
        True if in the opening power hour (first 30 min) or
        closing power hour (last 30 min before close).
        """
        dt = _local_time(market)
        if dt is None or dt.weekday() >= 5:
            return False
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        t = dt.time()
        opening = h["power_start"] <= t < h["power_end"]
        closing = h["close_power_start"] <= t < h["close"]
        return opening or closing

    def minutes_to_open(self, market: str) -> Optional[int]:
        """
        Minutes until market opens (positive = not yet open, 0 = open, negative = already open).
        """
        dt = _local_time(market)
        if dt is None:
            return None
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        today_open = dt.replace(
            hour=h["open"].hour,
            minute=h["open"].minute,
            second=0, microsecond=0,
        )
        return int((today_open - dt).total_seconds() / 60)

    def minutes_to_close(self, market: str) -> Optional[int]:
        """
        Minutes until market closes (positive = open, 0 or negative = closed).
        """
        dt = _local_time(market)
        if dt is None:
            return None
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        today_close = dt.replace(
            hour=h["close"].hour,
            minute=h["close"].minute,
            second=0, microsecond=0,
        )
        return int((today_close - dt).total_seconds() / 60)

    def current_session(self, market: str) -> str:
        """Return one of: 'pre', 'power_hour', 'open', 'close', 'after', 'closed'."""
        if self.is_power_hour(market):
            return "power_hour"
        if self.is_pre_market(market):
            return "pre"
        if self.is_open(market):
            return "open"
        if self.is_after_hours(market):
            return "after"
        return "closed"

    def should_trade(self, market: str) -> bool:
        """
        True when it is appropriate to place new trades.
        Avoids first and last 5 minutes of session (slippage risk).
        """
        dt = _local_time(market)
        if dt is None or not self.is_open(market):
            return False
        h = _MARKET_HOURS.get(market, _MARKET_HOURS["us"])
        t = dt.time()
        # Avoid first 5 min
        if t < dtime(h["open"].hour, h["open"].minute + 5):
            return False
        # Avoid last 5 min
        close_minus5 = dtime(h["close"].hour, h["close"].minute - 5)
        if t >= close_minus5:
            return False
        return True
