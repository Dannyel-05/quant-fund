"""
MarketCalendar — 2026 US/UK trading calendars with FOMC dates.

Usage
-----
mc = MarketCalendar()
mc.is_trading_day("us", date(2026, 1, 1))  # False — New Year's Day
mc.next_trading_day("uk", date(2026, 12, 25))
mc.days_to_next_fomc(date.today())
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

Market = Literal["us", "uk"]

# ── 2026 US market holidays (NYSE) ────────────────────────────────────────────
_US_HOLIDAYS_2026: set[date] = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 11, 27), # Black Friday (early close — treated as holiday for simplicity)
    date(2026, 12, 24), # Christmas Eve (early close — treated as holiday)
    date(2026, 12, 25), # Christmas Day
}

# ── 2026 UK market holidays (LSE) ────────────────────────────────────────────
_UK_HOLIDAYS_2026: set[date] = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 4, 6),   # Easter Monday
    date(2026, 5, 4),   # Early May Bank Holiday
    date(2026, 5, 25),  # Spring Bank Holiday
    date(2026, 8, 31),  # Summer Bank Holiday
    date(2026, 12, 24), # Christmas Eve (LSE closed)
    date(2026, 12, 25), # Christmas Day
    date(2026, 12, 28), # Boxing Day (observed)
    date(2026, 12, 31), # New Year's Eve (LSE early close/closed)
}

# ── 2026 FOMC meeting dates (decision day) ────────────────────────────────────
_FOMC_DATES_2026: list[date] = [
    date(2026, 1, 29),
    date(2026, 3, 19),
    date(2026, 4, 30),  # tentative — check Fed calendar
    date(2026, 6, 18),
    date(2026, 7, 30),
    date(2026, 9, 17),
    date(2026, 10, 29),
    date(2026, 12, 10),
]

_HOLIDAYS: dict[str, set[date]] = {
    "us": _US_HOLIDAYS_2026,
    "uk": _UK_HOLIDAYS_2026,
}


class MarketCalendar:
    """
    Simple static calendar for 2026 US and UK markets.
    """

    def is_trading_day(self, market: Market, dt: date | None = None) -> bool:
        if dt is None:
            dt = date.today()
        if dt.weekday() >= 5:  # Sat=5, Sun=6
            return False
        return dt not in _HOLIDAYS.get(market, set())

    def is_us_trading_day(self, dt: date | None = None) -> bool:
        return self.is_trading_day("us", dt)

    def is_uk_trading_day(self, dt: date | None = None) -> bool:
        return self.is_trading_day("uk", dt)

    def next_trading_day(self, market: Market, dt: date | None = None) -> date:
        if dt is None:
            dt = date.today()
        candidate = dt + timedelta(days=1)
        while not self.is_trading_day(market, candidate):
            candidate += timedelta(days=1)
        return candidate

    def prev_trading_day(self, market: Market, dt: date | None = None) -> date:
        if dt is None:
            dt = date.today()
        candidate = dt - timedelta(days=1)
        while not self.is_trading_day(market, candidate):
            candidate -= timedelta(days=1)
        return candidate

    def trading_days_between(self, market: Market, start: date, end: date) -> int:
        count = 0
        cur = start
        while cur <= end:
            if self.is_trading_day(market, cur):
                count += 1
            cur += timedelta(days=1)
        return count

    def days_to_next_fomc(self, dt: date | None = None) -> int | None:
        if dt is None:
            dt = date.today()
        future = [d for d in _FOMC_DATES_2026 if d > dt]
        if not future:
            return None
        return (future[0] - dt).days

    def next_fomc_date(self, dt: date | None = None) -> date | None:
        if dt is None:
            dt = date.today()
        future = [d for d in _FOMC_DATES_2026 if d > dt]
        return future[0] if future else None

    def is_fomc_week(self, dt: date | None = None) -> bool:
        """True if dt falls in the same ISO week as an FOMC decision day."""
        if dt is None:
            dt = date.today()
        for fomc in _FOMC_DATES_2026:
            if dt.isocalendar()[:2] == fomc.isocalendar()[:2]:
                return True
        return False

    def holiday_name(self, market: Market, dt: date) -> str | None:
        """Return a human-readable label for a holiday if applicable."""
        _US_NAMES = {
            date(2026, 1, 1):  "New Year's Day",
            date(2026, 1, 19): "MLK Day",
            date(2026, 2, 16): "Presidents' Day",
            date(2026, 4, 3):  "Good Friday",
            date(2026, 5, 25): "Memorial Day",
            date(2026, 6, 19): "Juneteenth",
            date(2026, 7, 3):  "Independence Day (observed)",
            date(2026, 9, 7):  "Labor Day",
            date(2026, 11, 26):"Thanksgiving",
            date(2026, 12, 25):"Christmas Day",
        }
        _UK_NAMES = {
            date(2026, 1, 1):  "New Year's Day",
            date(2026, 4, 3):  "Good Friday",
            date(2026, 4, 6):  "Easter Monday",
            date(2026, 5, 4):  "May Bank Holiday",
            date(2026, 5, 25): "Spring Bank Holiday",
            date(2026, 8, 31): "Summer Bank Holiday",
            date(2026, 12, 25):"Christmas Day",
            date(2026, 12, 28):"Boxing Day",
        }
        names = {"us": _US_NAMES, "uk": _UK_NAMES}.get(market, {})
        return names.get(dt)
