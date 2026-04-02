"""
Calendar Effects Signal
========================
Detects seasonal, calendar-based trading opportunities:
  - January effect (small-cap outperformance)
  - Tax loss selling reversal
  - End-of-quarter window dressing
  - Earnings season timing
  - Fed meeting calendar
  - Options expiry calendar

All effects generate size modifiers for PEAD and other signals.
"""
import json
import logging
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Earnings season windows: (month_start, day_start, month_end, day_end)
_EARNINGS_SEASONS: List[Tuple[int, int, int, int]] = [
    (1, 15, 2, 15),   # Q4 earnings season
    (4, 15, 5, 15),   # Q1 earnings season
    (7, 15, 8, 15),   # Q2 earnings season
    (10, 15, 11, 15), # Q3 earnings season
]

# End-of-quarter months
_QUARTER_END_MONTHS = {3, 6, 9, 12}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def is_nth_trading_day_of_month(dt: datetime, n: int) -> bool:
    """
    Approximate check whether dt falls on or near the nth business day
    of the month.  Uses calendar days with a ±1 day tolerance to account
    for weekends and public holidays.
    """
    # Count business days from the 1st of the month up to dt.date()
    first = date(dt.year, dt.month, 1)
    target = dt.date() if hasattr(dt, "date") else dt
    bdays = 0
    cursor = first
    while cursor <= target:
        if cursor.weekday() < 5:  # Monday–Friday
            bdays += 1
        if cursor == target:
            break
        cursor += timedelta(days=1)
    return abs(bdays - n) <= 1


def days_to_next_friday(dt: datetime) -> int:
    """Return the number of calendar days to the next Friday from dt."""
    weekday = dt.weekday()  # Monday=0, Friday=4
    days_ahead = (4 - weekday) % 7
    if days_ahead == 0:
        days_ahead = 7  # already Friday → next Friday
    return days_ahead


def is_triple_witching_week(dt: datetime) -> bool:
    """
    Return True if dt falls in the triple-witching expiry week.

    Triple witching = 3rd Friday of March, June, September, December.
    We flag the entire Mon-Fri week containing that Friday.
    """
    if dt.month not in {3, 6, 9, 12}:
        return False
    # Find 3rd Friday of the month
    first_day = date(dt.year, dt.month, 1)
    friday_count = 0
    cursor = first_day
    third_friday = None
    while cursor.month == dt.month:
        if cursor.weekday() == 4:  # Friday
            friday_count += 1
            if friday_count == 3:
                third_friday = cursor
                break
        cursor += timedelta(days=1)

    if third_friday is None:
        return False

    # Week containing third_friday: Monday to Friday
    week_start = third_friday - timedelta(days=4)
    target = dt.date() if hasattr(dt, "date") else dt
    return week_start <= target <= third_friday


def get_earnings_season_week(dt: datetime) -> int:
    """
    Return which week (1-4) of earnings season dt falls in, or 0 if between seasons.

    Earnings seasons:
      Jan 15 – Feb 15 (Q4)
      Apr 15 – May 15 (Q1)
      Jul 15 – Aug 15 (Q2)
      Oct 15 – Nov 15 (Q3)
    """
    target = dt.date() if hasattr(dt, "date") else dt
    year = target.year

    for m_start, d_start, m_end, d_end in _EARNINGS_SEASONS:
        season_start = date(year, m_start, d_start)
        season_end = date(year, m_end, d_end)
        if season_start <= target <= season_end:
            days_in = (target - season_start).days
            week = min(4, days_in // 7 + 1)
            return week

    return 0  # between seasons


def _is_last_week_of_quarter(dt: datetime) -> bool:
    """Return True if dt is in the final 7 calendar days of a quarter-end month."""
    target = dt.date() if hasattr(dt, "date") else dt
    if target.month not in _QUARTER_END_MONTHS:
        return False
    # Find last day of the month
    if target.month == 12:
        last_day = date(target.year, 12, 31)
    else:
        next_month_first = date(target.year, target.month + 1, 1)
        last_day = next_month_first - timedelta(days=1)
    return (last_day - target).days <= 6


# ---------------------------------------------------------------------------
# Calendar effect modifiers
# ---------------------------------------------------------------------------

def january_effect(dt: datetime, is_small_cap: bool = True) -> float:
    """
    Small-cap outperformance in early January.

    Returns +0.10 for small caps in the first 15 days of January.
    """
    target = dt.date() if hasattr(dt, "date") else dt
    if target.month == 1 and target.day <= 15 and is_small_cap:
        return 0.10
    return 0.0


def tax_loss_reversal(dt: datetime) -> float:
    """
    Tax-loss selling reversal effect: first 7 days of January.

    Stocks beaten down in December often rebound in early January.
    Returns +0.08.
    """
    target = dt.date() if hasattr(dt, "date") else dt
    if target.month == 1 and target.day <= 7:
        return 0.08
    return 0.0


def window_dressing(dt: datetime, ytd_return_pct: Optional[float] = None) -> float:
    """
    End-of-quarter window dressing effect.

    Fund managers buy winners in the last week of each quarter.
    Returns +0.05 for strong YTD performers at quarter-end.
    """
    if not _is_last_week_of_quarter(dt):
        return 0.0
    if ytd_return_pct is not None and ytd_return_pct > 10:
        return 0.05
    return 0.0


def earnings_season_timing(dt: datetime) -> float:
    """
    PEAD signals are strongest in the first two weeks of earnings season.

    Returns:
      +0.10  weeks 1-2 of earnings season
      -0.05  weeks 3-4 of earnings season
      -0.10  between seasons
    """
    week = get_earnings_season_week(dt)
    if week in (1, 2):
        return 0.10
    if week in (3, 4):
        return -0.05
    return -0.10  # between seasons


def fed_meeting_proximity(dt: datetime, days_to_next_meeting: Optional[int]) -> float:
    """
    Reduce position sizing near FOMC meetings.

    Returns:
      -0.30  meeting day (days_to_next_meeting <= 1)
      -0.25  1-7 days before meeting
      -0.15  8-14 days before meeting
       0.0   otherwise
    """
    if days_to_next_meeting is None:
        return 0.0
    if days_to_next_meeting <= 1:
        return -0.30
    if days_to_next_meeting <= 7:
        return -0.25
    if days_to_next_meeting <= 14:
        return -0.15
    return 0.0


def options_expiry_proximity(dt: datetime) -> float:
    """
    Reduce position sizing near options expiry events.

    Returns:
      -0.20  triple-witching week (Mar/Jun/Sep/Dec 3rd Friday)
      -0.10  standard monthly expiry week (3rd week of month)
       0.0   otherwise
    """
    if is_triple_witching_week(dt):
        return -0.20

    days_to_friday = days_to_next_friday(dt)
    target = dt.date() if hasattr(dt, "date") else dt
    is_exp_week = (days_to_friday <= 5) and (14 <= target.day <= 21)
    if is_exp_week:
        return -0.10

    return 0.0


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

def _get_archive_db_path() -> str:
    """Resolve path to permanent_archive.db in output/."""
    base = Path(__file__).resolve().parent.parent
    return str(base / "output" / "permanent_archive.db")


def _ensure_calendar_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_signals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            date             TEXT NOT NULL,
            total_modifier   REAL,
            january_effect   REAL,
            tax_loss_reversal REAL,
            earnings_season  REAL,
            options_expiry   REAL,
            fed_proximity    REAL,
            calculated_at    TEXT
        )
        """
    )
    conn.commit()


def _store_calendar_signal(record: Dict) -> None:
    """Persist calendar signal record to permanent_archive.db."""
    try:
        db_path = _get_archive_db_path()
        conn = sqlite3.connect(db_path)
        _ensure_calendar_table(conn)
        conn.execute(
            """
            INSERT INTO calendar_signals
              (date, total_modifier, january_effect, tax_loss_reversal,
               earnings_season, options_expiry, fed_proximity, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["effective_date"][:10],
                record["total_modifier"],
                record["january_effect"],
                record["tax_loss_reversal"],
                record["earnings_season"],
                record["options_expiry"],
                record["fed_proximity"],
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        logger.debug("calendar_signals: stored record for %s", record["effective_date"][:10])
    except Exception as exc:
        logger.warning("calendar_signals: DB store failed: %s", exc)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class CalendarEffectsSignal:
    """
    Aggregates seasonal and calendar-based modifiers for PEAD and
    other signal sizing.

    Total modifier is clamped to [-0.40, +0.30].
    """

    def __init__(self, config: dict):
        self.config = config

    def get_composite_modifier(
        self,
        dt: Optional[datetime] = None,
        is_small_cap: bool = True,
        days_to_fed: Optional[int] = None,
        ytd_return_pct: Optional[float] = None,
        store_to_db: bool = True,
    ) -> Dict:
        """
        Compute all calendar effects and return a composite modifier dict.

        Parameters
        ----------
        dt             : reference datetime (defaults to UTC now)
        is_small_cap   : whether the target ticker is small-cap
        days_to_fed    : calendar days to next FOMC meeting, or None
        ytd_return_pct : year-to-date return of the ticker (for window dressing)
        store_to_db    : whether to persist to permanent_archive.db

        Returns
        -------
        {
          "total_modifier":    float  (clamped [-0.40, +0.30]),
          "january_effect":    float,
          "tax_loss_reversal": float,
          "earnings_season":   float,
          "options_expiry":    float,
          "fed_proximity":     float,
          "effective_date":    str (ISO 8601)
        }
        """
        if dt is None:
            dt = datetime.utcnow()

        jan_eff = january_effect(dt, is_small_cap=is_small_cap)
        tax_rev = tax_loss_reversal(dt)
        earn_seas = earnings_season_timing(dt)
        opts_exp = options_expiry_proximity(dt)
        fed_prox = fed_meeting_proximity(dt, days_to_fed)
        win_dress = window_dressing(dt, ytd_return_pct=ytd_return_pct)

        raw_total = jan_eff + tax_rev + earn_seas + opts_exp + fed_prox + win_dress
        total = max(-0.40, min(0.30, raw_total))

        result = {
            "total_modifier": round(total, 4),
            "january_effect": jan_eff,
            "tax_loss_reversal": tax_rev,
            "earnings_season": earn_seas,
            "options_expiry": opts_exp,
            "fed_proximity": fed_prox,
            "window_dressing": win_dress,
            "effective_date": dt.isoformat(),
        }

        logger.info(
            "CalendarEffects %s: total=%.3f (jan=%.2f, tax=%.2f, earn=%.2f, "
            "opts=%.2f, fed=%.2f, win=%.2f)",
            dt.strftime("%Y-%m-%d"),
            total,
            jan_eff,
            tax_rev,
            earn_seas,
            opts_exp,
            fed_prox,
            win_dress,
        )

        if store_to_db:
            _store_calendar_signal(result)

        return result

    def get_pead_timing_modifier(
        self,
        dt: Optional[datetime] = None,
        is_small_cap: bool = True,
        days_to_fed: Optional[int] = None,
    ) -> float:
        """
        Shortcut — returns just the total_modifier for PEAD signal sizing.

        Parameters
        ----------
        dt           : reference datetime (defaults to UTC now)
        is_small_cap : whether the target ticker is small-cap
        days_to_fed  : calendar days to next FOMC meeting, or None
        """
        result = self.get_composite_modifier(
            dt=dt,
            is_small_cap=is_small_cap,
            days_to_fed=days_to_fed,
            store_to_db=False,
        )
        return result["total_modifier"]
