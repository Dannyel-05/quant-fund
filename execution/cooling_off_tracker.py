"""
StockCoolingOffTracker — prevents re-entry into a stock for a configurable
period after a losing exit, with 5 early-release conditions.

Cooling-off period (default 5 trading days) is bypassed early if:
  1. Earnings surprise (beat > 5%) occurs during the cooling-off window.
  2. Volume surge > 3× 20-day average (institutional accumulation signal).
  3. A strong positive altdata signal (score > 0.7) arrives.
  4. Price drops > 15% from exit price (deeper value entry opportunity).
  5. Short-squeeze conditions detected (days-to-cover falls below 2).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_COOLING_DAYS = 5      # trading days
_EARLY_RELEASE_CONDITIONS = [
    "earnings_beat",
    "volume_surge",
    "strong_altdata",
    "price_drop_15pct",
    "short_squeeze",
]


class StockCoolingOffTracker:
    """
    Tracks per-ticker cooling-off periods after losing exits.
    """

    def __init__(self, cooling_days: int = _DEFAULT_COOLING_DAYS) -> None:
        self._cooling_days = cooling_days
        # ticker → {exit_date, exit_price, release_date, early_released, reason}
        self._entries: dict[str, dict] = {}

    # ── registration ──────────────────────────────────────────────────────────

    def register_exit(
        self,
        ticker: str,
        exit_date: date,
        exit_price: float,
        pnl_pct: float,
    ) -> None:
        """
        Call after closing a position.
        Only imposes cooling-off on losses (pnl_pct < 0).
        """
        if pnl_pct >= 0:
            # Winning exits don't trigger cooling-off
            self._entries.pop(ticker, None)
            return

        release_date = exit_date + timedelta(days=self._cooling_days)
        self._entries[ticker] = {
            "exit_date":      exit_date,
            "exit_price":     exit_price,
            "pnl_pct":        pnl_pct,
            "release_date":   release_date,
            "early_released": False,
            "release_reason": None,
        }
        logger.info(
            "CoolingOff: %s locked until %s (pnl=%.1f%%)",
            ticker, release_date, pnl_pct * 100
        )

    # ── query ─────────────────────────────────────────────────────────────────

    def is_cooling_off(self, ticker: str, as_of: date | None = None) -> bool:
        """Return True if ticker is in a cooling-off period."""
        entry = self._entries.get(ticker)
        if entry is None:
            return False
        if entry["early_released"]:
            return False
        today = as_of or date.today()
        if today >= entry["release_date"]:
            self._entries.pop(ticker, None)
            return False
        return True

    def days_remaining(self, ticker: str, as_of: date | None = None) -> int:
        """Return calendar days remaining in cooling-off period (0 if not cooling)."""
        entry = self._entries.get(ticker)
        if entry is None or entry["early_released"]:
            return 0
        today = as_of or date.today()
        remaining = (entry["release_date"] - today).days
        return max(0, remaining)

    # ── early release ─────────────────────────────────────────────────────────

    def check_early_release(
        self,
        ticker: str,
        current_price: float | None = None,
        earnings_beat_pct: float | None = None,
        volume_ratio: float | None = None,       # current vol / 20d avg
        altdata_score: float | None = None,
        days_to_cover: float | None = None,
        as_of: date | None = None,
    ) -> bool:
        """
        Evaluate the 5 early-release conditions.
        Returns True if the ticker has been released early.
        Mutates entry['early_released'] in place.
        """
        entry = self._entries.get(ticker)
        if entry is None or entry["early_released"]:
            return entry is not None and entry["early_released"]

        reasons: list[str] = []

        # 1. Earnings beat > 5%
        if earnings_beat_pct is not None and earnings_beat_pct > 0.05:
            reasons.append("earnings_beat")

        # 2. Volume surge > 3× 20d average
        if volume_ratio is not None and volume_ratio > 3.0:
            reasons.append("volume_surge")

        # 3. Strong altdata signal
        if altdata_score is not None and altdata_score > 0.7:
            reasons.append("strong_altdata")

        # 4. Price dropped > 15% from exit price
        if current_price is not None and entry["exit_price"] > 0:
            drop = (entry["exit_price"] - current_price) / entry["exit_price"]
            if drop > 0.15:
                reasons.append("price_drop_15pct")

        # 5. Short squeeze: days-to-cover < 2
        if days_to_cover is not None and days_to_cover < 2.0:
            reasons.append("short_squeeze")

        if reasons:
            entry["early_released"] = True
            entry["release_reason"] = ",".join(reasons)
            logger.info(
                "CoolingOff: %s EARLY RELEASE via [%s]",
                ticker, entry["release_reason"]
            )
            return True

        return False

    # ── cleanup ───────────────────────────────────────────────────────────────

    def expire_old_entries(self, as_of: date | None = None) -> int:
        """Remove entries whose release_date has passed. Returns count removed."""
        today = as_of or date.today()
        expired = [t for t, e in self._entries.items() if today >= e["release_date"]]
        for t in expired:
            self._entries.pop(t)
        return len(expired)

    # ── status ────────────────────────────────────────────────────────────────

    def status(self, as_of: date | None = None) -> list[dict]:
        today = as_of or date.today()
        result = []
        for ticker, entry in self._entries.items():
            if entry["early_released"]:
                continue
            remaining = max(0, (entry["release_date"] - today).days)
            result.append({
                "ticker":        ticker,
                "exit_date":     str(entry["exit_date"]),
                "exit_price":    entry["exit_price"],
                "pnl_pct":       round(entry["pnl_pct"] * 100, 2),
                "release_date":  str(entry["release_date"]),
                "days_remaining": remaining,
            })
        return result

    def active_count(self) -> int:
        return sum(1 for e in self._entries.values() if not e["early_released"])
