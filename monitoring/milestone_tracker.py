"""
MilestoneTracker — tracks and celebrates bot performance milestones.

Milestones:
  - First profitable trade
  - 10th trade
  - 50th trade
  - 100th trade
  - First 1% daily gain
  - First 5% weekly gain
  - Equity milestones: $101k, $105k, $110k, $125k, $150k, $200k

Sends Telegram alert on each new milestone (never repeats).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

_MILESTONE_FILE = "output/milestones.json"

_EQUITY_MILESTONES = [101_000, 105_000, 110_000, 115_000, 125_000,
                      150_000, 175_000, 200_000, 250_000, 300_000]

_TRADE_COUNT_MILESTONES = [1, 10, 25, 50, 100, 200, 500, 1000]

_DAILY_RETURN_MILESTONES = [0.01, 0.02, 0.05, 0.10]   # 1%, 2%, 5%, 10%

_WEEKLY_RETURN_MILESTONES = [0.05, 0.10, 0.20]


class MilestoneTracker:
    """
    Persistent milestone tracker with Telegram notifications.
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._milestones: dict[str, Any] = self._load()

    # ── update methods ────────────────────────────────────────────────────────

    def check_trade_count(self, total_trades: int) -> None:
        for m in _TRADE_COUNT_MILESTONES:
            key = f"trades_{m}"
            if total_trades >= m and not self._milestones.get(key):
                self._hit(key, f"Trade #{m} reached! Total trades: {total_trades}")

    def check_equity(self, equity: float) -> None:
        for m in _EQUITY_MILESTONES:
            key = f"equity_{m}"
            if equity >= m and not self._milestones.get(key):
                self._hit(key, f"Equity milestone: ${equity:,.0f} (crossed ${m:,})")

    def check_daily_return(self, daily_return_pct: float) -> None:
        for m in _DAILY_RETURN_MILESTONES:
            key = f"daily_{int(m * 100)}pct"
            if daily_return_pct >= m and not self._milestones.get(key):
                self._hit(key, f"Best daily return milestone: {daily_return_pct:.1%}")

    def check_weekly_return(self, weekly_return_pct: float) -> None:
        for m in _WEEKLY_RETURN_MILESTONES:
            key = f"weekly_{int(m * 100)}pct"
            if weekly_return_pct >= m and not self._milestones.get(key):
                self._hit(key, f"Best weekly return milestone: {weekly_return_pct:.1%}")

    def check_first_profit(self, pnl_pct: float) -> None:
        if pnl_pct > 0 and not self._milestones.get("first_profit"):
            self._hit("first_profit", f"First profitable trade! Return: {pnl_pct:.2%}")

    def check_win_streak(self, streak: int) -> None:
        for m in [3, 5, 10]:
            key = f"win_streak_{m}"
            if streak >= m and not self._milestones.get(key):
                self._hit(key, f"Win streak of {streak}! Longest: {streak}")

    # ── internal ──────────────────────────────────────────────────────────────

    def _hit(self, key: str, message: str) -> None:
        self._milestones[key] = {
            "hit_at": datetime.now().isoformat(),
            "message": message,
        }
        self._save()
        logger.info("MILESTONE: %s", message)
        self._telegram(f"MILESTONE REACHED\n{message}")

    def _telegram(self, text: str) -> None:
        try:
            tg = self._config.get("telegram", {})
            token   = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
            if not token or not chat_id:
                return
            import requests
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": f"[MilestoneTracker]\n{text}"},
                timeout=10,
            )
        except Exception:
            pass

    def _load(self) -> dict:
        try:
            if os.path.exists(_MILESTONE_FILE):
                with open(_MILESTONE_FILE) as fh:
                    return json.load(fh)
        except Exception:
            pass
        return {}

    def _save(self) -> None:
        try:
            os.makedirs(os.path.dirname(_MILESTONE_FILE), exist_ok=True)
            with open(_MILESTONE_FILE, "w") as fh:
                json.dump(self._milestones, fh, indent=2)
        except Exception as exc:
            logger.debug("MilestoneTracker save error: %s", exc)

    def status(self) -> dict:
        return {
            "total_milestones_hit": len(self._milestones),
            "milestones": self._milestones,
        }
