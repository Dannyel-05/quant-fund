"""
TrailingStopManager — tiered trailing stops replacing time-based exits.

Tiers (based on unrealised gain from entry):
  TIER1 : gain < 15%  → trailing stop 15% below peak
  TIER2 : 15–25% gain → trailing stop 20% below peak
  TIER3 : > 25% gain  → trailing stop 25% below peak

Usage
-----
tm = TrailingStopManager()
tm.observe(ticker, current_price, entry_price)
if tm.should_exit(ticker, current_price):
    # close position
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── tier configuration ────────────────────────────────────────────────────────
_TIERS: list[tuple[float, float]] = [
    # (gain_threshold, trailing_stop_pct)
    (0.25, 0.25),   # TIER3: gain > 25% → 25% trailing stop
    (0.15, 0.20),   # TIER2: gain 15–25% → 20% trailing stop
    (0.00, 0.15),   # TIER1: default    → 15% trailing stop
]


def _trailing_pct(gain_pct: float) -> float:
    for threshold, pct in _TIERS:
        if gain_pct >= threshold:
            return pct
    return 0.15


@dataclass
class _Position:
    ticker: str
    entry_price: float
    peak_price: float
    current_stop: float
    tier: int = 1
    first_observed: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())


class TrailingStopManager:
    """
    Manages tiered trailing stops for all open positions.
    """

    def __init__(self) -> None:
        self._positions: dict[str, _Position] = {}

    # ── position management ───────────────────────────────────────────────────

    def add_position(self, ticker: str, entry_price: float, current_price: float | None = None) -> None:
        """Register a new position.  current_price defaults to entry_price."""
        price = current_price if current_price is not None else entry_price
        gain = (price - entry_price) / entry_price if entry_price > 0 else 0.0
        trail = _trailing_pct(gain)
        stop = price * (1.0 - trail)
        tier = self._gain_to_tier(gain)
        self._positions[ticker] = _Position(
            ticker=ticker,
            entry_price=entry_price,
            peak_price=price,
            current_stop=stop,
            tier=tier,
        )
        logger.info(
            "TrailingStop: added %s entry=%.4f stop=%.4f (tier %d, trail=%.0f%%)",
            ticker, entry_price, stop, tier, trail * 100
        )

    def remove_position(self, ticker: str) -> None:
        self._positions.pop(ticker, None)

    def has_position(self, ticker: str) -> bool:
        return ticker in self._positions

    # ── price update + stop calculation ──────────────────────────────────────

    def observe(self, ticker: str, current_price: float, entry_price: float | None = None) -> None:
        """
        Update trailing stop for ticker given current price.
        Auto-adds position if not tracked and entry_price is provided.
        """
        if ticker not in self._positions:
            if entry_price is None:
                return
            self.add_position(ticker, entry_price, current_price)
            return

        pos = self._positions[ticker]

        # Update peak
        if current_price > pos.peak_price:
            pos.peak_price = current_price

        gain = (pos.peak_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0.0
        trail = _trailing_pct(gain)
        new_stop = pos.peak_price * (1.0 - trail)
        new_tier = self._gain_to_tier(gain)

        # Stop only moves up
        if new_stop > pos.current_stop:
            if new_tier != pos.tier:
                logger.info(
                    "TrailingStop: %s tier %d→%d (gain=%.1f%%), stop %.4f→%.4f",
                    ticker, pos.tier, new_tier, gain * 100, pos.current_stop, new_stop
                )
            pos.current_stop = new_stop
            pos.tier = new_tier

        pos.last_updated = datetime.now().isoformat()

    def should_exit(self, ticker: str, current_price: float) -> bool:
        """
        Return True if current_price is at or below the trailing stop level.
        """
        if ticker not in self._positions:
            return False
        pos = self._positions[ticker]
        if current_price <= pos.current_stop:
            logger.info(
                "TrailingStop: EXIT triggered for %s — price=%.4f stop=%.4f (tier %d)",
                ticker, current_price, pos.current_stop, pos.tier
            )
            return True
        return False

    def stop_price(self, ticker: str) -> Optional[float]:
        pos = self._positions.get(ticker)
        return pos.current_stop if pos else None

    def tier(self, ticker: str) -> Optional[int]:
        pos = self._positions.get(ticker)
        return pos.tier if pos else None

    # ── bulk initialisation (for existing positions at startup) ───────────────

    def initialise_from_positions(self, positions: list[dict]) -> int:
        """
        Bulk-add existing positions.
        Each dict must have keys: ticker, entry_price.
        Optionally: current_price.
        Returns number of positions added.
        """
        added = 0
        for p in positions:
            ticker = p.get("ticker") or p.get("symbol", "")
            entry = p.get("entry_price") or p.get("avg_entry_price") or 0.0
            current = p.get("current_price") or p.get("last_price") or entry
            if not ticker or not entry:
                continue
            self.add_position(ticker, float(entry), float(current))
            added += 1
        logger.info("TrailingStopManager: initialised %d positions from existing portfolio", added)
        return added

    # ── status ────────────────────────────────────────────────────────────────

    def status(self) -> list[dict]:
        out = []
        for ticker, pos in self._positions.items():
            gain = (pos.peak_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0.0
            trail = _trailing_pct(gain)
            out.append({
                "ticker":       ticker,
                "entry_price":  pos.entry_price,
                "peak_price":   pos.peak_price,
                "current_stop": pos.current_stop,
                "tier":         pos.tier,
                "gain_pct":     round(gain * 100, 2),
                "trail_pct":    round(trail * 100, 1),
            })
        return out

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _gain_to_tier(gain: float) -> int:
        if gain >= 0.25:
            return 3
        if gain >= 0.15:
            return 2
        return 1
