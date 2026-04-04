"""
PreTrainer — loads historical backtest trades and primes the closeloop
learning system.  RAM-safe: processes in batches of 100 with gc.collect()
between batches.  Live trades are weighted 3× backtest trades.
"""
from __future__ import annotations

import gc
import logging
import sqlite3
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

_BATCH_SIZE = 100
_LIVE_WEIGHT = 3.0   # live trades count 3× vs backtest
_BACKTEST_WEIGHT = 1.0

_RAM_CHECK_GB = 1.7   # pause if RAM used exceeds this level


def _get_ram_used_gb() -> float:
    """Return GB of RAM currently in use (best effort)."""
    try:
        import psutil
        return psutil.virtual_memory().used / 1e9
    except ImportError:
        pass
    try:
        info: dict[str, int] = {}
        with open("/proc/meminfo") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = int(parts[1])
        total = info.get("MemTotal", 0)
        avail = info.get("MemAvailable", 0)
        return (total - avail) * 1024 / 1e9
    except Exception:
        return 0.0


class PreTrainer:
    """
    Load historical backtest trades from the closeloop DB and inject them
    into the learning system as synthetic observations.

    Parameters
    ----------
    config : dict
        Full bot config.
    weight_updater : object
        The WeightUpdater instance from closeloop/learning/weight_updater.py.
        Must have an ``observe(trade_dict, weight=1.0)`` method (or equivalent).
    """

    def __init__(self, config: dict, weight_updater: Any | None = None) -> None:
        self._config = config
        self._wu = weight_updater
        db_path = config.get("closeloop", {}).get("storage_path", "closeloop/storage/closeloop.db")
        self._db_path = db_path

    def run(self) -> dict:
        """
        Run pre-training.  Returns a summary dict.
        """
        logger.info("PreTrainer: starting pre-training pass")
        summary = {
            "backtest_loaded": 0,
            "live_loaded": 0,
            "batches": 0,
            "errors": 0,
            "skipped_ram": 0,
        }

        # ── 1. load backtest trades ───────────────────────────────────────────
        try:
            bt_trades = self._load_backtest_trades()
            logger.info("PreTrainer: %d backtest trades found", len(bt_trades))
            self._process_in_batches(bt_trades, _BACKTEST_WEIGHT, summary, label="backtest")
            summary["backtest_loaded"] = len(bt_trades)
        except Exception as exc:
            logger.exception("PreTrainer: backtest load error: %s", exc)
            summary["errors"] += 1

        # ── 2. load live closed trades (higher weight) ────────────────────────
        try:
            live_trades = self._load_live_trades()
            logger.info("PreTrainer: %d live trades found", len(live_trades))
            self._process_in_batches(live_trades, _LIVE_WEIGHT, summary, label="live")
            summary["live_loaded"] = len(live_trades)
        except Exception as exc:
            logger.exception("PreTrainer: live trade load error: %s", exc)
            summary["errors"] += 1

        logger.info(
            "PreTrainer: done — backtest=%d live=%d batches=%d errors=%d skipped_ram=%d",
            summary["backtest_loaded"],
            summary["live_loaded"],
            summary["batches"],
            summary["errors"],
            summary["skipped_ram"],
        )
        return summary

    # ── loading ───────────────────────────────────────────────────────────────

    def _load_backtest_trades(self) -> list[dict]:
        trades: list[dict] = []
        try:
            con = sqlite3.connect(self._db_path, timeout=30)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            # Try the standard closed_trades table
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {r[0] for r in cur.fetchall()}

            if "backtest_trades" in tables:
                cur.execute("SELECT * FROM backtest_trades ORDER BY exit_date ASC")
            elif "closed_trades" in tables:
                cur.execute(
                    "SELECT * FROM closed_trades WHERE source='backtest' ORDER BY exit_date ASC"
                )
            else:
                logger.warning("PreTrainer: no backtest_trades or closed_trades table")
                con.close()
                return trades

            for row in cur.fetchall():
                trades.append(dict(row))
            con.close()
        except Exception as exc:
            logger.warning("PreTrainer: DB error loading backtest trades: %s", exc)
        return trades

    def _load_live_trades(self) -> list[dict]:
        trades: list[dict] = []
        try:
            con = sqlite3.connect(self._db_path, timeout=30)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {r[0] for r in cur.fetchall()}

            if "closed_trades" in tables:
                cur.execute(
                    "SELECT * FROM closed_trades WHERE (source IS NULL OR source != 'backtest') "
                    "ORDER BY exit_date ASC"
                )
                for row in cur.fetchall():
                    trades.append(dict(row))
            con.close()
        except Exception as exc:
            logger.warning("PreTrainer: DB error loading live trades: %s", exc)
        return trades

    # ── processing ────────────────────────────────────────────────────────────

    def _process_in_batches(
        self,
        trades: list[dict],
        weight: float,
        summary: dict,
        label: str,
    ) -> None:
        for i in range(0, len(trades), _BATCH_SIZE):
            batch = trades[i : i + _BATCH_SIZE]

            # RAM guard
            ram_gb = _get_ram_used_gb()
            if ram_gb > _RAM_CHECK_GB:
                logger.warning(
                    "PreTrainer: RAM %.2fGB > %.2fGB — pausing batch (skipped %d %s trades)",
                    ram_gb, _RAM_CHECK_GB, len(trades) - i, label
                )
                summary["skipped_ram"] += len(trades) - i
                gc.collect()
                break

            for trade in batch:
                self._observe(trade, weight)

            summary["batches"] += 1
            gc.collect()
            logger.debug("PreTrainer: processed %s batch %d/%d", label, i // _BATCH_SIZE + 1,
                         (len(trades) + _BATCH_SIZE - 1) // _BATCH_SIZE)

    def _observe(self, trade: dict, weight: float) -> None:
        """Feed a single trade observation into the weight updater."""
        if self._wu is None:
            return
        try:
            if hasattr(self._wu, "observe"):
                self._wu.observe(trade, weight=weight)
            elif hasattr(self._wu, "update_weights"):
                self._wu.update_weights(trade, weight=weight)
            else:
                logger.debug("PreTrainer: weight_updater has no observe/update_weights method")
        except Exception as exc:
            logger.debug("PreTrainer: observe error for trade %s: %s", trade.get("id"), exc)
