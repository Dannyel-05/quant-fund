"""
Records all signal combinations per trade. Discovers proven and toxic combinations.
"""
import logging
import math
from itertools import combinations
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in signal_interaction_ledger")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False


def _sharpe(pnl_series: List[float]) -> float:
    if len(pnl_series) < 2:
        return 0.0
    if _HAS_NUMPY:
        arr = np.array(pnl_series, dtype=float)
        std = float(np.std(arr, ddof=1))
        mean = float(np.mean(arr))
    else:
        mean = sum(pnl_series) / len(pnl_series)
        variance = sum((v - mean) ** 2 for v in pnl_series) / (len(pnl_series) - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
    if std == 0:
        return 0.0
    return (mean / std) * math.sqrt(252)


class SignalInteractionLedger:
    """Tracks pairwise and triple signal combinations to surface proven and toxic pairings."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = config or {}
        # In-memory ledger: combo_key -> {n, wins, pnl_series}
        self._ledger: Dict[str, Dict] = {}

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(self, active_signals: List[str], closed_trade: dict) -> None:
        """
        For every combination of 2 or 3 signals in active_signals:
          combo_key = "|".join(sorted(signals))
          Track: n_occurrences, win_rate, mean_pnl, sharpe
          Store in closeloop_store.signal_interactions
          Minimum 5 observations before reporting

        Also tracks entry timing interaction:
          PEAD+ALT_CONFLUENCE scale-in vs immediate entry.
        """
        if not active_signals:
            return

        net_pnl = closed_trade.get("net_pnl", 0.0)
        was_profitable = net_pnl > 0

        # 2- and 3-signal combinations
        signal_list = list(active_signals)
        for size in (2, 3):
            if len(signal_list) < size:
                continue
            for combo in combinations(signal_list, size):
                combo_key = "|".join(sorted(combo))
                self._record_combo(combo_key, net_pnl, was_profitable)

        # Entry timing interaction: PEAD + ALT_CONFLUENCE scale-in
        if "PEAD" in signal_list and "ALT_CONFLUENCE" in signal_list:
            tranche = closed_trade.get("scale_in_tranche", 0) or 0
            if tranche > 0:
                scale_key = "PEAD|ALT_CONFLUENCE|SCALE_IN"
                self._record_combo(scale_key, net_pnl, was_profitable)

        # Persist all updated combos to store
        if self._store is not None:
            for combo_key, data in self._ledger.items():
                try:
                    n = data["n"]
                    wins = data["wins"]
                    pnl_series = data["pnl_series"]
                    win_rate = wins / n if n > 0 else 0.0
                    mean_pnl = sum(pnl_series) / n if n > 0 else 0.0
                    sharpe = _sharpe(pnl_series)
                    # vs_single_signal_improvement: placeholder (no single baseline here)
                    self._store.upsert_signal_interaction(
                        combo_key, n, win_rate, mean_pnl, sharpe, 0.0
                    )
                except Exception as exc:
                    logger.warning("Failed to persist combo %s: %s", combo_key, exc)

    def _record_combo(self, combo_key: str, net_pnl: float, was_profitable: bool) -> None:
        if combo_key not in self._ledger:
            self._ledger[combo_key] = {"n": 0, "wins": 0, "pnl_series": []}
        entry = self._ledger[combo_key]
        entry["n"] += 1
        entry["pnl_series"].append(net_pnl)
        if was_profitable:
            entry["wins"] += 1

    # ------------------------------------------------------------------
    # Multiplier
    # ------------------------------------------------------------------

    def get_multiplier(self, active_signals: List[str]) -> float:
        """
        proven  (win_rate > 0.65, n >= 5, sharpe > 1.0): 1.2x
        toxic   (win_rate < 0.40, n >= 5):                0.5x
        conflicting (opposing directions, both > 0.5):    0.5x if historically worse
        else:                                              1.0x

        Checks combinations of 2 and 3; takes the most conservative multiplier.
        """
        # First try the store's built-in helper
        if self._store is not None:
            try:
                return self._store.get_interaction_multiplier(active_signals)
            except Exception as exc:
                logger.warning("store.get_interaction_multiplier failed: %s", exc)

        # Fallback to in-memory ledger
        best_multiplier = 1.0
        signal_list = list(active_signals)
        for size in (2, 3):
            if len(signal_list) < size:
                continue
            for combo in combinations(signal_list, size):
                combo_key = "|".join(sorted(combo))
                data = self._ledger.get(combo_key)
                if data is None or data["n"] < 5:
                    continue
                n = data["n"]
                win_rate = data["wins"] / n
                sharpe = _sharpe(data["pnl_series"])
                if win_rate > 0.65 and sharpe > 1.0:
                    candidate = 1.2
                elif win_rate < 0.40:
                    candidate = 0.5
                else:
                    candidate = 1.0
                # Take the most conservative (lowest) multiplier
                best_multiplier = min(best_multiplier, candidate)

        return best_multiplier

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def render_top_combinations(self, n: int = 10) -> str:
        """Return formatted text table of best combinations (by Sharpe, min 5 observations)."""
        # Merge in-memory and store data; use in-memory for simplicity
        eligible = []
        for combo_key, data in self._ledger.items():
            if data["n"] < 5:
                continue
            n_trades = data["n"]
            win_rate = data["wins"] / n_trades
            mean_pnl = sum(data["pnl_series"]) / n_trades
            sharpe = _sharpe(data["pnl_series"])
            eligible.append({
                "combo": combo_key,
                "n": n_trades,
                "win_rate": win_rate,
                "mean_pnl": mean_pnl,
                "sharpe": sharpe,
            })

        eligible.sort(key=lambda x: x["sharpe"], reverse=True)
        top = eligible[:n]

        if not top:
            return "No combinations with >= 5 observations yet.\n"

        header = f"{'Combination':<50} {'N':>5} {'Win%':>7} {'MeanPnL':>10} {'Sharpe':>8}\n"
        sep = "-" * 82 + "\n"
        rows = []
        for item in top:
            rows.append(
                f"{item['combo']:<50} {item['n']:>5} "
                f"{item['win_rate']*100:>6.1f}% "
                f"{item['mean_pnl']:>10.2f} "
                f"{item['sharpe']:>8.3f}"
            )
        return header + sep + "\n".join(rows) + "\n"
