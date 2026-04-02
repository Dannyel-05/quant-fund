"""
Expanding-window walk-forward analysis.

Splits historical data into N equal OOS windows, trains on all
preceding data, and evaluates on the OOS window.
"""
import logging
from typing import Callable, Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class WalkForwardAnalysis:
    def __init__(self, config: dict, engine):
        self.config = config
        self.engine = engine
        bt = config["backtest"]
        self.train_pct = bt["train_pct"]

    def run(
        self,
        signal_generator: Callable,
        price_data: Dict[str, pd.DataFrame],
        market: str = "us",
        n_windows: int = 5,
    ) -> Dict:
        """
        signal_generator(price_data_subset) -> signals DataFrame

        Returns aggregated out-of-sample results across all windows.
        """
        all_dates = self._common_dates(price_data)
        if len(all_dates) < 200:
            logger.warning("Insufficient data for walk-forward (%d dates)", len(all_dates))
            return {}

        n = len(all_dates)
        min_train = int(n * self.train_pct)
        window_size = (n - min_train) // n_windows

        window_results = []
        for i in range(n_windows):
            train_end_idx = min_train + i * window_size
            test_start_idx = train_end_idx
            test_end_idx = min(test_start_idx + window_size, n)

            if test_end_idx - test_start_idx < 20:
                break

            train_dates = all_dates[:train_end_idx]
            test_dates = all_dates[test_start_idx:test_end_idx]

            train_data = {t: df[df.index.isin(train_dates)] for t, df in price_data.items()}

            try:
                signals = signal_generator(train_data)
            except Exception as e:
                logger.error("Signal generation failed for window %d: %s", i, e)
                continue

            if signals.empty:
                continue

            oos_signals = signals[
                signals["entry_date"].between(test_dates[0], test_dates[-1])
            ]
            result = self.engine.run(oos_signals, price_data, market)
            result["window"] = i
            result["train_end"] = train_dates[-1]
            result["test_start"] = test_dates[0]
            result["test_end"] = test_dates[-1]
            window_results.append(result)

        return self._aggregate(window_results)

    def _common_dates(self, price_data: Dict[str, pd.DataFrame]) -> pd.DatetimeIndex:
        non_empty = [df.index for df in price_data.values() if not df.empty]
        if not non_empty:
            return pd.DatetimeIndex([])
        common = sorted(set.intersection(*[set(idx) for idx in non_empty]))
        return pd.DatetimeIndex(common)

    def _aggregate(self, results: List[Dict]) -> Dict:
        if not results:
            return {}

        trade_frames = [r["trades"] for r in results if not r["trades"].empty]
        all_trades = (
            pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
        )

        sharpes = [r["metrics"].get("sharpe", 0) for r in results if r.get("metrics")]
        returns = [r["metrics"].get("total_return", 0) for r in results if r.get("metrics")]

        return {
            "all_trades": all_trades,
            "window_results": results,
            "mean_oos_sharpe": float(np.mean(sharpes)) if sharpes else 0.0,
            "std_oos_sharpe": float(np.std(sharpes)) if sharpes else 0.0,
            "mean_oos_return": float(np.mean(returns)) if returns else 0.0,
            "n_windows": len(results),
        }
