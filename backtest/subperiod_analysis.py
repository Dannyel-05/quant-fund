"""
Sub-period Backtest Analysis
==============================
Splits backtest results into historical sub-periods and computes
performance metrics for each, enabling detection of regime-conditional edge.

Sub-periods (configurable, defaults to major macro epochs):
  2010-2014: QE recovery, low-vol bull market
  2014-2018: Mid-cycle expansion
  2018-2020: Late cycle + COVID crash
  2020-2022: COVID recovery + inflation spike
  2022-2024: Rate hiking cycle + bear market recovery

Usage:
  from backtest.subperiod_analysis import SubperiodAnalyser
  analyser = SubperiodAnalyser()
  report = analyser.analyse(equity_curve, trades_df)
  print(report)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_PERIODS = [
    ("2010-2014", "2010-01-01", "2014-12-31", "QE Recovery"),
    ("2014-2018", "2015-01-01", "2018-12-31", "Mid-Cycle Expansion"),
    ("2018-2021", "2019-01-01", "2021-06-30", "Late Cycle + COVID"),
    ("2021-2023", "2021-07-01", "2023-06-30", "COVID Recovery + Inflation + Rate Hikes"),
    ("2023-2026", "2023-07-01", "2026-12-31", "Rate Hike Plateau + AI Boom"),
]


@dataclass
class PeriodMetrics:
    label: str
    description: str
    start: str
    end: str
    n_trades: int
    total_return_pct: float
    cagr_pct: float
    sharpe: float
    max_drawdown_pct: float
    win_rate_pct: float
    avg_return_pct: float
    calmar: float


class SubperiodAnalyser:
    """
    Takes backtest equity curve and trades DataFrame and produces
    sub-period performance breakdown.
    """

    def __init__(self, periods: Optional[List[Tuple]] = None):
        self.periods = periods or DEFAULT_PERIODS

    def analyse(
        self,
        equity: pd.Series,
        trades: pd.DataFrame,
        initial_capital: float = 50_000.0,
    ) -> Dict:
        """
        Parameters
        ----------
        equity  : pd.Series, DatetimeIndex → portfolio value
        trades  : pd.DataFrame with at least [entry_date, exit_date, return] columns
        """
        results: List[PeriodMetrics] = []

        for label, start_str, end_str, description in self.periods:
            start = pd.Timestamp(start_str)
            end   = pd.Timestamp(end_str)

            # Slice equity curve
            eq_slice = equity.loc[
                (equity.index >= start) & (equity.index <= end)
            ]

            if len(eq_slice) < 2:
                continue

            # Slice trades
            date_col = None
            for c in ("exit_date", "entry_date", "date"):
                if c in trades.columns:
                    date_col = c
                    break

            if date_col:
                ts = pd.to_datetime(trades[date_col], errors="coerce")
                tr_slice = trades[(ts >= start) & (ts <= end)]
            else:
                tr_slice = pd.DataFrame()

            metrics = self._compute(eq_slice, tr_slice, label, description, start_str, end_str)
            if metrics:
                results.append(metrics)

        return {
            "periods": results,
            "full": self._compute(equity, trades, "Full Period",
                                  "All data", str(equity.index[0].date()),
                                  str(equity.index[-1].date())),
            "best_period": max(results, key=lambda r: r.sharpe).label if results else "N/A",
            "worst_period": min(results, key=lambda r: r.sharpe).label if results else "N/A",
            "consistent": all(r.sharpe > 0 for r in results) if results else False,
        }

    def _compute(
        self,
        equity: pd.Series,
        trades: pd.DataFrame,
        label: str,
        description: str,
        start: str,
        end: str,
    ) -> Optional[PeriodMetrics]:
        if len(equity) < 2:
            return None

        ret_series = equity.pct_change().dropna()
        total_ret  = float(equity.iloc[-1] / equity.iloc[0] - 1) * 100
        years      = max((equity.index[-1] - equity.index[0]).days / 365.25, 1e-6)
        cagr       = float(((1 + total_ret / 100) ** (1 / years) - 1) * 100)
        sharpe     = float((ret_series.mean() / (ret_series.std() or 1e-8)) * np.sqrt(252))

        roll_max = equity.expanding().max()
        max_dd   = float(((equity - roll_max) / roll_max).min() * 100)
        calmar   = abs(cagr / max_dd) if max_dd != 0 else 0.0

        n_trades   = len(trades)
        win_rate   = 0.0
        avg_ret    = 0.0
        if n_trades > 0:
            ret_col = None
            for c in ("return", "return_pct", "pnl_pct"):
                if c in trades.columns:
                    ret_col = c
                    break
            if ret_col:
                rets   = trades[ret_col].dropna()
                win_rate = float((rets > 0).mean() * 100)
                avg_ret  = float(rets.mean())

        return PeriodMetrics(
            label=label,
            description=description,
            start=start,
            end=end,
            n_trades=n_trades,
            total_return_pct=round(total_ret, 2),
            cagr_pct=round(cagr, 2),
            sharpe=round(sharpe, 3),
            max_drawdown_pct=round(max_dd, 2),
            win_rate_pct=round(win_rate, 1),
            avg_return_pct=round(avg_ret, 3),
            calmar=round(calmar, 2),
        )

    def format_report(self, result: Dict) -> str:
        lines = [
            "SUB-PERIOD PERFORMANCE ANALYSIS",
            "=" * 80,
            f"{'Period':<16} {'Trades':>7} {'TotalRet':>9} {'CAGR':>7} {'Sharpe':>7} "
            f"{'MaxDD':>8} {'WinRate':>8} {'Calmar':>7}",
            "-" * 80,
        ]
        for p in result.get("periods", []):
            lines.append(
                f"{p.label:<16} {p.n_trades:>7} {p.total_return_pct:>8.1f}% "
                f"{p.cagr_pct:>6.1f}% {p.sharpe:>7.2f} "
                f"{p.max_drawdown_pct:>7.1f}% {p.win_rate_pct:>7.1f}% "
                f"{p.calmar:>7.2f}"
            )
        lines.append("-" * 80)
        full = result.get("full")
        if full:
            lines.append(
                f"{'FULL PERIOD':<16} {full.n_trades:>7} {full.total_return_pct:>8.1f}% "
                f"{full.cagr_pct:>6.1f}% {full.sharpe:>7.2f} "
                f"{full.max_drawdown_pct:>7.1f}% {full.win_rate_pct:>7.1f}% "
                f"{full.calmar:>7.2f}"
            )
        lines.extend([
            "=" * 80,
            f"  Best period:  {result.get('best_period', 'N/A')}",
            f"  Worst period: {result.get('worst_period', 'N/A')}",
            f"  Consistent (Sharpe>0 all periods): {result.get('consistent', False)}",
        ])
        return "\n".join(lines)
