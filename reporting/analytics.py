"""
Performance analytics: full metric suite, text reports, equity plots.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class Analytics:
    def __init__(self, config: dict):
        self.config = config
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_metrics(
        self,
        equity: pd.Series,
        trades: pd.DataFrame = None,
        benchmark: pd.Series = None,
    ) -> Dict:
        if equity.empty or len(equity) < 2:
            return {}

        ret = equity.pct_change().dropna()

        metrics = {
            "total_return": self._total_return(equity),
            "cagr": self._cagr(equity),
            "sharpe": self._sharpe(ret),
            "sortino": self._sortino(ret),
            "calmar": self._calmar(equity),
            "max_drawdown": self._max_drawdown(equity),
            "max_drawdown_duration_days": self._max_dd_duration(equity),
            "volatility_ann": float(ret.std() * np.sqrt(252)),
            "skew": float(ret.skew()),
            "kurtosis": float(ret.kurtosis()),
            "var_95": float(np.percentile(ret, 5)),
            "cvar_95": float(ret[ret <= np.percentile(ret, 5)].mean()),
        }

        if trades is not None and not trades.empty:
            metrics.update(self._trade_metrics(trades))

        if benchmark is not None and not benchmark.empty:
            metrics.update(self._relative_metrics(ret, benchmark.pct_change().dropna()))

        return metrics

    def generate_report(
        self, results: Dict, output_file: str = None
    ) -> str:
        metrics = results.get("metrics", {})
        trades = results.get("trades", pd.DataFrame())
        market = (results.get("market") or "").upper()

        lines = [
            "# Strategy Performance Report",
            f"Generated : {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        ]
        if market:
            lines.append(f"Market    : {market}")
        lines += [
            "",
            "## Return Metrics",
            f"  Total Return   : {metrics.get('total_return', 0):>10.2%}",
            f"  CAGR           : {metrics.get('cagr', 0):>10.2%}",
            f"  Volatility     : {metrics.get('volatility_ann', 0):>10.2%}",
            "",
            "## Risk-Adjusted",
            f"  Sharpe         : {self._fmt_ratio(metrics.get('sharpe'))}",
            f"  Sortino        : {self._fmt_ratio(metrics.get('sortino'))}",
            f"  Calmar         : {metrics.get('calmar', 0):>10.2f}",
            "",
            "## Drawdown",
            f"  Max Drawdown   : {metrics.get('max_drawdown', 0):>10.2%}",
            f"  Max DD Duration: {metrics.get('max_drawdown_duration_days', 0):>10.0f} days",
            "",
            "## Distribution",
            f"  Skew           : {metrics.get('skew', 0):>10.2f}",
            f"  Kurtosis       : {metrics.get('kurtosis', 0):>10.2f}",
            f"  VaR (95%)      : {metrics.get('var_95', 0):>10.2%}",
            f"  CVaR (95%)     : {metrics.get('cvar_95', 0):>10.2%}",
        ]

        if "n_trades" in metrics:
            lines += [
                "",
                "## Trade Statistics",
                f"  N Trades       : {metrics.get('n_trades', 0):>10.0f}",
                f"  Win Rate       : {metrics.get('win_rate', 0):>10.2%}",
                f"  Avg Return     : {metrics.get('avg_trade_return', 0):>10.2%}",
                f"  Avg Win        : {metrics.get('avg_win', 0):>10.2%}",
                f"  Avg Loss       : {metrics.get('avg_loss', 0):>10.2%}",
                f"  Profit Factor  : {metrics.get('profit_factor', 0):>10.2f}",
                f"  Avg Holding    : {metrics.get('avg_holding_days', 0):>10.1f} days",
            ]

        if "alpha" in metrics:
            lines += [
                "",
                "## vs Benchmark",
                f"  Alpha (ann.)   : {metrics.get('alpha', 0):>10.2%}",
                f"  Beta           : {metrics.get('beta', 0):>10.2f}",
                f"  Info Ratio     : {metrics.get('information_ratio', 0):>10.2f}",
                f"  Tracking Error : {metrics.get('tracking_error', 0):>10.2%}",
            ]

        mc = results.get("monte_carlo", {})
        if mc:
            lines += [
                "",
                "## Monte Carlo (bootstrap)",
                f"  Prob. Profit   : {mc.get('prob_profit', 0):>10.2%}",
                f"  Prob. Ruin     : {mc.get('prob_ruin', 0):>10.2%}",
                f"  Median End Val : {mc.get('final_value', {}).get('percentiles', {}).get('50', 0):>10,.0f}",
                f"  p5  End Val    : {mc.get('final_value', {}).get('percentiles', {}).get('5', 0):>10,.0f}",
                f"  p95 End Val    : {mc.get('final_value', {}).get('percentiles', {}).get('95', 0):>10,.0f}",
            ]

        report = "\n".join(lines)

        if output_file:
            path = self.output_dir / output_file
            path.write_text(report)
            logger.info("Report saved to %s", path)

        return report

    def save_results(self, results: Dict, prefix: str = "backtest") -> None:
        trades = results.get("trades", pd.DataFrame())
        equity = results.get("equity_curve", pd.Series())
        metrics = results.get("metrics", {})

        if not trades.empty:
            trades.to_csv(self.output_dir / f"{prefix}_trades.csv", index=False)

        if not equity.empty:
            equity.to_csv(
                self.output_dir / f"{prefix}_equity.csv", header=["equity"]
            )

        if metrics:
            serialized = {
                k: (float(v) if isinstance(v, (np.floating, np.integer)) else v)
                for k, v in metrics.items()
            }
            (self.output_dir / f"{prefix}_metrics.json").write_text(
                json.dumps(serialized, indent=2)
            )

        mc = results.get("monte_carlo")
        if mc:
            (self.output_dir / f"{prefix}_monte_carlo.json").write_text(
                json.dumps(mc, indent=2, default=float)
            )

    def plot_equity_curve(
        self,
        equity: pd.Series,
        benchmark: pd.Series = None,
        title: str = "Equity Curve",
        filename: str = None,
    ) -> None:
        try:
            import matplotlib.pyplot as plt

            fig, (ax1, ax2) = plt.subplots(
                2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [3, 1]}
            )

            norm = equity / equity.iloc[0] * 100
            ax1.plot(norm.index, norm.values, label="Strategy", lw=1.5, color="steelblue")

            if benchmark is not None and not benchmark.empty:
                bm = benchmark.reindex(equity.index, method="ffill").dropna()
                bm_norm = bm / bm.iloc[0] * 100
                ax1.plot(
                    bm_norm.index, bm_norm.values, label="Benchmark", lw=1, color="gray", alpha=0.7
                )

            ax1.set_title(title, fontsize=13)
            ax1.set_ylabel("Growth of $100")
            ax1.legend()
            ax1.grid(True, alpha=0.3)

            rolling_max = equity.expanding().max()
            dd = (equity - rolling_max) / rolling_max * 100
            ax2.fill_between(dd.index, dd.values, 0, alpha=0.5, color="crimson")
            ax2.set_ylabel("Drawdown (%)")
            ax2.set_xlabel("Date")
            ax2.grid(True, alpha=0.3)

            plt.tight_layout()
            fname = filename or f"{title.lower().replace(' ', '_')}.png"
            path = self.output_dir / fname
            plt.savefig(path, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Chart saved to %s", path)

        except ImportError:
            logger.warning("matplotlib not installed — skipping plot")

    def plot_trade_analysis(self, trades: pd.DataFrame, prefix: str = "backtest") -> None:
        if trades.empty:
            return
        try:
            import matplotlib.pyplot as plt

            fig, axes = plt.subplots(2, 2, figsize=(14, 10))

            # Return distribution
            axes[0, 0].hist(trades["return"] * 100, bins=40, color="steelblue", edgecolor="white")
            axes[0, 0].axvline(0, color="red", lw=1.2, ls="--")
            axes[0, 0].set_title("Trade Return Distribution")
            axes[0, 0].set_xlabel("Return (%)")

            # Cumulative P&L
            cum_pnl = trades.sort_values("exit_date")["net_pnl"].cumsum()
            axes[0, 1].plot(range(len(cum_pnl)), cum_pnl.values, color="steelblue")
            axes[0, 1].set_title("Cumulative Net P&L by Trade")
            axes[0, 1].set_xlabel("Trade #")
            axes[0, 1].set_ylabel("Cumulative P&L")

            # Return vs surprise
            if "surprise_pct" in trades.columns:
                axes[1, 0].scatter(
                    trades["surprise_pct"] * 100,
                    trades["return"] * 100,
                    alpha=0.4,
                    s=20,
                    color="steelblue",
                )
                axes[1, 0].axhline(0, color="red", lw=1, ls="--")
                axes[1, 0].axvline(0, color="red", lw=1, ls="--")
                axes[1, 0].set_title("Return vs Earnings Surprise")
                axes[1, 0].set_xlabel("Surprise (%)")
                axes[1, 0].set_ylabel("Trade Return (%)")

            # Holding period distribution
            if "holding_days" in trades.columns:
                axes[1, 1].hist(
                    trades["holding_days"], bins=20, color="steelblue", edgecolor="white"
                )
                axes[1, 1].set_title("Holding Period Distribution")
                axes[1, 1].set_xlabel("Days")

            plt.suptitle(f"Trade Analysis — {prefix}", fontsize=13)
            plt.tight_layout()
            path = self.output_dir / f"{prefix}_trade_analysis.png"
            plt.savefig(path, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Trade analysis chart saved to %s", path)

        except ImportError:
            logger.warning("matplotlib not installed — skipping plot")

    # ------------------------------------------------------------------
    # Metric helpers
    # ------------------------------------------------------------------

    def _fmt_ratio(self, v) -> str:
        """Format a ratio metric; show 'N/A (< 20 obs)' when value is nan."""
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "       N/A (< 20 obs)"
        return f"{v:>10.2f}"

    def _total_return(self, equity: pd.Series) -> float:
        return float(equity.iloc[-1] / equity.iloc[0] - 1)

    def _cagr(self, equity: pd.Series) -> float:
        years = max((equity.index[-1] - equity.index[0]).days / 365.25, 1e-6)
        return float((1 + self._total_return(equity)) ** (1 / years) - 1)

    def _sharpe(self, returns: pd.Series, rf: float = 0.0, min_obs: int = 20) -> float:
        # Require at least min_obs daily return observations for a meaningful Sharpe.
        # Sparse equity curves (e.g. 2 trades) produce only a handful of non-zero
        # returns, making the annualised ratio statistically meaningless.
        clean = returns.dropna()
        if len(clean) < min_obs:
            return float("nan")
        excess = clean - rf / 252
        std = excess.std()
        return float((excess.mean() / std) * np.sqrt(252)) if std > 0 else 0.0

    def _sortino(self, returns: pd.Series, rf: float = 0.0, min_obs: int = 20) -> float:
        clean = returns.dropna()
        if len(clean) < min_obs:
            return float("nan")
        excess = clean - rf / 252
        downside_std = excess[excess < 0].std()
        return float((excess.mean() / downside_std) * np.sqrt(252)) if downside_std > 0 else 0.0

    def _max_drawdown(self, equity: pd.Series) -> float:
        peak = equity.expanding().max()
        return float(((equity - peak) / peak).min())

    def _calmar(self, equity: pd.Series) -> float:
        mdd = abs(self._max_drawdown(equity))
        return float(self._cagr(equity) / mdd) if mdd > 0 else 0.0

    def _max_dd_duration(self, equity: pd.Series) -> int:
        peak = equity.expanding().max()
        in_dd = equity < peak
        max_dur, cur = 0, 0
        for v in in_dd:
            cur = cur + 1 if v else 0
            max_dur = max(max_dur, cur)
        return max_dur

    def _trade_metrics(self, trades: pd.DataFrame) -> Dict:
        ret = trades["return"].dropna()
        winners = ret[ret > 0]
        losers = ret[ret <= 0]
        profit_factor = (
            float(abs(winners.sum() / losers.sum())) if losers.sum() != 0 else float("inf")
        )
        return {
            "n_trades": len(trades),
            "win_rate": float((ret > 0).mean()),
            "avg_trade_return": float(ret.mean()),
            "avg_win": float(winners.mean()) if len(winners) else 0.0,
            "avg_loss": float(losers.mean()) if len(losers) else 0.0,
            "profit_factor": profit_factor,
            "avg_holding_days": float(
                trades["holding_days"].mean() if "holding_days" in trades.columns else 0
            ),
        }

    def _relative_metrics(
        self, strategy_ret: pd.Series, benchmark_ret: pd.Series
    ) -> Dict:
        s, b = strategy_ret.align(benchmark_ret, join="inner")
        s, b = s.dropna(), b.dropna()
        if len(s) < 20:
            return {}
        excess = s - b
        cov = np.cov(s, b)
        beta = float(cov[0, 1] / cov[1, 1]) if cov[1, 1] > 0 else 1.0
        alpha = float(excess.mean() * 252)
        ir = float((excess.mean() / excess.std()) * np.sqrt(252)) if excess.std() > 0 else 0.0
        return {
            "alpha": alpha,
            "beta": beta,
            "information_ratio": ir,
            "tracking_error": float(excess.std() * np.sqrt(252)),
        }
