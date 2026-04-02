"""
Benchmark tracker: 4-benchmark comparison + Information Ratio.

Benchmarks: SPY (US large cap), IWM (US small cap), EWU (UK), ACWI (global).
IR = mean(active_return) / std(active_return) * √252
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_BENCHMARKS = {
    "SPY": "US Large Cap",
    "IWM": "US Small Cap",
    "EWU": "UK Equity",
    "ACWI": "Global",
}


class BenchmarkTracker:
    """
    Tracks fund performance vs 4 benchmarks.
    Computes Information Ratio and active returns.
    """

    def __init__(self, store=None, config=None):
        self.store = store
        self.config = config or {}

    def record(
        self,
        date: str,
        fund_return: float,
        fund_value: float,
        benchmark_returns: Optional[Dict[str, float]] = None,
    ) -> None:
        """Record a daily performance observation."""
        if benchmark_returns is None:
            benchmark_returns = self._fetch_benchmark_returns()

        if self.store:
            try:
                self.store.record_benchmark(
                    date=date,
                    fund_return=fund_return,
                    fund_value=fund_value,
                    benchmark_returns=benchmark_returns,
                )
            except Exception as exc:
                logger.warning("BenchmarkTracker.record store error: %s", exc)

    def compute_ir(self, benchmark: str = "IWM", window_days: int = 252) -> Dict:
        """
        Compute Information Ratio vs specified benchmark.
        IR = mean(active_return) / std(active_return) * √252

        Returns {"ir": float, "active_return_mean": float, "active_return_std": float,
                 "n_obs": int, "benchmark": str}
        """
        if not self.store:
            return {"ir": 0.0, "n_obs": 0, "benchmark": benchmark}

        try:
            rows = self.store.get_benchmark_history(window_days=window_days)
            if not rows:
                return {"ir": 0.0, "n_obs": 0, "benchmark": benchmark}

            fund_rets = [r["fund_return"] for r in rows]
            bench_rets = [r.get(f"benchmark_{benchmark.lower()}", 0.0) for r in rows]

            active = [f - b for f, b in zip(fund_rets, bench_rets)]
            if len(active) < 5:
                return {"ir": 0.0, "n_obs": len(active), "benchmark": benchmark}

            mean_active = float(np.mean(active))
            std_active = float(np.std(active, ddof=1))
            ir = (mean_active / std_active * np.sqrt(252)) if std_active > 1e-9 else 0.0

            return {
                "ir": round(ir, 4),
                "active_return_mean": round(mean_active, 6),
                "active_return_std": round(std_active, 6),
                "n_obs": len(active),
                "benchmark": benchmark,
                "benchmark_name": _BENCHMARKS.get(benchmark, benchmark),
            }
        except Exception as exc:
            logger.warning("BenchmarkTracker.compute_ir: %s", exc)
            return {"ir": 0.0, "n_obs": 0, "benchmark": benchmark}

    def full_comparison(self) -> Dict:
        """
        Compute performance stats vs all 4 benchmarks.
        Returns nested dict keyed by benchmark ticker.
        """
        results = {}
        for bm in _BENCHMARKS:
            results[bm] = self.compute_ir(benchmark=bm)
        return results

    def summary_text(self) -> str:
        """Return a human-readable comparison table."""
        lines = ["BENCHMARK COMPARISON", "=" * 50]
        for bm, name in _BENCHMARKS.items():
            ir_data = self.compute_ir(benchmark=bm)
            ir = ir_data.get("ir", 0.0)
            n = ir_data.get("n_obs", 0)
            rating = "STRONG" if ir > 0.5 else ("POSITIVE" if ir > 0 else "NEGATIVE")
            lines.append(f"  {bm:5s} ({name:15s}): IR={ir:+.3f}  [{rating}]  n={n}")
        return "\n".join(lines)

    def update(self, *args, **kwargs) -> None:
        """Stub — called by autopsy pipeline; no-op until live data available."""
        pass

    def _fetch_benchmark_returns(self) -> Dict[str, float]:
        """Fetch today's returns for all benchmarks via yfinance."""
        result = {}
        try:
            import yfinance as yf
            for bm in _BENCHMARKS:
                try:
                    hist = yf.Ticker(bm).history(period="2d")
                    if len(hist) >= 2:
                        ret = float(hist["Close"].pct_change().iloc[-1])
                        result[bm] = ret
                    else:
                        result[bm] = 0.0
                except Exception:
                    result[bm] = 0.0
        except ImportError:
            pass
        return result
