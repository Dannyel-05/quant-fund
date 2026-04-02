"""
Tracks signal performance conditional on macro regime, VIX bucket, market, sector,
UMCI, lunar phase, and correlation regime.
"""
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from closeloop.storage.closeloop_store import ClosedLoopStore
except ImportError:
    ClosedLoopStore = None  # type: ignore
    logger.warning("ClosedLoopStore unavailable in regime_tracker")

try:
    import yfinance as yf
    _HAS_YF = True
except ImportError:
    yf = None  # type: ignore
    _HAS_YF = False
    logger.warning("yfinance not available — current_regime() will return UNKNOWN")

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False


MACRO_REGIMES = [
    "RISK_ON",
    "RISK_OFF",
    "GOLDILOCKS",
    "STAGFLATION",
    "RECESSION_RISK",
    "UNKNOWN",
]

VIX_BUCKETS: Dict[str, tuple] = {
    "LOW":     (0, 15),
    "MEDIUM":  (15, 25),
    "HIGH":    (25, 35),
    "EXTREME": (35, 999),
}


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def classify_vix_bucket(vix: float) -> str:
    """Classify a VIX reading into LOW / MEDIUM / HIGH / EXTREME."""
    for bucket, (lo, hi) in VIX_BUCKETS.items():
        if lo <= vix < hi:
            return bucket
    return "EXTREME"


def classify_macro_regime(fred_data: dict) -> str:
    """
    Classify macro regime from economic indicators dict.

    Keys consulted:
      gdp_growth_pct   — annualised real GDP growth (%)
      cpi_yoy          — CPI year-over-year (%)
      unemployment_pct — unemployment rate (%)
      vix              — VIX level
      yield_spread     — 10y minus 3m treasury spread (bps or %)
      unemployment_change — recent change in unemployment rate
      index_drawdown_pct  — major index drawdown from peak (%)

    GOLDILOCKS:     GDP growth > 2%, CPI < 3%, unemployment < 5%
    STAGFLATION:    CPI > 5% AND GDP growth < 1%
    RECESSION_RISK: unemployment rising AND yield curve inverted
    RISK_OFF:       VIX > 25 OR major index down > 10% from peak
    RISK_ON:        growth > 0, VIX < 20
    UNKNOWN:        insufficient data
    """
    if not fred_data:
        return "UNKNOWN"

    gdp = fred_data.get("gdp_growth_pct")
    cpi = fred_data.get("cpi_yoy")
    unemp = fred_data.get("unemployment_pct")
    vix = fred_data.get("vix")
    spread = fred_data.get("yield_spread")
    unemp_chg = fred_data.get("unemployment_change", 0.0) or 0.0
    idx_dd = fred_data.get("index_drawdown_pct", 0.0) or 0.0

    # Priority order matters
    if gdp is not None and cpi is not None and unemp is not None:
        if gdp > 2.0 and cpi < 3.0 and unemp < 5.0:
            return "GOLDILOCKS"

    if cpi is not None and gdp is not None:
        if cpi > 5.0 and gdp < 1.0:
            return "STAGFLATION"

    if unemp_chg > 0.3 and spread is not None and spread < 0:
        return "RECESSION_RISK"

    if (vix is not None and vix > 25) or idx_dd > 10.0:
        return "RISK_OFF"

    if gdp is not None and vix is not None:
        if gdp > 0 and vix < 20:
            return "RISK_ON"

    return "UNKNOWN"


def _sharpe(pnl_series: List[float]) -> float:
    if len(pnl_series) < 2:
        return 0.0
    if _HAS_NUMPY:
        arr = np.array(pnl_series, dtype=float)
        std = float(np.std(arr, ddof=1))
        mean = float(np.mean(arr))
    else:
        n = len(pnl_series)
        mean = sum(pnl_series) / n
        variance = sum((v - mean) ** 2 for v in pnl_series) / (n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
    return (mean / std) * math.sqrt(252) if std > 0 else 0.0


class RegimeTracker:
    """Maintains per-signal, per-regime performance statistics."""

    def __init__(self, store=None, config=None):
        self._store = store
        self._config = config or {}
        # In-memory: (signal_name, macro_regime, vix_bucket) -> {n, wins, pnl_series}
        self._cache: Dict[tuple, Dict] = {}

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(self, macro_regime: str, attribution: List[Dict]) -> None:
        """Update regime-conditional performance for each attributed signal."""
        if not attribution:
            return

        for attr in attribution:
            signal_name: str = attr.get("signal_name", "unknown")
            attributed_pnl: float = attr.get("attributed_pnl", 0.0)
            was_correct: bool = bool(attr.get("was_correct", False))

            # Determine vix bucket from attribution context (best effort)
            vix_level = attr.get("vix_level", 0.0) or 0.0
            vix_bucket = classify_vix_bucket(float(vix_level))

            key = (signal_name, macro_regime, vix_bucket)
            if key not in self._cache:
                self._cache[key] = {"n": 0, "wins": 0, "pnl_series": []}

            entry = self._cache[key]
            entry["n"] += 1
            entry["pnl_series"].append(attributed_pnl)
            if was_correct:
                entry["wins"] += 1

            # Persist to store
            if self._store is not None:
                try:
                    n = entry["n"]
                    wins = entry["wins"]
                    pnl_series = entry["pnl_series"]
                    win_rate = wins / n if n > 0 else 0.0
                    mean_pnl = sum(pnl_series) / n if n > 0 else 0.0
                    sharpe = _sharpe(pnl_series)
                    best = max(pnl_series)
                    worst = min(pnl_series)
                    self._store.upsert_signal_regime_perf(
                        signal_name, macro_regime, vix_bucket,
                        n, win_rate, mean_pnl, sharpe, best, worst,
                    )
                except Exception as exc:
                    logger.warning(
                        "RegimeTracker.update persist failed for %s/%s/%s: %s",
                        signal_name, macro_regime, vix_bucket, exc,
                    )

    # ------------------------------------------------------------------
    # Multiplier
    # ------------------------------------------------------------------

    def get_regime_multiplier(
        self, signal_name: str, macro_regime: str, vix: float
    ) -> float:
        """
        multiplier = sharpe(signal, regime) / mean_sharpe(signal)
        Returns 1.0 if < 10 trades in this regime for this signal.
        Clamped to [0.3, 2.0].
        """
        vix_bucket = classify_vix_bucket(vix)

        # Try store first
        if self._store is not None:
            try:
                m = self._store.get_regime_weight_multiplier(
                    signal_name, macro_regime, vix_bucket
                )
                return max(0.3, min(2.0, m))
            except Exception as exc:
                logger.warning("get_regime_multiplier store call failed: %s", exc)

        # Fallback: in-memory cache
        key = (signal_name, macro_regime, vix_bucket)
        entry = self._cache.get(key)
        if entry is None or entry["n"] < 10:
            return 1.0

        this_sharpe = _sharpe(entry["pnl_series"])

        # Mean sharpe across all regimes for this signal
        all_sharpes = []
        for (sn, _, _), data in self._cache.items():
            if sn == signal_name and data["n"] >= 5:
                all_sharpes.append(_sharpe(data["pnl_series"]))

        mean_sharpe = sum(all_sharpes) / len(all_sharpes) if all_sharpes else 0.0
        if mean_sharpe == 0:
            return 1.0

        multiplier = this_sharpe / mean_sharpe
        return max(0.3, min(2.0, multiplier))

    # ------------------------------------------------------------------
    # Current regime (live)
    # ------------------------------------------------------------------

    def current_regime(self) -> Dict:
        """
        Attempt to classify current regime using yfinance data.
        Fetches: ^VIX, ^TNX (10y yield), ^IRX (3m yield).
        Returns {macro_regime, vix_bucket, vix_level, yield_curve_spread}.
        """
        result: Dict = {
            "macro_regime": "UNKNOWN",
            "vix_bucket": "UNKNOWN",
            "vix_level": None,
            "yield_curve_spread": None,
        }

        if not _HAS_YF:
            return result

        try:
            tickers = yf.download(
                ["^VIX", "^TNX", "^IRX"],
                period="5d",
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            # Handle multi-index or single-level columns
            def _last(symbol: str, col: str = "Close") -> Optional[float]:
                try:
                    if hasattr(tickers.columns, "levels"):
                        series = tickers[col][symbol].dropna()
                    else:
                        series = tickers[col].dropna()
                    return float(series.iloc[-1]) if len(series) > 0 else None
                except Exception:
                    return None

            vix = _last("^VIX")
            tnx = _last("^TNX")   # 10-year yield
            irx = _last("^IRX")   # 3-month yield (annualised, %)

            result["vix_level"] = vix

            if vix is not None:
                result["vix_bucket"] = classify_vix_bucket(vix)

            spread = None
            if tnx is not None and irx is not None:
                spread = tnx - irx
            result["yield_curve_spread"] = spread

            # Build a proxy fred_data dict for classify_macro_regime
            fred_proxy: Dict = {}
            if vix is not None:
                fred_proxy["vix"] = vix
            if spread is not None:
                fred_proxy["yield_spread"] = spread

            regime = classify_macro_regime(fred_proxy)
            result["macro_regime"] = regime

        except Exception as exc:
            logger.warning("current_regime() failed: %s", exc)

        return result

    # ------------------------------------------------------------------
    # Alert on regime change
    # ------------------------------------------------------------------

    def regime_alert_if_changed(self, previous: str, current: str) -> Optional[str]:
        """Return an alert message if the macro regime has changed, None otherwise."""
        if previous == current:
            return None
        return (
            f"REGIME CHANGE DETECTED: {previous} -> {current}. "
            "Review signal weights and stress caps."
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def render_performance_table(self) -> str:
        """Formatted table: signal x regime → win_rate, sharpe, n_trades"""
        if not self._cache:
            return "No regime performance data recorded yet.\n"

        header = (
            f"{'Signal':<30} {'Regime':<16} {'VIX Bucket':<12} "
            f"{'N':>5} {'Win%':>7} {'Sharpe':>8}\n"
        )
        sep = "-" * 82 + "\n"
        lines = [header, sep]

        sorted_keys = sorted(self._cache.keys(), key=lambda k: (k[0], k[1], k[2]))
        for (sig, regime, vix_bucket) in sorted_keys:
            entry = self._cache[(sig, regime, vix_bucket)]
            n = entry["n"]
            win_rate = entry["wins"] / n if n > 0 else 0.0
            sharpe = _sharpe(entry["pnl_series"])
            lines.append(
                f"{sig:<30} {regime:<16} {vix_bucket:<12} "
                f"{n:>5} {win_rate*100:>6.1f}% {sharpe:>8.3f}"
            )

        lines.append("")
        return "\n".join(lines)
