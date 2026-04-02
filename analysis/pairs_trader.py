"""
Pairs Trading Engine
=====================
Cointegration-based pairs trading: find cointegrated pairs, compute spreads,
generate entry/exit signals, and persist discoveries to historical_db.db.

Classes:
  - PairCandidate        — dataclass for a discovered pair
  - CointegrationScanner — test all pairs in a universe for cointegration
  - PairsSignalEngine    — spread z-score signals for active pairs
  - PairsTrader          — main orchestrator (config-aware, DB-backed)

Dependencies (graceful on failure):
  - statsmodels          (Engle-Granger cointegration test, OLS)
  - scipy                (fallback statistics)
  - sqlite3              (persistence in output/historical_db.db)
  - yfinance             (only used in __main__ test block)
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependency: statsmodels
# ---------------------------------------------------------------------------
try:
    from statsmodels.tsa.stattools import coint as _sm_coint
    from statsmodels.regression.linear_model import OLS as _OLS
    from statsmodels.tools import add_constant as _add_constant
    STATSMODELS_AVAILABLE = True
except ImportError:
    _sm_coint = None
    _OLS = None
    _add_constant = None
    STATSMODELS_AVAILABLE = False
    logger.warning("statsmodels not installed — cointegration tests unavailable. "
                   "Install with: pip install statsmodels")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COINT_PVALUE_THRESHOLD   = 0.05
MIN_CORRELATION          = 0.60
MIN_HALFLIFE_DAYS        = 5
MAX_HALFLIFE_DAYS        = 30
ZSCORE_ENTRY             = 2.0
ZSCORE_EXIT              = 0.0
ZSCORE_STOP              = 3.5
ZSCORE_LOOKBACK          = 30       # days for rolling z-score

DB_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS pairs_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_a        TEXT NOT NULL,
    ticker_b        TEXT NOT NULL,
    discovered_at   TEXT NOT NULL,
    p_value         REAL,
    hedge_ratio     REAL,
    half_life       REAL,
    correlation     REAL,
    zscore          REAL,
    direction_a     INTEGER,
    direction_b     INTEGER,
    signal_strength REAL,
    entry_reason    TEXT,
    is_active       INTEGER DEFAULT 1,
    updated_at      TEXT
);
"""

DB_TABLE_COINTEGRATION_LOG = """
CREATE TABLE IF NOT EXISTS cointegration_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_a        TEXT NOT NULL,
    ticker_b        TEXT NOT NULL,
    tested_at       TEXT NOT NULL,
    p_value         REAL,
    hedge_ratio     REAL,
    half_life       REAL,
    correlation     REAL,
    passed_filter   INTEGER DEFAULT 0
);
"""


# ===========================================================================
# PairCandidate dataclass
# ===========================================================================

@dataclass
class PairCandidate:
    ticker_a:    str
    ticker_b:    str
    p_value:     float
    hedge_ratio: float
    half_life:   float
    correlation: float
    series_a:    Optional[pd.Series] = field(default=None, repr=False)
    series_b:    Optional[pd.Series] = field(default=None, repr=False)

    def __str__(self) -> str:
        return (f"PairCandidate({self.ticker_a}/{self.ticker_b} "
                f"p={self.p_value:.3f} hr={self.hedge_ratio:.3f} "
                f"hl={self.half_life:.1f}d corr={self.correlation:.3f})")


# ===========================================================================
# CointegrationScanner
# ===========================================================================

class CointegrationScanner:
    """
    Tests all pairs in a ticker universe for cointegration using the
    Engle-Granger two-step procedure.

    Filtering criteria:
      - Engle-Granger p-value < 0.05
      - Spread half-life between 5 and 30 days
      - Pearson correlation > 0.60
    """

    # ------------------------------------------------------------------
    def estimate_hedge_ratio(self, series_a: pd.Series,
                              series_b: pd.Series) -> float:
        """
        OLS regression of series_a on series_b.
        Returns the regression coefficient (hedge ratio).
        """
        try:
            a = np.array(series_a.dropna(), dtype=float)
            b = np.array(series_b.dropna(), dtype=float)
            n = min(len(a), len(b))
            a, b = a[-n:], b[-n:]

            if STATSMODELS_AVAILABLE and _add_constant is not None:
                X   = _add_constant(b)
                res = _OLS(a, X).fit()
                return float(res.params[1])
            else:
                # Fallback: numpy lstsq
                X = np.column_stack([np.ones(n), b])
                coef, *_ = np.linalg.lstsq(X, a, rcond=None)
                return float(coef[1])
        except Exception as exc:
            logger.warning("estimate_hedge_ratio error: %s", exc)
            return 1.0

    # ------------------------------------------------------------------
    def half_life(self, spread_series: pd.Series) -> Optional[float]:
        """
        OU half-life of the spread series.
        Returns days (float) or None if not mean-reverting.
        """
        try:
            arr = np.array(spread_series.dropna(), dtype=float)
            if len(arr) < 20:
                return None

            lag   = arr[:-1]
            delta = np.diff(arr)
            X     = np.column_stack([np.ones(len(lag)), lag])
            coef, *_ = np.linalg.lstsq(X, delta, rcond=None)
            lam = coef[1]

            if lam >= 0:
                return None
            hl = float(-np.log(2) / lam)
            return hl if 0 < hl < 365 else None

        except Exception as exc:
            logger.warning("half_life error: %s", exc)
            return None

    # ------------------------------------------------------------------
    def _align_series(self, price_a: pd.Series,
                      price_b: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """Inner-join on index and drop NAs."""
        df = pd.DataFrame({"a": price_a, "b": price_b}).dropna()
        return df["a"], df["b"]

    # ------------------------------------------------------------------
    def find_pairs(self, tickers: List[str],
                   price_data_dict: Dict[str, pd.DataFrame]) -> List[PairCandidate]:
        """
        Test all O(n²) pairs for cointegration.

        *price_data_dict* maps ticker → OHLCV DataFrame with a 'Close' column.
        Returns filtered list of PairCandidate objects, sorted by p_value.
        """
        if not STATSMODELS_AVAILABLE:
            logger.error("statsmodels required for cointegration tests.")
            return []

        candidates: List[PairCandidate] = []

        # Build close price series dict
        close: Dict[str, pd.Series] = {}
        for t in tickers:
            df = price_data_dict.get(t)
            if df is not None and "Close" in df.columns:
                close[t] = df["Close"].dropna()

        valid_tickers = [t for t in tickers if t in close and len(close[t]) >= 60]
        logger.info("CointegrationScanner: testing %d pairs from %d tickers",
                    len(list(combinations(valid_tickers, 2))), len(valid_tickers))

        for ticker_a, ticker_b in combinations(valid_tickers, 2):
            try:
                sa, sb = self._align_series(close[ticker_a], close[ticker_b])
                if len(sa) < 60:
                    continue

                # Pearson correlation first (fast filter)
                corr = float(np.corrcoef(sa.values, sb.values)[0, 1])
                if abs(corr) < MIN_CORRELATION:
                    continue

                # Engle-Granger cointegration test
                _, p_value, _ = _sm_coint(sa.values, sb.values)
                p_value = float(p_value)

                # Hedge ratio and spread
                hr     = self.estimate_hedge_ratio(sa, sb)
                spread = sa - hr * sb
                hl     = self.half_life(spread)

                passed = (
                    p_value < COINT_PVALUE_THRESHOLD
                    and hl is not None
                    and MIN_HALFLIFE_DAYS <= hl <= MAX_HALFLIFE_DAYS
                )

                logger.debug("  %s/%s  p=%.3f hl=%s corr=%.2f %s",
                             ticker_a, ticker_b, p_value,
                             f"{hl:.1f}d" if hl else "N/A",
                             corr, "PASS" if passed else "fail")

                if passed:
                    candidates.append(PairCandidate(
                        ticker_a=ticker_a,
                        ticker_b=ticker_b,
                        p_value=p_value,
                        hedge_ratio=hr,
                        half_life=hl,
                        correlation=corr,
                        series_a=sa,
                        series_b=sb,
                    ))

            except Exception as exc:
                logger.warning("CointegrationScanner error (%s/%s): %s",
                               ticker_a, ticker_b, exc)

        candidates.sort(key=lambda c: c.p_value)
        logger.info("CointegrationScanner: found %d candidate pairs", len(candidates))
        return candidates


# ===========================================================================
# PairsSignalEngine
# ===========================================================================

class PairsSignalEngine:
    """
    Generates entry/exit signals for a list of cointegrated pairs based on
    the spread z-score.

    Signal rules:
      z > +2.0  → short A, long B  (A overperformed)
      z < -2.0  → long A, short B  (A underperformed)
      |z| < 0.0 → exit
      |z| > 3.5 → stop (size down / close position)
    """

    # ------------------------------------------------------------------
    def get_spread(self, series_a: pd.Series, series_b: pd.Series,
                   hedge_ratio: float) -> float:
        """Return the most recent spread value: a - hedge_ratio * b."""
        try:
            sa, sb = series_a.dropna(), series_b.dropna()
            n = min(len(sa), len(sb))
            return float(sa.iloc[-n:].values[-1] - hedge_ratio * sb.iloc[-n:].values[-1])
        except Exception as exc:
            logger.warning("get_spread error: %s", exc)
            return 0.0

    # ------------------------------------------------------------------
    def get_zscore(self, series_a: pd.Series, series_b: pd.Series,
                   hedge_ratio: float, lookback: int = ZSCORE_LOOKBACK) -> float:
        """
        Compute the z-score of the spread over the last *lookback* bars.
        """
        try:
            sa, sb = series_a.dropna().values, series_b.dropna().values
            n = min(len(sa), len(sb), lookback + 1)
            sa, sb = sa[-n:], sb[-n:]

            spread  = sa - hedge_ratio * sb
            window  = spread[-lookback:] if len(spread) >= lookback else spread
            mu      = float(window.mean())
            sigma   = float(window.std())
            if sigma == 0:
                return 0.0
            current = float(spread[-1])
            return float((current - mu) / sigma)
        except Exception as exc:
            logger.warning("get_zscore error: %s", exc)
            return 0.0

    # ------------------------------------------------------------------
    def generate_signal(self, pair: PairCandidate) -> Dict[str, Any]:
        """
        Generate a trading signal for a PairCandidate.

        Returns dict:
            direction_a     — +1 (long) / -1 (short) / 0 (flat)
            direction_b     — +1 / -1 / 0
            zscore          — current spread z-score
            strength        — abs(zscore) / ZSCORE_ENTRY capped at 1.0
            entry_reason    — descriptive string
            is_active       — bool
        """
        null_signal = {
            "direction_a": 0, "direction_b": 0,
            "zscore": None, "strength": 0.0,
            "entry_reason": "no_signal", "is_active": False,
        }

        if pair.series_a is None or pair.series_b is None:
            null_signal["entry_reason"] = "missing_price_data"
            return null_signal

        try:
            zscore = self.get_zscore(pair.series_a, pair.series_b,
                                     pair.hedge_ratio)
            strength = min(abs(zscore) / ZSCORE_ENTRY, 1.0)

            if abs(zscore) > ZSCORE_STOP:
                return {
                    "direction_a": 0, "direction_b": 0,
                    "zscore": round(zscore, 3),
                    "strength": strength,
                    "entry_reason": f"stop_triggered z={zscore:.2f} > {ZSCORE_STOP}",
                    "is_active": False,
                }

            if zscore > ZSCORE_ENTRY:
                # A expensive relative to B: short A, long B
                return {
                    "direction_a": -1, "direction_b": 1,
                    "zscore": round(zscore, 3),
                    "strength": round(strength, 3),
                    "entry_reason": f"spread_high z={zscore:.2f} short_{pair.ticker_a}_long_{pair.ticker_b}",
                    "is_active": True,
                }

            if zscore < -ZSCORE_ENTRY:
                # A cheap relative to B: long A, short B
                return {
                    "direction_a": 1, "direction_b": -1,
                    "zscore": round(zscore, 3),
                    "strength": round(strength, 3),
                    "entry_reason": f"spread_low z={zscore:.2f} long_{pair.ticker_a}_short_{pair.ticker_b}",
                    "is_active": True,
                }

            # Within band — exit / no position
            return {
                "direction_a": 0, "direction_b": 0,
                "zscore": round(zscore, 3),
                "strength": 0.0,
                "entry_reason": f"within_band z={zscore:.2f}",
                "is_active": False,
            }

        except Exception as exc:
            logger.warning("generate_signal error for %s/%s: %s",
                           pair.ticker_a, pair.ticker_b, exc)
            null_signal["entry_reason"] = f"error: {exc}"
            return null_signal

    # ------------------------------------------------------------------
    def scan_pairs(self, pairs_list: List[PairCandidate],
                   price_data_dict: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Refresh price data into each PairCandidate, then generate signals.
        Returns list of signal dicts for pairs with is_active=True.
        """
        active_signals = []
        for pair in pairs_list:
            # Refresh series from price_data_dict if available
            for attr, ticker in [("series_a", pair.ticker_a),
                                  ("series_b", pair.ticker_b)]:
                df = price_data_dict.get(ticker)
                if df is not None and "Close" in df.columns:
                    setattr(pair, attr, df["Close"].dropna())

            signal = self.generate_signal(pair)
            signal["ticker_a"] = pair.ticker_a
            signal["ticker_b"] = pair.ticker_b
            signal["p_value"]  = pair.p_value
            signal["hedge_ratio"] = pair.hedge_ratio
            signal["half_life"]   = pair.half_life

            if signal["is_active"]:
                active_signals.append(signal)

        return active_signals


# ===========================================================================
# PairsTrader  (main orchestrator)
# ===========================================================================

class PairsTrader:
    """
    Full pairs trading pipeline:
      1. CointegrationScanner → find pairs
      2. PairsSignalEngine   → generate signals
      3. SQLite persistence  → store in historical_db.db
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        self._config   = self._load_config(config_path)
        self._scanner  = CointegrationScanner()
        self._engine   = PairsSignalEngine()
        self._db_path  = self._resolve_db_path()
        self._pairs:   List[PairCandidate] = []
        self._signals: List[Dict[str, Any]] = []
        self._init_db()

    # ------------------------------------------------------------------
    @staticmethod
    def _load_config(path: str) -> Dict:
        try:
            import yaml
            with open(path) as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    # ------------------------------------------------------------------
    def _resolve_db_path(self) -> Path:
        # Try config override, else default
        db = self._config.get("pairs", {}).get("db_path",
               "output/historical_db.db")
        p = Path(db)
        if not p.is_absolute():
            # Resolve relative to repo root (parent of analysis/)
            p = Path(__file__).parent.parent / p
        return p

    # ------------------------------------------------------------------
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    # ------------------------------------------------------------------
    def _init_db(self):
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = self._get_conn()
            conn.executescript(DB_TABLE_CREATE + DB_TABLE_COINTEGRATION_LOG)
            conn.commit()
            conn.close()
            logger.info("PairsTrader: DB initialised at %s", self._db_path)
        except Exception as exc:
            logger.error("PairsTrader._init_db error: %s", exc)

    # ------------------------------------------------------------------
    def _log_cointegration(self, candidates: List[PairCandidate],
                            all_tested: List[Tuple[str, str, float, float,
                                                   Optional[float], float]]):
        """Permanently log all cointegration test results."""
        try:
            now = datetime.utcnow().isoformat()
            conn = self._get_conn()
            for pair in candidates:
                conn.execute("""
                    INSERT INTO cointegration_log
                        (ticker_a, ticker_b, tested_at, p_value, hedge_ratio,
                         half_life, correlation, passed_filter)
                    VALUES (?,?,?,?,?,?,?,1)
                """, (pair.ticker_a, pair.ticker_b, now, pair.p_value,
                      pair.hedge_ratio, pair.half_life, pair.correlation))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning("_log_cointegration error: %s", exc)

    # ------------------------------------------------------------------
    def _persist_signals(self, signals: List[Dict[str, Any]]):
        """Upsert active signals into the pairs_signals table."""
        try:
            now  = datetime.utcnow().isoformat()
            conn = self._get_conn()
            # Mark old signals inactive
            conn.execute("UPDATE pairs_signals SET is_active=0, updated_at=?", (now,))

            for sig in signals:
                conn.execute("""
                    INSERT INTO pairs_signals
                        (ticker_a, ticker_b, discovered_at, p_value, hedge_ratio,
                         half_life, correlation, zscore, direction_a, direction_b,
                         signal_strength, entry_reason, is_active, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?)
                """, (
                    sig["ticker_a"], sig["ticker_b"], now,
                    sig.get("p_value"), sig.get("hedge_ratio"),
                    sig.get("half_life"), sig.get("correlation"),
                    sig.get("zscore"), sig.get("direction_a"),
                    sig.get("direction_b"), sig.get("strength"),
                    sig.get("entry_reason"), now,
                ))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning("_persist_signals error: %s", exc)

    # ------------------------------------------------------------------
    def scan(self, tickers: List[str],
             price_data: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """Convenience alias for find_and_scan() with auto-fetch when price_data omitted."""
        return self.find_and_scan(tickers, price_data)

    def find_and_scan(self, tickers: List[str],
                      price_data: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """
        Full pipeline:
          1. Find cointegrated pairs
          2. Generate signals
          3. Persist to DB

        Returns dict: {pairs: [...], signals: [...], n_pairs, n_signals}
        """
        if price_data is None:
            price_data = self._auto_fetch(tickers)

        self._pairs   = self._scanner.find_pairs(tickers, price_data)
        self._signals = self._engine.scan_pairs(self._pairs, price_data)

        self._log_cointegration(self._pairs, [])
        self._persist_signals(self._signals)

        return {
            "pairs":    [str(p) for p in self._pairs],
            "signals":  self._signals,
            "n_pairs":  len(self._pairs),
            "n_signals": len(self._signals),
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _auto_fetch(tickers: List[str]) -> Dict[str, pd.DataFrame]:
        """Download OHLCV for each ticker from yfinance."""
        try:
            import yfinance as yf
            raw = yf.download(tickers, period="2y", auto_adjust=True,
                              group_by="ticker", progress=False)
            data: Dict[str, pd.DataFrame] = {}
            for t in tickers:
                try:
                    df = raw[t].copy() if hasattr(raw.columns, "levels") else raw.copy()
                    if not df.empty:
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = [c[0] for c in df.columns]
                        data[t] = df
                except Exception:
                    pass
            return data
        except Exception as exc:
            logger.warning("PairsTrader auto-fetch failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    def get_active_signals(self) -> List[Dict[str, Any]]:
        """Return cached active pair signals from the last run."""
        return [s for s in self._signals if s.get("is_active")]


# ===========================================================================
# Standalone test
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    TEST_TICKERS = ["JPM", "GS", "MS", "BAC", "C", "WFC", "USB", "PNC"]

    print("=" * 60)
    print("Pairs Trader — standalone test")
    print(f"Tickers: {TEST_TICKERS}")
    print("=" * 60)

    try:
        import yfinance as yf
        raw = yf.download(TEST_TICKERS, period="2y", auto_adjust=True,
                          group_by="ticker", progress=False)
        price_data: Dict[str, pd.DataFrame] = {}
        for t in TEST_TICKERS:
            try:
                if hasattr(raw.columns, "levels"):
                    df = raw[t].copy()
                else:
                    df = raw.copy()
                if not df.empty:
                    price_data[t] = df
            except Exception:
                pass
    except Exception as e:
        print(f"yfinance download failed: {e}")
        price_data = {}

    if not price_data:
        print("No price data available — cannot run test.")
    else:
        trader  = PairsTrader(config_path="config/settings.yaml")
        results = trader.find_and_scan(TEST_TICKERS, price_data)

        print(f"\nCointegrated pairs found: {results['n_pairs']}")
        for s in results["pairs"]:
            print(f"  {s}")

        print(f"\nActive signals: {results['n_signals']}")
        for sig in results["signals"]:
            print(f"  {sig['ticker_a']}/{sig['ticker_b']}  "
                  f"z={sig['zscore']}  "
                  f"dir_a={sig['direction_a']}  dir_b={sig['direction_b']}  "
                  f"reason: {sig['entry_reason']}")
