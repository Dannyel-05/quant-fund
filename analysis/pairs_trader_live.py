"""
PairsTraderLive — discovers and monitors cointegrated pairs.

Uses Kalman filter for dynamic hedge ratio estimation (KalmanPairsTrader).
Runs ADF test to confirm cointegration.
Generates z-score signals for pairs spread trading.

Pairs universe: scans top liquid US tickers within same sector.
"""
from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from statsmodels.tsa.stattools import adfuller
    ADF_AVAILABLE = True
except ImportError:
    ADF_AVAILABLE = False

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False


_DB_PATH = "closeloop/storage/closeloop.db"

# Minimum sector pairs to scan (same-sector pairs are more likely cointegrated)
_SEED_PAIRS: List[Tuple[str, str]] = [
    # Tech
    ("AAPL", "MSFT"), ("GOOGL", "META"), ("AMD", "NVDA"),
    # Finance
    ("JPM", "BAC"), ("GS", "MS"),
    # Energy
    ("XOM", "CVX"),
    # Retail
    ("WMT", "TGT"),
    # Healthcare
    ("JNJ", "PFE"),
    # Telecoms
    ("T", "VZ"),
]


def _adf_test(spread: np.ndarray) -> float:
    """Return ADF test p-value. Lower = more stationary = better cointegration."""
    if not ADF_AVAILABLE or len(spread) < 20:
        return 1.0
    try:
        result = adfuller(spread, autolag='AIC')
        return float(result[1])  # p-value
    except Exception:
        return 1.0


def _half_life(spread: np.ndarray) -> float:
    """
    Ornstein-Uhlenbeck half-life estimation via OLS.
    HL = -log(2) / log(1 + beta)
    where beta is OLS coefficient of spread_t on spread_{t-1}.
    """
    if len(spread) < 10:
        return float('inf')
    try:
        lag   = spread[:-1]
        delta = spread[1:] - spread[:-1]
        # OLS: delta = alpha + beta * lag
        A = np.column_stack([np.ones(len(lag)), lag])
        result = np.linalg.lstsq(A, delta, rcond=None)
        beta = float(result[0][1])
        if beta >= 0 or beta <= -2:
            return float('inf')
        hl = -np.log(2) / np.log(1 + beta)
        return float(hl)
    except Exception:
        return float('inf')


class PairsTraderLive:
    """
    Discovers cointegrated pairs and generates spread trading signals.
    Stores results in closeloop.db (cointegration_log, pairs_signals).
    """

    def __init__(self, db_path: str = _DB_PATH) -> None:
        self._db   = db_path
        self._kalman_traders: Dict[str, object] = {}  # "A:B" → KalmanPairsTrader
        self._ensure_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _ensure_tables(self) -> None:
        conn = self._conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cointegration_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_a TEXT NOT NULL,
                ticker_b TEXT NOT NULL,
                hedge_ratio REAL,
                intercept REAL,
                half_life_days REAL,
                adf_pvalue REAL,
                correlation REAL,
                discovered_date TEXT,
                last_tested TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pairs_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_a TEXT,
                ticker_b TEXT,
                z_score REAL,
                hedge_ratio REAL,
                half_life_days REAL,
                signal INTEGER,
                signal_date TEXT,
                spread REAL,
                spread_mean REAL,
                spread_std REAL
            )
        """)
        conn.commit()
        conn.close()

    def discover_pairs(self, lookback_days: int = 252) -> List[Dict]:
        """
        Test all seed pairs for cointegration. Store valid ones in cointegration_log.
        Returns list of valid pair dicts.
        """
        if not YF_AVAILABLE:
            logger.warning("PairsTraderLive: yfinance not available")
            return []

        valid = []
        all_tickers = list({t for pair in _SEED_PAIRS for t in pair})

        logger.info("PairsTraderLive: downloading %d tickers", len(all_tickers))
        try:
            from datetime import timedelta
            end   = date.today()
            start = end - timedelta(days=lookback_days + 30)
            raw = yf.download(
                all_tickers, start=str(start), end=str(end),
                auto_adjust=True, progress=False, threads=True
            )
            if isinstance(raw.columns, pd.MultiIndex):
                closes = raw["Close"]
            else:
                closes = raw
        except Exception as exc:
            logger.warning("PairsTraderLive download failed: %s", exc)
            return []

        today_str = datetime.utcnow().isoformat()

        for a, b in _SEED_PAIRS:
            try:
                if a not in closes.columns or b not in closes.columns:
                    continue
                pa = closes[a].dropna()
                pb = closes[b].dropna()
                idx = pa.index.intersection(pb.index)
                if len(idx) < 60:
                    continue
                pa, pb = pa.loc[idx].values, pb.loc[idx].values

                # OLS hedge ratio (initial estimate for ADF)
                A     = np.column_stack([pa, np.ones(len(pa))])
                coefs = np.linalg.lstsq(A, pb, rcond=None)[0]
                beta0, alpha0 = float(coefs[0]), float(coefs[1])
                spread = pb - beta0 * pa - alpha0

                adf_p = _adf_test(spread)
                hl    = _half_life(spread)
                corr  = float(np.corrcoef(pa, pb)[0, 1])

                if adf_p < 0.05 and 1.0 <= hl <= 30.0:
                    pair_dict = {
                        "ticker_a": a, "ticker_b": b,
                        "hedge_ratio": beta0, "intercept": alpha0,
                        "half_life_days": hl, "adf_pvalue": adf_p,
                        "correlation": corr,
                    }
                    valid.append(pair_dict)
                    self._upsert_pair(pair_dict, today_str)
                    logger.info(
                        "PairsTrader: valid pair %s/%s — HL=%.1fd adf_p=%.3f corr=%.2f",
                        a, b, hl, adf_p, corr
                    )
                else:
                    logger.debug(
                        "PairsTrader: skip %s/%s — HL=%.1fd adf_p=%.3f",
                        a, b, hl, adf_p
                    )
            except Exception as exc:
                logger.debug("PairsTrader: error testing %s/%s: %s", a, b, exc)

        logger.info("PairsTraderLive: %d valid pairs discovered", len(valid))
        return valid

    def _upsert_pair(self, pair: Dict, today_str: str) -> None:
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM cointegration_log WHERE ticker_a=? AND ticker_b=?",
            (pair["ticker_a"], pair["ticker_b"])
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE cointegration_log
                   SET hedge_ratio=?, intercept=?, half_life_days=?,
                       adf_pvalue=?, correlation=?, last_tested=?, is_active=1
                   WHERE ticker_a=? AND ticker_b=?""",
                (pair["hedge_ratio"], pair["intercept"], pair["half_life_days"],
                 pair["adf_pvalue"], pair["correlation"], today_str,
                 pair["ticker_a"], pair["ticker_b"])
            )
        else:
            conn.execute(
                """INSERT INTO cointegration_log
                   (ticker_a, ticker_b, hedge_ratio, intercept, half_life_days,
                    adf_pvalue, correlation, discovered_date, last_tested, is_active)
                   VALUES (?,?,?,?,?,?,?,?,?,1)""",
                (pair["ticker_a"], pair["ticker_b"], pair["hedge_ratio"],
                 pair["intercept"], pair["half_life_days"], pair["adf_pvalue"],
                 pair["correlation"], today_str, today_str)
            )
        conn.commit()
        conn.close()

    def generate_signals(self, prices: Optional[Dict[str, float]] = None) -> List[Dict]:
        """
        For each active pair in cointegration_log, compute Kalman z-score signal.
        Stores results in pairs_signals. Returns list of signal dicts.
        """
        conn = self._conn()
        pairs = conn.execute(
            "SELECT ticker_a, ticker_b, hedge_ratio, intercept, half_life_days "
            "FROM cointegration_log WHERE is_active=1"
        ).fetchall()
        conn.close()

        if not pairs:
            return []

        if prices is None:
            # Fetch live prices
            tickers = list({t for row in pairs for t in (row[0], row[1])})
            try:
                raw = yf.download(tickers, period="2d", auto_adjust=True,
                                  progress=False, threads=False)
                if isinstance(raw.columns, pd.MultiIndex):
                    closes = raw["Close"].iloc[-1]
                else:
                    closes = raw.iloc[-1]
                prices = {t: float(closes[t]) for t in tickers if t in closes and not np.isnan(closes[t])}
            except Exception as exc:
                logger.warning("PairsTrader generate_signals: price fetch failed: %s", exc)
                return []

        signals = []
        today_str = datetime.utcnow().isoformat()

        for row in pairs:
            a, b = row[0], row[1]
            hl   = float(row[4])
            key  = f"{a}:{b}"

            if a not in prices or b not in prices:
                continue

            pa, pb = prices[a], prices[b]

            # Get or create KalmanPairsTrader
            if key not in self._kalman_traders:
                try:
                    from analysis.mathematical_signals import KalmanPairsTrader
                    self._kalman_traders[key] = KalmanPairsTrader(delta=1e-4)
                except Exception:
                    continue

            kpt = self._kalman_traders[key]
            result = kpt.update(pa, pb)

            sig = {
                "ticker_a": a, "ticker_b": b,
                "z_score": result["z_score"],
                "hedge_ratio": result["hedge_ratio"],
                "half_life_days": hl,
                "signal": result["signal"],
                "signal_date": today_str,
                "spread": result["spread"],
                "spread_mean": result["spread_mean"],
                "spread_std": result["spread_std"],
            }
            signals.append(sig)

            # Persist to DB
            try:
                c = self._conn()
                c.execute(
                    """INSERT INTO pairs_signals
                       (ticker_a, ticker_b, z_score, hedge_ratio, half_life_days,
                        signal, signal_date, spread, spread_mean, spread_std)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (a, b, result["z_score"], result["hedge_ratio"], hl,
                     result["signal"], today_str, result["spread"],
                     result["spread_mean"], result["spread_std"])
                )
                c.commit()
                c.close()
            except Exception as exc:
                logger.debug("PairsTrader signal store error: %s", exc)

        return signals

    def status_summary(self) -> str:
        """Return a one-line status string for Telegram."""
        conn = self._conn()
        n_pairs  = conn.execute("SELECT COUNT(*) FROM cointegration_log WHERE is_active=1").fetchone()[0]
        n_sigs   = conn.execute("SELECT COUNT(*) FROM pairs_signals").fetchone()[0]
        active   = conn.execute(
            "SELECT ticker_a, ticker_b, z_score, signal FROM pairs_signals "
            "ORDER BY id DESC LIMIT 10"
        ).fetchall()
        conn.close()
        actives = [f"{r[0]}/{r[1]} z={r[2]:.2f}" for r in active if r[3] != 0]
        return (
            f"Pairs Trading: {n_pairs} cointegrated pairs | "
            f"{n_sigs} signals stored | "
            f"Active: {actives or 'none'}"
        )
