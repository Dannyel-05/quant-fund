"""
STEP 8 — Rates/Credit Collector
================================
Fetches interest rate and credit market data from yfinance and FRED.
Calculates yield curve signals, credit stress indicators, and Fed meeting
proximity metrics. Stores everything permanently in SQLite databases.

Databases written to:
  - output/permanent_archive.db  (raw_macro_data table)
  - output/historical_db.db      (rates_data, rates_signals tables)
"""

import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yaml
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

YFINANCE_YIELD_SYMBOLS: Dict[str, str] = {
    "IRX": "^IRX",   # 13-week T-bill
    "FVX": "^FVX",   # 5-year treasury
    "TNX": "^TNX",   # 10-year treasury
    "TYX": "^TYX",   # 30-year treasury
}

FRED_YIELD_SERIES: List[str] = [
    "DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5",
    "DGS7", "DGS10", "DGS20", "DGS30",
    "T10Y2Y", "T10Y3M",
    "IRLTLT01GBM156N",   # UK 10-year gilt
]

FRED_CREDIT_SERIES: List[str] = [
    "BAMLH0A0HYM2",   # US High Yield OAS
    "BAMLC0A0CM",     # US Investment Grade OAS
    "TEDRATE",        # TED Spread
    "T10YIE",         # 10yr TIPS Breakeven Inflation
]

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Fed meeting dates 2024-2027 (all hardcoded)
FED_MEETING_DATES: List[date] = [
    # 2024
    date(2024, 1, 31), date(2024, 3, 20), date(2024, 5, 1), date(2024, 6, 12),
    date(2024, 7, 31), date(2024, 9, 18), date(2024, 11, 7), date(2024, 12, 18),
    # 2025
    date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7), date(2025, 6, 18),
    date(2025, 7, 30), date(2025, 9, 17), date(2025, 11, 5), date(2025, 12, 17),
    # 2026
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 5, 6), date(2026, 6, 17),
    date(2026, 7, 29), date(2026, 9, 16), date(2026, 11, 4), date(2026, 12, 16),
    # 2027
    date(2027, 1, 27), date(2027, 3, 17), date(2027, 5, 5), date(2027, 6, 16),
    date(2027, 7, 28), date(2027, 9, 15), date(2027, 11, 3), date(2027, 12, 15),
]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_conn(db_path: str) -> sqlite3.Connection:
    """Open (creating if necessary) a SQLite connection with WAL mode."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_raw_macro_table(conn: sqlite3.Connection) -> None:
    """Create raw_macro_data table if not present (permanent_archive.db).

    Uses the canonical schema: series_name, series_id, date, value, source, fetched_at
    to match permanent_archive.db created by setup_permanent_archive.py.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_macro_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            series_name TEXT,
            series_id   TEXT,
            date        TEXT,
            value       REAL,
            source      TEXT,
            fetched_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rmd_series ON raw_macro_data(series_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rmd_date ON raw_macro_data(date)")
    conn.commit()


def _ensure_rates_data_table(conn: sqlite3.Connection) -> None:
    """Create rates_data table in historical_db.db."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rates_data (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            source        TEXT NOT NULL,
            series_id     TEXT NOT NULL,
            series_name   TEXT,
            obs_date      TEXT NOT NULL,
            value         REAL,
            fetched_at    TEXT NOT NULL,
            UNIQUE(series_id, obs_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rd_series ON rates_data(series_id, obs_date)")
    conn.commit()


def _ensure_rates_signals_table(conn: sqlite3.Connection) -> None:
    """Create rates_signals table in historical_db.db."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rates_signals (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            calc_date             TEXT NOT NULL,
            yield_curve_slope     REAL,
            inversion_depth       REAL,
            inversion_duration    INTEGER,
            yield_momentum_10yr   REAL,
            yields_rising_fast    INTEGER,
            credit_stress_level   REAL,
            hy_spread             REAL,
            ig_spread             REAL,
            ted_spread            REAL,
            breakeven_inflation   REAL,
            rates_regime          TEXT,
            fetched_at            TEXT NOT NULL,
            UNIQUE(calc_date)
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# FRED helpers
# ---------------------------------------------------------------------------

def _fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame:
    """
    Fetch ALL available observations for a FRED series.
    Returns DataFrame with columns: date (str), value (float).
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "1900-01-01",
        "observation_end": "9999-12-31",
        "limit": 100000,
    }
    try:
        resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        observations = data.get("observations", [])
        rows = []
        for obs in observations:
            val_str = obs.get("value", ".")
            if val_str == ".":
                continue
            try:
                rows.append({"date": obs["date"], "value": float(val_str)})
            except (ValueError, KeyError):
                continue
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "value"])
    except Exception as exc:
        logger.warning("FRED fetch failed for %s: %s", series_id, exc)
        return pd.DataFrame(columns=["date", "value"])


# ---------------------------------------------------------------------------
# Main collector class
# ---------------------------------------------------------------------------

class RatesCreditCollector:
    """
    Fetches, stores, and analyses interest rate and credit market data.

    Usage:
        collector = RatesCreditCollector(config_path='config/settings.yaml')
        summary = collector.collect()
        status  = collector.get_yield_curve_status()
    """

    def __init__(
        self,
        config_path: str = "config/settings.yaml",
        archive_db_path: str = "output/permanent_archive.db",
        historical_db_path: str = "output/historical_db.db",
    ):
        self.archive_db_path = archive_db_path
        self.historical_db_path = historical_db_path
        # Accept a dict directly or a file path string
        if isinstance(config_path, dict):
            self.config = config_path
        elif isinstance(config_path, str):
            self.config = self._load_config(config_path)
        else:
            self.config = {}
        self.fred_api_key: str = (
            self.config.get("api_keys", {}).get("fred", "") or
            self.config.get("altdata", {}).get("collectors", {}).get("fred", {}).get("api_key", "")
        )
        # In-memory cache of recent series data for signal calculation
        self._dgs10: Optional[pd.Series] = None
        self._dgs2: Optional[pd.Series] = None
        self._hy_spread: Optional[pd.Series] = None
        self._ig_spread: Optional[pd.Series] = None
        self._ted_spread: Optional[pd.Series] = None
        self._t10yie: Optional[pd.Series] = None

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(path: str) -> dict:
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning("Could not load config from %s: %s", path, exc)
            return {}

    # ------------------------------------------------------------------
    # Public API — main entry point
    # ------------------------------------------------------------------

    def collect(self) -> Dict:
        """
        Full data collection run:
          1. yfinance yield curve symbols (full history)
          2. FRED yield curve series (full history)
          3. FRED credit spread series (full history)
          4. Calculate and store derived signals

        Returns a summary dict.
        """
        logger.info("RatesCreditCollector.collect() starting")
        summary = {
            "yfinance_rows": 0,
            "fred_yield_rows": 0,
            "fred_credit_rows": 0,
            "signals_rows": 0,
            "errors": [],
        }

        # ── 1. yfinance yield symbols ─────────────────────────────────
        yf_rows = self._collect_yfinance_yields()
        summary["yfinance_rows"] = yf_rows

        # ── 2. FRED yield series ──────────────────────────────────────
        if self.fred_api_key:
            for series_id in FRED_YIELD_SERIES:
                rows = self._collect_fred_series(series_id, "fred_yield")
                summary["fred_yield_rows"] += rows
                time.sleep(0.25)  # FRED rate limiting
        else:
            logger.warning("No FRED API key — skipping FRED yield series")
            summary["errors"].append("No FRED API key configured")

        # ── 3. FRED credit series ──────────────────────────────────────
        if self.fred_api_key:
            for series_id in FRED_CREDIT_SERIES:
                rows = self._collect_fred_series(series_id, "fred_credit")
                summary["fred_credit_rows"] += rows
                time.sleep(0.25)

        # ── 4. Load series into memory for signal calc ─────────────────
        self._load_series_into_memory()

        # ── 5. Calculate and store signals ─────────────────────────────
        sig_rows = self._calculate_and_store_signals()
        summary["signals_rows"] = sig_rows

        logger.info(
            "collect() done — yf=%d fred_yield=%d fred_credit=%d signals=%d",
            summary["yfinance_rows"],
            summary["fred_yield_rows"],
            summary["fred_credit_rows"],
            summary["signals_rows"],
        )
        return summary

    # ------------------------------------------------------------------
    # yfinance collection
    # ------------------------------------------------------------------

    def _collect_yfinance_yields(self) -> int:
        """Download full yield history from yfinance and store in both DBs."""
        total_rows = 0
        fetched_at = datetime.utcnow().isoformat()

        archive_conn = _get_conn(self.archive_db_path)
        hist_conn = _get_conn(self.historical_db_path)
        try:
            _ensure_raw_macro_table(archive_conn)
            _ensure_rates_data_table(hist_conn)

            for name, symbol in YFINANCE_YIELD_SYMBOLS.items():
                logger.info("yfinance: fetching %s (%s)", symbol, name)
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(period="max", auto_adjust=True)
                    if hist.empty:
                        logger.warning("yfinance: no data for %s", symbol)
                        continue

                    rows_archive = []
                    rows_hist = []
                    for idx, row in hist.iterrows():
                        obs_date = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
                        close_val = float(row["Close"]) if pd.notna(row["Close"]) else None
                        if close_val is None:
                            continue
                        rows_archive.append((
                            "yfinance", name, symbol, obs_date, close_val, fetched_at
                        ))
                        rows_hist.append((
                            "yfinance", name, symbol, obs_date, close_val, fetched_at
                        ))

                    archive_conn.executemany("""
                        INSERT OR IGNORE INTO raw_macro_data
                            (series_name, series_id, date, value, source, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [(name, symbol, r[3], r[4], r[0], r[5]) for r in rows_archive])
                    archive_conn.commit()

                    hist_conn.executemany("""
                        INSERT OR REPLACE INTO rates_data
                            (source, series_id, series_name, obs_date, value, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, rows_hist)
                    hist_conn.commit()

                    total_rows += len(rows_archive)
                    logger.info("yfinance: stored %d rows for %s", len(rows_archive), symbol)
                    time.sleep(0.5)

                except Exception as exc:
                    logger.error("yfinance error for %s: %s", symbol, exc)

        finally:
            archive_conn.close()
            hist_conn.close()

        return total_rows

    # ------------------------------------------------------------------
    # FRED collection
    # ------------------------------------------------------------------

    def _collect_fred_series(self, series_id: str, category: str) -> int:
        """Fetch a single FRED series and write to both databases."""
        logger.info("FRED: fetching %s (%s)", series_id, category)
        fetched_at = datetime.utcnow().isoformat()

        df = _fetch_fred_series(series_id, self.fred_api_key)
        if df.empty:
            return 0

        archive_conn = _get_conn(self.archive_db_path)
        hist_conn = _get_conn(self.historical_db_path)
        try:
            _ensure_raw_macro_table(archive_conn)
            _ensure_rates_data_table(hist_conn)

            rows = [
                ("fred", series_id, series_id, row["date"], row["value"], fetched_at)
                for _, row in df.iterrows()
            ]

            archive_conn.executemany("""
                INSERT OR IGNORE INTO raw_macro_data
                    (series_name, series_id, date, value, source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(r[2], r[1], r[3], r[4], r[0], r[5]) for r in rows])
            archive_conn.commit()

            hist_conn.executemany("""
                INSERT OR REPLACE INTO rates_data
                    (source, series_id, series_name, obs_date, value, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
            hist_conn.commit()

            logger.info("FRED: stored %d rows for %s", len(rows), series_id)
            return len(rows)

        finally:
            archive_conn.close()
            hist_conn.close()

    # ------------------------------------------------------------------
    # Load series into memory
    # ------------------------------------------------------------------

    def _load_series_into_memory(self) -> None:
        """Read key series from historical_db.db into pandas Series for calcs."""
        conn = _get_conn(self.historical_db_path)
        try:
            def _load(series_id: str) -> Optional[pd.Series]:
                try:
                    df = pd.read_sql_query(
                        "SELECT obs_date, value FROM rates_data WHERE series_id=? ORDER BY obs_date",
                        conn, params=(series_id,)
                    )
                    if df.empty:
                        return None
                    df["obs_date"] = pd.to_datetime(df["obs_date"])
                    df = df.dropna(subset=["value"]).set_index("obs_date")["value"]
                    return df
                except Exception:
                    return None

            self._dgs10 = _load("DGS10")
            self._dgs2 = _load("DGS2")
            self._hy_spread = _load("BAMLH0A0HYM2")
            self._ig_spread = _load("BAMLC0A0CM")
            self._ted_spread = _load("TEDRATE")
            self._t10yie = _load("T10YIE")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Signal calculation
    # ------------------------------------------------------------------

    def _calculate_and_store_signals(self) -> int:
        """
        Compute derived signals for each available date and store in
        rates_signals table in historical_db.db.
        """
        if self._dgs10 is None or self._dgs2 is None:
            logger.warning("DGS10 or DGS2 not available — skipping signal calc")
            return 0

        # Align DGS10 and DGS2 to common dates
        combined = pd.DataFrame({"dgs10": self._dgs10, "dgs2": self._dgs2}).dropna()
        if combined.empty:
            return 0

        combined["slope"] = combined["dgs10"] - combined["dgs2"]
        combined["inversion_depth"] = combined["slope"].apply(lambda x: abs(min(x, 0.0)))

        # Inversion duration: consecutive business days where slope < 0
        combined["inverted"] = (combined["slope"] < 0).astype(int)
        streak = []
        count = 0
        for inv in combined["inverted"]:
            if inv:
                count += 1
            else:
                count = 0
            streak.append(count)
        combined["inversion_duration"] = streak

        # 4-week momentum on DGS10 (~20 trading days)
        combined["yield_momentum_10yr"] = combined["dgs10"].diff(20)
        combined["yields_rising_fast"] = (combined["yield_momentum_10yr"] > 0.5).astype(int)

        # HY spread z-score (252-day rolling)
        if self._hy_spread is not None:
            hy_aligned = self._hy_spread.reindex(combined.index, method="ffill")
            hy_mean = hy_aligned.rolling(252, min_periods=60).mean()
            hy_std = hy_aligned.rolling(252, min_periods=60).std()
            combined["hy_spread"] = hy_aligned
            combined["credit_stress_level"] = (hy_aligned - hy_mean) / hy_std.replace(0, np.nan)
        else:
            combined["hy_spread"] = np.nan
            combined["credit_stress_level"] = np.nan

        if self._ig_spread is not None:
            combined["ig_spread"] = self._ig_spread.reindex(combined.index, method="ffill")
        else:
            combined["ig_spread"] = np.nan

        if self._ted_spread is not None:
            combined["ted_spread"] = self._ted_spread.reindex(combined.index, method="ffill")
        else:
            combined["ted_spread"] = np.nan

        if self._t10yie is not None:
            combined["breakeven_inflation"] = self._t10yie.reindex(combined.index, method="ffill")
        else:
            combined["breakeven_inflation"] = np.nan

        # Regime classification per row
        combined["rates_regime"] = combined.apply(self._classify_rates_regime_row, axis=1)

        fetched_at = datetime.utcnow().isoformat()
        rows = []
        for idx, row in combined.iterrows():
            calc_date = idx.strftime("%Y-%m-%d")
            rows.append((
                calc_date,
                _safe_float(row.get("slope")),
                _safe_float(row.get("inversion_depth")),
                int(row.get("inversion_duration", 0)),
                _safe_float(row.get("yield_momentum_10yr")),
                int(row.get("yields_rising_fast", 0)),
                _safe_float(row.get("credit_stress_level")),
                _safe_float(row.get("hy_spread")),
                _safe_float(row.get("ig_spread")),
                _safe_float(row.get("ted_spread")),
                _safe_float(row.get("breakeven_inflation")),
                str(row.get("rates_regime", "NEUTRAL")),
                fetched_at,
            ))

        conn = _get_conn(self.historical_db_path)
        try:
            _ensure_rates_signals_table(conn)
            conn.executemany("""
                INSERT OR REPLACE INTO rates_signals (
                    calc_date, yield_curve_slope, inversion_depth, inversion_duration,
                    yield_momentum_10yr, yields_rising_fast, credit_stress_level,
                    hy_spread, ig_spread, ted_spread, breakeven_inflation,
                    rates_regime, fetched_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            conn.commit()
            logger.info("Stored %d rates_signals rows", len(rows))
            return len(rows)
        finally:
            conn.close()

    @staticmethod
    def _classify_rates_regime_row(row: pd.Series) -> str:
        """Classify a single date's rates regime."""
        hy = row.get("hy_spread", np.nan)
        stress = row.get("credit_stress_level", np.nan)
        slope = row.get("slope", 0.0)
        ted = row.get("ted_spread", np.nan)

        hy_ok = not pd.isna(hy)
        stress_ok = not pd.isna(stress)

        if hy_ok and hy > 800:
            return "CRISIS"
        if (hy_ok and hy > 500) or (stress_ok and stress > 2.5):
            return "TIGHT"
        if (hy_ok and hy < 300) and slope > 0.5:
            return "EASY"
        return "NEUTRAL"

    # ------------------------------------------------------------------
    # Public signal accessors
    # ------------------------------------------------------------------

    def get_yield_curve_status(self) -> Dict:
        """Return current yield curve status dict."""
        conn = _get_conn(self.historical_db_path)
        try:
            _ensure_rates_signals_table(conn)
            row = conn.execute("""
                SELECT calc_date, yield_curve_slope, inversion_depth,
                       inversion_duration, yield_momentum_10yr, yields_rising_fast
                FROM rates_signals
                ORDER BY calc_date DESC
                LIMIT 1
            """).fetchone()
        finally:
            conn.close()

        if row is None:
            return {
                "calc_date": None, "slope": None, "is_inverted": False,
                "inversion_weeks": 0, "momentum": None, "yields_rising_fast": False,
            }
        slope = row[1]
        return {
            "calc_date": row[0],
            "slope": slope,
            "is_inverted": (slope is not None and slope < 0),
            "inversion_weeks": int((row[3] or 0) / 5),  # trading days → weeks
            "momentum": row[4],
            "yields_rising_fast": bool(row[5]),
        }

    def get_credit_conditions(self) -> Dict:
        """Return current credit conditions dict."""
        conn = _get_conn(self.historical_db_path)
        try:
            _ensure_rates_signals_table(conn)
            row = conn.execute("""
                SELECT calc_date, hy_spread, ig_spread, credit_stress_level, ted_spread
                FROM rates_signals
                ORDER BY calc_date DESC
                LIMIT 1
            """).fetchone()
        finally:
            conn.close()

        if row is None:
            return {"hy_spread": None, "ig_spread": None, "credit_stress_z": None, "ted_spread": None}
        return {
            "calc_date": row[0],
            "hy_spread": row[1],
            "ig_spread": row[2],
            "credit_stress_z": row[3],
            "ted_spread": row[4],
        }

    def get_rates_regime(self) -> str:
        """Return current rates regime string: EASY/NEUTRAL/TIGHT/CRISIS."""
        conn = _get_conn(self.historical_db_path)
        try:
            _ensure_rates_signals_table(conn)
            row = conn.execute("""
                SELECT rates_regime FROM rates_signals
                ORDER BY calc_date DESC LIMIT 1
            """).fetchone()
        finally:
            conn.close()
        return row[0] if row else "NEUTRAL"

    def get_position_sizing_modifier(self) -> float:
        """
        Combined position sizing modifier based on:
          - Rates regime (CRISIS=0.2, TIGHT=0.6, NEUTRAL=1.0, EASY=1.1)
          - Fed meeting proximity (via get_position_size_multiplier())
        """
        regime_modifiers = {
            "CRISIS": 0.2,
            "TIGHT": 0.6,
            "NEUTRAL": 1.0,
            "EASY": 1.1,
        }
        regime = self.get_rates_regime()
        regime_mod = regime_modifiers.get(regime, 1.0)
        fed_mod = self.get_position_size_multiplier()
        return round(regime_mod * fed_mod, 4)

    def get_breakeven_inflation(self) -> Optional[float]:
        """Return most recent T10YIE (10yr TIPS breakeven inflation) value."""
        conn = _get_conn(self.historical_db_path)
        try:
            _ensure_rates_signals_table(conn)
            row = conn.execute("""
                SELECT breakeven_inflation FROM rates_signals
                WHERE breakeven_inflation IS NOT NULL
                ORDER BY calc_date DESC LIMIT 1
            """).fetchone()
        finally:
            conn.close()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Fed meeting calendar
    # ------------------------------------------------------------------

    def days_to_next_fed_meeting(self) -> int:
        """Return number of calendar days until the next Fed meeting."""
        today = date.today()
        future_dates = [d for d in FED_MEETING_DATES if d >= today]
        if not future_dates:
            return 999
        next_meeting = min(future_dates)
        return (next_meeting - today).days

    def get_position_size_multiplier(self) -> float:
        """
        Return position size multiplier based on proximity to next Fed meeting:
          T-14 days: 0.85
          T-7  days: 0.75
          T-0  (meeting day): 0.0
          Otherwise: 1.0
        """
        days = self.days_to_next_fed_meeting()
        if days == 0:
            return 0.0
        if days <= 7:
            return 0.75
        if days <= 14:
            return 0.85
        return 1.0


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    """Convert value to float, returning None if not possible."""
    try:
        f = float(val)
        return None if (f != f) else f  # catch NaN
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import os
    os.chdir(Path(__file__).resolve().parents[2])  # cd to repo root

    collector = RatesCreditCollector(
        config_path="config/settings.yaml",
        archive_db_path="output/permanent_archive.db",
        historical_db_path="output/historical_db.db",
    )

    print("\n=== Running rates/credit data collection ===")
    summary = collector.collect()
    print(f"Collection summary: {summary}")

    print("\n--- Yield Curve Status ---")
    yc = collector.get_yield_curve_status()
    for k, v in yc.items():
        print(f"  {k}: {v}")

    print("\n--- Credit Conditions ---")
    cc = collector.get_credit_conditions()
    for k, v in cc.items():
        print(f"  {k}: {v}")

    print(f"\n--- Rates Regime: {collector.get_rates_regime()} ---")
    print(f"--- Breakeven Inflation: {collector.get_breakeven_inflation()} ---")
    print(f"--- Days to next Fed meeting: {collector.days_to_next_fed_meeting()} ---")
    print(f"--- Fed position size multiplier: {collector.get_position_size_multiplier()} ---")
    print(f"--- Combined position sizing modifier: {collector.get_position_sizing_modifier()} ---")
