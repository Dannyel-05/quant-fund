"""
STEP 3 — Shipping Intelligence Collector
==========================================
Collects and analyses shipping market data to build the ShippingStressIndex (SSI),
which feeds into sector-level signal modifiers across the pipeline.

Data sources
------------
- Baltic Dry Index (BDI)   : stooq.com CSV (primary), yfinance BDRY ETF (fallback)
- Shipping stocks          : yfinance (BDRY, ZIM, MATX, SBLK, GOGL, EGLE, DSX, NMM,
                             GNK, SB, PNTM)

Outputs
-------
- permanent_archive.db  : raw_shipping_data, raw_commodity_prices (via PermanentArchive)
- historical_db.db      : shipping_data table (time-series indicators)

Run directly for a one-shot collection:
    python data/collectors/shipping_intelligence.py
"""

from __future__ import annotations

import io
import logging
import os
import sqlite3
import sys
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yaml

# Suppress noisy yfinance / pandas warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Path setup — allow running from any working directory
# ---------------------------------------------------------------------------

_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_FILE_DIR, "..", ".."))
sys.path.insert(0, _REPO_ROOT)

from output.setup_permanent_archive import PermanentArchive, DEFAULT_DB_PATH  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("shipping_intelligence")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STOOQ_BDI_URL = "https://stooq.com/q/d/l/?s=bdi&i=d"
STOOQ_HISTORY_START = "2000-01-01"

SHIPPING_STOCKS = [
    "BDRY", "ZIM", "MATX", "SBLK", "GOGL",
    "EGLE", "DSX", "NMM", "GNK", "SB", "PNTM",
]
STOCK_HISTORY_START = "2010-01-01"

HISTORICAL_DB_PATH = os.path.join(_REPO_ROOT, "output", "historical_db.db")
PERMANENT_DB_PATH = DEFAULT_DB_PATH

# SSI weights
SSI_WEIGHT_BDI = 0.30
SSI_WEIGHT_STOCK_COMPOSITE = 0.20
SSI_WEIGHT_CONTAINER = 0.25          # redistributed if no container data

SSI_HIGH_THRESHOLD = 1.5
SSI_LOW_THRESHOLD = -1.5

# Sector impact maps
SECTOR_IMPACTS_HIGH_STRESS: Dict[str, float] = {
    "retailers": -0.3,
    "food_manufacturers": -0.2,
    "electronics": -0.2,
    "domestic_producers": +0.3,
    "air_freight": +0.2,
    "shipping": +0.3,
}

SECTOR_IMPACTS_LOW_STRESS: Dict[str, float] = {
    "retailers": +0.2,
    "importers": +0.2,
    "shipping": -0.2,
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _rolling_zscore(series: pd.Series, window: int = 252) -> pd.Series:
    """Compute rolling z-score with min_periods=window//2."""
    mean = series.rolling(window, min_periods=window // 2).mean()
    std = series.rolling(window, min_periods=window // 2).std()
    return (series - mean) / std.replace(0, np.nan)


def _percentile_rank(series: pd.Series) -> pd.Series:
    """Historical percentile rank (0–100) of each value."""
    return series.expanding().apply(
        lambda x: float(pd.Series(x).rank(pct=True).iloc[-1]) * 100,
        raw=False,
    )


def _load_config() -> Dict[str, Any]:
    config_path = os.path.join(_REPO_ROOT, "config", "settings.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path) as fh:
                return yaml.safe_load(fh) or {}
        except Exception as exc:
            logger.warning("Could not load config: %s", exc)
    return {}


# ---------------------------------------------------------------------------
# Historical DB helpers
# ---------------------------------------------------------------------------

def _get_hist_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(HISTORICAL_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_shipping_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shipping_data (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            date                    TEXT NOT NULL,
            bdi_value               REAL,
            bdi_ma5                 REAL,
            bdi_ma20                REAL,
            bdi_ma60                REAL,
            bdi_zscore_252          REAL,
            bdi_pct_rank            REAL,
            bdi_roc_1w              REAL,
            bdi_roc_4w              REAL,
            bdi_source              TEXT,
            stock_composite_zscore  REAL,
            shipping_stress_index   REAL,
            stress_regime           TEXT,
            fetched_at              TEXT,
            UNIQUE(date)
        )
        """
    )
    conn.commit()


def _upsert_shipping_row(conn: sqlite3.Connection, row: Dict[str, Any]):
    conn.execute(
        """
        INSERT INTO shipping_data
            (date, bdi_value, bdi_ma5, bdi_ma20, bdi_ma60, bdi_zscore_252,
             bdi_pct_rank, bdi_roc_1w, bdi_roc_4w, bdi_source,
             stock_composite_zscore, shipping_stress_index, stress_regime,
             fetched_at)
        VALUES
            (:date, :bdi_value, :bdi_ma5, :bdi_ma20, :bdi_ma60,
             :bdi_zscore_252, :bdi_pct_rank, :bdi_roc_1w, :bdi_roc_4w,
             :bdi_source, :stock_composite_zscore, :shipping_stress_index,
             :stress_regime, :fetched_at)
        ON CONFLICT(date) DO UPDATE SET
            bdi_value              = excluded.bdi_value,
            bdi_ma5                = excluded.bdi_ma5,
            bdi_ma20               = excluded.bdi_ma20,
            bdi_ma60               = excluded.bdi_ma60,
            bdi_zscore_252         = excluded.bdi_zscore_252,
            bdi_pct_rank           = excluded.bdi_pct_rank,
            bdi_roc_1w             = excluded.bdi_roc_1w,
            bdi_roc_4w             = excluded.bdi_roc_4w,
            bdi_source             = excluded.bdi_source,
            stock_composite_zscore = excluded.stock_composite_zscore,
            shipping_stress_index  = excluded.shipping_stress_index,
            stress_regime          = excluded.stress_regime,
            fetched_at             = excluded.fetched_at
        """,
        row,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# BDI data fetching
# ---------------------------------------------------------------------------

class BDIFetcher:
    """Fetches Baltic Dry Index history from stooq.com with BDRY ETF fallback."""

    def fetch(self) -> Tuple[pd.DataFrame, str]:
        """
        Returns (df, source) where df has columns [date, value] and source is
        'BDI' or 'PROXY_BDRY'.
        """
        df = self._fetch_stooq()
        if df is not None and not df.empty:
            logger.info("BDI: fetched %d rows from stooq.com", len(df))
            return df, "BDI"

        logger.warning("stooq BDI fetch failed — falling back to BDRY ETF proxy")
        df = self._fetch_bdry_proxy()
        if df is not None and not df.empty:
            logger.info("BDI proxy: fetched %d rows for BDRY", len(df))
            return df, "PROXY_BDRY"

        raise RuntimeError("Could not obtain BDI data from any source.")

    def _fetch_stooq(self) -> Optional[pd.DataFrame]:
        try:
            resp = requests.get(STOOQ_BDI_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or "No data" in text or len(text) < 50:
                logger.warning("stooq returned empty/invalid response")
                return None
            df = pd.read_csv(io.StringIO(text))
            df.columns = [c.lower().strip() for c in df.columns]
            # stooq columns: date, open, high, low, close, volume
            if "date" not in df.columns or "close" not in df.columns:
                logger.warning("stooq CSV missing expected columns: %s", list(df.columns))
                return None
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date", "close"])
            df = df[df["date"] >= pd.Timestamp(STOOQ_HISTORY_START)]
            df = df.sort_values("date").reset_index(drop=True)
            df = df.rename(columns={"close": "value"})
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"])
            return df[["date", "value"]]
        except Exception as exc:
            logger.warning("stooq fetch error: %s", exc)
            return None

    def _fetch_bdry_proxy(self) -> Optional[pd.DataFrame]:
        try:
            import yfinance as yf
            ticker = yf.Ticker("BDRY")
            hist = ticker.history(start=STOOQ_HISTORY_START, auto_adjust=True)
            if hist.empty:
                return None
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            hist["date"] = pd.to_datetime(hist["date"], utc=True).dt.tz_localize(None)
            df = hist[["date", "close"]].rename(columns={"close": "value"})
            df = df.sort_values("date").reset_index(drop=True)
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            return df.dropna(subset=["value"])
        except Exception as exc:
            logger.warning("BDRY proxy fetch error: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Shipping stock fetching
# ---------------------------------------------------------------------------

class ShippingStockFetcher:
    """Fetches price history for shipping stocks via yfinance."""

    def fetch_all(self) -> Dict[str, pd.DataFrame]:
        """Returns dict ticker -> DataFrame(date, open, high, low, close, volume)."""
        results: Dict[str, pd.DataFrame] = {}
        for ticker in SHIPPING_STOCKS:
            df = self._fetch_ticker(ticker)
            if df is not None and not df.empty:
                results[ticker] = df
            else:
                logger.warning("No data for shipping stock %s", ticker)
        logger.info("Shipping stocks fetched: %d / %d", len(results), len(SHIPPING_STOCKS))
        return results

    def _fetch_ticker(self, ticker: str) -> Optional[pd.DataFrame]:
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            hist = t.history(start=STOCK_HISTORY_START, auto_adjust=True)
            if hist.empty:
                return None
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            hist["date"] = pd.to_datetime(hist["date"], utc=True).dt.tz_localize(None)
            hist = hist.sort_values("date").reset_index(drop=True)
            for col in ("open", "high", "low", "close", "volume"):
                if col not in hist.columns:
                    hist[col] = np.nan
            return hist[["date", "open", "high", "low", "close", "volume"]]
        except Exception as exc:
            logger.warning("yfinance error for %s: %s", ticker, exc)
            return None


# ---------------------------------------------------------------------------
# Indicator computation
# ---------------------------------------------------------------------------

def compute_bdi_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with columns [date, value], compute:
      ma5, ma20, ma60, zscore_252, pct_rank, roc_1w, roc_4w
    Returns the enriched DataFrame.
    """
    df = df.copy().sort_values("date").reset_index(drop=True)
    v = df["value"]

    df["ma5"] = v.rolling(5, min_periods=2).mean()
    df["ma20"] = v.rolling(20, min_periods=5).mean()
    df["ma60"] = v.rolling(60, min_periods=15).mean()
    df["zscore_252"] = _rolling_zscore(v, 252)
    df["pct_rank"] = _percentile_rank(v)
    df["roc_1w"] = v.pct_change(5)     # ~1 trading week
    df["roc_4w"] = v.pct_change(20)    # ~4 trading weeks

    return df


def compute_stock_composite(stock_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Compute ShippingStockComposite = equal-weight average z-score of all
    available shipping stocks.  Returns DataFrame(date, composite_zscore,
    n_stocks).
    """
    if not stock_data:
        return pd.DataFrame(columns=["date", "composite_zscore", "n_stocks"])

    z_frames = []
    for ticker, df in stock_data.items():
        df = df.copy().sort_values("date").set_index("date")
        z = _rolling_zscore(df["close"], 252).rename(ticker)
        z_frames.append(z)

    combined = pd.concat(z_frames, axis=1)
    composite = combined.mean(axis=1).rename("composite_zscore")
    n_stocks = combined.notna().sum(axis=1).rename("n_stocks")

    result = pd.concat([composite, n_stocks], axis=1).reset_index()
    result.columns = ["date", "composite_zscore", "n_stocks"]
    return result


def compute_ssi(
    bdi_df: pd.DataFrame,
    stock_composite_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute ShippingStressIndex = weighted average of available components.
    Container rate component is absent — weights are redistributed proportionally.

    Returns merged DataFrame with ssi and stress_regime columns.
    """
    bdi = bdi_df[["date", "zscore_252"]].copy().rename(columns={"zscore_252": "bdi_z"})
    bdi["date"] = pd.to_datetime(bdi["date"])

    stock = stock_composite_df[["date", "composite_zscore"]].copy()
    stock["date"] = pd.to_datetime(stock["date"])

    merged = pd.merge(bdi, stock, on="date", how="outer").sort_values("date")
    merged = merged.reset_index(drop=True)

    # Redistribute weights when container rate is absent
    total_available_weight = SSI_WEIGHT_BDI + SSI_WEIGHT_STOCK_COMPOSITE
    w_bdi = SSI_WEIGHT_BDI / total_available_weight
    w_stock = SSI_WEIGHT_STOCK_COMPOSITE / total_available_weight

    # Compute SSI row-by-row to handle NaNs gracefully
    ssi_values = []
    for _, row in merged.iterrows():
        components = []
        weights = []
        if pd.notna(row.get("bdi_z")):
            components.append(row["bdi_z"] * w_bdi)
            weights.append(w_bdi)
        if pd.notna(row.get("composite_zscore")):
            components.append(row["composite_zscore"] * w_stock)
            weights.append(w_stock)
        if components:
            total_w = sum(weights)
            ssi = sum(components) / total_w if total_w > 0 else np.nan
        else:
            ssi = np.nan
        ssi_values.append(ssi)

    merged["ssi"] = ssi_values
    merged["stress_regime"] = merged["ssi"].apply(_classify_stress)
    return merged


def _classify_stress(ssi: float) -> str:
    if pd.isna(ssi):
        return "UNKNOWN"
    if ssi > SSI_HIGH_THRESHOLD:
        return "HIGH"
    if ssi < SSI_LOW_THRESHOLD:
        return "LOW"
    return "NEUTRAL"


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _persist_bdi_to_archive(
    archive: PermanentArchive,
    df: pd.DataFrame,
    source: str,
) -> int:
    """Insert BDI rows into raw_shipping_data. Returns count of rows inserted."""
    inserted = 0
    for _, row in df.iterrows():
        try:
            archive.insert_shipping(
                index_name="BDI",
                date=row["date"].strftime("%Y-%m-%d"),
                value=float(row["value"]),
                source=source,
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # duplicate — skip silently
        except Exception as exc:
            logger.debug("BDI archive insert error: %s", exc)
    return inserted


def _persist_stocks_to_archive(
    archive: PermanentArchive,
    stock_data: Dict[str, pd.DataFrame],
) -> int:
    """Insert shipping stock OHLCV into raw_commodity_prices."""
    inserted = 0
    for ticker, df in stock_data.items():
        for _, row in df.iterrows():
            try:
                archive.insert_commodity(
                    commodity="SHIPPING_STOCK",
                    symbol=ticker,
                    date=row["date"].strftime("%Y-%m-%d"),
                    open=float(row.get("open") or 0),
                    high=float(row.get("high") or 0),
                    low=float(row.get("low") or 0),
                    close=float(row.get("close") or 0),
                    volume=float(row.get("volume") or 0),
                    source="yfinance",
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
            except Exception as exc:
                logger.debug("Stock archive insert error (%s): %s", ticker, exc)
    return inserted


def _persist_ssi_to_hist_db(
    conn: sqlite3.Connection,
    bdi_indicators: pd.DataFrame,
    ssi_df: pd.DataFrame,
    bdi_source: str,
):
    """Upsert merged SSI + BDI indicator rows into historical_db shipping_data."""
    bdi_idx = bdi_indicators.set_index("date")
    ssi_idx = ssi_df.set_index("date")

    fetched = _utcnow()
    for date, ssi_row in ssi_idx.iterrows():
        bdi_row = bdi_idx.loc[date] if date in bdi_idx.index else pd.Series()

        def _get(series, col):
            v = series.get(col, np.nan)
            return None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)

        row = {
            "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
            "bdi_value": _get(bdi_row, "value"),
            "bdi_ma5": _get(bdi_row, "ma5"),
            "bdi_ma20": _get(bdi_row, "ma20"),
            "bdi_ma60": _get(bdi_row, "ma60"),
            "bdi_zscore_252": _get(bdi_row, "zscore_252"),
            "bdi_pct_rank": _get(bdi_row, "pct_rank"),
            "bdi_roc_1w": _get(bdi_row, "roc_1w"),
            "bdi_roc_4w": _get(bdi_row, "roc_4w"),
            "bdi_source": bdi_source,
            "stock_composite_zscore": _get(ssi_row, "composite_zscore"),
            "shipping_stress_index": _get(ssi_row, "ssi"),
            "stress_regime": str(ssi_row.get("stress_regime", "UNKNOWN")),
            "fetched_at": fetched,
        }
        _upsert_shipping_row(conn, row)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class ShippingIntelligence:
    """
    Collects, computes, and persists shipping market intelligence.

    Example
    -------
    si = ShippingIntelligence()
    summary = si.collect()
    print(si.get_current_stress())
    print(si.get_sector_impacts())
    bdi_hist = si.get_historical_bdi(days=90)
    """

    def __init__(
        self,
        archive_db_path: str = PERMANENT_DB_PATH,
        hist_db_path: str = HISTORICAL_DB_PATH,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.archive_db_path = archive_db_path
        self.hist_db_path = hist_db_path
        self.config = config or _load_config()

        # Cached state populated after collect()
        self._bdi_df: Optional[pd.DataFrame] = None
        self._bdi_source: str = "BDI"
        self._stock_data: Dict[str, pd.DataFrame] = {}
        self._stock_composite: Optional[pd.DataFrame] = None
        self._ssi_df: Optional[pd.DataFrame] = None

    # ── Public API ───────────────────────────────────────────────────────────

    def collect(self) -> Dict[str, Any]:
        """
        Run all data collection steps.  Returns a summary dict.
        Safe to call repeatedly — uses upsert / idempotent inserts.
        """
        summary: Dict[str, Any] = {
            "timestamp": _utcnow(),
            "bdi_rows": 0,
            "bdi_source": "NONE",
            "stock_rows_inserted": 0,
            "stocks_fetched": 0,
            "ssi_current": None,
            "stress_regime": "UNKNOWN",
            "errors": [],
        }

        archive = PermanentArchive(self.archive_db_path)
        hist_conn = _get_hist_conn()
        _ensure_shipping_table(hist_conn)

        # Step 1: BDI
        try:
            bdi_raw, bdi_source = BDIFetcher().fetch()
            self._bdi_df = compute_bdi_indicators(bdi_raw)
            self._bdi_source = bdi_source
            summary["bdi_source"] = bdi_source
            summary["bdi_rows"] = len(bdi_raw)
            logger.info("BDI indicators computed (%d rows, source=%s)", len(self._bdi_df), bdi_source)

            # Persist raw BDI
            _persist_bdi_to_archive(archive, bdi_raw, bdi_source)
        except Exception as exc:
            logger.error("BDI collection failed: %s", exc)
            summary["errors"].append(f"BDI: {exc}")

        # Step 2: Shipping stocks
        try:
            self._stock_data = ShippingStockFetcher().fetch_all()
            summary["stocks_fetched"] = len(self._stock_data)

            rows_inserted = _persist_stocks_to_archive(archive, self._stock_data)
            summary["stock_rows_inserted"] = rows_inserted
            logger.info("Shipping stocks persisted: %d total OHLCV rows", rows_inserted)

            self._stock_composite = compute_stock_composite(self._stock_data)
        except Exception as exc:
            logger.error("Shipping stock collection failed: %s", exc)
            summary["errors"].append(f"Stocks: {exc}")
            self._stock_composite = pd.DataFrame(columns=["date", "composite_zscore", "n_stocks"])

        # Step 3: SSI
        try:
            bdi_for_ssi = self._bdi_df if self._bdi_df is not None else pd.DataFrame(columns=["date", "zscore_252"])
            composite_for_ssi = self._stock_composite if self._stock_composite is not None else pd.DataFrame(columns=["date", "composite_zscore"])

            self._ssi_df = compute_ssi(bdi_for_ssi, composite_for_ssi)

            # Store in historical_db
            _persist_ssi_to_hist_db(hist_conn, bdi_for_ssi, self._ssi_df, self._bdi_source)
            logger.info("SSI persisted to historical_db (%d rows)", len(self._ssi_df))

            current = self.get_current_stress()
            regime = _classify_stress(current) if current is not None else "UNKNOWN"
            summary["ssi_current"] = current
            summary["stress_regime"] = regime
            logger.info("Current SSI: %.3f  Regime: %s", current or float("nan"), regime)
        except Exception as exc:
            logger.error("SSI computation failed: %s", exc)
            summary["errors"].append(f"SSI: {exc}")

        hist_conn.close()
        archive.close()
        return summary

    def get_current_stress(self) -> Optional[float]:
        """
        Returns the most recent ShippingStressIndex as a float, or None if
        data has not been collected yet.
        """
        if self._ssi_df is None or self._ssi_df.empty:
            # Try loading from historical_db
            return self._load_latest_ssi_from_db()
        latest = self._ssi_df.dropna(subset=["ssi"])
        if latest.empty:
            return None
        return float(latest.iloc[-1]["ssi"])

    def get_sector_impacts(self, ssi: Optional[float] = None) -> Dict[str, float]:
        """
        Returns a dict of sector -> signal modifier for the current (or
        provided) ShippingStressIndex.

        Example
        -------
        impacts = si.get_sector_impacts()
        # {'retailers': -0.3, 'food_manufacturers': -0.2, ...}
        """
        if ssi is None:
            ssi = self.get_current_stress()
        if ssi is None:
            return {}

        if ssi > SSI_HIGH_THRESHOLD:
            return dict(SECTOR_IMPACTS_HIGH_STRESS)
        if ssi < SSI_LOW_THRESHOLD:
            return dict(SECTOR_IMPACTS_LOW_STRESS)
        return {}

    def get_historical_bdi(self, days: int = 252) -> pd.DataFrame:
        """
        Returns a DataFrame with BDI history and indicators for the last
        `days` trading days.  Loads from historical_db if collect() has not
        been called in this session.

        Columns: date, bdi_value, bdi_ma5, bdi_ma20, bdi_ma60,
                 bdi_zscore_252, bdi_pct_rank, bdi_roc_1w, bdi_roc_4w,
                 stock_composite_zscore, shipping_stress_index, stress_regime
        """
        if self._bdi_df is not None:
            df = self._bdi_df.tail(days).copy()
            return df

        # Fall back to historical_db
        return self._load_bdi_from_db(days)

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _load_latest_ssi_from_db(self) -> Optional[float]:
        try:
            conn = sqlite3.connect(self.hist_db_path)
            row = conn.execute(
                "SELECT shipping_stress_index FROM shipping_data "
                "WHERE shipping_stress_index IS NOT NULL "
                "ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            return float(row[0]) if row else None
        except Exception as exc:
            logger.debug("Could not load SSI from hist DB: %s", exc)
            return None

    def _load_bdi_from_db(self, days: int) -> pd.DataFrame:
        try:
            conn = sqlite3.connect(self.hist_db_path)
            cutoff = (datetime.now() - timedelta(days=days * 1.5)).strftime("%Y-%m-%d")
            df = pd.read_sql(
                "SELECT date, bdi_value, bdi_ma5, bdi_ma20, bdi_ma60, "
                "bdi_zscore_252, bdi_pct_rank, bdi_roc_1w, bdi_roc_4w, "
                "stock_composite_zscore, shipping_stress_index, stress_regime "
                "FROM shipping_data WHERE date >= ? ORDER BY date DESC LIMIT ?",
                conn,
                params=(cutoff, days),
            )
            conn.close()
            return df.sort_values("date").reset_index(drop=True)
        except Exception as exc:
            logger.warning("Could not load BDI history from DB: %s", exc)
            return pd.DataFrame()


# ---------------------------------------------------------------------------
# Module-level convenience functions (for use by other pipeline modules)
# ---------------------------------------------------------------------------

def get_current_shipping_stress() -> Optional[float]:
    """Quick access to latest SSI from historical_db without running collect()."""
    si = ShippingIntelligence()
    return si._load_latest_ssi_from_db()


def get_shipping_sector_impacts() -> Dict[str, float]:
    """Return sector impact dict based on current SSI stored in historical_db."""
    si = ShippingIntelligence()
    ssi = si.get_current_stress()
    return si.get_sector_impacts(ssi)


# ---------------------------------------------------------------------------
# __main__ entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    print("\n" + "=" * 60)
    print("  SHIPPING INTELLIGENCE COLLECTOR")
    print("=" * 60 + "\n")

    si = ShippingIntelligence()
    summary = si.collect()

    print("\n--- Collection Summary ---")
    print(json.dumps(
        {k: (round(v, 4) if isinstance(v, float) else v) for k, v in summary.items()},
        indent=2,
    ))

    ssi = si.get_current_stress()
    impacts = si.get_sector_impacts(ssi)

    print(f"\nCurrent ShippingStressIndex : {ssi:.3f}" if ssi is not None else "\nSSI: N/A")
    print(f"Stress regime               : {_classify_stress(ssi) if ssi is not None else 'UNKNOWN'}")

    if impacts:
        print("\nSector Impacts:")
        for sector, modifier in impacts.items():
            sign = "+" if modifier > 0 else ""
            print(f"  {sector:<25} {sign}{modifier:.1f}")
    else:
        print("\nNo sector impacts (stress is NEUTRAL).")

    print("\nBDI — last 5 days:")
    hist = si.get_historical_bdi(days=5)
    if not hist.empty:
        cols = ["date", "value", "ma20", "zscore_252", "roc_4w"] if "value" in hist.columns else hist.columns.tolist()[:5]
        available = [c for c in cols if c in hist.columns]
        print(hist[available].tail(5).to_string(index=False))
    else:
        print("  (no history available)")

    print("\nDone.\n")
