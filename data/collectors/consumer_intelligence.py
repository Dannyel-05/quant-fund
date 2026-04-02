"""
STEP 6 — Consumer Intelligence

Collects consumer economic data from:
  - FRED API (sentiment, spending, savings, employment, housing, inflation)
  - yfinance (payment processor proxy signals: V, MA, AXP, DFS)

Builds composite health indices and stores all history permanently in SQLite.
"""

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "settings.yaml"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"
_HIST_DB = _ROOT / "output" / "historical_db.db"

# ── Config ────────────────────────────────────────────────────────────────────

def _load_config() -> Dict[str, Any]:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as exc:
        logger.warning("Could not load settings.yaml: %s", exc)
        return {}


# ── FRED series definitions ───────────────────────────────────────────────────

FRED_SERIES: Dict[str, str] = {
    # Consumer Sentiment
    "UMCSENT": "University of Michigan Consumer Sentiment",
    "CSCICP03USM665S": "OECD Consumer Confidence Index — US",
    # Spending / Savings
    "PSAVERT": "Personal Saving Rate",
    "PCE": "Personal Consumption Expenditures",
    "PCEC96": "Real Personal Consumption Expenditures",
    "RRSFS": "Advance Real Retail and Food Services Sales",
    "RSXFS": "Advance Retail Sales: Retail and Food Services",
    "REVOLNS": "Revolving Consumer Credit Outstanding",
    # Employment
    "UNRATE": "Civilian Unemployment Rate",
    "ICSA": "Initial Claims for Unemployment Insurance",
    "CCSA": "Continued Claims (Insured Unemployment)",
    "PAYEMS": "All Employees, Total Nonfarm",
    "AWHI": "Average Weekly Hours of Production and Nonsupervisory Employees",
    # Housing
    "HOUST": "Housing Starts",
    "PERMIT": "New Privately Owned Housing Units Authorized",
    "CSUSHPINSA": "S&P/Case-Shiller U.S. National Home Price Index",
    "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average",
    "MSPUS": "Median Sales Price of Houses Sold for the United States",
    # Inflation
    "CPIAUCSL": "Consumer Price Index for All Urban Consumers: All Items",
    "CPILFESL": "CPI: All Items Less Food and Energy (Core)",
    "CPIUFDSL": "CPI: Food",
    "CPIENGSL": "CPI: Energy",
    "PPIACO": "Producer Price Index: All Commodities",
    "PPIFGS": "PPI: Finished Goods",
    # UK
    "GBRCPIALLMINMEI": "CPI: All Items, United Kingdom",
    "LRHUTTTTGBM156S": "Harmonised Unemployment Rate: Total: All Persons — United Kingdom",
}

# Composite index series requirements
_CONSUMER_HEALTH_SERIES = ["UMCSENT", "UNRATE", "RSXFS", "PSAVERT"]
_HOUSING_HEALTH_SERIES = ["HOUST", "CSUSHPINSA", "MORTGAGE30US"]
_INFLATION_SERIES = ["CPIAUCSL", "PPIACO"]

# Payment processor proxy tickers
_PAYMENT_TICKERS = ["V", "MA", "AXP", "DFS"]

# ── Database helpers ──────────────────────────────────────────────────────────

def _get_perm_conn() -> sqlite3.Connection:
    _PERM_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_PERM_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _get_hist_conn() -> sqlite3.Connection:
    _HIST_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_HIST_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_perm_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_macro_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id   TEXT NOT NULL,
            series_name TEXT,
            date        TEXT NOT NULL,
            value       REAL,
            source      TEXT DEFAULT 'fred',
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(series_id, date)
        );
    """)
    conn.commit()


def _init_hist_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS macro_series (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id   TEXT NOT NULL,
            series_name TEXT,
            date        TEXT NOT NULL,
            value       REAL,
            source      TEXT DEFAULT 'fred',
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(series_id, date)
        );

        CREATE TABLE IF NOT EXISTS payment_processor_signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            date        TEXT NOT NULL,
            close       REAL,
            return_30d  REAL,
            composite   REAL,
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(ticker, date)
        );
    """)
    conn.commit()


# ── FRED fetching ─────────────────────────────────────────────────────────────

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def _fetch_fred_series(series_id: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetch ALL available history for a FRED series."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "1900-01-01",
        "observation_end": datetime.today().strftime("%Y-%m-%d"),
        "limit": 100000,
    }
    resp = requests.get(_FRED_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("observations", [])


def _store_fred_observations(
    perm_conn: sqlite3.Connection,
    hist_conn: sqlite3.Connection,
    series_id: str,
    series_name: str,
    observations: List[Dict[str, Any]],
) -> None:
    """Upsert FRED observations into both permanent and historical DBs."""
    rows = []
    for obs in observations:
        val_str = obs.get("value", ".")
        if val_str == "." or val_str is None:
            continue
        try:
            val = float(val_str)
        except (ValueError, TypeError):
            continue
        rows.append((series_id, series_name, obs["date"], val))

    perm_conn.executemany(
        "INSERT OR IGNORE INTO raw_macro_data (series_id, series_name, date, value) VALUES (?,?,?,?)",
        rows,
    )
    perm_conn.commit()

    hist_conn.executemany(
        "INSERT OR IGNORE INTO macro_series (series_id, series_name, date, value) VALUES (?,?,?,?)",
        rows,
    )
    hist_conn.commit()


def _get_latest_value(
    conn: sqlite3.Connection, series_id: str, table: str = "raw_macro_data"
) -> Optional[float]:
    row = conn.execute(
        f"SELECT value FROM {table} WHERE series_id=? ORDER BY date DESC LIMIT 1",
        (series_id,),
    ).fetchone()
    return row[0] if row else None


def _get_series_history(
    conn: sqlite3.Connection,
    series_id: str,
    table: str = "raw_macro_data",
    limit: int = 500,
) -> List[Tuple[str, float]]:
    """Returns list of (date, value) tuples, ascending."""
    rows = conn.execute(
        f"SELECT date, value FROM {table} WHERE series_id=? ORDER BY date ASC LIMIT ?",
        (series_id, limit),
    ).fetchall()
    return rows


# ── Statistics helpers ────────────────────────────────────────────────────────

def _zscore(values: List[float], window: int = 60) -> Optional[float]:
    """Compute z-score of the most recent value against a rolling window."""
    if len(values) < 2:
        return None
    recent = values[-window:] if len(values) >= window else values
    if len(recent) < 2:
        return None
    mean = sum(recent) / len(recent)
    variance = sum((x - mean) ** 2 for x in recent) / (len(recent) - 1)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return (recent[-1] - mean) / std


def _trend(values: List[float], periods: int = 4) -> str:
    """Returns 'UP', 'DOWN', or 'FLAT' based on last N periods."""
    if len(values) < periods + 1:
        return "INSUFFICIENT_DATA"
    recent = values[-(periods + 1):]
    slope = recent[-1] - recent[0]
    threshold = 0.01 * abs(recent[0]) if recent[0] != 0 else 0.001
    if slope > threshold:
        return "UP"
    if slope < -threshold:
        return "DOWN"
    return "FLAT"


def _safe_clamp(value: Optional[float], lo: float = -1.0, hi: float = 1.0) -> float:
    if value is None:
        return 0.0
    return max(lo, min(hi, value))


# ── Payment processor signals ─────────────────────────────────────────────────

def _fetch_payment_signals(hist_conn: sqlite3.Connection) -> Optional[float]:
    """
    Fetch payment processor stocks, compute 30-day return, store and return
    equal-weight composite.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed; skipping payment processor signals.")
        return None

    composite_values: Dict[str, float] = {}

    for ticker in _PAYMENT_TICKERS:
        try:
            stk = yf.Ticker(ticker)
            hist = stk.history(start="2010-01-01", auto_adjust=True)
            if hist.empty:
                logger.warning("No price data for %s", ticker)
                continue

            hist = hist.sort_index()
            close_col = "Close"
            closes = hist[close_col].dropna()

            # Compute 30-day return at each date
            rows = []
            dates = closes.index.tolist()
            for i, dt in enumerate(dates):
                date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                close_val = float(closes.iloc[i])
                ret_30d = None
                if i >= 30:
                    prev_close = float(closes.iloc[i - 30])
                    if prev_close != 0:
                        ret_30d = (close_val - prev_close) / prev_close
                rows.append((ticker, date_str, close_val, ret_30d))

            hist_conn.executemany(
                """INSERT OR IGNORE INTO payment_processor_signals
                   (ticker, date, close, return_30d) VALUES (?,?,?,?)""",
                rows,
            )
            hist_conn.commit()

            # Latest 30-day return for composite
            if len(closes) >= 31:
                last_close = float(closes.iloc[-1])
                prev_close = float(closes.iloc[-31])
                if prev_close != 0:
                    composite_values[ticker] = (last_close - prev_close) / prev_close

            logger.info("Payment processor %s: %d rows stored", ticker, len(rows))

        except Exception as exc:
            logger.error("Payment processor %s failed: %s", ticker, exc)

    if not composite_values:
        return None

    composite = sum(composite_values.values()) / len(composite_values)

    # Update composite column for latest date
    today_str = datetime.today().strftime("%Y-%m-%d")
    for ticker in composite_values:
        hist_conn.execute(
            """UPDATE payment_processor_signals
               SET composite=? WHERE ticker=? AND date=(
                   SELECT MAX(date) FROM payment_processor_signals WHERE ticker=?)""",
            (composite, ticker, ticker),
        )
    hist_conn.commit()

    logger.info("PaymentProcessorComposite (30d return, equal-weight): %.4f", composite)
    return composite


# ── Main class ────────────────────────────────────────────────────────────────

class ConsumerIntelligence:
    """
    Fetches and analyses consumer economic data from FRED and yfinance.
    Provides composite health indices for signal generation.
    """

    def __init__(self):
        self._config = _load_config()
        self._fred_key: str = self._config.get("api_keys", {}).get("fred", "")
        self._series_cache: Dict[str, List[Tuple[str, float]]] = {}
        self._latest_cache: Dict[str, float] = {}

    # ── Internal helpers ───────────────────────────────────────────────────

    def _load_series(self, series_id: str) -> List[float]:
        """Return cached list of float values for a series (ascending date)."""
        if series_id not in self._series_cache:
            return []
        return [v for _, v in self._series_cache[series_id]]

    def _load_cache_from_db(self, perm_conn: sqlite3.Connection) -> None:
        """Populate in-memory series cache from DB for fast index computation."""
        for series_id in FRED_SERIES:
            rows = _get_series_history(perm_conn, series_id, limit=500)
            if rows:
                self._series_cache[series_id] = rows
                self._latest_cache[series_id] = rows[-1][1]

    # ── Public API ─────────────────────────────────────────────────────────

    def collect(self) -> Dict[str, Any]:
        """
        Fetch all FRED series and payment processor data.
        Stores ALL history permanently. Returns summary dict.
        """
        if not self._fred_key:
            logger.error("FRED API key not configured. Set api_keys.fred in settings.yaml.")

        perm_conn = _get_perm_conn()
        hist_conn = _get_hist_conn()
        results: Dict[str, Any] = {"series_collected": 0, "series_failed": 0, "errors": []}

        try:
            _init_perm_db(perm_conn)
            _init_hist_db(hist_conn)

            # Fetch FRED series
            for series_id, series_name in FRED_SERIES.items():
                try:
                    logger.info("Fetching FRED %s — %s", series_id, series_name)
                    observations = _fetch_fred_series(series_id, self._fred_key)
                    _store_fred_observations(
                        perm_conn, hist_conn, series_id, series_name, observations
                    )
                    results["series_collected"] += 1
                    logger.info("  Stored %d observations", len(observations))
                except Exception as exc:
                    logger.error("FRED %s failed: %s", series_id, exc)
                    results["series_failed"] += 1
                    results["errors"].append(f"{series_id}: {exc}")

            # Payment processor signals
            logger.info("=== Collecting payment processor signals ===")
            try:
                composite = _fetch_payment_signals(hist_conn)
                results["payment_composite_30d"] = composite
            except Exception as exc:
                logger.error("Payment processor collection failed: %s", exc)
                results["payment_composite_30d"] = None

            # Populate in-memory cache for index computations
            self._load_cache_from_db(perm_conn)

            results["consumer_health_index"] = self.get_consumer_health_index()
            results["housing_health_index"] = self.get_housing_health_index()
            results["inflation_pressure"] = self.get_inflation_pressure()

        finally:
            perm_conn.close()
            hist_conn.close()

        logger.info(
            "Consumer Intelligence: %d series collected, %d failed",
            results["series_collected"],
            results["series_failed"],
        )
        return results

    def get_consumer_health_index(self) -> float:
        """
        Returns float in [-1, 1].
        Weighted combination of UMCSENT, UNRATE (inverted), RSXFS, PSAVERT z-scores.
        """
        weights = {
            "UMCSENT": 0.25,
            "UNRATE": -0.25,   # inverted: high unemployment = low score
            "RSXFS": 0.25,
            "PSAVERT": 0.25,
        }
        score = 0.0
        total_weight = 0.0
        for series_id, weight in weights.items():
            vals = self._load_series(series_id)
            z = _zscore(vals)
            if z is not None:
                score += weight * z
                total_weight += abs(weight)

        if total_weight == 0:
            return 0.0
        # Normalise to roughly [-1, 1] — z-score of 3 maps to ~1
        normalized = _safe_clamp(score / 3.0)
        return round(normalized, 4)

    def get_housing_health_index(self) -> float:
        """
        Returns float in [-1, 1].
        Weighted: HOUST (0.35), CSUSHPINSA (0.35), MORTGAGE30US inverted (0.30).
        """
        weights = {
            "HOUST": 0.35,
            "CSUSHPINSA": 0.35,
            "MORTGAGE30US": -0.30,   # higher rates = lower score
        }
        score = 0.0
        total_weight = 0.0
        for series_id, weight in weights.items():
            vals = self._load_series(series_id)
            z = _zscore(vals)
            if z is not None:
                score += weight * z
                total_weight += abs(weight)

        if total_weight == 0:
            return 0.0
        normalized = _safe_clamp(score / 3.0)
        return round(normalized, 4)

    def get_inflation_pressure(self) -> float:
        """
        Returns float where positive = elevated inflation pressure.
        Computed as z-score of CPI vs PPI spread.
        """
        cpi_vals = self._load_series("CPIAUCSL")
        ppi_vals = self._load_series("PPIACO")

        if not cpi_vals or not ppi_vals:
            return 0.0

        # Align by length (take the last N matching points)
        n = min(len(cpi_vals), len(ppi_vals))
        cpi_recent = cpi_vals[-n:]
        ppi_recent = ppi_vals[-n:]
        spreads = [c - p for c, p in zip(cpi_recent, ppi_recent)]
        z = _zscore(spreads)
        return round(z if z is not None else 0.0, 4)

    def get_latest_values(self) -> Dict[str, Optional[float]]:
        """Returns dict of series_id -> latest value for all configured series."""
        return {sid: self._latest_cache.get(sid) for sid in FRED_SERIES}

    def get_trend(self, series_id: str, periods: int = 4) -> str:
        """
        Returns trend direction for a FRED series: 'UP', 'DOWN', 'FLAT',
        or 'INSUFFICIENT_DATA' / 'SERIES_NOT_FOUND'.
        """
        vals = self._load_series(series_id)
        if not vals:
            return "SERIES_NOT_FOUND"
        return _trend(vals, periods=periods)

    def get_payment_composite(self) -> Optional[float]:
        """
        Load latest PaymentProcessorComposite from DB.
        Returns None if not yet collected.
        """
        try:
            conn = _get_hist_conn()
            row = conn.execute(
                "SELECT composite FROM payment_processor_signals "
                "WHERE composite IS NOT NULL ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            return row[0] if row else None
        except Exception as exc:
            logger.error("Could not load payment composite: %s", exc)
            return None


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint

    intelligence = ConsumerIntelligence()

    print("\n" + "=" * 70)
    print("STEP 6 — CONSUMER INTELLIGENCE")
    print("=" * 70)

    summary = intelligence.collect()
    print("\n--- Collection Summary ---")
    pprint.pprint({k: v for k, v in summary.items() if k != "errors"})

    if summary.get("errors"):
        print(f"\n--- {len(summary['errors'])} Errors ---")
        for e in summary["errors"][:5]:
            print(f"  {e}")

    print("\n--- Composite Indices ---")
    print(f"  ConsumerHealthIndex   : {intelligence.get_consumer_health_index():+.4f}  "
          f"(range -1 to +1)")
    print(f"  HousingHealthIndex    : {intelligence.get_housing_health_index():+.4f}  "
          f"(range -1 to +1)")
    print(f"  InflationPressure     : {intelligence.get_inflation_pressure():+.4f}  "
          f"(positive = elevated)")

    print("\n--- Key Latest Values ---")
    latest = intelligence.get_latest_values()
    for sid in ["UMCSENT", "UNRATE", "RSXFS", "PSAVERT", "CPIAUCSL", "MORTGAGE30US"]:
        val = latest.get(sid)
        trend = intelligence.get_trend(sid)
        print(f"  {sid:<25} {val!r:>12}   trend: {trend}")

    print("\n--- Trends (4 periods) ---")
    for sid in _CONSUMER_HEALTH_SERIES:
        print(f"  {sid}: {intelligence.get_trend(sid, periods=4)}")

    pay = intelligence.get_payment_composite()
    print(f"\n  PaymentProcessorComposite (30d): {pay!r}")

    print(f"\nPermanent store  : {_PERM_DB}")
    print(f"Historical store : {_HIST_DB}")
    print("=" * 70)
