"""
STEP 7 — Commodity Collector

Collects comprehensive commodity price history and generates sector impact signals.

Sources:
  - yfinance: futures (CL=F, GC=F, etc.) and commodity ETFs (XLE, XME, etc.)
  - All history fetched back to 1983 where available

Stores OHLCV data permanently in SQLite and computes:
  - Moving averages (20, 50, 200-day)
  - Z-scores vs 252-day window
  - Rate of change (1w, 4w, 12w)
  - 52-week high/low position
  - Cross-commodity correlations
  - Sector impact modifiers
  - Lead-lag analysis (copper/oil vs sector earnings)
"""

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


# ── Commodity universe definitions ────────────────────────────────────────────

ENERGY_FUTURES = {
    "CL=F": "WTI Crude Oil",
    "BZ=F": "Brent Crude",
    "NG=F": "Natural Gas",
    "HO=F": "Heating Oil",
    "RB=F": "RBOB Gasoline",
}

METAL_FUTURES = {
    "HG=F": "Copper",
    "GC=F": "Gold",
    "SI=F": "Silver",
    "PL=F": "Platinum",
    "PA=F": "Palladium",
}

AGRI_FUTURES = {
    "ZW=F": "Wheat",
    "ZC=F": "Corn",
    "ZS=F": "Soybeans",
    "KC=F": "Coffee",
    "CT=F": "Cotton",
    "SB=F": "Sugar",
    "CC=F": "Cocoa",
}

COMMODITY_ETFS = {
    "XLE": "Energy Select Sector SPDR",
    "XME": "SPDR S&P Metals & Mining",
    "LIT": "Global X Lithium & Battery Tech",
    "COPX": "Global X Copper Miners",
    "REMX": "VanEck Rare Earth/Strategic Metals",
    "URA": "Global X Uranium",
    "SLX": "VanEck Steel",
    "MOO": "VanEck Agribusiness",
    "WEAT": "Teucrium Wheat Fund",
    "CORN": "Teucrium Corn Fund",
    "SOYB": "Teucrium Soybean Fund",
    "DBA": "Invesco DB Agriculture Fund",
}

ALL_SYMBOLS: Dict[str, str] = {
    **ENERGY_FUTURES,
    **METAL_FUTURES,
    **AGRI_FUTURES,
    **COMMODITY_ETFS,
}

# Sector classification for grouping
ENERGY_SYMBOLS = list(ENERGY_FUTURES.keys()) + ["XLE"]
METAL_SYMBOLS = list(METAL_FUTURES.keys()) + ["XME", "LIT", "COPX", "REMX", "URA", "SLX"]
AGRI_SYMBOLS = list(AGRI_FUTURES.keys()) + ["MOO", "WEAT", "CORN", "SOYB", "DBA"]

# ── Commodity sector impact rules ─────────────────────────────────────────────

# (symbol, threshold_1m_pct, sector_impacts dict)
IMPACT_RULES: List[Tuple[str, float, Dict[str, float]]] = [
    (
        "CL=F",
        0.15,  # Oil > 15% in 1 month
        {
            "airlines": -0.40,
            "trucking": -0.30,
            "plastics": -0.20,
            "energy_producers": +0.40,
        },
    ),
    (
        "HG=F",
        0.10,  # Copper > 10% in 1 month
        {
            "industrials": +0.20,
            "electronics": -0.10,
            "mining": +0.30,
        },
    ),
    (
        "ZW=F",
        0.20,  # Wheat > 20% in 1 month
        {
            "food_manufacturers": -0.30,
            "restaurants": -0.20,
            "agriculture": +0.20,
        },
    ),
]

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


def _init_dbs(perm_conn: sqlite3.Connection, hist_conn: sqlite3.Connection) -> None:
    perm_conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_commodity_prices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            name        TEXT,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            adj_close   REAL,
            volume      REAL,
            source      TEXT DEFAULT 'yfinance',
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(symbol, date)
        );

        CREATE TABLE IF NOT EXISTS commodity_lead_lag (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity       TEXT NOT NULL,
            sector          TEXT NOT NULL,
            lag_weeks       INTEGER NOT NULL,
            correlation     REAL,
            p_value         REAL,
            significant     INTEGER DEFAULT 0,
            sample_size     INTEGER,
            computed_at     TEXT DEFAULT (datetime('now')),
            UNIQUE(commodity, sector, lag_weeks)
        );
    """)
    perm_conn.commit()

    hist_conn.executescript("""
        CREATE TABLE IF NOT EXISTS commodity_prices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            name        TEXT,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            adj_close   REAL,
            volume      REAL,
            source      TEXT DEFAULT 'yfinance',
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(symbol, date)
        );
    """)
    hist_conn.commit()


# ── Statistical helpers ───────────────────────────────────────────────────────

def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _std(vals: List[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = _mean(vals)
    variance = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
    return variance ** 0.5


def _zscore_last(vals: List[float], window: int = 252) -> Optional[float]:
    if len(vals) < 2:
        return None
    window_vals = vals[-window:] if len(vals) >= window else vals
    s = _std(window_vals)
    if s == 0:
        return 0.0
    return (window_vals[-1] - _mean(window_vals)) / s


def _rate_of_change(vals: List[float], periods: int) -> Optional[float]:
    if len(vals) <= periods:
        return None
    prev = vals[-(periods + 1)]
    if prev == 0:
        return None
    return (vals[-1] - prev) / prev


def _rolling_ma(vals: List[float], window: int) -> Optional[float]:
    if len(vals) < window:
        return None
    return _mean(vals[-window:])


def _pct_position_52w(vals: List[float]) -> Optional[float]:
    """Where is the latest close within its 52-week (252-day) range?"""
    window = vals[-252:] if len(vals) >= 252 else vals
    if not window:
        return None
    lo, hi = min(window), max(window)
    if hi == lo:
        return 0.5
    return (window[-1] - lo) / (hi - lo)


def _pearson_correlation(x: List[float], y: List[float]) -> Tuple[float, int]:
    """Returns (pearson_r, sample_size). No scipy dependency."""
    n = min(len(x), len(y))
    if n < 3:
        return (0.0, n)
    x = x[-n:]
    y = y[-n:]
    mx, my = _mean(x), _mean(y)
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    den = (_std(x) * _std(y) * (n - 1))
    if den == 0:
        return (0.0, n)
    return (num / den, n)


def _approximate_p_value(r: float, n: int) -> float:
    """Very rough approximation of two-tailed p-value for Pearson r."""
    import math
    if n <= 2 or abs(r) >= 1.0:
        return 1.0
    t = r * math.sqrt((n - 2) / (1 - r ** 2 + 1e-12))
    # Rough lookup table for p-value
    abs_t = abs(t)
    if abs_t > 3.5:
        return 0.001
    if abs_t > 2.6:
        return 0.01
    if abs_t > 2.0:
        return 0.05
    if abs_t > 1.6:
        return 0.10
    return 0.50


# ── Price fetching ────────────────────────────────────────────────────────────

def _fetch_and_store_symbol(
    symbol: str,
    name: str,
    perm_conn: sqlite3.Connection,
    hist_conn: sqlite3.Connection,
) -> List[Dict[str, Any]]:
    """Fetch full OHLCV history for one symbol and store in both DBs."""
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance not installed. Run: pip install yfinance")

    tkr = yf.Ticker(symbol)
    hist = tkr.history(start="1983-01-01", auto_adjust=True)
    if hist.empty:
        logger.warning("No data for %s", symbol)
        return []

    hist = hist.sort_index()
    rows = []
    for dt, row in hist.iterrows():
        date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
        rows.append((
            symbol, name, date_str,
            _safe_float(row.get("Open")),
            _safe_float(row.get("High")),
            _safe_float(row.get("Low")),
            _safe_float(row.get("Close")),
            _safe_float(row.get("Close")),  # adj_close = Close since auto_adjust=True
            _safe_float(row.get("Volume")),
        ))

    # raw_commodity_prices uses canonical schema: commodity, symbol, date, open, high, low, close, volume, source, fetched_at
    from datetime import datetime as _dt
    fetched_at = _dt.utcnow().isoformat()
    perm_rows = [
        (name, symbol, r[2], r[3], r[4], r[5], r[6], r[8], 'yfinance', fetched_at)
        for r in rows
    ]
    perm_conn.executemany("""
        INSERT OR IGNORE INTO raw_commodity_prices
        (commodity, symbol, date, open, high, low, close, volume, source, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, perm_rows)
    perm_conn.commit()

    insert_sql = """
        INSERT OR IGNORE INTO {table}
        (symbol, name, date, open, high, low, close, adj_close, volume)
        VALUES (?,?,?,?,?,?,?,?,?)
    """

    hist_conn.executemany(insert_sql.format(table="commodity_prices"), rows)  # hist table keeps name column
    hist_conn.commit()

    logger.info("  %s: stored %d rows", symbol, len(rows))
    return [
        {
            "date": r[2],
            "open": r[3], "high": r[4], "low": r[5],
            "close": r[6], "adj_close": r[7], "volume": r[8],
        }
        for r in rows
    ]


def _safe_float(val: Any) -> Optional[float]:
    try:
        f = float(val)
        import math
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


# ── Indicators computation ────────────────────────────────────────────────────

def _compute_indicators(closes: List[float]) -> Dict[str, Any]:
    """
    Compute all technical / statistical indicators for a price series.
    Returns a flat dict.
    """
    return {
        "ma_20": _rolling_ma(closes, 20),
        "ma_50": _rolling_ma(closes, 50),
        "ma_200": _rolling_ma(closes, 200),
        "zscore_252d": _zscore_last(closes, 252),
        "pct_52w_position": _pct_position_52w(closes),
        "roc_1w": _rate_of_change(closes, 5),
        "roc_4w": _rate_of_change(closes, 21),
        "roc_12w": _rate_of_change(closes, 63),
        "latest_close": closes[-1] if closes else None,
    }


# ── Correlation helpers ───────────────────────────────────────────────────────

def _align_series(
    a: List[Dict[str, Any]], b: List[Dict[str, Any]]
) -> Tuple[List[float], List[float]]:
    """
    Align two price series by date, return (closes_a, closes_b).
    """
    dates_a = {r["date"]: r["close"] for r in a if r.get("close") is not None}
    dates_b = {r["date"]: r["close"] for r in b if r.get("close") is not None}
    common = sorted(set(dates_a) & set(dates_b))
    xa = [dates_a[d] for d in common]
    xb = [dates_b[d] for d in common]
    return xa, xb


# ── Lead-lag analysis ─────────────────────────────────────────────────────────

_LEAD_LAG_PAIRS = [
    ("HG=F", "industrials"),     # copper -> industrials
    ("CL=F", "energy"),          # oil -> energy
]

_LAG_WEEKS = [2, 4, 6, 8, 12]


def _run_lead_lag(
    closes_commodity: List[float],
    closes_sector: List[float],
    commodity: str,
    sector: str,
    perm_conn: sqlite3.Connection,
) -> List[Dict[str, Any]]:
    """
    Test predictive power of commodity at multiple lag weeks.
    Stores significant relationships in commodity_lead_lag table.
    Returns list of result dicts.
    """
    results = []
    for lag_weeks in _LAG_WEEKS:
        lag_days = lag_weeks * 5  # trading days
        if len(closes_commodity) <= lag_days or len(closes_sector) <= lag_days:
            continue

        # Shift: commodity leads sector by lag_days
        x = closes_commodity[:-lag_days]
        y = closes_sector[lag_days:]
        n = min(len(x), len(y))
        x, y = x[-n:], y[-n:]

        # Use returns instead of raw prices to avoid spurious correlation
        x_ret = [x[i] / x[i - 1] - 1 for i in range(1, len(x))]
        y_ret = [y[i] / y[i - 1] - 1 for i in range(1, len(y))]
        n = min(len(x_ret), len(y_ret))
        x_ret, y_ret = x_ret[-n:], y_ret[-n:]

        r, sample = _pearson_correlation(x_ret, y_ret)
        p = _approximate_p_value(r, sample)
        significant = 1 if p < 0.05 else 0

        try:
            perm_conn.execute(
                """INSERT OR REPLACE INTO commodity_lead_lag
                   (commodity, sector, lag_weeks, correlation, p_value, significant, sample_size)
                   VALUES (?,?,?,?,?,?,?)""",
                (commodity, sector, lag_weeks, round(r, 4), round(p, 4), significant, sample),
            )
        except Exception as exc:
            logger.error("lead_lag insert error: %s", exc)

        results.append({
            "commodity": commodity,
            "sector": sector,
            "lag_weeks": lag_weeks,
            "correlation": round(r, 4),
            "p_value": round(p, 4),
            "significant": bool(significant),
            "sample_size": sample,
        })

    perm_conn.commit()
    return results


# ── Main class ────────────────────────────────────────────────────────────────

class CommodityCollector:
    """
    Comprehensive commodity data collection, storage, and signal generation.
    """

    def __init__(self):
        self._config = _load_config()
        # In-memory cache: symbol -> list of price dicts (ascending date)
        self._price_cache: Dict[str, List[Dict[str, Any]]] = {}
        # Computed indicators cache: symbol -> dict
        self._indicators_cache: Dict[str, Dict[str, Any]] = {}
        # lead-lag results
        self._lead_lag_results: List[Dict[str, Any]] = []

    # ── Internal helpers ───────────────────────────────────────────────────

    def _closes(self, symbol: str) -> List[float]:
        """Return list of close prices (ascending date) for a symbol."""
        data = self._price_cache.get(symbol, [])
        return [r["close"] for r in data if r.get("close") is not None]

    def _load_cache_from_db(self, conn: sqlite3.Connection) -> None:
        """Populate in-memory price cache from permanent DB."""
        for symbol in ALL_SYMBOLS:
            rows = conn.execute(
                "SELECT date, open, high, low, close, adj_close, volume "
                "FROM raw_commodity_prices WHERE symbol=? ORDER BY date ASC",
                (symbol,),
            ).fetchall()
            self._price_cache[symbol] = [
                {
                    "date": r[0], "open": r[1], "high": r[2],
                    "low": r[3], "close": r[4], "adj_close": r[5], "volume": r[6],
                }
                for r in rows
            ]

        # Pre-compute indicators
        for symbol in ALL_SYMBOLS:
            closes = self._closes(symbol)
            if closes:
                self._indicators_cache[symbol] = _compute_indicators(closes)

    # ── Public API ─────────────────────────────────────────────────────────

    def collect(self) -> Dict[str, Any]:
        """
        Fetch all commodity OHLCV history, store permanently, compute indicators
        and lead-lag analysis. Returns summary dict.
        """
        perm_conn = _get_perm_conn()
        hist_conn = _get_hist_conn()
        summary: Dict[str, Any] = {
            "symbols_collected": 0,
            "symbols_failed": 0,
            "errors": [],
        }

        try:
            _init_dbs(perm_conn, hist_conn)

            # Fetch all symbols
            for symbol, name in ALL_SYMBOLS.items():
                try:
                    logger.info("Fetching %s — %s", symbol, name)
                    rows = _fetch_and_store_symbol(symbol, name, perm_conn, hist_conn)
                    self._price_cache[symbol] = rows
                    summary["symbols_collected"] += 1
                except Exception as exc:
                    logger.error("  %s failed: %s", symbol, exc)
                    summary["symbols_failed"] += 1
                    summary["errors"].append(f"{symbol}: {exc}")

            # Pre-compute indicators for all cached symbols
            for symbol in ALL_SYMBOLS:
                closes = self._closes(symbol)
                if closes:
                    self._indicators_cache[symbol] = _compute_indicators(closes)

            # Lead-lag analysis
            logger.info("=== Running lead-lag analysis ===")
            for commodity, sector in _LEAD_LAG_PAIRS:
                comm_closes = self._closes(commodity)
                # For sector, use XLE (energy) or XME (industrials proxy)
                sector_proxy = "XLE" if sector == "energy" else "XME"
                sect_closes = self._closes(sector_proxy)
                if comm_closes and sect_closes:
                    results = _run_lead_lag(
                        comm_closes, sect_closes, commodity, sector, perm_conn
                    )
                    self._lead_lag_results.extend(results)
                    sig = [r for r in results if r["significant"]]
                    logger.info(
                        "  %s -> %s: %d lags tested, %d significant",
                        commodity, sector, len(results), len(sig),
                    )

            # Rolling 90-day correlations to benchmark ETFs
            logger.info("=== Computing 90-day correlations ===")
            summary["correlations"] = self._compute_90d_correlations()

            summary["sector_impacts"] = self.get_sector_impacts()
            summary["energy_composite"] = self.get_energy_composite()
            summary["metals_composite"] = self.get_metals_composite()

        finally:
            perm_conn.close()
            hist_conn.close()

        logger.info(
            "Commodity Collector: %d symbols collected, %d failed",
            summary["symbols_collected"],
            summary["symbols_failed"],
        )
        return summary

    def _compute_90d_correlations(self) -> Dict[str, Dict[str, float]]:
        """
        Compute 90-day rolling correlation of each symbol to XLE, XME, MOO.
        Returns dict: symbol -> {XLE: r, XME: r, MOO: r}
        """
        benchmarks = {"XLE": "energy", "XME": "metals", "MOO": "agriculture"}
        correlations: Dict[str, Dict[str, float]] = {}

        for symbol in ALL_SYMBOLS:
            if symbol in benchmarks:
                continue
            s_closes = self._closes(symbol)
            if len(s_closes) < 90:
                continue
            correlations[symbol] = {}
            s90 = s_closes[-90:]
            for bench, _ in benchmarks.items():
                b_closes = self._closes(bench)
                if len(b_closes) < 90:
                    continue
                b90 = b_closes[-90:]
                n = min(len(s90), len(b90))
                r, _ = _pearson_correlation(s90[-n:], b90[-n:])
                correlations[symbol][bench] = round(r, 4)

        return correlations

    def get_commodity_data(self, symbol: str, days: int = 252) -> Optional[Any]:
        """
        Returns a DataFrame-like structure (list of dicts) with price data
        and computed indicators for the requested symbol over last N days.

        Returns None if data not available. Import pandas if available;
        otherwise returns a list of dicts.
        """
        data = self._price_cache.get(symbol, [])
        if not data:
            logger.warning("No data for symbol %s. Run collect() first.", symbol)
            return None

        recent = data[-days:]
        closes = [r["close"] for r in recent if r.get("close") is not None]
        indicators = _compute_indicators(closes)

        # Attach indicators to each row
        result = []
        for i, row in enumerate(recent):
            row_copy = dict(row)
            row_copy.update({
                "ma_20": _rolling_ma(closes[: i + 1], 20),
                "ma_50": _rolling_ma(closes[: i + 1], 50),
                "ma_200": _rolling_ma(closes[: i + 1], 200),
            })
            result.append(row_copy)

        # Append summary indicators
        result_with_meta = {
            "symbol": symbol,
            "name": ALL_SYMBOLS.get(symbol, symbol),
            "rows": result,
            "indicators": indicators,
        }

        try:
            import pandas as pd
            df = pd.DataFrame(result)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date").sort_index()
            return df
        except ImportError:
            return result_with_meta

    def get_sector_impacts(self) -> Dict[str, float]:
        """
        Returns dict of sector -> signal_modifier based on current commodity moves.
        Applies IMPACT_RULES using 1-month rate of change.
        """
        impacts: Dict[str, float] = {}

        for symbol, threshold, sector_map in IMPACT_RULES:
            closes = self._closes(symbol)
            roc_1m = _rate_of_change(closes, 21)
            if roc_1m is None:
                continue

            if abs(roc_1m) >= threshold:
                sign = 1 if roc_1m > 0 else -1
                # Scale the impact proportionally to how much threshold was breached
                scale = min(abs(roc_1m) / threshold, 2.0)  # cap at 2x
                for sector, modifier in sector_map.items():
                    effective = modifier * sign * scale
                    current = impacts.get(sector, 0.0)
                    impacts[sector] = round(current + effective, 4)

        # Convert to multipliers: 0 impact -> 1.0x, -0.4 impact -> 0.6x
        return {sector: round(1.0 + impact, 4) for sector, impact in impacts.items()}

    def get_commodity_signals(self) -> List[Dict[str, Any]]:
        """
        Returns list of signal dicts for all commodities.
        Each dict: {symbol, name, signal_direction, z_score, 1m_change, latest_close}
        """
        signals = []
        for symbol, name in ALL_SYMBOLS.items():
            closes = self._closes(symbol)
            if not closes:
                continue

            z = _zscore_last(closes, 252)
            roc_1m = _rate_of_change(closes, 21)

            if z is None:
                continue

            if z > 1.5:
                direction = "BULLISH"
            elif z < -1.5:
                direction = "BEARISH"
            else:
                direction = "NEUTRAL"

            signals.append({
                "symbol": symbol,
                "name": name,
                "signal_direction": direction,
                "z_score": round(z, 4),
                "1m_change": round(roc_1m, 4) if roc_1m is not None else None,
                "latest_close": closes[-1],
            })

        return sorted(signals, key=lambda x: abs(x["z_score"]), reverse=True)

    def get_energy_composite(self) -> float:
        """
        Returns equal-weight z-score composite of energy commodities (futures only).
        """
        z_scores = []
        for symbol in ENERGY_FUTURES:
            closes = self._closes(symbol)
            z = _zscore_last(closes, 252)
            if z is not None:
                z_scores.append(z)
        if not z_scores:
            return 0.0
        return round(sum(z_scores) / len(z_scores), 4)

    def get_metals_composite(self) -> float:
        """
        Returns equal-weight z-score composite of metal commodities (futures only).
        """
        z_scores = []
        for symbol in METAL_FUTURES:
            closes = self._closes(symbol)
            z = _zscore_last(closes, 252)
            if z is not None:
                z_scores.append(z)
        if not z_scores:
            return 0.0
        return round(sum(z_scores) / len(z_scores), 4)

    def get_lead_lag_results(self) -> List[Dict[str, Any]]:
        """Returns stored lead-lag analysis results."""
        return list(self._lead_lag_results)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint

    collector = CommodityCollector()

    print("\n" + "=" * 70)
    print("STEP 7 — COMMODITY COLLECTOR")
    print("=" * 70)

    summary = collector.collect()

    print("\n--- Collection Summary ---")
    pprint.pprint({
        k: v for k, v in summary.items()
        if k not in ("errors", "correlations")
    })

    if summary.get("errors"):
        print(f"\n--- {len(summary['errors'])} Errors ---")
        for e in summary["errors"][:5]:
            print(f"  {e}")

    print("\n--- Commodity Signals (top 10 by |z-score|) ---")
    signals = collector.get_commodity_signals()
    for s in signals[:10]:
        print(
            f"  {s['symbol']:<8} {s['name']:<35} "
            f"z={s['z_score']:+.2f}  1m={s['1m_change']:+.2%}  {s['signal_direction']}"
            if s["1m_change"] is not None else
            f"  {s['symbol']:<8} {s['name']:<35} z={s['z_score']:+.2f}  {s['signal_direction']}"
        )

    print("\n--- Sector Impacts ---")
    pprint.pprint(collector.get_sector_impacts())

    print(f"\n--- Energy Composite Z-Score : {collector.get_energy_composite():+.4f}")
    print(f"--- Metals Composite Z-Score : {collector.get_metals_composite():+.4f}")

    print("\n--- Lead-Lag Analysis ---")
    for r in collector.get_lead_lag_results():
        sig_flag = "*** SIGNIFICANT ***" if r["significant"] else ""
        print(
            f"  {r['commodity']} -> {r['sector']:<15} "
            f"lag={r['lag_weeks']}w  r={r['correlation']:+.3f}  "
            f"p={r['p_value']:.3f}  n={r['sample_size']}  {sig_flag}"
        )

    print("\n--- Sample: CL=F (WTI Crude) last 5 rows ---")
    crude_data = collector.get_commodity_data("CL=F", days=5)
    if crude_data is not None:
        try:
            print(crude_data.tail(5).to_string())
        except AttributeError:
            rows = crude_data.get("rows", []) if isinstance(crude_data, dict) else crude_data
            for r in rows[-5:]:
                print(f"  {r.get('date')}  close={r.get('close')}")

    print(f"\nPermanent store  : {_PERM_DB}")
    print(f"Historical store : {_HIST_DB}")
    print("=" * 70)
