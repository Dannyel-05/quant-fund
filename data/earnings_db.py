"""
SQLite storage layer for earnings observations.

Tables:
  earnings_observations      — one row per ticker × earnings event, with
                               EPS fields, price captures, altdata signals,
                               and market context.
  earnings_calendar_forward  — upcoming earnings events to monitor.
"""
import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# Thread-local connections (WAL mode allows concurrent readers + one writer)
_local = threading.local()

_DDL_OBSERVATIONS = """
CREATE TABLE IF NOT EXISTS earnings_observations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT    NOT NULL,
    earnings_date       TEXT    NOT NULL,   -- ISO-8601 date YYYY-MM-DD
    market              TEXT    NOT NULL DEFAULT 'us',

    -- EPS fields
    eps_actual          REAL,
    eps_estimate        REAL,
    eps_difference      REAL,
    surprise_pct        REAL,               -- (actual-estimate)/|estimate|
    surprise_percent_yf REAL,               -- raw surprisePercent from yfinance
    data_quality        TEXT,               -- 'high' | 'low' | 'missing'

    -- Price captures
    price_t0            REAL,               -- close on earnings_date
    price_t1            REAL,               -- close 1 trading day after
    price_t3            REAL,               -- close 3 trading days after
    price_t5            REAL,               -- close 5 trading days after
    price_t10           REAL,               -- close 10 trading days after
    price_t20           REAL,               -- close 20 trading days after

    -- Derived returns
    return_t1           REAL,               -- (price_t1 / price_t0) - 1
    return_t3           REAL,
    return_t5           REAL,
    return_t10          REAL,
    return_t20          REAL,

    -- Volume context
    volume_t0           REAL,               -- volume on earnings_date
    volume_avg_20d      REAL,               -- 20-day avg volume before event
    volume_surge        REAL,               -- volume_t0 / volume_avg_20d

    -- Market context
    vix_t0              REAL,               -- VIX close on earnings_date
    spy_return_5d       REAL,               -- SPY return in 5 days before event
    sector_etf_return_5d REAL,              -- sector ETF return 5 days before
    sector_etf_ticker   TEXT,               -- e.g. XLK, XLV

    -- Alt-data signals (from altdata module, -1 to +1)
    altdata_sentiment        REAL,          -- combined sentiment score
    reddit_score             REAL,
    news_score               REAL,
    sec_score                REAL,          -- SEC 8-K filing tone
    beat_quality_multiplier  REAL,          -- deepdata BeatQualityClassifier final_pead_multiplier

    -- Metadata
    collected_at        TEXT NOT NULL,      -- ISO-8601 datetime of collection
    source              TEXT DEFAULT 'yfinance',

    UNIQUE (ticker, earnings_date)
)
"""

_DDL_CALENDAR = """
CREATE TABLE IF NOT EXISTS earnings_calendar_forward (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    earnings_date   TEXT NOT NULL,          -- expected date (may be approximate)
    market          TEXT NOT NULL DEFAULT 'us',
    eps_estimate    REAL,
    added_at        TEXT NOT NULL,
    UNIQUE (ticker, earnings_date)
)
"""

_DDL_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS pre_earnings_snapshots (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT NOT NULL,
    earnings_date           TEXT NOT NULL,
    snapshot_taken_at       TEXT NOT NULL,
    days_before_earnings    INTEGER,

    -- Altdata scores
    altdata_sentiment       REAL,
    reddit_score            REAL,
    news_score              REAL,
    sec_score               REAL,

    -- Deepdata scores
    options_smfi            REAL,
    options_iv_rank         REAL,
    options_put_call        REAL,
    options_dark_pool       REAL,
    short_squeeze_score     REAL,
    congressional_signal    REAL,
    beat_quality_multiplier REAL,

    -- Macro context
    vix                     REAL,
    macro_regime            INTEGER,
    macro_regime_name       TEXT,
    spy_return_5d           REAL,
    sector_etf_return_5d    REAL,
    sector_etf              TEXT,

    -- Price context
    price_at_snapshot       REAL,
    volume_surge            REAL,

    -- Post-earnings outcome (filled in after earnings)
    outcome_return_t1       REAL,
    outcome_return_t3       REAL,
    outcome_return_t5       REAL,
    outcome_return_t20      REAL,
    outcome_eps_surprise    REAL,
    outcome_captured_at     TEXT,

    UNIQUE (ticker, earnings_date)
)
"""

_DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_obs_ticker     ON earnings_observations (ticker)",
    "CREATE INDEX IF NOT EXISTS idx_obs_date       ON earnings_observations (earnings_date)",
    "CREATE INDEX IF NOT EXISTS idx_obs_quality    ON earnings_observations (data_quality)",
    "CREATE INDEX IF NOT EXISTS idx_cal_date       ON earnings_calendar_forward (earnings_date)",
    "CREATE INDEX IF NOT EXISTS idx_snap_ticker    ON pre_earnings_snapshots (ticker)",
    "CREATE INDEX IF NOT EXISTS idx_snap_date      ON pre_earnings_snapshots (earnings_date)",
]


class EarningsDB:
    def __init__(self, db_path: str = "output/earnings.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if not getattr(_local, "conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            _local.conn = conn
        return _local.conn

    @contextmanager
    def _cursor(self):
        conn = self._connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._cursor() as cur:
            cur.execute(_DDL_OBSERVATIONS)
            cur.execute(_DDL_CALENDAR)
            cur.execute(_DDL_SNAPSHOTS)
            for idx_sql in _DDL_INDEXES:
                cur.execute(idx_sql)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Add any columns introduced after initial schema creation."""
        migrations = [
            ("earnings_observations", "beat_quality_multiplier",    "REAL"),
            ("earnings_observations", "insider_signal_score_90d",   "INTEGER"),
            ("earnings_observations", "insider_cluster_buy_score",  "REAL"),
            ("earnings_observations", "insider_cluster_sell_score", "REAL"),
            ("earnings_observations", "dip_buy_cluster_flag",       "INTEGER"),
            ("earnings_observations", "post_earnings_dip_buy_flag", "INTEGER"),
            ("earnings_observations", "n_insiders_bought_90d",      "INTEGER"),
            ("earnings_observations", "n_insiders_sold_90d",        "INTEGER"),
            ("earnings_observations", "ceo_bought_90d",             "INTEGER"),
            ("earnings_observations", "cfo_bought_90d",             "INTEGER"),
        ]
        conn = self._connect()
        for table, column, col_type in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                conn.commit()
                logger.debug("Migration: added %s.%s", table, column)
            except sqlite3.OperationalError:
                pass  # column already exists

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    def upsert_observation(self, record: dict) -> None:
        """Insert or replace an earnings observation."""
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        update_str = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ("ticker", "earnings_date"))
        sql = f"""
            INSERT INTO earnings_observations ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(ticker, earnings_date) DO UPDATE SET {update_str}
        """
        with self._cursor() as cur:
            cur.execute(sql, list(record.values()))

    def upsert_observations_batch(self, records: List[dict]) -> int:
        """Bulk upsert; returns number of rows written."""
        if not records:
            return 0
        written = 0
        for record in records:
            try:
                self.upsert_observation(record)
                written += 1
            except Exception as e:
                logger.warning("Failed to upsert %s @ %s: %s",
                               record.get("ticker"), record.get("earnings_date"), e)
        return written

    def get_observations(
        self,
        ticker: Optional[str] = None,
        since: Optional[str] = None,
        quality: Optional[str] = None,
        limit: int = 1000,
    ) -> List[dict]:
        clauses, params = [], []
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        if since:
            clauses.append("earnings_date >= ?")
            params.append(since)
        if quality:
            clauses.append("data_quality = ?")
            params.append(quality)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM earnings_observations {where} ORDER BY earnings_date DESC LIMIT ?"
        params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def update_insider_fields(
        self, ticker: str, earnings_date: str, insider_fields: dict
    ) -> None:
        """Update insider analysis fields for an earnings observation."""
        if not insider_fields:
            return
        set_clause = ", ".join(f"{k}=?" for k in insider_fields)
        vals = list(insider_fields.values()) + [ticker, earnings_date]
        with self._cursor() as cur:
            cur.execute(
                f"UPDATE earnings_observations SET {set_clause} WHERE ticker=? AND earnings_date=?",
                vals,
            )

    def update_altdata_scores(
        self,
        ticker: str,
        altdata_sentiment: Optional[float] = None,
        reddit_score: Optional[float] = None,
        news_score: Optional[float] = None,
        sec_score: Optional[float] = None,
        beat_quality_multiplier: Optional[float] = None,
    ) -> int:
        """
        Update altdata / deepdata scores for all calendar entries or observations
        matching the ticker.  Returns number of rows updated.
        """
        fields, params = [], []
        if altdata_sentiment is not None:
            fields.append("altdata_sentiment = ?")
            params.append(altdata_sentiment)
        if reddit_score is not None:
            fields.append("reddit_score = ?")
            params.append(reddit_score)
        if news_score is not None:
            fields.append("news_score = ?")
            params.append(news_score)
        if sec_score is not None:
            fields.append("sec_score = ?")
            params.append(sec_score)
        if beat_quality_multiplier is not None:
            fields.append("beat_quality_multiplier = ?")
            params.append(beat_quality_multiplier)

        if not fields:
            return 0

        params.append(ticker)
        sql = f"UPDATE earnings_observations SET {', '.join(fields)} WHERE ticker = ?"
        conn = self._connect()
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Pre-earnings snapshots
    # ------------------------------------------------------------------

    def upsert_snapshot(self, record: dict) -> None:
        """Insert or replace a pre-earnings snapshot."""
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        update_str = ", ".join(
            f"{c}=excluded.{c}" for c in cols if c not in ("ticker", "earnings_date")
        )
        sql = f"""
            INSERT INTO pre_earnings_snapshots ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(ticker, earnings_date) DO UPDATE SET {update_str}
        """
        with self._cursor() as cur:
            cur.execute(sql, list(record.values()))

    def get_snapshot(self, ticker: str, earnings_date: Optional[str] = None) -> Optional[dict]:
        """Return most recent snapshot for ticker (optionally filtered by earnings_date)."""
        if earnings_date:
            sql = "SELECT * FROM pre_earnings_snapshots WHERE ticker=? AND earnings_date=? ORDER BY snapshot_taken_at DESC LIMIT 1"
            params = [ticker, earnings_date]
        else:
            sql = "SELECT * FROM pre_earnings_snapshots WHERE ticker=? ORDER BY snapshot_taken_at DESC LIMIT 1"
            params = [ticker]
        with self._cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def get_all_snapshots(self, days_ahead: int = 60) -> List[dict]:
        """Return all snapshots with earnings in the next N days."""
        sql = """
            SELECT * FROM pre_earnings_snapshots
            WHERE earnings_date >= date('now')
              AND earnings_date <= date('now', ?)
            ORDER BY earnings_date, ticker
        """
        with self._cursor() as cur:
            cur.execute(sql, [f"+{days_ahead} days"])
            return [dict(row) for row in cur.fetchall()]

    def update_snapshot_outcome(
        self,
        ticker: str,
        earnings_date: str,
        return_t1: Optional[float] = None,
        return_t3: Optional[float] = None,
        return_t5: Optional[float] = None,
        return_t20: Optional[float] = None,
        eps_surprise: Optional[float] = None,
    ) -> int:
        """Fill in post-earnings outcome fields on an existing snapshot."""
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") if True else None
        # import here to avoid circular
        from datetime import datetime as _dt
        now_str = _dt.now().strftime("%Y-%m-%dT%H:%M:%S")

        fields, params = ["outcome_captured_at = ?"], [now_str]
        for col, val in [
            ("outcome_return_t1",    return_t1),
            ("outcome_return_t3",    return_t3),
            ("outcome_return_t5",    return_t5),
            ("outcome_return_t20",   return_t20),
            ("outcome_eps_surprise", eps_surprise),
        ]:
            if val is not None:
                fields.append(f"{col} = ?")
                params.append(val)

        params += [ticker, earnings_date]
        sql = f"UPDATE pre_earnings_snapshots SET {', '.join(fields)} WHERE ticker=? AND earnings_date=?"
        conn = self._connect()
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Calendar
    # ------------------------------------------------------------------

    def upsert_calendar(self, record: dict) -> None:
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        update_str = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ("ticker", "earnings_date"))
        sql = f"""
            INSERT INTO earnings_calendar_forward ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(ticker, earnings_date) DO UPDATE SET {update_str}
        """
        with self._cursor() as cur:
            cur.execute(sql, list(record.values()))

    def get_upcoming_calendar(self, days_ahead: int = 14) -> List[dict]:
        sql = """
            SELECT * FROM earnings_calendar_forward
            WHERE earnings_date >= date('now')
              AND earnings_date <= date('now', ?)
            ORDER BY earnings_date, ticker
        """
        with self._cursor() as cur:
            cur.execute(sql, [f"+{days_ahead} days"])
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Status / summary
    # ------------------------------------------------------------------

    def status(self) -> dict:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM earnings_observations")
            total = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM earnings_observations WHERE data_quality = 'high'"
            )
            high = cur.fetchone()[0]

            cur.execute(
                "SELECT MIN(earnings_date), MAX(earnings_date) FROM earnings_observations"
            )
            row = cur.fetchone()
            date_min, date_max = row[0], row[1]

            cur.execute("SELECT COUNT(DISTINCT ticker) FROM earnings_observations")
            n_tickers = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM earnings_observations WHERE return_t20 IS NOT NULL"
            )
            with_returns = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM earnings_calendar_forward")
            cal_rows = cur.fetchone()[0]

        return {
            "total_observations": total,
            "high_quality":       high,
            "tickers":            n_tickers,
            "date_range":         f"{date_min} → {date_max}",
            "with_returns":       with_returns,
            "calendar_entries":   cal_rows,
            "db_path":            str(self.db_path),
        }
