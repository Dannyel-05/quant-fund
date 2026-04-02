"""
Phase 8: Intelligence Database

SQLite storage for the intelligence layer:
  company_profiles          — enriched per-ticker metadata (sector, size, quality scores)
  signal_effectiveness      — per-signal backtested metrics (accuracy, avg return, Sharpe)
  pattern_discovery         — discovered feature combinations and their predictive power
  cross_asset_correlations  — lead-lag correlations between assets and indicators
  readthrough_coefficients  — large-cap → peer readthrough historical coefficients
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_local = threading.local()

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL_COMPANY_PROFILES = """
CREATE TABLE IF NOT EXISTS company_profiles (
    ticker                  TEXT PRIMARY KEY,
    company_name            TEXT,
    sector                  TEXT,
    sub_sector              TEXT,
    market_cap_category     TEXT,           -- micro/small/mid/large
    market                  TEXT DEFAULT 'us',

    -- Quality scores (0-100, updated periodically)
    earnings_quality_score  REAL,           -- consistency of beats, data quality
    signal_reliability      REAL,           -- historical signal accuracy for this ticker
    altdata_coverage        REAL,           -- % of events with alt-data

    -- Fundamental characteristics
    avg_eps_surprise_pct    REAL,           -- historical average EPS surprise %
    avg_return_t5           REAL,           -- average 5-day return post-earnings
    beat_rate               REAL,           -- % of events where EPS beat estimate
    n_earnings_events       INTEGER,        -- total events in DB

    -- Momentum / drift characteristics
    avg_pead_return         REAL,           -- avg 20-day return after beat
    pead_consistency        REAL,           -- std dev of PEAD returns (lower = more consistent)

    -- Large-cap influence
    primary_large_cap       TEXT,           -- ticker of highest-weight influencer
    readthrough_sensitivity REAL,           -- avg readthrough coefficient from large-caps

    -- Metadata
    last_updated            TEXT,
    data_source             TEXT DEFAULT 'computed'
)
"""

_DDL_SIGNAL_EFFECTIVENESS = """
CREATE TABLE IF NOT EXISTS signal_effectiveness (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name             TEXT NOT NULL,      -- e.g. 'surprise_pct', 'altdata_sentiment', 'volume_surge'
    signal_type             TEXT,               -- 'single' | 'composite' | 'macro' | 'altdata' | 'technical'
    sector                  TEXT,               -- NULL = all sectors
    market_regime           TEXT,               -- NULL = all regimes
    ticker_subset           TEXT,               -- NULL = all tickers; or comma-sep list

    -- Effectiveness metrics
    n_observations          INTEGER,
    accuracy_direction      REAL,               -- % of times signal got direction right
    avg_return_when_bull    REAL,               -- avg t+5 return when signal > threshold
    avg_return_when_bear    REAL,               -- avg t+5 return when signal < -threshold
    sharpe_ratio            REAL,
    win_rate                REAL,               -- % of trades profitable
    avg_win                 REAL,
    avg_loss                REAL,
    max_drawdown            REAL,

    -- Thresholds
    optimal_threshold       REAL,               -- signal value threshold for entry
    signal_decay_days       INTEGER,            -- how many days before signal loses edge

    -- Statistical significance
    p_value                 REAL,
    confidence_level        REAL,               -- 0-1

    computed_at             TEXT NOT NULL,
    UNIQUE (signal_name, sector, market_regime, ticker_subset)
)
"""

_DDL_PATTERN_DISCOVERY = """
CREATE TABLE IF NOT EXISTS pattern_discovery (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_id              TEXT UNIQUE NOT NULL,   -- hash of feature combination
    pattern_name            TEXT,
    pattern_type            TEXT,               -- 'single' | 'combination' | 'regime_conditional' | 'temporal'
    sector                  TEXT,
    market_regime           TEXT,

    -- Feature combination (JSON)
    features_json           TEXT,               -- JSON: {feature: threshold, ...}
    feature_count           INTEGER,

    -- Performance
    n_occurrences           INTEGER,
    avg_return_t5           REAL,
    avg_return_t20          REAL,
    win_rate                REAL,
    sharpe_ratio            REAL,
    max_drawdown            REAL,
    confidence_score        REAL,               -- 0-1 pattern reliability

    -- Temporal characteristics
    best_month              INTEGER,            -- 1-12, month where pattern is strongest
    best_day_of_week        INTEGER,            -- 0=Mon, 4=Fri
    signal_decay_days       INTEGER,

    discovered_at           TEXT NOT NULL,
    last_validated          TEXT
)
"""

_DDL_CROSS_ASSET = """
CREATE TABLE IF NOT EXISTS cross_asset_correlations (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_a                 TEXT NOT NULL,      -- e.g. 'XOM' (large-cap)
    asset_b                 TEXT NOT NULL,      -- e.g. 'METC' (peer)
    relationship_type       TEXT,               -- 'readthrough' | 'sector' | 'macro_lead' | 'commodity_link'

    -- Correlation metrics
    correlation             REAL,               -- Pearson correlation of post-earnings returns
    lead_lag_days           INTEGER,            -- positive = A leads B
    n_events                INTEGER,
    p_value                 REAL,

    -- Context
    sector                  TEXT,
    sub_sector              TEXT,
    computed_at             TEXT NOT NULL,

    UNIQUE (asset_a, asset_b, relationship_type)
)
"""

_DDL_READTHROUGH_COEFFICIENTS = """
CREATE TABLE IF NOT EXISTS readthrough_coefficients (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    large_ticker            TEXT NOT NULL,
    peer_ticker             TEXT NOT NULL,
    coeff                   REAL,               -- 0.1 to 2.0 (1.0 = neutral)
    correlation             REAL,               -- raw Pearson
    n_events                INTEGER,
    start_date              TEXT,
    end_date                TEXT,
    computed_at             TEXT NOT NULL,
    UNIQUE (large_ticker, peer_ticker)
)
"""

_DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_prof_sector    ON company_profiles (sector)",
    "CREATE INDEX IF NOT EXISTS idx_sig_name       ON signal_effectiveness (signal_name)",
    "CREATE INDEX IF NOT EXISTS idx_sig_sector     ON signal_effectiveness (sector)",
    "CREATE INDEX IF NOT EXISTS idx_pat_type       ON pattern_discovery (pattern_type)",
    "CREATE INDEX IF NOT EXISTS idx_pat_sector     ON pattern_discovery (sector)",
    "CREATE INDEX IF NOT EXISTS idx_ca_a           ON cross_asset_correlations (asset_a)",
    "CREATE INDEX IF NOT EXISTS idx_ca_b           ON cross_asset_correlations (asset_b)",
    "CREATE INDEX IF NOT EXISTS idx_rt_large       ON readthrough_coefficients (large_ticker)",
    "CREATE INDEX IF NOT EXISTS idx_rt_peer        ON readthrough_coefficients (peer_ticker)",
]


class IntelligenceDB:
    def __init__(self, db_path: str = "output/intelligence_db.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if not getattr(_local, "intel_conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            _local.intel_conn = conn
        return _local.intel_conn

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
            cur.execute(_DDL_COMPANY_PROFILES)
            cur.execute(_DDL_SIGNAL_EFFECTIVENESS)
            cur.execute(_DDL_PATTERN_DISCOVERY)
            cur.execute(_DDL_CROSS_ASSET)
            cur.execute(_DDL_READTHROUGH_COEFFICIENTS)
            for idx_sql in _DDL_INDEXES:
                cur.execute(idx_sql)

    # ------------------------------------------------------------------
    # Generic upsert helper
    # ------------------------------------------------------------------

    def _upsert(self, table: str, record: dict, conflict_cols: List[str]) -> None:
        if not record:
            return
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        update_str = ", ".join(
            f"{c}=excluded.{c}" for c in cols if c not in conflict_cols
        )
        conflict_str = ", ".join(conflict_cols)
        sql = f"""
            INSERT INTO {table} ({col_str})
            VALUES ({placeholders})
            ON CONFLICT({conflict_str}) DO UPDATE SET {update_str}
        """
        with self._cursor() as cur:
            cur.execute(sql, list(record.values()))

    # ------------------------------------------------------------------
    # Company Profiles
    # ------------------------------------------------------------------

    def upsert_profile(self, record: dict) -> None:
        self._upsert("company_profiles", record, ["ticker"])

    def get_profile(self, ticker: str) -> Optional[dict]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM company_profiles WHERE ticker=?", [ticker])
            row = cur.fetchone()
            return dict(row) if row else None

    def get_all_profiles(self, sector: Optional[str] = None) -> List[dict]:
        if sector:
            sql = "SELECT * FROM company_profiles WHERE sector=? ORDER BY ticker"
            params = [sector]
        else:
            sql = "SELECT * FROM company_profiles ORDER BY ticker"
            params = []
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def upsert_profiles_batch(self, records: List[dict]) -> int:
        written = 0
        for r in records:
            try:
                self.upsert_profile(r)
                written += 1
            except Exception as e:
                logger.warning("Profile upsert failed %s: %s", r.get("ticker"), e)
        return written

    # ------------------------------------------------------------------
    # Signal Effectiveness
    # ------------------------------------------------------------------

    def upsert_signal_effectiveness(self, record: dict) -> None:
        self._upsert(
            "signal_effectiveness", record,
            ["signal_name", "sector", "market_regime", "ticker_subset"]
        )

    def get_signal_effectiveness(
        self,
        signal_name: Optional[str] = None,
        sector: Optional[str] = None,
        min_observations: int = 10,
    ) -> List[dict]:
        clauses, params = ["n_observations >= ?"], [min_observations]
        if signal_name:
            clauses.append("signal_name = ?")
            params.append(signal_name)
        if sector:
            clauses.append("(sector = ? OR sector IS NULL)")
            params.append(sector)
        where = "WHERE " + " AND ".join(clauses)
        sql = f"SELECT * FROM signal_effectiveness {where} ORDER BY sharpe_ratio DESC NULLS LAST"
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Pattern Discovery
    # ------------------------------------------------------------------

    def upsert_pattern(self, record: dict) -> None:
        self._upsert("pattern_discovery", record, ["pattern_id"])

    def get_patterns(
        self,
        pattern_type: Optional[str] = None,
        sector: Optional[str] = None,
        min_confidence: float = 0.6,
        min_occurrences: int = 5,
    ) -> List[dict]:
        clauses = ["confidence_score >= ?", "n_occurrences >= ?"]
        params = [min_confidence, min_occurrences]
        if pattern_type:
            clauses.append("pattern_type = ?")
            params.append(pattern_type)
        if sector:
            clauses.append("(sector = ? OR sector IS NULL)")
            params.append(sector)
        where = "WHERE " + " AND ".join(clauses)
        sql = f"SELECT * FROM pattern_discovery {where} ORDER BY sharpe_ratio DESC NULLS LAST"
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Cross-Asset Correlations
    # ------------------------------------------------------------------

    def upsert_correlation(self, record: dict) -> None:
        self._upsert("cross_asset_correlations", record, ["asset_a", "asset_b", "relationship_type"])

    def get_correlations(
        self,
        asset: Optional[str] = None,
        relationship_type: Optional[str] = None,
    ) -> List[dict]:
        clauses, params = [], []
        if asset:
            clauses.append("(asset_a = ? OR asset_b = ?)")
            params.extend([asset, asset])
        if relationship_type:
            clauses.append("relationship_type = ?")
            params.append(relationship_type)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM cross_asset_correlations {where} ORDER BY ABS(correlation) DESC"
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Readthrough Coefficients
    # ------------------------------------------------------------------

    def upsert_readthrough_coeff(self, record: dict) -> None:
        self._upsert("readthrough_coefficients", record, ["large_ticker", "peer_ticker"])

    def get_readthrough_coeffs(self, peer_ticker: str) -> List[dict]:
        sql = "SELECT * FROM readthrough_coefficients WHERE peer_ticker=? ORDER BY coeff DESC"
        with self._cursor() as cur:
            cur.execute(sql, [peer_ticker])
            return [dict(row) for row in cur.fetchall()]

    def upsert_readthrough_batch(self, records: List[dict]) -> int:
        written = 0
        for r in records:
            try:
                self.upsert_readthrough_coeff(r)
                written += 1
            except Exception as e:
                logger.warning("Readthrough upsert failed: %s", e)
        return written

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        tables = [
            "company_profiles",
            "signal_effectiveness",
            "pattern_discovery",
            "cross_asset_correlations",
            "readthrough_coefficients",
        ]
        counts = {}
        with self._cursor() as cur:
            for t in tables:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
        counts["db_path"] = str(self.db_path)
        return counts
