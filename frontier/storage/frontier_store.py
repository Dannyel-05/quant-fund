"""
SQLite-backed storage layer for the frontier intelligence pipeline.

Designed around the core principle: log EVERYTHING permanently.
Data that fails validation today may become significant tomorrow.

Tables:
  raw_signals       — every raw signal reading from every collector
  umci_history      — every UMCI reading with full dimensional breakdown
  validation_results— every validation attempt, pass or fail
  watchlist         — discovered correlations and their evidence
  parameter_history — every parameter drift event
  evidence_records  — live track record for each signal
  signal_log        — generated frontier signals
  interaction_log   — cross-signal interaction test results
"""
import json
import logging
import os
import shutil
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_CREATE_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS raw_signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    collector   TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    ticker      TEXT,
    market      TEXT,
    value       REAL,
    raw_data    TEXT,
    quality     REAL DEFAULT 1.0,
    collected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS umci_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    umci            REAL NOT NULL,
    level           TEXT NOT NULL,
    physical        REAL,
    social          REAL,
    scientific      REAL,
    financial       REAL,
    altdata         REAL,
    dominant_dim    TEXT,
    position_mult   REAL,
    halt            INTEGER DEFAULT 0,
    full_breakdown  TEXT,
    recorded_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name     TEXT NOT NULL,
    passed          INTEGER NOT NULL,
    t_stat          REAL,
    p_value         REAL,
    p_bonferroni    REAL,
    monte_carlo_pct REAL,
    sharpe          REAL,
    deflated_sharpe REAL,
    fsp             REAL,
    evidence_grade  TEXT,
    n_obs           INTEGER,
    regime_stable   INTEGER,
    full_result     TEXT,
    validated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT,
    formula         TEXT,
    correlation     REAL,
    optimal_lag     INTEGER,
    t_stat          REAL,
    p_bonferroni    REAL,
    monte_carlo_pct REAL,
    deflated_sharpe REAL,
    n_obs           INTEGER,
    validation_status TEXT DEFAULT 'PENDING',
    live_days       INTEGER DEFAULT 0,
    live_accuracy   REAL,
    sizing_tier     INTEGER DEFAULT 5,
    notes           TEXT,
    discovered_at   TEXT NOT NULL,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS parameter_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name     TEXT NOT NULL,
    param_name      TEXT NOT NULL,
    published_value REAL NOT NULL,
    old_value       REAL NOT NULL,
    new_value       REAL NOT NULL,
    performance_delta REAL,
    reason          TEXT,
    changed_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence_records (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name     TEXT NOT NULL,
    sizing_tier     INTEGER NOT NULL,
    live_days       INTEGER DEFAULT 0,
    live_accuracy   REAL,
    live_sharpe     REAL,
    n_live_signals  INTEGER DEFAULT 0,
    last_promoted   TEXT,
    updated_at      TEXT NOT NULL,
    UNIQUE(signal_name)
);

CREATE TABLE IF NOT EXISTS signal_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    signal_name     TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    direction       INTEGER,
    confidence      REAL,
    sizing_tier     INTEGER,
    position_size   REAL,
    sources         TEXT,
    generated_at    TEXT NOT NULL,
    outcome_return  REAL,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS interaction_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    components      TEXT NOT NULL,
    value           REAL,
    tested          INTEGER DEFAULT 0,
    t_stat          REAL,
    p_value         REAL,
    significant     INTEGER DEFAULT 0,
    logged_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_collector  ON raw_signals(collector, collected_at);
CREATE INDEX IF NOT EXISTS idx_raw_ticker     ON raw_signals(ticker, collected_at);
CREATE INDEX IF NOT EXISTS idx_umci_ts        ON umci_history(recorded_at);
CREATE INDEX IF NOT EXISTS idx_val_signal     ON validation_results(signal_name, validated_at);
CREATE INDEX IF NOT EXISTS idx_siglog_ticker  ON signal_log(ticker, generated_at);
"""


class FrontierStore:
    """Thread-safe SQLite store for the frontier intelligence pipeline."""

    def __init__(self, config: dict):
        cfg = config.get("frontier", {})
        db_path = cfg.get("storage_path", "frontier/storage/frontier.db")
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()
        logger.info(f"FrontierStore initialised at {self._path}")

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self._path), check_same_thread=False, timeout=30
            )
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self) -> None:
        self._conn().executescript(_CREATE_SQL)
        self._conn().commit()

    # ------------------------------------------------------------------
    # Raw signals (log everything permanently)
    # ------------------------------------------------------------------

    def store_raw(self, collector: str, signal_name: str, value: float,
                  ticker: str = None, market: str = None,
                  raw_data: Any = None, quality: float = 1.0) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO raw_signals
               (collector, signal_name, ticker, market, value, raw_data, quality, collected_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (collector, signal_name, ticker, market, value,
             json.dumps(raw_data) if raw_data is not None else None, quality, now),
        )
        conn.commit()
        return cur.lastrowid

    def get_raw_history(self, signal_name: str, days_back: int = 365,
                         ticker: str = None) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        if ticker:
            rows = self._conn().execute(
                "SELECT * FROM raw_signals WHERE signal_name=? AND ticker=? AND collected_at>=? ORDER BY collected_at",
                (signal_name, ticker, cutoff),
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM raw_signals WHERE signal_name=? AND collected_at>=? ORDER BY collected_at",
                (signal_name, cutoff),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("raw_data"):
                try:
                    d["raw_data"] = json.loads(d["raw_data"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # UMCI history
    # ------------------------------------------------------------------

    def store_umci(self, umci: float, breakdown: Dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        dims = breakdown.get("dimensions", {})
        conn = self._conn()
        conn.execute(
            """INSERT INTO umci_history
               (umci, level, physical, social, scientific, financial, altdata,
                dominant_dim, position_mult, halt, full_breakdown, recorded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                umci,
                breakdown.get("level"),
                dims.get("physical"),
                dims.get("social"),
                dims.get("scientific"),
                dims.get("financial"),
                dims.get("altdata"),
                breakdown.get("dominant_dimension"),
                breakdown.get("position_multiplier"),
                int(breakdown.get("halt_new_positions", False)),
                json.dumps(breakdown),
                now,
            ),
        )
        conn.commit()

    def get_umci_history(self, n: int = 252) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT ?", (n,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("full_breakdown"):
                try:
                    d["full_breakdown"] = json.loads(d["full_breakdown"])
                except Exception:
                    pass
            result.append(d)
        return list(reversed(result))

    def get_last_umci(self) -> Optional[Dict]:
        row = self._conn().execute(
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()
        if row:
            d = dict(row)
            if d.get("full_breakdown"):
                try:
                    d["full_breakdown"] = json.loads(d["full_breakdown"])
                except Exception:
                    pass
            return d
        return None

    # ------------------------------------------------------------------
    # Validation results (log everything — even failures)
    # ------------------------------------------------------------------

    def store_validation(self, signal_name: str, result: Dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO validation_results
               (signal_name, passed, t_stat, p_value, p_bonferroni, monte_carlo_pct,
                sharpe, deflated_sharpe, fsp, evidence_grade, n_obs, regime_stable,
                full_result, validated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                signal_name,
                int(result.get("passed", False)),
                result.get("t_stat"),
                result.get("p_value"),
                result.get("p_bonferroni"),
                result.get("monte_carlo_pct"),
                result.get("sharpe"),
                result.get("deflated_sharpe"),
                result.get("fsp"),
                result.get("evidence_grade"),
                result.get("n_obs"),
                int(result.get("regime_stable", False)),
                json.dumps(result),
                now,
            ),
        )
        conn.commit()

    def get_validation_history(self, signal_name: str) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM validation_results WHERE signal_name=? ORDER BY validated_at",
            (signal_name,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Watchlist
    # ------------------------------------------------------------------

    def upsert_watchlist(self, entry: Dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO watchlist
               (name, description, formula, correlation, optimal_lag, t_stat,
                p_bonferroni, monte_carlo_pct, deflated_sharpe, n_obs,
                validation_status, live_days, live_accuracy, sizing_tier,
                notes, discovered_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                 description=excluded.description,
                 correlation=excluded.correlation,
                 monte_carlo_pct=excluded.monte_carlo_pct,
                 deflated_sharpe=excluded.deflated_sharpe,
                 n_obs=excluded.n_obs,
                 validation_status=excluded.validation_status,
                 live_days=excluded.live_days,
                 live_accuracy=excluded.live_accuracy,
                 sizing_tier=excluded.sizing_tier,
                 notes=excluded.notes,
                 updated_at=excluded.updated_at""",
            (
                entry["name"], entry.get("description"), entry.get("formula"),
                entry.get("correlation"), entry.get("optimal_lag"),
                entry.get("t_stat"), entry.get("p_bonferroni"),
                entry.get("monte_carlo_pct"), entry.get("deflated_sharpe"),
                entry.get("n_obs"), entry.get("validation_status", "PENDING"),
                entry.get("live_days", 0), entry.get("live_accuracy"),
                entry.get("sizing_tier", 5), entry.get("notes"),
                entry.get("discovered_at", now), now,
            ),
        )
        conn.commit()

    def get_watchlist(self, status: Optional[str] = None, limit: int = 10) -> List[Dict]:
        if status:
            rows = self._conn().execute(
                "SELECT * FROM watchlist WHERE validation_status=? ORDER BY deflated_sharpe DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM watchlist ORDER BY deflated_sharpe DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Parameter drift history
    # ------------------------------------------------------------------

    def log_parameter_drift(self, signal_name: str, param_name: str,
                             published: float, old_val: float, new_val: float,
                             delta: float = None, reason: str = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO parameter_history
               (signal_name, param_name, published_value, old_value, new_value,
                performance_delta, reason, changed_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (signal_name, param_name, published, old_val, new_val, delta, reason, now),
        )
        conn.commit()

    def get_parameter_history(self, signal_name: str) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM parameter_history WHERE signal_name=? ORDER BY changed_at",
            (signal_name,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Evidence records and tier management
    # ------------------------------------------------------------------

    def update_evidence(self, signal_name: str, sizing_tier: int,
                         live_days: int, live_accuracy: float = None,
                         live_sharpe: float = None, n_live: int = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO evidence_records
               (signal_name, sizing_tier, live_days, live_accuracy, live_sharpe,
                n_live_signals, updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(signal_name) DO UPDATE SET
                 sizing_tier=excluded.sizing_tier,
                 live_days=excluded.live_days,
                 live_accuracy=COALESCE(excluded.live_accuracy, live_accuracy),
                 live_sharpe=COALESCE(excluded.live_sharpe, live_sharpe),
                 n_live_signals=COALESCE(excluded.n_live_signals, n_live_signals),
                 updated_at=excluded.updated_at""",
            (signal_name, sizing_tier, live_days, live_accuracy,
             live_sharpe, n_live, now),
        )
        conn.commit()

    def get_evidence(self, signal_name: str) -> Optional[Dict]:
        row = self._conn().execute(
            "SELECT * FROM evidence_records WHERE signal_name=?", (signal_name,)
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Signal log
    # ------------------------------------------------------------------

    def log_signal(self, ticker: str, signal_name: str, signal_type: str,
                    direction: int, confidence: float, sizing_tier: int,
                    position_size: float = None, sources: List[str] = None) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO signal_log
               (ticker, signal_name, signal_type, direction, confidence, sizing_tier,
                position_size, sources, generated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (ticker, signal_name, signal_type, direction, confidence, sizing_tier,
             position_size, json.dumps(sources) if sources else None, now),
        )
        conn.commit()
        return cur.lastrowid

    def get_recent_signals(self, hours_back: int = 48, limit: int = 100) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        rows = self._conn().execute(
            "SELECT * FROM signal_log WHERE generated_at>=? ORDER BY generated_at DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Status summary
    # ------------------------------------------------------------------

    def status_summary(self) -> Dict:
        conn = self._conn()
        last_umci = self.get_last_umci()
        return {
            "raw_signals_24h": conn.execute(
                "SELECT COUNT(*) FROM raw_signals WHERE collected_at >= datetime('now','-1 day')"
            ).fetchone()[0],
            "validations_total": conn.execute(
                "SELECT COUNT(*) FROM validation_results"
            ).fetchone()[0],
            "validations_passed": conn.execute(
                "SELECT COUNT(*) FROM validation_results WHERE passed=1"
            ).fetchone()[0],
            "watchlist_size": conn.execute(
                "SELECT COUNT(*) FROM watchlist"
            ).fetchone()[0],
            "monitoring_signals": conn.execute(
                "SELECT COUNT(*) FROM watchlist WHERE validation_status='MONITORING'"
            ).fetchone()[0],
            "signals_24h": conn.execute(
                "SELECT COUNT(*) FROM signal_log WHERE generated_at >= datetime('now','-1 day')"
            ).fetchone()[0],
            "last_umci": last_umci.get("umci") if last_umci else None,
            "last_umci_level": last_umci.get("level") if last_umci else None,
        }

    # ------------------------------------------------------------------
    # Backup / close
    # ------------------------------------------------------------------

    def backup(self, backup_dir: str = "output/backups") -> str:
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        dest = os.path.join(backup_dir, f"frontier_{stamp}.db")
        shutil.copy2(str(self._path), dest)
        logger.info(f"FrontierStore backed up to {dest}")
        return dest

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
