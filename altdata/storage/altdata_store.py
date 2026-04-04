"""
SQLite-backed storage layer for all altdata.

Tables:
  raw_data           — raw collector output (one row per fetch)
  sentiment_scores   — processed sentiment per ticker/source
  features           — engineered feature vectors per ticker/date
  model_versions     — model snapshots with metadata
  signals            — generated alt-data signals
  anomaly_candidates — candidates surfaced by NonsenseDetector / StatisticalValidator
  notifications      — notification log
  alt_data_pnl       — forward P&L attributed to each signal
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
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS raw_data (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    ticker      TEXT,
    market      TEXT,
    data_type   TEXT    NOT NULL,
    value       REAL,
    raw_data    TEXT,
    quality     REAL    DEFAULT 1.0,
    collected_at TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS sentiment_scores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    score       REAL    NOT NULL,
    confidence  REAL    DEFAULT 1.0,
    method      TEXT,
    computed_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS features (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    NOT NULL,
    feature_date TEXT   NOT NULL,
    features    TEXT    NOT NULL,   -- JSON blob
    version     TEXT    DEFAULT '1',
    created_at  TEXT    NOT NULL,
    UNIQUE(ticker, feature_date, version)
);

CREATE TABLE IF NOT EXISTS model_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT    NOT NULL UNIQUE,
    model_type  TEXT    NOT NULL,
    accuracy    REAL,
    sharpe      REAL,
    file_path   TEXT    NOT NULL,
    is_active   INTEGER DEFAULT 0,
    trained_at  TEXT    NOT NULL,
    metadata    TEXT    -- JSON blob
);

CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    signal_type     TEXT    NOT NULL,
    direction       INTEGER NOT NULL,  -- +1 long, -1 short
    confidence      REAL    NOT NULL,
    confluence      REAL,
    sources         TEXT,              -- JSON list
    model_version   TEXT,
    generated_at    TEXT    NOT NULL,
    outcome_return  REAL,
    outcome_days    INTEGER,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS anomaly_candidates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    description TEXT,
    sharpe      REAL,
    nonsense_score REAL,
    status      TEXT    DEFAULT 'candidate',  -- candidate|validated|live|rejected
    metadata    TEXT,
    found_at    TEXT    NOT NULL,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS notifications (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    level       TEXT    NOT NULL,  -- INFO|WARNING|ALERT|CRITICAL
    title       TEXT    NOT NULL,
    body        TEXT,
    channel     TEXT,
    sent_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS alt_data_pnl (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id   INTEGER REFERENCES signals(id),
    ticker      TEXT    NOT NULL,
    entry_date  TEXT    NOT NULL,
    exit_date   TEXT,
    pnl         REAL,
    holding_days INTEGER,
    recorded_at TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_source     ON raw_data(source, collected_at);
CREATE INDEX IF NOT EXISTS idx_raw_ticker     ON raw_data(ticker, collected_at);
CREATE INDEX IF NOT EXISTS idx_sentiment_tick ON sentiment_scores(ticker, computed_at);
CREATE INDEX IF NOT EXISTS idx_features_tick  ON features(ticker, feature_date);
CREATE INDEX IF NOT EXISTS idx_signals_tick   ON signals(ticker, generated_at);
"""


class AltDataStore:
    """Thread-safe SQLite store for all alt-data pipeline state."""

    def __init__(self, config: dict):
        cfg = config.get("altdata", {}).get("storage", {})
        db_path = cfg.get("db_path", "output/altdata.db")
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()
        logger.info(f"AltDataStore initialised at {self._path}")

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self._path), check_same_thread=False, timeout=30
            )
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(_CREATE_SQL)
        conn.commit()

    # ------------------------------------------------------------------
    # Raw data
    # ------------------------------------------------------------------

    def store_raw(self, result: Dict[str, Any]) -> int:
        """Store a CollectorResult dict. Returns inserted row id."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO raw_data
               (source, ticker, market, data_type, value, raw_data, quality, collected_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                result.get("source", "unknown"),
                result.get("ticker"),
                result.get("market"),
                result.get("data_type", "unknown"),
                result.get("value"),
                json.dumps(result.get("raw_data")) if result.get("raw_data") is not None else None,
                result.get("quality_score", 1.0),
                result.get("timestamp", now),
            ),
        )
        conn.commit()
        return cur.lastrowid

    def get_raw(
        self,
        source: Optional[str] = None,
        ticker: Optional[str] = None,
        hours_back: int = 48,
        limit: int = 500,
    ) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        clauses = ["collected_at >= ?"]
        params: List[Any] = [cutoff]
        if source:
            clauses.append("source = ?")
            params.append(source)
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        params.append(limit)
        sql = f"SELECT * FROM raw_data WHERE {' AND '.join(clauses)} ORDER BY collected_at DESC LIMIT ?"
        rows = self._conn().execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Sentiment scores
    # ------------------------------------------------------------------

    def store_sentiment(
        self,
        ticker: str,
        source: str,
        score: float,
        confidence: float = 1.0,
        method: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO sentiment_scores
               (ticker, source, score, confidence, method, computed_at)
               VALUES (?,?,?,?,?,?)""",
            (ticker, source, score, confidence, method, now),
        )
        conn.commit()

    def get_sentiment(self, ticker: str, hours_back: int = 48) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        rows = self._conn().execute(
            "SELECT * FROM sentiment_scores WHERE ticker=? AND computed_at>=? ORDER BY computed_at DESC",
            (ticker, cutoff),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Features
    # ------------------------------------------------------------------

    def store_features(
        self, ticker: str, feature_date: str, features: Dict, version: str = "1"
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO features
               (ticker, feature_date, features, version, created_at)
               VALUES (?,?,?,?,?)""",
            (ticker, feature_date, json.dumps(features), version, now),
        )
        conn.commit()

    def get_features(
        self, ticker: str, days_back: int = 30, version: str = "1"
    ) -> List[Dict]:
        from datetime import timedelta
        import pandas as pd
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
        rows = self._conn().execute(
            """SELECT * FROM features
               WHERE ticker=? AND feature_date>=? AND version=?
               ORDER BY feature_date DESC""",
            (ticker, cutoff, version),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["features"] = json.loads(d["features"])
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Model versions
    # ------------------------------------------------------------------

    def store_model_version(
        self,
        version: str,
        model_type: str,
        file_path: str,
        accuracy: Optional[float] = None,
        sharpe: Optional[float] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO model_versions
               (version, model_type, accuracy, sharpe, file_path, is_active, trained_at, metadata)
               VALUES (?,?,?,?,?,0,?,?)""",
            (
                version,
                model_type,
                accuracy,
                sharpe,
                file_path,
                now,
                json.dumps(metadata) if metadata else None,
            ),
        )
        conn.commit()
        logger.info(f"Stored model version {version} ({model_type})")

    def get_active_model(self) -> Optional[Dict]:
        row = self._conn().execute(
            "SELECT * FROM model_versions WHERE is_active=1 ORDER BY trained_at DESC LIMIT 1"
        ).fetchone()
        if row:
            d = dict(row)
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            return d
        return None

    def set_active_model(self, version: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE model_versions SET is_active=0")
        conn.execute("UPDATE model_versions SET is_active=1 WHERE version=?", (version,))
        conn.commit()
        logger.info(f"Set active model to {version}")

    def list_model_versions(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM model_versions ORDER BY trained_at DESC LIMIT ?", (limit,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def log_signal(
        self,
        ticker: str,
        signal_type: str,
        direction: int,
        confidence: float,
        confluence: Optional[float] = None,
        sources: Optional[List[str]] = None,
        model_version: Optional[str] = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO signals
               (ticker, signal_type, direction, confidence, confluence, sources, model_version, generated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                ticker,
                signal_type,
                direction,
                confidence,
                confluence,
                json.dumps(sources) if sources else None,
                model_version,
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid

    def update_signal_outcome(
        self, signal_id: int, outcome_return: float, outcome_days: int
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """UPDATE signals SET outcome_return=?, outcome_days=?, resolved_at=?
               WHERE id=?""",
            (outcome_return, outcome_days, now, signal_id),
        )
        conn.commit()

    def get_signal_accuracy(
        self, signal_type: Optional[str] = None, min_signals: int = 10
    ) -> Dict:
        where = "WHERE outcome_return IS NOT NULL"
        params: List[Any] = []
        if signal_type:
            where += " AND signal_type=?"
            params.append(signal_type)
        rows = self._conn().execute(
            f"SELECT direction, outcome_return FROM signals {where}", params
        ).fetchall()
        if len(rows) < min_signals:
            return {"n": len(rows), "accuracy": None, "mean_return": None}
        correct = sum(1 for r in rows if r["direction"] * r["outcome_return"] > 0)
        returns = [r["outcome_return"] for r in rows]
        return {
            "n": len(rows),
            "accuracy": correct / len(rows),
            "mean_return": sum(returns) / len(returns),
        }

    def get_recent_signals(self, hours_back: int = 48, limit: int = 100) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        rows = self._conn().execute(
            "SELECT * FROM signals WHERE generated_at>=? ORDER BY generated_at DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("sources"):
                d["sources"] = json.loads(d["sources"])
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Anomaly candidates
    # ------------------------------------------------------------------

    def store_anomaly_candidate(
        self,
        name: str,
        description: str,
        sharpe: float,
        nonsense_score: float,
        metadata: Optional[Dict] = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO anomaly_candidates
               (name, description, sharpe, nonsense_score, status, metadata, found_at, updated_at)
               VALUES (?,?,?,?,'candidate',?,?,?)""",
            (name, description, sharpe, nonsense_score, json.dumps(metadata) if metadata else None, now, now),
        )
        conn.commit()
        return cur.lastrowid

    def update_anomaly_status(self, anomaly_id: int, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            "UPDATE anomaly_candidates SET status=?, updated_at=? WHERE id=?",
            (status, now, anomaly_id),
        )
        conn.commit()

    def get_anomaly_candidates(self, status: Optional[str] = None) -> List[Dict]:
        if status:
            rows = self._conn().execute(
                "SELECT * FROM anomaly_candidates WHERE status=? ORDER BY sharpe DESC",
                (status,),
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM anomaly_candidates ORDER BY sharpe DESC"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    def log_notification(
        self, level: str, title: str, body: str = "", channel: str = "all"
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            "INSERT INTO notifications (level, title, body, channel, sent_at) VALUES (?,?,?,?,?)",
            (level, title, body, channel, now),
        )
        conn.commit()

    # ------------------------------------------------------------------
    # P&L tracking
    # ------------------------------------------------------------------

    def record_pnl(
        self,
        signal_id: int,
        ticker: str,
        entry_date: str,
        pnl: float,
        exit_date: Optional[str] = None,
        holding_days: Optional[int] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO alt_data_pnl
               (signal_id, ticker, entry_date, exit_date, pnl, holding_days, recorded_at)
               VALUES (?,?,?,?,?,?,?)""",
            (signal_id, ticker, entry_date, exit_date, pnl, holding_days, now),
        )
        conn.commit()

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup(self, backup_dir: str = "output/backups") -> str:
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        dest = os.path.join(backup_dir, f"altdata_{stamp}.db")
        shutil.copy2(str(self._path), dest)
        logger.info(f"AltDataStore backed up to {dest}")
        return dest

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
