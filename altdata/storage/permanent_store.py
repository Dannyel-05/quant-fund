"""
Permanent Log Store (Refinement 7) — append-only archive.

output/permanent_log.db stores EVERYTHING that happens in the system,
never deleted, with SQLite FTS5 full-text search across all text fields.

Tables:
  permanent_log      — every event, signal, trade, article, post, prediction
  prediction_log     — explicit predictions with outcome tracking
  weekly_accuracy    — weekly accuracy summary per prediction type

CLI usage:
  python main.py search "semiconductor inventory"
  python main.py search "SHEN guidance"

Usage:
    from altdata.storage.permanent_store import PermanentStore
    store = PermanentStore()
    store.log_event(event_type="SIGNAL", ticker="SHEN", description="PEAD signal +1", ...)
    store.log_prediction(ticker="SHEN", direction=1, confidence=0.8, ...)
    results = store.search("SHEN guidance")
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_local = threading.local()

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS permanent_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,      -- SIGNAL | TRADE | ARTICLE | SOCIAL | INSIDER |
                                        -- EARNINGS | ANALYST | CONGRESSIONAL | PATTERN |
                                        -- WEIGHT_CHANGE | PREDICTION | SYSTEM
    ticker          TEXT,
    market          TEXT,
    event_date      TEXT NOT NULL,
    title           TEXT,               -- short summary
    description     TEXT,               -- full description (never truncated)
    data_json       TEXT,               -- full structured data as JSON
    signal_direction INTEGER,           -- +1 | -1 | 0 | NULL
    signal_value    REAL,
    confidence      REAL,
    source          TEXT,
    outcome_return  REAL,               -- filled in after the fact
    outcome_date    TEXT,
    was_correct     INTEGER,            -- 1=yes, 0=no, NULL=unresolved
    logged_at       TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS permanent_fts USING fts5(
    event_type,
    ticker,
    title,
    description,
    data_json,
    source,
    content='permanent_log',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS plog_ai AFTER INSERT ON permanent_log BEGIN
    INSERT INTO permanent_fts(rowid, event_type, ticker, title, description, data_json, source)
    VALUES (new.id, new.event_type, new.ticker, new.title, new.description, new.data_json, new.source);
END;

CREATE TABLE IF NOT EXISTS prediction_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    source_url      TEXT,
    predicted_at    TEXT NOT NULL,
    prediction_type TEXT NOT NULL,  -- 'direction' | 'price_target' | 'eps_beat' | 'event'
    direction       INTEGER,        -- +1 long, -1 short, 0 neutral
    price_target    REAL,
    confidence      REAL,
    horizon_days    INTEGER,
    supporting_data TEXT,           -- JSON: what signals drove this
    model_version   TEXT,
    -- Outcome (filled in after horizon_days elapsed)
    outcome_return  REAL,
    outcome_date    TEXT,
    was_correct     INTEGER,
    accuracy_note   TEXT,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS weekly_accuracy (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    week_ending     TEXT NOT NULL,
    prediction_type TEXT NOT NULL,
    n_predictions   INTEGER,
    n_resolved      INTEGER,
    n_correct       INTEGER,
    accuracy_pct    REAL,
    avg_return      REAL,
    sharpe          REAL,
    computed_at     TEXT NOT NULL,
    UNIQUE (week_ending, prediction_type)
);

CREATE INDEX IF NOT EXISTS idx_plog_ticker ON permanent_log (ticker);
CREATE INDEX IF NOT EXISTS idx_plog_type   ON permanent_log (event_type);
CREATE INDEX IF NOT EXISTS idx_plog_date   ON permanent_log (event_date);
CREATE INDEX IF NOT EXISTS idx_pred_ticker ON prediction_log (ticker);
CREATE INDEX IF NOT EXISTS idx_pred_type   ON prediction_log (prediction_type);
"""


class PermanentStore:
    """
    Append-only permanent event log with FTS5 full-text search.
    Nothing is ever deleted from this database.
    """

    def __init__(self, db_path: str = "output/permanent_log.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if not getattr(_local, "perm_conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.executescript("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            conn.row_factory = sqlite3.Row
            _local.perm_conn = conn
        return _local.perm_conn

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

    def _init_db(self) -> None:
        conn = self._connect()
        conn.executescript(_DDL)
        conn.commit()

    # ------------------------------------------------------------------
    # Logging Events
    # ------------------------------------------------------------------

    def log_event(
        self,
        event_type: str,
        ticker: str = "",
        market: str = "us",
        title: str = "",
        description: str = "",
        data: Optional[Dict] = None,
        signal_direction: Optional[int] = None,
        signal_value: Optional[float] = None,
        confidence: Optional[float] = None,
        source: str = "",
        event_date: Optional[str] = None,
    ) -> int:
        """Log any system event. Returns the inserted row id."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO permanent_log
                   (event_type, ticker, market, event_date, title, description, data_json,
                    signal_direction, signal_value, confidence, source, logged_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    event_type,
                    ticker or "",
                    market or "us",
                    event_date or now[:10],
                    title[:500] if title else "",
                    description or "",
                    json.dumps(data, default=str) if data else None,
                    signal_direction,
                    signal_value,
                    confidence,
                    source or "",
                    now,
                ]
            )
            return cur.lastrowid

    def log_signal(self, ticker: str, signal_type: str, direction: int,
                   value: float, confidence: float, data: Dict = None) -> int:
        return self.log_event(
            event_type="SIGNAL",
            ticker=ticker,
            title=f"{signal_type} {'+' if direction > 0 else ''}{direction} ({value:.3f})",
            description=f"Signal {signal_type} generated for {ticker}: direction={direction}, value={value:.4f}",
            data=data,
            signal_direction=direction,
            signal_value=value,
            confidence=confidence,
            source=signal_type,
        )

    def log_trade(self, ticker: str, action: str, price: float,
                  shares: float, data: Dict = None) -> int:
        return self.log_event(
            event_type="TRADE",
            ticker=ticker,
            title=f"{action} {ticker} @ {price:.2f} x {shares:.0f}",
            description=f"Trade: {action} {shares:.0f} shares of {ticker} @ ${price:.2f}",
            data=data,
            source="paper_trader",
        )

    def log_earnings(self, ticker: str, earnings_date: str, surprise_pct: float,
                     eps_actual: float, eps_estimate: float, data: Dict = None) -> int:
        direction = 1 if surprise_pct > 0 else -1 if surprise_pct < 0 else 0
        return self.log_event(
            event_type="EARNINGS",
            ticker=ticker,
            event_date=earnings_date,
            title=f"{ticker} earnings: surprise={surprise_pct:+.1%} EPS={eps_actual:.2f} vs {eps_estimate:.2f}",
            description=f"Earnings for {ticker} on {earnings_date}: actual={eps_actual}, est={eps_estimate}, surprise={surprise_pct:+.1%}",
            data=data,
            signal_direction=direction,
            signal_value=surprise_pct,
            source="earnings_collector",
        )

    def log_insider(self, ticker: str, insider_name: str, classification: str,
                    value_usd: float, data: Dict = None) -> int:
        return self.log_event(
            event_type="INSIDER",
            ticker=ticker,
            title=f"{ticker} insider {classification}: {insider_name} ${value_usd:,.0f}",
            description=f"Insider transaction for {ticker}: {insider_name} [{classification}] ${value_usd:,.0f}",
            data=data,
            source="sec_edgar",
        )

    def log_article(self, ticker: str, url: str, title: str, sentiment: float,
                    data: Dict = None) -> int:
        direction = 1 if sentiment > 0.1 else -1 if sentiment < -0.1 else 0
        return self.log_event(
            event_type="ARTICLE",
            ticker=ticker,
            title=title[:500],
            description=f"Article collected for {ticker}: {url}",
            data=data,
            signal_direction=direction,
            signal_value=sentiment,
            source="article_reader",
        )

    def log_analyst(self, ticker: str, firm: str, action: str, from_rating: str,
                    to_rating: str, target: float = None, data: Dict = None) -> int:
        return self.log_event(
            event_type="ANALYST",
            ticker=ticker,
            title=f"{ticker}: {firm} {action} → {to_rating}" + (f" PT ${target:.0f}" if target else ""),
            description=f"Analyst rating change: {firm} {action} {from_rating} → {to_rating} for {ticker}",
            data=data,
            source="finviz",
        )

    def log_pattern(self, pattern_name: str, sector: str, avg_return: float,
                    confidence: float, data: Dict = None) -> int:
        return self.log_event(
            event_type="PATTERN",
            ticker=sector,
            title=f"Pattern discovered: {pattern_name} (ret={avg_return:+.2%}, conf={confidence:.2f})",
            description=f"Intelligence engine discovered pattern: {pattern_name}",
            data=data,
            signal_value=avg_return,
            confidence=confidence,
            source="intelligence_engine",
        )

    def log_weight_change(self, signal_name: str, old_weight: float, new_weight: float,
                          reason: str, data: Dict = None) -> int:
        return self.log_event(
            event_type="WEIGHT_CHANGE",
            title=f"Weight change: {signal_name} {old_weight:.3f} → {new_weight:.3f}",
            description=f"Signal weight updated: {signal_name}. Reason: {reason}",
            data=data,
            source="closeloop",
        )

    # ------------------------------------------------------------------
    # Prediction Tracking
    # ------------------------------------------------------------------

    def log_prediction(
        self,
        ticker: str,
        prediction_type: str,
        direction: Optional[int] = None,
        price_target: Optional[float] = None,
        confidence: float = 0.5,
        horizon_days: int = 5,
        supporting_data: Optional[Dict] = None,
        model_version: str = "",
        source_url: str = "",
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO prediction_log
                   (ticker, source_url, predicted_at, prediction_type, direction,
                    price_target, confidence, horizon_days, supporting_data, model_version)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [
                    ticker, source_url, now, prediction_type, direction,
                    price_target, confidence, horizon_days,
                    json.dumps(supporting_data, default=str) if supporting_data else None,
                    model_version,
                ]
            )
            pred_id = cur.lastrowid

        # Also log to permanent_log
        self.log_event(
            event_type="PREDICTION",
            ticker=ticker,
            title=f"Prediction: {ticker} {prediction_type} dir={direction} conf={confidence:.2f}",
            description=f"System prediction for {ticker}: type={prediction_type}, direction={direction}, horizon={horizon_days}d",
            data={"prediction_id": pred_id, **(supporting_data or {})},
            signal_direction=direction,
            confidence=confidence,
            source=model_version,
        )
        return pred_id

    def resolve_prediction(
        self,
        prediction_id: int,
        outcome_return: float,
        outcome_date: str,
    ) -> None:
        """Record the actual outcome of a prediction."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT prediction_type, direction FROM prediction_log WHERE id=?",
                [prediction_id]
            )
            row = cur.fetchone()
            if not row:
                return

            direction = row["direction"]
            was_correct = 1 if (direction or 0) * outcome_return > 0 else 0

            cur.execute(
                """UPDATE prediction_log
                   SET outcome_return=?, outcome_date=?, was_correct=?,
                       resolved_at=?
                   WHERE id=?""",
                [outcome_return, outcome_date, was_correct,
                 datetime.now(timezone.utc).isoformat(), prediction_id]
            )

    def get_unresolved_predictions(self, ticker: str = None) -> List[Dict]:
        clauses = ["was_correct IS NULL"]
        params = []
        if ticker:
            clauses.append("ticker=?")
            params.append(ticker)
        where = "WHERE " + " AND ".join(clauses)
        with self._cursor() as cur:
            cur.execute(f"SELECT * FROM prediction_log {where} ORDER BY predicted_at", params)
            return [dict(r) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # Search (FTS5)
    # ------------------------------------------------------------------

    def search(self, query: str, limit: int = 20, event_type: str = None) -> List[Dict]:
        """
        Full-text search across all logged events.
        Returns results ordered by relevance.
        """
        try:
            params = [query, limit]
            type_filter = ""
            if event_type:
                type_filter = "AND p.event_type=?"
                params = [query] + [event_type] + [limit]

            with self._cursor() as cur:
                cur.execute(
                    f"""SELECT p.* FROM permanent_log p
                        JOIN permanent_fts f ON p.id = f.rowid
                        WHERE permanent_fts MATCH ?
                        {type_filter}
                        ORDER BY rank LIMIT ?""",
                    params
                )
                results = []
                for row in cur.fetchall():
                    d = dict(row)
                    if d.get("data_json"):
                        try:
                            d["data"] = json.loads(d["data_json"])
                        except Exception:
                            d["data"] = {}
                    results.append(d)
                return results
        except Exception as e:
            logger.warning("permanent_store: search error: %s", e)
            return []

    # ------------------------------------------------------------------
    # Weekly Accuracy Report
    # ------------------------------------------------------------------

    def compute_weekly_accuracy(self) -> List[Dict]:
        """
        Compute weekly accuracy summary for all prediction types.
        Stored in weekly_accuracy table.
        """
        import math
        now = datetime.now(timezone.utc).isoformat()
        results = []

        with self._cursor() as cur:
            cur.execute("SELECT DISTINCT prediction_type FROM prediction_log")
            types = [r[0] for r in cur.fetchall()]

        for ptype in types:
            with self._cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) as n, SUM(CASE WHEN was_correct IS NOT NULL THEN 1 ELSE 0 END) as n_resolved,
                              SUM(CASE WHEN was_correct=1 THEN 1 ELSE 0 END) as n_correct,
                              AVG(outcome_return) as avg_ret,
                              STDEV(outcome_return) as std_ret
                       FROM prediction_log WHERE prediction_type=?""",
                    [ptype]
                )
                row = dict(cur.fetchone())

            n = row.get("n") or 0
            n_resolved = row.get("n_resolved") or 0
            n_correct  = row.get("n_correct") or 0
            avg_ret    = row.get("avg_ret") or 0.0
            std_ret    = row.get("std_ret") or 1.0

            accuracy_pct = n_correct / max(1, n_resolved)
            sharpe = (avg_ret / max(1e-9, std_ret)) * math.sqrt(252) if n_resolved >= 5 else 0.0

            week_ending = datetime.now().strftime("%Y-%W")

            with self._cursor() as cur:
                cur.execute(
                    """INSERT OR REPLACE INTO weekly_accuracy
                       (week_ending, prediction_type, n_predictions, n_resolved, n_correct,
                        accuracy_pct, avg_return, sharpe, computed_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    [week_ending, ptype, n, n_resolved, n_correct,
                     accuracy_pct, avg_ret, sharpe, now]
                )

            results.append({
                "prediction_type": ptype,
                "week_ending":     week_ending,
                "n_predictions":   n,
                "n_resolved":      n_resolved,
                "accuracy_pct":    round(accuracy_pct, 4),
                "avg_return":      round(avg_ret, 5),
                "sharpe":          round(sharpe, 4),
            })

        return results

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> Dict:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM permanent_log")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM prediction_log")
            pred_total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM prediction_log WHERE was_correct IS NOT NULL")
            pred_resolved = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM prediction_log WHERE was_correct=1")
            pred_correct = cur.fetchone()[0]
            cur.execute("SELECT event_type, COUNT(*) as n FROM permanent_log GROUP BY event_type ORDER BY n DESC")
            by_type = {r[0]: r[1] for r in cur.fetchall()}

        return {
            "total_events":        total,
            "predictions":         pred_total,
            "predictions_resolved": pred_resolved,
            "predictions_correct":  pred_correct,
            "accuracy_pct":        round(pred_correct / max(1, pred_resolved), 4),
            "by_event_type":       by_type,
            "db_path":             str(self.db_path),
        }
