"""
SQLite-backed storage layer for the deepdata pipeline.

Tables:
  options_flow        — options flow snapshots per ticker
  short_interest      — short interest reports (biweekly FINRA + FCA)
  squeeze_events      — historical and predicted squeeze events
  transcripts         — earnings call transcript metadata + scores
  patent_data         — patent filing and citation records
  supply_chain        — relationship graph edges + risk scores
  congressional       — congressional disclosure records
  congressional_members — member track records
  earnings_quality    — beat classification results
  deepdata_signals    — generated deep-data signals
  factor_exposures    — factor loading snapshots
  pattern_registry    — validated cross-module patterns
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

CREATE TABLE IF NOT EXISTS options_flow (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    market          TEXT NOT NULL,
    smfi            REAL,
    iv_rank         REAL,
    put_call_ratio  REAL,
    net_gamma       REAL,
    dark_pool_score REAL,
    unusual_activity INTEGER DEFAULT 0,
    raw             TEXT,
    collected_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS short_interest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    market          TEXT NOT NULL,
    short_float_pct REAL,
    days_to_cover   REAL,
    short_ratio     REAL,
    si_change_pct   REAL,
    trend           TEXT,
    squeeze_score   REAL,
    squeeze_flag    INTEGER DEFAULT 0,
    report_date     TEXT,
    collected_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS squeeze_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    event_type      TEXT NOT NULL,  -- 'historical'|'predicted'
    start_date      TEXT,
    peak_gain_pct   REAL,
    duration_days   INTEGER,
    pre_short_float REAL,
    probability     REAL,  -- for predicted events
    features        TEXT,  -- JSON
    recorded_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transcripts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    earnings_date       TEXT NOT NULL,
    source              TEXT,
    hedge_ratio         REAL,
    forward_ratio       REAL,
    we_ratio            REAL,
    passive_ratio       REAL,
    tone_shift          REAL,
    deflection_score    REAL,
    prepared_sentiment  REAL,
    qa_sentiment        REAL,
    linguistic_score    REAL,
    guidance_signal     TEXT,
    guidance_score      REAL,
    beat_quality        TEXT,
    pead_multiplier     REAL,
    raw_scores          TEXT,  -- JSON
    analysed_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS patent_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    market          TEXT NOT NULL,
    patent_number   TEXT,
    filing_date     TEXT,
    grant_date      TEXT,
    cpc_class       TEXT,
    citations       INTEGER DEFAULT 0,
    velocity_score  REAL,
    innovation_score REAL,
    collected_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS supply_chain (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_ticker TEXT,
    customer_ticker TEXT,
    dependency_weight REAL,
    relationship_type TEXT,  -- 'supplier_to'|'customer_of'
    depth           INTEGER DEFAULT 1,
    source          TEXT,
    upstream_risk   REAL,
    downstream_risk REAL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS congressional (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    member          TEXT NOT NULL,
    chamber         TEXT,
    ticker          TEXT NOT NULL,
    transaction_type TEXT,  -- 'buy'|'sell'|'option_exercise'
    amount_min      REAL,
    amount_max      REAL,
    transaction_date TEXT,
    filing_date     TEXT,
    delay_days      INTEGER,
    signal_strength REAL,
    credibility     TEXT,
    committee_power REAL,
    collected_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS congressional_members (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    member          TEXT NOT NULL UNIQUE,
    chamber         TEXT,
    accuracy        REAL,
    excess_return   REAL,
    information_ratio REAL,
    total_trades    INTEGER DEFAULT 0,
    credibility     TEXT,
    credibility_score REAL,
    committees      TEXT,  -- JSON list
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS earnings_quality (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    earnings_date   TEXT NOT NULL,
    beat_quality    TEXT,
    quality_score   REAL,
    revenue_beat_pct REAL,
    eps_beat_pct    REAL,
    guidance_signal TEXT,
    pead_multiplier REAL,
    suppress_pead   INTEGER DEFAULT 0,
    details         TEXT,  -- JSON
    analysed_at     TEXT NOT NULL,
    UNIQUE(ticker, earnings_date)
);

CREATE TABLE IF NOT EXISTS deepdata_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    tier            INTEGER NOT NULL,
    direction       INTEGER,  -- +1 / -1
    confidence      REAL,
    confluence      REAL,
    pead_modifier   REAL,
    sources         TEXT,  -- JSON
    generated_at    TEXT NOT NULL,
    outcome_return  REAL,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS factor_exposures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    momentum        REAL,
    value           REAL,
    quality         REAL,
    size            REAL,
    volatility      REAL,
    earnings_quality_factor REAL,
    altdata_factor  REAL,
    supply_chain_factor REAL,
    congressional_factor REAL,
    raw             TEXT,  -- JSON of all factors
    computed_at     TEXT NOT NULL,
    UNIQUE(ticker, snapshot_date)
);

CREATE TABLE IF NOT EXISTS pattern_registry (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    modules         TEXT NOT NULL,  -- JSON list
    sharpe          REAL,
    p_value         REAL,
    permutation_pct REAL,
    dsr             REAL,
    nonsense_score  REAL,
    status          TEXT DEFAULT 'candidate',
    economic_story  TEXT,
    found_at        TEXT NOT NULL,
    last_checked    TEXT
);

CREATE INDEX IF NOT EXISTS idx_options_ticker   ON options_flow(ticker, collected_at);
CREATE INDEX IF NOT EXISTS idx_si_ticker        ON short_interest(ticker, report_date);
CREATE INDEX IF NOT EXISTS idx_congress_ticker  ON congressional(ticker, transaction_date);
CREATE INDEX IF NOT EXISTS idx_signals_ticker   ON deepdata_signals(ticker, generated_at);
CREATE INDEX IF NOT EXISTS idx_eq_ticker        ON earnings_quality(ticker, earnings_date);
CREATE INDEX IF NOT EXISTS idx_factors_ticker   ON factor_exposures(ticker, snapshot_date);
"""


class DeepDataStore:
    """Thread-safe SQLite store for the deepdata intelligence pipeline."""

    def __init__(self, config: dict):
        cfg = config.get("deepdata", {})
        db_path = cfg.get("storage_path", "deepdata/storage/deepdata.db")
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()
        logger.info(f"DeepDataStore initialised at {self._path}")

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
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        return self._local.conn

    def _init_db(self) -> None:
        self._conn().executescript(_CREATE_SQL)
        self._conn().commit()

    # ------------------------------------------------------------------
    # Options flow
    # ------------------------------------------------------------------

    def store_options_flow(self, ticker: str, market: str, data: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO options_flow
               (ticker, market, smfi, iv_rank, put_call_ratio, net_gamma,
                dark_pool_score, unusual_activity, raw, collected_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, market,
                data.get("smfi"), data.get("iv_rank"), data.get("put_call_ratio"),
                data.get("net_gamma"), data.get("dark_pool_score"),
                int(data.get("unusual_activity", False)),
                json.dumps(data.get("raw", {})), now,
            ),
        )
        conn.commit()

    def get_options_flow(self, ticker: str, hours_back: int = 48) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        rows = self._conn().execute(
            "SELECT * FROM options_flow WHERE ticker=? AND collected_at>=? ORDER BY collected_at DESC",
            (ticker, cutoff),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_unusual_options(self, hours_back: int = 24, limit: int = 20) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        rows = self._conn().execute(
            """SELECT * FROM options_flow
               WHERE collected_at>=? AND unusual_activity=1
               ORDER BY smfi DESC LIMIT ?""",
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Short interest
    # ------------------------------------------------------------------

    def store_short_interest(self, ticker: str, market: str, data: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT INTO short_interest
               (ticker, market, short_float_pct, days_to_cover, short_ratio,
                si_change_pct, trend, squeeze_score, squeeze_flag, report_date, collected_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, market,
                data.get("short_float_pct"), data.get("days_to_cover"),
                data.get("short_ratio"), data.get("si_change_pct"),
                data.get("trend"), data.get("squeeze_score"),
                int(data.get("squeeze_flag", False)),
                data.get("report_date"), now,
            ),
        )
        conn.commit()

    def get_squeeze_candidates(self, min_score: float = 60) -> List[Dict]:
        rows = self._conn().execute(
            """SELECT * FROM short_interest WHERE squeeze_score >= ?
               ORDER BY squeeze_score DESC""",
            (min_score,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Transcripts
    # ------------------------------------------------------------------

    def store_transcript(self, ticker: str, earnings_date: str, scores: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO transcripts
               (ticker, earnings_date, source, hedge_ratio, forward_ratio, we_ratio,
                passive_ratio, tone_shift, deflection_score, prepared_sentiment,
                qa_sentiment, linguistic_score, guidance_signal, guidance_score,
                beat_quality, pead_multiplier, raw_scores, analysed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, earnings_date, scores.get("source"),
                scores.get("hedge_ratio"), scores.get("forward_ratio"),
                scores.get("we_ratio"), scores.get("passive_ratio"),
                scores.get("tone_shift"), scores.get("deflection_score"),
                scores.get("prepared_sentiment"), scores.get("qa_sentiment"),
                scores.get("linguistic_score"), scores.get("guidance_signal"),
                scores.get("guidance_score"), scores.get("beat_quality"),
                scores.get("pead_multiplier"),
                json.dumps(scores), now,
            ),
        )
        conn.commit()

    def get_recent_transcripts(self, days_back: int = 30, limit: int = 20) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        rows = self._conn().execute(
            "SELECT * FROM transcripts WHERE analysed_at>=? ORDER BY analysed_at DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("raw_scores"):
                try:
                    d["raw_scores"] = json.loads(d["raw_scores"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Congressional
    # ------------------------------------------------------------------

    def store_congressional(self, disclosure: dict) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO congressional
               (member, chamber, ticker, transaction_type, amount_min, amount_max,
                transaction_date, filing_date, delay_days, signal_strength, credibility,
                committee_power, collected_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                disclosure.get("member"), disclosure.get("chamber"),
                disclosure.get("ticker"), disclosure.get("transaction_type"),
                disclosure.get("amount_min"), disclosure.get("amount_max"),
                disclosure.get("transaction_date"), disclosure.get("filing_date"),
                disclosure.get("delay_days"), disclosure.get("signal_strength"),
                disclosure.get("credibility"), disclosure.get("committee_power"), now,
            ),
        )
        conn.commit()
        return cur.lastrowid

    def get_recent_congressional(self, days_back: int = 30,
                                  ticker: Optional[str] = None) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
        if ticker:
            rows = self._conn().execute(
                "SELECT * FROM congressional WHERE ticker=? AND transaction_date>=? ORDER BY transaction_date DESC",
                (ticker, cutoff),
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM congressional WHERE transaction_date>=? ORDER BY transaction_date DESC",
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_member(self, member: str, record: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO congressional_members
               (member, chamber, accuracy, excess_return, information_ratio,
                total_trades, credibility, credibility_score, committees, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                member, record.get("chamber"),
                record.get("accuracy"), record.get("excess_return"),
                record.get("information_ratio"), record.get("total_trades", 0),
                record.get("credibility"), record.get("credibility_score"),
                json.dumps(record.get("committees", [])), now,
            ),
        )
        conn.commit()

    def get_all_members(self) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM congressional_members ORDER BY credibility_score DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("committees"):
                try:
                    d["committees"] = json.loads(d["committees"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Earnings quality
    # ------------------------------------------------------------------

    def store_earnings_quality(self, ticker: str, earnings_date: str,
                                classification: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO earnings_quality
               (ticker, earnings_date, beat_quality, quality_score, revenue_beat_pct,
                eps_beat_pct, guidance_signal, pead_multiplier, suppress_pead,
                details, analysed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, earnings_date,
                classification.get("beat_quality"),
                classification.get("quality_score"),
                classification.get("revenue_beat_pct"),
                classification.get("eps_beat_pct"),
                classification.get("guidance_signal"),
                classification.get("final_pead_multiplier"),
                int(classification.get("suppress_pead", False)),
                json.dumps(classification), now,
            ),
        )
        conn.commit()

    def get_pead_modifier(self, ticker: str) -> Optional[float]:
        """Get the most recent PEAD multiplier for a ticker."""
        row = self._conn().execute(
            "SELECT pead_multiplier, suppress_pead FROM earnings_quality WHERE ticker=? ORDER BY earnings_date DESC LIMIT 1",
            (ticker,),
        ).fetchone()
        if row:
            if row["suppress_pead"]:
                return 0.0
            return row["pead_multiplier"]
        return None

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def log_signal(self, signal: dict) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        cur = conn.execute(
            """INSERT INTO deepdata_signals
               (ticker, signal_type, tier, direction, confidence, confluence,
                pead_modifier, sources, generated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                signal.get("ticker"), signal.get("signal_type"),
                signal.get("tier", 3), signal.get("direction"),
                signal.get("confidence"), signal.get("confluence"),
                signal.get("pead_modifier"),
                json.dumps(signal.get("sources", [])), now,
            ),
        )
        conn.commit()
        return cur.lastrowid

    def get_recent_signals(self, hours_back: int = 48, tier: Optional[int] = None,
                            limit: int = 50) -> List[Dict]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        if tier:
            rows = self._conn().execute(
                "SELECT * FROM deepdata_signals WHERE generated_at>=? AND tier=? ORDER BY generated_at DESC LIMIT ?",
                (cutoff, tier, limit),
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM deepdata_signals WHERE generated_at>=? ORDER BY generated_at DESC LIMIT ?",
                (cutoff, limit),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("sources"):
                try:
                    d["sources"] = json.loads(d["sources"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Factor exposures
    # ------------------------------------------------------------------

    def store_factors(self, ticker: str, snapshot_date: str, factors: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO factor_exposures
               (ticker, snapshot_date, momentum, value, quality, size, volatility,
                earnings_quality_factor, altdata_factor, supply_chain_factor,
                congressional_factor, raw, computed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, snapshot_date,
                factors.get("momentum"), factors.get("value"),
                factors.get("quality"), factors.get("size"),
                factors.get("volatility"),
                factors.get("earnings_quality_factor"),
                factors.get("altdata_factor"),
                factors.get("supply_chain_factor"),
                factors.get("congressional_factor"),
                json.dumps(factors), now,
            ),
        )
        conn.commit()

    # ------------------------------------------------------------------
    # Pattern registry
    # ------------------------------------------------------------------

    def store_pattern(self, pattern: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            """INSERT OR REPLACE INTO pattern_registry
               (name, modules, sharpe, p_value, permutation_pct, dsr, nonsense_score,
                status, economic_story, found_at, last_checked)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                pattern.get("name"),
                json.dumps(pattern.get("modules", [])),
                pattern.get("sharpe"), pattern.get("p_value"),
                pattern.get("permutation_pct"), pattern.get("dsr"),
                pattern.get("nonsense_score"),
                pattern.get("status", "candidate"),
                pattern.get("economic_story"), now, now,
            ),
        )
        conn.commit()

    def get_live_patterns(self) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM pattern_registry WHERE status='live' ORDER BY sharpe DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("modules"):
                try:
                    d["modules"] = json.loads(d["modules"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Status summary
    # ------------------------------------------------------------------

    def status_summary(self) -> Dict:
        conn = self._conn()
        return {
            "options_records_24h": conn.execute(
                "SELECT COUNT(*) FROM options_flow WHERE collected_at >= datetime('now','-1 day')"
            ).fetchone()[0],
            "squeeze_candidates": conn.execute(
                "SELECT COUNT(*) FROM short_interest WHERE squeeze_score >= 60"
            ).fetchone()[0],
            "transcripts_30d": conn.execute(
                "SELECT COUNT(*) FROM transcripts WHERE analysed_at >= datetime('now','-30 days')"
            ).fetchone()[0],
            "congressional_30d": conn.execute(
                "SELECT COUNT(*) FROM congressional WHERE transaction_date >= date('now','-30 days')"
            ).fetchone()[0],
            "tier1_signals_24h": conn.execute(
                "SELECT COUNT(*) FROM deepdata_signals WHERE tier=1 AND generated_at >= datetime('now','-1 day')"
            ).fetchone()[0],
            "live_patterns": conn.execute(
                "SELECT COUNT(*) FROM pattern_registry WHERE status='live'"
            ).fetchone()[0],
        }

    # ------------------------------------------------------------------
    # Backup / close
    # ------------------------------------------------------------------

    def backup(self, backup_dir: str = "output/backups") -> str:
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        dest = os.path.join(backup_dir, f"deepdata_{stamp}.db")
        shutil.copy2(str(self._path), dest)
        logger.info(f"DeepDataStore backed up to {dest}")
        return dest

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
