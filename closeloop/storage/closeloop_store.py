"""
ClosedLoopStore — SQLite WAL-mode persistent store for the closed-loop
learning and trading intelligence system.

All tables are created on first connect; never dropped automatically.
Thread-safe: one connection per thread via threading.local().
"""
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_CREATE_TABLES = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS trade_ledger (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                    TEXT NOT NULL,
    market                    TEXT NOT NULL,
    direction                 INTEGER NOT NULL,        -- +1 / -1
    entry_date                TEXT,
    exit_date                 TEXT,
    entry_price               REAL,
    exit_price                REAL,
    position_size             REAL,
    gross_pnl                 REAL,
    net_pnl                   REAL,
    fees_paid                 REAL DEFAULT 0,
    holding_days              INTEGER,
    exit_reason               TEXT,
    signals_at_entry          TEXT,                   -- JSON
    macro_regime              TEXT,
    vix_level                 REAL,
    umci_score                REAL,
    lunar_phase               REAL,
    geomagnetic_kp            REAL,
    market_cap_usd            REAL,
    sector                    TEXT,
    entry_timing_score        REAL,
    scale_in_tranche          INTEGER DEFAULT 0,
    peer_influence_score      REAL DEFAULT 0,
    analyst_revision_score    REAL DEFAULT 0,
    academic_tailwind_score   REAL DEFAULT 0,
    news_context_score        REAL DEFAULT 0,
    index_rebalancing_pressure REAL DEFAULT 0,
    merger_spillover_flag     INTEGER DEFAULT 0,
    was_profitable            INTEGER,
    pnl_pct                   REAL,
    annualised_return         REAL,
    pnl_attributed            INTEGER DEFAULT 0,
    attribution_complete      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pnl_attribution (
    id                            INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id                      INTEGER NOT NULL,
    signal_name                   TEXT NOT NULL,
    signal_source_module          TEXT,
    signal_strength_at_entry      REAL,
    signal_direction              INTEGER,
    attributed_pnl                REAL,
    attributed_pnl_pct            REAL,
    was_signal_correct            INTEGER,
    counterfactual_pnl_without_signal REAL,
    created_at                    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS entry_timing_outcomes (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id                  INTEGER NOT NULL,
    ticker                    TEXT NOT NULL,
    intended_entry_price      REAL,
    actual_entry_price        REAL,
    entry_timing_score        REAL,
    waited_days               INTEGER DEFAULT 0,
    scale_in_tranche          INTEGER DEFAULT 0,
    tranche_entry_price       REAL,
    pnl_vs_immediate_entry    REAL,
    entry_method              TEXT,
    entry_conditions_met      TEXT,                   -- JSON list
    created_at                TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS signal_regime_performance (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name   TEXT NOT NULL,
    macro_regime  TEXT NOT NULL,
    vix_bucket    TEXT NOT NULL,
    n_trades      INTEGER DEFAULT 0,
    win_rate      REAL,
    mean_pnl      REAL,
    sharpe        REAL,
    best_trade_pnl  REAL,
    worst_trade_pnl REAL,
    last_updated  TEXT DEFAULT (datetime('now')),
    UNIQUE(signal_name, macro_regime, vix_bucket)
);

CREATE TABLE IF NOT EXISTS signal_interactions (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_combination          TEXT NOT NULL UNIQUE,
    n_occurrences               INTEGER DEFAULT 0,
    win_rate                    REAL,
    mean_pnl                    REAL,
    sharpe                      REAL,
    vs_single_signal_improvement REAL,
    last_updated                TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS peer_influence_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_ticker      TEXT NOT NULL,
    trigger_event       TEXT,
    influenced_ticker   TEXT NOT NULL,
    influence_type      TEXT,
    predicted_direction INTEGER,
    actual_direction    INTEGER,
    predicted_magnitude REAL,
    actual_magnitude    REAL,
    lag_days            INTEGER,
    was_correct         INTEGER,
    pnl                 REAL,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analyst_revision_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    revision_type       TEXT,
    revision_magnitude  REAL,
    revision_date       TEXT,
    forward_return_5d   REAL,
    forward_return_20d  REAL,
    pnl_if_traded       REAL,
    combined_with_pead  INTEGER DEFAULT 0,
    pead_improved       INTEGER,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS academic_company_matches (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT NOT NULL,
    paper_id                TEXT,
    paper_title             TEXT,
    relevance_score         REAL,
    citation_velocity       REAL,
    matched_date            TEXT,
    subsequent_return_90d   REAL,
    was_predictive          INTEGER,
    created_at              TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stress_learning_outcomes (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_name               TEXT NOT NULL,
    run_date                    TEXT NOT NULL,
    predicted_loss_pct          REAL,
    actual_loss_pct_if_occurred REAL,
    signals_flagged_vulnerable  TEXT,  -- JSON list
    signals_actually_failed     TEXT,  -- JSON list
    prediction_accuracy         REAL,
    used_for_weight_update      INTEGER DEFAULT 0,
    created_at                  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stress_predictions (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_name               TEXT NOT NULL,
    generated_date              TEXT NOT NULL,
    confidence                  REAL,
    predicted_loss_pct          REAL,
    predicted_vulnerable_signals TEXT, -- JSON list
    prediction_basis_n_trades   INTEGER,
    outcome_date                TEXT,
    outcome_actual_loss         REAL,
    created_at                  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS drawdown_events (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    start_date              TEXT,
    end_date                TEXT,
    trough_date             TEXT,
    drawdown_pct            REAL,
    recovery_days           INTEGER,
    contributing_signals    TEXT,  -- JSON
    contributing_regimes    TEXT,  -- JSON
    contributing_sectors    TEXT,  -- JSON
    contributing_markets    TEXT,  -- JSON
    forensics_complete      INTEGER DEFAULT 0,
    forensics_report        TEXT,
    failure_type            TEXT,
    prevention_applied      TEXT,
    created_at              TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS signal_weights (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name     TEXT NOT NULL UNIQUE,
    weight          REAL NOT NULL DEFAULT 1.0,
    previous_weight REAL,
    change_reason   TEXT,
    changed_at      TEXT DEFAULT (datetime('now')),
    n_trades_basis  INTEGER DEFAULT 0,
    sharpe_basis    REAL,
    auto_updated    INTEGER DEFAULT 0,
    approved        INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS weight_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name         TEXT NOT NULL,
    old_weight          REAL,
    new_weight          REAL,
    change_pct          REAL,
    changed_at          TEXT DEFAULT (datetime('now')),
    trigger             TEXT,
    performance_before  REAL,
    performance_after   REAL,
    was_beneficial      INTEGER
);

CREATE TABLE IF NOT EXISTS tax_ledger (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    tax_year                    TEXT NOT NULL,
    trade_id                    INTEGER,
    ticker                      TEXT NOT NULL,
    gain_loss_gbp               REAL,
    disposal_date               TEXT,
    acquisition_date            TEXT,
    section_104_pool            REAL,
    bed_and_breakfast_flag      INTEGER DEFAULT 0,
    cumulative_gains_this_year  REAL DEFAULT 0,
    remaining_allowance         REAL,
    created_at                  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS benchmark_performance (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    date                        TEXT NOT NULL UNIQUE,
    portfolio_value             REAL,
    benchmark_us_value          REAL,
    benchmark_uk_value          REAL,
    benchmark_smallcap_value    REAL,
    active_return_us            REAL,
    active_return_uk            REAL,
    information_ratio_rolling_252d REAL,
    tracking_error              REAL,
    created_at                  TEXT DEFAULT (datetime('now'))
);
"""


class ClosedLoopStore:
    """Thread-safe SQLite store for all closed-loop learning data."""

    def __init__(self, config: Optional[Dict] = None):
        cfg = (config or {}).get("closeloop", {})
        db_path = cfg.get("storage_path", "closeloop/storage/closeloop.db")
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(_CREATE_TABLES)
        conn.commit()

    # ------------------------------------------------------------------
    # Trade ledger
    # ------------------------------------------------------------------

    def record_trade(self, trade: Dict, entry_context: Optional[Dict] = None) -> int:
        ctx = entry_context or {}
        signals_json = json.dumps(ctx.get("signals_at_entry", {}))
        conn = self._conn()
        pnl = trade.get("net_pnl", 0.0)
        entry_price = trade.get("entry_price", 1.0) or 1.0
        hold = trade.get("holding_days", 0) or 0
        pnl_pct = pnl / (entry_price * trade.get("position_size", 1.0)) if entry_price else 0.0
        ann_ret = pnl_pct * (252.0 / max(hold, 1))

        cur = conn.execute("""
            INSERT INTO trade_ledger (
                ticker, market, direction,
                entry_date, exit_date, entry_price, exit_price,
                position_size, gross_pnl, net_pnl, fees_paid,
                holding_days, exit_reason,
                signals_at_entry, macro_regime, vix_level, umci_score,
                lunar_phase, geomagnetic_kp, market_cap_usd, sector,
                entry_timing_score, scale_in_tranche,
                peer_influence_score, analyst_revision_score,
                academic_tailwind_score, news_context_score,
                index_rebalancing_pressure, merger_spillover_flag,
                was_profitable, pnl_pct, annualised_return
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade.get("ticker"), trade.get("market", "us"), trade.get("direction", 1),
            trade.get("entry_date"), trade.get("exit_date"),
            trade.get("entry_price"), trade.get("exit_price"),
            trade.get("position_size"), trade.get("gross_pnl", pnl), pnl,
            trade.get("fees_paid", 0.0), hold, trade.get("exit_reason"),
            signals_json, ctx.get("macro_regime"), ctx.get("vix_level"),
            ctx.get("umci_score"), ctx.get("lunar_phase"), ctx.get("geomagnetic_kp"),
            ctx.get("market_cap_usd"), ctx.get("sector"),
            ctx.get("entry_timing_score"), ctx.get("scale_in_tranche", 0),
            ctx.get("peer_influence_score", 0.0), ctx.get("analyst_revision_score", 0.0),
            ctx.get("academic_tailwind_score", 0.0), ctx.get("news_context_score", 0.0),
            ctx.get("index_rebalancing_pressure", 0.0), int(ctx.get("merger_spillover_flag", False)),
            int(pnl > 0), round(pnl_pct, 6), round(ann_ret, 6),
        ))
        conn.commit()
        return cur.lastrowid

    def get_trades(self, n: int = 252, market: Optional[str] = None, limit: int = None) -> List[Dict]:
        if limit is not None:
            n = limit
        q = "SELECT * FROM trade_ledger"
        params: list = []
        if market:
            q += " WHERE market=?"
            params.append(market)
        q += " ORDER BY id DESC LIMIT ?"
        params.append(n)
        rows = self._conn().execute(q, params).fetchall()
        return [dict(r) for r in rows]

    def log_autopsy_complete(self, trade_id: int) -> None:
        self._conn().execute(
            "UPDATE trade_ledger SET attribution_complete=1 WHERE id=?", (trade_id,)
        )
        self._conn().commit()

    # ------------------------------------------------------------------
    # PnL attribution
    # ------------------------------------------------------------------

    def record_attribution(self, trade_id: int, attrs: List[Dict]) -> None:
        conn = self._conn()
        for a in attrs:
            conn.execute("""
                INSERT INTO pnl_attribution
                (trade_id, signal_name, signal_source_module,
                 signal_strength_at_entry, signal_direction,
                 attributed_pnl, attributed_pnl_pct,
                 was_signal_correct, counterfactual_pnl_without_signal)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                trade_id, a.get("signal_name"), a.get("signal_source_module"),
                a.get("signal_strength", 0.0), a.get("signal_direction", 0),
                a.get("attributed_pnl", 0.0), a.get("attributed_pnl_pct", 0.0),
                int(a.get("was_correct", False)), a.get("counterfactual_pnl"),
            ))
        conn.execute("UPDATE trade_ledger SET pnl_attributed=1 WHERE id=?", (trade_id,))
        conn.commit()

    def get_signal_scorecard(self, signal_name: str) -> Dict:
        rows = self._conn().execute("""
            SELECT COUNT(*) as n, SUM(attributed_pnl) as total_pnl,
                   AVG(attributed_pnl) as mean_pnl,
                   SUM(CASE WHEN was_signal_correct=1 THEN 1 ELSE 0 END) as wins
            FROM pnl_attribution WHERE signal_name=?
        """, (signal_name,)).fetchone()
        if not rows or rows["n"] == 0:
            return {"signal_name": signal_name, "n_trades": 0}
        n = rows["n"]
        return {
            "signal_name": signal_name,
            "n_trades": n,
            "total_pnl": rows["total_pnl"],
            "mean_pnl": rows["mean_pnl"],
            "win_rate": rows["wins"] / n,
        }

    # ------------------------------------------------------------------
    # Entry timing
    # ------------------------------------------------------------------

    def record_entry_timing(self, trade_id: int, timing: Dict) -> None:
        conn = self._conn()
        conn.execute("""
            INSERT INTO entry_timing_outcomes
            (trade_id, ticker, intended_entry_price, actual_entry_price,
             entry_timing_score, waited_days, scale_in_tranche,
             tranche_entry_price, pnl_vs_immediate_entry,
             entry_method, entry_conditions_met)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade_id, timing.get("ticker"),
            timing.get("intended_entry_price"), timing.get("actual_entry_price"),
            timing.get("entry_timing_score"), timing.get("waited_days", 0),
            timing.get("scale_in_tranche", 0), timing.get("tranche_entry_price"),
            timing.get("pnl_vs_immediate_entry"),
            timing.get("entry_method"),
            json.dumps(timing.get("entry_conditions_met", [])),
        ))
        conn.commit()

    # ------------------------------------------------------------------
    # Signal weights
    # ------------------------------------------------------------------

    def get_signal_weight(self, signal_name: str, default: float = 1.0) -> float:
        row = self._conn().execute(
            "SELECT weight FROM signal_weights WHERE signal_name=?", (signal_name,)
        ).fetchone()
        return row["weight"] if row else default

    def set_signal_weight(
        self,
        signal_name: str,
        new_weight: float,
        reason: str = "",
        n_trades: int = 0,
        sharpe: float = 0.0,
        auto: bool = True,
    ) -> None:
        conn = self._conn()
        existing = conn.execute(
            "SELECT weight FROM signal_weights WHERE signal_name=?", (signal_name,)
        ).fetchone()
        old_weight = existing["weight"] if existing else 1.0

        conn.execute("""
            INSERT INTO signal_weights
                (signal_name, weight, previous_weight, change_reason,
                 n_trades_basis, sharpe_basis, auto_updated)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(signal_name) DO UPDATE SET
                weight=excluded.weight,
                previous_weight=signal_weights.weight,
                change_reason=excluded.change_reason,
                changed_at=datetime('now'),
                n_trades_basis=excluded.n_trades_basis,
                sharpe_basis=excluded.sharpe_basis,
                auto_updated=excluded.auto_updated
        """, (signal_name, new_weight, old_weight, reason, n_trades, sharpe, int(auto)))

        conn.execute("""
            INSERT INTO weight_history
                (signal_name, old_weight, new_weight, change_pct, trigger)
            VALUES (?,?,?,?,?)
        """, (
            signal_name, old_weight, new_weight,
            (new_weight - old_weight) / max(abs(old_weight), 1e-9) * 100,
            reason,
        ))
        conn.commit()

    def get_all_weights(self) -> Dict[str, float]:
        rows = self._conn().execute("SELECT signal_name, weight FROM signal_weights").fetchall()
        return {r["signal_name"]: r["weight"] for r in rows}

    # ------------------------------------------------------------------
    # Signal regime performance
    # ------------------------------------------------------------------

    def upsert_signal_regime_perf(
        self, signal_name: str, macro_regime: str, vix_bucket: str,
        n_trades: int, win_rate: float, mean_pnl: float, sharpe: float,
        best: float, worst: float,
    ) -> None:
        self._conn().execute("""
            INSERT INTO signal_regime_performance
                (signal_name, macro_regime, vix_bucket, n_trades, win_rate,
                 mean_pnl, sharpe, best_trade_pnl, worst_trade_pnl)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(signal_name, macro_regime, vix_bucket) DO UPDATE SET
                n_trades=excluded.n_trades,
                win_rate=excluded.win_rate,
                mean_pnl=excluded.mean_pnl,
                sharpe=excluded.sharpe,
                best_trade_pnl=excluded.best_trade_pnl,
                worst_trade_pnl=excluded.worst_trade_pnl,
                last_updated=datetime('now')
        """, (signal_name, macro_regime, vix_bucket, n_trades, win_rate, mean_pnl, sharpe, best, worst))
        self._conn().commit()

    def get_regime_weight_multiplier(
        self, signal_name: str, macro_regime: str, vix_bucket: str
    ) -> float:
        row = self._conn().execute("""
            SELECT sharpe,
                   AVG(sharpe) OVER () AS mean_sharpe
            FROM signal_regime_performance
            WHERE signal_name=? AND macro_regime=? AND vix_bucket=?
        """, (signal_name, macro_regime, vix_bucket)).fetchone()
        if not row or not row["mean_sharpe"] or row["mean_sharpe"] == 0:
            return 1.0
        return max(0.1, min(3.0, (row["sharpe"] or 0.0) / row["mean_sharpe"]))

    # ------------------------------------------------------------------
    # Signal interactions
    # ------------------------------------------------------------------

    def upsert_signal_interaction(self, combo: str, n: int, win_rate: float,
                                   mean_pnl: float, sharpe: float, vs_single: float) -> None:
        self._conn().execute("""
            INSERT INTO signal_interactions
                (signal_combination, n_occurrences, win_rate, mean_pnl,
                 sharpe, vs_single_signal_improvement)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(signal_combination) DO UPDATE SET
                n_occurrences=excluded.n_occurrences,
                win_rate=excluded.win_rate,
                mean_pnl=excluded.mean_pnl,
                sharpe=excluded.sharpe,
                vs_single_signal_improvement=excluded.vs_single_signal_improvement,
                last_updated=datetime('now')
        """, (combo, n, win_rate, mean_pnl, sharpe, vs_single))
        self._conn().commit()

    def get_interaction_multiplier(self, signal_names: List[str]) -> float:
        combo = "|".join(sorted(signal_names))
        row = self._conn().execute(
            "SELECT win_rate, n_occurrences FROM signal_interactions WHERE signal_combination=?",
            (combo,)
        ).fetchone()
        if not row or row["n_occurrences"] < 5:
            return 1.0
        if row["win_rate"] > 0.65:
            return 1.2
        if row["win_rate"] < 0.40:
            return 0.5
        return 1.0

    # ------------------------------------------------------------------
    # Drawdown events
    # ------------------------------------------------------------------

    def record_drawdown(self, event: Dict) -> int:
        cur = self._conn().execute("""
            INSERT INTO drawdown_events
                (start_date, end_date, trough_date, drawdown_pct, recovery_days,
                 contributing_signals, contributing_regimes, contributing_sectors,
                 contributing_markets, failure_type, forensics_report)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event.get("start_date"), event.get("end_date"), event.get("trough_date"),
            event.get("drawdown_pct"), event.get("recovery_days"),
            json.dumps(event.get("contributing_signals", [])),
            json.dumps(event.get("contributing_regimes", [])),
            json.dumps(event.get("contributing_sectors", [])),
            json.dumps(event.get("contributing_markets", [])),
            event.get("failure_type"), event.get("forensics_report"),
        ))
        self._conn().commit()
        return cur.lastrowid

    # ------------------------------------------------------------------
    # Stress tests
    # ------------------------------------------------------------------

    def record_stress_outcome(self, outcome: Dict) -> None:
        self._conn().execute("""
            INSERT INTO stress_learning_outcomes
                (scenario_name, run_date, predicted_loss_pct,
                 signals_flagged_vulnerable, signals_actually_failed,
                 prediction_accuracy)
            VALUES (?,?,?,?,?,?)
        """, (
            outcome.get("scenario_name"), outcome.get("run_date"),
            outcome.get("predicted_loss_pct"),
            json.dumps(outcome.get("signals_flagged_vulnerable", [])),
            json.dumps(outcome.get("signals_actually_failed", [])),
            outcome.get("prediction_accuracy"),
        ))
        self._conn().commit()

    def record_stress_prediction(self, pred: Dict) -> None:
        self._conn().execute("""
            INSERT INTO stress_predictions
                (scenario_name, generated_date, confidence, predicted_loss_pct,
                 predicted_vulnerable_signals, prediction_basis_n_trades)
            VALUES (?,?,?,?,?,?)
        """, (
            pred.get("scenario_name"),
            pred.get("generated_date", datetime.now(timezone.utc).isoformat()),
            pred.get("confidence"), pred.get("predicted_loss_pct"),
            json.dumps(pred.get("predicted_vulnerable_signals", [])),
            pred.get("prediction_basis_n_trades", 0),
        ))
        self._conn().commit()

    # ------------------------------------------------------------------
    # Tax ledger
    # ------------------------------------------------------------------

    def record_tax_disposal(self, disposal: Dict) -> None:
        self._conn().execute("""
            INSERT INTO tax_ledger
                (tax_year, trade_id, ticker, gain_loss_gbp, disposal_date,
                 acquisition_date, section_104_pool, bed_and_breakfast_flag,
                 cumulative_gains_this_year, remaining_allowance)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            disposal.get("tax_year"), disposal.get("trade_id"),
            disposal.get("ticker"), disposal.get("gain_loss_gbp"),
            disposal.get("disposal_date"), disposal.get("acquisition_date"),
            disposal.get("section_104_pool"), int(disposal.get("bb_flag", False)),
            disposal.get("cumulative_gains", 0.0), disposal.get("remaining_allowance"),
        ))
        self._conn().commit()

    def get_ytd_gains(self, tax_year: str) -> float:
        row = self._conn().execute(
            "SELECT SUM(gain_loss_gbp) as total FROM tax_ledger WHERE tax_year=?",
            (tax_year,)
        ).fetchone()
        return row["total"] or 0.0

    # ------------------------------------------------------------------
    # Benchmark
    # ------------------------------------------------------------------

    def record_benchmark(self, date: str, portfolio_value: float, benchmarks: Dict) -> None:
        self._conn().execute("""
            INSERT OR REPLACE INTO benchmark_performance
                (date, portfolio_value, benchmark_us_value, benchmark_uk_value,
                 benchmark_smallcap_value, active_return_us, active_return_uk,
                 information_ratio_rolling_252d, tracking_error)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            date, portfolio_value,
            benchmarks.get("us"), benchmarks.get("uk"), benchmarks.get("smallcap"),
            benchmarks.get("active_return_us"), benchmarks.get("active_return_uk"),
            benchmarks.get("ir_252"), benchmarks.get("tracking_error"),
        ))
        self._conn().commit()

    def get_benchmark_history(self, n: int = 252) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM benchmark_performance ORDER BY date DESC LIMIT ?", (n,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Status summary
    # ------------------------------------------------------------------

    def status_summary(self) -> Dict:
        conn = self._conn()
        tables = {
            "trades": "SELECT COUNT(*) FROM trade_ledger",
            "attributed": "SELECT COUNT(*) FROM pnl_attribution",
            "weights": "SELECT COUNT(*) FROM signal_weights",
            "drawdowns": "SELECT COUNT(*) FROM drawdown_events",
            "stress_runs": "SELECT COUNT(*) FROM stress_learning_outcomes",
            "tax_records": "SELECT COUNT(*) FROM tax_ledger",
            "benchmark_days": "SELECT COUNT(*) FROM benchmark_performance",
        }
        result = {"db_path": str(self._db_path)}
        for key, q in tables.items():
            try:
                result[key] = conn.execute(q).fetchone()[0]
            except Exception:
                result[key] = 0

        # Latest trade
        row = conn.execute(
            "SELECT ticker, exit_date, net_pnl FROM trade_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            t = dict(row)
            result["last_trade"] = f"{t.get('ticker','?')} {t.get('exit_date','?')} pnl={t.get('net_pnl',0):.2f}"

        # Win rate
        row = conn.execute(
            "SELECT AVG(was_profitable) FROM trade_ledger WHERE attribution_complete=1"
        ).fetchone()
        result["overall_win_rate"] = round((row[0] or 0.0) * 100, 1)

        return result

    # ------------------------------------------------------------------
    # Peer influence
    # ------------------------------------------------------------------

    def record_peer_influence(self, outcome: Dict) -> None:
        self._conn().execute("""
            INSERT INTO peer_influence_outcomes
                (trigger_ticker, trigger_event, influenced_ticker, influence_type,
                 predicted_direction, actual_direction,
                 predicted_magnitude, actual_magnitude, lag_days, was_correct, pnl)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            outcome.get("trigger_ticker"), outcome.get("trigger_event"),
            outcome.get("influenced_ticker"), outcome.get("influence_type"),
            outcome.get("predicted_direction"), outcome.get("actual_direction"),
            outcome.get("predicted_magnitude"), outcome.get("actual_magnitude"),
            outcome.get("lag_days"), int(outcome.get("was_correct", False)),
            outcome.get("pnl"),
        ))
        self._conn().commit()

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup(self, backup_dir: str = "backups/closeloop") -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dest = Path(backup_dir) / f"closeloop_{ts}.db"
        dest.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy2(str(self._db_path), str(dest))
        logger.info(f"ClosedLoopStore backed up to {dest}")
        return str(dest)

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    # ------------------------------------------------------------------
    # Compatibility / convenience methods
    # ------------------------------------------------------------------

    def get_all_signal_weights(self) -> List[Dict]:
        """Return all signal weights as list of dicts."""
        rows = self._conn().execute(
            "SELECT signal_name, weight, changed_at FROM signal_weights ORDER BY signal_name"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_attributions(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT signal_name, attributed_pnl, counterfactual_pnl_without_signal as counterfactual_pnl "
            "FROM pnl_attribution ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_entry_outcomes(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM entry_timing_outcomes ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_peer_outcomes(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM peer_influence_outcomes ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_analyst_outcomes(self, limit: int = 20) -> List[Dict]:
        """Return analyst revision outcomes from trade ledger (proxy via analyst_revision_score)."""
        rows = self._conn().execute(
            "SELECT ticker, entry_date, analyst_revision_score, net_pnl "
            "FROM trade_ledger WHERE analyst_revision_score IS NOT NULL AND analyst_revision_score != 0 "
            "ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_drawdown_events(self, limit: int = 10) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM drawdown_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_interactions(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT signal_combination as signal_combo, n_occurrences as n_trades, "
            "win_rate, mean_pnl, sharpe, "
            "(CASE WHEN win_rate > 0.65 THEN 1.2 WHEN win_rate < 0.40 THEN 0.5 ELSE 1.0 END) as multiplier "
            "FROM signal_interactions ORDER BY n_occurrences DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_weight_history(self, limit: int = 20) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT signal_name, old_weight, new_weight, trigger, changed_at as updated_at "
            "FROM weight_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_stress_outcomes(self, limit: int = 100) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM stress_learning_outcomes ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def reconcile_phantom_positions(self, real_tickers: set) -> int:
        """Mark open positions not in real_tickers as phantom_cleanup with zero PnL."""
        if not real_tickers:
            # If no real tickers provided, close everything open
            placeholders = ""
            params = []
        else:
            placeholders = ",".join("?" * len(real_tickers))
            params = list(real_tickers)
        conn = self._conn()
        if real_tickers:
            cursor = conn.execute(
                f"""UPDATE trade_ledger
                    SET exit_date = datetime('now'),
                        exit_price = entry_price,
                        net_pnl = 0,
                        gross_pnl = 0,
                        exit_reason = 'phantom_cleanup',
                        was_profitable = 0
                    WHERE exit_date IS NULL
                      AND ticker NOT IN ({placeholders})""",
                params,
            )
        else:
            cursor = conn.execute(
                """UPDATE trade_ledger
                   SET exit_date = datetime('now'),
                       exit_price = entry_price,
                       net_pnl = 0,
                       gross_pnl = 0,
                       exit_reason = 'phantom_cleanup',
                       was_profitable = 0
                   WHERE exit_date IS NULL"""
            )
        conn.commit()
        return cursor.rowcount

    # Overload record_stress_outcome to accept both dict and kwargs
    def record_stress_outcome(self, outcome: Dict = None, *, run_date: str = None,
                               scenario_name: str = None, weighted_stress_risk: float = None,
                               crisis_fragile: bool = None, top_scenario: str = None,
                               conditions: str = None) -> None:
        """Accept either a dict or keyword arguments."""
        if outcome is None:
            outcome = {
                "scenario_name": scenario_name,
                "run_date": run_date,
                "predicted_loss_pct": weighted_stress_risk,
                "signals_flagged_vulnerable": [top_scenario] if top_scenario else [],
                "signals_actually_failed": [],
                "prediction_accuracy": None,
            }
        self._conn().execute("""
            INSERT INTO stress_learning_outcomes
                (scenario_name, run_date, predicted_loss_pct,
                 signals_flagged_vulnerable, signals_actually_failed,
                 prediction_accuracy)
            VALUES (?,?,?,?,?,?)
        """, (
            outcome.get("scenario_name"), outcome.get("run_date"),
            outcome.get("predicted_loss_pct"),
            json.dumps(outcome.get("signals_flagged_vulnerable", [])),
            json.dumps(outcome.get("signals_actually_failed", [])),
            outcome.get("prediction_accuracy"),
        ))
        self._conn().commit()

    # Overload record_tax_disposal to accept both dict and kwargs
    def record_tax_disposal(self, disposal: Dict = None, *, ticker: str = None,
                             disposal_date: str = None, disposal_proceeds: float = None,
                             allowable_cost: float = None, gain: float = None,
                             disposal_type: str = None, pool_shares_after: float = None,
                             pool_cost_after: float = None) -> None:
        """Accept either a dict or keyword arguments."""
        if disposal is None:
            disposal = {
                "ticker": ticker,
                "disposal_date": disposal_date,
                "gain_loss_gbp": gain,
            }
        tax_year = str(datetime.now().year)
        self._conn().execute("""
            INSERT INTO tax_ledger
                (tax_year, trade_id, ticker, gain_loss_gbp, disposal_date,
                 acquisition_date, section_104_pool, bed_and_breakfast_flag,
                 cumulative_gains_this_year, remaining_allowance)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            disposal.get("tax_year", tax_year), disposal.get("trade_id"),
            disposal.get("ticker"), disposal.get("gain_loss_gbp"),
            disposal.get("disposal_date"), disposal.get("acquisition_date"),
            disposal.get("section_104_pool"), int(disposal.get("bb_flag", False)),
            disposal.get("cumulative_gains", 0.0), disposal.get("remaining_allowance"),
        ))
        self._conn().commit()

    def get_ytd_gains(self, tax_year: str = None) -> Dict:
        """Return YTD gains and losses. Defaults to current year."""
        if tax_year is None:
            tax_year = str(datetime.now().year)
        row = self._conn().execute(
            "SELECT SUM(CASE WHEN gain_loss_gbp > 0 THEN gain_loss_gbp ELSE 0 END) as total_gains, "
            "SUM(CASE WHEN gain_loss_gbp < 0 THEN ABS(gain_loss_gbp) ELSE 0 END) as total_losses "
            "FROM tax_ledger WHERE tax_year=?", (tax_year,)
        ).fetchone()
        return {
            "total_gains": row[0] or 0.0,
            "total_losses": row[1] or 0.0,
            "tax_year": tax_year,
        }

    # Overload record_benchmark to accept new signature
    def record_benchmark(self, date: str = None, portfolio_value: float = None,
                         benchmarks: Dict = None, *, fund_return: float = None,
                         fund_value: float = None, benchmark_returns: Dict = None) -> None:
        """Accept both old dict signature and new keyword signature."""
        if benchmark_returns is not None:
            # New signature from BenchmarkTracker
            benchmarks = {
                "us": benchmark_returns.get("SPY"),
                "uk": benchmark_returns.get("EWU"),
                "smallcap": benchmark_returns.get("IWM"),
                "active_return_us": (fund_return or 0) - (benchmark_returns.get("SPY") or 0),
                "active_return_uk": (fund_return or 0) - (benchmark_returns.get("EWU") or 0),
            }
            portfolio_value = fund_value or portfolio_value or 0.0
        self._conn().execute("""
            INSERT OR REPLACE INTO benchmark_performance
                (date, portfolio_value, benchmark_us_value, benchmark_uk_value,
                 benchmark_smallcap_value, active_return_us, active_return_uk,
                 information_ratio_rolling_252d, tracking_error)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            date, portfolio_value,
            (benchmarks or {}).get("us"), (benchmarks or {}).get("uk"),
            (benchmarks or {}).get("smallcap"),
            (benchmarks or {}).get("active_return_us"),
            (benchmarks or {}).get("active_return_uk"),
            (benchmarks or {}).get("ir_252"),
            (benchmarks or {}).get("tracking_error"),
        ))
        self._conn().commit()

    def get_benchmark_history(self, n: int = 252, window_days: int = None) -> List[Dict]:
        """Return benchmark history. window_days is an alias for n."""
        limit = window_days if window_days is not None else n
        rows = self._conn().execute(
            "SELECT * FROM benchmark_performance ORDER BY date DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Adaptive sizer support
    # ------------------------------------------------------------------

    def count_completed_trades(self) -> int:
        """Count trades with a recorded exit."""
        try:
            row = self._conn().execute(
                "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NOT NULL"
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def get_paper_equity(self, initial_capital: float = 100000.0) -> float:
        """Return current paper equity = initial_capital + sum of net_pnl."""
        try:
            row = self._conn().execute(
                "SELECT SUM(net_pnl) FROM trade_ledger WHERE exit_date IS NOT NULL"
            ).fetchone()
            realised = float(row[0] or 0.0)
            return initial_capital + realised
        except Exception:
            return initial_capital

    def get_open_positions(self) -> List[Dict]:
        """Return trades that have no exit recorded yet."""
        try:
            rows = self._conn().execute(
                "SELECT * FROM trade_ledger WHERE exit_date IS NULL ORDER BY id DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def open_trade(self, ticker: str, direction: str, entry_price: float,
                   shares: float, position_value: float, signal_type: str,
                   signal_score: float, all_signals: list = None,
                   context: dict = None, sizing_reasoning: str = '',
                   phase: str = 'PHASE_1') -> int:
        """Record a newly opened trade position."""
        direction_int = 1 if direction == 'LONG' else -1
        trade = {
            'ticker': ticker,
            'market': 'uk' if ticker.endswith('.L') else 'us',
            'direction': direction_int,
            'entry_date': datetime.now(timezone.utc).isoformat(),
            'entry_price': entry_price,
            'position_size': shares,
            'signals_at_entry': json.dumps({
                'signal_type': signal_type,
                'signal_score': signal_score,
                'all_signals': all_signals or [],
                'phase': phase,
                'sizing_reasoning': sizing_reasoning,
            }),
            'macro_regime': (context or {}).get('macro', {}).get('regime'),
            'sector': (context or {}).get('sector'),
        }
        return self.record_trade(trade, context or {})

    def get_signal_performance_by_type(self) -> Dict[str, Dict]:
        """Return win rate, avg return, n_trades by signal type from trade history."""
        result: Dict[str, Dict] = {}
        try:
            from collections import defaultdict
            rows = self._conn().execute(
                """SELECT signals_at_entry, was_profitable, pnl_pct
                   FROM trade_ledger
                   WHERE exit_date IS NOT NULL
                     AND signals_at_entry IS NOT NULL"""
            ).fetchall()
            buckets: Dict = defaultdict(lambda: {
                'wins': 0, 'n': 0, 'wins_pnl': 0.0, 'losses_pnl': 0.0
            })
            for row in rows:
                try:
                    sig_data = json.loads(row[0] or '{}')
                    stype = sig_data.get('signal_type', 'UNKNOWN')
                    profitable = int(row[1] or 0)
                    pnl = float(row[2] or 0.0)
                    b = buckets[stype]
                    b['n'] += 1
                    b['wins'] += profitable
                    if profitable:
                        b['wins_pnl'] += pnl
                    else:
                        b['losses_pnl'] += pnl
                except Exception:
                    pass
            for stype, b in buckets.items():
                n = b['n']
                wins = b['wins']
                win_rate = wins / n if n > 0 else 0.5
                avg_win = b['wins_pnl'] / max(wins, 1)
                avg_loss = b['losses_pnl'] / max(n - wins, 1)
                result[stype] = {
                    'n_trades': n,
                    'win_rate': round(win_rate, 4),
                    'avg_win_pct': round(avg_win, 4),
                    'avg_loss_pct': round(avg_loss, 4),
                    'min_profitable_score': 0.35,
                }
        except Exception as e:
            logger.warning('get_signal_performance_by_type: %s', e)
        return result

    def log_entry_timing_outcome(self, outcome_or_trade_id, outcome_data=None) -> bool:
        """
        Log an entry timing outcome to entry_timing_outcomes table.

        Supports two calling conventions:
          1. log_entry_timing_outcome(outcome_dict)   — called from EntryLearner
          2. log_entry_timing_outcome(trade_id, outcome_data_dict)
        """
        try:
            if outcome_data is not None:
                # Convention 2: trade_id + dict
                outcome = dict(outcome_data)
                outcome["trade_id"] = outcome_or_trade_id
            elif isinstance(outcome_or_trade_id, dict):
                # Convention 1: single dict
                outcome = outcome_or_trade_id
            else:
                logger.warning("log_entry_timing_outcome: unrecognised arguments")
                return False

            conn = self._conn()
            conn.execute("""
                INSERT INTO entry_timing_outcomes
                (trade_id, ticker, intended_entry_price, actual_entry_price,
                 entry_timing_score, waited_days, scale_in_tranche,
                 tranche_entry_price, pnl_vs_immediate_entry,
                 entry_method, entry_conditions_met)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                outcome.get("trade_id"),
                outcome.get("ticker", ""),
                outcome.get("immediate_entry_price"),   # intended = immediate (no-wait baseline)
                outcome.get("actual_entry_price"),
                outcome.get("entry_timing_score"),
                outcome.get("waited_days", 0),
                outcome.get("scale_in_tranche", 0),
                outcome.get("tranche_entry_price"),
                outcome.get("entry_alpha"),              # pnl_vs_immediate = entry_alpha
                outcome.get("entry_condition", outcome.get("entry_method")),
                json.dumps(outcome.get("entry_conditions_met", [])),
            ))
            conn.commit()
            return True
        except Exception as e:
            logger.warning("log_entry_timing_outcome failed: %s", e)
            return False

    def get_altdata_confluence(self, ticker: str) -> float:
        """Return altdata confluence proxy for ticker (0–1)."""
        try:
            row = self._conn().execute(
                "SELECT AVG(pnl_pct) FROM trade_ledger "
                "WHERE ticker=? AND exit_date IS NOT NULL",
                (ticker,)
            ).fetchone()
            if row and row[0] is not None:
                return float(min(1.0, max(0.0, 0.5 + float(row[0]) * 2)))
        except Exception:
            pass
        return 0.5


# Alias for backwards-compatible imports
CloseloopStore = ClosedLoopStore
