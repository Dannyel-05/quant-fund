"""
Signal Decay Monitor
=====================
Tracks rolling performance of live signals at 30d / 90d / 180d windows.
Detects DECAYING and SEVERELY_DEGRADED signals and issues automatic size
reduction recommendations.

Classes:
  - SignalPerformanceRecord  — dataclass for a single evaluation
  - SignalDecayMonitor       — main class; reads trade log, computes metrics,
                               stores to historical_db.db, issues alerts
  - DecayAlert               — alert dataclass

Usage (CLI):
  python3 -m analysis.signal_decay_monitor
  python3 -m analysis.signal_decay_monitor --signal pead_us
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]
_HIST_DB = _ROOT / "output" / "historical_db.db"
_PAPER_LOG = _ROOT / "logs" / "paper_trading.jsonl"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"

# Decay thresholds
_WIN_RATE_FLOOR = 0.42          # below this → DECAYING
_WIN_RATE_SEVERE = 0.33         # below this → SEVERELY_DEGRADED
_SHARPE_FLOOR = 0.40            # below this → DECAYING
_SHARPE_SEVERE = 0.0            # below this → SEVERELY_DEGRADED
_SIZE_REDUCTION_DECAYING = 0.50 # halve position size
_SIZE_REDUCTION_SEVERE = 0.25   # quarter position size

WINDOWS = [30, 90, 180]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SignalPerformanceRecord:
    signal_name: str
    window_days: int
    n_trades: int
    win_rate: float
    avg_return: float
    sharpe: float
    sortino: float
    max_drawdown: float
    status: str           # HEALTHY / DECAYING / SEVERELY_DEGRADED / INSUFFICIENT_DATA
    size_multiplier: float
    evaluated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class DecayAlert:
    signal_name: str
    window_days: int
    severity: str          # DECAYING / SEVERELY_DEGRADED
    win_rate: float
    sharpe: float
    size_multiplier: float
    message: str
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS signal_decay_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_name     TEXT    NOT NULL,
            window_days     INTEGER NOT NULL,
            n_trades        INTEGER,
            win_rate        REAL,
            avg_return      REAL,
            sharpe          REAL,
            sortino         REAL,
            max_drawdown    REAL,
            status          TEXT,
            size_multiplier REAL,
            evaluated_at    TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sdl_sig ON signal_decay_log(signal_name, window_days, evaluated_at);

        CREATE TABLE IF NOT EXISTS signal_decay_alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_name  TEXT NOT NULL,
            window_days  INTEGER NOT NULL,
            severity     TEXT,
            win_rate     REAL,
            sharpe       REAL,
            size_multiplier REAL,
            message      TEXT,
            created_at   TEXT NOT NULL
        );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Core monitor
# ---------------------------------------------------------------------------

class SignalDecayMonitor:
    """
    Reads the paper-trading JSONL log (and the signal_registry.json) to compute
    rolling win-rate and Sharpe at 30 / 90 / 180-day windows for every signal.

    Persists results to historical_db.db :: signal_decay_log.
    Issues DecayAlert objects when thresholds are breached.
    """

    def __init__(self):
        self.db_path = _HIST_DB
        self.paper_log = _PAPER_LOG
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------ public

    def run(self, signal_filter: Optional[str] = None) -> Dict[str, List[SignalPerformanceRecord]]:
        """
        Evaluate all signals (or just `signal_filter`).
        Returns dict: signal_name → list of SignalPerformanceRecord (one per window).
        """
        trades = self._load_trades()
        if trades.empty:
            logger.warning("SignalDecayMonitor: no trades found in %s", self.paper_log)
            return {}

        signal_names = trades["signal"].dropna().unique().tolist()
        if signal_filter:
            signal_names = [s for s in signal_names if s == signal_filter]

        results: Dict[str, List[SignalPerformanceRecord]] = {}
        alerts: List[DecayAlert] = []

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            _ensure_tables(conn)

            for sig in signal_names:
                sig_trades = trades[trades["signal"] == sig].copy()
                records = []
                for w in WINDOWS:
                    rec = self._evaluate_window(sig, sig_trades, w)
                    records.append(rec)
                    self._persist_record(conn, rec)
                    if rec.status in ("DECAYING", "SEVERELY_DEGRADED"):
                        alert = self._make_alert(rec)
                        alerts.append(alert)
                        self._persist_alert(conn, alert)
                results[sig] = records
            conn.commit()

        if alerts:
            self._print_alerts(alerts)

        return results

    def status_report(self) -> str:
        """Return a formatted status report string."""
        results = self.run()
        if not results:
            return "SignalDecayMonitor: no trade data found."

        lines = [
            "SIGNAL DECAY MONITOR",
            "=" * 70,
            f"Evaluated {len(results)} signal(s)  |  Windows: 30d / 90d / 180d",
            "",
        ]
        for sig, records in sorted(results.items()):
            lines.append(f"  {sig}")
            for rec in records:
                icon = {"HEALTHY": "✓", "DECAYING": "⚠", "SEVERELY_DEGRADED": "✗",
                        "INSUFFICIENT_DATA": "-"}.get(rec.status, "?")
                lines.append(
                    f"    [{icon}] {rec.window_days:3d}d  "
                    f"n={rec.n_trades:4d}  "
                    f"win={rec.win_rate:.0%}  "
                    f"sharpe={rec.sharpe:+.2f}  "
                    f"size_mult={rec.size_multiplier:.2f}x  "
                    f"status={rec.status}"
                )
            lines.append("")
        return "\n".join(lines)

    # ----------------------------------------------------------------- private

    def _load_trades(self) -> pd.DataFrame:
        """Load closed trades from paper_trading.jsonl."""
        rows = []
        if not self.paper_log.exists():
            logger.debug("SignalDecayMonitor: paper log not found at %s", self.paper_log)
            # Try to load from historical_db.db as fallback
            return self._load_trades_from_db()

        with open(self.paper_log) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("type") == "trade_close" or "return_pct" in rec:
                        rows.append(rec)
                except json.JSONDecodeError:
                    pass

        if rows:
            df = pd.DataFrame(rows)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df

        return self._load_trades_from_db()

    def _load_trades_from_db(self) -> pd.DataFrame:
        """Load trades from historical_db.db as fallback."""
        if not self.db_path.exists():
            return pd.DataFrame()
        try:
            with sqlite3.connect(self.db_path) as conn:
                tables = [r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()]
                if "trades" in tables:
                    df = pd.read_sql("SELECT * FROM trades WHERE return_pct IS NOT NULL", conn)
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    elif "exit_date" in df.columns:
                        df["date"] = pd.to_datetime(df["exit_date"], errors="coerce")
                    if "signal" not in df.columns and "signal_type" in df.columns:
                        df["signal"] = df["signal_type"]
                    elif "signal" not in df.columns:
                        df["signal"] = "pead"
                    return df
                elif "backtest_trades" in tables:
                    df = pd.read_sql("SELECT * FROM backtest_trades", conn)
                    if "signal" not in df.columns:
                        df["signal"] = "pead"
                    for col in ("exit_date", "entry_date", "date"):
                        if col in df.columns:
                            df["date"] = pd.to_datetime(df[col], errors="coerce")
                            break
                    return df
        except Exception as exc:
            logger.debug("SignalDecayMonitor DB fallback failed: %s", exc)
        return pd.DataFrame()

    def _evaluate_window(
        self,
        signal_name: str,
        trades: pd.DataFrame,
        window_days: int,
    ) -> SignalPerformanceRecord:
        """Compute metrics for the last `window_days` of trades."""
        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=window_days)

        date_col = None
        for c in ("date", "exit_date", "close_date"):
            if c in trades.columns:
                date_col = c
                break

        if date_col is None:
            return SignalPerformanceRecord(
                signal_name=signal_name, window_days=window_days,
                n_trades=len(trades), win_rate=0, avg_return=0,
                sharpe=0, sortino=0, max_drawdown=0,
                status="INSUFFICIENT_DATA", size_multiplier=1.0,
            )

        ts = pd.to_datetime(trades[date_col], errors="coerce")
        mask = ts >= cutoff
        window_trades = trades[mask].copy()

        if len(window_trades) < 5:
            return SignalPerformanceRecord(
                signal_name=signal_name, window_days=window_days,
                n_trades=len(window_trades), win_rate=0, avg_return=0,
                sharpe=0, sortino=0, max_drawdown=0,
                status="INSUFFICIENT_DATA", size_multiplier=1.0,
            )

        ret_col = None
        for c in ("return_pct", "pnl_pct", "return", "pnl"):
            if c in window_trades.columns:
                ret_col = c
                break

        if ret_col is None:
            return SignalPerformanceRecord(
                signal_name=signal_name, window_days=window_days,
                n_trades=len(window_trades), win_rate=0, avg_return=0,
                sharpe=0, sortino=0, max_drawdown=0,
                status="INSUFFICIENT_DATA", size_multiplier=1.0,
            )

        returns = window_trades[ret_col].dropna().astype(float)
        n = len(returns)
        if n < 5:
            return SignalPerformanceRecord(
                signal_name=signal_name, window_days=window_days,
                n_trades=n, win_rate=0, avg_return=0,
                sharpe=0, sortino=0, max_drawdown=0,
                status="INSUFFICIENT_DATA", size_multiplier=1.0,
            )

        win_rate = float((returns > 0).mean())
        avg_ret = float(returns.mean())
        std = float(returns.std()) or 1e-8
        downside = float(returns[returns < 0].std()) or 1e-8
        sharpe = avg_ret / std * np.sqrt(252 / max(window_days, 1))
        sortino = avg_ret / downside * np.sqrt(252 / max(window_days, 1))

        # Max drawdown on cumulative returns
        cum = (1 + returns / 100).cumprod()
        roll_max = cum.cummax()
        drawdowns = (cum - roll_max) / roll_max
        max_dd = float(drawdowns.min())

        # Classify
        if win_rate < _WIN_RATE_SEVERE or sharpe < _SHARPE_SEVERE:
            status = "SEVERELY_DEGRADED"
            size_mult = _SIZE_REDUCTION_SEVERE
        elif win_rate < _WIN_RATE_FLOOR or sharpe < _SHARPE_FLOOR:
            status = "DECAYING"
            size_mult = _SIZE_REDUCTION_DECAYING
        else:
            status = "HEALTHY"
            size_mult = 1.0

        return SignalPerformanceRecord(
            signal_name=signal_name,
            window_days=window_days,
            n_trades=n,
            win_rate=win_rate,
            avg_return=avg_ret,
            sharpe=sharpe,
            sortino=sortino,
            max_drawdown=max_dd,
            status=status,
            size_multiplier=size_mult,
        )

    def _make_alert(self, rec: SignalPerformanceRecord) -> DecayAlert:
        msg = (
            f"{rec.signal_name} [{rec.window_days}d window]: "
            f"win_rate={rec.win_rate:.0%}, sharpe={rec.sharpe:.2f} → "
            f"{rec.status}. Reduce size to {rec.size_multiplier:.0%}."
        )
        return DecayAlert(
            signal_name=rec.signal_name,
            window_days=rec.window_days,
            severity=rec.status,
            win_rate=rec.win_rate,
            sharpe=rec.sharpe,
            size_multiplier=rec.size_multiplier,
            message=msg,
        )

    def _persist_record(self, conn: sqlite3.Connection, rec: SignalPerformanceRecord) -> None:
        conn.execute("""
            INSERT INTO signal_decay_log
              (signal_name, window_days, n_trades, win_rate, avg_return,
               sharpe, sortino, max_drawdown, status, size_multiplier, evaluated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            rec.signal_name, rec.window_days, rec.n_trades,
            rec.win_rate, rec.avg_return, rec.sharpe,
            rec.sortino, rec.max_drawdown, rec.status,
            rec.size_multiplier, rec.evaluated_at,
        ))

    def _persist_alert(self, conn: sqlite3.Connection, alert: DecayAlert) -> None:
        conn.execute("""
            INSERT INTO signal_decay_alerts
              (signal_name, window_days, severity, win_rate, sharpe,
               size_multiplier, message, created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            alert.signal_name, alert.window_days, alert.severity,
            alert.win_rate, alert.sharpe, alert.size_multiplier,
            alert.message, alert.created_at,
        ))

    def _print_alerts(self, alerts: List[DecayAlert]) -> None:
        print("\n" + "!" * 70)
        print("  SIGNAL DECAY ALERTS")
        print("!" * 70)
        for a in alerts:
            print(f"  [{a.severity}]  {a.message}")
        print("!" * 70 + "\n")

    def get_size_multiplier(self, signal_name: str, window_days: int = 90) -> float:
        """
        Quick lookup: return current size multiplier for a signal at a given window.
        Returns 1.0 if no data is available.
        """
        if not self.db_path.exists():
            return 1.0
        try:
            with sqlite3.connect(self.db_path) as conn:
                _ensure_tables(conn)
                row = conn.execute("""
                    SELECT size_multiplier FROM signal_decay_log
                    WHERE signal_name=? AND window_days=?
                    ORDER BY evaluated_at DESC LIMIT 1
                """, (signal_name, window_days)).fetchone()
                return float(row[0]) if row else 1.0
        except Exception:
            return 1.0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Signal Decay Monitor")
    parser.add_argument("--signal", default=None, help="Filter to a single signal name")
    args = parser.parse_args()

    monitor = SignalDecayMonitor()
    print(monitor.status_report())
