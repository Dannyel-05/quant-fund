"""
Initialises simulation.db for storing all simulation runs.
Run once. Safe to re-run (uses IF NOT EXISTS).
"""
import sqlite3
import os

SIM_DB_PATH = os.path.join(os.path.dirname(__file__), 'simulation.db')

conn = sqlite3.connect(SIM_DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA foreign_keys=ON")

conn.executescript("""
CREATE TABLE IF NOT EXISTS simulation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    sim_date TEXT NOT NULL,
    created_at TEXT NOT NULL,
    market TEXT NOT NULL,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    gross_pnl REAL DEFAULT 0.0,
    net_pnl REAL DEFAULT 0.0,
    sharpe_ratio REAL,
    sortino_ratio REAL,
    max_drawdown REAL,
    win_rate REAL,
    profit_factor REAL,
    avg_trade_duration_min REAL,
    status TEXT DEFAULT 'completed',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS simulation_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    quantity INTEGER NOT NULL,
    entry_time TEXT NOT NULL,
    exit_time TEXT,
    simulated_slippage REAL DEFAULT 0.0,
    simulated_spread_cost REAL DEFAULT 0.0,
    simulated_latency_ms REAL DEFAULT 0.0,
    fill_ratio REAL DEFAULT 1.0,
    gross_pnl REAL DEFAULT 0.0,
    net_pnl REAL DEFAULT 0.0,
    exit_reason TEXT,
    signals_used TEXT,
    FOREIGN KEY (run_id) REFERENCES simulation_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simulation_signal_attribution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    trade_id INTEGER NOT NULL,
    signal_name TEXT NOT NULL,
    signal_value REAL,
    signal_weight REAL,
    contribution REAL,
    FOREIGN KEY (run_id) REFERENCES simulation_runs(run_id),
    FOREIGN KEY (trade_id) REFERENCES simulation_trades(id)
);

CREATE TABLE IF NOT EXISTS equity_curves (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    equity REAL NOT NULL,
    drawdown REAL DEFAULT 0.0,
    FOREIGN KEY (run_id) REFERENCES simulation_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_sim_runs_date ON simulation_runs(sim_date);
CREATE INDEX IF NOT EXISTS idx_sim_trades_run ON simulation_trades(run_id);
CREATE INDEX IF NOT EXISTS idx_equity_run ON equity_curves(run_id);
""")

conn.commit()
conn.close()
print(f"simulation.db initialised successfully at {SIM_DB_PATH}")
