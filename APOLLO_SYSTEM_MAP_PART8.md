# APOLLO SYSTEM MAP — PART 8
## Storage Layer: Frontier Store and DeepData Store

Generated: 2026-04-08
Files read: 2
Lines read: frontier_store.py = 515, deepdata_store.py = 650

---

---

# FILE 1: /home/dannyelticala/quant-fund/frontier/storage/frontier_store.py

---

## A) PURPOSE

SQLite-backed, thread-safe storage layer for the frontier intelligence pipeline. The stated design principle is "log EVERYTHING permanently" — data that fails validation today may become significant tomorrow. Every signal reading, UMCI reading, validation attempt (pass or fail), discovered correlation, parameter drift event, live track record entry, generated frontier signal, and cross-signal interaction test result is written to this one database file. The file creates and manages `frontier/storage/frontier.db` by default.

---

## B) CLASSES AND METHODS

### Class: `FrontierStore`

**Docstring:** "Thread-safe SQLite store for the frontier intelligence pipeline."

Uses `threading.local()` so each thread gets its own SQLite connection. WAL journal mode is set both at schema creation time and again per-connection in `_conn()`.

---

#### `__init__(self, config: dict)`

- **Input:** `config` dict. Reads `config["frontier"]["storage_path"]`, defaulting to `"frontier/storage/frontier.db"`.
- **Output:** None. Side effect: creates parent directory if missing, initialises the database schema.
- **DB writes:** Executes `_CREATE_SQL` (all `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` statements).
- **Logs:** `INFO` — "FrontierStore initialised at {path}"

---

#### `_conn(self) -> sqlite3.Connection`

- **Input:** None (uses `self._local` thread-local storage).
- **Output:** `sqlite3.Connection`. `row_factory` is set to `sqlite3.Row`. WAL mode is set per connection.
- **DB reads/writes:** None directly. Opens a new connection if one does not exist for this thread.
- **Behaviour:** `check_same_thread=False`, `timeout=30`. Does NOT set `PRAGMA foreign_keys=ON` per connection — that pragma is only in the `_CREATE_SQL` script run at init. Foreign keys are therefore off for all subsequent connections opened after init.

---

#### `_init_db(self) -> None`

- **Input:** None.
- **Output:** None.
- **DB writes:** Calls `executescript(_CREATE_SQL)` then `commit()`. Creates all 7 tables and 5 indexes.

---

#### `store_raw(self, collector, signal_name, value, ticker=None, market=None, raw_data=None, quality=1.0) -> int`

- **Input:**
  - `collector: str` — name of the data collector
  - `signal_name: str` — name of the signal
  - `value: float` — numeric signal value
  - `ticker: str` (optional) — equity ticker
  - `market: str` (optional) — market identifier
  - `raw_data: Any` (optional) — arbitrary data, JSON-serialised before storage
  - `quality: float` (default 1.0) — data quality score
- **Output:** `int` — `lastrowid` of the inserted row.
- **DB writes:** `INSERT INTO raw_signals (collector, signal_name, ticker, market, value, raw_data, quality, collected_at)`
- **Timestamp:** `datetime.now(timezone.utc).isoformat()`

SQL:
```sql
INSERT INTO raw_signals
  (collector, signal_name, ticker, market, value, raw_data, quality, collected_at)
  VALUES (?,?,?,?,?,?,?,?)
```

---

#### `get_raw_history(self, signal_name, days_back=365, ticker=None) -> List[Dict]`

- **Input:**
  - `signal_name: str`
  - `days_back: int` (default 365)
  - `ticker: str` (optional)
- **Output:** `List[Dict]`. Each dict is a row from `raw_signals`. `raw_data` column is JSON-decoded if present; decode errors are silently swallowed.
- **DB reads:** `SELECT * FROM raw_signals WHERE signal_name=? [AND ticker=?] AND collected_at>=? ORDER BY collected_at`

SQL (no ticker):
```sql
SELECT * FROM raw_signals
WHERE signal_name=? AND collected_at>=?
ORDER BY collected_at
```

SQL (with ticker):
```sql
SELECT * FROM raw_signals
WHERE signal_name=? AND ticker=? AND collected_at>=?
ORDER BY collected_at
```

---

#### `store_umci(self, umci, breakdown) -> None`

- **Input:**
  - `umci: float` — composite UMCI score
  - `breakdown: Dict` — full dimensional breakdown dict with keys: `level`, `dimensions` (sub-dict with `physical`, `social`, `scientific`, `financial`, `altdata`), `dominant_dimension`, `position_multiplier`, `halt_new_positions`
- **Output:** None.
- **DB writes:** `INSERT INTO umci_history (...)`

SQL:
```sql
INSERT INTO umci_history
  (umci, level, physical, social, scientific, financial, altdata,
   dominant_dim, position_mult, halt, full_breakdown, recorded_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
```

- `halt` is stored as `int(breakdown.get("halt_new_positions", False))` — 0 or 1.
- `full_breakdown` is `json.dumps(breakdown)`.

---

#### `get_umci_history(self, n=252) -> List[Dict]`

- **Input:** `n: int` (default 252) — number of records to return.
- **Output:** `List[Dict]`, chronological order (oldest first). `full_breakdown` is JSON-decoded; decode errors silently swallowed.
- **DB reads:**

SQL:
```sql
SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT ?
```

Then `list(reversed(result))` to return chronological order.

---

#### `get_last_umci(self) -> Optional[Dict]`

- **Input:** None.
- **Output:** `Optional[Dict]` — single most-recent row from `umci_history`, or `None`. `full_breakdown` JSON-decoded with silent swallow on error.
- **DB reads:**

SQL:
```sql
SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1
```

---

#### `store_validation(self, signal_name, result) -> None`

- **Input:**
  - `signal_name: str`
  - `result: Dict` — validation result with keys: `passed`, `t_stat`, `p_value`, `p_bonferroni`, `monte_carlo_pct`, `sharpe`, `deflated_sharpe`, `fsp`, `evidence_grade`, `n_obs`, `regime_stable`
- **Output:** None.
- **DB writes:** `INSERT INTO validation_results (...)`

SQL:
```sql
INSERT INTO validation_results
  (signal_name, passed, t_stat, p_value, p_bonferroni, monte_carlo_pct,
   sharpe, deflated_sharpe, fsp, evidence_grade, n_obs, regime_stable,
   full_result, validated_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
```

- `passed` = `int(result.get("passed", False))` — 0 or 1.
- `regime_stable` = `int(result.get("regime_stable", False))` — 0 or 1.
- `full_result` = `json.dumps(result)` (entire dict stored verbatim).

---

#### `get_validation_history(self, signal_name) -> List[Dict]`

- **Input:** `signal_name: str`
- **Output:** `List[Dict]` — all validation rows for the signal in chronological order.
- **DB reads:**

SQL:
```sql
SELECT * FROM validation_results WHERE signal_name=? ORDER BY validated_at
```

---

#### `upsert_watchlist(self, entry) -> None`

- **Input:** `entry: Dict` with keys: `name` (required), `description`, `formula`, `correlation`, `optimal_lag`, `t_stat`, `p_bonferroni`, `monte_carlo_pct`, `deflated_sharpe`, `n_obs`, `validation_status`, `live_days`, `live_accuracy`, `sizing_tier`, `notes`, `discovered_at`
- **Output:** None.
- **DB writes:** `INSERT INTO watchlist ... ON CONFLICT(name) DO UPDATE SET ...`

SQL:
```sql
INSERT INTO watchlist
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
  updated_at=excluded.updated_at
```

Note: `t_stat`, `p_bonferroni`, `optimal_lag`, and `formula` are NOT in the ON CONFLICT UPDATE SET list — once a row is inserted, those columns can only be changed via a full row replacement or direct SQL.

---

#### `get_watchlist(self, status=None, limit=10) -> List[Dict]`

- **Input:**
  - `status: Optional[str]` — filter by `validation_status`
  - `limit: int` (default 10)
- **Output:** `List[Dict]` ordered by `deflated_sharpe DESC`.
- **DB reads:**

SQL (no status filter):
```sql
SELECT * FROM watchlist ORDER BY deflated_sharpe DESC LIMIT ?
```

SQL (with status filter):
```sql
SELECT * FROM watchlist WHERE validation_status=? ORDER BY deflated_sharpe DESC LIMIT ?
```

---

#### `log_parameter_drift(self, signal_name, param_name, published, old_val, new_val, delta=None, reason=None) -> None`

- **Input:**
  - `signal_name: str`
  - `param_name: str`
  - `published: float` — the published/original value
  - `old_val: float` — the previous live value
  - `new_val: float` — the new live value
  - `delta: float` (optional) — performance change
  - `reason: str` (optional) — human-readable reason
- **Output:** None.
- **DB writes:** `INSERT INTO parameter_history (...)`

SQL:
```sql
INSERT INTO parameter_history
  (signal_name, param_name, published_value, old_value, new_value,
   performance_delta, reason, changed_at)
  VALUES (?,?,?,?,?,?,?,?)
```

---

#### `get_parameter_history(self, signal_name) -> List[Dict]`

- **Input:** `signal_name: str`
- **Output:** `List[Dict]` — all parameter drift records for the signal in chronological order.
- **DB reads:**

SQL:
```sql
SELECT * FROM parameter_history WHERE signal_name=? ORDER BY changed_at
```

---

#### `update_evidence(self, signal_name, sizing_tier, live_days, live_accuracy=None, live_sharpe=None, n_live=None) -> None`

- **Input:**
  - `signal_name: str`
  - `sizing_tier: int`
  - `live_days: int`
  - `live_accuracy: float` (optional)
  - `live_sharpe: float` (optional)
  - `n_live: int` (optional) — number of live signals
- **Output:** None.
- **DB writes:** `INSERT INTO evidence_records ... ON CONFLICT(signal_name) DO UPDATE SET ...`

SQL:
```sql
INSERT INTO evidence_records
  (signal_name, sizing_tier, live_days, live_accuracy, live_sharpe,
   n_live_signals, updated_at)
  VALUES (?,?,?,?,?,?,?)
ON CONFLICT(signal_name) DO UPDATE SET
  sizing_tier=excluded.sizing_tier,
  live_days=excluded.live_days,
  live_accuracy=COALESCE(excluded.live_accuracy, live_accuracy),
  live_sharpe=COALESCE(excluded.live_sharpe, live_sharpe),
  n_live_signals=COALESCE(excluded.n_live_signals, n_live_signals),
  updated_at=excluded.updated_at
```

Note: `COALESCE(excluded.live_accuracy, live_accuracy)` preserves the existing value when `None` is passed. `last_promoted` column is defined in the schema but never written by this method.

---

#### `get_evidence(self, signal_name) -> Optional[Dict]`

- **Input:** `signal_name: str`
- **Output:** `Optional[Dict]` — the evidence record, or `None`.
- **DB reads:**

SQL:
```sql
SELECT * FROM evidence_records WHERE signal_name=?
```

---

#### `log_signal(self, ticker, signal_name, signal_type, direction, confidence, sizing_tier, position_size=None, sources=None) -> int`

- **Input:**
  - `ticker: str`
  - `signal_name: str`
  - `signal_type: str`
  - `direction: int` — +1 or -1
  - `confidence: float`
  - `sizing_tier: int`
  - `position_size: float` (optional)
  - `sources: List[str]` (optional) — JSON-serialised before storage
- **Output:** `int` — `lastrowid`.
- **DB writes:** `INSERT INTO signal_log (...)`

SQL:
```sql
INSERT INTO signal_log
  (ticker, signal_name, signal_type, direction, confidence, sizing_tier,
   position_size, sources, generated_at)
  VALUES (?,?,?,?,?,?,?,?,?)
```

Note: `outcome_return` and `resolved_at` columns are defined in the schema but never written by this method. There is no method anywhere in this file that updates those columns. Signal outcomes are never resolved.

---

#### `get_recent_signals(self, hours_back=48, limit=100) -> List[Dict]`

- **Input:**
  - `hours_back: int` (default 48)
  - `limit: int` (default 100)
- **Output:** `List[Dict]` — signals from the last N hours, newest first.
- **DB reads:**

SQL:
```sql
SELECT * FROM signal_log
WHERE generated_at>=?
ORDER BY generated_at DESC LIMIT ?
```

---

#### `status_summary(self) -> Dict`

- **Input:** None.
- **Output:** `Dict` with keys:
  - `raw_signals_24h` — count of raw signals in last 24 hours
  - `validations_total` — total validation results count
  - `validations_passed` — count where `passed=1`
  - `watchlist_size` — total watchlist rows
  - `monitoring_signals` — count where `validation_status='MONITORING'`
  - `signals_24h` — signal_log rows in last 24 hours
  - `last_umci` — most recent UMCI float, or None
  - `last_umci_level` — most recent UMCI level string, or None
- **DB reads:** 6 COUNT queries + calls `get_last_umci()`.

SQL queries:
```sql
SELECT COUNT(*) FROM raw_signals WHERE collected_at >= datetime('now','-1 day')
SELECT COUNT(*) FROM validation_results
SELECT COUNT(*) FROM validation_results WHERE passed=1
SELECT COUNT(*) FROM watchlist
SELECT COUNT(*) FROM watchlist WHERE validation_status='MONITORING'
SELECT COUNT(*) FROM signal_log WHERE generated_at >= datetime('now','-1 day')
```

---

#### `backup(self, backup_dir="output/backups") -> str`

- **Input:** `backup_dir: str` (default `"output/backups"`)
- **Output:** `str` — path of the backup file.
- **Action:** Creates backup directory if missing, copies the database file using `shutil.copy2()`. Filename format: `frontier_{YYYYMMDD}.db`.
- **DB reads/writes:** None via SQLite. File system copy only.

---

#### `close(self) -> None`

- **Input:** None.
- **Output:** None.
- **Action:** Closes the thread-local connection if open, sets it to `None`.

---

## C) MATHEMATICS

No mathematical formulas are computed inside this file. It is a pure storage layer. All values (t_stat, p_value, sharpe, deflated_sharpe, monte_carlo_pct, UMCI, etc.) are accepted as already-computed inputs and stored verbatim.

---

## D) DATA FLOWS

**Inputs (what enters):**
- Raw signal readings from collectors (name, value, quality, ticker, market, raw_data)
- UMCI score and dimensional breakdown dict from UMCI computation layer
- Validation result dicts from signal validation layer
- Watchlist entries (discovered correlations with statistical evidence)
- Parameter drift events (old/new parameter values)
- Evidence records (live track record data per signal)
- Generated frontier signal descriptors (ticker, direction, confidence, tier, sources)
- Interaction test results (cross-signal combinations, t_stat, significance flag)

**Outputs (what leaves):**
- `store_raw` → returns `lastrowid`
- `get_raw_history` → `List[Dict]` from `raw_signals`
- `get_umci_history` → `List[Dict]` from `umci_history`
- `get_last_umci` → `Optional[Dict]` from `umci_history`
- `get_validation_history` → `List[Dict]` from `validation_results`
- `get_watchlist` → `List[Dict]` from `watchlist`
- `get_parameter_history` → `List[Dict]` from `parameter_history`
- `get_evidence` → `Optional[Dict]` from `evidence_records`
- `get_recent_signals` → `List[Dict]` from `signal_log`
- `status_summary` → `Dict` of counts and last UMCI

**Database file:** `frontier/storage/frontier.db` (default path, configurable via `config["frontier"]["storage_path"]`)

**Tables read:**
- `raw_signals`
- `umci_history`
- `validation_results`
- `watchlist`
- `parameter_history`
- `evidence_records`
- `signal_log`

**Tables written:**
- `raw_signals`
- `umci_history`
- `validation_results`
- `watchlist`
- `parameter_history`
- `evidence_records`
- `signal_log`

**Table defined but NEVER written by any method in this file:**
- `interaction_log` — schema defined in `_CREATE_SQL`, no `store_` or `log_` method exists for it.

---

## E) DEPENDENCIES

**Internal Apollo imports:**
- None. This file has no imports from other Apollo modules.

**External library imports:**
- `json` (stdlib)
- `logging` (stdlib)
- `os` (stdlib)
- `shutil` (stdlib)
- `sqlite3` (stdlib)
- `threading` (stdlib)
- `datetime`, `timezone` from `datetime` (stdlib)
- `Path` from `pathlib` (stdlib)
- `Any`, `Dict`, `List`, `Optional` from `typing` (stdlib)

All dependencies are Python standard library. No third-party packages.

---

## F) WIRING STATUS

**Connected to live trading path: YES, indirectly.**

`FrontierStore` is the persistence layer for the entire frontier intelligence pipeline. It stores:
1. UMCI readings — the UMCI drives position sizing multipliers and halt-new-positions flags used by the live trading engine.
2. Generated frontier signals (`signal_log`) — these are the output signals of the frontier pipeline that feed into position decisions.
3. Evidence records (`evidence_records`) — `sizing_tier` per signal directly affects position sizes in live trading.
4. Watchlist — governs which signals are promoted and allowed to trade.

Any live trading logic that reads UMCI, reads frontier signals, or reads signal tier assignments will use data that passed through this store. The store itself does not call into the trading engine — it is written to by upstream frontier modules and read from by downstream sizing/signal modules.

---

## G) ISSUES FOUND

1. **`interaction_log` table is dead.** The table is defined in `_CREATE_SQL` with 9 columns but there is no `store_interaction()`, `log_interaction()`, or any INSERT method for this table anywhere in this file. The table is created at startup and sits empty. It cannot receive data without code that does not exist in this file.

2. **`signal_log.outcome_return` and `signal_log.resolved_at` are never written.** Both columns are defined in the schema. `log_signal()` inserts rows but leaves both columns as NULL. There is no `resolve_signal()` or `update_signal_outcome()` method. Signal outcomes are permanently unresolved — the live track record for frontier signals has no P&L data.

3. **`evidence_records.last_promoted` is never written.** The column is defined in the schema but `update_evidence()` does not include it in either the INSERT or the ON CONFLICT UPDATE. It will always be NULL.

4. **`PRAGMA foreign_keys=ON` is only executed during `_init_db()`.** The `_conn()` method that creates per-thread connections sets WAL mode but does NOT re-execute `PRAGMA foreign_keys=ON`. Because SQLite pragmas are per-connection, foreign key enforcement is disabled for all thread connections created after the initialising thread. Since `foreign_keys` is used in the schema `_CREATE_SQL` pragma but the `raw_signals` and other tables have no foreign key constraints defined anyway, the practical impact is zero — but the pragma in `_CREATE_SQL` does nothing for child threads.

5. **`upsert_watchlist` ON CONFLICT does not update `t_stat`, `p_bonferroni`, `optimal_lag`, or `formula`.** These four columns are written on first INSERT only. If a signal is re-discovered with better statistics, those columns remain at their original inserted values. The update clause omits them.

6. **`get_watchlist` default `limit=10` may silently truncate results.** A caller asking for the full watchlist with no arguments gets at most 10 rows regardless of how many exist. No warning or count is returned.

7. **`store_raw` JSON-encodes `raw_data` with `json.dumps()` but uses no error handling.** If `raw_data` contains non-serialisable objects (e.g., numpy types, datetime objects), the call raises `TypeError` and no row is inserted. The exception propagates to the caller with no wrapping.

8. **WAL mode is set twice per connection.** `_CREATE_SQL` begins with `PRAGMA journal_mode=WAL;` which runs at `_init_db()` time. Then `_conn()` executes it again on every new connection. Harmless but redundant.

9. **`interaction_log` has no indexes.** The table has no `CREATE INDEX` statements unlike other tables. If it were ever populated, queries would be full table scans.

---

---

# FILE 2: /home/dannyelticala/quant-fund/deepdata/storage/deepdata_store.py

---

## A) PURPOSE

SQLite-backed, thread-safe storage layer for the deepdata intelligence pipeline. Stores all deep alternative data collected and processed by the deepdata pipeline: options flow snapshots, short interest reports, squeeze events, earnings call transcript scores, patent data, supply chain relationship graphs, congressional disclosure records, member track records, earnings quality classifications, generated deepdata signals, factor exposure snapshots, and validated cross-module patterns. Default database file: `deepdata/storage/deepdata.db`.

---

## B) CLASSES AND METHODS

### Class: `DeepDataStore`

**Docstring:** "Thread-safe SQLite store for the deepdata intelligence pipeline."

Uses `threading.local()` for per-thread SQLite connections. WAL journal mode and foreign keys are set per connection in `_conn()` (unlike FrontierStore, this correctly re-enables `PRAGMA foreign_keys=ON` per connection).

---

#### `__init__(self, config: dict)`

- **Input:** `config` dict. Reads `config["deepdata"]["storage_path"]`, defaulting to `"deepdata/storage/deepdata.db"`.
- **Output:** None. Creates parent directory, initialises schema.
- **DB writes:** Executes `_CREATE_SQL` (all `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`).
- **Logs:** `INFO` — "DeepDataStore initialised at {path}"

---

#### `_conn(self) -> sqlite3.Connection`

- **Input:** None.
- **Output:** `sqlite3.Connection`. `row_factory = sqlite3.Row`. WAL mode and foreign keys set per connection.
- **DB reads/writes:** None. Opens connection lazily per thread.

Note: Unlike `FrontierStore._conn()`, this method calls BOTH `PRAGMA journal_mode=WAL` AND `PRAGMA foreign_keys=ON` on every new connection. Foreign key enforcement is therefore actually active per connection in DeepDataStore.

---

#### `_init_db(self) -> None`

- **Input:** None.
- **Output:** None.
- **DB writes:** `executescript(_CREATE_SQL)` then `commit()`. Creates 12 tables and 6 indexes.

---

#### `store_options_flow(self, ticker, market, data) -> None`

- **Input:**
  - `ticker: str`
  - `market: str`
  - `data: dict` with keys: `smfi`, `iv_rank`, `put_call_ratio`, `net_gamma`, `dark_pool_score`, `unusual_activity`, `raw`
- **Output:** None.
- **DB writes:** `INSERT INTO options_flow (...)`

SQL:
```sql
INSERT INTO options_flow
  (ticker, market, smfi, iv_rank, put_call_ratio, net_gamma,
   dark_pool_score, unusual_activity, raw, collected_at)
  VALUES (?,?,?,?,?,?,?,?,?,?)
```

- `unusual_activity` = `int(data.get("unusual_activity", False))` — 0 or 1.
- `raw` = `json.dumps(data.get("raw", {}))`.

---

#### `get_options_flow(self, ticker, hours_back=48) -> List[Dict]`

- **Input:**
  - `ticker: str`
  - `hours_back: int` (default 48)
- **Output:** `List[Dict]` — all options flow records for ticker in last N hours, newest first.
- **DB reads:**

SQL:
```sql
SELECT * FROM options_flow
WHERE ticker=? AND collected_at>=?
ORDER BY collected_at DESC
```

---

#### `get_unusual_options(self, hours_back=24, limit=20) -> List[Dict]`

- **Input:**
  - `hours_back: int` (default 24)
  - `limit: int` (default 20)
- **Output:** `List[Dict]` — options flow rows where `unusual_activity=1`, ordered by `smfi DESC`.
- **DB reads:**

SQL:
```sql
SELECT * FROM options_flow
WHERE collected_at>=? AND unusual_activity=1
ORDER BY smfi DESC LIMIT ?
```

---

#### `store_short_interest(self, ticker, market, data) -> None`

- **Input:**
  - `ticker: str`
  - `market: str`
  - `data: dict` with keys: `short_float_pct`, `days_to_cover`, `short_ratio`, `si_change_pct`, `trend`, `squeeze_score`, `squeeze_flag`, `report_date`
- **Output:** None.
- **DB writes:** `INSERT INTO short_interest (...)`

SQL:
```sql
INSERT INTO short_interest
  (ticker, market, short_float_pct, days_to_cover, short_ratio,
   si_change_pct, trend, squeeze_score, squeeze_flag, report_date, collected_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
```

- `squeeze_flag` = `int(data.get("squeeze_flag", False))` — 0 or 1.

---

#### `get_squeeze_candidates(self, min_score=60) -> List[Dict]`

- **Input:** `min_score: float` (default 60) — minimum `squeeze_score` threshold.
- **Output:** `List[Dict]` — all short_interest rows with `squeeze_score >= min_score`, ordered descending.
- **DB reads:**

SQL:
```sql
SELECT * FROM short_interest
WHERE squeeze_score >= ?
ORDER BY squeeze_score DESC
```

Note: No date filter. This returns ALL rows ever stored for all tickers above the threshold, including arbitrarily old short interest reports. There is no `hours_back` or `days_back` parameter.

---

#### `store_transcript(self, ticker, earnings_date, scores) -> None`

- **Input:**
  - `ticker: str`
  - `earnings_date: str` — ISO date string
  - `scores: dict` with keys: `source`, `hedge_ratio`, `forward_ratio`, `we_ratio`, `passive_ratio`, `tone_shift`, `deflection_score`, `prepared_sentiment`, `qa_sentiment`, `linguistic_score`, `guidance_signal`, `guidance_score`, `beat_quality`, `pead_multiplier`
- **Output:** None.
- **DB writes:** `INSERT OR REPLACE INTO transcripts (...)`

SQL:
```sql
INSERT OR REPLACE INTO transcripts
  (ticker, earnings_date, source, hedge_ratio, forward_ratio, we_ratio,
   passive_ratio, tone_shift, deflection_score, prepared_sentiment,
   qa_sentiment, linguistic_score, guidance_signal, guidance_score,
   beat_quality, pead_multiplier, raw_scores, analysed_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
```

- `raw_scores` = `json.dumps(scores)` (entire scores dict stored).
- Uses `INSERT OR REPLACE`, not upsert with UPDATE SET. This means on re-analysis, the entire row including the `id` is deleted and re-inserted. Row IDs are not stable across re-analysis.

Note: `transcripts` table has no UNIQUE constraint defined in `_CREATE_SQL` on `(ticker, earnings_date)`. The `INSERT OR REPLACE` logic therefore always inserts a new row and never replaces — the `OR REPLACE` clause can only trigger on a UNIQUE or PRIMARY KEY conflict. Each call to `store_transcript()` for the same ticker/earnings_date pair inserts a duplicate row.

---

#### `get_recent_transcripts(self, days_back=30, limit=20) -> List[Dict]`

- **Input:**
  - `days_back: int` (default 30)
  - `limit: int` (default 20)
- **Output:** `List[Dict]` — most recent transcript rows. `raw_scores` JSON-decoded with silent error swallow.
- **DB reads:**

SQL:
```sql
SELECT * FROM transcripts
WHERE analysed_at>=?
ORDER BY analysed_at DESC LIMIT ?
```

---

#### `store_congressional(self, disclosure) -> int`

- **Input:** `disclosure: dict` with keys: `member`, `chamber`, `ticker`, `transaction_type`, `amount_min`, `amount_max`, `transaction_date`, `filing_date`, `delay_days`, `signal_strength`, `credibility`, `committee_power`
- **Output:** `int` — `lastrowid`.
- **DB writes:** `INSERT INTO congressional (...)`

SQL:
```sql
INSERT INTO congressional
  (member, chamber, ticker, transaction_type, amount_min, amount_max,
   transaction_date, filing_date, delay_days, signal_strength, credibility,
   committee_power, collected_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
```

---

#### `get_recent_congressional(self, days_back=30, ticker=None) -> List[Dict]`

- **Input:**
  - `days_back: int` (default 30)
  - `ticker: Optional[str]`
- **Output:** `List[Dict]` — congressional disclosures. Filters on `transaction_date` (not `collected_at`).
- **DB reads:**

SQL (no ticker):
```sql
SELECT * FROM congressional
WHERE transaction_date>=?
ORDER BY transaction_date DESC
```

SQL (with ticker):
```sql
SELECT * FROM congressional
WHERE ticker=? AND transaction_date>=?
ORDER BY transaction_date DESC
```

Note: `cutoff` uses `.date().isoformat()` — a date string (YYYY-MM-DD), not a datetime string. This is consistent with comparing against `transaction_date` which is stored as a date string. Different approach from other methods which compare ISO datetime strings against datetime columns.

---

#### `update_member(self, member, record) -> None`

- **Input:**
  - `member: str`
  - `record: dict` with keys: `chamber`, `accuracy`, `excess_return`, `information_ratio`, `total_trades`, `credibility`, `credibility_score`, `committees`
- **Output:** None.
- **DB writes:** `INSERT OR REPLACE INTO congressional_members (...)`

SQL:
```sql
INSERT OR REPLACE INTO congressional_members
  (member, chamber, accuracy, excess_return, information_ratio,
   total_trades, credibility, credibility_score, committees, updated_at)
  VALUES (?,?,?,?,?,?,?,?,?,?)
```

- `committees` = `json.dumps(record.get("committees", []))`.
- `congressional_members` has `UNIQUE(member)`. `INSERT OR REPLACE` here will trigger on that unique constraint and correctly replace the existing row. Row IDs are unstable on updates.

---

#### `get_all_members(self) -> List[Dict]`

- **Input:** None.
- **Output:** `List[Dict]` — all congressional member track records ordered by `credibility_score DESC`. `committees` JSON-decoded with silent error swallow.
- **DB reads:**

SQL:
```sql
SELECT * FROM congressional_members ORDER BY credibility_score DESC
```

---

#### `store_earnings_quality(self, ticker, earnings_date, classification) -> None`

- **Input:**
  - `ticker: str`
  - `earnings_date: str`
  - `classification: dict` with keys: `beat_quality`, `quality_score`, `revenue_beat_pct`, `eps_beat_pct`, `guidance_signal`, `final_pead_multiplier`, `suppress_pead`
- **Output:** None.
- **DB writes:** `INSERT OR REPLACE INTO earnings_quality (...)`

SQL:
```sql
INSERT OR REPLACE INTO earnings_quality
  (ticker, earnings_date, beat_quality, quality_score, revenue_beat_pct,
   eps_beat_pct, guidance_signal, pead_multiplier, suppress_pead,
   details, analysed_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
```

- `pead_multiplier` is read from `classification.get("final_pead_multiplier")` — note the key name in the input dict is `final_pead_multiplier` but the column is named `pead_multiplier`.
- `suppress_pead` = `int(classification.get("suppress_pead", False))`.
- `details` = `json.dumps(classification)`.
- `earnings_quality` has `UNIQUE(ticker, earnings_date)`. `INSERT OR REPLACE` correctly upserts here. Row IDs are unstable on updates.

---

#### `get_pead_modifier(self, ticker) -> Optional[float]`

- **Input:** `ticker: str`
- **Output:** `Optional[float]` — most recent PEAD multiplier for the ticker, or `0.0` if `suppress_pead=1`, or `None` if no record exists.
- **DB reads:**

SQL:
```sql
SELECT pead_multiplier, suppress_pead FROM earnings_quality
WHERE ticker=?
ORDER BY earnings_date DESC LIMIT 1
```

Logic:
```
if row["suppress_pead"]:
    return 0.0
return row["pead_multiplier"]
```

---

#### `log_signal(self, signal) -> int`

- **Input:** `signal: dict` with keys: `ticker`, `signal_type`, `tier` (default 3), `direction`, `confidence`, `confluence`, `pead_modifier`, `sources`
- **Output:** `int` — `lastrowid`.
- **DB writes:** `INSERT INTO deepdata_signals (...)`

SQL:
```sql
INSERT INTO deepdata_signals
  (ticker, signal_type, tier, direction, confidence, confluence,
   pead_modifier, sources, generated_at)
  VALUES (?,?,?,?,?,?,?,?,?)
```

- `sources` = `json.dumps(signal.get("sources", []))`.
- `outcome_return` and `resolved_at` columns are never written (same pattern as FrontierStore's `signal_log`).

---

#### `get_recent_signals(self, hours_back=48, tier=None, limit=50) -> List[Dict]`

- **Input:**
  - `hours_back: int` (default 48)
  - `tier: Optional[int]` — filter by tier
  - `limit: int` (default 50)
- **Output:** `List[Dict]` — signals from last N hours. `sources` JSON-decoded with silent error swallow.
- **DB reads:**

SQL (no tier filter):
```sql
SELECT * FROM deepdata_signals
WHERE generated_at>=?
ORDER BY generated_at DESC LIMIT ?
```

SQL (with tier filter):
```sql
SELECT * FROM deepdata_signals
WHERE generated_at>=? AND tier=?
ORDER BY generated_at DESC LIMIT ?
```

---

#### `store_factors(self, ticker, snapshot_date, factors) -> None`

- **Input:**
  - `ticker: str`
  - `snapshot_date: str` — ISO date string
  - `factors: dict` with keys: `momentum`, `value`, `quality`, `size`, `volatility`, `earnings_quality_factor`, `altdata_factor`, `supply_chain_factor`, `congressional_factor`
- **Output:** None.
- **DB writes:** `INSERT OR REPLACE INTO factor_exposures (...)`

SQL:
```sql
INSERT OR REPLACE INTO factor_exposures
  (ticker, snapshot_date, momentum, value, quality, size, volatility,
   earnings_quality_factor, altdata_factor, supply_chain_factor,
   congressional_factor, raw, computed_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
```

- `raw` = `json.dumps(factors)`.
- `factor_exposures` has `UNIQUE(ticker, snapshot_date)`. `INSERT OR REPLACE` correctly upserts.

Note: There is no `get_factors()` read method in this file. Factor data can be written but not retrieved through the store's public API.

---

#### `store_pattern(self, pattern) -> None`

- **Input:** `pattern: dict` with keys: `name`, `modules`, `sharpe`, `p_value`, `permutation_pct`, `dsr`, `nonsense_score`, `status`, `economic_story`
- **Output:** None.
- **DB writes:** `INSERT OR REPLACE INTO pattern_registry (...)`

SQL:
```sql
INSERT OR REPLACE INTO pattern_registry
  (name, modules, sharpe, p_value, permutation_pct, dsr, nonsense_score,
   status, economic_story, found_at, last_checked)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
```

- `modules` = `json.dumps(pattern.get("modules", []))`.
- Both `found_at` and `last_checked` are set to `now` (current UTC timestamp). On a replace, `found_at` will be overwritten with the time of the update, not the original discovery time. The original `found_at` is lost on re-insert.
- `pattern_registry` has `UNIQUE(name)`. `INSERT OR REPLACE` correctly upserts.

---

#### `get_live_patterns(self) -> List[Dict]`

- **Input:** None.
- **Output:** `List[Dict]` — all patterns with `status='live'`, ordered by `sharpe DESC`. `modules` JSON-decoded with silent error swallow.
- **DB reads:**

SQL:
```sql
SELECT * FROM pattern_registry WHERE status='live' ORDER BY sharpe DESC
```

---

#### `status_summary(self) -> Dict`

- **Input:** None.
- **Output:** `Dict` with keys:
  - `options_records_24h` — options_flow count in last 24 hours
  - `squeeze_candidates` — short_interest rows with `squeeze_score >= 60`
  - `transcripts_30d` — transcripts count in last 30 days
  - `congressional_30d` — congressional rows by `transaction_date` in last 30 days
  - `tier1_signals_24h` — deepdata_signals with `tier=1` in last 24 hours
  - `live_patterns` — pattern_registry rows with `status='live'`
- **DB reads:** 6 COUNT queries.

SQL queries:
```sql
SELECT COUNT(*) FROM options_flow WHERE collected_at >= datetime('now','-1 day')
SELECT COUNT(*) FROM short_interest WHERE squeeze_score >= 60
SELECT COUNT(*) FROM transcripts WHERE analysed_at >= datetime('now','-30 days')
SELECT COUNT(*) FROM congressional WHERE transaction_date >= date('now','-30 days')
SELECT COUNT(*) FROM deepdata_signals WHERE tier=1 AND generated_at >= datetime('now','-1 day')
SELECT COUNT(*) FROM pattern_registry WHERE status='live'
```

---

#### `backup(self, backup_dir="output/backups") -> str`

- **Input:** `backup_dir: str` (default `"output/backups"`)
- **Output:** `str` — path of the backup file.
- **Action:** Creates backup directory, copies database with `shutil.copy2()`. Filename format: `deepdata_{YYYYMMDD}.db`.
- **DB reads/writes:** None via SQLite.

---

#### `close(self) -> None`

- **Input:** None.
- **Output:** None.
- **Action:** Closes the thread-local connection if open.

---

## C) MATHEMATICS

No mathematical formulas are computed inside this file. It is a pure storage layer. All computed values (SMFI, squeeze_score, IV rank, linguistic_score, PEAD multiplier, DSR, permutation_pct, etc.) are accepted as already-computed inputs from upstream processing modules.

The only conditional logic approaching a formula is in `get_pead_modifier()`:
```
if suppress_pead:
    return 0.0
else:
    return row["pead_multiplier"]
```

This is a lookup with a suppression gate, not a computation.

---

## D) DATA FLOWS

**Inputs (what enters):**
- Options flow snapshots from options data collector (SMFI, IV rank, put-call ratio, net gamma, dark pool score, unusual activity flag)
- Short interest reports from FINRA/FCA (short float %, days to cover, short ratio, squeeze score)
- Squeeze event records (historical and predicted, with probability and features)
- Earnings transcript analysis scores (hedge ratio, forward ratio, linguistic score, PEAD multiplier)
- Patent filing and citation records (per ticker, per patent)
- Supply chain relationship edges (supplier-customer pairs with dependency weights and risk scores)
- Congressional disclosure records (member, ticker, transaction type, amounts, timing, signal strength)
- Congressional member track records (accuracy, excess return, information ratio, credibility)
- Earnings quality classifications (beat quality, revenue/EPS beat pcts, guidance signal, PEAD multiplier)
- Generated deepdata signals (ticker, type, tier, direction, confidence, confluence, PEAD modifier, sources)
- Factor exposure snapshots (momentum, value, quality, size, volatility, altdata, supply chain, congressional)
- Cross-module pattern definitions (name, constituent modules, Sharpe, p-value, permutation_pct, DSR)

**Outputs (what leaves):**
- `get_options_flow` → `List[Dict]` from `options_flow`
- `get_unusual_options` → `List[Dict]` from `options_flow`
- `get_squeeze_candidates` → `List[Dict]` from `short_interest`
- `get_recent_transcripts` → `List[Dict]` from `transcripts`
- `get_recent_congressional` → `List[Dict]` from `congressional`
- `get_all_members` → `List[Dict]` from `congressional_members`
- `get_pead_modifier` → `Optional[float]` from `earnings_quality`
- `get_recent_signals` → `List[Dict]` from `deepdata_signals`
- `get_live_patterns` → `List[Dict]` from `pattern_registry`
- `status_summary` → `Dict` of counts

**Database file:** `deepdata/storage/deepdata.db` (default, configurable via `config["deepdata"]["storage_path"]`)

**Tables written:**
- `options_flow`
- `short_interest`
- `squeeze_events` (schema defined; no write method in this file — see Issues)
- `transcripts`
- `patent_data` (schema defined; no write method in this file — see Issues)
- `supply_chain` (schema defined; no write method in this file — see Issues)
- `congressional`
- `congressional_members`
- `earnings_quality`
- `deepdata_signals`
- `factor_exposures`
- `pattern_registry`

**Tables read:**
- `options_flow`
- `short_interest`
- `transcripts`
- `congressional`
- `congressional_members`
- `earnings_quality`
- `deepdata_signals`
- `pattern_registry`

**Tables NEVER read by any method in this file:**
- `squeeze_events`
- `patent_data`
- `supply_chain`
- `factor_exposures`

---

## E) DEPENDENCIES

**Internal Apollo imports:**
- None. This file has no imports from other Apollo modules.

**External library imports:**
- `json` (stdlib)
- `logging` (stdlib)
- `os` (stdlib)
- `shutil` (stdlib)
- `sqlite3` (stdlib)
- `threading` (stdlib)
- `datetime`, `timezone` from `datetime` (stdlib)
- `Path` from `pathlib` (stdlib)
- `Any`, `Dict`, `List`, `Optional` from `typing` (stdlib)

All dependencies are Python standard library. No third-party packages.

---

## F) WIRING STATUS

**Connected to live trading path: YES, indirectly.**

`DeepDataStore` is the persistence layer for the deepdata intelligence pipeline. It stores:
1. Deepdata signals (`deepdata_signals`) with tier, direction, confidence, and confluence — these feed into position sizing decisions.
2. PEAD modifiers (`earnings_quality`, read via `get_pead_modifier()`) — the PEAD modifier is applied to signal weights around earnings events in live trading.
3. Live patterns (`pattern_registry`, read via `get_live_patterns()`) — live patterns may gate which signal combinations are used.
4. Congressional signal strength — congressional disclosures with signal strength feed into directional signals.

The store does not call into the trading engine directly. It is written to by upstream deepdata collection/analysis modules and read from by downstream signal aggregation and sizing modules.

---

## G) ISSUES FOUND

1. **`squeeze_events` table has no write method.** Schema is defined with 9 columns for both historical and predicted squeeze events, including probability and JSON features. No `store_squeeze_event()` or any INSERT method for this table exists in the file. The table is created at startup and stays empty. There is also no read method for it.

2. **`patent_data` table has no write method.** Schema is defined with 10 columns for patent filing and citation records. No `store_patent()` or any INSERT method exists. No read method either. The table is created at startup and stays empty.

3. **`supply_chain` table has no write method.** Schema is defined with 9 columns for supplier-customer relationship graph edges. No `store_supply_chain()` or any INSERT method exists. No read method either. Created at startup, stays empty.

4. **`factor_exposures` table has no read method.** `store_factors()` exists and works, but there is no `get_factors()` or any SELECT method. Factor data written to this table is unreadable through the store's public API. The data accumulates with no retrieval path.

5. **`store_transcript()` uses `INSERT OR REPLACE` but `transcripts` has no UNIQUE constraint on `(ticker, earnings_date)`.** The schema `CREATE TABLE transcripts` does not include `UNIQUE(ticker, earnings_date)`. Therefore the `OR REPLACE` conflict resolution clause never triggers. Every call to `store_transcript()` for the same ticker+earnings_date pair inserts a duplicate row rather than replacing. Over time, the same transcript analysis will accumulate multiple identical rows. `get_recent_transcripts()` will return all duplicates.

6. **`store_pattern()` overwrites `found_at` on every upsert.** Both `found_at` and `last_checked` are set to `now` in the INSERT OR REPLACE. When a pattern is re-evaluated and stored again, its original discovery date is lost because `found_at` is replaced with the update timestamp. There is no mechanism to preserve the original `found_at`.

7. **`get_squeeze_candidates()` has no date filter.** It returns all rows from `short_interest` ever stored where `squeeze_score >= min_score`, regardless of when the report was collected. A short interest report from months ago with a high squeeze score will appear in results alongside current reports. There is no `days_back` parameter.

8. **`deepdata_signals.outcome_return` and `deepdata_signals.resolved_at` are never written.** Same pattern as `frontier_store.py`. Columns defined in schema, INSERT method never writes them, no update/resolve method exists. Signal P&L is permanently unresolved.

9. **`store_short_interest()` does not deduplicate.** Each call inserts a new row. For biweekly FINRA/FCA reports covering the same reporting period, a re-ingest would produce duplicate rows. There is no UNIQUE constraint on `(ticker, report_date)` to prevent this. `get_squeeze_candidates()` would then double-count or return duplicates for the same ticker.

10. **`status_summary()` squeeze_candidates count uses hardcoded threshold 60**, not `get_squeeze_candidates()`'s default parameter. These two entry points into the same data use the same threshold by coincidence, but they are not linked — changing the `get_squeeze_candidates()` default would not change the `status_summary()` count.

11. **`PRAGMA foreign_keys=ON` is set per connection in `_conn()`.** This is correct behaviour (contrast with FrontierStore). However, no table in this file actually defines any foreign key constraints, so the pragma has no practical effect.

12. **`store_congressional()` has no deduplication.** Multiple calls for the same disclosure (same member, ticker, transaction_date) insert multiple rows. There is no UNIQUE constraint. Duplicate disclosure records would silently accumulate.

---

---

# TABLE SCHEMAS — COMPLETE DOCUMENTATION

---

## FRONTIER STORE TABLES (frontier.db)

---

### Table: `raw_signals`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| collector | TEXT | NO | — | NO |
| signal_name | TEXT | NO | — | NO |
| ticker | TEXT | YES | NULL | NO |
| market | TEXT | YES | NULL | NO |
| value | REAL | YES | NULL | NO |
| raw_data | TEXT | YES | NULL | NO |
| quality | REAL | YES | 1.0 | NO |
| collected_at | TEXT | NO | — | NO |

**Indexes:** `idx_raw_collector ON raw_signals(collector, collected_at)`, `idx_raw_ticker ON raw_signals(ticker, collected_at)`

**Written by:** `FrontierStore.store_raw()` — frontier signal collectors
**Read by:** `FrontierStore.get_raw_history()`, `FrontierStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone; depends on whether frontier collectors are running. Infrastructure exists.

---

### Table: `umci_history`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| umci | REAL | NO | — | NO |
| level | TEXT | NO | — | NO |
| physical | REAL | YES | NULL | NO |
| social | REAL | YES | NULL | NO |
| scientific | REAL | YES | NULL | NO |
| financial | REAL | YES | NULL | NO |
| altdata | REAL | YES | NULL | NO |
| dominant_dim | TEXT | YES | NULL | NO |
| position_mult | REAL | YES | NULL | NO |
| halt | INTEGER | YES | 0 | NO |
| full_breakdown | TEXT | YES | NULL | NO |
| recorded_at | TEXT | NO | — | NO |

**Indexes:** `idx_umci_ts ON umci_history(recorded_at)`

**Written by:** `FrontierStore.store_umci()` — UMCI computation module
**Read by:** `FrontierStore.get_umci_history()`, `FrontierStore.get_last_umci()`, `FrontierStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `validation_results`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| signal_name | TEXT | NO | — | NO |
| passed | INTEGER | NO | — | NO |
| t_stat | REAL | YES | NULL | NO |
| p_value | REAL | YES | NULL | NO |
| p_bonferroni | REAL | YES | NULL | NO |
| monte_carlo_pct | REAL | YES | NULL | NO |
| sharpe | REAL | YES | NULL | NO |
| deflated_sharpe | REAL | YES | NULL | NO |
| fsp | REAL | YES | NULL | NO |
| evidence_grade | TEXT | YES | NULL | NO |
| n_obs | INTEGER | YES | NULL | NO |
| regime_stable | INTEGER | YES | NULL | NO |
| full_result | TEXT | YES | NULL | NO |
| validated_at | TEXT | NO | — | NO |

**Indexes:** `idx_val_signal ON validation_results(signal_name, validated_at)`

**Written by:** `FrontierStore.store_validation()` — signal validation module
**Read by:** `FrontierStore.get_validation_history()`, `FrontierStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `watchlist`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| name | TEXT | NO | — | NO |
| description | TEXT | YES | NULL | NO |
| formula | TEXT | YES | NULL | NO |
| correlation | REAL | YES | NULL | NO |
| optimal_lag | INTEGER | YES | NULL | NO |
| t_stat | REAL | YES | NULL | NO |
| p_bonferroni | REAL | YES | NULL | NO |
| monte_carlo_pct | REAL | YES | NULL | NO |
| deflated_sharpe | REAL | YES | NULL | NO |
| n_obs | INTEGER | YES | NULL | NO |
| validation_status | TEXT | YES | 'PENDING' | NO |
| live_days | INTEGER | YES | 0 | NO |
| live_accuracy | REAL | YES | NULL | NO |
| sizing_tier | INTEGER | YES | 5 | NO |
| notes | TEXT | YES | NULL | NO |
| discovered_at | TEXT | NO | — | NO |
| updated_at | TEXT | YES | NULL | NO |

**Unique constraint:** `UNIQUE(name)`

**Written by:** `FrontierStore.upsert_watchlist()` — correlation discovery module
**Read by:** `FrontierStore.get_watchlist()`, `FrontierStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `parameter_history`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| signal_name | TEXT | NO | — | NO |
| param_name | TEXT | NO | — | NO |
| published_value | REAL | NO | — | NO |
| old_value | REAL | NO | — | NO |
| new_value | REAL | NO | — | NO |
| performance_delta | REAL | YES | NULL | NO |
| reason | TEXT | YES | NULL | NO |
| changed_at | TEXT | NO | — | NO |

**Written by:** `FrontierStore.log_parameter_drift()` — parameter monitoring module
**Read by:** `FrontierStore.get_parameter_history()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `evidence_records`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| signal_name | TEXT | NO | — | NO |
| sizing_tier | INTEGER | NO | — | NO |
| live_days | INTEGER | YES | 0 | NO |
| live_accuracy | REAL | YES | NULL | NO |
| live_sharpe | REAL | YES | NULL | NO |
| n_live_signals | INTEGER | YES | 0 | NO |
| last_promoted | TEXT | YES | NULL | NO |
| updated_at | TEXT | NO | — | NO |

**Unique constraint:** `UNIQUE(signal_name)`

**Written by:** `FrontierStore.update_evidence()` — evidence accumulation module
**Read by:** `FrontierStore.get_evidence()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** `last_promoted` is never written by any method in this file.

---

### Table: `signal_log`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| signal_name | TEXT | NO | — | NO |
| signal_type | TEXT | NO | — | NO |
| direction | INTEGER | YES | NULL | NO |
| confidence | REAL | YES | NULL | NO |
| sizing_tier | INTEGER | YES | NULL | NO |
| position_size | REAL | YES | NULL | NO |
| sources | TEXT | YES | NULL | NO |
| generated_at | TEXT | NO | — | NO |
| outcome_return | REAL | YES | NULL | NO |
| resolved_at | TEXT | YES | NULL | NO |

**Indexes:** `idx_siglog_ticker ON signal_log(ticker, generated_at)`

**Written by:** `FrontierStore.log_signal()` — frontier signal generator
**Read by:** `FrontierStore.get_recent_signals()`, `FrontierStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** `outcome_return` and `resolved_at` are never written by any method. All outcome columns are permanently NULL.

---

### Table: `interaction_log`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| name | TEXT | NO | — | NO |
| components | TEXT | NO | — | NO |
| value | REAL | YES | NULL | NO |
| tested | INTEGER | YES | 0 | NO |
| t_stat | REAL | YES | NULL | NO |
| p_value | REAL | YES | NULL | NO |
| significant | INTEGER | YES | 0 | NO |
| logged_at | TEXT | NO | — | NO |

**Written by:** NOTHING — no INSERT method exists for this table in this file.
**Read by:** NOTHING — no SELECT method exists for this table in this file.
**Currently receiving data:** NO. This table is defined but completely unreachable through the store's public API.

---

## DEEPDATA STORE TABLES (deepdata.db)

---

### Table: `options_flow`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| market | TEXT | NO | — | NO |
| smfi | REAL | YES | NULL | NO |
| iv_rank | REAL | YES | NULL | NO |
| put_call_ratio | REAL | YES | NULL | NO |
| net_gamma | REAL | YES | NULL | NO |
| dark_pool_score | REAL | YES | NULL | NO |
| unusual_activity | INTEGER | YES | 0 | NO |
| raw | TEXT | YES | NULL | NO |
| collected_at | TEXT | NO | — | NO |

**Indexes:** `idx_options_ticker ON options_flow(ticker, collected_at)`

**Written by:** `DeepDataStore.store_options_flow()` — options flow collector
**Read by:** `DeepDataStore.get_options_flow()`, `DeepDataStore.get_unusual_options()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `short_interest`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| market | TEXT | NO | — | NO |
| short_float_pct | REAL | YES | NULL | NO |
| days_to_cover | REAL | YES | NULL | NO |
| short_ratio | REAL | YES | NULL | NO |
| si_change_pct | REAL | YES | NULL | NO |
| trend | TEXT | YES | NULL | NO |
| squeeze_score | REAL | YES | NULL | NO |
| squeeze_flag | INTEGER | YES | 0 | NO |
| report_date | TEXT | YES | NULL | NO |
| collected_at | TEXT | NO | — | NO |

**Indexes:** `idx_si_ticker ON short_interest(ticker, report_date)`

**Written by:** `DeepDataStore.store_short_interest()` — short interest collector
**Read by:** `DeepDataStore.get_squeeze_candidates()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** No UNIQUE constraint on `(ticker, report_date)`. Duplicate rows can accumulate.

---

### Table: `squeeze_events`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| event_type | TEXT | NO | — | NO |
| start_date | TEXT | YES | NULL | NO |
| peak_gain_pct | REAL | YES | NULL | NO |
| duration_days | INTEGER | YES | NULL | NO |
| pre_short_float | REAL | YES | NULL | NO |
| probability | REAL | YES | NULL | NO |
| features | TEXT | YES | NULL | NO |
| recorded_at | TEXT | NO | — | NO |

**Written by:** NOTHING — no INSERT method exists for this table in this file.
**Read by:** NOTHING — no SELECT method exists for this table in this file.
**Currently receiving data:** NO. Dead table.

---

### Table: `transcripts`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| earnings_date | TEXT | NO | — | NO |
| source | TEXT | YES | NULL | NO |
| hedge_ratio | REAL | YES | NULL | NO |
| forward_ratio | REAL | YES | NULL | NO |
| we_ratio | REAL | YES | NULL | NO |
| passive_ratio | REAL | YES | NULL | NO |
| tone_shift | REAL | YES | NULL | NO |
| deflection_score | REAL | YES | NULL | NO |
| prepared_sentiment | REAL | YES | NULL | NO |
| qa_sentiment | REAL | YES | NULL | NO |
| linguistic_score | REAL | YES | NULL | NO |
| guidance_signal | TEXT | YES | NULL | NO |
| guidance_score | REAL | YES | NULL | NO |
| beat_quality | TEXT | YES | NULL | NO |
| pead_multiplier | REAL | YES | NULL | NO |
| raw_scores | TEXT | YES | NULL | NO |
| analysed_at | TEXT | NO | — | NO |

**Written by:** `DeepDataStore.store_transcript()` — transcript analysis module
**Read by:** `DeepDataStore.get_recent_transcripts()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** No UNIQUE constraint on `(ticker, earnings_date)`. `INSERT OR REPLACE` never triggers; each call inserts a new duplicate row.

---

### Table: `patent_data`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| market | TEXT | NO | — | NO |
| patent_number | TEXT | YES | NULL | NO |
| filing_date | TEXT | YES | NULL | NO |
| grant_date | TEXT | YES | NULL | NO |
| cpc_class | TEXT | YES | NULL | NO |
| citations | INTEGER | YES | 0 | NO |
| velocity_score | REAL | YES | NULL | NO |
| innovation_score | REAL | YES | NULL | NO |
| collected_at | TEXT | NO | — | NO |

**Written by:** NOTHING — no INSERT method exists for this table in this file.
**Read by:** NOTHING — no SELECT method exists for this table in this file.
**Currently receiving data:** NO. Dead table.

---

### Table: `supply_chain`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| supplier_ticker | TEXT | YES | NULL | NO |
| customer_ticker | TEXT | YES | NULL | NO |
| dependency_weight | REAL | YES | NULL | NO |
| relationship_type | TEXT | YES | NULL | NO |
| depth | INTEGER | YES | 1 | NO |
| source | TEXT | YES | NULL | NO |
| upstream_risk | REAL | YES | NULL | NO |
| downstream_risk | REAL | YES | NULL | NO |
| updated_at | TEXT | NO | — | NO |

**Written by:** NOTHING — no INSERT method exists for this table in this file.
**Read by:** NOTHING — no SELECT method exists for this table in this file.
**Currently receiving data:** NO. Dead table.
**Note:** `supply_chain_factor` column in `factor_exposures` references supply chain data conceptually, but this table is not populated.

---

### Table: `congressional`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| member | TEXT | NO | — | NO |
| chamber | TEXT | YES | NULL | NO |
| ticker | TEXT | NO | — | NO |
| transaction_type | TEXT | YES | NULL | NO |
| amount_min | REAL | YES | NULL | NO |
| amount_max | REAL | YES | NULL | NO |
| transaction_date | TEXT | YES | NULL | NO |
| filing_date | TEXT | YES | NULL | NO |
| delay_days | INTEGER | YES | NULL | NO |
| signal_strength | REAL | YES | NULL | NO |
| credibility | TEXT | YES | NULL | NO |
| committee_power | REAL | YES | NULL | NO |
| collected_at | TEXT | NO | — | NO |

**Indexes:** `idx_congress_ticker ON congressional(ticker, transaction_date)`

**Written by:** `DeepDataStore.store_congressional()` — congressional disclosure collector
**Read by:** `DeepDataStore.get_recent_congressional()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** No UNIQUE constraint. Duplicate disclosures can be inserted on re-collection.

---

### Table: `congressional_members`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| member | TEXT | NO | — | NO |
| chamber | TEXT | YES | NULL | NO |
| accuracy | REAL | YES | NULL | NO |
| excess_return | REAL | YES | NULL | NO |
| information_ratio | REAL | YES | NULL | NO |
| total_trades | INTEGER | YES | 0 | NO |
| credibility | TEXT | YES | NULL | NO |
| credibility_score | REAL | YES | NULL | NO |
| committees | TEXT | YES | NULL | NO |
| updated_at | TEXT | NO | — | NO |

**Unique constraint:** `UNIQUE(member)`

**Written by:** `DeepDataStore.update_member()` — congressional analysis module
**Read by:** `DeepDataStore.get_all_members()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `earnings_quality`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| earnings_date | TEXT | NO | — | NO |
| beat_quality | TEXT | YES | NULL | NO |
| quality_score | REAL | YES | NULL | NO |
| revenue_beat_pct | REAL | YES | NULL | NO |
| eps_beat_pct | REAL | YES | NULL | NO |
| guidance_signal | TEXT | YES | NULL | NO |
| pead_multiplier | REAL | YES | NULL | NO |
| suppress_pead | INTEGER | YES | 0 | NO |
| details | TEXT | YES | NULL | NO |
| analysed_at | TEXT | NO | — | NO |

**Unique constraint:** `UNIQUE(ticker, earnings_date)`

**Written by:** `DeepDataStore.store_earnings_quality()` — earnings quality classifier
**Read by:** `DeepDataStore.get_pead_modifier()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.

---

### Table: `deepdata_signals`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| signal_type | TEXT | NO | — | NO |
| tier | INTEGER | NO | — | NO |
| direction | INTEGER | YES | NULL | NO |
| confidence | REAL | YES | NULL | NO |
| confluence | REAL | YES | NULL | NO |
| pead_modifier | REAL | YES | NULL | NO |
| sources | TEXT | YES | NULL | NO |
| generated_at | TEXT | NO | — | NO |
| outcome_return | REAL | YES | NULL | NO |
| resolved_at | TEXT | YES | NULL | NO |

**Indexes:** `idx_signals_ticker ON deepdata_signals(ticker, generated_at)`

**Written by:** `DeepDataStore.log_signal()` — deepdata signal generator
**Read by:** `DeepDataStore.get_recent_signals()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** `outcome_return` and `resolved_at` are never written. All outcome columns permanently NULL.

---

### Table: `factor_exposures`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| ticker | TEXT | NO | — | NO |
| snapshot_date | TEXT | NO | — | NO |
| momentum | REAL | YES | NULL | NO |
| value | REAL | YES | NULL | NO |
| quality | REAL | YES | NULL | NO |
| size | REAL | YES | NULL | NO |
| volatility | REAL | YES | NULL | NO |
| earnings_quality_factor | REAL | YES | NULL | NO |
| altdata_factor | REAL | YES | NULL | NO |
| supply_chain_factor | REAL | YES | NULL | NO |
| congressional_factor | REAL | YES | NULL | NO |
| raw | TEXT | YES | NULL | NO |
| computed_at | TEXT | NO | — | NO |

**Unique constraint:** `UNIQUE(ticker, snapshot_date)`

**Indexes:** `idx_factors_ticker ON factor_exposures(ticker, snapshot_date)`

**Written by:** `DeepDataStore.store_factors()` — factor computation module
**Read by:** NOTHING — no SELECT method exists for this table in this file.
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists for writing but data is unreadable through the store.

---

### Table: `pattern_registry`

| Column | Type | Nullable | Default | Primary Key |
|--------|------|----------|---------|-------------|
| id | INTEGER | NO | AUTOINCREMENT | YES |
| name | TEXT | NO | — | NO |
| modules | TEXT | NO | — | NO |
| sharpe | REAL | YES | NULL | NO |
| p_value | REAL | YES | NULL | NO |
| permutation_pct | REAL | YES | NULL | NO |
| dsr | REAL | YES | NULL | NO |
| nonsense_score | REAL | YES | NULL | NO |
| status | TEXT | YES | 'candidate' | NO |
| economic_story | TEXT | YES | NULL | NO |
| found_at | TEXT | NO | — | NO |
| last_checked | TEXT | YES | NULL | NO |

**Unique constraint:** `UNIQUE(name)`

**Written by:** `DeepDataStore.store_pattern()` — pattern discovery module
**Read by:** `DeepDataStore.get_live_patterns()`, `DeepDataStore.status_summary()`
**Currently receiving data:** Cannot be determined from code alone. Infrastructure exists.
**Note:** `found_at` is overwritten on every upsert, losing original discovery timestamp.

---

---

# GROUP 8 GATE

**Files read:**
1. `/home/dannyelticala/quant-fund/frontier/storage/frontier_store.py` (515 lines, read completely)
2. `/home/dannyelticala/quant-fund/deepdata/storage/deepdata_store.py` (650 lines, read completely)

**Tables documented (19 total):**

From frontier.db (7 tables):
- `raw_signals`
- `umci_history`
- `validation_results`
- `watchlist`
- `parameter_history`
- `evidence_records`
- `signal_log`
- `interaction_log`

From deepdata.db (12 tables):
- `options_flow`
- `short_interest`
- `squeeze_events`
- `transcripts`
- `patent_data`
- `supply_chain`
- `congressional`
- `congressional_members`
- `earnings_quality`
- `deepdata_signals`
- `factor_exposures`
- `pattern_registry`

**Key findings:**

Both stores are pure Python stdlib SQLite wrappers — no third-party dependencies. Both use threading.local() for per-thread connections and WAL journal mode. Both define more tables than they have methods to support. Signal outcome tracking (outcome_return, resolved_at) is defined in both stores' signal tables but never written — the live P&L track record for both frontier and deepdata signals is permanently empty. Both stores use UTC ISO timestamps as TEXT columns rather than native SQLite DATETIME, which is consistent but means SQLite date functions (datetime('now')) must match that format — they do in the COUNT queries.

**Dead tables (defined but never written to):**
- `frontier.db` — `interaction_log`: schema defined, zero read or write methods in frontier_store.py
- `deepdata.db` — `squeeze_events`: schema defined, zero read or write methods in deepdata_store.py
- `deepdata.db` — `patent_data`: schema defined, zero read or write methods in deepdata_store.py
- `deepdata.db` — `supply_chain`: schema defined, zero read or write methods in deepdata_store.py

**Tables written but never read through the store API:**
- `deepdata.db` — `factor_exposures`: `store_factors()` exists, no `get_factors()` method

**Contradictions:**

1. `transcripts` table: `store_transcript()` uses `INSERT OR REPLACE` with the stated intent of deduplication, but the `transcripts` schema has no UNIQUE constraint on `(ticker, earnings_date)`. The OR REPLACE clause cannot trigger. Every call inserts a new row. The docstring and intent contradict the actual behaviour.

2. `store_pattern()` sets both `found_at` and `last_checked` to `now` on every upsert. The field name `found_at` implies the original discovery date, but it is silently overwritten on every re-evaluation. The field cannot actually store when the pattern was first found.

3. `upsert_watchlist()` ON CONFLICT clause does not update `t_stat`, `p_bonferroni`, `optimal_lag`, or `formula`. These are statistical fields that would naturally change as more data is collected, but they are frozen at initial INSERT values. Other statistical fields (`correlation`, `monte_carlo_pct`, `deflated_sharpe`, `n_obs`) are updated. The split is unexplained and inconsistent.

4. `frontier_store._conn()` does NOT set `PRAGMA foreign_keys=ON` per connection, while `deepdata_store._conn()` does. This is an inconsistency between the two stores which are otherwise structurally identical. Neither store actually defines foreign key constraints in its tables, so the practical effect is zero.

5. Both signal tables (`frontier.signal_log` and `deepdata.deepdata_signals`) include `outcome_return` and `resolved_at` columns, but no resolution method exists in either file. The system logs signals and never closes the loop on their outcomes. This contradicts the design goal of building a "live track record" for signals.

**Proceed to GROUP 9: YES**
