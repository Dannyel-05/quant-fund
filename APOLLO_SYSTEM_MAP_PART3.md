# APOLLO SYSTEM MAP — PART 3
## Trade Recording and Cooling-Off Mechanics

Generated: 2026-04-08
Files read: 2 (all lines, no skimming)

---

## FILE 1: `/home/dannyelticala/quant-fund/closeloop/storage/closeloop_store.py`

---

### A) PURPOSE

`ClosedLoopStore` is the central SQLite persistence layer for the entire closed-loop learning system. It stores every trade in `trade_ledger`, records PnL attribution, entry timing outcomes, signal weights and weight history, signal regime performance, signal interaction data, peer influence outcomes, analyst revision outcomes, academic paper matches, stress test outcomes, stress predictions, drawdown events, tax disposals, and benchmark performance.

The database is created on first connect. No tables are dropped automatically. Thread safety is achieved by using one SQLite connection per thread via `threading.local()`. WAL journal mode is used.

The class also exposes a backwards-compatible alias: `CloseloopStore = ClosedLoopStore` at module level.

---

### B) CLASSES AND METHODS

**Class: `ClosedLoopStore`**

#### `__init__(self, config: Optional[Dict] = None) -> None`
- Inputs: optional config dict
- Reads `config["closeloop"]["storage_path"]`; defaults to `"closeloop/storage/closeloop.db"`
- Creates parent directory if missing
- Calls `_init_db()`

#### `_conn(self) -> sqlite3.Connection`
- Inputs: none
- Returns: thread-local SQLite connection
- Sets `row_factory = sqlite3.Row`, enables WAL and foreign keys on new connections

#### `_init_db(self) -> None`
- Inputs: none
- Runs the full `_CREATE_TABLES` script via `executescript()`
- Calls `_ensure_columns()` for migration

#### `_ensure_columns(self) -> None`
- Inputs: none
- Reads `PRAGMA table_info(trade_ledger)` to get existing columns
- If `order_status` column is missing: `ALTER TABLE trade_ledger ADD COLUMN order_status TEXT DEFAULT 'unknown'`
- If `is_phantom` column is missing: `ALTER TABLE trade_ledger ADD COLUMN is_phantom INTEGER DEFAULT 0`
- DB reads: `trade_ledger` schema
- DB writes: `trade_ledger` schema (ALTER TABLE)

#### `record_trade(self, trade: Dict, entry_context: Optional[Dict] = None) -> int`
- Inputs: `trade` dict, optional `entry_context` dict
- Returns: integer row ID of inserted or updated record
- Full logic documented in SECTION 3A and 3B below

#### `get_trades(self, n: int = 252, market: Optional[str] = None, limit: int = None) -> List[Dict]`
- Inputs: n (default 252), optional market filter string, optional limit (overrides n if set)
- Returns: list of dicts from `trade_ledger`, ordered by `id DESC`, limited to n rows
- SQL: `SELECT * FROM trade_ledger [WHERE market=?] ORDER BY id DESC LIMIT ?`
- DB reads: `trade_ledger` (all columns)

#### `log_autopsy_complete(self, trade_id: int) -> None`
- Inputs: trade_id integer
- SQL: `UPDATE trade_ledger SET attribution_complete=1 WHERE id=?`
- DB writes: `trade_ledger.attribution_complete`

#### `record_attribution(self, trade_id: int, attrs: List[Dict]) -> None`
- Inputs: trade_id, list of attribution dicts
- For each attr dict, INSERTs into `pnl_attribution`:
  - `trade_id` ← trade_id
  - `signal_name` ← `a.get("signal_name")`
  - `signal_source_module` ← `a.get("signal_source_module")`
  - `signal_strength_at_entry` ← `a.get("signal_strength", 0.0)`
  - `signal_direction` ← `a.get("signal_direction", 0)`
  - `attributed_pnl` ← `a.get("attributed_pnl", 0.0)`
  - `attributed_pnl_pct` ← `a.get("attributed_pnl_pct", 0.0)`
  - `was_signal_correct` ← `int(a.get("was_correct", False))`
  - `counterfactual_pnl_without_signal` ← `a.get("counterfactual_pnl")`
- After all inserts: `UPDATE trade_ledger SET pnl_attributed=1 WHERE id=?`
- DB writes: `pnl_attribution` (new rows), `trade_ledger.pnl_attributed`

#### `get_signal_scorecard(self, signal_name: str) -> Dict`
- Inputs: signal_name string
- SQL: `SELECT COUNT(*) as n, SUM(attributed_pnl) as total_pnl, AVG(attributed_pnl) as mean_pnl, SUM(CASE WHEN was_signal_correct=1 THEN 1 ELSE 0 END) as wins FROM pnl_attribution WHERE signal_name=?`
- Returns dict with keys: `signal_name`, `n_trades`, `total_pnl`, `mean_pnl`, `win_rate`
- Returns `{"signal_name": signal_name, "n_trades": 0}` if no rows
- DB reads: `pnl_attribution`

#### `record_entry_timing(self, trade_id: int, timing: Dict) -> None`
- Inputs: trade_id, timing dict
- INSERTs into `entry_timing_outcomes`:
  - `trade_id`, `ticker`, `intended_entry_price`, `actual_entry_price`, `entry_timing_score`, `waited_days`, `scale_in_tranche`, `tranche_entry_price`, `pnl_vs_immediate_entry`, `entry_method`
  - `entry_conditions_met` ← `json.dumps(timing.get("entry_conditions_met", []))`
- DB writes: `entry_timing_outcomes`

#### `get_signal_weight(self, signal_name: str, default: float = 1.0) -> float`
- Inputs: signal_name, default weight (1.0)
- SQL: `SELECT weight FROM signal_weights WHERE signal_name=?`
- Returns weight float or default
- DB reads: `signal_weights`

#### `set_signal_weight(self, signal_name, new_weight, reason, n_trades, sharpe, auto) -> None`
- Inputs: signal_name, new_weight float, reason string, n_trades int, sharpe float, auto bool
- Reads current weight from `signal_weights`
- UPSERT into `signal_weights` via `ON CONFLICT(signal_name) DO UPDATE SET ...`
- INSERTs into `weight_history`: old_weight, new_weight, change_pct, trigger
- change_pct formula: `(new_weight - old_weight) / max(abs(old_weight), 1e-9) * 100`
- DB reads: `signal_weights`
- DB writes: `signal_weights`, `weight_history`

#### `get_all_weights(self) -> Dict[str, float]`
- SQL: `SELECT signal_name, weight FROM signal_weights`
- Returns dict mapping signal_name → weight
- DB reads: `signal_weights`

#### `upsert_signal_regime_perf(self, signal_name, macro_regime, vix_bucket, n_trades, win_rate, mean_pnl, sharpe, best, worst) -> None`
- UPSERT into `signal_regime_performance` with `ON CONFLICT(signal_name, macro_regime, vix_bucket) DO UPDATE SET ...`
- DB writes: `signal_regime_performance`

#### `get_regime_weight_multiplier(self, signal_name, macro_regime, vix_bucket) -> float`
- Inputs: signal_name, macro_regime, vix_bucket
- SQL: `SELECT sharpe, AVG(sharpe) OVER () AS mean_sharpe FROM signal_regime_performance WHERE signal_name=? AND macro_regime=? AND vix_bucket=?`
- Returns: `max(0.1, min(3.0, sharpe / mean_sharpe))`, or 1.0 if missing/zero
- DB reads: `signal_regime_performance`

#### `upsert_signal_interaction(self, combo, n, win_rate, mean_pnl, sharpe, vs_single) -> None`
- UPSERT into `signal_interactions` with `ON CONFLICT(signal_combination) DO UPDATE SET ...`
- DB writes: `signal_interactions`

#### `get_interaction_multiplier(self, signal_names: List[str]) -> float`
- combo key = `"|".join(sorted(signal_names))`
- SQL: `SELECT win_rate, n_occurrences FROM signal_interactions WHERE signal_combination=?`
- Returns 1.0 if fewer than 5 occurrences
- Returns 1.2 if win_rate > 0.65
- Returns 0.5 if win_rate < 0.40
- Returns 1.0 otherwise
- DB reads: `signal_interactions`

#### `record_drawdown(self, event: Dict) -> int`
- Inputs: event dict
- INSERT into `drawdown_events`; `contributing_signals`, `contributing_regimes`, `contributing_sectors`, `contributing_markets` serialized as JSON
- Returns: lastrowid
- DB writes: `drawdown_events`

#### `record_stress_outcome(self, outcome: Dict = None, *, run_date, scenario_name, weighted_stress_risk, crisis_fragile, top_scenario, conditions) -> None`
- Overloaded: accepts either a dict or keyword arguments
- If called with kwargs, builds outcome dict mapping `weighted_stress_risk` → `predicted_loss_pct`, `top_scenario` → first element of `signals_flagged_vulnerable`
- INSERT into `stress_learning_outcomes`
- Note: method is defined twice in the file (lines 669 and 934); the second definition at line 934 overrides the first — the first definition at line 669 is dead code
- DB writes: `stress_learning_outcomes`

#### `record_stress_prediction(self, pred: Dict) -> None`
- INSERT into `stress_predictions`: scenario_name, generated_date, confidence, predicted_loss_pct, predicted_vulnerable_signals (JSON), prediction_basis_n_trades
- DB writes: `stress_predictions`

#### `record_tax_disposal(self, disposal: Dict = None, *, ticker, disposal_date, disposal_proceeds, allowable_cost, gain, disposal_type, pool_shares_after, pool_cost_after) -> None`
- Overloaded: accepts either a dict or keyword arguments
- If called with kwargs, builds minimal disposal dict with ticker, disposal_date, gain_loss_gbp = gain; all other disposal fields (section_104_pool, bb_flag, etc.) default to None/0
- tax_year defaults to `str(datetime.now().year)` when kwargs form used
- INSERT into `tax_ledger`
- Note: method is defined twice in the file (lines 704 and 964); the second definition overrides the first — the first definition is dead code
- DB writes: `tax_ledger`

#### `get_ytd_gains(self, tax_year: str = None) -> Dict` (overloaded)
- Original definition at line 720 returns a float (total sum)
- Override at line 992 returns a dict with keys `total_gains`, `total_losses`, `tax_year`
- The dict version (line 992) overrides and replaces the float version (line 720)
- SQL: `SELECT SUM(CASE WHEN gain_loss_gbp > 0 ...) as total_gains, SUM(CASE WHEN gain_loss_gbp < 0 ... ABS(...)) as total_losses FROM tax_ledger WHERE tax_year=?`
- DB reads: `tax_ledger`

#### `record_benchmark(self, date, portfolio_value, benchmarks, *, fund_return, fund_value, benchmark_returns) -> None` (overloaded)
- Original definition at line 731 accepts (date, portfolio_value, benchmarks dict)
- Override at line 1008 accepts both old dict signature and new keyword signature
- If `benchmark_returns` is provided (new BenchmarkTracker signature), builds benchmarks dict mapping SPY→us, EWU→uk, IWM→smallcap, computes active_return_us and active_return_uk
- INSERT OR REPLACE into `benchmark_performance`
- DB writes: `benchmark_performance`

#### `get_benchmark_history(self, n: int = 252, window_days: int = None) -> List[Dict]` (overloaded)
- `window_days` is alias for n; takes precedence if both provided
- SQL: `SELECT * FROM benchmark_performance ORDER BY date DESC LIMIT ?`
- DB reads: `benchmark_performance`

#### `status_summary(self) -> Dict`
- Runs COUNT queries against 7 tables: `trade_ledger`, `pnl_attribution`, `signal_weights`, `drawdown_events`, `stress_learning_outcomes`, `tax_ledger`, `benchmark_performance`
- Gets latest trade: `SELECT ticker, exit_date, net_pnl FROM trade_ledger ORDER BY id DESC LIMIT 1`
- Gets overall win rate: `SELECT AVG(was_profitable) FROM trade_ledger WHERE attribution_complete=1`
- Returns dict with `db_path`, counts, `last_trade` string, `overall_win_rate` (×100, rounded to 1 decimal)
- DB reads: 7 tables

#### `record_peer_influence(self, outcome: Dict) -> None`
- INSERT into `peer_influence_outcomes`
- DB writes: `peer_influence_outcomes`

#### `backup(self, backup_dir: str = "backups/closeloop") -> str`
- Copies DB file to `{backup_dir}/closeloop_{timestamp}.db`
- Returns destination path string

#### `close(self) -> None`
- Closes thread-local connection

#### `get_all_signal_weights(self) -> List[Dict]`
- SQL: `SELECT signal_name, weight, changed_at FROM signal_weights ORDER BY signal_name`
- DB reads: `signal_weights`

#### `get_attributions(self, limit: int = 20) -> List[Dict]`
- SQL: `SELECT signal_name, attributed_pnl, counterfactual_pnl_without_signal as counterfactual_pnl FROM pnl_attribution ORDER BY id DESC LIMIT ?`
- DB reads: `pnl_attribution`

#### `get_entry_outcomes(self, limit: int = 20) -> List[Dict]`
- SQL: `SELECT * FROM entry_timing_outcomes ORDER BY id DESC LIMIT ?`
- DB reads: `entry_timing_outcomes`

#### `get_peer_outcomes(self, limit: int = 20) -> List[Dict]`
- SQL: `SELECT * FROM peer_influence_outcomes ORDER BY id DESC LIMIT ?`
- DB reads: `peer_influence_outcomes`

#### `get_analyst_outcomes(self, limit: int = 20) -> List[Dict]`
- SQL: `SELECT ticker, entry_date, analyst_revision_score, net_pnl FROM trade_ledger WHERE analyst_revision_score IS NOT NULL AND analyst_revision_score != 0 ORDER BY id DESC LIMIT ?`
- Note: does NOT use `analyst_revision_outcomes` table — reads proxy data from `trade_ledger`
- DB reads: `trade_ledger`

#### `get_drawdown_events(self, limit: int = 10) -> List[Dict]`
- SQL: `SELECT * FROM drawdown_events ORDER BY id DESC LIMIT ?`
- DB reads: `drawdown_events`

#### `get_interactions(self, limit: int = 20) -> List[Dict]`
- SQL includes inline multiplier logic: `CASE WHEN win_rate > 0.65 THEN 1.2 WHEN win_rate < 0.40 THEN 0.5 ELSE 1.0 END`
- DB reads: `signal_interactions`

#### `get_weight_history(self, limit: int = 20) -> List[Dict]`
- SQL: `SELECT signal_name, old_weight, new_weight, trigger, changed_at as updated_at FROM weight_history ORDER BY id DESC LIMIT ?`
- DB reads: `weight_history`

#### `get_stress_outcomes(self, limit: int = 100) -> List[Dict]`
- SQL: `SELECT * FROM stress_learning_outcomes ORDER BY id DESC LIMIT ?`
- DB reads: `stress_learning_outcomes`

#### `reconcile_phantom_positions(self, real_tickers: set) -> int`
- If `real_tickers` is non-empty: closes all open records WHERE ticker NOT IN (real_tickers)
- If `real_tickers` is empty: closes ALL open records
- Sets: `exit_date = datetime('now')`, `exit_price = entry_price`, `net_pnl = 0`, `gross_pnl = 0`, `exit_reason = 'phantom_cleanup'`, `was_profitable = 0`
- Does NOT set `is_phantom = 1` — only sets `exit_reason = 'phantom_cleanup'`
- Returns rowcount of rows affected
- DB writes: `trade_ledger` (UPDATE WHERE exit_date IS NULL)

#### `count_completed_trades(self) -> int`
- SQL: `SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NOT NULL AND (is_phantom = 0 OR is_phantom IS NULL) AND exit_reason NOT IN ('phantom_cleanup', 'phantom_duplicate', 'superseded')`
- DB reads: `trade_ledger`

#### `get_paper_equity(self, initial_capital: float = 100000.0) -> float`
- SQL: `SELECT SUM(net_pnl) FROM trade_ledger WHERE exit_date IS NOT NULL AND (is_phantom = 0 OR is_phantom IS NULL)`
- Returns: `initial_capital + sum(net_pnl)`
- DB reads: `trade_ledger`

#### `get_open_positions(self) -> List[Dict]`
- SQL: `SELECT * FROM trade_ledger WHERE exit_date IS NULL ORDER BY id DESC`
- Returns all columns for open records; returns empty list on exception
- DB reads: `trade_ledger`

#### `open_trade(self, ticker, direction, entry_price, shares, position_value, signal_type, signal_score, all_signals, context, sizing_reasoning, phase) -> int`
- Builds a trade dict and calls `record_trade(trade, context or {})`
- `direction_int = 1 if direction == 'LONG' else -1`
- `market = 'uk' if ticker.endswith('.L') else 'us'`
- `entry_date = datetime.now(timezone.utc).isoformat()`
- `signals_at_entry` stored as JSON string (not passed as entry_context key): `{"signal_type": ..., "signal_score": ..., "all_signals": ..., "phase": ..., "sizing_reasoning": ...}`
- `macro_regime = context.get('macro', {}).get('regime')`
- `sector = context.get('sector')`
- DB writes: `trade_ledger` (via record_trade)

#### `get_signal_performance_by_type(self) -> Dict[str, Dict]`
- SQL: `SELECT signals_at_entry, was_profitable, pnl_pct FROM trade_ledger WHERE exit_date IS NOT NULL AND signals_at_entry IS NOT NULL`
- Parses `signals_at_entry` JSON, groups by `signal_type`
- Returns per-type dict with: n_trades, win_rate, avg_win_pct, avg_loss_pct, min_profitable_score (hardcoded 0.35)
- DB reads: `trade_ledger`

#### `log_entry_timing_outcome(self, outcome_or_trade_id, outcome_data=None) -> bool`
- Supports two calling conventions:
  1. Single dict: `log_entry_timing_outcome(outcome_dict)`
  2. Trade ID + dict: `log_entry_timing_outcome(trade_id, outcome_data_dict)`
- `intended_entry_price` ← `outcome.get("immediate_entry_price")`
- `pnl_vs_immediate_entry` ← `outcome.get("entry_alpha")`
- `entry_method` ← `outcome.get("entry_condition", outcome.get("entry_method"))`
- INSERT into `entry_timing_outcomes`
- Returns True on success, False on exception
- DB writes: `entry_timing_outcomes`

#### `get_altdata_confluence(self, ticker: str) -> float`
- SQL: `SELECT AVG(pnl_pct) FROM trade_ledger WHERE ticker=? AND exit_date IS NOT NULL`
- Formula: `min(1.0, max(0.0, 0.5 + avg_pnl_pct * 2))`
- Returns 0.5 if no data
- DB reads: `trade_ledger`

---

### C) MATHEMATICS

**pnl_pct (INSERT path in record_trade):**
```
pnl_pct = net_pnl / (entry_price * position_size)   # if entry_price != 0, else 0.0
```

**annualised_return (INSERT path in record_trade):**
```
ann_ret = pnl_pct * (252.0 / max(holding_days, 1))
```

**pnl_pct (UPDATE path in record_trade — close):**
```
_pnl_pct = pnl / (_ep * _pos_size)   # where _ep = entry_price, _pos_size = position_size
# if _ep == 0: _pnl_pct = 0.0
```

**annualised_return (UPDATE path in record_trade — close):**
```
_ann_ret = _pnl_pct * (252.0 / max(_hold, 1))
```

**weight change_pct (set_signal_weight):**
```
change_pct = (new_weight - old_weight) / max(abs(old_weight), 1e-9) * 100
```

**regime weight multiplier (get_regime_weight_multiplier):**
```
multiplier = max(0.1, min(3.0, sharpe / mean_sharpe))
# Returns 1.0 if mean_sharpe is None or 0
```

**interaction multiplier (get_interaction_multiplier):**
```
if n_occurrences < 5:  return 1.0
if win_rate > 0.65:    return 1.2
if win_rate < 0.40:    return 0.5
else:                  return 1.0
```

**altdata confluence proxy (get_altdata_confluence):**
```
confluence = min(1.0, max(0.0, 0.5 + avg_pnl_pct * 2))
```

---

### D) DATA FLOWS

**DB file:** `closeloop/storage/closeloop.db` (path from config or default)

**Tables created on init:**
1. `trade_ledger` — primary trade record store
2. `pnl_attribution` — per-signal PnL attribution records
3. `entry_timing_outcomes` — entry timing analytics
4. `signal_regime_performance` — signal performance by regime/VIX bucket
5. `signal_interactions` — signal combination performance
6. `peer_influence_outcomes` — peer/contagion trade outcomes
7. `analyst_revision_outcomes` — analyst revision tracking (created but never written by this class)
8. `academic_company_matches` — paper-ticker matches (created but never written by this class)
9. `stress_learning_outcomes` — stress test outcomes
10. `stress_predictions` — forward stress predictions
11. `drawdown_events` — drawdown forensics
12. `signal_weights` — current signal weights
13. `weight_history` — historical weight changes
14. `tax_ledger` — CGT disposal records
15. `benchmark_performance` — vs-benchmark tracking

**Migration columns added (if absent):**
- `trade_ledger.order_status TEXT DEFAULT 'unknown'`
- `trade_ledger.is_phantom INTEGER DEFAULT 0`

**What enters record_trade():**
- `trade` dict from `paper_trader.py`
- `entry_context` dict (context_at_open) from `_capture_context()`

**What leaves record_trade():**
- Integer row ID (trade_id) returned to caller

---

### E) DEPENDENCIES

**Internal Apollo imports (runtime, inside methods):**
- None at module level — all internal imports are via callers

**External libraries:**
- `json` (stdlib)
- `logging` (stdlib)
- `sqlite3` (stdlib)
- `threading` (stdlib)
- `datetime`, `timezone` (stdlib)
- `pathlib.Path` (stdlib)
- `typing` (stdlib)
- `shutil` (stdlib, inside `backup()`)
- `collections.defaultdict` (stdlib, inside `get_signal_performance_by_type()`)

---

### F) WIRING STATUS

**Connected to live trading path: YES**

- `paper_trader.py` imports `ClosedLoopStore` at line 133 and instantiates it as `self.closeloop`
- `paper_trader.py` also lazy-loads a second reference as `self._store` inside `run_scan()` at line 587
- Both `self.closeloop` and `self._store` point to `ClosedLoopStore`; they may be different instances referencing the same DB file
- `record_trade()` is called on every trade open (line 1489 in paper_trader) and every trade close (line 2113 in paper_trader)
- `get_open_positions()` is called on startup to restore in-memory state from DB (line 379 in paper_trader)
- The store is also used by: `adaptive_position_sizer.py`, `batch_retrainer.py`, `regime_tracker.py`, `weight_updater.py`, `module_wirer.py`, `drawdown_forensics.py`, `pnl_attributor.py`, `signal_interaction_ledger.py`, `trade_autopsy.py`, `pead_signal.py`, and `main.py`

---

### G) ISSUES FOUND

1. **Double method definitions (dead code overrides):** `record_stress_outcome` is defined twice (lines 669 and 934). `record_tax_disposal` is defined twice (lines 704 and 964). `get_ytd_gains` is defined twice (lines 720 and 992). `record_benchmark` is defined twice (lines 731 and 1008). `get_benchmark_history` is defined twice (lines 746 and 1039). In each case, Python silently uses the second definition and the first is completely dead code. The first `get_ytd_gains` returns a float; the second returns a dict — any caller expecting a float will receive a dict and may break silently.

2. **Two separate ClosedLoopStore instances:** `paper_trader.py` instantiates `ClosedLoopStore` as both `self.closeloop` (line 134) and `self._store` (lazy, line 587). These are separate Python objects sharing the same DB file. WAL mode handles concurrent writes, but the in-memory state of each instance (thread-local connections) is separate. This is not a crash risk but is confusing and wastes resources.

3. **`open_trade()` stores `signals_at_entry` as a JSON string in the `trade` dict, NOT in `entry_context`.** When `record_trade()` runs, it reads `entry_context.get("signals_at_entry", {})` and serializes that. But `open_trade()` puts signals under `trade['signals_at_entry']` as a JSON string. The INSERT then also writes `trade.get("entry_price")` etc. correctly, but `signals_at_entry` will be the JSON string from the trade dict, not from entry_context. This means context_at_open's signals_at_entry is ignored when open_trade() is used.

4. **`analyst_revision_outcomes` and `academic_company_matches` tables are created but never written.** No `INSERT` statements exist in this file for either table. They are schema-only artifacts.

5. **`reconcile_phantom_positions()` does not set `is_phantom=1`** on records it closes with `exit_reason='phantom_cleanup'`. The `count_completed_trades()` filter excludes `exit_reason='phantom_cleanup'` but also checks `is_phantom=0 OR is_phantom IS NULL`, which means these records are excluded from count by reason but NOT by the is_phantom flag — producing correct count but logically inconsistent state.

6. **`get_analyst_outcomes()` reads proxy data from `trade_ledger.analyst_revision_score`**, not from the `analyst_revision_outcomes` table. The docstring says "proxy via analyst_revision_score". The actual table is never queried.

7. **`pnl_pct` formula uses position_size as share count, not position value.** `pnl / (entry_price * position_size)` assumes position_size is number of shares. If position_size is dollar value, the formula produces a wrong denominator. Callers must be consistent.

8. **The `record_trade()` close UPDATE path does not guard against already-closed records before updating.** It searches for `exit_date IS NULL` rows and updates the first match. A second concurrent close call would find no open row and fall through to the INSERT fallback, where the DEDUP guard then checks for existing closed records. This path works but relies on two separate guard layers rather than a single atomic operation.

---
---

## FILE 2: `/home/dannyelticala/quant-fund/execution/cooling_off_tracker.py`

---

### A) PURPOSE

`StockCoolingOffTracker` prevents Apollo from re-entering a ticker position for a configurable number of calendar days (default 5) after a losing stop-loss exit. It implements 5 conditions that can release the lockout early. The tracker is backed by a dedicated SQLite database (`cooling_off.db`) so lockouts survive process restarts.

---

### B) CLASSES AND METHODS

**Class: `StockCoolingOffTracker`**

#### `__init__(self, cooling_days: int = 5) -> None`
- Inputs: cooling_days (default 5)
- Initializes `self._cooling_days = cooling_days`
- Initializes `self._entries: dict[str, dict] = {}` (in-memory dict, ticker → entry dict)
- Calls `_init_db()` then `_load_from_db()`

#### `_init_db(self) -> None`
- Inputs: none
- Creates `cooling_off.db` directory if missing
- Creates table `cooling_off` if not exists:
  - `ticker TEXT PRIMARY KEY`
  - `exit_date TEXT NOT NULL`
  - `exit_price REAL`
  - `pnl_pct REAL`
  - `exit_reason TEXT`
  - `locked_at TEXT NOT NULL`
- Failures logged as WARNING, not raised

#### `_save_to_db(self, ticker: str, entry: dict) -> None`
- SQL: `INSERT OR REPLACE INTO cooling_off (ticker, exit_date, exit_price, pnl_pct, exit_reason, locked_at) VALUES (?, ?, ?, ?, ?, ?)`
- `locked_at` = `datetime.utcnow().isoformat()` (current UTC time at time of call)
- `exit_date` = `str(entry["exit_date"])`
- Failures logged as WARNING, not raised
- DB writes: `cooling_off`

#### `_delete_from_db(self, ticker: str) -> None`
- SQL: `DELETE FROM cooling_off WHERE ticker=?`
- Failures logged as WARNING, not raised
- DB writes: `cooling_off`

#### `_load_from_db(self) -> None`
- If DB file does not exist, returns immediately
- SQL: `SELECT ticker, exit_date, exit_price, pnl_pct, exit_reason FROM cooling_off`
- For each row: parses `exit_date` as ISO date (first 10 chars), computes `release_date = exit_dt + timedelta(days=self._cooling_days)`
- Only loads rows where `today < release_date` (skips expired)
- Populates `self._entries[ticker]` with dict containing: `exit_date`, `exit_price`, `pnl_pct`, `exit_reason`, `release_date`, `early_released=False`, `release_reason=None`
- Prunes expired rows from DB: `DELETE FROM cooling_off WHERE exit_date < ?` (cutoff = today - cooling_days - 1 day)
- Logs INFO with count if entries loaded
- Failures logged as WARNING
- DB reads: `cooling_off`
- DB writes: `cooling_off` (DELETE expired)

#### `register_exit(self, ticker, exit_date, exit_price, pnl_pct, exit_reason="unknown") -> None`
- Inputs: ticker string, exit_date (date object), exit_price float, pnl_pct float, exit_reason string
- **Gate 1:** If `pnl_pct >= 0` (winning exit): removes ticker from `self._entries` (clears any prior lockout) and returns immediately — no lockout
- **Gate 2:** If `exit_reason not in {"atr_stop", "trailing_stop"}` OR `pnl_pct > -0.02`: returns without lockout
- If both gates pass: creates entry dict, stores in `self._entries[ticker]`, calls `_save_to_db()`
- `release_date = exit_date + timedelta(days=self._cooling_days)`
- Logs INFO on lockout
- DB writes: `cooling_off` (via `_save_to_db`)

#### `is_cooling_off(self, ticker: str, as_of: date | None = None) -> bool`
- Inputs: ticker, optional reference date (defaults to today)
- Returns False if ticker not in `self._entries`
- Returns False if `entry["early_released"]` is True
- If `today >= entry["release_date"]`: removes from `self._entries`, calls `_delete_from_db()`, returns False
- Otherwise returns True
- DB writes: `cooling_off` (DELETE on expiry)

#### `days_remaining(self, ticker: str, as_of: date | None = None) -> int`
- Returns 0 if ticker not in entries or early_released
- Returns `max(0, (release_date - today).days)`
- No DB access

#### `check_early_release(self, ticker, current_price, earnings_beat_pct, volume_ratio, altdata_score, days_to_cover, as_of) -> bool`
- Inputs: ticker, and 5 optional float signals
- Returns False (and entry not found returns False) if ticker not in entries
- Returns True immediately if already `early_released`
- Evaluates 5 conditions in order:
  1. `earnings_beat_pct > 0.05` → appends `"earnings_beat"`
  2. `volume_ratio > 3.0` → appends `"volume_surge"`
  3. `altdata_score > 0.7` → appends `"strong_altdata"`
  4. `(exit_price - current_price) / exit_price > 0.15` → appends `"price_drop_15pct"` (only if exit_price > 0)
  5. `days_to_cover < 2.0` → appends `"short_squeeze"`
- If any conditions met: sets `entry["early_released"] = True`, `entry["release_reason"] = ",".join(reasons)`, logs INFO, returns True
- If no conditions met: returns False
- **No DB write on early release** — the early_released flag exists only in memory; not persisted to `cooling_off.db`

#### `expire_old_entries(self, as_of: date | None = None) -> int`
- Removes in-memory entries where `today >= release_date`
- Does NOT delete from DB
- Returns count removed

#### `status(self, as_of: date | None = None) -> list[dict]`
- Returns list of dicts for non-early-released entries
- Each dict: `ticker`, `exit_date`, `exit_price`, `pnl_pct` (×100, rounded to 2dp), `release_date`, `days_remaining`
- No DB access

#### `active_count(self) -> int`
- Returns count of entries where `not early_released`
- No DB access

---

### C) MATHEMATICS

**release_date:**
```
release_date = exit_date + timedelta(days=cooling_days)
```
(cooling_days is calendar days, not trading days — docstring says "5 trading days" but implementation uses `timedelta(days=5)` which is calendar days)

**Lockout trigger condition:**
```
pnl_pct < 0
AND exit_reason in {"atr_stop", "trailing_stop"}
AND pnl_pct <= -0.02
```
(Note: the code checks `pnl_pct > _MIN_LOSS_TO_LOCKOUT` to skip, where `_MIN_LOSS_TO_LOCKOUT = -0.02`. So the lockout fires only when `pnl_pct <= -0.02`.)

**Early release — price drop condition:**
```
drop = (exit_price - current_price) / exit_price
fires when: drop > 0.15
```

**Expiry pruning cutoff on load:**
```
cutoff = (today - timedelta(days=cooling_days + 1)).isoformat()
DELETE FROM cooling_off WHERE exit_date < cutoff
```

**days_remaining:**
```
remaining = max(0, (release_date - today).days)
```

**status() pnl display:**
```
pnl_pct_display = round(entry["pnl_pct"] * 100, 2)
```

---

### D) DATA FLOWS

**DB file:** `{QUANT_FUND_DIR}/closeloop/storage/cooling_off.db`
- `QUANT_FUND_DIR` from environment variable; falls back to `os.getcwd()`
- File path: `os.path.join(env_dir, "closeloop", "storage", "cooling_off.db")`

**Table:** `cooling_off`
- Columns: `ticker`, `exit_date`, `exit_price`, `pnl_pct`, `exit_reason`, `locked_at`

**What enters:**
- `register_exit()` called by `paper_trader.py` on every position close
- `is_cooling_off()` called by `paper_trader.py` on every potential new entry

**What leaves:**
- Boolean from `is_cooling_off()` — used by `paper_trader.py` to block entry
- `days_remaining()` for log message only

**In-memory state:** `self._entries` dict — loaded from DB on init, updated on register/check/expire

**What is NOT persisted to DB:** `early_released` flag and `release_reason` — these exist only in memory and are lost on process restart

---

### E) DEPENDENCIES

**Internal Apollo imports:** None

**External libraries:**
- `logging` (stdlib)
- `os` (stdlib)
- `sqlite3` (stdlib)
- `datetime`, `date`, `timedelta` (stdlib)
- `typing.Optional` (stdlib)

---

### F) WIRING STATUS

**Connected to live trading path: YES**

In `paper_trader.py`:
- Line 304: `from execution.cooling_off_tracker import StockCoolingOffTracker`
- Line 305: `self._cooling_off = StockCoolingOffTracker(cooling_days=5)`
- Lines 1358–1365: `is_cooling_off(ticker)` checked before every new entry; blocks entry and returns None if True
- Lines 2073–2092: `register_exit()` called after every position close, skipping phantom trades

The check happens AFTER the duplicate-position guard and AFTER the observation-mode check but BEFORE order submission. A blocked ticker is logged at INFO level and the function returns None (no order placed).

---

### G) ISSUES FOUND

1. **Calendar days vs trading days mismatch:** The module docstring says "5 trading days" but `timedelta(days=5)` counts calendar days. A 5-calendar-day lockout that spans a weekend is only 3 trading days. This is a semantic discrepancy between documented and actual behavior.

2. **Early release flag is not persisted to DB.** If the process restarts while a ticker is in early-released state, `_load_from_db()` will reload the ticker's entry with `early_released=False`, reinstating the lockout. The ticker will be blocked again until the calendar release_date passes.

3. **`expire_old_entries()` does not delete from DB.** It removes from `self._entries` only. Expired rows remain in the DB until the next `_load_from_db()` call (which prunes them). If `expire_old_entries()` is called between process restarts, the DB rows are not cleaned up.

4. **`_load_from_db()` does not re-check `locked_at`** for stuck entries. If a row has an `exit_date` in the past but within the cooling window, it is loaded regardless of when it was written. This is correct behavior, but if the system clock moves backward or `cooling_days` is changed between restarts, stale entries could persist longer or be dropped too early.

5. **`check_early_release()` is never called anywhere in the codebase** (not called by `paper_trader.py` or any other file found during search). The 5 early-release conditions are implemented but have no caller — they are effectively dead functionality.

6. **Winning exits clear the cooling-off entry** (`self._entries.pop(ticker, None)`) but do not delete the corresponding DB row. If a ticker was locked, then wins (which shouldn't happen while locked, but in theory), the in-memory entry is cleared but the DB row remains until the next `_load_from_db()` prunes it.

---
---

## SECTION 3 — TRADE RECORDING COMPLETE MAP

---

### 3A: `record_trade()` — Complete Input-to-DB Column Mapping

`record_trade()` handles both the OPEN path (INSERT new row) and the CLOSE path (UPDATE existing row). The path taken depends on whether `trade.get("exit_date")` is None.

#### OPEN PATH (INSERT) — every column in `trade_ledger`:

| Column | Value Written | Source |
|--------|--------------|--------|
| `id` | AUTO (AUTOINCREMENT) | DB |
| `ticker` | `trade.get("ticker")` | trade dict |
| `market` | `trade.get("market", "us")` | trade dict, default "us" |
| `direction` | `trade.get("direction", 1)` | trade dict, default 1 |
| `entry_date` | `trade.get("entry_date")` | trade dict |
| `exit_date` | `trade.get("exit_date")` | trade dict (NULL on open) |
| `entry_price` | `trade.get("entry_price")` | trade dict |
| `exit_price` | `trade.get("exit_price")` | trade dict (NULL on open) |
| `position_size` | `trade.get("position_size")` | trade dict |
| `gross_pnl` | `trade.get("gross_pnl", net_pnl)` | trade dict, falls back to net_pnl |
| `net_pnl` | `trade.get("net_pnl") or 0.0` | trade dict |
| `fees_paid` | `trade.get("fees_paid", 0.0)` | trade dict, default 0.0 |
| `holding_days` | `trade.get("holding_days", 0) or 0` | trade dict, default 0 |
| `exit_reason` | `trade.get("exit_reason")` | trade dict (NULL on open) |
| `signals_at_entry` | `json.dumps(ctx.get("signals_at_entry", {}))` | entry_context dict, JSON |
| `macro_regime` | `ctx.get("macro_regime")` | entry_context dict |
| `vix_level` | `ctx.get("vix_level")` | entry_context dict |
| `umci_score` | `ctx.get("umci_score")` | entry_context dict |
| `lunar_phase` | `ctx.get("lunar_phase")` | entry_context dict |
| `geomagnetic_kp` | `ctx.get("geomagnetic_kp")` | entry_context dict |
| `market_cap_usd` | `ctx.get("market_cap_usd")` | entry_context dict |
| `sector` | `ctx.get("sector")` | entry_context dict |
| `entry_timing_score` | `ctx.get("entry_timing_score")` | entry_context dict |
| `scale_in_tranche` | `ctx.get("scale_in_tranche", 0)` | entry_context dict, default 0 |
| `peer_influence_score` | `ctx.get("peer_influence_score", 0.0)` | entry_context dict |
| `analyst_revision_score` | `ctx.get("analyst_revision_score", 0.0)` | entry_context dict |
| `academic_tailwind_score` | `ctx.get("academic_tailwind_score", 0.0)` | entry_context dict |
| `news_context_score` | `ctx.get("news_context_score", 0.0)` | entry_context dict |
| `index_rebalancing_pressure` | `ctx.get("index_rebalancing_pressure", 0.0)` | entry_context dict |
| `merger_spillover_flag` | `int(ctx.get("merger_spillover_flag", False))` | entry_context dict |
| `was_profitable` | `int(net_pnl > 0)` | computed |
| `pnl_pct` | `round(net_pnl / (entry_price * position_size), 6)` | computed |
| `annualised_return` | `round(pnl_pct * (252.0 / max(holding_days, 1)), 6)` | computed |
| `pnl_attributed` | NOT in INSERT — DB default 0 | default |
| `attribution_complete` | NOT in INSERT — DB default 0 | default |
| `order_status` | NOT in INSERT — DB default 'unknown' | migration default |
| `is_phantom` | NOT in INSERT — DB default 0 | migration default |

**Note on macro_regime:** `ctx.get("macro_regime")` reads from top-level key of entry_context. But `_capture_context()` in paper_trader stores macro regime under `ctx["macro"]["regime"]`, NOT `ctx["macro_regime"]`. This means `macro_regime` in `trade_ledger` is almost always NULL when called from the normal live trade path.

---

### 3B: `close_trade()` — Complete Exit Recording Path

There is no method named `close_trade()` in `closeloop_store.py`. The close recording is embedded inside `record_trade()` as a conditional branch triggered when `trade.get("exit_date") is not None`.

#### UPDATE PATH (normal close):

**Trigger condition:** `trade.get("exit_date") is not None` AND a matching open row found by:
```sql
SELECT id FROM trade_ledger
WHERE ticker=? AND entry_date=? AND entry_price=? AND direction=?
  AND exit_date IS NULL
LIMIT 1
```
(Parameters: `trade.get("ticker")`, `trade.get("entry_date")`, `trade.get("entry_price")`, `trade.get("direction", 1)`)

**Columns updated:**

| Column | Value Written | Source |
|--------|--------------|--------|
| `exit_date` | `trade.get("exit_date")` | trade dict |
| `exit_price` | `trade.get("exit_price")` | trade dict |
| `gross_pnl` | `trade.get("gross_pnl", net_pnl)` | trade dict |
| `net_pnl` | `trade.get("net_pnl") or 0.0` | trade dict |
| `holding_days` | `trade.get("holding_days", 0) or 0` | trade dict |
| `exit_reason` | `trade.get("exit_reason")` | trade dict |
| `is_phantom` | `trade.get("is_phantom", 0)` | trade dict |
| `position_size` | `trade.get("position_size")` | trade dict |
| `sector` | `trade.get("sector")` | trade dict |
| `order_status` | `'closed'` (hardcoded) | hardcoded |
| `was_profitable` | `int(net_pnl > 0)` | computed |
| `pnl_pct` | `round(net_pnl / (entry_price * position_size), 6)` | computed |
| `annualised_return` | `round(pnl_pct * (252.0 / max(holding_days, 1)), 6)` | computed |

**Columns NOT updated (retain open-record values):**
`ticker`, `market`, `direction`, `entry_date`, `entry_price`, `fees_paid`, `signals_at_entry`, `macro_regime`, `vix_level`, `umci_score`, `lunar_phase`, `geomagnetic_kp`, `market_cap_usd`, `entry_timing_score`, `scale_in_tranche`, `peer_influence_score`, `analyst_revision_score`, `academic_tailwind_score`, `news_context_score`, `index_rebalancing_pressure`, `merger_spillover_flag`, `pnl_attributed`, `attribution_complete`

#### INSERT FALLBACK PATH:

**Trigger condition:** `trade.get("exit_date") is not None` AND no open row found by the SELECT above (logger.warning emitted).

**Duplicate close guard (before INSERT):** Checks for existing closed record:
```sql
SELECT id FROM trade_ledger
WHERE ticker=? AND entry_date=? AND entry_price=? AND direction=? AND exit_date IS NOT NULL
```
If found: logs `[DEDUP]` warning and returns existing id — no INSERT.

If not found: falls through to full INSERT with all columns, including exit_date and exit_reason populated. Uses same INSERT SQL as the open path (all columns). `was_profitable`, `pnl_pct`, `annualised_return` computed same way as open INSERT.

#### Duplicate close guard for open records:

Before any INSERT on the open path: checks for existing open record with same (ticker, entry_date, entry_price, direction) where `exit_date IS NULL`. If found: logs warning, returns existing id — no INSERT.

---

### 3C: `context_at_open` Schema

`context_at_open` is built by `_capture_context()` in `paper_trader.py` and passed as `entry_context` to `record_trade()`.

| Key | Source | Type | Persisted to DB? |
|-----|--------|------|-----------------|
| `timestamp` | `datetime.utcnow().isoformat()` | str | No |
| `pead` | dict from signal.to_dict() | dict | No — not a top-level ctx key mapped to any column |
| `pead.surprise_pct` | signal dict | float or None | No |
| `pead.surprise_zscore` | signal dict | float or None | No |
| `pead.quality` | signal dict | any | No |
| `pead.volume_surge` | signal dict | any | No |
| `macro` | dict | dict | No (nested) |
| `macro.regime` | MacroSignalEngine briefing | str or None | No — `macro_regime` column reads `ctx.get("macro_regime")` not `ctx["macro"]["regime"]` — always NULL |
| `macro.vix` | yfinance ^VIX download (cached) | float or None | No — `vix_level` reads `ctx.get("vix_level")` not `ctx["macro"]["vix"]` — always NULL |
| `macro.yield_curve_slope_bps` | RatesCreditCollector | float or None | No |
| `macro.hy_spread_bps` | RatesCreditCollector | float or None | No |
| `altdata` | dict | dict | No |
| `altdata.confluence_score` | ClosedLoopStore.get_altdata_confluence() | float | No |
| `altdata.sentiment` | None (hardcoded) | None | No |
| `shipping_stress_index` | output/historical_db.db, shipping_data table | any or None | No |
| `hmm_state` | MathematicalSignals.get_combined_signal() | str: "BULL"/"BEAR"/"NEUTRAL" | No |
| `hmm_state_raw` | MathematicalSignals.get_combined_signal() | float | No |
| `earnings_quality` | SimFin.calculate_earnings_quality() | dict or None | No |
| `earnings_quality.score` | SimFin | float | No |
| `earnings_quality.tier` | SimFin | str | No |
| `has_sec_crisis_alert` | SEC fulltext search "going concern" last 7 days | bool | No |
| `geopolitical_risk` | output/permanent_log.db, macro_context table | str | No |
| `signals_at_entry` | signal dict (signal_type, strength, confidence, tier, strategy) | dict | YES → `trade_ledger.signals_at_entry` as JSON |
| `active_signals` | paper_trader enrichment (multi-signal data) | list of dicts | No |
| `macro_regime` | NOT set by _capture_context — only set if explicitly added | — | Would be → `trade_ledger.macro_regime` if present |
| `vix_level` | NOT set by _capture_context — only set if explicitly added | — | Would be → `trade_ledger.vix_level` if present |
| `sector` | NOT set by _capture_context — explicitly added by paper_trader | str | YES → `trade_ledger.sector` |

**Critical finding:** `_capture_context()` stores macro data under nested keys (`ctx["macro"]["regime"]`, `ctx["macro"]["vix"]`) but `record_trade()` reads flat keys (`ctx.get("macro_regime")`, `ctx.get("vix_level")`). These keys never match. As a result, `trade_ledger.macro_regime` and `trade_ledger.vix_level` are NULL for all trades recorded via the normal live path.

---

### 3D: Cooling-Off Mechanics

#### Trigger: what causes cooldown

A cooldown is registered by `register_exit()` only when ALL three conditions are true:
1. `pnl_pct < 0` (losing exit)
2. `exit_reason` is `"atr_stop"` or `"trailing_stop"` (stop-loss exits only)
3. `pnl_pct <= -0.02` (loss exceeds 2% — not trivial noise)

Exits with reason `volume_dry_up`, `time_exit`, `signal_reversal`, `max_hold_days`, or any other reason do NOT trigger cooldown. Winning exits also clear any prior lockout for that ticker.

Phantom trades are excluded from cooldown registration (`paper_trader.py` line 2073: `if self._cooling_off is not None and not _is_phantom`).

#### Storage: where it lives

**Primary storage:** SQLite database at `{QUANT_FUND_DIR}/closeloop/storage/cooling_off.db`, table `cooling_off`

**In-memory mirror:** `self._entries` dict (ticker → entry dict). This is the authoritative runtime state. The DB is the persistence layer loaded on startup.

**What is in the DB:** ticker, exit_date, exit_price, pnl_pct, exit_reason, locked_at

**What is ONLY in memory (not in DB):** early_released flag, release_reason string, release_date (recomputed from exit_date + cooling_days on load), entry_date

#### Check: how paper_trader checks it

In `paper_trader.py`, before submitting a new buy order (inside the entry logic):
```python
if self._cooling_off is not None:
    if self._cooling_off.is_cooling_off(ticker):
        days_left = self._cooling_off.days_remaining(ticker)
        logger.info("%s: blocked by cooling-off (%d days remaining)", ticker, days_left)
        return None
```
`is_cooling_off()` checks in order:
1. Is ticker in `self._entries`? If not → False (not blocked)
2. Is `early_released` True? → False (not blocked)
3. Has `release_date` passed? → removes from memory and DB, returns False
4. Otherwise → True (blocked)

#### Duration: how long cooldown lasts

`cooling_days = 5` calendar days (hardcoded when instantiated in paper_trader: `StockCoolingOffTracker(cooling_days=5)`)

`release_date = exit_date + timedelta(days=5)`

The ticker is unblocked when `date.today() >= release_date`.

**Note:** The module docstring says "5 trading days" but the implementation uses calendar days. A Friday exit releases on Wednesday of the following week (5 calendar days later), which is only 3 trading days. This is a documentation/implementation mismatch.

Early release conditions exist (5 of them, see check_early_release method) but are never called by any file in the codebase — the 5 early-release paths are unreachable dead functionality.

---

## SECTION 3 GATE

**Files read:**
- `/home/dannyelticala/quant-fund/closeloop/storage/closeloop_store.py` (1223 lines, complete)
- `/home/dannyelticala/quant-fund/execution/cooling_off_tracker.py` (300 lines, complete)

**Key findings:**
1. `record_trade()` serves as both open and close recorder — no separate `close_trade()` or `get_open_trades()` methods exist
2. Five methods are defined twice in `closeloop_store.py` — second definitions override first (dead code): `record_stress_outcome`, `record_tax_disposal`, `get_ytd_gains`, `record_benchmark`, `get_benchmark_history`
3. `macro_regime` and `vix_level` columns in `trade_ledger` are always NULL on live trades because `_capture_context()` nests them under `ctx["macro"]["regime"]` but `record_trade()` reads flat `ctx.get("macro_regime")`
4. `check_early_release()` is implemented with 5 conditions but has no callers — dead code in production
5. Early release flag is in-memory only — lost on process restart, reinstating lockout
6. `analyst_revision_outcomes` and `academic_company_matches` tables are created but never populated
7. `get_analyst_outcomes()` reads proxy data from `trade_ledger`, not from its own table
8. `reconcile_phantom_positions()` does not set `is_phantom=1` on records it closes as phantom_cleanup
9. Two separate `ClosedLoopStore` instances may exist concurrently in `paper_trader.py` (`self.closeloop` and `self._store`)
10. cooling_off.db is stored inside the closeloop/storage directory, same location as closeloop.db
11. Winning exits clear prior lockout entries from memory but NOT from DB

**Contradictions found:**
- cooling_off_tracker.py docstring: "5 trading days" — implementation: `timedelta(days=5)` which is calendar days
- `get_ytd_gains()` first definition (line 720) returns a float; second definition (line 992) returns a dict — callers expecting float will silently receive a dict
- `open_trade()` stores signals in `trade['signals_at_entry']` as a JSON string; `record_trade()` reads `ctx.get("signals_at_entry", {})` from entry_context — these are different objects

**Data flows documented:**
- `paper_trader._capture_context()` → `entry_context` dict → `record_trade()` → `trade_ledger` (INSERT)
- `paper_trader` close logic → `trade` dict → `record_trade()` → `trade_ledger` (UPDATE)
- `paper_trader.register_exit()` → `StockCoolingOffTracker._entries` + `cooling_off.db`
- `paper_trader` entry check → `StockCoolingOffTracker.is_cooling_off()` → block or allow
- `ClosedLoopStore.get_open_positions()` → `paper_trader` startup position restore

**Proceed to Section 4: YES**
