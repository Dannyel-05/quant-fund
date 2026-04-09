# APOLLO SYSTEM MAP — PART 6
## GROUP 6: Close-Loop Learning System
### Files: trade_autopsy.py, pnl_attributor.py, weight_updater.py, batch_retrainer.py

---

## FILE 1: /home/dannyelticala/quant-fund/closeloop/autopsy/trade_autopsy.py

### A) PURPOSE
Orchestrates all post-trade analysis steps every time a position closes. Acts as the central hub that instantiates and calls nine downstream analysis modules in sequence. Designed so that one failing step never stops the others — every step is wrapped in an individual try/except block. Produces an AutopsyReport dataclass summarising the outcome of all steps.

---

### B) CLASSES AND METHODS

#### Class: AutopsyReport (dataclass)
A result container. Not callable — no methods.

Fields:
- `trade_id: int` — assigned DB id of the trade, initialised to -1
- `ticker: str` — stock ticker symbol
- `net_pnl: float` — total P&L of the closed trade
- `pnl_pct: float` — P&L as a fraction of entry value
- `was_profitable: bool` — True if net_pnl > 0
- `macro_regime: str` — market regime label from entry_context
- `attribution: List[Dict]` — list of per-signal attribution records, default empty list
- `entry_alpha: float` — timing alpha value, default 0.0
- `weight_changes: List[Dict]` — list of weight change records, default empty list
- `notes: List[str]` — log of what each step did or failed to do, default empty list

---

#### Class: TradeAutopsy
Main orchestrator class.

**Method: `__init__(self, store=None, config=None)`**
- Inputs: `store` (ClosedLoopStore instance or None), `config` (dict or None)
- Outputs: None (initialises instance)
- What it does:
  - Stores `store` and `config` as instance attributes
  - Warns via logger if store is None
  - Attempts to instantiate nine sub-modules (each wrapped in try/except using try/except ImportError guards at module level):
    1. `self._attributor` = PnLAttributor(store, config)
    2. `self._regime_tracker` = RegimeTracker(store, config)
    3. `self._interaction_ledger` = SignalInteractionLedger(store, config)
    4. `self._weight_updater` = WeightUpdater(store, config)
    5. `self._drawdown_forensics` = DrawdownForensics(store, config)
    6. `self._benchmark_tracker` = BenchmarkTracker(store, config)
    7. `self._tax_manager` = TaxManager(store, config)
    8. `self._entry_learner` = EntryLearner(store, config)
    9. `self._stress_learner` = StressLearner(store, config)
  - If any import fails at module level, that class is set to None and a warning is logged; the __init__ skips instantiation of that sub-module
- DB reads: None directly
- DB writes: None directly

**Method: `run(self, closed_trade: dict, entry_context: dict) -> AutopsyReport`**
- Inputs:
  - `closed_trade`: dict with keys: ticker, net_pnl, entry_price, position_size, direction, scale_in_tranche, holding_days, signals_at_entry (all optional with defaults)
  - `entry_context`: dict with keys: macro_regime, active_signals, peer_influence_score, merger_spillover_flag, peer_trigger_ticker, peer_trigger_event, immediate_open_pnl (all optional with defaults)
- Outputs: `AutopsyReport` instance
- What it does (12 steps, each isolated in try/except):

  **Step 1 — Record trade:**
  - If store is not None: calls `self._store.record_trade(closed_trade, entry_context)` which returns a trade_id integer
  - Sets `report.trade_id = trade_id`
  - DB writes: one record via `store.record_trade` (table unknown — defined in ClosedLoopStore)

  **Step 2 — PnL attribution:**
  - Condition: `self._attributor is not None AND trade_id >= 0`
  - Calls `self._attributor.attribute(trade_id, closed_trade, entry_context)`
  - Returns list of attribution dicts, stored in `report.attribution`
  - DB writes: via PnLAttributor (see File 2)

  **Step 3 — RegimeTracker:**
  - Condition: `self._regime_tracker is not None`
  - Calls `self._regime_tracker.update(macro_regime, attribution)`
  - DB writes: unknown (defined in RegimeTracker, not in this file)

  **Step 4 — SignalInteractionLedger:**
  - Condition: `self._interaction_ledger is not None`
  - Extracts `active_signals = list(entry_context.get("active_signals", []))`
  - Only calls update if `active_signals` is non-empty
  - Calls `self._interaction_ledger.update(active_signals, closed_trade)`
  - DB writes: unknown (defined in SignalInteractionLedger, not in this file)

  **Step 5 — WeightUpdater soft update:**
  - Condition: `self._weight_updater is not None AND attribution is non-empty`
  - Calls `self._weight_updater.soft_update(attribution, entry_context)`
  - Returns list of weight change dicts, stored in `report.weight_changes`
  - DB writes: via WeightUpdater (see File 3)

  **Step 6 — DrawdownForensics:**
  - Condition: `self._drawdown_forensics is not None`
  - Calls `self._drawdown_forensics.check_and_record(closed_trade)`
  - DB writes: unknown (defined in DrawdownForensics, not in this file)

  **Step 7 — BenchmarkTracker:**
  - Condition: `self._benchmark_tracker is not None`
  - Calls `self._benchmark_tracker.update(closed_trade, entry_context)`
  - DB writes: unknown (defined in BenchmarkTracker, not in this file)

  **Step 8 — TaxManager disposal:**
  - Condition: `self._tax_manager is not None AND trade_id >= 0`
  - Calls `self._tax_manager.record_disposal(trade_id, closed_trade)`
  - DB writes: unknown (defined in TaxManager, not in this file)

  **Step 9 — EntryLearner outcome:**
  - Condition: `self._entry_learner is not None AND trade_id >= 0`
  - Calls `self._entry_learner.record_outcome(trade_id, closed_trade, entry_context)`
  - If return value is not None, casts to float and stores as `report.entry_alpha`
  - DB writes: unknown (defined in EntryLearner, not in this file)

  **Step 10 — StressLearner vulnerability:**
  - Condition: `self._stress_learner is not None`
  - Calls `self._stress_learner.update_signal_vulnerability(closed_trade, entry_context)`
  - DB writes: unknown (defined in StressLearner, not in this file)

  **Step 11 — Peer influence outcome:**
  - Condition: `(merger_flag OR peer_score > 0.3) AND store is not None AND trade_id >= 0`
  - Builds `influence_outcome` dict with fields: trigger_ticker, trigger_event, influenced_ticker, influence_type, predicted_direction, actual_direction, predicted_magnitude, actual_magnitude, lag_days, was_correct, pnl
  - Calls `self._store.record_peer_influence(influence_outcome)`
  - DB writes: one record in peer_influence table (table name unknown — defined in ClosedLoopStore)

  **Step 12 — Log autopsy complete:**
  - Condition: `store is not None AND trade_id >= 0`
  - Calls `self._store.log_autopsy_complete(trade_id)`
  - Logs INFO message with trade_id, ticker, net_pnl, macro_regime
  - DB writes: unknown (defined in ClosedLoopStore)

---

### C) MATHEMATICS

FORMULA [pnl_pct]:
  Variables:
    - `net_pnl` — from closed_trade.get("net_pnl", 0.0)
    - `entry_price` — from closed_trade.get("entry_price", 1.0), floored to 1.0 if falsy
    - `position_size` — from closed_trade.get("position_size", 1.0), floored to 1.0 if falsy
  Calculation: `pnl_pct = net_pnl / (entry_price * position_size)`
  Output: stored in AutopsyReport.pnl_pct; used in Step 11 as `actual_magnitude`

FORMULA [actual_magnitude in peer influence outcome]:
  Variables:
    - `pnl_pct` — computed above
  Calculation: `actual_magnitude = abs(pnl_pct)`
  Output: stored in influence_outcome dict field "actual_magnitude"

FORMULA [actual_direction in peer influence outcome]:
  Variables:
    - `net_pnl` — from closed_trade.get("net_pnl", 0.0)
  Calculation: `actual_direction = 1 if net_pnl > 0 else -1`
  Output: stored in influence_outcome dict field "actual_direction"

---

### D) DATA FLOWS

ENTERS:
- `closed_trade` dict: ticker, net_pnl, entry_price, position_size, direction, scale_in_tranche, holding_days, signals_at_entry
- `entry_context` dict: macro_regime, active_signals, peer_influence_score, merger_spillover_flag, peer_trigger_ticker, peer_trigger_event, immediate_open_pnl

LEAVES:
- `AutopsyReport` dataclass returned from run()
- DB records written via store.record_trade (table: unknown)
- DB records written via store.record_peer_influence (table: peer_influence or similar)
- DB records written via store.log_autopsy_complete (table: unknown)
- DB records written via PnLAttributor, RegimeTracker, SignalInteractionLedger, WeightUpdater, DrawdownForensics, BenchmarkTracker, TaxManager, EntryLearner, StressLearner (each to their own tables, defined elsewhere)

DB TABLES READ: None directly in this file
DB TABLES WRITTEN: unknown names (all writes delegated to sub-modules and store methods)

---

### E) DEPENDENCIES

Internal Apollo imports (all wrapped in try/except ImportError):
- `closeloop.storage.closeloop_store.ClosedLoopStore`
- `closeloop.autopsy.pnl_attributor.PnLAttributor`
- `closeloop.learning.regime_tracker.RegimeTracker`
- `closeloop.autopsy.signal_interaction_ledger.SignalInteractionLedger`
- `closeloop.learning.weight_updater.WeightUpdater`
- `closeloop.autopsy.drawdown_forensics.DrawdownForensics`
- `closeloop.risk.benchmark_tracker.BenchmarkTracker`
- `closeloop.risk.tax_manager.TaxManager`
- `closeloop.entry.entry_learner.EntryLearner`
- `closeloop.stress.stress_learner.StressLearner`

External libraries:
- `logging` (stdlib)
- `dataclasses` (stdlib): dataclass, field
- `typing` (stdlib): Dict, List, Optional

---

### F) WIRING STATUS

TradeAutopsy.run() is intended to be called when a position closes. The file's docstring states "Runs automatically every time a position closes." However, no caller of TradeAutopsy.run() is visible within this file. Whether something in the live trading path actually calls it depends on what imports and invokes TradeAutopsy — that wiring is external to this file.

---

### G) ISSUES FOUND

1. **trade_id gate on Step 2:** Step 2 (PnL attribution) only runs if `trade_id >= 0`. The initial value is -1. If Step 1 fails (store is None or record_trade raises), trade_id stays at -1 and Step 2 is silently skipped. No error is raised; the autopsy continues without attribution data. This cascades: Step 5 (WeightUpdater) only runs if `attribution` is non-empty, so a Step 1 failure silently prevents weight updates.

2. **Step 11 actual_direction logic error:** `actual_direction = 1 if net_pnl > 0 else -1`. A break-even trade (net_pnl == 0) is coded as direction -1, which is incorrect. A zero-pnl trade is categorised as a loss in peer influence records.

3. **Nine sub-modules imported with blanket ImportError suppression:** If any sub-module fails to import for any reason other than an import path issue (e.g., a syntax error in the imported file), the module-level try/except catches it silently, sets the class to None, and all downstream steps involving that class are silently skipped. No exception is re-raised.

4. **AutopsyReport.trade_id initialised to -1 and may never be set:** If store is None at construction time, trade_id stays -1 throughout and multiple steps are conditionally blocked. The AutopsyReport returned will have trade_id=-1 with no failure indication beyond the notes list.

5. **No caller is identified in this file:** The docstring promises "runs automatically every time a position closes" but no scheduling, signal, or direct call is present here. Whether this promise is fulfilled depends entirely on external code.

---
---

## FILE 2: /home/dannyelticala/quant-fund/closeloop/autopsy/pnl_attributor.py

### A) PURPOSE
Decomposes trade P&L across all contributing signals using counterfactual simulation. For each active signal at entry, estimates what the P&L would have been without that signal (the counterfactual), then attributes the difference as that signal's contribution. Also maintains an in-memory scorecard cache per signal with win rate and Sharpe ratio, and can return a recommendation (INCREASE / INVESTIGATE / DECREASE / MAINTAIN) per signal.

---

### B) CLASSES AND METHODS

#### Module-level function: `_std(values: List[float]) -> float`
- Inputs: list of floats
- Outputs: float (sample standard deviation)
- What it does: pure-Python implementation of sample standard deviation (ddof=1). Returns 0.0 if fewer than 2 values. Computes mean, then variance = sum of squared deviations / (n-1), then sqrt(variance).
- DB reads/writes: none

#### Module-level function: `_sharpe(pnl_series: List[float]) -> float`
- Inputs: list of floats (P&L values)
- Outputs: float (annualised Sharpe ratio)
- What it does: computes annualised Sharpe ratio assuming zero risk-free rate. Uses numpy if available (ddof=1 std), otherwise uses _std(). Returns 0.0 if series is empty or std == 0.
- DB reads/writes: none

---

#### Class: PnLAttributor

**Method: `__init__(self, store=None, config=None)`**
- Inputs: `store` (ClosedLoopStore or None), `config` (dict or None)
- Outputs: None
- What it does: stores store and config; initialises `self._scorecard_cache` as empty dict mapping signal_name to {pnl_series, wins, n}
- DB reads/writes: none

---

**Method: `attribute(self, trade_id: int, closed_trade: dict, entry_context: dict) -> List[Dict]`**
- Inputs:
  - `trade_id`: int — the stored trade identifier
  - `closed_trade`: dict with keys: net_pnl, direction, position_size, entry_price, scale_in_tranche
  - `entry_context`: dict with keys: active_signals (list of str or dict), immediate_open_pnl, peer_influence_score, plus signal-specific keys like `{signal_name}_planned_pnl`
- Outputs: list of attribution dicts

What it does:
  1. Extracts actual_pnl, direction, position_size, entry_price, actual_return from closed_trade
  2. Extracts active_signals from entry_context; normalises list-of-str to list-of-dict (str signals get strength=1.0, role="secondary")
  3. For each signal dict, runs counterfactual logic depending on `role`:
     - role == "primary_trigger": counterfactual_pnl = 0.0
     - role == "size_boost": counterfactual_pnl = actual_pnl * 0.80
     - role == "early_exit": counterfactual_pnl = entry_context.get("{signal_name}_planned_pnl", actual_pnl * 1.1)
     - else (secondary): weight_fraction = strength / total_strength; counterfactual_pnl = actual_pnl * (1.0 - weight_fraction)
  4. attributed_pnl = actual_pnl - counterfactual_pnl
  5. attributed_pnl_pct = attributed_pnl / (entry_price * position_size)
  6. was_correct = (direction * actual_return) > 0
  7. Builds attr_record dict with 8 standard fields; conditionally adds entry_alpha, scale_in_tranche, peer_influence_score
  8. Calls `self._update_cache(signal_name, attributed_pnl, was_correct)`
  9. After all signals processed: if attributions non-empty and store not None, calls `self._store.record_attribution(trade_id, attributions)`
  10. Returns attributions list

- DB reads: none directly
- DB writes: `self._store.record_attribution(trade_id, attributions)` — table name unknown, defined in ClosedLoopStore

---

**Method: `_update_cache(self, signal_name: str, pnl: float, was_correct: bool) -> None`**
- Inputs: signal_name string, pnl float, was_correct bool
- Outputs: None
- What it does: initialises cache entry for signal_name if not present; appends pnl to pnl_series; increments n; increments wins if was_correct
- DB reads/writes: none

---

**Method: `get_scorecard(self, signal_name: str) -> Dict`**
- Inputs: `signal_name` string
- Outputs: dict with keys: signal_name, n_trades, total_pnl, mean_pnl, win_rate, sharpe, recommendation
- What it does:
  1. If store available, calls `self._store.get_signal_scorecard(signal_name)` for live data
  2. Falls back to in-memory cache if store unavailable
  3. Prefers store values for n, total_pnl, mean_pnl, win_rate; always computes sharpe from in-memory pnl_series
  4. Applies recommendation rules (see Mathematics section)
- DB reads: `store.get_signal_scorecard(signal_name)` — table unknown, defined in ClosedLoopStore
- DB writes: none

---

### C) MATHEMATICS

FORMULA [actual_return]:
  Variables:
    - `actual_pnl` — closed_trade.get("net_pnl", 0.0)
    - `entry_price` — closed_trade.get("entry_price", 1.0)
    - `position_size` — closed_trade.get("position_size", 1.0)
  Calculation: `actual_return = actual_pnl / (entry_price * position_size) if entry_price else 0.0`
  Output: used in was_correct calculation

FORMULA [was_correct]:
  Variables:
    - `direction` — closed_trade.get("direction", 1)
    - `actual_return` — computed above
  Calculation: `was_correct = (direction * actual_return) > 0`
  Output: stored in attr_record["was_correct"]

FORMULA [counterfactual_pnl — primary_trigger]:
  Variables:
    - role == "primary_trigger"
  Calculation: `counterfactual_pnl = 0.0`
  Output: attributed_pnl = actual_pnl - 0.0 = actual_pnl (full credit to primary trigger)

FORMULA [counterfactual_pnl — size_boost]:
  Variables:
    - `actual_pnl` — from closed_trade
    - `base_fraction = 0.80` — hardcoded constant
  Calculation: `counterfactual_pnl = actual_pnl * 0.80`
  Output: attributed_pnl = actual_pnl * 0.20 (20% of P&L attributed to size boost)

FORMULA [counterfactual_pnl — early_exit]:
  Variables:
    - `signal_name` — name of the signal
    - `actual_pnl` — from closed_trade
    - `planned_hold_pnl` — entry_context.get("{signal_name}_planned_pnl", actual_pnl * 1.1)
  Calculation: `counterfactual_pnl = float(planned_hold_pnl)`
  Output: attributed_pnl = actual_pnl - planned_hold_pnl (typically negative — signal caused early exit that hurt returns relative to planned hold)

FORMULA [counterfactual_pnl — secondary (generic)]:
  Variables:
    - `strength` — sig.get("strength", 1.0) cast to float
    - `total_strength` — sum of all signal strengths in signal_dicts, floored to 1.0 if zero
    - `actual_pnl` — from closed_trade
  Calculation:
    `weight_fraction = strength / total_strength`
    `counterfactual_pnl = actual_pnl * (1.0 - weight_fraction)`
  Output: `attributed_pnl = actual_pnl - counterfactual_pnl = actual_pnl * weight_fraction`

FORMULA [attributed_pnl]:
  Variables:
    - `actual_pnl` — from closed_trade
    - `counterfactual_pnl` — computed by one of the four branches above
  Calculation: `attributed_pnl = actual_pnl - counterfactual_pnl`
  Output: stored in attr_record["attributed_pnl"]; fed to WeightUpdater.soft_update

FORMULA [attributed_pnl_pct]:
  Variables:
    - `attributed_pnl` — computed above
    - `entry_price` — from closed_trade
    - `position_size` — from closed_trade
    - `denominator` = entry_price * position_size
  Calculation: `attributed_pnl_pct = attributed_pnl / denominator if denominator else 0.0`
  Output: stored in attr_record["attributed_pnl_pct"]

FORMULA [entry_alpha (EntryTimer only)]:
  Variables:
    - `actual_pnl` — from closed_trade
    - `immediate_open_pnl` — entry_context.get("immediate_open_pnl", actual_pnl)
  Calculation: `entry_alpha = actual_pnl - float(immediate_open_pnl)`
  Output: stored in attr_record["entry_alpha"]

FORMULA [annualised Sharpe — _sharpe function]:
  Variables:
    - `pnl_series` — list of floats
    - `mean` — arithmetic mean of series
    - `std` — sample standard deviation (ddof=1)
    - sqrt(252) — annualisation factor (252 trading days)
  Calculation: `sharpe = (mean / std) * math.sqrt(252)` if std > 0 else 0.0
  Output: used in get_scorecard recommendation logic

FORMULA [standard deviation — _std function]:
  Variables:
    - `values` — list of floats
    - `mean` = sum(values) / len(values)
    - `variance` = sum((v - mean)^2 for v in values) / (len(values) - 1)
  Calculation: `std = math.sqrt(variance)` if variance > 0 else 0.0
  Output: used by _sharpe when numpy unavailable

FORMULA [scorecard recommendation rules]:
  Variables:
    - `n` — trade count
    - `sharpe` — annualised Sharpe from pnl_series
    - `win_rate` — wins / n
  Calculation (applied in order):
    IF n >= 10 AND sharpe > 1.0 AND win_rate > 0.55 → recommendation = "INCREASE"
    ELIF n >= 20 AND sharpe < 0 AND win_rate < 0.40 → recommendation = "INVESTIGATE"
    ELIF n >= 20 AND sharpe < -0.5 → recommendation = "DECREASE"
    ELSE → recommendation = "MAINTAIN"
  Output: returned in get_scorecard dict

---

### D) DATA FLOWS

ENTERS attribute():
- trade_id int
- closed_trade dict: net_pnl, direction, position_size, entry_price, scale_in_tranche
- entry_context dict: active_signals, immediate_open_pnl, peer_influence_score, {signal_name}_planned_pnl keys

LEAVES attribute():
- List[Dict] attributions returned to caller (TradeAutopsy)
- DB write: store.record_attribution(trade_id, attributions) — writes attribution records, table name unknown

ENTERS get_scorecard():
- signal_name string

LEAVES get_scorecard():
- Dict with scorecard data
- DB read: store.get_signal_scorecard(signal_name) — table name unknown

---

### E) DEPENDENCIES

Internal Apollo imports (try/except at module level):
- `closeloop.storage.closeloop_store.ClosedLoopStore`

External libraries:
- `logging` (stdlib)
- `math` (stdlib)
- `typing` (stdlib): Dict, List, Optional
- `numpy` (optional, try/except at module level) — used for Sharpe computation

---

### F) WIRING STATUS

PnLAttributor is instantiated by TradeAutopsy.__init__() and called in TradeAutopsy.run() Step 2. It is connected to the post-trade path through TradeAutopsy. Attribution data is passed to WeightUpdater.soft_update() in Step 5. The scorecard (get_scorecard) does not appear to be called by any other file in GROUP 6 — it may be used externally or unused.

---

### G) ISSUES FOUND

1. **was_correct uses direction from closed_trade, not signal direction:** `was_correct = (direction * actual_return) > 0` uses the overall trade direction, not the individual signal's direction. All signals in a trade share the same was_correct value regardless of whether each signal actually predicted the outcome independently.

2. **early_exit counterfactual default is actual_pnl * 1.1:** If the entry_context key `{signal_name}_planned_pnl` is not set, the default is `actual_pnl * 1.1` — i.e., assumes the counterfactual hold would have yielded 10% more P&L. This is fabricated data, not measured. The attributed_pnl for early_exit signals will be negative by default even when the early exit was beneficial.

3. **In-memory scorecard cache is process-local and ephemeral:** `_scorecard_cache` is an instance variable on PnLAttributor. If TradeAutopsy (and thus PnLAttributor) is re-instantiated on every trade, the cache is reset each time and contains zero history. The cache only accumulates within the lifetime of a single TradeAutopsy instance.

4. **get_scorecard Sharpe always uses in-memory pnl_series only:** Even when store data is preferred for n, total_pnl, mean_pnl, and win_rate, the Sharpe is always computed from `cache.get("pnl_series", [])` — the local in-memory series. If the cache is empty (new instance), Sharpe is always 0.0 regardless of stored history.

5. **Scorecard n vs Sharpe mismatch:** n may come from store (e.g. n=200) while sharpe is computed from an empty in-memory series (result: 0.0). The recommendation logic therefore always evaluates sharpe=0.0 when cache is empty, suppressing INCREASE and DECREASE recommendations even with hundreds of trades.

6. **No minimum sample size guard on attribute():** If active_signals is empty, attributions is empty, record_attribution is not called, and WeightUpdater.soft_update() is silently skipped in TradeAutopsy Step 5. No warning is emitted.

7. **total_strength floored to 1.0:** `total_strength = sum(...) or 1.0` — if all signal strengths sum to zero (e.g., all strength=0.0), weight_fraction = 0.0 / 1.0 = 0.0, and counterfactual_pnl = actual_pnl * (1 - 0) = actual_pnl, making attributed_pnl = 0.0 for every signal. No warning.

---
---

## FILE 3: /home/dannyelticala/quant-fund/closeloop/learning/weight_updater.py

### A) PURPOSE
Manages signal weight updates via two mechanisms: (1) a soft per-trade gradient update applied after every trade autopsy, and (2) a weekly batch optimisation using either cvxpy mean-variance optimisation or a proportional-Sharpe fallback. Also provides stress regime caps: VIX-based caps on fragile signals, and an across-the-board 30% reduction during CRISIS correlation regimes.

---

### B) CLASSES AND METHODS

#### Module-level function: `_sharpe_from_series(pnl_series: List[float]) -> float`
- Inputs: list of floats
- Outputs: float (annualised Sharpe)
- What it does: same formula as pnl_attributor._sharpe — annualised Sharpe with ddof=1, zero risk-free rate, sqrt(252) annualisation. Returns 0.0 if fewer than 2 values or std==0.
- DB reads/writes: none

---

#### Module-level constant: `DEFAULT_WEIGHT_BOUNDS`
`{"min": 0.05, "max": 3.0}`

---

#### Class: WeightUpdater

**Method: `__init__(self, store=None, config=None)`**
- Inputs: `store` (ClosedLoopStore or None), `config` (dict or None)
- Outputs: None
- What it does:
  - Stores store reference
  - Extracts config sub-path: `config.get("closeloop", {}).get("learning", {})`
  - Reads from config with defaults:
    - `self._soft_update_rate: float` = cfg.get("soft_update_rate", 0.02)
    - `self._max_change: float` = cfg.get("max_weight_change_per_update", 0.15)
    - `self._bounds: Dict` = cfg.get("weight_bounds", DEFAULT_WEIGHT_BOUNDS)
    - `self._min_trades: int` = cfg.get("min_trades_before_weight_update", 10)
    - `self._w_min: float` = bounds.get("min", 0.05)
    - `self._w_max: float` = bounds.get("max", 3.0)
- DB reads/writes: none

---

**Method: `soft_update(self, attribution: List[Dict], entry_context: dict) -> List[Dict]`**
- Inputs:
  - `attribution`: list of dicts from PnLAttributor.attribute() with keys: signal_name, attributed_pnl, (others unused)
  - `entry_context`: dict (not actually used in soft_update — present for potential future use)
- Outputs: list of change dicts: {signal_name, old_weight, new_weight, reason}
- What it does (per signal in attribution):
  1. Fetches n_trades from store.get_signal_scorecard(signal_name); if store is None or fails, n_trades = 0
  2. If n_trades < self._min_trades (default 10): skip this signal, log debug, continue
  3. Fetches current_weight from store.get_signal_weight(signal_name, default=1.0); if store unavailable, defaults to 1.0
  4. Computes gradient = attributed_pnl * self._soft_update_rate
  5. raw_new = current_weight * (1 + gradient)
  6. Enforces max change: max_delta = current_weight * self._max_change; if abs(raw_new - current_weight) > max_delta, clips direction
  7. Clips to [w_min, w_max]
  8. If abs(new_weight - current_weight) < 1e-9: skips (no meaningful change)
  9. Persists via store.set_signal_weight(signal_name, new_weight, reason=..., n_trades=..., sharpe=0.0, auto=True)
     NOTE: sharpe is hardcoded to 0.0 in soft_update
  10. Appends change record to changes list
- DB reads: store.get_signal_scorecard(signal_name), store.get_signal_weight(signal_name)
- DB writes: store.set_signal_weight(signal_name, new_weight, ...) — table: signal_weights (confirmed by get_weight_summary query)

---

**Method: `batch_update(self, all_trades: List[Dict]) -> Dict`**
- Inputs: `all_trades` — list of trade dicts each with keys: signals_at_entry (JSON string or dict), net_pnl
- Outputs: dict of {signal_name: new_weight}
- What it does:
  1. Iterates all_trades; parses signals_at_entry (JSON or dict); appends net_pnl to signal_pnl[sig] for each signal found
  2. If signal_pnl is empty: logs warning, returns {}
  3. Computes per-signal Sharpe ratios
  4. Fetches current_weights from store.get_all_weights()
  5. If cvxpy AND numpy available AND len(signal_names) >= 2: runs mean-variance optimisation (see Mathematics)
  6. If cvxpy not available or fails: uses proportional-Sharpe fallback (see Mathematics)
  7. Applies 70/30 smoothing: blended = 0.70 * current + 0.30 * optimal
  8. Clips blended to [w_min, w_max]
  9. Logs WARNING for any signal with change_pct > 20%
  10. Persists each signal via store.set_signal_weight(sig, blended, reason=..., n_trades=..., sharpe=..., auto=True)
  11. Returns new_weights dict
- DB reads: store.get_all_weights()
- DB writes: store.set_signal_weight(...) for each signal — table: signal_weights

---

**Method: `apply_stress_caps(self, vix_level: float, crisis_fragile_signals: List[str]) -> None`**
- Inputs: `vix_level` float, `crisis_fragile_signals` list of signal name strings
- Outputs: None
- What it does:
  - If store is None: logs warning, returns
  - Fetches all_weights from store.get_all_weights()
  - If vix_level > 25: for each signal in crisis_fragile_signals, caps weight at max(w_min, current * 0.5)
  - Persists capped weight via store.set_signal_weight(sig, capped, reason="STRESS_CAP_APPLIED vix=...", auto=True)
  - Logs WARNING for each cap applied
- DB reads: store.get_all_weights()
- DB writes: store.set_signal_weight(...) — table: signal_weights

---

**Method: `apply_crisis_reduction(self) -> None`**
- Inputs: none
- Outputs: None
- What it does:
  - If store is None: returns immediately (no log)
  - Fetches all_weights from store.get_all_weights()
  - For each signal, reduces weight by 30%: reduced = max(w_min, current * 0.70)
  - Persists via store.set_signal_weight(sig, reduced, reason="CRISIS_REGIME_REDUCTION -30%", auto=True)
  - Logs WARNING for each reduction
- DB reads: store.get_all_weights()
- DB writes: store.set_signal_weight(...) — table: signal_weights

---

**Method: `get_weight_summary(self) -> str`**
- Inputs: none
- Outputs: formatted string table
- What it does:
  - If store is None: returns "No store available.\n"
  - Executes direct SQL on store._conn():
    `SELECT sw.signal_name, sw.weight, sw.previous_weight, sw.n_trades_basis, sw.sharpe_basis FROM signal_weights sw ORDER BY sw.weight DESC`
  - Formats result as fixed-width text table: Signal | Weight | Change% | N Trades | Sharpe
  - Change% = (current - previous) / abs(previous) * 100
- DB reads: table `signal_weights`, columns: signal_name, weight, previous_weight, n_trades_basis, sharpe_basis
- DB writes: none

---

### C) MATHEMATICS

FORMULA [soft_update gradient]:
  Variables:
    - `attributed_pnl` — attr.get("attributed_pnl", 0.0) from attribution dict
    - `self._soft_update_rate` — config "soft_update_rate", default 0.02
  Calculation: `gradient = attributed_pnl * self._soft_update_rate`
  Output: used to compute raw_new weight

FORMULA [soft_update raw_new weight]:
  Variables:
    - `current_weight` — from store.get_signal_weight, default 1.0
    - `gradient` — computed above
  Calculation: `raw_new = current_weight * (1.0 + gradient)`
  Output: clipped to produce new_weight

FORMULA [soft_update max change enforcement]:
  Variables:
    - `current_weight`
    - `self._max_change` — config "max_weight_change_per_update", default 0.15
    - `max_delta = current_weight * self._max_change`
    - `direction = 1.0 if raw_new > current_weight else -1.0`
  Calculation (only if abs(raw_new - current_weight) > max_delta):
    `raw_new = current_weight + direction * max_delta`
  Output: raw_new after max-change enforcement

FORMULA [soft_update final weight clip]:
  Variables:
    - `raw_new` — after max-change enforcement
    - `self._w_min` = 0.05 (default), `self._w_max` = 3.0 (default)
  Calculation: `new_weight = max(self._w_min, min(self._w_max, raw_new))`
  Output: new_weight persisted to store

FORMULA [batch_update cvxpy mean-variance objective]:
  Variables:
    - `pnl_matrix` — numpy array shape (max_len, n_signals), padded with zeros at start
    - `mu` = pnl_matrix.mean(axis=0) — per-signal mean P&L
    - `cov` = np.cov(pnl_matrix, rowvar=False) — covariance matrix
    - `risk_aversion = 2.0` — hardcoded constant
    - `w` = cp.Variable(len(signal_names))
  Calculation:
    Maximize: `mu @ w - 0.5 * risk_aversion * cp.quad_form(w, cov)`
    Subject to: `w >= self._w_min` and `w <= self._w_max`
    Solver: cp.ECOS with warm_start=True
  Output: optimal weights vector w.value

FORMULA [batch_update proportional-Sharpe fallback]:
  Variables:
    - `sharpes` — dict of signal_name -> Sharpe ratio
    - `positive_sharpes` = {s: max(0.0, sharpes[s]) for s in signal_names}
    - `total` = sum(positive_sharpes.values()) or 1.0
    - `n_signals` = len(signal_names)
  Calculation (per signal s):
    `optimal[s] = max(w_min, min(w_max, (positive_sharpes[s] / total) * n_signals))`
  Output: optimal weights dict

FORMULA [batch_update 70/30 smoothing]:
  Variables:
    - `cur` = current_weights.get(s, 1.0)
    - `optimal[s]` — from cvxpy or Sharpe fallback
  Calculation: `blended = 0.70 * cur + 0.30 * optimal[s]`
    then: `blended = max(w_min, min(w_max, blended))`
  Output: new_weights[s] persisted to store

FORMULA [stress cap]:
  Variables:
    - `current` — current weight from store
    - `self._w_min` = 0.05
    - condition: `vix_level > 25`
  Calculation: `capped = max(self._w_min, current * 0.5)`
  Output: weight persisted to store with reason STRESS_CAP_APPLIED

FORMULA [crisis reduction]:
  Variables:
    - `current` — current weight
    - `self._w_min` = 0.05
  Calculation: `reduced = max(self._w_min, current * 0.70)`
  Output: weight persisted to store with reason CRISIS_REGIME_REDUCTION -30%

FORMULA [significant change notification — batch_update]:
  Variables:
    - `blended` — new weight
    - `cur` — old weight
  Calculation: `change_pct = abs(blended - cur) / max(abs(cur), 1e-9) * 100`
  Threshold: > 20.0% triggers WARNING log
  Output: log message only

FORMULA [weight summary change%]:
  Variables:
    - `cur = r["weight"] or 1.0`
    - `prev = r["previous_weight"] or 1.0`
  Calculation: `change = (cur - prev) / max(abs(prev), 1e-9) * 100`
  Output: displayed in summary table

FORMULA [annualised Sharpe — _sharpe_from_series]:
  Variables:
    - `pnl_series` — list of floats
    - `mean`, `std` (ddof=1)
    - sqrt(252) — annualisation factor
  Calculation: `(mean / std) * math.sqrt(252)` if std > 0 else 0.0
  Output: used in batch_update and logged with weight changes

---

### D) DATA FLOWS

ENTERS soft_update():
- attribution list from PnLAttributor.attribute()
- entry_context dict (unused in current implementation)

LEAVES soft_update():
- List[Dict] weight change records returned to TradeAutopsy
- DB reads: signal_weights table (via store.get_signal_scorecard, store.get_signal_weight)
- DB writes: signal_weights table (via store.set_signal_weight)

ENTERS batch_update():
- all_trades list from BatchRetrainer (via store.get_trades)

LEAVES batch_update():
- Dict {signal_name: new_weight}
- DB reads: store.get_all_weights() — table: signal_weights
- DB writes: store.set_signal_weight() — table: signal_weights

DB TABLES CONFIRMED:
- `signal_weights` — columns: signal_name, weight, previous_weight, n_trades_basis, sharpe_basis (from get_weight_summary raw SQL)

---

### E) DEPENDENCIES

Internal Apollo imports (try/except at module level):
- `closeloop.storage.closeloop_store.ClosedLoopStore`

External libraries:
- `logging` (stdlib)
- `math` (stdlib)
- `datetime` (stdlib)
- `typing` (stdlib)
- `numpy` (optional, try/except) — used for pnl matrix, mean, cov
- `cvxpy` (optional, try/except) — used for mean-variance optimisation

---

### F) WIRING STATUS

soft_update() is called by TradeAutopsy.run() Step 5 after every trade close. This path is connected.

batch_update() is called by BatchRetrainer.run() Steps 6/7. BatchRetrainer.run() is described as running "on Sunday evening" but no scheduler or cron is visible in these files.

apply_stress_caps() and apply_crisis_reduction() have no visible callers within GROUP 6. They appear to be dead code without an external caller invoking them.

get_weight_summary() has no visible caller in GROUP 6.

---

### G) ISSUES FOUND

1. **soft_update persists sharpe=0.0 always:** The store.set_signal_weight call in soft_update hardcodes `sharpe=0.0` regardless of actual signal Sharpe. The signal_weights table will have sharpe_basis=0.0 for all soft-update entries, making the get_weight_summary Sharpe column meaningless for soft-update records.

2. **batch_update reads signals_at_entry as JSON string but stores them as trade-level P&L:** The attribution assigns P&L per signal by splitting the trade's net_pnl across all signals present. Every signal in a trade gets the full net_pnl appended to its series (not its attributed portion). This means signal P&L series are inflated and not the same as pnl_attributor's attributed_pnl values.

3. **batch_update pnl_matrix padding zeros:** The pnl_matrix is initialised to all zeros and series are placed at the end (`pnl_matrix[-len(series):, j] = series`). Signals with fewer trades than max_len have leading zeros in their P&L series. These zeros enter the covariance calculation and distort mean and variance estimates for shorter-history signals.

4. **apply_stress_caps caller not found:** No call to apply_stress_caps() or apply_crisis_reduction() is present in GROUP 6. These methods cannot self-trigger.

5. **entry_context argument in soft_update is unused:** The method signature accepts entry_context but never references it. This may indicate planned functionality that was never implemented.

6. **get_weight_summary calls store._conn() directly:** This accesses a private method on the store object, bypassing any abstraction layer the store provides. If the store's internal connection method is renamed or changes, this silently breaks.

7. **n_trades in soft_update is read from scorecard, not from the attribution data:** The attribution dict already carries attributed_pnl, but the trade count gate queries the store separately. If the store is unavailable, n_trades = 0 and ALL signals are skipped — soft_update returns an empty list silently.

---
---

## FILE 4: /home/dannyelticala/quant-fund/closeloop/learning/batch_retrainer.py

### A) PURPOSE
Runs the full deep-retrain cycle, designed to execute on Sunday evenings. Pulls 52 weeks of closed trades, computes per-signal performance statistics, computes three categories of alpha (entry timing, peer influence, analyst revision), calls WeightUpdater.batch_update() for portfolio optimisation, saves a full text report to disk, and logs notifications for significant weight changes.

---

### B) CLASSES AND METHODS

#### Module-level function: `_sharpe(pnl_series: List[float]) -> float`
- Inputs: list of floats
- Outputs: float (annualised Sharpe, same formula as other files)
- DB reads/writes: none

---

#### Class: BatchRetrainer

**Method: `__init__(self, store=None, config=None)`**
- Inputs: `store` (ClosedLoopStore or None), `config` (dict or None)
- Outputs: None
- What it does:
  - Stores store and config references
  - Extracts `self._cl_cfg = config.get("closeloop", {})`
  - Reads from config:
    - `self._output_dir` = Path(cl_cfg.get("retrain_output_dir", "output/weight_updates"))
    - `self._retrain_day` = cl_cfg.get("batch_retrain_day", "Sunday")
    - `self._n_weeks` = cl_cfg.get("retrain_lookback_weeks", 52)
  - Attempts to instantiate WeightUpdater(store, config) → self._weight_updater
  - Attempts to instantiate RegimeTracker(store, config) → self._regime_tracker
  - Both wrapped in try/except
- DB reads/writes: none

---

**Method: `should_run_today(self) -> bool`**
- Inputs: none
- Outputs: bool
- What it does: gets today's weekday name from datetime.now(timezone.utc).strftime("%A"); compares case-insensitively to self._retrain_day (default "Sunday")
- DB reads/writes: none

---

**Method: `run(self) -> Dict`**
- Inputs: none
- Outputs: dict with keys: n_trades, signals_updated, report_path, weight_changes
- What it does (9 steps, all try/except isolated):

  **Step 1 — Pull trades:**
  - `n_lookback = self._n_weeks * 5` (default: 52 * 5 = 260, comment says "use 500 to be safe" but actual calculation uses n_weeks * 5)
  - Calls `self._store.get_trades(n=n_lookback)`
  - Sets result["n_trades"]
  - DB reads: store.get_trades(n=260) — table unknown, defined in ClosedLoopStore

  **Step 2 — Per-signal stats:**
  - Calls `self._compute_signal_stats(trades)`

  **Step 3 — Entry timing alpha:**
  - Calls `self._compute_entry_timing_alpha()`
  - DB reads: entry_timing_outcomes table (see _compute_entry_timing_alpha)

  **Step 4 — Peer influence alpha:**
  - Calls `self._compute_peer_alpha()`
  - DB reads: peer_influence_outcomes table

  **Step 5 — Analyst revision alpha:**
  - Calls `self._compute_analyst_alpha()`
  - DB reads: analyst_revision_outcomes table

  **Step 6+7 — Batch optimisation:**
  - Condition: `self._weight_updater is not None AND trades is non-empty`
  - Calls `self._weight_updater.batch_update(trades)`
  - Sets result["signals_updated"] = len(weight_changes)
  - DB reads/writes: via WeightUpdater.batch_update (signal_weights table)

  **Step 8 — Save report:**
  - Calls `self._generate_report(...)` then `self._save_report(report_text)`
  - Writes file to: output/weight_updates/retrain_{timestamp}.txt
  - DB reads/writes: none (file I/O only)

  **Step 9 — Notify significant changes:**
  - Fetches current_weights from store.get_all_weights()
  - For each signal in weight_changes: computes change_pct; logs WARNING if > 10%
  - NOTE: this comparison happens AFTER weights have already been written in Step 6/7, so current_weights fetched here are the NEW weights, not the old ones — the comparison is against already-updated values
  - DB reads: store.get_all_weights()

---

**Method: `_compute_signal_stats(self, trades: List[Dict]) -> Dict[str, Dict]`**
- Inputs: list of trade dicts
- Outputs: dict of {signal_name: {n, mean_pnl, win_rate, sharpe, best_regime, worst_regime}}
- What it does:
  - Parses signals_at_entry (JSON or dict) from each trade
  - Accumulates per-signal: pnl_series (net_pnl per trade), win count, regime-bucketed pnl series
  - Computes per-signal: n, mean_pnl, win_rate, Sharpe
  - Finds best_regime and worst_regime by mean P&L within each regime bucket
- DB reads/writes: none (operates on in-memory trades list)

---

**Method: `_compute_entry_timing_alpha(self) -> float`**
- Inputs: none
- Outputs: float (average pnl_vs_immediate_entry)
- What it does: executes direct SQL on store._conn():
  `SELECT AVG(pnl_vs_immediate_entry) as avg FROM entry_timing_outcomes WHERE pnl_vs_immediate_entry IS NOT NULL`
  Returns avg or 0.0 on failure
- DB reads: table `entry_timing_outcomes`, column `pnl_vs_immediate_entry`
- DB writes: none

---

**Method: `_compute_peer_alpha(self) -> Dict`**
- Inputs: none
- Outputs: dict with keys: n, accuracy, mean_pnl
- What it does: executes direct SQL on store._conn():
  `SELECT COUNT(*) as n, AVG(CASE WHEN was_correct=1 THEN 1.0 ELSE 0.0 END) as accuracy, AVG(pnl) as mean_pnl FROM peer_influence_outcomes`
- DB reads: table `peer_influence_outcomes`, columns: was_correct, pnl
- DB writes: none

---

**Method: `_compute_analyst_alpha(self) -> Dict`**
- Inputs: none
- Outputs: dict with keys: n, avg_fwd_5d, avg_fwd_20d, avg_pnl, pead_improvement_rate
- What it does: executes direct SQL on store._conn():
  `SELECT COUNT(*) as n, AVG(forward_return_5d) as avg_5d, AVG(forward_return_20d) as avg_20d, AVG(pnl_if_traded) as avg_pnl, AVG(CASE WHEN pead_improved=1 THEN 1.0 ELSE 0.0 END) as pead_improvement_rate FROM analyst_revision_outcomes`
- DB reads: table `analyst_revision_outcomes`, columns: forward_return_5d, forward_return_20d, pnl_if_traded, pead_improved
- DB writes: none

---

**Method: `_generate_report(self, stats, weight_changes, entry_timing_alpha, peer_alpha, analyst_alpha, n_trades) -> str`**
- Inputs: all analysis results from run()
- Outputs: formatted text report string
- What it does: assembles fixed-width text report with five sections: Signal Performance Table, Entry Timing Alpha, Peer Influence Performance, Analyst Revision Alpha, Weight Changes
- DB reads/writes: none

---

**Method: `_save_report(self, report_text: str) -> str`**
- Inputs: report_text string
- Outputs: path string of saved file
- What it does:
  - Creates output_dir if needed (parents=True, exist_ok=True)
  - Generates filename: `retrain_{YYYYmmdd_HHMMSS}.txt`
  - Writes UTF-8 text to that path
  - Returns path string
- DB reads/writes: none (file system only)

---

### C) MATHEMATICS

FORMULA [n_lookback]:
  Variables:
    - `self._n_weeks` = config.get("retrain_lookback_weeks", 52)
  Calculation: `n_lookback = self._n_weeks * 5`
  Output: used as argument to store.get_trades(n=n_lookback); default = 260
  Note: the comment in code says "use a generous 500 to be safe" but the actual calculation yields 260 when n_weeks=52

FORMULA [signal mean_pnl in _compute_signal_stats]:
  Variables:
    - `pnl_list` — list of net_pnl values for trades where signal was active
    - `n` = len(pnl_list)
  Calculation: `mean_pnl = sum(pnl_list) / n`
  Output: stored in stats[sig]["mean_pnl"]

FORMULA [signal win_rate in _compute_signal_stats]:
  Variables:
    - `signal_wins[sig]` — count of was_profitable == True trades
    - `n` — total trade count for signal
  Calculation: `win_rate = signal_wins[sig] / n`
  Output: stored in stats[sig]["win_rate"]

FORMULA [regime mean P&L in _compute_signal_stats]:
  Variables:
    - `regime_means` = {r: sum(v) / len(v) for r, v in signal_regime_pnl[sig].items() if v}
  Calculation:
    `best_regime = max(regime_means, key=lambda r: regime_means[r])`
    `worst_regime = min(regime_means, key=lambda r: regime_means[r])`
  Output: stored in stats[sig]["best_regime"] and stats[sig]["worst_regime"]

FORMULA [entry timing alpha]:
  Variables:
    - `pnl_vs_immediate_entry` — column in entry_timing_outcomes table
  Calculation: `AVG(pnl_vs_immediate_entry)` via SQL
  Output: returned as float, included in retrain report

FORMULA [peer influence accuracy]:
  Variables:
    - `was_correct` — column in peer_influence_outcomes (0 or 1)
  Calculation: `AVG(CASE WHEN was_correct=1 THEN 1.0 ELSE 0.0 END)` via SQL
  Output: returned in peer_alpha dict

FORMULA [significant change notification — run() Step 9]:
  Variables:
    - `new_w` — from weight_changes dict (already written to DB)
    - `old_w` = current_weights.get(sig, 1.0) — fetched AFTER write
  Calculation: `change_pct = abs(new_w - old_w) / max(abs(old_w), 1e-9) * 100`
  Threshold: > 10.0%
  Output: WARNING log message

FORMULA [annualised Sharpe — _sharpe function]:
  Same as Files 2 and 3: `(mean / std) * math.sqrt(252)` with ddof=1, returns 0.0 if std == 0

---

### D) DATA FLOWS

ENTERS run():
- No arguments; reads everything from store

LEAVES run():
- Dict result returned to caller
- File written to: output/weight_updates/retrain_{timestamp}.txt
- DB reads:
  - store.get_trades(n=260) — table unknown
  - entry_timing_outcomes table (columns: pnl_vs_immediate_entry)
  - peer_influence_outcomes table (columns: was_correct, pnl)
  - analyst_revision_outcomes table (columns: forward_return_5d, forward_return_20d, pnl_if_traded, pead_improved)
  - store.get_all_weights() — table: signal_weights (Step 9)
- DB writes:
  - signal_weights table (via WeightUpdater.batch_update)

---

### E) DEPENDENCIES

Internal Apollo imports (try/except at module level):
- `closeloop.storage.closeloop_store.ClosedLoopStore`
- `closeloop.learning.weight_updater.WeightUpdater`
- `closeloop.learning.regime_tracker.RegimeTracker`

External libraries:
- `logging` (stdlib)
- `math` (stdlib)
- `os` (stdlib, imported but never used)
- `datetime` (stdlib)
- `pathlib.Path` (stdlib)
- `typing` (stdlib)
- `numpy` (optional, try/except)
- `json` (stdlib, imported inside _compute_signal_stats method)

---

### F) WIRING STATUS

BatchRetrainer.run() is meant to be called on Sunday evenings. The class provides should_run_today() for a caller to check, but no scheduler, cron, or orchestrator call is visible in GROUP 6. It is not self-scheduling. Whether any external process calls it is not determinable from these four files.

RegimeTracker is imported and instantiated in __init__ but is never used in any method of BatchRetrainer — self._regime_tracker is set but never called.

---

### G) ISSUES FOUND

1. **n_lookback comment contradicts code:** The comment says "use a generous 500 to be safe" but the actual calculation is `self._n_weeks * 5 = 260`. The comment is misleading and the actual lookback is 260 trades, not 500.

2. **Step 9 notification reads current_weights AFTER weights have been written:** In Step 6/7, batch_update writes new weights to the signal_weights table. Step 9 then calls store.get_all_weights() — which now returns the UPDATED weights. The comparison `old_w = current_weights.get(sig, 1.0)` uses the new weight as the baseline. The change_pct will almost always be 0% or very small, making the 10% notification threshold effectively never triggered.

3. **os imported but never used:** `import os` is at the top of the file with no reference to `os` anywhere else in the file.

4. **RegimeTracker instantiated but never used:** `self._regime_tracker` is set in __init__ but no method in BatchRetrainer calls it. It is dead weight in this file.

5. **No minimum trade count guard in run():** There is no check on `len(trades)` before calling _compute_signal_stats or batch_update. If the store returns 0 trades (e.g., early in operation), batch_update receives an empty list and returns {} (which it handles gracefully). However, signal_stats will also be empty and the report will contain empty tables with no warning.

6. **_compute_signal_stats uses signals_at_entry keys as raw signal names, not role-aware:** It iterates `for sig in signals` where signals is a parsed JSON dict or list. If signals_at_entry is a dict, it iterates keys. If it is a list of dicts, individual dict items (not strings) silently pass through the `if not isinstance(sig, str): continue` guard — so list-of-dict format signals are silently excluded.

7. **Report generation is decoupled from Step 9 notifications:** The report (Step 8) is generated from weight_changes returned by batch_update. The notifications (Step 9) compare weight_changes against store.get_all_weights() which are already updated. The two steps use different baselines for "old" weights, so the report's "Updated/Unchanged" column and Step 9 notifications are not consistent with each other.

8. **Direct SQL in alpha methods bypasses store abstraction:** `_compute_entry_timing_alpha`, `_compute_peer_alpha`, and `_compute_analyst_alpha` all call `self._store._conn().execute(...)` directly, bypassing any store API. If the store connection method is renamed or if the DB schema changes, these methods break silently (they catch Exception and return 0.0/{}).

---

---

# SECTION 4A — LEARNING SYSTEM ARCHITECTURE

## Intended Data Flow with Status Assessment

```
Trade closes
│
│ [TRIGGER: NEVER CONNECTED as confirmed within GROUP 6]
│ No call to TradeAutopsy.run() is present in trade_autopsy.py itself.
│ The docstring states it "runs automatically every time a position closes"
│ but the exact caller and call site are not visible in these four files.
│
▼
TradeAutopsy.run(closed_trade, entry_context)
  STATUS: PARTIALLY WORKING
  Reason: The method is correctly written and internally consistent.
  However, whether it is ever actually called on trade close is
  unverifiable from GROUP 6 files. All nine sub-modules are
  imported with try/except — if any import fails, that sub-module
  is silently skipped.

  Step-by-step:
  1. Extracts ticker, net_pnl, entry_price, position_size from closed_trade
  2. Computes pnl_pct = net_pnl / (entry_price * position_size)
  3. Sets was_profitable = net_pnl > 0
  4. Extracts macro_regime from entry_context
  5. Initialises AutopsyReport with trade_id=-1
  6. Step 1: calls store.record_trade() → gets trade_id (blocks all
     subsequent steps if store is None)
  7. Step 2: calls PnLAttributor.attribute() only if trade_id >= 0
  8. Step 3: calls RegimeTracker.update(macro_regime, attribution)
  9. Step 4: calls SignalInteractionLedger.update() only if
     active_signals is non-empty
  10. Step 5: calls WeightUpdater.soft_update() only if attribution
      is non-empty (depends on Step 2 succeeding)
  11. Steps 6-12: downstream modules called with per-step guards
│
▼
PnLAttributor.attribute(trade_id, closed_trade, entry_context)
  STATUS: PARTIALLY WORKING
  What it does:
  - For each signal in entry_context["active_signals"]:
    - Determines counterfactual P&L based on signal role
    - attributed_pnl = actual_pnl - counterfactual_pnl
    - was_correct = (direction * actual_return) > 0
    - Appends attr_record to attributions list
    - Updates in-memory _scorecard_cache
  - Calls store.record_attribution(trade_id, attributions)

  Where it writes:
  - DB table: unknown name (store.record_attribution)
  - In-memory: PnLAttributor._scorecard_cache (ephemeral, lost on restart)

  Why partially working:
  - In-memory scorecard cache does not survive process restart
  - was_correct is trade-level, not signal-level
  - early_exit counterfactual default is fabricated (actual_pnl * 1.1)
  - Sharpe in get_scorecard() always computes from in-memory series,
    so is always 0.0 on a fresh instance
│
▼
WeightUpdater.soft_update(attribution, entry_context)
  STATUS: PARTIALLY WORKING

  Called by TradeAutopsy Step 5 — ONLY if attribution is non-empty.
  Attribution is only non-empty if Step 2 (PnLAttributor) succeeded
  AND trade_id >= 0.

  For each signal in attribution:
  - Fetches n_trades from store.get_signal_scorecard()
  - GATE: skips if n_trades < min_trades (default 10)
  - Fetches current_weight from store.get_signal_weight()
  - gradient = attributed_pnl * 0.02
  - raw_new = current_weight * (1 + gradient)
  - Enforces ±15% max change per update
  - Clips to [0.05, 3.0]
  - Writes to signal_weights table via store.set_signal_weight()
    with sharpe=0.0 (hardcoded — ISSUE)

  Why partially working:
  - Depends on attribution being non-empty (two upstream failure points)
  - n_trades gate skips all signals with fewer than 10 trades
  - Persists sharpe_basis=0.0 always for soft updates
  - entry_context argument accepted but never used
│
▼
[Updated weights land in DB table: signal_weights]
  STATUS: WRITTEN BUT READ PATH TO paper_trader._process() UNKNOWN

  signal_weights table columns confirmed:
    signal_name, weight, previous_weight, n_trades_basis, sharpe_basis

  How paper_trader._process() reads these weights:
    NOT VISIBLE IN GROUP 6.
    WeightUpdater.get_weight_summary() reads signal_weights directly.
    Whether paper_trader calls store.get_signal_weight() per signal,
    or reads the whole table, or reads weights from a config dict,
    is not determinable from these four files.
│
▼
BatchRetrainer.run()
  STATUS: NEVER CONNECTED (no scheduler visible in GROUP 6)

  When it fires: should_run_today() checks if today is Sunday.
  But nothing in GROUP 6 calls should_run_today() or run().
  An external scheduler (cron, APScheduler, etc.) must call it.
  Whether that scheduler exists is not visible here.

  What conditions are required:
  - store must be not None (soft fail otherwise)
  - self._weight_updater must be not None (soft fail otherwise)
  - trades list must be non-empty (soft fail if empty)
  - No explicit minimum trade count check in run() itself

  What it does:
  - Pulls last 260 trades (52 weeks * 5)
  - Computes per-signal stats, entry alpha, peer alpha, analyst alpha
  - Calls WeightUpdater.batch_update(trades) for portfolio optimisation
  - batch_update tries cvxpy mean-variance; falls back to proportional Sharpe
  - Applies 70/30 smoothing: 70% current + 30% optimal
  - Writes new weights to signal_weights table
  - Saves text report to output/weight_updates/retrain_{ts}.txt
  - Logs WARNING for changes > 10% (but comparison is broken — see Issues)
│
▼
[How updated weights reach paper_trader._process()]
  STATUS: UNKNOWN — NOT VISIBLE IN GROUP 6

  The signal_weights table is written by both soft_update and batch_update.
  paper_trader._process() presumably reads signal weights at decision time,
  but the read path is not present in any of the four GROUP 6 files.
```

---

# SECTION 4B — RETRAINING THRESHOLDS AND GUARDS

From `batch_retrainer.py` exactly as in code:

**Minimum trade count:**
- Variable name: there is NO explicit minimum trade count variable in BatchRetrainer
- The check for minimum trades lives in WeightUpdater.__init__, not BatchRetrainer:
  `self._min_trades: int = self._cfg.get("min_trades_before_weight_update", 10)`
  (this controls soft_update only, not batch_update)
- In BatchRetrainer.run(), there is no minimum trade count guard before calling batch_update
- The only implicit gate is: `if self._weight_updater is not None and trades:` (line 161)
  — trades must be a non-empty list; a single trade qualifies

**Minimum days since last retrain:**
- Variable name: NONE — no such variable exists in BatchRetrainer
- No check of last retrain date is performed anywhere in the code
- The only timing mechanism is `should_run_today()` which checks the weekday name

**Minimum win rate requirement:**
- NONE — no minimum win rate check exists in BatchRetrainer.run()

**Minimum data quality requirement:**
- NONE — no data quality checks exist

**Other guards:**
- `self._weight_updater is not None` — if WeightUpdater failed to import or init, batch_update is skipped
- `trades` is truthy — if store returns empty list, batch_update is skipped
- Each of the 9 steps in run() is individually try/except wrapped

**Exact code for the only guard that controls batch_update execution (lines 161-166):**
```python
if self._weight_updater is not None and trades:
    weight_changes = self._weight_updater.batch_update(trades)
    result["signals_updated"] = len(weight_changes)
else:
    logger.warning("BatchRetrainer: WeightUpdater unavailable or no trades")
```

**What happens if conditions not met:**
- If store is None: logs "BatchRetrainer: no store available", trades = [], continues with all steps returning empty/zero values, saves a report with no data
- If trades is empty: logs "BatchRetrainer: WeightUpdater unavailable or no trades", weight_changes = {}, report shows no weight changes
- If WeightUpdater is None: same as empty trades path
- No exception is raised in any case; run() always returns a result dict

---

# SECTION 4C — SIGNAL WEIGHTS: WHERE ARE THEY?

**Where signal weights are stored:**
- DB table: `signal_weights`
- Confirmed by direct SQL in WeightUpdater.get_weight_summary() (lines 362-368):
  ```python
  SELECT sw.signal_name, sw.weight, sw.previous_weight, sw.n_trades_basis, sw.sharpe_basis
  FROM signal_weights sw
  ORDER BY sw.weight DESC
  ```
- Columns confirmed: signal_name, weight, previous_weight, n_trades_basis, sharpe_basis
- Additional columns likely exist (updated_at, reason, auto flag) based on store.set_signal_weight() call signature

**Default weight for each signal:**
- When store.get_signal_weight(signal_name, default=1.0) is called and no record exists: default = 1.0
- In batch_update: `cur = current_weights.get(s, 1.0)` — missing signals default to 1.0
- No signal-specific defaults exist; all signals default to weight 1.0

**Whether any signal weights have ever been updated:**
- Not determinable from GROUP 6 files alone. The signal_weights table is written by:
  - WeightUpdater.soft_update() — fires after each trade with trade_id >= 0, attribution non-empty, and n_trades >= 10
  - WeightUpdater.batch_update() — fires if BatchRetrainer.run() is called
  - WeightUpdater.apply_stress_caps() — fires if called externally (no caller visible)
  - WeightUpdater.apply_crisis_reduction() — fires if called externally (no caller visible)
- If the system has fewer than 10 completed trades per signal, soft_update has never written any weights
- If no scheduler calls BatchRetrainer, batch_update has never written any weights
- The table may be entirely empty

**How paper_trader._process() reads signal weights:**
- NOT VISIBLE IN GROUP 6. The read path from signal_weights to paper_trader._process() is not present in any of the four files documented here.

**What happens if signal_weights table is empty:**
- In soft_update: store.get_signal_weight() returns the default of 1.0 for all signals; updates proceed from weight=1.0
- In batch_update: store.get_all_weights() returns {}; cur = current_weights.get(s, 1.0) = 1.0 for all signals; blended = 0.70 * 1.0 + 0.30 * optimal[s]; weights are still calculated and written
- In get_weight_summary(): returns "No signal weights recorded yet.\n"
- For paper_trader._process(): behavior unknown (read path not in GROUP 6)

---

# SECTION 4D — WEIGHT UPDATER LOGIC

From `weight_updater.py` exactly as coded:

**Exact algorithm used (soft_update):**
1. For each signal in attribution list:
   a. Query store for n_trades via get_signal_scorecard
   b. Gate: if n_trades < min_trades (default 10), skip
   c. Fetch current_weight from store (default 1.0 if unavailable)
   d. gradient = attributed_pnl * soft_update_rate (default 0.02)
   e. raw_new = current_weight * (1 + gradient)
   f. If abs(raw_new - current_weight) > current_weight * max_change (default 0.15): clip to ±15%
   g. new_weight = max(0.05, min(3.0, raw_new))
   h. If abs(new_weight - current_weight) < 1e-9: skip (no meaningful change)
   i. Persist to signal_weights with sharpe=0.0 (hardcoded)

**Exact algorithm used (batch_update):**
1. Parse signals_at_entry from each trade (JSON); build signal_pnl dict
2. Compute per-signal Sharpe ratios
3. Fetch current weights from store
4. If cvxpy + numpy available AND >= 2 signals:
   - Build pnl_matrix (padded with leading zeros)
   - Compute mu (mean per signal), cov (covariance matrix)
   - Solve: Maximize mu @ w - 0.5 * 2.0 * quad_form(w, cov)
     subject to: w >= 0.05, w <= 3.0
   - Solver: ECOS with warm_start=True
5. Fallback if cvxpy unavailable or fails:
   - positive_sharpes = max(0, Sharpe) per signal
   - optimal[s] = clip((positive_sharpes[s] / total) * n_signals, 0.05, 3.0)
6. Blend: new_weight = 0.70 * current + 0.30 * optimal (clipped to [0.05, 3.0])
7. Persist to signal_weights table

**Input data required:**
- soft_update: attribution list (from PnLAttributor.attribute()), store access for n_trades and current_weight
- batch_update: list of trade dicts with signals_at_entry (JSON) and net_pnl fields

**Output produced:**
- soft_update: List[Dict] of {signal_name, old_weight, new_weight, reason}; weights written to signal_weights table
- batch_update: Dict of {signal_name: new_weight}; weights written to signal_weights table

**Minimum rows of pnl_attribution needed to fire:**
- soft_update: no minimum on pnl_attribution rows; minimum is per-SIGNAL n_trades >= 10 (checked via store scorecard, not from attribution length)
- batch_update: no minimum — fires with 1 trade if signal_pnl is non-empty

**Mathematical formula for weight update:**

SOFT UPDATE:
```
gradient       = attributed_pnl × soft_update_rate
raw_new        = current_weight × (1 + gradient)
max_delta      = current_weight × max_weight_change_per_update
if |raw_new - current_weight| > max_delta:
    raw_new    = current_weight + sign(raw_new - current_weight) × max_delta
new_weight     = clip(raw_new, w_min, w_max)
```
Default parameter values: soft_update_rate=0.02, max_weight_change_per_update=0.15, w_min=0.05, w_max=3.0

BATCH UPDATE (cvxpy path):
```
Maximize: μᵀw − ½ × 2.0 × wᵀΣw
Subject to: 0.05 ≤ wᵢ ≤ 3.0 for all i
where μ = column means of pnl_matrix
      Σ = covariance matrix of pnl_matrix
```
BATCH UPDATE (Sharpe fallback):
```
optimal[s] = clip((max(0, Sharpe[s]) / Σ max(0, Sharpe)) × N_signals, 0.05, 3.0)
```
BATCH UPDATE (smoothing applied to both paths):
```
new_weight[s] = clip(0.70 × current[s] + 0.30 × optimal[s], 0.05, 3.0)
```

---

# SECTION 4 GATE

**Files read:**
1. /home/dannyelticala/quant-fund/closeloop/autopsy/trade_autopsy.py — read completely (352 lines)
2. /home/dannyelticala/quant-fund/closeloop/autopsy/pnl_attributor.py — read completely (241 lines)
3. /home/dannyelticala/quant-fund/closeloop/learning/weight_updater.py — read completely (388 lines)
4. /home/dannyelticala/quant-fund/closeloop/learning/batch_retrainer.py — read completely (445 lines)

**Key findings:**
1. TradeAutopsy.run() has no visible caller within GROUP 6; the trigger from trade close to autopsy is unverified
2. All nine sub-modules in TradeAutopsy are imported with silent try/except — any import failure silently disables that module
3. Attribution (Step 2) depends on trade_id >= 0 which depends on store.record_trade() (Step 1) succeeding; a store=None condition silently blocks attribution and all weight updates
4. WeightUpdater.soft_update() hardcodes sharpe=0.0 when persisting — signal_weights table has corrupt sharpe_basis for all soft-update rows
5. PnLAttributor._scorecard_cache is in-memory and ephemeral; on any process restart, Sharpe in get_scorecard() returns 0.0 regardless of trade history
6. get_scorecard() recommendation system is permanently suppressed on fresh instances (sharpe=0.0 never reaches INCREASE/DECREASE thresholds)
7. BatchRetrainer has no minimum trade count, no minimum win rate, no minimum data quality guards — it will run on 1 trade
8. BatchRetrainer Step 9 notification reads current_weights AFTER they have been updated; change_pct will almost always be ~0% making the 10% threshold effectively never triggered
9. BatchRetrainer `os` import is unused
10. BatchRetrainer self._regime_tracker is instantiated but never called in any method
11. BatchRetrainer n_lookback comment says 500 but code calculates 260 (52 * 5)
12. batch_update padding with leading zeros in pnl_matrix distorts covariance estimates for signals with short history
13. batch_update assigns full trade net_pnl to each signal (not attributed P&L) — inflating P&L estimates for all signals
14. apply_stress_caps() and apply_crisis_reduction() have no visible callers in GROUP 6 — dead code
15. WeightUpdater.get_weight_summary() accesses store._conn() directly, bypassing store abstraction
16. BatchRetrainer alpha methods (_compute_entry_timing_alpha, _compute_peer_alpha, _compute_analyst_alpha) access store._conn() directly
17. How paper_trader._process() reads signal weights is not visible in any GROUP 6 file
18. signal_weights table may be entirely empty if fewer than 10 trades per signal have closed and BatchRetrainer has not been called by a scheduler
19. early_exit role counterfactual defaults to actual_pnl * 1.1 (fabricated — 10% higher assumed hold P&L)
20. was_correct in attribution is trade-level, not signal-level — all signals in a trade share identical was_correct value
21. WeightUpdater.soft_update() accepts entry_context argument but never uses it

**Contradictions found:**
1. BatchRetrainer comment "use a generous 500 to be safe" vs actual calculation producing 260
2. get_scorecard() uses store data for n/total_pnl/mean_pnl/win_rate but always computes Sharpe from in-memory cache — the two data sources are inconsistent; n could be 500 (store) while Sharpe is 0.0 (empty cache)
3. batch_update Step 9 notification purports to find changes > 10% but reads post-update weights as the baseline, making change_pct ≈ 0% always
4. trade_autopsy.py docstring says "runs automatically every time a position closes" but no auto-trigger or scheduler is present in the file
5. WeightUpdater.apply_stress_caps() docstring mentions "If correlation_regime == CRISIS: reduce all weights by 30%" — this is a separate method (apply_crisis_reduction) with no call link between correlation_regime detection and the method

**Formulas extracted:**
1. pnl_pct = net_pnl / (entry_price * position_size) [trade_autopsy.py]
2. actual_return = actual_pnl / (entry_price * position_size) [pnl_attributor.py]
3. was_correct = (direction * actual_return) > 0 [pnl_attributor.py]
4. counterfactual_pnl [primary_trigger] = 0.0 [pnl_attributor.py]
5. counterfactual_pnl [size_boost] = actual_pnl * 0.80 [pnl_attributor.py]
6. counterfactual_pnl [early_exit] = planned_hold_pnl (default: actual_pnl * 1.1) [pnl_attributor.py]
7. weight_fraction = strength / total_strength [pnl_attributor.py]
8. counterfactual_pnl [secondary] = actual_pnl * (1 - weight_fraction) [pnl_attributor.py]
9. attributed_pnl = actual_pnl - counterfactual_pnl [pnl_attributor.py]
10. attributed_pnl_pct = attributed_pnl / (entry_price * position_size) [pnl_attributor.py]
11. entry_alpha = actual_pnl - immediate_open_pnl [pnl_attributor.py]
12. annualised Sharpe = (mean / std) * sqrt(252), ddof=1, zero risk-free rate [all four files]
13. sample std = sqrt(sum((v - mean)^2) / (n-1)) [pnl_attributor.py, weight_updater.py, batch_retrainer.py]
14. scorecard recommendation: n>=10 and sharpe>1.0 and win_rate>0.55 → INCREASE; n>=20 and sharpe<0 and win_rate<0.40 → INVESTIGATE; n>=20 and sharpe<-0.5 → DECREASE; else MAINTAIN [pnl_attributor.py]
15. soft_update gradient = attributed_pnl * soft_update_rate [weight_updater.py]
16. soft_update raw_new = current_weight * (1 + gradient) [weight_updater.py]
17. soft_update max_delta = current_weight * max_weight_change_per_update [weight_updater.py]
18. soft_update new_weight = clip(raw_new, w_min, w_max) = clip(raw_new, 0.05, 3.0) [weight_updater.py]
19. batch_update cvxpy objective: Maximize mu@w - 0.5 * 2.0 * quad_form(w, cov), w in [0.05, 3.0] [weight_updater.py]
20. batch_update Sharpe fallback: optimal[s] = clip((max(0,Sharpe[s]) / total) * N_signals, 0.05, 3.0) [weight_updater.py]
21. batch_update smoothing: new_weight = clip(0.70 * current + 0.30 * optimal, 0.05, 3.0) [weight_updater.py]
22. stress cap: capped = max(w_min, current * 0.5) when vix > 25 [weight_updater.py]
23. crisis reduction: reduced = max(w_min, current * 0.70) [weight_updater.py]
24. significant change pct = abs(new - old) / max(abs(old), 1e-9) * 100 [weight_updater.py, batch_retrainer.py]
25. n_lookback = n_weeks * 5 (default = 260) [batch_retrainer.py]
26. signal mean_pnl = sum(pnl_list) / n [batch_retrainer.py]
27. signal win_rate = signal_wins / n [batch_retrainer.py]
28. peer accuracy = AVG(CASE WHEN was_correct=1 THEN 1.0 ELSE 0.0 END) via SQL [batch_retrainer.py]
29. weight summary change% = (cur - prev) / max(abs(prev), 1e-9) * 100 [weight_updater.py]

**Data flows documented:**
1. closed_trade dict + entry_context dict → TradeAutopsy.run() → AutopsyReport
2. TradeAutopsy → store.record_trade() → DB (table unknown)
3. TradeAutopsy → PnLAttributor.attribute() → attributions list → store.record_attribution() → DB (table unknown)
4. attributions list → TradeAutopsy Step 5 → WeightUpdater.soft_update() → signal_weights table
5. WeightUpdater.soft_update() reads: signal_weights (n_trades via scorecard, current weight)
6. WeightUpdater.soft_update() writes: signal_weights (signal_name, new_weight, reason, n_trades, sharpe=0.0)
7. BatchRetrainer.run() reads: store.get_trades() → trade list
8. BatchRetrainer._compute_entry_timing_alpha() reads: entry_timing_outcomes.pnl_vs_immediate_entry
9. BatchRetrainer._compute_peer_alpha() reads: peer_influence_outcomes (was_correct, pnl)
10. BatchRetrainer._compute_analyst_alpha() reads: analyst_revision_outcomes (forward_return_5d, forward_return_20d, pnl_if_traded, pead_improved)
11. BatchRetrainer → WeightUpdater.batch_update(trades) → signal_weights table (written)
12. BatchRetrainer → file system write: output/weight_updates/retrain_{ts}.txt
13. TradeAutopsy Step 11 → store.record_peer_influence() → DB (peer_influence table)
14. signal_weights table → [unknown read path] → paper_trader._process()

**Proceed to GROUP 7: YES**
