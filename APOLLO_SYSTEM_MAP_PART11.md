# APOLLO SYSTEM MAP — PART 11
## GROUP 11 GATE: Module Dependency Graph, Dead Code Map, Circular Dependency Check

**Generated:** 2026-04-08
**Instruction:** DOCUMENT ONLY. Nothing was fixed or changed.
**Coverage:** All .py files across the entire quant-fund/ codebase documented across Parts 1–11C.

---

## GROUP 11 GATE

### Summary Statistics

| Group | Files | Part(s) |
|-------|-------|---------|
| main.py + config + market calendar | 5 | Part 1 |
| closeloop/ core + store + autopsies | ~35 | Part 2–3 |
| altdata/ + learning/ + notifications/ | ~30 | Part 4–5 |
| data/collectors/ | ~15 | Part 6 |
| deepdata/ (partial) | ~20 | Part 7A–7B |
| analysis/ | ~10 | Part 8 |
| backtest/ + simulations/ | ~10 | Part 9 |
| additional altdata + closeloop | ~15 | Part 10 |
| analysis/ (full) + altdata/ (full) | 52 | Part 11A |
| closeloop/ + deepdata/ + backtest/ + archive/ | 60 | Part 11B |
| core/ + data/ + intelligence/ + signals/ + execution/ + risk/ + monitoring/ + frontier/ | 95 | Part 11C |

**Total estimated distinct files documented: ~300+**

---

## SECTION 11A: Module Dependency Graph

This graph shows import relationships between major subsystems. Arrows indicate "imports from" (`A → B` means A imports B).

```
main.py
  → intelligence/daily_pipeline.py
  → intelligence/automation_scheduler.py
  → execution/trading_bot.py
  → execution/paper_trader.py
  → execution/broker_interface.py
  → execution/adaptive_position_sizer.py
  → execution/cooling_off_tracker.py
  → execution/alpaca_stream.py
  → monitoring/monitor_runner.py
  → monitoring/health_dashboard.py
  → monitoring/server_monitor.py
  → monitoring/private_bot.py
  → monitoring/realtime_monitor.py
  → monitoring/preflight_check.py
  → closeloop/* (store, learning, risk)
  → altdata/* (collector, learning, notifications)
  → data/* (collectors, universe, earnings)
  → analysis/* (crowding, pairs, regime, PEAD)
  → signals/* (all signal generators)
  → risk/manager.py
  → frontier/* (engine, store, sizer)
  → deepdata/* (liquidity, congressional, options)

execution/trading_bot.py
  → execution/paper_trader.py
  → execution/broker_interface.py
  → execution/adaptive_position_sizer.py
  → execution/cooling_off_tracker.py
  → execution/alpaca_stream.py
  → monitoring/monitor_runner.py
  → intelligence/daily_pipeline.py
  → signals/*

execution/paper_trader.py
  → analysis/market_calendar.py
  → execution/broker_interface.py
  → risk/manager.py
  → signals/*
  → closeloop/store/closeloop_store.py
  → deepdata/microstructure/liquidity_scorer.py

execution/adaptive_position_sizer.py
  → frontier/signals/frontier_signal_engine.py  [UMCI]
  → altdata/reasoning/symbolic_regression.py    [milestones]
  → output/permanent_archive.db                 [DB — WRONG PATH]

risk/manager.py
  → deepdata/microstructure/liquidity_scorer.py
  → [pandas, numpy]

signals/calendar_effects_signal.py
  → output/permanent_archive.db  [DB — WRONG PATH vs permanent_store.py]

signals/sector_rotation_signal.py
  → output/historical_db.db

signals/gap_signal.py
  → [yfinance]

signals/momentum_signal.py
  → [pandas]

signals/mean_reversion_signal.py
  → [pandas]

signals/options_earnings_signal.py
  → [yfinance, pandas, numpy]

signals/pead_signal.py
  → [pandas]

signals/insider_momentum_signal.py
  → [requests, pandas]

signals/anomaly_scanner.py
  → altdata/*
  → analysis/*

signals/signal_registry.py
  → [no internal imports — pure registry]

signals/signal_validator.py
  → [pandas, numpy, scipy]

intelligence/automation_scheduler.py
  → intelligence/daily_pipeline.py
  → execution/paper_trader.py
  → core/retraining_controller.py
  → [schedule, subprocess]

intelligence/daily_pipeline.py
  → data/*
  → signals/*
  → altdata/*
  → main [DYNAMIC IMPORT — tight coupling]
  → [sqlite3 direct — multiple DBs]

core/retraining_controller.py
  → closeloop/storage/closeloop.db  [DB direct]
  → simulations/shadow.db           [DB direct]
  → altdata/models/                 [file I/O]

core/scan_scheduler.py
  → [asyncio only — no internal imports]

core/async_infrastructure.py
  → [asyncio, aiohttp — no internal imports]

monitoring/monitor_runner.py
  → monitoring/alert_monitor.py
  → monitoring/telegram_logger.py
  → monitoring/health_reporter.py
  → monitoring/self_diagnostic.py

monitoring/health_reporter.py
  → monitoring/telegram_logger.py
  → monitoring/system_stats.py

monitoring/alert_monitor.py
  → monitoring/system_stats.py
  → monitoring/telegram_logger.py

monitoring/self_diagnostic.py
  → monitoring/system_stats.py
  → monitoring/telegram_logger.py

monitoring/health_dashboard.py
  → paper_trader [optional injection]
  → regime_detector [optional injection]
  → frontier/storage/frontier_store.py [optional injection]

monitoring/chart_generator.py
  → [matplotlib, numpy, yaml, sqlite3 — no internal imports]

monitoring/private_bot.py
  → [urllib.request, sqlite3, yaml — no internal imports]
  → monitoring/chart_generator.py  [via _cmd_report]

monitoring/dashboard/app.py
  → [flask, sqlite3, yaml — no internal imports]

monitoring/realtime_monitor.py
  → altdata/*
  → closeloop/store/permanent_store.py
  → [yfinance, requests, schedule]

monitoring/external_source_monitor.py
  → execution/feature_manager.py

monitoring/server_monitor.py
  → [gc, shutil — no internal imports]
  → Alpaca API [injected]

frontier/signals/frontier_signal_engine.py
  → frontier/physical/*  (6 collectors)
  → frontier/social/*    (6 collectors)
  → frontier/scientific/* (5 collectors)
  → frontier/financial_frontier/* (3 collectors)
  → frontier/equations/unified_complexity_index.py
  → frontier/storage/frontier_store.py

frontier/equations/derived_formulas.py
  → [math, numpy, pandas — no internal imports]

frontier/equations/cross_signal_interactions.py
  → [math — no internal imports]

frontier/equations/frontier_signal_purity.py
  → frontier/equations/derived_formulas.py

frontier/equations/unified_complexity_index.py
  → frontier/equations/derived_formulas.py  [GRAI, ASI formulas]

frontier/validation/frontier_validator.py
  → frontier/validation/evidence_tracker.py

frontier/validation/evidence_tracker.py
  → frontier/sizing/frontier_sizer.py  [check_tier_promotion]

frontier/sizing/frontier_sizer.py
  → frontier/storage/frontier_store.py  [optional]

frontier/meta_learning/parameter_drifter.py
  → frontier/storage/frontier_store.py  [optional injection]

frontier/meta_learning/correlation_discoverer.py
  → [numpy, pandas — no internal imports]

frontier/meta_learning/discovery_registry.py
  → [json, pathlib — no internal imports]

frontier/meta_learning/watchlist_manager.py
  → frontier/storage/frontier_store.py
  → frontier/meta_learning/discovery_registry.py

frontier/dashboard/frontier_dashboard.py
  → frontier/signals/frontier_signal_engine.py
  → frontier/storage/frontier_store.py
  → frontier/meta_learning/parameter_drifter.py
  → frontier/meta_learning/discovery_registry.py

data/large_cap_influence.py
  → [yfinance, pandas, sqlite3 — no internal imports]

data/universe_builder.py
  → [requests, pandas, bs4 — no internal imports]

data/earnings_collector.py
  → altdata/*  [sentiment enrichment]
  → intelligence/*  [intelligence update]

data/historical_collector.py
  → data/earnings_collector.py
  → altdata/*
  → data/db_utils.py

closeloop/store/closeloop_store.py
  → [sqlite3 — no complex internal imports]

closeloop/store/permanent_store.py
  → [sqlite3]

altdata/notifications/notifier.py
  → monitoring/telegram_logger.py
```

---

## SECTION 11B: Dead Code Map

The following code is present in the codebase but has no live execution path or is definitively unreachable:

### 1. Never-Executed Training Code
**File:** `core/retraining_controller.py` — `_initiate_shadow_training()`
- Registers a record in shadow.db but contains no actual training logic
- Comment: "training deferred until altdata store populated with sufficient data"
- ML models are never trained in production without manual invocation of `altdata/learning/weekly_retrainer.py`

### 2. Commented-Out Weekly Report Job
**File:** `intelligence/automation_scheduler.py` — weekly report job block
- 6 lines of commented-out code for `schedule.every().sunday.at("03:00").do(weekly_report)`
- Comment: "DISABLED: Weekly report de-duplicated — now only in trading_bot.py"

**File:** `monitoring/monitor_runner.py` — weekly report scheduling block
- 6 lines of commented-out code
- Comment: "DISABLED: Weekly report now sent exclusively from trading_bot.py Sunday 09:00 UTC"

### 3. Stub Methods That Always Return Constants
**File:** `analysis/crowding_detector.py` — `_get_factor_dispersion(ticker)`
- Always returns 0.5; factor dispersion is never computed
- Contributes 0.20 × 0.5 = 0.10 permanently to every CRI calculation

### 4. Unreachable Variable Assignment
**File:** `data/earnings_db.py` — `update_snapshot_outcome()`
- First assignment to `now_str` immediately overwritten by second assignment on next line
- First computed value is dead

### 5. Unused Class Attributes
**File:** `core/scan_scheduler.py` — `_scan_count` and `_last_full_cycle`
- Initialised in `__init__` but never incremented or read
- No code path updates these counters

### 6. Shadow DB Schema Dependency
**File:** `core/retraining_controller.py` — writes to `simulations/shadow.db`
- Tables `retraining_events` and `model_registry` must pre-exist
- No `_init_db()` call; first `log_retraining_event()` call will fail silently if DB/table absent

### 7. Milestone Discovery Trigger (Effectively Dead Until PHASE_FREE)
**File:** `execution/adaptive_position_sizer.py` — `auto_trigger_discovery()`
- Only fires at trade counts 200, 400, 600, 800, 1000, 1500, 2000
- Requires SymbolicRegressionEngine to be importable and functional
- In practice, at <500 trades (current likely state), only the 200-trade milestone could fire

### 8. Never-Matched Milestone Strings
**File:** `frontier/scientific/quantum_readiness_tracker.py`
- `QUANTUM_MILESTONES` in `derived_formulas.py` has keys like `"100_logical_qubit"`, `"error_correction_at_scale"` etc.
- The tracker checks against hardcoded strings that may not match the dict keys exactly
- `milestone_score` computed in `calc_qtpi()` will always be 0.0 in practice

### 9. Cross-Signal Interactions With No Input Data
**File:** `frontier/equations/cross_signal_interactions.py`
- `schumann_reddit_lunar_combo()`: `lunar_phase_angle_rad` has no collector
- `church_congress_hiring_combo()`: `congressional_signal_strength` and `hiring_momentum` have no collectors
- These three interaction functions always receive 0.0 for the missing inputs → always return 0.0

### 10. AltDataComplexity Components With No Collectors
**File:** `frontier/equations/unified_complexity_index.py` — `AltDataComplexity` class
- `shipping_index` component: ShippingIntelligence data not piped to UMCI
- `wikipedia_trend` component: no Wikipedia collector registered in FrontierSignalEngine
- Both always contribute 0.0 to the Social Complexity dimension

### 11. Disconnected Proxy Collectors
**File:** `frontier/physical/schumann_collector.py`
- Returns NOAA solar wind speed data labeled as "Schumann resonance"
- Schumann resonance ≠ solar wind speed; this data is fundamentally mislabeled

**File:** `frontier/physical/satellite_imagery_collector.py`
- Returns ETF volume data labeled as "satellite imagery activity"
- No actual satellite imagery processing occurs

---

## SECTION 11C: Circular Dependency Check

No hard circular imports detected at the module level. The following near-circularity concerns exist:

### 1. dynamic import in daily_pipeline.py → main.py
**Pattern:** `intelligence/daily_pipeline.py._take_pre_earnings_snapshot()` dynamically imports `main`
- This creates a runtime dependency on the top-level script
- Not a compile-time circular import but a tight runtime coupling
- If `daily_pipeline.py` is imported at startup before `main.py` is fully initialised, the dynamic import will find a partially-constructed module

### 2. evidence_tracker.py ↔ frontier_sizer.py
**File:** `frontier/validation/evidence_tracker.py.check_promotion()` imports from `frontier/sizing/frontier_sizer.py`
**File:** `frontier/sizing/frontier_sizer.py` imports from `frontier/storage/frontier_store.py`
- `frontier_sizer.py` does NOT import `evidence_tracker.py`
- No circular dependency; unidirectional: evidence_tracker → frontier_sizer → frontier_store

### 3. Notifier → telegram_logger → (no back-import)
**Pattern:** `altdata/notifications/notifier.py` → `monitoring/telegram_logger.py`
- `telegram_logger.py` has no imports from `altdata/`
- No circular dependency

### 4. Monitoring modules import from altdata/data
**Pattern:** `monitoring/health_reporter.py` imports collector classes via `importlib.import_module()`
- Dynamic importlib usage avoids compile-time circular imports
- No circular dependency

### 5. Confirmed Clean Boundaries
The following subsystem boundaries are clean (no cross-imports in either direction):
- `core/` ← nothing from `signals/`, `execution/`, `monitoring/` imports into core
- `frontier/equations/` ← nothing from `frontier/physical/`, `frontier/social/` etc. imports into derived_formulas.py
- `risk/manager.py` ← no module that risk/ imports from imports risk/ back
- `signals/*` individual signal files ← no signal file imports another signal file

---

## SYSTEM-WIDE CRITICAL ISSUES SUMMARY

This section consolidates the most significant issues found across ALL parts of the system map.

### Priority 1: Path Inconsistency (Silent Data Loss)
Multiple modules write to `output/permanent_archive.db` while `permanent_store.py` writes to `output/permanent_log.db`. Affected modules:
- `execution/adaptive_position_sizer.py` (phase history)
- `signals/calendar_effects_signal.py` (calendar signals table)
- `monitoring/self_diagnostic.py` (DB accessibility check)
- `monitoring/preflight_check.py` (reads from this path)

These modules are reading and writing to a DB that `permanent_store.py` never touches. Phase history, calendar signals, and preflight check results are stored in an unread database.

### Priority 2: Holiday Tables Expire 2026
- `execution/trading_bot.py`: `_US_HOLIDAYS_2026`, `_UK_HOLIDAYS_2026`
- `signals/calendar_effects_signal.py`: FOMC dates for 2026 only
- `analysis/market_calendar.py` (from prior sessions): 2026-only holiday tables
- All will silently fail to detect holidays from January 2027

### Priority 3: Training Never Executes
- `core/retraining_controller.py`: `_initiate_shadow_training()` registers a DB record but runs no training code
- ML models are static until manual invocation of `altdata/learning/weekly_retrainer.py`
- The dormancy gate (≥500 trades, ≥30 days) has never been reached — retraining controller always returns dormant

### Priority 4: sector_rotation_signal.py Operator Precedence Bug
- `rotating_in = list(current_top5 - old_top5 & old_bot5)` — `&` binds before `-`
- Actual computation: `current_top5 - (old_top5 & old_bot5)`
- Correct intent: `(current_top5 - old_top5) & old_bot5`
- This bug means `detect_rotation()` never correctly identifies sectors rotating from bottom-5 into top-5

### Priority 5: insider_momentum_signal.py Direction Blindness
- All Form 4 filings (insider buying AND selling) generate LONG signals
- Insider selling is a bearish signal but is treated as bullish
- The SEC EDGAR full-text search may also return false positives from other companies' filings mentioning the ticker

---

*END OF APOLLO_SYSTEM_MAP_PART11.md*
