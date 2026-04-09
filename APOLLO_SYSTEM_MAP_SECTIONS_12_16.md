# APOLLO SYSTEM MAP — SECTIONS 12–16
## Final Analysis, Issue Register, and Recommended Fix Sequence

**Generated:** 2026-04-08
**Instruction:** DOCUMENT ONLY. Nothing was fixed or changed.
**Scope:** Synthesised from all Parts 1–11C of the master document (~300 files, ~24,000 lines of source documentation).

---

# SECTION 12: CONTRADICTION AND INCONSISTENCY REGISTER

## 12A: Key / Interface Mismatches

These are cases where two components expect a different interface, key name, data type, or calling convention from each other.

### 12A-1: RiskManager Regime Key Type Mismatch
**Files:** `risk/manager.py`, `execution/paper_trader.py`
- `_REGIME_MULTIPLIERS` in `risk/manager.py` uses integer keys `{0: ..., 1: ..., 2: ..., 3: ..., 4: ...}`
- `paper_trader.py` passes regime names via a conversion dict (e.g. `"BULL"` → `2`)
- If the conversion dict is missing any regime name or returns a string, the dict lookup silently KeyErrors or returns the default, producing the wrong position multiplier with no warning

### 12A-2: Permanent Archive DB Path Split
**Files:** `closeloop/store/permanent_store.py` writes to `output/permanent_log.db`
**Files that write/read `output/permanent_archive.db`:** `execution/adaptive_position_sizer.py`, `signals/calendar_effects_signal.py`, `monitoring/self_diagnostic.py`, `monitoring/preflight_check.py`, `analysis/frontier_validator.py`
- Two different physical SQLite files masquerade under the name "permanent archive"
- Data written by adaptive_position_sizer (phase history) is never read by permanent_store.py
- Preflight check verifies connectivity to a DB that the learning and monitoring systems do not use
- `analysis/frontier_validator.py` hardcodes `"output/permanent_archive.db"` — should be `permanent_log.db`

### 12A-3: AdaptivePositionSizer DB Path vs. PermanentStore DB Path
**Files:** `execution/adaptive_position_sizer.py` (uses `output/permanent_archive.db`), `closeloop/store/permanent_store.py` (uses `output/permanent_log.db`)
- Phase milestone history is written to the wrong file and is never read by any downstream component
- Specifically documented in GROUP 11C GATE

### 12A-4: CalendarEffectsSignal DB Path
**File:** `signals/calendar_effects_signal.py`
- Calendar signals table written to `output/permanent_archive.db`
- No downstream reader from this path exists; signals are permanently orphaned

### 12A-5: CloseLoop Attribution Engine Bypasses Abstraction
**File:** `closeloop/learning/attribution_engine.py`
- Opens SQLite connections directly to `closeloop.db`, bypassing `ClosedLoopStore`
- If `ClosedLoopStore` changes its schema, `attribution_engine.py` will silently use the old schema or fail

### 12A-6: TaxManager Wrong Method Name
**File:** `closeloop/risk/tax_manager.py` — `record_acquisition()`
- Calls `store.record_tax_disposal()` instead of the acquisition recording method
- Tax acquisition records are stored as disposals — both acquisition and disposal records land in the disposal table

### 12A-7: TIER_SIZE_MULTIPLIERS Divergence
**Files:** `execution/chunked_scanner.py`, `data/universe.py`
- `chunked_scanner.py` TIER_2 multiplier: 0.8; `universe.py` TIER_2 multiplier: 0.70
- `chunked_scanner.py` TIER_3 multiplier: 0.6; `universe.py` TIER_3 multiplier: 0.50
- Universe sizing and scan sizing apply different tier weights to the same universe partitions

### 12A-8: CoolingOffTracker Docstring vs. Implementation
**File:** `execution/cooling_off_tracker.py`
- Docstring specifies "5 trading days" lockout
- Implementation uses calendar days (datetime arithmetic), not trading days
- Actual lockout is shorter than documented during weeks with holidays

### 12A-9: Sector Rotation Signal Operator Precedence Bug
**File:** `signals/sector_rotation_signal.py` — `detect_rotation()`
- Code: `rotating_in = list(current_top5 - old_top5 & old_bot5)`
- Python operator precedence: `&` binds before `-`
- Actual: `current_top5 - (old_top5 & old_bot5)` — sectors currently in top-5 that were NOT in the intersection of old_top5 and old_bot5
- Intended: `(current_top5 - old_top5) & old_bot5` — sectors newly in top-5 that were previously in bottom-5
- The signal never correctly identifies genuine bottom-to-top rotations

### 12A-10: BacktestEngine vs. ExecutionFeasibility ADV Cap
**Files:** `backtest/engine.py` (15% ADV cap), `analysis/execution_feasibility.py` (5% ADV cap)
- Backtests use 3× higher fill cap than the feasibility filter
- Backtest results are optimistically biased for large orders

### 12A-11: Market Impact Model Inconsistency
**Files:** `backtest/engine.py` (IMPACT_K = 0.1), `analysis/execution_feasibility.py` (η = 0.15 primary, η = 0.20 for large caps)
- Two different square-root market impact models with different coefficients
- No documented rationale for the discrepancy

### 12A-12: RSI Implementation Split
**Files:** `analysis/technical_indicators.py` — RSI uses Wilder EWM smoothing
**Files:** `signals/mean_reversion_signal.py` — RSI uses simple rolling mean (SMA)
- Same indicator computed two different ways; signals may diverge on the same ticker

### 12A-13: InsiderMomentum Direction Blindness
**File:** `signals/insider_momentum_signal.py`
- SEC EDGAR Form 4 filings include both insider buys (bullish) and insider sells (bearish)
- Both are processed as LONG/BUY signals
- Net effect: insider selling generates spurious buy signals

### 12A-14: Innovation Signal Weight Discrepancy
**Files:** `frontier/signals/frontier_signal_engine.py` (uses `score` field), `frontier/equations/cross_signal_interactions.py` (uses `calc_innovation_signal()` function)
- Same innovation signal concept computed via two different paths with different weights

### 12A-15: ModuleWirer Docstring vs. Actual Disconnect Count
**File:** (wiring module, from earlier parts)
- Docstring states "12 disconnects"
- Actual count from full system map: 24+ disconnected modules documented

---

## 12B: Assumption Violations

These are cases where code makes an assumption about the environment, data, or dependencies that is demonstrably false or fragile.

### 12B-1: Holiday Tables Valid Only for 2026
**Files:** `execution/trading_bot.py` (`_US_HOLIDAYS_2026`, `_UK_HOLIDAYS_2026`), `signals/calendar_effects_signal.py` (FOMC dates 2026 only), `analysis/market_calendar.py` (all dates hardcoded 2026)
- All holiday/FOMC detection silently breaks on 2027-01-01
- No runtime check warns that tables have expired
- All three files are independently hardcoded; a fix to one does not fix the others

### 12B-2: Earnings Scheduler Fires on UK Weekday Only via UTC
**File:** `data/earnings_scheduler.py`
- Weekday check uses local server time (DigitalOcean VPS in UTC)
- During BST (British Summer Time, UTC+1), a check that triggers at 00:00 UTC on Sunday fires at 01:00 BST Sunday — but may incorrectly classify Saturday 23:00 UTC as a weekday

### 12B-3: MarketTimer Holiday Blindness
**File:** `analysis/market_timer.py`
- `is_open()` returns True on UK public holidays
- No holiday calendar integration in this module
- Signals may be generated or positions taken on closed market days

### 12B-4: Universe Filter Incorrect pence-to-USD Conversion
**File:** `data/universe.py` — `_passes_uk_filters()`
- Divides USD marketCap by 100, assuming pence conversion
- Market cap data from yfinance is already in USD for UK ADRs/listings
- UK stocks listed on LSE have marketCap in GBP pence via some feeds but USD in others
- Division by 100 causes UK stocks to appear 100× smaller than they are, incorrectly filtering them out

### 12B-5: Earnings "Surprise Percent" Stored as Fraction
**File:** `data/earnings_calendar.py`
- Field named `surprisePercent` stored as a fraction (0.05 = 5%)
- Consumers expecting a percent (5.0) will treat all earnings surprises as near-zero

### 12B-6: ClosedLoop DB Duplicate Ledger Entries
**File:** `core/retraining_controller.py`
- Uses `SELECT DISTINCT` as a workaround for a known duplicate-recording bug in `trade_ledger`
- The root cause (duplicates being written) is not fixed; DISTINCT masks it for the retraining query but other consumers may see duplicates

### 12B-7: Revenue Renamed to EPS in Fallback Earnings
**File:** `data/earnings_calendar.py` — `_fallback_earnings()`
- yfinance "Earnings" column (which is revenue) renamed to `epsActual`
- Downstream PEAD signal reads `epsActual` and computes an earnings surprise against this corrupted value

### 12B-8: GMM Regime Detector Not in Production Path
**File:** `analysis/bayesian_regime.py`
- Documented as "comparison mode" alongside `RegimeDetector`
- Three separate regime detection implementations exist (RegimeDetector, BayesianRegimeDetector, HMMSignals)
- No documented reconciliation of three competing regime signals; only one feeds production

### 12B-9: CrowdingDetector Factor Dispersion Always 0.5
**File:** `analysis/crowding_detector.py` — `_get_factor_dispersion()`
- Always returns 0.5 regardless of actual factor dispersion
- CRI (Crowding Risk Index) formula: SI×0.30 + corr×0.30 + inst×0.20 + dispersion×0.20
- The 0.20 weight on dispersion is permanently 0.10 — CRI is systematically low by up to 0.10

### 12B-10: SimFin Estimates Endpoint Returns Empty
**File:** `analysis/earnings_revision_scorer.py` — `_fetch_simfin_estimates()`
- SimFin free API does not provide consensus estimates
- Function always returns `{}`
- Earnings revision scores are always 0 (no revisions ever detected)

### 12B-11: IEX Free Tier vs. SIP NBBO
**File:** `execution/alpaca_stream.py`
- Uses IEX free tier for live price data
- IEX is a single exchange feed, not the consolidated SIP feed (NBBO)
- Bid/ask spreads and last prices may differ significantly from NBBO, especially for thinly traded stocks

### 12B-12: Schumann Collector Mislabels Solar Wind Data
**File:** `frontier/physical/schumann_collector.py`
- Fetches NOAA solar wind proton speed from ACE/DSCOVR satellite
- Labels output as "Schumann resonance frequency"
- These are entirely different physical phenomena; the data is physically meaningless for its stated purpose

### 12B-13: Satellite Imagery Collector Uses ETF Volume
**File:** `frontier/physical/satellite_imagery_collector.py`
- Fetches XRT (retail ETF) volume as a proxy for satellite imagery activity
- No actual satellite imagery processing occurs
- Output labeled as "satellite activity index" is actually retail equity trading volume

### 12B-14: BenchmarkTracker Update Is a No-Op Stub
**File:** `closeloop/risk/benchmark_tracker.py` — `update()`
- Method exists but performs no computation
- Benchmark performance tracking silently produces no data

### 12B-15: StressLearner Asymmetric Learning Rates Cause Drift
**File:** `closeloop/stress/stress_learner.py`
- Stressed loss → fragility_score += 0.05
- Stressed win → fragility_score -= 0.03
- Net effect per paired stressed trade: +0.02 drift toward high fragility
- Over time, all positions trend toward maximum fragility regardless of actual performance

### 12B-16: SqueezePredictor Never Has Enough History
**File:** `deepdata/short_interest/squeeze_scorer.py` — `layer3_predict()`
- Fresh SqueezePredictor with single data point; requires 5 historical squeezes to produce prediction
- Always returns None in production; Layer 3 squeeze prediction is permanently inactive

### 12B-17: Patents Collector CPC Class Is Country Code
**File:** `deepdata/patents/uspto_collector.py`
- `cpc_class` field populated with country code (e.g. "US") not CPC technology classification
- Patent technology analysis based on this field is meaningless

### 12B-18: UK IPO Collector Uses Deprecated Endpoint
**File:** `deepdata/patents/uk_ipo_collector.py` — `detect_us_patent_filing()`
- Calls PatentsView API v0/v1 endpoint, deprecated and returning HTTP 410 Gone since 2025
- All US patent detection calls fail silently

### 12B-19: Congressional Returns Always Zero
**File:** `deepdata/congressional/member_tracker.py`
- `excess_return` and `information_ratio` always 0.0 in all code paths
- Congressional trade performance tracking produces no actual return attribution

### 12B-20: Alpaca Broker Silently Disabled by "PASTE" Literal
**File:** `execution/broker_interface.py`
- Checks if API key contains the literal string "PASTE"
- If credentials are not updated from template, the AlpacaPaperBroker silently degrades to PaperBroker (flat $1 commission, no real fills)
- No warning is emitted to Telegram or logs

---

## 12C: Silent Failures

These are cases where code encounters an error or unexpected condition and continues with incorrect results rather than raising an exception or alerting.

### 12C-1: TradeAutopsy trade_id=-1 Cascade
**File:** `closeloop/autopsy/trade_autopsy.py`
- If trade lookup returns no record, `trade_id` is set to -1
- All subsequent DB writes (attribution, outcomes) use trade_id=-1
- Creates phantom records associated with a non-existent trade; no exception raised

### 12C-2: FrontierSignalEngine Swallows Collector Init Errors
**File:** `frontier/signals/frontier_signal_engine.py`
- `cls(self._config)` called for social collectors that accept no config argument
- TypeError caught silently; collector skipped without log entry
- Missing collectors contribute 0.0 to UMCI with no indication they failed

### 12C-3: ModuleWirer Silent Disconnection
**File:** (wiring module)
- Modules not successfully wired are silently excluded from the signal pipeline
- No Telegram alert or log warning when a module fails to wire
- System continues with reduced signal coverage

### 12C-4: StressLearner JSON Single-Quote Replacement
**File:** `closeloop/stress/stress_learner.py`
- Parses JSON by replacing single quotes with double quotes
- Any JSON value containing a legitimate single quote (e.g., company names) will corrupt the parse
- Exception caught silently; stress record lost

### 12C-5: HMM/GMM Falls Through If Library Missing
**Files:** `analysis/mathematical_signals.py`, `analysis/bayesian_regime.py`
- If `hmmlearn` or `sklearn` not installed, modules return empty signal dicts silently
- No alert; signals simply absent from aggregation

### 12C-6: SymbolicRegression Empty Return
**File:** `analysis/symbolic_regression.py`
- If neither PySR nor gplearn is installed, returns empty dict
- No log warning; discovery pipeline continues with zero equations discovered

### 12C-7: Daily Pipeline Direct SQLite Without Transactions
**File:** `intelligence/daily_pipeline.py` — `run_macro_briefing()`
- 750+ lines of direct `sqlite3` calls outside transaction blocks
- A mid-function exception leaves the DB in a partially written state
- No rollback mechanism

### 12C-8: MonitorRunner Drops All Messages on 401
**File:** `monitoring/monitor_runner.py` — `_process_retry_queue()`
- On HTTP 401 Unauthorized from Telegram, drops ALL queued messages
- Telegram token expiry causes permanent loss of queued alerts without user notification

### 12C-9: server_monitor _alerts_sent Never Cleared
**File:** `monitoring/server_monitor.py`
- `_alerts_sent` set accumulates alert type strings and is never cleared
- Persistent conditions (e.g., sustained high RAM) trigger only one alert, ever
- Subsequent occurrences are permanently suppressed

### 12C-10: Earnings Scheduler Fires on Wrong Day (BST)
**File:** `data/earnings_scheduler.py`
- UTC weekday check may misclassify Saturday night UTC as a Friday during BST transition
- Earnings collection runs on incorrect day without any log warning

### 12C-11: PrefixCheck Reads Wrong DB, Reports Success
**File:** `monitoring/preflight_check.py`
- Opens `output/permanent_archive.db` for DB accessibility check
- `permanent_store.py` uses `output/permanent_log.db`
- Preflight reports "permanent DB accessible" even if `permanent_log.db` is corrupt or missing

### 12C-12: Multi-Timeframe Filter Always Passes
**File:** `analysis/multi_timeframe.py`
- On data unavailability (any exception), returns `True` (pass)
- Filter is silently neutralised whenever data is unavailable
- Stocks that should be filtered continue through the signal pipeline

### 12C-13: Cooling Off Winning Exit Clears Lockout Unconditionally
**File:** `execution/cooling_off_tracker.py`
- A winning exit unconditionally clears the lockout, even if a losing exit lockout was placed the same day
- A losing exit followed immediately by a winning exit in the same ticker produces no net lockout

### 12C-14: Shadow DB Tables Not Initialised
**File:** `core/retraining_controller.py`
- Writes to `simulations/shadow.db` tables `retraining_events` and `model_registry`
- No `_init_db()` call before first write
- If DB or tables do not exist, first retraining log call raises an unhandled sqlite3.OperationalError

### 12C-15: TaxManager Acquisition Double-Match
**File:** `closeloop/risk/tax_manager.py`
- `_match_same_day()` and `_match_bed_and_breakfast()` do not remove matched acquisitions from the pool
- The same acquisition record can be matched against multiple disposals
- UK tax calculations (share matching rules) are incorrect

### 12C-16: IV Rank Uses Fixed Range Not Historical IV
**File:** `signals/options_earnings_signal.py`
- IV rank proxy uses fixed range [15%, 65%] rather than the actual historical IV range for each ticker
- All IV rank calculations are systematically miscalibrated; high-IV stocks appear lower than they are

### 12C-17: PutCallAnalyser Z-Score Always Zero
**File:** `deepdata/options/put_call_analyser.py`
- Z-score computed from a single-point history list passed to `scipy.stats.zscore()`
- Single-point zscore is always 0.0 by definition
- All put/call anomaly detection signals are permanently 0.0

### 12C-18: Sector Contagion Fetches Uncached yfinance
**File:** `signals/filters.py` — `sector_contagion()`
- `yf.Ticker(ticker).info` called on every invocation without caching
- Each filter call makes a live HTTP request; network failure silently produces incorrect filter results

---

## 12D: State Synchronisation Issues

These are cases where two or more components maintain overlapping or dependent state that can become inconsistent.

### 12D-1: Weekly Report Triple Scheduling
**Files:** `execution/trading_bot.py` (active, Sunday 09:00 UTC), `intelligence/automation_scheduler.py` (disabled, commented out), `monitoring/monitor_runner.py` (disabled, commented out)
- Only one source is active but the disable comments in two files create maintenance risk
- If either disabled block is re-enabled, users receive duplicate weekly reports

### 12D-2: Realtime Monitor Overlaps With Trading Bot Scan Loops
**Files:** `monitoring/realtime_monitor.py`, `execution/trading_bot.py`
- Both modules independently scan for conditions and may generate identical Telegram alerts
- No deduplication between the two scan loops
- Duplicate alerts are sent to Telegram when both detect the same condition simultaneously

### 12D-3: Health Dashboard JSON Race Condition
**File:** `monitoring/health_dashboard.py`
- Overwrites `output/dashboard.json` in-place without atomic write (no tmp-file + rename)
- Flask web dashboard reading `dashboard.json` concurrently may read a half-written file
- Corrupted JSON causes Flask to return an error page

### 12D-4: CoolingOff DB Uses DELETE Journal (Not WAL)
**File:** `data/db_utils.py` patch does not apply to cooling_off.db (separate connection or cooling_off_tracker opens its own connection outside the monkey-patch window)
- `cooling_off.db` uses DELETE journal mode, not WAL
- High-frequency read/write in cooling_off_tracker during signal generation may produce write conflicts or SQLITE_BUSY errors

### 12D-5: TradeAutopsy Has No Confirmed Caller
**File:** `closeloop/autopsy/trade_autopsy.py`
- Module exists and is wired but no confirmed caller path was found in the codebase
- Trade autopsy analysis may never execute in production

### 12D-6: Shadow Training Records Written But Training Never Runs
**File:** `core/retraining_controller.py`
- `_initiate_shadow_training()` writes a "training initiated" record to shadow.db
- No actual training code follows; the record implies training completed when it did not
- `model_registry` table in shadow.db will contain stale "initiated" entries indefinitely

### 12D-7: FeatureManager Disables Sources Without Telegram Alert
**File:** `monitoring/external_source_monitor.py`
- When a data source is disabled, FeatureManager is notified but no Telegram message is sent
- Operator has no real-time visibility into data source degradation unless actively checking dashboard

### 12D-8: Scan Scheduler Dead Counters
**File:** `core/scan_scheduler.py`
- `_scan_count` and `_last_full_cycle` initialised but never updated
- Any code reading these counters (health checks, dashboards) sees permanently stale zeros

### 12D-9: StressLearner exit_vip Parameter Never Used
**File:** `closeloop/stress/stress_learner.py`
- `exit_vix` parameter accepted in function signature but never read in the body
- Stress learning does not differentiate between high-VIX and low-VIX exit conditions

### 12D-10: Retraining Controller Dormancy Gate Never Reached
**File:** `core/retraining_controller.py`
- Requires ≥500 real trades AND ≥30 trading days before initiating retraining
- Current live system (as of 2026-04-08) is well below 500 trades
- Retraining controller always reports "dormant"; automated ML updates never execute

---

# SECTION 13: MANDATORY SECOND PASS

## 13A: Formula Verification

All formulas documented in the master map, verified for internal consistency.

### 13A-1: UMCI Formula
**File:** `frontier/equations/unified_complexity_index.py`
`UMCI = (PC×0.20 + SC×0.25 + SciC×0.15 + FFC×0.25 + ADC×0.15) × 100`
- Weights sum: 0.20 + 0.25 + 0.15 + 0.25 + 0.15 = **1.00** ✓
- However: `SC` (Social Complexity) includes `shipping_index` and `wikipedia_trend` components that have no collectors — always 0.0. `ADC` (AltDataComplexity) also has dead components.
- Effective formula with known-zero components: UMCI is computed on reduced data permanently

### 13A-2: CRI Formula
**File:** `analysis/crowding_detector.py`
`CRI = SI×0.30 + corr×0.30 + inst×0.20 + dispersion×0.20`
- Weights sum: 0.30 + 0.30 + 0.20 + 0.20 = **1.00** ✓
- `dispersion` always = 0.5 → CRI permanently inflated by 0.10 constant

### 13A-3: Kelly Position Sizing
**File:** `risk/manager.py`
`size = Kelly_fraction × signal_strength × regime_multiplier × UMCI_scalar × portfolio_value`
- Kelly fraction calculated correctly from win rate and win/loss ratio
- UMCI_scalar ranges [0.5, 1.5] based on UMCI decile
- regime_multiplier from `_REGIME_MULTIPLIERS` (integer key lookup)
- **Verified consistent** except for the regime key type mismatch (see 12A-1)

### 13A-4: Adaptive Position Sizer Phases
**File:** `execution/adaptive_position_sizer.py`
- PHASE_1: 0–49 trades — fixed fractional sizing
- PHASE_2: 50–99 trades — Kelly introduced at 25% weight
- PHASE_3: 100–199 trades — Kelly at 50% weight
- PHASE_4: 200–499 trades — Kelly at 75% weight, UMCI enabled
- PHASE_5: 500–999 trades — full Kelly + UMCI
- PHASE_FREE: 2000+ trades — unconstrained optimisation
- **Verified:** 6 phases, phase boundaries are discrete and non-overlapping ✓
- **Issue:** DB path `output/permanent_archive.db` for phase persistence is wrong (see 12A-2)

### 13A-5: UMCI Dimension Weights
`PC = Physical Complexity × 0.20`
`SC = Social Complexity × 0.25`
`SciC = Scientific Complexity × 0.15`
`FFC = Financial Frontier Complexity × 0.25`
`ADC = AltData Complexity × 0.15`
- Weights sum = **1.00** ✓

### 13A-6: Market Impact Models (Both In Use)
**backtest/engine.py:** `impact = IMPACT_K × sqrt(order_size / ADV)` where IMPACT_K = 0.1
**analysis/execution_feasibility.py:** `impact = η × sqrt(order_size / ADV)` where η = 0.15 (or 0.20)
- Two separate square-root models with different coefficients
- **Inconsistency confirmed** — see 12A-11

### 13A-7: PEAD Signal Regime Multipliers
**File:** `analysis/macro_signal_engine.py`
- CRISIS: 0.1, RECESSION: 0.5, STAGFLATION: 0.6, NEUTRAL: 0.8, GROWTH: 1.0, GOLDILOCKS: 1.2
- Multipliers verified as monotonically increasing with regime positivity ✓
- Range [0.1, 1.2] — PEAD signals almost entirely suppressed in CRISIS regime

### 13A-8: Cooling Off Threshold Formula
**File:** `execution/cooling_off_tracker.py`
- Trigger: `atr_stop` or `trailing_stop` exit with loss > 2% of entry
- Lockout: 5 calendar days from exit timestamp
- **Issue:** Calendar days not trading days (see 12A-8)

### 13A-9: BacktestEngine Slippage Model
**File:** `backtest/engine.py`
- Fixed slippage + market impact model with IMPACT_K=0.1
- Stop loss at 25% — extremely wide for equity positions
- **Verified:** 25% stop_loss_pct is wider than any typical strategy parameter

### 13A-10: Crowding Risk Integration
- CRI contributes to position sizing reduction when above threshold
- Exact threshold and reduction formula not documented in extracted content
- **Status:** Partially verified — integration confirmed, exact formula not captured

---

## 13B: Data Flow Completeness

Tracing signals from raw data through to trade execution.

### 13B-1: Live Signal Path (Verified Complete)
```
Raw price data (yfinance/Alpaca) →
  data/historical_collector.py →
    historical_db.db (price_history) →
      signals/[momentum|mean_reversion|gap|pead|sector_rotation|calendar_effects|options_earnings|insider_momentum].py →
        SignalAggregator →
          RiskManager (Kelly × regime × UMCI) →
            AdaptivePositionSizer →
              PaperTrader →
                AlpacaPaperBroker →
                  Alpaca Paper API
```
**Status:** End-to-end path documented. Multiple correctness issues at individual nodes (see Section 15).

### 13B-2: Learning Feedback Path (Incomplete)
```
AlpacaPaperBroker fill →
  ClosedLoopStore (trade_ledger) →
    TradeAutopsy [NO CONFIRMED CALLER] →
      PnLAttributor →
        WeightUpdater →
          BatchRetrainer →
            [DORMANCY GATE — never cleared] →
              RetrainingController →
                _initiate_shadow_training() [STUB — no actual training]
```
**Status:** Path exists in code but breaks at TradeAutopsy (no confirmed caller) and terminates at RetrainingController stub. ML models are never updated in production.

### 13B-3: UMCI Data Flow (Partially Dead)
```
frontier/physical/*  (6 collectors, 2 mislabeled) →
frontier/social/*    (6 collectors, 4 with no data) →
frontier/scientific/* (5 collectors, quantum always 0) →
frontier/financial_frontier/* (3 collectors) →
  FrontierSignalEngine →
    unified_complexity_index.py →
      UMCI score →
        AdaptivePositionSizer (phase 4+) →
          Kelly × UMCI_scalar
```
**Status:** Many inputs permanently 0.0; UMCI value is systematically underestimated.

### 13B-4: Monitoring Alert Path (Verified)
```
trading_bot.py scan →
  condition detected →
    MonitorRunner.send() →
      telegram_logger.py →
        Telegram Bot API →
          private_bot.py long-polling [separate process]
```
**Status:** Functional. Rate limiter exists. No backoff on Telegram errors in private_bot.py (see 12C-8).

### 13B-5: Tax Accounting Path (Broken)
```
Trade fill →
  TaxManager.record_acquisition() →
    [WRONG: calls record_tax_disposal()] →
      disposal table (acquisitions stored as disposals) →
        HMRC matching rules [BROKEN: double-match possible]
```
**Status:** Tax accounting data is fundamentally corrupted.

### 13B-6: Frontier Discovery Path (Mostly Dormant)
```
AdaptivePositionSizer (milestone reached) →
  auto_trigger_discovery() →
    SymbolicRegressionEngine →
      [requires PySR or gplearn installed] →
        discovered_equations (permanent_archive.db — 0 rows) →
          frontier_validator →
            evidence_tracker →
              frontier_sizer
```
**Status:** No equations discovered to date (0 rows in discovered_equations). Discovery requires 200+ trades first.

### 13B-7: Congressional Data Path (Returns Only Zeros)
```
deepdata/congressional/quiver_quant_collector.py →
  deepdata.db (congressional table, 12 rows) →
    deepdata/congressional/member_tracker.py →
      excess_return = 0.0, information_ratio = 0.0 [ALWAYS] →
        frontier/equations/cross_signal_interactions.py →
          congressional_signal_strength = 0.0 [ALWAYS]
```
**Status:** Congressional signal permanently zero.

---

## 13C: Undocumented References

Components referenced in the codebase but with no corresponding source file documented.

### 13C-1: `monitoring/rate_limiter.py`
- Referenced in `intelligence/reasoning_engine.py`
- Earlier pass noted this module as "missing"; subsequent pass found it EXISTS
- **Status corrected:** `monitoring/rate_limiter.py` is present

### 13C-2: `altdata/reasoning/symbolic_regression.py`
- Referenced in `execution/adaptive_position_sizer.py` for milestone triggers
- Earlier pass documented `analysis/symbolic_regression.py` — may be different file or alias
- **Status:** Potential second symbolic regression module not fully documented

### 13C-3: `closeloop/storage/closeloop.db` Table Schema
- `retraining_controller.py` uses DISTINCT workaround for duplicate records in `trade_ledger`
- The root cause (what creates duplicates) is not identified in the documentation

### 13C-4: `execution/feature_manager.py`
- Referenced by `monitoring/external_source_monitor.py`
- Not independently documented in any Part
- **Status:** Undocumented module

### 13C-5: `altdata/learning/weekly_retrainer.py`
- Referenced as the only path to ML model updates
- Not independently documented
- **Status:** Undocumented module

---

## 13D: Internal Contradictions Found in Master Document

### 13D-1: ModuleWirer Disconnect Count
- Part 11B documents ModuleWirer docstring claiming "12 disconnects"
- Parts 11A + 11B + 11C collectively document 24+ disconnected modules
- **Resolution:** Docstring is stale and understates actual disconnection count

### 13D-2: rate_limiter.py Existence
- Earlier parts implied `monitoring/rate_limiter.py` was missing
- Part 11C corrects this: the file exists and is used by `reasoning_engine.py`
- **Resolution:** File is present; earlier implication was incorrect

### 13D-3: TradeAutopsy Caller Status
- Module is documented as wired
- No confirmed caller path found in the codebase
- **Status:** Contradiction unresolved — either caller exists and was missed, or autopsy never runs

### 13D-4: Weekly Report Sender
- Part 11C documents report as sent from `trading_bot.py` Sunday 09:00 UTC
- `intelligence/automation_scheduler.py` and `monitoring/monitor_runner.py` have it disabled
- **Resolution:** Single active sender confirmed; disabled copies are dead code

---

## 13E: Missing Files

Files referenced in code but not found or documented:

| Reference | In File | Status |
|-----------|---------|--------|
| `monitoring/rate_limiter.py` | `intelligence/reasoning_engine.py` | EXISTS (corrected) |
| `execution/feature_manager.py` | `monitoring/external_source_monitor.py` | UNDOCUMENTED |
| `altdata/learning/weekly_retrainer.py` | `core/retraining_controller.py` comments | UNDOCUMENTED |
| `simulations/shadow.db` (init) | `core/retraining_controller.py` | NO _init_db() — may be absent |
| `altdata/models/` (directory) | `core/retraining_controller.py` | UNDOCUMENTED state |
| `output/permanent_archive.db` | 5 modules | MAY NOT EXIST — permanent_store.py writes to `permanent_log.db` |

---

## 13F: Second Pass Additions

Items discovered or clarified during final synthesis that were not present in earlier part-level summaries.

### 13F-1: `output/permanent_archive.db` May Not Exist as File
If `permanent_store.py` has always written to `permanent_log.db`, then `output/permanent_archive.db` may never have been created. All five modules that open this path would silently create an empty SQLite file and write to it, but the file would contain different tables than `permanent_log.db`. This would need to be verified against the filesystem, but the documentation consistently shows zero rows in all affected tables.

### 13F-2: Three Monitoring Loops Run Concurrently
`execution/trading_bot.py`, `monitoring/realtime_monitor.py`, and `monitoring/monitor_runner.py` all run scanning or monitoring loops. Their scheduling is not coordinated and they do not share state. This is a source of duplicate Telegram alerts.

### 13F-3: Cooling Off DB Journal Mode Gap
`data/db_utils.py`'s `patch_sqlite3()` applies WAL mode globally, but the patch wraps `sqlite3.connect`. If `cooling_off_tracker.py` uses a connection that was opened before the patch or uses a different connection factory, it retains DELETE journal mode. This needs filesystem verification (`PRAGMA journal_mode`) but the documentation shows `cooling_off.db` in DELETE mode.

### 13F-4: Hardcoded START_DATE in Three Monitoring Files
`monitoring/health_reporter.py`, `monitoring/weekly_report.py`, and `monitoring/dashboard/app.py` all hardcode `START_DATE = "2026-04-03"`. All statistics (P&L, win rate, trade count from start) computed against this date will become stale as the system ages. No mechanism updates this value.

### 13F-5: Private Bot Token Exposure Risk
`monitoring/private_bot.py` contains a hardcoded Telegram bot token and `PRIVATE_CHAT_ID`. Authentication is by chat ID only. If the bot token is leaked (e.g., via a git push of config files or a compromised VPS), any actor with the token can send commands to the bot and receive financial position data.

---

# SECTION 14: SYSTEM HEALTH MATRIX

Format: Component | Designed To Do | Actually Does | Status | Evidence

---

## 14A: Scheduled Jobs and Orchestrators

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `main.py` | Entry point, wire all modules, start trading loop | Wires modules, calls patch_sqlite3(), starts TradingBot | LIVE | Part 1 |
| `execution/trading_bot.py` | Orchestrate scanning, signal generation, execution, monitoring | Runs 3-tier async scan + inline scheduler; sends weekly report | LIVE | Part 11C |
| `intelligence/automation_scheduler.py` | Schedule 10 daily/weekly pipeline jobs | Runs 9 of 10 jobs (weekly report disabled); subprocess to main.py | LIVE (partial) | Part 11C |
| `intelligence/daily_pipeline.py` | Collect macro data, run briefings, snapshot earnings | Runs 750+ lines direct sqlite3; dynamically imports main | LIVE (fragile) | Part 11C |
| `core/scan_scheduler.py` | 3-tier async scan scheduler with frequency management | Runs scans but _scan_count/_last_full_cycle never updated | LIVE (dead counters) | Part 11C |
| `core/retraining_controller.py` | Gate and initiate ML retraining after sufficient trades | Always returns dormant (<500 trades); shadow training is a stub | DORMANT | Part 11C |
| `monitoring/monitor_runner.py` | Run monitoring checks, relay alerts | Runs health checks; drops all messages on 401 | LIVE (fragile) | Part 11C |
| `monitoring/preflight_check.py` | Pre-market system check at 13:00 UTC | Checks wrong DB path; runs only once daily at 13:00 UTC (no UK pre-market check) | PARTIALLY BROKEN | Part 11C |

---

## 14B: Signal Generators

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `signals/pead_signal.py` | Generate PEAD earnings surprise signals | Generates signals; uses corrupted epsActual from fallback earnings | LIVE (data quality issue) | Part 11C |
| `signals/momentum_signal.py` | 126-day momentum signal | Generates signals; no vol normalization; 126d lookback vs 60d check inconsistency | LIVE (minor issues) | Part 11C |
| `signals/mean_reversion_signal.py` | RSI mean reversion signal | Uses SMA RSI not EWM — inconsistent with technical_indicators.py | LIVE (inconsistent) | Part 11C |
| `signals/gap_signal.py` | Overnight gap signal | LIVE; MAX_VOLUME_RATIO is a minimum threshold (misleading name) | LIVE | Part 11C |
| `signals/sector_rotation_signal.py` | Detect sectors rotating from bottom-5 to top-5 | Operator precedence bug — never correctly identifies rotations | BROKEN | Part 11C |
| `signals/insider_momentum_signal.py` | Long signal on insider buying | Generates LONG on both buys and sells; full-text may return false positives | BROKEN (direction) | Part 11C |
| `signals/options_earnings_signal.py` | IV rank + earnings surprise options signal | IV rank uses fixed [15%,65%] range not historical IV — miscalibrated | LIVE (miscalibrated) | Part 11C |
| `signals/calendar_effects_signal.py` | FOMC, turn-of-month, holiday effects | 2026 FOMC dates only; DB path wrong | LIVE (expires 2026) | Part 11C |
| `analysis/macro_signal_engine.py` | 6-regime PEAD multiplier engine | LIVE; multipliers verified correct | LIVE | Part 11A |
| `analysis/mathematical_signals.py` | 3-state HMM regime signal | Runs if hmmlearn installed; skipped silently otherwise | OPTIONAL | Part 11A |
| `analysis/crowding_detector.py` | Crowding risk index | CRI = SI×0.30 + corr×0.30 + inst×0.20 + 0.10 (constant) | LIVE (biased) | Part 11A |
| `analysis/earnings_revision_scorer.py` | Earnings revision momentum | Always returns 0 — SimFin has no consensus estimates | BROKEN (always zero) | Part 11A |
| `analysis/factor_model.py` | Fama-French 6-factor OLS | LIVE; runs weekly | LIVE | Part 11A |
| `analysis/regime_detector.py` | VIX + MA200 regime classifier | LIVE; bull bias when MA200 < 200 bars of history | LIVE (new-install bias) | Part 11A |
| `analysis/bayesian_regime.py` | GMM probabilistic regime classifier | Comparison mode; not in production path | COMPARISON ONLY | Part 11A |
| `analysis/factor_arbitrage.py` | Factor arbitrage signals | No downstream consumer | DISCONNECTED | Part 11A |
| `analysis/multi_timeframe.py` | Multi-timeframe confirmation filter | Fails open (always True) on data unavailability | BROKEN (always passes) | Part 11A |
| `analysis/options_flow_analyser.py` | Options flow analysis | No downstream consumer | DISCONNECTED | Part 11A |
| `analysis/pairs_trader.py` | Pairs trading signals | No downstream consumer | DISCONNECTED | Part 11A |
| `analysis/pairs_trader_live.py` | Live pairs trading | KalmanPairsTrader NameError; disconnected | BROKEN + DISCONNECTED | Part 11A |
| `analysis/portfolio_optimiser.py` | Portfolio optimisation | No downstream consumer | DISCONNECTED | Part 11A |
| `analysis/symbolic_regression.py` | Symbolic equation discovery | No PySR/gplearn → empty; no downstream consumer | DISCONNECTED | Part 11A |
| `analysis/threshold_optimizer.py` | Signal threshold optimisation | DB path inconsistency; disconnected | DISCONNECTED | Part 11A |
| `analysis/sector_rotation_tracker.py` | Sector rotation tracking | LIVE but 24s blocking per call (12 ETFs × 2s sleep) | LIVE (blocking) | Part 11A |

---

## 14C: Execution Components

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `execution/paper_trader.py` | Execute paper trades via broker | LIVE; wires to AlpacaPaperBroker; records in ClosedLoopStore | LIVE | Part 11C |
| `execution/broker_interface.py` | Interface to Alpaca Paper API | Silently degrades to PaperBroker if "PASTE" in API key | LIVE (fragile) | Part 11C |
| `execution/adaptive_position_sizer.py` | 6-phase Kelly+UMCI position sizing | LIVE; wrong DB path for phase history | LIVE (data isolation) | Part 11C |
| `execution/cooling_off_tracker.py` | 5-day lockout after losing exits | Uses calendar days not trading days; winning exit clears unconditionally | LIVE (minor bugs) | Part 11C |
| `execution/alpaca_stream.py` | Live price streaming via Alpaca | IEX free tier (not SIP); no subscription refresh after reconnect | LIVE (data quality) | Part 11C |
| `execution/chunked_scanner.py` | Chunked universe scanning | Tier multipliers differ from universe.py; max_workers=1 unused | LIVE (inconsistent) | Part 11C |
| `risk/manager.py` | Kelly × regime × UMCI position sizing | LIVE; integer regime key lookup (mismatch risk) | LIVE | Part 11C |

---

## 14D: Learning and Closeloop Components

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `closeloop/autopsy/trade_autopsy.py` | Analyse closed trades for learning | Exists; no confirmed caller; trade_id=-1 silent cascade | LIKELY DORMANT | Part 11B |
| `closeloop/learning/attribution_engine.py` | Attribute P&L to signals | Opens SQLite directly bypassing ClosedLoopStore | LIVE (fragile) | Part 11B |
| `closeloop/learning/weight_updater.py` | Update signal weights from outcomes | soft_update hardcodes sharpe=0.0; apply_stress_caps() dead code | LIVE (degraded) | Session summary |
| `closeloop/learning/pre_trainer.py` | Pre-train models before live trading | RAM guard too aggressive; disconnected | DISCONNECTED | Part 11B |
| `closeloop/risk/benchmark_tracker.py` | Track performance vs benchmark | update() is no-op stub; imports without try/except | BROKEN (stub) | Part 11B |
| `closeloop/risk/tax_manager.py` | UK tax matching (HMRC rules) | record_acquisition() writes to disposal table; double-match possible | BROKEN | Part 11B |
| `closeloop/stress/stress_learner.py` | Learn from stressed market exits | Asymmetric learning rates → drift to high fragility; fragile JSON parse | LIVE (biased) | Part 11B |
| `core/retraining_controller.py` | Initiate shadow model retraining | Always dormant; _initiate_shadow_training() is a stub | DORMANT | Part 11C |

---

## 14E: Frontier / UMCI Components

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `frontier/signals/frontier_signal_engine.py` | Aggregate 20 collectors into UMCI | Aggregates; silently drops collectors that fail init | LIVE (degraded) | Part 11C |
| `frontier/physical/schumann_collector.py` | Collect Schumann resonance data | Returns solar wind speed labeled as Schumann resonance | MISLABELED | Part 11C |
| `frontier/physical/satellite_imagery_collector.py` | Collect satellite imagery data | Returns XRT ETF volume labeled as satellite activity | MISLABELED | Part 11C |
| `frontier/equations/unified_complexity_index.py` | 5-dimensional UMCI | Computes correctly; shipping+wikipedia always 0.0 | LIVE (underestimated) | Part 11C |
| `frontier/equations/cross_signal_interactions.py` | Cross-signal interaction formulas | congressional, hiring, lunar inputs always 0.0 | LIVE (3 always zero) | Part 11C |
| `frontier/validation/frontier_validator.py` | Validate frontier signals (7 tests) | DB path wrong; BH variable convention inverted | BROKEN (path) | Part 11A/11C |
| `frontier/validation/evidence_tracker.py` | Grade signals D/F | D/F boundary logic error (falls to "D" when should be "F") | LIVE (grading error) | Part 11C |
| `frontier/meta_learning/parameter_drifter.py` | Drift parameters over time | Functional | LIVE | Part 11C |
| `frontier/meta_learning/discovery_registry.py` | Registry for discovered equations | 0 equations discovered to date | LIVE (empty) | Part 10 |
| `frontier/scientific/quantum_readiness_tracker.py` | Track quantum computing milestones | milestone_score always 0.0 due to key mismatch | BROKEN (always zero) | Part 11 |

---

## 14F: Data Collection and Universe

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `data/universe.py` | Build trading universe | UK filter divides marketCap by 100 incorrectly | LIVE (UK bias) | Part 11C |
| `data/historical_collector.py` | Collect historical price data | LIVE; 48,510 rows in price_history | LIVE | Part 10 |
| `data/earnings_calendar.py` | Earnings calendar with EPS data | Revenue renamed to epsActual in fallback; surprisePercent as fraction | LIVE (corrupted fallback) | Part 11C |
| `data/earnings_scheduler.py` | Schedule earnings collection | UTC weekday check; BST timing issue | LIVE (BST issue) | Part 11C |
| `data/delisted_universe.py` | Track delisted stocks | Contains still-listed stocks (SMCI listed as delisted 2019) | STALE | Part 11C |
| `deepdata/congressional/member_tracker.py` | Track congressional trade returns | excess_return and information_ratio always 0.0 | BROKEN (always zero) | Part 11B |
| `deepdata/options/flow_monitor.py` | Monitor unusual options flow | _get_unusual_vol always passes only calls side | BROKEN (one-sided) | Part 11B |
| `deepdata/options/put_call_analyser.py` | Put/call ratio anomaly detection | z-score always 0.0 (single data point) | BROKEN (always zero) | Part 11B |
| `deepdata/patents/uspto_collector.py` | USPTO patent data | citations_received=0 hardcoded; cpc_class is country code | BROKEN (data quality) | Part 11B |
| `deepdata/patents/uk_ipo_collector.py` | UK patent filings | Calls HTTP 410 Gone deprecated endpoint | BROKEN (dead endpoint) | Part 11B |
| `deepdata/short_interest/squeeze_scorer.py` | Short squeeze prediction | layer3_predict() always None (insufficient history) | BROKEN (always None) | Part 11B |

---

## 14G: Monitoring and Alerting

| Component | Designed To Do | Actually Does | Status | Evidence |
|-----------|---------------|---------------|--------|----------|
| `monitoring/private_bot.py` | Telegram command interface | LIVE; hardcoded CHAT_ID; no backoff on errors | LIVE (fragile) | Part 11C |
| `monitoring/server_monitor.py` | Alert on high RAM/CPU | One-time alert only (_alerts_sent never cleared) | LIVE (one-alert) | Part 11C |
| `monitoring/health_reporter.py` | Report system health | START_DATE hardcoded 2026-04-03; HTTP checks block thread | LIVE (hardcoded date) | Part 11C |
| `monitoring/weekly_report.py` | Weekly P&L report | Uses correct DB path (permanent_log.db); START_DATE hardcoded | LIVE (hardcoded date) | Part 11C |
| `monitoring/health_dashboard.py` | Dashboard JSON for web app | Race condition on JSON write | LIVE (race condition) | Part 11C |
| `monitoring/dashboard/app.py` | Flask web dashboard | START_DATE hardcoded; Flask secret_key from config | LIVE (hardcoded date) | Part 11C |
| `monitoring/realtime_monitor.py` | Real-time condition monitoring | Overlaps with trading_bot.py; potential duplicate alerts | LIVE (duplicates) | Part 11C |
| `monitoring/milestone_tracker.py` | Track P&L milestones | Starting milestone $101k hardcoded | LIVE (hardcoded) | Part 11C |
| `monitoring/external_source_monitor.py` | Monitor data source health | No Telegram alert on source disable | LIVE (silent disable) | Part 11C |
| `intelligence/reasoning_engine.py` | LLM-powered trade reasoning | Layer 2 uses claude-sonnet-4-20250514; rate limiter present | LIVE | Part 11C |

---

## 14H: Databases

| Database | Designed For | Actual Row Counts | Status | Evidence |
|----------|-------------|------------------|--------|----------|
| `closeloop.db` | Trade ledger, signal weights, closed loop state | Active (rows confirmed, known duplicate issue) | LIVE (duplicates) | Part 10 |
| `cooling_off.db` | Short-term cooling off lockouts | 0 rows; DELETE journal mode | LIVE (empty, journal mode issue) | Part 10 |
| `historical_db.db` | Price history, rates, commodities | 48,510 price_history; 245,227 rates; 161,428 commodities; 9 empty tables | LIVE (partial) | Part 10 |
| `permanent_archive.db` | Long-term macro/raw data archive | 17.5M raw_macro_data; 10.1M commodity; 135K SEC fulltext; 7 empty tables | LIVE | Part 10 |
| `permanent_log.db` | Permanent event log | All 0 rows | EMPTY | Part 10 |
| `frontier.db` | Frontier signal history | 20 umci_history; 6 raw_signals; all others 0 | LIVE (sparse) | Part 10 |
| `deepdata.db` | Alternative data sources | 12 congressional; 25 options_flow; 5 short_interest; others 0 | LIVE (sparse) | Part 10 |
| `altdata.db` | Alternative data raw | 211 raw_data rows | LIVE (sparse) | Part 10 |
| `earnings.db` | Earnings calendar | 1 forward calendar row | LIVE (sparse) | Part 10 |
| `insider_analysis.db` | Insider transactions | 5 insider_transactions | LIVE (sparse) | Part 10 |
| `intelligence_db.db` | Reasoning engine outputs | All 0 rows | EMPTY | Part 10 |
| `shadow.db` | Shadow model training records | All 0 rows; no _init_db() | EMPTY (no schema init) | Part 10/11C |
| `simulation.db` | Backtest simulation runs | 3 runs; 147 trades | LIVE (sparse) | Part 10 |
| `closeloop_data.db` | Duplicate artifact | Parallel file to closeloop.db | ARTIFACT | Part 10 |

---

# SECTION 15: ISSUE REGISTER WITH IMPACT ASSESSMENT

## 15A: Live Trading Impact Issues (Direct Effect on P&L or Risk)

| # | Issue | File | Severity | Impact |
|---|-------|------|----------|--------|
| 1 | sector_rotation_signal.py operator precedence bug | `signals/sector_rotation_signal.py` | CRITICAL | Sector rotation signals are never correctly generated; entire signal is effectively noise |
| 2 | insider_momentum_signal.py treats sells as buys | `signals/insider_momentum_signal.py` | CRITICAL | Insider selling generates spurious long signals; direct adverse P&L impact |
| 3 | Regime key type mismatch (RiskManager) | `risk/manager.py` + `execution/paper_trader.py` | HIGH | Wrong position multiplier applied if string regime passed directly |
| 4 | Alpaca broker silently degrades to PaperBroker | `execution/broker_interface.py` | HIGH | System may never reach Alpaca API; fills are simulated at wrong prices |
| 5 | IV rank fixed range [15%,65%] not historical | `signals/options_earnings_signal.py` | HIGH | All options signals systematically miscalibrated |
| 6 | MarketTimer is_open() returns True on UK holidays | `analysis/market_timer.py` | HIGH | Signals generated and trades attempted on closed market days |
| 7 | Holiday tables expire 2026 | `execution/trading_bot.py`, `signals/calendar_effects_signal.py`, `analysis/market_calendar.py` | HIGH | All holiday detection breaks from 2027-01-01 |
| 8 | UK universe filter divides marketCap by 100 | `data/universe.py` | HIGH | UK stocks systematically excluded or include tiny-caps by error |
| 9 | Earnings fallback renames revenue to epsActual | `data/earnings_calendar.py` | HIGH | PEAD signals computed against wrong fundamental data |
| 10 | IEX free tier not SIP NBBO | `execution/alpaca_stream.py` | MEDIUM | Bid/ask and last price may differ from true market; fills at wrong prices |
| 11 | Multi-timeframe filter always passes on data error | `analysis/multi_timeframe.py` | MEDIUM | Filter neutralised; stocks that should be filtered through to signal pipeline |
| 12 | cooling_off.db DELETE journal mode | `execution/cooling_off_tracker.py` | MEDIUM | Write conflicts under concurrent access; SQLITE_BUSY possible during trading |
| 13 | CrowdingDetector factor dispersion always 0.5 | `analysis/crowding_detector.py` | MEDIUM | CRI permanently understated by 0.10 across all positions |
| 14 | Cooling off uses calendar days not trading days | `execution/cooling_off_tracker.py` | MEDIUM | Lockout shorter than intended during holiday weeks |
| 15 | Winning exit clears cooling-off unconditionally | `execution/cooling_off_tracker.py` | MEDIUM | A losing then winning exit in same ticker produces no net lockout |
| 16 | sector_rotation_tracker.py 24s blocking | `analysis/sector_rotation_tracker.py` | MEDIUM | 24 second blocking call delays signal generation during each sector compute |
| 17 | options flow monitor passes only calls side | `deepdata/options/flow_monitor.py` | LOW | Put-side unusual flow missed; signal is half-blind |
| 18 | Gap signal MAX_VOLUME_RATIO is a minimum threshold | `signals/gap_signal.py` | LOW | Misleading name; actual logic is correct but documentation and readability risk |

---

## 15B: Learning System Issues

| # | Issue | File | Severity | Impact |
|---|-------|------|----------|--------|
| 1 | ML models never retrained in production | `core/retraining_controller.py` | CRITICAL | System never learns; all signal weights remain at initial values |
| 2 | TradeAutopsy has no confirmed caller | `closeloop/autopsy/trade_autopsy.py` | HIGH | Trade-level learning analysis never executes |
| 3 | _initiate_shadow_training() is a stub | `core/retraining_controller.py` | HIGH | Even when retraining is triggered, no actual training runs |
| 4 | shadow.db has no _init_db() | `core/retraining_controller.py` | HIGH | First retraining log attempt fails silently; retraining history never recorded |
| 5 | TaxManager writes acquisitions as disposals | `closeloop/risk/tax_manager.py` | HIGH | Tax records entirely corrupted; UK HMRC reporting would be incorrect |
| 6 | TaxManager double-match on acquisitions | `closeloop/risk/tax_manager.py` | HIGH | Same share lot matched against multiple disposals; CGT calculations wrong |
| 7 | StressLearner asymmetric learning rates | `closeloop/stress/stress_learner.py` | MEDIUM | All positions drift toward maximum fragility over time |
| 8 | Attribution engine bypasses ClosedLoopStore | `closeloop/learning/attribution_engine.py` | MEDIUM | Schema changes to closeloop.db will silently break attribution |
| 9 | WeightUpdater soft_update hardcodes sharpe=0.0 | `closeloop/learning/weight_updater.py` | MEDIUM | Signal weight updates do not incorporate Sharpe ratio |
| 10 | Congressional returns always 0.0 | `deepdata/congressional/member_tracker.py` | MEDIUM | Congressional trade performance permanently zero; signal unusable |
| 11 | SqueezePredictor always returns None | `deepdata/short_interest/squeeze_scorer.py` | MEDIUM | Layer 3 short squeeze prediction permanently inactive |
| 12 | PutCallAnalyser z-score always 0.0 | `deepdata/options/put_call_analyser.py` | MEDIUM | All put/call anomaly signals permanently zero |
| 13 | Benchmark tracker update() is a stub | `closeloop/risk/benchmark_tracker.py` | LOW | No benchmark performance comparison data available |
| 14 | StressLearner fragile JSON single-quote parse | `closeloop/stress/stress_learner.py` | LOW | Stress records with company names containing apostrophes silently lost |

---

## 15C: Monitoring and Display Issues

| # | Issue | File | Severity | Impact |
|---|-------|------|----------|--------|
| 1 | server_monitor _alerts_sent never cleared | `monitoring/server_monitor.py` | HIGH | Persistent RAM/CPU conditions only alert once; operator unaware of sustained issues |
| 2 | MonitorRunner drops all messages on 401 | `monitoring/monitor_runner.py` | HIGH | Telegram token expiry causes permanent loss of queued alerts |
| 3 | Preflight checks wrong DB | `monitoring/preflight_check.py` | HIGH | Reports "permanent DB accessible" even if actual permanent_log.db is corrupt |
| 4 | Health dashboard JSON race condition | `monitoring/health_dashboard.py` | MEDIUM | Flask may read half-written JSON; dashboard errors during update windows |
| 5 | START_DATE hardcoded in 3 monitoring files | `monitoring/health_reporter.py`, `monitoring/weekly_report.py`, `monitoring/dashboard/app.py` | MEDIUM | All statistics computed from 2026-04-03 permanently; becomes misleading after 1 year |
| 6 | Realtime monitor duplicates trading_bot alerts | `monitoring/realtime_monitor.py` | MEDIUM | Duplicate Telegram messages on same condition |
| 7 | Private bot no backoff on Telegram errors | `monitoring/private_bot.py` | MEDIUM | Long-polling loop may hammer Telegram API on rate limit |
| 8 | External source disable not alerted to Telegram | `monitoring/external_source_monitor.py` | MEDIUM | Data source degradation invisible to operator |
| 9 | milestone_tracker starting point hardcoded $101k | `monitoring/milestone_tracker.py` | LOW | Milestone thresholds not configurable; wrong if starting capital differs |
| 10 | health_reporter.py HTTP checks block thread | `monitoring/health_reporter.py` | LOW | Monitoring thread blocked during API health checks at 06:00 UTC |

---

## 15D: Architectural and Data Integrity Issues

| # | Issue | File | Severity | Impact |
|---|-------|------|----------|--------|
| 1 | permanent_archive.db vs permanent_log.db split | 5+ modules | CRITICAL | Phase history, calendar signals, preflight results written to unread DB; permanent data isolation |
| 2 | Three independent regime detectors, only one in production | `analysis/regime_detector.py`, `analysis/bayesian_regime.py`, `analysis/mathematical_signals.py` | HIGH | Regime signal quality is un-benchmarked; best detector may not be in use |
| 3 | Trade ledger known duplicate entries | `core/retraining_controller.py`, `closeloop/store/closeloop_store.py` | HIGH | Trade count inflation; retraining dormancy gate based on inflated count |
| 4 | daily_pipeline.py dynamically imports main | `intelligence/daily_pipeline.py` | HIGH | Tight runtime coupling; may find partially-initialised main module |
| 5 | credentials in plaintext in settings.yaml | `config/settings.yaml` | HIGH | Full API credentials exposed; Telegram token, Alpaca keys, API keys all plaintext |
| 6 | Private bot no auth beyond chat_id | `monitoring/private_bot.py` | HIGH | Token compromise gives full financial data access |
| 7 | Schumann collector returns wrong physical data | `frontier/physical/schumann_collector.py` | HIGH | UMCI Physical Complexity component based on meaningless measurement |
| 8 | Satellite imagery collector returns ETF volume | `frontier/physical/satellite_imagery_collector.py` | HIGH | Second UMCI Physical Complexity component based on ETF data not imagery |
| 9 | SimFin consensus estimates always empty | `analysis/earnings_revision_scorer.py` | HIGH | Earnings revision signal permanently zero; entire module non-functional |
| 10 | UK IPO collector calls HTTP 410 endpoint | `deepdata/patents/uk_ipo_collector.py` | HIGH | All UK patent detection fails silently |
| 11 | 8 disconnected analysis modules | Multiple `analysis/*.py` | MEDIUM | Code complexity without benefit; dead maintenance burden |
| 12 | 24+ disconnected modules total system-wide | Multiple subsystems | MEDIUM | Large surface area of untested, unmaintained code |
| 13 | Quantum milestone_score always 0.0 | `frontier/scientific/quantum_readiness_tracker.py` | MEDIUM | Quantum component of UMCI always zero |
| 14 | Congressional, hiring, lunar inputs always 0.0 | `frontier/equations/cross_signal_interactions.py` | MEDIUM | Three interaction formulas permanently produce 0.0 |
| 15 | frontier_validator BH naming inverted | `frontier/validation/frontier_validator.py` | MEDIUM | Code logic uses inverted convention; validation interpretations may be wrong |
| 16 | evidence_tracker D/F grading boundary error | `frontier/validation/evidence_tracker.py` | MEDIUM | Some Grade F signals promoted to Grade D incorrectly |
| 17 | SMCI listed as delisted in delisted_universe.py | `data/delisted_universe.py` | LOW | Active stock treated as delisted; excluded from universe scanning |
| 18 | 3 services with empty API keys in settings.yaml | `config/settings.yaml` | LOW | Companies House, QuiverQuant, Reddit collectors silently produce no data |
| 19 | 10 dead config sections in settings.yaml | `config/settings.yaml` | LOW | Settings for non-existent collectors; config noise |
| 20 | FRED and OpenWeatherMap keys duplicated in config | `config/settings.yaml` | LOW | Credential management risk; two places to update on rotation |
| 21 | Dashboard password "apollo2026" plaintext in config | `config/settings.yaml` | LOW | Web dashboard password exposed in version control if config committed |

---

## 15E: Deferred / Low Priority Issues

| # | Issue | File | Notes |
|---|-------|------|-------|
| 1 | Monte Carlo no random seed | `backtest/monte_carlo.py` | Non-reproducible backtest runs |
| 2 | Monte Carlo mixed return conventions | `backtest/monte_carlo.py` | Log vs arithmetic returns mixed |
| 3 | backtest stop_loss_pct=0.25 very wide | `backtest/engine.py` | 25% stop allows large drawdowns in simulation |
| 4 | backtest _exec_price() dead code | `backtest/engine.py` | Unreachable execution price function |
| 5 | pairs_trader_live KalmanPairsTrader NameError | `analysis/pairs_trader_live.py` | Would error on first call if ever wired |
| 6 | scan_scheduler stop() doesn't cancel sleeping coroutines | `core/scan_scheduler.py` | Graceful shutdown incomplete |
| 7 | trading_bot.py no SIGTERM/SIGINT handler | `execution/trading_bot.py` | Ungraceful shutdown on VPS restart |
| 8 | earnings_db.py dead variable assignment | `data/earnings_db.py` | Minor; first now_str value computed and immediately overwritten |
| 9 | pre_trainer.py RAM guard too aggressive at 1.7GB | `closeloop/learning/pre_trainer.py` | Prevents pre-training on standard VPS RAM |
| 10 | market_timer.py UTC fallback 1h wrong in BST | `analysis/market_timer.py` | UK timing off by 1 hour in BST without pytz |
| 11 | frontier/sizing/frontier_sizer.py evidence_tracker grade D/F boundary | `frontier/validation/evidence_tracker.py` | Same as 15D-16 |
| 12 | PatentsView US endpoint deprecated 2025 | `deepdata/patents/uk_ipo_collector.py` | Same as 15D-10 |
| 13 | chart_generator.py uses subprocess for chart render | `monitoring/private_bot.py` | Potential zombie processes on VPS |
| 14 | automation_scheduler.py subprocess to main.py exit code not checked | `intelligence/automation_scheduler.py` | Silent failures in scheduled pipeline |
| 15 | weak_update only uses sharpe 0.0 | `closeloop/learning/weight_updater.py` | Weights never incorporate risk-adjusted performance |

---

# SECTION 16: RECOMMENDED FIX SEQUENCE

**DOCUMENTATION ONLY — No fixes implemented. This is a prioritised roadmap.**

---

## GROUP A: Fix Before Next Live Trading Session (Critical / Safety)

These issues directly affect position sizing, signal direction, or risk management with potential for immediate adverse P&L impact.

| Priority | Fix | Files | Risk of Not Fixing |
|----------|-----|-------|-------------------|
| A-1 | Fix sector_rotation_signal operator precedence | `signals/sector_rotation_signal.py` line with `current_top5 - old_top5 & old_bot5` | Signal generates noise not rotation detection |
| A-2 | Fix insider_momentum_signal to filter sells | `signals/insider_momentum_signal.py` | Insider selling generates spurious long signals |
| A-3 | Verify Alpaca API key not containing "PASTE" | `config/settings.yaml` + `execution/broker_interface.py` | All fills may be simulated PaperBroker, not Alpaca |
| A-4 | Fix regime key type consistency | `risk/manager.py` + `execution/paper_trader.py` | Wrong position multiplier on regime transitions |
| A-5 | Verify permanent DB path — resolve `permanent_archive.db` vs `permanent_log.db` | `execution/adaptive_position_sizer.py`, `signals/calendar_effects_signal.py`, `monitoring/preflight_check.py`, `analysis/frontier_validator.py` | Phase history, calendar signals, preflight results in unread isolated DB |

---

## GROUP B: Fix Within First Week (High Impact on Correctness)

These issues cause systematic bias or incorrect calculations that accumulate over time.

| Priority | Fix | Files | Risk of Not Fixing |
|----------|-----|-------|-------------------|
| B-1 | Fix earnings fallback — do not rename revenue to epsActual | `data/earnings_calendar.py` | PEAD signals computed from wrong data |
| B-2 | Fix UK universe filter marketCap division | `data/universe.py` | UK stocks systematically mistreated |
| B-3 | Add 2027 holiday tables (and use a library like `pandas_market_calendars`) | `execution/trading_bot.py`, `signals/calendar_effects_signal.py`, `analysis/market_calendar.py` | All holiday detection breaks 2027-01-01 |
| B-4 | Fix multi-timeframe filter fail-open | `analysis/multi_timeframe.py` | Filter always passes on data errors |
| B-5 | Fix TaxManager — record_acquisition() calls correct method; fix double-match | `closeloop/risk/tax_manager.py` | Tax records corrupted; HMRC compliance broken |
| B-6 | Clear _alerts_sent in server_monitor periodically | `monitoring/server_monitor.py` | Persistent conditions never re-alerted |
| B-7 | Fix IV rank to use historical IV range per ticker | `signals/options_earnings_signal.py` | Systematically miscalibrated options signals |
| B-8 | Fix surprise percent — store as percent not fraction | `data/earnings_calendar.py` | Earnings surprises appear near-zero to consumers |
| B-9 | Fix preflight DB path | `monitoring/preflight_check.py` | Preflight reports wrong DB as healthy |

---

## GROUP C: Fix Within First Month (Structural Integrity)

These issues affect monitoring reliability, data quality, and system observability.

| Priority | Fix | Files | Risk of Not Fixing |
|----------|-----|-------|-------------------|
| C-1 | Add _init_db() to shadow.db schema initialisation | `core/retraining_controller.py` | First retraining log fails |
| C-2 | Identify and wire confirmed caller for TradeAutopsy | `closeloop/autopsy/trade_autopsy.py` | Learning loop is incomplete |
| C-3 | Fix MonitorRunner — do not drop all messages on 401; add backoff | `monitoring/monitor_runner.py` | Alert queue destroyed on token expiry |
| C-4 | Fix health dashboard atomic JSON write | `monitoring/health_dashboard.py` | Race condition corrupts dashboard JSON |
| C-5 | Add MarketTimer holiday calendar integration | `analysis/market_timer.py` | Signals generated on closed days |
| C-6 | Fix StressLearner asymmetric learning rates | `closeloop/stress/stress_learner.py` | All positions drift to max fragility |
| C-7 | Fix cooling_off.db to use WAL mode | `execution/cooling_off_tracker.py` or `data/db_utils.py` | Write conflicts under concurrent access |
| C-8 | Replace schumann and satellite collectors with real data sources or mark as proxy | `frontier/physical/schumann_collector.py`, `frontier/physical/satellite_imagery_collector.py` | UMCI Physical Complexity based on mislabeled data |
| C-9 | Fix frontier_validator.py DB path | `analysis/frontier_validator.py` | Validator writes to unread isolated DB |
| C-10 | Make START_DATE configurable (read from settings.yaml) | `monitoring/health_reporter.py`, `monitoring/weekly_report.py`, `monitoring/dashboard/app.py` | Statistics permanently anchored to 2026-04-03 |

---

## GROUP D: Fix Within First Quarter (Signal Quality and Learning)

These issues degrade signal quality or prevent the learning system from functioning.

| Priority | Fix | Files | Risk of Not Fixing |
|----------|-----|-------|-------------------|
| D-1 | Implement actual shadow training in _initiate_shadow_training() | `core/retraining_controller.py` | ML models never update; system never learns |
| D-2 | Reduce retraining dormancy gate or add manual override | `core/retraining_controller.py` | Retraining always dormant at current trade volumes |
| D-3 | Fix WeightUpdater to use real Sharpe (not hardcoded 0.0) | `closeloop/learning/weight_updater.py` | Signal weights never incorporate risk-adjusted performance |
| D-4 | Fix congressional member_tracker to compute actual returns | `deepdata/congressional/member_tracker.py` | Congressional signal permanently zero |
| D-5 | Fix PutCallAnalyser z-score to use rolling history | `deepdata/options/put_call_analyser.py` | Anomaly detection signal permanently zero |
| D-6 | Fix earnings_revision_scorer — find working consensus estimates source | `analysis/earnings_revision_scorer.py` | Earnings revision signal permanently zero |
| D-7 | Fix crowding_detector _get_factor_dispersion() | `analysis/crowding_detector.py` | CRI permanently biased by 0.10 |
| D-8 | Reconcile three regime detectors — benchmark and select one | `analysis/regime_detector.py`, `analysis/bayesian_regime.py`, `analysis/mathematical_signals.py` | May be using sub-optimal regime detector |
| D-9 | Fix evidence_tracker D/F grading boundary | `frontier/validation/evidence_tracker.py` | Some Grade F signals wrongly promoted to D |
| D-10 | Fix frontier_validator BH convention labeling | `frontier/validation/frontier_validator.py` | Hypothesis test results interpreted backwards |
| D-11 | Fix or remove quantum_readiness_tracker milestone key mismatch | `frontier/scientific/quantum_readiness_tracker.py` | Quantum UMCI component permanently zero |
| D-12 | Add subscription refresh after reconnect in alpaca_stream | `execution/alpaca_stream.py` | Tickers added after reconnect receive no price updates |

---

## GROUP E: Deferred / Housekeeping (No Urgent Impact)

| Priority | Fix | Files | Notes |
|----------|-----|-------|-------|
| E-1 | Update PatentsView API to v2 endpoint | `deepdata/patents/uk_ipo_collector.py` | Current endpoint returns 410 Gone |
| E-2 | Fix USPTO cpc_class to populate technology class not country code | `deepdata/patents/uspto_collector.py` | Patent technology analysis meaningless |
| E-3 | Add SIGTERM/SIGINT handler to trading_bot | `execution/trading_bot.py` | Graceful shutdown on VPS restart/reboot |
| E-4 | Add random seed to Monte Carlo | `backtest/monte_carlo.py` | Reproducible backtest runs |
| E-5 | Fix Monte Carlo mixed return conventions | `backtest/monte_carlo.py` | Consistent return calculations |
| E-6 | Remove or implement 8 disconnected analysis modules | Multiple `analysis/*.py` | Reduce maintenance burden |
| E-7 | Fix scan_scheduler stop() to cancel sleeping coroutines | `core/scan_scheduler.py` | Clean async shutdown |
| E-8 | Standardise RSI to one implementation (EWM) | `signals/mean_reversion_signal.py` | Consistent with technical_indicators.py |
| E-9 | Fix market_timer.py to require pytz (or handle BST correctly without it) | `analysis/market_timer.py` | UK timing off by 1h in BST |
| E-10 | Fix sector_rotation_tracker to use async-friendly sleeping | `analysis/sector_rotation_tracker.py` | 24s blocking removed |
| E-11 | Fix automation_scheduler subprocess to check exit code | `intelligence/automation_scheduler.py` | Silent pipeline failures |
| E-12 | Update SMCI status in delisted_universe.py | `data/delisted_universe.py` | Active stock excluded from scanning |
| E-13 | Move API credentials out of plaintext YAML | `config/settings.yaml` | Security best practice |
| E-14 | Remove dead config sections from settings.yaml | `config/settings.yaml` | Config hygiene |
| E-15 | Fix pairs_trader_live KalmanPairsTrader import | `analysis/pairs_trader_live.py` | Would NameError if ever wired |
| E-16 | Move weekly report to dedicated scheduler (remove from trading_bot inline) | `execution/trading_bot.py` | Separation of concerns |
| E-17 | Remove closeloop_data.db artifact or document its purpose | `output/closeloop_data.db` | Duplicate file causes confusion |
| E-18 | Fix daily_pipeline run_macro_briefing() to use transactions | `intelligence/daily_pipeline.py` | Partial writes on exception |

---

# COMPLETION LINE

**APOLLO SYSTEM MAP — DOCUMENTATION COMPLETE**

| Metric | Count |
|--------|-------|
| Total source files documented | ~300 |
| Parts produced | 11 + Gate + Sections 12–16 |
| Distinct Python classes / modules documented | ~300+ |
| Named formulas documented | 10 (UMCI, CRI, Kelly, 6-phase sizing, 3 market impact variants, PEAD multipliers, cooling-off trigger, squeeze predictor) |
| Data flows traced end-to-end | 7 (live signal path, learning feedback, UMCI, monitoring alert, tax accounting, frontier discovery, congressional) |
| Contradictions / inconsistencies registered | 49 (15A: 15, 12B: 20, 12C: 18, 12D: 10; some overlap across categories) |
| Total issues in Issue Register | 67 (15A: 18, 15B: 14, 15C: 10, 15D: 21, 15E: 15) |
| Confirmed dead / disconnected modules | 24+ (8 analysis, 2 closeloop, 1 deepdata, 1 backtest, frontier cross-signal always-zero, retraining stub, tax manager, benchmark stub) |
| Databases documented | 14 |
| Fix groups | 5 (A: 5 critical, B: 9 high, C: 10 structural, D: 12 signal quality, E: 18 deferred) |
| Total recommended fixes | 54 |
| Generation date | 2026-04-08 |
| Instruction compliance | DOCUMENT ONLY — no code was modified |

---

*END OF APOLLO_SYSTEM_MAP_SECTIONS_12_16.md*
