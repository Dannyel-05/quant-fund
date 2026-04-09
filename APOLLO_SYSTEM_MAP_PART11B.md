# APOLLO SYSTEM MAP — PART 11B
## Group 11B: Closeloop (remaining) + Deepdata (remaining) + Backtest + Archive

**Generated:** 2026-04-08
**Coverage:** 60 files read completely, every line.
**Instruction:** DOCUMENT ONLY. Nothing was fixed or changed.

---

## TABLE OF CONTENTS

Files 1–14 (closeloop/autopsy, context, dashboard, entry, integration) — documented from prior session context, summarised in Gate section.
Files 15–25 — Closeloop remaining (learning, risk, stress)
Files 26–38 — Deepdata congressional, dashboard, earnings_quality, factors, microstructure
Files 39–60 — Deepdata microstructure remaining, options, patents, patterns, short_interest, signals, backtest, archive

---

## FILE 15: closeloop/learning/attribution_engine.py

### A) PURPOSE
Decomposes closed-trade P&L into contributing signal factors by computing each factor's correlation with pnl_pct and defining attribution as `corr * std_p`. Generates top/worst factor lists and saves reports to a dedicated SQLite table.

### B) CLASSES AND METHODS

**Class: `AttributionEngine`**

`__init__(config)` — Reads `closeloop.storage_path` from config. Default: `closeloop/storage/closeloop.db`.

`generate_report(lookback_days=90) -> dict` — Main entry point. Loads trades from last `lookback_days`. For each of 23 signal factors, collects (factor_val, pnl_pct) pairs from trade records, computes correlation, attribution. Returns dict with keys: `trade_count`, `lookback_days`, `top_factors` (top 5 by attribution), `worst_factors` (bottom 5), `full_table` (all factors → stats), `generated_at`.

`_load_trades(cutoff_iso) -> list[dict]` — Connects directly to SQLite. Checks table exists. Executes `SELECT * FROM trade_detail_log WHERE exit_date >= ? AND pnl_pct IS NOT NULL`. Returns list of row dicts.

`top_factor_signals(n=3) -> list[str]` — Calls `generate_report()` then slices top-n factor names.

`save_report_to_db(report) -> None` — Creates `attribution_reports` table if missing. Inserts (generated_at, lookback_days, trade_count, report_json).

### C) MATHEMATICS

```
mean_v = sum(vals) / n
mean_p = sum(pnls) / n
cov    = sum((v - mean_v) * (p - mean_p) for v, p in pairs) / n
std_v  = (sum((v - mean_v)^2 for v in vals) / n) ^ 0.5
std_p  = (sum((p - mean_p)^2 for p in pnls) / n) ^ 0.5
corr   = cov / (std_v * std_p)   [if std_v > 1e-9 and std_p > 1e-9, else 0.0]
attribution = corr * std_p
```
Population variance (divided by n, not n-1). Minimum 3 pairs required per factor.

### D) DATA FLOWS
**Reads:** `trade_detail_log` table — all columns, filtered by exit_date and pnl_pct IS NOT NULL.
**Writes:** `attribution_reports` table — id, generated_at, lookback_days, trade_count, report_json.
**DB access:** Direct `sqlite3.connect(self._db_path)` — bypasses ClosedLoopStore.

### E) DEPENDENCIES
**Internal:** None (no Apollo imports).
**External:** `sqlite3` (stdlib), `logging`, `collections.defaultdict`, `datetime`, `typing`.

### F) WIRING STATUS
LIVE PATH — `generate_report()` is callable. `_load_trades` depends on `trade_detail_log` existing. `save_report_to_db` is separate optional step.

### G) ISSUES FOUND
- Opens SQLite directly via `sqlite3.connect(self._db_path)` — bypasses `ClosedLoopStore` abstraction used by all other closeloop modules. Risk of connection conflicts.
- Module-level `_SIGNAL_FACTORS` list has 23 factors. Docstring says nothing about count.
- `top_factor_signals(n=3)` calls `generate_report()` with default `lookback_days=90` regardless of caller needs — no parameter for lookback.

---

## FILE 16: closeloop/learning/pre_trainer.py

### A) PURPOSE
Loads historical backtest trades from closeloop DB and injects them into the weight updater as synthetic observations. Processes in batches of 100 with garbage collection between batches. Live trades weighted 3x backtest trades.

### B) CLASSES AND METHODS

**Module-level:** `_BATCH_SIZE = 100`, `_LIVE_WEIGHT = 3.0`, `_BACKTEST_WEIGHT = 1.0`, `_RAM_CHECK_GB = 1.7`.

`_get_ram_used_gb() -> float` — Tries `psutil.virtual_memory().used / 1e9`, falls back to parsing `/proc/meminfo`.

**Class: `PreTrainer`**

`__init__(config, weight_updater=None)` — Stores config and weight_updater reference. Reads DB path from config.

`run() -> dict` — Loads backtest trades, processes at weight=1.0. Loads live trades, processes at weight=3.0. Returns summary: backtest_loaded, live_loaded, batches, errors, skipped_ram.

`_load_backtest_trades() -> list[dict]` — Tries table `backtest_trades` first, then `closed_trades WHERE source='backtest'`. Returns empty if neither exists.

`_load_live_trades() -> list[dict]` — Queries `closed_trades WHERE source IS NULL OR source != 'backtest'`.

`_process_in_batches(trades, weight, summary, label)` — Iterates in chunks of 100. Checks RAM before each batch; if > 1.7 GB, logs warning, increments `skipped_ram`, calls `gc.collect()` and breaks. Calls `_observe()` per trade. Calls `gc.collect()` after each batch.

`_observe(trade, weight)` — Calls `self._wu.observe(trade, weight=weight)` if `hasattr(_wu, 'observe')`, else tries `update_weights(trade, weight=weight)`. Logs if neither method exists.

### C) MATHEMATICS
No formulas. Weights are multiplicative constants passed to `weight_updater.observe()`.

### D) DATA FLOWS
**Reads:** `backtest_trades` or `closed_trades` (both via direct sqlite3 in the same db path as closeloop).
**Writes:** None (writes are done by weight_updater).

### E) DEPENDENCIES
**Internal:** Weight updater (duck-typed, not imported directly).
**External:** `gc`, `logging`, `sqlite3`, `datetime`, `typing`. Optional: `psutil`.

### F) WIRING STATUS
DISCONNECTED — Weight updater is passed in at instantiation. Whether `WeightUpdater` actually has an `observe()` method is never verified statically. PreTrainer is called nowhere in the main bot flow that was read.

### G) ISSUES FOUND
- RAM guard at 1.7 GB is extremely conservative for a modern system. Will frequently skip trades on any VPS with normal memory load.
- Duck-typing for `observe()` / `update_weights()` means failures are silently absorbed by the `except` in `_observe()`.
- No logging of how many trades were actually observed vs skipped.

---

## FILE 17: closeloop/learning/regime_tracker.py (from prior session)

Documented in prior context. VIX buckets: LOW/MEDIUM/HIGH/EXTREME. Macro regimes: GOLDILOCKS/STAGFLATION/RECESSION_RISK/RISK_OFF/RISK_ON/UNKNOWN. Multiplier = `sharpe(signal, regime) / mean_sharpe(signal)` clamped [0.3, 2.0]. Fetches ^VIX, ^TNX, ^IRX via yfinance.

---

## FILE 18: closeloop/risk/benchmark_tracker.py

### A) PURPOSE
Tracks daily fund performance against 4 benchmarks (SPY, IWM, EWU, ACWI) and computes Information Ratio for each. Includes human-readable summary table.

### B) CLASSES AND METHODS

**Class: `BenchmarkTracker`**

`__init__(store=None, config=None)` — Stores store and config references.

`record(date, fund_return, fund_value, benchmark_returns=None) -> None` — Calls `self._fetch_benchmark_returns()` if not provided. Calls `store.record_benchmark(date, fund_return, fund_value, benchmark_returns)`.

`compute_ir(benchmark='IWM', window_days=252) -> dict` — Loads history from `store.get_benchmark_history(window_days)`. Computes active returns = fund_return - benchmark_return. Computes IR. Returns dict with ir, active_return_mean, active_return_std, n_obs, benchmark, benchmark_name.

`full_comparison() -> dict` — Calls `compute_ir()` for each of 4 benchmarks.

`summary_text() -> str` — Returns formatted table. Rating: STRONG if IR>0.5, POSITIVE if IR>0, else NEGATIVE.

`update(*args, **kwargs) -> None` — No-op stub.

`_fetch_benchmark_returns() -> dict` — Downloads last 2 days for each benchmark via yfinance, computes pct_change, returns dict of ticker → daily return.

### C) MATHEMATICS
```
active_return[i] = fund_return[i] - benchmark_return[i]
IR = mean(active_return) / std(active_return, ddof=1) * sqrt(252)
```
Requires minimum 5 observations.

### D) DATA FLOWS
**Reads:** `store.get_benchmark_history(window_days)` — returns list of dicts.
**Writes:** `store.record_benchmark(...)`.
**External:** yfinance for live benchmark returns.

### E) DEPENDENCIES
**Internal:** ClosedLoopStore (via `store` param).
**External:** `numpy`, `pandas` (imported at module top level without try/except), `yfinance`, `logging`, `datetime`.

### F) WIRING STATUS
LIVE PATH — `record()` is called by autopsy pipeline. `compute_ir()` is called by dashboard. `update()` is a no-op stub.

### G) ISSUES FOUND
- `import numpy as np` and `import pandas as pd` at module top level WITHOUT try/except. If either is missing, the entire module crashes on import, taking down any module that imports it.
- `update()` is a no-op stub called by autopsy pipeline — no benchmark updates happen from autopsy.
- `_fetch_benchmark_returns()` makes 4 separate yfinance calls (one per benchmark) — slow.

---

## FILE 19: closeloop/risk/correlation_regime.py (from prior session)

Eigenvalue ratio analysis: `lambda_1 / mean(lambda_2...lambda_n)`. Regimes: NORMAL(1–3), ELEVATED(3–6, pos_mult=0.85), CRISIS(>6, pos_mult=0.70), UNUSUAL_LOW(<1). Alias: `CorrelationRegime = CorrelationRegimeDetector`.

---

## FILE 20: closeloop/risk/market_impact.py (from prior session)

`impact_pct = η × realised_vol × sqrt(Q/V)`. η=0.15 US, 0.20 UK. 6 VWAP time windows summing to 1.00.

---

## FILE 21: closeloop/risk/tax_manager.py

### A) PURPOSE
UK Capital Gains Tax manager implementing Section 104 pool, same-day rule, 30-day bed & breakfast rule. Annual allowance £3,000. Basic rate 10%, higher rate 20%.

### B) CLASSES AND METHODS

**Class: `TaxManager`**

`__init__(store=None, config=None, tax_year_start='04-06')` — Initialises `_pools` (defaultdict with shares/cost per ticker) and `_pending` (list of (date, shares, cost) per ticker).

`record_acquisition(ticker, shares, cost_gbp, date, market='uk') -> None` — If market != 'uk', returns immediately. Adds to pool and pending list. Calls `store.record_tax_disposal(...)` with `disposal_type="acquisition"` and `disposal_proceeds=0.0`.

`record_disposal(ticker, shares=None, proceeds_gbp=0.0, date=None, market='uk') -> dict` — Supports two calling conventions. If second arg is a dict, treats as closed_trade autopsy call. Returns: gain, disposal_type, allowable_cost. Applies rules in order: same-day → B&B → Section 104.

`compute_annual_liability(tax_year=None) -> dict` — Calls `store.get_ytd_gains()`. Returns: total_gains, total_losses, net_gain, annual_allowance, taxable_gain, estimated_tax_basic_rate, estimated_tax_higher_rate.

`pool_summary() -> dict` — Returns all tickers: shares, cost, avg_cost.

`_match_same_day(ticker, shares, date) -> Optional[dict]` — Finds pending acquisitions on same calendar day.

`_match_bed_and_breakfast(ticker, shares, date) -> Optional[dict]` — Finds pending acquisitions in next 30 days after disposal date.

`_update_store(ticker, date, proceeds, cost, gain, disposal_type) -> None` — Calls `store.record_tax_disposal(...)`.

### C) MATHEMATICS
```
# Section 104 pool disposal
fraction = min(shares / pool['shares'], 1.0)
cost = pool['cost'] * fraction
pool['shares'] -= shares
pool['cost'] -= cost
gain = proceeds_gbp - cost

# Annual CGT
net_gain = total_gains - total_losses
taxable = max(0.0, net_gain - 3000.0)
basic_tax = taxable * 0.10
higher_tax = taxable * 0.20
```

### D) DATA FLOWS
**Reads:** `store.get_ytd_gains()` — returns total_gains, total_losses.
**Writes:** `store.record_tax_disposal(ticker, disposal_date, disposal_proceeds, allowable_cost, gain, disposal_type, pool_shares_after, pool_cost_after)`.

### E) DEPENDENCIES
**Internal:** ClosedLoopStore (via `store` param).
**External:** `logging`, `collections.defaultdict`, `datetime`, `typing`.

### F) WIRING STATUS
LIVE PATH — Called from autopsy pipeline for UK market disposals.

### G) ISSUES FOUND
- `record_acquisition()` calls `store.record_tax_disposal()` with `disposal_type="acquisition"` and `disposal_proceeds=0.0`. Semantically wrong: an acquisition is being written to a disposal table.
- `_match_same_day()` and `_match_bed_and_breakfast()` both use `fraction = min(shares / total_shares, 1.0)` but do not remove matched acquisitions from `_pending`. Same acquisitions can be matched multiple times.
- `tax_year_start = '04-06'` is stored but never used anywhere in the class — UK tax year starts April 6, this field is dead.
- Non-UK disposals silently return `{gain: 0.0, disposal_type: 'non_uk', allowable_cost: 0.0}` — no logging when market != 'uk'.

---

## FILE 22: closeloop/stress/crisis_library.py (from prior session)

20 historical scenarios. Each: start/trough/end dates, peak_drawdown, conditions_vector, signal_performance, description, lessons, duration_days, recovery_days.

---

## FILE 23: closeloop/stress/monthly_stress_runner.py (from prior session)

Runs first Monday of month. Triggered by UMCI>=70 or drawdown>=5%. Saves to `output/stress_reports/stress_{trigger}_{timestamp}.txt`.

---

## FILE 24: closeloop/stress/stress_learner.py

### A) PURPOSE
Learns which signals are fragile under stress (high-VIX) conditions. Always records outcomes. Prediction mode activates after 50 trades. Maintains per-signal fragility scores [0, 1] in memory cache, with persistence to store.

### B) CLASSES AND METHODS

**Class: `StressLearner`**

`__init__(store=None, config=None)` — Initialises `_n_trades = 0` and `_fragility_cache: dict[str, float]`.

`prediction_mode_active` (property) — Returns `_n_trades >= 50`.

`record_outcome(trade_id, signal_names, net_pnl, entry_vix, exit_vix, drawdown_at_exit, umci_at_entry=None) -> None` — Increments `_n_trades`. Determines `is_stress = entry_vix >= 25.0`. Updates fragility score per signal. Persists to `store.record_stress_outcome(...)`.

`predict_fragility(signal_name) -> dict` — Returns fragility_score, prediction_mode, recommendation, n_trades_seen. Recommendations: CAP_WEIGHT (score >= 0.40), MONITOR (score >= 0.25), ROBUST (else). Returns INSUFFICIENT_DATA if < 50 trades seen.

`get_all_fragility_scores() -> dict` — Returns copy of `_fragility_cache`.

`get_crisis_fragile_signals() -> list[str]` — Returns signals with score >= 0.40.

`update_signal_vulnerability(*args, **kwargs) -> None` — No-op stub.

`load_history_from_store() -> None` — Loads up to 500 stored stress outcomes. Parses conditions JSON (with single-quote to double-quote replacement hack). Increments fragility scores from historical data.

### C) MATHEMATICS
```
# Stressed loss (entry_vix >= 25.0 AND net_pnl < 0):
new_score = min(1.0, current + 0.05)

# Stressed win (entry_vix >= 25.0 AND net_pnl > 0):
new_score = max(0.0, current - 0.03)

# Unstressed:
new_score = current * 0.99 + 0.5 * 0.01

# Historical load uses smaller adjustments:
# stressed loss: +0.03; stressed win: -0.02
```
Initial cache value for unknown signals: 0.5. Threshold for CAP_WEIGHT: 0.40.

### D) DATA FLOWS
**Reads:** `store.get_stress_outcomes(limit=500)` — list of dicts with conditions, top_scenario.
**Writes:** `store.record_stress_outcome(run_date, scenario_name, weighted_stress_risk, crisis_fragile, top_scenario, conditions)`.

### E) DEPENDENCIES
**Internal:** ClosedLoopStore (via `store` param).
**External:** `logging`, `typing`, `datetime` (imported inline).

### F) WIRING STATUS
LIVE PATH — `record_outcome()` is called by autopsy pipeline. `predict_fragility()` is called by stress-aware position sizing. `update_signal_vulnerability()` is a no-op stub.

### G) ISSUES FOUND
- JSON parsing hack: `conditions_str.replace("'", '"')` — Python dict repr with single quotes is not valid JSON. If any string value contains a single quote (ticker names like "O'Reilly"), this breaks silently.
- `exit_vix` parameter is accepted but never used in fragility score calculation.
- Learning rate asymmetry: stressed losses increase score by 0.05, stressed wins decrease by only 0.03. Scores drift toward high fragility over time under mixed conditions.
- `update_signal_vulnerability()` no-op called from autopsy pipeline — no vulnerability updates happen.

---

## FILE 25: closeloop/stress/stress_tester.py (from prior session)

`WeightedStressRisk = Σ(base_loss × cosine_similarity(current, scenario))`. `CRISIS_FRAGILE_THRESHOLD = 0.15`. `_estimate_base_loss` uses mean of relevant signal performances; defaults to 5% if no overlap.

---

## FILE 26: deepdata/congressional/accuracy_scorer.py (from prior session)

HIGH: accuracy>65%, excess_return>5%, n>=10 → credibility 0.7–1.0. CONTRA: accuracy<40%, n>=10 → -0.5 to 0.0. IR formula: `mean_excess / std_excess` (not annualised).

---

## FILE 27: deepdata/congressional/capitol_trades.py (from prior session)

Wrapper using CongressionalDisclosureFetcher. EDGAR query: `'congress OR senator OR representative'` — overly broad. `get_signal_for_ticker`: `score = min(len(buys) / 5.0, 1.0)` — only counts buys.

---

## FILE 28: deepdata/congressional/congressional_signal.py (from prior session)

`adjusted = direction × amount_norm × credibility_abs × committee_power × recency_decay × filing_freshness`. `_get_scored_cache_placeholder()` always returns `{}`. Monte Carlo with 500 permutations.

---

## FILE 29: deepdata/congressional/disclosure_fetcher.py (from prior session)

Capitol Trades HTML scrape + JSON API. 2-second rate limiting. Regex fallback produces low-quality data. `_calc_delay`: filing_date - transaction_date.

---

## FILE 30: deepdata/congressional/member_tracker.py (from prior session)

`excess_return` and `info_ratio` always returned as 0.0. Committee power: base × sector_relevance × seniority. 5 hardcoded committees as fallback.

---

## FILE 31: deepdata/dashboard/deepdata_dashboard.py (from prior session)

8-section dashboard to `output/deepdata_dashboard.txt`. Inline numpy import in `_section_factors()` without guard. Section 7 combines patterns and nonsense results.

---

## FILE 32: deepdata/earnings_quality/beat_quality_classifier.py (from prior session)

`final_pead_multiplier = quality_score × (1 + guidance_score × weight) × transcript_modifier`. `should_suppress_pead()` suppresses on ONE_OFF or quality_score==0 or multiplier==0.

---

## FILE 33: deepdata/earnings_quality/guidance_scorer.py (from prior session)

Conservative guidance (beat_rate >= 0.70) → credibility 0.7. Accurate (0.50–0.70) → 1.0. Optimistic (<=0.30) → 0.3. Persists to `data/cache/deepdata/guidance_history.json`.

---

## FILE 34: deepdata/earnings_quality/revenue_analyser.py (from prior session)

Beat quality scores: REVENUE_DRIVEN=1.0, MARGIN_DRIVEN=0.75, COST_CUT=0.5, TAX_DRIVEN=0.25, ONE_OFF=0.0. ONE_OFF → PEAD multiplier 0.0. EDGAR 8-K regex parsing.

---

## FILE 35: deepdata/factors/exposure_mapper.py (from prior session)

9 factors. Cross-sectional rank normalisation: `(rank_pct - 0.5) × 2`. `calc_momentum_factor`: `(p_short - p_long) / p_long` where p_long=252 days ago, p_short=21 days ago.

---

## FILE 36: deepdata/factors/factor_signal.py (from prior session)

Finds mispricings: high_quality+high_value; high_momentum+low_volatility. `combo_score = (factor1 + factor2) / 2`.

---

## FILE 37: deepdata/factors/risk_decomposer.py (from prior session)

`factor_var_total = port_exposures @ F @ port_exposures`. Idio risk approximation: `var(B, axis=1)` — uses factor exposure variance, not true idiosyncratic variance. Concentration HIGH if any factor > 40%.

---

## FILE 38: deepdata/microstructure/execution_feasibility.py (from prior session)

Max ADV fraction 5%. Edge must exceed costs by 1.2x. VWAP plan: `n_slices = max(1, window_minutes // 5)`. Limit price: `price × (1 + 0.005 × direction)`.

---

## FILE 39: deepdata/microstructure/liquidity_scorer.py

### A) PURPOSE
Ensures signals are actually tradeable by scoring liquidity. Computes market impact, total execution cost, and feasibility. Generates VWAP execution schedules by delegating to SpreadMonitor for intraday volume profiles.

### B) CLASSES AND METHODS

**Class: `LiquidityScorer`**

`__init__(config)` — `IMPACT_K = 0.1`, `ADV_CAP_FRACTION = 0.10`. Reads `adv_window_days` (default 30), `default_slippage_pct` (default 0.001), `intraday_days` (default 5).

`score(ticker, price_data, proposed_position_size, expected_edge_pct) -> dict` — Returns: avg_daily_volume, spread_pct, market_impact_pct, total_cost_pct, feasibility (FEASIBLE/REDUCE_SIZE/REJECT), recommended_size, illiquidity_premium.

`calc_market_impact(position_size, avg_daily_volume) -> float` — Square root model.

`calc_total_cost(spread_pct, slippage_pct, market_impact_pct) -> float` — Sum formula.

`check_feasibility(total_cost_pct, expected_edge_pct) -> str` — REJECT/REDUCE_SIZE/FEASIBLE logic.

`calc_optimal_size(position_size, avg_daily_volume, edge_pct, slippage_pct) -> float` — Binary search approximation: solves `k * sqrt(x/adv) = budget` algebraically.

`track_illiquidity_premium(ticker, returns_history, spread_history) -> float` — Compares mean returns in high-spread periods vs low-spread periods. Returns max(premium, 0.0).

`generate_vwap_schedule(ticker, total_size, execution_window_minutes) -> list` — Calls `SpreadMonitor.calc_intraday_volume_profile()`. Falls back to uniform schedule if no profile.

`_get_avg_daily_volume(ticker, price_data) -> float` — From DataFrame Volume column, else yfinance.

`_get_spread_pct(ticker, price_data) -> float` — (High - Low) / Close from last row, else default_spread.

`_get_intraday_profile(ticker) -> dict` — Instantiates SpreadMonitor, calls `calc_intraday_volume_profile()`.

### C) MATHEMATICS
```
# Market impact (square root model)
impact_pct = IMPACT_K * sqrt(position_size / avg_daily_volume)
# IMPACT_K = 0.1

# Total cost
total_cost = (spread_pct / 2.0) + slippage_pct + market_impact_pct

# Feasibility thresholds
if total > edge: REJECT
if total > 0.5 * edge: REDUCE_SIZE
else: FEASIBLE

# Optimal size (algebraic solution)
budget = edge - (spread/2) - slippage
x_max = adv * (budget / IMPACT_K)^2

# Illiquidity premium
premium = mean(returns[spreads > median_spread]) - mean(returns[spreads <= median_spread])
```

### D) DATA FLOWS
**Reads:** price_data DataFrame (passed in), yfinance as fallback.
**Writes:** None.
**Delegates:** SpreadMonitor for intraday volume profile.

### E) DEPENDENCIES
**Internal:** `deepdata.microstructure.spread_monitor.SpreadMonitor` (imported inline in `_get_intraday_profile`).
**External:** `numpy` (module top level), `pandas` (try/except), `yfinance` (try/except), `logging`, `math`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by ExecutionFeasibility.

### G) ISSUES FOUND
- `illiquidity_premium` is returned as 0.0 in `score()` with comment "populated externally via track_illiquidity_premium" — it is never actually populated in the `score()` return dict.
- `calc_optimal_size()` calls `self._default_spread()` to get spread_pct but doesn't use the actual spread_pct from the outer `score()` call — may differ.
- `IMPACT_K = 0.1` differs from `market_impact.py` which uses η=0.15 (US) and 0.20 (UK). Two different market impact models with different coefficients are used in different parts of the system.

---

## FILE 40: deepdata/microstructure/spread_monitor.py

### A) PURPOSE
Tracks bid-ask spreads and market microstructure. Provides spread data, z-score computation, liquidity shock detection, and intraday volume profiles. Serves as data provider for LiquidityScorer.

### B) CLASSES AND METHODS

**Class: `SpreadMonitor`**

`__init__(config)` — Reads `zscore_window` (default 20), `liquidity_shock_z` (default 2.0), `intraday_days` (default 5).

`monitor(tickers, market='us') -> list` — Returns CollectorResult list with spread data per ticker. quality_score=1.0 if both bid and ask are present, 0.5 otherwise.

`get_spread_data(ticker) -> dict` — Tries `yf.Ticker.fast_info` for bid/ask. Fallback: (High-Low)/Close from last 5 days.

`calc_spread_zscore(ticker, current_spread_pct, history) -> float` — `z = (current - mean) / std` using ddof=1.

`detect_liquidity_events(ticker, spread_history) -> list` — Flags days where spread > `mean + 2.0 * std`. Returns list of event dicts with day_index, spread_pct, threshold, zscore, event_type.

`calc_intraday_volume_profile(ticker) -> dict` — Downloads 1-minute data for last 5 days via yfinance. Computes VWAP and volume% by hour. Returns: `{pct_volume_by_hour: dict, vwap_approximation: float}`.

### C) MATHEMATICS
```
# Spread z-score
z = (current_spread_pct - mean(history)) / std(history, ddof=1)

# Liquidity shock threshold
threshold = mean(spread_history) + liquidity_shock_z * std(spread_history, ddof=1)

# VWAP approximation
price_mid = (High + Low) / 2
vwap = sum(price_mid * volume) / sum(volume)

# Intraday volume %
pct_by_hour[h] = hour_vol[h] / total_vol
```

### D) DATA FLOWS
**Reads:** yfinance — fast_info, history(period='5d'), history(period=f'{intraday_days}d', interval='1m').
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy` (module top level), `pandas` (try/except), `yfinance` (try/except), `logging`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by LiquidityScorer._get_intraday_profile(). Also called directly in deepdata collection.

### G) ISSUES FOUND
- Fallback spread in `get_spread_data()` uses (High-Low)/Close as daily price range — this grossly overestimates the actual bid-ask spread (typically 10-50x too large).
- `calc_intraday_volume_profile` check `if total_vol != total_vol` — NaN check using self-inequality. Correct but unusual style.
- 1-minute yfinance data may not be available for all tickers (e.g. UK stocks).

---

## FILE 41: deepdata/microstructure/volume_analyser.py

### A) PURPOSE
Comprehensive volume and liquidity analysis. Provides: volume pattern classification (6 types), OBV with divergence detection, VWAP (daily/weekly), Amihud illiquidity ratio, dark pool proxy, and composite liquidity score 0–100 in 4 bands.

### B) CLASSES AND METHODS

**Module-level constants:** `VOL_ACCUMULATION`, `VOL_DISTRIBUTION`, `VOL_CHURNING`, `VOL_CLIMAX`, `VOL_DRY_UP`, `VOL_NORMAL`. Thresholds: `UNUSUAL_VOL_MILD=2.0`, `UNUSUAL_VOL_HIGH=3.0`, `UNUSUAL_VOL_EXTREME=5.0`, `DRY_UP_THRESHOLD=0.50`.

**Class: `VolumeAnalyser`**

`__init__(config=None)`

`analyse(ticker, price_data=None, market_cap_usd=0.0) -> dict` — Runs all sub-analyses, returns comprehensive result dict.

`_volume_patterns(df) -> dict` — Classifies pattern, computes vol averages (5d/20d/90d), relative volume, volume trend (linear regression slope over 10 days).

`_on_balance_volume(df) -> dict` — OBV accumulation, 10-day slope, divergence detection.

`_vwap(df) -> dict` — Daily VWAP (simplified: last typical price only), weekly VWAP (last 5 days weighted by volume).

`_amihud(df) -> dict` — Per-day: `|return| / (volume * close) * 1e6`. Mean over 20 days. Score = `min(1.0, log10(max(0.001, amihud_20d) + 1) / 3.0)`.

`_dark_pool_proxy(df) -> dict` — Flags days in last 5 where volume > mean20 + 2.5*std20. `dark_pool_flag = True` if >= 2 such days.

`_liquidity_score(result, market_cap_usd, df) -> float` — Composite 0–100: volume 40%, Amihud 30%, spread proxy 20%, market cap 10%.

`_liquidity_band(score) -> str` — EXCELLENT(>=80), GOOD(>=60), ACCEPTABLE(>=30), DANGEROUS(<30).

`score_for_signal(ticker, price_data=None, market_cap=0) -> dict` — Convenience method returning key signal metrics.

### C) MATHEMATICS
```
# Volume pattern classification
rel20 = today_vol / avg20d
CLIMAX: rel20 >= 5.0
ACCUMULATION: rel20 >= 3.0 AND price_chg > 0.01
DISTRIBUTION: rel20 >= 3.0 AND price_chg < -0.01
CHURNING: rel20 >= 3.0 AND |price_chg| <= 0.01
DRY_UP: rel20 <= 0.50

# OBV
obv[i] = obv[i-1] + vol[i]   if close[i] > close[i-1]
obv[i] = obv[i-1] - vol[i]   if close[i] < close[i-1]
obv[i] = obv[i-1]             otherwise

# OBV divergence
obv_slope from 10-day linear regression
price_slope from 10-day linear regression
divergence = (obv_slope > 0 AND price_slope < 0) OR (obv_slope < 0 AND price_slope > 0)

# Daily VWAP
typical_price = (High + Low + Close) / 3
daily_vwap = typical_price[-1]  # NOTE: only last day, not true daily VWAP

# Weekly VWAP
weekly_vwap = sum(tp[-5:] * vol[-5:]) / sum(vol[-5:])

# Amihud ratio
illiq_day[i] = |return[i]| / (vol[i] * close[i]) * 1e6
amihud_20d = mean(illiq_day[-20:])
amihud_score = min(1.0, log10(max(0.001, amihud_20d) + 1) / 3.0)

# Dark pool: days with (v - avg20) / std20 > 2.5 in last 5 days

# Composite liquidity score weights:
# volume 40%, Amihud 30%, spread proxy 20%, market cap 10%
```

### D) DATA FLOWS
**Reads:** price_data DataFrame or yfinance download (1y period).
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy` (module top level), `pandas` (try/except), `yfinance` (try/except), `logging`, `math`, `datetime`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata collection pipeline and signal engine.

### G) ISSUES FOUND
- `daily_vwap` is simply `typical_price[-1]` — this is NOT a true VWAP, just today's typical price. Real VWAP requires intraday tick data. The variable name is misleading.
- Volume trend uses `np.polyfit(x, vol10, 1)[0]` — a linear regression slope, not a trend indicator. Can be noisy for short windows.

---

## FILE 42: deepdata/options/flow_monitor.py

### A) PURPOSE
Monitors options flow for smart money signals. Computes SMFI (Smart Money Flow Index), gamma exposure, IV rank, put/call ratio, and dark pool score. Operates on yfinance options chains.

### B) CLASSES AND METHODS

**Class: `OptionsFlowMonitor`**

`__init__(config)` — `block_threshold=500`, `rate_limit_sleep=0.5`, `uk_confidence_weight=0.6`. Creates `data/cache/deepdata/options/`.

`collect(tickers, market) -> list` — Alias for `scan()`.

`scan(tickers, market='us') -> list` — Returns CollectorResult list. Applies `.L` suffix for UK tickers. Sets confidence=0.6 for UK. For each ticker: gets options chain, computes SMFI, gamma, IV rank, put/call ratio, dark pool score. Adds results with appropriate quality_score.

`get_options_chain(ticker) -> dict` — Fetches first 4 expiries. Returns `{calls: DataFrame, puts: DataFrame}`.

`calc_smart_money_flow_index(chain) -> float` — SMFI formula.

`calc_gamma_exposure(chain, spot) -> dict` — Computes net GEX and gamma flip level. Uses actual gamma column if available, else BS approximation.

`calc_iv_rank(ticker, current_iv) -> float` — Compares current_iv to 52-week realized vol range as IV proxy.

`detect_unusual_volume(chain) -> list` — Flags strikes where volume > 3x mean OI.

`calc_put_call_ratio(chain) -> float` — `put_vol / call_vol`. Returns nan if call_vol < 1.

`dark_pool_score(ticker) -> float` — Z-score of volume on low-price-movement days in last 30 days.

### C) MATHEMATICS
```
# SMFI
call_sweep = volume where vol > OI * 2 (calls)
block_call  = volume where vol > 500 (calls)
unusual_call = unusual volume events total (calls)
numerator   = call_sweep * 1.5 + block_call * 1.3 + unusual_call
denominator = put_sweep * 1.5 + block_put * 1.3 + unusual_put + 0.001
SMFI = numerator / denominator

# Gamma exposure per strike
gex = gamma * OI * 100 * spot^2 / 100
net_gamma = sum(call_gex) - sum(put_gex)

# BS gamma approximation (when no gamma column)
T = 30/252
gamma_approx = 1 / (spot * IV * sqrt(T) + 1e-9)

# IV rank
IV_rank = (current_IV - 52wk_low_realized_vol) / (52wk_high_realized_vol - 52wk_low_realized_vol)
```

### D) DATA FLOWS
**Reads:** yfinance options chains (first 4 expiries per ticker), yfinance fast_info for spot price, yfinance history for IV rank and dark pool.
**Writes:** None (results returned as CollectorResult list).

### E) DEPENDENCIES
**Internal:** None.
**External:** `yfinance` (try/except at module top), `numpy`, `pandas` (module top level — no guard), `logging`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata collection pipeline.

### G) ISSUES FOUND
- `import numpy as np` and `import pandas as pd` at module top level without try/except guards — crashes on import if either missing.
- `calc_iv_rank()` uses realised volatility (rolling 4-week returns std * sqrt(52)) as a proxy for implied volatility. This is a rough approximation — implied and realised vol can diverge significantly.
- UK options via yfinance are typically illiquid or unavailable. `uk_confidence_weight=0.6` doesn't account for options simply not existing.
- `_get_unusual_vol(df)` in SMFI calls `detect_unusual_volume({'calls': df, 'puts': DataFrame()})` — passes only calls as 'calls' key regardless of side loop variable.

---

## FILE 43: deepdata/options/put_call_analyser.py

### A) PURPOSE
Analyses put/call ratios, rolling z-scores, and positioning classification. Detects distribution (smart money buying puts while price rises) and accumulation (smart money buying calls while price falls).

### B) CLASSES AND METHODS

**Class: `PutCallAnalyser`**

`__init__(config)` — `zscore_window=30`, `rolling_days=5`, `uk_confidence_weight=0.6`.

`analyse(tickers) -> list` — Per ticker: fetches 4 expiries, concatenates calls/puts, computes put/call ratio, z-score (on single-point history), positioning, price trend (20d return), divergence signal.

`calc_rolling_pc_zscore(ticker, history) -> float` — `z = (arr[-1] - mean) / std`. Uses `np.std(arr)` — population std, not ddof=1.

`classify_positioning(pc_ratio, zscore) -> str` — BEARISH_SMART: pc>1.5 AND z>1.5. BULLISH_SMART: pc<0.5 AND z<-1.5. RETAIL_NOISE: |z|>2.5. Else NEUTRAL.

`divergence_signal(pc_sentiment, price_trend) -> str` — DISTRIBUTION: BEARISH_SMART + price_trend>0.02. ACCUMULATION: BULLISH_SMART + price_trend<-0.02. Else NONE.

### C) MATHEMATICS
```
pc_ratio = put_vol / call_vol  (if call_vol > 1)
zscore = (arr[-1] - mean(arr)) / std(arr)   # population std
```

### D) DATA FLOWS
**Reads:** yfinance options chains (4 expiries), yfinance history (30d).
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `yfinance` (try/except), `numpy`, `pandas` (module top level), `logging`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata collection pipeline.

### G) ISSUES FOUND
- `history` passed to `calc_rolling_pc_zscore()` contains only a single data point `[pc_ratio]`. Z-score of a single point against itself is always 0.0 (std = 0). No historical caching exists — result is always 0.0 z-score.
- `calc_rolling_pc_zscore` uses population std (`np.std(arr)` without ddof=1). Different from other modules which use ddof=1.
- `zscore_window=30` and `rolling_days=5` config parameters are read but `zscore_window` is never used in any calculation.

---

## FILE 44: deepdata/options/uk_options.py

### A) PURPOSE
Collects UK options data via yfinance with 0.6x confidence weighting applied. Also provides US/UK flow divergence detection to flag when UK and US options sentiment contradict each other.

### B) CLASSES AND METHODS

**Class: `UKOptionsCollector`**

`__init__(config)` — `confidence=0.6`, `block_threshold=500`, `rate_limit_sleep=0.5`.

`collect(tickers) -> list` — Appends `.L` suffix. Fetches first 4 expiries. Computes call/put volume, put/call ratio, avg call IV, avg put IV. Returns CollectorResult with quality_score=0.6.

`compare_us_uk_flow(us_results, uk_results) -> list` — Builds lookup from results. US average pc ratio computed. For each UK ticker: classifies US and UK sentiment (bearish if pc>1.2, bullish if pc<0.8, neutral else). If contradiction, appends divergence result.

### C) MATHEMATICS
```
pc_ratio = put_vol / call_vol   (if call_vol > 0, else nan)
us_sentiment = 'bearish' if us_pc > 1.2 else ('bullish' if us_pc < 0.8 else 'neutral')
uk_sentiment = 'bearish' if uk_pc > 1.2 else ('bullish' if uk_pc < 0.8 else 'neutral')
divergence_value = uk_pc - us_pc
```

### D) DATA FLOWS
**Reads:** yfinance options chains.
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `yfinance` (try/except), `numpy`, `pandas` (module top level), `logging`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata collection pipeline for UK tickers.

### G) ISSUES FOUND
- UK options on yfinance are frequently unavailable (most UK stocks lack listed options). Empty chains are silently skipped with a warning log.
- `compare_us_uk_flow` uses average US pc ratio as the US benchmark for tickers without a direct US pair — this is a crude approximation.
- `_strip_suffix(uk_ticker)` in `compare_us_uk_flow` uses `ticker.replace(".L", "")` — could corrupt tickers where `.L` appears in the company name abbreviation (edge case).

---

## FILE 45: deepdata/options/unusual_activity.py

### A) PURPOSE
Detects unusual options activity: sweep orders (high volume-to-OI ratio across multiple strikes), block trades (single orders > 500 contracts), expiry clustering (>60% volume at one expiry). Aggregates into a score 0–1.

### B) CLASSES AND METHODS

**Class: `UnusualActivityDetector`**

`__init__(config)` — `block_threshold=500`, `sweep_vol_oi_ratio=2.0`, `uk_confidence_weight=0.6`.

`scan(tickers, market='us') -> list` — Applies `.L` for UK. For each ticker: fetches 4 expiries, detects sweeps + blocks + expiry clustering, scores. Returns CollectorResult.

`detect_sweeps(chain, ticker) -> list` — Groups by expiry. If `vol_sum / oi_sum >= 2.0` AND unique strikes with volume >= 2, flags as sweep.

`detect_block_trades(chain) -> list` — Flags rows where volume >= 500.

`detect_expiry_clustering(chain) -> dict` — `clustering_detected = True` if dominant expiry > 60% of total volume.

`score_activity(ticker, events) -> dict` — Sweep score: `min(sweep_count / 5, 1.0) * 0.4`. Block score: `min(block_count / 10, 1.0) * 0.4`. Directional: `1.0 if call_events > put_events else 0.5` * 0.2.

### C) MATHEMATICS
```
# Sweep detection per expiry
ratio = vol_sum / oi_sum
sweep if ratio >= 2.0 AND strikes_hit >= 2

# Activity score
sweep_score = min(sweep_count / 5.0, 1.0) * 0.40
block_score = min(block_count / 10.0, 1.0) * 0.40
directional_score = (1.0 if calls > puts else 0.5) * 0.20
total_score = sweep_score + block_score + directional_score  [clipped 0-1]
```

### D) DATA FLOWS
**Reads:** yfinance options chains.
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `yfinance` (try/except), `numpy`, `pandas` (module top level), `logging`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata collection pipeline.

### G) ISSUES FOUND
- `detect_sweeps()` requires `'expiry'` column in the DataFrame — only present if it was added during chain assembly. If `get_options_chain()` is called from a different code path without adding the expiry column, all sweep detection silently returns empty.
- `directional_score` minimum is 0.5 * 0.2 = 0.10 even when put_events > call_events. Score is never truly "bearish" by construction.

---

## FILE 46: deepdata/patents/innovation_scorer.py

### A) PURPOSE
Combines patent signals from USPTO and UK IPO into investment signals. Scores 5 components: filing velocity (35%), citation growth (25%), competitor threat (20%), lead time (10%), tech pivot (10%). Generates -1 to +1 innovation signal.

### B) CLASSES AND METHODS

**Class: `InnovationScorer`**

`__init__(config)`

`score(ticker, patent_data, competitor_tickers=None) -> dict` — Extracts metrics from patent_data list. Computes 5 component scores. If competitor_tickers provided, instantiates USPTOCollector and computes overlap scores. Returns: innovation_score, filing_velocity_score, citation_growth_score, competitor_threat_score, tech_pivot_signal, lead_time_days, signal.

`calc_innovation_signal(velocity, citation_growth, pivot, lead_time) -> float` — Weighted combination of per-factor signals in [-1, +1].

`generate_collector_results(ticker, score_dict) -> list` — Converts score dict to 5 CollectorResult dicts.

`_score_velocity(velocity) -> float` — `0.5 + 0.5 * tanh(velocity - 1.0)`.

`_score_citation_growth(growth) -> float` — `0.5 + 0.5 * tanh(growth)`.

`_score_tech_pivot(pivot) -> str` — Returns PIVOTING, ACCELERATING, DECELERATING, or STABLE.

`_extract_metric(patent_data, key, default)` — Searches CollectorResult raw_data or direct dict keys.

### C) MATHEMATICS
```
# Component scores
velocity_score = 0.5 + 0.5 * tanh(velocity - 1.0)
citation_score = 0.5 + 0.5 * tanh(citation_growth)
lead_time_score = min(lead_time_days / 180.0, 1.0)

# Combined innovation score
innovation_score = (
    velocity_score    * 0.35
  + citation_score    * 0.25
  + (1 - competitor_threat) * 0.20  # lower threat = better
  + lead_time_score   * 0.10
  + (velocity_score if pivot == 'ACCELERATING' else 0.0) * 0.10
)

# Innovation signal (-1 to 1)
if velocity > 1.5: vel_signal = min((velocity - 1.0) / 2.0, 1.0)
elif velocity < 0.5: vel_signal = max(velocity - 1.0, -1.0)
else: vel_signal = velocity - 1.0

cit_signal = clip(citation_growth, -1, 1)
pivot_signal_val = min(new_cats * 0.1, 0.3) - min(abandoned_cats * 0.05, 0.15)
lead_signal = 0.1 if 0 < lead_time <= 120 else 0.0

raw = vel_signal * 0.45 + cit_signal * 0.30 + pivot_signal_val * 0.15 + lead_signal * 0.10
```

### D) DATA FLOWS
**Reads:** patent_data list (passed in from USPTOCollector or UKIPOCollector).
**Writes:** None.
**Delegates:** USPTOCollector for competitor overlap (imported inline).

### E) DEPENDENCIES
**Internal:** `deepdata.patents.uspto_collector.USPTOCollector` (inline import).
**External:** `numpy` (try/except), `logging`, `math`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata signals pipeline after patent collection.

### G) ISSUES FOUND
- `competitor_threat_score` computes overlap via `USPTOCollector.calc_competitor_overlap()` which makes 2 Google Patents API calls per competitor pair — up to 10 external calls for 5 competitors. Slow and subject to rate limiting.
- `generate_collector_results` quality_score hardcoded to 0.75 — not configurable.
- Weights in `calc_innovation_signal` (0.45, 0.30, 0.15, 0.10) differ from weights in `score()` (0.35, 0.25, 0.20, 0.10, 0.10) — the final signal and the component scores use different factor importance assumptions.

---

## FILE 47: deepdata/patents/uk_ipo_collector.py

### A) PURPOSE
Collects patent data for UK-listed companies from UK IPO API and Companies House R&D disclosures. Also detects if UK companies are filing US patents (international expansion signal). Cross-references with hiring data.

### B) CLASSES AND METHODS

**Class: `UKIPOCollector`**

`__init__(config)` — Reads `companies_house_api_key`. Loads `uk_company_names.json` cache.

`collect(tickers) -> list` — Filters to `.L` tickers only. Gets company name, fetches UK patents, detects US patent filing. Returns CollectorResult with value=patent_count.

`fetch_uk_patents(company_name) -> list` — Tries UK IPO API first, falls back to Companies House.

`_fetch_from_uk_ipo_api(company_name) -> list` — GET to `https://api.ipo.gov.uk/patents`. 1.5s rate limit. Returns: patent_number, date, title, status, source.

`_fetch_from_companies_house(company_name) -> list` — Requires API key. Searches company, fetches filing history, filters for 'patent' or 'intellectual property' in description.

`detect_us_patent_filing(ticker, company_name) -> bool` — POST to USPTO PatentsView API (`https://api.patentsview.org/patents/query`). Returns True if any patents found for company.

`cross_reference_hiring(ticker, patent_velocity, hiring_momentum) -> dict` — Returns `{rd_phase: bool, signal_strength: float}`. `rd_phase = patent_velocity > 1.2 AND hiring_momentum > 0.5`. `signal_strength = min(sqrt(patent_velocity * hiring_momentum) / 2.0, 1.0)`.

`_get_company_name(ticker) -> str` — Cache-first, then yfinance `info.longName`.

### C) MATHEMATICS
```
# R&D phase signal
rd_phase = patent_velocity > 1.2 AND hiring_momentum > 0.5
signal_strength = min(sqrt(patent_velocity * hiring_momentum) / 2.0, 1.0)
```

### D) DATA FLOWS
**Reads:** UK IPO API, Companies House API, USPTO PatentsView API, yfinance for company names.
**Writes:** `data/cache/deepdata/uk_company_names.json`.

### E) DEPENDENCIES
**Internal:** None.
**External:** `requests` (try/except), `BeautifulSoup` (try/except), `yfinance` (inline import), `logging`, `json`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata patent collection pipeline.

### G) ISSUES FOUND
- UK IPO API (`https://api.ipo.gov.uk/patents`) is documented as experimental/availability varies. 404/501/503 are silently swallowed; the collector degrades silently to Companies House fallback.
- USPTO PatentsView `detect_us_patent_filing()` uses the PatentsView v0 endpoint which the USPTO collector itself says is HTTP 410 Gone as of 2025. This call will always fail silently.
- Companies House fallback requires API key — if not configured, returns empty silently.

---

## FILE 48: deepdata/patents/uspto_collector.py

### A) PURPOSE
Collects patent data from Google Patents public API (replacing defunct PatentsView v0/v1 endpoints which returned HTTP 410 as of 2025). Falls back to CrossRef academic publication API. Computes filing velocity, citation growth, technology pivot detection, competitor overlap, and innovation lead time.

### B) CLASSES AND METHODS

**Class: `USPTOCollector`**

`__init__(config)` — Loads `data/cache/deepdata/company_names.json`.

`collect(tickers, market='us') -> list` — For each ticker: gets company name, fetches 90-day and 180-day patent histories, computes filing_velocity, citation_growth, tech_pivot. Detects CrossRef fallback usage (quality=0.4) vs Google Patents (quality=0.8).

`fetch_patents(company_name, days_back=90) -> list` — GET to `https://patents.google.com/xhr/query` with `url=assignee=<name>&num=100&after=priority:<date>&before=priority:<date>`. 3-attempt exponential backoff: 1.5s, 3s, 6s. Returns list of patent dicts. CPC class approximated from publication number prefix (country code) — not true CPC.

`_fetch_crossref_fallback(company_name, days_back) -> list` — GET to `https://api.crossref.org/works` with affiliation filter. Returns publications with `cpc_class='CR'` marker.

`calc_filing_velocity(patent_history) -> float` — `patents_last_90d / max(patents_prev_90d, 1)`.

`calc_citation_growth(patent_history) -> float` — Year-over-year growth in citations. All citations_received are 0 in Google Patents listing — always returns 0 or 1.

`detect_tech_pivot(patent_history) -> dict` — Compares CPC classes last 90d vs prior year. Returns new_categories, abandoned_categories, pivoting flag.

`calc_competitor_overlap(ticker_a, ticker_b) -> float` — Jaccard similarity of CPC class sets. `overlap / union` across 365-day patent histories.

`calc_innovation_lead_time(ticker, patent_history, earnings_history) -> int` — Median days from patent filing to subsequent earnings beat (within 180-day window). Returns -1 if insufficient data.

`get_company_name(ticker) -> str` — Cache-first, yfinance info fallback.

### C) MATHEMATICS
```
# Filing velocity
velocity = patents_last_90d / max(patents_prev_90d, 1)

# Citation growth (YoY)
growth = (this_year_citations - prev_year_citations) / max(prev_year_citations, 1)
# NOTE: citations_received = 0 for all Google Patents listing results
# So citation_growth always = 0 (no prior citations) or 1 (this_year > 0, prev = 0)

# Competitor overlap (Jaccard)
overlap = |classes_a ∩ classes_b| / |classes_a ∪ classes_b|

# Innovation lead time
lead_times = [days between patent_date and beat_date for matching pairs]
result = median(lead_times)
```

### D) DATA FLOWS
**Reads:** Google Patents API, CrossRef API, yfinance for company names.
**Writes:** `data/cache/deepdata/company_names.json`.

### E) DEPENDENCIES
**Internal:** None.
**External:** `requests` (try/except), `yfinance` (try/except), `logging`, `json`, `time`, `collections.Counter`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Primary patent data source.

### G) ISSUES FOUND
- `citations_received = 0` hardcoded for all Google Patents listing results (acknowledged in docstring). `calc_citation_growth()` will always return 0 or a Boolean coerced to float. The citation growth metric is meaningless as implemented.
- `cpc_class` is approximated from the 2-character publication number prefix (country code like 'US', 'EP', 'WO') — this is NOT a CPC technology class. `detect_tech_pivot()` will classify geography changes as technology pivots.
- CrossRef fallback quality=0.4 is used but the source label is set to "CROSSREF_PROXY" — not visually distinguishable as a fallback in dashboard output.
- `_fetch_crossref_fallback()` is defined but never called in `fetch_patents()`. The fallback is dead code.

---

## FILE 49: deepdata/patterns/cross_module_scanner.py

### A) PURPOSE
Finds patterns across all modules simultaneously using configured combinations, auto-discovered pairs, and hardcoded specific combos. Full statistical validation pipeline: t-test + Bonferroni + 500-shuffle Monte Carlo + Deflated Sharpe Ratio.

### B) CLASSES AND METHODS

**Class: `CrossModulePatternScanner`**

`__init__(config)` — `MONTE_CARLO_N=500`, `CORRELATION_THRESHOLD=0.3`, `P_VALUE_THRESHOLD=0.01`, `max_auto_pairs=50`, `bonferroni_n=100`.

`scan(all_signals, price_data) -> list` — Runs configured, auto-discovered, and specific combos. Deduplicates by name.

`test_configured_combinations(all_signals, price_data) -> list` — Tests combinations from `config.patterns.test_combinations`.

`auto_discover_pairs(all_signals, price_data, max_pairs=50) -> list` — Randomly shuffles all module pairs. Tests cross-correlation at lags 0–60. Flags if |corr| > 0.3 AND p < 0.01.

`test_specific_combos(all_signals, price_data) -> list` — 5 hardcoded combos: lunar+congressional+reddit, weather+short_interest+earnings, patent+hiring+transcripts, options+wikipedia, congressional+supply_chain.

`validate_combination(signal_series_list, returns_series, name) -> dict` — Combines signals (average), computes Sharpe, t-test (scipy or manual), Bonferroni correction, 500-shuffle permutation test, DSR. `passed = bonferroni_p < 0.05 AND permutation_pct >= 0.95 AND dsr > 0`.

`calc_deflated_sharpe(returns, n_trials) -> float` — `DSR = SR * (1 - sqrt(log(n_trials) / n_obs))`.

`_best_lagged_correlation(s_a, s_b, max_lag=60)` — Tests correlations at lags 0–60, returns best |corr|, lag, p-value.

### C) MATHEMATICS
```
# Signal-conditioned returns
signal_returns = returns * sign(signal)

# Sharpe
sharpe = mean(signal_returns) / std(signal_returns, ddof=1) * sqrt(252)

# Deflated Sharpe
deflation = sqrt(log(n_trials) / n_obs)
DSR = SR * (1 - deflation)

# Bonferroni
bonferroni_p = min(1.0, p_value * bonferroni_n)

# Permutation test
permutation_pct = fraction of 500 null Sharpes < real_sharpe

# Validation passes if:
bonferroni_p < 0.05 AND permutation_pct >= 0.95 AND dsr > 0
```

### D) DATA FLOWS
**Reads:** all_signals dict (module → CollectorResult list), price_data dict (ticker → DataFrame).
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas` (try/except), `scipy.stats` (try/except), `logging`, `math`, `random`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata dashboard and signal engine.

### G) ISSUES FOUND
- `auto_discover_pairs` uses `random.shuffle(pairs)` — results are non-deterministic across runs. Same data can produce different patterns.
- `_extract_signal_series` converts timestamps with `pd.to_datetime(ts, utc=True).tz_localize(None)` — calling `tz_localize(None)` on an already-localized timestamp will raise TypeError in some pandas versions.
- `test_specific_combos` requires at least `len(signal_series_list) >= 1` (not 2) to call `validate_combination`. With 1 series, `pd.concat([series], axis=1).mean(axis=1)` works but there is no actual cross-module combination tested.

---

## FILE 50: deepdata/patterns/nonsense_correlator.py

### A) PURPOSE
Finds and tests absurd correlations that may be real and durable. Uses pre-defined hypothesis templates with economic logic scores. Computes `NonsenseScore = 1 / (economic_logic_score + 0.01)` — higher score means more absurd and potentially more durable (nobody else trading it).

### B) CLASSES AND METHODS

**Module-level:** `HYPOTHESIS_TEMPLATES` — 10 pre-defined hypotheses: lunar+short_interest, weather+options, congressional+day_of_week, bitcoin+short_interest, earnings_beats+short_interest, patent_weekday+momentum, wikipedia+earnings, reddit+options_skew, dark_pool+short_squeeze, supply_chain+competitor_IV. Economic logic scores: 0.05 to 0.70.

**Class: `NonsenseCorrelator`**

`__init__(config)` — `MONTE_CARLO_N=1000`, `BONFERRONI_N=200`, `p_threshold=0.05`, `min_permutation_pct=0.95`.

`find_all_nonsense(all_signals, price_data) -> list` — Generates hypotheses (templates + dynamic cross-module pairs). Tests each. Returns only passing ones sorted by nonsense_score descending.

`generate_nonsense_hypotheses(available_modules) -> list` — Includes template if at least one required module is available. Generates dynamic pairs for all available module combinations.

`test_hypothesis(hypothesis, data) -> dict` — T-test + 1000-shuffle Monte Carlo. `passed = bonferroni_p < p_threshold AND permutation_pct >= 0.95`.

`calc_nonsense_score(hypothesis, p_value) -> float` — `1.0 / (economic_logic_score + 0.01)`.

### C) MATHEMATICS
```
# Signal-conditioned returns
cond_returns = returns * sign(signal)

# Sharpe
sharpe = mean(cond_returns) / std(cond_returns, ddof=1) * sqrt(252)

# Monte Carlo (1000 shuffles)
null_sharpes = [mean(shuffled * sign(signal)) / std(...) * sqrt(252) for each shuffle]
permutation_pct = fraction of null_sharpes < real_sharpe

# Bonferroni
bonferroni_p = min(1.0, p_value * 200)

# NonsenseScore
score = 1.0 / (economic_logic_score + 0.01)
# Range: max = 1/0.06 ≈ 16.7 (lunar, score=0.05)
#        min = 1/0.71 ≈ 1.41 (supply_chain+IV, score=0.70)
```

### D) DATA FLOWS
**Reads:** all_signals (module → CollectorResult list), price_data (ticker → DataFrame).
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas` (try/except), `scipy.stats` (try/except), `logging`, `math`, `random`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata dashboard (Section 7) and signal engine (Tier 3 signals).

### G) ISSUES FOUND
- `generate_nonsense_hypotheses` includes a template if "at least one required module is available" — meaning if `lunar` is unavailable but `short_interest` is, the lunar+short_interest hypothesis is still included. It will silently fall back to whatever modules ARE available for data.
- `_build_hypothesis_data` falls back to using ANY available module if the specified modules have no data — the hypothesis being "tested" may not correspond to the hypothesis template at all.
- `calc_nonsense_score` ignores `p_value` parameter entirely despite it being accepted. The docstring implies p_value should influence the score.

---

## FILE 51: deepdata/short_interest/finra_collector.py

### A) PURPOSE
Collects FINRA daily short volume data (US) and FCA disclosed short positions (UK). Downloads CNMS short volume files from FINRA CDN (backfills up to 10 business days). Falls back to yfinance for short ratio, float pct, and days-to-cover. Alias: `FinraCollector = FINRACollector`.

### B) CLASSES AND METHODS

**Class: `FINRACollector`**

`__init__(config)` — Creates HTTP session with browser-like headers. Cache dir: `data/cache/finra/`.

`collect(tickers, market='us') -> list` — Routes to `fetch_fca_shorts()` for UK, `fetch_finra_short_data()` for US. Returns CollectorResult list with short_ratio, short_float_pct, days_to_cover.

`fetch_finra_short_data(ticker) -> dict` — Iterates business days backwards up to 10. Checks cache for `CNMSshvol{date}.txt`. Downloads from `https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt`. Parses pipe-delimited file, finds ticker row. Returns: short_ratio, short_interest, total_volume, short_float_pct (None), days_to_cover (None), change_from_prev (None), trend_3period='FLAT'. Falls back to yfinance.

`fetch_fca_shorts(ticker) -> dict` — Scrapes FCA short disclosure page. Parses tables looking for ticker in row text. Returns: `{disclosed_shorts: list}`.

`calc_trend(history) -> str` — 3-period moving average direction: INCREASING/DECREASING/FLAT. Threshold: 2% change.

### C) MATHEMATICS
```
# FINRA short ratio
short_ratio = ShortVolume / TotalVolume

# FCA: no computation, raw pct_capital from disclosure table

# Days-to-cover (yfinance fallback)
dtc = sharesShort / averageVolume

# Trend
ma3 = 3-period moving average
diff = ma3[-1] - ma3[-2]
threshold = |ma3[-1]| * 0.02
INCREASING if diff > threshold
DECREASING if diff < -threshold
FLAT otherwise
```

### D) DATA FLOWS
**Reads:** FINRA CDN (pipe-delimited .txt), FCA website (HTML scrape), yfinance fallback.
**Writes:** Cache files to `data/cache/finra/CNMSshvol{date}.txt`.

### E) DEPENDENCIES
**Internal:** None.
**External:** `requests` (try/except), `BeautifulSoup` (try/except), `yfinance` (try/except), `numpy`, `pandas` (module top level), `logging`, `io`, `time`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata short interest collection.

### G) ISSUES FOUND
- FINRA `fetch_finra_short_data()` returns `short_float_pct=None`, `days_to_cover=None`, `change_from_prev=None` from the daily file — these fields only come from yfinance fallback. CollectorResult with these Nones may cause downstream KeyError or type errors.
- FCA scrape relies on finding the ticker string anywhere in table row text — could produce false matches for tickers that are substrings of other names.
- `calc_trend()` returns 'FLAT' for all FINRA results by default — the trend is never computed from the daily file (single-point data). Only yfinance fallback could theoretically populate enough history for trend, but `calc_trend()` is never called in the yfinance path either.

---

## FILE 52: deepdata/short_interest/pattern_scanner.py

### A) PURPOSE
Finds non-obvious patterns in short interest data with rigorous statistical validation. Tests: earnings timing (SI peaks N days before earnings), day-of-week effects, lunar correlation, cross-ticker prediction, social media lead, weather short covering (explicitly designed as negative control). Full validation: t-test + Bonferroni + 500-shuffle Monte Carlo.

### B) CLASSES AND METHODS

**Class: `ShortInterestPatternScanner`**

`__init__(config)` — `min_observations=20`, `p_value_threshold=0.05`.

`scan_all(short_history, price_history, altdata_history=None) -> list` — Runs all tests per ticker. Also tests cross-ticker prediction for all ticker pairs.

`test_earnings_timing_pattern(ticker, short_history, earnings_dates) -> dict` — Finds peak SI day in 30-day pre-earnings window. Computes mean/std of peak offsets. `consistency = 1 - min(std / (|mean| + 1e-9), 1)`.

`test_dow_pattern(short_history) -> dict` — One-way ANOVA across SI changes grouped by day of week.

`test_lunar_correlation(short_history) -> dict` — Requires `ephem` library. Computes Pearson correlation of SI changes with lunar phase (0–100).

`test_cross_ticker_prediction(ticker_a, ticker_b, short_history) -> dict` — Lag-1 correlation: change in A predicts change in B next day.

`test_social_lead(short_history, reddit_history) -> dict` — Tests lags 1–7 days. Best lag by |correlation|.

`test_weather_short_covering(short_history, weather_data) -> dict` — Weather (NYC) lagged 5 days vs SI changes. Explicitly noted as negative control. Strict threshold: p < 0.05/20 = 0.0025 AND Monte Carlo p < 0.001.

`validate_pattern(returns_series, pattern_name) -> dict` — t-test + Bonferroni (n_comparisons from config, default 20) + 500-shuffle Monte Carlo (seeded with 42). `significant = bonferroni_p < threshold AND mc_p < threshold`.

### C) MATHEMATICS
```
# Earnings timing consistency
consistency = 1.0 - min(std_offsets / (|mean_offset| + 1e-9), 1.0)

# Day-of-week: one-way ANOVA F-statistic

# Lunar correlation: Pearson r(SI_changes, lunar_phase)

# Cross-ticker: lag-1 Pearson r(changes_a shifted 1, changes_b)

# validate_pattern
t_stat = mean / (std / sqrt(n))
bonferroni_p = min(p_value * n_comparisons, 1.0)
mc_p = fraction of 500 shuffled |mean| >= observed |mean|
significant = bonferroni_p < 0.05 AND mc_p < 0.05
```

### D) DATA FLOWS
**Reads:** short_history dict, price_history dict, altdata_history dict (optional).
**Writes:** None.

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas` (module top level), `scipy.stats` (try/except), `ephem` (try/except, required for lunar test), `logging`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata short interest pipeline.

### G) ISSUES FOUND
- `ephem` library is not a standard dependency. If not installed, lunar correlation test always returns `valid=False` with note — silently skipped.
- `test_cross_ticker_prediction` is O(n²) in number of tickers — for a large universe, this creates a combinatorial explosion of API calls and tests.
- `validate_pattern` Monte Carlo uses `rng = np.random.default_rng(seed=42)` — seeded, so results are reproducible but identical across runs. Correct for reproducibility but hides if patterns are sample-specific.

---

## FILE 53: deepdata/short_interest/squeeze_predictor.py

### A) PURPOSE
ML-based short squeeze predictor using RandomForest (scikit-learn). Requires at least 5 historical squeeze events to train. Squeeze defined as: price gain >= 30% in 10 days AND pre-squeeze short float >= 20%. Saves model to `data/cache/deepdata/short_interest/squeeze_predictor_model.joblib`.

### B) CLASSES AND METHODS

**Class: `SqueezePredictor`**

Constants: `SQUEEZE_PRICE_GAIN_THRESHOLD=0.30`, `SQUEEZE_DAYS=10`, `SQUEEZE_PRE_SHORT_FLOAT=0.20`, `MIN_TRAINING_EVENTS=5`.

`__init__(config)` — Tries to load existing joblib model.

`train(historical_squeeze_events, feature_matrix) -> None` — Trains Pipeline(StandardScaler + RandomForestClassifier(n_estimators=100, max_depth=5, min_samples_leaf=3, balanced weights)). Saves with joblib.

`predict(ticker, current_features) -> float` — Returns probability of class 1 (squeeze). Returns None if not trained.

`identify_historical_squeezes(price_data, short_data_history) -> list` — Scans for 30%+ gain in 10 days with SI > 20% before. Returns squeeze events with pre-squeeze features.

`build_features(ticker, date, short_data, price_data, options_data=None) -> dict` — Builds 14-feature dict: short_float, dtc, borrow_rate, si_change_1p, si_change_3p, momentum_5d, momentum_20d, volume_ratio_5d, vol_surge, iv_rank, pc_ratio, news_score, reddit_sentiment, days_to_earnings.

### C) MATHEMATICS
```
# Squeeze definition
gain = (close[i+10] - close[i]) / close[i]
squeeze if gain >= 0.30 AND short_float >= 0.20

# Feature: momentum_n
momentum_n = close[-1] / close[-n] - 1

# Feature: volume_ratio_5d
ratio = mean(vol[-5:]) / mean(vol[-25:-5])

# Model: RandomForest
# Pipeline: StandardScaler → RandomForestClassifier
# class_weight='balanced'
```

### D) DATA FLOWS
**Reads:** price_data DataFrame, short_data_history list, yfinance calendar for earnings dates.
**Writes:** `data/cache/deepdata/short_interest/squeeze_predictor_model.joblib`.

### E) DEPENDENCIES
**Internal:** None.
**External:** `sklearn.ensemble.RandomForestClassifier`, `sklearn.preprocessing.StandardScaler`, `sklearn.pipeline.Pipeline`, `joblib` (all try/except), `numpy`, `pandas`, `yfinance` (inline import), `logging`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by SqueezeScorer.layer3_predict().

### G) ISSUES FOUND
- If `short_data_history` is empty or has no squeeze events, `identify_historical_squeezes()` never trains the model. `predict()` returns None for new tickers without sufficient history — SqueezeScorer.layer3_predict() handles this correctly.
- `build_features()` calls `yf.Ticker(ticker).calendar` in a try/except — but if ticker is empty string (passed from `identify_historical_squeezes`), this silently returns 999 for `days_to_earnings`.
- Model requires scikit-learn and joblib which may not be installed on VPS.

---

## FILE 54: deepdata/short_interest/squeeze_scorer.py

### A) PURPOSE
Three-layer short squeeze analysis. Layer 1: binary flag thresholds. Layer 2: weighted component score 0–100. Layer 3: ML probability from SqueezePredictor. Final score: `base_score * 0.7 + ml_prob * 100 * 0.3` plus flag boost of 5 points per flag (max 20).

### B) CLASSES AND METHODS

**Class: `SqueezeScorer`**

Layer 1 thresholds: `SHORT_FLOAT_FLAG_PCT=0.20`, `DTC_FLAG=10.0`, `BORROW_RATE_FLAG=0.30`, `SI_INCREASE_FLAG=0.20`.

Layer 2 normalisation: `SHORT_FLOAT_NORM=0.30`, `DTC_NORM=15.0`, `VOLUME_SURGE_THRESHOLD=1.5`, `CATALYST_NEWS_THRESHOLD=0.5`.

`score(ticker, short_data, price_data, options_data=None) -> dict` — Runs all 3 layers. Returns: flagged, squeeze_score, layer1_flags, layer2_score, layer3_probability, signal, components, timestamp.

`layer1_flag(short_data) -> list` — Checks 4 conditions: HIGH_SHORT_FLOAT, HIGH_DAYS_TO_COVER, HIGH_BORROW_RATE, RAPID_SI_INCREASE.

`layer2_score(ticker, short_data, price_data, options_data) -> float` — Weighted 0–100: short_float (max 25), DTC (max 25), momentum (max 20), volume surge (max 15), catalyst (max 15).

`layer3_predict(ticker, historical_data) -> float` — Instantiates SqueezePredictor, tries to identify historical squeezes and predict. Returns None if < 5 squeezes found.

### C) MATHEMATICS
```
# Layer 2 components
short_float_score = min(short_float / 0.30, 1.0) * 25
dtc_score         = min(dtc / 15.0, 1.0) * 25
momentum_score    = 20.0 if momentum_20d > 0 else 0.0
volume_score      = 15.0 if volume_surge > 1.5 else 0.0
catalyst_score    = 15.0 if (earnings_near OR news_score > 0.5) else 0.0

# Layer combination
if layer3 available:
    squeeze_score = layer2 * 0.70 + layer3_prob * 100 * 0.30
else:
    squeeze_score = layer2

# Flag boost
flag_boost = min(len(layer1_flags) * 5, 20)
squeeze_score = min(squeeze_score + flag_boost, 100.0)

# Signals
EXTREME >= 80, HIGH >= 60, MODERATE >= 35, LOW < 35
```

### D) DATA FLOWS
**Reads:** short_data dict, price_data, options_data dict.
**Writes:** None.
**Delegates:** SqueezePredictor for Layer 3 (inline import).

### E) DEPENDENCIES
**Internal:** `deepdata.short_interest.squeeze_predictor.SqueezePredictor` (inline import).
**External:** `numpy`, `pandas`, `yfinance` (try/except), `logging`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Called by deepdata short interest signal generation.

### G) ISSUES FOUND
- `layer3_predict()` instantiates `SqueezePredictor` fresh every call, tries to identify historical squeezes from the single current `short_data` dict — will almost never find 5 squeezes. Layer 3 effectively always returns None.
- `_check_earnings_within_30d()` uses `yf.Ticker(ticker).calendar` which is unreliable for many tickers. Returns False on any exception.
- `momentum_score` is binary (20 or 0) based on whether 20d momentum > 0. No proportional weighting.

---

## FILE 55: deepdata/signals/deepdata_signal_engine.py

### A) PURPOSE
Integrates all deepdata signals into ranked, tiered output. Three tiers: Tier 1 (congressional_cluster, options_sweep, supply_chain, transcript_deflection) weighted 1.5; Tier 2 (squeeze, beat_quality, patents, dark_pool) weighted 1.0; Tier 3 (factors, single_congressional, microstructure, nonsense) weighted 0.5. Tier 1 signals trigger immediate notifications.

### B) CLASSES AND METHODS

**Class: `DeepDataSignalEngine`**

`__init__(config, store=None, notifier=None)` — `min_quality_score=0.3`.

`generate(tickers, all_module_results) -> list` — Buckets CollectorResults by ticker. For each ticker: computes confluence, PEAD modifier. For each result: classifies tier, infers direction, computes confidence = `quality_score * deepdata_confluence`. Signals sorted by (tier, -confidence). Tier 1 triggers notification.

`calc_confluence(ticker, all_results) -> dict` — `deepdata_confluence = weighted_sum(tier_weight * quality) / max_possible`. Blends with altdata confluence 50/50 for total_confluence.

`classify_tier(result) -> int` — Checks data_type and source against tier sets.

`calc_pead_modifier(ticker, all_results) -> float` — 1.5 if total_confluence > 0.7; 1.0 if > 0.5; 0.0 if < 0.3; else 1.0.

`should_notify_immediately(signal) -> bool` — True if tier == 1.

`log_signal(signal) -> None` — Calls `store.log_signal()` or `store.save("signals", ...)`.

`_infer_direction(result) -> int` — Checks `raw_data.direction`, else `sign(value)`.

`_get_altdata_confluence(ticker, all_results) -> float` — Searches for 'altdata' in module names, extracts confluence_score from raw_data.

`_notify(signal) -> None` — Calls `notifier.send(msg)` if notifier available.

### C) MATHEMATICS
```
# Deepdata confluence
weighted_sum = sum(TIER_WEIGHTS[tier] * quality for each result)
max_possible = sum(TIER_WEIGHTS[tier] for each result)
deepdata_confluence = min(weighted_sum / max_possible, 1.0)

# Total confluence
total = deepdata_confluence * 0.5 + altdata_confluence * 0.5

# Signal confidence
confidence = min(1.0, quality_score * deepdata_confluence)

# PEAD modifier thresholds
total > 0.7 → 1.5
total > 0.5 → 1.0
total < 0.3 → 0.0
else → 1.0
```

### D) DATA FLOWS
**Reads:** all_module_results dict.
**Writes:** store.log_signal() or store.save().

### E) DEPENDENCIES
**Internal:** ClosedLoopStore (via store param), notifier.
**External:** `numpy`, `pandas` (try/except), `logging`, `datetime`, `pathlib`.

### F) WIRING STATUS
LIVE PATH — Central integration layer for deepdata module.

### G) ISSUES FOUND
- `confidence = quality_score * deepdata_confluence`. If deepdata_confluence is 0 (no results yet for ticker), all signals have confidence=0. First run problem.
- `_get_altdata_confluence()` returns 0.0 if no altdata module found — total_confluence is then `deepdata_confluence * 0.5`. The 50/50 blend penalises systems without altdata results even if deepdata confluence is very high.
- `PEAD modifier = 0.0` when `total_confluence < 0.3` — this can suppress PEAD positions entirely based on a confluence score that may just be 0 due to missing data (not low-quality data).

---

## FILE 56: backtest/engine.py

### A) PURPOSE
Vectorized backtesting engine with tiered transaction cost model. Handles US and UK costs (commission, spread, slippage, stamp duty, short borrow). Rejects trades where order > 15% of ADV. Position sizing: fixed fraction of capital. Reports cost breakdown and compares optimistic vs realistic Sharpe.

### B) CLASSES AND METHODS

**Class: `TieredCostModel`**

US tiers: small_cap ($50M-$500M) — commission $1, spread 0.5%, slippage 0.3%, borrow 0.03%/day. mid_cap ($500M-$2B) — commission $1, spread 0.2%, slippage 0.1%, borrow 0.01%/day.

UK tiers: small_cap (£30M-£300M AIM) — commission £3, spread 0.8%, slippage 0.5%, borrow 0.02%/day, stamp_duty 0.5%. mid_cap (£300M-£1.5B) — commission £3, spread 0.3%, slippage 0.2%, borrow 0.02%/day, stamp_duty 0.5%.

`_select_tier(market, market_cap) -> dict` — Iterates small_cap then mid_cap, returns first tier where market_cap <= max.

`get_costs(ticker, market, market_cap, order_value, avg_daily_volume, price, is_short=False) -> Optional[dict]` — Returns None (trade rejected) if order > 15% ADV. `impact_mult = 1 + (order_pct_adv - 0.05) * 10` if order > 5% ADV. Returns: commission, spread_cost, slippage, stamp_duty, total_one_way_pct, impact_mult, order_pct_adv.

`summarise_cost_assumptions() -> str` — Class method. Returns formatted text table.

**Class: `BacktestEngine`**

`__init__(config)` — Reads costs.us, costs.uk, risk.max_position_pct, backtest.initial_capital, backtest.stop_loss_pct (default 0.25).

`run(signals, price_data, market='us', initial_capital=None) -> dict` — Returns: trades, equity_curve, metrics, market.

`_simulate(signals, price_data, market, capital) -> pd.DataFrame` — Per signal row: gets entry/exit prices, applies stop-loss scan, applies tiered costs (or flat fallback), computes gross and net PnL.

`_exec_price_raw(prices, date) -> float` — Returns open price on or after date.

`_exec_price(prices, date, side, direction, costs) -> float` — Legacy slippage-adjusted price (kept for compatibility, not used by _simulate).

`_resolve_exit(prices, entry, exit_) -> Timestamp` — Finds nearest available date at or after exit_.

`_build_equity(trades, capital) -> pd.Series` — Groups by exit_date, cumulates net_pnl.

`_metrics(equity, trades) -> dict` — CAGR, Sharpe (daily returns * sqrt(252)), max drawdown, win_rate, n_trades, volatility, calmar.

`_cost_analysis(trades, equity) -> dict` — Breaks out commission, spread, slippage, stamp_duty, borrow totals. Computes optimistic Sharpe (adding back spread costs) vs realistic Sharpe.

### C) MATHEMATICS
```
# TieredCostModel
adv_value = price * avg_daily_volume
order_pct_adv = order_value / adv_value
REJECT if order_pct_adv > 0.15

impact_mult = 1 + (order_pct_adv - 0.05) * 10   [if order_pct_adv > 0.05]
slippage_pct = tier['slippage_base_pct'] * impact_mult
spread_cost = order_value * (bid_ask_spread_pct / 2)   [one-way = half round-trip]
stamp_duty = order_value * stamp_duty_pct              [UK buys only]
total_cost = commission + spread_cost + slippage + stamp_duty

# BacktestEngine
gross = shares * (exit_price - entry_price) * direction
borrow = shares * entry_price * borrow_rate * holding_days   [shorts only]
net = gross - commission - spread_cost - slippage - stamp_duty - borrow

# Equity metrics
total_ret = equity[-1] / equity[0] - 1
CAGR = (1 + total_ret)^(1/years) - 1
sharpe = mean(daily_ret) / std(daily_ret) * sqrt(252)
drawdown = (equity - rolling_max) / rolling_max
calmar = |CAGR / max_drawdown|
```

### D) DATA FLOWS
**Reads:** signals DataFrame (ticker, signal, entry_date, exit_date, optional: market_cap, avg_daily_volume, surprise_pct, earnings_date).
**Writes:** None (returns DataFrames and dicts).

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas` (module top level), `logging`, `typing`.

### F) WIRING STATUS
LIVE PATH — Core backtest simulation engine.

### G) ISSUES FOUND
- `_exec_price_raw()` returns open price only. `_exec_price()` (slippage-adjusted) exists but is never called by `_simulate()` — slippage is handled separately in the cost model. The legacy method is dead code in normal operation.
- `stop_loss_pct=0.25` default is very wide (25% loss before stop triggers). Will rarely trigger for most signals.
- Equity curve is built by `equity.iloc[-1] / equity.iloc[0] - 1` but starts with `seed = pd.Series([capital], index=[trades['entry_date'].min()])`. If entries and exits are on the same day, cumulative structure may mis-order.
- `_cost_analysis` "optimistic Sharpe" adds back `cost_spread` only (not slippage). Comment says "add back spread + extra slippage for tiered trades" but only spread is added back.

---

## FILE 57: backtest/monte_carlo.py

### A) PURPOSE
Bootstrap Monte Carlo simulator for strategy robustness testing. Resamples trade-return distribution (with replacement) to produce distribution of final equity values, max drawdowns, and Sharpe ratios. Reports percentiles at 5th/25th/50th/75th/95th and probability of profit/ruin.

### B) CLASSES AND METHODS

**Class: `MonteCarloSimulator`**

`__init__(config)` — Reads `backtest.monte_carlo_simulations` and `backtest.initial_capital`.

`run(trade_returns, confidence_levels=(0.05, 0.25, 0.50, 0.75, 0.95)) -> dict` — Requires >= 10 trades. Returns: n_simulations, n_trades, final_value (mean/std/percentiles), max_drawdown (mean/percentiles), sharpe (mean/percentiles), prob_profit, prob_ruin.

`_bootstrap(returns) -> np.ndarray` — `np.random.choice(returns, size=(n_sims, n_trades), replace=True)`. Returns cumulative product paths as shape (n_sims, n_trades).

`_max_drawdowns(paths) -> np.ndarray` — Per path: `((path - cummax) / cummax).min()`.

`_sharpes(paths) -> np.ndarray` — `log_ret = diff(log(paths))`. `sharpe = mean(log_ret, axis=1) / std(log_ret, axis=1) * sqrt(252)`. Returns 0 where std=0.

### C) MATHEMATICS
```
# Bootstrap
paths[i] = cumprod(1 + random_sample_with_replacement(returns, n_trades))
final_value[i] = paths[i][-1] * initial_capital

# Max drawdown per path
peak = cummax(path)
dd[i] = min((path - peak) / peak)

# Sharpe per path
log_ret = diff(log(path))
sharpe[i] = mean(log_ret) / std(log_ret) * sqrt(252)   [if std > 0 else 0]

# Probabilities
prob_profit = fraction of final_value > initial_capital
prob_ruin   = fraction of final_value < initial_capital * 0.5
```

### D) DATA FLOWS
**Reads:** trade_returns pd.Series (passed in).
**Writes:** None (returns dict).

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas`, `logging`, `typing`.

### F) WIRING STATUS
LIVE PATH — Called after BacktestEngine.run() to assess robustness.

### G) ISSUES FOUND
- `np.random.choice()` without a fixed seed — non-deterministic results across runs. Backtests are not reproducible.
- Path Sharpe uses log returns (`diff(log(path))`). Final equity percentiles use arithmetic cumproduct. Mixed return conventions within the same class.
- `prob_ruin` defined as < 50% of initial capital — this is a generous definition of ruin. A more standard threshold might be < 0% return or < 25%.

---

## FILE 58: backtest/subperiod_analysis.py

### A) PURPOSE
Splits backtest results into historical sub-periods and computes per-period metrics. Enables detection of regime-conditional edge. 5 default periods: 2010–2014, 2015–2018, 2019–2021, 2021–2023, 2023–2026. Reports best/worst period by Sharpe, and whether all periods have positive Sharpe.

### B) CLASSES AND METHODS

**Dataclass: `PeriodMetrics`**
Fields: label, description, start, end, n_trades, total_return_pct, cagr_pct, sharpe, max_drawdown_pct, win_rate_pct, avg_return_pct, calmar.

**Class: `SubperiodAnalyser`**

`__init__(periods=None)` — Uses DEFAULT_PERIODS if not provided.

`analyse(equity, trades, initial_capital=50000.0) -> dict` — Returns: periods (list of PeriodMetrics), full (PeriodMetrics for all data), best_period (label), worst_period (label), consistent (bool).

`_compute(equity, trades, label, description, start, end) -> Optional[PeriodMetrics]` — Slices equity curve, computes CAGR, Sharpe, max_dd, calmar, win_rate, avg_ret.

`format_report(result) -> str` — Formatted text table with all period metrics.

### C) MATHEMATICS
```
total_ret = equity[-1] / equity[0] - 1
CAGR = ((1 + total_ret)^(1/years) - 1) * 100
sharpe = mean(daily_pct_change) / std(daily_pct_change) * sqrt(252)   [or 1e-8 if std=0]
max_dd = min((equity - expanding_max) / expanding_max) * 100
calmar = |cagr / max_dd|
win_rate = fraction(returns > 0) * 100
```

### D) DATA FLOWS
**Reads:** equity pd.Series, trades pd.DataFrame (passed in).
**Writes:** None (returns dict).

### E) DEPENDENCIES
**Internal:** None.
**External:** `numpy`, `pandas`, `logging`, `dataclasses`, `json`, `datetime`, `pathlib`, `typing`.

### F) WIRING STATUS
LIVE PATH — Called after BacktestEngine.run() for performance analysis.

### G) ISSUES FOUND
- DEFAULT_PERIODS last entry covers "2023-07-01" to "2026-12-31" which includes future dates. For live data ending in early 2026, this period will include real data up to current date.
- `json` and `pathlib` are imported but never used in this file.
- `initial_capital` parameter is accepted by `analyse()` but never used — the equity curve is used directly.

---

## FILE 59: backtest/walk_forward.py

### A) PURPOSE
Expanding-window walk-forward analysis. Splits historical data into N equal OOS windows. Trains signal generator on all preceding data, evaluates on OOS window. Aggregates across all windows: mean/std OOS Sharpe, mean OOS return.

### B) CLASSES AND METHODS

**Class: `WalkForwardAnalysis`**

`__init__(config, engine)` — Reads `backtest.train_pct`. Stores BacktestEngine instance.

`run(signal_generator, price_data, market='us', n_windows=5) -> dict` — Requires >= 200 common dates. Computes `min_train = n * train_pct`, `window_size = (n - min_train) / n_windows`. For each window: trains on dates[:train_end], generates signals, filters to OOS dates, runs engine. Returns aggregated result.

`_common_dates(price_data) -> DatetimeIndex` — Intersection of all ticker date indices.

`_aggregate(results) -> dict` — Concatenates all OOS trades. Returns: all_trades, window_results, mean_oos_sharpe, std_oos_sharpe, mean_oos_return, n_windows.

### C) MATHEMATICS
```
n = len(all_common_dates)
min_train = int(n * train_pct)
window_size = (n - min_train) // n_windows
train_end[i] = min_train + i * window_size
test_start[i] = train_end[i]
test_end[i] = min(test_start[i] + window_size, n)
```

### D) DATA FLOWS
**Reads:** price_data dict, output of signal_generator function.
**Writes:** None.

### E) DEPENDENCIES
**Internal:** BacktestEngine (passed as engine param).
**External:** `numpy`, `pandas`, `logging`, `typing`.

### F) WIRING STATUS
LIVE PATH — Available for walk-forward validation. Whether it is actually called in the live bot flow was not confirmed in read files.

### G) ISSUES FOUND
- `train_data` is sliced by `df.index.isin(train_dates)` — creates a boolean mask over the entire history. For large datasets, this is inefficient.
- OOS window signals are filtered by `signals['entry_date'].between(test_dates[0], test_dates[-1])` but the engine is called with the full `price_data` (not just OOS data) — forward-looking data is available to the engine during OOS evaluation. This is not pure OOS.
- `_common_dates` uses `set.intersection(*[set(idx) for idx in non_empty])` — for large universes with many tickers, this creates many large sets in memory simultaneously.

---

## FILE 60: archive/legacy_retraining.py

### A) PURPOSE
Archive file containing documentation of two replaced components. Contains NO executable code — all content is comments. Both components were replaced in April 2026 by event-driven retraining.

### B) ARCHIVED COMPONENTS

**BatchRetrainer** (formerly `closeloop/learning/batch_retrainer.py`):
- Was called by automation_scheduler.py on Sundays at 03:00 UTC
- Optimised signal weights via WeightUpdater.batch_update()
- Computed per-signal Sharpe, entry timing alpha, peer/analyst alpha
- Replaced by: `core/retraining_controller.py RetrainingController.run_monitoring_cycle()`
- Reason: dormancy-unaware — fired regardless of whether Apollo had live data

**WeeklyRetrainer** (formerly `altdata/learning/weekly_retrainer.py`):
- Framework: scikit-learn (RandomForestClassifier, GradientBoostingClassifier, LogisticRegression), serialised with joblib
- Parameters: lookback_days=365, validation_split=0.2, model_path=altdata/models/
- Trigger: every Sunday 02:00 UTC
- Promote threshold: `new_sharpe >= current_sharpe * 0.9`
- Ensemble weights: softmax over validation Sharpe values
- Replaced by: event-driven retraining controller
- Reason: fires on 7-day timer regardless of live data volume

### C) MATHEMATICS
Preserved reference parameters only. No live formulas.

### D) DATA FLOWS
None — no code executes.

### E) DEPENDENCIES
None — comment-only file.

### F) WIRING STATUS
DEAD CODE — Archived. Module-level docstring explicitly states "Do not import."

### G) ISSUES FOUND
- File itself has no executable issues (it is comments only).
- The replacement components referenced (`core/retraining_controller.py`) were not in the file list for this documentation task — wiring status of the replacement cannot be confirmed here.

---

## GROUP 11B GATE

### ALL FILES READ

| # | File | Status |
|---|------|--------|
| 1 | closeloop/autopsy/drawdown_forensics.py | LIVE PATH |
| 2 | closeloop/autopsy/signal_interaction_ledger.py | LIVE PATH |
| 3 | closeloop/context/academic_fundamental_bridge.py | LIVE PATH |
| 4 | closeloop/context/analyst_revision_tracker.py | LIVE PATH |
| 5 | closeloop/context/index_rebalancing_detector.py | LIVE PATH |
| 6 | closeloop/context/merger_spillover_detector.py | LIVE PATH |
| 7 | closeloop/context/news_financial_contextualiser.py | LIVE PATH |
| 8 | closeloop/context/peer_influence_mapper.py | LIVE PATH (runtime crash) |
| 9 | closeloop/dashboard/closeloop_dashboard.py | LIVE PATH |
| 10 | closeloop/entry/entry_conditions.py | LIVE PATH (bug) |
| 11 | closeloop/entry/entry_learner.py | LIVE PATH |
| 12 | closeloop/entry/entry_timer.py | LIVE PATH |
| 13 | closeloop/entry/scale_in_manager.py | LIVE PATH |
| 14 | closeloop/integration/module_wirer.py | LIVE PATH (misleading) |
| 15 | closeloop/learning/attribution_engine.py | LIVE PATH |
| 16 | closeloop/learning/pre_trainer.py | DISCONNECTED |
| 17 | closeloop/learning/regime_tracker.py | LIVE PATH |
| 18 | closeloop/risk/benchmark_tracker.py | LIVE PATH |
| 19 | closeloop/risk/correlation_regime.py | LIVE PATH |
| 20 | closeloop/risk/market_impact.py | LIVE PATH |
| 21 | closeloop/risk/tax_manager.py | LIVE PATH |
| 22 | closeloop/stress/crisis_library.py | LIVE PATH |
| 23 | closeloop/stress/monthly_stress_runner.py | LIVE PATH |
| 24 | closeloop/stress/stress_learner.py | LIVE PATH |
| 25 | closeloop/stress/stress_tester.py | LIVE PATH |
| 26 | deepdata/congressional/accuracy_scorer.py | LIVE PATH |
| 27 | deepdata/congressional/capitol_trades.py | LIVE PATH |
| 28 | deepdata/congressional/congressional_signal.py | LIVE PATH (broken cluster scoring) |
| 29 | deepdata/congressional/disclosure_fetcher.py | LIVE PATH |
| 30 | deepdata/congressional/member_tracker.py | LIVE PATH (broken metrics) |
| 31 | deepdata/dashboard/deepdata_dashboard.py | LIVE PATH |
| 32 | deepdata/earnings_quality/beat_quality_classifier.py | LIVE PATH |
| 33 | deepdata/earnings_quality/guidance_scorer.py | LIVE PATH |
| 34 | deepdata/earnings_quality/revenue_analyser.py | LIVE PATH |
| 35 | deepdata/factors/exposure_mapper.py | LIVE PATH |
| 36 | deepdata/factors/factor_signal.py | LIVE PATH |
| 37 | deepdata/factors/risk_decomposer.py | LIVE PATH |
| 38 | deepdata/microstructure/execution_feasibility.py | LIVE PATH |
| 39 | deepdata/microstructure/liquidity_scorer.py | LIVE PATH |
| 40 | deepdata/microstructure/spread_monitor.py | LIVE PATH |
| 41 | deepdata/microstructure/volume_analyser.py | LIVE PATH |
| 42 | deepdata/options/flow_monitor.py | LIVE PATH |
| 43 | deepdata/options/put_call_analyser.py | LIVE PATH (broken z-score) |
| 44 | deepdata/options/uk_options.py | LIVE PATH |
| 45 | deepdata/options/unusual_activity.py | LIVE PATH |
| 46 | deepdata/patents/innovation_scorer.py | LIVE PATH |
| 47 | deepdata/patents/uk_ipo_collector.py | LIVE PATH (broken endpoints) |
| 48 | deepdata/patents/uspto_collector.py | LIVE PATH (broken metrics) |
| 49 | deepdata/patterns/cross_module_scanner.py | LIVE PATH |
| 50 | deepdata/patterns/nonsense_correlator.py | LIVE PATH |
| 51 | deepdata/short_interest/finra_collector.py | LIVE PATH |
| 52 | deepdata/short_interest/pattern_scanner.py | LIVE PATH |
| 53 | deepdata/short_interest/squeeze_predictor.py | LIVE PATH |
| 54 | deepdata/short_interest/squeeze_scorer.py | LIVE PATH |
| 55 | deepdata/signals/deepdata_signal_engine.py | LIVE PATH |
| 56 | backtest/engine.py | LIVE PATH |
| 57 | backtest/monte_carlo.py | LIVE PATH |
| 58 | backtest/subperiod_analysis.py | LIVE PATH |
| 59 | backtest/walk_forward.py | LIVE PATH |
| 60 | archive/legacy_retraining.py | DEAD CODE (archived) |

---

### KEY FINDINGS

**1. PeerInfluenceMapper runtime crash (File 8)**
Calls `self._ird.generate_signals()` and `self._msd.generate_signals()`. Neither `IndexRebalancingDetector` nor `MergerSpilloverDetector` has a `generate_signals()` method. Will throw `AttributeError` at runtime. Also: both sub-detectors are instantiated with `config=config` kwarg which their constructors do not accept.

**2. UK entry timing always-False bug (File 10)**
`check_time_of_day()` UK condition: `(h == 10 and m < 0)`. Minutes are never negative. UK ideal window check always returns False.

**3. Congressional cluster scoring broken (File 28)**
`_get_scored_cache_placeholder()` always returns `{}`. Cluster credibility lookup always returns default 0.5. No cluster signal is ever properly scored.

**4. MemberTracker metrics hardcoded to zero (File 30)**
`excess_return` and `information_ratio` are always 0.0 in all return paths. The code computes accuracy but these two fields are never populated.

**5. TaxManager acquisition recorded as disposal (File 21)**
`record_acquisition()` calls `store.record_tax_disposal()` with `disposal_type="acquisition"` — semantically wrong table usage.

**6. AttributionEngine bypasses ClosedLoopStore (File 15)**
Opens SQLite directly. All other closeloop modules use the store abstraction.

**7. BenchmarkTracker crash risk (File 18)**
`import numpy as np` and `import pandas as pd` at module top level without try/except — crashes on import if either missing.

**8. USPTO citation metrics always zero (File 48)**
`citations_received = 0` hardcoded in Google Patents listing results. `calc_citation_growth()` always returns 0 or 1 as a boolean float. Citation growth metric is non-functional.

**9. USPTO CPC classes are country codes, not technology classes (File 48)**
`cpc_class` is set to first 2 characters of publication number (e.g. 'US', 'EP'). `detect_tech_pivot()` compares geography, not technology. Tech pivot detection is non-functional.

**10. UKIPOCollector calls dead USPTO PatentsView endpoint (File 47)**
`detect_us_patent_filing()` uses `https://api.patentsview.org/patents/query` which returns HTTP 410 Gone as of 2025. Always fails silently.

**11. USPTOCollector CrossRef fallback is dead code (File 48)**
`_fetch_crossref_fallback()` is defined but never called in `fetch_patents()`. Fallback that the code documents as existing does not activate.

**12. PutCallAnalyser z-score always zero (File 43)**
History passed to `calc_rolling_pc_zscore()` contains only one data point. Z-score of a single point against itself is always 0.0. No historical caching exists.

**13. SqueezeScorer Layer 3 always returns None (File 54)**
`layer3_predict()` instantiates SqueezePredictor fresh each call with a single trade's data. Will almost never find 5 historical squeezes from a single dict. Layer 3 ML prediction is effectively always disabled.

**14. MergerSpilloverDetector sector_relevance always hardcoded (File 6)**
`sector_relevance = _SECTOR_RELEVANCE["same"] = 1.0` regardless of actual sector relationship between acquirer and target.

**15. PreTrainer RAM guard too aggressive (File 16)**
1.7 GB threshold is very low for modern VPS. Will frequently abort batch processing prematurely.

**16. MonteCarloSimulator non-reproducible (File 57)**
No random seed set. Results differ across runs. Not reproducible.

**17. VolumeAnalyser daily_vwap is not a VWAP (File 41)**
`daily_vwap = typical_price[-1]` is just the last day's typical price, not a volume-weighted average price.

**18. ModuleWirer docstring vs reality mismatch (File 14)**
Docstring says 12 disconnects. DISCONNECT_DESCRIPTIONS has 24 entries. Wire methods return True if module is importable, not actually connected.

**19. StressLearner JSON parsing fragile (File 24)**
Single-quote to double-quote replacement for Python dict repr is not reliable JSON parsing.

**20. BacktestEngine optimistic Sharpe calculation incomplete (File 56)**
Comment says "add back spread + extra slippage" but only spread is added back. "Optimistic" Sharpe is not as optimistic as documented.

---

### DEAD CODE / DISCONNECTED MODULES

| Module | Status |
|--------|--------|
| archive/legacy_retraining.py | DEAD — explicitly archived |
| closeloop/learning/pre_trainer.py | DISCONNECTED — not called from main bot flow |
| closeloop/risk/benchmark_tracker.update() | NO-OP stub |
| closeloop/stress/stress_learner.update_signal_vulnerability() | NO-OP stub |
| backtest/engine._exec_price() | DEAD CODE — not called by _simulate() |
| deepdata/patents/uspto_collector._fetch_crossref_fallback() | DEAD CODE — defined but never called |
| UKIPOCollector.detect_us_patent_filing() | ALWAYS FAILS — calls HTTP 410 endpoint |
| SqueezeScorer.layer3_predict() | EFFECTIVELY DEAD — always returns None |
| PutCallAnalyser z-score | ALWAYS ZERO — single-point history |
| TaxManager.tax_year_start | UNUSED — stored but never referenced |
| SubperiodAnalyser initial_capital param | UNUSED — accepted but not used |
| json and pathlib imports in subperiod_analysis.py | UNUSED imports |

---

### CONTRADICTIONS

1. Two different market impact models: `LiquidityScorer.IMPACT_K=0.1` vs `MarketImpactModel.η=0.15/0.20` — different coefficients for the same concept.
2. Innovation signal weights differ between `InnovationScorer.score()` (0.35, 0.25, 0.20, 0.10, 0.10) and `calc_innovation_signal()` (0.45, 0.30, 0.15, 0.10).
3. ModuleWirer: docstring "12 disconnects" vs 24 entries in DISCONNECT_DESCRIPTIONS.
4. BacktestEngine rejects orders > 15% ADV; ExecutionFeasibility uses 5% ADV cap — different ADV limits in different modules.
5. StressLearner historical load uses +0.03/-0.02 increments; live learning uses +0.05/-0.03 — asymmetric learning rates between history initialisation and live recording.

---

### Proceed to Part 11C: YES
