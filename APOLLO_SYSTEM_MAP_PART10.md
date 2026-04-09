# APOLLO SYSTEM MAP — PART 10
## Database Schema Map + Configuration System

Generated: 2026-04-08
Queries run: Steps 1–4 as specified. All data is from live inspection of the running system.

---

# SECTION 6 — DATABASE COMPLETE SCHEMA MAP

## 6A: CLOSELOOP.DB DETAILED AUDIT

**Path:** `closeloop/storage/closeloop.db`
**WAL mode:** `wal`
**Config key:** `closeloop.storage_path`

The canonical trade ledger and learning database. This is the primary write destination for the paper trader and the primary read source for all monitoring, reporting, and attribution modules.

---

### TABLE: trade_ledger — 761 rows

The master record of every trade attempt.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| market | TEXT | yes | — | 0 |
| direction | INTEGER | yes | — | 0 |
| entry_date | TEXT | yes | — | 0 |
| exit_date | TEXT | yes | — | 0 |
| entry_price | REAL | yes | — | 0 |
| exit_price | REAL | yes | — | 0 |
| position_size | REAL | yes | — | 0 |
| gross_pnl | REAL | yes | — | 0 |
| net_pnl | REAL | yes | — | 0 |
| fees_paid | REAL | yes | 0 | 0 |
| holding_days | INTEGER | yes | — | 0 |
| exit_reason | TEXT | yes | — | 0 |
| signals_at_entry | TEXT | yes | — | 0 |
| macro_regime | TEXT | yes | — | 0 |
| vix_level | REAL | yes | — | 0 |
| umci_score | REAL | yes | — | 0 |
| lunar_phase | REAL | yes | — | 0 |
| geomagnetic_kp | REAL | yes | — | 0 |
| market_cap_usd | REAL | yes | — | 0 |
| sector | TEXT | yes | — | 0 |
| entry_timing_score | REAL | yes | — | 0 |
| scale_in_tranche | INTEGER | yes | 0 | 0 |
| peer_influence_score | REAL | yes | 0 | 0 |
| analyst_revision_score | REAL | yes | 0 | 0 |
| academic_tailwind_score | REAL | yes | 0 | 0 |
| news_context_score | REAL | yes | 0 | 0 |
| index_rebalancing_pressure | REAL | yes | 0 | 0 |
| merger_spillover_flag | INTEGER | yes | 0 | 0 |
| was_profitable | INTEGER | yes | — | 0 |
| pnl_pct | REAL | yes | — | 0 |
| annualised_return | REAL | yes | — | 0 |
| pnl_attributed | INTEGER | yes | 0 | 0 |
| attribution_complete | INTEGER | yes | 0 | 0 |
| order_status | TEXT | yes | 'unknown' | 0 |
| is_phantom | INTEGER | yes | 0 | 0 |

**Written by:** `execution/paper_trader.py`, `execution/trading_bot.py`, `closeloop/storage/closeloop_store.py`, `scripts/backfill_and_validate.py`
**Read by:** `closeloop/learning/attribution_engine.py`, `closeloop/learning/pre_trainer.py`, `closeloop/context/analyst_revision_tracker.py`, `analysis/factor_model.py`, `monitoring/chart_generator.py`, `monitoring/dashboard/app.py`, `monitoring/health_dashboard.py`, `monitoring/health_reporter.py`, `monitoring/weekly_report.py`, `monitoring/private_bot.py`, `monitoring/preflight_check.py`, `core/retraining_controller.py`

---

### TABLE: pnl_attribution — 2 rows

Per-signal attribution for each closed trade. Populated by the attribution engine post-exit.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| trade_id | INTEGER | yes | — | 0 |
| signal_name | TEXT | yes | — | 0 |
| signal_source_module | TEXT | yes | — | 0 |
| signal_strength_at_entry | REAL | yes | — | 0 |
| signal_direction | INTEGER | yes | — | 0 |
| attributed_pnl | REAL | yes | — | 0 |
| attributed_pnl_pct | REAL | yes | — | 0 |
| was_signal_correct | INTEGER | yes | — | 0 |
| counterfactual_pnl_without_signal | REAL | yes | — | 0 |
| created_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `closeloop/learning/attribution_engine.py`
**Read by:** `closeloop/learning/pre_trainer.py`, `monitoring/dashboard/app.py`, `monitoring/weekly_report.py`

---

### TABLE: entry_timing_outcomes — 135 rows

Records actual vs. intended entry prices to measure entry timing quality.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| trade_id | INTEGER | yes | — | 0 |
| ticker | TEXT | yes | — | 0 |
| intended_entry_price | REAL | yes | — | 0 |
| actual_entry_price | REAL | yes | — | 0 |
| entry_timing_score | REAL | yes | — | 0 |
| waited_days | INTEGER | yes | 0 | 0 |
| scale_in_tranche | INTEGER | yes | 0 | 0 |
| tranche_entry_price | REAL | yes | — | 0 |
| pnl_vs_immediate_entry | REAL | yes | — | 0 |
| entry_method | TEXT | yes | — | 0 |
| entry_conditions_met | TEXT | yes | — | 0 |
| created_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `closeloop/entry/entry_learner.py`, `execution/paper_trader.py`
**Read by:** `closeloop/learning/pre_trainer.py`, `monitoring/dashboard/app.py`

---

### TABLE: earnings_revisions — 10 rows

Tracks analyst EPS estimate revisions per ticker.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| estimate_date | TEXT | yes | — | 0 |
| eps_estimate | REAL | yes | — | 0 |
| eps_actual | REAL | yes | — | 0 |
| revision_pct | REAL | yes | — | 0 |
| revision_score | REAL | yes | — | 0 |
| stored_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `analysis/earnings_revision_scorer.py`, `closeloop/context/analyst_revision_tracker.py`
**Read by:** `execution/paper_trader.py`, `closeloop/learning/pre_trainer.py`

---

### TABLE: insider_transactions — 424 rows

Insider buy/sell activity for universe tickers.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| insider_name | TEXT | yes | — | 0 |
| title | TEXT | yes | — | 0 |
| transaction_date | TEXT | yes | — | 0 |
| shares | REAL | yes | — | 0 |
| price_per_share | REAL | yes | — | 0 |
| transaction_type | TEXT | yes | — | 0 |
| value_usd | REAL | yes | — | 0 |
| is_ceo_cfo | INTEGER | yes | 0 | 0 |
| filing_url | TEXT | yes | — | 0 |
| stored_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/insider_transaction_collector.py`
**Read by:** `closeloop/learning/pre_trainer.py`, `execution/paper_trader.py`

---

### TABLE: job_postings — 93 rows

Hiring momentum data per ticker.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| company | TEXT | yes | — | 0 |
| collection_date | TEXT | yes | — | 0 |
| total_postings | INTEGER | yes | 0 | 0 |
| engineering_count | INTEGER | yes | 0 | 0 |
| sales_count | INTEGER | yes | 0 | 0 |
| admin_count | INTEGER | yes | 0 | 0 |
| growth_rate | REAL | yes | 0.0 | 0 |
| source | TEXT | yes | — | 0 |
| stored_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/job_postings_collector.py`
**Read by:** `execution/paper_trader.py`, `closeloop/learning/pre_trainer.py`

---

### TABLE: short_interest — 5 rows

Short interest metrics per ticker.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| report_date | TEXT | yes | — | 0 |
| short_interest | REAL | yes | — | 0 |
| float_shares | REAL | yes | — | 0 |
| short_ratio | REAL | yes | — | 0 |
| days_to_cover | REAL | yes | — | 0 |
| short_squeeze_score | REAL | yes | — | 0 |
| updated_at | TEXT | yes | — | 0 |

**Written by:** `data/collectors/short_interest_collector.py`
**Read by:** `execution/paper_trader.py`, `closeloop/learning/pre_trainer.py`

---

### TABLE: cointegration_log — 3 rows

Discovered cointegrated pairs with ADF test results.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker_a | TEXT | yes | — | 0 |
| ticker_b | TEXT | yes | — | 0 |
| hedge_ratio | REAL | yes | — | 0 |
| intercept | REAL | yes | — | 0 |
| half_life_days | REAL | yes | — | 0 |
| adf_pvalue | REAL | yes | — | 0 |
| correlation | REAL | yes | — | 0 |
| discovered_date | TEXT | yes | — | 0 |
| last_tested | TEXT | yes | — | 0 |
| is_active | INTEGER | yes | 1 | 0 |

**Written by:** `analysis/pairs_trader_live.py`
**Read by:** `analysis/pairs_trader_live.py`

---

### TABLE: pairs_signals — 0 rows

Live z-score signals from pairs trading.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker_a | TEXT | yes | — | 0 |
| ticker_b | TEXT | yes | — | 0 |
| z_score | REAL | yes | — | 0 |
| hedge_ratio | REAL | yes | — | 0 |
| half_life_days | REAL | yes | — | 0 |
| signal | INTEGER | yes | — | 0 |
| signal_date | TEXT | yes | — | 0 |
| spread | REAL | yes | — | 0 |
| spread_mean | REAL | yes | — | 0 |
| spread_std | REAL | yes | — | 0 |

**Written by:** `analysis/pairs_trader_live.py`
**Read by:** `execution/paper_trader.py`

---

### TABLE: factor_exposures — 7 rows

Fama-French 6-factor exposures per ticker per run date.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| run_date | TEXT | yes | — | 0 |
| alpha | REAL | yes | — | 0 |
| beta_mkt | REAL | yes | — | 0 |
| beta_smb | REAL | yes | — | 0 |
| beta_hml | REAL | yes | — | 0 |
| beta_rmw | REAL | yes | — | 0 |
| beta_cma | REAL | yes | — | 0 |
| beta_mom | REAL | yes | — | 0 |
| r_squared | REAL | yes | — | 0 |

**Written by:** `analysis/factor_model.py`
**Read by:** `closeloop/learning/pre_trainer.py`, `monitoring/dashboard/app.py`

---

### TABLE: frontier_signal_validation — 23 rows

Results from closeloop's internal validation of frontier signals.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| signal_name | TEXT | yes | — | 0 |
| run_date | TEXT | yes | — | 0 |
| n_obs | INTEGER | yes | — | 0 |
| correlation | REAL | yes | — | 0 |
| t_stat | REAL | yes | — | 0 |
| p_value | REAL | yes | — | 0 |
| sharpe | REAL | yes | — | 0 |
| status | TEXT | yes | — | 0 |
| weight | REAL | yes | 0.0 | 0 |

**Written by:** `analysis/frontier_validator.py`
**Read by:** `monitoring/dashboard/app.py`, `monitoring/weekly_report.py`

---

### TABLE: signal_regime_performance — 2 rows

Per-signal performance broken down by macro regime and VIX bucket.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| signal_name | TEXT | yes | — | 0 |
| macro_regime | TEXT | yes | — | 0 |
| vix_bucket | TEXT | yes | — | 0 |
| n_trades | INTEGER | yes | 0 | 0 |
| win_rate | REAL | yes | — | 0 |
| mean_pnl | REAL | yes | — | 0 |
| sharpe | REAL | yes | — | 0 |
| best_trade_pnl | REAL | yes | — | 0 |
| worst_trade_pnl | REAL | yes | — | 0 |
| last_updated | TEXT | yes | datetime('now') | 0 |

**Written by:** `closeloop/learning/attribution_engine.py`
**Read by:** `monitoring/dashboard/app.py`, `monitoring/weekly_report.py`

---

### TABLE: stress_learning_outcomes — 3 rows

Historical accuracy of stress scenario predictions.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| scenario_name | TEXT | yes | — | 0 |
| run_date | TEXT | yes | — | 0 |
| predicted_loss_pct | REAL | yes | — | 0 |
| actual_loss_pct_if_occurred | REAL | yes | — | 0 |
| signals_flagged_vulnerable | TEXT | yes | — | 0 |
| signals_actually_failed | TEXT | yes | — | 0 |
| prediction_accuracy | REAL | yes | — | 0 |
| used_for_weight_update | INTEGER | yes | 0 | 0 |
| created_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `closeloop/stress/` module
**Read by:** `closeloop/learning/pre_trainer.py`, `monitoring/weekly_report.py`

---

### Empty / Unfilled Tables in closeloop.db

The following tables exist in the schema but contain 0 rows as of 2026-04-08:

| Table | Purpose |
|-------|---------|
| academic_company_matches | Links academic papers to tickers for tailwind scoring |
| analyst_revision_outcomes | Tracks whether analyst revision signals predicted actual returns |
| benchmark_performance | Rolling comparison vs SPX/FTSE/smallcap |
| drawdown_events | Forensics log of drawdown events |
| peer_influence_outcomes | Tracks accuracy of peer-effect predictions |
| signal_interactions | Tracks synergistic or conflicting signal combinations |
| signal_weights | Live adaptive signal weights (replaced by attribution) |
| stress_predictions | Forward-looking stress scenario predictions |
| tax_ledger | CGT tracking (UK) |
| trade_detail_log | Redundant detailed per-trade snapshot (separate from trade_ledger) |
| weight_history | History of signal weight changes |

**Written by:** Various closeloop sub-modules; none have fired yet at this stage of the system lifecycle.

---

### 6A AUDIT RESULTS — QUERY OUTPUT

#### ORDER STATUS BREAKDOWN (trade_ledger, 761 rows total)

| order_status | count | open (no exit_date) | closed (has exit_date) |
|-------------|-------|---------------------|------------------------|
| unknown | 471 | 0 | 471 |
| superseded | 290 | 65 | 225 |

Notes:
- `unknown` is the default value for `order_status`. All 471 rows with this status are closed (have exit_date).
- `superseded` captures 290 records where a later deduplication/backfill pass marked the row. 65 of these have no exit_date (still recorded as open at time of supersession).
- There is no `filled`, `cancelled`, or `open` status present. The field appears to reflect post-hoc classification only.

---

#### EXIT REASON BREAKDOWN (closed trades only)

| exit_reason | count |
|-------------|-------|
| signal_reversal | 305 |
| phantom_duplicate | 150 |
| phantom_cleanup | 145 |
| NULL | 51 |
| atr_stop | 33 |
| volume_dry_up | 11 |
| test | 1 |

Notes:
- 305 exits via `signal_reversal` are legitimate system-driven exits.
- 295 exits are classified as phantom (`phantom_duplicate` = 150, `phantom_cleanup` = 145). These are artefacts of deduplication runs and are not real trades.
- 51 rows have NULL exit_reason despite having an exit_date. These represent entries where the exit path did not populate the reason field.
- Only 33 exits via `atr_stop` and 11 via `volume_dry_up` represent stop/filter-triggered exits.
- 1 test trade exists.

---

#### SIGNALS_AT_ENTRY STATUS (closed trades only)

| status | count |
|--------|-------|
| EMPTY | 677 |
| FILLED | 19 |

Notes:
- 97.3% of closed trades have no signals captured in `signals_at_entry`. This field is supposed to be a JSON blob of all signal values at time of entry. It is almost universally not populated.
- Only 19 trades (2.7%) have a meaningful signals_at_entry value.
- This is a critical gap for attribution and post-hoc analysis.

---

#### PNL_ATTRIBUTION STATUS (all 761 rows)

| attribution_complete | count |
|---------------------|-------|
| 0 (not complete) | 553 |
| 1 (complete) | 208 |

Notes:
- 72.7% of trades have not completed PnL attribution.
- Only 208 trades (27.3%) have `attribution_complete = 1`.
- The `pnl_attribution` table itself has only 2 rows, meaning the attribution table is almost empty despite 208 trades flagged as attributed. This suggests the flag may be set independently of the table population, or the table was truncated.

---

#### TRADE DATE RANGE

- Entry dates: `2026-04-02T14:23:03` to `2026-04-08T14:47:41`
- Exit dates: `2026-04-02 17:38:16` to `2026-04-08T15:06:02`

Notes:
- All 761 trades span only 6 days (2026-04-02 to 2026-04-08). The system has been running in paper trading mode for less than one week.
- Entry date format is inconsistent: some rows use ISO datetime with T separator, others use space separator. This reflects the evolution of the timestamp format during the week.

---

## 6B: ALL OTHER DATABASES

---

### COOLING_OFF.DB

**Path:** `closeloop/storage/cooling_off.db`
**WAL mode:** `delete` (not WAL — rollback journal mode)
**Config key:** `feature_flags.cooling_off_tracker`

#### TABLE: cooling_off — 0 rows

Tracks tickers currently in a post-exit cooling-off period to prevent immediate re-entry.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| ticker | TEXT | no | — | 1 |
| exit_date | TEXT | yes | — | 0 |
| exit_price | REAL | yes | — | 0 |
| pnl_pct | REAL | yes | — | 0 |
| exit_reason | TEXT | yes | — | 0 |
| locked_at | TEXT | yes | — | 0 |

**Written by:** `execution/cooling_off_tracker.py`, `execution/paper_trader.py`
**Read by:** `execution/paper_trader.py`, `monitoring/preflight_check.py`

Note: This database uses DELETE journal mode, not WAL. It is a high-frequency read/write database (checked before every trade entry) so the non-WAL mode may cause contention under load.

---

### HISTORICAL_DB.DB

**Path:** `output/historical_db.db`
**WAL mode:** `wal`

Primary storage for price history, fundamentals, macro data, and alternative structured datasets.

#### TABLE: price_history — 48,510 rows

OHLCV price data per ticker per date.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| date | TEXT | yes | — | 0 |
| open/high/low/close | REAL | yes | — | 0 |
| adj_close | REAL | yes | — | 0 |
| volume | REAL | yes | — | 0 |
| source | TEXT | yes | 'yfinance' | 0 |
| delisted | INTEGER | yes | 0 | 0 |

**Written by:** `data/historical_collector.py`, `data/historical_db.py`
**Read by:** `analysis/factor_model.py`, `analysis/pairs_trader.py`, `analysis/macro_signal_engine.py`, `execution/paper_trader.py`, `monitoring/chart_generator.py`, `monitoring/dashboard/app.py`

---

#### TABLE: quarterly_financials — 32 rows

Income statement data per ticker per period.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| period | TEXT | yes | — | 0 |
| period_type | TEXT | yes | 'quarterly' | 0 |
| revenue/gross_profit/operating_income/net_income/ebitda | REAL | yes | — | 0 |
| eps_basic/eps_diluted | REAL | yes | — | 0 |
| shares_outstanding | REAL | yes | — | 0 |
| gross_margin/operating_margin/net_margin | REAL | yes | — | 0 |
| revenue_growth_yoy | REAL | yes | — | 0 |
| source | TEXT | yes | 'yfinance' | 0 |

**Written by:** `data/historical_collector.py`
**Read by:** `analysis/factor_model.py`, `execution/paper_trader.py`

---

#### TABLE: balance_sheet — 33 rows

Balance sheet data per ticker per period.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| period/period_type | TEXT | yes | 'quarterly' | 0 |
| total_assets/liabilities/equity | REAL | yes | — | 0 |
| cash_and_equiv/total_debt/net_debt | REAL | yes | — | 0 |
| current_assets/current_liabilities | REAL | yes | — | 0 |
| current_ratio/debt_to_equity | REAL | yes | — | 0 |
| book_value_per_share | REAL | yes | — | 0 |
| source | TEXT | yes | 'yfinance' | 0 |

**Written by:** `data/historical_collector.py`
**Read by:** `analysis/factor_model.py`

---

#### TABLE: cash_flow — 32 rows

Cash flow statement data per ticker per period.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| period/period_type | TEXT | yes | 'quarterly' | 0 |
| operating_cf/investing_cf/financing_cf | REAL | yes | — | 0 |
| capex/free_cash_flow | REAL | yes | — | 0 |
| dividends_paid/buybacks | REAL | yes | — | 0 |
| source | TEXT | yes | 'yfinance' | 0 |

**Written by:** `data/historical_collector.py`
**Read by:** `analysis/factor_model.py`

---

#### TABLE: macro_series — 25,995 rows

FRED macro series time series data.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| series_id/series_name | TEXT | yes | — | 0 |
| date | TEXT | yes | — | 0 |
| value | REAL | yes | — | 0 |
| source | TEXT | yes | 'fred' | 0 |
| collected_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/rates_credit_collector.py`
**Read by:** `analysis/macro_signal_engine.py`

---

#### TABLE: rates_data — 245,227 rows

The largest table in historical_db. Raw interest rate and credit series from FRED and other sources.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| source/series_id/series_name | TEXT | yes | — | 0 |
| obs_date | TEXT | yes | — | 0 |
| value | REAL | yes | — | 0 |
| fetched_at | TEXT | yes | — | 0 |

**Written by:** `data/collectors/rates_credit_collector.py`
**Read by:** `analysis/macro_signal_engine.py`

---

#### TABLE: rates_signals — 12,458 rows

Derived rate signals (yield curve slope, inversions, credit stress, etc.).

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| calc_date | TEXT | yes | — | 0 |
| yield_curve_slope/inversion_depth | REAL | yes | — | 0 |
| inversion_duration | INTEGER | yes | — | 0 |
| yield_momentum_10yr | REAL | yes | — | 0 |
| yields_rising_fast | INTEGER | yes | — | 0 |
| credit_stress_level/hy_spread/ig_spread/ted_spread | REAL | yes | — | 0 |
| breakeven_inflation | REAL | yes | — | 0 |
| rates_regime | TEXT | yes | — | 0 |
| fetched_at | TEXT | yes | — | 0 |

**Written by:** `data/collectors/rates_credit_collector.py`
**Read by:** `analysis/macro_signal_engine.py`, `execution/paper_trader.py`

---

#### TABLE: commodity_prices — 161,428 rows

Daily OHLCV for commodity tickers via yfinance.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| symbol/name | TEXT | yes | — | 0 |
| date | TEXT | yes | — | 0 |
| open/high/low/close/adj_close/volume | REAL | yes | — | 0 |
| source | TEXT | yes | 'yfinance' | 0 |
| collected_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/commodity_collector.py`
**Read by:** `analysis/macro_signal_engine.py`

---

#### TABLE: payment_processor_signals — 12,270 rows

Composite signals derived from payment processor stock price behaviour as consumer spending proxy.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker/date/close | TEXT/REAL | yes | — | 0 |
| return_30d/composite | REAL | yes | — | 0 |
| collected_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/consumer_intelligence.py`
**Read by:** `analysis/macro_signal_engine.py`

---

#### TABLE: shipping_data — 4,090 rows

Baltic Dry Index and shipping stress signals.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| date | TEXT | yes | — | 0 |
| bdi_value/bdi_ma5/bdi_ma20/bdi_ma60 | REAL | yes | — | 0 |
| bdi_zscore_252/bdi_pct_rank | REAL | yes | — | 0 |
| bdi_roc_1w/bdi_roc_4w/bdi_source | REAL/TEXT | yes | — | 0 |
| stock_composite_zscore/shipping_stress_index | REAL | yes | — | 0 |
| stress_regime | TEXT | yes | — | 0 |
| fetched_at | TEXT | yes | — | 0 |

**Written by:** `data/collectors/shipping_intelligence.py`
**Read by:** `analysis/macro_signal_engine.py`, `execution/paper_trader.py`

---

#### TABLE: weather_data — 152 rows

Weather observations for key US and UK cities.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| location/country | TEXT | yes | — | 0 |
| date | TEXT | yes | — | 0 |
| source | TEXT | yes | — | 0 |
| temp_max/temp_min/temp_mean | REAL | yes | — | 0 |
| precipitation/snowfall/windspeed_max | REAL | yes | — | 0 |
| weathercode | INTEGER | yes | — | 0 |
| temperature_anomaly/precip_anomaly | REAL | yes | — | 0 |
| weather_risk_score | REAL | yes | — | 0 |
| is_extreme | INTEGER | yes | 0 | 0 |
| collected_at | TEXT | yes | datetime('now') | 0 |

**Written by:** `data/collectors/geographic_intelligence.py`
**Read by:** `execution/paper_trader.py`

---

#### TABLE: sector_rotation — 30 rows

Top/bottom sectors and rotation signals per date.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| date | TEXT | yes | — | 0 |
| top_sectors/bottom_sectors | TEXT | yes | — | 0 |
| rotating_in/rotating_out | TEXT | yes | — | 0 |
| sector_scores | TEXT | yes | — | 0 |
| calculated_at | TEXT | yes | — | 0 |

**Written by:** `data/collectors/technology_intelligence.py` or a sector rotation module
**Read by:** `execution/paper_trader.py`, `monitoring/dashboard/app.py`

---

#### TABLE: crowding_risk — 2 rows

Portfolio-level crowding index to scale position size.

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| date/crowding_index/label | TEXT/REAL | yes | — | 0 |
| size_multiplier | REAL | yes | — | 0 |
| short_interest_score/correlation_score/institutional_score/dispersion_score | REAL | yes | — | 0 |
| calculated_at | TEXT | yes | — | 0 |

**Written by:** `analysis/crowding_detector.py`
**Read by:** `execution/paper_trader.py`

---

#### TABLE: cointegration_log (historical) — 10 rows

Pairs cointegration test results stored in historical_db (separate from closeloop.db copy).

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker_a/ticker_b | TEXT | yes | — | 0 |
| tested_at | TEXT | yes | — | 0 |
| p_value/hedge_ratio/half_life/correlation | REAL | yes | — | 0 |
| passed_filter | INTEGER | yes | 0 | 0 |

**Written by:** `analysis/pairs_trader.py`
**Read by:** `analysis/pairs_trader_live.py`

---

#### Empty / Unfilled Tables in historical_db.db

| Table | Purpose |
|-------|---------|
| macro_context | Daily macro snapshot (SPX, VIX, rates, FX, sector ETFs, commodities) |
| news_context | Per-ticker news headlines with sentiment |
| pairs_signals | Live pairs trading signals |
| insider_transactions | SEC Form 4 insider data (separate from closeloop copy) |
| institutional_ownership | 13F institutional ownership data |
| edgar_filings | SEC filing index |
| proxy_data | DEF14A executive compensation data |
| earnings_enriched | Enriched earnings events with all contextual data joined |
| delisted_companies | Universe management for delisted tickers |

---

### PERMANENT_ARCHIVE.DB

**Path:** `output/permanent_archive.db`
**WAL mode:** `wal`

The largest database. Stores raw ingested data intended for permanent retention, including raw news articles, geopolitical events, commodity history, macro data, and government data.

#### Notable Tables by Row Count

| Table | Rows | Description |
|-------|------|-------------|
| raw_macro_data | 17,576,284 | Raw FRED/BLS macro series (all series, all dates) |
| raw_commodity_prices | 10,169,551 | Raw commodity OHLCV data before aggregation |
| raw_geopolitical_events | 120,049 | GDELT and other geopolitical event records |
| raw_geopolitical_events_fts | 120,049 | FTS5 mirror for full-text search |
| raw_geopolitical_events_fts_docsize | 120,049 | FTS5 docsize shadow table |
| sec_fulltext_alerts | 135,437 | SEC EFTS keyword alerts with snippets |
| raw_articles | 11,831 | Full-text news articles |
| raw_articles_fts | 11,831 | FTS5 mirror for article search |
| raw_articles_fts_docsize | 11,831 | FTS5 docsize shadow |
| tech_knowledge_graph | 64 | Ticker-to-tech-theme mappings |
| tech_intelligence | 2,588 | Tech trend metrics |
| source_credibility | 18 | News source trust scores |
| bls_data | 20,350 | BLS labour market series |
| fda_calendar | 638 | FDA PDUFA dates |
| earnings_quality | 365 | Earnings quality scores per ticker |
| narrative_shifts | 134 | Narrative sentiment shift scores |
| raw_government_contracts | 172 | Government contract awards |
| raw_shipping_data | 6,056 | Raw shipping index data |
| commodity_lead_lag | 10 | Commodity-to-sector lead-lag correlations |
| calendar_signals | 5 | Seasonal calendar effect scores |
| article_connections | 3,128 | Cross-article relationship graph |
| raw_weather_data | 480 | Raw weather API responses |
| delisted_tickers | 6 | Archive of delisted tickers |
| quantitative_claims | 3 | Verified quantitative claims from articles |
| signals_log | 2 | Signal log entries |
| earnings_context_log | 1 | Macro context snapshots at earnings events |

**Written by:** `data/collectors/advanced_news_intelligence.py`, `data/collectors/geopolitical_collector.py`, `data/collectors/commodity_collector.py`, `data/collectors/rates_credit_collector.py`, `data/collectors/shipping_intelligence.py`, `data/collectors/government_data_collector.py`, `data/collectors/sec_fulltext_collector.py`, `data/collectors/technology_intelligence.py`, `data/collectors/social_influence_tracker.py`, `data/collectors/regulatory_intelligence.py`, `analysis/symbolic_regression.py`, `analysis/macro_signal_engine.py`, `analysis/signal_decay_monitor.py`, `analysis/threshold_optimizer.py`
**Read by:** `analysis/frontier_validator.py`, `analysis/macro_signal_engine.py`, `analysis/signal_decay_monitor.py`, `execution/adaptive_position_sizer.py`

#### Empty / Unfilled Tables in permanent_archive.db

| Table | Purpose |
|-------|---------|
| raw_congressional_trades | Congressional trade disclosures |
| raw_filings | Full-text SEC filings |
| raw_insider_transactions | Raw insider data before processing |
| raw_social_posts | Reddit/social media posts |
| phase_history | Bot operational phase transitions |
| predictions_log | Forward prediction log |
| discovered_equations | Symbolic regression discoveries |

---

### FRONTIER.DB

**Path:** `frontier/storage/frontier.db`
**WAL mode:** `wal`
**Config key:** `frontier.storage_path`

Frontier module's own database for signal research pipeline state.

| Table | Rows | Description |
|-------|------|-------------|
| umci_history | 20 | History of UMCI (Universal Market Complexity Index) readings |
| raw_signals | 6 | Raw signals collected by frontier collectors |
| evidence_records | 0 | Per-signal promotion evidence log |
| interaction_log | 0 | Signal interaction test log |
| parameter_history | 0 | Signal parameter drift history |
| signal_log | 0 | Signals that have been issued |
| validation_results | 0 | Validation test results per signal |
| watchlist | 0 | Signals under evaluation |

#### TABLE: umci_history — 20 rows (full schema)

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| umci | REAL | yes | — | 0 |
| level | TEXT | yes | — | 0 |
| physical/social/scientific/financial/altdata | REAL | yes | — | 0 |
| dominant_dim | TEXT | yes | — | 0 |
| position_mult | REAL | yes | — | 0 |
| halt | INTEGER | yes | 0 | 0 |
| full_breakdown | TEXT | yes | — | 0 |
| recorded_at | TEXT | yes | — | 0 |

**Written by:** `frontier/` complexity index module
**Read by:** `execution/paper_trader.py`, `monitoring/private_bot.py`, `monitoring/weekly_report.py`, `monitoring/dashboard/app.py`

---

### DEEPDATA.DB

**Path:** `deepdata/storage/deepdata.db`
**WAL mode:** `wal`
**Config key:** `deepdata.storage_path`

Stores alternative data signals: options flow, congressional trades, short interest, supply chain, transcripts, patents.

| Table | Rows | Description |
|-------|------|-------------|
| congressional | 12 | Congressional member trade filings |
| options_flow | 25 | Options SMFI, IV rank, put/call ratio per ticker |
| short_interest | 5 | Short interest and squeeze probability per ticker |
| congressional_members | 0 | Member accuracy track records |
| deepdata_signals | 0 | Composed deepdata signals |
| earnings_quality | 0 | Per-earnings beat quality analysis |
| factor_exposures | 0 | Custom factor exposures |
| patent_data | 0 | Patent filing data |
| pattern_registry | 0 | Cross-module pattern discoveries |
| squeeze_events | 0 | Documented short squeeze events |
| supply_chain | 0 | Supplier-customer relationship graph |
| transcripts | 0 | Earnings call linguistic analysis |

#### TABLE: congressional — 12 rows (full schema)

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| member | TEXT | yes | — | 0 |
| chamber | TEXT | yes | — | 0 |
| ticker | TEXT | yes | — | 0 |
| transaction_type | TEXT | yes | — | 0 |
| amount_min/amount_max | REAL | yes | — | 0 |
| transaction_date/filing_date | TEXT | yes | — | 0 |
| delay_days | INTEGER | yes | — | 0 |
| signal_strength | REAL | yes | — | 0 |
| credibility | TEXT | yes | — | 0 |
| committee_power | REAL | yes | — | 0 |
| collected_at | TEXT | yes | — | 0 |

#### TABLE: options_flow — 25 rows (full schema)

| Column | Type | Nullable | Default | PK |
|--------|------|----------|---------|-----|
| id | INTEGER | no | — | 1 |
| ticker | TEXT | yes | — | 0 |
| market | TEXT | yes | — | 0 |
| smfi | REAL | yes | — | 0 |
| iv_rank | REAL | yes | — | 0 |
| put_call_ratio | REAL | yes | — | 0 |
| net_gamma | REAL | yes | — | 0 |
| dark_pool_score | REAL | yes | — | 0 |
| unusual_activity | INTEGER | yes | 0 | 0 |
| raw | TEXT | yes | — | 0 |
| collected_at | TEXT | yes | — | 0 |

**Written by:** `deepdata/` module collectors
**Read by:** `execution/paper_trader.py`, `monitoring/private_bot.py`, `monitoring/weekly_report.py`, `monitoring/dashboard/app.py`, `scripts/backfill_and_validate.py`

---

## 6C: DATABASES NOT IN ORIGINAL LIST

The following `.db` files were found on disk that were not in the original audit list. They are documented here with schema and row counts.

### Root-level stubs (0 bytes, no tables)

These files exist but are empty placeholders, likely created by an early startup attempt before storage paths were standardized to subdirectories:

| File | Size | Note |
|------|------|------|
| `closeloop.db` | 0 bytes | Empty placeholder |
| `closeloop_data.db` | 98 KB | Schema matches closeloop.db but all tables empty except stress_learning_outcomes (3 rows) — appears to be an old initialization artifact |
| `deepdata.db` | 0 bytes | Empty placeholder |
| `frontier.db` | 0 bytes | Empty placeholder |
| `historical.db` | 0 bytes | Empty placeholder |
| `permanent.db` | 0 bytes | Empty placeholder |
| `data/market_data.db` | 0 bytes | Empty placeholder |
| `altdata/storage/altdata.db` | 0 bytes | Empty placeholder (real altdata storage is `output/altdata.db`) |

### output/altdata.db — 204 KB

Altdata module secondary storage.

| Table | Rows | Description |
|-------|------|-------------|
| raw_data | 211 | Raw collected altdata values (source, ticker, data_type, value) |
| alt_data_pnl | 0 | PnL tracking per altdata signal |
| anomaly_candidates | 0 | Anomaly signals under evaluation |
| features | 0 | ML feature snapshots |
| model_versions | 0 | Altdata model version registry |
| notifications | 0 | Notification log |
| sentiment_scores | 0 | Sentiment scores (schema exists, table empty) |

**Written by:** `altdata/` module
**Read by:** `execution/paper_trader.py`

---

### data/cache/stocktwits_baseline.db — 12 KB

| Table | Rows | Description |
|-------|------|-------------|
| baseline | 0 | Stocktwits bull ratio and watcher count baseline (ticker, date, bull_ratio, watcher_count) |

**Written by:** altdata Stocktwits collector
**Read by:** altdata collector for sentiment normalization

---

### output/earnings.db — 65 KB

PEAD-specific earnings data store.

| Table | Rows | Description |
|-------|------|-------------|
| earnings_calendar_forward | 1 | Upcoming earnings dates |
| pre_earnings_snapshots | 1 | All signal context captured pre-earnings |
| earnings_observations | 0 | Post-earnings outcome tracking |

**Written by:** PEAD signal module, `execution/paper_trader.py`
**Read by:** Signal engine, closeloop learning

---

### output/insider_analysis.db — 45 KB

Processed insider transaction analysis.

| Table | Rows | Description |
|-------|------|-------------|
| insider_transactions | 5 | Classified insider transactions with signal scoring |
| insider_cluster_state | 0 | Per-ticker buy/sell cluster state |
| insider_track_records | 0 | Per-insider historical accuracy |

**Written by:** `data/collectors/insider_transaction_collector.py`
**Read by:** `execution/paper_trader.py`

---

### output/intelligence_db.db — 94 KB

Cross-signal intelligence and pattern discovery.

| Table | Rows | Description |
|-------|------|-------------|
| company_profiles | 0 | Per-ticker PEAD and signal reliability profile |
| cross_asset_correlations | 0 | Discovered cross-asset relationships |
| pattern_discovery | 0 | Multi-feature pattern discoveries |
| readthrough_coefficients | 0 | Large-cap to peer readthrough coefficients |
| signal_effectiveness | 0 | Per-signal effectiveness by regime and sector |

**Written by:** `intelligence/daily_pipeline.py`
**Read by:** `execution/paper_trader.py`

---

### output/permanent_log.db — 61 KB

Permanent event log with FTS5 support.

| Table | Rows | Description |
|-------|------|-------------|
| permanent_log | 0 | Master event log (signals, decisions, alerts) |
| permanent_fts | 0 | FTS5 full-text search mirror |
| prediction_log | 0 | Forward prediction registry |
| weekly_accuracy | 0 | Weekly accuracy summary |

**Written by:** Frontier and closeloop permanent logging subsystem
**Read by:** `monitoring/dashboard/app.py`, `monitoring/weekly_report.py`

---

### simulations/simulation.db — 90 KB

Intraday simulation results.

| Table | Rows | Description |
|-------|------|-------------|
| simulation_runs | 3 | Three completed simulation runs |
| simulation_trades | 147 | Individual trades from simulation runs |
| equity_curves | 147 | Per-timestamp equity curve data |
| simulation_signal_attribution | 0 | Signal attribution for simulation trades |

**Written by:** Simulation engine
**Read by:** `monitoring/dashboard/app.py`

---

### simulations/shadow.db — 24 KB

Shadow model comparison infrastructure (not yet active).

| Table | Rows | Description |
|-------|------|-------------|
| model_registry | 0 | Candidate model versions |
| shadow_decisions | 0 | Live vs. shadow signal comparisons |
| retraining_events | 0 | Retraining trigger events |

**Written by:** `core/retraining_controller.py`
**Read by:** `monitoring/dashboard/app.py`

---

### output/backup_closeloop_pre_backfill_dedup_2026-04-07.db — 328 KB

A backup of closeloop.db taken on 2026-04-07 before the backfill deduplication script was run. This database has the same schema as closeloop.db. Key table counts differ from the live database, showing the state before dedup: `entry_timing_outcomes` had 69 rows (vs 135 now), `insider_transactions` had 165 rows (vs 424 now), `frontier_signal_validation` had 23 rows (same). This is a static reference backup and is not read by any live module.

### Backup files under output/backups/

| File | Description |
|------|-------------|
| `output/backups/altdata_20260331.db` | Altdata DB backup 2026-03-31 |
| `output/backups/altdata_20260401.db` | Altdata DB backup 2026-04-01 |
| `output/backups/altdata_20260402.db` | Altdata DB backup 2026-04-02 |
| `output/backups/deepdata_20260328.db` | Deepdata DB backup 2026-03-28 |
| `output/backups/deepdata_20260331.db` | Deepdata DB backup 2026-03-31 |
| `output/backups/deepdata_20260401.db` | Deepdata DB backup 2026-04-01 |
| `output/backups/deepdata_20260402.db` | Deepdata DB backup 2026-04-02 |

These are static point-in-time backups. Not read by any live module.

Note: A second set of all files under `quant-fund/quant-fund/` appears to be a nested duplicate of the project root, likely from a git clone or rsync error. These databases mirror the structure of the root project. They are not the live databases.

---

## SECTION 6 GATE

PASS. All 6 databases from the original list were inspected. All tables and schemas are documented. Audit queries ran successfully. 23 additional .db files were found and documented.

---

# SECTION 10 — CONFIGURATION SYSTEM

**Config file:** `/home/dannyelticala/quant-fund/config/settings.yaml`
**Format:** YAML
**Loaded by:** `main.py` and most operational modules via a common config loader. Modules reference it as `config['section']['key']`.

---

## 10A: CONFIGURATION STRUCTURE

### Top-Level Key: `alpaca`

Controls the Alpaca paper trading broker connection.

| Sub-key | Value | Controls |
|---------|-------|---------|
| base_url | https://paper-api.alpaca.markets | REST API endpoint |
| data_url | https://data.alpaca.markets | Market data endpoint |
| enabled | true | Whether Alpaca integration is active |
| paper_trading | true | Enforces paper trading mode (no real money) |

**Read by:** `execution/paper_trader.py`, `execution/trading_bot.py`

---

### Top-Level Key: `altdata`

Controls the alternative data collection module.

| Sub-key | Controls |
|---------|---------|
| `enabled` | Whether the altdata module runs |
| `collectors.companies_house.api_key` | Companies House API credential |
| `collectors.companies_house.enabled` | Whether Companies House filing collector runs |
| `collectors.companies_house.scan_interval_hours` | Polling frequency (6h) |
| `collectors.companies_house.watch_filing_types` | Filing types to watch: CS01, AA, PSC, TM01, AP01 |
| `collectors.fred.api_key` | FRED API credential (duplicated under `api_keys.fred`) |
| `collectors.fred.enabled` | Whether FRED collector runs |
| `collectors.fred.scan_interval_hours` | Polling frequency (24h) |
| `collectors.fred.series` | FRED series to collect: UNRATE, CPIAUCSL, DGS10, VIXCLS, T10Y2Y, DCOILWTICO, DEXUSUK, RETAILSMNSA, INDPRO |
| `collectors.google_trends.enabled` | Whether Google Trends collector runs |
| `collectors.google_trends.geo_uk/geo_us` | Geographic scopes |
| `collectors.google_trends.lookback_days` | 90 days lookback |
| `collectors.google_trends.scan_interval_hours` | Polling frequency (12h) |
| `collectors.jobs.enabled` | Whether job postings collector runs |
| `collectors.jobs.scan_interval_hours` | Polling frequency (24h) |
| `collectors.jobs.sources` | Sources: bls_api, indeed_rss, reed_uk |
| `collectors.jobs.track_companies` | Whether to track individual companies |
| `collectors.lunar.enabled` | Whether lunar phase tracker runs |
| `collectors.lunar.track_eclipses/perigee/phases` | Which lunar events to record |
| `collectors.news.enabled` | Whether RSS news collector runs |
| `collectors.news.rss_feeds` | Yahoo Finance and BBC Business RSS feeds |
| `collectors.news.scan_interval_minutes` | Polling frequency (20 min) |
| `collectors.news.sources` | Sources: rss_feeds |
| `collectors.reddit.client_id/client_secret` | Reddit API credentials (empty — not configured) |
| `collectors.reddit.enabled` | Whether Reddit collector runs (enabled but credentials empty) |
| `collectors.reddit.lookback_hours` | 48h lookback |
| `collectors.reddit.min_upvotes` | Filter threshold (10) |
| `collectors.reddit.scan_interval_minutes` | Polling frequency (30 min) |
| `collectors.reddit.subreddits` | Subreddits: wsb, stocks, investing, UKInvesting, pennystocks, smallcapstocks |
| `collectors.sec_edgar.enabled` | Whether SEC EDGAR collector runs |
| `collectors.sec_edgar.form_types` | Forms: 4, 13F, 8-K, SC 13G |
| `collectors.sec_edgar.scan_interval_hours` | Polling frequency (4h) |
| `collectors.shipping.enabled` | Whether BDI shipping collector runs |
| `collectors.shipping.scan_interval_hours` | Polling frequency (24h) |
| `collectors.stocktwits.enabled` | Whether Stocktwits collector runs |
| `collectors.stocktwits.min_watchers` | Minimum watcher count filter (5) |
| `collectors.stocktwits.scan_interval_minutes` | Polling frequency (15 min) |
| `collectors.weather.api_key` | OpenWeatherMap API key |
| `collectors.weather.enabled` | Whether weather collector runs |
| `collectors.weather.locations` | UK cities: London, Manchester, Birmingham, Edinburgh, Bristol; US cities: New York, Chicago, Los Angeles, Houston, Seattle |
| `collectors.weather.scan_interval_hours` | Polling frequency (6h) |
| `collectors.wikipedia.enabled` | Whether Wikipedia page-view collector runs |
| `collectors.wikipedia.lookback_days` | 30 days lookback |
| `collectors.wikipedia.scan_interval_hours` | Polling frequency (24h) |
| `collectors.wikipedia.track_edit_frequency/track_page_views` | What to track |
| `learning.model_versioning.*` | Auto-rollback, comparison window, version retention |
| `learning.online_learning.*` | Decay factor (0.995), learning rate (0.01), min samples (100) |
| `learning.weekly_retrain.*` | Sunday 02:00 retrain, 365-day lookback, 80/20 train/validate split |
| `log_path` | logs/altdata.log |
| `model_path` | altdata/models/ |
| `nonsense_threshold` | 0.05 — p-value threshold for rejecting spurious signals |
| `signals.independent_trading` | Whether altdata signals can trigger trades independently |
| `signals.min_confidence` | 0.6 — minimum confidence to issue a signal |
| `signals.notification_methods` | terminal, log_file, desktop_notification |
| `signals.notify_on_signal` | Whether to send notifications on signal generation |
| `storage_path` | altdata/storage/altdata.db |

**Read by:** `altdata/` module, `altdata/collector/reddit_collector.py`, `altdata/notifications/notifier.py`

---

### Top-Level Key: `api_keys`

Central credential store. All keys are in plaintext in the YAML file.

| Sub-key | Service | Status |
|---------|---------|--------|
| adzuna_app_id | Adzuna job postings | Configured |
| adzuna_app_key | Adzuna job postings | Configured |
| alpaca_api_key | Alpaca paper trading | Configured |
| alpaca_secret_key | Alpaca paper trading | Configured |
| alpha_vantage | Alpha Vantage market data | Configured |
| companies_house | Companies House (UK) | Empty string — not configured |
| eia | EIA electricity data | Configured |
| esa_copernicus_user | ESA Copernicus satellite imagery | Configured (email) |
| esa_copernicus_password | ESA Copernicus satellite imagery | Configured |
| finnhub | Finnhub market data | Configured |
| fred | FRED macro data | Configured (also duplicated under `altdata.collectors.fred.api_key`) |
| marketstack | Marketstack market data | Configured |
| nasdaq_data_link | Nasdaq Data Link (Quandl) | Configured |
| news_api | NewsAPI | Configured |
| openweathermap | OpenWeatherMap | Configured (also duplicated under `altdata.collectors.weather.api_key`) |
| polygon | Polygon.io market data | Configured |
| quiver_quant | QuiverQuant congressional data | Empty string — not configured |
| reddit_client_id | Reddit API | Empty string — not configured |
| reddit_client_secret | Reddit API | Empty string — not configured |
| simfin | SimFin financial data | Configured |
| waqi | World Air Quality Index | Configured |

**Read by:** Every data collector module. Each collector reads its specific key from `api_keys`.

---

### Top-Level Key: `backtest`

Controls backtesting parameters.

| Sub-key | Value | Controls |
|---------|-------|---------|
| benchmark_uk | ^FTSE | UK benchmark ticker |
| benchmark_us | ^GSPC | US benchmark ticker |
| end_date | 2026-12-31 | Backtest end date |
| initial_capital | 100,000 | Starting capital |
| monte_carlo_simulations | 1,000 | Monte Carlo iteration count |
| start_date | 2021-01-01 | Backtest start date |
| stop_loss_pct | 0.25 | Default stop-loss level |
| test_pct | 0.15 | Test set proportion |
| train_pct | 0.70 | Training set proportion |
| validate_pct | 0.15 | Validation set proportion |

**Read by:** `backtest/engine.py`, `backtest/monte_carlo.py`, `backtest/walk_forward.py`

---

### Top-Level Key: `closeloop`

Controls the closeloop learning and analysis module.

| Sub-key | Controls |
|---------|---------|
| `autopsy.capture_fields` | 31 fields to capture per trade for post-mortem analysis |
| `autopsy.enabled` | Whether autopsy logging is active |
| `benchmark.track_active_return/track_information_ratio` | Benchmark tracking features |
| `benchmark.uk/us/uk_smallcap/us_smallcap` | Benchmark tickers for 4 comparisons |
| `context.academic_bridge.*` | Academic paper relevance tracking (threshold 0.3, semantic scholar) |
| `context.analyst_revisions.*` | 30-day analyst revision lookback |
| `context.index_rebalancing.*` | Index rebalancing pressure (5-day window, 4 indices) |
| `context.merger_spillover.*` | M&A peer premium multiplier (0.3), 30-day lookback |
| `context.news_financial_context.*` | 4-quarter news financial context |
| `context.peer_influence.*` | Peer influence decay (0.6), max 10 peers |
| `enabled` | Whether closeloop module runs |
| `entry.enabled` | Whether entry timing optimization is active |
| `entry.extension_threshold_atr` | 2.0 ATR extension blocks immediate entry |
| `entry.learn_from_entries` | Whether to record entry timing outcomes |
| `entry.max_wait_days` | 5 days maximum wait before taking entry |
| `entry.min_entries_before_learning` | 20 entries before learning fires |
| `entry.pullback_threshold_pct` | 2% pullback threshold |
| `entry.scale_in.*` | Scale-in parameters: 33%/33%/34% tranches, 3 confirmation days, abort after 10 days |
| `learning.batch_retrain_day/time` | Sunday 03:00 batch retrain |
| `learning.fully_automatic` | Weights update automatically without approval |
| `learning.log_every_weight_change` | Log all weight changes |
| `learning.max_weight_change_per_update` | 15% maximum single update |
| `learning.min_trades_before_weight_update` | 10 trades minimum |
| `learning.online_update_after_each_trade` | Real-time weight updates |
| `learning.soft_update_rate` | 2% soft update rate |
| `learning.weight_bounds` | Min 0.05, Max 3.0 |
| `log_everything` | Log all operations |
| `log_path` | logs/closeloop.log |
| `storage_path` | closeloop/storage/closeloop.db |
| `stress.crisis_scenarios` | Run all crisis scenarios |
| `stress.learning_mode` | Learn from stress test outcomes |
| `stress.output_path` | output/stress_tests/ |
| `stress.prediction_confidence_threshold` | 0.6 |
| `stress.prediction_mode_min_trades` | 50 trades before prediction mode |
| `stress.run_monthly/run_on_new_position` | When stress tests fire |
| `tax.bed_and_breakfast_days` | 30-day B&B rule |
| `tax.cgt_annual_allowance_gbp` | £3,000 CGT allowance |
| `tax.cgt_rate_basic/higher` | 18%/24% CGT rates |
| `tax.jurisdiction` | UK |
| `tax.loss_harvesting` | Whether to optimize tax-loss harvesting |
| `tax.tax_year_end_month` | April |

**Read by:** `closeloop/` module, `closeloop/entry/entry_learner.py`, `execution/paper_trader.py`

---

### Top-Level Key: `costs`

Trading cost model for both markets.

| Sub-key | Value | Controls |
|---------|-------|---------|
| `uk.commission_per_trade_gbp` | £3.00 | Per-trade commission |
| `uk.short_availability_check` | true | Check short availability before shorting |
| `uk.short_borrow_daily` | 0.02% | Daily short borrow rate |
| `uk.slippage_pct` | 0.2% | Slippage assumption |
| `uk.stamp_duty_pct` | 0.5% | UK stamp duty on buys |
| `us.commission_per_trade` | $1.00 | Per-trade commission |
| `us.short_borrow_daily` | 0.01% | Daily short borrow rate |
| `us.slippage_pct` | 0.1% | Slippage assumption |
| `us.stamp_duty_pct` | 0.0% | No stamp duty |

**Read by:** `execution/paper_trader.py`, `execution/trading_bot.py`, `backtest/engine.py`

---

### Top-Level Key: `dashboard`

Web dashboard server settings.

| Sub-key | Value | Controls |
|---------|-------|---------|
| host | 0.0.0.0 | Bind address (all interfaces) |
| password | apollo2026 | Dashboard login password (plaintext) |
| port | 8080 | HTTP port |

**Read by:** `monitoring/dashboard/app.py`

---

### Top-Level Key: `deepdata`

Controls the deepdata module (options, congressional, earnings quality, transcripts, etc.).

| Sub-key | Controls |
|---------|---------|
| `congressional.*` | 1-year accuracy window, min 10 trades, 5 tracked committees |
| `earnings_quality.*` | 5 beat quality categories, weights for guidance/revenue/cash flow |
| `enabled` | Whether deepdata module runs |
| `factors.*` | 5 standard + 4 custom factors, weekly update frequency |
| `log_path` | logs/deepdata.log |
| `microstructure.*` | 30-min execution window, 5% ADV liquidity threshold, square root market impact model |
| `model_path` | deepdata/models/ |
| `options.*` | US primary/UK secondary, 30-min scan, 6 tracked metrics, 95th percentile unusual threshold |
| `patents.*` | 180-day lead time, USPTO/UK IPO sources |
| `patterns.*` | 5 cross-module test combinations, 30 minimum observations |
| `short_interest.*` | FINRA source, biweekly update, squeeze thresholds defined |
| `storage_path` | deepdata/storage/deepdata.db |
| `supply_chain.*` | 3-level relationship depth, 0.7 readthrough decay |
| `transcripts.*` | FinBERT enabled, 11 linguistic features tracked |

**Read by:** `deepdata/` module

---

### Top-Level Key: `feature_flags`

Binary on/off switches for major system features. All are `true`.

| Flag | Controls |
|------|---------|
| alpaca_rate_limiter | Rate limiting on Alpaca API calls |
| attribution_engine | PnL attribution module |
| cooling_off_tracker | Post-exit cooling period enforcement |
| external_source_monitor | External data source health monitoring |
| market_timer | Market hours gating |
| milestone_tracker | Portfolio milestone detection |
| multi_timeframe | Multi-timeframe signal confirmation |
| news_context_enricher | News context enrichment at signal time |
| pre_trainer | Pre-training module for ML models |
| regime_detector | Macro regime detection |
| risk_filters | Pre-trade risk filter checks |
| sector_rotation | Sector rotation signal |
| server_monitor | Server health monitoring |
| signal_contradiction | Signal contradiction detection |
| technical_indicators | Technical indicator computation |
| trailing_stops | Trailing stop logic |

**Read by:** `execution/paper_trader.py`, `main.py`

---

### Top-Level Key: `filters`

Pre-trade signal filters.

| Sub-key | Value | Controls |
|---------|-------|---------|
| earnings_quality | true | Filter trades with low earnings quality scores |
| news_lookback_hours | 48 | How far back to check for adverse news |
| sector_contagion_check | true | Block if sector peer is in distress |
| sentiment_check | true | Check altdata sentiment before entry |
| short_availability_check | true | Check short borrow availability |

**Read by:** `execution/paper_trader.py`

---

### Top-Level Key: `frontier`

Controls the frontier module (UMCI, experimental signals, discovery pipeline).

| Sub-key | Controls |
|---------|---------|
| `complexity_index.*` | UMCI weights (physical 20%, social 25%, financial 25%, scientific 15%, altdata 15%), strategy adjustments per regime |
| `discovery.*` | Signal discovery: auto-validate, always flag for review, do not auto-promote, weekly review, watchlist size 10 |
| `enabled` | Whether frontier module runs |
| `financial_frontier.*` | Building permits (18-month lead), LLM perplexity (GPT-2 baseline), option expiry overhang (0.3 gamma threshold) |
| `log_everything_permanently` | Log all frontier activity to permanent store |
| `log_path` | logs/frontier.log |
| `parameter_drifting.*` | 1% drift rate, max 30% from published values, min 100 observations |
| `physical.*` | Canal congestion (3 sources), electricity (EIA/National Grid), geomagnetic (NOAA), pollen (Open-Meteo), satellite (Sentinel ESA), Schumann resonance (Tomsk University) |
| `scientific.*` | Academic citations (Semantic Scholar), AMR research (PubMed/WHO/CDC), food safety (FDA/FSA), quantum readiness (arXiv/USPTO), soil health (USDA) |
| `sizing.*` | 5-tier evidence system: Tier 1 (full evidence + 6mo live = 100% size), Tier 2 (3-6mo live = 50%), Tier 3 (0-3mo = 25%), Tier 4 (backtest only = 10%), Tier 5 (monitoring = 0%) |
| `social.*` | Attention economy (48h lookback), church attendance (Pew Research), divorce filings (HMCTS/state courts), HQ traffic (Google Popular Times), obituaries (Legacy.com/Find-a-Grave), social contagion (Twitter) |
| `storage_path` | frontier/storage/frontier.db |
| `validation.*` | Bonferroni + deflated Sharpe + Monte Carlo (1000 perms) + t-test + regime stability required |

**Read by:** `frontier/` module, `frontier/storage/frontier_store.py`

---

### Top-Level Key: `logging`

Log rotation settings.

| Sub-key | Value | Controls |
|---------|-------|---------|
| backup_count | 5 | Log file rotation count |
| feature_errors_log | logs/feature_errors.log | Path for feature engineering errors |
| level | INFO | Log level |
| max_bytes | 10,485,760 | 10 MB per log file before rotation |
| upgrade_checks_log | logs/upgrade_checks.log | Path for upgrade check logs |

**Read by:** All modules that configure Python logging.

---

### Top-Level Key: `markets`

Universe filtering rules for each market.

| Sub-key | Value | Controls |
|---------|-------|---------|
| `uk.enabled` | true | UK market active |
| `uk.exchanges` | [LSE] | Exchange filter |
| `uk.market_cap_max_gbp` | £1.5B | Market cap ceiling |
| `uk.market_cap_min_gbp` | £30M | Market cap floor |
| `uk.avg_daily_volume_min` | 50,000 | Liquidity filter |
| `uk.min_price_gbp` | £0.50 | Minimum price |
| `uk.price_divisor` | 100 | LSE prices are in pence; divide by 100 for GBP |
| `uk.suffix` | .L | yfinance ticker suffix |
| `us.enabled` | true | US market active |
| `us.exchanges` | [NYSE, NASDAQ] | Exchange filter |
| `us.market_cap_max_usd` | $2B | Market cap ceiling |
| `us.market_cap_min_usd` | $50M | Market cap floor |
| `us.avg_daily_volume_min` | 100,000 | Liquidity filter |
| `us.min_price_usd` | $1.00 | Minimum price |

**Read by:** Universe builder, `execution/paper_trader.py`

---

### Top-Level Key: `notifications`

Alert delivery configuration.

| Sub-key | Value | Controls |
|---------|-------|---------|
| log | true | Write notifications to log |
| telegram.bot_token | (configured) | Telegram bot credential |
| telegram.chat_id | (configured) | Telegram target chat |
| telegram.enabled | true | Whether Telegram notifications are active |
| terminal | true | Print notifications to terminal |

**Read by:** `monitoring/private_bot.py`, notification modules

---

### Top-Level Key: `paper_trading`

Paper trading operational parameters.

| Sub-key | Controls |
|---------|---------|
| `adaptive_sizing.*` | 5-phase sizing regime: Phase 1 (0.15%–0.4%, max 200 positions) through Phase 5 (1.0%–2.5%, max 75 positions). Account halt at 15% drawdown. Never exceed 3% of account on any single trade. |
| `bot.auto_restart` | Auto-restart on crash |
| `bot.data_collection_interval_minutes` | Data collection frequency (30 min) |
| `bot.log_all_contexts/log_all_signals` | Comprehensive logging flags |
| `email_alerts` | false — email alerts disabled |
| `log_to_file` | true |
| `min_confidence` | 0.25 — minimum combined signal confidence to trade |
| `observation_mode` | false — bot is actively trading, not just observing |
| `scan_time_uk` | 08:15 — UK market scan time |
| `scan_time_us` | 09:45 — US market scan time |
| `signal_thresholds.gap_min_score` | 0.15 |
| `signal_thresholds.mean_reversion_min_score` | 0.15 |
| `signal_thresholds.min_combined_score` | 0.25 |
| `signal_thresholds.momentum_min_score` | 0.15 |
| `signal_thresholds.pead_min_surprise` | 0.02 (2% EPS surprise minimum) |

**Read by:** `execution/paper_trader.py`, `execution/trading_bot.py`

---

### Top-Level Key: `risk`

Portfolio-level risk management parameters.

| Sub-key | Value | Controls |
|---------|-------|---------|
| atr_stop_multiplier | 2.0 | ATR multiplier for stop placement |
| correlation_limit | 0.75 | Maximum allowed correlation between open positions |
| execution_window_minutes | 30 | Maximum time to complete execution |
| kelly_fraction | 0.5 | Half-Kelly position sizing |
| max_drawdown_halt_pct | 15% | Halt trading if portfolio drawdown exceeds 15% |
| max_market_exposure_pct | 60% | Maximum gross exposure |
| max_position_pct | 5% | Maximum single position size |
| max_sector_exposure_pct | 25% | Maximum sector concentration |
| max_total_positions | 150 | Maximum concurrent open positions |
| vwap_execution | true | VWAP-based execution |

**Read by:** `execution/paper_trader.py`, risk modules

---

### Top-Level Key: `server`

VPS health monitoring thresholds.

| Sub-key | Value | Controls |
|---------|-------|---------|
| alpaca_balance_min_usd | $5,000 | Alert if Alpaca balance drops below |
| check_interval_seconds | 300 | Health check frequency (5 min) |
| disk_critical_gb | 2.0 GB | Critical disk alert threshold |
| disk_warning_gb | 5.0 GB | Warning disk alert threshold |
| ram_critical_gb | 1.8 GB | Critical RAM alert threshold |
| ram_warning_gb | 1.5 GB | Warning RAM alert threshold |

**Read by:** `monitoring/` server monitor module

---

### Top-Level Key: `signal`

Signal-level parameters for PEAD and anomaly signals.

| Sub-key | Controls |
|---------|---------|
| `anomaly.auto_promote` | false — anomaly signals require manual review before promotion |
| `anomaly.max_correlation_to_existing` | 0.3 — reject if too correlated with existing signals |
| `anomaly.min_observations` | 50 |
| `anomaly.min_sharpe` | 1.0 |
| `anomaly.validation_required` | true |
| `pead.earnings_surprise_threshold` | 8% — minimum EPS surprise to trigger PEAD signal |
| `pead.holding_period_days` | 20 — PEAD holding period |
| `pead.lookback_days` | 504 (2 years) |
| `pead.volume_surge_multiplier` | 1.3x average volume required |
| `pead.zscore_window` | 60-day z-score window |

**Read by:** PEAD signal module, anomaly detection module, `execution/paper_trader.py`

---

### Top-Level Key: `universe`

Controls the stock universe definition and management.

| Sub-key | Value | Controls |
|---------|-------|---------|
| auto_rebuild_weekly | true | Rebuild universe weekly |
| auto_refresh_days | 30 | Days between full universe refreshes |
| context_tiers | [3] | Which tiers are used for context (not trading) |
| global_context_enabled | true | Include global context signals |
| include_large_caps | true | Include large caps in universe |
| include_micro | true | Include micro-caps |
| include_uk | true | Include UK stocks |
| max_primary_universe | 5,000 | Maximum Tier 1 stocks |
| max_secondary_universe | 3,000 | Maximum Tier 2 stocks |
| max_universe_size | 10,000 | Total universe cap |
| primary_tiers | [1] | Tier 1 = actively traded |
| scan_tiers | all | Scan all tiers on each cycle |
| secondary_tiers | [2] | Tier 2 = watchlist |
| uk_enabled | true | UK universe active |

**Read by:** Universe builder, `execution/paper_trader.py`, `main.py`

---

## 10B: API KEYS AND CREDENTIALS

### Services Requiring API Keys

| Service | Config Path | Module That Reads It | If Missing or Wrong |
|---------|-------------|---------------------|---------------------|
| Alpaca (trading) | `api_keys.alpaca_api_key` + `api_keys.alpaca_secret_key` | `execution/paper_trader.py`, `execution/trading_bot.py` | Authentication failure; bot cannot submit or query orders |
| FRED (macro data) | `api_keys.fred` and `altdata.collectors.fred.api_key` | `data/collectors/rates_credit_collector.py` | FRED API returns 429 or 403; macro series not updated |
| Alpha Vantage | `api_keys.alpha_vantage` | Data collectors | Data collection falls back or fails silently |
| Adzuna (job data) | `api_keys.adzuna_app_id` + `api_keys.adzuna_app_key` | `data/collectors/job_postings_collector.py` | Job posting data not collected; hiring momentum signal blank |
| EIA (electricity) | `api_keys.eia` | Frontier physical module | Electricity demand signal not computed |
| ESA Copernicus (satellite) | `api_keys.esa_copernicus_user` + `api_keys.esa_copernicus_password` | Frontier physical module | Satellite imagery not fetched |
| Finnhub | `api_keys.finnhub` | Data collectors | Market data fallback required |
| Marketstack | `api_keys.marketstack` | Data collectors | Market data fallback required |
| Nasdaq Data Link | `api_keys.nasdaq_data_link` | Data collectors | Alternative data series unavailable |
| NewsAPI | `api_keys.news_api` | `data/collectors/advanced_news_intelligence.py` | News collection falls back to RSS only |
| OpenWeatherMap | `api_keys.openweathermap` (also `altdata.collectors.weather.api_key`) | `data/collectors/geographic_intelligence.py` | Weather data not collected; weather_risk_score will be NULL |
| Polygon.io | `api_keys.polygon` | Data collectors | Market data source unavailable |
| SimFin | `api_keys.simfin` | `data/collectors/simfin_collector.py` | Financial statement data not updated |
| WAQI (air quality) | `api_keys.waqi` | Frontier physical module | Air quality / pollen signals not computed |
| Telegram | `notifications.telegram.bot_token` + `notifications.telegram.chat_id` | `monitoring/private_bot.py` | Telegram alerts not delivered; log-only mode |
| Dashboard | `dashboard.password` | `monitoring/dashboard/app.py` | Dashboard accessible without authentication if empty |

### Services With Empty / Not Configured Keys

The following services are enabled in the config but have no API key set. They will either error on first call, return empty data, or silently skip collection:

| Service | Config Path | Current Value | Module | Consequence |
|---------|-------------|---------------|--------|-------------|
| Companies House | `api_keys.companies_house` | `''` (empty) | Companies House collector | UK corporate filing alerts not generated |
| QuiverQuant | `api_keys.quiver_quant` | `''` (empty) | `data/collectors/quiver_collector.py` | Congressional trading data not from QuiverQuant; `deepdata.congressional.source` is set to `quiverquant_free` but key is missing |
| Reddit | `api_keys.reddit_client_id` + `api_keys.reddit_client_secret` | `''` (empty) | `altdata/collector/reddit_collector.py` | Reddit sentiment not collected despite `collectors.reddit.enabled: true` |

### Key Duplication

Two API keys are stored in two separate locations in the config:

- FRED API key: appears at `api_keys.fred` and at `altdata.collectors.fred.api_key`
- OpenWeatherMap key: appears at `api_keys.openweathermap` and at `altdata.collectors.weather.api_key`

If these are updated, both locations must be changed.

### Config Keys Set But Potentially Unread (Dead Config)

Based on presence in YAML and absence of corresponding table rows or module activity:

| Key | Reason Suspected Dead |
|-----|----------------------|
| `altdata.collectors.lunar.*` | Lunar phase is tracked (column exists in trade_ledger) but lunar collector is likely a simple computation, not a live API; may be internal-only |
| `frontier.social.church_attendance.*` | Pew Research is annual-only; no tables show this data |
| `frontier.social.divorce_filings.*` | No corresponding tables found with data |
| `frontier.social.hq_traffic.*` | No tables contain HQ traffic data; Google Popular Times scraping is legally fragile |
| `frontier.social.obituary.*` | No tables show obituary data |
| `frontier.physical.schumann.*` | Schumann resonance — published_effect is marked "speculative"; no data tables found |
| `frontier.physical.satellite.*` | ESA Sentinel satellite data — no corresponding archive tables found |
| `closeloop.tax.*` | `tax_ledger` table has 0 rows |
| `closeloop.stress.*` | `stress_predictions` has 0 rows; `drawdown_events` has 0 rows |
| `closeloop.benchmark.*` | `benchmark_performance` has 0 rows |

### Config Keys Read But Not Explicitly Set (Would Cause KeyError)

No explicit KeyErrors were identified from config inspection. The system appears to use `.get()` with defaults in most places, or all required keys are present.

---

## SECTION 10 GATE

PASS. All top-level config keys documented. All API keys located and status noted. Three services (Companies House, QuiverQuant, Reddit) are enabled but have no credentials set. Two key duplications documented. Dead config entries listed based on 0-row table evidence.

---

## Files Read and Queries Run

**Queries run:**
1. `closeloop/storage/closeloop.db` — full schema, all tables, row counts
2. `closeloop/storage/cooling_off.db` — full schema
3. `output/historical_db.db` — full schema
4. `output/permanent_archive.db` — full schema
5. `frontier/storage/frontier.db` — full schema
6. `deepdata/storage/deepdata.db` — full schema
7. Detailed audit queries on `closeloop.db` (order status, exit reasons, signals_at_entry, attribution, date range)
8. `find` on all `.db` files
9. Schema queries on 17 additional databases

**Files read:**
- `/home/dannyelticala/quant-fund/config/settings.yaml`
- Module grep searches across all `.py` files in project

---

## Key Findings

1. **Trade ledger is 6 days old.** All 761 trades span 2026-04-02 to 2026-04-08. The system has been in paper trading for under one week.

2. **295 of 696 closed trades are phantoms.** 42.4% of exits are `phantom_duplicate` or `phantom_cleanup`. These are artefacts of a deduplication backfill run on 2026-04-07 (confirmed by the backup file date). They are not real trades.

3. **signals_at_entry is almost completely empty.** 97.3% of closed trades (677/696) have no signal data captured at entry. This severely limits attribution quality. The `pnl_attribution` table has only 2 rows despite 208 trades flagged `attribution_complete = 1`.

4. **cooling_off.db is not in WAL mode.** All other active databases use WAL. The cooling_off database uses DELETE journal mode, which causes locking under concurrent reads — a risk during high-frequency market scans.

5. **Three API keys are empty but services are marked enabled.** Reddit, Companies House, and QuiverQuant collectors will fail on first real collection attempt.

6. **Two API keys are duplicated in config.** FRED and OpenWeatherMap keys appear under both `api_keys.*` and `altdata.collectors.*`. They are currently identical but a future update to one location only would create a split-brain.

7. **The permanent_archive.db raw_macro_data table has 17.5 million rows.** This is by far the largest table in the system. Any full-table scan on this table without indexed filters will be slow.

8. **Many tables exist with 0 rows.** Large portions of the planned feature set (transcripts, supply chain, patents, congressional member tracking, tax ledger, benchmark performance, stress predictions, drawdown events, obituaries, satellite data) have not yet accumulated data. The system is early-stage relative to its schema.

9. **A nested `quant-fund/quant-fund/` directory exists.** This appears to be a git clone or rsync artifact. It contains mirror copies of databases and code. No live module should be reading from this directory, but its presence on disk could cause confusion.

10. **order_status field defaults to 'unknown'.** Only two values exist: `unknown` (the hardcoded default) and `superseded`. There is no `filled` or `open` status, suggesting the order lifecycle status field is not fully implemented.

---

## Contradictions

1. `pnl_attribution` table has 2 rows, but 208 trades have `attribution_complete = 1` in `trade_ledger`. These two figures contradict each other. Either the flag is set without the attribution data being written, or the attribution table was cleared after the flag was set.

2. `reddit.enabled` is `true` in config but `reddit_client_id` and `reddit_client_secret` are empty strings. The collector cannot authenticate. The config state and the operational state are inconsistent.

3. `deepdata.congressional.source` is set to `quiverquant_free` but `api_keys.quiver_quant` is empty. QuiverQuant data collection is configured but uncredentialed.

4. FRED API key appears twice in config with the same value. If changed in one place and not the other, the two config sections will diverge.

---

## Proceed to GROUP 11: YES
