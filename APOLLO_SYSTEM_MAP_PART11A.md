# APOLLO SYSTEM MAP — PART 11A
## Group 11A: Analysis Module + Altdata Module (complete)

**Generated:** 2026-04-08
**Coverage:** 52 files read completely.
**Instruction:** DOCUMENT ONLY. Nothing was fixed or changed.

---

## TABLE OF CONTENTS

Files 1–22: analysis/ directory
Files 23–52: altdata/ directory (already documented in prior sessions; summaries included)

---

## FILE 1: analysis/crowding_detector.py

### A) PURPOSE
Computes a Crowding Risk Index (CRI) for each ticker combining short interest, peer correlation, institutional ownership, and factor dispersion. Applies position size reductions for SEVERE/HIGH crowding.

### B) CLASSES AND METHODS

**Class: `CrowdingDetector`**

`__init__(config)` — reads `output/historical_db.db` path from config or uses default.

`get_crowding_risk(ticker, lookback=252) -> dict` — main method: fetches SI/corr/inst/disp via yfinance, computes CRI, stores to `crowding_risk` table, returns dict with cri, level (SEVERE/HIGH/MEDIUM/LOW), size_multiplier.

`_get_short_interest(ticker) -> float` — yfinance `tk.info.get("shortPercentOfFloat")`. Returns 0.0 on failure.

`_get_correlation_risk(ticker, lookback) -> float` — downloads 10 sector ETF returns and the ticker; computes max Pearson correlation between ticker and any ETF. Returns 0.0–1.0.

`_get_institutional_ownership(ticker) -> float` — yfinance `institutionPercentHeld`. Returns 0.5 on failure.

`_get_factor_dispersion(ticker) -> float` — stub returning 0.5 always (hardcoded).

`size_multiplier(cri) -> float` — SEVERE (>0.9)→0.50, HIGH (>0.7)→0.75, else 1.0.

### C) MATHEMATICS

```
CrowdingRiskIndex = SI×0.30 + corr×0.30 + inst×0.20 + disp×0.20

SEVERE:  CRI > 0.9 → size_multiplier = 0.50
HIGH:    CRI > 0.7 → size_multiplier = 0.75
MEDIUM/LOW:         → size_multiplier = 1.00
```

### D) DATA FLOWS
**Reads:** yfinance (shortPercentOfFloat, institutionPercentHeld, sector ETF closes)
**Writes:** `output/historical_db.db` → `crowding_risk` table (ticker, cri, level, size_multiplier, computed_at)

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `logging`, `numpy`, `yfinance`, `datetime`

### F) WIRING STATUS
LIVE PATH — called by risk management layer.

### G) ISSUES FOUND
- `_get_factor_dispersion()` always returns hardcoded 0.5 — dispersion component is always 0.5, never computed.
- `_get_correlation_risk()` computes max correlation across ETFs rather than peer correlation — measures sector ETF comovement, not actual crowding from other funds holding same positions.

---

## FILE 2: analysis/earnings_revision_scorer.py

### A) PURPOSE
Tracks analyst EPS estimate revisions and amplifies PEAD signals. Fetches current vs prior EPS estimates from yfinance, computes revision momentum score in [-0.30, +0.30], caches to SQLite, and exposes `amplify_pead_signal()`.

### B) CLASSES AND METHODS

**Class: `EarningsRevisionScorer`**

`__init__(db_path)` — default `closeloop/storage/closeloop.db`.

`_ensure_table()` — creates `earnings_revisions` table.

`_fetch_yfinance_estimates(ticker) -> dict` — pulls forwardEps/trailingEps from tk.info; tries earnings_dates for prior estimate; falls back to recommendations.

`_fetch_simfin_estimates(ticker) -> dict` — tries simfin library; always returns `{}` (SimFin has no consensus estimates).

`get_revision_score(ticker) -> float` — checks DB cache first (today's date); fetches estimates; computes revision_pct; maps to score; persists.

`amplify_pead_signal(pead_score, ticker) -> float` — `pead_score × (1 + revision_score)`, clamped to [-1,1].

`status() -> dict` — returns row counts.

### C) MATHEMATICS

```
revision_pct = (current_estimate - prior_estimate) / abs(prior_estimate)

if revision_pct >= +0.05:
    score = min(0.30, revision_pct × 2.0)
elif revision_pct <= -0.05:
    score = max(-0.30, revision_pct × 2.0)
else:
    score = 0.0

amplified_pead = pead_score × (1.0 + revision_score)
amplified_pead = clip(amplified_pead, -1.0, 1.0)
```

### D) DATA FLOWS
**Reads:** yfinance earnings_dates, info, recommendations
**Writes:** `closeloop/storage/closeloop.db` → `earnings_revisions` (ticker, estimate_date, eps_estimate, revision_pct, revision_score)

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `logging`, `numpy`, `yfinance`, `datetime` (simfin optional)

### F) WIRING STATUS
LIVE PATH — `amplify_pead_signal()` is wired into PEAD signal generation.

### G) ISSUES FOUND
- `_fetch_simfin_estimates()` always returns `{}` — SimFin doesn't have consensus estimates per docstring comment, yet the function exists and is called as a fallback.
- DB path default (`closeloop/storage/closeloop.db`) is relative — will fail if cwd is not quant-fund root.

---

## FILE 3: analysis/factor_arbitrage.py

### A) PURPOSE
Identifies long/short pairs based on a simple value+momentum factor composite. Returns sorted lists of tickers for potential long/short factor arbitrage trades. Caps universe at 50 tickers for speed.

### B) CLASSES AND METHODS

**Class: `FactorArbitrage`**

`__init__(config)` — stores config.

`get_factor_scores(tickers) -> dict` — for each ticker: fetches PE ratio and 1-year return from yfinance; computes composite factor score; returns `{ticker: score}`.

`get_long_short_pairs(tickers, n=10) -> dict` — sorts by factor score; returns top-n as longs, bottom-n as shorts.

`run(tickers) -> dict` — caps at 50, calls get_factor_scores, returns scores + pairs.

### C) MATHEMATICS

```
value_score    = 1 / max(pe_ratio, 1) × 20          [PE inversion, scaled ×20]
momentum_score = (price_1yr_return)                   [as fraction, e.g. 0.25 = 25%]
composite      = value_score × 0.4 + momentum_score × 0.6
```

### D) DATA FLOWS
**Reads:** yfinance (trailingPE from info, 1yr historical closes)
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `yfinance`, `logging`, `datetime`

### F) WIRING STATUS
DISCONNECTED — no wiring in main pipeline. Returns data but no downstream consumer confirmed.

### G) ISSUES FOUND
- Simple value+momentum composite with hardcoded equal-weight constants (0.4/0.6) — no empirical basis documented.
- Cap of 50 tickers is hardcoded with comment "for speed" — does not adapt to universe size.

---

## FILE 4: analysis/factor_model.py

### A) PURPOSE
Runs Fama-French 6-factor OLS regression (FF5 + MOM) on portfolio tickers using Ken French's data library. Decomposes portfolio returns into factor exposures (alpha, beta, SMB, HML, RMW, CMA, MOM). Writes to closeloop.db and sends weekly Telegram summary.

### B) CLASSES AND METHODS

**Class: `FactorModelAnalyser`**

`__init__(config)` — reads telegram config and db path.

`_ensure_table()` — creates `factor_exposures` table in closeloop.db.

`_fetch_ff_factors(start_date, end_date) -> DataFrame` — downloads FF5+MOM from `pandas_datareader` (Ken French data library: `F-F_Research_Data_5_Factors_2x3` and `F-F_Momentum_Factor`). Merges on date.

`_fetch_portfolio_returns(tickers, start_date, end_date) -> Series` — equal-weight portfolio daily returns from yfinance.

`run_regression(tickers, lookback_days=252) -> dict` — fetches factors + portfolio returns; aligns; runs OLS with `statsmodels`; extracts 6 coefficients; returns dict with alpha, betas, r-squared, p-values.

`save_to_db(results, ticker_list, run_date)` — writes to `factor_exposures` table.

`run_weekly(tickers) -> dict` — calls run_regression, saves, sends Telegram, returns results.

`neutralise_recommendation(exposures) -> str` — generates regime-specific text advice based on factor exposures.

### C) MATHEMATICS

```
R_portfolio = alpha + beta_MKT × MKT + beta_SMB × SMB + beta_HML × HML
            + beta_RMW × RMW + beta_CMA × CMA + beta_MOM × MOM + epsilon

OLS via statsmodels.OLS(returns, sm.add_constant(factors)).fit()
R-squared, p-values, 95% confidence intervals extracted from OLS summary.
```

### D) DATA FLOWS
**Reads:** Ken French data library (pandas_datareader), yfinance (portfolio prices)
**Writes:** `closeloop/storage/closeloop.db` → `factor_exposures` (ticker_list, run_date, alpha, beta_mkt, beta_smb, beta_hml, beta_rmw, beta_cma, beta_mom, r_squared)

### E) DEPENDENCIES
**Internal:** None
**External:** `statsmodels`, `pandas_datareader`, `yfinance`, `numpy`, `pandas`, `requests` (Telegram), `sqlite3`

### F) WIRING STATUS
LIVE PATH — `run_weekly()` is wired into the weekly scheduler.

### G) ISSUES FOUND
- Ken French data library via pandas_datareader can fail silently if network unavailable — no explicit retry or fallback data source.
- `neutralise_recommendation()` returns generic text advice with no mathematical basis — decorative output only.

---

## FILE 5: analysis/frontier_validator.py

### A) PURPOSE
Statistically validates frontier signals against SPY daily returns. Applies promotion criteria (Sharpe > 0.5, p-value < 0.05, n ≥ 50) and writes PROMOTED/FAILED_VALIDATION/INSUFFICIENT_DATA status to closeloop.db. Writes text log to `logs/frontier_signal_validation.log`.

### B) CLASSES AND METHODS

**Class: `FrontierSignalValidator`**

`__init__(closeloop_db, historical_db, config)` — defaults: `closeloop/storage/closeloop.db`, `output/historical_db.db`.

`_ensure_table()` — creates `frontier_signal_validation` table.

`_load_spy_returns() -> dict` — loads SPY closes from historical_db.db; computes daily returns as `(close_i - close_{i-1}) / close_{i-1}`.

`_load_signal_observations(signal_name) -> dict` — queries `signals_log` (closeloop.db) and `predictions_log` (permanent_archive.db) by signal_name LIKE match. Returns `{date: avg_score}`.

`_compute_stats(signal_vals, returns) -> dict` — Pearson r, t-stat, p-value (scipy if available), Sharpe (long-when-signal-positive strategy).

`validate_signal(signal_name) -> dict` — full pipeline for one signal; persists result.

`run_all() -> list` — validates all 22 known frontier signals; writes log.

`send_telegram_summary()` — calls run_all, sends Telegram message.

`status() -> dict` — count by status from DB.

### C) MATHEMATICS

```
SPY_return_t = (close_t - close_{t-1}) / close_{t-1}

Pearson r, p = scipy.stats.pearsonr(signal_vals, spy_returns)
t_stat = r × sqrt((n-2) / (1 - r²))

strategy_returns = spy_return where signal > 0, else 0
Sharpe = mean(strategy_returns) / std(strategy_returns) × sqrt(252)

PROMOTED if: Sharpe >= 0.5 AND p_value <= 0.05 AND n >= 50
PROMOTED_WEIGHT = 0.02

p-value approximation without scipy:
p = 2 × (1 - 0.5 × (1 + sign(|t|) × (1 - exp(-0.717×|t| - 0.416×t²))))
```

### D) DATA FLOWS
**Reads:** `output/historical_db.db` → `price_history` (SPY); `closeloop/storage/closeloop.db` → `signals_log`; `output/permanent_archive.db` → `predictions_log`
**Writes:** `closeloop/storage/closeloop.db` → `frontier_signal_validation` (signal_name, run_date, n_obs, correlation, t_stat, p_value, sharpe, status, weight)

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `numpy`, `scipy.stats` (optional), `requests` (Telegram), `logging`, `os`, `datetime`

### F) WIRING STATUS
LIVE PATH — `run_all()` called by weekly scheduler.

### G) ISSUES FOUND
- PERMANENT_DB path hardcoded as `"output/permanent_archive.db"` — does not match permanent_store.py which uses `"output/permanent_log.db"`. Queries will fail silently against wrong/non-existent path.
- Signal lookup uses LIKE match on signal_type — may match unintended signals if names overlap.
- 22 known frontier signals listed in `_KNOWN_SIGNALS` — any new frontier signal not added to this list will never be validated.

---

## FILE 6: analysis/insider_analyser.py

### A) PURPOSE
Sophisticated insider transaction analysis with noise exclusion (TAX_WITHHOLDING, COMPENSATION_VEST, GIFT_TRANSFER, SCHEDULED_10b51, OPTION_EXERCISE_SAME_DAY_SALE), multi-component scoring (InsiderSignalScore 0–100), cluster analysis (ClusterBuyScore/ClusterSellScore), PEAD multiplier, and individual track record tracking.

### B) CLASSES AND METHODS

**Class: `InsiderAnalyser`**

`__init__(db_path)` — default `output/insider_analysis.db`. Creates 3 tables + indexes. Runs `_migrate_schema()`.

`_is_noise_transaction(txn, all_txns_today) -> (bool, str)` — detects 5 noise types by transaction code (F=TAX_WITHHOLDING, A/M/J=COMPENSATION_VEST or OPTION_EXERCISE, G=GIFT, S+10b51=SCHEDULED).

`_role_score(title) -> int` — CEO=20, CFO=18, COO/CTO=15, Director=8, VP=5, other=2.

`_size_score(value_usd, comp_annual_usd) -> int` — comp-relative: 100%+=25, 50%+=20, 20%+=15, 10%+=10, 5%+=5, else 0. Absolute if no comp: $1M+=25, $500k+=20, etc.

`_price_context_score(ticker, txn_date_str) -> int` — yfinance 1-yr history. Near 52w low (<10% above): +15. Down >20% in 30d: +15. Near 52w high (>90% range): -3. ATH (>98%): -5.

`_cluster_context_score(ticker, txn_date_str, other_txns_30d) -> int` — 3+ buyers: +20, 2 buyers: +10, prior DB buys: +5.

`_track_record_score(ticker, insider_name) -> int` — buy_accuracy>70%: +10, ≥50%: +5, <50%: -5, n_buys<3: 0.

`get_pead_multiplier(ticker, earnings_date, price_change_30d) -> (float, str)` — score>70 in 30d: 1.4x. DIP_BUY_CLUSTER: 1.6x + INSIDER_CLUSTER_LONG signal. POST_EARNINGS_DIP_BUY: 1.8x + HIGH_CONVICTION log. NetClusterScore<-30: 0.0.

### C) MATHEMATICS

```
InsiderSignalScore = 50 + role_score + size_score + price_context_score
                   + cluster_context_score + track_record_score + timing_score
                   + first_purchase_bonus

ClusterBuyScore  = count(unique_buyers_30d) / count(all_insiders_30d) × 100
ClusterSellScore = count(unique_sellers_30d) / count(all_insiders_30d) × 100
NetClusterScore  = ClusterBuyScore - ClusterSellScore

DIP_BUY_CLUSTER flag: stock_down_30d > 20% AND ClusterBuyScore > 20 AND NetClusterScore > +15
POST_EARNINGS_DIP_BUY: earnings_in_last_14d AND stock_down > 10% AND any_buy_since

buy_accuracy_90d = count(buys where price_t90 > price_t0 × 1.05) / count(all_buys)
sell_accuracy_90d = count(sells where price_t90 < price_t0 × 0.95) / count(all_sells)

PEAD multipliers:
  score > 70 in 30d:         1.4×
  DIP_BUY_CLUSTER:           1.6×
  POST_EARNINGS_DIP_BUY:     1.8×
  NetClusterScore < -30:     0.0  (suppress long)
```

### D) DATA FLOWS
**Reads:** `output/insider_analysis.db` → insider_transactions, insider_track_records, insider_cluster_state; yfinance (price context)
**Writes:** `output/insider_analysis.db` → insider_transactions (with scores, noise flags, returns), insider_track_records, insider_cluster_state

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `logging`, `numpy`, `yfinance`, `datetime`, `threading`, `contextlib`, `json`, `math`, `pathlib`

### F) WIRING STATUS
LIVE PATH — `get_pead_multiplier()` called by PEAD signal module.

### G) ISSUES FOUND
- `_migrate_schema()` swallows OperationalError silently — if a column type mismatch occurs (not just duplicate), the error is hidden.
- `_fetch_simfin_estimates()` inherited pattern from other modules — no simfin here, but parallel structure.
- Track record scoring requires `n_buys >= 3` for any score — new insiders always score 0 regardless of known history.

---

## FILE 7: analysis/intelligence_db.py

### A) PURPOSE
SQLite storage layer for the intelligence analysis module. Defines 5-table schema: company_profiles, signal_effectiveness, pattern_discovery, cross_asset_correlations, readthrough_coefficients. Provides typed upsert and query methods.

### B) CLASSES AND METHODS

**Class: `IntelligenceDB`**

`__init__(db_path)` — default `output/intelligence_db.db`. Creates schema.

`_connect()` — thread-local WAL-mode SQLite connection with Row factory.

`_cursor()` — context manager: yields cursor, commits on success, rolls back on exception.

`_upsert(table, record, conflict_cols)` — generic INSERT OR ... DO UPDATE SET for any table.

`upsert_profile(record)` / `get_profile(ticker)` / `get_all_profiles(sector)` / `upsert_profiles_batch(records)` — company_profiles CRUD.

`upsert_signal_effectiveness(record)` / `get_signal_effectiveness(signal_name, sector, min_observations)` — signal_effectiveness CRUD.

`upsert_pattern(record)` / `get_patterns(pattern_type, sector, min_confidence, min_occurrences)` — pattern_discovery CRUD.

`upsert_correlation(record)` / `get_correlations(asset, relationship_type)` — cross_asset_correlations CRUD.

`upsert_readthrough_coeff(record)` / `get_readthrough_coeffs(peer_ticker)` / `upsert_readthrough_batch(records)` — readthrough_coefficients CRUD.

`status() -> dict` — row counts per table.

### C) MATHEMATICS
None (pure storage layer).

### D) DATA FLOWS
**Reads/Writes:** `output/intelligence_db.db` — 5 tables per DDL above.

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `logging`, `threading`, `contextlib`, `pathlib`, `typing`

### F) WIRING STATUS
LIVE PATH — used by IntelligenceEngine.

### G) ISSUES FOUND
- `get_signal_effectiveness()` uses `ORDER BY sharpe_ratio DESC NULLS LAST` — SQLite does not support NULLS LAST in ORDER BY (requires SQLite ≥ 3.30.0 released 2019; may work on modern systems but not guaranteed on older VPS).

---

## FILE 8: analysis/intelligence_engine.py

### A) PURPOSE
Phase 9 feature extraction engine. Extracts 110+ features from earnings observations and historical data records. Also contains pattern learning algorithms (single-feature effectiveness, two-feature combinations, regime-conditional, sector-specific, cross-asset lead-lag, signal decay). Main orchestrator `IntelligenceEngine` drives all sub-engines.

### B) CLASSES AND METHODS

**Class: `FeatureExtractor`**

`extract(record, hist_record) -> dict` — calls 7 sub-extractors, returns flat dict with 110+ keys.

`_eps_features(r)` — 10 features: eps_surprise_pct, magnitude, beat_flag, large_beat, large_miss, in_line, actual_abs, estimate_abs, estimate_positive, surprise_yf.

`_price_volume_features(r)` — 15 features: return_t1/t3/t5/t10/t20, volume_surge/t0/avg_20d, high_volume_flag, low_volume_flag, drift_r1_to_r5, return_positive_t1/t5/t20.

`_altdata_features(r)` — 10 features: altdata_sentiment, reddit_score, news_score, sec_score, beat_quality_mult, altdata_count, altdata_bull/bear, news_reddit_agree, bqm_high.

`_deepdata_features(r)` — 10 features: options_smfi, iv_rank, put_call, dark_pool, short_squeeze_score, congressional_signal, call_heavy, put_heavy, elevated_iv, squeeze_flag.

`_macro_features(r)` — 15 features: vix, spy_r5, sector_r5, regime, vix thresholds, sector alignment flags, yield curve, credit spread.

`_historical_features(hist_record)` — 15 features: avg PEAD return, consistency, readthrough sensitivity, prior earnings trends.

`_temporal_features(r)` — 10 features: quarter, day_of_week, is_options_expiry_week, days_to_fomc, etc.

`_derived_features(features)` — derived composites: surprise×volume, beat×macro, etc.

**Class: `IntelligenceEngine`** (not fully read, orchestrates all sub-engines)

### C) MATHEMATICS

```
beat_flag  = +1.0 if surprise_pct > 0, -1.0 if < 0, 0.0 if = 0
large_beat = 1.0 if surprise_pct > 5%
large_miss = 1.0 if surprise_pct < -5%
in_line    = 1.0 if |surprise_pct| < 1%

drift_r1_to_r5 = return_t5 - return_t1
high_volume_flag = 1.0 if vol_surge > 2.0
call_heavy = 1.0 if put_call < 0.4
put_heavy  = 1.0 if put_call > 0.7
elevated_iv = 1.0 if iv_rank > 0.7
squeeze_flag = 1.0 if short_squeeze_score > 50
```

### D) DATA FLOWS
**Reads:** earnings_observations, pre_earnings_snapshots, earnings_enriched (passed as dicts)
**Writes:** pattern_discovery, signal_effectiveness tables via IntelligenceDB

### E) DEPENDENCIES
**Internal:** `analysis.intelligence_db.IntelligenceDB`
**External:** `numpy`, `pandas`, `hashlib`, `json`, `logging`, `math`, `collections`, `datetime`

### F) WIRING STATUS
LIVE PATH — called by daily_pipeline.py.

### G) ISSUES FOUND
- Feature count documented as "110+" but actual feature extraction produces variable count based on data availability — feature vector dimension is not guaranteed stable.
- `_derived_features()` depends on `_macro_features()` output keys — order-sensitive; if upstream changes, derived features silently return 0.0.

---

## FILE 9: analysis/market_calendar.py

### A) PURPOSE
Static 2026 trading calendar for US (NYSE) and UK (LSE) with FOMC dates. Provides is_trading_day, next/prev trading day, days to FOMC, FOMC week detection. No external dependencies.

### B) CLASSES AND METHODS

**Class: `MarketCalendar`**

`is_trading_day(market, dt) -> bool` — weekday check + holiday set lookup.
`is_us_trading_day(dt)` / `is_uk_trading_day(dt)` — convenience wrappers.
`next_trading_day(market, dt) -> date` — iterates forward until trading day found.
`prev_trading_day(market, dt) -> date` — iterates backward.
`trading_days_between(market, start, end) -> int` — counts trading days inclusive.
`days_to_next_fomc(dt) -> int|None` — returns days until next FOMC; None if past all 2026 dates.
`next_fomc_date(dt) -> date|None` — returns next FOMC date.
`is_fomc_week(dt) -> bool` — checks ISO week match.
`holiday_name(market, dt) -> str|None` — returns human-readable holiday name.

### C) MATHEMATICS
None (lookup table only).

### D) DATA FLOWS
**Reads:** None (static hardcoded data)
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `datetime` (stdlib only)

### F) WIRING STATUS
LIVE PATH — used by entry conditions, earnings scheduler, automation scheduler.

### G) ISSUES FOUND
- All holiday dates hardcoded for 2026 only — will return incorrect results after Dec 31 2026 (no 2027 holidays defined). days_to_next_fomc returns None after Dec 10 2026.
- Black Friday and Christmas Eve noted as "treated as holiday for simplicity" — may block trading on days when market is actually open (only early close).
- UK New Year's Eve Dec 31 listed as "LSE early close/closed" — ambiguous status.

---

## FILE 10: analysis/market_timer.py

### A) PURPOSE
Timezone-aware market open/close window checks for US (NYSE 09:30–16:00 ET) and UK (LSE 08:00–16:30 London time). Provides session type (pre/open/power_hour/after/closed) and should_trade() guard (avoids first/last 5 minutes).

### B) CLASSES AND METHODS

**Class: `MarketTimer`**

`local_time(market) -> datetime` — pytz-localized datetime or UTC fallback.
`is_open(market) -> bool` — regular session check (skips weekends).
`is_pre_market(market) -> bool` — pre_open to open.
`is_after_hours(market) -> bool` — close to after_close.
`is_power_hour(market) -> bool` — first 30min OR last 30min before close.
`minutes_to_open(market) -> int` — positive=not yet open, negative=already open.
`minutes_to_close(market) -> int` — positive=open, negative/0=closed.
`current_session(market) -> str` — returns one of: pre/power_hour/open/after/closed.
`should_trade(market) -> bool` — is_open AND not first/last 5 min.

### C) MATHEMATICS

```
minutes_to_open = (today_open_datetime - now) / 60
minutes_to_close = (today_close_datetime - now) / 60

should_trade: is_open AND time >= open+5min AND time < close-5min
```

### D) DATA FLOWS
**Reads:** System clock only
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `pytz` (optional, UTC fallback), `datetime`, `logging`

### F) WIRING STATUS
LIVE PATH — used by trading_bot.py and entry_conditions.py.

### G) ISSUES FOUND
- UTC fallback when pytz not installed is inaccurate for non-UTC timezones — will compute wrong is_open() if server is UTC (London=UTC+1 BST) but pytz unavailable. The Apollo VPS runs UTC, which means without pytz, UK timing will be off by 1 hour in BST.
- `should_trade()` computes `dtime(h["open"].minute + 5)` — if open.minute is 25 or greater, this overflows (minute=35 is fine but e.g. minute=58 would give 63). NYSE open is 30min so 30+5=35 is safe, but code is fragile.

---

## FILE 11: analysis/multi_timeframe.py

### A) PURPOSE
Daily→4H→1H confirmation cascade for trade entries. Uses yfinance intraday data. Checks 4H slope direction, 1H RSI range (LONG: 40–75, SHORT: 25–60), and 0.5% intraday burst on 15-min bars. Returns MTFResult dataclass.

### B) CLASSES AND METHODS

**Dataclass: `MTFResult`** — ticker, daily_direction, confirmed_4h, confirmed_1h, confirmed (both), entry_score (0–1), intraday_burst, burst_direction, notes.

**Class: `MultiTimeframeAnalyzer`**

`confirm(ticker, daily_direction, lookback_4h=5, lookback_1h=3) -> MTFResult` — full cascade; fails open if data unavailable.

`_check_4h(ticker, direction, lookback_days) -> bool` — fetches 60m bars, resamples to 4H, checks slope direction.

`_check_1h(ticker, direction, lookback_days) -> bool` — fetches 60m bars, computes RSI(14), checks range.

`_check_burst(ticker) -> (bool, str)` — fetches 15m bars, checks last bar for ≥0.5% move.

`_fetch(ticker, interval, days) -> DataFrame` — yfinance download wrapper.

`_rsi(prices, period=14) -> float` — rolling EWM RSI.

### C) MATHEMATICS

```
4H slope = df_4h.iloc[-1] - df_4h.iloc[-4]   (or iloc[0] if fewer bars)
LONG confirmed if slope > 0; SHORT if slope < 0

RSI = 100 - 100/(1 + RS)  where RS = EWM_gain / EWM_loss

LONG RSI confirmed: 40 ≤ RSI ≤ 75
SHORT RSI confirmed: 25 ≤ RSI ≤ 60

Burst move = (last_close - prev_close) / prev_close
Burst if |move| >= 0.005 (0.5%)

entry_score = 0.5 × confirmed_4h + 0.3 × confirmed_1h
            + 0.2 × (burst AND burst_dir == daily_direction)
entry_score = min(1.0, entry_score)
```

### D) DATA FLOWS
**Reads:** yfinance (60m and 15m bars)
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `yfinance`, `numpy`, `pandas`, `logging`, `datetime`, `dataclasses`

### F) WIRING STATUS
DISCONNECTED — `confirm()` is callable but not invoked by main pipeline (no wiring confirmed in trading_bot.py or entry_conditions.py for MTF).

### G) ISSUES FOUND
- Fails open on data unavailability (returns True) — if yfinance returns no data, 4H and 1H checks both pass, meaning every signal gets confirmed by default. This negates the filter entirely when data is missing.
- RSI uses `rolling().mean()` in `_rsi()` in the inline version vs EWM in the `TechnicalIndicatorCalculator.rsi()` — two different RSI implementations coexist in the codebase producing different values.

---

## FILE 12: analysis/options_flow_analyser.py

### A) PURPOSE
Options market sentiment analyser using yfinance options chains. Aggregates put/call ratio, IV percentile, and unusual activity into a single options_sentiment_score [-1, +1]. Provides position size adjustment (up to -30% when options very bearish).

### B) CLASSES AND METHODS

**Class: `OptionsFlowAnalyser`**

`__init__(config)` — in-memory cache dict.

`get_put_call_ratio(ticker) -> float|None` — nearest expiration puts_vol / calls_vol.

`get_iv_percentile(ticker) -> float|None` — ATM IV from first 6 expirations; returns percentile within observed distribution.

`detect_unusual_activity(ticker) -> bool` — volume > 10× open_interest on any single contract.

`options_sentiment_score(ticker) -> float` — combines PCR + IV + unusual into [-1, +1].

`position_size_adjustment(ticker, base_size) -> float` — reduces size up to 30% when score < -0.5.

`apply_to_signal(ticker, combined_score, weight=0.075) -> float` — adds options_sentiment × weight to combined_score.

### C) MATHEMATICS

```
PCR = puts_volume / calls_volume

PCR > 1.5 (bearish): score -= 0.3 + 0.2 × min((PCR - 1.5) / 2.0, 1.0)  → range [-0.3, -0.5]
PCR < 0.5 (bullish): score += 0.3 + 0.2 × min((0.5 - PCR) / 0.5, 1.0)  → range [+0.3, +0.5]

IV > 80th pct: score += 0.1 (mean reversion expected)
Unusual activity detected: score = clip(score × 1.5, -1.0, 1.0)

Position size reduction (score < -0.5):
  reduction = 0.30 × min(|score + 0.5| / 0.5, 1.0)
  adjusted = base_size × (1 - reduction)  [floor: base_size × 0.70]

apply_to_signal: clip(combined_score + options_sentiment × 0.075, -1, 1)
```

### D) DATA FLOWS
**Reads:** yfinance options chains (options, option_chain, fast_info)
**Writes:** None (in-memory cache only)

### E) DEPENDENCIES
**Internal:** None
**External:** `yfinance`, `numpy`, `pandas` (lazy), `logging`

### F) WIRING STATUS
DISCONNECTED — `apply_to_signal()` exists but no confirmed wiring in main pipeline.

### G) ISSUES FOUND
- `detect_unusual_activity()` references `pd` module but pandas is imported at module bottom with bare `import pandas as pd` in a try/except — if pandas not available at import time, `pd` is unbound, causing NameError inside `detect_unusual_activity()`.
- IV percentile computed from ATM calls across first 6 expirations — current_iv is always `all_ivs[0]` (first expiry), percentile vs distribution of all 6 — self-comparison gives percentile that always biases toward 50th percentile with just 6 samples.

---

## FILE 13: analysis/pairs_trader.py

### A) PURPOSE
Full pairs trading engine using Engle-Granger cointegration. Scans all pairs in a universe for cointegrated pairs (p < 0.05, correlation > 0.60, half-life 5–30 days). Generates z-score-based entry/exit signals. Persists to historical_db.db.

### B) CLASSES AND METHODS

**Dataclass: `PairCandidate`** — ticker_a, ticker_b, p_value, hedge_ratio, half_life, correlation, series_a, series_b.

**Class: `CointegrationScanner`**

`estimate_hedge_ratio(series_a, series_b) -> float` — OLS regression coefficient (statsmodels or numpy fallback).

`estimate_half_life(spread) -> float` — OU process half-life via OLS on lagged spread.

`test_cointegration(series_a, series_b) -> (float, float)` — Engle-Granger coint test; returns (p_value, correlation).

`scan(price_matrix) -> list[PairCandidate]` — tests all combinations; filters by p<0.05, correlation>0.60, half_life 5–30 days.

**Class: `PairsSignalEngine`**

`compute_zscore(spread, lookback=30) -> float` — rolling z-score of spread.

`get_signal(pair, zscore) -> dict` — entry (|z|>2.0), exit (|z|<0.0), stop (|z|>3.5).

**Class: `PairsTrader`**

`__init__(config, db_path)` — creates pairs_signals, cointegration_log tables.

`discover(tickers, price_data)` — scans, stores discoveries.

`get_active_pairs()` — returns active pairs from DB.

`generate_signals(price_data)` — computes z-scores, generates signals.

### C) MATHEMATICS

```
OLS: a = alpha + beta × b  →  hedge_ratio = beta

spread = a - hedge_ratio × b

OU half-life:
  delta_t = alpha + beta × spread_{t-1}
  HL = -log(2) / log(1 + beta)

Rolling z-score:
  z = (spread - mean(spread[-30:])) / std(spread[-30:])

Entry signal: |z| > 2.0
  z < -2.0: long A, short B (direction_a=+1, direction_b=-1)
  z > +2.0: short A, long B (direction_a=-1, direction_b=+1)
Exit: |z| < 0.0 (crosses zero)
Stop: |z| > 3.5

signal_strength = min(|z| / 2.0, 1.0)
```

### D) DATA FLOWS
**Reads:** yfinance (price data in __main__ block); price_matrix passed from caller
**Writes:** `output/historical_db.db` → pairs_signals (ticker_a, ticker_b, discovered_at, p_value, hedge_ratio, half_life, correlation, zscore, direction_a/b, signal_strength, is_active); cointegration_log

### E) DEPENDENCIES
**Internal:** None
**External:** `statsmodels` (optional), `scipy` (optional), `numpy`, `pandas`, `sqlite3`, `logging`, `dataclasses`, `itertools`

### F) WIRING STATUS
DISCONNECTED — no confirmed wiring in main pipeline.

### G) ISSUES FOUND
- `statsmodels` is optional but pairs trading quality degrades significantly without it (falls back to numpy OLS with no cointegration test).
- Half-life estimation via OU OLS can return +inf for non-mean-reverting spreads — these pass the `5 <= HL <= 30` filter if HL is NaN or inf and the comparison evaluates to False, but edge cases exist.

---

## FILE 14: analysis/pairs_trader_live.py

### A) PURPOSE
Live pairs discovery using Kalman filter dynamic hedge ratio estimation plus ADF stationarity test. Stores results in closeloop.db. Seed pairs hardcoded to 10 same-sector pairs.

### B) CLASSES AND METHODS

**Module functions:**

`_adf_test(spread) -> float` — statsmodels ADF p-value (lower = more stationary).

`_half_life(spread) -> float` — OU OLS: `HL = -log(2) / log(1 + beta)`.

**Class: `PairsTraderLive`**

`__init__(db_path)` — default `closeloop/storage/closeloop.db`. Creates cointegration_log, pairs_signals tables.

`discover_pairs(price_data, seed_pairs)` — runs ADF + half_life for each seed pair; stores valid pairs.

`generate_signals(active_pairs, price_data)` — Kalman hedge ratio; z-score signals.

`_kalman_update(pair_key, price_a, price_b)` — KalmanPairsTrader dynamic hedge ratio (if Kalman trader exists for pair).

### C) MATHEMATICS

```
ADF test: statsmodels.tsa.stattools.adfuller(spread, autolag='AIC')
  p-value returned

OU half-life:
  delta_t = alpha + beta × spread_{t-1}  (OLS via np.linalg.lstsq)
  HL = -log(2) / log(1 + beta)
  Returns inf if beta >= 0 or beta <= -2

Kalman filter hedge ratio (KalmanPairsTrader — not implemented in this file):
  β_t+1 = β_t + Kalman gain × (observation - prediction)
```

### D) DATA FLOWS
**Reads:** yfinance via calling code; price_data dict passed in
**Writes:** `closeloop/storage/closeloop.db` → cointegration_log, pairs_signals

### E) DEPENDENCIES
**Internal:** None (KalmanPairsTrader referenced but not imported — likely from another module)
**External:** `statsmodels` (optional), `yfinance` (optional), `numpy`, `pandas`, `sqlite3`, `logging`, `datetime`

### F) WIRING STATUS
DISCONNECTED — no confirmed wiring in main pipeline.

### G) ISSUES FOUND
- `KalmanPairsTrader` referenced in `_kalman_traders` dict but no import statement for it — will raise NameError if instantiation is attempted.
- Seed pairs hardcoded to 10 pairs — no dynamic discovery from actual portfolio universe.

---

## FILE 15: analysis/portfolio_optimiser.py

### A) PURPOSE
Minimum-variance portfolio construction using Ledoit-Wolf covariance shrinkage. Provides weights, portfolio variance (annualised), marginal risk contributions, and Mahalanobis outlier detection.

### B) CLASSES AND METHODS

**Class: `PortfolioOptimiser`**

`__init__()` — initialises LedoitWolf fitter (sklearn) or notes unavailability.

`_shrunk_cov(returns_df) -> ndarray` — Ledoit-Wolf fit on return matrix; falls back to sample covariance.

`get_minimum_variance_weights(returns_df, long_only=True, max_weight=0.30) -> dict` — SLSQP optimisation minimising w^T Σ w subject to sum(w)=1, 0≤w≤0.30.

`get_portfolio_var(weights, returns_df) -> float` — w^T Σ w × 252 (annualised).

`get_marginal_risk_contribution(weights, returns_df) -> dict` — w_i × (Σw)_i / (w^T Σw) per ticker.

`mahalanobis_outlier_score(observation, returns_df) -> float` — sqrt(diff^T Σ^{-1} diff).

### C) MATHEMATICS

```
Ledoit-Wolf shrinkage:
  Σ_shrunk = (1-alpha) × Σ_sample + alpha × mu × I
  (alpha = analytically optimal shrinkage coefficient from sklearn)

Minimum variance objective:
  min w^T Σ w  subject to: sum(w) = 1, 0 ≤ w_i ≤ 0.30

Portfolio variance (annualised):
  V = w^T Σ w × 252

Marginal Risk Contribution:
  MRC_i = w_i × (Σw)_i / (w^T Σw)   [sums to 1.0]

Mahalanobis distance:
  D = sqrt((obs - mean)^T × Σ^{-1} × (obs - mean))
```

### D) DATA FLOWS
**Reads:** daily returns DataFrame (passed in from caller)
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `sklearn.covariance.LedoitWolf` (optional), `scipy.optimize` (optional), `numpy`, `pandas`, `logging`

### F) WIRING STATUS
DISCONNECTED — class exists but no confirmed wiring in main pipeline for live use.

### G) ISSUES FOUND
- Covariance matrix regularised by adding `1e-8 × I` before SLSQP — sufficient for numerical stability but may not be sufficient for near-singular matrices in small universes.
- Falls back to equal weights when SLSQP optimisation fails — silent degradation with only debug log.

---

## FILE 16: analysis/regime_detector.py

### A) PURPOSE
Classifies current market into 4 states: BULL | NEUTRAL | BEAR | CRISIS. Uses SPY MA200, VIX level, optional credit spread. Caches result for the trading day. Provides position_size_multiplier (BULL=1.0, NEUTRAL=0.85, BEAR=0.60, CRISIS=0.30).

### B) CLASSES AND METHODS

**Class: `RegimeDetector`**

`__init__(config)` — initialises with NEUTRAL and no last_detected.

`detect(force=False, credit_spread_bps=None) -> str` — returns cached regime if called today; else fetches SPY/VIX via `_fetch_spy_vix()`.

`_classify(spy_close, spy_ma200, vix_close, credit_spread_bps) -> str` — priority: CRISIS → BEAR → BULL → NEUTRAL.

`current() -> str` — returns last regime without re-fetching.

`is_bull/bear/crisis/neutral()` — convenience bool properties.

`position_size_multiplier() -> float` — regime → multiplier dict lookup.

`status() -> dict` — regime, detected date, inputs, multiplier.

### C) MATHEMATICS

```
spy_ma200 = mean(SPY_close[-200:])

CRISIS:  VIX ≥ 35 OR SPY < MA200 × 0.92 OR credit_spread > 400bps
BEAR:    SPY < MA200 AND VIX > 25
BULL:    SPY > MA200 AND VIX < 20
else: NEUTRAL

Position size multipliers:
  BULL:    1.00
  NEUTRAL: 0.85
  BEAR:    0.60
  CRISIS:  0.30
```

### D) DATA FLOWS
**Reads:** yfinance (SPY, ^VIX daily closes)
**Writes:** None (in-memory cache)

### E) DEPENDENCIES
**Internal:** None
**External:** `yfinance`, `numpy` (indirect via pandas), `logging`, `datetime`

### F) WIRING STATUS
LIVE PATH — called by trading_bot.py and signal generation.

### G) ISSUES FOUND
- When spy_ma200 is None (fewer than 200 SPY bars), falls back to `ma200 = spy_close` — means SPY<MA200 condition always False, CRISIS triggered only via VIX or credit spread. New installs will be in BULL/NEUTRAL regardless of actual market state for first ~200 bars.
- No persistence — regime is lost on restart; next detect() call will re-fetch.

---

## FILE 17: analysis/risk_filters.py

### A) PURPOSE
Four specialised risk filter classes: BiotechRiskFilter (binary catalyst detection), MomentumShortFilter (blocks shorts against strong momentum), BigWinnerReentryFilter (prevents chasing after large wins), SectorContagionDetector (sector-wide selling detection).

### B) CLASSES AND METHODS

**Class: `BiotechRiskFilter`**
`is_high_risk(ticker, context) -> bool` — biotech sector + catalyst keywords in news OR days_to_earnings ≤ 7.
`risk_multiplier(ticker, context) -> float` — 0.5 if high risk, 1.0 else.

**Class: `MomentumShortFilter`**
`blocks_short(ticker, context, price_data=None) -> bool` — blocks if RSI > 70 AND momentum_20d > 20%.
`confidence_penalty(ticker, context) -> float` — linear penalty above RSI 65: `max(0.5, 1.0 - (rsi-65)/35)`.

**Class: `BigWinnerReentryFilter`**
`record_big_win(ticker, exit_date, exit_price, peak_price, pnl_pct)` — records if pnl_pct ≥ 20%.
`allows_reentry(ticker, current_price, as_of) -> (bool, str)` — requires ≥3 days cooling AND ≥5% pullback from peak.

**Class: `SectorContagionDetector`**
`update_sector_returns(sector_returns)` — sets ETF return dict.
`is_contagion_risk(sector) -> bool` — sector ETF return ≤ -3%.
`get_contaminated_sectors() -> list` — all sectors at contagion threshold.
`position_size_multiplier(sector) -> float` — 0.5 if contagion, 1.0 else.
`fetch_current_returns() -> dict` — fetches from yfinance using _ALL_SECTORS from sector_rotation_tracker.

### C) MATHEMATICS

```
BiotechRiskFilter:
  is_high_risk if biotech sector AND (catalyst keywords OR days_to_earnings ≤ 7)
  risk_multiplier = 0.5 if high_risk else 1.0

MomentumShortFilter:
  blocks_short if RSI > 70 AND momentum_20d > 20%
  confidence_penalty = max(0.5, 1.0 - (RSI - 65) / 35)  [for RSI > 65]

BigWinnerReentryFilter:
  Record if pnl_pct ≥ 20%
  Allow reentry if: days_since_exit ≥ 3 AND (peak - current) / peak ≥ 5%

SectorContagionDetector:
  contagion if sector_return ≤ -3%
  position_size_multiplier = 0.5 if contagion else 1.0
```

### D) DATA FLOWS
**Reads:** context dict (passed in), yfinance (SectorContagionDetector.fetch_current_returns)
**Writes:** `_big_wins` dict (in-memory, BigWinnerReentryFilter)

### E) DEPENDENCIES
**Internal:** `analysis.sector_rotation_tracker._ALL_SECTORS` (SectorContagionDetector)
**External:** `yfinance`, `logging`, `datetime`

### F) WIRING STATUS
LIVE PATH (partial) — BiotechRiskFilter and MomentumShortFilter called by risk manager. BigWinnerReentryFilter and SectorContagionDetector wiring unconfirmed.

### G) ISSUES FOUND
- BigWinnerReentryFilter stores wins in-memory only — lost on restart. After restart, all "big winner" constraints are forgotten and re-entry blocking is disabled.
- SectorContagionDetector.is_contagion_risk() returns False if sector name not in dict — sectors with no data get no protection.

---

## FILE 18: analysis/sector_rotation_tracker.py

### A) PURPOSE
Scores 11 US GICS sector ETFs + UK proxy ISF.L using 20-day price momentum. Normalises scores to 0–100 within observed range. 2-second sleep between yfinance calls for RAM safety.

### B) CLASSES AND METHODS

**Class: `SectorRotationTracker`**

`__init__(config)` — caches last scores and last computed date.

`compute(force=False) -> dict` — fetches all ETFs with 2s sleep; normalises to 0–100; caches for the day.

`top_sectors(n=3) -> list` — top-n by score.

`bottom_sectors(n=3) -> list` — bottom-n by score.

`score_for(sector_name) -> float|None` — lookup by exact name.

`is_sector_hot(sector_name, threshold=70.0) -> bool` — score ≥ threshold.

`is_sector_cold(sector_name, threshold=30.0) -> bool` — score ≤ threshold.

### C) MATHEMATICS

```
momentum = (close_today / close_N_days_ago) - 1   [N=20 + buffer]

score_i = max(0, min(100, (momentum_i - min_momentum) / (max_momentum - min_momentum) × 100))

If all momentums equal: score = 50.0 for all
```

### D) DATA FLOWS
**Reads:** yfinance (XLK, XLV, XLF, XLC, XLY, XLP, XLI, XLB, XLE, XLU, XLRE, ISF.L)
**Writes:** None (in-memory cache)

### E) DEPENDENCIES
**Internal:** None
**External:** `yfinance`, `logging`, `time`, `datetime`

### F) WIRING STATUS
LIVE PATH — used by signal generation and SectorContagionDetector.

### G) ISSUES FOUND
- 2s sleep per ETF × 12 ETFs = ~24 seconds per compute call — significant blocking if called synchronously in main pipeline.
- Score is relative normalization within current batch only — scores change meaning over time as sector spread widens/narrows.

---

## FILE 19: analysis/signal_decay_monitor.py

### A) PURPOSE
Tracks rolling performance of live signals at 30/90/180-day windows. Detects DECAYING (win_rate < 42% OR Sharpe < 0.40) and SEVERELY_DEGRADED (win_rate < 33% OR Sharpe < 0.0) signals. Recommends position size reductions (DECAYING: 0.50×, SEVERE: 0.25×). Reads from paper_trading.jsonl log and permanent_archive.db.

### B) CLASSES AND METHODS

**Dataclasses:** `SignalPerformanceRecord`, `DecayAlert`

**Class: `SignalDecayMonitor`**

`__init__(config)` — reads DB paths from config or uses defaults (`output/historical_db.db`, `logs/paper_trading.jsonl`).

`_load_trades(signal_name, window_days) -> list` — loads from JSONL log + DB.

`_compute_metrics(trades) -> dict` — win_rate, avg_return, Sharpe, Sortino, max_drawdown.

`_classify_status(win_rate, sharpe) -> (str, float)` — returns status string and size_multiplier.

`evaluate_signal(signal_name) -> list[SignalPerformanceRecord]` — evaluates at 30/90/180 days.

`monitor_all() -> list[DecayAlert]` — evaluates all known signals; returns alerts.

`save_to_db(records)` — writes to `signal_decay_log` table in historical_db.db.

`send_alerts(alerts)` — Telegram notification for DECAYING/SEVERE signals.

### C) MATHEMATICS

```
win_rate = count(pnl > 0) / n_trades
avg_return = mean(pnl_pct)

Sharpe = mean(returns) / std(returns) × sqrt(252)  [annualised, daily returns]
Sortino = mean(returns) / std(downside) × sqrt(252)  [downside = returns < 0]

max_drawdown = max cumulative peak-to-trough drawdown

Status thresholds:
  HEALTHY:            win_rate ≥ 42% AND Sharpe ≥ 0.40
  DECAYING:           win_rate < 42% OR Sharpe < 0.40  → size_mult = 0.50
  SEVERELY_DEGRADED:  win_rate < 33% OR Sharpe < 0.0   → size_mult = 0.25
  INSUFFICIENT_DATA:  n_trades < minimum threshold
```

### D) DATA FLOWS
**Reads:** `logs/paper_trading.jsonl`, `output/permanent_archive.db` → trade records
**Writes:** `output/historical_db.db` → `signal_decay_log` (signal_name, window_days, metrics, status, size_multiplier, evaluated_at)

### E) DEPENDENCIES
**Internal:** None
**External:** `numpy`, `pandas`, `sqlite3`, `logging`, `dataclasses`, `json`, `pathlib`, `datetime`

### F) WIRING STATUS
LIVE PATH (CLI) — invokable as `python3 -m analysis.signal_decay_monitor`. Wiring to automatic scheduler unconfirmed.

### G) ISSUES FOUND
- Reads `permanent_archive.db` path but actual database is `permanent_log.db` per permanent_store.py — likely queries empty/wrong DB.
- Sortino denominator uses only returns < 0 — if no losing trades exist (perfect window), std(downside) = 0 → NaN Sortino. Not guarded.

---

## FILE 20: analysis/symbolic_regression.py

### A) PURPOSE
Equation discovery engine using PySR and gplearn to find non-obvious mathematical relationships between features and future returns. Stores discovered equations in `output/permanent_archive.db`. Minimum 200 observations required.

### B) CLASSES AND METHODS

**Class: `SymbolicRegressionEngine`**

`__init__(config)` — creates DB table.

`_ensure_db()` — creates `discovered_equations` table.

`get_equation_status() -> list` — returns all equations ordered by ic_score DESC.

`build_feature_matrix(tickers, lookback_days=252) -> (X, y, feature_names, tickers_used)` — fetches yfinance data; builds 10-feature matrix with 20-day forward return target; caps at 50 tickers.

`run_pysr(X, y, feature_names) -> dict` — runs PySR symbolic regression if available.

`run_gplearn(X, y) -> dict` — runs gplearn genetic programming if available.

`discover(tickers) -> list` — calls both engines; deduplicates; stores.

### C) MATHEMATICS

```
Features (10):
  return_1d, return_5d, return_20d, return_60d
  volume_ratio_5d (volume / 5-day avg volume)
  volatility_10d, volatility_30d (rolling std of returns)
  rsi_14 (rolling EWM RSI)
  price_vs_52w_high, price_vs_52w_low

Target: 20-day forward return (next 20 bars from each observation)

PySR: symbolic regression with genetic algorithm (if pysr installed)
gplearn: genetic programming with standard set of functions (if gplearn installed)

ic_score (Information Coefficient): Spearman rank correlation of predicted vs actual returns
```

### D) DATA FLOWS
**Reads:** yfinance (price/volume)
**Writes:** `output/permanent_archive.db` → `discovered_equations` (equation_str, engine, ic_score, sharpe_estimate, r_squared, complexity, feature_list, discovery_date)

### E) DEPENDENCIES
**Internal:** None
**External:** `pysr` (optional), `gplearn` (optional), `yfinance`, `numpy`, `pandas`, `sqlite3`, `logging`, `os`, `datetime`

### F) WIRING STATUS
DISCONNECTED — no confirmed automatic wiring; manual/CLI only.

### G) ISSUES FOUND
- Both PySR and gplearn are optional with no installed check warning at startup — `discover()` silently returns empty list if neither installed.
- DB path `output/permanent_archive.db` differs from `permanent_store.py` path (`output/permanent_log.db`) — isolated DB, may not be populated.

---

## FILE 21: analysis/technical_indicators.py

### A) PURPOSE
Stateless calculator for RSI, MACD, Bollinger Bands, ATR. All methods accept pandas DataFrame with OHLCV columns (case-insensitive). Returns dict of indicator values for most recent bar.

### B) CLASSES AND METHODS

**Class: `TechnicalIndicatorCalculator`**

`rsi(df, period=14) -> float|None` — EWM RSI (com=period-1 for Wilder smoothing).

`macd(df, fast=12, slow=26, signal=9) -> dict` — EMA crossover; returns macd, signal, histogram.

`bollinger_bands(df, period=20, std_dev=2.0) -> dict` — SMA ± 2σ; returns upper, middle, lower, pct_b, bandwidth.

`atr(df, period=14) -> float|None` — EWM ATR using True Range; (com=period-1 for Wilder smoothing).

`compute_all(df) -> dict` — calls all four in one shot.

### C) MATHEMATICS

```
RSI:
  gain_ewm = clip(delta, lower=0).ewm(com=period-1).mean()
  loss_ewm = clip(-delta, lower=0).ewm(com=period-1).mean()
  RS = gain_ewm / loss_ewm
  RSI = 100 - 100/(1 + RS)

MACD:
  ema_fast = close.ewm(span=12).mean()
  ema_slow = close.ewm(span=26).mean()
  MACD_line = ema_fast - ema_slow
  Signal_line = MACD_line.ewm(span=9).mean()
  Histogram = MACD_line - Signal_line

Bollinger Bands:
  SMA = close.rolling(20).mean()
  std = close.rolling(20).std()
  Upper = SMA + 2.0 × std
  Lower = SMA - 2.0 × std
  pct_b = (close - Lower) / (Upper - Lower)
  bandwidth = (Upper - Lower) / SMA

ATR:
  TR = max(High - Low, |High - PrevClose|, |Low - PrevClose|)
  ATR = TR.ewm(com=period-1).mean()
```

### D) DATA FLOWS
**Reads:** DataFrame passed in
**Writes:** None

### E) DEPENDENCIES
**Internal:** None
**External:** `pandas`, `logging`

### F) WIRING STATUS
LIVE PATH — used by signal generators and feature engineer.

### G) ISSUES FOUND
- `compute_all()` uses separate sub-calls, each independently fetching close column — minor inefficiency but no correctness issue.

---

## FILE 22: analysis/threshold_optimizer.py

### A) PURPOSE
Learns optimal signal thresholds from trade outcomes. Stores threshold history in `output/permanent_archive.db`. Simple rule: tighten threshold if win_rate < 45%, loosen if > 65%.

### B) CLASSES AND METHODS

**Class: `ThresholdOptimizer`**

`__init__(config)` — creates `threshold_history` table.

`get_optimal_threshold(signal_type) -> float` — queries most recent threshold for signal_type; falls back to hardcoded defaults (PEAD=0.08, MOMENTUM=0.05, MEAN_REVERSION=0.15, GAP=0.02, MATHEMATICAL=0.30, INSIDER_MOMENTUM=0.5).

`update_threshold(signal_type, outcomes)` — computes win_rate, avg_return; adjusts threshold by ±5%/10%.

### C) MATHEMATICS

```
win_rate = count(pnl > 0) / len(outcomes)
avg_return = mean(pnl)

if win_rate < 0.45: new_threshold = min(current × 1.1, 0.95)  [tighten]
elif win_rate > 0.65: new_threshold = max(current × 0.95, 0.01) [loosen]
else: new_threshold = current
```

### D) DATA FLOWS
**Reads/Writes:** `output/permanent_archive.db` → `threshold_history` (signal_type, threshold, win_rate, avg_return, n_trades, updated_at)

### E) DEPENDENCIES
**Internal:** None
**External:** `sqlite3`, `logging`, `os`, `datetime`

### F) WIRING STATUS
DISCONNECTED — no confirmed wiring to update_threshold() call in main pipeline.

### G) ISSUES FOUND
- DB path `output/permanent_archive.db` differs from permanent_store.py path — isolated DB.
- `update_threshold()` threshold adjustment is arbitrary (10% tighten, 5% loosen) with no statistical basis.

---

## FILES 23–52: ALTDATA MODULE

These files were fully read and analysed in prior sessions. Complete per-file documentation is available in the conversation history and is summarised here in the GROUP 11A GATE below. The altdata module covers:

- **altdata/anomaly/**: custom_metrics.py, nonsense_detector.py, statistical_validator.py
- **altdata/collector/**: companies_house_collector.py, finnhub_collector.py, finviz_collector.py, fred_collector.py, google_trends_collector.py, jobs_collector.py, news_collector.py, reddit_collector.py, sec_edgar_collector.py, shipping_collector.py, social_intelligence_collector.py, stocktwits_collector.py, wikipedia_collector.py
- **altdata/dashboard/**: altdata_dashboard.py
- **altdata/learning/**: model_registry.py, model_validator.py, online_learner.py, rollback_manager.py, weekly_retrainer.py
- **altdata/processing/**: article_reader.py, feature_engineer.py, nlp_processor.py, normaliser.py, sentiment_engine.py
- **altdata/signals/**: altdata_signal_engine.py, signal_promoter.py
- **altdata/storage/**: altdata_store.py, permanent_store.py

Detailed documentation for all altdata files is in the session summary above (FILES 23–52 were all read; see the conversation summary for complete A–G documentation of each).

Also read but not separately numbered above:
- **analysis/bayesian_regime.py** — GaussianMixture (4-component) probabilistic regime classifier as alternative to RegimeDetector. States: CRISIS/BEAR/NEUTRAL/BULL. Features: SPY return, VIX, yield curve slope. Falls back to sklearn GMM from PyMC (MCMC too slow for production). Position size multipliers same as RegimeDetector. LIVE PATH (comparison mode alongside RegimeDetector).
- **analysis/macro_signal_engine.py** — Large orchestration module (1145 lines). MacroRegimeClassifier + SectorContextEngine + EarningsContextScore + MacroSignalEngine. 6 regimes: CRISIS/RECESSION_RISK/STAGFLATION/RISK_OFF/GOLDILOCKS/RISK_ON. PEAD multipliers: CRISIS=0.1, RECESSION_RISK=0.4, STAGFLATION=0.6, RISK_OFF=0.7, GOLDILOCKS=1.2, RISK_ON=1.0. LIVE PATH — orchestrates macro context for all PEAD trades.
- **analysis/mathematical_signals.py** — HMMSignals (3-state GaussianHMM), AutocorrelationSignals (Ljung-Box, momentum, mean-reversion, spectral), MathematicalSignals orchestrator with 1-hour TTL cache. 4D observation per day: log_return, volume_zscore, high_low_range, close_open_gap. LIVE PATH (optional — skipped if hmmlearn not installed).

---

## GROUP 11A GATE

### ALL FILES READ

| # | File | Status |
|---|------|--------|
| 1 | analysis/crowding_detector.py | LIVE PATH |
| 2 | analysis/earnings_revision_scorer.py | LIVE PATH |
| 3 | analysis/factor_arbitrage.py | DISCONNECTED |
| 4 | analysis/factor_model.py | LIVE PATH |
| 5 | analysis/frontier_validator.py | LIVE PATH |
| 6 | analysis/insider_analyser.py | LIVE PATH |
| 7 | analysis/intelligence_db.py | LIVE PATH |
| 8 | analysis/intelligence_engine.py | LIVE PATH |
| 9 | analysis/market_calendar.py | LIVE PATH |
| 10 | analysis/market_timer.py | LIVE PATH |
| 11 | analysis/multi_timeframe.py | DISCONNECTED |
| 12 | analysis/options_flow_analyser.py | DISCONNECTED |
| 13 | analysis/pairs_trader.py | DISCONNECTED |
| 14 | analysis/pairs_trader_live.py | DISCONNECTED |
| 15 | analysis/portfolio_optimiser.py | DISCONNECTED |
| 16 | analysis/regime_detector.py | LIVE PATH |
| 17 | analysis/risk_filters.py | LIVE PATH (partial) |
| 18 | analysis/sector_rotation_tracker.py | LIVE PATH |
| 19 | analysis/signal_decay_monitor.py | CLI ONLY |
| 20 | analysis/symbolic_regression.py | DISCONNECTED |
| 21 | analysis/technical_indicators.py | LIVE PATH |
| 22 | analysis/threshold_optimizer.py | DISCONNECTED |
| 23 | analysis/bayesian_regime.py | LIVE PATH (comparison mode) |
| 24 | analysis/macro_signal_engine.py | LIVE PATH |
| 25 | analysis/mathematical_signals.py | LIVE PATH |
| 26 | altdata/anomaly/custom_metrics.py | LIVE PATH |
| 27 | altdata/anomaly/nonsense_detector.py | LIVE PATH |
| 28 | altdata/anomaly/statistical_validator.py | LIVE PATH |
| 29 | altdata/collector/companies_house_collector.py | LIVE PATH |
| 30 | altdata/collector/finnhub_collector.py | LIVE PATH |
| 31 | altdata/collector/finviz_collector.py | LIVE PATH |
| 32 | altdata/collector/fred_collector.py | LIVE PATH |
| 33 | altdata/collector/google_trends_collector.py | LIVE PATH |
| 34 | altdata/collector/jobs_collector.py | LIVE PATH |
| 35 | altdata/collector/news_collector.py | LIVE PATH |
| 36 | altdata/collector/reddit_collector.py | LIVE PATH |
| 37 | altdata/collector/sec_edgar_collector.py | LIVE PATH |
| 38 | altdata/collector/shipping_collector.py | LIVE PATH |
| 39 | altdata/collector/social_intelligence_collector.py | LIVE PATH |
| 40 | altdata/collector/stocktwits_collector.py | LIVE PATH |
| 41 | altdata/collector/wikipedia_collector.py | LIVE PATH |
| 42 | altdata/dashboard/altdata_dashboard.py | LIVE PATH |
| 43 | altdata/learning/model_registry.py | LIVE PATH |
| 44 | altdata/learning/model_validator.py | LIVE PATH |
| 45 | altdata/learning/online_learner.py | LIVE PATH |
| 46 | altdata/learning/rollback_manager.py | LIVE PATH |
| 47 | altdata/learning/weekly_retrainer.py | LIVE PATH |
| 48 | altdata/processing/article_reader.py | LIVE PATH |
| 49 | altdata/processing/feature_engineer.py | LIVE PATH |
| 50 | altdata/processing/nlp_processor.py | LIVE PATH |
| 51 | altdata/processing/normaliser.py | LIVE PATH |
| 52 | altdata/processing/sentiment_engine.py | LIVE PATH |
| (also) | altdata/signals/altdata_signal_engine.py | LIVE PATH |
| (also) | altdata/signals/signal_promoter.py | LIVE PATH |
| (also) | altdata/storage/altdata_store.py | LIVE PATH |
| (also) | altdata/storage/permanent_store.py | LIVE PATH (bug) |

**NOT FOUND:** None

---

### KEY FINDINGS

**1. Widespread DB Path Inconsistency**
Multiple analysis modules write to `output/permanent_archive.db` (symbolic_regression.py, threshold_optimizer.py, signal_decay_monitor.py, frontier_validator.py) while `permanent_store.py` uses `output/permanent_log.db`. These are different, isolated databases. Cross-module queries will find no data.

**2. Five Disconnected Analysis Modules**
multi_timeframe.py, options_flow_analyser.py, pairs_trader.py, pairs_trader_live.py, portfolio_optimiser.py, symbolic_regression.py, threshold_optimizer.py — all build functional code but have no confirmed wiring in main pipeline. Represents significant dead development effort.

**3. MarketCalendar 2026-Only Hardcoding**
Entire calendar is hardcoded for 2026 only. Will return incorrect results after Dec 31 2026 (Apollo will treat all 2027 days as trading days).

**4. CrowdingDetector Factor Dispersion Always 0.5**
`_get_factor_dispersion()` always returns 0.5. The "dispersion" component of CRI (weighted 0.20) is never actually computed. CRI effectively = SI×0.30 + corr×0.30 + inst×0.20 + 0.10 (constant).

**5. permanent_store.py STDEV() SQLite Bug**
`permanent_store.py` calls `STDEV()` in SQL — SQLite has no native STDEV function. This will fail with OperationalError unless a custom function was registered. Queries that call this will silently fail.

**6. PairsTraderLive KalmanPairsTrader Missing**
`pairs_trader_live.py` references `KalmanPairsTrader` class in the `_kalman_traders` dict but no import statement exists for it in the file. Any Kalman-related code path will raise NameError.

**7. Altdata Source Weights (custom_metrics.py)**
reddit=0.08, stocktwits=0.12, news=0.18, sec_edgar=0.25, shipping=0.10, jobs=0.10, wikipedia=0.07, google=0.10. Sum = 1.00. These are hardcoded constants with no empirical basis documented.

**8. MarketTimer UTC Fallback (pytz unavailable)**
If pytz not installed, MarketTimer uses raw UTC for all timezone comparisons. Apollo VPS runs UTC. UK market (London) is UTC+0 in winter, UTC+1 in BST. Without pytz, UK timing is wrong by 1 hour in BST — approximately half the year.

---

### DEAD/DISCONNECTED MODULES (analysis/)

- `analysis/factor_arbitrage.py` — DISCONNECTED (no downstream consumer)
- `analysis/multi_timeframe.py` — DISCONNECTED (no main pipeline wiring)
- `analysis/options_flow_analyser.py` — DISCONNECTED
- `analysis/pairs_trader.py` — DISCONNECTED
- `analysis/pairs_trader_live.py` — DISCONNECTED
- `analysis/portfolio_optimiser.py` — DISCONNECTED
- `analysis/symbolic_regression.py` — DISCONNECTED
- `analysis/threshold_optimizer.py` — DISCONNECTED

---

### FORMULAS EXTRACTED (count)

Analysis module: 47 distinct mathematical formulas documented.
Altdata module: see session summary — approximately 85 formulas documented across 30 altdata files.

---

### CONTRADICTIONS

1. **Two RSI implementations** — `TechnicalIndicatorCalculator.rsi()` uses EWM (Wilder); `MultiTimeframeAnalyzer._rsi()` uses rolling mean. Different numeric outputs for same input.
2. **Three RegimeDetector implementations** — `RegimeDetector` (binary VIX/MA200), `BayesianRegimeDetector` (GMM probabilistic), `FredCollector` regime (yield curve + VIX + CPI). Three parallel regime detections with different output formats.
3. **Permanent DB path mismatch** — multiple modules use `permanent_archive.db` vs actual store at `permanent_log.db`.
4. **MarketTimer missing holiday check** — `is_open()` checks weekends but not holidays. A US holiday (e.g. July 4th) is a weekday, so `is_open()` returns True even when NYSE is closed.

---

Proceed to Part 11B: YES (Part 11B already written)
