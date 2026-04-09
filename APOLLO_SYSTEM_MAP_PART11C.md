# Apollo System Map — Part 11C
## Comprehensive Documentation: Core, Data, Intelligence, Frontier, Signals, Execution

---

## FILE 1: core/async_infrastructure.py

### A) Purpose
Pure async HTTP infrastructure layer. Provides token-bucket rate limiting, TTL caching with stale-value fallback, per-API health tracking with auto-disable, and a universal safe_fetch() function. No signal or trade logic.

### B) Classes and Methods

**RateLimiter**
- `__init__(calls_per_second, burst_limit=5)` — initialises token bucket; sets `_tokens = burst_limit`, `_last_refill = time.monotonic()`, `_lock = asyncio.Lock()`
- `acquire()` — async; refills tokens proportional to elapsed time; if `_tokens < 1`, sleeps `(1 - _tokens) / calls_per_second` seconds then sets `_tokens = 0`; otherwise decrements `_tokens -= 1`

**TTLCache**
- `__init__()` — empty `_store: Dict[str, tuple]`; stores `(value, expiry_time)`
- `get(key)` — returns value if `time.monotonic() < expiry`; else None
- `set(key, value, ttl_seconds)` — stores `(value, time.monotonic() + ttl_seconds)`
- `get_stale(key)` — returns value even if expired; None if key absent

**APIHealthTracker**
- `__init__(failure_threshold=10, window=60)` — `_failures`, `_disabled`, `_latencies` as defaultdict/dicts
- `record_success(api_name, latency_ms)` — appends to `_latencies`; prunes entries older than `window`
- `record_failure(api_name)` — appends timestamp; prunes window; if `len(failures) >= failure_threshold`, disables for 300 seconds
- `is_available(api_name)` — checks `_disabled` dict; re-enables if cooldown expired; returns bool
- `get_avg_latency(api_name)` — mean of recent latencies in window; None if empty

**safe_fetch()** (module-level async function)
- Inputs: session, url, api_name, rate_limiter, cache, health_tracker, cache_key, ttl=60, timeout_sec=3.0, max_retries=2, fallback_value=None, params, headers
- Returns: cached value → stale value → fallback_value; never raises
- Logic: check fresh cache → check health → retry loop up to max_retries+1 with 0.5*(attempt+1) sleep between; on HTTP 200 caches and returns; on failure records to health_tracker

### C) Mathematics
- Token refill: `tokens = min(burst_limit, tokens + elapsed * calls_per_second)`
- Wait time when tokens < 1: `wait_time = (1 - tokens) / calls_per_second`
- Latency: `latency_ms = (time.monotonic() - start) * 1000`
- Retry sleep: `0.5 * (attempt + 1)` seconds

### D) Data Flows
- No DB reads or writes
- In-memory TTLCache only
- HTTP GET via aiohttp.ClientSession

### E) Dependencies
- Internal: none
- External: asyncio, aiohttp, time, logging, collections.defaultdict, typing

### F) Wiring Status
[LIVE PATH] — imported by data collectors and any module using safe_fetch

### G) Issues Found
- `_tokens = 0` after sleep rather than a fractional remainder — minor token accounting imprecision
- APIHealthTracker `record_success` prunes latencies on every call using `time.monotonic()` called twice in same line (inside list comprehension), not the same `now` used for the append — minor drift

---

## FILE 2: core/scan_scheduler.py

### A) Purpose
Three-tier async scan scheduler. Tier 1 (price/vol/volatility, default 20s), Tier 2 (news/earnings/macro, default 120s), Tier 3 (alt data, default 600s). Each tier runs in its own asyncio loop so a slow Tier 3 never blocks Tier 1.

### B) Classes and Methods

**TieredScanScheduler**
- `__init__(tier1_interval=20, tier2_interval=120, tier3_interval=600)` — stores intervals; `_running=False`; three collector lists; `_scan_count=0`; `_last_full_cycle=None`
- `register_collector(collector_fn, tier)` — appends `collector_fn` to `_tier{tier}_collectors` list; silently ignores invalid tier numbers
- `_run_tier(tier_num, collectors, interval)` — async loop; `asyncio.gather(*[c() for c in collectors], return_exceptions=True)`; logs exceptions per collector; sleeps `max(0, interval - elapsed)`
- `start()` — sets `_running=True`; calls `asyncio.gather` over all three `_run_tier` coroutines
- `stop()` — sets `_running=False`; does not cancel running coroutines

### C) Mathematics
- Sleep: `sleep_time = max(0, interval - elapsed)` where `elapsed = time.monotonic() - start`

### D) Data Flows
- No DB reads or writes
- Calls registered collector coroutines; results are discarded (only exceptions logged)

### E) Dependencies
- Internal: none
- External: asyncio, logging, time, datetime

### F) Wiring Status
[LIVE PATH] — instantiated in main orchestration

### G) Issues Found
- `_scan_count` and `_last_full_cycle` are initialised but never updated — dead tracking fields
- `stop()` only sets `_running=False`; any currently-sleeping `asyncio.sleep` in `_run_tier` will complete before the loop checks `_running` again — graceful shutdown can take up to `interval` seconds with no cancellation
- Invalid tier numbers silently drop the collector with no warning

---

## FILE 3: data/db_utils.py

### A) Purpose
Shared SQLite connection utility. Ensures every connection uses WAL journal mode, 30-second busy timeout, NORMAL synchronous mode, 32MB page cache, and foreign key enforcement. Provides `open_db()` and `patch_sqlite3()`.

### B) Classes and Methods

**open_db(path, **kwargs)**
- Returns `sqlite3.Connection` with 5 PRAGMAs applied
- `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=30000`, `PRAGMA synchronous=NORMAL`, `PRAGMA cache_size=-32000`, `PRAGMA foreign_keys=ON`
- PRAGMA errors caught and logged as warnings; connection is returned regardless

**patch_sqlite3()**
- Monkey-patches `sqlite3.connect` globally; wraps original connect; applies same 5 PRAGMAs silently (`:memory:` DBs ignore WAL without error)
- Logs info on successful patch

### C) Mathematics
- `_WAL_TIMEOUT_MS = 30_000` (30 seconds)
- `cache_size = -32000` (negative = KB units → 32 MB)

### D) Data Flows
- No reads or writes; manages connections only

### E) Dependencies
- Internal: none
- External: sqlite3, logging

### F) Wiring Status
[LIVE PATH] — `patch_sqlite3()` called once at process startup in main.py

### G) Issues Found
- None significant

---

## FILE 4: data/cleaner.py

### A) Purpose
OHLCV data cleaning and return computation. Removes bad data, enforces OHLC consistency, forward-fills gaps up to 5 days, and removes outlier single-day moves >50%.

### B) Classes and Methods

**DataCleaner**
- `clean_ohlcv(df)` — drops rows where `close` is NaN; forward-fills up to 5 periods; drops rows with negative close or negative volume; identifies single-day moves >50% (`abs(close.pct_change()) > 0.50`) and replaces with NaN then forward-fills; re-enforces OHLC: `high = max(open,high,low,close)`, `low = min(open,high,low,close)`
- `compute_returns(df, col='close')` — `df[col].pct_change().dropna()`
- `compute_log_returns(df, col='close')` — `np.log(df[col] / df[col].shift(1)).dropna()`

### C) Mathematics
- Outlier detection: `abs(close.pct_change()) > 0.50`
- Log returns: `ln(P_t / P_{t-1})`
- OHLC re-enforcement: `high = max(O,H,L,C)`, `low = min(O,H,L,C)` per row

### D) Data Flows
- Input: pandas DataFrame with OHLCV columns
- Output: cleaned DataFrame; no DB interaction

### E) Dependencies
- Internal: none
- External: pandas, numpy, logging

### F) Wiring Status
[LIVE PATH] — called by DataFetcher and signal computation

### G) Issues Found
- Forward-fill after outlier replacement propagates stale close into the clean row; OHLC re-enforcement then uses that stale close for high/low calculation — stale data can persist

---

## FILE 5: data/earnings_cache.py

### A) Purpose
JSON file cache for earnings data. Rebuilt daily. Method 1: Finnhub bulk calendar (3-year lookback + 90-day forward). Method 2: yfinance fallback for first 200 tickers missing data.

### B) Classes and Methods

**EarningsCache**
- `__init__(config)` — sets `cache_path = output/earnings_cache.json`
- `needs_refresh()` — True if `cache_date != today` OR `covered < 10`
- `bulk_fetch(tickers)` — Finnhub `/calendar/earnings?from=3yr_ago&to=+90d`; then yfinance fallback for first 200 missing tickers using `.earnings_dates`
- `get(ticker)` — returns cached list of earnings dicts for ticker

### C) Mathematics
- Lookback: 3 years (1095 days)
- Forward: +90 days
- Yfinance fallback capped at 200 tickers

### D) Data Flows
- DB reads: none
- DB writes: none
- File: `output/earnings_cache.json` (read/write)
- External: Finnhub REST API, yfinance

### E) Dependencies
- Internal: none
- External: requests, yfinance, json, datetime, pathlib

### F) Wiring Status
[LIVE PATH] — used by EarningsCalendar

### G) Issues Found
- Yfinance fallback capped at 200 tickers — remaining tickers get no earnings data if Finnhub misses them

---

## FILE 6: data/earnings_calendar.py

### A) Purpose
4-source earnings surprise data merger. Sources: Finnhub (primary), Alpha Vantage (secondary), yfinance `earnings_dates` (tertiary, 25+ quarters), yfinance `earnings_history` (last resort, 4 quarters). De-duplicates by keeping first occurrence.

### B) Classes and Methods

**EarningsCalendar**
- `get_earnings_surprise(ticker)` — merges all 4 sources; de-duplicates by `(ticker, date)`; returns list of dicts with `surprisePercent` stored as fraction
- `_finnhub_earnings(ticker)` — calls Finnhub `/stock/earnings`; divides surprise by 100
- `_alphavantage_earnings(ticker)` — calls AV `EARNINGS`; divides surprise by 100
- `_yfinance_earnings_dates(ticker)` — uses `.earnings_dates` property; up to 25 quarters
- `_fallback_earnings(ticker)` — uses `.quarterly_earnings`; renames columns

### C) Mathematics
- `surprisePercent = raw_value / 100` (applied by both Finnhub and AV parsers — stored as fraction, e.g. 0.1676 for 16.76%)

### D) Data Flows
- DB reads: none
- External APIs: Finnhub, Alpha Vantage, yfinance
- Output: list of dicts per ticker

### E) Dependencies
- Internal: none
- External: requests, yfinance, pandas, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_fallback_earnings()` renames `"Earnings"` column to `"epsActual"` from `quarterly_earnings` — but `quarterly_earnings["Earnings"]` is revenue, not EPS — silent data corruption in fallback path
- `surprisePercent` stored as fraction but field name says "Percent" — naming inconsistency across codebase

---

## FILE 7: data/earnings_collector.py

### A) Purpose
Collects historical earnings observations (7-year default) and upcoming 30-day forward calendar. Builds per-event records with surprise %, price returns at t+0/1/3/5/10/20, volume surge, sentiment, and intelligence signals.

### B) Classes and Methods

**EarningsCollector**
- `collect(tickers, lookback_days=2555)` — for each ticker: fetches earnings from EarningsCalendar; fetches OHLCV; calls `_build_record()`; upserts to EarningsDB
- `collect_calendar(tickers, forward_days=30)` — fetches upcoming earnings events; stores to `earnings_calendar_forward`
- `_build_record(ticker, event, ohlcv_df)` — builds full observation record
- `_spy_return_5d(date)` — fetches SPY OHLCV; computes `close[-1]/close[0] - 1`
- `run_intelligence_update()` — triggers intelligence signal generation
- `get_intelligence_signals()` — returns cached intelligence signals

### C) Mathematics
- `surprise_pct = (actual - estimate) / abs(estimate)`
- Price return at horizon: `ret(tx) = close_tx / close_t0 - 1`
- Volume surge: `volume_surge = vol_t0 / vol_avg` (20-day average)
- SPY 5d return: `close[-1] / close[0] - 1` over last available days

### D) Data Flows
- DB reads: none directly (reads via EarningsCalendar)
- DB writes: EarningsDB.upsert_observations_batch(), upsert_calendar_batch()
- External: yfinance, EarningsCalendar, AltDataStore (lazy-loaded)

### E) Dependencies
- Internal: data.earnings_calendar, data.earnings_db, data.fetcher
- External: yfinance, pandas, numpy, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `surprise_pct` computed as `(actual-estimate)/abs(estimate)` but yfinance pct path may divide by 100 again when yfinance already returns fraction — potential double-division on yfinance path
- `_spy_return_5d()` uses `close[-1]/close[0] - 1` over the full available slice ending at event date — not a strict 5-trading-day window

---

## FILE 8: data/earnings_db.py

### A) Purpose
SQLite persistence for earnings data. Three tables: `earnings_observations` (historical with UNIQUE ticker+date), `earnings_calendar_forward` (upcoming), `pre_earnings_snapshots`. Thread-local connections, WAL mode, sqlite3.Row factory.

### B) Classes and Methods

**EarningsDB**
- `__init__(db_path)` — sets path; initialises thread-local connection
- `_conn()` — thread-local connection with WAL + busy_timeout + Row factory
- `_migrate_schema()` — adds `beat_quality_multiplier` + 9 insider columns via ALTER TABLE IF NOT EXISTS; silently skips existing columns
- `upsert_observations_batch(records)` — row-by-row INSERT OR REPLACE loop
- `upsert_calendar_batch(events)` — row-by-row upsert to `earnings_calendar_forward`
- `get_observations(ticker, limit)` — SELECT from earnings_observations ORDER BY date DESC
- `update_snapshot_outcome(ticker, date, outcome_data)` — updates pre_earnings_snapshots
- `get_pre_earnings_snapshot(ticker, date)` — SELECT from pre_earnings_snapshots

### C) Mathematics
- None in DB layer

### D) Data Flows
- DB reads: earnings_observations, earnings_calendar_forward, pre_earnings_snapshots
- DB writes: all three tables
- File: `data/earnings.db`

### E) Dependencies
- Internal: data.db_utils (indirectly via WAL settings)
- External: sqlite3, threading, logging, datetime

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `update_snapshot_outcome()`: `now_str` is assigned twice — first value immediately overwritten; dead code on line ~95
- `upsert_observations_batch()`: row-by-row upsert inside a loop is not true bulk insert — no executemany

---

## FILE 9: data/earnings_scheduler.py

### A) Purpose
Schedules daily earnings collection at 18:00 UK time every weekday. Collects last 90 days of observations and 30-day forward calendar using the `schedule` library.

### B) Classes and Methods

**EarningsScheduler**
- `__init__(config)` — sets up EarningsCollector and EarningsDB
- `_uk_time_str()` — returns current time as Europe/London string for display
- `_is_weekday()` — `datetime.now().weekday() < 5`
- `run_collection()` — calls collect(last 90 days) + collect_calendar(30 days)
- `start()` — registers `schedule.every().day.at("18:00").do(job)`; runs schedule loop

### C) Mathematics
- Lookback: 90 days
- Forward: 30 days

### D) Data Flows
- Calls EarningsCollector which writes to EarningsDB

### E) Dependencies
- Internal: data.earnings_collector, data.earnings_db
- External: schedule, datetime, pytz, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_is_weekday()` uses `datetime.now().weekday()` — local server time, not UK time. If VPS is in UTC and the scheduled 18:00 fires in UTC, it may fire on a UK Saturday if the UK is observing BST (UTC+1). `_uk_time_str()` correctly uses `Europe/London` but `_is_weekday()` does not.

---

## FILE 10: data/fetcher.py

### A) Purpose
DataFetcher: pickle-based file cache for OHLCV and ticker info. Fetches single or batch tickers via yfinance. Applies UK price divisor (pence→GBP). Handles MultiIndex column output from yfinance.

### B) Classes and Methods

**DataFetcher**
- `__init__(config)` — sets cache dir `data/cache/`; default TTL 24h; price_divisor from config (default 100 for UK)
- `fetch_ohlcv(ticker, start, end, market)` — checks pickle cache; downloads via yfinance; applies divisor if UK; calls DataCleaner.clean_ohlcv(); saves to pickle
- `fetch_universe_data(tickers, start, end, market)` — batches into groups of 50; single-ticker vs multi-ticker yfinance download paths; normalises columns
- `fetch_ticker_info(ticker)` — TTL 168h (1 week); returns yfinance `.info` dict
- `_extract_ticker(df, ticker)` — extracts single ticker from MultiIndex DataFrame
- `normalize_columns(df)` — lowercases column names; renames standard yfinance column names

### C) Mathematics
- UK price adjustment: `close = close / price_divisor` (default /100)
- Cache TTL: configurable, default 86400 seconds (24h)

### D) Data Flows
- DB reads: none
- File reads/writes: `data/cache/*.pkl`
- External: yfinance

### E) Dependencies
- Internal: data.cleaner
- External: yfinance, pandas, pickle, pathlib, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `fetch_universe_data()` single-ticker path calls `normalize_columns()` directly; multi-ticker path uses `_extract_ticker()` then normalizes — divergent code paths may produce different column sets
- UK price divisor applied to OHLCV but `fetch_ticker_info()` market cap is returned raw (yfinance may report in USD for .L tickers)

---

## FILE 11: data/historical_db.py

### A) Purpose
12-table SQLite store for all historical data: price_history, quarterly_financials, balance_sheet, cash_flow, edgar_filings, insider_transactions, proxy_data, institutional_ownership, macro_context, news_context, earnings_enriched, delisted_companies. Thread-local connections, WAL+NORMAL synchronous.

### B) Classes and Methods

**HistoricalDB**
- `__init__(db_path)` — thread-local; `_init_db()` creates all 12 tables
- `_conn()` — thread-local WAL connection with Row factory
- `_upsert(table, record)` — single-row `INSERT OR REPLACE`
- `_upsert_many(table, records, conflict_cols)` — `INSERT INTO ... ON CONFLICT DO UPDATE SET excluded.*`
- `upsert_prices(ticker, records)` — writes to price_history
- `upsert_financials / upsert_balance_sheet / upsert_cash_flow` — writes to respective tables
- `upsert_filings / upsert_insider_txns / upsert_proxy / upsert_institutional` — writes respective tables
- `upsert_macro / upsert_news / upsert_enriched / upsert_delisted` — writes remaining tables
- `status()` — returns dict of row counts for all 12 tables

### C) Mathematics
- None in storage layer

### D) Data Flows
- DB file: `data/historical_db.db`
- All 12 tables read and written

### E) Dependencies
- Internal: data.db_utils (WAL settings inherited via patch_sqlite3)
- External: sqlite3, threading, logging, json

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `ON CONFLICT DO UPDATE SET excluded.*` is not standard SQLite syntax — should be explicit column list; behaviour depends on SQLite version (may fail silently on older versions)

---

## FILE 12: data/large_cap_influence.py

### A) Purpose
Tracks 100 large-cap bellwether stocks across 11 sectors to generate readthrough signals for peer stocks. Computes historical correlation coefficients between large-cap 1-day returns and peer 5-day returns.

### B) Classes and Methods

**LargeCapInfluenceEngine**
- `__init__(config)` — loads 100 bellwethers with sector weights (1.0–2.0)
- `get_recent_large_cap_events(days=5)` — fetches OHLCV for all bellwethers; computes 1-day returns
- `get_readthrough_signals(sector, days=5)` — filters bellwethers by sector; returns events with signals
- `compute_historical_coefficients(large_cap_ticker, peer_ticker)` — Pearson corr of t+1 large-cap returns vs t+5 peer returns; `coeff = 1.0 + corr`, clamped [0.1, 2.0]
- `score_peer(peer_ticker, sector)` — weighted composite of large-cap signals for peer
- `_classify_signal(score, return_t1)` — maps score to STRONG_POSITIVE/POSITIVE/NEUTRAL/NEGATIVE/STRONG_NEGATIVE

### C) Mathematics
- Raw signal: `raw = (sp * 0.6) + (r1 * 0.4)` where sp = sector performance, r1 = 1-day return
- Scaled: `signal = raw * 10`, clamped [-1, 1]
- Composite: `weighted_sum / total_weight` where `adjusted_weight = base_weight * max(0.1, coeff)`
- Historical coeff: `coeff = 1.0 + pearson_corr(large_t1_returns, peer_t5_returns)`, clamped [0.1, 2.0]

### D) Data Flows
- External: yfinance for price history
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: yfinance, pandas, numpy, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `score_peer()` calls `_classify_signal(composite, return_t1=0)` — always passes `return_t1=0`, so classification is purely score-based; return confirmation is ignored

---

## FILE 13: data/universe_builder.py

### A) Purpose
Builds universe CSV files from Wikipedia, StockAnalysis.com ETF holdings, and hardcoded supplemental lists. Outputs tier CSVs for US and UK markets.

### B) Classes and Methods

**UniverseBuilder**
- `build()` — orchestrates all fetch methods; writes 8 CSV files
- `_fetch_wikipedia_sp500/sp400/sp600/nasdaq100/ftse100/ftse250()` — scrapes Wikipedia tables
- `_fetch_stockanalysis_etf(etf_symbol)` — scrapes StockAnalysis.com holdings page
- `assign_tiers(tickers)` — all US tickers → TIER_1_SMALLCAP by default; SP500 members → TIER_2

### C) Mathematics
- No mathematical operations

### D) Data Flows
- Web scraping: Wikipedia, StockAnalysis.com
- Outputs: `universe_us_tier1/2/3.csv`, `universe_us_micro.csv`, `universe_uk_tier1/2.csv`, `universe_all.csv`, `universe_us.csv`, `universe_uk.csv`

### E) Dependencies
- Internal: none
- External: requests, pandas, BeautifulSoup, logging

### F) Wiring Status
[LIVE PATH — run periodically to refresh universe]

### G) Issues Found
- `universe_us_tier3.csv` is populated as `sp500_set - set(us_all)` — S&P500 tickers NOT already in the scraped universe; this is near-empty, not actual large-caps
- All US tickers assigned TIER_1_SMALLCAP by default; no actual market cap data used for tier assignment

---

## FILE 14: data/universe.py

### A) Purpose
UniverseManager loads tier CSVs, applies market cap/volume/price filters (via yfinance or fast-path skip), and provides ticker lookups. Universe class wraps UniverseManager for convenience.

### B) Classes and Methods

**UniverseManager**
- `__init__(config)` — loads CSVs for all tiers
- `apply_filters(tickers, market)` — yfinance market cap, volume, price checks per tier
- `apply_filters_fast(tickers)` — skips API calls for pre-screened CSVs
- `get_tickers_by_tier(tier)` — returns list for specified tier
- `_passes_uk_filters(info)` — checks marketCap/100 > min_cap (divides by 100)

**Universe**
- `get_all_tickers()`, `get_us_tickers()`, `get_uk_tickers()`, `get_ticker_tier(ticker)`, `_build_tier_map()` — convenience wrappers

### C) Mathematics
- TIER_SIZE_MULTIPLIERS: MICRO=0.25, TIER_1=1.0, TIER_2=0.70, TIER_3=0.50
- TIER_SIGNAL_GATES: MICRO min_zscore=2.0/confluence=0.80; TIER_1=0.5/0.30; TIER_2=2.5/0.75; TIER_3=3.0/0.85
- UK filter: `marketCap / 100 > min_cap`

### D) Data Flows
- File reads: universe CSV files
- External: yfinance (apply_filters only)

### E) Dependencies
- Internal: none
- External: pandas, yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_passes_uk_filters()` divides marketCap by 100 (treating as pence), but yfinance `.info["marketCap"]` for .L tickers may be denominated in USD — division by 100 produces incorrect comparison

---

## FILE 15: data/delisted_universe.py

### A) Purpose
Static list of ~200 DELISTED_TICKERS dicts (ticker, company_name, sector, delisted_date, delisted_reason, acquiring_company). ACTIVE_DELISTED filters out "skip" entries and placeholder suffixes.

### B) Classes and Methods
- `get_delisted_tickers()` — returns list of ticker strings from ACTIVE_DELISTED
- `get_delisted_records()` — returns full ACTIVE_DELISTED list
- `load_delisted_into_db(db)` — calls HistoricalDB.upsert_delisted() for each record

### C) Mathematics
- None

### D) Data Flows
- DB writes: historical_db.delisted_companies table (via load_delisted_into_db)

### E) Dependencies
- Internal: data.historical_db
- External: logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Many entries have `delisted_reason="skip"` and `delisted_date=None` — these are still-listed stocks incorrectly placed in the delisted list (e.g. PANW, ACAD, FCEL, SSYS)
- SMCI listed as "compliance" with `delisted_date="2019-01-01"` but SMCI re-listed and is actively traded
- ACTIVE_DELISTED filtering is fragile — relies on suffix matching (`_old`, `_sp`, `_delist`, `_2010`)

---

## FILE 16: intelligence/reasoning_engine.py

### A) Purpose
Two-layer reasoning engine. Layer 1 (deterministic): interprets signals, regimes, conflict detection, trade context assembly. Layer 2 (LLM): wraps Layer 1 output only; sends structured context dict to Claude claude-sonnet-4-20250514 for plain-English translation. Falls back to deterministic summary on any failure.

### B) Classes and Methods

**ReasoningEngine**
- `__init__()` — initialises cache dicts; 10-min TTL; loads Anthropic API key from env or settings.yaml
- `_load_api_key()` — reads `ANTHROPIC_API_KEY` env var; falls back to `config/settings.yaml`
- `interpret_signal(signal_name, score)` — deterministic threshold lookup from `_SIGNAL_THRESHOLDS`; returns label/direction/strength/raw_score
- `interpret_regime(regime, confidence)` — lookup from `_REGIME_PROFILES`; adds confidence_label (high/moderate/low)
- `detect_conflicts(signals_dict)` — 4 conflict rules checking cross-signal thresholds
- `build_trade_context(ticker, trade_data, signals, regime, news)` — assembles full context dict; sorts signals by score; calls interpret_signal for each; calls detect_conflicts; builds filter_pass_reasons; assembles risk_metrics
- `explain_why_no_trade(scan_results)` — deterministic plain-text explanation of filter rejections
- `build_deterministic_summary(context)` — plain-English fallback from Layer 1 context dict
- `llm_explain(context, question)` — async; SHA256 cache key; calls Claude via `anthropic.AsyncAnthropic`; falls back to `build_deterministic_summary` on any failure

### C) Mathematics
- Confidence label thresholds: high >= 0.70, moderate >= 0.45, else low
- Conflict rules: momentum > 0.60 AND mean_rev > 0.50; gap > 0.60 AND mean_rev > 0.55; abs(kalman) > 0.50 AND pairs > 1.8; wavelet > 0.60 AND mean_rev > 0.60
- Cache TTL: 600 seconds (10 minutes)
- Cache key: `SHA256(context_json + question)[:32]`

### D) Data Flows
- DB reads: none
- External: Anthropic API (claude-sonnet-4-20250514, max_tokens=600)
- File reads: config/settings.yaml (for API key)

### E) Dependencies
- Internal: monitoring.rate_limiter.RateLimiter (imported at runtime in llm_explain)
- External: anthropic, yaml, json, hashlib, logging, os, pathlib

### F) Wiring Status
[LIVE PATH] for Layer 1; [DISCONNECTED — runtime failure] for Layer 2 LLM path

### G) Issues Found
- `from monitoring.rate_limiter import RateLimiter` — no `monitoring/` module found in the file tree; this import will raise ImportError at runtime; caught by `except ImportError` block, falls back to deterministic summary silently
- `import time` appears inside `llm_explain` method but is also imported as `import time as _t` later in same function — redundant local imports

---

## FILE 17: frontier/dashboard/frontier_dashboard.py

### A) Purpose
Plain-text dashboard renderer. Writes snapshot to `output/frontier_dashboard.txt`. Optionally uses `rich` library for coloured console output. Pulls UMCI, active signals, watchlist, cross-signal interactions, parameter drift.

### B) Classes and Methods

**FrontierDashboard**
- `__init__(store, engine, drifter, registry)` — stores references to all frontier components
- `render()` — assembles dashboard sections; writes to file; optionally prints with rich

### C) Mathematics
- None

### D) Data Flows
- Reads: FrontierStore (UMCI, signals, watchlist), FrontierSignalEngine, ParameterDrifter, DiscoveryRegistry
- Writes: `output/frontier_dashboard.txt`

### E) Dependencies
- Internal: frontier.storage.frontier_store, frontier.signals.frontier_signal_engine, frontier.meta_learning.parameter_drifter, frontier.meta_learning.discovery_registry
- External: rich (optional), logging, pathlib

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 18: frontier/equations/cross_signal_interactions.py

### A) Purpose
Seven cross-signal interaction functions — all are products of three inputs. `get_all_interactions()` evaluates all seven, defaulting missing signals to 0.0.

### B) Classes and Methods
All functions are module-level:
- `geomagnetic_expiry_divorce_trinity(grai, gamma_overhang, divorce_anomaly)` → product of three
- `schumann_reddit_lunar(schumann, reddit, lunar_phase_angle)` → `schumann * reddit * cos(lunar_phase_angle)`
- `church_congress_hiring(church, congressional, hiring)` → product
- `pollen_squeeze_satellite(pollen, short_squeeze, satellite_drop)` → product
- `canal_obituary_permit(canal, obituary, permit)` → product
- `electricity_citation_perplexity(electricity, citation, lpas)` → product
- `quantum_amr_contagion(qtpi, amr_urgency, r0)` → `qtpi * amr_urgency * max(r0 - 1.0, 0.0)`
- `get_all_interactions(signals_dict)` — evaluates all 7; missing signals default to 0.0

### C) Mathematics
- Each function: simple product of 3 inputs (with noted exceptions)
- `schumann_reddit_lunar` includes `cos(lunar_phase_angle)`
- `quantum_amr_contagion` clips R0 contribution: `max(r0 - 1.0, 0.0)`

### D) Data Flows
- No DB reads/writes; pure computation

### E) Dependencies
- Internal: none
- External: math

### F) Wiring Status
[LIVE PATH] — called by UMCI computation and FrontierDashboard

### G) Issues Found
- `congressional` signal has no corresponding collector in the COLLECTORS list in frontier_signal_engine.py — always 0.0
- `hiring` signal has no corresponding collector — always 0.0
- `lunar_phase_angle` not computed by any collector — defaults to 0.0 making `cos(0) = 1.0`; effectively schumann * reddit * 1.0

---

## FILE 19: frontier/equations/derived_formulas.py

### A) Purpose
All mathematical formula implementations for frontier signals. GRAI, ASI, AMS, SCV/SIR model, KLS, ExpectedDrift, DLI, LPAS, QTPI, FSP, FrontierValueScore, ParameterDrifter Bayesian update.

### B) Classes and Methods
All module-level functions:

- `calc_grai(kp_readings, geo_weights, session_overlaps, hours_since_peak, lambda_decay, alpha, vix, vix_mean)` — full GRAI formula
- `grai_position_multiplier(grai)` — tiered multiplier
- `calc_asi(topic_counts)` — Shannon entropy of topic distribution, normalised
- `calc_attention_mispricing_score(asi, stock_news_share, mean_reversion_strength)` — AMS
- `calc_revert_window(asi)` — `5 + 15*(1-ASI)` days
- `calc_scv(susceptible, infected, recovered, beta, gamma)` — SIR model dI/dt
- `scv_position_size_multiplier(r0)` — `min(r0, 2.0) / 2.0`
- `calc_knowledge_loss_score(role, tenure_years, succession_signal, analyst_coverage)` — KLS
- `calc_expected_drift(kls, market_cap_millions)` — ExpectedDrift
- `calc_dli(divorce_rate, baseline_mean, baseline_std)` — standardised z-score
- `calc_sector_impact(dli, sensitivity, income_weight, lag_decay, months_ahead)` — SectorImpact
- `calc_lpas(current_perplexity, rolling_mean, rolling_std)` — z-score
- `calc_composite_lpas(scores_dict)` — weighted average (sec_8k=1.0, earnings_release=0.8, press_release=0.5, ceo_letter=0.9)
- `calc_qtpi(arxiv_velocity, patent_velocity, milestone_score)` — `0.3*arxiv + 0.3*patent + 0.4*milestone`
- `calc_fsp(signal_values, factor_values_dict)` — `1 - max(abs(corr(signal, factor_f)))`
- `calc_frontier_value_score(fsp, validated_sharpe, evidence_tier)` — `FSP * max(sharpe, 0) * tier_weight`

### C) Mathematics
- GRAI: `Σ[Kp(l) × geo_weight(l) × session_overlap(l)] × exp(-λ × hours_since_peak) × (1 + α × VIX/VIX_mean)`
- GRAI multiplier: 1.0 if GRAI<1; linear to 0.9 if GRAI<3; GRAI/10 reduction if GRAI<6; hard cap 0.6
- ASI: `H(topic_distribution) / log₂(n_topics)` where `H = -Σ p_i log₂(p_i)`
- AMS: `(1-ASI) × (1-stock_news_share) × mean_reversion_strength`
- RevertWindowDays: `5 + 15*(1-ASI)`
- SCV: `dI/dt = β*S*I/N - γ*I`; `R₀ = β/γ`; `SCV = dI/dt / max(I, 1)`
- scv_position_size_multiplier: `min(R₀, 2.0) / 2.0`
- KLS: `role_weight × tenure_years^0.5 × (1-succession_signal) × (1/analyst_coverage)`
- ExpectedDrift: `-0.03 × KLS × (1/√market_cap_millions)`
- ExpectedDriftDuration: `10 + 20×KLS`
- DLI: `(divorce_rate - baseline_mean) / baseline_std`
- SectorImpact: `DLI × sensitivity × income_weight × exp(-lag_decay × months_ahead)`
- LPAS: `(current_perplexity - rolling_mean) / rolling_std`
- QTPI: `0.3*arxiv_velocity + 0.3*patent_velocity + 0.4*milestone_score`
- FSP: `1 - max|corr(signal, factor_f)|` for f in known_factors
- FrontierValueScore: `FSP × max(validated_sharpe, 0) × tier_weight` (T1=1.0, T2=0.75, T3=0.5, T4=0.25, T5=0.0)

### D) Data Flows
- No DB reads/writes; pure computation

### E) Dependencies
- Internal: none
- External: math, statistics, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant; formulas match documented design

---

## FILE 20: frontier/equations/frontier_signal_purity.py

### A) Purpose
SignalPurityTracker tracks FSP and FrontierValueScore history per signal. Detects declining FSP trend (>0.05 drop vs 3-period average). Provides purity ranking.

### B) Classes and Methods

**SignalPurityTracker**
- `__init__()` — `_history: dict[str, list]`
- `update(signal_name, signal_values, factor_values_dict, validated_sharpe, evidence_tier)` — computes FSP via `calc_fsp`; computes FrontierValueScore; appends to history; checks declining trend
- `get_purity_ranking()` — returns all tracked signals sorted by latest FSP descending
- `_detect_decline(signal_name)` — True if latest FSP drops >0.05 vs mean of last 3

### C) Mathematics
- FSP: see derived_formulas
- Decline: `latest_fsp < mean(last_3_fsp) - 0.05`

### D) Data Flows
- No DB reads/writes; in-memory history only

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- History is in-memory only — lost on process restart; no persistence

---

## FILE 21: frontier/equations/unified_complexity_index.py

### A) Purpose
Computes UMCI (0–100) from 5 dimensional sub-scores. Sets position multiplier and halt flag based on level. UMCILogger appends to JSONL log file.

### B) Classes and Methods

Module-level functions:
- `calc_physical_complexity(grai, schumann, pollen, electricity, canal)` — weighted sum
- `calc_social_complexity(asi, scv_max, divorce, obituary, church)` — weighted sum (note: uses `(1-ASI)`)
- `calc_scientific_complexity(qtpi, amr, citation, soil)` — weighted sum
- `calc_financial_frontier_complexity(lpas, option_overhang, permits, food)` — weighted sum
- `calc_altdata_complexity(reddit, wikipedia, shipping, insider)` — weighted sum
- `calc_umci(pc, sc, sci, ffc, adc, config)` — composite; returns (umci_score, breakdown_dict)

**UMCILogger**
- `__init__(path)` — JSONL at `logs/umci_history.jsonl`
- `log(umci, breakdown)` — append-only JSONL
- `get_history(n=252)` — reads last n lines
- `last_comparable()` — returns last entry with same level

### C) Mathematics
- PhysicalComplexity: `GRAI×0.35 + schumann×0.15 + pollen×0.10 + electricity×0.25 + canal×0.15`
- SocialComplexity: `(1-ASI)×0.30 + SCV_max×0.25 + divorce×0.15 + obituary×0.15 + church×0.15`
- ScientificComplexity: `QTPI×0.40 + amr×0.25 + citation×0.20 + soil×0.15`
- FinancialFrontierComplexity: `LPAS×0.35 + option_overhang×0.30 + permits×0.20 + food×0.15`
- AltDataComplexity: `reddit×0.25 + wikipedia×0.20 + shipping×0.25 + insider×0.30`
- UMCI: `(PC×0.20 + SC×0.25 + SciC×0.15 + FFC×0.25 + ADC×0.15) × 100`
- Level thresholds: LOW(0–30, mult=1.0), MEDIUM(30–60, mult=0.85), HIGH(60–80, mult=0.65), EXTREME(80–95, mult=0.30), UNPRECEDENTED(95–101, mult=0.0, halt=True)

### D) Data Flows
- No DB reads/writes
- File appends: `logs/umci_history.jsonl`

### E) Dependencies
- Internal: none
- External: json, logging, pathlib

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `shipping` and `wikipedia` signals have no corresponding collectors in frontier_signal_engine.py COLLECTORS list — always 0.0 in AltDataComplexity

---

## FILE 22: frontier/financial_frontier/building_permit_collector.py

### A) Purpose
Fetches FRED PERMIT series (US housing starts/building permits) via CSV endpoint (no API key required). Computes inflection signal vs 12-month average.

### B) Classes and Methods

**BuildingPermitCollector**
- `collect()` — fetches FRED CSV; computes inflection; returns signal dict

### C) Mathematics
- `inflection = (latest - twelve_month_avg) / twelve_month_avg`, clamped [-1, 1]

### D) Data Flows
- External: FRED CSV endpoint (`fred.stlouisfed.org/graph/fredgraph.csv?id=PERMIT`)
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: requests, pandas, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 23: frontier/financial_frontier/llm_perplexity_scorer.py

### A) Purpose
Fetches financial news headlines via yfinance; computes text perplexity using GPT-2 (HuggingFace transformers) or a proxy (standard deviation of sentence lengths). Maintains rolling 30-reading history. Computes LPAS z-score.

### B) Classes and Methods

**LLMPerplexityScorer**
- `__init__()` — GPT-2 model cached at class level; history at `logs/lpas_history.json`
- `collect()` — fetches SPY/QQQ/IWM news; computes perplexity; logs; returns LPAS signal
- `_compute_perplexity(text)` — GPT-2 forward pass or sentence-length proxy
- `calc_lpas()` — z-score of latest vs rolling history

### C) Mathematics
- GPT-2 perplexity: `exp(-mean(log_probs))` over tokenised text
- Proxy perplexity: `20 + std(sentence_lengths) * 1.8`, clamped [10, 500]
- LPAS: `(current - rolling_mean) / rolling_std`
- Quality: 1.0 (GPT-2), 0.4 (proxy), 0.2 (no headlines)

### D) Data Flows
- File: `logs/lpas_history.json` (read/write, max 30 entries)
- External: yfinance (news), HuggingFace transformers (GPT-2)

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: yfinance, transformers (optional), torch (optional), json, logging

### F) Wiring Status
[LIVE PATH with degradation] — falls back to proxy if transformers not installed

### G) Issues Found
- GPT-2 model loaded at class level (module attribute) — first instantiation loads ~500MB model; subsequent instantiations skip loading but model stays in memory for process lifetime

---

## FILE 24: frontier/financial_frontier/option_expiry_overhang.py

### A) Purpose
Fetches SPY/QQQ/IWM options chains via yfinance. Computes Gamma Exposure (GEX) as proxy for dealer hedging pressure.

### B) Classes and Methods

**OptionExpiryOverhangCollector**
- `collect()` — fetches options chains; computes GEX per ticker; returns mean, clamped [-1, 1]

### C) Mathematics
- GEX per ticker: `Σ(gamma × open_interest × 100) / price²` (calls positive, puts negative from dealer perspective)
- Normalised: mean GEX across tickers, clamped [-1, 1]

### D) Data Flows
- External: yfinance options chains
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- yfinance options chains may have missing/NaN gamma values for OTM options — no explicit NaN handling shown

---

## FILE 25: frontier/meta_learning/correlation_discoverer.py

### A) Purpose
Discovers correlations between frontier signals and asset returns using rolling Pearson correlation and Monte Carlo permutation test (500 shuffles, 95th percentile threshold). Also tests pairwise signal interaction products.

### B) Classes and Methods

**CorrelationDiscoverer**
- `__init__(config)` — default window=20, min_abs_corr=0.15, max_abs_corr=0.85, min_obs=30
- `discover(signal_series, returns_series)` — tests lag-0, lag-1, lag-5 versions of signal; filters by min/max corr and min_obs
- `discover_interactions(signals_dict, returns_series)` — pairwise products of signals (up to 50 pairs); same filter
- `rolling_corr_mean(s1, s2, window=20)` — mean of rolling correlation
- `lag_corr(s1, s2, lags)` — Pearson at each lag
- `monte_carlo_permutation_test(signal, returns, n_shuffles=500)` — returns fraction of shuffles beaten

### C) Mathematics
- Rolling correlation: Pearson over 20-day window, mean of rolling values
- Permutation test: 500 random shuffles; fraction where observed Pearson > shuffled Pearson
- Threshold: `abs(corr) >= 0.15` AND `abs(corr) <= 0.85` AND `n_obs >= 30`

### D) Data Flows
- No DB reads/writes; in-memory computation

### E) Dependencies
- Internal: none
- External: numpy, pandas (optional), math, random

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Falls back to pure-Python Pearson when numpy unavailable — slower but functionally equivalent

---

## FILE 26: frontier/meta_learning/discovery_registry.py

### A) Purpose
JSONL-backed append-only registry for signal lifecycle management. Status flow: watchlist → validating → live (or failed/retired). In-memory dict with JSONL as persistent log; on load, last record per name wins.

### B) Classes and Methods

**DiscoveryRegistry**
- `__init__(path)` — JSONL at `logs/discovery_registry.jsonl`
- `register(name, description, formula, initial_evidence)` — appends new entry
- `update_status(name, status, evidence)` — appends updated entry
- `get(name)` — returns latest record from in-memory dict
- `get_all()`, `get_watchlist()`, `get_live()` — filtered views
- `summary()` — counts by status

### C) Mathematics
- None

### D) Data Flows
- File: `logs/discovery_registry.jsonl` (append-only)

### E) Dependencies
- Internal: none
- External: json, logging, pathlib

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Append-only JSONL grows unbounded — no pruning or archival mechanism

---

## FILE 27: frontier/meta_learning/parameter_drifter.py

### A) Purpose
Bayesian parameter update mechanism. Updates 6 published signal parameters toward empirical evidence, clamped to [anchor*0.5, anchor*2.0].

### B) Classes and Methods

**ParameterDrifter**
- `__init__(config, store=None)` — 6 anchors: grai_decay_lambda=0.15, grai_volatility_amplifier=0.30, kelly_base_fraction=0.25, scv_beta=0.30, scv_gamma=0.10, fsp_min_threshold=0.50
- `drift(param_name, evidence, weight=0.1)` — Bayesian update; clamp; log to store
- `get_current(param_name)` — returns current value
- `reset_to_anchor(param_name)` — resets to published value
- `summary()` — dict of all current values
- `drift_all(evidence_dict, weight=0.1)` — apply drift to all available params

### C) Mathematics
- `new_value = (1 - weight) * current + weight * evidence`
- Clamp: `[anchor * 0.5, anchor * 2.0]`

### D) Data Flows
- Writes to FrontierStore.log_parameter_drift() if store provided

### E) Dependencies
- Internal: frontier.storage.frontier_store
- External: logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- In-memory only; no persistence of drifted values across restarts (relies on FrontierStore for audit log but does not reload drifted values on startup)

---

## FILE 28: frontier/meta_learning/watchlist_manager.py

### A) Purpose
Keeps FrontierStore (SQLite) and DiscoveryRegistry (JSONL) in sync for signal lifecycle management.

### B) Classes and Methods

**WatchlistManager**
- `__init__(store, registry)` — stores references
- `add(name, description, formula, evidence)` — upserts to store; registers in registry
- `promote_to_validating(name, evidence)` — updates both store and registry
- `promote_to_live(name, evidence)` — updates both store and registry
- `fail(name, reason)` — marks failed in both
- `get_all(status)` — reads from store
- `render_watchlist_table()` — plain-text table string

### C) Mathematics
- None

### D) Data Flows
- DB reads/writes: FrontierStore.watchlist table
- File reads/writes: DiscoveryRegistry JSONL

### E) Dependencies
- Internal: frontier.storage.frontier_store, frontier.meta_learning.discovery_registry
- External: logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 29: frontier/physical/canal_congestion_collector.py

### A) Purpose
Proxy for global shipping congestion using BDRY → BOAT → BSEP ETF price z-scores. 20-day rolling z-score, clamped [-3, 3].

### B) Classes and Methods

**CanalCongestionCollector**
- `collect()` — tries ETFs in order; 20-day z-score of close; returns signal dict with quality degrading by rank (1.0, 0.8, 0.7)

### C) Mathematics
- `z = (close - mean_20d) / std_20d`, clamped [-3, 3]

### D) Data Flows
- External: yfinance (BDRY, BOAT, BSEP)

### E) Dependencies
- Internal: none
- External: yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- BDRY/BOAT/BSEP ETFs may be delisted or thinly traded — quality fallback chain provides degraded signal

---

## FILE 30: frontier/physical/electricity_collector.py

### A) Purpose
UK National Grid ESO CKAN API — fetches 48 records; computes z-score of latest demand vs prior 47. Tries field names ENGLAND_WALES_DEMAND, ND, TSD, TRANSMISSION_SYSTEM_DEMAND.

### B) Classes and Methods

**ElectricityCollector**
- `collect()` — CKAN API; z-score; clamp [-3, 3]; returns signal dict

### C) Mathematics
- `z = (latest - mean(prior_47)) / std(prior_47)`, clamped [-3, 3]

### D) Data Flows
- External: UK National Grid ESO CKAN API (resource ID 177f6fa4...)

### E) Dependencies
- Internal: none
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- CKAN resource ID is hardcoded — may change without notice; no version pin

---

## FILE 31: frontier/physical/geomagnetic_collector.py

### A) Purpose
Fetches Kp geomagnetic index from NOAA (1-minute primary, 3-hourly fallback). Averages last 3 readings. Computes GRAI using derived_formulas.calc_grai(). VIX fetched via yfinance.

### B) Classes and Methods

**GeomagneticCollector**
- `collect()` — NOAA API; averages last 3 Kp readings; fetches VIX; calls calc_grai(); returns signal dict
- quality: 1.0 (primary NOAA), 0.7 (3-hourly fallback)

### C) Mathematics
- GRAI: see derived_formulas.calc_grai()

### D) Data Flows
- External: NOAA Kp endpoint, yfinance (^VIX)

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: requests, yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- VIX fallback: `_HISTORICAL_VIX_MEAN = 20.0` hardcoded; used when ^VIX fetch fails

---

## FILE 32: frontier/physical/pollen_collector.py

### A) Purpose
Open-Meteo Air Quality API; 6 pollen types (alder, birch, grass, mugwort, olive, ragweed). Default location: New York City (40.7128°N, -74.0060°W). Computes pollen stress index.

### B) Classes and Methods

**PollenCollector**
- `collect()` — Open-Meteo API; sums all 6 pollen types; computes stress index; returns signal dict

### C) Mathematics
- `pollen_stress_index = total_grains_per_m3 / 1000.0`
- `quality_score = 1.0 - (missing_types / 6)`

### D) Data Flows
- External: Open-Meteo Air Quality API (no API key)

### E) Dependencies
- Internal: none
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Location hardcoded to NYC; not configurable without code change

---

## FILE 33: frontier/physical/satellite_imagery_collector.py

### A) Purpose
Uses XRT (retail ETF) volume as proxy for satellite-detected retail foot traffic. 20-day rolling z-score, then inverted: `activity_drop = -1 × vol_zscore`. Positive = below-average volume.

### B) Classes and Methods

**SatelliteImageryCollector**
- `collect()` — yfinance XRT (fallback XLY); 20-day volume z-score; invert; return signal dict

### C) Mathematics
- `vol_zscore = (vol - mean_20d) / std_20d`
- `activity_drop = -1 × vol_zscore`

### D) Data Flows
- External: yfinance (XRT, XLY)

### E) Dependencies
- Internal: none
- External: yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- ETF volume as satellite imagery proxy is a very loose approximation; signal_name `"satellite_activity_drop"` overstates the data source

---

## FILE 34: frontier/physical/schumann_collector.py

### A) Purpose
NOAA RTSW solar wind 1-minute JSON. Uses proton speed as proxy for Schumann resonance anomaly.

### B) Classes and Methods

**SchumannCollector**
- `collect()` — NOAA solar wind endpoint; `deviation = (proton_speed - 400) / 400`, clamped [-1, 1]

### C) Mathematics
- `deviation = (proton_speed - 400) / 400`, clamped [-1, 1]
- Quality: 1.0 if `200 <= speed <= 1000` km/s

### D) Data Flows
- External: NOAA RTSW solar wind JSON

### E) Dependencies
- Internal: none
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Solar wind proton speed is not the Schumann resonance — Schumann resonances are Earth's electromagnetic resonances (~7.83 Hz); this uses a completely different physical measurement as a proxy

---

## FILE 35: frontier/scientific/academic_citation_tracker.py

### A) Purpose
Semantic Scholar API for 4 finance/quant topics. Computes citation velocity as total citations / total papers, normalised to [0, 1] with ceiling at 1000 citations/paper.

### B) Classes and Methods

**AcademicCitationTracker**
- `collect()` — Semantic Scholar search for 4 topics; sums citations/papers; returns signal dict

### C) Mathematics
- `citation_velocity = total_citations / total_papers`
- `normalised = min(1.0, velocity / 1000.0)`

### D) Data Flows
- External: Semantic Scholar API

### E) Dependencies
- Internal: none
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Normalisation ceiling of 1000 citations/paper is very high; most results will be near zero — low signal dynamic range

---

## FILE 36: frontier/scientific/amr_research_tracker.py

### A) Purpose
PubMed ESearch for "antimicrobial resistance" in last 30 days. Counts publications. Baseline: 500 papers/month.

### B) Classes and Methods

**AMRResearchTracker**
- `collect()` — PubMed ESearch API; counts results; `amr_urgency = min(1.0, count / 500)`

### C) Mathematics
- `amr_urgency = min(1.0, count / 500)`

### D) Data Flows
- External: NCBI PubMed ESearch API

### E) Dependencies
- Internal: none
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 37: frontier/scientific/food_safety_collector.py

### A) Purpose
FDA food recalls RSS feed. Counts items published in last 30 days. Baseline: 20 recalls/month.

### B) Classes and Methods

**FoodSafetyCollector**
- `collect()` — FDA RSS; counts recalls in 30 days; `food_safety_risk = min(1.0, recall_count / 20)`

### C) Mathematics
- `food_safety_risk = min(1.0, recall_count / 20)`

### D) Data Flows
- External: FDA RSS feed

### E) Dependencies
- Internal: none
- External: requests, xml.etree.ElementTree, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 38: frontier/scientific/quantum_readiness_tracker.py

### A) Purpose
ArXiv quant-ph papers (last 20, counts those in last 30 days) + QTUM ETF volume anomaly (5d/20d avg - 1). Computes QTPI via calc_qtpi().

### B) Classes and Methods

**QuantumReadinessTracker**
- `collect()` — ArXiv API; QTUM yfinance; calls calc_qtpi(); returns signal dict

### C) Mathematics
- `arxiv_velocity = recent_count / 50`
- `patent_velocity = max(0, 5d_vol_avg/20d_vol_avg - 1)` (QTUM volume proxy)
- Hardcoded milestones: ["50_qubit_system", "quantum_advantage_demonstrated"]
- QTPI: `0.3*arxiv + 0.3*patent + 0.4*milestone`

### D) Data Flows
- External: ArXiv API, yfinance (QTUM)

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: requests, yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Milestone score is hardcoded as a list of strings — no mechanism to detect if milestones have actually been achieved; milestone_score is likely always 0 unless manually updated

---

## FILE 39: frontier/scientific/soil_health_collector.py

### A) Purpose
DBA (agricultural ETF) 60-day price z-score. Positive signal when ag prices are depressed (degradation = positive when z-score is negative).

### B) Classes and Methods

**SoilHealthCollector**
- `collect()` — yfinance DBA; 60-day z-score; `degradation = min(1.0, max(0.0, -zscore / 3.0))`

### C) Mathematics
- `zscore = (close - mean_60d) / std_60d`
- `degradation = min(1.0, max(0.0, -zscore / 3.0))`

### D) Data Flows
- External: yfinance (DBA)

### E) Dependencies
- Internal: none
- External: yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- DBA ETF performance reflects commodity futures, not soil health directly — proxy is very loose

---

## FILE 40: frontier/social/attention_economy.py

### A) Purpose
Google Trends via pytrends for 5 financial topics. Computes ASI (Attention Saturation Index) and AMS. Applies randomised 45–90 second sleep before each pytrends call to avoid rate limits.

### B) Classes and Methods

**AttentionEconomyCollector**
- `collect()` — pytrends for "stock market", "interest rates", "inflation", "recession", "fed reserve"; last month; computes ASI; uses hardcoded `mean_reversion_strength=0.5` for AMS

### C) Mathematics
- ASI via calc_asi(topic_counts): Shannon entropy normalised
- AMS via calc_attention_mispricing_score(asi, stock_news_share, 0.5)
- `stock_news_share = topic_counts["stock market"] / 100.0`

### D) Data Flows
- External: pytrends (Google Trends)
- No DB reads/writes

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: pytrends (optional), logging, random, time

### F) Wiring Status
[LIVE PATH with degradation] — falls back to ASI=0.5 (quality=0.0) on pytrends failure

### G) Issues Found
- 45–90 second blocking sleep is synchronous; if called from async context this blocks the event loop
- `mean_reversion_strength` hardcoded to 0.5 — not derived from any actual data

---

## FILE 41: frontier/social/church_attendance.py

### A) Purpose
Google Trends for "church near me" and "sunday service" as risk-aversion proxy. Seasonal cosine fallback (peaks January, amplitude ±0.2).

### B) Classes and Methods

**ChurchAttendanceCollector**
- `collect()` — pytrends 3-month; mean of keyword interests; `signal = (mean_interest / 100.0) - 0.5`
- `_seasonal_fallback()` — `0.2 * cos(2π(month-1)/12)`

### C) Mathematics
- `signal = (mean_interest / 100.0) - 0.5` — centred at 0
- `seasonal = 0.2 * cos(2π(month-1)/12)`

### D) Data Flows
- External: pytrends

### E) Dependencies
- Internal: none
- External: pytrends (optional), math, logging

### F) Wiring Status
[LIVE PATH with degradation]

### G) Issues Found
- 45-second blocking sleep (same issue as attention_economy.py)

---

## FILE 42: frontier/social/divorce_filing_collector.py

### A) Purpose
Google Trends for "divorce lawyer", "divorce attorney", "legal separation". Computes DLI (Divorce Lead Indicator) as z-score. Computes sector impacts for 6-month horizon.

### B) Classes and Methods

**DivorceFilingCollector**
- `collect()` — pytrends 3-month; mean / 100 → rate; calc_dli(); sector_impacts with `months_ahead=6, income_weight=1.0, lag_decay=0.1`

### C) Mathematics
- `rate = mean(keyword_values) / 100.0`
- DLI: `(rate - 0.5) / 0.15` (baseline_mean=0.5, baseline_std=0.15)
- SectorImpact: `DLI × sensitivity × 1.0 × exp(-0.1 × 6)` per sector

### D) Data Flows
- External: pytrends

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: pytrends (optional), math, logging

### F) Wiring Status
[LIVE PATH with degradation]

### G) Issues Found
- 45-second blocking sleep
- Sector impacts computed but only stored in raw_data — not surfaced to any downstream consumer

---

## FILE 43: frontier/social/hq_traffic_monitor.py

### A) Purpose
Composite proxy for corporate HQ foot traffic. VNQ (office REIT) 20-day z-score × (1 - WFH Google Trends signal). Positive = rising office occupancy.

### B) Classes and Methods

**HQTrafficMonitor**
- `collect(tickers=None)` — fetches VNQ z-score; fetches WFH pytrends signal; computes composite
- `_fetch_vnq_zscore()` — yfinance VNQ 3mo; 20-day z-score
- `_fetch_wfh_signal()` — pytrends "work from home" 3-month; normalised 0–1

### C) Mathematics
- VNQ z-score: `(close - mean_20d) / std_20d` (population variance, not sample)
- `hq_traffic = office_reit_zscore × (1 - wfh_signal)`
- Quality: sources_ok / 2.0 where sources_ok = count of error-free fetches

### D) Data Flows
- External: yfinance (VNQ), pytrends

### E) Dependencies
- Internal: none
- External: yfinance, pytrends (optional), requests, logging

### F) Wiring Status
[LIVE PATH with degradation]

### G) Issues Found
- VNQ z-score uses population variance (divides by N not N-1) — minor statistical imprecision
- 45-second blocking pytrends sleep in WFH fetch

---

## FILE 44: frontier/social/obituary_tracker.py

### A) Purpose
SEC EDGAR full-text search for 8-K Item 5.02 resignation filings in last 30 days. For each departure: infers role from text, computes KLS and ExpectedDrift. Returns mean absolute expected drift.

### B) Classes and Methods

**ObituaryTracker**
- `collect(tickers=None)` — SEC EDGAR search; parses hits; computes KLS/drift per departure; mean abs drift
- `_fetch_sec_filings(days_back=30)` — GET EDGAR efts.sec.gov search endpoint
- `_classify_role(text)` — keyword matching for ceo/cfo/coo/cto/chief_scientist/founder/director/other_csuite

### C) Mathematics
- KLS: `role_weight × tenure_years^0.5 × (1-succession_signal) × (1/analyst_coverage)` with defaults: tenure=3.0, succession=0.2, analyst_coverage=5
- ExpectedDrift: `-0.03 × KLS × (1/√market_cap_millions)` with default market_cap=500M
- Quality: `min(1.0, 0.1 + (n_filings / 50.0) * 0.9)`

### D) Data Flows
- External: SEC EDGAR search API

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: requests, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- All defaults are constants — actual tenure, succession, coverage, market cap not fetched from any data source; KLS and drift are effectively constant per role
- Role classification from limited text snippet (period_of_report + display_date_filed + entity_name + description) — description field is often empty in EDGAR search results; most roles will classify as "other_csuite"
- EDGAR efts.sec.gov endpoint is an unofficial search API; not guaranteed stable

---

## FILE 45: frontier/social/social_contagion_mapper.py

### A) Purpose
Reddit hot posts (wallstreetbets, investing, stocks) via public JSON API. Counts ticker mentions. Applies SIR epidemic model with hardcoded parameters. Returns R0.

### B) Classes and Methods

**SocialContagionMapper**
- `collect(tickers=None)` — fetches 25 hot posts from each subreddit; counts mentions in titles using regex `\b([A-Z]{2,5})\b`; feeds to calc_scv()

### C) Mathematics
- SIR parameters: susceptible=1000.0, infected=unique_mentioned, recovered=infected*0.3
- beta=0.3, gamma=0.1 (hardcoded)
- R0=beta/gamma=3.0 (effectively constant since parameters are hardcoded)
- SCV: `dI/dt = beta*S*I/N - gamma*I`; `SCV = dI/dt / max(I,1)`
- Quality: `min(1.0, posts_fetched / 75.0)`

### D) Data Flows
- External: Reddit public JSON API (`reddit.com/r/{sub}/hot.json`)

### E) Dependencies
- Internal: frontier.equations.derived_formulas
- External: requests, re, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Beta and gamma hardcoded — R0=beta/gamma=3.0 is always the same unless unique_mentioned changes S/I/N ratios; SCV is the only variable output
- Regex `\b([A-Z]{2,5})\b` matches common English words (e.g. "A", "AT", "IT", "THE" is 3 chars — wait, "THE" is 3 upper? No — Reddit titles are mixed case; caps-only regex reduces false positives but common ticker overlaps remain (e.g. "A" = Agilent, "IT" = Gartner)

---

## FILE 46: frontier/signals/frontier_signal_engine.py

### A) Purpose
Orchestrates all 20 frontier collectors. Computes UMCI snapshot. Generates directional signals per ticker based on GRAI, ASI, LPAS thresholds. Tries FrontierStore cache first; falls back to live collection if fewer than 5 signals cached.

### B) Classes and Methods

**FrontierSignalEngine**
- `__init__(store, config)` — stores FrontierStore reference and config
- `_collect_all()` — dynamic import and instantiate each collector from COLLECTORS list; calls `cls(self._config).collect()`
- `_get_cached_or_collect()` — tries store for last 24h signals; if < 5 found, runs live collection; applies neutral defaults for any missing
- `get_umci_snapshot()` — calls all dimension computations; stores to FrontierStore; returns {umci, breakdown, signals}
- `generate(tickers, market)` — directional rules: GRAI>0.6→-1; ASI<0.3→+1, ASI>0.8→-1; abs(LPAS)>2→amplify caution; clamps direction [-1,1]; confidence = `(1.0 - umci/100.0) × max(0.3, mean_quality)`

### C) Mathematics
- Confidence: `(1.0 - umci/100.0) × max(0.3, mean_quality)`
- GRAI thresholds: >0.6 (risk-off), <0.2 (calm)
- ASI thresholds: <0.3 (dispersed attention), >0.8 (saturated)
- LPAS threshold: abs > 2.0 (unusual language)

### D) Data Flows
- DB reads: FrontierStore.get_raw_history() for each of 20 signal names
- DB writes: FrontierStore.store_umci()
- Calls: all 20 collector classes

### E) Dependencies
- Internal: all frontier collector modules, frontier.equations.unified_complexity_index, frontier.storage.frontier_store
- External: importlib, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_collect_all()` instantiates collectors as `cls(self._config)` — but AttentionEconomyCollector, ChurchAttendanceCollector, DivorceFilingCollector, etc. take no `config` argument in their `__init__` — will raise TypeError for those collectors; caught by `except Exception` and logged as debug
- mean_quality computation: `quality_scores = [... for _, result in (self._collect_all().items() if len(signals) < 5 else {}.items())]` — if len(signals) >= 5 (cache hit), this is `{}.items()` → empty list → mean_quality defaults to 0.6; collectors are run again unnecessarily when len(signals) < 5 (already done in _get_cached_or_collect)

---

## FILE 47: frontier/sizing/frontier_sizer.py

### A) Purpose
Tiered position sizing for frontier signals. Tier 1–5 with Kelly fractions (1.00/0.50/0.25/0.10/0.00). UMCI multiplier applied on top. Tier promotion criteria checking.

### B) Classes and Methods

**calculate_frontier_position_size()** (module-level function)
- Inputs: signal_name, direction, confidence, evidence_tier, base_kelly_fraction, portfolio_value, current_price, umci_multiplier, config
- Blocks if tier_fraction==0 or umci_multiplier==0
- `final_kelly = base_kelly_fraction * tier_fraction * umci_multiplier * confidence`
- `notional = min(final_kelly * portfolio_value, cap)`
- `shares = int(notional / current_price) * direction`

**check_tier_promotion()** (module-level function)
- Checks: live_days >= next_tier min; oos_sharpe >= next_tier min; fsp >= 0.5; tier 2→1 requires >= 2 replications
- Returns: {promoted, new_tier, reason}

### C) Mathematics
- TIER_KELLY_FRACTIONS: {1:1.00, 2:0.50, 3:0.25, 4:0.10, 5:0.00}
- TIER_MIN_LIVE_DAYS: {1:365, 2:180, 3:90, 4:30, 5:0}
- TIER_MIN_OOS_SHARPE: {1:1.5, 2:1.0, 3:0.7, 4:0.3, 5:0.0}
- `final_kelly = base_kelly × tier_fraction × umci_multiplier × confidence`
- `cap = max_single_position_pct × portfolio_value` (default max_single_position_pct=0.02)
- `notional = min(final_kelly × portfolio_value, cap)`

### D) Data Flows
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `notional` is then multiplied by `direction` in the return value but `shares` already incorporates `direction` — the returned `notional` and `shares` may have inconsistent sign handling

---

## FILE 48: frontier/storage/frontier_store.py

### A) Purpose
Thread-safe SQLite store for the full frontier intelligence pipeline. 8 tables: raw_signals, umci_history, validation_results, watchlist, parameter_history, evidence_records, signal_log, interaction_log. "Log everything permanently" principle.

### B) Classes and Methods

**FrontierStore**
- `__init__(config)` — path from `config["frontier"]["storage_path"]`; thread-local connections; `_init_db()`
- `_conn()` — thread-local with WAL and Row factory
- `_init_db()` — executescript with full CREATE TABLE IF NOT EXISTS DDL + indexes
- `store_raw(collector, signal_name, value, ticker, market, raw_data, quality)` — INSERT into raw_signals
- `get_raw_history(signal_name, days_back, ticker)` — SELECT with time filter; deserialises raw_data JSON
- `store_umci(umci, breakdown)` — INSERT into umci_history with dimensional breakdown
- `get_umci_history(n=252)`, `get_last_umci()` — SELECT from umci_history
- `store_validation(signal_name, result)` — INSERT into validation_results
- `get_validation_history(signal_name)` — SELECT from validation_results
- `upsert_watchlist(entry)` — INSERT OR UPDATE watchlist
- `get_watchlist(status, limit)` — SELECT ordered by deflated_sharpe DESC
- `log_parameter_drift(signal_name, param_name, published, old_val, new_val, delta, reason)` — INSERT into parameter_history
- `get_parameter_history(signal_name)` — SELECT from parameter_history
- `update_evidence(signal_name, sizing_tier, live_days, live_accuracy, live_sharpe, n_live)` — INSERT OR UPDATE evidence_records
- `get_evidence(signal_name)` — SELECT from evidence_records
- `log_signal(ticker, signal_name, signal_type, direction, confidence, sizing_tier, position_size, sources)` — INSERT into signal_log
- `get_recent_signals(hours_back, limit)` — SELECT from signal_log with time filter
- `status_summary()` — counts across 6 tables
- `backup(backup_dir)` — shutil.copy2 to dated file
- `close()` — closes thread-local connection

### C) Mathematics
- None in storage layer

### D) Data Flows
- DB file: `frontier/storage/frontier.db` (default)
- All 8 tables read and written

### E) Dependencies
- Internal: none
- External: sqlite3, threading, json, shutil, logging, pathlib

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_init_db()` uses `executescript()` which auto-commits; then calls `self._conn().commit()` — redundant commit after executescript
- Thread-local connections means each thread creates its own WAL connection; with many short-lived threads this creates many connection objects without guaranteed cleanup

---

## FILE 49: frontier/validation/evidence_tracker.py

### A) Purpose
Accumulates live trade outcomes for frontier signals. Computes running accuracy, live Sharpe (annualised), win rate. Triggers tier promotion check via frontier_sizer.check_tier_promotion().

### B) Classes and Methods

**assign_evidence_grade()** (module-level function)
- Inputs: has_published_paper, replications, oos_sharpe, monte_carlo_pct, benjamini_pass, fsp
- Grade A: published + >=2 reps + SR>1.5 + MC>99%
- Grade B: SR>1.0 + MC>95% + BH pass + FSP>0.5
- Grade C: SR>0.5 + MC>90%
- Grade D: SR>0, MC>=90%
- Grade F: otherwise

**grade_to_tier()** (module-level function)
- Maps A→1, B→2, C→3, D→4, F→5

**EvidenceTracker**
- `__init__(signal_name, initial_tier=4)` — in-memory `_records` list
- `record_outcome(direction, entry_price, exit_price, hold_days, timestamp)` — `pnl_pct = direction * (exit_price - entry_price) / entry_price`; appends to records
- `live_stats()` — n_trades, win_rate, live_sharpe (annualised × √252), mean_pnl_pct, live_days (sum of hold_days)
- `check_promotion(fsp, validated_replications)` — calls check_tier_promotion(); updates current_tier if promoted

### C) Mathematics
- `pnl_pct = direction * (exit_price - entry_price) / entry_price`
- `live_sharpe = (mean_pnl / std_pnl) × √252`
- `live_days = sum(hold_days)` — proxy, not calendar days

### D) Data Flows
- In-memory only; calls frontier_sizer.check_tier_promotion()

### E) Dependencies
- Internal: frontier.sizing.frontier_sizer
- External: numpy, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `live_days = sum(hold_days)` is an approximation — parallel positions would double-count calendar days
- Grade D requires `oos_sharpe > 0 AND monte_carlo_pct >= 0.90`; but Grade F check is `oos_sharpe <= 0 OR monte_carlo_pct < 0.90`; if SR > 0 but MC < 0.90, falls through all checks to return "D" — inconsistency (should be "F")

---

## FILE 50: frontier/validation/frontier_validator.py

### A) Purpose
Seven-test validation suite for candidate frontier signals: IS Sharpe, OOS Sharpe, Monte Carlo permutation, Benjamini-Hochberg FDR, Deflated Sharpe Ratio, Stability (OOS/IS decay), AR(1) autocorrelation. Plus novelty certification (<0.7 max correlation to existing signals).

### B) Classes and Methods

**_sharpe(returns, ann_factor=252)** — `(mean/std) × √ann`
**_monte_carlo_pctile(returns, n_shuffles=500)** — fraction of random shuffles whose Sharpe < observed; seed=42
**_deflated_sharpe(observed_sr, n_trials, n_obs)** — `SR × max(0, 1 - √(log(n_trials)/n_obs))`
**_benjamini_hochberg(p_values, fdr=0.05)** — standard BH procedure

**FrontierValidator**
- `__init__(config)` — reads `config["frontier"]["validation"]` settings
- `validate(signal_name, is_returns, oos_returns, n_trials_searched, has_published_paper, replications, existing_signals, p_values_batch)` — runs all 7 tests + novelty; assigns evidence grade; returns full result dict
- `_novelty_cert(signal_name, oos_returns, existing_signals)` — max abs Pearson correlation to all existing signals; pass if < 0.7
- `quick_validate(signal_name, returns)` — 70/30 IS/OOS split; calls validate()

### C) Mathematics
- IS/OOS Sharpe: `(mean/std) × √252`; threshold IS>=1.0, OOS>=0.7
- Monte Carlo: 500 shuffles, seed=42; threshold >=0.95 (top 5%)
- DSR: `SR_OOS × max(0, 1 - √(log(n_trials)/n_obs))`; threshold >=0.50
- Stability: `OOS_SR / IS_SR`; threshold >=0.50
- AR(1): `abs(autocorr(lag=1))`; threshold <=0.30
- Novelty: max abs Pearson to existing signals; threshold <0.70
- BH: standard FDR at 0.05

### D) Data Flows
- No DB reads/writes; pure computation
- Calls assign_evidence_grade() and grade_to_tier() from evidence_tracker

### E) Dependencies
- Internal: frontier.validation.evidence_tracker
- External: numpy, pandas, scipy (not directly used — no import shown), math, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- BH test: `bh_results[-1]` assumes signal under test is the last entry in p_values_batch — fragile assumption; no explicit index tracking
- `_benjamini_hochberg` only marks rejections for p-values meeting the threshold; signals with p-value NOT rejected (i.e., NOT significant) return `False` — the variable name `reject` is backwards from standard BH convention (True = survives, False = rejected in the code, but BH convention is True = reject null)

---

## FILE 51: signals/anomaly_scanner.py

### A) Purpose
Scans a returns matrix for four recurring statistical anomalies: day-of-week effect, month-of-year effect, momentum (cross-sectional), mean reversion (cross-sectional). Filters by min_obs and min_sharpe. Deduplicates by max correlation to existing signals.

### B) Classes and Methods

**AnomalyScanner**
- `__init__(config)` — reads `config["signal"]["anomaly"]`: min_obs, min_sharpe, max_correlation_to_existing
- `scan(returns)` — runs all 4 scanners; filters; sorts by Sharpe desc
- `deduplicate(anomalies, existing_series)` — removes candidates correlated >max_corr to each other or existing
- `_calendar_dow(returns)` — mean cross-sectional return by day of week; Sharpe per DOW
- `_calendar_month(returns)` — mean cross-sectional return by month
- `_momentum(returns)` — 4 lookbacks × 3 forwards = 12 combinations; long past winners
- `_mean_reversion(returns)` — 3 lookbacks × 3 forwards = 9 combinations; short past winners
- `_cross_sectional_signal(returns, lookback, forward, sign)` — rolling sum lookback, shifted forward; sign determines long/short
- `_sharpe(returns, ann=252)` — static; `(mean/std) × √252`

### C) Mathematics
- Momentum: `sig = sign(rolling_sum_lookback(shifted_1)) × rolling_sum_forward(shifted_-forward)`
- Sharpe: `(mean/std) × √252`; requires >=10 obs and std > 0

### D) Data Flows
- Input: DataFrame (columns=tickers, index=dates)
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: numpy, pandas, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `scan()` checks `hasattr(returns.index, "dayofweek")` — calendar anomalies require DatetimeIndex; if index is not datetime, scan() returns empty list silently
- `_cross_sectional_signal` uses `pd.concat(parts)` across all tickers then `.dropna()` — this pools returns across tickers which may have different sample sizes and introduces cross-sectional correlation in the pooled series

---

## FILE 52: signals/filters.py

### A) Purpose
Pre-trade signal filters. Earnings quality (revenue drop >20%), sector contagion (sector ETF down >3% in 5 days), short availability (market cap > £30M), swing entry confirmation (price vs MA20, RSI, volume), near-earnings block, near-ex-dividend block, broad market trend (SPY vs MA50). All filters fail open.

### B) Classes and Methods

**SignalFilters**
- `__init__(config)` — ETF cache and SPY cache
- `run_all(ticker, market, price_data, signal_date, direction)` — orchestrates all enabled filters; returns {ticker, passed, failures}
- `earnings_quality(ticker, signal_date)` — yfinance quarterly_financials; `rev.iloc[0] < rev.iloc[1] * 0.80`
- `sector_contagion(ticker, signal_date)` — sector ETF 5-day return < -3%
- `short_availability(ticker)` — `marketCap > 30_000_000`
- `sentiment(ticker)` — placeholder; always True
- `_compute_rsi(close, window=14)` — standard RSI
- `swing_entry_confirmation(ticker, direction, price_data)` — MA20, RSI, volume >=80% of 20d avg
- `near_earnings(ticker, days=5)` — yfinance calendar; blocks if earnings within days
- `near_ex_dividend(ticker, days=3)` — yfinance info.exDividendDate; blocks if ex-div within days
- `broad_market_trend(direction)` — SPY vs MA50; cached per instance; long only if SPY above MA50
- `invalidate_spy_cache()` — resets SPY cache

### C) Mathematics
- Revenue quality: `rev.iloc[0] < rev.iloc[1] * 0.80` (latest vs prior)
- Sector return: `(close[-1] / close[0]) - 1 > -0.03` for 5-day window
- RSI: `100 - 100/(1 + gain/loss)` over 14-period rolling
- Volume confirmation: `last_vol >= avg_vol * 0.80`
- SPY MA50: `close.rolling(50).mean()`

### D) Data Flows
- External: yfinance (per-filter fetches)
- No DB reads/writes

### E) Dependencies
- Internal: none
- External: yfinance, pandas, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `earnings_quality()` compares `rev.iloc[0]` (most recent) to `rev.iloc[1]` (prior) — yfinance `quarterly_financials` columns are ordered newest-first; this comparison is correct directionally
- `sector_contagion()` fetches `yf.Ticker(ticker).info` to get sector on every call — expensive yfinance call; not cached
- `sentiment()` is a placeholder that always returns True — no actual sentiment check

---

## FILE 53: signals/short_selling_filter.py

### A) Purpose
Validates short positions: market cap thresholds ($50M, $100M), short interest >30% (crowded), days-to-cover >10, borrow cost > expected return. Fails open on data errors.

### B) Classes and Methods

**get_short_interest(ticker)** — yfinance `shortPercentOfFloat × 100`
**get_days_to_cover(ticker)** — yfinance `shortRatio` or `sharesShort / averageVolume`
**estimate_borrow_cost_annual(short_interest_pct)** — tiered: <10%→2%, <20%→6%, <30%→15%, >=30%→40%
**get_market_cap(ticker)** — yfinance `marketCap`
**_squeeze_risk_label(dtc)** — LOW(<3), MEDIUM(<7), HIGH(>=7)
**validate_short(ticker, expected_return_pct, holding_days=20)** — sequential checks; returns {shortable, reason/metrics}

**ShortSellingFilter**
- `__init__(config)` — stores config
- `filter(ticker, expected_return_pct, holding_days=20)` — calls validate_short(); fails open on exception

### C) Mathematics
- `borrow_cost_period = annual_borrow × (holding_days / 252.0)`
- Net expected return: `expected_return_pct - borrow_cost_period`
- Block condition: `borrow_cost_period > abs(expected_return_pct)`

### D) Data Flows
- External: yfinance (multiple info fetches per call)

### E) Dependencies
- Internal: none
- External: yfinance, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `validate_short()` fetches market cap, short interest, and DTC via separate yfinance `.info` calls — 3 separate API calls per validation; `.info` dict is fetched each time (not cached within the function)

---

## FILE 54: signals/signal_registry.py

### A) Purpose
JSON file-backed persistent registry of candidate and live signals. Status lifecycle: candidate → validated → live (or retired). Auto-promote controlled by config flag.

### B) Classes and Methods

**SignalRegistry**
- `__init__(config, path="output/signal_registry.json")` — loads from JSON on init
- `register(name, signal_type, params, metadata)` — overwrites if exists (logs warning)
- `promote(name, validation)` — if `config["signal"]["anomaly"]["auto_promote"]` is True → "live"; else → "validated"
- `set_live(name)` — manual promotion
- `retire(name, reason)` — sets status="retired"
- `get_live()`, `get_candidates()`, `get_validated()` — filtered lists
- `get(name)` — single entry
- `list_all()` — returns DataFrame
- `_load()`, `_save()` — JSON file I/O

### C) Mathematics
- None

### D) Data Flows
- File: `output/signal_registry.json` (read/write)

### E) Dependencies
- Internal: none
- External: json, pandas, logging, pathlib

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `register()` silently overwrites existing entries (only a warning log) — risk of losing validation history for an existing signal
- `registered_at` uses `datetime.now().isoformat()` without timezone — naive datetime

---

## FILE 55: signals/signal_validator.py

### A) Purpose
Out-of-sample validation using train/val/test split from backtest config. A signal passes if both val and test Sharpe >= 50% of min_sharpe threshold.

### B) Classes and Methods

**SignalValidator**
- `__init__(config)` — reads train_pct, val_pct, min_sharpe from config
- `validate(signal_returns)` — splits into train/val/test; computes Sharpe for each; passes if val >= min_sharpe*0.5 AND test >= min_sharpe*0.5
- `ttest(returns, alpha=0.05)` — one-sample t-test vs 0; returns (p_value, bool)
- `_sharpe(returns, ann=252)` — static; `(mean/std) × √252`

### C) Mathematics
- Split: `i1 = int(n * train_pct)`, `i2 = int(n * (train_pct + val_pct))`
- Threshold: `min_sharpe * 0.5`
- Sharpe: `(mean/std) × √252`
- t-test: one-sample, null = mean return = 0

### D) Data Flows
- No DB reads/writes; pure computation

### E) Dependencies
- Internal: none
- External: numpy, pandas, scipy.stats, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Minimum n=100 required — for daily returns this is only ~4 months of data; low threshold for production signal validation
- OOS threshold is 50% of min_sharpe (not 50% of full Sharpe) — lenient; e.g. if min_sharpe=1.0, passes at OOS Sharpe=0.5

---

## FILE 56: execution/alpaca_rate_limiter.py

### A) Purpose
Multi-bucket token-bucket rate limiter for Alpaca API. Separate buckets for data (200/min), trading (60/min), account (50/min), default (100/min). Thread-safe. Supports acquire() and context-manager limit().

### B) Classes and Methods

**_TokenBucket**
- `__init__(capacity, refill_period)` — `refill_rate = capacity / refill_period` tokens/second
- `_refill()` — `tokens = min(capacity, tokens + elapsed × refill_rate)`
- `acquire(tokens=1, timeout=60.0)` — blocking; wait = `(tokens - available) / refill_rate`; returns bool
- `available()` — current token count

**AlpacaRateLimiter**
- `__init__(limits)` — creates one _TokenBucket per endpoint category
- `acquire(endpoint, tokens, timeout)` — acquires from appropriate bucket; logs timeout warning
- `limit(endpoint, timeout)` — context manager wrapping acquire()
- `available(endpoint)` — passthrough to bucket
- `status()` — dict of available/capacity/calls_made per bucket
- `reset_counts()` — resets call counters

### C) Mathematics
- Refill: `tokens = min(capacity, tokens + elapsed × (capacity / refill_period))`
- Wait time: `(tokens_needed - current_tokens) / refill_rate`

### D) Data Flows
- No DB reads/writes; pure rate control

### E) Dependencies
- Internal: none
- External: threading, time, contextlib, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- None significant

---

## FILE 57: execution/chunked_scanner.py

### A) Purpose
Splits large ticker universes into batches of 200 for scanning. Fetches 600 days of price data per chunk. Runs all signals on each ticker; finds best signal by score; applies tier multiplier; places order via broker.

### B) Classes and Methods

**ChunkedScanner**
- `__init__(paper_trader, chunk_size=200, max_workers=1, delay_between_chunks=1.5)` — stores trader reference; max_workers unused (sequential execution)
- `scan_chunk(tickers_chunk, chunk_num, total_chunks, account_equity, max_new_positions)` — batch price fetch; per-ticker: build context, run signals, find best, size position, place order, record in closeloop
- `scan_all(tickers, account_equity, max_new_positions)` — splits into chunks; calls scan_chunk; aggregates counts

### C) Mathematics
- TIER_SIZE_MULTIPLIERS (local, differs from universe.py): TIER_1=1.0, TIER_2=0.8, TIER_3=0.6, MICRO=0.4, UK_1=1.0, UK_2=0.7, UNKNOWN=0.5
- `final_value = sizing['position_value'] × tier_mult`
- `shares = max(1, int(final_value / current_price))`
- Estimated time: `total_chunks × 12` seconds (printed to stdout)

### D) Data Flows
- Reads: DataFetcher.fetch_universe_data() (600 days of price data)
- Writes: broker.place_order(), closeloop.open_trade()

### E) Dependencies
- Internal: (via paper_trader) data.fetcher, execution.paper_trader, execution.broker_interface
- External: logging, time, datetime, pandas

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `max_workers=1` defined but unused — no parallel chunk processing despite the parameter name
- `close` column access: `price_data['close'].iloc[-1] if 'close' in price_data.columns else price_data.iloc[:, 3].iloc[-1]` — fallback uses column index 3 (0-indexed) which assumes OHLCV column order; may pick wrong column
- TIER_SIZE_MULTIPLIERS here differ from those in universe.py (TIER_2=0.8 vs 0.70; TIER_3=0.6 vs 0.50) — inconsistency

---

## FILE 58: execution/feature_manager.py

### A) Purpose
Singleton FeatureManager. Wraps optional bot features with auto-disable after 3 consecutive failures and Telegram alert. Loads feature flags from config. Provides wrap() and @guarded decorator.

### B) Classes and Methods

**FeatureManager**
- `__init__(config)` — loads `config["feature_flags"]` dict; initialises failures/disabled_at dicts
- `get(config)` — classmethod singleton; raises RuntimeError if not yet initialised and no config provided
- `reset()` — classmethod; for testing only
- `_load_flags()` — iterates `feature_flags` dict from config
- `is_enabled(name)` — returns flag; default True if not listed
- `disable(name, reason)` — sets flag False; records timestamp; sends Telegram alert
- `enable(name)` — re-enables; resets failure count
- `wrap(name, fn, *args, default, **kwargs)` — calls fn if enabled; increments failure count on exception; disables after 3 failures; returns default on failure
- `guarded(name, default)` — decorator wrapping wrap()
- `_log_feature_error(name, exc)` — appends to `logs/feature_errors.log`
- `_alert(msg)` — posts to Telegram if token and chat_id configured
- `status_dict()` — flags, failures, disabled_at timestamps

### C) Mathematics
- Disable threshold: 3 consecutive failures

### D) Data Flows
- File writes: `logs/feature_errors.log`
- External: Telegram API (if configured)

### E) Dependencies
- Internal: none
- External: threading, time, functools, collections, requests (for Telegram), logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `is_enabled()` defaults to True for unlisted features — any feature not in config is enabled by default; new untested features could silently activate

---

## FILE 59: execution/trailing_stops.py

### A) Purpose
TrailingStopManager: tiered trailing stops. Tier 1 (gain <15%): 15% trailing; Tier 2 (15–25%): 20% trailing; Tier 3 (>25%): 25% trailing. Stop only moves upward. Bulk initialisation from existing portfolio.

### B) Classes and Methods

**_trailing_pct(gain_pct)** — returns trailing stop percentage per tier
**_Position** (dataclass) — ticker, entry_price, peak_price, current_stop, tier, first_observed, last_updated

**TrailingStopManager**
- `__init__()` — `_positions: dict[str, _Position]`
- `add_position(ticker, entry_price, current_price)` — registers new position; computes initial stop
- `remove_position(ticker)` — removes from dict
- `has_position(ticker)` — membership check
- `observe(ticker, current_price, entry_price)` — updates peak; recomputes stop (stop only moves up); logs tier changes
- `should_exit(ticker, current_price)` — True if `current_price <= current_stop`
- `stop_price(ticker)` — returns current stop level
- `tier(ticker)` — returns current tier
- `initialise_from_positions(positions)` — bulk add from list of dicts
- `status()` — list of dicts with all position metrics

### C) Mathematics
- _TIERS: [(0.25, 0.25), (0.15, 0.20), (0.00, 0.15)]
- `gain = (peak_price - entry_price) / entry_price`
- `trail = _trailing_pct(gain)`
- `new_stop = peak_price × (1.0 - trail)`
- Stop only updates if `new_stop > current_stop`

### D) Data Flows
- In-memory only; no DB reads/writes

### E) Dependencies
- Internal: none
- External: dataclasses, datetime, logging

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- No persistence; stops are lost on process restart; `initialise_from_positions()` must be called with current prices at startup to restore stops
- Short positions are not handled — stop logic assumes long (current_price <= stop triggers exit); for shorts the logic would be inverted

---

## FILE 60: core/retraining_controller.py

### A) Purpose
Governs the full ML model retraining lifecycle: dormancy gate (minimum 500 real trades + 30 trading days before eligible), performance trigger detection (Sharpe decline, win rate decay, drawdown increase), shadow training registration, candidate validation, and rollback capability. Runs every 6 hours via AutomationScheduler.

### B) Classes and Methods

**RetrainingController**
- `__init__()` — creates directories: `altdata/models/`, `altdata/models/candidates/`, `altdata/models/archive/`
- `check_dormancy() -> (bool, str)` — queries `closeloop/storage/closeloop.db` trade_ledger for DISTINCT(ticker||entry_date) with ABS(net_pnl)>0.01 and is_phantom=0; returns (is_dormant, reason)
- `compute_rolling_metrics(window_days=14) -> Optional[dict]` — Sharpe, Sortino, max_drawdown, win_rate, profit_factor from recent trades; returns None if <20 trades
- `check_retraining_triggers(current, baseline) -> (bool, str, str)` — compares 14-day vs 60-day rolling windows; triggers if Sharpe decline>0.3, win_rate decline>0.08, or drawdown increase>0.05
- `log_retraining_event(reason, metric, value, threshold)` — writes to `simulations/shadow.db` → retraining_events table
- `run_monitoring_cycle()` — main 6-hour cycle: dormancy → triggers → shadow training
- `_initiate_shadow_training(current, baseline, reason)` — registers candidate in shadow.db model_registry; actual training deferred (logs note to use altdata/learning/weekly_retrainer.py)
- `_validate_candidate(version_id, candidate, live) -> bool` — rejects if Sharpe improvement <0.10, drawdown worse by >0.02, or win_rate declined
- `rollback_to_version(version_id) -> bool` — copies `altdata/models/archive/{id}.joblib` → `altdata/models/live_model.joblib`; backs up current as `.pre_rollback`

### C) Mathematics
```
Sharpe (rolling) = mean(pnl) / std(pnl) × sqrt(252)
Sortino = mean(pnl) / std(losing_pnl) × sqrt(252)
max_drawdown = max((peak - equity) / peak) over trade sequence
win_rate = winners / total_trades
profit_factor = sum(winners) / abs(sum(losers))

Trigger thresholds:
  SHARPE_DECLINE_THRESHOLD    = 0.30
  WIN_RATE_DECLINE_THRESHOLD  = 0.08
  DRAWDOWN_INCREASE_THRESHOLD = 0.05
  ROLLING_WINDOW_DAYS         = 14 (current) vs 60 (baseline)

Deployment gate:
  MIN_SHARPE_IMPROVEMENT = 0.10
  MAX_DRAWDOWN_INCREASE  = 0.02
```

### D) Data Flows
- **Reads:** `closeloop/storage/closeloop.db` → `trade_ledger` (net_pnl, entry_date, is_phantom)
- **Writes:** `simulations/shadow.db` → `retraining_events`, `model_registry`
- **File I/O:** copies .joblib files in `altdata/models/archive/` and `altdata/models/candidates/`

### E) Dependencies
- Internal: none (standalone)
- External: `sqlite3`, `logging`, `json`, `uuid`, `os`, `datetime`, `statistics`, `math`, `shutil`

### F) Wiring Status
[LIVE PATH] — called by `intelligence/automation_scheduler.py` every 6 hours via `retraining_monitor` job.

### G) Issues Found
- `_initiate_shadow_training()` only writes a registry entry; actual training code is deferred — no training ever executes in live production without `weekly_retrainer.py` being invoked separately
- `compute_rolling_metrics()`: equity starts at hardcoded $100,000 regardless of actual portfolio value; max_drawdown calculation is over pnl sequence only, not real equity curve
- `check_dormancy()`: DISTINCT workaround comment references a "duplicate-recording bug" — a known upstream bug in trade_ledger schema that causes duplicate rows
- `simulations/shadow.db` must be pre-created with `retraining_events` and `model_registry` tables; no `_init_db()` method; will fail silently on first run

---

## FILE 61: execution/alpaca_stream.py

### A) Purpose
Real-time websocket price stream from Alpaca IEX free tier. Runs in a background daemon thread with its own asyncio event loop. Provides a PriceCache for real-time price lookups, spike/urgent detection, and 30-point rolling price history per ticker.

### B) Classes and Methods

**PriceCache**
- `update(ticker, price, volume, ts)` — skips moves <0.5% (_CACHE_THRESHOLD); detects SPIKE (≥2% in 5min) and URGENT (≥5% in 10min); returns flag or None
- `get_price(ticker)` — returns current price; warns if stale (>300s)
- `get_move_pct(ticker, minutes)` — returns % move over last N minutes from history deque
- `get_all_prices()` — snapshot dict of all cached tickers
- `get_spike_flags()` — returns active spike flags
- `mark_connected() / mark_disconnected()` — connectivity state
- `stats()` — dict of total_updates, connected, last_update, ticker_count

**AlpacaStreamWorker**
- `__init__(config, tickers)` — stores config; creates PriceCache; sets up rotating log handler at `logs/alpaca_stream.log`
- `start()` — launches daemon thread with private asyncio event loop
- `_run_loop()` — asyncio event loop for the daemon thread
- `_stream()` — main websocket coroutine; connects to `wss://stream.data.alpaca.markets/v2/iex`; authenticates; subscribes; dispatches messages; auto-reconnects with 30s delay
- `_handle_message(msg_list)` — processes `b` (bar) and `t` (trade) message types; calls cache.update()
- `add_tickers(tickers)` — subscribes new tickers to running stream
- `is_connected()` — returns cache._connected

**Module-level functions**
- `start_stream(config, tickers) -> AlpacaStreamWorker` — creates and starts singleton worker
- `get_stream_cache() -> PriceCache` — returns module-level cache singleton
- `get_stream_worker() -> AlpacaStreamWorker` — returns worker singleton

### C) Mathematics
```
SPIKE detection:  |price_change| >= 2.0% in 5-minute window
URGENT detection: |price_change| >= 5.0% in 10-minute window
move_pct = (current - oldest_in_window) / oldest_in_window × 100
Cache threshold: only update if |pct_change| >= 0.5%
```

### D) Data Flows
- **Reads:** Alpaca websocket `wss://stream.data.alpaca.markets/v2/iex` (bar + trade messages)
- **Writes:** `logs/alpaca_stream.log` (rotating, 10MB, 5 backups); PriceCache in-memory

### E) Dependencies
- Internal: none
- External: `asyncio`, `json`, `threading`, `websockets` (implied by websocket usage), `logging`, `collections.deque`, `datetime`

### F) Wiring Status
[LIVE PATH] — started by `execution/trading_bot.py` at bot startup; PriceCache consumed by trading bot's scan loops.

### G) Issues Found
- Uses Alpaca IEX (free tier) not SIP — IEX is a subset of total market volume; prices may not reflect true NBBO
- `_CACHE_THRESHOLD = 0.005` means small-cap stocks with slow drift (<0.5%/tick) may have stale cache entries that never update
- No subscription refresh after reconnect if `add_tickers()` was called while disconnected — tickers added post-disconnect are not re-subscribed on reconnect

---

## FILE 62: execution/paper_trader.py

### A) Purpose
Main paper trading loop. Generates signals, sizes positions, manages exits (scale-out, ATR stop, volume dry-up, signal reversal, time-based). Manages correlation limits (max 3 same-sector, max 2 with corr>0.6). Runs in OBSERVATION MODE at 25% Kelly. Full context captured at open; trade autopsy on close.

### B) Classes and Methods

**Module-level helpers**
- `_is_market_open(market)` — uses MarketCalendar; UTC time comparison
- `_next_market_open_str(market)` — human-readable next open string

**PaperTrader** (main class, implied from usage)
- Scale-out at 50% and 100% of target (33%+33%+34% scheme)
- ATR stop multiplier: 1.5×
- Volume dry-up: <30% of 20d avg for 2 consecutive days
- Correlation management enforced before entry
- OBSERVATION_MODE: 25% of normal Kelly sizing
- Below-threshold signals logged as OBSERVED_NOT_TRADED

### C) Mathematics
```
Scale-out levels: [50% of target, 100% of target]
Scale-out fractions: [33%, 33%] — 34% runs to exit_date
ATR stop: entry ± 1.5 × ATR
Volume dry-up: volume < 30% of 20d avg for 2 consecutive days
Observation size: obs_size_fraction × normal_kelly
Correlation thresholds:
  max same-sector positions: 3
  max high-corr (>0.60) positions: 2
```

### D) Data Flows
- **Reads:** yfinance (price, volume), MarketCalendar, signal generators, RiskManager
- **Writes:** closeloop_store (trade open/close context, autopsy), logs

### E) Dependencies
- Internal: `analysis.market_calendar`, `execution.broker_interface`, `risk.manager`, signal classes, `closeloop.*`
- External: `numpy`, `pandas`, `yfinance`, `schedule`, `logging`, `datetime`

### F) Wiring Status
[LIVE PATH] — core trading execution engine.

### G) Issues Found
- OBSERVATION_MODE hardcoded at 25% of Kelly — no config override
- Scale-out only at 50%/100% of target; misses scenarios where target is never reached (position held until time-exit)
- Correlation computation requires live price data on each scan — adds latency to scan loops

---

## FILE 63: execution/broker_interface.py

### A) Purpose
Abstract broker interface with two implementations: PaperBroker (in-memory simulation with slippage/commission/stamp duty) and AlpacaPaperBroker (direct HTTP to Alpaca paper trading API, with simulation fallback). Both implement the same ABC contract.

### B) Classes and Methods

**BrokerInterface (ABC)**
- `place_order(ticker, qty, side, order_type, price) -> dict`
- `get_positions() -> list`
- `get_account_value() -> float`
- `get_cash() -> float`

**PaperBroker**
- In-memory `_positions` dict with FIFO average cost tracking
- `place_order()` — applies slippage (0.05% market, 0% limit), commission ($1/trade), stamp duty (0.5% UK buys)
- `get_positions()` — returns current holdings with avg cost

**AlpacaPaperBroker**
- Connects to `https://paper-api.alpaca.markets` via direct HTTP (no SDK)
- Falls back to simulation mode if credentials absent or `"PASTE"` literal found in key
- `place_order()` — POST to `/v2/orders`; handles JSON response
- `get_positions()` — GET `/v2/positions`
- `get_account_value()`, `get_cash()` — GET `/v2/account`

### C) Mathematics
```
PaperBroker execution:
  market order fill = current_price × (1 + 0.0005) for BUY
  market order fill = current_price × (1 - 0.0005) for SELL
  commission = $1.00 per trade
  stamp duty = 0.005 × notional (UK buys only)
  avg_cost = FIFO weighted average
```

### D) Data Flows
- **PaperBroker:** in-memory only
- **AlpacaPaperBroker:** reads/writes Alpaca paper trading API; no local DB

### E) Dependencies
- Internal: none
- External: `abc`, `requests` (AlpacaPaperBroker), `logging`, `datetime`

### F) Wiring Status
[LIVE PATH] — instantiated by trading_bot.py.

### G) Issues Found
- `"PASTE"` literal check in API key disables AlpacaPaperBroker silently — falls back to simulation with no warning in logs at INFO level
- PaperBroker commission is flat $1/trade — does not scale with position size; unrealistic for large orders
- No order cancellation or amendment methods in ABC — only market/limit entry supported

---

## FILE 64: execution/adaptive_position_sizer.py

### A) Purpose
6-phase adaptive position sizing system. PHASE_1–PHASE_5 impose conservative caps while trade count is below 2000. PHASE_FREE (≥2000 trades) uses autonomous fractional Kelly. Applies confluence multipliers, macro multipliers, UMCI multipliers. Triggers SymbolicRegressionEngine at trade milestones.

### B) Classes and Methods

**AdaptivePositionSizer**
- `__init__(config, phase_override)` — loads config; initialises phase from trade count
- `get_phase(trade_count) -> dict` — returns phase dict with min_pct, max_pct, description
- `_load_trade_count() -> int` — reads `output/permanent_archive.db` phase_history table
- `size_position(signal, portfolio_value, trade_count, macro_regime, umci_score) -> dict` — main sizing method; returns pct, notional, reasoning
- `_apply_confluence_multiplier(n_agreeing_signals) -> float` — 4+→2.0×, 3→1.6×, 2→1.3×, 1→1.0×
- `_apply_macro_multiplier(macro_regime) -> float` — GOLDILOCKS→1.30, CRISIS→0.20
- `_apply_umci_multiplier(umci_score) -> float` — <30→1.0, <60→0.85, <80→0.65, <95→0.30, else→0.10
- `_kelly_fraction(p, b) -> float` — PHASE_FREE only: (p×b - q)/b × 0.25 (25% fractional)
- `auto_trigger_discovery(trade_count)` — fires SymbolicRegressionEngine at milestones 200/400/600/800/1000/1500/2000

### C) Mathematics
```
PHASES:
  PHASE_1 (0–100):   base 0.15%–0.40%
  PHASE_2 (100–300): base 0.30%–0.80%
  PHASE_3 (300–600): base 0.50%–1.20%
  PHASE_4 (600–1000):base 0.70%–1.80%
  PHASE_5 (1000–2000):base 1.00%–2.50%
  PHASE_FREE (≥2000): fractional Kelly = (p×b - q)/b × 0.25

Multiplier chain:
  final_pct = base_pct × confluence × macro × umci
  clamped to [never_below=0.1%, never_exceed=3%]

Absolute limits:
  sector_max = 20%, market_max = 60%
  halt if drawdown ≥ 15%
```

### D) Data Flows
- **Reads:** `output/permanent_archive.db` → phase_history (trade count)
- **Writes:** `output/permanent_archive.db` → phase_history (phase transitions)

### E) Dependencies
- Internal: `frontier.signals.frontier_signal_engine` (UMCI), `altdata.reasoning.symbolic_regression` (milestone triggers)
- External: `sqlite3`, `logging`, `datetime`, `math`

### F) Wiring Status
[LIVE PATH] — called by paper_trader.py for every entry decision.

### G) Issues Found
- DB path `output/permanent_archive.db` does not match `permanent_store.py` which uses `output/permanent_log.db` — these are different files; phase history may be written to a DB that other modules never read
- `auto_trigger_discovery()` imports SymbolicRegressionEngine dynamically — if that module is missing, discovery silently fails with no log at WARNING level
- PHASE_FREE Kelly fraction capped at 25% with no config override

---

## FILE 65: execution/cooling_off_tracker.py

### A) Purpose
Prevents re-entry into a ticker for 5 trading days after a losing stop-loss exit. Only triggers on `atr_stop` or `trailing_stop` exits with >2% loss. Includes 5 early-release conditions. Persisted to SQLite at `closeloop/storage/cooling_off.db`.

### B) Classes and Methods

**StockCoolingOffTracker**
- `__init__(cooling_days=5)` — initialises DB and loads active lockouts
- `_init_db()` — creates `cooling_off` table
- `_save_to_db(ticker, entry)` / `_delete_from_db(ticker)` / `_load_from_db()` — SQLite persistence
- `register_exit(ticker, exit_date, exit_price, pnl_pct, exit_reason)` — only locks if exit_reason in {atr_stop, trailing_stop} AND pnl_pct < -0.02; winning exits clear existing lockouts
- `is_cooling_off(ticker, as_of) -> bool` — True if active lockout; auto-expires on date
- `days_remaining(ticker, as_of) -> int` — calendar days left in cooling period
- `check_early_release(ticker, current_price, earnings_beat_pct, volume_ratio, altdata_score, days_to_cover, as_of) -> bool` — evaluates 5 conditions; marks entry early_released in place
- `expire_old_entries(as_of) -> int` — batch cleanup of expired entries
- `status(as_of) -> list[dict]` — all active lockouts with days_remaining
- `active_count() -> int`

### C) Mathematics
```
Lockout conditions (ALL must be true):
  exit_reason in {atr_stop, trailing_stop}
  pnl_pct < -0.02 (2%+ loss)

Early release (ANY one sufficient):
  1. earnings_beat_pct > 0.05 (5% beat)
  2. volume_ratio > 3.0 (3× 20d avg)
  3. altdata_score > 0.7
  4. (exit_price - current_price) / exit_price > 0.15 (15% price drop from exit)
  5. days_to_cover < 2.0
```

### D) Data Flows
- **Reads/Writes:** `closeloop/storage/cooling_off.db` → `cooling_off` table

### E) Dependencies
- Internal: none
- External: `sqlite3`, `os`, `logging`, `datetime`

### F) Wiring Status
[LIVE PATH] — checked before every new entry in trading_bot.py.

### G) Issues Found
- Cooling-off period is 5 calendar days, not 5 trading days (despite docstring saying "trading days")
- Winning exits unconditionally clear lockouts — a winning exit on a previously locked ticker removes the lock even if the stock is being re-entered too quickly after a loss on a different date

---

## FILE 66: execution/trading_bot.py

### A) Purpose
Main orchestration bot. Schedules UK (9 scans) and US (8 scans) market windows. Manages data collection (every 30 minutes), morning/close/weekly pipeline runs, weekly report, stream worker, cooling-off checks. Contains holiday tables for 2026 only.

### B) Classes and Methods

**TradingBot (main class)**
- `__init__(config)` — loads config; instantiates all subsystems
- `start()` — starts MonitorRunner, stream worker, scheduled jobs
- `_run_scan(market)` — orchestrates a single scan cycle: fetch prices, generate signals, check cooling-off, size positions, execute via broker
- `_run_data_collection()` — 30-minute data update
- `_run_weekly_report()` — Sunday 09:00 UTC; sends weekly Telegram report
- Holiday tables: `_US_HOLIDAYS_2026`, `_UK_HOLIDAYS_2026` — hardcoded sets of date strings for 2026 only

**Scan schedules (UTC):**
- UK: 9 scans from 07:00–15:30
- US: 8 scans from 14:00–20:30
- `DATA_COLLECTION_INTERVAL_MINUTES = 30`

### C) Mathematics
No novel mathematics — orchestration only.

### D) Data Flows
- **Reads:** all subsystem outputs, stream cache, databases
- **Writes:** orchestrates writes through subsystems; no direct DB writes

### E) Dependencies
- Internal: virtually all other modules (paper_trader, broker_interface, monitor_runner, daily_pipeline, cooling_off_tracker, adaptive_position_sizer, alpaca_stream)
- External: `schedule`, `threading`, `datetime`, `logging`

### F) Wiring Status
[LIVE PATH] — entry point for live trading.

### G) Issues Found
- Holiday tables hardcoded for 2026 only — will fail silently in 2027 (no holidays detected → may trade on public holidays)
- No graceful shutdown handler (SIGTERM/SIGINT); stream worker may leave open websocket connections
- Weekly report is sent from trading_bot.py at Sunday 09:00 UTC AND commented out in both monitor_runner.py and automation_scheduler.py — only one path active

---

## FILE 67: risk/manager.py

### A) Purpose
RiskManager: computes Kelly-based position sizes, applies UMCI and macro regime multipliers, integrates LiquidityScorer (REJECT/REDUCE_SIZE), computes ATR-based stops, and enforces absolute position limits.

### B) Classes and Methods

**RiskManager**
- `__init__(config)` — reads kelly_fraction (default 0.5) from config
- `size_position(ticker, signal_strength, win_rate, avg_win, avg_loss, portfolio_value, regime, umci_score, price_data) -> dict` — full sizing pipeline; returns pct, notional, stop_price, reasoning
- `_kelly(p, b) -> float` — Half-Kelly × |signal_strength| formula
- `_umci_multiplier(umci_score) -> float` — tiered multiplier
- `_regime_multiplier(regime) -> float` — reads `_REGIME_MULTIPLIERS` dict by integer key
- `_check_liquidity(ticker, price_data) -> dict` — calls LiquidityScorer; returns REJECT/REDUCE_SIZE/OK
- `_compute_atr_stop(price_data, atr_mult=2.0) -> float` — 14-period EWM ATR; stop at entry ± atr_mult×ATR

### C) Mathematics
```
f* = (b×p - q) / b       [Kelly criterion]
  b = avg_win / avg_loss
  p = win_rate, q = 1 - p

kelly_size = f* × kelly_fraction × |signal_strength| × regime_mult × umci_mult
clamped to [0, max_position_pct from config]

UMCI multipliers (score→multiplier):
  <30  → 1.00
  <60  → 0.85
  <80  → 0.65
  <95  → 0.30
  else → 0.10

_REGIME_MULTIPLIERS (int keys):
  0 (RISK_ON)       → 1.00
  1 (GOLDILOCKS)    → 1.15
  2 (STAGFLATION)   → 0.70
  3 (RISK_OFF)      → 0.50
  4 (RECESSION_RISK)→ 0.30

ATR: pd.concat([H-L, |H-C.shift()|, |L-C.shift()|]).max(axis=1).rolling(14).mean() × atr_mult
```

### D) Data Flows
- **Reads:** price_data (DataFrame), LiquidityScorer (deepdata module)
- **Writes:** none directly (returns sizing dict)

### E) Dependencies
- Internal: `deepdata.microstructure.liquidity_scorer.LiquidityScorer`
- External: `numpy`, `pandas`, `logging`, `math`

### F) Wiring Status
[LIVE PATH] — called by paper_trader.py for every entry.

### G) Issues Found
- `_REGIME_MULTIPLIERS` uses integer keys 0–4 but `paper_trader.py` passes string regime names via `_REGIME_STR_TO_INT` dict — mismatch if string passed directly
- LiquidityScorer REJECT causes position size = 0.0 silently (no Telegram alert); position simply not taken

---

## FILE 68: signals/calendar_effects_signal.py

### A) Purpose
CalendarEffectsSignal: 6 time-based modifiers (january_effect, tax_loss_reversal, earnings_season_timing, options_expiry_proximity, fed_meeting_proximity, window_dressing). Total signal clamped to [-0.40, +0.30].

### B) Classes and Methods

**CalendarEffectsSignal**
- `generate(ticker, price_data, date) -> dict` — sums all 6 modifiers; clamps total; writes to DB
- `_january_effect(date, is_small_cap) -> float` — +0.10 if Jan ≤ day 15 AND small_cap
- `_tax_loss_reversal(date) -> float` — +0.08 if Jan ≤ day 7
- `_earnings_season_timing(date) -> float` — +0.10 weeks 1–2, -0.05 weeks 3–4, -0.10 between seasons
- `_options_expiry_proximity(date) -> float` — -0.20 triple-witching Friday, -0.10 standard expiry week
- `_fed_meeting_proximity(date) -> float` — -0.30 meeting day, -0.25 within 7d, -0.15 within 14d
- `_window_dressing(date) -> float` — +0.05 last 3 days of quarter

### C) Mathematics
```
total = sum(6 modifiers)
final = clamp(total, -0.40, +0.30)

Fed meeting detection: hardcoded list of 2026 FOMC dates
Earnings season: week numbers computed via datetime.isocalendar()
```

### D) Data Flows
- **Reads:** date (system clock), config (is_small_cap flag)
- **Writes:** `output/permanent_archive.db` → `calendar_signals` table

### E) Dependencies
- Internal: none
- External: `datetime`, `sqlite3`, `logging`

### F) Wiring Status
[LIVE PATH] — one of the 8+ signal generators called during scan loops.

### G) Issues Found
- FOMC dates hardcoded for 2026 only — same issue as holiday tables
- DB path `output/permanent_archive.db` inconsistency with permanent_store.py
- `_options_expiry_proximity()` detects triple-witching by checking if it's the third Friday of March/June/September/December — correct logic but assumes US calendar; UK has different expiry dates

---

## FILE 69: signals/gap_signal.py

### A) Purpose
GapSignal: detects overnight gaps 2–8% in price. Regime-conditioned: BULL→continuation, NEUTRAL/BEAR→fade. Applies volume filter (>1.5× 20d avg) and sector alignment check. CRISIS→skip.

### B) Classes and Methods

**GapSignal**
- `generate(ticker, price_data, market_regime) -> Optional[dict]` — returns signal dict or None
- `_compute_gap(price_data) -> float` — (open[today] - close[yesterday]) / close[yesterday]
- `_check_volume(price_data) -> bool` — today_vol / 20d_avg > 1.5
- `_check_sector_alignment(ticker, gap_direction) -> bool` — fetches sector ETF; if ETF gapping same direction → skip fade

### C) Mathematics
```
gap_pct = (open_today - close_yesterday) / close_yesterday

Thresholds: MIN_GAP_PCT=2%, MAX_GAP_PCT=8%
Volume filter: MAX_VOLUME_RATIO=1.5× 20d avg

BULL regime:
  up-gap  → LONG CONTINUATION: score = min(gap_pct×8, 0.7)
  down-gap → LONG FADE:        score = min(gap_pct×10, 1.0)

NEUTRAL/BEAR:
  fade the gap (opposite direction)

CRISIS: skip all gaps
```

### D) Data Flows
- **Reads:** price_data (OHLCV), yfinance (sector ETF), market_regime string

### E) Dependencies
- Internal: none
- External: `yfinance`, `pandas`, `logging`

### F) Wiring Status
[LIVE PATH] — called during scan loops.

### G) Issues Found
- Sector alignment check fetches yfinance data on every generate() call — no caching
- `MAX_VOLUME_RATIO` naming is confusing (it is a minimum threshold, not a maximum)

---

## FILE 70: signals/momentum_signal.py

### A) Purpose
MomentumSignal: composite 1m/3m/6m return signal. Fires if |score|>0.05. Requires ≥60 days of price data.

### B) Classes and Methods

**MomentumSignal**
- `generate(ticker, price_data) -> Optional[dict]` — computes score; returns dict or None

### C) Mathematics
```
r1m  = (close[-1] - close[-21]) / close[-21]
r3m  = (close[-1] - close[-63]) / close[-63]
r6m  = (close[-1] - close[-126]) / close[-126]
score = r1m×0.5 + r3m×0.3 + r6m×0.2
fires if |score| > 0.05
signal_score = min(|score|×3, 1.0)
direction = LONG if score>0 else SHORT
```

### D) Data Flows
- **Reads:** price_data (close prices)

### E) Dependencies
- External: `pandas`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Returns are not risk-adjusted (no volatility normalisation) — high-vol stocks get same score as low-vol stocks for equivalent returns
- 126 trading days (6 months) requires ≥60d check — inconsistent; 126>60

---

## FILE 71: signals/mean_reversion_signal.py

### A) Purpose
MeanReversionSignal: Z-score Bollinger + SMA-based RSI. Entry at extreme readings (z<-2.0 AND RSI<35 for LONG; z>2.0 AND RSI>65 for SHORT). Requires ≥30 days of data.

### B) Classes and Methods

**MeanReversionSignal**
- `generate(ticker, price_data) -> Optional[dict]` — computes zscore and RSI; returns signal dict or None

### C) Mathematics
```
SMA20  = close.rolling(20).mean()
STD20  = close.rolling(20).std()
zscore = (close[-1] - SMA20[-1]) / STD20[-1]

RSI (SMA-based, not EWM):
  delta = close.diff()
  gain  = delta.clip(lower=0).rolling(14).mean()
  loss  = (-delta).clip(lower=0).rolling(14).mean()
  rs    = gain / loss
  rsi   = 100 - 100 / (1 + rs)

Entry:
  LONG:  zscore < -2.0 AND rsi < 35
  SHORT: zscore > +2.0 AND rsi > 65
```

### D) Data Flows
- **Reads:** price_data (close prices)

### E) Dependencies
- External: `pandas`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- RSI uses SMA (rolling mean) not standard EWM — diverges from Wilder's original RSI; inconsistent with technical_indicators.py which uses EWM

---

## FILE 72: signals/options_earnings_signal.py

### A) Purpose
OptionsEarningsSignal: 3 sub-signals (BUY_STRADDLE, SELL_IV_CRUSH, BUY_OTM_CALLS/PUTS) based on days-to-earnings, IV rank proxy, and historical vs implied move. PEAD confidence modifier via call/put ratio.

### B) Classes and Methods

**OptionsEarningsSignal**
- `generate(ticker, price_data, days_to_earnings) -> Optional[dict]` — entry gate: days in [3,5]
- `_compute_iv_rank(price_data) -> float` — proxy formula (not true IV rank)
- `_compute_implied_move(ticker, price_data) -> float` — ATM straddle: (call_ask + put_ask) / current_price × 100
- `_compute_historical_move(price_data, window=8) -> float` — average of 8 prior earnings-window moves
- `_pead_confidence_modifier(c_p_ratio, direction) -> float` — C/P>2→1.25×; C/P<0.5→0.75×

### C) Mathematics
```
iv_rank = min(100, max(0, (iv_current - 0.15) / 0.50 × 100))
  [proxy only — no historical IV comparison]

implied_move_pct = (call_ask + put_ask) / current_price × 100

BUY_STRADDLE: days∈[3,5] AND iv_rank<50 AND hist_move > implied×1.1
SELL_IV_CRUSH: days∈[3,5] AND iv_rank≥70
BUY_OTM_CALLS/PUTS: directional play; sizing_fraction = 0.10 (10% of stock notional)

PEAD modifier:
  Long PEAD: C/P>2 → 1.25×; C/P<0.5 → 0.75×
  Short PEAD: inverse
```

### D) Data Flows
- **Reads:** price_data, options chain via yfinance

### E) Dependencies
- External: `yfinance`, `pandas`, `numpy`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `iv_rank` proxy is not true IV rank — it maps current IV against a fixed range [15%, 65%] instead of comparing against historical IV values; this is a systematic miscalculation
- Options chain data from yfinance may be delayed 15–20 minutes and is unavailable for UK stocks

---

## FILE 73: signals/pead_signal.py

### A) Purpose
PEADSignal (Post-Earnings Announcement Drift): buys day+2 after surprise >8% (with day+1 direction confirmation). Dynamic hold period: >20% surprise→30d, 10–20%→20d, 8–10%→12d. Data quality gate discards epsDifference-only events.

### B) Classes and Methods

**PEADSignal**
- `generate(ticker, earnings_data) -> Optional[dict]` — main method; applies quality gate; checks direction
- `_check_data_quality(event) -> bool` — requires BOTH epsActual AND epsEstimate
- `_compute_surprise(actual, estimate) -> float` — (actual-estimate)/|estimate|
- `_hold_period(surprise_pct) -> int` — dynamic hold: >20%→30, 10-20%→20, else→12
- `_day1_direction_check(price_data, event_date) -> bool` — checks if day+1 close > open

### C) Mathematics
```
surprise_pct = (epsActual - epsEstimate) / |epsEstimate|
threshold: 8% (surprise_threshold)
zscore_gate: 0.5 (must exceed historical zscore)

Hold periods:
  surprise > 20%: 30 days
  10% ≤ surprise ≤ 20%: 20 days
  8% ≤ surprise < 10%: 12 days
```

### D) Data Flows
- **Reads:** earnings_data dict (epsActual, epsEstimate), price_data

### E) Dependencies
- External: `pandas`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- zscore_gate requires historical surprise distribution but this is not computed per-ticker — the gate may silently pass or fail all events depending on how zscore is populated by caller
- Day+1 direction check waits until day+2 entry — means signal fires 2 days after earnings; may miss most of the drift

---

## FILE 74: signals/sector_rotation_signal.py

### A) Purpose
SectorRotationSignal: composite relative strength (0.5×4w + 0.3×12w + 0.2×26w) of 11 sector ETFs vs SPY. Detects rotation in/out. Applies PEAD modifier (±0.15). Writes to historical_db.db.

### B) Classes and Methods

**SectorRotationSignal**
- `generate(ticker, sector, date) -> dict` — fetches ETF prices; computes RS composite; generates signal
- `detect_rotation() -> dict` — finds sectors rotating in/out of top-5 vs prior period
- `_rs_composite(etf, spy) -> float` — weighted relative strength
- `_pead_modifier(sector, rotating_in, rotating_out) -> float` — ±0.15 adjustment

### C) Mathematics
```
rs_4w  = (ETF[-1] - ETF[-20])  / ETF[-20] - (SPY[-1] - SPY[-20])  / SPY[-20]
rs_12w = (ETF[-1] - ETF[-63])  / ETF[-63] - (SPY[-1] - SPY[-63])  / SPY[-63]
rs_26w = (ETF[-1] - ETF[-126]) / ETF[-126] - (SPY[-1] - SPY[-126]) / SPY[-126]

RS_composite = 0.5×rs_4w + 0.3×rs_12w + 0.2×rs_26w

PEAD modifier:
  top_5 or rotating_in  → +0.15
  bottom_5 or rotating_out → -0.15
```

### D) Data Flows
- **Reads:** yfinance (sector ETFs + SPY prices)
- **Writes:** `output/historical_db.db` → `sector_rotation` table

### E) Dependencies
- External: `yfinance`, `pandas`, `sqlite3`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `detect_rotation()` bug: `rotating_in = list(current_top5 - old_top5 & old_bot5)` — `&` binds tighter than `-`; correct expression is `(current_top5 - old_top5) & old_bot5`; this returns wrong sectors
- No caching of ETF prices — fetches fresh data on every generate() call

---

## FILE 75: signals/insider_momentum_signal.py

### A) Purpose
InsiderMomentumSignal: searches SEC EDGAR full-text search for Form 4 filings for a ticker in the last 60 days. If ≥2 filings found, generates LONG signal with score = min(count/10, 1.0).

### B) Classes and Methods

**InsiderMomentumSignal**
- `__init__(config)` — sets User-Agent header
- `get_insider_trades(ticker, days_back=90) -> list` — GET `efts.sec.gov/LATEST/search-index` with Form 4 filter
- `generate(ticker, price_data) -> list` — calls get_insider_trades(60d); if ≥2 hits → LONG signal

### C) Mathematics
```
score = min(n_filings / 10.0, 1.0)
LONG if n_filings >= 2
```

### D) Data Flows
- **Reads:** SEC EDGAR full-text search API (efts.sec.gov)

### E) Dependencies
- External: `requests`, `pandas`, `logging`, `datetime`

### F) Wiring Status
[LIVE PATH — basic]

### G) Issues Found
- Uses full-text search (`efts.sec.gov/LATEST/search-index`) rather than structured EDGAR filing API — ticker search in free text may return false positives (e.g., "AAPL" appearing in a filing text for a different company)
- No parsing of transaction type (buy vs sell) — both insider buying and selling generate LONG signals indiscriminately
- No deduplication — 2 filings by the same insider on the same day count as 2 separate signals

---

## FILE 76: intelligence/automation_scheduler.py

### A) Purpose
AutomationScheduler: 10 scheduled jobs using `schedule` library. Runs as a separate thread. Jobs include data collection, price updates, morning intelligence, UK/US scans, midday checks, EOD summary, and 6-hour retraining monitor. Weekly report job is commented out (disabled).

### B) Classes and Methods

**AutomationScheduler**
- `__init__(config)` — initialises 10 jobs
- `_setup_jobs()` — schedules all jobs; weekly report commented out
- `start() / stop()` — daemon thread lifecycle
- `_run_job(fn, name)` — wrapper with error handling and timing

**Scheduled jobs (UTC):**
- `06:00` — data_collect (subprocess: `main.py --data-collect`)
- `06:30` — update_prices
- `07:00` — morning_intelligence
- `08:15` — uk_scan
- `14:45` — us_scan
- `18:00` — midday_check
- `21:30` — eod
- `Sun 03:00` — weekly (disabled, commented out: "DISABLED: Weekly report de-duplicated — now only in trading_bot.py")
- `every 6h` — retraining_monitor

### C) Mathematics
No novel mathematics — scheduling only.

### D) Data Flows
- Calls `main.py` via subprocess for data collection
- Calls `DailyPipeline`, `PaperTrader`, `RetrainingController` directly

### E) Dependencies
- Internal: `intelligence.daily_pipeline`, `execution.paper_trader`, `core.retraining_controller`
- External: `schedule`, `threading`, `subprocess`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Weekly report disabled in this scheduler — only fires from trading_bot.py at Sunday 09:00 UTC
- Subprocess call to `main.py` for data collection introduces process boundary; failure modes not captured (exit code not checked)

---

## FILE 77: intelligence/daily_pipeline.py

### A) Purpose
DailyPipeline: orchestrates morning prep (7AM ET), close processing, weekly summary, and macro briefing. Runs readthrough signals for all universe tickers during morning. Writes macro briefing to output/macro_briefing_{date}.txt and output/macro_briefing_latest.txt.

### B) Classes and Methods

**DailyPipeline**
- `__init__(config)` — lazy initialisation; no DB connections at init
- `run_morning()` — readthrough signals for universe; pre-earnings snapshots
- `run_close()` — EOD position review, signal updates
- `run_weekly()` — weekly summary generation
- `run_macro_briefing()` — 750+ line method; reads multiple DBs directly via sqlite3.connect; generates text briefing
- `_take_pre_earnings_snapshot()` — imports `main` module dynamically; calls `m.cmd_pead_snapshot()`

### C) Mathematics
```
Consumer health thresholds (in macro briefing):
  STRONG: score > 0.5
  MODERATE: score > 0.1
  WEAK: score > -0.3
  DETERIORATING: else
```

### D) Data Flows
- **Reads:** multiple DBs directly (closeloop.db, historical_db.db, altdata.db, intelligence_db.db)
- **Writes:** `output/macro_briefing_{date}.txt`, `output/macro_briefing_latest.txt`

### E) Dependencies
- Internal: `data.*` collectors, `signals.*`, `altdata.*`, `main` (dynamic import)
- External: `sqlite3`, `logging`, `datetime`, `pathlib`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_take_pre_earnings_snapshot()` dynamically imports `main` module — tight coupling to the top-level script; fails in any context where main.py is not importable
- `run_macro_briefing()` makes 750+ lines of direct sqlite3 calls without transactions — long-running read while DB may be written concurrently

---

## FILE 78: monitoring/alert_monitor.py

### A) Purpose
AlertMonitor: per-minute checks for RAM, API outages, equity drawdown, model accuracy drift, new equation discoveries, position sizing issues. Rate-limits non-critical alerts at max 3/hour. Pauses Alpaca stream if RAM>1800MB; resumes at 1536MB.

### B) Classes and Methods

**run_all_checks(config, stream_worker)** — module-level function; calls all check functions
- RAM check: alert at 1800MB, pause stream; resume at 1536MB
- API outage: persists state to `output/api_outage_state.json`; re-alerts every 6h if still down
- Equity drawdown check
- Model accuracy drift check
- New equation ID detection

**Module-level state:** `_collector_fail_counts`, `_known_equation_ids`, `_last_model_accuracy`, `_stream_paused_for_ram`

### C) Mathematics
```
RAM alert: used_mb >= 1800
RAM resume: used_mb < 1536
Non-critical rate limit: max 3 alerts per 60-minute rolling window
API re-alert: every 6 hours if outage persists
```

### D) Data Flows
- **Reads:** /proc/meminfo (via system_stats), `output/api_outage_state.json`
- **Writes:** `output/api_outage_state.json`; Telegram alerts

### E) Dependencies
- Internal: `monitoring.system_stats`, `monitoring.telegram_logger`
- External: `threading`, `time`, `json`, `os`, `logging`

### F) Wiring Status
[LIVE PATH] — called every 60s by MonitorRunner.

### G) Issues Found
- Rate limit state (`_collector_fail_counts` etc.) is module-level global — reset on process restart; alert history lost
- Stream pause (`_stream_paused_for_ram`) persists across MonitorRunner tick cycles but not across process restarts

---

## FILE 79: monitoring/cache.py

### A) Purpose
Shared TTL cache for all monitoring and new Apollo ecosystem modules. Separate from `core/async_infrastructure.py` TTLCache — this is a synchronous version. Module-level singleton via `get_shared_cache()`.

### B) Classes and Methods

**TTLCache**
- `__init__()` — `_store: dict[str, tuple[Any, float]]` keyed by hash
- `set(key, value, ttl)` — stores with monotonic expiry time
- `get(key)` — returns value if not expired; deletes on expiry
- `delete(key)` — explicit eviction
- `clear_expired() -> int` — prunes all expired entries; returns count
- `cache_key_for(fn_name, *args, **kwargs) -> str` — namespaced key via MD5
- `get_or_set(key, value_fn, ttl) -> Any` — synchronous read-through cache
- `hash_query(context_str, question) -> str` — SHA-256 key for LLM responses

**TTL constants (seconds):** TTL_PRICE=30, TTL_NEWS=300, TTL_MACRO=1800, TTL_REGIME=600, TTL_CHART=300, TTL_LLM=600

### C) Mathematics
- No novel mathematics — MD5/SHA-256 hashing for keys; monotonic time for expiry

### D) Data Flows
- In-memory only

### E) Dependencies
- External: `time`, `hashlib`, `json`, `logging`

### F) Wiring Status
[LIVE PATH — infrastructure]

### G) Issues Found
- MD5 used for cache key generation — collision-safe for this use case (non-cryptographic), but mixing MD5 and SHA-256 (`hash_query`) is inconsistent
- No maximum size; unbounded growth possible for TTL_PRICE (30s) keys if many tickers are polled

---

## FILE 80: monitoring/rate_limiter.py

### A) Purpose
Async token-bucket rate limiter with per-API limits, exponential backoff, and sustained 5xx error detection. Separate from `core/async_infrastructure.py` — this version is for the monitoring/new-module ecosystem and includes `call_with_retry()`.

### B) Classes and Methods

**_TokenBucket** — `acquire() -> float` (seconds to wait); refills at `calls_per_minute/60` tokens/second

**RateLimiter**
- `__init__()` — creates buckets for: telegram(30/min), finnhub(60/min), newsapi(20/min), fred(30/min), simfin(10/min), alpaca(200/min), yfinance(60/min), anthropic(20/min), default(30/min)
- `acquire(api)` — async; waits if throttled
- `call_with_retry(api, coro_fn, *args, cached_value, endpoint, **kwargs)` — async; exponential backoff (2^attempt, max 60s); returns cached_value on timeout; drops after max_retries; detects sustained 5xx (>300s)

### C) Mathematics
```
tokens = min(capacity, tokens + elapsed × rate)
wait = (1.0 - tokens) / rate  [if tokens < 1]
backoff = min(2^attempt, 60)  [exponential, capped at 60s]
5xx sustained: alert if 1st error > 300s ago
```

### D) Data Flows
- In-memory only

### E) Dependencies
- External: `asyncio`, `time`, `logging`

### F) Wiring Status
[LIVE PATH — infrastructure] Note: `intelligence/reasoning_engine.py` imports `from monitoring.rate_limiter import RateLimiter` — this IS the module, confirming the import works. The session summary noted the import failed but `monitoring/rate_limiter.py` exists, so the failure in reasoning_engine.py was an earlier version issue.

### G) Issues Found
- The module exists and exports `RateLimiter` — reasoning_engine.py import should succeed (contradicts earlier finding that the module does not exist; this needs re-verification)
- `_error_timestamps` prunes entries older than 310s but the sustained failure threshold is 300s — 10s buffer is minimal

---

## FILE 81: monitoring/telegram_logger.py

### A) Purpose
Structured Telegram message logger. Thread-safe JSONL log per month at `output/telegram_history/telegram_log_YYYY-MM.json`. In-memory retry queue for failed deliveries. Never deletes logs.

### B) Classes and Methods

**Module-level functions (thread-safe via `threading.Lock`):**
- `log_message(msg_type, content, delivered) -> None` — appends JSONL record
- `queue_retry(msg_type, content) -> None` — adds to `_retry_queue` (in-memory list)
- `pop_retry_queue() -> list` — clears and returns all pending retries (atomic)

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Writes:** `output/telegram_history/telegram_log_YYYY-MM.json` (JSONL, append-only, never deleted)

### E) Dependencies
- External: `threading`, `logging`, `json`, `pathlib`, `datetime`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Retry queue is in-memory — lost on process restart; MonitorRunner only retries queued messages from the current process
- JSONL files are never rotated or deleted — grows unboundedly over months

---

## FILE 82: monitoring/system_stats.py

### A) Purpose
Low-level system stats reader using /proc files instead of psutil. Provides RAM, CPU (via load average proxy), disk space, log age. Used by self_diagnostic.py and health_reporter.py.

### B) Classes and Methods

**Module-level functions:**
- `get_ram_mb() -> (used_mb, total_mb, pct)` — reads `/proc/meminfo`; MemUsed = MemTotal - MemAvailable
- `get_cpu_pct() -> float` — reads `/proc/loadavg` 1-minute load avg; divides by n_cpus (from `/proc/cpuinfo`); multiplies by 100
- `get_disk_gb() -> (used_gb, total_gb, pct)` — `shutil.disk_usage("/")`
- `get_log_last_write(path) -> float` — seconds since file was last modified (`os.path.getmtime`)
- `is_pm2_running() -> bool` — `pgrep -x PM2` then `pgrep -f pm2`

### C) Mathematics
```
cpu_pct = (load_avg_1min / n_cpus) × 100
  [this is NOT instantaneous CPU%; it is load average normalized by CPU count]
  [load_avg > n_cpus means system is overloaded]
```

### D) Data Flows
- **Reads:** `/proc/meminfo`, `/proc/loadavg`, `/proc/cpuinfo`, filesystem metadata

### E) Dependencies
- External: `os`, `shutil`, `subprocess`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- CPU% proxy uses 1-minute load average — not true instantaneous CPU%; can significantly overestimate or underestimate actual usage
- `/proc/meminfo` MemUsed calculation uses MemAvailable (not MemFree) which is correct for Linux; this is actually a correct approach

---

## FILE 83: monitoring/milestone_tracker.py

### A) Purpose
MilestoneTracker: fires Telegram messages at equity and trade count milestones. Persistent deduplication via `output/milestones.json`. Never sends duplicates.

### B) Classes and Methods

**MilestoneTracker**
- `__init__(config)` — loads `output/milestones.json`; creates if missing
- `check(portfolio_value, trade_count)` — checks all equity and trade milestones; sends Telegram on first hit
- `_send_milestone(key, message)` — marks in JSON; sends via notifier

**Equity milestones (USD):** 101k, 105k, 110k, 115k, 125k, 150k, 175k, 200k, 250k, 300k
**Trade count milestones:** 1, 10, 25, 50, 100, 200, 500, 1000

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Reads/Writes:** `output/milestones.json`

### E) Dependencies
- Internal: `altdata.notifications.notifier.Notifier`
- External: `json`, `os`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Milestone at $101k suggests starting capital near $100k — initial capital amount is hardcoded in milestone list, not read from config
- No milestone for losses / drawdown warnings

---

## FILE 84: monitoring/monitor_runner.py

### A) Purpose
MonitorRunner: background daemon thread with 60-second heartbeat. Schedules daily health report (06:00 UTC), 6-hour self-diagnostics, per-minute alert checks, and 5-minute Telegram retry queue processing. Weekly report from this module is disabled.

### B) Classes and Methods

**MonitorRunner**
- `__init__(config, stream_worker)` — sets scheduling state
- `start() / stop() / is_alive()` — daemon thread lifecycle
- `set_stream_worker(worker)` — injects Alpaca stream worker after init
- `_run()` — main 60s loop calling `_tick()`
- `_tick()` — all scheduling logic (daily report, diagnostic, alerts, retry)
- `_process_retry_queue()` — pops retry queue; sends via Notifier; re-queues on failure; stops retrying all if 401 Unauthorized

**Module-level singleton:**
- `start_monitoring(config, stream_worker) -> MonitorRunner`
- `get_monitor_runner() -> Optional[MonitorRunner]`

### C) Mathematics
```
Quiet hours: 22:00–07:00 UTC (only CRITICAL alerts)
Daily report: 06:00 UTC (once per day)
Diagnostic: every 6 hours at minute < 2
Alert check: every 60s
Retry queue: every 5 minutes (at minutes divisible by 5)
```

### D) Data Flows
- **Reads:** telegram_logger retry queue
- **Writes:** delegates to sub-modules (health_reporter, self_diagnostic, alert_monitor, notifier)

### E) Dependencies
- Internal: `monitoring.alert_monitor`, `monitoring.telegram_logger`, `monitoring.health_reporter`, `monitoring.self_diagnostic`
- External: `threading`, `time`, `datetime`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Weekly report disabled in this module ("DISABLED: de-duplicated — now only in trading_bot.py")
- `_process_retry_queue()` on 401 Unauthorized drops ALL remaining queued messages — one bad token causes permanent message loss from the retry queue

---

## FILE 85: monitoring/self_diagnostic.py

### A) Purpose
6-hour silent self-diagnostic. Checks RAM, CPU, disk, log freshness, DB accessibility, and collector import health. Saves to `logs/diagnostics/` permanently. Only sends Telegram for failing checks (silent on pass). During quiet hours (22:00–07:00 UTC), only "All databases accessible" check is reported.

### B) Classes and Methods

**Module-level functions:**
- `run_diagnostic(config, quiet_hours=False)` — main entry point
- `_check(name, fn) -> (name, passed, detail)` — wrapper
- `_collectors_ok()` — tries importing 13 collector classes
- `_ram_ok()` — threshold 1800MB
- `_cpu_ok()` — threshold 80%
- `_disk_ok()` — threshold 80%
- `_logs_being_written()` / `_core_logs_ok()` — checks quant_fund.log and alpaca_stream.log freshness (<3600s)
- `_dbs_accessible()` — tries sqlite3.connect on 4 databases

**Checked DBs:** `output/permanent_archive.db`, `closeloop/storage/closeloop.db`, `output/historical_db.db`, `output/altdata.db`

### C) Mathematics
```
RAM limit: 1800 MB
CPU limit: 80%
Disk limit: 80%
Log stale: 3600s (1 hour)
```

### D) Data Flows
- **Reads:** /proc files (via system_stats), DB files (sqlite3 ping), log file modification times
- **Writes:** `logs/diagnostics/diagnostic_{datetime}.json` (permanent, never deleted)

### E) Dependencies
- Internal: `monitoring.system_stats`, `monitoring.telegram_logger`
- External: `importlib`, `json`, `sqlite3`, `logging`, `pathlib`, `datetime`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_CRITICAL_CHECK_NAMES = {"All databases accessible"}` — only one check is critical; all other DB/CPU/RAM/log failures are suppressed during quiet hours (22:00–07:00 UTC)
- DB check uses `output/permanent_archive.db` — inconsistent with `permanent_store.py` which writes to `output/permanent_log.db`; checks a file that may not be written to by the main permanent store

---

## FILE 86: monitoring/health_reporter.py

### A) Purpose
Generates and sends daily (07:00 UK) and weekly Telegram health reports. Checks 5 API keys (finnhub, fred, alpha_vantage, news_api, marketstack) and 13 collector class imports. Saves to `logs/daily_health_reports/` and `logs/weekly_reports/` permanently.

### B) Classes and Methods

**Module-level functions:**
- `send_daily_report(config)` — collects API health, collector health, trading stats, system stats; formats and sends Telegram
- `send_weekly_report(config)` — extended version with weekly PnL summary
- `_check_apis(config) -> dict` — live HTTP check of 5 APIs; returns {name: status}
- `_check_collectors(config) -> dict` — importlib check of 13 collectors
- `_get_trading_stats(config) -> dict` — queries DBs for recent trade activity
- `_format_daily(metrics) -> str` — Telegram-formatted text
- `_format_weekly(metrics) -> str` — Telegram-formatted text

**13 collectors checked:** shipping, consumer, geopolitical, rates, commodities, sec_fulltext, alt_quiver, tech_intel, usa_spending, bls, news, edgar, finnhub

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Reads:** 5 external API endpoints (HTTP GET); closeloop.db; /proc files (via system_stats)
- **Writes:** `logs/daily_health_reports/health_{date}.txt`, `logs/weekly_reports/weekly_{date}.txt`; Telegram

### E) Dependencies
- Internal: `monitoring.telegram_logger`, `monitoring.system_stats`
- External: `requests`, `sqlite3`, `json`, `logging`, `pathlib`, `importlib`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- API checks make live HTTP requests in the MonitorRunner thread (06:00 UTC) — if any API is slow, this blocks the monitoring thread for up to `timeout=5s × n_apis`
- alpha_vantage and marketstack keys may be absent — `MISSING_KEY` status sent in report silently

---

## FILE 87: monitoring/weekly_report.py

### A) Purpose
8-section weekly Telegram report covering PnL, positions, signals, regime, frontier, risk metrics, deepdata, and system health. Read-only DB access. START_DATE hardcoded "2026-04-03".

### B) Classes and Methods

**send_weekly_report(config)** — main module-level function; assembles 8 sections from DB queries; sends to Telegram

**Sections:**
1. Weekly PnL summary
2. Open positions
3. Top signals this week
4. Macro regime
5. Frontier intelligence snapshot
6. Risk metrics (drawdown, Sharpe)
7. DeepData highlights
8. System health

### C) Mathematics
- No novel mathematics — DB aggregations only

### D) Data Flows
- **Reads:** `output/permanent_log.db` (DB_PERMANENT), `frontier/storage/frontier.db` (DB_FRONTIER), `closeloop/storage/closeloop.db`

### E) Dependencies
- Internal: `monitoring.telegram_logger`
- External: `sqlite3`, `logging`, `datetime`

### F) Wiring Status
[LIVE PATH — called from trading_bot.py Sunday 09:00 UTC only]

### G) Issues Found
- START_DATE = "2026-04-03" hardcoded — all historical queries from this date; will show wrong stats after bot has been running >1 year without updating this constant
- Uses `output/permanent_log.db` (consistent with permanent_store.py) — this is the correct path unlike other monitoring files

---

## FILE 88: monitoring/preflight_check.py

### A) Purpose
PreFlightChecker (13:00 UTC, 90min before US open), PreMarketScanner (14:00 UTC), EndOfDayReporter (21:15 UTC). Checks 14 collector classes for health before market open.

### B) Classes and Methods

**PreFlightChecker**
- `run()` — checks 14 collectors via importlib; generates pre-flight report; Telegram alert if any fail
- **14 collectors checked:** includes all altdata + data.collectors

**PreMarketScanner**
- `run()` — scans universe for pre-market movers (price gaps, news, earnings)

**EndOfDayReporter**
- `run()` — end-of-day position summary and signal log

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Reads:** `output/permanent_archive.db` (DB path mismatch)
- **Writes:** Telegram alerts on failures

### E) Dependencies
- Internal: `altdata.*`, `data.collectors.*`
- External: `importlib`, `sqlite3`, `logging`

### F) Wiring Status
[LIVE PATH — called by AutomationScheduler]

### G) Issues Found
- Uses `output/permanent_archive.db` — mismatch with permanent_store.py's `output/permanent_log.db`
- PreFlightChecker at 13:00 UTC is 90 minutes before NYSE open (14:30 UTC) — correct for US, but UK market is already open since 08:00 UTC; UK pre-flight check has no equivalent

---

## FILE 89: monitoring/health_dashboard.py

### A) Purpose
HealthDashboard: aggregates live system metrics (phase, trade count, open positions, today PnL, active signals, pairs, regime, Kalman status) every 5 minutes. Writes to `logs/apollo_health_dashboard.log` (overwrite) and `output/dashboard.json` (overwrite). Sends daily Telegram summary at 09:00 UTC.

### B) Classes and Methods

**HealthDashboard**
- `__init__(config, store, paper_trader, regime_detector, pairs_trader, closeloop_db)` — accepts injected subsystems
- `generate() -> dict` — collects all metrics; returns dashboard dict
- `write() -> None` — calls generate(); writes log + JSON files
- `start_background(interval_seconds=300) -> None` — daemon thread; calls write() every 5 minutes
- `_get_phase_metrics()` — reads closeloop.db trade_ledger
- `_get_open_positions()` — reads from paper_trader or DB
- `_get_today_pnl()`, `_get_signal_count()`, `_get_pairs_count()`, `_get_regime()` — various DB/object reads

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Reads:** `closeloop/storage/closeloop.db`, injected objects (paper_trader, regime_detector)
- **Writes:** `logs/apollo_health_dashboard.log` (overwrite), `output/dashboard.json` (overwrite), `output/last_dashboard_telegram_date.txt`

### E) Dependencies
- Internal: paper_trader, regime_detector, pairs_trader, frontier store (all optional injections)
- External: `json`, `sqlite3`, `threading`, `shutil`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Dashboard log is overwritten every cycle — no history preserved beyond the last snapshot
- `output/dashboard.json` is overwritten (not appended) — race condition possible if web dashboard reads while write is in progress

---

## FILE 90: monitoring/external_source_monitor.py

### A) Purpose
ExternalSourceMonitor: checks 6 external sources (Alpaca, FRED, yfinance, Companies House, News API, SimFin) every 15 minutes. Disables collectors after 3 consecutive failures via FeatureManager injection. Re-enables on recovery.

### B) Classes and Methods

**_SourceStatus** — per-source: ok, failures, last_ok, last_fail, error_msg; `record_ok()`, `record_fail() -> bool` (returns True if threshold crossed)

**ExternalSourceMonitor**
- `__init__(config)` — creates 6 _SourceStatus instances; `_fail_threshold = 3`
- `set_feature_manager(fm)` — dependency injection
- `start() / stop()` — daemon thread; 15-minute check interval
- `_check_all()` — checks each source; calls fm.disable_feature() on threshold breach
- `_check_alpaca()`, `_check_fred()`, `_check_yfinance()`, `_check_companies_house()`, `_check_news_api()`, `_check_simfin()` — individual checks
- `get_status() -> dict` — returns current _SourceStatus for all sources

### C) Mathematics
```
_FAIL_THRESHOLD = 3 consecutive failures → disable collector
_DEFAULT_CHECK_INTERVAL = 900s (15 minutes)
```

### D) Data Flows
- **Reads:** HTTP endpoints (Alpaca, FRED, etc.)
- **Writes:** calls FeatureManager to disable/enable features; no DB writes

### E) Dependencies
- Internal: `execution.feature_manager.FeatureManager`
- External: `requests`, `threading`, `time`, `datetime`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- Companies House API (UK) requires an API key — if not configured, checks fail silently (exception caught, failure recorded)
- No Telegram alert sent on source disable — only FeatureManager notification; user may not know a source has been disabled

---

## FILE 91: monitoring/chart_generator.py

### A) Purpose
Generates 4 chart types (equity curve, sector breakdown, signal heatmap, frontier UMCI) as PNG files and delivers to Telegram as photo messages. Uses matplotlib Agg backend. Charts saved to /tmp, sent, then cleaned up.

### B) Classes and Methods

**Module-level functions:**
- `generate_equity_curve(config)` — reads closeloop.db trade_ledger; matplotlib line chart
- `generate_sector_breakdown(config)` — pie/bar chart of sector exposure
- `generate_signal_heatmap(config)` — signal type frequency heatmap
- `generate_frontier_chart(config)` — UMCI history from frontier.db

**Helpers:**
- `_db_path()` — tries closeloop.db; falls back to closeloop_data.db if empty (<100 bytes)
- `_safe_db_query(db_path, sql, params)` — silent-fail sqlite3 query
- `_save_chart(fig, name) -> str` — saves to /tmp with timestamp
- `_send_photo_to_telegram(photo_path, caption) -> bool` — multipart form HTTP POST; no external library (urllib.request only)

**DB paths:** closeloop.db, historical_db.db, frontier.db, deepdata.db; START_DATE = "2026-04-03"

### C) Mathematics
- No novel mathematics — matplotlib visualisation only

### D) Data Flows
- **Reads:** closeloop.db, historical_db.db, frontier.db, deepdata.db
- **Writes:** /tmp PNG files (ephemeral); Telegram (photo message)

### E) Dependencies
- External: `matplotlib`, `numpy`, `yaml`, `sqlite3`, `asyncio`, `urllib.request`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- START_DATE = "2026-04-03" hardcoded — same issue as weekly_report.py
- `_db_path()` fallback checks file size <100 bytes — may use the wrong DB if closeloop.db was just created but is empty (>100 bytes due to SQLite header)
- No cleanup of /tmp files on Telegram send failure — accumulates chart PNGs in /tmp

---

## FILE 92: monitoring/realtime_monitor.py

### A) Purpose
RealtimeMonitor: runs every 15 minutes during market hours for position checks and every 30 minutes for universe scans. Detects price spikes (3%), volume spikes (3×), sector moves (1% ETF). Generates structured Alert objects with severity (HIGH/MEDIUM/LOW/INFO).

### B) Classes and Methods

**Alert (dataclass-style)**
- Fields: event_type, severity, tickers, title, description, signal_implication (BULLISH/BEARISH/NEUTRAL), confidence, recommended_action, data

**RealtimeMonitor**
- `run()` — blocking; runs during market hours only (uses schedule library)
- `run_position_check()` — manual single position check
- `run_universe_scan()` — manual universe scan
- `_scan_positions()` — for each open position: price, volume, news, social, options checks
- `_scan_universe()` — price spikes (3%), volume spikes (3×), sector ETF moves (1%)
- `_detect_earnings()`, `_detect_insider()`, `_detect_congressional()`, `_detect_ma()`, `_detect_8k()`, `_detect_fda()` — immediate-detection event checks

### C) Mathematics
```
PRICE_SPIKE_PCT = 0.03 (3%)
VOLUME_SPIKE_X  = 3.0  (3× 20d avg)
SECTOR_MOVE_PCT = 0.01 (1%)
```

### D) Data Flows
- **Reads:** yfinance (prices, volume), SEC EDGAR, social APIs, options data
- **Writes:** `permanent_store` (via PermanentStore), logs, Telegram

### E) Dependencies
- Internal: `altdata.*`, `data.collectors.*`, `closeloop.*`
- External: `yfinance`, `requests`, `schedule`, `pandas`, `logging`

### F) Wiring Status
[LIVE PATH — but may overlap with trading_bot.py scan loops]

### G) Issues Found
- Overlaps functionally with trading_bot.py scan loops — two independent scan systems may generate duplicate alerts
- No deduplication of alerts across consecutive cycles

---

## FILE 93: monitoring/server_monitor.py

### A) Purpose
ServerMonitor: periodic server health checks (disk, RAM, Alpaca balance). Runs every 5 minutes in a daemon thread. Alert deduplication via `_alerts_sent` set (in-memory). Calls `gc.collect()` on RAM CRITICAL.

### B) Classes and Methods

**ServerMonitor**
- `__init__(config)` — reads thresholds from config.server: ram_warning_gb=1.5, ram_critical_gb=1.8, disk_warning_gb=5.0, disk_critical_gb=2.0, alpaca_balance_min_usd=5000
- `set_alpaca(api)` — injects Alpaca API after init
- `start() / stop()` — daemon thread lifecycle
- `_check_all()` — RAM, disk, Alpaca balance checks
- `_check_ram()` — shutil or /proc based
- `_check_disk()` — `shutil.disk_usage("/")`
- `_check_alpaca_balance()` — calls injected alpaca_api
- `_alert(key, message)` — sends Telegram; debounces via `_alerts_sent` set (no re-alert once sent)

### C) Mathematics
```
RAM: warning ≥ 1.5 GB, critical ≥ 1.8 GB → gc.collect()
Disk: warning ≥ 5.0 GB free space needed (inverse: alert when <5.0 GB), critical <2.0 GB
Alpaca: alert if balance < $5,000
Check interval: 300s
```

### D) Data Flows
- **Reads:** /proc/meminfo or shutil, Alpaca API
- **Writes:** Telegram alerts

### E) Dependencies
- Internal: Alpaca API (injected)
- External: `gc`, `shutil`, `os`, `threading`, `time`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- `_alerts_sent` set never cleared — once an alert is sent (e.g., low balance), it is NEVER re-sent even if condition persists or worsens; alert deduplication is too aggressive
- `gc.collect()` on RAM critical — this frees Python objects but not native allocations (e.g., numpy arrays); may have minimal effect

---

## FILE 94: monitoring/private_bot.py

### A) Purpose
Telegram inline menu bot with NLP routing and trade explanation engine. Runs as daemon thread using long-polling (getUpdates). All DB access strictly read-only. PRIVATE_CHAT_ID hardcoded "8508697534". Provides 20+ command handlers.

### B) Classes and Methods

**Module-level Telegram helpers:**
- `_tg_request(token, method, data, timeout)` — urllib.request multipart/JSON POST
- `_send_message(token, chat_id, text, parse_mode, reply_markup)` — Telegram sendMessage
- `_send_document(token, chat_id, document_path, caption)` — Telegram sendDocument

**PrivateBot**
- `__init__()` — loads config; sets chat_id filter
- `start()` / `stop()` — daemon thread
- `_poll()` — long-polling getUpdates loop
- `_dispatch(update)` — routes by /command or inline text to handlers
- `_cmd_status()` — portfolio summary
- `_cmd_positions()` — open positions from DB
- `_cmd_trades()` — recent trade history
- `_cmd_explain(ticker)` — trade explanation from trade_ledger
- `_cmd_frontier()` — frontier signal status
- `_cmd_pnl()` — PnL breakdown
- `_cmd_signals()` — active signals today
- `_cmd_report()` — triggers chart_generator
- `_cmd_help()` — command list

### C) Mathematics
- No novel mathematics — read-only DB queries and formatting

### D) Data Flows
- **Reads:** closeloop.db, historical_db.db, frontier.db, deepdata.db, intelligence_db.db (all read-only)
- **Writes:** Telegram messages/documents/photos

### E) Dependencies
- External: `asyncio`, `json`, `sqlite3`, `threading`, `urllib.request`, `yaml`, `logging`

### F) Wiring Status
[LIVE PATH]

### G) Issues Found
- PRIVATE_CHAT_ID = "8508697534" hardcoded in source — any change requires code edit
- No authentication beyond chat_id filtering — if Telegram bot token is compromised, anyone who knows the bot token can extract private financial data by impersonating the chat
- Long-polling loop has no backoff on Telegram API errors — tight loop possible on network failure

---

## FILE 95: monitoring/dashboard/app.py

### A) Purpose
Flask web dashboard. Dark-themed (Bootstrap 5, Chart.js). Session-based authentication (24h). All DB access strictly read-only. Multiple routes for portfolio, trades, signals, regime, frontier, and alerts.

### B) Classes and Methods

**Flask routes (all read-only DB access):**
- `GET /` — dashboard home with portfolio summary
- `GET /login` + `POST /login` — session auth (password from config)
- `GET /logout`
- `GET /positions` — open positions
- `GET /trades` — trade history
- `GET /signals` — signal log
- `GET /regime` — macro regime
- `GET /frontier` — frontier intelligence
- `GET /alerts` — alert history
- `GET /api/stats` — JSON endpoint for Chart.js
- `GET /api/equity_curve` — equity curve data for charting

**Helpers:** `_safe_query()`, `_safe_scalar()`, `_db_path()`, `_load_config()`

### C) Mathematics
- No novel mathematics

### D) Data Flows
- **Reads:** closeloop.db, historical_db.db, frontier.db, deepdata.db, intelligence_db.db (all read-only)
- **Writes:** session cookies

### E) Dependencies
- External: `flask`, `yaml`, `sqlite3`, `json`, `subprocess`, `logging`

### F) Wiring Status
[LIVE PATH — optional; runs on separate port]

### G) Issues Found
- START_DATE = "2026-04-03" hardcoded — all time-range queries from this date
- `_db_path()` fallback checks closeloop.db size <100 bytes — unreliable
- Flask sessions use `app.secret_key` from config; if not set, sessions are insecure
- `subprocess` imported but usage not visible in first 80 lines — may be used for shell commands; potential injection risk in read-only dashboard

---

## GROUP 11C GATE (UPDATED)

### Inventory Counts
- core/: 3 files documented (async_infrastructure, scan_scheduler, retraining_controller)
- data/: 11 files documented (earnings_cache, earnings_calendar, earnings_collector, earnings_db, earnings_scheduler, fetcher, historical_db, large_cap_influence, universe_builder, universe, delisted_universe)
- intelligence/: 3 files documented (reasoning_engine, automation_scheduler, daily_pipeline)
- signals/: 13 files documented (anomaly_scanner, filters, short_selling_filter, signal_registry, signal_validator, calendar_effects_signal, gap_signal, momentum_signal, mean_reversion_signal, options_earnings_signal, pead_signal, sector_rotation_signal, insider_momentum_signal)
- execution/: 9 files documented (alpaca_rate_limiter, chunked_scanner, feature_manager, trailing_stops, alpaca_stream, paper_trader, broker_interface, adaptive_position_sizer, cooling_off_tracker, trading_bot)
- risk/: 1 file documented (manager)
- monitoring/: 18 files documented (alert_monitor, cache, rate_limiter, telegram_logger, system_stats, milestone_tracker, monitor_runner, self_diagnostic, health_reporter, weekly_report, preflight_check, health_dashboard, external_source_monitor, chart_generator, realtime_monitor, server_monitor, private_bot, dashboard/app)
- frontier/dashboard/: 1 file documented
- frontier/equations/: 4 files documented
- frontier/financial_frontier/: 3 files documented
- frontier/meta_learning/: 4 files documented
- frontier/physical/: 6 files documented
- frontier/scientific/: 5 files documented
- frontier/social/: 6 files documented
- frontier/signals/: 1 file documented
- frontier/sizing/: 1 file documented
- frontier/storage/: 1 file documented
- frontier/validation/: 2 files documented

**Total: 95 files documented in Part11C (combined with 36 files in Part11B and 52 files in Part11A)**

### Key Findings (Cumulative — all findings from this Part)

**Runtime Failures**
1. `intelligence/reasoning_engine.py`: `from monitoring.rate_limiter import RateLimiter` — the module EXISTS at `monitoring/rate_limiter.py`; earlier finding of "module not found" was incorrect. Import should succeed.
2. `frontier/signals/frontier_signal_engine.py`: `cls(self._config)` instantiation fails for social collectors that take no config argument; caught silently; those collectors never run from the engine

**Silent Data Corruptions**
3. `data/earnings_calendar.py`: `_fallback_earnings()` maps revenue field to epsActual
4. `data/earnings_collector.py`: potential double-division of yfinance surprise_pct
5. `data/universe.py`: `_passes_uk_filters()` divides USD market cap by 100 (pence assumption)

**Logical Errors**
6. `data/earnings_scheduler.py`: weekday check uses server local time, not UK timezone
7. `data/universe_builder.py`: tier3 CSV = S&P500 tickers not already scraped — near-empty
8. `signals/signal_registry.py`: register() overwrites existing entries with only a warning
9. `frontier/validation/evidence_tracker.py`: Grade D/F boundary inconsistency
10. `frontier/validation/frontier_validator.py`: BH variable naming convention inverted
11. `execution/chunked_scanner.py`: TIER_SIZE_MULTIPLIERS differ from universe.py values
12. `signals/sector_rotation_signal.py`: `detect_rotation()` operator precedence bug — `&` binds before `-`
13. `signals/insider_momentum_signal.py`: all Form 4 filings (buys AND sells) generate LONG signals
14. `signals/options_earnings_signal.py`: iv_rank proxy not true IV rank — fixed range [15%, 65%] used
15. `execution/trading_bot.py`: holiday tables 2026-only — will trade on 2027 public holidays
16. `signals/calendar_effects_signal.py`: FOMC dates hardcoded for 2026 only
17. `monitoring/server_monitor.py`: `_alerts_sent` set never cleared — no re-alerts on persistent conditions
18. `monitoring/weekly_report.py`: START_DATE hardcoded "2026-04-03"
19. `monitoring/chart_generator.py`: START_DATE hardcoded "2026-04-03"
20. `monitoring/dashboard/app.py`: START_DATE hardcoded "2026-04-03"
21. `core/retraining_controller.py`: equity in max_drawdown starts at $100k hardcoded; actual portfolio value not used
22. `execution/cooling_off_tracker.py`: docstring says "trading days" but implementation uses calendar days

**Dead Code**
23. `data/earnings_db.py`: `update_snapshot_outcome()` — first `now_str` assignment overwritten immediately
24. `core/scan_scheduler.py`: `_scan_count` and `_last_full_cycle` never updated
25. `data/historical_db.py`: `ON CONFLICT DO UPDATE SET excluded.*` non-standard syntax
26. `core/retraining_controller.py`: `_initiate_shadow_training()` only registers a DB record — no actual training code executes

**Disconnected / Proxy Signals**
27. `frontier/physical/schumann_collector.py`: solar wind speed ≠ Schumann resonance
28. `frontier/physical/satellite_imagery_collector.py`: ETF volume ≠ satellite imagery
29. `frontier/scientific/quantum_readiness_tracker.py`: milestone_score always 0 (hardcoded strings never matched)
30. `frontier/equations/cross_signal_interactions.py`: congressional, hiring, lunar_phase_angle signals have no collectors — always 0.0
31. `frontier/equations/unified_complexity_index.py`: shipping and wikipedia signals have no collectors — always 0.0 in AltDataComplexity

**DB Path Inconsistencies**
32. `execution/adaptive_position_sizer.py` + `signals/calendar_effects_signal.py` + `monitoring/self_diagnostic.py` + `monitoring/preflight_check.py`: use `output/permanent_archive.db`; `permanent_store.py` uses `output/permanent_log.db` — these are different files
33. `monitoring/server_monitor.py`: `_alerts_sent` set never cleared — persistent conditions never re-alerted after first notification

**Performance Issues**
34. `signals/filters.py`: `sector_contagion()` fetches `yf.Ticker(ticker).info` on every call (not cached)
35. `signals/short_selling_filter.py`: 3 separate yfinance `.info` calls per validate_short()
36. `frontier/social/*.py`: 45–90 second blocking synchronous sleeps in async-capable codebase
37. `monitoring/health_reporter.py`: live HTTP API checks in MonitorRunner thread at 06:00 UTC — blocks monitoring thread
38. `signals/gap_signal.py`: sector alignment check fetches yfinance on every generate() call — not cached
39. `monitoring/private_bot.py`: no backoff on Telegram API errors in long-polling loop

**Architectural Concerns**
40. `intelligence/daily_pipeline.py`: `_take_pre_earnings_snapshot()` dynamically imports `main` module — tight coupling
41. `monitoring/realtime_monitor.py`: functionally overlaps with trading_bot.py scan loops — potential duplicate alerts
42. `monitoring/monitor_runner.py`: `_process_retry_queue()` drops ALL remaining messages on 401 Unauthorized
43. `monitoring/health_dashboard.py`: `output/dashboard.json` overwritten without atomic write — race condition with web dashboard reads

---

*END OF APOLLO_SYSTEM_MAP_PART11C.md*
