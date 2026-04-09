# APOLLO SYSTEM MAP — PART 1
# Generated: 2026-04-08
# Files documented: GROUP 1 (5 files read completely, every line)

---

## FILE 1: /home/dannyelticala/quant-fund/main.py

### A) PURPOSE
Entry point for all Apollo CLI commands; routes argparse subcommands to their handler functions, bootstraps logging, monkey-patches sqlite3 for WAL mode on every connection, enforces a single-instance PID lock when starting the bot, and contains every CLI command implementation in the same file.

---

### B) CLASSES AND METHODS

**Module-level: `_wal_connect(database, *args, **kwargs)`**
- Inputs: database path string, passthrough args/kwargs for sqlite3.connect
- Outputs: sqlite3 Connection object with WAL mode, 30s busy_timeout, NORMAL synchronous, -32000 cache_size, foreign_keys=ON
- Reads DB: none directly (patches the connect call itself)
- Writes DB: sets PRAGMAs on every new connection system-wide

**`_acquire_pid_lock() -> bool`**
- Inputs: none
- Outputs: True if lock acquired, False if another instance is alive
- Reads file: `output/apollo.pid`
- Writes file: `output/apollo.pid` (PID of current process)
- Behaviour: uses `os.kill(old_pid, 0)` to test liveness; stale PID files silently overwritten

**`load_config(path: str = "config/settings.yaml") -> dict`**
- Inputs: path string
- Outputs: dict from YAML parse
- Reads file: `config/settings.yaml`

**`setup_logging() -> None`**
- Inputs: none
- Outputs: none (side effect: configures root logger)
- Creates dirs: `logs/`, `output/`
- Creates/appends file: `logs/quant_fund.log` (RotatingFileHandler, 50MB, 3 backups)
- Suppresses: yfinance (CRITICAL), peewee (WARNING), urllib3 (WARNING)

**`_load_macro_regime(config: dict) -> int`**
- Inputs: config dict
- Outputs: int (0=RISK_ON … 4=RECESSION_RISK); default 0
- Reads DB: `output/altdata.db` via AltDataStore — queries `raw_data` for source="fred", ticker="MACRO", hours_back=168, data_type="macro_regime", field "regime_code"

**`_get_altdata_confluence(store, ticker: str) -> float`**
- Inputs: AltDataStore instance, ticker string
- Outputs: float average sentiment score or 0.0
- Reads DB: altdata sentiment table via `store.get_sentiment(ticker, hours_back=168)`

**`cmd_backtest(config, market, tickers_file, max_tickers) -> None`**
- Inputs: config dict, market str, optional tickers_file path, optional max_tickers int
- Outputs: prints report to stdout; saves CSV/JSON/MD files to `output/`
- Reads DB: altdata store (regime), earnings DB
- Reads files: universe CSV, tickers_file if provided
- Writes files: `output/backtest_{market}_equity.csv`, `output/backtest_{market}_trades.csv`, `output/backtest_{market}_monte_carlo.json`, `output/backtest_{market}_report.md`, `output/backtest_{market}_subperiods.json`
- Key logic: loads delisted universe for survivorship bias fix (US only), applies altdata confluence gate if CONFLUENCE_THRESHOLD != 0.0, calls SignalAggregator check (fail-open), runs MonteCarloSimulator, SubperiodAnalyser, benchmark comparison

**`cmd_anomaly_scan(config, market) -> None`**
- Inputs: config dict, market str
- Outputs: prints anomaly table; writes to signal registry DB
- Reads DB: universe, price data
- Writes DB: signal_registry via `registry.register()` and `registry.promote()`

**`cmd_paper_trade(config) -> None`**
- Inputs: config dict
- Outputs: runs blocking PaperTrader loop
- Note: constructs PaperTrader with legacy positional args (fetcher, generators, risk, broker) — this signature differs from the modern PaperTrader(config) call used in trading_bot.py (see ISSUES)

**`cmd_paper_trade_once(config) -> None`**
- Inputs: config dict
- Outputs: prints one-shot scan results to stdout
- Reads file: none (runs scan directly)
- Note: constructs PaperTrader with legacy signature; comment says "200-ticker sample" but code caps at limit=100

**`cmd_paper_trade_status(config) -> None`**
- Inputs: config dict
- Outputs: prints macro regime, trade counts, open positions to stdout
- Reads file: `logs/paper_trading.jsonl`
- Reads DB: closeloop_store (open positions)

**`cmd_paper_trade_history(config) -> None`**
- Inputs: config dict
- Outputs: prints last 20 closed trades to stdout
- Reads file: `logs/paper_trading.jsonl`

**`cmd_status(config) -> None`**
- Inputs: config dict
- Outputs: prints system health to stdout with health score
- Reads APIs: finnhub, fred, alpha_vantage, news_api, marketstack (live HTTP calls)
- Reads DBs: `output/altdata.db`, `output/historical_db.db`, `output/earnings.db`, `deepdata/storage/deepdata.db`, `frontier/storage/frontier.db`, `closeloop/storage/closeloop.db`, `output/permanent_log.db`
- Reads file: `output/bot_status.json` (for stream status)

**`cmd_reports(config, args) -> None`**
- Inputs: config dict, parsed args
- Outputs: prints reports to stdout
- Reads files: `logs/daily_health_reports/`, `logs/weekly_reports/`, `logs/diagnostics/`, `logs/alerts/`, `output/telegram_history/`
- Subcommands: list, show, telegram, alerts, send_daily, diagnose

**`cmd_altdata_collect(config, tickers) -> None`**
- Inputs: config dict, optional tickers list
- Outputs: prints collection summary to stdout; backs up altdata store
- Reads DB: altdata store (for store_raw)
- Writes DB: `output/altdata.db` via store.store_raw() for each result
- Collectors called: reddit, stocktwits, news, sec_edgar, companies_house, fred, shipping, jobs, wikipedia, google_trends, weather, lunar (12 total)

**`cmd_altdata_signals(config, tickers) -> None`**
- Inputs: config dict, optional tickers list
- Outputs: logs signals to logger
- Reads DB: altdata store
- Calls: AltDataSignalEngine.generate()

**`cmd_altdata_dashboard(config) -> None`**
- Inputs: config dict
- Outputs: prints dashboard to terminal; writes `output/daily_dashboard.txt`

**`cmd_altdata_nonsense(config) -> None`**
- Inputs: config dict
- Outputs: logs nonsense scores to logger
- Reads DB: altdata store anomaly candidates

**`cmd_altdata_rollback(config, to_version) -> None`**
- Inputs: config dict, version string
- Outputs: rolls back model via RollbackManager

**`cmd_altdata_status(config) -> None`**
- Inputs: config dict
- Outputs: prints pipeline status

**`_default_tickers(config) -> list`**
- Inputs: config dict
- Outputs: list of up to 50 US tickers from universe, or 10-item hardcoded fallback

**`cmd_promote(config, signal_name) -> None`**
- Inputs: config dict, signal_name str
- Outputs: promotes signal to "live" in signal registry

**`cmd_deepdata_status/collect/dashboard/options/squeeze/congress/patterns/transcript`**
- Inputs: config dict, various per-function args
- Outputs: prints results to stdout
- Read/write DBs: deepdata.db, various deepdata collectors

**`cmd_frontier_status/collect/umci/dashboard/discover/watchlist/validate/geomagnetic/attention/quantum`**
- Inputs: config dict, various per-function args
- Outputs: prints results to stdout
- Read/write DBs: `frontier/storage/frontier.db`

**`cmd_pead_snapshot(config, ticker, earnings_date) -> dict`**
- Inputs: config dict, ticker str, optional earnings_date str (YYYY-MM-DD)
- Outputs: snapshot dict; writes to earnings_db; prints formatted summary
- Reads DBs: earnings_db (upcoming calendar), altdata store (sentiment), deepdata (options, short interest, congressional), historical_db (rates, shipping)
- Writes DB: `output/earnings.db` via `db.upsert_snapshot(snapshot)`
- Reads APIs: yfinance (price, volume, calendar, sector info)
- Key maths (composite signal):
  - `smfi_norm = max(-1.0, min(1.0, (options_smfi - 1.0) / 2.0))`
  - `bq_norm = (beat_quality_multiplier - 1.0) / 0.5`
  - `composite = sum(v * w for _, v, w in signal_inputs) / total_w`
  - `congressional_signal = (buy_count - sell_count) / total`
  - `vol_surge = vol_today / vol_avg`

**`cmd_pead_observe(config, tickers) -> None`**
- Inputs: config dict, optional tickers list
- Outputs: prints summary; writes to earnings_db
- Reads DBs: earnings_db (upcoming calendar), altdata store
- Writes DB: `output/earnings.db` via `db.update_altdata_scores()`

**`cmd_earnings_collect/calendar/status`**
- Inputs: config dict, various
- Outputs: prints status to stdout
- Reads/writes: `output/earnings.db`

**`cmd_historical_collect(config, tickers, start, phases) -> None`**
- Inputs: config dict, optional tickers, start date, list of phase names
- Outputs: prints progress; writes to historical_db
- Writes DB: `output/historical_db.db` via HistoricalCollector
- Phases: prices, financials, edgar, macro, enrich, news, delisted

**`cmd_historical_status/delisted`** — read/print from historical_db

**`cmd_intelligence_run/report/score/morning/close/weekly/status/readthrough`** — delegate to DailyPipeline or IntelligenceEngine

**`cmd_search(config, query, limit, event_type) -> None`**
- Reads DB: `output/permanent_log.db` via PermanentStore FTS5

**`cmd_monitor_once/run`** — delegate to RealtimeMonitor

**`cmd_closeloop_status/dashboard/stress/weights/autopsy/tax/benchmark/wire/entry/peers/revisions`** — delegate to ClosedLoopStore and closeloop sub-modules

**`main() -> None`**
- Inputs: sys.argv via argparse
- Outputs: dispatches to cmd_* functions
- Calls `setup_logging()` first, then `load_config()`, then routes command

**`_run_bot_command(config, args) -> None`**
- Inputs: config dict, parsed args
- Handles: start, stop, pause, resume, status, cron-setup
- `start` (foreground): acquires PID lock, constructs TradingBot, calls run_continuous()
- `start --background`: launches `python3 main.py bot start` as detached subprocess via subprocess.Popen; writes PID to `output/bot.pid`; does NOT acquire PID lock in parent
- `stop`: reads `output/bot.pid`, sends SIGTERM
- `pause`/`resume`: sends SIGUSR1/SIGUSR2 to bot PID (but TradingBot has NO signal handlers for SIGUSR1/SIGUSR2 — see ISSUES)

**`_bot_signal_file(action) -> None`**
- Sends SIGUSR1 (PAUSE) or SIGUSR2 (RESUME) to bot PID

**`_show_bot_status() -> None`**
- Reads file: `output/bot_status.json`; prints to stdout

**`_setup_cron() -> None`**
- Writes file: `scripts/bot_watchdog.sh`
- Modifies crontab (or writes `scripts/quant_fund.cron` + systemd service as fallback)
- Installs: @reboot start, */5 watchdog, 0 6 universe rebuild, 30 21 EOD scan

---

### C) MATHEMATICS

All in `cmd_pead_snapshot`:
- `smfi_norm = max(-1.0, min(1.0, (options_smfi - 1.0) / 2.0))`
- `bq_norm = (beat_quality_multiplier - 1.0) / 0.5`
- `composite = sum(v * w for _, v, w in signal_inputs) / total_w`
  - Weights: altdata=0.25, options_smfi=0.20, squeeze=0.15, congress=0.15, beat_quality=0.25
- `congressional_signal = (buy_count - sell_count) / total`
- `vol_surge = vol_today / vol_avg` where vol_avg = mean of last 21 days excluding today

In `cmd_backtest` (altdata confluence gate):
- `if confluence * direction < CONFLUENCE_THRESHOLD: block signal`

Health score in `cmd_status`:
- `total_score = min(100, api_health_score + min(30, db_health_score) + min(10, collector_health // 10))`
- `api_health_score`: +20 per working key, +10 per rate-limited key (max 5 keys = 100)

---

### D) DATA FLOWS

**INTO main.py:**
- `config/settings.yaml` — loaded at startup for all commands
- `sys.argv` — CLI arguments
- `output/bot_status.json` — read by `_show_bot_status()` and `cmd_status()`
- `logs/paper_trading.jsonl` — read by status/history commands
- Live API responses — finnhub, fred, alpha_vantage, news_api, marketstack (in cmd_status)
- All databases (read for status/report commands)

**OUT OF main.py:**
- `output/apollo.pid` — written by `_acquire_pid_lock()`
- `output/bot.pid` — written by background start and `_setup_cron()`
- `scripts/bot_watchdog.sh` — written by `_setup_cron()`
- `scripts/quant_fund.cron` — written by `_setup_cron()` fallback
- `~/.config/systemd/user/quant-fund-bot.service` — written by `_setup_cron()` fallback
- `logs/quant_fund.log` — logging output
- `output/backtest_*` files — written by `cmd_backtest()`
- `output/earnings.db` — written by `cmd_pead_snapshot()` and `cmd_pead_observe()`
- `output/altdata.db` — written by `cmd_altdata_collect()`
- `output/historical_db.db` — written by `cmd_historical_collect()`

**DB tables read:**
- `output/altdata.db`: raw_data, sentiment_scores, anomaly_candidates, model_registry
- `output/historical_db.db`: shipping_data, rates_data, macro_series
- `output/earnings.db`: earnings_observations, upcoming_calendar, pre_earnings_snapshots
- `closeloop/storage/closeloop.db`: trade_ledger, signal_weights
- `output/permanent_log.db`: FTS5 event log

---

### E) DEPENDENCIES

**Apollo-internal imports:**
- `data.fetcher`, `data.cleaner`, `data.earnings_calendar`, `data.universe`, `data.historical_collector`, `data.historical_db`, `data.earnings_db`, `data.earnings_collector`, `data.large_cap_influence`, `data.delisted_universe`
- `signals.pead_signal`, `signals.filters`, `signals.anomaly_scanner`, `signals.signal_validator`, `signals.signal_registry`
- `backtest.engine`, `backtest.monte_carlo`, `backtest.subperiod_analysis`
- `reporting.analytics`
- `risk.manager`
- `execution.broker_interface`, `execution.paper_trader`
- `altdata.storage.altdata_store`, `altdata.notifications.notifier`, `altdata.storage.permanent_store`
- `altdata.collector.*` (12 collectors)
- `altdata.signals.altdata_signal_engine`
- `altdata.anomaly.nonsense_detector`, `altdata.learning.rollback_manager`
- `altdata.dashboard.altdata_dashboard`
- `deepdata.storage.deepdata_store`, `deepdata.options.flow_monitor`, `deepdata.short_interest.*`, `deepdata.congressional.*`, `deepdata.transcripts.*`, `deepdata.patterns.*`, `deepdata.earnings_quality.beat_quality_classifier`
- `frontier.storage.frontier_store`, `frontier.equations.*`, `frontier.sizing.*`, `frontier.validation.*`, `frontier.meta_learning.*`, `frontier.physical.*`, `frontier.social.*`, `frontier.scientific.*`, `frontier.financial_frontier.*`, `frontier.dashboard.*`, `frontier.signals.frontier_signal_engine`
- `intelligence.daily_pipeline`, `intelligence.automation_scheduler`
- `analysis.intelligence_db`, `analysis.intelligence_engine`, `analysis.macro_signal_engine`, `analysis.symbolic_regression`
- `closeloop.storage.closeloop_store`, `closeloop.learning.batch_retrainer`, `closeloop.integration.signal_aggregator`, `closeloop.dashboard.*`, `closeloop.stress.*`, `closeloop.risk.*`, `closeloop.context.*`, `closeloop.entry.*`
- `monitoring.realtime_monitor`, `monitoring.health_reporter`, `monitoring.self_diagnostic`, `monitoring.telegram_logger`, `monitoring.weekly_report`
- `data.universe_builder` (via cron script string)
- `data.earnings_scheduler`
- `execution.trading_bot`

**External libraries:**
- `argparse`, `logging`, `os`, `sys`, `sqlite3`, `pathlib`, `yaml`, `json`, `subprocess`, `signal`, `time`, `datetime`, `tempfile`, `glob`, `importlib`, `traceback`, `shutil`
- `pandas` (in cmd_backtest, cmd_status)
- `yfinance` (in cmd_pead_snapshot, cmd_deepdata_squeeze, cmd_status)
- `requests` (in cmd_status API health checks)

---

### F) WIRING STATUS

main.py is the sole entry point. When `bot start` is invoked:
1. Acquires PID lock (`output/apollo.pid`)
2. Imports and constructs `TradingBot` from `execution/trading_bot.py`
3. Calls `TradingBot.run_continuous()` which is the live trading loop
4. All live scanning, data collection, morning briefings, and monitoring flow through TradingBot

main.py is therefore fully connected to the live trading path via the `bot start` command path. All other commands are CLI utilities and do not affect the running bot.

---

### G) ISSUES FOUND

1. **PAUSE/RESUME signals are dead:** `_bot_signal_file('PAUSE')` sends SIGUSR1 and `_bot_signal_file('RESUME')` sends SIGUSR2 to the bot process. TradingBot in `execution/trading_bot.py` has NO signal handlers registered for SIGUSR1 or SIGUSR2. Only SIGTERM is handled. Pause/resume via CLI will silently fail (SIGUSR1/SIGUSR2 default action on Linux is to terminate the process — this would kill the bot).

2. **Background start does not acquire PID lock:** When `bot start --background` is invoked, a new subprocess is spawned with `subprocess.Popen`. The parent writes the child PID to `output/bot.pid` but does NOT call `_acquire_pid_lock()`. The child process (when it runs `bot start` without `--background`) will call `_acquire_pid_lock()` — but by then the parent has already written to `output/bot.pid`, so the child will read its own PID, find the process alive, and abort with "Apollo is already running." This creates a race condition. Actually on re-reading: the child runs `main.py bot start` (without --background), which calls `_acquire_pid_lock()`. The parent wrote the child PID to `output/bot.pid` but `_PID_LOCK_FILE` is `output/apollo.pid` not `output/bot.pid`. There are TWO PID files: `output/apollo.pid` (lock) and `output/bot.pid` (stop target). The background path only writes to `output/bot.pid` and not to `output/apollo.pid`, so the child's `_acquire_pid_lock()` reads `output/apollo.pid` which doesn't exist yet and succeeds. This is consistent but creates confusion between the two PID files.

3. **`cmd_paper_trade` and `cmd_paper_trade_once` use legacy PaperTrader signature:** Both construct PaperTrader with positional arguments `(config, fetcher, generators, risk, broker)`. The bot's live path calls `PaperTrader(config)` with only a config dict. If PaperTrader's `__init__` only accepts `(self, config)`, these CLI commands will fail at runtime.

4. **Comment/code mismatch in `cmd_paper_trade_once`:** The print statement says "200-ticker sample" but code passes `limit=100`.

5. **`cmd_earnings_collect` references undefined `log`:** At line 2609, `log.info(...)` is called but `log` is not defined in `cmd_earnings_collect`'s local scope. The module-level `logger` is not aliased to `log` there. This will raise `NameError: name 'log' is not defined` at runtime.

6. **`_setup_cron` EOD cron job calls deprecated interface:** The cron entry calls `python3 main.py paper_trade --once` but the `--once` flag is parsed only if `paper_trade_command` is not set. This should work correctly per the parser logic.

7. **`cmd_validate` imports `pd` in frontier equations smoke test but `pd` is not imported at module level** — it is imported locally inside the try block, which is fine, but numpy is also imported locally. No issue, just noted.

---

---

## FILE 2: /home/dannyelticala/quant-fund/execution/trading_bot.py

### A) PURPOSE
The master continuous trading loop controller that coordinates UK/US market scans, data collection, morning briefings, position checks, and a suite of scheduled background tasks; also initialises all collector subsystems, monitoring threads, the Alpaca stream, the dashboard, and the private Telegram bot at startup.

---

### B) CLASSES AND METHODS

**Module-level constants:**
- `_US_HOLIDAYS_2026`: set of date objects (10 NYSE holidays for 2026)
- `_UK_HOLIDAYS_2026`: set of date objects (8 LSE holidays for 2026)
- `_MARKET_HOLIDAYS`: dict mapping 'UK' and 'US' to their holiday sets

**Class `TradingBot`**

Class-level constants:
- `MARKET_SCHEDULE`: dict with 'UK' and 'US' sub-dicts containing open_hour_gmt, open_minute, close_hour_gmt, close_minute, scan_times_gmt list, currency, ticker_suffix
  - UK: 08:00–16:30 GMT; scans at 08:15, 09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00, 16:15
  - US: 14:30–21:00 GMT; scans at 14:45, 15:30, 16:00, 17:00, 18:00, 19:00, 20:00, 20:45
- `DATA_COLLECTION_INTERVAL_MINUTES = 30`

**`__init__(self, config: dict)`**
- Inputs: config dict
- Outputs: initialised TradingBot instance
- Creates dirs: `output/`, `logs/`
- Instantiates: PaperTrader(config), DailyPipeline(config) (try/except), AlpacaStream (try/except), MonitorRunner (try/except), PrivateBot (try/except), Dashboard thread (try/except)
- Calls: `_init_collectors()`, `_save_status('INITIALISED')`
- Writes file: `output/bot_status.json`
- State vars initialised: last_uk_scan, last_us_scan, last_data_collection, last_morning_briefing, last_sec_fulltext_date, last_eod_sim_date, last_price_refresh_date, scan_count=0, errors_today=0, articles_fetched_today=0, signals_tickers_today=[], _scan_fired_this_minute={}

**`_init_collectors(self) -> None`**
- Inputs: none (uses self.config)
- Outputs: populates self.collectors dict
- Collectors attempted (15 total):
  - shipping: ShippingIntelligence(config) — `data.collectors.shipping_intelligence`
  - consumer: ConsumerIntelligence(config) — `data.collectors.consumer_intelligence`
  - geopolitical: GeopoliticalCollector(config) — `data.collectors.geopolitical_collector`
  - rates: RatesCreditCollector('config/settings.yaml') — `data.collectors.rates_credit_collector` — NOTE: receives config path string, not config dict
  - commodities: CommodityCollector(config) — `data.collectors.commodity_collector`
  - sec_fulltext: SECFullTextCollector() — `data.collectors.sec_fulltext_collector` — no args
  - alt_quiver: AlternativeQuiverCollector(config) — `data.collectors.alternative_quiver_collector`
  - tech_intel: TechnologyIntelligence(config) — `data.collectors.technology_intelligence`
  - usa_spending: USASpendingCollector(config) — `data.collectors.government_data_collector`
  - bls: BLSCollector(config) — `data.collectors.government_data_collector`
  - insider_txn: InsiderTransactionCollector() — `data.collectors.insider_transaction_collector` — no args
  - job_postings: JobPostingsCollector(config) — `data.collectors.job_postings_collector`
  - news: NewsCollector(config) — `altdata.collector.news_collector`
  - edgar: SECEdgarCollector(config) — `altdata.collector.sec_edgar_collector`
  - finnhub: FinnhubCollector(config) — `altdata.collector.finnhub_collector`
- After loading, calls `check_for_delisted()` from `data.collectors.technology_intelligence` for 5 watchlists
- Logs count: "{n}/{15} collectors loaded"

**`_save_status(self, status: str, extra: Dict = None) -> None`**
- Inputs: status string, optional extra dict
- Outputs: none (side effect)
- Reads: `self.trader.sizer.get_phase_summary()`, `get_stream_cache().stats()`, `self.is_market_open('us')`, `self.is_market_open('uk')`
- Writes file: `output/bot_status.json` — JSON with status, timestamp, running, paused, scan_count, errors_today, articles_fetched_today, last_uk_scan, last_us_scan, last_data_collection, phase, use_alpaca, stream stats, us_market_open, uk_market_open, market_status string

**`_save_pid(self) -> None`**
- Inputs: none
- Writes file: `output/bot.pid` with `str(os.getpid())`

**`is_market_open(self, market: str) -> bool`**
- Inputs: market string ('UK' or 'US', case-sensitive in MARKET_SCHEDULE lookup but 'us'/'uk' also passed in run_continuous via 'uk'/'UK')
- Outputs: bool
- Logic: checks UTC weekday (False if weekend), checks holiday table, checks if now within [open_t, close_t] inclusive
- NOTE: market parameter is passed as 'UK' and 'US' from run_continuous, but MARKET_SCHEDULE keys are 'UK' and 'US'. Holiday lookup uses `_MARKET_HOLIDAYS.get(market, set())` — the keys in _MARKET_HOLIDAYS are 'UK' and 'US'. This works correctly.

**`is_scan_time(self, market: str) -> bool`**
- Inputs: market string ('UK' or 'US')
- Outputs: bool
- Logic: for each scheduled scan time, checks if abs(now - target) <= 90 seconds; deduplicates via `_scan_fired_this_minute` dict keyed by "MARKET:HH:MM" → today's date string
- NOTE: market parameter is checked against MARKET_SCHEDULE — works with 'UK'/'US' only

**`should_run_morning_briefing(self) -> bool`**
- Inputs: none
- Outputs: bool
- Logic: True if UTC hour == 6 and minute < 5 and (last_morning_briefing is None or >= 1 day ago)

**`should_collect_data(self) -> bool`**
- Inputs: none
- Outputs: bool
- Logic: True if last_data_collection is None; else True if elapsed minutes >= DATA_COLLECTION_INTERVAL_MINUTES (30)
- NOTE: uses `datetime.now()` (local) to compute elapsed but loop uses `datetime.utcnow()` — inconsistency if server is not in UTC (see ISSUES)

**`run_data_collection(self) -> Dict`**
- Inputs: none (uses self.collectors, self.config, self.signals_tickers_today)
- Outputs: dict mapping collector name → record count or error string
- Per-collector dispatch logic:
  - sec_fulltext: `collector.run_full_daily_scan()` — at most once per calendar day (guarded by `last_sec_fulltext_date`)
  - finnhub: `collector.collect(_sample, 'us')` where _sample is last 20 distinct tickers from `trade_ledger` ordered by id DESC — reads closeloop.db
  - news, edgar (ticker-based): skipped in bulk; handled per-ticker in `_fetch_articles_for_tickers`
  - tech_intel: `collector.collect_all()`
  - bls: `collector.collect_all_series()`
  - usa_spending: `collector.get_recent_all_awards()` then `collector.store_awards(result)`
  - alt_quiver: `collector.get_senate_trades(days_back=30)` + `collector.get_house_trades(days_back=30)`
  - insider_txn: `collector.collect(days_back=7, max_filings=100)`
  - job_postings: `collector.collect(ticker_map)` where ticker_map is open non-phantom positions from trade_ledger — reads closeloop.db
  - default: `collector.collect(market='us')` or `collector.run()`
- After collectors: calls `_fetch_articles_for_tickers(priority_tickers)` for signals_tickers_today (capped at 100, then 50 in inner loop)
- After articles: calls FrontierSignalEngine.get_umci_snapshot() — instantiates FrontierStore and FrontierSignalEngine each cycle
- On error: calls `check_collector_failures(config, name, error)` from monitoring.alert_monitor
- On success: calls `reset_collector_ok(name)` from monitoring.alert_monitor
- Reads DB (for finnhub): `closeloop/storage/closeloop.db` — `trade_ledger` table (last 20 by id)
- Reads DB (for job_postings): `closeloop/storage/closeloop.db` — `trade_ledger` WHERE exit_date IS NULL AND is_phantom=0 LIMIT 50
- Writes: updates `self.last_data_collection`, `self.articles_fetched_today`, `self.last_sec_fulltext_date`

**`_fetch_articles_for_tickers(self, tickers: List[str]) -> int`**
- Inputs: list of tickers (capped to 50 internally)
- Outputs: int count of articles fetched
- Calls: news_col.collect([ticker], 'us'), finnhub_col.collect([ticker], 'us'), edgar_col.collect([ticker], 'us')
- All errors silently swallowed (bare except: pass)

**`run_morning_briefing(self) -> None`**
- Inputs: none
- Outputs: none (side effect: prints briefing, updates last_morning_briefing)
- Calls: `self.pipeline.run_macro_briefing()` if pipeline is not None
- Updates: `self.last_morning_briefing = datetime.now()`

**`run_market_scan(self, market: str) -> Dict`**
- Inputs: market string ('UK' or 'US')
- Outputs: dict with positions_opened, signal_tickers, total_actions
- If paused: returns {} immediately
- Calls: `self.trader.run_scan(market=market.lower())`
- Updates: scan_count, last_uk_scan or last_us_scan, signals_tickers_today (capped at 500)
- Calls: `self.trader.sizer.auto_trigger_discovery()` (silently ignores errors)
- Calls: `self._save_status('RUNNING', {...})`

**`_check_open_positions(self) -> None`**
- Inputs: none
- Outputs: none (side effect: calls check_exit_conditions)
- Logic: gets positions from closeloop or broker, gets equity, calls `self.trader.check_exit_conditions(positions, equity)`
- Default equity: 100000 if no closeloop

**`run_continuous(self) -> None`**
- Inputs: none
- Outputs: none (blocking loop until self.running = False)
- On startup: calls `self.trader.reconcile_positions()`, sends Telegram notification if positions reconciled
- Registers SIGTERM handler to set self.running = False
- Main loop (wakes every 10s, acts once per minute):
  - 06:00 UTC: `run_morning_briefing()` if `should_run_morning_briefing()`
  - Every iteration: `_preflight_tick(config, now, trader)` from monitoring.preflight_check
  - When UK open + scan time: `run_market_scan('UK')`
  - When US open + scan time: `run_market_scan('US')`
  - Every 30 min: `run_data_collection()` if `should_collect_data()`
  - Every 15 min (minute % 15 == 0): `_check_open_positions()`
  - Sunday 09:00 UTC: WeeklyReportGenerator().send_weekly_report()
  - After 21:00 UTC, once per day: daily price refresh (HistoricalCollector, reads open positions from closeloop.db)
  - After 21:30 UTC, once per day: `run_daily_simulation()` from simulations.sim_scheduler
  - Every 6h at :00 (hour % 6 == 0, minute == 0): SelfDiagnostic(config).run()
  - Every 6h at :01 (hour % 6 == 0, minute == 1): RetrainingController().run_monitoring_cycle()
  - Sunday 03:00 UTC: BatchRetrainer(config).run()
  - Midnight (00:00): reset errors_today, articles_fetched_today, signals_tickers_today
  - On KeyboardInterrupt: clean stop
  - On Exception: log error, sleep 30s, continue
- After loop: `_save_status('STOPPED')`

**`pause(self) -> None`** — sets self.paused=True, saves status 'PAUSED'
**`resume(self) -> None`** — sets self.paused=False, saves status 'RUNNING'
**`stop(self) -> None`** — sets self.running=False, saves status 'STOPPED'

---

### C) MATHEMATICS

In `is_market_open`:
- `open_t <= now <= close_t` (inclusive range check on datetime objects)

In `is_scan_time`:
- `abs((now - target).total_seconds()) <= 90` — 90-second window for scan trigger deduplication

In `should_collect_data`:
- `elapsed = (datetime.now() - self.last_data_collection).total_seconds() / 60`
- `return elapsed >= 30` (DATA_COLLECTION_INTERVAL_MINUTES)

In `should_run_morning_briefing`:
- `(now - self.last_morning_briefing).days >= 1` — day-level deduplication

In `_save_status` (health score display not calculated here, just formatted)

---

### D) DATA FLOWS

**INTO trading_bot.py:**
- config dict (from main.py)
- Market open/close schedules and holidays (hardcoded in file)
- Trade results from `self.trader.run_scan()`
- Position data from closeloop or broker
- `closeloop/storage/closeloop.db` — read by run_data_collection (finnhub sample, job_postings tickers) and run_continuous (price refresh tickers)

**OUT OF trading_bot.py:**
- `output/bot_status.json` — written every scan and at status changes
- `output/bot.pid` — written by `_save_pid()` at start of run_continuous
- Delegates all data writes to sub-modules (collectors, pipeline, etc.)

**DB reads:**
- `closeloop/storage/closeloop.db`: `trade_ledger` (SELECT DISTINCT ticker ORDER BY id DESC LIMIT 20 for finnhub; WHERE exit_date IS NULL AND is_phantom=0 LIMIT 50 for job_postings; WHERE exit_date IS NULL AND is_phantom=0 AND order_status!='superseded' LIMIT 100 for price refresh)

**DB writes (indirect):**
- Via collectors, pipeline, trader — not directly in this file

---

### E) DEPENDENCIES

**Apollo-internal imports (in __init__ and methods):**
- `execution.paper_trader.PaperTrader`
- `intelligence.daily_pipeline.DailyPipeline`
- `execution.alpaca_stream.start_stream`, `get_stream_cache`
- `data.universe.UniverseManager`
- `data.fetcher.DataFetcher`
- `monitoring.monitor_runner.start_monitoring`
- `monitoring.private_bot.start_private_bot`
- `monitoring.dashboard.app.start_dashboard`
- `monitoring.alert_monitor.check_collector_failures`, `reset_collector_ok`
- `monitoring.preflight_check.tick`
- `monitoring.weekly_report.WeeklyReportGenerator`
- `monitoring.self_diagnostic.SelfDiagnostic`
- `altdata.notifications.notifier.Notifier`
- `data.collectors.shipping_intelligence.ShippingIntelligence`, `check_for_delisted`
- `data.collectors.consumer_intelligence.ConsumerIntelligence`
- `data.collectors.geopolitical_collector.GeopoliticalCollector`
- `data.collectors.rates_credit_collector.RatesCreditCollector`
- `data.collectors.commodity_collector.CommodityCollector`
- `data.collectors.sec_fulltext_collector.SECFullTextCollector`
- `data.collectors.alternative_quiver_collector.AlternativeQuiverCollector`
- `data.collectors.technology_intelligence.TechnologyIntelligence`
- `data.collectors.government_data_collector.USASpendingCollector`, `BLSCollector`
- `data.collectors.insider_transaction_collector.InsiderTransactionCollector`
- `data.collectors.job_postings_collector.JobPostingsCollector`
- `altdata.collector.news_collector.NewsCollector`
- `altdata.collector.sec_edgar_collector.SECEdgarCollector`
- `altdata.collector.finnhub_collector.FinnhubCollector`
- `frontier.storage.frontier_store.FrontierStore`
- `frontier.signals.frontier_signal_engine.FrontierSignalEngine`
- `data.historical_collector.HistoricalCollector`
- `simulations.sim_scheduler.run_daily_simulation`
- `core.retraining_controller.RetrainingController`
- `closeloop.learning.batch_retrainer.BatchRetrainer`

**External libraries:**
- `json`, `logging`, `os`, `signal`, `time`, `datetime`, `pathlib`, `typing`
- `sqlite3` (imported inline for closeloop.db queries)
- `importlib` (for dynamic collector loading)
- `traceback` (in run_market_scan exception handler)
- `threading` (for dashboard thread)

---

### F) WIRING STATUS

TradingBot is the live trading loop. It is directly connected to:
- PaperTrader (via self.trader) — executes all trades
- DailyPipeline — morning briefings
- All 15 data collectors — data collection cycle
- AlpacaStream — real-time price feed (if available)
- MonitorRunner — self-monitoring
- RetrainingController — model health monitoring (every 6h)
- BatchRetrainer — weekly deep analysis
- SimScheduler — EOD simulation

This file is the live trading path core.

---

### G) ISSUES FOUND

1. **`is_market_open` called with lowercase 'us'/'uk' in run_continuous but also 'UK'/'US':** `run_continuous` calls `is_market_open('UK')` and `is_market_open('US')` (uppercase), and `_save_status` calls `is_market_open('us')` and `is_market_open('uk')` (lowercase). The method does `self.MARKET_SCHEDULE[market]` — `MARKET_SCHEDULE` keys are 'UK' and 'US' (uppercase). Lowercase calls will raise `KeyError: 'us'` every time `_save_status` is called. This is a silent failure only because the exception is caught by the outer try/except in `_save_status`.

2. **`should_collect_data` uses `datetime.now()` (local time) but the main loop uses `datetime.utcnow()`:** If the VPS timezone is set to anything other than UTC, the 30-minute interval calculation will still work correctly (it's a delta), but the inconsistency is a latent bug if timestamp comparisons with UTC values are ever introduced.

3. **FrontierStore and FrontierSignalEngine instantiated fresh every 30-minute data collection cycle:** In `run_data_collection()`, `FrontierStore(self.config)` and `FrontierSignalEngine(...)` are created on every call. If these open DB connections, this leaks connections every 30 minutes.

4. **Dashboard started as daemon thread with no error recovery:** `threading.Thread(target=start_dashboard, daemon=True, name="apollo-dashboard").start()` — if the dashboard crashes, it silently disappears with no restart or logging beyond the initial `__init__` warning.

5. **No SIGUSR1/SIGUSR2 handlers registered:** main.py's `_bot_signal_file('PAUSE')` sends SIGUSR1 to the bot process but TradingBot only registers SIGTERM. On Linux, SIGUSR1 default action is to terminate the process. Sending PAUSE will kill the bot.

6. **`_check_open_positions` fallback equity hardcoded at 100000:** If closeloop is unavailable, `equity = 100000` regardless of actual capital. `check_exit_conditions` will receive a wrong equity value.

7. **`self.last_sec_fulltext_date` guard uses `datetime.now().strftime('%Y-%m-%d')` (local time):** If the server timezone differs from UTC, the daily guard may fire at different times than expected.

8. **Weekly report is checked TWICE:** `run_continuous` checks `weekday() == 6 and hour == 9 and minute == 0` for WeeklyReportGenerator. AutomationScheduler also had `job_weekly_report` at 09:00 but it is now commented out. If AutomationScheduler were run separately, the report would only duplicate once — but the comment says it was de-duplicated to here only. This is resolved but the comment in automation_scheduler.py references trading_bot.py as the sole source.

9. **Holiday tables only cover 2026:** `_US_HOLIDAYS_2026` and `_UK_HOLIDAYS_2026` are hardcoded for 2026. When 2027 begins, no holidays will be recognised and the bot will trade on public holidays.

---

---

## FILE 3: /home/dannyelticala/quant-fund/intelligence/automation_scheduler.py

### A) PURPOSE
A standalone `schedule`-library-based daily automation scheduler that runs all data collection, market scans, and EOD jobs on a fixed UTC timetable; intended to be run as a separate process from the main bot, but overlaps significantly with TradingBot's inline scheduling logic.

---

### B) CLASSES AND METHODS

**Module-level:**
- `_ROOT = Path(__file__).resolve().parents[1]` — project root
- `_load_config() -> dict` — loads `config/settings.yaml` relative to _ROOT

**`job_collect_data() -> None`**
- Inputs: none
- Outputs: runs subprocess, blocks up to 1800s
- What it does: runs `python3 main.py altdata collect` as subprocess in _ROOT
- Reads DB: none directly (subprocess does)
- Writes DB: none directly

**`job_update_prices() -> None`**
- Inputs: none
- Outputs: runs subprocess, blocks up to 1800s
- What it does: runs `python3 main.py historical collect --phases prices --start {5_days_ago}` as subprocess

**`job_morning_intelligence() -> None`**
- Inputs: none
- Outputs: runs DailyPipeline.run_morning() in-process
- Calls: `DailyPipeline(config).run_morning()`
- Reads/writes: earnings_db, intelligence_db (via DailyPipeline)

**`job_uk_scan() -> None`**
- Inputs: none
- Outputs: runs PaperTrader UK scan
- Calls: `PaperTrader(config).run_scan(market='uk')`
- NOTE: Creates a fresh PaperTrader instance with no existing state. Does not reuse any running bot state.

**`job_us_scan() -> None`**
- Inputs: none
- Outputs: runs PaperTrader US scan
- Calls: `PaperTrader(config).run_scan(market='us')`
- Same note as job_uk_scan

**`job_midday_check() -> None`**
- Inputs: none
- Outputs: runs PaperTrader open position check
- Calls: `PaperTrader(config).check_open_positions()`
- NOTE: PaperTrader does not have a `check_open_positions()` method in trading_bot.py's init — this is called on a fresh PaperTrader instance. See ISSUES.

**`job_eod() -> None`**
- Inputs: none
- Outputs: runs PaperTrader EOD, then daily simulation
- Calls: `PaperTrader(config).run_eod()`, then `run_daily_simulation()`

**`job_weekly() -> None`**
- Inputs: none
- Outputs: runs BatchRetrainer if Sunday
- Guard: checks `datetime.utcnow().weekday() != 6` — returns immediately if not Sunday
- Calls: `BatchRetrainer(config).run()`

**`job_retraining_monitor() -> None`**
- Inputs: none
- Outputs: runs RetrainingController monitoring cycle
- Calls: `RetrainingController().run_monitoring_cycle()`

**`job_weekly_report() -> None`**
- Inputs: none
- Outputs: (DISABLED — not scheduled; function exists but not registered)
- Guard: `datetime.utcnow().weekday() != 6` check
- Calls: `WeeklyReportGenerator().send_weekly_report()`

**Class `AutomationScheduler`**

**`__init__(self)`**
- Sets `self._running = False`

**`setup(self) -> None`**
- Registers schedule jobs:
  - 06:00 → `job_collect_data`
  - 06:30 → `job_update_prices`
  - 07:00 → `job_morning_intelligence`
  - 08:15 → `job_uk_scan`
  - 14:45 → `job_us_scan`
  - 18:00 → `job_midday_check`
  - 21:30 → `job_eod`
  - 03:00 → `job_weekly` (runs every day, but guard inside checks weekday)
  - DISABLED: `job_weekly_report` at 09:00 (commented out)
  - every 6 hours → `job_retraining_monitor`
- Logs: "Automation scheduler configured with 10 jobs" — but only 9 jobs are actually registered (weekly_report is disabled); the count comment is wrong

**`run(self) -> None`**
- Calls `setup()`, sets `_running=True`
- Registers SIGINT and SIGTERM handlers to set `_running=False`
- Polls `schedule.run_pending()` every 30 seconds

**`status(self) -> str`**
- Calls `setup()` (re-registers all jobs into global schedule), returns formatted string of next run times

---

### C) MATHEMATICS

- `start = (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")` in `job_update_prices` — 5-day lookback window

---

### D) DATA FLOWS

**INTO automation_scheduler.py:**
- `config/settings.yaml` — loaded by `_load_config()` for morning intelligence, uk/us scans, EOD, weekly jobs
- Schedule library triggers

**OUT OF automation_scheduler.py:**
- Subprocess calls: `main.py altdata collect`, `main.py historical collect` — these write to altdata.db, historical_db.db
- In-process calls delegate to DailyPipeline, PaperTrader, BatchRetrainer, RetrainingController

**DB reads/writes:** indirect via sub-modules

---

### E) DEPENDENCIES

**Apollo-internal:**
- `intelligence.daily_pipeline.DailyPipeline`
- `execution.paper_trader.PaperTrader`
- `simulations.sim_scheduler.run_daily_simulation`
- `closeloop.learning.batch_retrainer.BatchRetrainer`
- `core.retraining_controller.RetrainingController`
- `monitoring.weekly_report.WeeklyReportGenerator`

**External:**
- `logging`, `signal`, `sys`, `time`, `datetime`, `pathlib`
- `schedule` (third-party)
- `yaml`

---

### F) WIRING STATUS

AutomationScheduler is PARTIALLY connected. It is a standalone process invoked via `python3 main.py schedule start` or `python3 intelligence/automation_scheduler.py`. It is NOT started by TradingBot or main.py's bot path. If both TradingBot and AutomationScheduler are running simultaneously, they will both execute scans, EOD runs, and retraining monitors independently — creating duplicate work and potential DB contention.

In practice, AutomationScheduler appears to be a legacy/alternative scheduler that predates TradingBot's inline scheduling. TradingBot now handles all the same jobs internally.

---

### G) ISSUES FOUND

1. **Log message claims 10 jobs but only 9 are registered:** `logger.info("Automation scheduler configured with 10 jobs")` — `job_weekly_report` is commented out, leaving 9 registered jobs.

2. **`job_midday_check` calls `PaperTrader.check_open_positions()`:** This method does not exist on PaperTrader — TradingBot calls `self.trader.check_exit_conditions(positions, equity)` (not `check_open_positions`). PaperTrader's public API would need to expose `check_open_positions()` or this will raise AttributeError at runtime.

3. **Creates a fresh PaperTrader per job:** `job_uk_scan`, `job_us_scan`, `job_midday_check`, `job_eod` all construct `PaperTrader(config)` from scratch. This means no shared state with a running TradingBot, no shared position tracking, and no awareness of positions opened in the same session. Scans via AutomationScheduler run independently of the live bot's state.

4. **`status()` method calls `setup()` every time:** Each call to `status()` re-registers all jobs into the global `schedule` module state. The `schedule` library uses a global list. Calling `setup()` twice doubles all jobs. Calling `status()` multiple times without running will cause jobs to be registered repeatedly.

5. **`job_weekly` guard checks `datetime.utcnow().weekday() != 6`:** The job is scheduled at 03:00 every day but only runs on Sunday. This is correct but wasteful — 6 of every 7 executions are no-ops.

6. **Dual scheduling conflict:** If AutomationScheduler and TradingBot are both running, `job_retraining_monitor` and TradingBot's inline `RetrainingController().run_monitoring_cycle()` will both run every 6 hours. Each creates a fresh `RetrainingController` instance, so no shared state, but both will independently check dormancy and potentially both log and write to shadow.db.

7. **Schedule library uses UTC times (the library itself is timezone-naive):** The schedule times match UTC labels in the docstring, but `schedule` library does not know about UTC — it uses the system clock. If server is not in UTC, all times will be offset.

---

---

## FILE 4: /home/dannyelticala/quant-fund/intelligence/daily_pipeline.py

### A) PURPOSE
Orchestrates the three intelligence pipeline jobs (morning prep, market close, weekly deep analysis) and provides a macro briefing generator that aggregates data from shipping, rates, consumer, geopolitical, and overnight news sources into a formatted text report.

---

### B) CLASSES AND METHODS

**Class `DailyPipeline`**

**`__init__(self, config: dict)`**
- Inputs: config dict
- Outputs: initialised DailyPipeline instance
- State: lazy-init handles for _earnings_db, _intel_db, _hist_db, _engine, _influence (all None until first use)

**`_get_earnings_db(self) -> EarningsDB`**
- Lazy: imports and instantiates `EarningsDB(config.get("earnings_db_path", "output/earnings.db"))`

**`_get_intel_db(self) -> IntelligenceDB`**
- Lazy: imports and instantiates `IntelligenceDB(config.get("intelligence_db_path", "output/intelligence_db.db"))`

**`_get_hist_db(self) -> HistoricalDB or None`**
- Lazy: imports and instantiates `HistoricalDB(config.get("historical_db_path", "output/historical_db.db"))` — returns None if unavailable

**`_get_engine(self) -> IntelligenceEngine`**
- Lazy: imports and instantiates `IntelligenceEngine(_get_intel_db(), _get_earnings_db(), _get_hist_db())`

**`_get_influence_engine(self) -> LargeCapInfluenceEngine or None`**
- Lazy: imports and instantiates `LargeCapInfluenceEngine(_get_hist_db(), _get_earnings_db())`

**`_get_universe(self) -> List[str]`**
- Inputs: none (uses self.config)
- Outputs: list of ticker strings
- Logic: tries config["universe_path"], then "data/universe_us_tier1.csv", then "data/universe_us.csv"
- Parses CSV: splits on comma, takes first column, skips rows where col in ("TICKER", "SYMBOL", "")

**`run_morning(self) -> dict`**
- Inputs: none
- Outputs: dict with readthrough_signals, snapshots, errors counts
- Steps:
  1. `influence.get_readthrough_signals(universe, days_lookback=3)` — computes readthrough signals
  2. Stores each signal to intelligence_db: `intel_db.upsert_correlation({asset_a, asset_b, relationship_type="readthrough", correlation, lead_lag_days=1, n_events, p_value=None, sector, sub_sector=None, computed_at})`
  3. `earnings_db.get_upcoming_calendar(days_ahead=2)` — gets next 2 days of reporters
  4. For each: `self._take_pre_earnings_snapshot(ticker, earnings_date)`
  5. `self._print_morning_briefing(readthrough_signals)` — stdout only
  6. `self.run_macro_briefing()` — full macro briefing
- Reads DBs: earnings_db (upcoming calendar), intelligence_db (upsert only)
- Writes DBs: intelligence_db (`cross_asset_correlations` table via upsert_correlation)

**`_take_pre_earnings_snapshot(self, ticker, earnings_date) -> None`**
- Inputs: ticker str, earnings_date str
- Outputs: none (side effect: stores snapshot)
- Calls: `import main as m; m.cmd_pead_snapshot(config, ticker, earnings_date)`
- NOTE: imports main module at runtime from within intelligence/daily_pipeline.py — circular import risk

**`_print_morning_briefing(self, readthrough_signals) -> None`**
- Inputs: list of readthrough signal dicts
- Outputs: prints to stdout only
- Shows: top 5 bullish (score > 0.2) and top 5 bearish (score < -0.2) readthrough signals

**`run_close(self) -> dict`**
- Inputs: none
- Outputs: dict with outcomes_captured, intelligence_patterns, profiles, errors
- Steps:
  1. `_capture_pending_outcomes()` — fills in missing return fields for past observations
  2. `engine.run()` — intelligence engine full run

**`_capture_pending_outcomes(self) -> int`**
- Inputs: none
- Outputs: int count of outcomes captured
- Logic: queries earnings_observations for last 30 days without return_t5; for each, downloads price history via yfinance if earnings was >= 5 days ago; computes returns at t+1, t+3, t+5, t+10, t+20
- Formula: `updates[col] = round(float(closes[offset]) / p0 - 1, 6)` where p0 = first close after earnings date
- Reads DB: earnings_db via `get_observations(since=30d_ago, limit=200)`
- Reads API: `yf.download(ticker, start=edate, end=edate+30d)`
- Writes DB: earnings_db via `upsert_observation(record)`
- NOTE: `earnings_db.get_all_snapshots(days_ahead=0)` is called first but the comment says "returns empty (future-only)" — this call appears to serve no purpose

**`run_weekly(self) -> dict`**
- Inputs: none
- Outputs: dict with coefficients, patterns, report_path, errors
- Steps:
  1. `influence.update_all_coefficients(universe, start_date="2010-01-01")` — recompute all readthrough coefficients
  2. For each entry in influence._coeff_cache: `intel_db.upsert_readthrough_coeff({large_ticker, peer_ticker, coeff, correlation, n_events, start_date, end_date, computed_at})`
  3. `engine.run()` — full intelligence run
  4. `engine.generate_report()` — saves to `output/reports/intelligence_{YYYYMMDD}.txt`; prints to stdout
- Reads DBs: earnings_db, intelligence_db, historical_db
- Writes DBs: intelligence_db (readthrough_coefficients table via upsert_readthrough_coeff)
- Writes file: `output/reports/intelligence_{YYYYMMDD}.txt`

**`status(self) -> dict`**
- Inputs: none
- Outputs: dict with pipeline status, earnings_db stats, intelligence_db stats, influence_engine summary

**`get_macro_state(self) -> dict`**
- Inputs: none
- Outputs: dict with keys: regime, pead_multiplier, shipping_stress, consumer_health, yield_curve_slope, is_inverted, geopolitical_risk
- Primary source: `MacroSignalEngine().run_full_analysis()` — populates all fields
- Fallbacks (only if primary failed for that field):
  - shipping_stress: `ShippingIntelligence().get_current_stress()`
  - yield_curve_slope + is_inverted: `RatesCreditCollector().get_yield_curve_status()`
  - consumer_health: `ConsumerIntelligence().get_consumer_health_index()`
  - geopolitical_risk: `GeopoliticalCollector().get_current_risk_level()`

**`run_macro_briefing(self, save_to_file: bool = True) -> str`**
- Inputs: save_to_file bool (default True)
- Outputs: briefing string; also prints to stdout; optionally saves to file
- Data sources gathered:
  - `get_macro_state()` — regime, pead_multiplier, shipping_stress, consumer_health, yield_curve_slope, is_inverted, geopolitical_risk
  - Direct DB read of `output/historical_db.db` — `shipping_data` table (bdi_value, bdi_roc_1w, shipping_stress_index, stress_regime)
  - `ShippingIntelligence().get_current_stress()` + `get_sector_impacts(ssi)`
  - `GeographicIntelligence().get_extreme_events(threshold=2.0)` — weather alerts
  - `RatesCreditCollector().get_yield_curve_status()` — yield curve slope, inverted flag
  - Direct DB read of `output/historical_db.db` — `rates_data` table (DGS10, DGS2)
  - `RatesCreditCollector().get_credit_conditions()` — HY spreads
  - `RatesCreditCollector().days_to_next_fed_meeting()`
  - `ConsumerIntelligence().get_latest_values()` + direct DB read `macro_series` table (UMCSENT, ICSA)
  - `ConsumerIntelligence().get_trend("UMCSENT")`
  - `ConsumerIntelligence().get_consumer_health_index()`
  - `GeopoliticalCollector().get_alerts()`
  - `GeopoliticalCollector().get_current_risk_level()`
  - NewsAPI HTTP request for top-headlines (business, en, pageSize=5) — uses config["api_keys"]["news_api"]
  - `MacroSignalEngine(config_path='config/settings.yaml').get_complete_briefing_data()` — for regime_confidence
- Trading guidance logic (position_pct):
  - RISK_OFF or inverted: favour Healthcare/Utilities/Consumer Staples, avoid Small Cap Growth/High Beta Tech/Cyclicals, position_pct=65
  - RISK_ON: favour Tech/Consumer Discretionary/Industrials, avoid Defensive Utilities/Long Duration Bonds, position_pct=100
  - STAGFLATION or HIGH_INFLATION: favour Energy/Materials/REIT, avoid Consumer Discretionary/High P/E Growth, position_pct=75
  - else: favour Quality Factor/Dividend Growers, avoid Highly Leveraged Names, position_pct=85
  - Adjustment: `position_pct = int(round(position_pct * float(pead_mult)))` if pead_mult available
- Writes files: `output/macro_briefing_{YYYYMMDD}.txt`, `output/macro_briefing_latest.txt`

---

### C) MATHEMATICS

In `_capture_pending_outcomes`:
- `updates[col] = round(float(closes[offset]) / p0 - 1, 6)` — simple return calculation
- Offsets: t+1=1, t+3=3, t+5=5, t+10=10, t+20=20 trading days

In `run_macro_briefing`:
- `bdi_change = f"{roc_1w*100:+.1f}%"` — percent conversion of rate of change
- `shipping_level = "HIGH" if ssi_val > 1.5 else ("LOW" if ssi_val < -1.5 else "NEUTRAL")`
- `hy_bps = hy * 100` — percent to bps conversion
- `hy_level = "TIGHT" if hy_bps < 300 else ("WIDE" if hy_bps > 500 else "NORMAL")`
- `yc_bps_str = f"{slope*100:+.0f}bps"` — yield curve slope in bps
- `yc_label = "INVERTED" if ... else ("FLAT" if abs(slope) < 0.25 else "NORMAL")` — FLAT when |slope| < 0.25 (25bps)
- `consumer_label`: STRONG if idx > 0.5, MODERATE if idx > 0.1, WEAK if idx > -0.3, DETERIORATING otherwise
- `position_pct = int(round(position_pct * float(pead_mult)))` — regime-adjusted position sizing

---

### D) DATA FLOWS

**INTO daily_pipeline.py:**
- config dict
- earnings_db (observations, upcoming calendar, snapshots)
- intelligence_db (existing patterns/correlations)
- historical_db (shipping_data, rates_data, macro_series)
- altdata_db (sentiment via MacroSignalEngine chain)
- Live APIs: yfinance (price history for outcome capture), NewsAPI (overnight headlines)
- Collectors: ShippingIntelligence, RatesCreditCollector, ConsumerIntelligence, GeopoliticalCollector, GeographicIntelligence, MacroSignalEngine

**OUT OF daily_pipeline.py:**
- intelligence_db writes: cross_asset_correlations, readthrough_coefficients
- earnings_db writes: earnings_observations (return fields via upsert_observation), pre_earnings_snapshots (via cmd_pead_snapshot)
- Files: `output/macro_briefing_{date}.txt`, `output/macro_briefing_latest.txt`, `output/reports/intelligence_{date}.txt`
- stdout (briefings, morning summary)

**DB tables read:**
- `output/earnings.db`: earnings_observations, upcoming_calendar, pre_earnings_snapshots
- `output/historical_db.db`: shipping_data, rates_data, macro_series
- `output/intelligence_db.db`: via IntelligenceEngine

**DB tables written:**
- `output/intelligence_db.db`: cross_asset_correlations, readthrough_coefficients
- `output/earnings.db`: earnings_observations (return updates), pre_earnings_snapshots (via cmd_pead_snapshot)

---

### E) DEPENDENCIES

**Apollo-internal:**
- `data.earnings_db.EarningsDB`
- `analysis.intelligence_db.IntelligenceDB`
- `data.historical_db.HistoricalDB`
- `analysis.intelligence_engine.IntelligenceEngine`
- `data.large_cap_influence.LargeCapInfluenceEngine`
- `analysis.macro_signal_engine.MacroSignalEngine`
- `data.collectors.shipping_intelligence.ShippingIntelligence`
- `data.collectors.rates_credit_collector.RatesCreditCollector`
- `data.collectors.consumer_intelligence.ConsumerIntelligence`
- `data.collectors.geopolitical_collector.GeopoliticalCollector`
- `data.collectors.geographic_intelligence.GeographicIntelligence`
- `main` (imported dynamically in `_take_pre_earnings_snapshot`)

**External:**
- `logging`, `os`, `datetime`, `pathlib`, `typing`
- `yfinance` (in _capture_pending_outcomes)
- `sqlite3` (direct SQL in run_macro_briefing — hardcoded path "output/historical_db.db")
- `requests` (in run_macro_briefing for NewsAPI)

---

### F) WIRING STATUS

DailyPipeline is connected to the live path via:
1. `TradingBot.__init__` instantiates it as `self.pipeline`
2. `TradingBot.run_morning_briefing()` calls `self.pipeline.run_macro_briefing()`
3. `AutomationScheduler.job_morning_intelligence()` calls `DailyPipeline.run_morning()`
4. CLI commands `intelligence morning/close/weekly` call it directly

`run_close()` and `run_weekly()` are NOT called from TradingBot's run_continuous loop — only `run_macro_briefing()` is called. `run_close()` and `run_weekly()` are only reachable via AutomationScheduler or CLI. This means outcome capture and intelligence engine updates do not run in the live bot loop.

---

### G) ISSUES FOUND

1. **`_take_pre_earnings_snapshot` imports `main` at runtime:** `import main as m` inside a method creates a circular import risk and couples the intelligence layer to the top-level entry point. If DailyPipeline is imported before main.py's module-level code runs (e.g., in tests or subprocesses), this will fail or execute the module-level SQLite monkey-patch a second time.

2. **`run_close()` is never called from TradingBot's live loop:** The live bot does NOT run outcome capture or the intelligence engine daily update. These only run via AutomationScheduler's `job_eod` (which calls `PaperTrader.run_eod()`, NOT `DailyPipeline.run_close()`) or via CLI. The outcome capture path (filling in return_t5 etc.) is therefore only available if run manually.

3. **`_capture_pending_outcomes` calls `earnings_db.get_all_snapshots(days_ahead=0)` with no use:** The result is stored to a variable named `snapshots` but never iterated — only `recent_obs` is used. This is dead code.

4. **Direct hardcoded DB paths in `run_macro_briefing`:** `sqlite3.connect("output/historical_db.db")` is used directly (4 separate connection opens) bypassing the WAL monkey-patch in main.py (because this module does not import sqlite3 at the module level — the connections opened here do not benefit from the WAL patch unless the patch already happened before this code runs).

5. **`__import__("datetime").timedelta` usage in `_capture_pending_outcomes`:** At line 311, `__import__("datetime").timedelta(days=30)` is used instead of the already-imported `datetime` module. This works but is unusual and suggests copy-paste from a different context.

6. **MacroSignalEngine instantiated twice in `run_macro_briefing`:** Once as `mse = MacroSignalEngine()` (no args) and once as `mse = MacroSignalEngine(config_path='config/settings.yaml')`. This may create two instances with different initialisation paths.

7. **`conf_prior` and `claims_prior` are initialised to "N/A" and never updated:** The briefing template uses `{conf_prior}` and `{claims_prior}` but neither is ever assigned a real value in the code. The briefing will always display "N/A" for prior consumer confidence and prior jobless claims.

---

---

## FILE 5: /home/dannyelticala/quant-fund/core/retraining_controller.py

### A) PURPOSE
Governs the ML model retraining lifecycle: checks dormancy conditions before any retraining is allowed, monitors rolling performance metrics, triggers shadow training when thresholds are breached, validates candidate models, and maintains a rollback registry — but actual model training is currently deferred (stub only).

---

### B) CLASSES AND METHODS

**Module-level constants:**
- `QUANT_DIR = '/home/dannyelticala/quant-fund'`
- `CLOSELOOP_DB = '/home/dannyelticala/quant-fund/closeloop/storage/closeloop.db'`
- `SIM_DB = '/home/dannyelticala/quant-fund/simulations/simulation.db'`
- `SHADOW_DB = '/home/dannyelticala/quant-fund/simulations/shadow.db'`
- `MODELS_DIR = '/home/dannyelticala/quant-fund/altdata/models'`
- `MIN_TRADING_DAYS = 30`
- `MIN_REAL_TRADES = 500`
- `SHARPE_DECLINE_THRESHOLD = 0.3`
- `WIN_RATE_DECLINE_THRESHOLD = 0.08`
- `DRAWDOWN_INCREASE_THRESHOLD = 0.05`
- `ROLLING_WINDOW_DAYS = 14`
- `MIN_SHARPE_IMPROVEMENT = 0.1`
- `MAX_DRAWDOWN_INCREASE = 0.02`

**Class `RetrainingController`**

**`__init__(self)`**
- Inputs: none
- Outputs: initialised instance
- Creates dirs: `altdata/models/`, `altdata/models/candidates/`, `altdata/models/archive/`

**`_get_conn(self, db_path: str) -> sqlite3.Connection`**
- Inputs: db_path string
- Outputs: sqlite3.Connection with WAL mode and Row row_factory
- NOTE: does NOT use the monkey-patched `_wal_connect` from main.py (uses raw `sqlite3.connect`), but manually sets WAL and row_factory

**`check_dormancy(self) -> Tuple[bool, str]`**
- Inputs: none
- Outputs: (is_dormant: bool, reason: str)
- Reads DB: `closeloop/storage/closeloop.db`
  - Query 1: `SELECT COUNT(DISTINCT ticker || entry_date) as count FROM trade_ledger WHERE ABS(net_pnl) > 0.01 AND is_phantom = 0`
  - Query 2: `SELECT COUNT(DISTINCT date(entry_date)) as days FROM trade_ledger WHERE ABS(net_pnl) > 0.01 AND is_phantom = 0`
- Logic: dormant if real_trade_count < 500 OR trading_days < 30
- On any exception: returns (True, "Dormant: database check failed")

**`compute_rolling_metrics(self, window_days: int = 14) -> Optional[Dict]`**
- Inputs: window_days int (default 14; called with 60 as baseline window)
- Outputs: dict with sharpe, sortino, max_drawdown, win_rate, profit_factor, trade_count, window_days — or None if < 20 trades
- Reads DB: `closeloop/storage/closeloop.db`
  - `SELECT net_pnl FROM trade_ledger WHERE ABS(net_pnl) > 0.01 AND is_phantom = 0 AND entry_date >= {cutoff} ORDER BY entry_date ASC`
- Uses: `statistics.mean`, `statistics.stdev`, `math.sqrt`

**`check_retraining_triggers(self, current: Dict, baseline: Dict) -> Tuple[bool, str, str]`**
- Inputs: current metrics dict, baseline metrics dict
- Outputs: (should_retrain: bool, reason: str, metric_name: str)
- Logic:
  - If `current['sharpe'] < baseline['sharpe'] - 0.3` → retrain ("Sharpe ratio declined significantly", "sharpe")
  - Elif `current['win_rate'] < baseline['win_rate'] - 0.08` → retrain ("Win rate deteriorated", "win_rate")
  - Elif `current['max_drawdown'] > baseline['max_drawdown'] + 0.05` → retrain ("Max drawdown increased", "max_drawdown")
  - Else → no retrain

**`log_retraining_event(self, reason, metric, value, threshold) -> None`**
- Inputs: reason str, metric str, value float, threshold float
- Outputs: none
- Writes DB: `simulations/shadow.db`
  - `INSERT INTO retraining_events (triggered_at, trigger_reason, trigger_metric, trigger_value, threshold_value, outcome) VALUES (?,?,?,?,?,'pending')`

**`run_monitoring_cycle(self) -> None`**
- Inputs: none
- Outputs: none (logs decisions)
- Flow:
  1. `check_dormancy()` — if dormant, log and return
  2. `compute_rolling_metrics(window_days=14)` — current 14-day window
  3. `compute_rolling_metrics(window_days=60)` — 60-day baseline
  4. If either is None: log "Insufficient data" and return
  5. `check_retraining_triggers(current, baseline)`
  6. If should_retrain: `log_retraining_event(...)` + `_initiate_shadow_training(...)`
  7. Else: log current Sharpe and win_rate

**`_initiate_shadow_training(self, current, baseline, reason) -> None`**
- Inputs: current metrics dict, baseline metrics dict, reason str
- Outputs: none (logs + writes to shadow.db)
- Creates version_id: `f"model_{UTC_datetime}_{uuid4_hex6}"`
- Writes DB: `simulations/shadow.db`
  - `INSERT INTO model_registry (version_id, created_at, status, sharpe_ratio, win_rate, max_drawdown, notes) VALUES (?,?,'training',?,?,?,?)`
- Actual training: DEFERRED — logs that training is deferred "until altdata store populated with sufficient data". No training code executes.

**`_validate_candidate(self, version_id, candidate_metrics, live_metrics) -> bool`**
- Inputs: version_id str, candidate_metrics dict, live_metrics dict
- Outputs: bool
- Logic:
  - Reject if `candidate_metrics['sharpe'] <= live_metrics['sharpe'] + 0.1`
  - Reject if `candidate_metrics['max_drawdown'] > live_metrics['max_drawdown'] + 0.02`
  - Reject if `candidate_metrics['win_rate'] < live_metrics['win_rate']`
  - Accept otherwise
- NOTE: this method is defined but NEVER CALLED anywhere in this file or in the calling code

**`rollback_to_version(self, version_id: str) -> bool`**
- Inputs: version_id str
- Outputs: bool (True if successful, False if archive not found)
- Reads file: `altdata/models/archive/{version_id}.joblib`
- Writes files: backs up `altdata/models/live_model.joblib` to `.pre_rollback`, then copies archive to live path

---

### C) MATHEMATICS

**`compute_rolling_metrics`:**
- `mean_pnl = statistics.mean(pnls)`
- `std_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 1`
- `sharpe = (mean_pnl / std_pnl * math.sqrt(252)) if std_pnl > 0 else 0`
- `ds_std = statistics.stdev(losers) if len(losers) > 1 else std_pnl` — downside std (PnL units only, not returns)
- `sortino = (mean_pnl / ds_std * math.sqrt(252)) if ds_std > 0 else 0`
- Max drawdown: equity starts at 100000.0, `dd = (peak - equity) / peak`
- `win_rate = len(winners) / len(pnls)`
- `profit_factor = (sum(winners) / abs(sum(losers))) if losers else 999.0`

**`check_retraining_triggers`:**
- Trigger conditions (comparisons only, no formulas):
  - `current['sharpe'] < baseline.get('sharpe', 0) - 0.3`
  - `current['win_rate'] < baseline.get('win_rate', 0) - 0.08`
  - `current['max_drawdown'] > baseline.get('max_drawdown', 0) + 0.05`

**`check_dormancy`:**
- `COUNT(DISTINCT ticker || entry_date)` — deduplicates trades by concatenating ticker+entry_date string

---

### D) DATA FLOWS

**INTO retraining_controller.py:**
- `closeloop/storage/closeloop.db` — `trade_ledger` table for trade counts, trading days, and recent PnL data

**OUT OF retraining_controller.py:**
- `simulations/shadow.db` — `retraining_events` table (INSERT on trigger), `model_registry` table (INSERT 'training' status on shadow training initiation)
- `altdata/models/` directory — created at __init__; `altdata/models/archive/`, `altdata/models/candidates/` subdirs
- `altdata/models/live_model.joblib` — written by `rollback_to_version()` only (not by normal training path)
- `altdata/models/live_model.joblib.pre_rollback` — backup created by rollback

**DB tables read:**
- `closeloop/storage/closeloop.db`: `trade_ledger` (net_pnl, is_phantom, entry_date columns)

**DB tables written:**
- `simulations/shadow.db`: `retraining_events`, `model_registry`

---

### E) DEPENDENCIES

**Apollo-internal:** none (self-contained)

**External:**
- `sqlite3`, `logging`, `json`, `uuid`, `os`, `datetime`, `typing`
- `statistics` (stdlib, imported inline in compute_rolling_metrics)
- `math` (stdlib, imported inline in compute_rolling_metrics)
- `shutil` (stdlib, imported inline in rollback_to_version)

---

### F) WIRING STATUS

RetrainingController is connected to the live path:
1. `TradingBot.run_continuous()` calls `RetrainingController().run_monitoring_cycle()` every 6 hours at minute == 1 (i.e., when `hour % 6 == 0 and minute == 1`)
2. `AutomationScheduler.job_retraining_monitor()` also calls it every 6 hours

It is connected in terms of being called, but the actual training is a stub. The system monitors, logs triggers to shadow.db, and registers candidates in model_registry — but no model is ever trained. The retraining system is dormant by design until 500 real trades and 30 trading days are accumulated.

---

### G) ISSUES FOUND

1. **`_validate_candidate` is never called:** The method is fully implemented and checks three validation criteria, but there is no code path that calls it. Shadow training registers a 'training' status entry in model_registry and then stops. The validation step has no caller.

2. **Actual training is a stub:** `_initiate_shadow_training` logs "training deferred" and inserts a record into model_registry with status='training' but executes no training code. The log message references `altdata/learning/weekly_retrainer.py` but that code is never called here. Models are never actually trained by this controller.

3. **Sharpe calculation uses raw PnL units, not returns:** `sharpe = mean_pnl / std_pnl * sqrt(252)` where mean_pnl and std_pnl are in dollar PnL, not percentage returns. Annualised Sharpe from PnL units is not dimensionally correct and will produce values that scale with position size rather than strategy quality. The value is used to detect relative decline, so comparative use may still be meaningful, but absolute Sharpe values are not comparable to standard Sharpe ratios.

4. **`check_dormancy` deduplication via string concatenation is fragile:** `COUNT(DISTINCT ticker || entry_date)` concatenates ticker and entry_date without a separator. "AAPL2026-01-01" and "AAPLX2026-01-0" would both produce "AAPL2026-01-01" (last character collision). The correct approach would use `ticker || '|' || entry_date`. This could undercount unique trades.

5. **Shadow DB may not have the required tables:** `retraining_events` and `model_registry` tables are assumed to exist in `simulations/shadow.db`. If shadow.db was not initialised with the correct schema, `log_retraining_event` and `_initiate_shadow_training` will fail with OperationalError. There is no CREATE TABLE or schema initialisation in this file.

6. **`baseline.get('sharpe', 0)` default of 0 means any positive current Sharpe prevents a retrain:** In `check_retraining_triggers`, if baseline_metrics has no 'sharpe' key (unlikely but possible if the dict is malformed), the trigger evaluates against 0. With `0 - 0.3 = -0.3`, a current Sharpe of -0.3 or better would not trigger retraining. This is a very permissive fallback.

7. **`rollback_to_version` is never called from any automated path:** The method exists and works, but there is no caller in the scheduler, bot loop, or CLI routing. Rollback must be invoked manually.

---

---

## SECTION 1A — STARTUP SEQUENCE

When `python3 main.py bot start` is invoked (foreground, the live path):

**Step 1:** Module-level code in main.py executes — `_sqlite3_raw.connect` is monkey-patched to `_wal_connect`; both `sqlite3.connect` references are patched. All subsequent DB connections in the entire process get WAL mode. — `main.py` module level

**Step 2:** `main()` is called — `setup_logging()` is invoked: creates `logs/` and `output/` dirs, configures RotatingFileHandler on `logs/quant_fund.log` (50MB, 3 backups), stream handler to stdout, suppresses yfinance/peewee/urllib3 noise. — `main.py:setup_logging`

**Step 3:** argparse parses `bot start` → routes to `_run_bot_command(config, args)` — `main.py:main`

**Step 4:** `load_config("config/settings.yaml")` is called, returns config dict. — `main.py:load_config`

**Step 5:** `_run_bot_command` detects `cmd == 'start'` and `background == False` → calls `_acquire_pid_lock()`. Checks if `output/apollo.pid` exists; if the process it contains is alive, prints error and exits. If dead or missing, writes current PID to `output/apollo.pid`. — `main.py:_acquire_pid_lock`

**Step 6:** `from execution.trading_bot import TradingBot` is imported. — `main.py:_run_bot_command`

**Step 7:** `TradingBot(config)` is constructed:

  **Step 7a:** `Path('output').mkdir(exist_ok=True)`, `Path('logs').mkdir(exist_ok=True)` — `trading_bot.py:TradingBot.__init__`

  **Step 7b:** `PaperTrader(config)` is instantiated as `self.trader`. — `trading_bot.py:TradingBot.__init__`

  **Step 7c:** `_init_collectors()` is called: attempts to import and instantiate all 15 data collectors; counts successes; logs "{n}/15 collectors loaded"; runs `check_for_delisted` for 5 watchlists. — `trading_bot.py:TradingBot._init_collectors`

  **Step 7d:** `DailyPipeline(config)` is instantiated as `self.pipeline` (wrapped in try/except). — `trading_bot.py:TradingBot.__init__`

  **Step 7e:** `start_stream(config, _us_tickers)` is called from `execution.alpaca_stream`; `_us_tickers` is loaded via `UniverseManager._default_tickers('us')`. Stream worker is stored as `self.stream_worker` (try/except). — `trading_bot.py:TradingBot.__init__`

  **Step 7f:** `start_monitoring(config, self.stream_worker)` is called from `monitoring.monitor_runner`. Stored as `self.monitor_runner` (try/except). — `trading_bot.py:TradingBot.__init__`

  **Step 7g:** `start_private_bot(config)` is called from `monitoring.private_bot`. Stored as `self.private_bot` (try/except). — `trading_bot.py:TradingBot.__init__`

  **Step 7h:** `start_dashboard` from `monitoring.dashboard.app` is started in a daemon thread named "apollo-dashboard" (try/except). — `trading_bot.py:TradingBot.__init__`

  **Step 7i:** State tracking variables initialised (all None or 0). — `trading_bot.py:TradingBot.__init__`

  **Step 7j:** `_save_status('INITIALISED')` — writes `output/bot_status.json`. — `trading_bot.py:TradingBot._save_status`

**Step 8:** `bot.run_continuous()` is called. — `main.py:_run_bot_command`

**Step 9:** `self.running = True`, `_save_pid()` writes `output/bot.pid`, `_save_status('STARTING')`. — `trading_bot.py:TradingBot.run_continuous`

**Step 10:** `self.trader.reconcile_positions()` is called to sync Alpaca positions with trade_ledger. If reconciliation finds discrepancies, a Telegram notification is sent via `Notifier(config)._send_telegram(...)`. — `trading_bot.py:TradingBot.run_continuous`

**Step 11:** Startup banner is printed to stdout: Phase, Alpaca connection status, Stream status, Collector count, Ctrl+C instruction. — `trading_bot.py:TradingBot.run_continuous`

**Step 12:** SIGTERM signal handler registered: sets `self.running = False`. — `trading_bot.py:TradingBot.run_continuous`

**Step 13:** Main loop begins (`while self.running`). Loop sleeps 10s per iteration; acts once per unique minute. — `trading_bot.py:TradingBot.run_continuous`

---

## SECTION 1B — SCHEDULER JOBS MAP

### Bot's Inline Scheduler (in `run_continuous` loop)

---

**Job name:** Morning Briefing
**Schedule:** 06:00–06:04 UTC daily (checks `hour == 6 and minute < 5`)
**Defined in:** `trading_bot.py:TradingBot.should_run_morning_briefing` / `run_morning_briefing`
**Calls:** `self.pipeline.run_macro_briefing()` → `intelligence/daily_pipeline.py:DailyPipeline.run_macro_briefing`
**Guards:** UTC hour==6, minute<5, at least 1 day since last briefing
**What it produces:** Macro briefing text saved to `output/macro_briefing_{date}.txt` and `output/macro_briefing_latest.txt`; printed to stdout; reads shipping, rates, consumer, geo data from collectors and historical_db
**Current status:** PARTIALLY WORKING
**Evidence:** `run_macro_briefing` is called correctly, but several data fields in the briefing are always "N/A" (`conf_prior`, `claims_prior` never set). Weather block relies on `GeographicIntelligence` which may or may not be available.

---

**Job name:** Preflight tick
**Schedule:** Every minute (called every loop iteration)
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `monitoring.preflight_check.tick(config, now=now, trader=self.trader)`
**Guards:** None (try/except, errors silently ignored)
**What it produces:** Pre-flight checks, pre-market scans at 13:00 UTC, 14:00 UTC; EOD close at 21:15 UTC
**Current status:** UNKNOWN — depends on monitoring.preflight_check implementation (not in GROUP 1)
**Evidence:** Called every minute but behavior internal to preflight_check module

---

**Job name:** UK Market Scan
**Schedule:** UK scan times GMT: 08:15, 09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00, 16:15 — 90-second windows, once per time per day
**Defined in:** `trading_bot.py:TradingBot.run_continuous` / `is_scan_time` / `run_market_scan`
**Calls:** `self.trader.run_scan(market='uk')` → `execution/paper_trader.py:PaperTrader.run_scan`
**Guards:** `is_market_open('UK')` (weekday, not holiday, within 08:00–16:30 GMT), `is_scan_time('UK')` (±90s window, once per time slot per day), `not self.paused`
**What it produces:** Trade entries/exits; updates scan_count, last_uk_scan, signals_tickers_today; saves status
**Current status:** WORKING
**Evidence:** Logic is complete and consistent; deduplication guard prevents double-firing

---

**Job name:** US Market Scan
**Schedule:** US scan times GMT: 14:45, 15:30, 16:00, 17:00, 18:00, 19:00, 20:00, 20:45 — 90-second windows, once per time per day
**Defined in:** `trading_bot.py:TradingBot.run_continuous` / `is_scan_time` / `run_market_scan`
**Calls:** `self.trader.run_scan(market='us')` → `execution/paper_trader.py:PaperTrader.run_scan`
**Guards:** `is_market_open('US')`, `is_scan_time('US')`, `not self.paused`
**What it produces:** Same as UK scan
**Current status:** WORKING
**Evidence:** Same as UK scan

---

**Job name:** Data Collection
**Schedule:** Every 30 minutes (elapsed since last_data_collection)
**Defined in:** `trading_bot.py:TradingBot.run_continuous` / `should_collect_data` / `run_data_collection`
**Calls:** All 15 collectors' respective methods; `_fetch_articles_for_tickers`; `FrontierSignalEngine.get_umci_snapshot()`
**Guards:** `should_collect_data()` — 30-minute interval; sec_fulltext additionally guarded by last_sec_fulltext_date (once/day)
**What it produces:** Data written to altdata db, historical db, sec fulltext db; article counts accumulated; UMCI snapshot stored to frontier store; articles fetched for signal tickers
**Current status:** PARTIALLY WORKING
**Evidence:** 15 collectors loaded but many may fail silently (all wrapped in try/except); FrontierStore/FrontierSignalEngine instantiated fresh every cycle (potential connection leak); finnhub and job_postings require closeloop.db reads which could fail

---

**Job name:** Position Check
**Schedule:** Every 15 minutes (when `now.minute % 15 == 0`)
**Defined in:** `trading_bot.py:TradingBot.run_continuous` / `_check_open_positions`
**Calls:** `self.trader.closeloop.get_open_positions()` or `self.trader.broker.get_positions()`; `self.trader.check_exit_conditions(positions, equity)`
**Guards:** `minute % 15 == 0`; silently ignores all errors
**What it produces:** Exit signals for open positions
**Current status:** WORKING
**Evidence:** Logic is straightforward; fallback to broker if closeloop unavailable

---

**Job name:** Weekly Report
**Schedule:** Sunday 09:00 UTC (`weekday() == 6 and hour == 9 and minute == 0`)
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `monitoring.weekly_report.WeeklyReportGenerator().send_weekly_report()`
**Guards:** Sunday + 09:00 UTC exact minute match
**What it produces:** Weekly report sent via Telegram (8 sections)
**Current status:** WORKING (if monitoring.weekly_report is implemented)
**Evidence:** Single Sunday trigger at exact minute; no deduplication guard (could double-fire if loop wakes on minute 9 twice — but the `last_check_minute` mechanism prevents this)

---

**Job name:** Daily Price Refresh
**Schedule:** After 21:00 UTC, once per calendar day
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `data.historical_collector.HistoricalCollector.collect_price_history(tickers, start=7_days_ago)`
**Guards:** `hour >= 21` + `last_price_refresh_date != today_str`
**What it produces:** Price history rows for open positions in historical_db (last 7 days)
**Current status:** WORKING
**Evidence:** Guard is correct; reads open positions from closeloop.db

---

**Job name:** EOD Simulation
**Schedule:** After 21:30 UTC, once per calendar day
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `simulations.sim_scheduler.run_daily_simulation()`
**Guards:** `hour >= 21 and minute >= 30` + `last_eod_sim_date != today_str`
**What it produces:** Daily simulation run (details in simulations module, not GROUP 1)
**Current status:** WORKING (call is correct; simulation internals not audited)
**Evidence:** Guard logic correct

---

**Job name:** Self-Diagnostic
**Schedule:** Every 6 hours at minute 0: 00:00, 06:00, 12:00, 18:00 UTC
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `monitoring.self_diagnostic.SelfDiagnostic(config).run()`
**Guards:** `hour % 6 == 0 and minute == 0`
**What it produces:** Diagnostic report; logs warnings if critical failures; logs pass count if OK
**Current status:** WORKING (if monitoring.self_diagnostic is implemented)
**Evidence:** Guard logic correct; results checked but not acted upon automatically

---

**Job name:** Retraining Monitor
**Schedule:** Every 6 hours at minute 1: 00:01, 06:01, 12:01, 18:01 UTC
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `core.retraining_controller.RetrainingController().run_monitoring_cycle()`
**Guards:** `hour % 6 == 0 and minute == 1`
**What it produces:** Dormancy check logged; if active, rolling metrics computed; if trigger fires, logs to shadow.db `retraining_events` and `model_registry`; no actual training occurs
**Current status:** PARTIALLY WORKING
**Evidence:** Dormancy check works; trigger detection works; shadow DB writes work; actual model training is a stub (deferred indefinitely)

---

**Job name:** Weekly Deep Analysis (BatchRetrainer)
**Schedule:** Sunday 03:00 UTC
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** `closeloop.learning.batch_retrainer.BatchRetrainer(config).run()`
**Guards:** `weekday() == 6 and hour == 3 and minute == 0`
**What it produces:** Batch retraining of closeloop signal weights
**Current status:** UNKNOWN
**Evidence:** Call is correct; internals of BatchRetrainer not in GROUP 1

---

**Job name:** Daily Counter Reset
**Schedule:** Midnight 00:00 UTC
**Defined in:** `trading_bot.py:TradingBot.run_continuous`
**Calls:** Resets `errors_today=0`, `articles_fetched_today=0`, `signals_tickers_today=[]`
**Guards:** `hour == 0 and minute == 0`
**What it produces:** In-memory counters reset
**Current status:** WORKING
**Evidence:** Simple assignments

---

### AutomationScheduler Jobs (intelligence/automation_scheduler.py)

(These only run if `python3 main.py schedule start` is invoked separately)

---

**Job name:** job_collect_data
**Schedule:** Daily 06:00 UTC
**Defined in:** `automation_scheduler.py:job_collect_data`
**Calls:** subprocess `python3 main.py altdata collect` (timeout 1800s)
**Guards:** None beyond schedule time
**What it produces:** All 12 altdata collectors run; results stored to altdata.db
**Current status:** WORKING (if altdata collect works)
**Evidence:** Subprocess call is correct

---

**Job name:** job_update_prices
**Schedule:** Daily 06:30 UTC
**Defined in:** `automation_scheduler.py:job_update_prices`
**Calls:** subprocess `python3 main.py historical collect --phases prices --start {5_days_ago}` (timeout 1800s)
**Guards:** None
**What it produces:** Price history updated for last 5 days
**Current status:** WORKING
**Evidence:** Subprocess call is correct

---

**Job name:** job_morning_intelligence
**Schedule:** Daily 07:00 UTC
**Defined in:** `automation_scheduler.py:job_morning_intelligence`
**Calls:** `DailyPipeline(config).run_morning()` in-process
**Guards:** None
**What it produces:** Readthrough signals computed and stored; pre-earnings snapshots taken; morning briefing printed; macro briefing generated
**Current status:** PARTIALLY WORKING
**Evidence:** run_morning() calls cmd_pead_snapshot via circular import of main; conf_prior/claims_prior always N/A in briefing

---

**Job name:** job_uk_scan
**Schedule:** Daily 08:15 UTC
**Defined in:** `automation_scheduler.py:job_uk_scan`
**Calls:** `PaperTrader(config).run_scan(market='uk')`
**Guards:** None — no market open check, no holiday check
**What it produces:** UK market scan with fresh PaperTrader instance
**Current status:** BROKEN
**Evidence:** No market open or holiday guard — will run even on weekends and holidays. Fresh PaperTrader has no shared state with running bot. Only fires at one fixed time (08:15) vs. bot's 9 scan times.

---

**Job name:** job_us_scan
**Schedule:** Daily 14:45 UTC
**Defined in:** `automation_scheduler.py:job_us_scan`
**Calls:** `PaperTrader(config).run_scan(market='us')`
**Guards:** None — no market open check, no holiday check
**What it produces:** US market scan with fresh PaperTrader instance
**Current status:** BROKEN
**Evidence:** Same issues as job_uk_scan. Only fires at one fixed time vs. bot's 8 scan times.

---

**Job name:** job_midday_check
**Schedule:** Daily 18:00 UTC
**Defined in:** `automation_scheduler.py:job_midday_check`
**Calls:** `PaperTrader(config).check_open_positions()`
**Guards:** None
**What it produces:** Intended to check open positions; will raise AttributeError
**Current status:** BROKEN
**Evidence:** PaperTrader does not expose `check_open_positions()` — method does not exist on PaperTrader (the bot uses `trader.check_exit_conditions()` via TradingBot._check_open_positions)

---

**Job name:** job_eod
**Schedule:** Daily 21:30 UTC
**Defined in:** `automation_scheduler.py:job_eod`
**Calls:** `PaperTrader(config).run_eod()`, then `run_daily_simulation()`
**Guards:** None
**What it produces:** EOD PaperTrader actions; daily simulation
**Current status:** PARTIALLY WORKING
**Evidence:** `run_eod()` existence on PaperTrader not confirmed from GROUP 1 files; simulation call is correct

---

**Job name:** job_weekly
**Schedule:** Daily 03:00 UTC (guard inside: only runs on Sunday)
**Defined in:** `automation_scheduler.py:job_weekly`
**Calls:** `BatchRetrainer(config).run()`
**Guards:** `datetime.utcnow().weekday() != 6` — skips if not Sunday
**What it produces:** Weekly batch retraining of signal weights
**Current status:** WORKING (if BatchRetrainer works)
**Evidence:** Guard logic is correct; runs 6 of 7 calls as no-ops

---

**Job name:** job_retraining_monitor
**Schedule:** Every 6 hours
**Defined in:** `automation_scheduler.py:job_retraining_monitor`
**Calls:** `RetrainingController().run_monitoring_cycle()`
**Guards:** None (internally checks dormancy)
**What it produces:** Same as inline bot retraining monitor; logs to shadow.db
**Current status:** PARTIALLY WORKING
**Evidence:** Same as bot's inline retraining monitor — training is a stub; if both bot and scheduler run simultaneously, this fires twice per 6h period

---

**Job name:** job_weekly_report (DISABLED)
**Schedule:** Was at 09:00 UTC daily (disabled)
**Defined in:** `automation_scheduler.py:job_weekly_report` (function exists, not registered)
**Calls:** `WeeklyReportGenerator().send_weekly_report()` — would only run on Sunday via guard
**Guards:** Function exists but `schedule.every().day.at("09:00").do(job_weekly_report)` is commented out
**What it produces:** Nothing (disabled)
**Current status:** DISABLED
**Evidence:** Comment in code: "DISABLED: Weekly report de-duplicated — now only in trading_bot.py"

---

## SECTION 1C — BACKGROUND THREADS MAP

---

**Thread: apollo-dashboard**
- How started: `threading.Thread(target=start_dashboard, daemon=True, name="apollo-dashboard").start()` — in `TradingBot.__init__`
- What it runs: `monitoring.dashboard.app.start_dashboard` — web dashboard server (Flask or similar)
- daemon=True
- What happens if it crashes: silently dies; no restart mechanism; no logging beyond initial `__init__` warning if start fails; the main bot continues running unaware

---

**Thread/Worker: AlpacaStream (stream_worker)**
- How started: `start_stream(config, _us_tickers)` returns a worker object stored as `self.stream_worker`. The exact threading mechanism (Thread, Process, asyncio) is inside `execution.alpaca_stream` (not in GROUP 1 files).
- What it runs: Alpaca websocket real-time price feed for US tickers
- daemon status: unknown (in alpaca_stream module, not read)
- What happens if it crashes: `self.stream_worker.is_alive()` check in startup banner will show DISCONNECTED; bot falls back to "yfinance fallback" per startup message; no automatic restart

---

**Thread/Worker: MonitorRunner (monitor_runner)**
- How started: `start_monitoring(config, self.stream_worker)` returns a runner object. Exact threading mechanism in `monitoring.monitor_runner` (not in GROUP 1 files).
- What it runs: Self-monitoring and health reporting
- daemon status: unknown
- What happens if it crashes: silently drops; initial `__init__` warning logged if start fails; no restart

---

**Thread/Worker: PrivateBot (private_bot)**
- How started: `start_private_bot(config)` returns a bot object. Exact threading mechanism in `monitoring.private_bot` (not in GROUP 1 files).
- What it runs: Telegram NLP + inline menu bot
- daemon status: unknown
- What happens if it crashes: silently drops; initial `__init__` warning logged if start fails; no restart

---

**Main loop: run_continuous**
- How started: called directly from `main.py:_run_bot_command` in the main thread
- What it runs: the primary trading loop (market scans, data collection, all inline scheduled jobs)
- daemon=False (main thread)
- What happens if it crashes: outer try/except in the while loop catches exceptions, increments `errors_today`, sleeps 30s, and continues. KeyboardInterrupt stops cleanly. SIGTERM stops cleanly.

---

## SECTION 1 GATE

**Files read:**
- `/home/dannyelticala/quant-fund/main.py` (3574 lines — read in 7 chunks, complete)
- `/home/dannyelticala/quant-fund/execution/trading_bot.py` (749 lines — read complete)
- `/home/dannyelticala/quant-fund/intelligence/automation_scheduler.py` (213 lines — read complete)
- `/home/dannyelticala/quant-fund/intelligence/daily_pipeline.py` (851 lines — read complete)
- `/home/dannyelticala/quant-fund/core/retraining_controller.py` (324 lines — read complete)

---

**Key findings:**

1. PAUSE/RESUME commands will kill the bot — SIGUSR1/SIGUSR2 sent by CLI but no handlers registered in TradingBot; Linux default action for SIGUSR1 is process termination
2. AutomationScheduler's `job_midday_check` calls `PaperTrader.check_open_positions()` — method does not exist; will raise AttributeError
3. AutomationScheduler's market scans have no market-open or holiday guards — will run on weekends and holidays
4. DailyPipeline's `_take_pre_earnings_snapshot` imports `main` at runtime — circular import risk
5. `run_close()` is never called from TradingBot's live loop — outcome capture and intelligence engine updates only via CLI or separate AutomationScheduler
6. Macro briefing always shows "N/A" for `conf_prior` and `claims_prior` — variables initialised but never assigned
7. `is_market_open` called with lowercase 'us'/'uk' in `_save_status` but MARKET_SCHEDULE and _MARKET_HOLIDAYS use uppercase 'UK'/'US' — KeyError on every `_save_status` call, silently swallowed
8. RetrainingController `_validate_candidate` method exists and is fully implemented but is never called — validation step is dead code
9. Actual ML model training in RetrainingController is a stub — `_initiate_shadow_training` only writes to shadow.db; no scikit-learn code executes
10. Holiday tables only cover 2026 — bot will trade on 2027 public holidays with no guard
11. Sharpe ratio in retraining controller computed from raw PnL dollars, not return percentages — not a standard Sharpe
12. `check_dormancy` uses string concatenation `ticker || entry_date` without separator — potential undercounting of unique trades
13. Shadow DB (`simulations/shadow.db`) requires pre-existing tables `retraining_events` and `model_registry` — no CREATE TABLE in retraining_controller.py; will fail if DB not pre-initialised
14. AutomationScheduler and TradingBot both independently schedule: weekly report (bot only, scheduler disabled), retraining monitor (both — double execution if both processes run), BatchRetrainer weekly (both)
15. `_setup_cron` installs cron entry calling `paper_trade --once` at 21:30 — this conflicts with bot's inline EOD simulation at 21:30 UTC if both are active
16. Background bot start uses `subprocess.Popen` without acquiring the PID lock in the parent, creating two PID files: `output/apollo.pid` (lock) and `output/bot.pid` (stop signal target) — not a bug but a confusing dual-file design
17. FrontierStore and FrontierSignalEngine instantiated fresh every 30-minute data collection cycle — potential connection leak
18. DailyPipeline's `run_macro_briefing` opens `output/historical_db.db` via raw `sqlite3.connect` directly — bypasses WAL monkey-patch if called before main.py's module-level code runs
19. `cmd_earnings_collect` references undefined name `log` (should be `logger` or a locally defined variable) — NameError at runtime

---

**Contradictions found:**

1. AutomationScheduler docstring and log message claim 10 jobs; only 9 are registered (weekly_report disabled)
2. `cmd_paper_trade_once` print says "200-ticker sample" but code caps at `limit=100`
3. AutomationScheduler comment says "DISABLED: Weekly report de-duplicated — now only in trading_bot.py" — confirmed correct, but the function `job_weekly_report` still exists and has a Sunday guard, creating confusion about intent
4. TradingBot `run_continuous` inline comment says "Pre-flight (13:00 UTC), pre-market scan (14:00 UTC), EOD (21:15 UTC)" for preflight_tick — these internal schedules are not verifiable from GROUP 1 (preflight_check not read)
5. Holiday tables labelled `_US_HOLIDAYS_2026` and `_UK_HOLIDAYS_2026` but `_MARKET_HOLIDAYS` dict uses keys 'UK' and 'US' — `is_market_open` looks up via the market parameter which comes in both lower and uppercase from different call sites; the holiday check `today in _MARKET_HOLIDAYS.get(market, set())` uses the same market string — lowercase 'uk'/'us' will miss the holiday tables entirely

---

**Data flows documented:**

- main.py → config/settings.yaml (read)
- main.py → output/apollo.pid (write, PID lock)
- main.py → output/bot.pid (write, stop signal)
- main.py → logs/quant_fund.log (write, rotating)
- main.py → output/earnings.db (write via cmd_pead_snapshot, cmd_pead_observe)
- main.py → output/altdata.db (write via cmd_altdata_collect)
- main.py → output/historical_db.db (write via cmd_historical_collect)
- trading_bot.py → output/bot_status.json (write, every scan + status change)
- trading_bot.py → output/bot.pid (write, run_continuous start)
- trading_bot.py → closeloop/storage/closeloop.db (read: trade_ledger for finnhub sample, job_postings tickers, price refresh tickers)
- trading_bot.py → all 15 collectors → their respective DBs (write, via collect methods)
- automation_scheduler.py → main.py (subprocess: altdata collect, historical collect)
- automation_scheduler.py → earnings_db, intelligence_db (write, via DailyPipeline.run_morning)
- daily_pipeline.py → output/intelligence_db.db (write: cross_asset_correlations, readthrough_coefficients)
- daily_pipeline.py → output/earnings.db (write: earnings_observations returns, pre_earnings_snapshots)
- daily_pipeline.py → output/historical_db.db (read: shipping_data, rates_data, macro_series)
- daily_pipeline.py → output/macro_briefing_*.txt (write)
- daily_pipeline.py → output/reports/intelligence_*.txt (write)
- retraining_controller.py → closeloop/storage/closeloop.db (read: trade_ledger PnL and dates)
- retraining_controller.py → simulations/shadow.db (write: retraining_events, model_registry)
- retraining_controller.py → altdata/models/ (directory creation; live_model.joblib only on rollback)

---

**Proceed to Section 2: YES**
