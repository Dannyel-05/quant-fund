# APOLLO SYSTEM MAP — PART 9
# Monitoring, Telegram Bot, and Notification Systems
# Generated: 2026-04-08

---

## FILE 1: /home/dannyelticala/quant-fund/monitoring/private_bot.py

### A) PURPOSE
Telegram private bot that provides an interactive control panel for the Apollo trading system. Runs as a daemon thread using long-polling (getUpdates). Responds to button presses from an inline keyboard and to natural-language text messages. All data access is strictly read-only from SQLite databases. Also supports a natural-language trade explanation engine that queries the ReasoningEngine for per-ticker analysis. Hardcoded to respond only to PRIVATE_CHAT_ID = "8508697534".

### B) CLASSES AND METHODS

#### Module-level constants
- `DB_CLOSELOOP` = `closeloop/storage/closeloop.db`
- `DB_CLOSELOOP_DATA` = `closeloop_data.db` (root-level fallback)
- `DB_HISTORICAL` = `output/historical_db.db`
- `DB_FRONTIER` = `frontier/storage/frontier.db`
- `DB_DEEPDATA` = `deepdata/storage/deepdata.db`
- `DB_INTELLIGENCE` = `output/intelligence_db.db`
- `START_DATE` = "2026-04-03"
- `PRIVATE_CHAT_ID` = "8508697534"

#### Module-level functions

**`_load_config() -> dict`**
- Inputs: none
- Outputs: parsed YAML dict from `config/settings.yaml`, or `{}` on failure

**`_db_path() -> str`**
- Inputs: none
- Outputs: returns `DB_CLOSELOOP` if its file size > 100 bytes, else `DB_CLOSELOOP_DATA`
- Logic: preference selector between two closeloop DB paths

**`_safe_query(db_path, sql, params) -> list`**
- Inputs: db_path (str), sql (str), params (tuple)
- Outputs: list of row dicts; empty list on any exception
- DB reads: arbitrary table/path as provided

**`_safe_scalar(db_path, sql, params, default) -> Any`**
- Inputs: db_path, sql, params, default value
- Outputs: first column of first row, or default

**`_tg_request(token, method, data, timeout) -> Optional[dict]`**
- Inputs: token (str), method (str), data (dict), timeout int default 15
- Outputs: JSON response dict or None
- Makes HTTPS POST to `https://api.telegram.org/bot{token}/{method}`

**`_send_message(token, chat_id, text, reply_markup, parse_mode) -> Optional[dict]`**
- Inputs: token, chat_id, text, optional reply_markup dict, parse_mode default "Markdown"
- Outputs: Telegram API response
- Truncates text to 4096 chars if over limit

**`_answer_callback(token, callback_query_id, text) -> None`**
- Inputs: token, callback_query_id, text
- Outputs: none; calls answerCallbackQuery

**`_send_photo(token, chat_id, photo_path, caption) -> bool`**
- Inputs: token, chat_id, photo_path (str), caption (str)
- Outputs: True if HTTP 200, False on exception
- Builds raw multipart/form-data body manually using urllib; no third-party HTTP lib

**`_build_menu_keyboard() -> dict`**
- Inputs: none
- Outputs: Telegram inline_keyboard dict with 9 rows × 2 buttons = 18 buttons total

**`_build_menu_header() -> str`**
- Inputs: none
- DB reads: `DB_FRONTIER.umci_history` (SELECT level, umci ORDER BY recorded_at DESC LIMIT 1); `output/bot_status.json` (file read, not DB); `_db_path().trade_ledger` (COUNT real trades, COUNT open positions)
- Outputs: formatted status string for the Telegram main menu header
- System calls: `subprocess.run(["ps", "aux"])` to find PID of `python3 main.py`

**`_handle_positions(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `_db_path().trade_ledger` WHERE exit_date IS NULL AND is_phantom=0, columns: ticker, entry_date, entry_price, exit_price, pnl_pct, net_pnl, position_size, signals_at_entry, holding_days, sector, direction, macro_regime
- Live enrichment: attempts to import `execution.broker_interface.AlpacaPaperBroker` and call `get_positions()` to get current_price for live PnL calculation
- Live PnL formula for LONG (direction >= 0): `(current_price - entry_price) / entry_price * 100`
- Live PnL formula for SHORT (direction < 0): `(entry_price - current_price) / entry_price * 100`
- Falls back to stored `pnl_pct` if no live price
- Displays: table of open positions with entry price, live PnL%, net PnL, size, signal, days, sector; total PnL row

**`_handle_performance(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `_db_path().trade_ledger` WHERE is_phantom=0 AND entry_date >= '2026-04-03'
- Calculations:
  - Win rate: `len(wins) / len(closed)`
  - Daily Sharpe: groups closed trades by exit_date, computes daily PnL, then `(mean / std) * sqrt(252)` (sample std dev, n-1)
  - Max drawdown: cumulative PnL series over sorted closed trades; `(peak - v) / 100_000.0` (hardcoded £100k base)
  - Avg win: sum of profitable trade net_pnl / count
  - Avg loss: sum of non-profitable net_pnl / count
  - Expectancy: `(win_rate * avg_win) + ((1 - win_rate) * avg_loss)`
- Signal breakdown: groups closed trades by `signals_at_entry[:20]`, reports wins/total/PnL per group, top 8 by PnL
- After text: attempts to import `monitoring.chart_generator._build_equity_curve` and send as photo

**`_handle_health(token, chat_id) -> None`**
- Inputs: token, chat_id
- System reads: `ps aux` (subprocess), `/proc/{pid}/status` (VmRSS), `shutil.disk_usage("/")`, `/proc/loadavg`
- DB reads: file size of closeloop, historical, frontier, deepdata DBs
- Log reads: last 2000 lines of `logs/quant_fund.log` for ERROR/CRITICAL
- Displays: PID, uptime, RAM MB, CPU load 1m, disk usage, DB sizes, last error line

**`_handle_collectors(token, chat_id) -> None`**
- Inputs: token, chat_id
- Log reads: last 3000 lines of `logs/quant_fund.log`, scanning in reverse for 13 collector class names
- DB reads (fallback for USASpendingCollector): `output/permanent_archive.db.raw_government_contracts` MAX(fetched_at)
- Collector names tracked: ShippingIntelligence, ConsumerIntelligence, GeopoliticalCollector, RatesCreditCollector, CommodityCollector, SECFullTextCollector, AlternativeQuiverCollector, TechnologyIntelligence, USASpendingCollector, BLSCollector, InsiderTransactionCollector, JobPostingsCollector, FinnhubCollector
- Log search aliases: CommodityCollector -> "Commodity Collector", ConsumerIntelligence -> "Consumer Intelligence", ShippingIntelligence -> "shipping_intelligence", GeopoliticalCollector -> "geopolitical_collector", FinnhubCollector -> "finnhub_collector"
- Displays: per-collector status (green OK or red error), last seen timestamp

**`_handle_intelligence(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_FRONTIER.umci_history` ORDER BY recorded_at DESC LIMIT 1; columns: level, umci, halt, position_mult, full_breakdown
- Parses `full_breakdown` JSON for HMM probability vector keys starting with "p_"
- Bar chart formula: `int(prob * 20)` blocks of "█" + remaining "░" to 20
- Falls back to individual columns: physical, social, scientific, financial, altdata
- After text: imports `monitoring.chart_generator._build_regime_chart` and sends as photo

**`_handle_errors(token, chat_id) -> None`**
- Inputs: token, chat_id
- Log reads: full `logs/quant_fund.log`, filtering lines containing today's UTC date string AND ("ERROR" or "CRITICAL")
- Displays: last 20 error lines (truncated to 120 chars each)

**`_handle_regime(token, chat_id) -> None`**
- Inputs: token, chat_id
- Imports: `intelligence.reasoning_engine.ReasoningEngine` (instantiated fresh each call)
- DB reads: `DB_FRONTIER.umci_history` ORDER BY recorded_at DESC LIMIT 1; `DB_FRONTIER.umci_history` WHERE recorded_at >= date('now', '-7 days') ORDER BY recorded_at ASC LIMIT 50
- Confidence calculation: `min((umci / 10.0), 1.0)` — converts raw UMCI score to 0-1 range
- Calls `re_engine.interpret_regime(regime, confidence)` for plain_english, sizing_impact, active_signals, suppressed_signals
- Regime log: deduplicates to last reading per day for 7 days

**`_handle_pairs(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.pairs_signals` ORDER BY updated_at DESC LIMIT 20; fallback to `_db_path().pairs_signals` ORDER BY signal_date DESC LIMIT 10
- Columns: ticker_a, ticker_b, zscore (or z_score), half_life (or half_life_days), p_value, signal_strength (or signal)
- Displays: table of up to 10 pairs with Z-score, half-life, p-value, signal

**`_handle_factors(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `_db_path().factor_exposures` ORDER BY run_date DESC LIMIT 20
- Columns: ticker, beta_mkt, beta_smb, beta_hml, beta_mom, beta_rmw, beta_cma, alpha, r_squared
- Deduplicates by ticker (first occurrence only)
- Displays: Fama-French 6-factor exposures table per ticker

**`_handle_options(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_DEEPDATA.options_flow` ORDER BY collected_at DESC LIMIT 20
- Columns: ticker (or symbol), put_call_ratio (or pcr), iv_percentile (or iv_rank), unusual_activity (or is_unusual), sentiment_score (or options_smfi)
- After table: attempts `monitoring.chart_generator._build_signal_performance_chart` and sends as photo

**`_handle_insider(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.insider_transactions` ORDER BY transaction_date DESC LIMIT 20; fallback to `_db_path().insider_transactions` ORDER BY stored_at DESC LIMIT 20
- Columns: ticker, reporter_name (or insider_name), reporter_title (or title), transaction_type, shares, total_value (or value_usd), transaction_date

**`_handle_kalman(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.price_history` WHERE ticker='SPY' ORDER BY date DESC LIMIT 60, columns: close, date
- Kalman filter parameters (hardcoded):
  - Q = 1e-5 (process noise)
  - R = 1e-2 (measurement noise)
  - Initial state: x = closes[0], P = 1.0
- Update equations per observation z:
  - `x_pred = x`
  - `P_pred = P + Q`
  - `K = P_pred / (P_pred + R)`  (Kalman gain)
  - `innov = z - x_pred`  (innovation)
  - `x = x_pred + K * innov`
  - `P = (1 - K) * P_pred`
- Trend: bullish if 5-day avg innovation > 0, bearish if < 0, neutral if == 0
- Requires minimum 10 rows of SPY data (5 valid prices)

**`_handle_wavelet(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.price_history` WHERE ticker='SPY' ORDER BY date DESC LIMIT 128, column: close
- Primary: uses `pywt.wavedec(closes, "db4", level=4)` returning [cA4, cD4, cD3, cD2, cD1]
  - A4 trend: cA4[-1] > cA4[-2] → Up, else Down
  - D3 position: cD3[-1] > 0.5 → overbought, < -0.5 → oversold, else neutral
  - D4 momentum: cD4[-1] > 0 → positive, else negative
  - Dominant cycle hardcoded as 2^3 = 8-day period
- Fallback (pywt not installed): 5d MA vs 20d MA comparison
- Requires minimum 32 rows

**`_handle_shipping(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.shipping_data` ORDER BY date DESC LIMIT 1
- Columns: bdi_value, shipping_stress_index, stress_regime, bdi_zscore_252, date

**`_handle_commodities(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.commodity_prices` ORDER BY date DESC LIMIT 100
- Columns: symbol, close (or adj_close), date
- Deduplicates by symbol, shows first 20 unique

**`_handle_db_stats(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: opens closeloop, historical, frontier, deepdata, intelligence DBs; counts tables via `sqlite_master`
- Shows: table count, file size in MB, last modified timestamp

**`_handle_force_report(token, chat_id) -> None`**
- Inputs: token, chat_id
- Imports `monitoring.weekly_report.WeeklyReportGenerator`, instantiates, calls `send_weekly_report()`
- NOTE: `send_weekly_report()` has a Sunday-only guard — calling from non-Sunday returns without sending sections

**`_handle_weekly_report(token, chat_id) -> None`**
- Simply delegates to `_handle_force_report`

**`_handle_news(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `DB_HISTORICAL.news_context` ORDER BY published_date DESC LIMIT 20
- Columns: ticker, headline, published_date, sentiment_raw

**`_handle_risk(token, chat_id) -> None`**
- Inputs: token, chat_id
- DB reads: `_db_path().trade_ledger` WHERE exit_date IS NULL AND is_phantom=0, columns: ticker, sector, position_size
- Calculates sector concentration: count per sector / total positions
- Flags sectors where percentage > 15% with "HIGH" warning

**`_handle_trade_explanation(token, chat_id, ticker) -> None`**
- Inputs: token, chat_id, ticker (str, will be uppercased)
- DB reads:
  - `_db_path().trade_ledger` WHERE ticker=? ORDER BY entry_date DESC LIMIT 1
  - `_db_path().trade_ledger` SELECT DISTINCT ticker (for case-insensitive fallback check)
  - `DB_HISTORICAL.price_history` WHERE ticker=? ORDER BY date DESC LIMIT 1, column: close
  - `_db_path().pnl_attribution` WHERE trade_id=?
  - `DB_HISTORICAL.news_context` WHERE ticker=? ORDER BY published_date DESC LIMIT 3
- Imports: `intelligence.reasoning_engine.ReasoningEngine`
- Calls: `re_engine.build_trade_context(ticker, trade_data, signals, macro_regime, news_rows)`
- LLM explanation: runs `re_engine.llm_explain(ctx, prompt)` in a new asyncio event loop with 20-second timeout
- Attribution display: shows was_signal_correct, signal_strength_at_entry, attributed_pnl for up to 5 attribution rows

**`_route_nlp(text) -> Optional[str]`**
- Inputs: text (str)
- Outputs: action code string or None
- Case-insensitive keyword matching against `_KEYWORD_ROUTES` list (20 entries)

**`_is_trade_explanation(text) -> Optional[str]`**
- Inputs: text (str)
- Outputs: ticker string or None
- Regex patterns (applied to uppercased text):
  - `WHY DID YOU (?:BUY|SELL) ([A-Z]{1,6}(?:\.[A-Z]+)?)`
  - `TRADE ANALYSIS\s+([A-Z]{1,6}(?:\.[A-Z]+)?)`
  - `(?:WHY|EXPLAIN|ANALYSE|ANALYZE|ANALYSIS)\s+([A-Z]{1,6}(?:\.[A-Z]+)?)`

**`_dispatch_action(token, chat_id, action) -> None`**
- Inputs: token, chat_id, action (str)
- Routes 18 known action codes to handler functions
- `nlp_no_trades` is a lambda that sends a static message

#### Class: `PrivateBot`

**`__init__(self, token, chat_id)`**
- Stores token, chat_id; initializes _offset=0, _running=False

**`_get_updates(self, timeout=20) -> list`**
- Calls `getUpdates` with long-poll timeout, allowed_updates: ["message", "callback_query"]

**`_handle_update(self, update) -> None`**
- Routes callback_query events (button presses) and message events
- Security: ignores updates from any chat_id not matching self._chat_id
- Both callback and message handlers run in daemon sub-threads

**`run(self) -> None`**
- Main long-polling loop; sends startup message on launch
- Exponential backoff on errors: `min(2 ** consecutive_errors, 60)` seconds

**`stop(self) -> None`**
- Sets _running = False

**`start_private_bot(config) -> Optional[PrivateBot]`**
- Module-level factory
- Reads token from `config.notifications.telegram.bot_token`
- Reads chat_id from `config.notifications.telegram.chat_id`, falls back to hardcoded PRIVATE_CHAT_ID
- Starts PrivateBot.run as daemon thread named "apollo-private-bot"

### C) MATHEMATICS

**Kalman filter (SPY smoothing):**
- Prediction: `x_pred = x`, `P_pred = P + Q` where Q=1e-5
- Update: `K = P_pred / (P_pred + R)` where R=1e-2
- State: `x = x_pred + K * (z - x_pred)`, `P = (1 - K) * P_pred`
- Trend: sign of `sum(innovations[-5:]) / 5`

**Sharpe ratio (performance handler):**
- Groups closed trades by exit_date, sums net_pnl per day
- `mean = sum(returns) / n`
- `var = sum((r - mean)^2 for r in returns) / (n - 1)`
- `std = sqrt(var)`
- `sharpe = (mean / std) * sqrt(252)` if std > 0

**Max drawdown (performance handler):**
- Cumulative PnL series from sorted closed trades
- `dd = (peak - v) / 100_000.0` where 100_000 is hardcoded initial capital
- `max_dd = max(dd values)`

**Expectancy:**
- `expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)`

**Live position PnL:**
- Long: `(current_price - entry_price) / entry_price * 100`
- Short: `(entry_price - current_price) / entry_price * 100`

**Regime confidence conversion:**
- `confidence = min(umci / 10.0, 1.0)` in _handle_regime

### D) DATA FLOWS

**Reads:**
- `closeloop/storage/closeloop.db` OR `closeloop_data.db`: trade_ledger (open/closed positions, PnL), factor_exposures, pairs_signals, signals_log, pnl_attribution, signal_regime_performance, insider_transactions (fallback)
- `frontier/storage/frontier.db`: umci_history (regime data)
- `output/historical_db.db`: price_history, pairs_signals, insider_transactions, news_context, shipping_data, commodity_prices, rates_signals, macro_context
- `deepdata/storage/deepdata.db`: options_flow
- `output/intelligence_db.db`: (path defined but _handle_db_stats only)
- `output/permanent_archive.db`: raw_government_contracts (USASpendingCollector fallback)
- `logs/quant_fund.log`: error scanning, collector status
- `output/bot_status.json`: phase, last cycle timestamp
- `/proc/{pid}/status`: RAM usage
- `/proc/loadavg`: CPU load
- Alpaca Paper API: live position current_price (via AlpacaPaperBroker)

**Writes:**
- None — all data access is read-only (confirmed by docstring)

### E) DEPENDENCIES

**Internal Apollo imports (attempted at runtime):**
- `execution.broker_interface.AlpacaPaperBroker` (live price enrichment in positions)
- `intelligence.reasoning_engine.ReasoningEngine` (regime interpretation, trade explanation)
- `monitoring.chart_generator._build_equity_curve` (performance chart)
- `monitoring.chart_generator._build_regime_chart` (intelligence chart)
- `monitoring.chart_generator._build_signal_performance_chart` (options chart)
- `monitoring.weekly_report.WeeklyReportGenerator` (force report)
- `analysis.mathematical_signals` (not directly, but via ReasoningEngine)

**External libraries:**
- `yaml` (config loading)
- `json` (Telegram payloads, signal parsing)
- `sqlite3` (DB access)
- `threading` (daemon threads for handlers)
- `urllib.request`, `urllib.parse` (Telegram API — no requests library)
- `numpy` (Kalman handler)
- `pywt` (wavelet handler, optional with fallback)
- `subprocess` (ps aux for PID detection)
- `re` (trade explanation pattern matching, phase extraction)
- `shutil` (disk usage)

### F) WIRING STATUS

Connected to live trading path via:
- Started as daemon thread by `start_private_bot()` which is called from `main.py` after bot initialization
- Reads live-ish data from the same SQLite databases that the trading bot writes to
- Attempts live Alpaca position data enrichment via AlpacaPaperBroker on every /positions query
- Does NOT receive callbacks from the trading engine — it polls DBs independently
- All handler threads are fire-and-forget daemon threads with no response handling

### G) ISSUES FOUND

1. **btn_weekly_report Sunday guard bypassed inconsistently**: `_handle_weekly_report` calls `_handle_force_report` which calls `send_weekly_report()` without `force=True`. `send_weekly_report()` has a Sunday-only guard (weekday == 6 check). So pressing "Weekly Report" button on any day other than Sunday silently returns without sending anything. No feedback to user that the report was skipped. The "Force Report" button has the same path. The button is labeled "Force Report" implying it should override the day guard, but it does not.

2. **_db_path() size threshold too small**: Uses `> 100 bytes` as the threshold. A freshly-created but empty SQLite file with only header is 0 bytes; a minimal DB with schema but no data could be much larger. This threshold may cause stale fallback to `DB_CLOSELOOP_DATA` if `closeloop.db` exists but is very small.

3. **Positions: live_pnl_pct fallback uses stored pnl_pct**: When no live price is available, `live_pnl_pct` is set to the stored `pnl_pct` column, which may be stale from entry time and does not reflect current market moves. The label "live PnL" is therefore misleading when live prices fail.

4. **Performance Sharpe uses daily PnL in absolute £, not returns**: Sharpe is calculated on daily absolute PnL values (£), not on percentage returns relative to capital. This produces a dimensional result (£/£ * sqrt(252) is unit-less coincidentally, but the interpretation is wrong — it is not comparable to a standard Sharpe ratio which uses return rates).

5. **Max drawdown denominator is hardcoded £100,000**: Hard-coded `_INITIAL_CAPITAL = 100_000.0` regardless of actual Alpaca paper account value or phase-adjusted capital. If the account has grown or shrunk, the drawdown calculation will be wrong.

6. **_handle_regime confidence division**: `confidence = min(umci / 10.0, 1.0)` assumes UMCI score ranges 0-10. If UMCI is stored as a percentage (0-100), confidence will always be capped at 1.0 and information is lost.

7. **AlpacaPaperBroker import in handler thread**: `_handle_positions` attempts `from execution.broker_interface import AlpacaPaperBroker` inside a handler daemon thread. If this import takes time or raises (e.g., during module init), the exception is silently caught and `live_price_map` is empty. No user feedback on Alpaca failure.

8. **_handle_collectors uses log scanning, not live state**: The collector status is derived entirely from text parsing of the last 3,000 log lines. If a collector logged successfully 3,001 lines ago, it will show as "no data" (⚪). This is stale by definition.

9. **nlp_no_trades action is a static message**: `"nlp_no_trades"` lambda returns a hardcoded string "scan results not cached yet" regardless of actual state. There is no actual check of why no trades are being made.

10. **_is_trade_explanation pattern "ANALYSIS" word boundary missing**: The pattern `(?:WHY|EXPLAIN|ANALYSE|ANALYZE|ANALYSIS)\s+([A-Z]{1,6})` will also match the word "ANALYSIS" in phrases like "PERFORMANCE ANALYSIS SPY", capturing "SPY" as a ticker even if user did not intend a trade explanation.

11. **LLM explain creates new asyncio event loop per call**: `_handle_trade_explanation` creates a new asyncio event loop with `asyncio.new_event_loop()` inside a handler thread. This could conflict with any existing event loop if the thread inherits one, and is wasteful. On timeout, the loop is closed but the task may still be running in the engine.

---

## FILE 2: /home/dannyelticala/quant-fund/monitoring/health_dashboard.py

### A) PURPOSE
Collects system health metrics every 5 minutes (configurable), writes to `logs/apollo_health_dashboard.log` and `output/dashboard.json` (both overwritten, not appended). Sends a daily Telegram summary at 09:00 UTC. Optionally receives live trading objects (paper_trader, regime_detector, pairs_trader) for in-memory state access, but falls back to DB reads if these are None.

### B) CLASSES AND METHODS

#### Module-level constants
- `_DASHBOARD_LOG` = "logs/apollo_health_dashboard.log"
- `_DASHBOARD_JSON` = "output/dashboard.json"
- `_DAILY_SENT_FILE` = "output/last_dashboard_telegram_date.txt"
- `_CLOSELOOP_DB` = "closeloop/storage/closeloop.db"

#### Class: `HealthDashboard`

**`__init__(self, config, store, paper_trader, regime_detector, pairs_trader, closeloop_db)`**
- Inputs: all optional; config dict, store object, paper_trader object, regime_detector object, pairs_trader object, closeloop_db path override
- Stores all as instance attributes; sets _running=False, _thread=None

**`generate(self) -> Dict[str, Any]`**
- Inputs: none
- Outputs: metrics dict with keys: timestamp, phase, real_trade_count, open_positions, today_pnl_usd, active_signals_today, pairs_active, regime_state, kalman_status, last_telegram_msg_id, disk_used_gb, disk_free_gb, disk_total_gb, ram_used_gb, ram_total_gb, cpu_load_1m (or cpu_pct)
- Calls all sub-collector methods

**`_get_phase_metrics(self) -> Dict[str, Any]`**
- DB reads: `closeloop.db.trade_ledger` COUNT WHERE exit_date IS NOT NULL AND (is_phantom=0 OR is_phantom IS NULL) AND exit_reason NOT IN ('phantom_cleanup','phantom_duplicate','superseded')
- Phase determination thresholds: 0→PHASE_1, >=100→PHASE_2, >=300→PHASE_3, >=600→PHASE_4, >=1000→PHASE_5, >=2000→PHASE_FREE
- Outputs: {"phase": str, "real_trade_count": int}

**`_get_open_positions(self) -> int`**
- DB reads: `closeloop.db.trade_ledger` COUNT WHERE exit_date IS NULL
- NOTE: Does NOT filter is_phantom — counts both real and phantom open positions

**`_get_today_pnl(self) -> float`**
- DB reads: `closeloop.db.trade_ledger` SUM(gross_pnl) WHERE date(exit_date) = today
- Uses gross_pnl, not net_pnl

**`_get_signal_count(self) -> int`**
- DB reads: `closeloop.db.signals_log` COUNT WHERE date(timestamp) = today

**`_get_pairs_count(self) -> int`**
- DB reads: `closeloop.db.cointegration_log` COUNT WHERE status='valid'

**`_get_regime(self) -> str`**
- If self._regime_detector is not None: calls `self._regime_detector.detect()` (live in-memory state)
- Otherwise returns "UNKNOWN"

**`_get_kalman_status(self) -> str`**
- Attempts `import pykalman`; if available → "active"
- Falls back to importing `analysis.mathematical_signals.KalmanSignalSmoother` → "available (pykalman missing — graceful degradation)"
- Otherwise → "unavailable"

**`_get_last_telegram_id(self) -> Optional[int]`**
- File reads: `output/last_telegram_msg_id.txt`
- Outputs: int or None

**`_get_system_resources(self) -> Dict[str, Any]`**
- File reads: `/proc/meminfo` (MemTotal, MemAvailable), `/proc/loadavg`
- `shutil.disk_usage("/")`
- Fallback for CPU: `psutil.cpu_percent(interval=0)` if /proc/loadavg fails

**`write(self, metrics=None) -> None`**
- Inputs: optional pre-generated metrics dict
- File writes: `output/dashboard.json` (overwrite), `logs/apollo_health_dashboard.log` (overwrite)
- Calls `generate()` if metrics is None

**`send_daily_telegram(self, metrics=None) -> None`**
- Guard: reads `output/last_dashboard_telegram_date.txt`; returns if today already sent
- Calls `generate()` if metrics is None
- Uses `requests.post` to Telegram API
- On success: writes today's date to `output/last_dashboard_telegram_date.txt`

**`start_background(self, interval_seconds=300) -> None`**
- Starts daemon thread named "HealthDashboard"
- Thread calls `_loop` with interval

**`stop(self) -> None`**
- Sets _running = False

**`_loop(self, interval) -> None`**
- Calls `generate()` then `write()` every `interval` seconds
- Calls `send_daily_telegram()` if `datetime.utcnow().hour == 9`

### C) MATHEMATICS

**RAM calculation:**
- `used_kb = MemTotal_kb - MemAvailable_kb`
- `ram_used_gb = used_kb * 1024 / 1e9`  (note: MemTotal in /proc/meminfo is already in kB, multiplying by 1024 converts to bytes, then /1e9 to GB — this is correct)

### D) DATA FLOWS

**Reads:**
- `closeloop/storage/closeloop.db`: trade_ledger (phase, open positions, today PnL), signals_log (signal count), cointegration_log (pairs count)
- `/proc/meminfo`, `/proc/loadavg` (system resources)
- `/` via shutil (disk usage)
- `output/last_dashboard_telegram_date.txt` (dedup guard)
- `output/last_telegram_msg_id.txt` (last Telegram message ID)

**Writes:**
- `output/dashboard.json` (overwrite every cycle)
- `logs/apollo_health_dashboard.log` (overwrite every cycle)
- `output/last_dashboard_telegram_date.txt` (after successful daily Telegram send)
- Telegram API (sendMessage)

### E) DEPENDENCIES

**Internal Apollo imports:**
- `analysis.mathematical_signals.KalmanSignalSmoother` (kalman status check only)
- `regime_detector` passed in constructor (optional)

**External libraries:**
- `json`, `sqlite3`, `threading`, `time`, `shutil`, `os`
- `requests` (Telegram daily summary — note: private_bot.py uses urllib, this uses requests)
- `pykalman` (optional, just import-tested)
- `psutil` (CPU fallback)
- `rich.console.Console` (not imported here — logging only via logger)

### F) WIRING STATUS

- Started by trading_bot.py or main.py passing live objects (paper_trader, regime_detector, pairs_trader)
- If live objects passed: `_get_regime()` uses in-memory regime state (fresh)
- If live objects NOT passed (None): `_get_regime()` returns "UNKNOWN" — no DB fallback for regime
- Regime detection inconsistency: private_bot.py reads frontier DB for regime; health_dashboard uses live object or returns UNKNOWN
- Connected to live trading path: yes, via daemon thread started from main process

### G) ISSUES FOUND

1. **`_get_open_positions` counts phantom positions**: No `is_phantom=0` filter. `SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL` includes phantom open positions. The displayed "Open Positions" count will be inflated.

2. **`_get_today_pnl` uses `gross_pnl` not `net_pnl`**: All other PnL displays in Apollo use `net_pnl`. The health dashboard daily PnL uses `gross_pnl`, which does not account for commissions/fees.

3. **`_get_regime()` returns "UNKNOWN" without DB fallback**: If the regime_detector object is not passed (None), it returns "UNKNOWN" rather than querying the frontier DB. The daily Telegram will show "UNKNOWN" for regime unless the live object is wired in.

4. **`send_daily_telegram` uses `requests` library**: The rest of the monitoring module (private_bot.py, weekly_report.py) uses `urllib` for Telegram API calls to avoid extra dependencies. health_dashboard.py uses `requests`, introducing an inconsistent dependency.

5. **Daily send guard is file-based with no locking**: Two concurrent calls to `send_daily_telegram` at exactly 09:00 UTC could both read the file before either writes, causing duplicate sends.

6. **Phase thresholds differ from health_reporter**: `_get_phase_metrics` uses exit_date IS NOT NULL AND is_phantom IS NULL filter, which is slightly different from other phase calculations in the codebase. Inconsistency in what counts as a "real trade."

---

## FILE 3: /home/dannyelticala/quant-fund/monitoring/monitor_runner.py

### A) PURPOSE
Orchestrates all background monitoring threads in a single 60-second heartbeat loop. Manages scheduling of: daily health report (6am UTC), weekly report (DISABLED — commented out), 6-hour self-diagnostics, per-minute alert checks, and 5-minute Telegram retry queue processing.

### B) CLASSES AND METHODS

#### Module-level constants
- `_UK_DAILY_REPORT_UTC_HOUR` = 6
- `_UK_WEEKLY_REPORT_UTC_HOUR` = 6 (weekly report is DISABLED/commented out)
- `_DIAGNOSTIC_INTERVAL_HOURS` = 6
- `_ALERT_CHECK_INTERVAL_SEC` = 60
- `_RETRY_INTERVAL_MIN` = 5
- `_QUIET_START_UTC` = 22
- `_QUIET_END_UTC` = 7

**`_is_quiet_hours(hour) -> bool`**
- Inputs: hour (int, UTC)
- Outputs: True during 22:00-07:00 UTC
- Formula: `hour >= 22 or hour < 7`

#### Class: `MonitorRunner`

**`__init__(self, config, stream_worker=None)`**
- Stores config dict, stream_worker reference
- Creates threading.Event() for stop signal
- Scheduling state: _last_daily_report_date, _last_weekly_report_week, _last_diagnostic_hour, _last_retry_min, _last_alert_ts

**`start(self) -> None`**
- Starts daemon thread named "monitor-runner"

**`stop(self) -> None`**
- Sets stop event

**`is_alive(self) -> bool`**
- Returns True if thread exists and is alive

**`set_stream_worker(self, worker) -> None`**
- Updates stream_worker reference (allows late registration from trading bot)

**`_run(self) -> None`**
- Main loop: calls `_tick()` every 60 seconds via `_stop.wait(timeout=60)`

**`_tick(self) -> None`**
- Inputs: none (reads `datetime.now(timezone.utc)`)
- Task 1: Daily health report — fires at hour==6, minute<2, once per date string; calls `send_daily_report(config)` in spawn thread
- Task 2: Weekly report — DISABLED (commented out); formerly Sunday 6am UTC
- Task 3: Self-diagnostic — fires every 6 hours at HH:00 (diag_slot = `(hour // 6) * 6`), minute<2; calls `run_diagnostic(config, quiet)` in spawn thread; quiet=True during 22:00-07:00
- Task 4: Alert checks — fires every 60s elapsed; calls `alert_monitor.run_all_checks(config, stream_worker)`
- Task 5: Retry queue — fires when `minute % 5 == 0` and minute != last retry minute; calls `_process_retry_queue()`

**`_process_retry_queue(self) -> None`**
- Calls `telegram_logger.pop_retry_queue()` to get failed messages
- Initializes `Notifier(config)` from `altdata.notifications.notifier`
- Attempts `n._send_telegram(message)` for each queued item
- If delivered: calls `telegram_logger.log_message(type+"_retry", message, True)`
- If not delivered: calls `telegram_logger.queue_retry(type, message)` to re-queue
- If 401 Unauthorized detected: drops ALL remaining queued items (no infinite retry)
- If Notifier fails to init: re-queues all items

**`_spawn(fn, *args) -> None`** (static)
- Starts short-lived daemon thread for fn(*args)

#### Module-level functions

**`start_monitoring(config, stream_worker=None) -> MonitorRunner`**
- Singleton pattern via `_runner` module-level variable
- If existing runner is alive: updates stream_worker if provided, returns existing runner
- Otherwise: creates new MonitorRunner, starts it, returns it

**`get_monitor_runner() -> Optional[MonitorRunner]`**
- Returns current `_runner` module global

### C) MATHEMATICS

**Diagnostic slot calculation:**
- `diag_slot = (now.hour // 6) * 6`
- Fires at hours 0, 6, 12, 18 UTC (four times per day)

### D) DATA FLOWS

**Reads:**
- `telegram_logger.pop_retry_queue()` (internal queue state)
- `alert_monitor.run_all_checks()` (reads from DBs indirectly)

**Writes:**
- `telegram_logger.log_message()` (retry log entries)
- `telegram_logger.queue_retry()` (re-queues failed messages)
- Telegram API (via Notifier._send_telegram in retry processor)

### E) DEPENDENCIES

**Internal Apollo imports:**
- `monitoring.alert_monitor` (alert checks every 60s)
- `monitoring.telegram_logger` (retry queue pop/push/log)
- `monitoring.health_reporter.send_daily_report` (daily report)
- `monitoring.health_reporter.send_weekly_report` (imported but disabled)
- `monitoring.self_diagnostic.run_diagnostic` (6-hour diagnostic)
- `altdata.notifications.notifier.Notifier` (Telegram retry sending)

**External libraries:**
- `threading`, `time`, `logging`
- `datetime` (timezone.utc)

### F) WIRING STATUS

- Started from main.py / trading_bot.py via `start_monitoring(config, stream_worker)`
- The weekly report scheduling is DISABLED — a comment in the code says "DISABLED: Weekly report now sent exclusively from trading_bot.py Sunday 09:00 UTC"
- Alert checks receive the `stream_worker` reference, giving alert_monitor access to live streaming state
- `set_stream_worker` allows the trading bot to register the stream worker after MonitorRunner is already started

### G) ISSUES FOUND

1. **Weekly report is doubly scheduled (disabled here, active in trading_bot)**: The comment says weekly report is now handled by trading_bot.py. This means there is a hidden dependency — if trading_bot.py's scheduling logic is changed or removed, no weekly reports would be sent. The disabled code creates confusion about where scheduling responsibility lies.

2. **Retry queue uses `minute % 5 == 0` with minute dedup**: If the bot loop sleeps for >5 minutes (error backoff), it could skip a retry window. The dedup on `minute` means only one attempt per exact minute value (e.g., minute=0 only fires once per hour at XX:00, not every 5 minutes strictly).

3. **`_process_retry_queue` creates fresh `Notifier` per call**: Each 5-minute retry cycle instantiates a new `Notifier(config)`, which reads config every time. This is safe but wasteful.

4. **Alert checks run on main monitor thread, not spawned**: `alert_monitor.run_all_checks()` is called directly in `_tick()`, not in a `_spawn()` thread. If alert checks are slow, they block the 60-second heartbeat loop, potentially delaying other scheduled tasks.

---

## FILE 4: /home/dannyelticala/quant-fund/monitoring/weekly_report.py

### A) PURPOSE
Generates and sends an 8-section weekly performance report to Telegram every Sunday at 09:00 UTC. All database access is read-only. Report covers: performance summary, signal performance, data collection health, phase progress, weaknesses, regime intelligence, top opportunities, and next-week outlook. Can be manually triggered but has a Sunday-only guard.

### B) CLASSES AND METHODS

#### Module-level constants
- `DB_CLOSELOOP` = `closeloop/storage/closeloop.db`
- `DB_CLOSELOOP_DATA` = `closeloop_data.db`
- `DB_HISTORICAL` = `output/historical_db.db`
- `DB_PERMANENT` = `output/permanent_log.db`
- `DB_FRONTIER` = `frontier/storage/frontier.db`
- `DB_DEEPDATA` = `deepdata/storage/deepdata.db`
- `DB_INTELLIGENCE` = `output/intelligence_db.db`
- `START_DATE` = "2026-04-03"
- `SIGNAL_TYPES` = ["pead", "momentum", "mean_reversion", "gap", "pairs", "options_flow", "insider", "wavelet", "kalman"]
- `_BASE_EQUITY` = 100_000.0

#### Module-level functions

**`_esc(text) -> str`**
- Escapes underscores for Telegram Markdown v1 (`_` → `\_`)

**`_open_db(path) -> sqlite3.Connection`**
- Opens SQLite with timeout=10, sets WAL mode, returns connection

**`_safe_query(path, sql, params) -> list`**
- Read-only query wrapper; returns [] on any error

**`_safe_scalar(path, sql, params, default)`**
- Returns first column of first row or default

**`_week_bounds() -> tuple[str, str]`**
- Returns (week_start, week_end) as ISO date strings
- `week_end` = today UTC, `week_start` = today - 7 days

**`_sharpe(returns) -> float`**
- Inputs: list of daily return values
- Annualised Sharpe: `(mean / std) * sqrt(252)` with sample std dev (n-1)
- Returns 0.0 if fewer than 2 values or std == 0

**`_max_drawdown(cum_pnl) -> float`**
- Inputs: cumulative PnL list
- Formula: `dd = (peak - v) / (_BASE_EQUITY + max(peak, 0.0))`
- Uses `_BASE_EQUITY + peak` as denominator to avoid near-zero division when peak is near zero
- Returns max dd fraction (0.0 to 1.0)

**`_load_config() -> dict`**
- Reads `config/settings.yaml`

**`_send_telegram(token, chat_id, text) -> bool`**
- Sends via urllib (no requests); truncates to 4000 chars; parse_mode="Markdown"

**`_section_1_performance(week_start, week_end) -> str`**
- DB reads: `DB_CLOSELOOP.trade_ledger` (week trades AND all-time trades since START_DATE)
  - Filter: `exit_date BETWEEN ? AND ?`, `is_phantom=0`, `ABS(net_pnl)>0.01`, `order_status != 'superseded'`
- Calculates: total_pnl_week, win_rate, avg_hold_days (prefers holding_days column, falls back to date arithmetic), best/worst trade by pnl_pct, daily returns → Sharpe, cumulative series → max drawdown
- SPY comparison: downloads via `yfinance.download("SPY", ...)` if available
- `pnl_pct_week = (total_pnl_week / 100000) * 100` (hardcoded £100k base)
- `avg_hold` displayed as hours if < 1 day, else days
- Displays: 1/8 performance section

**`_section_2_signal_performance(week_start, week_end) -> str`**
- DB reads:
  - `DB_CLOSELOOP.signal_regime_performance` WHERE last_updated >= week_start
  - `DB_CLOSELOOP.pnl_attribution` JOIN trade_ledger WHERE exit_date in range and is_phantom=0
  - Fallback: `DB_CLOSELOOP.trade_ledger` signals_at_entry JSON parsing
- Signal types tracked: 9 defined in SIGNAL_TYPES
- Fires, win rate, total PnL per signal; best/worst signal by win rate

**`_section_3_data_health() -> str`**
- DB reads: counts from closeloop.trade_ledger, historical.price_history, frontier.raw_signals, deepdata.options_flow (total rows and today's rows)
- `DB_FRONTIER.raw_signals` WHERE collected_at >= date('now', '-7 days') (count)
- Log reads: full `logs/quant_fund.log` scanning for rate limit events (429/rate limit) and ERROR/CRITICAL today
- Displays: DB row counts, frontier signal count, rate limit events, error count

**`_section_4_phase_progress(week_start, week_end) -> str`**
- DB reads: `DB_CLOSELOOP.trade_ledger` COUNT DISTINCT (ticker || entry_date) WHERE is_phantom=0 AND ABS(net_pnl)>0.01 (all-time and since week_start)
- Phase thresholds: 1→0-99, 2→100-299, 3→300-599, 4→600-999, 5→1000-1999, FREE→2000+
- `daily_avg = week_real / 7`
- `days_to_next = trades_to_next / daily_avg`
- Phantom count: `SELECT COUNT(*) FROM trade_ledger WHERE is_phantom=1`

**`_section_5_weaknesses(week_start, week_end) -> str`**
- DB reads: `DB_CLOSELOOP.trade_ledger` losers (net_pnl < -0.01, not superseded) and all_trades
- Analyses: signal win rate by signals_at_entry[:20] (flags < 40%), sector losses, long vs short win rates, most losses by macro_regime
- Long detection: `str(direction) in ("1", "long", "buy")`
- Short detection: `str(direction) in ("-1", "short", "sell")`

**`_section_6_regime_intelligence(week_start, week_end) -> str`**
- DB reads: `DB_FRONTIER.umci_history` WHERE recorded_at >= week_start ORDER BY ASC; `DB_HISTORICAL.rates_signals` ORDER BY calc_date DESC LIMIT 1
- Deduplicates to one reading per day, counts regime transitions
- Displays: daily regime log, transition count, current regime/multiplier/halt, yield curve slope, rates regime, HY spread

**`_section_7_opportunities(week_start, week_end) -> str`**
- DB reads: `DB_FRONTIER.signal_log` WHERE generated_at >= week_start ORDER BY confidence DESC LIMIT 10
- Fallback: `DB_CLOSELOOP.trade_ledger` GROUP BY ticker ORDER BY MAX(pnl_pct) DESC LIMIT 5
- Displays: top 5 signals with ticker, signal_name, confidence, outcome_return

**`_section_8_outlook() -> str`**
- DB reads: `DB_FRONTIER.umci_history` ORDER BY recorded_at DESC LIMIT 1; `DB_CLOSELOOP.trade_ledger` COUNT DISTINCT ticker WHERE exit_date IS NULL AND is_phantom=0; `DB_CLOSELOOP.trade_ledger` ticker+sector for open positions; `DB_HISTORICAL.macro_context` ORDER BY date DESC LIMIT 1
- UMCI level mapped to regime label via `_umci_level_to_regime(level)`
- yfinance fallback: looks up sector for tickers missing sector column (up to 20 tickers)
- Displays: regime implications, open positions, sector distribution with 15% concentration warning, VIX level

**`_umci_level_to_regime(level) -> str`**
- Maps: LOW→NEUTRAL, MEDIUM→NEUTRAL, ELEVATED→RISK_OFF, HIGH→RISK_OFF, EXTREME→RISK_OFF, CRITICAL→CRISIS, CRISIS→CRISIS; default NEUTRAL

#### Class: `WeeklyReportGenerator`

**`__init__(self)`**
- Loads config, extracts telegram token and chat_id (default "8508697534")

**`generate_report(self) -> list[str]`**
- Calls all 8 section functions, returns list of 8 strings

**`send_weekly_report(self, force=False) -> None`**
- Sunday-only guard: if `not force` and weekday != 6, logs and returns
- Sends each section as separate Telegram message
- Tracks success_count/8 sections

### C) MATHEMATICS

**Sharpe (weekly):**
- `mean = sum(daily_pnl_values) / n`
- `variance = sum((r - mean)^2) / (n-1)` (sample)
- `std = sqrt(variance)`
- `sharpe = (mean / std) * sqrt(252)`
- Note: calculated on absolute £ PnL values per day, not percentage returns

**Max drawdown:**
- `denominator = _BASE_EQUITY + max(peak, 0.0)` where `_BASE_EQUITY = 100_000.0`
- `dd = (peak - v) / denominator`
- This avoids divide-by-zero when peak is 0 but hardcodes £100k

**Weekly PnL percentage:**
- `pnl_pct_week = (total_pnl_week / 100000) * 100`

**Phase estimation:**
- `daily_avg = week_real / 7`
- `days_to_next = trades_to_next / daily_avg`

**Signal win rate:**
- `win_rate = wins / fires` per signal type

### D) DATA FLOWS

**Reads:**
- `closeloop/storage/closeloop.db`: trade_ledger (multiple queries), signal_regime_performance, pnl_attribution, signals_log (via signals_log table — NOTE: section 3 queries `DB_CLOSELOOP.signals_log` but section 4/5 queries use DB_CLOSELOOP)
- `frontier/storage/frontier.db`: umci_history, raw_signals, signal_log
- `output/historical_db.db`: price_history (row count), rates_signals, macro_context
- `deepdata/storage/deepdata.db`: options_flow (row count)
- `logs/quant_fund.log` (section 3 error/rate-limit scan)
- `config/settings.yaml` (config load)
- yfinance API (SPY data, sector lookups)

**Writes:**
- Telegram API (sendMessage per section)
- Nothing to disk

### E) DEPENDENCIES

**External libraries:**
- `json`, `logging`, `math`, `sqlite3`, `urllib.request`, `urllib.parse`
- `datetime`, `pathlib`, `typing`
- `yaml` (config load)
- `yfinance` (optional — SPY comparison and sector lookup with graceful failure)

### F) WIRING STATUS

- Triggered by `trading_bot.py` Sunday 09:00 UTC scheduler
- Can be triggered manually via `_handle_force_report` in private_bot.py (but Sunday guard applies)
- All data is read from DB — no live memory state access
- If DBs are empty (early operation), sections return "data unavailable" gracefully

### G) ISSUES FOUND

1. **Sunday guard on `send_weekly_report()` blocks "Force Report" button**: The `/force_report` Telegram button calls `send_weekly_report()` without `force=True`. On any non-Sunday the function silently returns. The user sees "Generating weekly report now..." but nothing is sent. This appears to be a design bug — the force report should pass `force=True`.

2. **`_week_bounds()` defines week as last 7 calendar days, not Mon-Sun**: A Sunday report covers the preceding 7 calendar days (Mon-Sun approximately), but the "week" concept is imprecise. Trades between last Sunday 00:00 and "today" 00:00 UTC are included; partial-day coverage at week boundaries.

3. **Section 2 signal tracking uses `signals_at_entry[:20]` not signal type**: Section 2 purports to show performance by signal type (pead, momentum, etc.), but the fallback path parses `signals_at_entry[:20]` as the signal name — which could be any arbitrary string stored in that column, not necessarily matching the 9 SIGNAL_TYPES list. If the column stores JSON or multi-signal strings, reporting will be incorrect.

4. **`DB_PERMANENT` = `output/permanent_log.db` but preflight_check.py uses `output/permanent_archive.db`**: The two monitoring files reference different paths for what appears to be the same permanent archive database. `weekly_report.py` defines `DB_PERMANENT = output/permanent_log.db`; `preflight_check.py` uses `_PERMANENT_DB = output/permanent_archive.db`. One of these paths may be wrong.

5. **Sharpe on absolute £ values (same as private_bot.py)**: Not a true risk-adjusted return metric. Two systems (private_bot._handle_performance and weekly_report._section_1) both compute Sharpe this way, making the displayed metric internally consistent but economically incorrect.

6. **SPY comparison does not adjust for portfolio beta**: Outperformance is calculated as raw `pnl_pct_week - spy_weekly`. This is not risk-adjusted alpha — it does not account for portfolio beta, leverage, or sector tilt.

7. **Section 7 uses `signal_log` table in `DB_FRONTIER`, Section 3 uses `raw_signals`**: Different table names for signals in the same frontier DB. The `signal_log` table (section 7) has `generated_at` and `confidence` columns; `raw_signals` (section 3) has `collected_at`. These may be different tables or the same table with different naming — unclear without schema inspection.

---

## FILE 5: /home/dannyelticala/quant-fund/monitoring/preflight_check.py

### A) PURPOSE
Three scheduled report generators called from the trading_bot.py main loop via `tick()`:
- `PreFlightChecker` — 13:00 UTC on trading days (90 min before US open): system readiness check
- `PreMarketScanner` — 14:00 UTC on trading days (30 min before US open): top signal scan
- `EndOfDayReporter` — 21:15 UTC on trading days (15 min after US close): daily trading summary

All save reports to disk and send via Telegram. Market day checking via `MarketCalendar` or weekday fallback.

### B) CLASSES AND METHODS

#### Module-level constants
- `_PREFLIGHT_DIR` = `logs/preflight`
- `_EOD_DIR` = `logs/eod_reports`
- `_ALPACA_STATUS` = `output/alpaca_status.json`
- `_CLOSELOOP_DB` = `closeloop/storage/closeloop.db`
- `_PERMANENT_DB` = `output/permanent_archive.db`
- `_HISTORICAL_DB` = `output/historical_db.db`
- `_ALTDATA_DB` = `output/altdata.db`

**`_COLLECTOR_CLASSES`** — list of 13 (module_path, class_name) pairs for all collectors

#### Module-level functions

**`_send_telegram(config, text) -> bool`**
- Uses `requests.post` to Telegram API
- Returns resp.ok

**`_is_trading_day(dt, market="us") -> bool`**
- Attempts `analysis.market_calendar.MarketCalendar().is_trading_day(market, dt)`
- Fallback: `dt.weekday() < 5` (Monday-Friday)

**`_collectors_healthy() -> tuple[int, int, list]`**
- For each of 13 collector class entries: attempts `importlib.import_module(mod_path)` and `getattr(mod, cls_name)`
- Returns (ok_count, total=13, failed_names)
- NOTE: Only tests importability, NOT whether collectors are actually running

**`_db_accessible(path) -> bool`**
- Opens DB with timeout=3, runs `PRAGMA integrity_check`

**`_get_alpaca_status(config) -> dict`**
- Reads api_keys.alpaca_api_key, api_keys.alpaca_secret_key from config
- Hits `{base_url}/v2/account` with headers
- If 200: returns equity, cash, connected=True; writes to `output/alpaca_status.json`
- If key contains "PASTE": returns connected=False, mode="no_key"

**`_get_phase_info() -> dict`**
- DB reads: `_CLOSELOOP_DB.trade_ledger` (real trade count, phantom count, open positions)
- Phase thresholds: 0→PHASE_1, >=100→PHASE_2, >=300→PHASE_3, >=600→PHASE_4, >=1000→PHASE_5, >=2000→PHASE_FREE
- Returns: phase, real_trades, phantom, open_pos, trades_to_next, next_phase

**`_get_cooling_off_count() -> int`**
- Attempts `from execution.cooling_off_tracker import CoolingOffTracker`
- Comment: "In-memory only — check the singleton if one exists / No persistent file, so return from DB fallback"
- Actually always returns 0 — no DB fallback implemented, import never used

**`_get_regime(config) -> str`**
- Instantiates `analysis.regime_detector.RegimeDetector(config)`, calls `.detect()`
- Returns "UNKNOWN" on failure

**`_get_spy_premarket() -> tuple[float, float]`**
- `yfinance.Ticker("SPY").history(period="2d", interval="1d")`
- Returns (last_close, pct_change) relative to prior day close

**`_count_signals_ready(config) -> int`**
- DB reads: `_PERMANENT_DB.signals_log` COUNT DISTINCT ticker WHERE timestamp >= datetime('now', '-24 hours')

**`_get_universe_size() -> int`**
- Reads `config/settings.yaml`, counts `universe.us_tickers` + `universe.uk_tickers`
- Fallback: `_HISTORICAL_DB.price_data` COUNT DISTINCT ticker

#### Class: `PreFlightChecker`

**`generate_report(self, config, trader=None) -> dict`**
- Inputs: config dict, optional trader object
- Calls: `_collectors_healthy()`, `_db_accessible()` for 4 DBs, `_get_alpaca_status()`, `_get_phase_info()`, `_get_regime()`, `_get_spy_premarket()`, `_count_signals_ready()`
- Bot PID: reads `output/bot.pid`, calls `os.kill(pid, 0)` to verify running
- Market gate: imports `execution.paper_trader._is_market_open("us")` — note: gate_active = NOT is_market_open (inverted)
- Returns dict with ~15 fields

**`format_message(self, report) -> str`**
- Inputs: report dict
- Outputs: formatted pre-flight string
- gate_line bug: both True and False branches produce "✅ Market Hours Gate: ACTIVE" (identical text regardless)

**`run(self, config, trader=None) -> bool`**
- Generates report, formats, saves to `logs/preflight/preflight_{ts}.txt`, sends Telegram

#### Class: `PreMarketScanner`

**`run(self, config, trader=None) -> bool`**
- Saves to `logs/preflight/prescan_{ts}.txt`, sends Telegram

**`_build(self, config, trader=None) -> dict`**
- DB reads: `_PERMANENT_DB.signals_log` WHERE timestamp >= datetime('now', '-24 hours') AND score >= 0.25 ORDER BY score DESC LIMIT 20
- Columns read: ticker, signal_type, score, direction
- Cooling count: `_CLOSELOOP_DB.trade_ledger` COUNT DISTINCT ticker WHERE exit_date >= date('now', '-2 days') AND gross_pnl < 0
- Position multiplier: introspects trader object for sizer.phase_multiplier
- Max new positions: `sizer.max_simultaneous_positions - phase["open_pos"]`

**`_format(self, report) -> str`**
- Confidence labels: score >= 0.6 → "High", >= 0.4 → "Medium", else "Watch"
- Shows top 10 of 20 signals

#### Class: `EndOfDayReporter`

**`run(self, config) -> bool`**
- Saves to `logs/eod_reports/eod_{ts}.txt`, sends Telegram

**`_build(self, config) -> dict`**
- DB reads: `_CLOSELOOP_DB.trade_ledger` (opened today, closed today, gross_pnl today, best/worst by gross_pnl)
- DB reads: `_PERMANENT_DB.signals_log` GROUP BY signal_type for today
- DB reads: `_PERMANENT_DB.macro_intelligence` COUNT WHERE date(stored_at) = today (subquery with LIMIT 10000)
- Calls `_collectors_healthy()`, `_get_phase_info()`, `_get_regime()`

**`_format(self, report) -> str`**
- Shows: trades opened/closed/PnL, best/worst, signals by type, open positions, phase, regime, new rows collected, collector health

#### Module-level scheduling

**`tick(config, now=None, trader=None) -> None`**
- Called every minute from trading_bot.py main loop
- Guards: only runs on trading days (via `_is_trading_day`)
- PreFlight: hour==13, minute<2, once per date
- PreScan: hour==14, minute==0, once per date  (NOTE: exact minute==0 only, not minute<2)
- EOD: hour==21, minute==15, once per date
- All three spawn daemon threads

**Module-level singletons:**
- `_preflight_checker = PreFlightChecker()` (instantiated at import)
- `_premarket_scanner = PreMarketScanner()` (instantiated at import)
- `_eod_reporter = EndOfDayReporter()` (instantiated at import)
- `_last_preflight_date`, `_last_prescan_date`, `_last_eod_date` (module-level state)

### C) MATHEMATICS

**Cooling count proxy in PreMarketScanner:**
- `COUNT DISTINCT ticker WHERE exit_date >= date('now', '-2 days') AND gross_pnl < 0`
- This is NOT the actual cooling-off tracker — it counts losing trades in last 2 days as a proxy

**Signal confidence tiers:**
- High: score >= 0.6
- Medium: 0.4 <= score < 0.6
- Watch: score < 0.4

### D) DATA FLOWS

**Reads:**
- `closeloop/storage/closeloop.db`: trade_ledger (phase, open positions, phantom count, EOD opened/closed/PnL/best/worst)
- `output/permanent_archive.db`: signals_log (24h signals, EOD signal types), macro_intelligence (new rows today)
- `output/historical_db.db`: price_data (universe size fallback)
- `output/altdata.db`: (only accessibility checked, no queries)
- Alpaca API: account status
- yfinance: SPY 2-day history
- `config/settings.yaml`: universe size, Telegram credentials
- `output/bot.pid`: PID verification

**Writes:**
- `output/alpaca_status.json` (after successful Alpaca ping)
- `logs/preflight/preflight_{ts}.txt` (preflight report)
- `logs/preflight/prescan_{ts}.txt` (pre-market scan)
- `logs/eod_reports/eod_{ts}.txt` (EOD report)
- Telegram API (sendMessage)

### E) DEPENDENCIES

**Internal Apollo imports:**
- `analysis.market_calendar.MarketCalendar` (trading day check)
- `analysis.regime_detector.RegimeDetector` (regime detection)
- `execution.paper_trader._is_market_open` (market gate check)
- `execution.cooling_off_tracker.CoolingOffTracker` (imported but never used)
- All 13 collector module/class pairs (import-tested only)

**External libraries:**
- `requests` (Telegram + Alpaca)
- `importlib` (dynamic collector import)
- `sqlite3`, `json`, `logging`, `os`
- `pathlib`, `typing`, `datetime`
- `yfinance` (SPY price)
- `yaml` (universe size)

### F) WIRING STATUS

- `tick()` is called from trading_bot.py main loop every minute
- Has access to `trader` object passed via `tick()` — PreMarketScanner uses it to read sizer.phase_multiplier
- PreFlight and EOD also accept trader but do not use it in the current implementation
- Connected to live trading path: yes, called synchronously from trading_bot's loop

### G) ISSUES FOUND

1. **`_get_cooling_off_count()` always returns 0**: The function body imports CoolingOffTracker (which is never used) and then returns 0. The comment says "No persistent file, so return from DB fallback" but no DB fallback is implemented. The preflight report always shows "0 active" cooling-off locks, which is misleading.

2. **`format_message` gate_line bug**: Both the `True` and `False` branches of the gate_line ternary produce the same string: `"✅ Market Hours Gate: ACTIVE"`. The false branch says "ACTIVE (market open)" with additional text but the same icon. The operator can never tell from the preflight message whether the gate is blocking or not.

3. **PreMarketScanner cooling count is not the actual CoolingOffTracker**: The cooling count is derived from `gross_pnl < 0 in last 2 days` — a crude proxy that does not match the actual CoolingOffTracker's in-memory state.

4. **PreScan fires only at exact minute==0**: `tick()` checks `now.minute == 0` for prescan (not `now.minute < 2` like the others). If the trading_bot's main loop happens to skip minute 0 exactly (e.g., is processing at 13:59:59 and next check is 14:00:30, then the minute check changes to 14:01), the prescan may not fire for that day.

5. **EOD report uses `gross_pnl` not `net_pnl`**: Consistent with health_dashboard but inconsistent with weekly_report and performance handler which use `net_pnl`.

6. **Collectors healthy check tests importability only**: `_collectors_healthy()` does `importlib.import_module` + `getattr` — this only verifies the class exists in the module, not that it is running, has data, or has connected to its data source.

7. **`_PERMANENT_DB` path differs from weekly_report.py**: preflight_check uses `output/permanent_archive.db`; weekly_report.py defines `DB_PERMANENT = output/permanent_log.db`. These are different paths and may point to different or nonexistent files.

8. **Module-level singleton instantiation at import time**: `_preflight_checker`, `_premarket_scanner`, `_eod_reporter` are all instantiated when the module is imported. If this module is imported in a context where logging or the DB is not yet ready, the init could silently fail.

---

## FILE 6: /home/dannyelticala/quant-fund/altdata/notifications/notifier.py

### A) PURPOSE
Multi-channel notification dispatcher for the alt-data pipeline. Supports four channels: terminal (ANSI/rich), log (JSON lines to `logs/alerts.log`), desktop (plyer OS notification), and Telegram (Bot API). Provides typed convenience methods for 8 notification trigger types. Used by MonitorRunner for retry queue processing and by various pipeline components for event notifications.

### B) CLASSES AND METHODS

#### Module-level constants
- `_LEVEL_COLOURS` = {"INFO": "green", "WARNING": "yellow", "ALERT": "bold red", "CRITICAL": "bold white on red"}
- `_LEVEL_ICONS` = {"INFO": "[i]", "WARNING": "[!]", "ALERT": "[!!]", "CRITICAL": "[!!!]"}

#### Module-level optional imports
- `rich.console.Console`, `rich.panel.Panel`, `rich.text.Text` → `_RICH` bool
- `plyer.notification` → `_PLYER` bool

#### Class: `Notifier`

**`__init__(self, config)`**
- Inputs: config dict
- Reads from `config.altdata.notifications`: channels, log_path, min_level
- Reads Telegram config from `config.notifications.telegram` (preferred if `enabled=True`) OR `config.altdata.notifications.telegram`
- Telegram fields: enabled, bot_token, chat_id
- If Telegram enabled and credentials present: adds "telegram" to channels
- Creates log directory, initializes Console if rich available

**`send(self, trigger, message, level="INFO", details=None) -> None`**
- Inputs: trigger (str), message (str), level (str: INFO/WARNING/ALERT/CRITICAL), details (optional dict)
- Level filter: `self._should_send(level)` checks level index >= min_level index
- Dispatches to enabled channels: terminal, log, desktop, telegram

**`new_signal(self, ticker, direction, confidence, sources=None, confluence=None) -> None`**
- Builds message: `"{ticker} {LONG/SHORT} | confidence={val} | confluence={val} | sources=[{src}]"`
- Calls `send("new_signal", msg, level="INFO", details={"ticker":..., "direction":...})`

**`pead_abort(self, ticker, reason) -> None`**
- Calls `send("pead_abort", "{ticker} — PEAD signal aborted: {reason}", level="WARNING")`

**`nonsense_candidate(self, name, nonsense_score, sharpe) -> None`**
- Calls `send("nonsense_candidate", "{name} | nonsense_score={} | sharpe={}", level="INFO")`

**`model_rollback(self, from_version, to_version, reason) -> None`**
- Calls `send("model_rollback", "Rolled back {from} → {to}: {reason}", level="ALERT")`

**`drawdown_halt(self, current_dd, limit) -> None`**
- Calls `send("drawdown_halt", "Trading halted — drawdown {val} exceeds limit {val}", level="CRITICAL")`

**`unusual_activity(self, ticker, metric, value, z_score) -> None`**
- Level: "WARNING" if `abs(z_score) < 4`, "ALERT" if `abs(z_score) >= 4`
- Calls `send("unusual_activity", "{ticker} — {metric}={value} (z={z_score}σ)")`

**`weekly_summary(self, stats) -> None`**
- Joins stats dict as "k: v | k: v" string
- Calls `send("weekly_summary", msg, level="INFO", details=stats)`

**`source_failure(self, source, error) -> None`**
- Calls `send("source_failure", "{source} — {error}", level="WARNING")`

**`_terminal(self, level, title, body, details) -> None`**
- Uses rich Text if available, else `print()` fallback

**`_log(self, ts, level, trigger, title, body, details) -> None`**
- Writes JSON record to `self._log_path` (appended)
- Record keys: ts, level, trigger, title, body, [details]

**`_telegram(self, level, title, body, details) -> None`**
- Sends via urllib (POST with urlencode)
- Message format: `"{icon} *{title}*\n{body}\n`{details_json}`"` (Markdown parse mode)
- Details JSON trimmed to 300 chars
- Timeout: 5 seconds

**`_desktop(self, level, title, body) -> None`**
- Uses plyer if available; body truncated to 200 chars; timeout=10s

**`_send_telegram(self, message) -> bool`**
- Alternative Telegram sender used by MonitorRunner retry queue
- Uses JSON payload (not urlencode), HTML parse mode (not Markdown)
- Timeout: 10 seconds
- On 401 HTTPError: raises (does not swallow) — caller must handle
- On other HTTPError or exception: returns False, logs warning

**`send_signal_telegram(self, ticker, direction, surprise_pct, confidence, size_pct, macro_regime, sector, mfs) -> bool`**
- Formatted PEAD/signal HTML alert; uses `_send_telegram()`
- Template: `"{icon} <b>{LONG/SHORT} SIGNAL — {ticker}</b>\nSurprise: {val}% | Confidence: {val}\nSize: {val}% (obs mode) | MFS: {val}\nMacro: {regime} | Sector: {sector}\n{timestamp}"`

**`send_trade_close_telegram(self, ticker, return_pct, holding_days, pnl_paper) -> bool`**
- HTML format: `"{icon} <b>TRADE CLOSED — {ticker}</b>\nReturn: {val}% in {days} days\nPaper P&L: {val}"`

**`send_daily_summary_telegram(self, regime, vix, n_open, daily_pnl, n_signals) -> bool`**
- HTML format: `"DAILY SUMMARY — {date}\nRegime: {regime} | VIX: {vix}\nOpen positions: {n_open} | P&L today: {pnl}\nSignals today: {n_signals}"`

**`send_alert_telegram(self, alert_type, details, action="") -> bool`**
- HTML format: `"⚠️ <b>ALERT — {alert_type}</b>\n{details}\n{'Action: '+action if action else ''}"`

**`_should_send(self, level) -> bool`**
- `_level_order.index(level) >= _level_order.index(self._min_level)`
- Returns True on ValueError (unknown level passes through)

### C) MATHEMATICS

**Unusual activity level determination:**
- `level = "WARNING" if abs(z_score) < 4 else "ALERT"`
- Threshold: z-score of 4 sigma

### D) DATA FLOWS

**Reads:**
- `config` dict (constructor)
- `config.altdata.notifications.channels`, `log_path`, `min_level`
- `config.notifications.telegram` (preferred) or `config.altdata.notifications.telegram`

**Writes:**
- `logs/alerts.log` (append, JSON lines per notification if "log" in channels)
- Telegram API (POST sendMessage)
- Terminal stdout (if "terminal" in channels)
- OS desktop notification (if "desktop" in channels and plyer available)

### E) DEPENDENCIES

**External libraries:**
- `json`, `logging`, `os`, `datetime`, `pathlib`, `typing`
- `urllib.request`, `urllib.parse` (Telegram API)
- `rich` (optional — terminal formatting)
- `plyer` (optional — desktop notifications)

### F) WIRING STATUS

- Used by MonitorRunner._process_retry_queue (for retry queue)
- Used by alt-data pipeline components for event notifications
- The `send_signal_telegram`, `send_trade_close_telegram`, `send_daily_summary_telegram` methods appear to be for the paper trading pipeline (PEAD signals, trade closes, daily summaries)
- Telegram channel only active if `config.notifications.telegram.enabled = True` OR `config.altdata.notifications.telegram.enabled = True`
- The `_send_telegram` method (used for retry) raises on 401 — this is the only place in the notification system where 401 propagates up instead of being silently caught

### G) ISSUES FOUND

1. **Two Telegram methods with different parse modes**: `_telegram()` uses `parse_mode=Markdown` via urlencode; `_send_telegram()` uses `parse_mode=HTML` via JSON. Depending on which path is used, the same alert will render differently (Markdown vs HTML formatting). Retry queue uses `_send_telegram()` (HTML); direct `send()` calls use `_telegram()` (Markdown). Messages originally sent via `send()` that fail and get re-queued will be re-sent with HTML format, potentially breaking Markdown formatting.

2. **Channel config logic**: `tg_cfg = tg_top if tg_top.get("enabled") else tg_alt` — if `notifications.telegram.enabled` is `False` (explicitly), `tg_alt` is used instead. This means setting `notifications.telegram.enabled: false` does not disable Telegram — it falls through to the altdata sub-config. To truly disable, both configs must not have enabled=True.

3. **`_should_send` returns True on ValueError**: If an unknown level string is passed (e.g., "DEBUG"), `_level_order.index()` raises ValueError and `_should_send` returns True, bypassing the level filter. This could allow low-priority messages to bypass the min_level gate.

4. **details JSON in `_telegram` is truncated at 300 chars mid-JSON**: `detail_str[:297] + "..."` produces invalid JSON in the message body. The `\`{detail_str}\`` code block in Markdown would contain truncated JSON. Not a crash but produces malformed output.

5. **`_log` uses append mode with no rotation**: Writes to `logs/alerts.log` indefinitely. No log rotation or size limit. On a long-running system this file will grow unboundedly.

6. **Notifier.channels is a class instance variable modified in `__init__`**: `self.channels = list(set(self.channels) | {"telegram"})` — using set() loses the original order. This is cosmetically inconsistent but functionally harmless.

---

## SECTION 9A — TELEGRAM COMMAND REGISTRY

All commands/actions are triggered via inline keyboard buttons or natural-language text routing. There are no traditional "/command" slash commands — the bot uses callback_data strings from inline buttons and keyword routing.

---

**Command/Button: hi | hello | menu | /start**
Handler method: Sends `_build_menu_header()` + `_build_menu_keyboard()` inline keyboard
What it queries: `frontier.umci_history` (regime), `output/bot_status.json` (phase/timestamp), `trade_ledger` (real trade count, open position count), `ps aux` (PID)
What it displays: Apollo Control Centre header with: bot PID, regime + UMCI score, phase + real trade count + trades to next phase, open positions count, last cycle timestamp
Known issues: Phase number extracted via regex from bot_status.json — if format changes, regex may fail silently showing "PHASE-1"

---

**Command/Button: btn_positions (📊 Positions)**
Handler method: `_handle_positions`
What it queries: `trade_ledger` WHERE exit_date IS NULL AND is_phantom=0; Alpaca API (live current_price enrichment)
What it displays: Table of open positions with: ticker, entry price, live PnL% (or stored pnl_pct if no live price), net PnL £, position size, signal at entry (14 chars), holding days, sector; total PnL row
Known issues: "Live PnL%" falls back silently to stored pnl_pct on Alpaca failure; column header labels "Signal" but shows signals_at_entry which may be JSON string

---

**Command/Button: btn_performance (💹 Performance)**
Handler method: `_handle_performance`
What it queries: `trade_ledger` WHERE is_phantom=0 AND entry_date >= '2026-04-03'; chart_generator for equity curve image
What it displays: Total/closed trade count, win rate, all-time PnL, Sharpe, max drawdown, avg win/loss, expectancy; top 8 signals by PnL; equity curve photo
Known issues: Sharpe calculated on absolute £ PnL (not returns); max drawdown denominator hardcoded at £100k

---

**Command/Button: btn_health (🔧 System Health)**
Handler method: `_handle_health`
What it queries: `ps aux` (PID/uptime), `/proc/{pid}/status` (RAM), `/proc/loadavg` (CPU), `shutil.disk_usage("/")` (disk), DB file sizes, last 2000 log lines (error)
What it displays: Bot PID, uptime, RAM MB, CPU 1m load, disk usage, 4 DB sizes, last error line
Known issues: Uptime from `ps aux` column 9 (elapsed CPU time, not wall-clock uptime)

---

**Command/Button: btn_collectors (📡 Collectors)**
Handler method: `_handle_collectors`
What it queries: Last 3000 lines of `quant_fund.log` (text scanning); `permanent_archive.db.raw_government_contracts` (USASpendingCollector fallback)
What it displays: 13 collectors with last-seen timestamp and status (🟢 OK or 🔴 error or ⚪ no data)
Known issues: Log-scanning shows stale data if collector hasn't appeared in last 3000 lines; status is based on last log line content, not actual collector health

---

**Command/Button: btn_intelligence (🧠 Intelligence)**
Handler method: `_handle_intelligence`
What it queries: `frontier.umci_history` ORDER BY recorded_at DESC LIMIT 1; chart_generator for regime photo
What it displays: Current regime level, UMCI score, sizing multiplier, halt status, HMM probability vector as bar chart (per p_* keys in full_breakdown JSON), or fallback pillar scores; regime chart photo
Known issues: If full_breakdown is empty or null, shows individual pillar scores from separate columns

---

**Command/Button: btn_errors (⚠️ Errors)**
Handler method: `_handle_errors`
What it queries: Full `quant_fund.log` scanning for today's UTC date + ERROR/CRITICAL
What it displays: Last 20 error lines (120 chars each) from today
Known issues: Reads entire log file line-by-line each call; no size limit on file scan

---

**Command/Button: btn_regime (🌍 Regime)**
Handler method: `_handle_regime`
What it queries: `frontier.umci_history` (current + 7-day history); `intelligence.reasoning_engine.ReasoningEngine` (instantiated fresh)
What it displays: Current regime + confidence, plain English interpretation, sizing impact, active/suppressed signals, 7-day daily regime log with icons
Known issues: UMCI confidence = min(umci/10, 1.0) — assumes UMCI is 0-10 scale; ReasoningEngine instantiated per call (potentially expensive)

---

**Command/Button: btn_weekly_report (📈 Weekly Report)**
Handler method: `_handle_weekly_report` → `_handle_force_report` → `WeeklyReportGenerator().send_weekly_report()`
What it queries: All 8 weekly report sections (extensive DB reads across all DBs)
What it displays: Sends "Generating weekly report now..." then attempts to send 8 sections
Known issues: Sunday-only guard in `send_weekly_report()` is NOT bypassed — pressing this button on any non-Sunday silently does nothing after the "Generating..." message

---

**Command/Button: btn_pairs (🔬 Pairs Trading)**
Handler method: `_handle_pairs`
What it queries: `historical_db.pairs_signals` ORDER BY updated_at DESC LIMIT 20; fallback `closeloop.pairs_signals`
What it displays: Table of up to 10 pairs with ticker_a/b, Z-score, half-life, p-value, signal strength
Known issues: Column name aliases (zscore vs z_score, half_life vs half_life_days) handled with fallback — unclear which table schema is current

---

**Command/Button: btn_factors (🧮 Factor Model)**
Handler method: `_handle_factors`
What it queries: `closeloop.factor_exposures` ORDER BY run_date DESC LIMIT 20
What it displays: Fama-French 6-factor table: ticker, beta_mkt, SMB, HML, MOM, RMW, CMA, alpha, R²
Known issues: Shows only first occurrence per ticker from 20 most recent rows — if factor model hasn't run, shows nothing

---

**Command/Button: btn_options (📊 Options Flow)**
Handler method: `_handle_options`
What it queries: `deepdata.options_flow` ORDER BY collected_at DESC LIMIT 20; chart_generator signal performance photo
What it displays: Table of put/call ratio, IV percentile, unusual activity flag, sentiment score per ticker
Known issues: Column name aliases handled (put_call_ratio vs pcr, iv_percentile vs iv_rank)

---

**Command/Button: btn_insider (👤 Insider Data)**
Handler method: `_handle_insider`
What it queries: `historical_db.insider_transactions` ORDER BY transaction_date DESC LIMIT 20; fallback `closeloop.insider_transactions`
What it displays: Table with insider name, title, transaction type, shares, total value, date
Known issues: Column name aliases (reporter_name vs insider_name, reporter_title vs title, total_value vs value_usd)

---

**Command/Button: btn_kalman (〰️ Kalman)**
Handler method: `_handle_kalman`
What it queries: `historical_db.price_history` WHERE ticker='SPY' ORDER BY date DESC LIMIT 60
What it displays: Kalman-smoothed SPY price, raw price, innovation (residual), 5d avg innovation, trend, filter variance P
Known issues: Kalman Q=1e-5, R=1e-2 are hardcoded; very slow noise tracking (high ratio of R to Q)

---

**Command/Button: btn_wavelet (🌊 Wavelet)**
Handler method: `_handle_wavelet`
What it queries: `historical_db.price_history` WHERE ticker='SPY' ORDER BY date DESC LIMIT 128
What it displays: db4 wavelet decomposition at level 4 — long-term trend (A4), swing position (D3), medium momentum (D4), dominant cycle period; fallback MA5 vs MA20 if pywt not installed
Known issues: Dominant cycle hardcoded as 8 days (2^3) regardless of actual D3 period

---

**Command/Button: btn_shipping (🚢 Shipping)**
Handler method: `_handle_shipping`
What it queries: `historical_db.shipping_data` ORDER BY date DESC LIMIT 1
What it displays: BDI value, BDI 252d Z-score, shipping stress index, stress regime, data date
Known issues: Shows only most recent row

---

**Command/Button: btn_commodities (📦 Commodities)**
Handler method: `_handle_commodities`
What it queries: `historical_db.commodity_prices` ORDER BY date DESC LIMIT 100
What it displays: Table of unique commodity symbols with price and date (first 20 unique)
Known issues: Fetches 100 rows but dedups to 20 unique symbols — could miss symbols if many entries for common symbols appear first

---

**Command/Button: btn_db_stats (🗄️ DB Stats)**
Handler method: `_handle_db_stats`
What it queries: Opens closeloop, historical, frontier, deepdata, intelligence DBs; counts tables via sqlite_master; gets file size and mtime
What it displays: Table count, size MB, last modified time per DB
Known issues: None significant

---

**Command/Button: btn_force_report (🔄 Force Report)**
Handler method: `_handle_force_report`
What it queries: Same as btn_weekly_report (full weekly report via WeeklyReportGenerator)
What it displays: "Generating weekly report now..." then 8 report sections (if Sunday)
Known issues: Identical to btn_weekly_report; Sunday guard not bypassed despite "Force" label

---

**NLP route: nlp_news** (keywords: news, articles, headlines)
Handler method: `_handle_news`
What it queries: `historical_db.news_context` ORDER BY published_date DESC LIMIT 20
What it displays: Last 20 news headlines with ticker, date, sentiment score (icon for >0.1/< -0.1)
Known issues: No handler is mapped in the inline keyboard (no button); only reachable via NLP text

---

**NLP route: nlp_risk** (keywords: biggest risk, risk, concentration)
Handler method: `_handle_risk`
What it queries: `trade_ledger` WHERE exit_date IS NULL AND is_phantom=0, columns: ticker, sector, position_size
What it displays: Open position count, sector concentration table with >15% flagged as HIGH
Known issues: Not wired to a menu button

---

**NLP route: nlp_no_trades** (keywords: why no trades, why not trading, no signals, no trades)
Handler method: lambda — static message only
What it queries: Nothing
What it displays: "No trade explanation data available — scan results not cached yet"
Known issues: Always shows the same static message regardless of actual state

---

**NLP trade explanation** (patterns: "WHY {TICKER}", "EXPLAIN {TICKER}", etc.)
Handler method: `_handle_trade_explanation`
What it queries: trade_ledger (last trade for ticker), price_history (current price), pnl_attribution, news_context, ReasoningEngine (LLM explanation with 20s timeout)
What it displays: Entry date/price, current price, PnL%, signal stack with scores, attribution data, conflicts, regime at entry, AI explanation (up to 500 chars)
Known issues: LLM call creates new asyncio event loop per invocation; 20s timeout may produce "timed out" frequently

---

**Total documented interactive commands/buttons: 18 inline buttons + 3 NLP routes + 1 trade explanation pattern = 22 total**

---

## SECTION 9B — MONITORING ARCHITECTURE

### How monitoring works

Apollo monitoring is split across four independent systems running concurrently:

1. **PrivateBot (private_bot.py)** — Daemon thread named "apollo-private-bot". Long-polls Telegram getUpdates API every 20 seconds. Each button press or message spawns an additional short-lived daemon thread for the handler, preventing the polling loop from blocking. No scheduler — purely reactive.

2. **HealthDashboard (health_dashboard.py)** — Daemon thread named "HealthDashboard". Runs a `time.sleep(interval)` loop (default 300s / 5 minutes). Collects metrics, overwrites dashboard files. Sends one daily Telegram message at 09:00 UTC via file-based dedup guard.

3. **MonitorRunner (monitor_runner.py)** — Daemon thread named "monitor-runner". 60-second `Event.wait()` heartbeat. Manages scheduling of: daily health report (6am UTC), self-diagnostics (6-hour intervals), alert checks (every 60s), and Telegram retry queue (every 5 minutes). Weekly report scheduling is **DISABLED** here.

4. **preflight_check.py** — NOT a thread. The `tick()` function is called synchronously from the trading_bot.py main loop every minute. Spawns daemon threads at specific UTC times (13:00, 14:00, 21:15) on trading days only.

### What runs as thread vs subprocess

| Component | Type | Thread name |
|-----------|------|-------------|
| PrivateBot.run | daemon thread | "apollo-private-bot" |
| PrivateBot handlers | short-lived daemon threads | unnamed |
| HealthDashboard._loop | daemon thread | "HealthDashboard" |
| MonitorRunner._run | daemon thread | "monitor-runner" |
| MonitorRunner daily report | spawned daemon thread | unnamed |
| MonitorRunner diagnostic | spawned daemon thread | unnamed |
| alert_monitor.run_all_checks | called directly in tick (NOT a thread) | — |
| Telegram retry queue | called directly in tick (NOT a thread) | — |
| preflight_check.tick | called from trading_bot loop | not a thread |
| PreFlightChecker.run | spawned daemon thread | "preflight" |
| PreMarketScanner.run | spawned daemon thread | "prescan" |
| EndOfDayReporter.run | spawned daemon thread | "eod_report" |
| ps aux (PID lookup) | subprocess | — |

### What has access to live trading state

- **MonitorRunner** receives `stream_worker` reference — passes it to `alert_monitor.run_all_checks()`
- **HealthDashboard** receives `paper_trader`, `regime_detector`, `pairs_trader` objects in constructor — `_get_regime()` calls `regime_detector.detect()` live if available
- **PreMarketScanner** receives `trader` object via `tick()` — reads `sizer.phase_multiplier` and `max_simultaneous_positions` via attribute introspection

### What does NOT have access (shows stale data)

- **PrivateBot** — reads only from SQLite DBs and log files. Has no reference to any live trading object. Positions and PnL data is as current as the last DB write by the trading engine.
- **WeeklyReportGenerator** — reads only from SQLite DBs
- **preflight_check.py** — reconstructs regime by instantiating a fresh `RegimeDetector` from config (not the live in-memory instance); reads phase from DB (not live sizer state)
- **HealthDashboard without live objects** — if `regime_detector=None`, shows "UNKNOWN" for regime

### How private_bot.py gets position data — from DB or from memory?

**From DB, not from memory.** Every query in private_bot.py uses `_safe_query()` or `_safe_scalar()` which open fresh SQLite connections to disk DBs. There is no reference to any live trading object in private_bot.py.

Exception: `_handle_positions` attempts to import and call `AlpacaPaperBroker.get_positions()` for live current_price enrichment only — this enriches the live PnL% display but the position list itself still comes from the DB. If the Alpaca call fails, it silently falls back to stored pnl_pct.

Data freshness: positions displayed in private_bot.py are only as fresh as the last time the trading engine wrote to `trade_ledger`. If the trading engine's DB write is delayed or buffered, private_bot will show stale positions.

---

## SECTION 9C — NOTIFICATION SYSTEM

### Every place send() or equivalent is called, with trigger, message, conditions

#### 1. HealthDashboard.send_daily_telegram
- Trigger: 09:00 UTC daily (checked in `_loop`)
- Condition: `_DAILY_SENT_FILE` does not contain today's date (dedup guard)
- Message: "[HealthDashboard] Daily Summary — {date}\nPhase: {phase} | Trades: {count}\nOpen positions: {n}\nToday PnL: ${pnl}\nRegime: {regime}\nPairs active: {n}\nRAM used: {gb} GB | Disk free: {gb} GB"
- Channel: Telegram via `requests.post`

#### 2. PrivateBot startup message
- Trigger: `PrivateBot.run()` called (bot thread starts)
- Condition: always (no guard)
- Message: "🤖 Apollo Private Bot Online\nSend `hi` or `menu` for the control panel."
- Channel: Telegram via `_send_message`

#### 3. All interactive button/NLP responses (18+ handlers)
- Trigger: User message or button press to PRIVATE_CHAT_ID only
- Condition: chat_id == PRIVATE_CHAT_ID ("8508697534")
- Message: varies by handler (position table, performance stats, etc.)
- Channel: Telegram via `_send_message` (text) or `_send_photo` (charts)

#### 4. PreFlightChecker.run
- Trigger: 13:00 UTC on US trading days (called from `tick()`)
- Condition: `_last_preflight_date != today` AND `_is_trading_day(today, "us")`
- Message: Full pre-flight report (bot status, collectors, DBs, Alpaca, phase, open positions, signals ready, regime, SPY price, warnings)
- Channel: Telegram via `requests.post`

#### 5. PreMarketScanner.run
- Trigger: 14:00 UTC on US trading days (exact minute 0)
- Condition: `_last_prescan_date != today` AND `_is_trading_day(today, "us")`
- Message: "📡 Pre-Market Signal Scan — 30min to Open\n{top 10 signals with ticker, type, score, direction, confidence tier}"
- Channel: Telegram via `requests.post`

#### 6. EndOfDayReporter.run
- Trigger: 21:15 UTC on US trading days
- Condition: `_last_eod_date != today` AND `_is_trading_day(today, "us")`
- Message: "📊 Apollo End of Day — {date}\nTrades opened/closed/PnL\nBest/worst trade\nSignals by type\nPortfolio status\nData collected"
- Channel: Telegram via `requests.post`

#### 7. MonitorRunner daily health report
- Trigger: 06:00 UTC daily, minute < 2
- Condition: `_last_daily_report_date != today`
- Message: content of `monitoring.health_reporter.send_daily_report(config)` (module not in GROUP 9)
- Channel: via health_reporter module

#### 8. MonitorRunner self-diagnostic
- Trigger: every 6 hours (00:00, 06:00, 12:00, 18:00 UTC), minute < 2
- Condition: `diag_slot != _last_diagnostic_hour`
- Message: content of `monitoring.self_diagnostic.run_diagnostic(config, quiet)` (module not in GROUP 9)
- Channel: via self_diagnostic module

#### 9. MonitorRunner retry queue
- Trigger: every 5 minutes (minute % 5 == 0)
- Condition: retry queue not empty
- Message: re-sends previously failed Telegram messages stored in `telegram_logger` queue
- Channel: `Notifier._send_telegram()` (HTML parse mode)

#### 10. WeeklyReportGenerator.send_weekly_report
- Trigger: Sunday 09:00 UTC (called from trading_bot.py scheduler)
- Condition: weekday == 6 (Sunday) OR force=True
- Message: 8 separate Telegram messages (sections 1-8 of weekly report)
- Channel: `_send_telegram` (urllib, Markdown)
- Also triggerable via: `_handle_weekly_report` / `_handle_force_report` in private_bot.py (but Sunday guard applies)

#### 11. Notifier.new_signal (altdata pipeline)
- Trigger: alt-data pipeline generates a new signal
- Condition: level >= min_level (INFO by default)
- Message: "{ticker} {LONG/SHORT} | confidence={val} | confluence={val} | sources=[{src}]"
- Channel: terminal + log + telegram (if configured)

#### 12. Notifier.pead_abort (altdata pipeline)
- Trigger: PEAD signal aborted due to adverse alt-data
- Condition: WARNING level >= min_level
- Message: "{ticker} — PEAD signal aborted: {reason}"
- Channel: terminal + log + telegram (if configured)

#### 13. Notifier.model_rollback (altdata pipeline)
- Trigger: model rolled back to prior version
- Condition: ALERT level >= min_level
- Message: "Rolled back {from} → {to}: {reason}"
- Channel: all configured

#### 14. Notifier.drawdown_halt (altdata pipeline)
- Trigger: drawdown halt triggered
- Condition: CRITICAL level — passes min_level filter unless min_level = "CRITICAL" (at exactly CRITICAL it still passes)
- Message: "Trading halted — drawdown {val} exceeds limit {val}"
- Channel: all configured

#### 15. Notifier.unusual_activity (altdata pipeline)
- Trigger: unusual volume/sentiment spike detected
- Condition: WARNING if z < 4σ, ALERT if z >= 4σ
- Message: "{ticker} — {metric}={value} (z={z}σ)"
- Channel: all configured

#### 16. Notifier.source_failure (altdata pipeline)
- Trigger: data source fails or returns poor quality data
- Condition: WARNING level >= min_level
- Message: "{source} — {error}"
- Channel: all configured

#### 17. Notifier.send_signal_telegram (direct formatted alert)
- Trigger: PEAD/signal generated by trading engine
- Condition: Telegram enabled and credentials present
- Message: HTML formatted LONG/SHORT signal with surprise%, confidence, size%, MFS, macro regime, sector
- Channel: Telegram only (direct `_send_telegram`)

#### 18. Notifier.send_trade_close_telegram (direct formatted alert)
- Trigger: Trade closed by paper_trader
- Condition: Telegram enabled and credentials present
- Message: HTML formatted trade close with return%, holding days, paper P&L
- Channel: Telegram only

#### 19. Notifier.send_daily_summary_telegram (direct formatted alert)
- Trigger: daily summary event from trading pipeline
- Condition: Telegram enabled and credentials present
- Message: HTML formatted daily summary with regime, VIX, open positions, daily PnL, signal count
- Channel: Telegram only

#### 20. Notifier.send_alert_telegram (direct formatted alert)
- Trigger: generic alert from trading pipeline
- Condition: Telegram enabled and credentials present
- Message: HTML formatted "⚠️ ALERT — {alert_type}\n{details}\n{action}"
- Channel: Telegram only

---

## GROUP 9 GATE

**Files read:**
1. /home/dannyelticala/quant-fund/monitoring/private_bot.py (1,482 lines)
2. /home/dannyelticala/quant-fund/monitoring/health_dashboard.py (364 lines)
3. /home/dannyelticala/quant-fund/monitoring/monitor_runner.py (196 lines)
4. /home/dannyelticala/quant-fund/monitoring/weekly_report.py (889 lines)
5. /home/dannyelticala/quant-fund/monitoring/preflight_check.py (719 lines)
6. /home/dannyelticala/quant-fund/altdata/notifications/notifier.py (405 lines)

**Key findings:**

1. **Sunday guard blocks "Force Report" button**: Pressing the Weekly Report or Force Report Telegram button on any non-Sunday silently does nothing after showing "Generating...". The force=True flag exists but is never passed when called from private_bot.py.

2. **Two different permanent DB paths**: `weekly_report.py` uses `output/permanent_log.db`; `preflight_check.py` uses `output/permanent_archive.db`. These are different filenames for what appears to be the same permanent archive.

3. **Cooling-off count always returns 0**: `_get_cooling_off_count()` in preflight_check.py always returns 0 — no actual check is performed. The preflight report "Cooling Off Locks: 0" is meaningless.

4. **gate_line bug in PreFlightChecker**: Both branches of the market gate ternary produce "✅ Market Hours Gate: ACTIVE" — operator cannot distinguish open vs closed gate from the preflight message.

5. **Sharpe calculated on absolute £ PnL, not returns**: Both private_bot._handle_performance and weekly_report use absolute £ daily PnL values instead of percentage returns. This is internally consistent but economically non-standard.

6. **Max drawdown hardcoded at £100k denominator**: Three files (private_bot, weekly_report, health_dashboard) all assume £100k starting capital. If actual capital differs, all drawdown figures are incorrect.

7. **HealthDashboard._get_open_positions includes phantom positions**: No `is_phantom=0` filter — phantom open positions inflate the count on the health dashboard.

8. **HealthDashboard._get_today_pnl uses gross_pnl**: Inconsistent with weekly_report and performance handler which use net_pnl. EOD reporter also uses gross_pnl.

9. **private_bot.py gets position data from DB, not live memory**: All position data is stale relative to last DB write. Live price enrichment attempted via Alpaca but silently falls back to stored pnl_pct on failure.

10. **Two Telegram parse modes in Notifier**: `_telegram()` uses Markdown; `_send_telegram()` uses HTML. Retried messages will have format mismatch (originally sent as Markdown, retried as HTML).

11. **Notifier._should_send returns True for unknown levels**: `ValueError` on unknown level bypasses the min_level filter.

12. **MonitorRunner alert_monitor called synchronously (not spawned)**: If alert_monitor.run_all_checks is slow, it blocks the 60-second heartbeat loop.

13. **Weekly report scheduling is commented out in MonitorRunner**: Comment says "DISABLED: now sent from trading_bot.py" — creates hidden dependency on trading_bot's scheduler; MonitorRunner no longer owns weekly report responsibility.

14. **_handle_regime confidence = min(umci/10, 1.0)**: Assumes UMCI is 0-10 scale. If UMCI is stored differently, confidence will always be 1.0.

15. **preflight_check prescan fires only at exact minute==0**: The prescan check uses `minute == 0` not `minute < 2` — more likely to miss than the preflight (minute < 2) and EOD (minute == 15) checks.

16. **HealthDashboard send_daily_telegram uses requests, not urllib**: Inconsistent with all other Telegram senders in monitoring module.

17. **`_handle_collectors` shows stale log data**: Status based on last 3000 log lines — a collector active 3001 lines ago shows as "no data".

18. **Notifier.channels set() loses order**: Cosmetic issue in channel dispatch ordering.

19. **LLM explain in trade explanation creates new event loop per call**: Potentially conflicts with existing loops; wasteful for a daemon thread context.

20. **alerts.log has no rotation**: Grows indefinitely.

**Telegram commands documented: 22** (18 inline buttons + 3 NLP routes + 1 trade explanation pattern)

**Contradictions:**

- `output/permanent_log.db` (weekly_report.py) vs `output/permanent_archive.db` (preflight_check.py) — same logical DB, two different paths used
- Drawdown denominator is £100k hardcoded in 3 places but actual paper account may differ
- HealthDashboard._get_open_positions counts phantoms; all other open position queries filter `is_phantom=0`
- `_get_regime()` in HealthDashboard returns "UNKNOWN" without DB fallback; private_bot and weekly_report read frontier DB directly
- Weekly report scheduling: disabled in MonitorRunner but comment says trading_bot.py handles it — ownership unclear
- "Force Report" button name implies override but Sunday guard is not bypassed
- Notifier sends Markdown in send(); HTML in _send_telegram() — same bot, different format

**Data flows:**

- `trade_ledger` → private_bot (open positions, performance, factor exposures, pairs, insider, risk, phase)
- `umci_history` (frontier) → private_bot (regime, intelligence, menu header), weekly_report (regime section, outlook)
- `price_history` (historical) → private_bot (Kalman, wavelet, shipping, commodities, current price for trade explanation)
- `options_flow` (deepdata) → private_bot (options section)
- `news_context` (historical) → private_bot (news, trade explanation context)
- `quant_fund.log` → private_bot (errors, collector status), weekly_report (rate limit count)
- `permanent_archive.db.signals_log` → preflight_check (signals ready count, prescan top signals, EOD signal types)
- `bot_status.json` → private_bot menu header (phase, last cycle)
- Alpaca API → preflight_check (account status), private_bot (live position prices)
- `output/dashboard.json` ← HealthDashboard writes (overwrite every 5 min)
- `logs/apollo_health_dashboard.log` ← HealthDashboard writes (overwrite every 5 min)
- `logs/alerts.log` ← Notifier writes (append)
- `logs/preflight/*.txt` ← PreFlightChecker writes
- `logs/eod_reports/*.txt` ← EndOfDayReporter writes
- Telegram API ← all monitoring components write (18+ notification events)

**Proceed to GROUP 10: YES**
