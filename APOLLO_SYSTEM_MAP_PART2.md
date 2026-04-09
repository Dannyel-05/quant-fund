# APOLLO SYSTEM MAP — PART 2
Generated: 2026-04-08
Files documented: GROUP 2 (execution layer + risk manager)

---

# FILE 1: /home/dannyelticala/quant-fund/execution/paper_trader.py

## A) PURPOSE

Main trading loop. Runs indefinitely via `schedule`. Executes signal scans for US (14:45 UTC) and UK (08:15 UTC) markets. Manages all open positions in-memory (`self.active` dict). Evaluates exit conditions every 30 minutes during market hours. Orchestrates position sizing, order placement, DB writes, context capture at open, trade autopsy on close, and Telegram notifications. Also manages observation mode (25% sizing), signal decay checks, correlation management, and position restore on restart.

---

## B) CLASSES AND METHODS

### Module-level functions

**`_is_market_open(market: str) -> bool`**
- Inputs: market string "us" or "uk"
- Imports `MarketCalendar` from `analysis.market_calendar`. Calls `cal.is_trading_day(market, today)`. Checks current UTC time against hardcoded market hours constants. US: 14:30–21:00 UTC. UK: 08:00–16:30 UTC.
- Output: bool
- DB reads: none directly (MarketCalendar may read internally)
- Issues: lambda closures for intraday exit checks at line 526–529 capture `m="us"` but each lambda hardcodes "us" — the UK exit at 16:45 passes `m="uk"` but the loop lambdas at line 526 all bind `m="us"`.

**`_next_market_open_str(market: str) -> str`**
- Returns human-readable string for next trading day open.

**`_append_log(entry: dict) -> None`**
- Appends JSON line to `logs/paper_trading.jsonl` (creates directory if missing).

---

### Class: `PaperTrader`

#### `__init__(self, config, fetcher=None, signal_generators=None, risk_manager=None, broker=None)`
- Inputs: config dict, optional injected components
- Reads `config.paper_trading`, `config.alpaca`, `config.api_keys`
- Decides broker: if `alpaca.enabled` and real API key present → `AlpacaPaperBroker`; else → `PaperBroker`
- Initialises all signal generators (PEAD, Momentum, MeanReversion, Gap, InsiderMomentum, CalendarEffects, OptionsEarnings, ShortSellingFilter), analysis models (MathematicalSignals, CrowdingDetector, SectorRotationSignal), alt-data collectors (SimFinCollector, SECFullTextCollector, AlternativeQuiverCollector), AdaptivePositionSizer (twice — lines 142 and 289, creating two instances), TrailingStopManager, StockCoolingOffTracker, EarningsCache, RiskManager, ClosedLoopStore
- Creates `signals_log` table in `output/permanent_archive.db` if absent
- Calls `_load_existing_positions()` at end of `__init__`
- DB writes: CREATE TABLE IF NOT EXISTS signals_log in output/permanent_archive.db

#### `_load_existing_positions(self) -> None`
- Reads open positions from two sources:
  1. Alpaca broker: calls `self.broker.get_positions()` → list of dicts with keys `symbol`, `qty`, `avg_entry_price`
  2. ClosedLoopStore: calls `self.closeloop.get_open_positions()` → list of dicts with keys `ticker`, `direction`, `entry_price`, `position_size`, `entry_date`, `signal_type`
- Populates `self.active[sym]` for each unrecognised position
- Restored position fields populated from Alpaca: `direction` (sign of qty), `entry_price` (avg_entry_price), `exit_date=None`, `atr=0`, `target_price=0`, `target_delta=0`, `scale_out_done=[]`, `sector="UNKNOWN"`, `shares`, `dry_up_days=0`, `market="us"`, `context_at_open={}`, `signal_type="restored"`, `tier="TIER_1_SMALLCAP"`, `entry_date=datetime.utcnow().isoformat()`, `days_held_at_restore=0`, `restore_date=datetime.utcnow().date()`
- Restored position fields populated from closeloop: same but `direction`, `entry_price`, `shares` from closeloop data; `days_held_at_restore` computed from entry_date to today; `signal_type` from closeloop record
- DB reads: Alpaca API GET /v2/positions; closeloop.get_open_positions() (reads trade_ledger)

#### `reconcile_positions(self) -> dict`
- Inputs: none (uses self.broker, self._store)
- Fetches Alpaca positions and trade_ledger open rows (WHERE exit_date IS NULL from closeloop/storage/closeloop.db)
- Positions in Alpaca but not in ledger → added to self.active with `signal_type="reconciled"`, minimal fields
- Positions in ledger but not in Alpaca → phantom-closed: UPDATE trade_ledger SET exit_date=?, exit_price=entry_price, gross_pnl=0.0, is_phantom=1 WHERE ticker=? AND exit_date IS NULL; removed from self.active
- Returns dict: `{matched, added, phantom_closed}`
- DB reads: closeloop/storage/closeloop.db — table trade_ledger columns ticker, entry_date, entry_price WHERE exit_date IS NULL
- DB writes: closeloop/storage/closeloop.db — UPDATE trade_ledger (exit_date, exit_price, gross_pnl, is_phantom) for phantom positions

#### `run(self) -> None`
- Infinite loop using `schedule` library
- Schedules: 07:00 `_morning_earnings_check`, 08:15 `scan_uk`, 14:45 `scan_us`, 21:30 `run_eod`
- Legacy intraday exit checks: schedule every hour 10:00–16:30 and 16:30 for "us", 16:45 for "uk" (NOTE: loop lambdas at lines 526–529 all bind `m="us"` — UK tickers never get intraday exit checks via this loop)
- Calls `schedule.run_pending()` every 30 seconds

#### `run_scan(self, market: str = "us", limit: int = 0, skip_slow_generators: bool = False) -> List[dict]`
- Initialises `self._store` (ClosedLoopStore) if not already set
- Fetches universe via `UniverseManager.get_universe(market)`
- Fetches price data for all tickers via `self.fetcher.fetch_universe_data(tickers, start, end, market)` (600 days back)
- Caches prices in `self._scan_price_cache`, resets per-scan caches
- Refreshes EarningsCache if needed (bulk Finnhub fetch)
- Runs PEAD generators: for each ticker with cached earnings, calls `gen.generate(ticker, price_data, earnings_data)` — skips tickers with no cached earnings entirely
- Runs extended generators: MOMENTUM, MEAN_REV, GAP, and optionally INSIDER_MOM (skipped in quick/limit mode)
- For each signal, calls `self._process(signal, market, price_df=pdf)`
- After all signals processed, collects signal tickers; falls back to open positions from trade_ledger if no signals
- Runs AdvancedNewsIntelligence on signal tickers; attaches VADER sentiment scores; sets `sentiment_exit_pressure=True` for LONG positions with compound < -0.3
- Returns list of action dicts
- DB reads: trade_ledger (fallback signal ticker lookup), multiple via sub-components

#### `_process(self, signal: pd.Series, market: str, price_df: pd.DataFrame = None) -> Optional[dict]`
- Central method for opening a new position
- Extracts `ticker`, `direction = int(signal["signal"])`, `confidence = float(signal.get("surprise_zscore", 0.0))`
- Gathers supporting signals from all loaded generators (MOMENTUM, MEAN_REV, GAP, INSIDER_MOM, SECTOR_ROTATION, CALENDAR, OPTIONS_EARNINGS) into `_raw_signals`
- Applies real-time stream boost: if `direction * rt_move > 2.0` → `boost = min(0.5, abs(rt_move) / 20.0)`, `confidence += boost`; if `direction * rt_move < -3.0` → `confidence *= 0.85`
- Returns None if ticker already in `self.active` with same direction; triggers `_close_position(reason="signal_reversal")` if opposite direction
- If `abs(confidence) < min_confidence`: logs OBSERVED_NOT_TRADED to jsonl and signals_db; returns obs_entry dict
- Gets price from price_df close column, overlays with real-time cache
- Runs `_passes_correlation_check(ticker, direction, sector)`
- Calls `self.risk.size_position(...)` via cached portfolio state
- If observation mode: `size_pct = size_pct * 0.25`
- Applies sector rotation modifier: `size_pct = size_pct * (1.0 + sr_mod)` where sr_mod in [-0.15, +0.15]
- Applies calendar effects modifier: `size_pct = size_pct * (1.0 + cal_mod)` where cal_mod in [-0.40, +0.30]
- Applies tier multiplier from `classify_tier(market_cap)` and `get_tier_size_multiplier(tier)`
- ABSOLUTE CAP: `size_pct = min(size_pct, 0.030)`
- Runs SignalAggregator check; returns None if `kelly_multiplier == 0`
- Runs cooling-off check; returns None if ticker in cooling-off
- Computes: `equity * size_pct = value`, `value / price = shares`
- Routes order to correct broker: AlpacaPaperBroker gets `(ticker, shares, side, direction=order_dir, fill_price=price)`, PaperBroker gets `(ticker=ticker, quantity=shares, direction=order_dir, fill_price=price)`
- If order status is "filled" or "submitted": updates `self.active[ticker]` dict; registers with TrailingStopManager; writes trade record to `self._store.record_trade(trade_record, context_at_open)` if order accepted; appends to paper_trading.jsonl; logs to signals_db if filled
- Returns dict `{type, ticker, direction, price, shares}` or None
- DB writes: closeloop_store.record_trade() → trade_ledger INSERT; signals_log INSERT in output/permanent_archive.db; append to logs/paper_trading.jsonl

#### `_check_exits(self, market: str) -> None`
- Returns immediately if market is closed (via `_is_market_open`)
- Iterates `self.active` items matching the market
- Gets live price via `_get_live_price`
- For each position, evaluates in order:
  1. ATR stop: computes `stop = entry - direction * atr_mult * atr` (1.0x for shorts, 1.5x for longs); fires if `(direction > 0 and price <= stop) or (direction < 0 and price >= stop)`; skips if `atr == 0`
  2. Trailing stop: calls `_trailing_stops.observe(ticker, price, entry_price=entry)` and `_trailing_stops.should_exit(ticker, price)`. Sends Telegram on tier graduation
  3. Scale-out: for each `_SCALE_OUT_LEVELS` (0.50, 1.00) not yet in `scale_out_done`: if `pnl_pct >= level * target_pct`: sells `shares * fraction` (0.33 each); updates `pos["shares"]` and `pos["scale_out_done"]`
  4. Volume dry-up: fetches last 25 days volume; if last volume < 30% of 20-day avg for 2+ consecutive days → close
  5. Time exit: if `exit_date` and `pd.Timestamp(exit_date) <= today` → close
  6. Max hold days: computes `total_days = days_held_at_restore + (today - restore_date).days`; if `>= max_hold_days` → close

#### `_close_position(self, ticker: str, reason: str = "unknown", price: Optional[float] = None) -> None`
- Gets current position from `self.active`; returns immediately if absent (duplicate-close guard)
- Fetches live price if not supplied
- Calls `self.broker.get_positions()` to check membership; if position absent from Alpaca → removes from self.active and returns without DB write
- Determines shares from positions list or self.active fallback
- If shares == 0: deletes from self.active and returns
- Places close order: `self.broker.place_order(ticker, abs(shares), close_dir, 'market', None, close_dir, price)`
- If broker returns `status == 'error'`: logs error, returns WITHOUT deleting from self.active and WITHOUT writing to DB
- Computes: `pnl_pct = direction * (price - entry) / entry * 100`; `holding_days`; `return_pct = pnl_pct / 100.0`
- Phantom detection: trade is phantom if `(not _market_was_open) and (abs(pnl_pct) < 0.001 or _held_minutes < 30)`
- Registers with cooling-off tracker (skips phantom trades); sends Telegram on loss
- Calls `self._store.record_trade(closed_trade, context_at_open)` — this is the DB close path
- Runs `TradeAutopsy.run(closed_trade_for_autopsy, context_at_open)`
- Appends to paper_trading.jsonl
- Deletes `self.active[ticker]`
- Removes from TrailingStopManager
- DB writes: closeloop_store.record_trade() (UPDATE trade_ledger with exit fields, or INSERT new); logs/paper_trading.jsonl append

#### `_capture_context(self, ticker: str, signal) -> dict`
- Builds full context dict at moment of trade open
- Sections: timestamp, pead (surprise_pct, surprise_zscore, quality, volume_surge), macro (regime from MacroSignalEngine, VIX from yfinance cached, yield_curve and HY spread from RatesCreditCollector), altdata (confluence_score from closeloop_store), shipping_stress_index (from output/historical_db.db table shipping_data), hmm_state + math_composite (from MathematicalSignals), earnings_quality (from SimFin), has_sec_crisis_alert (from SECFullTextCollector "going concern" search), geopolitical_risk (from output/permanent_log.db table macro_context), signals_at_entry
- Also calls `self.risk.set_macro_regime(_REGIME_STR_TO_INT[regime])` to update RiskManager
- DB reads: output/historical_db.db (shipping_data), output/permanent_log.db (macro_context)
- Returns: dict

#### `_log_to_signals_db(self, ticker, signal, confidence, direction, was_traded, context, trade_id) -> None`
- Inserts one row into `output/permanent_archive.db` table `signals_log`
- Fields: ticker, signal_type, signal_score, direction, was_traded, trade_id, macro_regime, vix, yield_curve, shipping_stress, consumer_health, crowding_risk, hmm_state, math_composite, earnings_quality, calendar_modifier, has_crisis_filing, all_context_json, timestamp
- DB writes: INSERT into output/permanent_archive.db.signals_log

#### `_compute_atr(self, ticker: str, window: int = 14, price_df: pd.DataFrame = None) -> float`
- Uses pre-fetched price_df if available and long enough; else calls `yf.download(ticker, period="40d")`
- Normalises column names to high/low/close
- Computes TR and ATR (see MATHEMATICS section)
- Returns float ATR value or 0.0 on failure

#### `_get_recent_volume(self, ticker: str, days: int = 25) -> Optional[pd.Series]`
- Calls `yf.download(ticker, period=f"{days+5}d")` → returns volume series of last `days` rows

#### `_get_live_price(self, ticker: str, fallback: Optional[float] = None) -> Optional[float]`
- UK tickers: always yfinance (`yf.Ticker(ticker).fast_info.last_price`)
- US tickers: tries stream cache `get_stream_cache().get_fresh_price(ticker)` (5-min stale threshold) first; falls back to yfinance

#### `_get_sector(self, ticker: str) -> str`
- Tries static `TICKER_TO_SECTOR` map from `signals.sector_rotation_signal`; then fetcher cache; then yfinance `.info["sector"]`

#### `_passes_correlation_check(self, ticker, direction, sector) -> bool`
- Sector check: if `>= 3 same-sector positions` → False
- Correlation check: computes Pearson correlation of returns vs up to 10 same-direction positions; if `>= 2` have correlation `> 0.6` → False

#### `_compute_correlation_matrix(self, tickers, period="60d", cached_prices=None) -> Optional[pd.DataFrame]`
- Uses scan price cache first (last 60 rows); falls back to `yf.download`
- Returns `df.pct_change().dropna().corr()`

#### `run_all_signals(self, ticker, price_data, context) -> List[Dict]`
- Runs all signal generators for one ticker; applies `_apply_contradiction_scores`

#### `_apply_contradiction_scores(signals: List[Dict]) -> List[Dict]` (static)
- Computes `contradiction_score = 2.0 * minority_wt / total_wt`
- If `< 0.2`: `score = min(score * 1.15, 5.0)`; if `> 0.6`: `score = score * 0.75`

#### `pre_short_checklist(self, ticker, context, price_data) -> Dict`
- 9 checks (regime, RSI+momentum, borrow, DTC, short_float, earnings, biotech, macro, volume, UK_penny)
- All must pass for SHORT to proceed

#### `build_full_context(self, ticker) -> Dict`
- Lighter version of `_capture_context` — used by external callers

#### `_morning_earnings_check(self) -> None`
- 07:00 UTC: fetches earnings calendar for top 50 US tickers via `yf.Ticker(t).calendar`; logs upcoming in next 7 days

#### `reconcile_positions(self) -> dict`
- See above

#### `check_exit_conditions(self, positions, equity) -> None`
- Wrapper that calls `_check_exits(market)` per position

#### `_log_performance(self) -> None`
- Gets broker positions and account value; logs to paper_trading.jsonl

#### `_portfolio_state(self) -> Dict`
- Builds portfolio dict with positions, account_value, capital, net_exposure, drawdown, sectors, sector_exposures

#### `get_open_positions_table(self) -> str`
- Formats self.active as ASCII table for CLI

---

# FILE 2: /home/dannyelticala/quant-fund/execution/broker_interface.py

## A) PURPOSE

Defines abstract `BrokerInterface` and two concrete implementations: `PaperBroker` (pure in-memory simulation) and `AlpacaPaperBroker` (Alpaca paper-trading REST API). PaperBroker tracks cash and positions in memory with slippage and commission. AlpacaPaperBroker sends real HTTP requests to Alpaca paper trading endpoint and falls back to simulation mode if credentials are absent or placeholder.

---

## B) CLASSES AND METHODS

### Abstract Class: `BrokerInterface(ABC)`
Abstract methods: `place_order(ticker, quantity, direction, fill_price, order_type)`, `get_positions()`, `get_account_value(current_prices)`, `get_cash()`

---

### Class: `PaperBroker(BrokerInterface)`

**`__init__(self, initial_capital: float, config: dict)`**
- Sets `self.cash = initial_capital`; `self.positions: Dict[str, float]` (ticker → net shares); `self.avg_prices: Dict[str, float]`; `self._log: List[Dict]`

**`place_order(self, ticker, quantity, direction, fill_price, order_type="market") -> Dict`**
- Inputs: ticker, quantity (float shares), direction ("buy"/"sell"/"short"/"cover"), fill_price
- Computes market type from ticker suffix (.L = uk, else us); reads `config["costs"][market]`
- `slip = fill_price * costs["slippage_pct"]`
- buy: `exec_price = fill_price + slip`; stamp = `exec_price * quantity * stamp_duty_pct`; `total_cost = exec_price * quantity + commission + stamp`; checks `total_cost > self.cash` → rejects; deducts from cash; calls `_update_position(ticker, +quantity, exec_price)`
- sell: `exec_price = fill_price - slip`; checks `self.positions.get(ticker, 0) < quantity` → rejects; `self.cash += exec_price * quantity - commission`; `_update_position(ticker, -quantity, exec_price)`
- short: `exec_price = fill_price - slip`; `self.cash += exec_price * quantity - commission`; `_update_position(ticker, -quantity, exec_price)`
- cover: `exec_price = fill_price + slip`; `total_cost = exec_price * quantity + commission`; checks cash; deducts; `_update_position(ticker, +quantity, exec_price)`
- Returns record dict with `status: "filled"` or rejection dict

**`get_positions(self) -> Dict[str, float]`** — returns non-zero positions

**`get_account_value(self, current_prices=None) -> float`**
- If no prices: returns `self.cash`
- Else: `self.cash + sum(shares * current_prices.get(t, avg_price))`

**`get_cash(self) -> float`** — returns `self.cash`

**`get_pnl(self, current_prices) -> float`** — `get_account_value(current_prices) - initial_capital`

**`get_trade_log(self) -> pd.DataFrame`** — returns `pd.DataFrame(self._log)`

**`_update_position(self, ticker, delta, price) -> None`**
- `old = positions.get(ticker, 0); new = old + delta`
- If `abs(new) < 1e-6`: remove ticker from positions and avg_prices
- Else if `old == 0 or (old > 0) != (new > 0)`: `avg_prices[ticker] = price` (new or flipped)
- Else: `avg_prices[ticker] = (avg_prices[ticker] * abs(old) + price * abs(delta)) / abs(new)` (weighted average)
- DB reads/writes: none (all in-memory)

---

### Class: `AlpacaPaperBroker(BrokerInterface)`

**`__init__(self, config: dict)`**
- Reads `config['api_keys']['alpaca_api_key']` and `alpaca_secret_key`
- `self.base_url = config['alpaca']['base_url']` (default `https://paper-api.alpaca.markets`)
- `self.data_url = config['alpaca']['data_url']` (default `https://data.alpaca.markets`)
- `self.enabled = bool(api_key) and 'PASTE' not in api_key`
- `self._connected = self.is_connected()` if enabled, else False
- Prints "CONNECTED" or "SIMULATION MODE"

**`is_connected(self) -> bool`**
- GET `{base_url}/v2/account` with 10s timeout → True if status 200

**`get_account(self) -> dict`**
- GET `{base_url}/v2/account` → returns JSON dict or {}

**`get_positions(self) -> list`**
- GET `{base_url}/v2/positions` → returns list of position dicts or []

**`get_account_value(self, current_prices=None) -> float`**
- Calls `get_account()`; returns `float(acc['portfolio_value'])` or 0.0

**`get_cash(self) -> float`**
- Calls `get_account()`; returns `float(acc['cash'])` or 0.0

**`place_order(self, ticker, qty, side, order_type="market", limit_price=None, direction="", fill_price=0.0) -> dict`**
- Inputs: ticker (str), qty (float), side ("buy"/"sell"), order_type, limit_price, direction (optional PaperBroker-style), fill_price (ignored)
- If not connected: returns `{'status': 'error', 'reason': 'not_connected'}`
- Maps direction to Alpaca side via `side_map = {'buy':'buy','sell':'sell','short':'sell','cover':'buy'}`
- Body: `{symbol: ticker, qty: str(int(max(1, qty))), side: side, type: order_type, time_in_force: 'day'}`; adds `limit_price` if limit order
- POST `{base_url}/v2/orders` with JSON body, 10s timeout
- Returns: if 200/201 → `{status:'submitted', order_id, ticker, qty, side}`; else `{status:'error', reason:response_text[:200]}`
- Exception → `{status:'error', reason:str(exc)}`

**`close_position(self, ticker: str) -> dict`**
- DELETE `{base_url}/v2/positions/{ticker}` → returns JSON if 200/207, else `{status:'error', code:status_code}`

**`get_order_history(self, limit=500) -> list`**
- GET `{base_url}/v2/orders?status=all&limit={limit}`

**`get_realtime_price(self, ticker: str) -> float`**
- GET `{data_url}/v2/stocks/{ticker}/quotes/latest`
- Returns `(ask + bid) / 2`, or ask, or bid, or 0.0

**`_get_bars(self, ticker, timeframe, days_back) -> pd.DataFrame`**
- GET `{data_url}/v2/stocks/{ticker}/bars` with params timeframe, start, end (now minus days_back), limit=10000, adjustment=raw
- Returns DataFrame with columns: timestamp (index), open, high, low, close, volume

**`get_bars_30min(self, ticker, days_back=30) -> pd.DataFrame`** — calls `_get_bars(..., '30Min', ...)`

**`get_bars_1min(self, ticker, days_back=5) -> pd.DataFrame`** — calls `_get_bars(..., '1Min', ...)`

**`is_market_open(self) -> bool`** — calls `get_market_hours()['is_open']`

**`get_market_hours(self) -> dict`** — GET `{base_url}/v2/clock`

**`submit_order(self, ticker, qty, side, order_type="market", **kwargs) -> dict`** — alias for `place_order`

**`get_open_orders(self) -> list`** — GET `{base_url}/v2/orders?status=open`

**`cancel_order(self, order_id: str) -> bool`** — DELETE `{base_url}/v2/orders/{order_id}` → True if 200/204

**`get_price(self, ticker: str) -> Optional[float]`**
- Tries `get_realtime_price(ticker)` first
- Falls back to `yfinance.Ticker(ticker).fast_info.last_price`

---

# FILE 3: /home/dannyelticala/quant-fund/execution/alpaca_stream.py

## A) PURPOSE

Real-time WebSocket price stream from Alpaca IEX free tier. Runs in a background daemon thread with its own asyncio event loop. Never blocks the main trading loop. Provides a thread-safe `PriceCache` singleton for live prices, move percentages, spike/urgent alerts. Sends Telegram notifications for URGENT moves (>5% in 10 minutes).

---

## B) CLASSES AND METHODS

### Class: `PriceCache`

**`__init__(self)`**
- `self._lock = threading.RLock()`
- `self._data: Dict[str, dict]` — ticker → {price, volume, timestamp, pct_from_prev}
- `self._history: Dict[str, deque(maxlen=30)]` — ticker → deque of (ts, price) tuples
- `self._spike_flags: Dict[str, str]` — ticker → "SPIKE"/"URGENT"
- `self._connected: bool`; `self._total_updates: int`; `self._last_update: Optional[datetime]`

**`update(self, ticker, price, volume, ts) -> Optional[str]`**
- If `abs(pct) < 0.005` (0.5%) → returns None (throttle)
- Else updates `self._data[ticker]` and appends to `self._history[ticker]`
- Calls `_check_alert` → returns "SPIKE", "URGENT", or None

**`_check_alert(self, ticker, now_price, now) -> Optional[str]`**
- Iterates history; for each of two windows:
  - (5 min, 2.0%, "SPIKE") and (10 min, 5.0%, "URGENT")
  - If `abs((now_price - px) / px * 100) >= threshold` for any price point within the window → returns label

**`get_price(self, ticker) -> Optional[float]`** — returns cached price or None

**`get_fresh_price(self, ticker, max_age_sec=300) -> Optional[float]`**
- Returns price only if timestamp age < `max_age_sec`; else None

**`get(self, ticker) -> Optional[dict]`** — returns copy of full data dict

**`get_move_pct(self, ticker, window_minutes=5) -> Optional[float]`**
- Finds oldest price in history within the window; returns `(current - baseline) / baseline * 100.0`

**`get_realtime_volatility(self, tickers, window_minutes=10) -> float`**
- Returns mean absolute % move across tickers

**`set_connected(v: bool) -> None`**; **`clear_spike_flag(ticker) -> None`**; **`is_connected() -> bool`**; **`get_spike_flags() -> dict`**; **`stats() -> dict`**

---

### Class: `AlpacaStreamWorker`

**`__init__(self, api_key, secret_key, universe: Set[str], config: dict)`**
- Sets up rotating log file at `logs/alpaca_stream.log` (5MB, 3 backups)
- Initialises `Notifier` for Telegram URGENT alerts

**`start(self) -> None`**
- Spawns daemon thread `alpaca-stream` running `_run()`

**`stop(self) -> None`** — sets stop event

**`update_universe(self, tickers: List[str]) -> None`** — updates `self._universe` (US only)

**`_run(self) -> None`**
- Creates new asyncio event loop; calls `_session()` in loop; reconnects after crash with `_RECONNECT_DELAY=30s`; max reconnects effectively unlimited (99999)

**`_session(self) -> None` (async)**
- Opens aiohttp WebSocket to `wss://stream.data.alpaca.markets/v2/iex`
- Handshake: receives connected message
- Auth: sends `{action:"auth", key, secret}`, checks for "authenticated" in response; else returns (triggers reconnect)
- Subscribe: sends `{action:"subscribe", bars:["*"]}` — subscribes to ALL bars, filters locally by universe
- Message loop: for each message, if `item["T"] == "b"` → calls `_on_bar(item)`

**`_on_bar(self, item: dict) -> None`**
- Skips if `item["S"]` not in `self._universe`
- Extracts: `price = float(item["c"] or item["vw"] or 0)`, `volume = float(item["v"])`, timestamp from `item["t"]`
- Calls `get_stream_cache().update(ticker, price, volume, ts)`
- If alert is "URGENT": calls `_send_urgent_alert` and `clear_spike_flag`
- If alert is "SPIKE": logs info only

**`_send_urgent_alert(self, ticker, pct, price, volume, direction, ts) -> None`**
- Formats message; calls `self._notifier.send(trigger="alpaca_stream_urgent", message, level="CRITICAL")`

---

### Module functions

**`get_stream_cache() -> PriceCache`** — returns module-level singleton, creates if None

**`get_stream_worker() -> Optional[AlpacaStreamWorker]`** — returns running worker or None

**`start_stream(config, tickers) -> Optional[AlpacaStreamWorker]`**
- Guard: if already running, returns existing worker
- Reads `api_keys.alpaca_api_key` and `alpaca_secret_key`; returns None if missing or contain "PASTE"
- Filters to US tickers only (excludes .L)
- Imports aiohttp; creates and starts AlpacaStreamWorker; stores in `_stream_worker` module singleton

**`_parse_ws(data) -> list`** — parses raw WS bytes/str to list of dicts

**`_ensure_utc(dt) -> datetime`** — attaches UTC timezone if naive

---

# FILE 4: /home/dannyelticala/quant-fund/execution/adaptive_position_sizer.py

## A) PURPOSE

Phase-based adaptive position sizer. Determines position size as a fraction of account equity based on current "phase" (determined by completed trade count), signal score, multi-factor confluence multiplier, macro regime, HMM state, earnings quality, crowding, sector rotation, signal decay, and historical win rate. Six phases: PHASE_1 (0–100 trades) through PHASE_5 (1000–2000 trades), then PHASE_FREE (2000+ trades) using full Kelly criterion. Absolute safety limits always apply.

---

## B) CLASSES AND METHODS

### Module-level constants

```
PHASES = {
  'PHASE_1': min_trades=0, max_trades=100, base_pct=0.0015, max_pct=0.004, min_signal=0.15, max_positions=200
  'PHASE_2': min_trades=100, max_trades=300, base_pct=0.003, max_pct=0.008, min_signal=0.22, max_positions=150
  'PHASE_3': min_trades=300, max_trades=600, base_pct=0.005, max_pct=0.012, min_signal=0.30, max_positions=120
  'PHASE_4': min_trades=600, max_trades=1000, base_pct=0.007, max_pct=0.018, min_signal=0.38, max_positions=100
  'PHASE_5': min_trades=1000, max_trades=2000, base_pct=0.010, max_pct=0.025, min_signal=0.45, max_positions=75
  'PHASE_FREE': min_trades=2000, base_pct=None, max_pct=None — fully autonomous
}

ABSOLUTE_LIMITS = {
  'never_exceed_pct': 0.030   (3.0% max per trade)
  'never_below_pct':  0.001   (0.1% floor)
  'account_halt_pct': 0.15    (15% drawdown → halt)
  'sector_max_pct':   0.20
  'market_max_pct':   0.60
}
```

### Class: `AdaptivePositionSizer`

**`__init__(self, config, closeloop_store=None)`**
- Stores config and store reference
- Calls `_ensure_db()` to create `phase_history` table in `output/permanent_archive.db`

**`get_current_phase(self) -> Dict`**
- Calls `self.store.count_completed_trades()` → n
- Iterates phases in reverse order (FREE→1); returns first where `n >= phase['min_trades']`
- Returns phase dict plus `name`, `n_trades`, `progress_pct`
- DB reads: closeloop_store (count completed trades from trade_ledger)

**`get_signal_type_performance(self, signal_type: str) -> Dict`**
- Refreshes cache hourly via `self.store.get_signal_performance_by_type()`
- Returns cached performance dict for signal_type or {}
- DB reads: closeloop_store (signal performance aggregation)

**`calculate_confluence_multiplier(self, signal_type, signal_score, context, all_signals_for_ticker) -> float`**
- Starting value: 1.0
- Signal confluence: counts agreeing signal types (same direction, score > 0.2); if n>=4: ×2.0; n==3: ×1.6; n==2: ×1.3
- Macro regime: GOLDILOCKS ×1.30, RISK_ON ×1.10, RISK_OFF ×0.70, STAGFLATION ×0.60, RECESSION_RISK ×0.35, CRISIS ×0.20
- HMM state: BULL+LONG ×1.20, BEAR+SHORT ×1.20, BEAR+LONG ×0.60, BULL+SHORT ×0.60
- Earnings quality: eq > 0.7 → ×1.20; eq < 0.3 → ×0.70
- Crowding: > 0.7 → ×0.50; > 0.5 → ×0.75
- Math composite: if `abs(math_comp) > 0.6` → ×1.15
- Sector rotation: sr_score > 0.5 and LONG → ×1.10; sr_score < -0.5 and SHORT → ×1.10
- Historical win rate (requires n_trades >= 20): wr > 0.60 → ×1.25; wr > 0.55 → ×1.10; wr < 0.40 → ×0.60; wr < 0.45 → ×0.75
- Clamps output to [0.3, 3.0]

**`size_position(self, signal_score, signal_type, account_equity, ticker, context, all_signals) -> Dict`**
- Gets phase; if PHASE_FREE → routes to `size_position_autonomous`
- Else: computes `score_mult = max(0.5, min(1.5, 0.7 + signal_score * 0.6))`; calls `calculate_confluence_multiplier`
- `raw_pct = base_pct * score_mult * confluence_mult`
- `clipped_pct = max(base_pct * 0.5, min(max_pct, raw_pct))`
- `final_pct = max(0.001, min(0.030, clipped_pct))`
- `final_value = round(account_equity * final_pct, 2)`
- Returns dict with position_pct, position_value, phase, base_pct, confluence_multiplier, n_agreeing_signals, n_completed_trades, scaling_reason

**`size_position_autonomous(self, signal_type, signal_score, context, all_signals, account_equity, ticker) -> Dict`**
- PHASE_FREE path
- Gets performance for signal_type; if n_trades >= 50: computes Kelly
- `b = avg_win / max(avg_loss, 0.001)`; `kelly = (p*b - q) / max(b, 0.001)` where p=win_rate, q=1-win_rate
- `fractional_kelly = max(0.001, min(0.030, kelly * 0.25))`
- Calls `calculate_confluence_multiplier`
- Gets regime multiplier: GOLDILOCKS 1.3, RISK_ON 1.0, RISK_OFF 0.6, STAGFLATION 0.5, RECESSION_RISK 0.3, CRISIS 0.1
- Gets signal decay multiplier from `SignalDecayMonitor`: SEVERELY_DEGRADED → 0.1; DECAYING → 0.5; else 1.0
- `raw_pct = fractional_kelly * confluence_mult * regime_mult * decay_mult`
- `final_pct = max(0.001, min(0.030, raw_pct))`
- Returns dict with position_pct, position_value, kelly_fraction, confluence_multiplier, regime_multiplier, decay_multiplier, phase='PHASE_FREE'

**`should_trade(self, signal_score, signal_type=None, context=None) -> bool`**
- Phase-based: returns `signal_score >= phase['min_signal']`; if CRISIS regime → doubles threshold
- PHASE_FREE: uses `store.get_signal_performance_by_type()['min_profitable_score']` or 0.35; CRISIS multiplies 1.5x, GOLDILOCKS multiplies 0.8x

**`should_halt(self, account_equity, starting_equity=100000.0) -> bool`**
- `dd = (starting_equity - account_equity) / max(starting_equity, 1)`; returns `dd > 0.15`

**`max_new_positions(self, current_open, account_equity=100000.0) -> int`**
- Phase-based: `max(0, phase['max_positions'] - current_open)`
- PHASE_FREE: `available_pct = 0.80 - current_open * 0.01`; `estimated = int(available_pct / 0.01)`; returns `max(0, min(200, estimated))`

**`auto_trigger_discovery(self) -> None`**
- At trade milestones (200, 400, 600, 800, 1000, 1500, 2000 ± 3): triggers `SymbolicRegressionEngine.run_discovery_pipeline()`

**`get_current_phase_number(self) -> int`** — backward-compat: returns 1–6

---

# FILE 5: /home/dannyelticala/quant-fund/risk/manager.py

## A) PURPOSE

Portfolio-level risk manager. Computes Kelly-based position sizes, applies macro regime multipliers and UMCI complexity multipliers, enforces sector concentration caps, checks portfolio-level limits (drawdown halt, max market exposure), computes ATR-based stops, validates correlation between new positions and existing portfolio, and calculates portfolio statistics.

---

## B) CLASSES AND METHODS

### Class: `RiskManager`

**`__init__(self, config: dict)`**
- Reads from `config["risk"]`:
  - `self.max_pos_pct = r["max_position_pct"]` (0.05 / 5%)
  - `self.max_sector_pct = r["max_sector_exposure_pct"]` (0.25 / 25%)
  - `self.max_market_pct = r["max_market_exposure_pct"]` (0.60 / 60%)
  - `self.max_positions = r["max_total_positions"]` (20)
  - `self.halt_drawdown = r["max_drawdown_halt_pct"]` (0.15 / 15%)
  - `self.kelly_fraction = r["kelly_fraction"]` (0.5)
  - `self.corr_limit = r["correlation_limit"]` (0.75)
  - `self.atr_mult = r["atr_stop_multiplier"]` (2.0)
- `self._macro_regime: int = 0` (RISK_ON default)
- `self._frontier_store = None` (lazy-loaded)

**Regime multipliers table:**
```
0 (RISK_ON):        long_multiplier=1.2, short_multiplier=0.9
1 (GOLDILOCKS):     long_multiplier=1.0, short_multiplier=1.0
2 (STAGFLATION):    long_multiplier=1.0, short_multiplier=1.0
3 (RISK_OFF):       long_multiplier=0.7, short_multiplier=1.2
4 (RECESSION_RISK): long_multiplier=0.5, short_multiplier=0.5
```

**`set_macro_regime(self, regime: int) -> None`**
- Updates `self._macro_regime` if valid key (0–4)

**`size_position(self, ticker, signal_strength, portfolio, price_data, win_rate=0.55, avg_win_loss=1.5, direction=1) -> float`**
- Returns 0.0 immediately if `check_limits(portfolio)` fails
- Computes UMCI multiplier from `FrontierStore.get_last_umci()`:
  - umci < 30: 1.0; < 60: 0.85; < 80: 0.65; < 95: 0.30; >= 95: 0.10
- `strength = min(abs(signal_strength), 3.0) / 3.0`
- `size = kelly_size(win_rate, avg_win_loss) * strength`
- `size = min(size, max_pos_pct)` (hard cap at 5%)
- Applies regime multiplier (long_multiplier if direction >= 0, else short_multiplier)
- Sector cap: `size = min(size, max(0.0, max_sector_pct - current_sector_exposure))`
- Max positions check: if `len(portfolio["positions"]) >= max_positions` → return 0.0
- If price_data provided: runs `LiquidityScorer` — REJECT → return 0.0; REDUCE_SIZE → `size = min(size, recommended/capital_proxy)`
- Applies UMCI multiplier last: `size = size * umci_multiplier`
- Returns `round(max(0.0, size), 4)`
- DB reads: FrontierStore (last UMCI score), LiquidityScorer (no direct DB call but uses price_data)

**`kelly_size(self, win_rate, win_loss_ratio) -> float`**
- `b = max(win_loss_ratio, 1e-6)`
- `p = clip(win_rate, 0, 1)`; `q = 1 - p`
- `kelly = (b * p - q) / b`
- Returns `max(0.0, kelly * self.kelly_fraction)`

**`atr_stop(self, price_data, window=14) -> Optional[float]`**
- Returns None if fewer than `window + 1` rows
- `tr = max(H-L, |H-C.shift()|, |L-C.shift()|)` per row
- `atr = tr.rolling(window).mean().iloc[-1]`
- Returns `float(atr * self.atr_mult)` (2.0x ATR)

**`check_limits(self, portfolio) -> bool`**
- Returns False if `portfolio["drawdown"] <= -halt_drawdown` (15%)
- Returns False if `abs(portfolio["net_exposure"]) >= max_market_pct` (60%)
- Else True

**`correlation_ok(self, new_returns, portfolio_returns) -> bool`**
- For each existing position: aligns returns, skips if fewer than 20 points, checks `abs(corr) > corr_limit` (0.75)

**`portfolio_stats(self, positions, prices) -> Dict`**
- `long_val = sum(v * prices[t] for v > 0)`
- `short_val = sum(abs(v) * prices[t] for v < 0)`
- Returns gross_exposure, net_exposure, long_exposure, short_exposure, n_positions

---

## C) MATHEMATICS

### ATR (True Range) — paper_trader.py `_compute_atr` and risk/manager.py `atr_stop`

```
TR_row = max(High - Low, |High - Close.shift(1)|, |Low - Close.shift(1)|)
ATR = TR_row.rolling(window=14).mean().iloc[-1]
```

**paper_trader.py usage (stop level):**
```
atr_mult = 1.0 if direction < 0 else 1.5   (_ATR_STOP_MULTIPLIER = 1.5)
stop = entry_price - direction * atr_mult * ATR
```

**risk/manager.py usage:**
```
atr_stop_distance = ATR * 2.0   (self.atr_mult = 2.0 from config)
```

Note: paper_trader uses 1.5x ATR for longs (hardcoded constant `_ATR_STOP_MULTIPLIER`), 1.0x for shorts. RiskManager uses 2.0x from config. These are independent calculations and are NOT reconciled.

---

### Kelly Criterion — risk/manager.py `kelly_size`

```
b = max(win_loss_ratio, 1e-6)
p = clip(win_rate, 0, 1)
q = 1 - p
full_kelly = (b * p - q) / b
fractional_kelly = max(0.0, full_kelly * kelly_fraction)   # kelly_fraction = 0.5 from config
```

---

### Kelly in AdaptivePositionSizer — PHASE_FREE path

```
b = avg_win_pct / max(avg_loss_pct, 0.001)
kelly = (p * b - q) / max(b, 0.001)    # p = win_rate, q = 1 - win_rate
fractional_kelly = max(0.001, min(0.030, kelly * 0.25))   # 25% Kelly with absolute limits
```

Note: RiskManager uses 50% Kelly (`kelly_fraction=0.5`). AdaptivePositionSizer uses 25% Kelly (hardcoded `* 0.25`). These are different fractions applied in different contexts with different inputs.

---

### Position Size (phased) — adaptive_position_sizer.py `size_position`

```
score_mult = max(0.5, min(1.5, 0.7 + signal_score * 0.6))
confluence_mult = calculate_confluence_multiplier(...)   # see below, [0.3, 3.0]
raw_pct = base_pct * score_mult * confluence_mult
clipped_pct = max(base_pct * 0.5, min(max_pct, raw_pct))
final_pct = max(0.001, min(0.030, clipped_pct))
final_value = round(account_equity * final_pct, 2)
```

---

### Position Size (PHASE_FREE autonomous) — adaptive_position_sizer.py `size_position_autonomous`

```
b = avg_win / max(avg_loss, 0.001)
kelly = (p * b - q) / max(b, 0.001)
fractional_kelly = max(0.001, min(0.030, kelly * 0.25))
raw_pct = fractional_kelly * confluence_mult * regime_mult * decay_mult
final_pct = max(0.001, min(0.030, raw_pct))
position_value = round(account_equity * final_pct, 2)
```

---

### Confluence Multiplier — adaptive_position_sizer.py `calculate_confluence_multiplier`

```
multiplier = 1.0

# Signal confluence (n_agree = unique signal types in same direction with score > 0.2)
if n_agree >= 4: multiplier *= 2.0
elif n_agree == 3: multiplier *= 1.6
elif n_agree == 2: multiplier *= 1.3

# Macro regime
multiplier *= regime_mults[regime]  # GOLDILOCKS:1.30, RISK_ON:1.10, RISK_OFF:0.70, STAGFLATION:0.60, RECESSION_RISK:0.35, CRISIS:0.20

# HMM
if (BULL and LONG) or (BEAR and SHORT): multiplier *= 1.20
if (BEAR and LONG) or (BULL and SHORT): multiplier *= 0.60

# Earnings quality (eq from SimFin)
if eq > 0.7: multiplier *= 1.20
elif eq < 0.3: multiplier *= 0.70

# Crowding risk
if crowding > 0.7: multiplier *= 0.50
elif crowding > 0.5: multiplier *= 0.75

# Math composite (HMM combined signal float)
if abs(math_comp) > 0.6: multiplier *= 1.15

# Sector rotation
if sr_score > 0.5 and LONG: multiplier *= 1.10
elif sr_score < -0.5 and SHORT: multiplier *= 1.10

# Historical win rate (requires >= 20 trades for this signal type)
if wr > 0.60: multiplier *= 1.25
elif wr > 0.55: multiplier *= 1.10
elif wr < 0.40: multiplier *= 0.60
elif wr < 0.45: multiplier *= 0.75

return max(0.3, min(3.0, multiplier))
```

---

### Final Size in paper_trader.py `_process` (applied after risk manager)

```
size_pct = risk.size_position(...)          # from RiskManager (Kelly × regime × UMCI)
if observation_mode: size_pct *= 0.25
size_pct = size_pct * (1.0 + sr_mod)       # sr_mod from SectorRotationSignal, range [-0.15, +0.15]
size_pct = size_pct * (1.0 + cal_mod)      # cal_mod from CalendarEffectsSignal, range [-0.40, +0.30]
size_pct = size_pct * tier_mult             # from classify_tier(market_cap) and get_tier_size_multiplier(tier)
size_pct = min(size_pct, 0.030)            # ABSOLUTE CAP 3%
value = equity * size_pct
shares = value / price
```

---

### Volume Dry-Up Check — paper_trader.py `_check_exits`

```
avg_vol = volume_series.iloc[-21:-1].mean()   # 20-day average (excludes current day)
last_vol = volume_series.iloc[-1]
if last_vol < avg_vol * 0.30:                  # _VOL_DRY_UP_THRESHOLD = 0.30
    dry_up_days += 1
    if dry_up_days >= 2:                       # _VOL_DRY_UP_DAYS = 2
        exit position
else:
    dry_up_days = 0
```

---

### Target Price Calculation — paper_trader.py `_process`

```
atr = _compute_atr(ticker, price_df=price_df)
surprise_pct = abs(signal.get("surprise_pct", 0.05))
target_delta = max(surprise_pct * price, atr)
target_price = price + direction * target_delta
```

---

### Scale-Out Trigger — paper_trader.py `_check_exits`

```
pnl_pct = direction * (price - entry) / entry        # unrealised return fraction
target_pct = target_delta / entry                    # if entry != 0 else 0.05
# Scale-out levels: 0.50 and 1.00 (of target_pct)
# Scale-out fractions: 0.33 and 0.33
if pnl_pct >= level * target_pct:                   # e.g. >= 0.50 * target_pct
    shares_out = pos["shares"] * fraction            # 33% of remaining shares
```

---

### Contradiction Score — paper_trader.py `_apply_contradiction_scores`

```
long_wt  = sum(score for LONG signals)
short_wt = sum(score for SHORT signals)
total_wt = long_wt + short_wt
minority_wt = min(long_wt, short_wt)
contradiction_score = 2.0 * minority_wt / total_wt   # 0.0=consensus, 1.0=50/50

if contradiction_score < 0.2: score = min(score * 1.15, 5.0)
elif contradiction_score > 0.6: score = score * 0.75
```

---

### Real-Time Confidence Boost — paper_trader.py `_process`

```
rt_move = get_stream_cache().get_move_pct(ticker, window_minutes=5)
if direction * rt_move > 2.0:
    boost = min(0.5, abs(rt_move) / 20.0)
    confidence += boost
elif direction * rt_move < -3.0:
    confidence *= 0.85
```

---

### Short RSI+Momentum Check — paper_trader.py `pre_short_checklist`

```
rsi = TechnicalIndicatorCalculator.rsi(price_data)
mom_20d = (closes[-1] - closes[-21]) / closes[-21]
short_momentum_ok = (rsi > 65 and mom_20d < 0)
```

---

### Max Hold Days (restart-aware) — paper_trader.py `_check_exits`

```
total_days = days_held_at_restore + (date.today() - restore_date).days
if total_days >= max_hold_days:
    close position
```

---

### PriceCache Spike Detection — alpaca_stream.py `_check_alert`

```
# For each price point within the window:
if abs((now_price - px) / px * 100) >= threshold:
    return label
# SPIKE: window=5min, threshold=2.0%
# URGENT: window=10min, threshold=5.0%
```

---

### PriceCache Move Percentage — alpaca_stream.py `get_move_pct`

```
# Finds earliest price point in history within window_minutes
current = history[-1][1]
baseline = first price in history where ts >= now - window
return (current - baseline) / baseline * 100.0
```

---

### PaperBroker Position Average Cost — broker_interface.py `_update_position`

```
old = current_position_shares
new = old + delta
if new ≈ 0: remove position
elif old == 0 or sign changed:
    avg_prices[ticker] = new_price   (reset)
else:
    avg_prices[ticker] = (avg_prices[ticker] * abs(old) + price * abs(delta)) / abs(new)
```

---

### RiskManager Position Size (full sequence) — risk/manager.py `size_position`

```
strength = min(abs(signal_strength), 3.0) / 3.0
size = kelly_size(win_rate=0.55, avg_win_loss=1.5) * strength
size = min(size, 0.05)                             # max_pos_pct
size = size * regime_mult                          # 0.5 to 1.2
size = min(size, max(0.0, 0.25 - current_sector_exposure))   # sector cap
# [liquidity scorer adjustment if applicable]
size = size * umci_multiplier                      # 0.10 to 1.0
return round(max(0.0, size), 4)
```

---

## D) DATA FLOWS

### paper_trader.py

**Inputs:**
- config dict (paper_trading, alpaca, api_keys, risk, backtest settings)
- price data from DataFetcher
- universe tickers from UniverseManager
- earnings data from EarningsCache
- signals from all signal generators
- live prices from AlpacaStream cache or yfinance
- Alpaca broker responses (order status, position list)
- DB: closeloop/storage/closeloop.db (open positions at startup, trade_ledger queries)
- DB: output/historical_db.db (shipping data for context)
- DB: output/permanent_log.db (geopolitical risk for context)
- DB: output/permanent_archive.db (signals_log reads for phase detection)

**Outputs:**
- DB: closeloop/storage/closeloop.db (INSERT/UPDATE trade_ledger via ClosedLoopStore.record_trade)
- DB: output/permanent_archive.db (INSERT signals_log)
- File: logs/paper_trading.jsonl (appended)
- Alpaca API: POST /v2/orders, GET /v2/positions
- Telegram: via Notifier (cooling-off loss alerts, trailing stop tier-up alerts)
- self.active dict (in-memory position state)

---

### broker_interface.py

**PaperBroker:** all in-memory (cash, positions dict, avg_prices dict, _log list). No DB reads or writes.

**AlpacaPaperBroker:**
- Inputs: Alpaca REST API responses
- Outputs: Alpaca REST API calls (GET /v2/account, GET /v2/positions, POST /v2/orders, DELETE /v2/positions/{ticker}, DELETE /v2/orders/{order_id}, GET /v2/orders, GET /v2/clock)
- Data endpoint: GET /v2/stocks/{ticker}/quotes/latest, GET /v2/stocks/{ticker}/bars

---

### alpaca_stream.py

**Inputs:** WebSocket frames from `wss://stream.data.alpaca.markets/v2/iex` (bar events only)
**Outputs:**
- PriceCache singleton (in-memory updates)
- Telegram via Notifier (URGENT alerts only)
- logs/alpaca_stream.log (rotating file handler)

---

### adaptive_position_sizer.py

**Inputs:** signal scores, context dicts, closeloop_store (for trade count and performance)
**Outputs:** position_pct and position_value dicts
**DB reads:** output/permanent_archive.db (phase_history create); closeloop_store.count_completed_trades(); closeloop_store.get_signal_performance_by_type()
**DB writes:** output/permanent_archive.db (CREATE TABLE phase_history — but no INSERTs seen in code)

---

### risk/manager.py

**Inputs:** portfolio dict, price_data DataFrame, signal strength floats
**Outputs:** position size float, ATR stop float, boolean checks
**DB reads:** FrontierStore (last UMCI score from frontier DB), LiquidityScorer (no direct DB)
**DB writes:** none

---

## E) DEPENDENCIES

### paper_trader.py

**Internal Apollo imports:**
- `analysis.market_calendar.MarketCalendar`
- `closeloop.storage.closeloop_store.ClosedLoopStore`
- `execution.adaptive_position_sizer.AdaptivePositionSizer`
- `data.universe.Universe`, `UniverseManager`, `classify_tier`, `get_tier_size_multiplier`
- `data.fetcher.DataFetcher`
- `execution.broker_interface.AlpacaPaperBroker`, `PaperBroker`
- `risk.manager.RiskManager`
- `signals.pead_signal.PEADSignal`
- `signals.momentum_signal.MomentumSignal`
- `signals.mean_reversion_signal.MeanReversionSignal`
- `signals.gap_signal.GapSignal`
- `signals.insider_momentum_signal.InsiderMomentumSignal`
- `signals.calendar_effects_signal.CalendarEffectsSignal`
- `signals.options_earnings_signal.OptionsEarningsSignal`
- `signals.short_selling_filter.ShortSellingFilter`
- `signals.sector_rotation_signal.SectorRotationSignal`, `TICKER_TO_SECTOR`
- `analysis.mathematical_signals.MathematicalSignals`
- `analysis.crowding_detector.CrowdingDetector`
- `analysis.macro_signal_engine.MacroSignalEngine`
- `analysis.signal_decay_monitor.SignalDecayMonitor`
- `analysis.regime_detector.RegimeDetector`
- `analysis.technical_indicators.TechnicalIndicatorCalculator`
- `data.collectors.simfin_collector.SimFinCollector`
- `data.collectors.sec_fulltext_collector.SECFullTextCollector`
- `data.collectors.alternative_quiver_collector.AlternativeQuiverCollector`
- `data.collectors.rates_credit_collector.RatesCreditCollector`
- `data.collectors.advanced_news_intelligence.AdvancedNewsIntelligence`
- `data.earnings_calendar.EarningsCalendar`
- `data.earnings_cache.EarningsCache`
- `execution.trailing_stops.TrailingStopManager`
- `execution.cooling_off_tracker.StockCoolingOffTracker`
- `execution.alpaca_stream.get_stream_cache`
- `closeloop.integration.signal_aggregator.SignalAggregator`
- `closeloop.autopsy.trade_autopsy.TradeAutopsy`
- `altdata.notifications.notifier.Notifier`

**External libraries:** `json`, `logging`, `time`, `datetime`, `pathlib.Path`, `typing`, `numpy`, `pandas`, `schedule`, `yfinance`, `sqlite3`, `vaderSentiment.vaderSentiment.SentimentIntensityAnalyzer`

---

### broker_interface.py

**Internal Apollo imports:** none
**External:** `logging`, `abc`, `datetime`, `typing`, `pandas`, `requests`, `yfinance` (optional fallback in `get_price`)

---

### alpaca_stream.py

**Internal Apollo imports:**
- `altdata.notifications.notifier.Notifier`

**External:** `asyncio`, `json`, `logging`, `threading`, `time`, `collections.defaultdict/deque`, `datetime`, `logging.handlers.RotatingFileHandler`, `pathlib.Path`, `typing`, `aiohttp` (runtime import in `_session`)

---

### adaptive_position_sizer.py

**Internal Apollo imports:**
- `closeloop.storage.closeloop_store.ClosedLoopStore` (via constructor injection)
- `analysis.signal_decay_monitor.SignalDecayMonitor` (runtime import in `size_position_autonomous`)
- `analysis.symbolic_regression.SymbolicRegressionEngine` (runtime import in `auto_trigger_discovery`)

**External:** `logging`, `sqlite3`, `os`, `datetime`, `typing`

---

### risk/manager.py

**Internal Apollo imports:**
- `frontier.storage.frontier_store.FrontierStore` (runtime import in `size_position`)
- `deepdata.microstructure.liquidity_scorer.LiquidityScorer` (runtime import in `size_position`)

**External:** `logging`, `typing`, `numpy`, `pandas`

---

## F) WIRING STATUS

### paper_trader.py
**Connected to live trading path: YES — central coordinator.**
- `PaperTrader.run()` is the main event loop
- `run_scan()` → `_process()` → `broker.place_order()` → Alpaca API (if configured)
- `_check_exits()` → `_close_position()` → `broker.place_order()` → Alpaca API
- `_store.record_trade()` → trade_ledger DB

### broker_interface.py
**Connected: YES.**
- `AlpacaPaperBroker` is instantiated by `PaperTrader.__init__` when Alpaca configured
- `place_order()` is called by `_process()` and `_close_position()` and scale-out logic
- `PaperBroker` used when Alpaca keys absent — simulation mode

### alpaca_stream.py
**Connected: PARTIAL — stream starts externally.**
- `start_stream()` must be called by an external entry point (main.py or similar); NOT called anywhere in paper_trader.py or broker_interface.py
- Once running, `get_stream_cache()` is used by `paper_trader._get_live_price()` and `_process()` real-time boost
- If `start_stream()` is never called, stream is never started but `get_stream_cache()` still works (returns stale/empty cache)
- No code in GROUP 2 files calls `start_stream()`

### adaptive_position_sizer.py
**Connected: PARTIAL — two instances created, but sizing path uses RiskManager not AdaptivePositionSizer.**
- `self.sizer` (line 142) and `self.adaptive_sizer` (line 289) are both created in `PaperTrader.__init__`
- Neither `self.sizer` nor `self.adaptive_sizer` is called in `_process()`
- `_process()` calls `self.risk.size_position(...)` (RiskManager) not AdaptivePositionSizer
- AdaptivePositionSizer is created but its `size_position()` method is never invoked in the trade path

### risk/manager.py
**Connected: YES — called in every trade.**
- `self.risk.size_position(...)` called in `_process()` to compute position size
- `self.risk.set_macro_regime(...)` called in `_capture_context()` and `build_full_context()`
- `self.risk.check_limits()` called inside `size_position()` itself

---

## G) ISSUES FOUND

### paper_trader.py

1. **AdaptivePositionSizer created twice** (lines 142 and 289 in `__init__`). Two instances created: `self.sizer` and `self.adaptive_sizer`. Neither is called in `_process()`. The trade path uses `self.risk` (RiskManager). AdaptivePositionSizer is entirely unused in the live trade flow.

2. **Alpaca stream never started from paper_trader.py.** `start_stream()` is imported in GROUP 2 files for cache access only (`get_stream_cache()`). If the external entry point does not call `start_stream()`, the real-time price cache is always empty and all price lookups fall back to yfinance. No error is raised — the system silently runs without real-time prices.

3. **Intraday exit loop bug.** The `for hh in range(10, 17): for mm in ("00", "30"):` loop at lines 523–528 creates lambdas with `m="us"` for all entries. The `.do(lambda m="us": self._check_exits(m))` always passes "us". UK intraday exit is only scheduled at 16:45 (one explicit call). UK positions are not checked intraday during their open hours.

4. **Duplicate close guard relies on Alpaca API availability.** In `_close_position`, if `self.broker.get_positions()` raises an exception, `positions` is set to None, and the code uses `pos.get("shares", 0)` as fallback. The duplicate guard (confirming position absence) is only active when positions list is non-empty and non-None. If Alpaca returns empty list (possible API error), the code proceeds with `pos.get("shares", 0)` — this is the correct fallback for empty list, but the comment on line 2015 says "If positions is None (exception) or empty list (possible API error), proceed" which contradicts line 2016 which handles the empty-list case separately from None.

5. **Phantom-close guard in reconcile_positions sets exit_price = entry_price (gross_pnl = 0).** This means phantom-closed trades always record 0 P&L even if position had unrealised gain. This skews phase progression counts and performance stats.

6. **context_at_open is empty dict {} for all restored positions.** When bot restarts, `_load_existing_positions()` sets `context_at_open={}` for all restored positions. When `_close_position()` calls `TradeAutopsy.run(closed_trade, context_at_open)`, the autopsy receives no context. The autopsy may silently produce incorrect or empty analysis for restored positions.

7. **ATR is 0 for all restored positions.** `atr=0` is hardcoded in both restoration paths (Alpaca and closeloop). The comment at line 1851–1853 notes this: "When atr==0 the stop degenerates to entry_price itself, which would fire immediately on any tick — skip until ATR is properly set." The skip (`if atr > 0`) means restored positions have NO ATR stop protection until the bot generates a new signal for that ticker and recomputes ATR. There is no code path that recomputes ATR for restored positions.

8. **Order placement call signature mismatch for scale-out.** At line 1908, scale-out calls: `self.broker.place_order(ticker, shares_out, close_dir, 'market', None, close_dir, price)`. For `AlpacaPaperBroker`, the signature is `place_order(ticker, qty, side, order_type, limit_price, direction, fill_price)` — the 6 positional arguments match. For `PaperBroker`, the signature is `place_order(ticker, quantity, direction, fill_price, order_type)` — this call passes wrong values. The scale-out call is not routed through the AlpacaBroker type check (unlike the entry order at lines 1376–1383). Scale-out will malfunction with PaperBroker.

9. **signals_log INSERT happens only if `_is_filled`** (not `_is_submitted`). Alpaca paper orders return "submitted" not "filled". The signal is logged to signals_log only for actual fills. For all Alpaca paper trades, `was_traded=True` is never written to signals_log. The learning loop's signals_log data will be incomplete for Alpaca paper mode.

10. **`_log_performance()` calls `self.broker.get_pnl(prices)` but AlpacaPaperBroker has no `get_pnl` method.** `AlpacaPaperBroker` only has `get_account_value()`. This will raise `AttributeError` every time `run_eod()` is called with Alpaca broker. The exception is caught by the outer `try/except Exception as e` at line 2351 which logs the error but continues.

11. **`_scan_price_cache` accessed before assignment.** `_passes_correlation_check` at line 2263 calls `getattr(self, '_scan_price_cache', None)`. The attribute is only assigned during `run_scan()` at line 613. If `_passes_correlation_check` is called outside of an active scan (e.g. via `check_exit_conditions`), `cached` will be None and correlation falls back to yfinance download.

12. **Article intelligence VADER sentiment attaches `sentiment_exit_pressure` to actions but no exit code acts on it.** At line 754–755, `action['sentiment_exit_pressure'] = True` is set, but there is no code in `_check_exits()` or `_close_position()` that reads `sentiment_exit_pressure`. This field has no effect on trade exits.

13. **`exit_date=None` for all restored positions.** The time-based exit check at line 1946 will never fire for restored positions since their `exit_date` is None. Combined with ATR=0, only trailing stop, volume dry-up, or max_hold_days can exit a restored position. Max_hold_days IS restart-aware (uses `days_held_at_restore`).

14. **Duplicate `from closeloop.storage.closeloop_store import ClosedLoopStore` initialization.** At lines 132–137, `self.closeloop` is created for AdaptivePositionSizer injection. At lines 583–589 inside `run_scan()`, `self._store` is also initialized. Both reference the same ClosedLoopStore but are separate instances throughout.

---

### broker_interface.py

15. **`AlpacaPaperBroker.place_order` truncates qty to integer.** `'qty': str(int(max(1, qty)))` — fractional shares are silently truncated. If the calculated `shares = value / price` is e.g. 1.7, only 1 share is sent to Alpaca. The position size mismatch between `self.active["shares"]` (1.7) and actual Alpaca position (1 share) is never reconciled.

16. **`AlpacaPaperBroker.get_positions` returns list (correct for Alpaca), but `BrokerInterface` abstract method declares return type as `Dict[str, float]`.** The abstract type annotation says `Dict[str, float]` but Alpaca returns a list of dicts. Code in paper_trader correctly handles both via `isinstance(raw_positions, list)` checks, but the interface contract is violated.

17. **`AlpacaPaperBroker` does not have `get_pnl()`.** PaperBroker has `get_pnl(current_prices)` but AlpacaPaperBroker does not. `PaperTrader._log_performance()` calls `self.broker.get_pnl(prices)` which will raise AttributeError when broker is AlpacaPaperBroker.

---

### alpaca_stream.py

18. **Stream subscribes to ALL bars (`bars: ["*"]`) and filters locally.** This means Alpaca streams every bar for every traded symbol on IEX. For large universes this could be a high message volume. Filtering happens in `_on_bar()` by checking `ticker not in self._universe`. This is correct but bandwidth-intensive.

19. **`_check_alert` iterates ALL history to find a price within the window, not just the most recent price at the start of the window.** It returns the label for the FIRST qualifying price in history, not specifically the price at exactly `now - window`. This means if a ticker spiked 5 minutes ago and partially recovered, the URGENT alert can still fire on the partially recovered price.

20. **No reconnect exponential backoff.** `_RECONNECT_DELAY = 30` seconds is fixed. All reconnect attempts use the same 30-second delay regardless of consecutive failure count.

---

### adaptive_position_sizer.py

21. **`phase_history` table is created but never written to.** `_ensure_db()` creates the table. There is no `INSERT` into `phase_history` anywhere in this file. Phase transitions are never recorded in the database despite the table existing for that purpose.

22. **`size_position_autonomous` fallback uses `PHASES['PHASE_5']['base_pct']` (0.010 = 1.0%) when fewer than 50 trades exist for the signal type.** This fallback is hardcoded and is applied before the ABSOLUTE_LIMITS clipping, meaning the autonomous path can default to 1% position size regardless of phase or context if the signal type is new.

23. **AdaptivePositionSizer is not connected to the live trade path.** It is created twice in PaperTrader but never called during `_process()`. The entire multi-phase adaptive sizing system is bypassed in live trading.

---

### risk/manager.py

24. **ATR stop multiplier in RiskManager (`atr_mult = 2.0`) differs from the ATR stop multiplier used in PaperTrader (`_ATR_STOP_MULTIPLIER = 1.5` for longs, `1.0` for shorts).** RiskManager's `atr_stop()` method is never called in paper_trader.py. The stop is computed inline in `_check_exits()` using the hardcoded `_ATR_STOP_MULTIPLIER`. RiskManager's `atr_stop()` method is entirely unused in the live trade path.

25. **`correlation_ok()` is not called in the live trade path.** PaperTrader has its own `_passes_correlation_check()` using a different threshold (0.6 vs RiskManager's `corr_limit = 0.75`). RiskManager.`correlation_ok()` is never invoked from paper_trader.py.

26. **`portfolio_stats()` is not called anywhere in paper_trader.py.** PaperTrader has its own `_portfolio_state()` method. RiskManager's `portfolio_stats()` is unused.

27. **`check_limits()` drawdown check uses signed comparison: `if portfolio.get("drawdown", 0.0) <= -self.halt_drawdown`.** PaperTrader's `_portfolio_state()` computes drawdown as `(value - initial) / initial` — positive when in profit, negative when in loss. The check `<= -0.15` correctly catches a 15%+ loss. However, `initial` in `_portfolio_state()` is `getattr(self.broker, "initial_capital", value)` — for `AlpacaPaperBroker`, there is no `initial_capital` attribute, so `initial = value` always, `dd = 0` always, and the drawdown halt never fires.

---

# SECTION 2A — TRADE OPEN SEQUENCE

From "scan fires" to "trade recorded in DB":

**Step 1: Scan trigger** — `schedule` fires `scan_us()` at 14:45 UTC (or `scan_uk()` at 08:15 UTC)
File: `execution/paper_trader.py:scan_us` → `run_scan(market="us")`

**Step 2: Universe + price data fetch** — `UniverseManager.get_universe(market)` returns ticker list; `self.fetcher.fetch_universe_data(tickers, start, end, market)` fetches 600 days of OHLCV
File: `execution/paper_trader.py:run_scan`

**Step 3: Earnings cache refresh (if needed)** — `EarningsCache.bulk_fetch(tickers)` if `needs_refresh()`
File: `execution/paper_trader.py:run_scan`

**Step 4: Signal evaluation** — PEAD generator called per ticker; extended generators (MOMENTUM, MEAN_REV, GAP, INSIDER_MOM) called per ticker; results collected as `todays` signals
File: `execution/paper_trader.py:run_scan`

**Step 5: Article intelligence** — `AdvancedNewsIntelligence.collect_and_analyse(signal_tickers)` + VADER sentiment; attaches `news_sentiment` and `article_narrative_shift` to action dicts
File: `execution/paper_trader.py:run_scan`

**Step 6: _process called** — for each signal row meeting the `entry_date >= today` filter
File: `execution/paper_trader.py:_process`

**Step 7: Multi-signal enrichment** — supporting signals gathered from all generators for confluence calculation
File: `execution/paper_trader.py:_process` (lines 1096–1188)

**Step 8: Real-time confidence boost** — stream cache queried for 5-min move; confidence adjusted ±
File: `execution/paper_trader.py:_process` (lines 1193–1212)

**Step 9: Existing position check** — if ticker in `self.active` and same direction → skip; if opposite → `_close_position(reason="signal_reversal")`
File: `execution/paper_trader.py:_process` (lines 1214–1220)

**Step 10: Confidence threshold check** — if `abs(confidence) < min_confidence` → log OBSERVED_NOT_TRADED to jsonl and signals_log; return obs_entry
File: `execution/paper_trader.py:_process` (lines 1222–1239)

**Step 11: Price retrieval** — from price_df close column, overlaid with real-time cache
File: `execution/paper_trader.py:_process` (lines 1241–1256)

**Step 12: Correlation / concentration check** — `_passes_correlation_check(ticker, direction, sector)` checks sector (max 3) and Pearson correlation (max 2 with corr > 0.6)
File: `execution/paper_trader.py:_passes_correlation_check`

**Step 13: Portfolio state cache** — `_portfolio_state()` called once per scan (cached on instance)
File: `execution/paper_trader.py:_process` (lines 1264–1268)

**Step 14: Position sizing via RiskManager** — `self.risk.size_position(ticker, signal_strength, portfolio, price_data=empty_df)` → Kelly × strength × sector cap × UMCI multiplier
File: `risk/manager.py:size_position`

**Step 15: Observation mode scaling** — if enabled: `size_pct *= 0.25`
File: `execution/paper_trader.py:_process` (lines 1278–1281)

**Step 16: Sector rotation modifier** — `size_pct *= (1.0 + sr_mod)`
File: `execution/paper_trader.py:_process` (lines 1282–1288)

**Step 17: Calendar effects modifier** — `size_pct *= (1.0 + cal_mod)` (cached per scan)
File: `execution/paper_trader.py:_process` (lines 1290–1300)

**Step 18: Tier multiplier** — `classify_tier(market_cap)` → `get_tier_size_multiplier(tier)` → `size_pct *= tier_mult`
File: `execution/paper_trader.py:_process` (lines 1302–1315)

**Step 19: Absolute cap enforcement** — `size_pct = min(size_pct, 0.030)`
File: `execution/paper_trader.py:_process` (line 1318)

**Step 20: SignalAggregator check** — `SignalAggregator.aggregate(ticker, pead_signal, context_signals)` → if `kelly_multiplier == 0` → return None; fails open
File: `execution/paper_trader.py:_process` (lines 1320–1355)

**Step 21: Cooling-off check** — `StockCoolingOffTracker.is_cooling_off(ticker)` → if True → return None
File: `execution/paper_trader.py:_process` (lines 1357–1365)

**Step 22: Shares calculation** — `equity = broker.get_account_value()`; `value = equity * size_pct`; `shares = value / price`
File: `execution/paper_trader.py:_process` (lines 1367–1370)

**Step 23: Order placement** — `broker.place_order(ticker, shares, side, direction=order_dir, fill_price=price)` → Alpaca POST /v2/orders
File: `execution/broker_interface.py:AlpacaPaperBroker.place_order`

**Step 24: Alpaca response handling** — if status "submitted" or "filled": proceed; else: return None
File: `execution/paper_trader.py:_process` (lines 1387–1391)

**Step 25: ATR computation** — `_compute_atr(ticker, price_df=price_df)` from pre-fetched data
File: `execution/paper_trader.py:_compute_atr`

**Step 26: Target price calculation** — `target_delta = max(surprise_pct * price, atr)`; `target_price = price + direction * target_delta`
File: `execution/paper_trader.py:_process` (lines 1393–1396)

**Step 27: Context capture** — `_capture_context(ticker, signal)` → builds full context dict (macro, VIX, HMM, earnings quality, SEC crisis, shipping stress, geopolitical risk, signals_at_entry)
File: `execution/paper_trader.py:_capture_context`

**Step 28: In-memory state update** — `self.active[ticker] = {...}` dict populated with all position fields; `self._cached_portfolio = None` (invalidated)
File: `execution/paper_trader.py:_process` (lines 1444–1461)

**Step 29: TrailingStopManager registration** — `_trailing_stops.add_position(ticker, entry_price, current_price)`
File: `execution/paper_trader.py:_process` (lines 1463–1468)

**Step 30: DB write — trade_ledger INSERT** — `self._store.record_trade(trade_record, context_at_open)` where `order_status = result.get('status', 'submitted')` (typically "submitted" for Alpaca paper)
File: `closeloop.storage.closeloop_store:record_trade` (called from `execution/paper_trader.py:_process` line 1489)

**Step 31: Trade ID stored in self.active** — `self.active[ticker]["trade_id"] = trade_id` returned from record_trade
File: `execution/paper_trader.py:_process` (line 1490)

**Step 32: signals_log INSERT** — only if `_is_filled` (NOT "submitted") — for Alpaca paper, this is NEVER reached because Alpaca returns "submitted"
File: `execution/paper_trader.py:_process` (lines 1513–1519) — NOTE: this is an issue; see ISSUES FOUND #9

**Step 33: paper_trading.jsonl append** — `_append_log({type:"trade_open", ...})`
File: `execution/paper_trader.py:_process` (lines 1498–1511)

**Step 34: Log message** — `logger.info("Opened BUY/SHORT {ticker} @ {price} | target | stop | ATR | obs_mode")`
File: `execution/paper_trader.py:_process` (lines 1520–1525)

**Note: Telegram notification at trade open is NOT present.** No Telegram call is made on trade open.

---

# SECTION 2B — TRADE CLOSE SEQUENCE

From "exit condition detected" to "DB updated":

### Exit reasons and detection points:

**Exit: atr_stop** — detected in `_check_exits()` line 1858–1862
- Condition: `(direction > 0 and price <= stop) or (direction < 0 and price >= stop)`
- Triggered only if `atr > 0` (restored positions skipped)
- Stop level: `entry - direction * atr_mult * atr` (1.5x for longs, 1.0x for shorts)

**Exit: trailing_stop** — detected in `_check_exits()` line 1888–1891
- Condition: `TrailingStopManager.should_exit(ticker, price)` returns True
- Activates after 5% gain (per TrailingStopManager logic)

**Exit: scale_out** — partial exit in `_check_exits()` lines 1901–1922
- Not a full close; partial shares reduced at 50% and 100% of target_pct
- Calls `broker.place_order(...)` directly; does NOT call `_close_position()`
- DB update: NOT triggered for scale-outs — no DB record is written when partial exits occur
- self.active["shares"] is reduced; self.active["scale_out_done"] appended

**Exit: volume_dry_up** — detected in `_check_exits()` lines 1924–1939
- Condition: `last_vol < avg_vol * 0.30` for `>= 2` consecutive days

**Exit: time_exit** — detected in `_check_exits()` lines 1944–1950
- Condition: `pos["exit_date"]` is not None and `pd.Timestamp(exit_date) <= today`
- Restored positions have `exit_date=None` so this never fires for them

**Exit: max_hold_days** — detected in `_check_exits()` lines 1953–1969
- Condition: `total_days >= max_hold_days` (default 18 from config `paper_trading.max_hold_days`)
- Restart-aware: `total_days = days_held_at_restore + (today - restore_date).days`

**Exit: signal_reversal** — detected in `_process()` lines 1216–1218
- Condition: ticker in `self.active` with opposite direction

### `_close_position()` sequence:

**Step 1: Duplicate close guard** — `self.active.get(ticker)` → returns immediately if None
File: `execution/paper_trader.py:_close_position` (line 1977)

**Step 2: Live price retrieval** — if price not supplied, calls `_get_live_price(ticker)`
File: `execution/paper_trader.py:_close_position` (lines 1981–1982)

**Step 3: Alpaca position membership check** — `broker.get_positions()` → if ticker not in Alpaca list: remove from self.active, return (no DB write)
File: `execution/paper_trader.py:_close_position` (lines 1983–2003)

**Step 4: Shares determination** — from Alpaca positions list `qty` field; falls back to `pos.get("shares")` if positions empty or None
File: `execution/paper_trader.py:_close_position` (lines 2005–2019)

**Step 5: Zero shares guard** — if shares == 0: delete from self.active, return
File: `execution/paper_trader.py:_close_position` (lines 2021–2023)

**Step 6: Close order placement** — `broker.place_order(ticker, abs(shares), close_dir, 'market', None, close_dir, price)` → Alpaca DELETE /v2/positions or sell order
File: `execution/broker_interface.py:AlpacaPaperBroker.place_order`

**Step 7: Broker rejection guard** — if `close_result.get('status') == 'error'`: logs error, returns WITHOUT removing from self.active, WITHOUT DB write
File: `execution/paper_trader.py:_close_position` (lines 2026–2031)

**Step 8: P&L calculation** — `pnl_pct = direction * (price - entry) / entry * 100`; `holding_days`; `return_pct = pnl_pct / 100.0`
File: `execution/paper_trader.py:_close_position` (lines 2032–2046)

**Step 9: Phantom detection** — if `(not _market_was_open) and (abs(pnl_pct) < 0.001 or _held_minutes < 30)` → `_is_phantom = True`
File: `execution/paper_trader.py:_close_position` (lines 2051–2070)

**Step 10: Cooling-off registration** — if not phantom: `StockCoolingOffTracker.register_exit(ticker, exit_date, exit_price, pnl_pct, exit_reason)`; if `return_pct < 0`: Telegram notification "COOLING OFF: {ticker} locked 5 days"
File: `execution/paper_trader.py:_close_position` (lines 2073–2092)

**Step 11: DB write — trade close** — `self._store.record_trade(closed_trade, context_at_open)` where closed_trade includes ticker, market, direction, entry_date, exit_date, entry_price, exit_price, position_size, gross_pnl, net_pnl, holding_days, exit_reason, sector, is_phantom
File: `closeloop.storage.closeloop_store:record_trade` — this is the UPDATE path if trade_id exists in ledger (record with entry), else INSERT new record. The actual UPDATE vs INSERT logic is inside ClosedLoopStore.
File: `execution/paper_trader.py:_close_position` (lines 2094–2115)

**Step 12: context_at_open recovery** — `context_at_open = pos.get("context_at_open", {})` — for restored positions this is `{}` (empty)
File: `execution/paper_trader.py:_close_position` (line 2036)

**Step 13: Trade autopsy** — `TradeAutopsy(store, config).run(closed_trade_for_autopsy, context_at_open)` — always triggered; may produce empty analysis if context is {}
File: `closeloop/autopsy/trade_autopsy.py:run` (called from `execution/paper_trader.py:_close_position` line 2136)

**Step 14: paper_trading.jsonl append** — `_append_log({type:"trade_close", ...})`
File: `execution/paper_trader.py:_close_position` (lines 2141–2155)

**Step 15: Remove from self.active** — `del self.active[ticker]`
File: `execution/paper_trader.py:_close_position` (line 2160)

**Step 16: TrailingStopManager deregistration** — `_trailing_stops.remove_position(ticker)`
File: `execution/paper_trader.py:_close_position` (lines 2161–2164)

**Note: Telegram notification at trade close is NOT sent by `_close_position()` itself.** Cooling-off Telegram is sent only on losses. No general "position closed" Telegram notification.

---

# SECTION 2C — POSITION RESTORE SEQUENCE (on restart)

**Trigger:** `PaperTrader.__init__()` calls `self._load_existing_positions()` at the very end of initialization.

**Two restore paths run in sequence:**

### Path 1: Alpaca-sourced restore

**DB query:** `self.broker.get_positions()` → HTTP GET `{base_url}/v2/positions`
Returns list of Alpaca position dicts with fields: `symbol`, `qty`, `side`, `avg_entry_price`, `market_value`, `unrealized_pl`, etc.

**Condition:** `self.use_alpaca and isinstance(self.broker, AlpacaPaperBroker) and self.broker._connected`

**Fields populated in restored position:**
```python
{
  "direction": 1 if float(p["qty"]) > 0 else -1,
  "entry_price": float(p["avg_entry_price"]),
  "exit_date": None,                          # MISSING — time exit never fires
  "atr": 0,                                   # MISSING — ATR stop never fires
  "target_price": 0,                          # MISSING — scale-out degrades
  "target_delta": 0,                          # MISSING — scale-out denominator is 0
  "scale_out_done": [],
  "sector": "UNKNOWN",                        # MISSING — correlation check uses UNKNOWN
  "shares": abs(float(p["qty"])),
  "dry_up_days": 0,
  "market": "us",                             # HARDCODED — UK Alpaca positions misclassified
  "context_at_open": {},                      # MISSING — autopsy receives empty context
  "signal_type": "restored",
  "tier": "TIER_1_SMALLCAP",                  # HARDCODED
  "entry_date": datetime.utcnow().isoformat(), # WRONG — reset to now, not original entry
  "days_held_at_restore": 0,                  # WRONG — set to 0, not actual days held
  "restore_date": datetime.utcnow().date(),
}
```

### Path 2: ClosedLoop-sourced restore

**DB query:** `self.closeloop.get_open_positions()` → reads trade_ledger WHERE exit_date IS NULL
Returns list with fields: `ticker`, `direction`, `entry_price`, `position_size`, `entry_date`, `signal_type`

**Condition:** ticker not already added from Alpaca restore

**Fields populated in restored position:**
```python
{
  "direction": int(p["direction"]),
  "entry_price": float(p["entry_price"]),
  "exit_date": None,                         # MISSING
  "atr": 0,                                  # MISSING
  "target_price": 0,                         # MISSING
  "target_delta": 0,                         # MISSING
  "scale_out_done": [],
  "sector": "UNKNOWN",                       # MISSING
  "shares": float(p["position_size"]),
  "dry_up_days": 0,
  "market": "us",                            # HARDCODED
  "context_at_open": {},                     # MISSING
  "signal_type": p["signal_type"],           # populated from closeloop record
  "tier": "TIER_1_SMALLCAP",                # HARDCODED
  "entry_date": p["entry_date"],             # preserved from closeloop record (correct)
  "days_held_at_restore": _days_held,        # computed from entry_date to today (correct)
  "restore_date": date.today(),
}
```

### Missing fields summary:
- `exit_date` — None → time exit never fires; position held indefinitely
- `atr` — 0 → ATR stop skipped entirely until position is naturally replaced
- `target_price` — 0 → scale-out trigger `pnl_pct >= level * target_pct` where target_pct = 0/entry = 0; scale-out fires immediately on ANY positive tick for longs
- `target_delta` — 0 → same issue
- `sector` — "UNKNOWN" → UNKNOWN is treated as a sector; max-3-per-sector check counts UNKNOWNs together
- `market` — hardcoded "us" → UK positions misclassified (cannot be reached via UK exit schedule)
- `context_at_open` — {} → autopsy produces no meaningful analysis
- `entry_date` (Alpaca path only) — reset to utcnow instead of preserved original date; holding days calculation incorrect

### Order_status of restored positions:
- Restored positions have no `order_status` field set
- Trade_ledger records inserted at open have `order_status` = "submitted" or "filled" (from Alpaca response)
- Reconcile-added positions are never written to trade_ledger at all

### ATR value for restored positions:
- **ATR = 0** in all restore paths
- No code path computes or updates ATR for a restored position after restart
- ATR stop is explicitly skipped when `atr == 0` (line 1854)
- Position remains without ATR protection until bot naturally closes or a new signal is generated for same ticker

---

# SECTION 2D — POSITION SIZING MATHEMATICS

### FORMULA 1: Kelly Fraction (base)
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:kelly_size`
Variables:
- `win_rate` — passed by caller; default 0.55 in `size_position()`
- `win_loss_ratio` (avg_win_loss) — passed by caller; default 1.5 in `size_position()`
- `self.kelly_fraction` — from `config["risk"]["kelly_fraction"]`; expected value 0.5

Calculation:
```
b = max(win_loss_ratio, 1e-6)
p = clip(win_rate, 0, 1)
q = 1 - p
kelly = (b * p - q) / b
return max(0.0, kelly * self.kelly_fraction)
```
With defaults: `b=1.5, p=0.55, q=0.45; kelly=(1.5*0.55-0.45)/1.5 = (0.825-0.45)/1.5 = 0.375/1.5 = 0.25; result = 0.25 * 0.5 = 0.125` (12.5% → then further capped)

Output: fraction of portfolio (uncapped); feeds into `size_position`
Caps/floors: none in this method; capped downstream

---

### FORMULA 2: Signal Strength Scaling
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:size_position`
Variables:
- `signal_strength` — `abs(signal["surprise_zscore"])` from paper_trader
- `strength` — derived

Calculation:
```
strength = min(abs(signal_strength), 3.0) / 3.0   # normalized [0, 1]
size = kelly_size(win_rate, avg_win_loss) * strength
```
Output: raw size fraction; feeds into hard cap and regime multiplier

---

### FORMULA 3: Hard Cap (RiskManager)
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:size_position`
```
size = min(size, self.max_pos_pct)   # max_pos_pct = 0.05 from config
```
Caps: 5% max per position

---

### FORMULA 4: Macro Regime Multiplier (RiskManager)
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:size_position`
Variables:
- `self._macro_regime` — integer 0–4, set by `set_macro_regime()` called from `_capture_context`
- `direction` — int >= 0 for long, < 0 for short

Calculation:
```
mults = _REGIME_MULTIPLIERS[self._macro_regime]
regime_mult = mults["long_multiplier"] if direction >= 0 else mults["short_multiplier"]
size = size * regime_mult
```
Values: RISK_ON long=1.2, GOLDILOCKS=1.0, STAGFLATION=1.0, RISK_OFF long=0.7, RECESSION short=0.5

---

### FORMULA 5: Sector Concentration Cap
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:size_position`
```
sector = portfolio["sectors"][ticker]
current_sector = portfolio["sector_exposures"][sector]
size = min(size, max(0.0, self.max_sector_pct - current_sector))
```
Caps: 0% to `max_sector_pct - current_sector` (25% - existing exposure)

---

### FORMULA 6: UMCI Complexity Multiplier
Source file: `/home/dannyelticala/quant-fund/risk/manager.py:size_position`
Variables:
- `umci` — last UMCI score from FrontierStore

Calculation:
```
if umci < 30:  umci_multiplier = 1.0
elif umci < 60: umci_multiplier = 0.85
elif umci < 80: umci_multiplier = 0.65
elif umci < 95: umci_multiplier = 0.30
else:           umci_multiplier = 0.10
size = size * umci_multiplier   # applied LAST in size_position
```

---

### FORMULA 7: Observation Mode Scaling
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
```
if self.observation_mode:
    size_pct = size_pct * self.obs_size_fraction   # obs_size_fraction = 0.25
```
Applied after RiskManager returns size_pct.

---

### FORMULA 8: Sector Rotation Modifier
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
Variables:
- `sr_mod` — from `SectorRotationSignal.get_modifier(ticker, sector)`, range [-0.15, +0.15]

Calculation:
```
size_pct = size_pct * (1.0 + sr_mod)
```

---

### FORMULA 9: Calendar Effects Modifier
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
Variables:
- `cal_mod` — from `CalendarEffectsSignal.get_composite_modifier()["total_modifier"]`, range [-0.40, +0.30]

Calculation:
```
size_pct = size_pct * (1.0 + cal_mod)
```
Cached per scan day (computed once, applied to all trades in same scan).

---

### FORMULA 10: Tier Multiplier
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
Variables:
- `cap` — market cap from `fetcher.fetch_ticker_info(ticker)["marketCap"]`
- `tier` — from `classify_tier(cap)` (defined in `data/universe.py`)
- `tier_mult` — from `get_tier_size_multiplier(tier)` (defined in `data/universe.py`)

Calculation:
```
size_pct = size_pct * tier_mult
```

---

### FORMULA 11: Absolute Cap (enforced last before shares calculation)
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
```
size_pct = min(size_pct, 0.030)   # 3.0% absolute maximum per trade
```
Applied after ALL multipliers.

---

### FORMULA 12: Shares Calculation
Source file: `/home/dannyelticala/quant-fund/execution/paper_trader.py:_process`
```
equity = getattr(self, '_cached_equity', None) or self.broker.get_account_value()
value = equity * size_pct
shares = value / price
```
Note: `shares` is fractional. `AlpacaPaperBroker.place_order` truncates to `int(max(1, qty))`.

---

### FORMULA 13: Kelly Fraction (PHASE_FREE autonomous)
Source file: `/home/dannyelticala/quant-fund/execution/adaptive_position_sizer.py:size_position_autonomous`
Variables:
- `win_rate` — from `store.get_signal_performance_by_type()[signal_type]["win_rate"]`
- `avg_win` — `store.get_signal_performance_by_type()[signal_type]["avg_win_pct"]`
- `avg_loss` — `abs(store.get_signal_performance_by_type()[signal_type]["avg_loss_pct"])`
- `confluence_mult` — from `calculate_confluence_multiplier(...)` [0.3, 3.0]
- `regime_mult` — GOLDILOCKS:1.3, RISK_ON:1.0, RISK_OFF:0.6, STAGFLATION:0.5, RECESSION_RISK:0.3, CRISIS:0.1
- `decay_mult` — from SignalDecayMonitor: 1.0 / 0.5 / 0.1

Calculation:
```
b = avg_win / max(avg_loss, 0.001)
kelly = (p * b - q) / max(b, 0.001)
fractional_kelly = max(0.001, min(0.030, kelly * 0.25))   # 25% Kelly with hard limits
raw_pct = fractional_kelly * confluence_mult * regime_mult * decay_mult
final_pct = max(0.001, min(0.030, raw_pct))
position_value = round(account_equity * final_pct, 2)
```
Note: this formula is NEVER reached in live trading because AdaptivePositionSizer is not called from `_process()`.

Output: position_pct and position_value (unused in current trading path)
Caps/floors: never_below_pct=0.001 (0.1%), never_exceed_pct=0.030 (3.0%)

---

### ORDER OF APPLICATION (in live trading path):

1. RiskManager.kelly_size: `max(0.0, ((b*p-q)/b) * kelly_fraction)`
2. Signal strength scaling: `kelly_result * (min(abs(z_score), 3.0) / 3.0)`
3. Hard cap: `min(result, max_pos_pct=0.05)`
4. Regime multiplier (RiskManager): `size * regime_mult`
5. Sector cap: `min(size, max_sector_pct - current_exposure)`
6. Liquidity scorer (optional): may reduce size
7. UMCI multiplier: `size * umci_mult`
8. Observation mode: `size * 0.25` (if enabled)
9. Sector rotation modifier: `size * (1.0 + sr_mod)`
10. Calendar effects modifier: `size * (1.0 + cal_mod)`
11. Tier multiplier: `size * tier_mult`
12. Absolute cap: `min(size, 0.030)`
13. Shares = `(equity * size) / price`
14. Truncation: `int(max(1, shares))` (by AlpacaPaperBroker)

---

# SECTION 2E — ALPACA BROKER INTERFACE

### `AlpacaPaperBroker.place_order()` signature:
```python
def place_order(self, ticker: str, qty: float, side: str,
                order_type: str = 'market',
                limit_price: Optional[float] = None,
                direction: str = '',
                fill_price: float = 0.0) -> dict:
```

### Parameters sent to Alpaca:
```python
body = {
    'symbol': ticker,
    'qty': str(int(max(1, qty))),       # truncated to integer, minimum 1
    'side': side,                        # 'buy' or 'sell'
    'type': order_type,                  # 'market' or 'limit'
    'time_in_force': 'day',              # hardcoded DAY order
}
# If order_type == 'limit' and limit_price is not None:
body['limit_price'] = str(limit_price)
```
Endpoint: POST `{base_url}/v2/orders`
Headers: `APCA-API-KEY-ID`, `APCA-API-SECRET-KEY`
Timeout: 10 seconds

### What Alpaca returns:
On success (200/201):
```json
{
  "id": "order_uuid",
  "status": "pending_new" or "new" or "accepted",
  ...
}
```
Mapped to: `{'status': 'submitted', 'order_id': data['id'], 'ticker': ticker, 'qty': qty, 'side': side}`

On failure (any non-200/201):
Returns `{'status': 'error', 'reason': r.text[:200]}`

On exception:
Returns `{'status': 'error', 'reason': str(exc)}`

### How return value is used (in paper_trader.py):
```python
_order_status = result.get("status", "error")
_is_filled = _order_status == "filled"
_is_submitted = _order_status == "submitted"
if _is_filled or _is_submitted:
    # → proceed with position recording
```
For Alpaca paper: `_order_status == "submitted"` is always True (Alpaca returns "pending_new" which is mapped to "submitted"). `_is_filled` is never True for Alpaca paper orders.

### Error handling for each failure mode:

**`not_connected`** — `is_connected()` returns False at startup → `self._connected = False` → all methods return error/empty immediately; prints "SIMULATION MODE"

**HTTP error from Alpaca (non-200/201)** — returns `{'status': 'error', 'reason': r.text[:200]}`; paper_trader checks `if result.get('status') == 'error'` at entry (line 1387) and returns None (position NOT opened)

**Network timeout / exception** — caught by `except Exception as exc`; returns `{'status': 'error', 'reason': str(exc)}`; same handling as HTTP error

**Close order rejection** — `_close_position` checks `close_result.get('status') == 'error'`; logs error and returns WITHOUT deleting from self.active and WITHOUT DB write (position remains open in-memory)

**Order placement routing error** — line 1384–1386: if the type-check routing itself throws, returns `{'status': 'error'}` which causes _process to return None

**`direction` mapping** — `side_map = {'buy':'buy', 'sell':'sell', 'short':'sell', 'cover':'buy'}` — maps PaperBroker direction strings to Alpaca side

---

### WebSocket stream (alpaca_stream.py):

**Events subscribed:**
- `{"action": "subscribe", "bars": ["*"]}` — subscribes to ALL minute bars for all IEX symbols

**Events that trigger actions:**
- Message with `T == "b"` (bar) → `_on_bar(item)` handler
- Alert level "SPIKE" (>2% in 5 min) → `logger.info` only; no Telegram
- Alert level "URGENT" (>5% in 10 min) → `logger.warning` + `_send_urgent_alert()` + Telegram via Notifier
- `WSMsgType.CLOSE` or `WSMsgType.ERROR` → break inner loop, trigger reconnect
- Auth failure (no "authenticated" in response) → return from `_session()`, trigger reconnect

**PriceCache query points in paper_trader.py:**
- `_get_live_price()` — `get_stream_cache().get_fresh_price(ticker)` (5-min stale threshold)
- `_process()` real-time boost — `get_stream_cache().get_move_pct(ticker, window_minutes=5)`

---

# SECTION 2 GATE

**Files read:**
1. /home/dannyelticala/quant-fund/execution/paper_trader.py (2422 lines, fully read)
2. /home/dannyelticala/quant-fund/execution/broker_interface.py (433 lines, fully read)
3. /home/dannyelticala/quant-fund/execution/alpaca_stream.py (512 lines, fully read)
4. /home/dannyelticala/quant-fund/execution/adaptive_position_sizer.py (506 lines, fully read)
5. /home/dannyelticala/quant-fund/risk/manager.py (214 lines, fully read)

**Key findings:**

1. AdaptivePositionSizer is created twice in PaperTrader.__init__ (self.sizer and self.adaptive_sizer) but is never called in the live trade path. RiskManager handles all sizing.

2. Alpaca stream is never started from within GROUP 2 files. start_stream() must be called externally. No error if stream is absent — cache silently returns None.

3. All restored positions have ATR=0, target_price=0, exit_date=None, context_at_open={}, market="us" (hardcoded), sector="UNKNOWN". No code path repairs these after restart.

4. Scale-out (partial exit) has no DB write. Only full closes trigger record_trade().

5. signals_log INSERT fires only on `_is_filled` status, which never occurs for Alpaca paper orders (all return "submitted"). The learning loop signals_log is empty in Alpaca paper mode.

6. AlpacaPaperBroker truncates shares to integer (min 1). This creates a mismatch between self.active["shares"] (fractional) and actual Alpaca position (integer).

7. _log_performance() calls broker.get_pnl() which does not exist on AlpacaPaperBroker. EOD performance logging fails silently.

8. Drawdown halt never fires for AlpacaPaperBroker because initial_capital attribute is absent, making drawdown always 0.

9. Scale-out order placement call does not route through AlpacaBroker type check; uses wrong argument order for PaperBroker.

10. UK tickers are hardcoded as "market: us" in all restore paths.

11. Intraday exit loop lambdas all pass "us" — UK positions are not evaluated during UK market hours via the scheduler.

12. RiskManager.atr_stop(), correlation_ok(), and portfolio_stats() are all unused in the live trade path.

13. Phase transitions are never written to the phase_history table despite it being created.

14. target_delta=0 on restored positions causes scale-out to fire immediately on any positive tick (target_pct = 0/entry = 0 → pnl_pct >= 0 * 0 = always true at level 0.50 × 0).

**Contradictions found:**

1. ATR stop multiplier: paper_trader.py uses 1.5x (longs) and 1.0x (shorts) hardcoded. RiskManager.atr_mult = 2.0 (from config). Both are defined but only paper_trader's is used.

2. Kelly fraction: RiskManager uses 50% Kelly (kelly_fraction from config). AdaptivePositionSizer uses 25% Kelly (hardcoded * 0.25). Two different fractions for the same concept.

3. Correlation limit: RiskManager.corr_limit = 0.75 (from config). PaperTrader._passes_correlation_check uses 0.6 (_CORRELATION_HIGH_THRESHOLD hardcoded). Different thresholds; RiskManager's is unused.

4. BrokerInterface.get_positions() abstract return type is `Dict[str, float]` but AlpacaPaperBroker returns `list`.

5. scale_out_done levels (0.50, 1.00) checked against `pnl_pct >= level * target_pct` — when target_pct = 0 (restored positions), the condition `pnl_pct >= 0` is true for all levels immediately on any non-negative price, causing phantom scale-outs.

6. Alpaca paper orders return status "submitted" but the trade is only logged to signals_log on "filled". The check `_is_filled or _is_submitted` allows position recording but `_is_filled` path for signals_log is never reached.

**Data flows documented:**
- Scan trigger → universe fetch → signal evaluation → confluence gathering → confidence boost → correlation check → portfolio sizing → broker order → Alpaca API → position recording → DB write → jsonl append
- Exit trigger → price fetch → Alpaca position check → close order → Alpaca API → P&L calculation → phantom detection → cooling-off → DB close write → autopsy → jsonl append
- Startup → Alpaca GET /v2/positions → self.active population (missing fields)
- WebSocket bar → PriceCache update → spike/urgent detection → Telegram (URGENT only)

**Proceed to Section 3: YES**
