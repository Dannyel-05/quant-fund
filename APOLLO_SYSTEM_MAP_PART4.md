# APOLLO SYSTEM MAP — PART 4
# GROUP 4: Signal Generators (All 8 Files)
# Generated: 2026-04-08

---

## FILE 1: /home/dannyelticala/quant-fund/signals/momentum_signal.py

### A) PURPOSE
Computes price momentum across three lookback windows (1-month, 3-month, 6-month) and produces a composite weighted momentum score for a single ticker. Generates a LONG or SHORT signal when the composite score exceeds a minimum threshold. This is a pure price-based, stateless signal with no DB interaction.

---

### B) CLASSES AND METHODS

#### Class: `MomentumSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict (used for storage only; no config keys are actually read by any method in this file)
- Output: None
- Action: Stores config as `self.config`. No further initialization.
- DB reads/writes: None

**`generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]`**
- Input:
  - `ticker`: string ticker symbol
  - `price_data`: pandas DataFrame of OHLCV data; must have at least 60 rows; expects a `close` column or uses column index 3 as fallback
- Output: `List[Dict]` — a list of 0 or 1 signal dicts. Each dict has keys: `ticker`, `direction`, `score`, `r1m`, `r3m`, `r6m`, `signal_type`
- Action:
  1. Guards for None data or fewer than 60 rows — returns empty list
  2. Resolves the close price series
  3. Computes 1-month return (r1m) using index -1 vs -21 if at least 21 rows; else 0
  4. Computes 3-month return (r3m) using index -1 vs -63 if at least 63 rows; else 0
  5. Computes 6-month return (r6m) using index -1 vs -126 if at least 126 rows; else 0
  6. Computes composite score: `r1m * 0.5 + r3m * 0.3 + r6m * 0.2`
  7. If `abs(score) > 0.05`: determines direction (LONG or SHORT), caps final score at 1.0 using `min(abs(score) * 3, 1.0)`, appends signal dict
  8. All exceptions caught by broad `except`; logged at DEBUG level; returns empty list on error
- DB reads/writes: None

---

### C) MATHEMATICS

**FORMULA [1-month return]:**
- Variables:
  - `close.iloc[-1]`: most recent close price (from price_data DataFrame)
  - `close.iloc[-21]`: close price 21 rows back (approximately 1 trading month)
- Calculation: `r1m = float(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else 0`
- Output: Fractional return over the past ~1 month; used as component of composite score

**FORMULA [3-month return]:**
- Variables:
  - `close.iloc[-1]`: most recent close
  - `close.iloc[-63]`: close price 63 rows back (approximately 3 trading months)
- Calculation: `r3m = float(close.iloc[-1] / close.iloc[-63] - 1) if len(close) >= 63 else 0`
- Output: Fractional return over ~3 months; used as component of composite score

**FORMULA [6-month return]:**
- Variables:
  - `close.iloc[-1]`: most recent close
  - `close.iloc[-126]`: close price 126 rows back (approximately 6 trading months)
- Calculation: `r6m = float(close.iloc[-1] / close.iloc[-126] - 1) if len(close) >= 126 else 0`
- Output: Fractional return over ~6 months; used as component of composite score

**FORMULA [composite momentum score]:**
- Variables:
  - `r1m`: 1-month return (computed above)
  - `r3m`: 3-month return (computed above)
  - `r6m`: 6-month return (computed above)
- Calculation: `score = r1m * 0.5 + r3m * 0.3 + r6m * 0.2`
- Output: Weighted momentum composite; positive = bullish, negative = bearish; threshold gate at abs > 0.05

**FORMULA [signal score normalization]:**
- Variables:
  - `score`: composite momentum score (computed above)
- Calculation: `min(abs(score) * 3, 1.0)`
- Output: Final signal `score` field in returned dict; clamped to [0.0, 1.0]

---

### D) DATA FLOWS
- Input: `price_data` DataFrame (caller-supplied; not fetched internally)
- Output: List of signal dicts returned to caller
- No DB reads; no DB writes; no file I/O; no network calls

---

### E) DEPENDENCIES
- Internal Apollo imports: None
- External libraries: `numpy` (np), `pandas` (pd), `logging`, `typing` (List, Dict)

---

### F) WIRING STATUS
The class is instantiated and `generate()` is called from `paper_trader.py`. The signal dicts produced flow into the paper trader's signal aggregation and trade decision logic. Wiring is standard and consistent with other signal classes.

---

### G) ISSUES FOUND
1. **Minimum data guard mismatch**: The guard requires `len(price_data) < 60` to skip, but r6m requires 126 rows. With 60–125 rows present, r6m silently falls back to 0. The composite is then computed without the 6-month component but without any warning. The caller does not know r6m is missing.
2. **r3m silently zero with 60–62 rows**: Same issue — 60 rows satisfies the guard but r3m also falls back to 0.
3. **Config is stored but never read**: `self.config` is set in `__init__` but no method reads any config key. Thresholds (0.05, 0.5, 3.0) and weights (0.5, 0.3, 0.2) are all hardcoded.
4. **Broad exception swallows errors silently**: Any runtime error (including e.g. data type errors) is logged at DEBUG only and returns an empty list with no indication to the caller that an error occurred.
5. **Column fallback is fragile**: If `close` column is absent, code uses `price_data.iloc[:, 3]`. If the DataFrame has fewer than 4 columns, this raises an IndexError that is then silently swallowed.

---
---

## FILE 2: /home/dannyelticala/quant-fund/signals/mean_reversion_signal.py

### A) PURPOSE
Generates mean-reversion signals using a 20-period Bollinger Band z-score combined with a 14-period RSI filter. Signals LONG when price is below the lower band and RSI is oversold; signals SHORT when price is above the upper band and RSI is overbought.

---

### B) CLASSES AND METHODS

#### Class: `MeanReversionSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict
- Output: None
- Action: Stores config. No config keys are read anywhere in the file.
- DB reads/writes: None

**`generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]`**
- Input:
  - `ticker`: string ticker symbol
  - `price_data`: pandas DataFrame; must have at least 30 rows; expects `close` column or uses column index 3
- Output: `List[Dict]` — 0 or 1 signal dicts per call. Each dict has keys: `ticker`, `direction`, `score`, `zscore`, `rsi`, `signal_type`
- Action:
  1. Guards for None or fewer than 30 rows — returns empty list
  2. Resolves close series
  3. Computes 20-period rolling mean and std
  4. Computes z-score: `(current - mean) / std` (std floored at 1 to avoid divide-by-zero)
  5. Computes RSI using 14-period rolling average of gains and losses
  6. If z-score < -2.0 AND RSI < 35: appends LONG signal
  7. If z-score > 2.0 AND RSI > 65: appends SHORT signal
  8. Signal score: `min(abs(zscore) / 3, 1.0)`
  9. Exceptions caught at DEBUG level; empty list returned
- DB reads/writes: None

---

### C) MATHEMATICS

**FORMULA [20-period Bollinger Band rolling mean]:**
- Variables:
  - `close`: pandas Series of close prices
- Calculation: `rolling_mean = close.rolling(20).mean()`
- Output: 20-bar rolling mean; used as band center

**FORMULA [20-period Bollinger Band rolling std]:**
- Variables:
  - `close`: pandas Series of close prices
- Calculation: `rolling_std = close.rolling(20).std()`
- Output: 20-bar rolling standard deviation; used as band width

**FORMULA [z-score]:**
- Variables:
  - `current`: `float(close.iloc[-1])` — most recent close
  - `mean`: `float(rolling_mean.iloc[-1])` — current rolling mean
  - `std`: `float(rolling_std.iloc[-1]) if float(rolling_std.iloc[-1]) > 0 else 1` — current rolling std, floored at 1
- Calculation: `zscore = (current - mean) / std`
- Output: Number of standard deviations current price is from 20-period mean; gates at abs > 2.0

**FORMULA [RSI delta]:**
- Variables:
  - `close`: pandas Series of close prices
- Calculation: `delta = close.diff()`
- Output: Daily price changes; split into gains and losses

**FORMULA [RSI gain / loss averages]:**
- Variables:
  - `delta`: daily price changes (computed above)
- Calculation:
  - `gain = delta.clip(lower=0).rolling(14).mean()`
  - `loss = (-delta.clip(upper=0)).rolling(14).mean()`
- Output: 14-period rolling average gain and loss

**FORMULA [RSI relative strength]:**
- Variables:
  - `gain`: 14-period rolling average gain
  - `loss`: 14-period rolling average loss (with 0 replaced by 1e-10 to avoid divide-by-zero)
- Calculation: `rs = gain / loss.replace(0, 1e-10)`
- Output: Ratio of avg gain to avg loss

**FORMULA [RSI]:**
- Variables:
  - `rs`: relative strength ratio (computed above)
- Calculation: `rsi = float(100 - 100 / (1 + rs.iloc[-1]))`
- Output: RSI value 0–100; gates: < 35 for LONG, > 65 for SHORT

**FORMULA [signal score]:**
- Variables:
  - `zscore`: z-score of current price vs 20-period mean
- Calculation: `min(abs(zscore) / 3, 1.0)`
- Output: Signal strength, clamped to [0.0, 1.0]

---

### D) DATA FLOWS
- Input: `price_data` DataFrame (caller-supplied)
- Output: List of signal dicts returned to caller
- No DB reads; no DB writes; no file I/O; no network calls

---

### E) DEPENDENCIES
- Internal Apollo imports: None
- External libraries: `numpy` (np), `pandas` (pd), `logging`, `typing` (List, Dict)

---

### F) WIRING STATUS
Instantiated and called from `paper_trader.py`. Signal dicts returned to caller for aggregation. Wiring is standard.

---

### G) ISSUES FOUND
1. **RSI uses SMA, not Wilder's smoothing**: Standard RSI uses Wilder's exponential moving average (EMA with alpha=1/14). This implementation uses a simple rolling mean. The resulting RSI values are numerically different from canonical RSI. No comment or documentation acknowledges this deviation.
2. **std floor of 1 is too large**: When `rolling_std.iloc[-1]` is 0, it is replaced with 1. For a stock trading at e.g. $10, this makes a 1-standard-deviation move equal to $1 (10%), making the z-score essentially meaningless. A more appropriate floor would be a fraction of current price.
3. **Config stored but never read**: Same as MomentumSignal — thresholds (2.0, 35, 65), RSI period (14), Bollinger period (20), score divisor (3) are all hardcoded.
4. **Minimum data guard of 30 rows is insufficient for RSI**: RSI with period 14 needs at least 15 rows for the first non-NaN value, but using `rs.iloc[-1]` on a rolling window that has NaN at position -1 would produce NaN RSI. With only 30 rows, the rolling mean and std are computed from the last 20 of those rows but the RSI's `loss.replace(0, 1e-10)` call uses the entire series. The `rsi` value could be NaN and the comparisons `rsi < 35` and `rsi > 65` on NaN evaluate to False silently — no signal generated, no warning.
5. **Broad exception swallows errors silently**.

---
---

## FILE 3: /home/dannyelticala/quant-fund/signals/gap_signal.py

### A) PURPOSE
Detects significant overnight price gaps (open vs prior close) and generates regime-conditioned trading signals. In NEUTRAL/BEAR regimes it fades the gap (opposing direction). In BULL regime it rides up-gaps (continuation) and fades down-gaps. In CRISIS regime all gap signals are suppressed. Additional filters: gap size must be 2–8%, high-volume gaps are skipped, signals are only generated within the opening 30-minute window, and gaps aligned with the sector ETF direction are skipped.

---

### B) CLASSES AND METHODS

#### Class: `GapSignal`

**`__init__(self, config: dict) -> None`**
- Input: `config` dict
- Output: None
- Action: Stores config, initializes `_regime_detector` to None, `_regime_cache` to None (note: `_regime_cache` is set but never used anywhere in the file), and `_sector_gap_cache` as an empty dict
- DB reads/writes: None

**`_get_regime(self) -> str`**
- Input: None (uses self)
- Output: String regime name: "NEUTRAL", "BULL", "BEAR", or "CRISIS"
- Action: Lazily imports and instantiates `RegimeDetector` from `analysis.regime_detector`. Calls `self._regime_detector.detect()`. Returns "NEUTRAL" on any import or runtime error.
- DB reads/writes: None directly; `RegimeDetector.detect()` may read DB

**`update_sector_gap(self, sector_etf: str, gap_direction: int) -> None`**
- Input:
  - `sector_etf`: string ETF ticker (must be in `_SECTOR_ETFS` set to be stored)
  - `gap_direction`: +1 (up gap) or -1 (down gap)
- Output: None
- Action: Writes `gap_direction` to `self._sector_gap_cache[sector_etf]` if `sector_etf` is in the known set of 15 sector ETFs
- DB reads/writes: None

**`_sector_aligned(self, ticker: str, gap_direction: int) -> bool`**
- Input:
  - `ticker`: string (not actually used in the logic; present as parameter but ignored)
  - `gap_direction`: +1 or -1
- Output: bool — True if any sector ETF in the cache gapped the same direction
- Action: Iterates `_sector_gap_cache`; returns True if any cached ETF direction equals `gap_direction`; returns False if cache is empty or no match
- DB reads/writes: None

**`generate(self, ticker: str, price_data: pd.DataFrame, within_open_window: bool = True, opening_volume: Optional[float] = None) -> List[Dict]`**
- Input:
  - `ticker`: string ticker symbol
  - `price_data`: pandas DataFrame with at least 5 rows; expected columns: `open`, `close`, `volume`
  - `within_open_window`: bool, True if called within first 30 minutes of session (default True)
  - `opening_volume`: float, today's opening volume for volume filter (optional)
- Output: `List[Dict]` — 0 or 1 signal dicts. Keys: `ticker`, `direction`, `score`, `gap_pct`, `signal_type`, `signal_subtype`, `gap_context`
- Action:
  1. Guards for None or fewer than 5 rows
  2. Resolves `open`, `close`, `volume` columns
  3. Computes `gap_pct = (today_open - prev_close) / prev_close`
  4. Gap size filter: skips if `abs(gap_pct) < 0.02` or `abs(gap_pct) > 0.08`
  5. Time filter: skips if `within_open_window` is False
  6. Volume filter: skips if `opening_volume / avg_vol_20d > 1.5`
  7. Regime gate: skips all if regime == "CRISIS"
  8. Sector alignment check: skips if sector cache has ETF gapping same direction
  9. Direction logic: BULL regime → up-gap is LONG continuation, down-gap is LONG fade; NEUTRAL/BEAR → fade always (SHORT for up-gap, LONG for down-gap)
  10. Appends signal dict with gap context sub-dict
- DB reads/writes: None directly; `_get_regime()` may trigger `RegimeDetector.detect()`

---

### C) MATHEMATICS

**FORMULA [gap percentage]:**
- Variables:
  - `today_open`: `float(price_data[open_col].iloc[-1])` — current row open price
  - `prev_close`: `float(price_data[close_col].iloc[-2])` — previous row close price
- Calculation: `gap_pct = (today_open - prev_close) / prev_close`
- Output: Signed fractional gap; positive = up gap, negative = down gap

**FORMULA [20-day average volume]:**
- Variables:
  - `vol_series`: `price_data[vol_col].astype(float)` — volume column as float
- Calculation: `avg_vol_20d = vol_series.iloc[-21:-1].mean()`
- Output: 20-bar average volume used for volume filter denominator

**FORMULA [volume ratio]:**
- Variables:
  - `opening_volume`: caller-supplied today's opening volume
  - `avg_vol_20d`: 20-day average volume (computed above)
- Calculation: `vol_ratio = opening_volume / avg_vol_20d`
- Output: Ratio; if > 1.5 signal is suppressed

**FORMULA [signal score — BULL regime continuation (up-gap)]:**
- Variables:
  - `abs_gap`: `abs(gap_pct)`
- Calculation: `score = min(abs_gap * 8, 0.7)`
- Output: Signal confidence score capped at 0.7

**FORMULA [signal score — fade or BULL down-gap]:**
- Variables:
  - `abs_gap`: `abs(gap_pct)`
- Calculation: `score = min(abs_gap * 10, 1.0)`
- Output: Signal confidence score capped at 1.0

---

### D) DATA FLOWS
- Input: `price_data` DataFrame (caller-supplied OHLCV)
- Input: `opening_volume` float (optional, caller-supplied)
- Input: `within_open_window` bool (caller-supplied)
- Output: List of signal dicts returned to caller
- Indirect read: `RegimeDetector.detect()` — may read DB (implementation in `analysis/regime_detector.py`)
- No direct DB reads or writes in this file

---

### E) DEPENDENCIES
- Internal Apollo imports:
  - `analysis.regime_detector.RegimeDetector` (lazy import inside `_get_regime`)
- External libraries: `pandas` (pd), `logging`, `typing` (Dict, List, Optional)

---

### F) WIRING STATUS
Called from `paper_trader.py`. The `within_open_window` and `opening_volume` parameters must be supplied by the caller at call time. If `paper_trader.py` calls `generate()` with defaults (`within_open_window=True`, `opening_volume=None`), the volume filter is effectively disabled and the time filter passes unconditionally. `update_sector_gap()` must also be called by the orchestrator to populate the sector cache; if not called the cache is always empty and the sector alignment check always returns False (no suppression).

---

### G) ISSUES FOUND
1. **`_regime_cache` is set but never used**: `self._regime_cache` is initialized to None in `__init__` but nothing ever writes to it or reads from it. The regime is re-fetched via `_get_regime()` on every `generate()` call.
2. **`ticker` parameter in `_sector_aligned` is ignored**: The method signature takes `ticker` but the logic never references it. The sector alignment check is the same for every ticker regardless of what sector it belongs to.
3. **Sector alignment logic is too broad**: `_sector_aligned` returns True if ANY sector ETF in the cache is gapping the same direction — not just the sector ETF for the ticker being evaluated. A tech stock would be suppressed if the energy ETF is gapping up.
4. **Default `within_open_window=True`**: The time filter defaults to allowing the signal, meaning if the caller forgets to pass this parameter, gap signals can fire at any time of day. This is backwards from a safety standpoint.
5. **`open_col` fallback is wrong**: If `open` column is absent, code falls back to `price_data.columns[0]`. This may not be the open price column depending on DataFrame structure.
6. **`gap_context["sector_aligned"]` is always hardcoded False**: Even when the sector alignment check passes and the signal is NOT suppressed, the `gap_context` dict always sets `"sector_aligned": False`. The True case is never populated because a True sector alignment exits before the dict is built.
7. **Volume filter uses iloc[-21:-1] which gives 20 items but excludes today**: This is intentional but undocumented. The slice `[-21:-1]` covers the 20 days prior to today's row. If the DataFrame has only 5 rows (the minimum), `iloc[-21:-1]` will attempt a 20-day window from a 5-row frame and return whatever is available without error — average is computed from fewer rows, no warning.

---
---

## FILE 4: /home/dannyelticala/quant-fund/signals/insider_momentum_signal.py

### A) PURPOSE
Generates bullish momentum signals based on the volume of SEC Form 4 insider trading filings for a ticker over the past 60 days. Makes live HTTP requests to the SEC EDGAR full-text search API. Any ticker with 2 or more Form 4 filings in 60 days gets a LONG signal regardless of whether filings represent buys or sells.

---

### B) CLASSES AND METHODS

#### Class: `InsiderMomentumSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict
- Output: None
- Action: Stores config; sets `self.headers` to `{'User-Agent': 'quant-fund research@quantfund.com'}` for SEC API calls
- DB reads/writes: None

**`get_insider_trades(self, ticker: str, days_back: int = 90) -> List[Dict]`**
- Input:
  - `ticker`: string ticker symbol
  - `days_back`: integer number of days to look back (default 90; overridden to 60 in `generate`)
- Output: `List[Dict]` — list of EDGAR search result hits; empty list on error
- Action:
  1. Computes `from_date` as `(datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')`
  2. Makes GET request to `https://efts.sec.gov/LATEST/search-index` with params: `q="{ticker}"`, `forms=4`, `dateRange=custom`, `startdt=from_date`, `enddt=today`
  3. Returns `r.json().get('hits', {}).get('hits', [])` on HTTP 200
  4. Returns empty list on non-200 or exception
- DB reads/writes: None

**`generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]`**
- Input:
  - `ticker`: string ticker symbol
  - `price_data`: pandas DataFrame (accepted but never used in the method body)
- Output: `List[Dict]` — 0 or 1 signal dicts. Keys: `ticker`, `direction`, `score`, `insider_filings_60d`, `signal_type`
- Action:
  1. Calls `self.get_insider_trades(ticker, days_back=60)` to retrieve Form 4 filings
  2. If 2 or more filings found: appends LONG signal with score `min(len(trades) / 10.0, 1.0)`
  3. Direction is always LONG; no SHORT signal is ever generated
  4. Exceptions caught at DEBUG level
- DB reads/writes: None

---

### C) MATHEMATICS

**FORMULA [insider signal score]:**
- Variables:
  - `len(trades)`: count of Form 4 filings returned from SEC EDGAR API in the past 60 days
- Calculation: `min(len(trades) / 10.0, 1.0)`
- Output: Signal score; reaches 1.0 at 10 or more filings; clamped to [0.0, 1.0]

---

### D) DATA FLOWS
- Input: `ticker` string
- Input: `price_data` DataFrame (passed in but never read)
- External HTTP request: GET `https://efts.sec.gov/LATEST/search-index` (live API call during signal generation)
- Output: List of signal dicts returned to caller
- No DB reads or writes

---

### E) DEPENDENCIES
- Internal Apollo imports: None
- External libraries: `requests`, `datetime` (datetime, timedelta), `pandas` (pd), `logging`, `typing` (List, Dict)

---

### F) WIRING STATUS
Called from `paper_trader.py`. Makes a live HTTP call to the SEC EDGAR API on every invocation. If the API is unavailable or returns non-200, an empty list is returned silently. No caching of API results.

---

### G) ISSUES FOUND
1. **No distinction between buy and sell filings**: Form 4 covers both insider purchases AND sales. The signal is LONG for any Form 4 activity including insider selling. Insider cluster selling is a bearish signal but this code treats it as bullish. There is no parsing of transaction type codes (P=purchase, S=sale) in the returned hits.
2. **No SHORT signal**: The generate method can only produce LONG signals. Insider selling patterns are entirely ignored.
3. **`price_data` parameter is unused**: Accepted for API consistency with other signals but never read.
4. **Hardcoded email in User-Agent header**: `'User-Agent': 'quant-fund research@quantfund.com'` — this email address is hardcoded and sent on every request to the SEC.
5. **Wrong SEC EDGAR endpoint**: The URL `https://efts.sec.gov/LATEST/search-index` is not a documented public EDGAR endpoint. The standard EDGAR full-text search endpoint is `https://efts.sec.gov/LATEST/search-index` — however, the response structure `.get('hits', {}).get('hits', [])` suggests it is targeting the Elasticsearch-style EDGAR EFTS API. If the API schema changes, the nested `.get('hits', {}).get('hits', [])` will silently return an empty list.
6. **Live HTTP call in signal hot path**: A 15-second timeout per call means every ticker processed can block signal generation for up to 15 seconds. With a universe of many tickers, this creates a serious latency risk.
7. **`days_back` default of 90 in `get_insider_trades` is overridden to 60 in `generate`**: The 90-day default in `get_insider_trades` is never used by `generate()`. The public default parameter is misleading.

---
---

## FILE 5: /home/dannyelticala/quant-fund/signals/sector_rotation_signal.py

### A) PURPOSE
Tracks the relative strength of 11 SPDR sector ETFs versus SPY across three timeframes (4-week, 12-week, 26-week) and detects rotation between sectors. Provides PEAD signal size modifiers (+/-0.15) based on whether a ticker's sector is in the top, bottom, or rotating cohort. Persists rotation state to `historical_db.db`. Does not directly generate trade signals — acts as a modifier/enrichment layer for PEAD and other signals.

---

### B) CLASSES AND METHODS

**Module-level functions (not class methods):**

**`fetch_returns(period_days: int) -> Dict[str, float]`**
- Input: `period_days` — integer number of trading days to compute return over
- Output: `Dict[str, float]` — maps ticker → total return fraction; empty dict on error
- Action: Downloads all 11 sector ETFs + SPY via `yf.download()` with `auto_adjust=True`. Trims to `period_days + 1` rows. For each ticker computes `series.iloc[-1] / series.iloc[0] - 1`.
- DB reads/writes: None

**`compute_relative_strength() -> Dict[str, float]`**
- Input: None
- Output: `Dict[str, float]` — maps sector_name → rs_composite score; empty dict on error
- Action: Calls `fetch_returns` three times (20, 60, 130 days). Computes excess return vs SPY for each window. Combines: `0.5 * rs_4w + 0.3 * rs_12w + 0.2 * rs_26w`. Returns sector_name keyed dict.
- DB reads/writes: None directly

**`get_ranking(scores: Optional[Dict[str, float]] = None) -> Dict`**
- Input: `scores` — optional pre-computed sector scores (calls `compute_relative_strength()` if None)
- Output: `Dict` with keys: `top_sectors` (list of top 3), `bottom_sectors` (list of bottom 3), `scores` (full dict)
- Action: Sorts sectors by composite rs score descending; slices top 3 and bottom 3
- DB reads/writes: None

**`detect_rotation(current_scores: Optional[Dict[str, float]] = None) -> Dict`**
- Input: `current_scores` — optional pre-computed current scores
- Output: `Dict` with keys: `rotating_in` (list), `rotating_out` (list)
- Action:
  1. Computes current top5 and bottom5 from current_scores
  2. Fetches `fetch_returns(40)` and `fetch_returns(20)` to construct an "old score" proxy
  3. Old score per sector: `r40 - r20` (40-day return minus 20-day return)
  4. Computes old top5 and old bot5
  5. `rotating_in = list(current_top5 - old_top5 & old_bot5)`
  6. `rotating_out = list(current_bot5 - old_bot5 & old_top5)`
- DB reads/writes: None

**`get_pead_modifier(sector, top_sectors, bottom_sectors, rotating_in, rotating_out) -> float`**
- Input: sector name string; four lists
- Output: float in `{-0.15, 0.0, +0.15}`
- Action: Returns +0.15 if sector in top_sectors or rotating_in; -0.15 if in bottom_sectors or rotating_out; 0.0 otherwise
- DB reads/writes: None

**`get_sector_for_ticker(ticker: str) -> str`**
- Input: `ticker` string
- Output: sector name string (lowercase, underscored)
- Action: Checks `TICKER_TO_SECTOR` hardcoded dict first. Falls back to `yf.Ticker(ticker).info.get("sector", "unknown")` with normalization. Returns "unknown" on any error.
- DB reads/writes: None

**`_get_db_path() -> str`**
- Input: None
- Output: String path to `output/historical_db.db`
- Action: Resolves path relative to file location

**`_ensure_table(conn: sqlite3.Connection) -> None`**
- Input: sqlite3 connection
- Output: None
- Action: Creates `sector_rotation` table if not exists
- DB reads/writes: Writes DDL to `historical_db.db`

**`_store_rotation(state: Dict) -> None`**
- Input: `state` dict with top_sectors, bottom_sectors, rotating_in, rotating_out, scores
- Output: None
- Action: Connects to `historical_db.db`, ensures table exists, inserts one row with JSON-encoded sector lists and scores
- DB reads/writes: WRITES to `historical_db.db` → `sector_rotation` table (columns: id, date, top_sectors, bottom_sectors, rotating_in, rotating_out, sector_scores, calculated_at)

#### Class: `SectorRotationSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict
- Output: None
- Action: Stores config; initializes `_state` to None
- DB reads/writes: None

**`run(self) -> Dict`**
- Input: None (uses self)
- Output: `Dict` with keys: `top_sectors`, `bottom_sectors`, `scores`, `rotating_in`, `rotating_out`, `calculated_at`
- Action: Calls `compute_relative_strength()`, `get_ranking()`, `detect_rotation()`. Assembles state dict. Sets `self._state`. Calls `_store_rotation(state)`. Returns state.
- DB reads/writes: WRITES to `historical_db.db` → `sector_rotation` table (via `_store_rotation`)

**`get_modifier(self, ticker: str, sector: str = None) -> float`**
- Input:
  - `ticker`: string
  - `sector`: optional string; if None, calls `get_sector_for_ticker(ticker)` (which may call yfinance)
- Output: float in `{-0.15, 0.0, +0.15}`
- Action: Calls `self.run()` if `_state` is None. Normalizes sector string. Calls `get_pead_modifier()` with state lists. Returns modifier.
- DB reads/writes: Indirect WRITE via `run()` → `_store_rotation()` on first call

---

### C) MATHEMATICS

**FORMULA [total return over period]:**
- Variables:
  - `series.iloc[-1]`: last close price in trimmed window
  - `series.iloc[0]`: first close price in trimmed window
- Calculation: `ret = float(series.iloc[-1]) / float(series.iloc[0]) - 1`
- Output: Fractional total return; computed per ETF per time window

**FORMULA [sector excess return per window]:**
- Variables:
  - `ret_Xw.get(etf, 0.0)`: ETF return for window X (4w, 12w, 26w)
  - `spy_Xw`: SPY return for same window
- Calculation:
  - `rs_4w = ret_4w.get(etf, 0.0) - spy_4w`
  - `rs_12w = ret_12w.get(etf, 0.0) - spy_12w`
  - `rs_26w = ret_26w.get(etf, 0.0) - spy_26w`
- Output: Excess return vs benchmark per window

**FORMULA [composite relative strength]:**
- Variables:
  - `rs_4w`: 4-week excess return (computed above)
  - `rs_12w`: 12-week excess return (computed above)
  - `rs_26w`: 26-week excess return (computed above)
- Calculation: `rs_composite = 0.5 * rs_4w + 0.3 * rs_12w + 0.2 * rs_26w`
- Output: Weighted composite RS score per sector; stored rounded to 6 decimal places

**FORMULA [rotation old-score proxy]:**
- Variables:
  - `r40`: 40-day return for ETF from `fetch_returns(40)`
  - `r20`: 20-day return for ETF from `fetch_returns(20)`
  - `spy_40`: SPY 40-day return
  - `spy_20`: SPY 20-day return
- Calculation: `old_scores[sector] = (r40 - spy_40) - (r20 - spy_20)` — implemented as `r40 - r20` where each is already excess-return vs SPY at time of call (note: the code does NOT subtract SPY from r40/r20 in the old_scores block — only raw ETF returns are differenced)
- Output: Proxy for past sector RS ranking; used to detect movement into/out of top/bottom cohorts

---

### D) DATA FLOWS
- Input: Live market data via `yfinance` (HTTP download, called multiple times per `run()`)
- Internal computation: 4 calls to `fetch_returns()` per full `run()` cycle (20d, 60d, 130d for RS; 40d and 20d again for rotation detection)
- Output: State dict returned to caller; sector modifier float returned via `get_modifier()`
- DB WRITES: `historical_db.db` → `sector_rotation` table (on every `run()` call)
- DB READS: None

---

### E) DEPENDENCIES
- Internal Apollo imports: None (self-contained)
- External libraries: `yfinance` (yf), `numpy` (np), `pandas` (pd), `sqlite3`, `json`, `logging`, `datetime` (datetime, timedelta), `pathlib` (Path), `typing` (Dict, List, Optional, Tuple)

---

### F) WIRING STATUS
`SectorRotationSignal.get_modifier()` is called by `pead_signal.py` (via `apply_context_multipliers`) and potentially by `paper_trader.py` directly to adjust PEAD position sizing. The `run()` method is also callable standalone. DB write path is active. This is a modifier, not a standalone trade signal generator.

---

### G) ISSUES FOUND
1. **`detect_rotation` operator precedence bug**: The expressions `rotating_in = list(current_top5 - old_top5 & old_bot5)` and `rotating_out = list(current_bot5 - old_bot5 & old_top5)` have a Python operator precedence problem. `&` (set intersection) has higher precedence than `-` (set difference). So `current_top5 - old_top5 & old_bot5` evaluates as `current_top5 - (old_top5 & old_bot5)`, not `(current_top5 - old_top5) & old_bot5`. The intended logic — "sectors now in the top that were previously in the bottom" — is silently producing wrong results.
2. **Old-score proxy does not subtract SPY**: In `detect_rotation`, the old_scores calculation does `r40 - r20` where r40 and r20 are raw ETF returns (not excess vs SPY). But the current_scores are excess vs SPY. The ranking comparison mixes two different return measures.
3. **4 separate yfinance download calls per `run()` cycle**: `fetch_returns` is called for 20d, 60d, 130d (in `compute_relative_strength`) and then again for 40d and 20d (in `detect_rotation`). The 20d window is downloaded twice. No caching between calls.
4. **`_TICKER_TO_SECTOR` hardcoded dict is sparse**: Most tickers in a real universe will fall through to the yfinance lookup fallback. The fallback makes an additional network call per ticker and returns unvalidated strings that may not match `SECTOR_ETFS` keys.
5. **`sector_rotation` table has no UNIQUE constraint on `date`**: Multiple rows per date accumulate on repeated `run()` calls. No deduplication or upsert logic.
6. **`get_modifier` calls `run()` on every object instantiation (first call)**: If `get_modifier` is called frequently from `pead_signal.py`, the expensive `run()` (4 network calls + DB write) fires on the first call per instance. If the instance is re-created per ticker, this runs for every ticker.

---
---

## FILE 6: /home/dannyelticala/quant-fund/signals/calendar_effects_signal.py

### A) PURPOSE
Computes seasonal and calendar-based size modifiers for PEAD and other signals. Detects: January effect, tax-loss selling reversal, end-of-quarter window dressing, earnings season timing, Fed meeting proximity, and options expiry proximity. All effects produce additive modifier floats that are summed and clamped. Persists the composite modifier to `permanent_archive.db`. Does not generate standalone trade signals; acts as a sizing overlay.

---

### B) CLASSES AND METHODS

**Module-level helper functions:**

**`is_nth_trading_day_of_month(dt: datetime, n: int) -> bool`**
- Input: `dt` datetime, `n` target business day number
- Output: bool — True if dt is within ±1 business day of the nth business day of the month
- Action: Counts weekdays (Mon–Fri) from the 1st of the month to dt; returns True if count differs from n by at most 1

**`days_to_next_friday(dt: datetime) -> int`**
- Input: `dt` datetime
- Output: int — calendar days to next Friday; returns 7 if dt is already Friday
- Action: Computes `(4 - weekday) % 7`; if result is 0, returns 7

**`is_triple_witching_week(dt: datetime) -> bool`**
- Input: `dt` datetime
- Output: bool — True if dt falls in the Mon–Fri week containing the 3rd Friday of March, June, September, or December
- Action: Only checks months {3, 6, 9, 12}; counts Fridays from month start to find 3rd Friday; checks if dt.date falls in that week

**`get_earnings_season_week(dt: datetime) -> int`**
- Input: `dt` datetime
- Output: int 0–4 — week number within earnings season (0 = between seasons, 1–4 = week within season)
- Action: Checks four earnings season windows (Jan15–Feb15, Apr15–May15, Jul15–Aug15, Oct15–Nov15); returns `min(4, days_in // 7 + 1)` within a season

**`_is_last_week_of_quarter(dt: datetime) -> bool`**
- Input: `dt` datetime
- Output: bool — True if dt is in the final 7 calendar days of a quarter-end month (Mar, Jun, Sep, Dec)
- Action: Finds last day of the month; returns True if `(last_day - target).days <= 6`

**`january_effect(dt: datetime, is_small_cap: bool = True) -> float`**
- Input: `dt`, `is_small_cap` bool
- Output: float — +0.10 or 0.0
- Action: Returns +0.10 if month == 1 AND day <= 15 AND is_small_cap; else 0.0

**`tax_loss_reversal(dt: datetime) -> float`**
- Input: `dt`
- Output: float — +0.08 or 0.0
- Action: Returns +0.08 if month == 1 AND day <= 7; else 0.0

**`window_dressing(dt: datetime, ytd_return_pct: Optional[float] = None) -> float`**
- Input: `dt`, `ytd_return_pct` optional float
- Output: float — +0.05 or 0.0
- Action: Returns +0.05 if in last week of quarter AND ytd_return_pct is not None AND ytd_return_pct > 10; else 0.0

**`earnings_season_timing(dt: datetime) -> float`**
- Input: `dt`
- Output: float — +0.10, -0.05, or -0.10
- Action: Weeks 1–2 of earnings season → +0.10; weeks 3–4 → -0.05; between seasons → -0.10

**`fed_meeting_proximity(dt: datetime, days_to_next_meeting: Optional[int]) -> float`**
- Input: `dt`, `days_to_next_meeting` optional int
- Output: float — -0.30, -0.25, -0.15, or 0.0
- Action: Returns -0.30 if days_to_next_meeting <= 1; -0.25 if <= 7; -0.15 if <= 14; 0.0 otherwise or if None

**`options_expiry_proximity(dt: datetime) -> float`**
- Input: `dt`
- Output: float — -0.20, -0.10, or 0.0
- Action: Triple witching week → -0.20; days_to_next_friday <= 5 AND 14 <= day <= 21 → -0.10; else 0.0

**`_get_archive_db_path() -> str`**
- Input: None
- Output: String path to `output/permanent_archive.db`

**`_ensure_calendar_table(conn: sqlite3.Connection) -> None`**
- Input: sqlite3 connection
- Output: None
- Action: Creates `calendar_signals` table if not exists
- DB reads/writes: DDL write to `permanent_archive.db`

**`_store_calendar_signal(record: Dict) -> None`**
- Input: `record` dict with fields: effective_date, total_modifier, january_effect, tax_loss_reversal, earnings_season, options_expiry, fed_proximity
- Output: None
- Action: Connects to `permanent_archive.db`, ensures table, inserts record
- DB reads/writes: WRITES to `permanent_archive.db` → `calendar_signals` table

#### Class: `CalendarEffectsSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict
- Output: None
- Action: Stores config. No config keys are read.
- DB reads/writes: None

**`get_composite_modifier(self, dt=None, is_small_cap=True, days_to_fed=None, ytd_return_pct=None, store_to_db=True) -> Dict`**
- Input:
  - `dt`: datetime (defaults to `datetime.utcnow()`)
  - `is_small_cap`: bool
  - `days_to_fed`: optional int — calendar days to next FOMC meeting
  - `ytd_return_pct`: optional float — YTD return for window dressing check
  - `store_to_db`: bool — whether to persist to DB (default True)
- Output: Dict with keys: `total_modifier`, `january_effect`, `tax_loss_reversal`, `earnings_season`, `options_expiry`, `fed_proximity`, `window_dressing`, `effective_date`
- Action:
  1. Computes each component modifier (6 functions called)
  2. Sums all components: `raw_total = jan_eff + tax_rev + earn_seas + opts_exp + fed_prox + win_dress`
  3. Clamps: `total = max(-0.40, min(0.30, raw_total))`
  4. Assembles result dict
  5. If `store_to_db=True`, calls `_store_calendar_signal(result)`
- DB reads/writes: WRITES to `permanent_archive.db` → `calendar_signals` (when store_to_db=True)

**`get_pead_timing_modifier(self, dt=None, is_small_cap=True, days_to_fed=None) -> float`**
- Input: same as `get_composite_modifier` minus `ytd_return_pct` and `store_to_db`
- Output: float — just `total_modifier` value
- Action: Calls `get_composite_modifier(store_to_db=False)` and returns `result["total_modifier"]`
- DB reads/writes: None (store_to_db=False)

---

### C) MATHEMATICS

**FORMULA [days to next Friday]:**
- Variables:
  - `weekday`: `dt.weekday()` (Monday=0, Friday=4)
- Calculation: `days_ahead = (4 - weekday) % 7`; if `days_ahead == 0`: `days_ahead = 7`
- Output: int calendar days to next Friday

**FORMULA [earnings season week number]:**
- Variables:
  - `days_in`: `(target - season_start).days`
- Calculation: `week = min(4, days_in // 7 + 1)`
- Output: int 1–4 representing week within earnings season

**FORMULA [IV rank proxy (referenced in options_earnings_signal.py but formula is there)]:**
- Not present in this file

**FORMULA [total calendar modifier sum]:**
- Variables:
  - `jan_eff`: result of `january_effect(dt, is_small_cap)` — +0.10 or 0.0
  - `tax_rev`: result of `tax_loss_reversal(dt)` — +0.08 or 0.0
  - `earn_seas`: result of `earnings_season_timing(dt)` — +0.10, -0.05, or -0.10
  - `opts_exp`: result of `options_expiry_proximity(dt)` — -0.20, -0.10, or 0.0
  - `fed_prox`: result of `fed_meeting_proximity(dt, days_to_fed)` — -0.30, -0.25, -0.15, or 0.0
  - `win_dress`: result of `window_dressing(dt, ytd_return_pct)` — +0.05 or 0.0
- Calculation: `raw_total = jan_eff + tax_rev + earn_seas + opts_exp + fed_prox + win_dress`
- Output: Unclamped sum of all calendar effects

**FORMULA [clamped total modifier]:**
- Variables:
  - `raw_total`: unclamped sum (computed above)
- Calculation: `total = max(-0.40, min(0.30, raw_total))`
- Output: Final total_modifier returned in dict; range [-0.40, +0.30]

---

### D) DATA FLOWS
- Input: `dt`, `is_small_cap`, `days_to_fed`, `ytd_return_pct` — all caller-supplied (no external data fetching)
- Output: Dict with modifier values; float returned from `get_pead_timing_modifier`
- DB WRITES: `permanent_archive.db` → `calendar_signals` table (when `store_to_db=True`)
- DB READS: None

---

### E) DEPENDENCIES
- Internal Apollo imports: None
- External libraries: `sqlite3`, `json`, `logging`, `datetime` (datetime, date, timedelta), `pathlib` (Path), `typing` (Dict, List, Optional, Tuple)

---

### F) WIRING STATUS
`get_pead_timing_modifier()` is designed to be called from `pead_signal.py` or `paper_trader.py` as a PEAD size adjustment. The caller must supply `days_to_fed` (days until next FOMC) — this value is not fetched internally and is None by default, making the Fed proximity effect always 0.0 unless the caller explicitly provides it. The `ytd_return_pct` for window dressing is also caller-supplied. DB write is wired to `permanent_archive.db`.

---

### G) ISSUES FOUND
1. **`days_to_fed` defaults to None throughout the call chain**: `fed_meeting_proximity` returns 0.0 when `days_to_next_meeting` is None. If the caller does not supply this value (which requires an external FOMC calendar), the Fed proximity effect is silently disabled for every call.
2. **`window_dressing` result key missing from `_store_calendar_signal`**: The `get_composite_modifier` result dict includes `"window_dressing"` key, but `_store_calendar_signal` does not store it — the INSERT statement stores only 6 components (january_effect, tax_loss_reversal, earnings_season, options_expiry, fed_proximity) and does not include window_dressing. The `calendar_signals` table also has no `window_dressing` column.
3. **`options_expiry_proximity` standard expiry check is approximate**: The check `(days_to_friday <= 5) and (14 <= target.day <= 21)` attempts to identify the standard monthly options expiry week (3rd Friday). However, `days_to_friday <= 5` starting Monday means the full Mon–Fri week, and `14 <= day <= 21` is a calendar-day range approximation. If the 3rd Friday falls on the 14th or 21st exactly, the logic may include or exclude days incorrectly.
4. **`january_effect` and `tax_loss_reversal` overlap in days 1–7 of January**: Both fire simultaneously in early January; they are additive (+0.18 combined) without documentation of whether this double-counting is intentional.
5. **`calendar_signals` table has no UNIQUE constraint on date**: Multiple records per date accumulate with repeated calls (same as sector_rotation table issue).
6. **Config is stored but never read**: All thresholds and modifiers are hardcoded constants.

---
---

## FILE 7: /home/dannyelticala/quant-fund/signals/options_earnings_signal.py

### A) PURPOSE
Analyzes options chain data around earnings events to generate pre-earnings straddle signals (BUY_STRADDLE or SELL_IV_CRUSH), post-earnings PEAD options overlay signals (BUY_OTM_CALLS or BUY_OTM_PUTS), and options flow-based PEAD confidence modifiers. Uses yfinance for all options data. No DB reads or writes.

---

### B) CLASSES AND METHODS

**Module-level functions:**

**`get_options_data(ticker: str) -> Optional[Dict]`**
- Input: `ticker` string
- Output: `Dict` with keys `calls` (DataFrame), `puts` (DataFrame), `expiry` (str) — or None on error
- Action: Fetches `yf.Ticker(ticker).options` (expiry list); selects nearest expiry (`expirations[0]`); fetches option chain; returns calls and puts DataFrames

**`implied_move(ticker: str) -> Optional[float]`**
- Input: `ticker` string
- Output: float percentage or None on error
- Action:
  1. Gets current price from `tk.info` or falls back to 2-day price history
  2. Calls `get_options_data()`
  3. Finds ATM call strike: closest to current price by absolute distance
  4. Gets ATM call and put ask prices; falls back to `lastPrice` if ask is 0
  5. Returns `(call_ask + put_ask) / current_price * 100`

**`historical_earnings_move(ticker: str, n_quarters: int = 8) -> Optional[float]`**
- Input: `ticker`, `n_quarters` (default 8)
- Output: float percentage (average absolute move) or None
- Action:
  1. Fetches `tk.earnings_dates` — filters to past dates only
  2. Downloads price history from oldest earnings date
  3. For each earnings date, finds the price on that date and the next trading day
  4. Computes `abs(p1 / p0 - 1) * 100` for each; returns `np.mean(moves)`

**`iv_rank(ticker: str) -> float`**
- Input: `ticker` string
- Output: float 0–100 (defaults to 50.0 on any error)
- Action:
  1. Fetches options data; finds ATM call by distance to current price
  2. Gets `impliedVolatility` column from ATM call row
  3. Returns proxy rank formula: `min(100, max(0, (iv_current - 0.15) / 0.50 * 100))`

**`generate_straddle_signal(ticker: str, days_to_earnings: int) -> Dict`**
- Input: `ticker`, `days_to_earnings` int
- Output: Dict with keys: `signal` (str), `implied_move` (float or None), `hist_move` (float or None), `iv_rank` (float)
- Action:
  1. Calls `iv_rank()`, `implied_move()`, `historical_earnings_move()`
  2. `in_window = 3 <= days_to_earnings <= 5`
  3. `iv_cheap = iv_r < 50`
  4. `hist_exceeds = hist_move > impl_move * 1.1`
  5. If in_window AND iv_cheap AND hist_exceeds: signal = "BUY_STRADDLE"
  6. Elif in_window AND iv_r >= 70: signal = "SELL_IV_CRUSH"
  7. Else: signal = "NONE"

**`generate_pead_options_signal(ticker: str, direction: int, days_after_earnings: int) -> Dict`**
- Input: `ticker`, `direction` (+1 or -1), `days_after_earnings` int
- Output: Dict with keys: `signal` (str), `sizing_fraction` (float, always 0.10)
- Action:
  1. If not `1 <= days_after_earnings <= 3`: returns NONE
  2. direction=+1 → signal="BUY_OTM_CALLS"
  3. direction=-1 → signal="BUY_OTM_PUTS"

**`get_pead_confidence_modifier(ticker: str, pead_direction: int) -> float`**
- Input: `ticker`, `pead_direction` (+1 or -1)
- Output: float (1.25, 0.75, or 1.0)
- Action:
  1. Fetches options data; sums total call volume and put volume
  2. Computes `call_put_ratio = total_call_vol / total_put_vol`
  3. If pead_direction == 1 AND call_put_ratio > 2.0: return 1.25
  4. If pead_direction == 1 AND call_put_ratio < 0.5: return 0.75
  5. If pead_direction == -1 AND call_put_ratio < 0.5: return 1.25
  6. If pead_direction == -1 AND call_put_ratio > 2.0: return 0.75
  7. Default: return 1.0

#### Class: `OptionsEarningsSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict
- Output: None
- Action: Stores config. No config keys are read.
- DB reads/writes: None

**`analyse(self, ticker: str, pead_direction=None, days_to_earnings=None, days_after_earnings=None) -> Dict`**
- Input:
  - `ticker`: string
  - `pead_direction`: optional +1 or -1
  - `days_to_earnings`: optional int
  - `days_after_earnings`: optional int
- Output: Dict with keys: `straddle` (straddle signal dict), `pead_options` (pead options signal dict), `confidence_modifier` (float)
- Action:
  1. If `days_to_earnings` is not None: calls `generate_straddle_signal()`
  2. If `pead_direction` and `days_after_earnings` are both not None: calls `generate_pead_options_signal()`
  3. If `pead_direction` is not None: calls `get_pead_confidence_modifier()`
  4. Returns assembled dict with defaults for any uncalled branches
- DB reads/writes: None

---

### C) MATHEMATICS

**FORMULA [implied move percentage]:**
- Variables:
  - `call_ask`: ask price of ATM call option (or lastPrice fallback)
  - `put_ask`: ask price of ATM put option (or lastPrice fallback)
  - `current_price`: current stock price from yfinance info or price history
- Calculation: `move_pct = (call_ask + put_ask) / current_price * 100`
- Output: Estimated percent move implied by options straddle pricing

**FORMULA [historical earnings move — per event]:**
- Variables:
  - `p0`: `float(close.iloc[loc])` — close price on earnings date
  - `p1`: `float(close.iloc[loc + 1])` — close price on next trading day
- Calculation: `move = abs(p1 / p0 - 1) * 100`
- Output: Absolute percentage move on earnings day; averaged across n_quarters

**FORMULA [historical earnings move — average]:**
- Variables:
  - `moves`: list of per-event absolute move percentages
- Calculation: `avg_move = float(np.mean(moves))`
- Output: Average historical earnings move; compared against implied_move

**FORMULA [IV rank proxy]:**
- Variables:
  - `iv_current`: `float(calls.loc[atm_idx, "impliedVolatility"])` — ATM call IV from yfinance options chain
- Calculation: `rank = min(100.0, max(0.0, (iv_current - 0.15) / 0.50 * 100.0))`
- Output: Proxy IV rank 0–100; assumes IV floor of 15% and ceiling of 65% (0.15 + 0.50)

**FORMULA [call/put volume ratio]:**
- Variables:
  - `total_call_vol`: `float(calls["volume"].fillna(0).sum())`
  - `total_put_vol`: `float(puts["volume"].fillna(0).sum())`
- Calculation: `call_put_ratio = total_call_vol / total_put_vol`
- Output: Ratio; threshold 2.0 = unusual call flow, threshold 0.5 = unusual put flow

**FORMULA [straddle BUY condition]:**
- Variables:
  - `in_window`: `3 <= days_to_earnings <= 5`
  - `iv_cheap`: `iv_r < 50`
  - `hist_exceeds`: `hist_move > impl_move * 1.1`
- Calculation: `in_window and iv_cheap and hist_exceeds`
- Output: Boolean triggering BUY_STRADDLE signal

---

### D) DATA FLOWS
- Input: `ticker` string; optional `pead_direction`, `days_to_earnings`, `days_after_earnings` — all caller-supplied
- External data: yfinance options chain data (HTTP, live per call); yfinance price history; yfinance earnings_dates
- Output: Dict of signals and modifiers returned to caller
- No DB reads or writes

---

### E) DEPENDENCIES
- Internal Apollo imports: None
- External libraries: `yfinance` (yf), `numpy` (np), `pandas` (pd), `logging`, `dataclasses` (dataclass), `datetime` (datetime, timedelta), `typing` (Dict, List, Optional, Tuple)

---

### F) WIRING STATUS
`OptionsEarningsSignal.analyse()` is designed to be called from `paper_trader.py` or from `pead_signal.py` as an overlay. The caller must supply `days_to_earnings` and `days_after_earnings` — these are not computed internally. If the caller never passes these, the straddle and pead_options branches never execute and only the confidence_modifier (if pead_direction supplied) is computed. The straddle signal ("BUY_STRADDLE", "SELL_IV_CRUSH") is informational — there is no evidence in this file that actual options orders are placed.

---

### G) ISSUES FOUND
1. **Straddle and PEAD options signals are informational only**: The returned `signal` strings ("BUY_STRADDLE", "BUY_OTM_CALLS", etc.) are not connected to any options order execution path visible in this file. It is unclear whether paper_trader.py acts on these values.
2. **`iv_rank` proxy formula assumes fixed IV floor and ceiling**: The formula `(iv_current - 0.15) / 0.50 * 100` assumes IV is always between 0.15 (15%) and 0.65 (65%). For high-volatility stocks (IV > 65%) the rank is capped at 100; for low-volatility stocks (IV < 15%) it returns 0 or less (clamped to 0). This is not true IV rank (which requires a 52-week history of IV) — it is a proxy with no acknowledgment of its limitations.
3. **`historical_earnings_move` uses `earnings_dates` which may not be available from yfinance for all tickers**: The `try/except` around `tk.earnings_dates` returns None on failure, causing the function to return None. The straddle check then cannot fire.
4. **`get_options_data` always uses the nearest expiry**: Using the nearest expiry for ATM straddle pricing is only valid when there are at least 3–5 days to expiry. For the last few days before expiry, ATM options may have very low time value and inflate implied_move incorrectly. No DTE (days-to-expiry) check is applied.
5. **`generate_pead_options_signal` always returns `sizing_fraction=0.10` hardcoded**: The sizing fraction cannot be configured and is the same regardless of signal conviction.
6. **`get_pead_confidence_modifier` divides by `total_put_vol`**: If `total_put_vol <= 0`, the function returns 1.0 (neutral). No protection against the case where `total_call_vol` is also 0.
7. **Config is stored but never read**.

---
---

## FILE 8: /home/dannyelticala/quant-fund/signals/pead_signal.py

### A) PURPOSE
Implements the Post-Earnings Announcement Drift (PEAD) strategy. Generates LONG (+1) or SHORT (-1) signals based on earnings surprises with volume confirmation, momentum confirmation (Day 1 price direction), z-score gating, and dynamic holding periods. Applies insider signal multipliers (via InsiderAnalyser) and context multipliers (macro score, altdata confluence, shipping stress, analyst revisions). Returns a DataFrame of signals rather than the list-of-dicts format used by simpler signals. This is the most complex signal in the system.

---

### B) CLASSES AND METHODS

#### Class: `PEADSignal`

**`__init__(self, config: dict)`**
- Input: `config` dict — expects `config["signal"]["pead"]` sub-dict
- Output: None
- Action:
  - Reads `cfg = config["signal"]["pead"]`
  - Sets `self.surprise_threshold = cfg.get("earnings_surprise_threshold", 0.08)`
  - Sets `self.volume_multiplier = cfg.get("volume_surge_multiplier", 1.3)`
  - Sets `self.holding_period = cfg.get("holding_period_days", 20)` (stored but overridden by `_dynamic_hold`)
  - Sets `self.zscore_window = cfg.get("zscore_window", 60)`
- DB reads/writes: None

**`generate(self, ticker: str, price_data: pd.DataFrame, earnings_history: pd.DataFrame = None, earnings_data: list = None) -> pd.DataFrame`**
- Input:
  - `ticker`: string
  - `price_data`: DataFrame with `close` and `volume` columns and DatetimeIndex
  - `earnings_history`: optional pre-built DataFrame with index=date and columns epsActual, epsEstimate, epsDifference, surprisePercent
  - `earnings_data`: optional list of dicts with keys: date, epsActual, epsEstimate (bypasses earnings_history building)
- Output: `pd.DataFrame` with columns: ticker, signal, entry_date, exit_date, earnings_date, surprise_pct, surprise_zscore, holding_days, data_quality; plus optional columns: insider_multiplier, insider_reason, conviction, size_multiplier
- Action:
  1. If `earnings_data` list is supplied and `earnings_history` is None: converts list to DataFrame (sets index to date, computes epsDifference)
  2. Guards for empty price_data or earnings_history
  3. Iterates over each row in earnings_history:
     - Calls `_calc_surprise()` — skips if None or quality=="low"
     - Skips if `abs(surprise) < self.surprise_threshold`
     - Calls `_volume_surge()` — skips if no volume surge
     - Calls `_momentum_entry()` — skips if None (Day 1 check fails)
     - Appends record
  4. Builds DataFrame, sorts by earnings_date
  5. Calls `_add_zscore()` — adds `surprise_zscore` column
  6. Applies z-score gate: drops rows where z-score is not NaN AND abs(z-score) <= 0.5
  7. Calls `_dynamic_hold()` per row — sets `holding_days`
  8. Calls `_nth_trading_day()` per row — sets `exit_date`
  9. If `config["signal"]["pead"]["use_insider_signals"]` is True (default): calls `apply_insider_multipliers()`
  10. Returns final DataFrame
- DB reads/writes: Indirect via `apply_insider_multipliers` → `InsiderAnalyser` (may read DB)

**`apply_insider_multipliers(self, signals_df: pd.DataFrame, ticker: str) -> pd.DataFrame`**
- Input: `signals_df` DataFrame of PEAD signals, `ticker` string
- Output: Modified DataFrame (rows may be dropped; columns added)
- Action:
  1. Lazy imports `InsiderAnalyser` from `analysis.insider_analyser`
  2. For each row: if signal direction <= 0 (SHORT), passes through unchanged
  3. For LONG signals: calls `ia.get_pead_multiplier(ticker, earnings_date, price_change_30d=0.0)`
  4. If multiplier == 0.0: suppresses signal (row dropped)
  5. If multiplier != 0: scales `surprise_pct` by multiplier; adds `insider_multiplier`, `insider_reason` columns
  6. If multiplier >= 1.8: adds `conviction="HIGH_CONVICTION"`; if >= 1.6: adds `conviction="CLUSTER_BUY"`
- DB reads/writes: Via `InsiderAnalyser` (reads insider data from DB)

**`apply_context_multipliers(self, signals: pd.DataFrame, ticker: str, sector: Optional[str] = None) -> pd.DataFrame`**
- Input: `signals` DataFrame, `ticker` string, `sector` optional string
- Output: Modified DataFrame with `size_multiplier` column updated
- Action:
  1. Initializes `size_multiplier` column to 1.0 if absent
  2. Starts `mult = 1.0`
  3. Block 1 — MacroSignalEngine EarningsContextScore:
     - If score >= 0.70: mult *= 1.20
     - If score <= 0.40: mult *= 0.70
  4. Block 2 — ClosedLoopStore altdata confluence:
     - If confluence >= 0.80: mult *= 1.30
     - If confluence <= 0.30: mult = 0.0 (suppression)
  5. Block 3 — Shipping stress index (retail sectors only):
     - Reads `shipping_data` table from `historical_db.db`
     - If SSI > 1.5: mult *= 0.70
  6. Block 4 — AnalystRevisionTracker:
     - "positive" → mult *= 1.20
     - "negative" → mult *= 0.80
  7. If mult != 1.0: multiplies all rows' size_multiplier by mult
- DB reads/writes: READS from `historical_db.db` → `shipping_data` table (column: metric='ShippingStressIndex', value, date); indirect reads via MacroSignalEngine, ClosedLoopStore, AnalystRevisionTracker

**`_calc_surprise(self, row, ticker: str = "") -> Tuple`**
- Input: `row` from earnings_history DataFrame, `ticker` string
- Output: Tuple `(surprise_fraction, quality)` where quality is "high", "low", or (None, None)
- Action:
  1. Path 1: If `surprisePercent` AND `epsActual` AND `epsEstimate` all present: returns `(surprisePercent, "high")` — treated as already-fractional
  2. Path 2: If `epsActual` AND `epsEstimate` both present AND `epsEstimate != 0`: returns `((actual - estimate) / abs(estimate), "high")`
  3. Path None: returns `(None, None)` — epsDifference-only or fully absent data

**`_volume_surge(self, price_data: pd.DataFrame, earnings_date) -> bool`**
- Input: `price_data`, `earnings_date`
- Output: bool
- Action: Slices price_data up to earnings_date; takes last 20 of the preceding rows as average; checks if volume on earnings_date >= `self.volume_multiplier * avg`

**`_momentum_entry(self, price_data: pd.DataFrame, earnings_date, direction: int) -> Optional[pd.Timestamp]`**
- Input: `price_data`, `earnings_date`, `direction` (+1 or -1)
- Output: `pd.Timestamp` entry date or None
- Action:
  1. Finds t0 (earnings day close) and post-earnings dates
  2. If fewer than 2 post-earnings dates: falls back to day+1 entry (post[0])
  3. Checks if day+1 close moved in signal direction vs t0 close (no minimum threshold)
  4. If confirmed: returns `post[1]` (day+2 entry)
  5. If not confirmed: returns None (signal suppressed)

**`_dynamic_hold(self, zscore: Optional[float], surprise_pct: float) -> int`**
- Input: `zscore` (not used in logic), `surprise_pct` float
- Output: int — holding period in days
- Action:
  - If `abs(surprise_pct) > 0.20`: return 30
  - If `abs(surprise_pct) > 0.10`: return 20
  - Else: return 12

**`_add_zscore(self, df: pd.DataFrame) -> pd.DataFrame`**
- Input: DataFrame with `surprise_pct` column
- Output: DataFrame with added `surprise_zscore` column
- Action: Computes rolling mean and std of `surprise_pct` with window=`self.zscore_window`, min_periods=5; `zscore = (sp - roll_mean) / (roll_std + 1e-8)`

**`_next_trading_day(self, index: pd.DatetimeIndex, date) -> Optional[pd.Timestamp]`**
- Input: price_data index, date
- Output: first date in index after `date`; None if none exist

**`_nth_trading_day(self, index: pd.DatetimeIndex, start: pd.Timestamp, n: int) -> Optional[pd.Timestamp]`**
- Input: price_data index, start date, n int
- Output: `future[n-1]` if at least n future dates exist; else `future[-1]` (last available)

---

### C) MATHEMATICS

**FORMULA [EPS surprise — path 1]:**
- Variables:
  - `pct`: `_f("surprisePercent")` from earnings_history row — pre-computed fraction (e.g. 0.1676 for 16.76% beat)
- Calculation: returned directly as-is: `return pct, "high"`
- Output: Surprise fraction; treated as already fractional

**FORMULA [EPS surprise — path 2]:**
- Variables:
  - `actual`: `_f("epsActual")` from row — float EPS actual
  - `estimate`: `_f("epsEstimate")` from row — float EPS consensus estimate
- Calculation: `result = (actual - estimate) / abs(estimate)`
- Output: Fractional EPS surprise; positive = beat, negative = miss

**FORMULA [EPS difference for earnings_data conversion]:**
- Variables:
  - `rec.get("epsActual", 0) or 0`: actual EPS from input dict
  - `rec.get("epsEstimate", 0) or 0`: estimated EPS from input dict
- Calculation: `float(rec.get("epsActual", 0) or 0) - float(rec.get("epsEstimate", 0) or 0)`
- Output: Absolute EPS difference stored in `epsDifference` column (not the fractional surprise)

**FORMULA [volume surge check]:**
- Variables:
  - `window.iloc[-1]`: volume on earnings date
  - `avg`: `window.iloc[:-1].tail(20).mean()` — mean of prior 20 days' volume
  - `self.volume_multiplier`: from config (default 1.3)
- Calculation: `window.iloc[-1] >= self.volume_multiplier * avg`
- Output: bool — True if earnings-day volume is at least 1.3x the 20-day average

**FORMULA [surprise z-score]:**
- Variables:
  - `sp`: `df["surprise_pct"]` series — all surprise values
  - `roll_mean`: `sp.rolling(self.zscore_window, min_periods=5).mean()`
  - `roll_std`: `sp.rolling(self.zscore_window, min_periods=5).std()`
- Calculation: `df["surprise_zscore"] = (sp - roll_mean) / (roll_std + 1e-8)`
- Output: Rolling z-score of each surprise relative to recent surprise history; z-score > 0.5 required to pass gate

**FORMULA [dynamic holding period]:**
- Variables:
  - `ab`: `abs(surprise_pct)`
  - `_HOLD_LONG_ABS = 0.20`
  - `_HOLD_MID_ABS = 0.10`
- Calculation:
  - `if ab > 0.20: return 30`
  - `if ab > 0.10: return 20`
  - `else: return 12`
- Output: int holding period in trading days

**FORMULA [Day 1 momentum confirmation]:**
- Variables:
  - `t0_close`: close price on earnings_date
  - `day1_close`: close price on day+1 after earnings
  - `direction`: +1 (long) or -1 (short)
- Calculation:
  - `confirmed = (direction == 1 and day1_close > t0_close) or (direction == -1 and day1_close < t0_close)`
- Output: bool — if True, enter on day+2; if False, suppress signal

**FORMULA [insider multiplier application to surprise_pct]:**
- Variables:
  - `row["surprise_pct"]`: original surprise fraction
  - `multiplier`: result from `ia.get_pead_multiplier()`
- Calculation: `new_row["surprise_pct"] = float(row.get("surprise_pct", 0)) * multiplier`
- Output: Scaled surprise_pct stored back in signal row

**FORMULA [context multiplier composition]:**
- Variables:
  - `mult`: starts at 1.0
  - MacroSignalEngine block: mult *= 1.20 (score >= 0.70) or mult *= 0.70 (score <= 0.40)
  - Altdata block: mult *= 1.30 (confluence >= 0.80) or mult = 0.0 (confluence <= 0.30)
  - SSI block: mult *= 0.70 (SSI > 1.5 AND retail sector)
  - Analyst block: mult *= 1.20 (positive) or mult *= 0.80 (negative)
- Calculation: All multipliers applied sequentially to running `mult`; then `signals["size_multiplier"] = signals["size_multiplier"] * mult`
- Output: Updated `size_multiplier` column in signals DataFrame

---

### D) DATA FLOWS
- Input: `price_data` DataFrame (caller-supplied); `earnings_history` or `earnings_data` (caller-supplied)
- Input: Config read from `config["signal"]["pead"]` sub-dict at init
- Output: `pd.DataFrame` of signals with ticker, signal (+1/-1), entry_date, exit_date, earnings_date, surprise_pct, surprise_zscore, holding_days, data_quality
- Internal reads: `historical_db.db` → `shipping_data` table (in `apply_context_multipliers`)
- Indirect reads: `InsiderAnalyser` DB reads; `MacroSignalEngine` reads; `ClosedLoopStore` reads; `AnalystRevisionTracker` reads
- No direct DB writes in this file

---

### E) DEPENDENCIES
- Internal Apollo imports:
  - `analysis.insider_analyser.InsiderAnalyser` (lazy import in `apply_insider_multipliers`)
  - `analysis.macro_signal_engine.MacroSignalEngine` (lazy import in `apply_context_multipliers`)
  - `closeloop.storage.closeloop_store.ClosedLoopStore` (lazy import in `apply_context_multipliers`)
  - `closeloop.context.analyst_revision_tracker.AnalystRevisionTracker` (lazy import in `apply_context_multipliers`)
- External libraries: `numpy` (np), `pandas` (pd), `logging`, `sqlite3`, `pathlib` (Path), `typing` (Optional, Tuple)

---

### F) WIRING STATUS
`PEADSignal.generate()` is called from `paper_trader.py` as the primary earnings drift signal. `apply_context_multipliers()` is a separate method that must be explicitly called by the paper trader — it is not called inside `generate()`. If the paper trader does not call `apply_context_multipliers()`, all context-based sizing adjustments (macro, altdata, SSI, analyst) are skipped. `apply_insider_multipliers()` IS called inside `generate()` if `use_insider_signals` is True in config. The output format (DataFrame) differs from all other signals (which return List[Dict]) — the paper trader must handle this separately.

---

### G) ISSUES FOUND
1. **`_dynamic_hold` receives `zscore` parameter but never uses it**: The method signature accepts `zscore: Optional[float]` but the implementation only branches on `abs(surprise_pct)`. The z-score has no effect on holding period despite the parameter's presence.
2. **`self.holding_period` config value is never used**: `__init__` reads `holding_period_days` from config and stores it as `self.holding_period`, but `_dynamic_hold` only uses the hardcoded constants `_HOLD_LONG_ABS` (0.20) and `_HOLD_MID_ABS` (0.10). The config-supplied holding period is dead code.
3. **`apply_context_multipliers` is not called inside `generate()`**: It must be called separately by the orchestrator. If not called, context multipliers never apply. There is no indication in `generate()` that this second method is expected to be called.
4. **`_nth_trading_day` fallback uses `future[-1]`**: When fewer than `n` future trading days exist in the price data, the exit date falls back to the last available date. This means the signal exits on whatever date the data ends — which for live data could be the most recent day, making the holding period shorter than intended with no warning.
5. **Altdata confluence suppression sets `mult = 0.0` directly**: If `confluence <= 0.30`, `mult` is set to exactly 0.0 (not multiplied). This also nullifies any prior multiplier accumulation from MacroSignalEngine in the same call. The suppression is absolute regardless of how strong the macro or insider signals were.
6. **Short signals are skipped by `apply_insider_multipliers`**: Only signals with `direction > 0` get insider multiplier treatment. Short PEAD signals are passed through unchanged — no insider-based suppression or amplification for shorts.
7. **`_calc_surprise` path 1 relies on `surprisePercent` being pre-normalized as a fraction**: The comment states "EarningsCalendar.get_earnings_surprise() already normalises to a fraction." If `surprisePercent` is stored as a raw percentage (e.g., 16.76 instead of 0.1676), path 1 produces a ~100x inflated surprise value with no validation or warning.
8. **`_volume_surge` slice `price_data.loc[:earnings_date]["volume"]`**: If `earnings_date` is a Timestamp not in the index (e.g., weekend), `.loc[:earnings_date]` will still work on a sorted index, but if `earnings_date` exactly matches an index entry, the resulting window's last entry IS the earnings date. Then `window.iloc[:-1].tail(20)` skips the earnings day's volume for the average — this is correct behavior, but if earnings are announced pre-market and the earnings "date" is actually the announcement date while the price "date" is a day earlier, the window boundary may shift.
9. **`surprise_pct` is mutated in-place in `apply_insider_multipliers`**: `new_row["surprise_pct"] = float(row.get("surprise_pct", 0)) * multiplier` modifies the surprise_pct column to reflect insider scaling. This means the surprise_pct in the output DataFrame is no longer the raw EPS surprise — it is the insider-adjusted value. Downstream code reading this column will see inflated/suppressed values that are not the actual earnings surprise.
10. **DB path construction in `apply_context_multipliers` uses `Path(__file__).resolve().parents[1]`**: This resolves correctly when `pead_signal.py` is in `signals/` and `output/` is at the project root. If the file is moved or called from a different context, the DB path breaks.

---
---

# SECTION 3A — SIGNAL REGISTRY

---

## Signal 1: MOMENTUM

**Signal name:** Momentum Signal
**File:** `/home/dannyelticala/quant-fund/signals/momentum_signal.py`
**Class:** `MomentumSignal`
**Method called from paper_trader.py:** `generate`
**Input:** `ticker` (string), `price_data` (pd.DataFrame with `close` column, minimum 60 rows)
**Output format:**
```
[{
  'ticker':       str,
  'direction':    'LONG' | 'SHORT',
  'score':        float  (0.0 to 1.0),
  'r1m':          float  (1-month return fraction),
  'r3m':          float  (3-month return fraction),
  'r6m':          float  (6-month return fraction),
  'signal_type':  'MOMENTUM'
}]
```
Empty list if no signal or on error.

**Direction logic:** Composite score = `r1m * 0.5 + r3m * 0.3 + r6m * 0.2`. If score > 0 → LONG; if score < 0 → SHORT. Only fires if `abs(score) > 0.05`.
**Score range:** 0.0 to 1.0 (formula: `min(abs(score) * 3, 1.0)`; reaches 1.0 at composite score of 0.333)
**Known issues:**
- Minimum guard of 60 rows does not prevent r3m (needs 63) and r6m (needs 126) from silently defaulting to 0
- Config is stored but never read; all parameters hardcoded
- Broad exception swallows errors silently at DEBUG level

**Wiring status:** FULLY WIRED

---

## Signal 2: MEAN REVERSION

**Signal name:** Mean Reversion Signal
**File:** `/home/dannyelticala/quant-fund/signals/mean_reversion_signal.py`
**Class:** `MeanReversionSignal`
**Method called from paper_trader.py:** `generate`
**Input:** `ticker` (string), `price_data` (pd.DataFrame with `close` column, minimum 30 rows)
**Output format:**
```
[{
  'ticker':       str,
  'direction':    'LONG' | 'SHORT',
  'score':        float  (0.0 to 1.0),
  'zscore':       float,
  'rsi':          float  (0–100),
  'signal_type':  'MEAN_REVERSION'
}]
```
Empty list if no signal or on error.

**Direction logic:** LONG when z-score < -2.0 AND RSI < 35. SHORT when z-score > 2.0 AND RSI > 65. No signal if conditions not met.
**Score range:** 0.0 to 1.0 (formula: `min(abs(zscore) / 3, 1.0)`; reaches 1.0 at z-score of 3.0 or higher)
**Known issues:**
- RSI uses simple 14-period rolling mean instead of Wilder's exponential smoothing — not canonical RSI
- Std floor of 1 is too large relative to typical stock prices
- With minimum 30 rows, RSI at index -1 may be NaN silently producing no signal
- Config never read; all parameters hardcoded

**Wiring status:** FULLY WIRED

---

## Signal 3: GAP

**Signal name:** Gap Signal
**File:** `/home/dannyelticala/quant-fund/signals/gap_signal.py`
**Class:** `GapSignal`
**Method called from paper_trader.py:** `generate`
**Input:**
- `ticker` (string)
- `price_data` (pd.DataFrame with `open`, `close`, `volume` columns, minimum 5 rows)
- `within_open_window` (bool, default True — caller must supply False outside first 30 min)
- `opening_volume` (float or None — caller must supply for volume filter to work)

**Output format:**
```
[{
  'ticker':         str,
  'direction':      'LONG' | 'SHORT',
  'score':          float  (0.0 to 1.0 for fades; 0.0 to 0.7 for BULL continuation),
  'gap_pct':        float,
  'signal_type':    'GAP',
  'signal_subtype': 'GAP_FADE' | 'GAP_CONTINUATION',
  'gap_context': {
    'regime':         str,
    'gap_pct':        float,
    'abs_gap_pct':    float,
    'gap_direction':  int (+1 or -1),
    'sector_aligned': False,  (always False — bug)
    'action':         str
  }
}]
```
Empty list if filtered or on error.

**Direction logic:**
- CRISIS regime: no signal
- BULL regime + up-gap: LONG (continuation)
- BULL regime + down-gap: LONG (fade)
- NEUTRAL/BEAR + up-gap: SHORT (fade)
- NEUTRAL/BEAR + down-gap: LONG (fade)
- Suppressed if sector ETF in cache is gapping same direction (sector alignment filter — but check is incorrectly ticker-agnostic)

**Score range:** 0.0 to 0.7 (BULL continuation: `min(abs_gap * 8, 0.7)`); 0.0 to 1.0 (fade: `min(abs_gap * 10, 1.0)`)
**Known issues:**
- `_regime_cache` initialized but never used
- `ticker` parameter in `_sector_aligned` is ignored — alignment check is sector-agnostic
- `gap_context["sector_aligned"]` is always hardcoded False
- Default `within_open_window=True` is an unsafe default
- `open_col` fallback to `columns[0]` may be wrong
- Operator-level bug: sector alignment fires for any ETF in the cache regardless of ticker's sector

**Wiring status:** FULLY WIRED (but sector alignment check is broken; volume filter requires caller to supply `opening_volume`)

---

## Signal 4: INSIDER MOMENTUM

**Signal name:** Insider Momentum Signal
**File:** `/home/dannyelticala/quant-fund/signals/insider_momentum_signal.py`
**Class:** `InsiderMomentumSignal`
**Method called from paper_trader.py:** `generate`
**Input:** `ticker` (string), `price_data` (pd.DataFrame — accepted but never used)
**Output format:**
```
[{
  'ticker':                str,
  'direction':             'LONG',
  'score':                 float  (0.0 to 1.0),
  'insider_filings_60d':   int,
  'signal_type':           'INSIDER_MOMENTUM'
}]
```
Empty list if fewer than 2 Form 4 filings or on error.

**Direction logic:** Always LONG if 2 or more Form 4 filings in the past 60 days. No SHORT signal ever generated. No distinction between buy and sell filings.
**Score range:** 0.0 to 1.0 (formula: `min(len(trades) / 10.0, 1.0)`; reaches 1.0 at 10+ filings)
**Known issues:**
- CRITICAL: No distinction between insider buys and sells — selling is falsely treated as bullish
- No SHORT signal capability
- Live HTTP call to SEC EDGAR API during signal generation; 15-second timeout; no caching
- `price_data` parameter accepted but never read
- Hardcoded email in User-Agent header
- Nonstandard SEC EDGAR endpoint; response parsing via nested `.get('hits',{}).get('hits',[])` — silently returns empty on schema changes

**Wiring status:** FULLY WIRED (but signal logic is fundamentally flawed — buys and sells not distinguished)

---

## Signal 5: SECTOR ROTATION

**Signal name:** Sector Rotation Signal
**File:** `/home/dannyelticala/quant-fund/signals/sector_rotation_signal.py`
**Class:** `SectorRotationSignal`
**Method called from paper_trader.py:** `get_modifier` (or `run` for full state computation)
**Input:**
- `run()`: no direct inputs; fetches live market data via yfinance
- `get_modifier(ticker, sector=None)`: ticker string; optional sector string

**Output format:**
- `run()` returns:
```
{
  'top_sectors':    [str, str, str],
  'bottom_sectors': [str, str, str],
  'scores':         {sector_name: float, ...},
  'rotating_in':    [str, ...],
  'rotating_out':   [str, ...],
  'calculated_at':  str (ISO 8601)
}
```
- `get_modifier()` returns: float in `{-0.15, 0.0, +0.15}`

**Direction logic:** Not a directional signal. Returns a size modifier: +0.15 (favored sector), -0.15 (disfavored sector), 0.0 (neutral). Used by PEAD signal to adjust position sizing.
**Score range:** Modifier: `{-0.15, 0.0, +0.15}`. Underlying RS composite: unbounded float (typically small, e.g. -0.05 to +0.05 range).
**Known issues:**
- CRITICAL: Operator precedence bug in `detect_rotation` — `&` evaluates before `-` making rotating_in/rotating_out calculations incorrect
- Old-score proxy in detect_rotation does not subtract SPY — mixes raw return vs excess return
- 4–5 yfinance download calls per `run()` cycle; 20-day window downloaded twice
- `sector_rotation` table has no UNIQUE constraint on date — duplicate rows accumulate
- `get_modifier` triggers `run()` (4 network calls + DB write) on first call per instance

**Wiring status:** PARTIALLY WIRED — `get_modifier` is used as a PEAD modifier; the rotation detection logic is broken due to operator precedence bug

---

## Signal 6: CALENDAR EFFECTS

**Signal name:** Calendar Effects Signal
**File:** `/home/dannyelticala/quant-fund/signals/calendar_effects_signal.py`
**Class:** `CalendarEffectsSignal`
**Method called from paper_trader.py:** `get_pead_timing_modifier` or `get_composite_modifier`
**Input:**
- `get_pead_timing_modifier(dt=None, is_small_cap=True, days_to_fed=None)`: all optional; caller must supply `days_to_fed` for FOMC effect to work
- `get_composite_modifier(...)`: same plus `ytd_return_pct`, `store_to_db`

**Output format:**
- `get_pead_timing_modifier()` returns: float (clamped to [-0.40, +0.30])
- `get_composite_modifier()` returns:
```
{
  'total_modifier':    float  (clamped [-0.40, +0.30]),
  'january_effect':    float,
  'tax_loss_reversal': float,
  'earnings_season':   float,
  'options_expiry':    float,
  'fed_proximity':     float,
  'window_dressing':   float,
  'effective_date':    str (ISO 8601)
}
```

**Direction logic:** Not directional. Returns a combined size modifier for PEAD and other signals. Positive modifier increases size; negative decreases.
**Score range:** Total modifier clamped to [-0.40, +0.30]
**Known issues:**
- `days_to_fed` defaults to None throughout; Fed proximity effect always 0.0 unless caller explicitly supplies FOMC calendar data
- `window_dressing` value is in the result dict but NOT stored to DB (missing column in calendar_signals table)
- `options_expiry_proximity` standard expiry check is an approximation that may be off by days
- January effect and tax-loss reversal both fire simultaneously Jan 1–7 (additive +0.18); not documented as intentional
- `calendar_signals` table has no UNIQUE constraint on date

**Wiring status:** PARTIALLY WIRED — modifier computation is functional; FOMC proximity effect is silently disabled without caller supplying `days_to_fed`; window_dressing stored in result but missing from DB

---

## Signal 7: OPTIONS EARNINGS

**Signal name:** Options Earnings Signal
**File:** `/home/dannyelticala/quant-fund/signals/options_earnings_signal.py`
**Class:** `OptionsEarningsSignal`
**Method called from paper_trader.py:** `analyse`
**Input:**
- `ticker` (string)
- `pead_direction` (optional +1 or -1)
- `days_to_earnings` (optional int — must be supplied for straddle check)
- `days_after_earnings` (optional int — must be supplied for pead options signal)

**Output format:**
```
{
  'straddle': {
    'signal':       'BUY_STRADDLE' | 'SELL_IV_CRUSH' | 'NONE',
    'implied_move': float or None,
    'hist_move':    float or None,
    'iv_rank':      float  (0–100)
  },
  'pead_options': {
    'signal':           'BUY_OTM_CALLS' | 'BUY_OTM_PUTS' | 'NONE',
    'sizing_fraction':  0.10  (always)
  },
  'confidence_modifier': float  (1.25, 1.0, or 0.75)
}
```

**Direction logic:**
- Straddle: BUY_STRADDLE if `3 <= days_to_earnings <= 5` AND `iv_rank < 50` AND `hist_move > impl_move * 1.1`; SELL_IV_CRUSH if in window AND `iv_rank >= 70`
- PEAD options: BUY_OTM_CALLS (direction=+1), BUY_OTM_PUTS (direction=-1), within 1–3 days after earnings
- Confidence modifier: 1.25 (unusual flow confirming direction), 0.75 (opposing flow), 1.0 (neutral)

**Score range:** No standalone score; confidence_modifier in `{0.75, 1.0, 1.25}`
**Known issues:**
- BUY_STRADDLE and BUY_OTM_CALLS/PUTS are informational only — no options order execution visible in this file
- IV rank is a proxy formula (not true 52-week IV rank); assumes IV floor 15%, ceiling 65%
- Always uses nearest options expiry regardless of DTE
- `sizing_fraction` hardcoded at 0.10; not configurable
- All optional parameters must be caller-supplied; if not supplied, branches silently skip
- Config never read

**Wiring status:** PARTIALLY WIRED — `analyse()` is callable but straddle/PEAD options signals are informational; no evidence of options order routing in this file

---

## Signal 8: PEAD

**Signal name:** PEAD Signal (Post-Earnings Announcement Drift)
**File:** `/home/dannyelticala/quant-fund/signals/pead_signal.py`
**Class:** `PEADSignal`
**Method called from paper_trader.py:** `generate`
**Input:**
- `ticker` (string)
- `price_data` (pd.DataFrame with `close` and `volume` columns, DatetimeIndex)
- `earnings_history` (pd.DataFrame, optional — pre-built earnings data)
- `earnings_data` (list of dicts, optional — bypasses earnings_history; each dict: `{date, epsActual, epsEstimate}`)

**Output format:** `pd.DataFrame` (NOT a list of dicts like all other signals) with columns:
```
ticker, signal (+1 or -1), entry_date, exit_date, earnings_date,
surprise_pct, surprise_zscore, holding_days, data_quality
```
Optional additional columns (when applicable):
```
insider_multiplier, insider_reason, conviction, size_multiplier
```
Empty DataFrame if no signals pass all gates.

**Direction logic:** `signal = +1` if EPS surprise > 0 (beat); `signal = -1` if EPS surprise < 0 (miss). Requires: abs(surprise) >= `surprise_threshold` (default 8%), volume surge on earnings day (>= 1.3x 20-day avg), Day 1 direction confirmation (price moved in signal direction on day+1), abs(z-score) > 0.5 (or NaN for early data).
**Score range:** No single score field. `surprise_pct` is the raw EPS surprise fraction (may be mutated by insider multiplier). `surprise_zscore` is the rolling z-score. `size_multiplier` column added by `apply_context_multipliers` (not called from `generate`).
**Known issues:**
- `_dynamic_hold` accepts zscore parameter but never uses it — hardcoded thresholds only
- `self.holding_period` from config is stored but never used (dead code)
- `apply_context_multipliers` must be called separately — not called from `generate()`; easily missed
- `_nth_trading_day` falls back silently to last available date when n trading days not available
- Altdata suppression (`confluence <= 0.30`) zeroes out all prior multipliers absolutely
- `apply_insider_multipliers` skips SHORT signals entirely
- `_calc_surprise` path 1 assumes `surprisePercent` is pre-normalized as fraction — if stored as raw percentage, 100x inflation occurs silently
- `surprise_pct` column is mutated by insider multiplier — no longer represents raw EPS surprise in output
- Output format (DataFrame) differs from all other signals (List[Dict]) — paper_trader must handle separately
- DB path in `apply_context_multipliers` uses `parents[1]` — breaks if file is relocated

**Wiring status:** FULLY WIRED (generate is called from paper_trader.py; `apply_context_multipliers` is a separate call that may or may not be made by the orchestrator)

---
---

# SECTION 3 GATE (PARTIAL — SIGNALS ONLY)

## Files Read
1. `/home/dannyelticala/quant-fund/signals/momentum_signal.py`
2. `/home/dannyelticala/quant-fund/signals/mean_reversion_signal.py`
3. `/home/dannyelticala/quant-fund/signals/gap_signal.py`
4. `/home/dannyelticala/quant-fund/signals/insider_momentum_signal.py`
5. `/home/dannyelticala/quant-fund/signals/sector_rotation_signal.py`
6. `/home/dannyelticala/quant-fund/signals/calendar_effects_signal.py`
7. `/home/dannyelticala/quant-fund/signals/options_earnings_signal.py`
8. `/home/dannyelticala/quant-fund/signals/pead_signal.py`

## Key Findings

1. **Output format inconsistency**: 7 of 8 signals return `List[Dict]`. PEAD returns `pd.DataFrame`. The paper trader must handle two different output types from the same signal interface. No shared base class or protocol enforces a common return type.

2. **Config is universally stored but rarely read**: Every signal stores `self.config` in `__init__` but most read zero config keys. Exceptions: PEADSignal reads 4 keys from `config["signal"]["pead"]`. All thresholds in all other signals are hardcoded.

3. **Broad exception swallowing is universal**: Every signal wraps its main logic in `try/except Exception as e` and logs at DEBUG. Failures are invisible at normal log levels.

4. **Sector rotation detection is mathematically broken**: The `detect_rotation` function has a Python operator precedence bug (`&` before `-`) and mixes raw returns with excess returns. The `rotating_in` and `rotating_out` lists are computed incorrectly on every run.

5. **Insider momentum signal cannot distinguish buys from sells**: All Form 4 activity (purchases AND sales) generates LONG signals. This is a fundamental logic error for a signal claiming to detect "bullish insider momentum."

6. **PEAD is the only signal that reads a DB during execution**: `apply_context_multipliers` reads `historical_db.db → shipping_data`. All other signals are pure computation or external API calls.

7. **DB writes target two different databases**: Sector rotation writes to `historical_db.db` → `sector_rotation`. Calendar effects writes to `permanent_archive.db` → `calendar_signals`. No signal reads back from these tables for its own computation.

8. **Calendar effects FOMC proximity is permanently disabled in practice**: `days_to_fed` defaults to None at every level of the call chain. Unless the caller supplies this value from an external FOMC calendar (not present in this file), the Fed proximity modifier is always 0.0.

9. **Options earnings signals are informational only**: The straddle and OTM options signals produce signal strings but there is no order routing to an options broker visible in this file. Whether paper_trader.py acts on these strings is unknown from this file alone.

10. **Gap signal sector alignment check is ticker-agnostic**: The `_sector_aligned` check returns True if ANY sector ETF in the cache is gapping the same direction — not the sector of the ticker being evaluated. A tech stock can be suppressed because the energy ETF is gapping.

11. **PEAD's `apply_context_multipliers` is not called from `generate()`**: The context multiplier chain (macro score, altdata, SSI, analyst revisions) is a dead code path unless the paper trader explicitly calls this method after `generate()`.

12. **Insider momentum signal makes live HTTP calls in the signal hot path**: Up to 15 seconds of blocking per ticker during signal generation.

13. **PEAD `surprise_pct` column is mutated by insider multiplier**: The output DataFrame's `surprise_pct` column no longer represents the raw EPS surprise when insider multipliers are applied — it is the scaled version. This affects any downstream code interpreting surprise_pct as an EPS figure.

14. **`window_dressing` is computed and returned by CalendarEffectsSignal but never stored to DB**: The `calendar_signals` table has no `window_dressing` column, and `_store_calendar_signal` does not include it in the INSERT.

15. **Multiple duplicate DB rows per day**: Neither `sector_rotation` nor `calendar_signals` tables have UNIQUE constraints on `date`. Every `run()` or `get_composite_modifier(store_to_db=True)` call appends a new row.

## Contradictions Found

1. **GapSignal `_regime_cache` is initialized but never populated or read**: The attribute exists but serves no function.

2. **`_dynamic_hold` accepts `zscore` but ignores it**: Method signature implies z-score modulates holding period; implementation uses only `surprise_pct`.

3. **InsiderMomentumSignal claims to detect "insider momentum" but generates LONG for all Form 4 filings including sells**: Sell filings generate the same LONG signal as buy filings.

4. **`sector_rotation.detect_rotation` claims to find sectors "rotating in from the bottom"** but the operator precedence bug means `(current_top5 - old_top5 & old_bot5)` is computed as `current_top5 - (old_top5 & old_bot5)` — sectors that were simultaneously in old_top5 AND old_bot5 (impossible — contradiction) are subtracted. The result is effectively `current_top5 - empty_set = current_top5`, making `rotating_in` equal to all of current_top5 minus nothing meaningful.

5. **PEADSignal stores `holding_period` from config but `_dynamic_hold` uses only hardcoded constants**: Config override of holding period silently has no effect.

6. **CalendarEffectsSignal stores config but reads zero config keys**: The config parameter is accepted and stored but all thresholds are hardcoded, making config-based tuning impossible.

7. **`gap_context["sector_aligned"]` is hardcoded False**: Even when sector alignment IS detected (and the signal is suppressed before reaching the dict-building code), the field is always False in output. True sector alignment is never recorded.

## Signal Formulas Extracted

Count: **27 distinct formulas**

1. Momentum 1-month return: `r1m = float(close.iloc[-1] / close.iloc[-21] - 1)`
2. Momentum 3-month return: `r3m = float(close.iloc[-1] / close.iloc[-63] - 1)`
3. Momentum 6-month return: `r6m = float(close.iloc[-1] / close.iloc[-126] - 1)`
4. Momentum composite score: `score = r1m * 0.5 + r3m * 0.3 + r6m * 0.2`
5. Momentum score normalization: `min(abs(score) * 3, 1.0)`
6. Mean reversion Bollinger mean: `close.rolling(20).mean()`
7. Mean reversion Bollinger std: `close.rolling(20).std()`
8. Mean reversion z-score: `zscore = (current - mean) / std` (std floored at 1)
9. Mean reversion RSI gains: `delta.clip(lower=0).rolling(14).mean()`
10. Mean reversion RSI losses: `(-delta.clip(upper=0)).rolling(14).mean()`
11. Mean reversion RS: `gain / loss.replace(0, 1e-10)`
12. Mean reversion RSI: `float(100 - 100 / (1 + rs.iloc[-1]))`
13. Mean reversion score: `min(abs(zscore) / 3, 1.0)`
14. Gap percentage: `gap_pct = (today_open - prev_close) / prev_close`
15. Gap 20-day average volume: `vol_series.iloc[-21:-1].mean()`
16. Gap volume ratio: `opening_volume / avg_vol_20d`
17. Gap score — BULL continuation: `min(abs_gap * 8, 0.7)`
18. Gap score — fade: `min(abs_gap * 10, 1.0)`
19. Insider signal score: `min(len(trades) / 10.0, 1.0)`
20. Sector rotation total return per window: `float(series.iloc[-1]) / float(series.iloc[0]) - 1`
21. Sector excess return per window: `ret_Xw.get(etf, 0.0) - spy_Xw`
22. Sector composite RS: `0.5 * rs_4w + 0.3 * rs_12w + 0.2 * rs_26w`
23. Calendar total modifier sum: `jan_eff + tax_rev + earn_seas + opts_exp + fed_prox + win_dress`
24. Calendar clamped modifier: `max(-0.40, min(0.30, raw_total))`
25. Options implied move: `(call_ask + put_ask) / current_price * 100`
26. Options IV rank proxy: `min(100.0, max(0.0, (iv_current - 0.15) / 0.50 * 100.0))`
27. PEAD EPS surprise: `(actual - estimate) / abs(estimate)` and `surprise_zscore = (sp - roll_mean) / (roll_std + 1e-8)`

---

## Proceed to GROUP 5 Reading: YES
