# APOLLO SYSTEM MAP — PART 7A
## Group 7 Collectors: First Half (8 Files)
**Generated:** 2026-04-08
**Scope:** data/historical_collector.py + data/collectors/ (first 7 collectors)

---

## FULL INVENTORY: /home/dannyelticala/quant-fund/data/collectors/

All `.py` files found:

| File | Description (from module docstring) |
|------|--------------------------------------|
| advanced_news_intelligence.py | (not in this batch) |
| alternative_quiver_collector.py | (not in this batch) |
| commodity_collector.py | STEP 7 — Commodity Collector |
| consumer_intelligence.py | STEP 6 — Consumer Intelligence |
| geopolitical_collector.py | (not in this batch) |
| geographic_intelligence.py | (not in this batch) |
| government_data_collector.py | Government data: BLS, Census, USASpending |
| insider_transaction_collector.py | SEC EDGAR Form 4 insider transactions |
| job_postings_collector.py | (not in this batch) |
| news_context_enricher.py | (not in this batch) |
| openbb_collector.py | (not in this batch) |
| polygon_collector.py | (not in this batch) |
| quiver_collector.py | (not in this batch) |
| rates_credit_collector.py | STEP 8 — Rates/Credit Collector |
| regulatory_intelligence.py | (not in this batch) |
| sec_fulltext_collector.py | (not in this batch) |
| shipping_intelligence.py | STEP 3 — Shipping Intelligence Collector |
| short_interest_collector.py | (not in this batch) |
| simfin_collector.py | (not in this batch) |
| social_influence_tracker.py | (not in this batch) |
| technology_intelligence.py | Technology Thematic Intelligence Collector |

**Total .py files in /data/collectors/: 21**

---

---

# FILE 1: data/historical_collector.py

## A) PURPOSE

The core historical data bootstrap collector. Fetches and stores multi-year price history, financial statements, SEC EDGAR filing metadata, institutional ownership, insider transactions, macro context series, and news. Also runs a Phase 5 enrichment pass that joins this historical data to earnings observations, computing derived features (revenue growth, margin trends, CF quality, insider activity, 8-K counts, macro regime). Phases are: 1a (prices), 1b (financials), 2 (EDGAR), 3 (macro), 5 (enrich earnings), 6 (news).

## B) CLASSES AND METHODS

### Class: `HistoricalCollector`

**`__init__(self, config: dict, db_path: str = "output/historical_db.db")`**
- Inputs: config dict, optional DB path
- Instantiates `HistoricalDB`, a requests.Session with EDGAR headers, and a CIK cache dict.
- No DB writes.

**`collect_price_history(self, tickers: List[str], start: str = "2010-01-01", delisted: bool = False) -> Dict[str, int]`**
- Inputs: list of tickers, start date, delisted flag
- Outputs: dict `{ticker: rows_upserted}`
- Calls `yf.download()` in batches of 20. Normalises OHLCV. Calls `self.db.upsert_prices()`.
- DB writes: `historical_db.db` — table `price_history` (via `HistoricalDB.upsert_prices`)

**`collect_financials(self, tickers: List[str], start: str = "2010-01-01") -> Dict[str, int]`**
- Inputs: list of tickers, start date
- Outputs: dict `{ticker: rows_upserted}`
- Calls `_collect_ticker_financials()` per ticker with 0.3s sleep between.

**`_collect_ticker_financials(self, ticker: str) -> int`**
- Inputs: ticker string
- Outputs: total rows inserted across all statement types
- Fetches quarterly + annual income statement, balance sheet, cash flow via yfinance.
- Computes derived metrics inline (see Mathematics section).
- DB writes: `historical_db.db` — tables `quarterly_financials`, `balance_sheet`, `cash_flow` (via `HistoricalDB` upsert methods)

**`collect_edgar(self, tickers: List[str], start: str = "2010-01-01", forms: List[str] = None) -> Dict[str, int]`**
- Default forms: `["8-K", "10-K", "10-Q", "4", "DEF 14A", "SC 13G", "SC 13G/A"]`
- Calls `_collect_edgar_ticker()` per ticker with `EDGAR_RATE * 3` sleep.
- DB writes: `historical_db.db` — tables `edgar_filings`, `insider_transactions`, `institutional_ownership`

**`_get_cik(self, ticker: str) -> Optional[str]`**
- Two-step CIK resolution: EDGAR browse atom feed, then `company_tickers.json` fallback.
- Caches result in `self._cik_cache`.

**`_collect_edgar_ticker(self, ticker: str, start: str, forms: List[str]) -> int`**
- Fetches `https://data.sec.gov/submissions/CIK{padded}.json`, iterates recent filings.
- For Form 4 filings (up to 500): calls `_parse_form4()`.
- Calls `_collect_institutional()` for ownership data.

**`_parse_form4(self, ticker, cik, acn_clean, pdoc, filed_date, now) -> List[dict]`**
- Fetches Form 4 XML from EDGAR. Parses `<nonDerivativeTransaction>` elements.
- Returns list of transaction dicts with: ticker, cik, reporter_name, reporter_title, transaction_date, transaction_type, shares, price_per_share, total_value, shares_owned_after, form_type, accession_number, collected_at.
- Note: early return (`return records`) if URL doesn't end in `.xml` — the partial fallback URL is constructed but never fetched.

**`_collect_institutional(self, ticker: str, cik: str, now: str) -> None`**
- Fetches `t.institutional_holders` via yfinance. Computes ownership_pct = total_shares / float_shares.
- DB writes: `historical_db.db` — table `institutional_ownership`

**`collect_macro(self, start: str = "2010-01-01") -> int`**
- Downloads all `MACRO_SYMBOLS` via `yf.download()`. Extracts Close column. Computes derived yield spread.
- DB writes: `historical_db.db` — table `macro_context`

**`enrich_earnings_observations(self, tickers: List[str] = None) -> int`**
- Inputs: optional ticker filter
- Loads `EarningsDB` (imports `data.earnings_db`). Fetches all observations, calls `_build_enriched_record()` per row.
- DB writes: `historical_db.db` — table `earnings_enriched` (via `db.upsert_enriched`)

**`_build_enriched_record(self, ticker, earnings_date, obs, now) -> Optional[dict]`**
- Joins data from: `quarterly_financials`, `cash_flow`, `balance_sheet`, `insider_transactions`, `edgar_filings`, `macro_context`, sector ETF prices, `news` table.
- Returns enriched dict with 22 derived fields.

**`collect_news(self, tickers: List[str], days_back: int = 365*5) -> Dict[str, int]`**
- Calls `_collect_ticker_news()` per ticker with 1.0s sleep.
- DB writes: `historical_db.db` — table `news` (via `db.upsert_news`)

**`_collect_ticker_news(self, ticker: str, days_back: int) -> int`**
- Google News RSS (`https://news.google.com/rss/search?q={ticker}+earnings+stock`). Up to 50 items per ticker.
- Also pulls stored EDGAR 8-K records from DB as `edgar_8k` source items.
- Computes `_simple_sentiment()` on each headline.

**`collect_all(self, tickers, start, include_macro, include_edgar, include_news) -> dict`**
- Orchestrates all phases in sequence. Returns summary count dict.

### Standalone Functions

**`_safe_float(v) -> Optional[float]`** — converts to float, rejects NaN/Inf, rounds to 6 decimal places.

**`_xml_text(root, path) -> Optional[str]`** — extracts text from XML element.

**`_simple_sentiment(text: str) -> float`** — naive keyword scoring (see Mathematics).

## C) MATHEMATICS

**Revenue Growth (quarter-over-quarter):**
```
rev_growth_1q = (rev_curr - rev_prev1) / abs(rev_prev1)
```

**Revenue Growth (year-over-year, 4 quarters):**
```
rev_growth_4q = (rev_curr - rev_prev4) / abs(rev_prev4)
```

**Revenue Acceleration:**
```
prev_growth = (fin_rows[1]["revenue"] - fin_rows[2]["revenue"]) / abs(fin_rows[2]["revenue"])
rev_accel = rev_growth_1q - prev_growth
```

**Gross Margin:**
```
gm = gross_profit / revenue
```

**Operating Margin:**
```
om = operating_income / revenue
```

**Net Margin:**
```
nm = net_income / revenue
```

**CF Quality Ratio:**
```
cf_ratio = operating_cash_flow / abs(net_income)
```

**Net Debt:**
```
net_debt = total_debt - cash_and_equiv
```

**Current Ratio:**
```
current_ratio = current_assets / current_liabilities
```

**Debt-to-Equity:**
```
debt_to_equity = total_debt / stockholders_equity
```

**Book Value Per Share:**
```
bvps = total_equity / shares_issued
```

**Free Cash Flow:**
```
fcf = operating_cash_flow + capex  # capex is typically negative in source data
```
(Falls back to `operating_cash_flow` alone if capex is None.)

**Ownership Percentage:**
```
own_pct = total_shares_held / float_shares
```

**Yield Spread (macro derived):**
```
t10y2y = tnx - irx   # TNX (10yr) minus IRX (13-week)
```
Note: this is NOT the standard 10Y-2Y spread; it is 10Y minus 13-week T-bill.

**Macro Regime Classification:**
```
if yield_spread < 0 and vix > 25: regime = 4  # RECESSION_RISK
elif vix > 20 and yield_spread < 0: regime = 3  # RISK_OFF
elif vix < 15 and yield_spread > 0.5: regime = 1  # GOLDILOCKS
else: regime = 0  # RISK_ON
```

**Simple Sentiment:**
```
pos_score = count of positive keywords in headline
neg_score = count of negative keywords in headline
sentiment = (pos_score - neg_score) / (pos_score + neg_score)
```
Returns 0.0 if no keywords found.

## D) DATA FLOWS

**Inputs:**
- yfinance API: OHLCV, financials, balance sheet, cash flow, institutional holders
- SEC EDGAR API: `data.sec.gov/submissions/`, `efts.sec.gov` search, Form 4 XML
- Google News RSS
- Internal DBs: `earnings_db` (via EarningsDB import)

**DB reads from `historical_db.db`:**
- `quarterly_financials` (WHERE ticker, period <= date, period_type='quarterly')
- `balance_sheet` (same filter)
- `cash_flow` (same filter)
- `insider_transactions` (WHERE ticker, date range)
- `edgar_filings` (WHERE ticker, form_type, date range)
- `macro_context` (WHERE date, for SPX/ETF returns and VIX/yield)
- `institutional_ownership` (WHERE ticker, period <= date)
- `news` (via `db.get_news()`)

**DB writes to `historical_db.db`:**
- `price_history`: ticker, date, open, high, low, close, adj_close, volume, delisted
- `quarterly_financials`: ticker, period, period_type, revenue, gross_profit, operating_income, net_income, ebitda, eps_basic, eps_diluted, shares_outstanding, gross_margin, operating_margin, net_margin, revenue_growth_yoy, source
- `balance_sheet`: ticker, period, period_type, total_assets, total_liabilities, total_equity, cash_and_equiv, total_debt, net_debt, current_assets, current_liabilities, current_ratio, debt_to_equity, book_value_per_share, source
- `cash_flow`: ticker, period, period_type, operating_cf, investing_cf, financing_cf, capex, free_cash_flow, dividends_paid, buybacks, source
- `edgar_filings`: ticker, cik, form_type, filed_date, period_of_report, accession_number, url, description, collected_at
- `insider_transactions`: ticker, cik, reporter_name, reporter_title, transaction_date, transaction_type, shares, price_per_share, total_value, shares_owned_after, form_type, accession_number, collected_at
- `institutional_ownership`: ticker, period, total_shares_held, institutions_count, ownership_pct, top_holder, top_holder_pct, qoq_change_pct, collected_at
- `macro_context`: date, spx_close, ndx_close, rut_close, ftse_close, vix, tnx, tyx, irx, dxy, gbpusd, eurusd, cl1_oil, gc1_gold, hg1_copper, xlk, xlv, xlf, xly, xlp, xle, xlu, xlb, xli, xlre, xlc, t10y2y, collected_at
- `earnings_enriched`: ticker, earnings_date, revenue_growth_1q, revenue_growth_4q, revenue_acceleration, gross_margin_trend, operating_margin_trend, cf_to_earnings_ratio, current_ratio, debt_to_equity, insider_net_shares, insider_txn_count, eightk_count_30d, eightk_positive_flags, eightk_negative_flags, vix_at_event, yield_spread_at_event, spx_return_20d, sector_return_20d, macro_regime, inst_ownership_change, news_sentiment_30d, news_count_30d, sector_peer_surprise_avg, readthrough_signal, enriched_at
- `news`: ticker, published_date, headline, source, url, sentiment_raw, is_press_release, edgar_form, collected_at

## E) DEPENDENCIES

**Internal Apollo:**
- `data.historical_db` (HistoricalDB class)
- `data.earnings_db` (EarningsDB — imported lazily inside enrich method)

**External:**
- `requests`
- `pandas`
- `numpy`
- `yfinance`
- Standard: `logging`, `time`, `json`, `re`, `xml.etree.ElementTree`, `datetime`, `pathlib`, `typing`

## F) WIRING STATUS

**CONNECTED — primary historical bootstrap path.** `HistoricalCollector.collect_all()` is called during initial data population. The `earnings_enriched` table it populates feeds the ML model training pipeline. The `macro_context` table is read directly by signal generators. The `price_history` table is read by scoring/ranking logic.

## G) ISSUES FOUND

1. **Yield spread mislabelled:** `t10y2y = tnx - irx` uses IRX (13-week T-bill) not DGS2 (2-year). The field name implies 10Y-2Y spread, but it is actually 10Y-13W. This is silently wrong and will differ significantly from the true 2-year yield spread.

2. **Form 4 XML parsing silent bail-out:** In `_parse_form4`, when `pdoc` does not end in `.xml`, the code constructs a broken fallback URL (`/data/{cik}/{acn}/0001`) and immediately `return records` (empty). No XML is fetched. This means non-XML primary documents silently produce zero insider transactions for that filing.

3. **Revenue growth YoY in financials is sequential, not year-over-year:** `revenue_growth_yoy` in `_collect_ticker_financials` is computed as `(rev - prev_rev) / abs(prev_rev)` where `prev_rev` is the previous column in sorted order — which could be adjacent quarter, not the same quarter prior year. The column name is therefore misleading.

4. **`sector_peer_surprise_avg` and `readthrough_signal` always None:** Both fields in `earnings_enriched` are hardcoded to `None` with a comment "filled by Phase 7" — but Phase 7 does not exist in this file. These fields are permanently null in all enriched records.

5. **`eightk_positive_flags` and `eightk_negative_flags` always 0:** Both are hardcoded to 0 in `_build_enriched_record` with comment "would need NLP". No NLP is applied.

6. **`_etf_return_nd` uses dynamic column name in SQL:** The method constructs `f"SELECT {col}..."` from user-supplied `etf_col` — a potential SQL injection vector if the ticker string is ever externally influenced.

7. **Macro symbol `DX-Y.NYB` for DXY:** This Yahoo Finance symbol for the US Dollar Index has had intermittent availability. If it returns empty, it is silently skipped with no fallback.

8. **`collect_all` skips Phase 4:** Phases are 1a, 1b, 2, 3, 5, 6. Phase 4 is absent entirely with no comment.

---

---

# COLLECTOR 1: shipping_intelligence.py

**Collector name:** ShippingIntelligence
**File:** `/home/dannyelticala/quant-fund/data/collectors/shipping_intelligence.py`
**Step label:** STEP 3

## A) PURPOSE

Collects Baltic Dry Index (BDI) and shipping stock prices to build the ShippingStressIndex (SSI), a composite z-score indicator. SSI feeds sector-level signal modifiers (retailers, air freight, shipping, etc.) into the broader pipeline.

## B) CLASSES AND METHODS

### Class: `BDIFetcher`

**`fetch(self) -> Tuple[pd.DataFrame, str]`**
- Returns `(df[date, value], source_string)` where source is `"BDI"` or `"PROXY_BDRY"`.
- Calls `_fetch_stooq()` first; falls back to `_fetch_bdry_proxy()` if stooq fails or returns a block page.

**`_fetch_stooq(self) -> Optional[pd.DataFrame]`**
- GET `https://stooq.com/q/d/l/?s=bdi&i=d` with 30s timeout. Parses CSV.
- Detects stooq block page by checking for "write to www@stooq.com" in response text.
- Returns `df[date, value]` or None.

**`_fetch_bdry_proxy(self) -> Optional[pd.DataFrame]`**
- Fetches BDRY ETF via yfinance (start=2000-01-01). Returns `df[date, value]` or None.

### Class: `ShippingStockFetcher`

**`fetch_all(self) -> Dict[str, pd.DataFrame]`**
- Iterates over SHIPPING_STOCKS list, calls `_fetch_ticker()` for each.
- Returns dict `{ticker: df[date, open, high, low, close, volume]}`.

**`_fetch_ticker(self, ticker: str) -> Optional[pd.DataFrame]`**
- yfinance `t.history(start="2010-01-01")`. Returns cleaned OHLCV DataFrame.

### Standalone Functions

**`compute_bdi_indicators(df: pd.DataFrame) -> pd.DataFrame`**
- Inputs: df with `[date, value]`
- Outputs: enriched df with ma5, ma20, ma60, zscore_252, pct_rank, roc_1w, roc_4w

**`compute_stock_composite(stock_data: Dict[str, pd.DataFrame]) -> pd.DataFrame`**
- Inputs: dict of ticker -> OHLCV frames
- Outputs: df with `[date, composite_zscore, n_stocks]`
- Equal-weight average of rolling 252-day z-scores across all available shipping stocks.

**`compute_ssi(bdi_df, stock_composite_df) -> pd.DataFrame`**
- Returns merged df with ssi and stress_regime columns.

**`_classify_stress(ssi: float) -> str`**
- Returns `"HIGH"`, `"LOW"`, or `"NEUTRAL"` based on SSI thresholds.

### Class: `ShippingIntelligence`

**`__init__(self, archive_db_path, hist_db_path, config)`**
- Initialises paths and cached state variables.

**`collect(self, market=None, **kwargs) -> Dict[str, Any]`**
- Full collection: BDI fetch → indicators → archive persist; stocks → composite; SSI → hist_db persist.
- Returns summary dict.

**`get_current_stress(self) -> Optional[float]`**
- Returns latest SSI from in-memory `_ssi_df`, or falls back to DB query.

**`get_sector_impacts(self, ssi=None) -> Dict[str, float]`**
- Returns sector modifier dict based on current or provided SSI.

**`get_historical_bdi(self, days=252) -> pd.DataFrame`**
- Returns BDI history from in-memory cache or DB fallback.

**`_load_latest_ssi_from_db(self) -> Optional[float]`**
- DB read: `historical_db.db` → `shipping_data` — most recent non-null `shipping_stress_index`.

**`_load_bdi_from_db(self, days) -> pd.DataFrame`**
- DB read: `historical_db.db` → `shipping_data` — last N rows.

### Module-level convenience functions

**`get_current_shipping_stress() -> Optional[float]`** — instantiates ShippingIntelligence, calls `_load_latest_ssi_from_db()`.

**`get_shipping_sector_impacts() -> Dict[str, float]`** — instantiates ShippingIntelligence, calls `get_sector_impacts()`.

## C) MATHEMATICS

**Rolling Z-Score (window=252):**
```
mean = series.rolling(window, min_periods=window//2).mean()
std  = series.rolling(window, min_periods=window//2).std()
zscore = (series - mean) / std   # std replaced with NaN where 0
```

**Percentile Rank (expanding):**
```
pct_rank = series.expanding().apply(lambda x: rank(x, pct=True).iloc[-1] * 100)
```

**BDI Moving Averages:**
```
ma5  = v.rolling(5,  min_periods=2).mean()
ma20 = v.rolling(20, min_periods=5).mean()
ma60 = v.rolling(60, min_periods=15).mean()
```

**Rate of Change:**
```
roc_1w = v.pct_change(5)   # ~1 trading week
roc_4w = v.pct_change(20)  # ~4 trading weeks
```

**ShippingStressIndex (SSI) — weight redistribution when container data absent:**
```
total_available_weight = 0.30 + 0.20  = 0.50
w_bdi   = 0.30 / 0.50 = 0.60
w_stock = 0.20 / 0.50 = 0.40

SSI = (bdi_zscore * w_bdi + composite_zscore * w_stock) / (w_bdi + w_stock)
    = (bdi_zscore * 0.60 + composite_zscore * 0.40)
```
If only one component is available, weight renormalises to 1.0 for that component.

**Stress Regime Classification:**
```
SSI > 1.5  → "HIGH"
SSI < -1.5 → "LOW"
else       → "NEUTRAL"
```

**Stock Composite:**
```
composite_zscore = equal_weight_mean(zscore_252 across all available tickers)
```

## D) DATA FLOWS

**External inputs:**
- stooq.com CSV (GET `https://stooq.com/q/d/l/?s=bdi&i=d`)
- yfinance: BDRY (fallback), BDRY/ZIM/MATX/SBLK/EGLE/DSX/NMM/GNK/SB (stocks)

**DB writes to `permanent_archive.db`:**
- `raw_shipping_data` (via `PermanentArchive.insert_shipping`): index_name="BDI", date, value, source
- `raw_commodity_prices` (via `PermanentArchive.insert_commodity`): commodity="SHIPPING_STOCK", symbol, date, open, high, low, close, volume, source="yfinance"

**DB writes to `historical_db.db`:**
- `shipping_data`: id, date (UNIQUE), bdi_value, bdi_ma5, bdi_ma20, bdi_ma60, bdi_zscore_252, bdi_pct_rank, bdi_roc_1w, bdi_roc_4w, bdi_source, stock_composite_zscore, shipping_stress_index, stress_regime, fetched_at

**DB reads from `historical_db.db`:**
- `shipping_data`: latest SSI (fallback when in-memory not populated)

## E) DEPENDENCIES

**Internal Apollo:**
- `output.setup_permanent_archive` (PermanentArchive, DEFAULT_DB_PATH)

**External:**
- `requests`
- `numpy`
- `pandas`
- `yfinance`
- `yaml`
- Standard: `io`, `logging`, `os`, `sqlite3`, `sys`, `warnings`, `datetime`, `typing`

## F) WIRING STATUS

**CONNECTED — live trading path.**
- `get_current_shipping_stress()` and `get_shipping_sector_impacts()` are module-level functions exposed for import by signal pipeline.
- `shipping_data` table in `historical_db.db` is actively written and read.
- SSI feeds sector impact modifiers (retailers -0.3, domestic_producers +0.3, air_freight +0.2, shipping +0.3 under HIGH stress).

## G) ISSUES FOUND

1. **stooq.com is currently blocked:** The code itself notes "stooq now returns a 'contact us' block page instead of CSV data." The primary BDI source is permanently non-functional. All runs use the BDRY ETF proxy. BDRY is an ETF, not the actual BDI index — scale and volatility differ.

2. **Container rate component (SSI_WEIGHT_CONTAINER = 0.25) is always absent:** The SSI was designed with three components but the container rate source was never implemented. The weight redistribution code runs every time, meaning the SSI formula effectively always operates with only 2 components. The constant `SSI_WEIGHT_CONTAINER = 0.25` is defined but never used.

3. **GOGL and PNTM removed from stock list:** Comments state GOGL was delisted from US exchange and PNTM was wound down. These were removed from the list, which is correct. However, the `SHIPPING_STOCKS` constant still reflects 9 tickers, meaning the historical composite has a break in composition.

4. **SSI computation iterates row-by-row with Python for loop:** `compute_ssi()` uses `merged.iterrows()` for the SSI calculation. This is unnecessary — pandas vectorised operations would be appropriate. Not a logic bug, but a performance issue for long history runs.

5. **`_persist_ssi_to_hist_db` iterates over SSI rows but BDI indicators may not align:** The function uses `ssi_idx.iterrows()` and lookups `bdi_idx.loc[date]`. If the SSI date set includes stock-only dates (from outer merge), `bdi_row` will be an empty Series, and all BDI columns will be None. This is handled gracefully but produces sparse BDI rows.

---

---

# COLLECTOR 2: consumer_intelligence.py

**Collector name:** ConsumerIntelligence
**File:** `/home/dannyelticala/quant-fund/data/collectors/consumer_intelligence.py`
**Step label:** STEP 6

## A) PURPOSE

Fetches macroeconomic consumer data from FRED (28 series covering sentiment, spending, savings, employment, housing, inflation) and payment processor proxy signals from yfinance (V, MA, AXP). Builds three composite indices: ConsumerHealthIndex, HousingHealthIndex, and InflationPressure. Stores all data permanently to both permanent_archive.db and historical_db.db.

## B) CLASSES AND METHODS

### Standalone functions

**`_load_config() -> Dict[str, Any]`** — loads `config/settings.yaml`.

**`_get_perm_conn() -> sqlite3.Connection`** — connects to `permanent_archive.db` with WAL mode.

**`_get_hist_conn() -> sqlite3.Connection`** — connects to `historical_db.db` with WAL mode.

**`_init_perm_db(conn) -> None`** — creates `raw_macro_data` table if not exists.

**`_init_hist_db(conn) -> None`** — creates `macro_series` and `payment_processor_signals` tables.

**`_fetch_fred_series(series_id: str, api_key: str) -> List[Dict]`**
- GET `https://api.stlouisfed.org/fred/series/observations` with start=1900-01-01, limit=100000.
- Returns list of observation dicts.

**`_store_fred_observations(perm_conn, hist_conn, series_id, series_name, observations) -> None`**
- Parses observations, skips "." values. Upserts to both DBs.
- DB writes: `permanent_archive.db` → `raw_macro_data`; `historical_db.db` → `macro_series`

**`_get_latest_value(conn, series_id, table) -> Optional[float]`** — DB read, latest value for series.

**`_get_series_history(conn, series_id, table, limit=500) -> List[Tuple]`** — DB read, ascending (date, value) tuples.

**`_zscore(values, window=60) -> Optional[float]`** — z-score of most recent value against rolling window.

**`_trend(values, periods=4) -> str`** — returns "UP", "DOWN", "FLAT", "INSUFFICIENT_DATA".

**`_safe_clamp(value, lo=-1.0, hi=1.0) -> float`** — clamps to [-1, 1], returns 0.0 if None.

**`_fetch_payment_signals(hist_conn) -> Optional[float]`**
- Fetches V, MA, AXP via yfinance (start=2010-01-01). Computes 30-day return at each date.
- DB writes: `historical_db.db` → `payment_processor_signals` (ticker, date, close, return_30d)
- Updates `composite` column on latest row for each ticker.
- Returns equal-weight composite of latest 30-day returns.

### Class: `ConsumerIntelligence`

**`__init__(self, config=None)`**
- Loads config, extracts FRED API key from `api_keys.fred`.
- Initialises `_series_cache` and `_latest_cache` dicts.

**`_load_series(self, series_id: str) -> List[float]`**
- Returns list of float values from in-memory cache.

**`_load_cache_from_db(self, perm_conn) -> None`**
- Queries `raw_macro_data` for all configured FRED series. Populates `_series_cache` and `_latest_cache`.

**`collect(self, market=None, **kwargs) -> Dict[str, Any]`**
- Fetches all 28 FRED series, then payment processor signals. Populates cache. Computes and returns all three indices.
- Returns dict with: series_collected, series_failed, errors, payment_composite_30d, consumer_health_index, housing_health_index, inflation_pressure.

**`get_consumer_health_index(self) -> float`**
- Weighted z-score composite. Returns float in [-1, 1].

**`get_housing_health_index(self) -> float`**
- Weighted z-score composite. Returns float in [-1, 1].

**`get_inflation_pressure(self) -> float`**
- Z-score of CPI vs PPI spread. Positive = elevated inflation.

**`get_latest_values(self) -> Dict[str, Optional[float]]`**
- Returns dict of `{series_id: latest_value}` for all configured FRED series.

**`get_trend(self, series_id, periods=4) -> str`**
- Returns trend string for a series. Returns "SERIES_NOT_FOUND" if not in cache.

**`get_payment_composite(self) -> Optional[float]`**
- DB read: `historical_db.db` → `payment_processor_signals` — latest non-null composite.

## C) MATHEMATICS

**Z-Score (window-based, last value):**
```
recent = values[-window:]  # up to last 60 values
mean = sum(recent) / len(recent)
variance = sum((x - mean)**2 for x in recent) / (len(recent) - 1)
std = variance ** 0.5
zscore = (recent[-1] - mean) / std
```
Returns 0.0 if std == 0.

**Trend (last N+1 periods):**
```
slope = values[-1] - values[-(periods+1)]
threshold = 0.01 * abs(values[-(periods+1)])  # 1% threshold
UP if slope > threshold, DOWN if slope < -threshold, else FLAT
```

**ConsumerHealthIndex:**
```
score = (UMCSENT_z * 0.25) + (UNRATE_z * -0.25) + (RSXFS_z * 0.25) + (PSAVERT_z * 0.25)
normalized = clamp(score / 3.0, -1.0, 1.0)
```

**HousingHealthIndex:**
```
score = (HOUST_z * 0.35) + (CSUSHPINSA_z * 0.35) + (MORTGAGE30US_z * -0.30)
normalized = clamp(score / 3.0, -1.0, 1.0)
```

**InflationPressure:**
```
spreads = [cpi_i - ppi_i for aligned pairs]
z = zscore(spreads, window=60)
```

**Payment Processor 30-day return:**
```
ret_30d = (close[i] - close[i-30]) / close[i-30]
```

**Payment Composite:**
```
composite = mean(ret_30d for each of V, MA, AXP)
```

## D) DATA FLOWS

**External inputs:**
- FRED API: 28 series (see FRED_SERIES dict in file)
- yfinance: V, MA, AXP price history

**DB writes to `permanent_archive.db`:**
- `raw_macro_data`: series_id, series_name, date, value, source='fred', collected_at

**DB writes to `historical_db.db`:**
- `macro_series`: series_id, series_name, date, value, source='fred', collected_at
- `payment_processor_signals`: ticker, date, close, return_30d, composite, collected_at

**DB reads:**
- `permanent_archive.db` → `raw_macro_data`: series history for cache population
- `historical_db.db` → `payment_processor_signals`: latest composite

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `requests`
- `yaml`
- `yfinance` (lazy import inside `_fetch_payment_signals`)
- Standard: `json`, `logging`, `sqlite3`, `datetime`, `pathlib`, `typing`

## F) WIRING STATUS

**PARTIAL — data stored, but no confirmed downstream consumer of composite indices in live signal path.**
The three composite indices (ConsumerHealthIndex, HousingHealthIndex, InflationPressure) are computed and returned in `collect()` results but are not persisted to any DB table — they are only in the return dict. If no pipeline step reads the return value and stores it, the computed indices are ephemeral. The raw FRED series data is persisted and could theoretically be read downstream.

## G) ISSUES FOUND

1. **Composite indices are NOT persisted to DB:** `get_consumer_health_index()`, `get_housing_health_index()`, and `get_inflation_pressure()` return floats that are added to the `collect()` return dict only. There is no SQL INSERT for these composite scores. Any downstream module must recompute them by calling the methods again (requiring cache population).

2. **DFS removed from payment tickers:** A comment states "DFS (Discover Financial Services) removed 2026-04-04: delisted from Yahoo Finance." The ticker list was correctly updated to `["V", "MA", "AXP"]`.

3. **`total_weight` accumulates `abs(weight)` but score uses signed weights:** In `get_consumer_health_index()`, `total_weight` counts the sum of `abs(weights)` (= 1.0), but the score is accumulated with signed weights. The normalization `score / 3.0` is not tied to `total_weight` — `total_weight` is computed but never used for normalization. It is dead code.

4. **CPI vs PPI alignment by series length, not by date:** In `get_inflation_pressure()`, the alignment is done by truncating both series to `min(len(cpi), len(ppi))` from the end. Since CPI and PPI have different reporting frequencies and history lengths, this date-mismatch will produce incorrect spread calculations.

5. **FRED has no rate limiting:** The `collect()` method fetches 28 FRED series sequentially with no sleep between requests. FRED allows ~120 requests/minute for free API keys. With 28 requests this is unlikely to trigger throttling, but there is no guard.

6. **UK series included (GBRCPIALLMINMEI, LRHUTTTTGBM156S):** Two UK series are in `FRED_SERIES` but are not included in any composite index (`_CONSUMER_HEALTH_SERIES`, `_HOUSING_HEALTH_SERIES`, `_INFLATION_SERIES`). They are fetched and stored but never used in any output.

---

---

# COLLECTOR 3: rates_credit_collector.py

**Collector name:** RatesCreditCollector
**File:** `/home/dannyelticala/quant-fund/data/collectors/rates_credit_collector.py`
**Step label:** STEP 8

## A) PURPOSE

Fetches interest rate and credit spread data from yfinance (^IRX, ^FVX, ^TNX, ^TYX) and FRED (yield curve series, credit spread series). Computes yield curve signals (slope, inversion depth/duration, 10yr momentum), credit stress (HY spread z-score), and Fed meeting proximity. Stores raw data in permanent_archive.db and computed signals in historical_db.db. Provides position sizing modifiers based on rates regime and Fed calendar.

## B) CLASSES AND METHODS

### Standalone functions

**`_get_conn(db_path: str) -> sqlite3.Connection`** — opens SQLite with WAL mode, 30s timeout.

**`_ensure_raw_macro_table(conn) -> None`** — creates `raw_macro_data` in permanent_archive.db.

**`_ensure_rates_data_table(conn) -> None`** — creates `rates_data` table in historical_db.db.

**`_ensure_rates_signals_table(conn) -> None`** — creates `rates_signals` table in historical_db.db.

**`_fetch_fred_series(series_id, api_key) -> pd.DataFrame`**
- GET FRED API with start=1900-01-01, end=9999-12-31, limit=100000.
- Returns DataFrame with `[date, value]`.

**`_safe_float(val) -> Optional[float]`** — converts to float, returns None for NaN.

### Class: `RatesCreditCollector`

**`__init__(self, config_path, archive_db_path, historical_db_path)`**
- Accepts dict or string path for config. Extracts `fred_api_key` from config.
- Initialises in-memory series caches: `_dgs10`, `_dgs2`, `_hy_spread`, `_ig_spread`, `_ted_spread`, `_t10yie`.

**`collect(self, tickers=None, market=None, **kwargs) -> Dict`**
- Orchestrates 5 steps: yfinance yields, FRED yield series, FRED credit series, load to memory, calculate signals.
- Returns summary dict with row counts.

**`_collect_yfinance_yields(self) -> int`**
- Fetches ^IRX, ^FVX, ^TNX, ^TYX via yfinance (period="max"). Writes to both DBs.
- DB writes: `permanent_archive.db` → `raw_macro_data`; `historical_db.db` → `rates_data`
- 0.5s sleep between tickers.

**`_collect_fred_series(self, series_id, category) -> int`**
- Calls `_fetch_fred_series()`, writes rows to both DBs.
- DB writes: `permanent_archive.db` → `raw_macro_data`; `historical_db.db` → `rates_data`

**`_load_series_into_memory(self) -> None`**
- Reads DGS10, DGS2, BAMLH0A0HYM2, BAMLC0A0CM, TEDRATE, T10YIE from `rates_data` into pandas Series.

**`_calculate_and_store_signals(self) -> int`**
- Requires DGS10 and DGS2. Aligns on common dates. Computes all signals. Stores to `rates_signals`.
- DB writes: `historical_db.db` → `rates_signals`

**`_classify_rates_regime_row(row: pd.Series) -> str`**
- Static method. Returns "CRISIS", "TIGHT", "EASY", or "NEUTRAL".

**`get_yield_curve_status(self) -> Dict`**
- DB read: `historical_db.db` → `rates_signals` — most recent row. Returns structured dict.

**`get_credit_conditions(self) -> Dict`**
- DB read: `historical_db.db` → `rates_signals` — most recent row. Returns credit dict.

**`get_rates_regime(self) -> str`**
- DB read: `historical_db.db` → `rates_signals`. Returns regime string.

**`get_position_sizing_modifier(self) -> float`**
- Combines regime modifier with Fed meeting proximity modifier.

**`get_breakeven_inflation(self) -> Optional[float]`**
- DB read: `historical_db.db` → `rates_signals`, latest non-null `breakeven_inflation`.

**`days_to_next_fed_meeting(self) -> int`**
- Uses hardcoded `FED_MEETING_DATES` list (2024-2027). Returns 999 if no future date found.

**`get_position_size_multiplier(self) -> float`**
- Returns multiplier based on days to next Fed meeting.

## C) MATHEMATICS

**Yield Curve Slope:**
```
slope = DGS10 - DGS2
```

**Inversion Depth:**
```
inversion_depth = abs(min(slope, 0.0))
```

**Inversion Duration:**
```
streak counter: increments when slope < 0, resets when slope >= 0
inversion_duration = consecutive trading days with slope < 0
```

**10yr Yield Momentum:**
```
yield_momentum_10yr = DGS10.diff(20)  # ~4 trading weeks
yields_rising_fast  = 1 if yield_momentum_10yr > 0.5 else 0
```

**Credit Stress Level (HY OAS z-score):**
```
hy_mean = hy_spread.rolling(252, min_periods=60).mean()
hy_std  = hy_spread.rolling(252, min_periods=60).std()
credit_stress_level = (hy_spread - hy_mean) / hy_std
```

**Rates Regime Classification:**
```
if hy_spread > 800: "CRISIS"
elif hy_spread > 500 or credit_stress_z > 2.5: "TIGHT"
elif hy_spread < 300 and slope > 0.5: "EASY"
else: "NEUTRAL"
```

**Combined Position Sizing Modifier:**
```
regime_mod = {CRISIS: 0.2, TIGHT: 0.6, NEUTRAL: 1.0, EASY: 1.1}[regime]
fed_mod    = {0 days: 0.0, <=7 days: 0.75, <=14 days: 0.85, else: 1.0}
modifier   = round(regime_mod * fed_mod, 4)
```

**Inversion Duration in weeks:**
```
inversion_weeks = inversion_duration / 5   # integer division — trading days to weeks
```

## D) DATA FLOWS

**External inputs:**
- yfinance: ^IRX (13W T-bill), ^FVX (5Y), ^TNX (10Y), ^TYX (30Y)
- FRED: DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS7, DGS10, DGS20, DGS30, T10Y2Y, T10Y3M, IRLTLT01GBM156N (UK gilt)
- FRED credit: BAMLH0A0HYM2 (HY OAS), BAMLC0A0CM (IG OAS), TEDRATE (TED), T10YIE (breakeven)

**DB writes to `permanent_archive.db`:**
- `raw_macro_data`: series_name, series_id, date, value, source, fetched_at

**DB writes to `historical_db.db`:**
- `rates_data`: source, series_id, series_name, obs_date, value, fetched_at (UNIQUE on series_id, obs_date)
- `rates_signals`: calc_date (UNIQUE), yield_curve_slope, inversion_depth, inversion_duration, yield_momentum_10yr, yields_rising_fast, credit_stress_level, hy_spread, ig_spread, ted_spread, breakeven_inflation, rates_regime, fetched_at

**DB reads from `historical_db.db`:**
- `rates_data`: DGS10, DGS2, HY, IG, TED, T10YIE series (for signal calculation)
- `rates_signals`: latest row (for status accessors and position sizing)

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `numpy`
- `pandas`
- `requests`
- `yaml`
- `yfinance`
- Standard: `logging`, `sqlite3`, `time`, `datetime`, `pathlib`, `typing`

## F) WIRING STATUS

**CONNECTED — live trading path.**
- `get_position_sizing_modifier()` is explicitly designed for use by position sizing logic.
- `get_rates_regime()` and `get_yield_curve_status()` are callable from signal pipeline.
- Fed meeting calendar hardcoded through 2027 — will need update in 2027.

## G) ISSUES FOUND

1. **FRED rate limiting: 0.25s sleep between requests.** FRED free API limit is 120 requests/minute (~0.5s/request safe minimum). With 17 total series (13 yield + 4 credit), 0.25s sleep = 4.25s total wait, which should be within limits but is tight. No retry logic.

2. **`_collect_yfinance_yields` uses `INSERT OR REPLACE` to `rates_data`:** This is inconsistent with `INSERT OR IGNORE` used for `raw_macro_data` in `archive_conn`. A re-run will silently overwrite rates_data rows (deleting old fetched_at) but keep the oldest archive rows unchanged.

3. **`raw_macro_data` schema conflict with consumer_intelligence.py:** The `rates_credit_collector` creates `raw_macro_data` with schema `(series_name, series_id, date, value, source, fetched_at)` and NO UNIQUE constraint. The `consumer_intelligence.py` creates the same table with `UNIQUE(series_id, date)`. Whichever runs first will define the schema. If `consumer_intelligence` runs first and creates the UNIQUE constraint, then `rates_credit_collector`'s `INSERT OR IGNORE` will silently skip duplicate dates; if rates runs first without UNIQUE, consumer's `INSERT OR IGNORE` will fail with "no such constraint." This is a DB schema collision.

4. **`days_to_next_fed_meeting()` returns 999 after 2027-12-15.** No meetings are defined beyond that date. After that point, `get_position_size_multiplier()` always returns 1.0 (the "else" branch), which is acceptable but silent.

5. **`inversion_weeks = int(inversion_duration / 5)` truncates to 0 for short inversions.** Inversions of 1-4 consecutive trading days appear as 0 weeks in `get_yield_curve_status()`.

6. **`_load_series_into_memory()` reads `rates_data` by `series_id` but FRED data has `series_id = series_id` while yfinance data has `series_id = symbol` (e.g., "^TNX").** The memory load only targets DGS10, DGS2, BAMLH0A0HYM2, etc. — FRED names. yfinance data stored under "^TNX" is never loaded into memory for signal calculation, even though yfinance data IS stored.

---

---

# COLLECTOR 4: commodity_collector.py

**Collector name:** CommodityCollector
**File:** `/home/dannyelticala/quant-fund/data/collectors/commodity_collector.py`
**Step label:** STEP 7

## A) PURPOSE

Collects comprehensive OHLCV history for 24 commodity symbols (energy futures, metal futures, agricultural futures, and commodity ETFs) via yfinance back to 1983. Computes moving averages, z-scores, rates of change, 52-week position, cross-commodity correlations, and lead-lag analysis. Generates sector impact modifiers based on commodity moves.

## B) CLASSES AND METHODS

### Standalone functions

**`_load_config() -> Dict`** — loads settings.yaml.

**`_get_perm_conn() / _get_hist_conn()`** — open respective DB connections.

**`_init_dbs(perm_conn, hist_conn) -> None`** — creates `raw_commodity_prices`, `commodity_lead_lag` (permanent) and `commodity_prices` (historical).

**`_mean(vals) / _std(vals)`** — pure Python statistics helpers. `_std` uses sample variance (n-1).

**`_zscore_last(vals, window=252) -> Optional[float]`** — z-score of last value against rolling window.

**`_rate_of_change(vals, periods) -> Optional[float]`**
- `(vals[-1] - vals[-(periods+1)]) / vals[-(periods+1)]`

**`_rolling_ma(vals, window) -> Optional[float]`** — mean of last N values.

**`_pct_position_52w(vals) -> Optional[float]`** — where latest close sits within 252-day range.

**`_pearson_correlation(x, y) -> Tuple[float, int]`**
- Pure Python implementation. Returns (r, sample_size).

**`_approximate_p_value(r, n) -> float`** — lookup table approximation of two-tailed p-value.

**`_fetch_and_store_symbol(symbol, name, perm_conn, hist_conn) -> List[Dict]`**
- yfinance `tkr.history(start="1983-01-01", auto_adjust=True)`.
- Stores to `raw_commodity_prices` (permanent) and `commodity_prices` (historical).
- Returns list of price dicts.

**`_safe_float(val) -> Optional[float]`** — rejects NaN, returns None.

**`_compute_indicators(closes) -> Dict`** — computes all technical indicators for a close price series.

**`_align_series(a, b) -> Tuple[List, List]`** — aligns two price series by date.

**`_run_lead_lag(closes_commodity, closes_sector, commodity, sector, perm_conn) -> List[Dict]`**
- Tests 5 lag windows (2, 4, 6, 8, 12 weeks). Returns correlation, p-value, significance.
- DB writes: `permanent_archive.db` → `commodity_lead_lag`

**`_group_avg_return` (module-level, defined in technology_intelligence.py but analogous):** N/A — in this file the return calculation is inline.

### Class: `CommodityCollector`

**`__init__(self, config=None)`** — initialises price_cache, indicators_cache, lead_lag_results.

**`_closes(self, symbol) -> List[float]`** — returns close price list from cache.

**`_load_cache_from_db(self, conn) -> None`**
- Reads `raw_commodity_prices` for all symbols. Populates price_cache and computes indicators_cache.

**`collect(self, market=None, **kwargs) -> Dict[str, Any]`**
- Fetches all 24 symbols, computes indicators, runs lead-lag analysis, computes 90-day correlations, sector impacts, energy/metals composites.
- Returns summary dict.

**`_compute_90d_correlations(self) -> Dict[str, Dict[str, float]]`**
- Correlates each symbol against XLE, XME, MOO benchmarks over last 90 closes.

**`get_commodity_data(self, symbol, days=252) -> Optional[Any]`**
- Returns DataFrame (or dict) with price data and per-row rolling MAs. Falls back to dict if pandas unavailable.

**`get_sector_impacts(self) -> Dict[str, float]`**
- Applies IMPACT_RULES. Returns `{sector: multiplier}` where 0 impact → 1.0x.

**`get_commodity_signals(self) -> List[Dict]`**
- For each symbol: computes z-score and 1m ROC. Classifies BULLISH (z>1.5), BEARISH (z<-1.5), NEUTRAL.

**`get_energy_composite(self) -> float`**
- Equal-weight z-score of energy futures only (5 symbols).

**`get_metals_composite(self) -> float`**
- Equal-weight z-score of metal futures only (5 symbols).

**`get_lead_lag_results(self) -> List[Dict]`** — returns stored lead-lag results.

## C) MATHEMATICS

**Z-Score (last value vs window):**
```
window_vals = closes[-252:]  # if len >= 252 else all
s = std(window_vals)  # sample std
zscore = (window_vals[-1] - mean(window_vals)) / s
```
Returns 0.0 if std == 0.

**Rate of Change:**
```
roc = (closes[-1] - closes[-(periods+1)]) / closes[-(periods+1)]
```
`roc_1w` = periods=5; `roc_4w` = periods=21; `roc_12w` = periods=63.

**52-Week Position:**
```
lo, hi = min(closes[-252:]), max(closes[-252:])
position = (closes[-1] - lo) / (hi - lo)
```
Returns 0.5 if hi == lo.

**Pearson Correlation (pure Python):**
```
num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
den = std(x) * std(y) * (n - 1)
r   = num / den
```

**Approximate P-value (t-statistic lookup):**
```
t = r * sqrt((n-2) / (1 - r^2 + 1e-12))
p = lookup(|t|): >3.5→0.001, >2.6→0.01, >2.0→0.05, >1.6→0.10, else→0.50
```

**Lead-lag (uses returns, not prices):**
```
x = commodity_closes[:-lag_days]   # commodity leads
y = sector_closes[lag_days:]        # sector follows
x_ret = [x[i]/x[i-1] - 1 for i...]
y_ret = [y[i]/y[i-1] - 1 for i...]
r, n  = pearson_correlation(x_ret, y_ret)
significant = 1 if p < 0.05 else 0
```

**Sector Impact Modifier:**
```
roc_1m = rate_of_change(closes, 21)
if abs(roc_1m) >= threshold:
    sign  = 1 if roc_1m > 0 else -1
    scale = min(abs(roc_1m) / threshold, 2.0)  # cap at 2x
    effective = modifier * sign * scale
multiplier = 1.0 + impact  # e.g. impact=-0.4 → multiplier=0.6
```

**IMPACT_RULES thresholds:**
- CL=F (WTI): 15% 1-month move → airlines -0.40, trucking -0.30, plastics -0.20, energy_producers +0.40
- HG=F (Copper): 10% 1-month move → industrials +0.20, electronics -0.10, mining +0.30
- ZW=F (Wheat): 20% 1-month move → food_manufacturers -0.30, restaurants -0.20, agriculture +0.20

## D) DATA FLOWS

**External inputs:**
- yfinance: 24 symbols back to 1983-01-01 (energy futures, metal futures, agri futures, commodity ETFs)

**DB writes to `permanent_archive.db`:**
- `raw_commodity_prices`: commodity (=name), symbol, date, open, high, low, close, volume, source='yfinance', fetched_at (UNIQUE on symbol, date; also has name, adj_close columns in this table vs alternative schema)
- `commodity_lead_lag`: commodity, sector, lag_weeks, correlation, p_value, significant, sample_size, computed_at (UNIQUE on commodity, sector, lag_weeks)

**DB writes to `historical_db.db`:**
- `commodity_prices`: symbol, name, date, open, high, low, close, adj_close, volume, source, collected_at (UNIQUE on symbol, date)

**DB reads:**
- `permanent_archive.db` → `raw_commodity_prices`: all columns for cache population (in `_load_cache_from_db`)

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `yfinance` (lazy import inside `_fetch_and_store_symbol` and `get_commodity_data`)
- `yaml`
- Standard: `json`, `logging`, `sqlite3`, `datetime`, `pathlib`, `math` (inside `_approximate_p_value`), `typing`
- pandas (optional — `get_commodity_data` tries to import, falls back to dict)

## F) WIRING STATUS

**PARTIAL — data stored, sector impacts computed, but upstream consumption unclear.**
`get_sector_impacts()` returns multipliers for airline, trucking, energy producer, food_manufacturer, restaurant, agriculture sectors. These multipliers are designed to modify signals, but no confirmed upstream call to `CommodityCollector.get_sector_impacts()` was found in this analysis. The lead-lag and 90-day correlation results are stored/returned but not clearly consumed downstream.

## G) ISSUES FOUND

1. **`raw_commodity_prices` schema conflict:** `commodity_collector.py` inserts with columns `(commodity, symbol, date, open, high, low, close, volume, source, fetched_at)` — 10 columns. But `shipping_intelligence.py` calls `archive.insert_commodity(commodity=..., symbol=..., ...)` through `PermanentArchive`. The `_init_dbs` in this file creates `raw_commodity_prices` with columns including `name`, `adj_close` (11 columns + `id`). If commodity_collector runs first, the table has one schema; if shipping_intelligence's PermanentArchive creates it first, it may have a different schema. There is a schema collision risk.

2. **`adj_close` is set to `Close` value:** Because yfinance is called with `auto_adjust=True`, the Close column is already adjusted. Storing it twice as both `close` and `adj_close` is redundant. Not a logic bug, but misleading.

3. **`_compute_90d_correlations` uses raw prices, not returns:** Price-level Pearson correlation between two different-scale price series is spurious. Returns should be used for correlation, not raw prices. This is noted as a bug — the lead-lag function correctly uses returns, but the 90-day correlation uses raw prices.

4. **`_run_lead_lag` computes returns from price series but the series alignment happens before the returns calculation.** If `x` and `y` from the lag shift are not date-aligned (they come from raw closes lists with no shared date index), the lag calculation may be spurious if there are gaps in one series.

5. **`_load_cache_from_db` reads from `raw_commodity_prices` but that table's column `adj_close` may not exist** if shipping_intelligence created the table first without that column. This would cause a sqlite3 OperationalError at cache load.

6. **No rate limiting between yfinance fetches.** 24 symbols fetched sequentially with no sleep. yfinance may throttle or return empty results for later symbols in the batch.

---

---

# COLLECTOR 5: technology_intelligence.py

**Collector name:** TechnologyIntelligence
**File:** `/home/dannyelticala/quant-fund/data/collectors/technology_intelligence.py`

## A) PURPOSE

Collects technology sector thematic data across five sub-domains: data centre REIT performance, semiconductor cycle tracking, EV adoption signals, FDA PDUFA calendar, and a technology knowledge graph (ticker-to-theme mapping). All data stored to `permanent_archive.db` only. No historical_db.db writes.

## B) CLASSES AND METHODS

### Standalone functions

**`_ensure_tech_tables(conn) -> None`** — creates `tech_intelligence`, `fda_calendar`, `tech_knowledge_graph` tables in `permanent_archive.db`.

**`_store_rows(conn, table, rows) -> int`** — generic INSERT OR IGNORE via DataFrame column inference.

**`_fetch_prices(tickers, period="1y") -> pd.DataFrame`** — yfinance batch download. Returns Close price DataFrame.

**`check_for_delisted(ticker_list, label) -> List[str]`** — checks each ticker's fast_info.market_cap; logs WARNING if exception raised. Returns problem ticker list.

### Class: `DataCentreIntelligence`

**`collect(self) -> Dict`**
- Tickers: EQIX, DLR, AMT, COR (REITs); VRT, SMCI, IREN (infra); NEE, AES, D, SO (power)
- Fetches 6-month prices via `_fetch_prices`. Computes 1m and 3m returns for each ticker.
- Computes REIT basket vs SPY relative 6m return.
- Returns result dict with metrics list.

**`store(self, conn, result) -> int`** — inserts metrics into `tech_intelligence`.

### Class: `SemiconductorCycleTracker`

**`collect(self) -> Dict`**
- Tickers: SOXX, SMH (proxy); LRCX, AMAT, KLAC, ASML (equipment); MU, WDC (memory); NVDA, AMD, INTC, TSM, QCOM (integrated)
- Fetches 1-year prices. Computes 3m and 6m returns per ticker. Computes equipment vs integrated relative strength.
- Derives cycle_phase: EXPANSION (avg>10), RECOVERY (avg>0), CONTRACTION.

**`store(self, conn, result) -> int`** — inserts to `tech_intelligence`.

### Class: `EVAdoptionTracker`

**`collect(self) -> Dict`**
- Tickers: TSLA/RIVN/LCID/NIO/LI/XPEV (EV makers); ALB/LAC/SQM (battery materials); CHPT/EVGO/BLNK/AMRC (charging); F/GM/STLA/HMC/TM (legacy OEM)
- Fetches 1-year prices. Computes 3m returns per group.
- Computes adoption_score = ev_3m_return * 0.6 + battery_3m_return * 0.4.

**`store(self, conn, result) -> int`** — inserts to `tech_intelligence`.

### Class: `FDACalendarTracker`

**`collect(self) -> Dict`**
- Primary: GET `https://www.biopharmacat.com/api/catalysts?type=pdufa&days_ahead=90`
- Fallback: yfinance prices for MRNA, BNTX, VRTX, REGN, BIIB, GILD, ABBV, BMY, PFE, LLY, AMGN
- Returns events list.

**`store(self, conn, result) -> int`** — inserts to `fda_calendar`.

### Class: `TechKnowledgeGraph`

**`build(self) -> Dict`**
- Iterates THEME_MAP (10 themes, ~50 unique tickers). Computes weight = 1 / number_of_themes_for_ticker.
- Returns nodes list.

**`get_themes_for_ticker(self, ticker) -> List[str]`** — returns theme list for a ticker.

**`get_tickers_for_theme(self, theme) -> List[str]`** — returns ticker list for a theme.

**`store(self, conn, result) -> int`** — INSERT OR REPLACE into `tech_knowledge_graph`.

### Class: `TechnologyIntelligence`

**`__init__(self, config=None)`** — instantiates all 5 sub-collectors.

**`collect_all(self) -> Dict`**
- Opens `permanent_archive.db`. Runs all 5 sub-collectors sequentially. Stores results.
- Returns summary dict with per-sub-collector row counts, cycle_phase, adoption_score, n_events.

**`summary(self) -> str`**
- Calls `collect_all()` and formats a text summary. NOTE: this triggers a full collection run.

## C) MATHEMATICS

**1-month return (DataCentreIntelligence):**
```
ret_1m = (s.iloc[-1] / s.iloc[max(0, len(s)-21)] - 1) * 100
```

**3-month return (all sub-collectors):**
```
ret_3m = (s.iloc[-1] / s.iloc[max(0, len(s)-63)] - 1) * 100
```

**REIT vs SPY relative return:**
```
reit_basket_start = reit_prices.iloc[0].mean()
reit_basket_end   = reit_prices.iloc[-1].mean()
reit_spy = reit_basket_end / reit_basket_start
spy_ret  = spy["SPY"].iloc[-1] / spy["SPY"].iloc[0]
relative = (reit_spy / spy_ret - 1) * 100
```

**Semiconductor cycle average:**
```
cycle_avg = sum(group_avg_3m_returns) / count_of_groups
cycle_phase: EXPANSION if cycle_avg > 10, RECOVERY if > 0, CONTRACTION otherwise
```

**Equipment vs integrated (cycle indicator):**
```
equip_vs_integrated = equipment_avg_3m_return - integrated_avg_3m_return
positive = late cycle (equipment outperforming)
```

**EV Adoption Score:**
```
adoption_score = ev_makers_3m_return * 0.6 + battery_material_3m_return * 0.4
STRONG if > 15, MODERATE if > 0, WEAK otherwise
```

**Tech Knowledge Graph weight:**
```
weight = 1.0 / count_of_themes_ticker_belongs_to
```
(More focused tickers have higher weight per theme.)

## D) DATA FLOWS

**External inputs:**
- yfinance: All tech sector tickers (prices, 6mo/1y history)
- BioPharma Catalyst API: `https://www.biopharmacat.com/api/catalysts?type=pdufa&days_ahead=90`

**DB writes to `permanent_archive.db` only:**
- `tech_intelligence`: id, category, subcategory, metric_name, metric_value, metric_text, source, date, fetched_at (NO UNIQUE constraint — duplicates accumulate on each run)
- `fda_calendar`: id, ticker, company_name, drug_name, pdufa_date, indication, source, fetched_at (INSERT OR IGNORE — dedup by all columns)
- `tech_knowledge_graph`: id, ticker, theme, weight, narrative, updated_at (UNIQUE on ticker+theme — upserted)

**No writes to `historical_db.db`.**

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `pandas`
- `requests`
- `yaml`
- `yfinance` (lazy import inside `_fetch_prices`)
- Standard: `json`, `logging`, `sqlite3`, `time`, `dataclasses`, `datetime`, `pathlib`, `typing`

## F) WIRING STATUS

**DATA DEAD END — not connected to live trading signal path.**
All data is stored in `permanent_archive.db` only. No downstream module was found reading `tech_intelligence`, `fda_calendar`, or `tech_knowledge_graph` tables for live signal generation. The knowledge graph and cycle signals appear unused by the trading pipeline. FDA calendar is collected but no FDA-event-driven trading logic was found reading from it.

## G) ISSUES FOUND

1. **`tech_intelligence` table has no UNIQUE constraint.** Each run of `collect_all()` appends new rows. Over time this table will accumulate unbounded duplicate metric rows for every run.

2. **`summary()` triggers a full collection run.** Calling `summary()` is a destructive side-effect — it re-fetches all API data, re-stores rows, and returns a text string. This is unexpected behavior for a method named "summary."

3. **BioPharma Catalyst API URL is unofficial/undocumented.** The URL `https://www.biopharmacat.com/api/catalysts?type=pdufa&days_ahead=90` is not a known public API. This will likely fail with a non-200 response, silently falling back to price monitoring mode. The fallback mode stores placeholder `pdufa_date="unknown"` rows that cannot be used for event-driven trading.

4. **LTHM removed:** Comment states "LTHM (Livent Corp) removed 2026-04-04: delisted from Yahoo Finance." Replaced by ALB coverage. Correct action, noted.

5. **CONE and CLNC removed from DataCentreIntelligence:** Comment states both removed 2026-04-04. Correct action, noted.

6. **EV group weights defined but not used:** `EVAdoptionTracker.collect()` iterates groups with a `weight` parameter (ev_maker=1.5, battery=1.0, charging=0.8, legacy=0.5) but the weight variable is never used in any calculation — adoption_score only uses EV makers (0.6) and battery materials (0.4). The other groups' metrics are stored but not weighted.

7. **`check_for_delisted()` uses `info.market_cap` as proxy for "alive."** Some valid small-cap tickers may not have market_cap available via `fast_info`, producing false positive delisted warnings.

---

---

# COLLECTOR 6: government_data_collector.py

**Collector name:** BLSCollector / CensusCollector / USASpendingCollector
**File:** `/home/dannyelticala/quant-fund/data/collectors/government_data_collector.py`

## A) PURPOSE

Three independent collector classes with no shared orchestrator:
- **BLSCollector**: Fetches BLS API time-series for CPI (4 series), PPI (3 series), and employment (2 series). Stores to `bls_data` table.
- **CensusCollector**: Fetches Census Bureau EITS API for building permits and retail sales. Returns DataFrames, does not persist to DB.
- **USASpendingCollector**: Fetches recent federal contract awards from USASpending.gov and stores to `raw_government_contracts` table.

## B) CLASSES AND METHODS

### Class: `BLSCollector`

**`__init__(self, config=None)`**
- Sets `DB_PATH = 'output/permanent_archive.db'`. Creates output dir. Calls `_ensure_db()`.

**`_ensure_db(self) -> None`**
- Creates `bls_data` table: id, series_id, series_name, date, value, yoy_change, fetched_at.

**`fetch_series(self, series_ids: List[str], years_back: int = 5) -> Dict`**
- POST `https://api.bls.gov/publicAPI/v2/timeseries/data/` with batches of 25 series.
- Returns dict `{series_id: [observation_dicts]}`.
- No API key used (public API v2 anonymous, limited to ~25 series/request, ~50 requests/day).

**`collect_all_series(self) -> Dict`**
- Collects all CPI + PPI + employment series (9 total). Stores to `bls_data`.
- DB writes: `permanent_archive.db` → `bls_data`: series_id, series_name, date, value, fetched_at (INSERT OR REPLACE).
- Returns raw data dict.

### Class: `CensusCollector`

**`__init__(self)`** — creates output dir only.

**`get_building_permits(self) -> pd.DataFrame`**
- GET `https://api.census.gov/data/timeseries/eits/bps?...&time=from+2022-01`
- Returns DataFrame with Census API columns: cell_value, time_slot_id, seasonally_adj, error_data.
- NO DB writes.

**`get_retail_sales(self) -> pd.DataFrame`**
- GET `https://api.census.gov/data/timeseries/eits/mrts?...&time=from+2022-01`
- Returns DataFrame with Census API columns: cell_value, time_slot_id, seasonally_adj, category_code.
- NO DB writes.

### Class: `USASpendingCollector`

**`__init__(self, config=None)`** — creates output dir only.

**`store_awards(self, awards: List[Dict]) -> int`**
- Inputs: list of award dicts (from `get_recent_all_awards`)
- Dedup check: queries `raw_government_contracts` for existing (award_date, amount, description[:100]).
- DB writes: `permanent_archive.db` → `raw_government_contracts`: ticker=NULL, amount, contract_pct_of_mcap=NULL, contract_signal, signal_score, description, award_date, fetched_at.
- Returns count stored.

**`get_recent_all_awards(self, min_amount: float = 5_000_000) -> List[Dict]`**
- POST `https://api.usaspending.gov/api/v2/search/spending_by_award/`
- Filters: award types A/B/C/D (contracts), last 7 days, min_amount threshold.
- Returns up to 100 results (fields: Recipient Name, Award Amount, Awarding Agency, Start Date, Description).

**`search_contracts(self, company_name, start_date) -> List[Dict]`**
- POST to same endpoint with recipient_search_text filter.
- Returns up to 100 results. No DB writes.

## C) MATHEMATICS

**BLS date construction:**
```
date_str = f"{year}-{period.replace('M','').zfill(2)}-01"
```
(Converts BLS period format "M01" to "01", producing "YYYY-MM-01" format.)

**USASpending signal scoring:**
```
signal_score = 0.3 if amount >= 50M else (0.2 if amount >= 10M else 0.1)
contract_signal = "LARGE_CONTRACT" if amount >= 50M else "CONTRACT"
```

## D) DATA FLOWS

**External inputs:**
- BLS public API v2: no key required
- Census Bureau EITS API: no key required
- USASpending.gov API v2: no key required

**DB writes to `permanent_archive.db`:**
- `bls_data` (created by BLSCollector): series_id, series_name, date, value, yoy_change (always NULL), fetched_at
- `raw_government_contracts` (pre-existing, from QuiverCollector schema): ticker(NULL), amount, contract_pct_of_mcap(NULL), contract_signal, signal_score, description, award_date, fetched_at

**No DB writes to `historical_db.db`.**

**Data NOT persisted:**
- CensusCollector: both methods return DataFrames with no DB write. Data is lost unless caller explicitly stores it.

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `requests`
- `pandas`
- Standard: `logging`, `os`, `sqlite3`, `datetime`, `typing`

## F) WIRING STATUS

**PARTIAL / DATA DEAD END for most components.**
- **BLSCollector:** Stores to `bls_data` table which is separate from the FRED `raw_macro_data` table. No downstream signal generator was found reading `bls_data`. The BLS data duplicates some FRED series (CPI, PPI, employment) but in a different table. Dead end.
- **CensusCollector:** `get_building_permits()` and `get_retail_sales()` are pure fetch functions with no DB persistence. Data is lost after each call unless the caller stores it. Neither method is called with storage. Dead end.
- **USASpendingCollector:** Stores to `raw_government_contracts` which is also written by QuiverCollector. The quiver_collector schema is used here with `ticker=NULL`. If signals are generated from `raw_government_contracts`, the USASpending rows will have null tickers and cannot be attributed to any stock. Partial dead end.

## G) ISSUES FOUND

1. **`bls_data.yoy_change` is always NULL:** The column exists in the schema but `collect_all_series()` never computes YoY change. All stored rows have `yoy_change = NULL`.

2. **CensusCollector persists nothing to DB.** Both `get_building_permits()` and `get_retail_sales()` return DataFrames. There are no DB writes. Data is lost on every call unless the calling code explicitly handles persistence — but no calling code was found.

3. **BLS data duplicates FRED series:** BLSCollector fetches CPI All Items (CUUR0000SA0), CPI Food, CPI Energy, PPI, and payroll data — all of which are also fetched by ConsumerIntelligence from FRED. The BLS data goes into `bls_data` while FRED data goes into `raw_macro_data` and `macro_series`. These are never joined or reconciled.

4. **BLS API daily limit:** BLS public API v2 without a registration key is limited to ~50 requests/day and 25 series/request. For 9 series (1 batch), this is fine, but no API key support is implemented.

5. **`raw_government_contracts` table is assumed to exist:** `USASpendingCollector.store_awards()` calls `INSERT INTO raw_government_contracts` without creating the table. If QuiverCollector has not run first to create this table, `store_awards()` will fail with a sqlite3.OperationalError. No `CREATE TABLE IF NOT EXISTS` is present.

6. **No `collect()` method on any class.** None of the three classes implement a `collect()` interface. This means the orchestrator in `data_pipeline.py` (or equivalent) cannot call them uniformly via the standard `collector.collect()` pattern used by all other collectors.

---

---

# COLLECTOR 7: insider_transaction_collector.py

**Collector name:** InsiderTransactionCollector
**File:** `/home/dannyelticala/quant-fund/data/collectors/insider_transaction_collector.py`

## A) PURPOSE

Fetches SEC EDGAR Form 4 filings (insider transactions) via the EDGAR full-text search API. Parses XML to extract transaction details. Stores to `insider_transactions` table in `closeloop.db`. Generates insider_cluster_signal: detects cluster buying (3+ insiders buying within 30 days) and returns a 0-1 score weighted by executive title and transaction type.

## B) CLASSES AND METHODS

**`_is_ceo_cfo(title: str) -> bool`** — checks if title contains CEO/CFO keywords.

### Class: `InsiderTransactionCollector`

**`__init__(self, db_path=None)`**
- Default DB: `closeloop/storage/closeloop.db` (NOT the standard permanent_archive.db or historical_db.db).
- Calls `_ensure_table()`. Initialises requests.Session with SEC EDGAR User-Agent header.

**`_ensure_table(self) -> None`**
- Creates `insider_transactions` table: id, ticker, insider_name, title, transaction_date, shares, price_per_share, transaction_type, value_usd, is_ceo_cfo, filing_url, stored_at. UNIQUE on (ticker, insider_name, transaction_date, transaction_type).

**`fetch_recent_form4(self, days_back: int = 1) -> List[Dict]`**
- GET `https://efts.sec.gov/LATEST/search-index` with forms="4", custom date range.
- Returns list of `{entity_name, file_date, filing_id}` dicts (up to 100).

**`parse_form4_xml(self, filing_url: str) -> Optional[Dict]`**
- Fetches Form 4 XML. Parses: issuerTradingSymbol, rptOwnerName, officerTitle.
- Extracts all `<nonDerivativeTransaction>` elements.
- Returns `{ticker, transactions: [list]}` or None.

**`_store_transactions(self, txs: List[Dict]) -> int`**
- INSERT OR IGNORE to `insider_transactions`. Returns count of actually inserted rows (using `SELECT changes()`).

**`collect(self, days_back: int = 1, max_filings: int = 50) -> int`**
- Searches EDGAR for recent Form 4 filings, parses each XML, stores transactions.
- Rate-limits: 1.0s sleep every 10 filings.
- Returns total rows stored.

**`generate_insider_signal(self, ticker: str, lookback_days: int = 30) -> float`**
- DB read: `closeloop.db` → `insider_transactions` WHERE ticker AND date >= since.
- Computes weighted net score. Returns float 0.0-1.0.

**`should_boost_signal(self, ticker, current_score, boost=0.10) -> float`**
- Calls `generate_insider_signal()`. If score >= 0.5, adds +0.10 to current_score.

**`status(self) -> Dict[str, Any]`**
- DB read: total row count and unique ticker count from `insider_transactions`.

## C) MATHEMATICS

**CEO/CFO weight multiplier:** 3.0 if title contains CEO/CFO keywords, else 1.0.

**Transaction type weights:**
```
Open-market purchase (P): weight = shares * ceo_mult * 2.0
Option exercise (A or M):  weight = shares * ceo_mult * 1.0
Open-market sale (S or D): weight = shares * ceo_mult  (no multiplier bonus)
```

**Net Insider Signal:**
```
net = (weighted_buys - weighted_sells) / (weighted_buys + weighted_sells + 1e-9)
```

**Cluster Buy Boost:**
```
if count(unique buying insiders) >= 3 AND net > 0:
    score = max(net, 0.8)  # floor at 0.8
else:
    score = max(0.0, net)
score = min(score, 1.0)   # cap at 1.0
```

**Signal Boost Application:**
```
if insider_score >= 0.5:
    new_score = min(1.0, current_score + 0.10)
```

## D) DATA FLOWS

**External inputs:**
- SEC EDGAR EFTS search: `https://efts.sec.gov/LATEST/search-index`
- SEC EDGAR XML: `https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{doc}`

**DB writes to `closeloop/storage/closeloop.db` (NOT permanent_archive.db or historical_db.db):**
- `insider_transactions`: ticker, insider_name, title, transaction_date, shares, price_per_share, transaction_type, value_usd, is_ceo_cfo, filing_url, stored_at

**DB reads from `closeloop/storage/closeloop.db`:**
- `insider_transactions`: WHERE ticker AND date range (for signal generation and status)

## E) DEPENDENCIES

**Internal Apollo:** None

**External:**
- `requests` (lazy import inside `__init__`)
- Standard: `logging`, `sqlite3`, `time`, `xml.etree.ElementTree`, `datetime`, `typing`

## F) WIRING STATUS

**CONNECTED — to closeloop.db signal path.**
- `should_boost_signal(ticker, current_score)` is designed to be called during scoring to add +0.10 to existing signal scores.
- Writes to `closeloop.db`, not to `permanent_archive.db` or `historical_db.db`.
- This is separate from `historical_collector.py` which ALSO fetches Form 4 filings and stores insider transactions to `historical_db.db` under a different schema.

## G) ISSUES FOUND

1. **Writes to `closeloop/storage/closeloop.db`, not to the standard DBs.** This collector is isolated from the main data stores. Signal consumers that read from `permanent_archive.db` or `historical_db.db` will NOT see this data.

2. **Duplicate insider transaction collection:** `historical_collector.py` ALSO fetches Form 4 XML and stores to `historical_db.db → insider_transactions` with a different schema (includes `cik`, `reporter_name`, `reporter_title`, `shares_owned_after`, `accession_number` vs this collector's simpler schema). Two separate insider transaction datasets exist in two separate databases with two different schemas.

3. **EDGAR filing_id parsing assumption.** The `collect()` method splits `filing_id` on ":" to get `accession_dashes` and `doc_name`. If EDGAR changes the `_id` format in search results, this parsing will silently produce wrong URLs. The accession number is split by "-" to get CIK — but EDGAR accession format is `NNNNNNNNNN-YY-NNNNNN`; the first segment is the filer's CIK padded to 10 digits. This is correct but fragile.

4. **`_EXER_CODES = {"A", "M"}` — option exercises are counted as buys.** Code "A" means "Acquisition" (which includes grants, awards, and exercises) and "M" means exercise of derivative. These are counted as buying insiders for cluster detection. This inflates buy counts; a grant of restricted stock is treated identically to an open-market purchase.

5. **`should_boost_signal` threshold of 0.5:** A signal is boosted if the insider score >= 0.5. But due to the cluster-buy floor of 0.8 and the net formula, most cluster-buy signals will be exactly 0.8. Single insider buys with no sells will produce net ≈ 1.0. This threshold may be too low and will boost signals whenever any insider buys.

6. **Rate limiting:** SEC EDGAR allows ~10 requests/second. The collector sleeps 1.0s every 10 filings (0.1s per filing effective rate), which is at the edge of EDGAR's limit. No backoff for 429 errors.

---

---

# SECTION 5 GATE PARTIAL — FIRST 8 COLLECTORS

## Files Read

1. `/home/dannyelticala/quant-fund/data/historical_collector.py`
2. `/home/dannyelticala/quant-fund/data/collectors/shipping_intelligence.py`
3. `/home/dannyelticala/quant-fund/data/collectors/consumer_intelligence.py`
4. `/home/dannyelticala/quant-fund/data/collectors/rates_credit_collector.py`
5. `/home/dannyelticala/quant-fund/data/collectors/commodity_collector.py`
6. `/home/dannyelticala/quant-fund/data/collectors/technology_intelligence.py`
7. `/home/dannyelticala/quant-fund/data/collectors/government_data_collector.py`
8. `/home/dannyelticala/quant-fund/data/collectors/insider_transaction_collector.py`

## Key Findings

1. **Two parallel insider transaction systems:** `historical_collector.py` and `insider_transaction_collector.py` both fetch SEC Form 4 data independently, storing to different databases (`historical_db.db` vs `closeloop.db`) with different schemas. These are never reconciled.

2. **Primary BDI source (stooq.com) is permanently blocked.** The shipping intelligence collector permanently uses BDRY ETF as a proxy for the Baltic Dry Index. This is documented in the code itself.

3. **Container rate component of SSI is permanently absent.** `SSI_WEIGHT_CONTAINER = 0.25` is defined but the data source for it was never implemented. SSI always runs as a 2-component indicator.

4. **ConsumerIntelligence composite indices are ephemeral.** The three computed indices (ConsumerHealthIndex, HousingHealthIndex, InflationPressure) are returned in the collect() dict but never persisted to DB. They are recomputed on demand.

5. **TechnologyIntelligence writes only to `permanent_archive.db` and appears to be a dead end.** No downstream signal generator reads `tech_intelligence`, `fda_calendar`, or `tech_knowledge_graph` tables.

6. **CensusCollector has zero persistence.** Both data-fetching methods return DataFrames with no DB writes.

7. **`raw_macro_data` schema collision** between `rates_credit_collector.py` (no UNIQUE constraint) and `consumer_intelligence.py` (UNIQUE on series_id, date). Whichever creates the table first wins; the other may silently fail to enforce its constraints.

8. **`raw_commodity_prices` schema collision** between `commodity_collector.py` and `shipping_intelligence.py` (via PermanentArchive). Column sets differ.

9. **`government_data_collector.py` has no `collect()` method on any class.** Cannot be called through the standard pipeline interface.

10. **`USASpendingCollector.store_awards()` assumes `raw_government_contracts` already exists.** No table creation — depends on QuiverCollector having run first.

11. **Duplicate macro data:** BLS (via `bls_data`) and FRED (via `raw_macro_data`) collect overlapping CPI, PPI, and employment data into different tables that are never joined.

## Contradictions

| Contradiction | Files Involved |
|---|---|
| `raw_macro_data` UNIQUE constraint: present in consumer_intelligence, absent in rates_credit_collector | consumer_intelligence.py vs rates_credit_collector.py |
| `raw_commodity_prices` column schema differs | commodity_collector.py vs shipping_intelligence.py (via PermanentArchive) |
| Two Form 4 parsers, two different schemas, two different databases | historical_collector.py vs insider_transaction_collector.py |
| BLS and FRED collect overlapping macro series into separate tables | government_data_collector.py vs consumer_intelligence.py |
| `t10y2y` in historical_collector is computed as TNX-IRX (10Y minus 13W), not 10Y minus 2Y as the name implies | historical_collector.py |

## Data Flows Summary

| Collector | External Source | Writes to permanent_archive.db | Writes to historical_db.db | Writes to closeloop.db | Connected to Signal? |
|---|---|---|---|---|---|
| historical_collector.py | yfinance, EDGAR, Google News | No | price_history, quarterly_financials, balance_sheet, cash_flow, edgar_filings, insider_transactions, institutional_ownership, macro_context, earnings_enriched, news | No | YES — earnings enrichment, macro context |
| shipping_intelligence.py | stooq(blocked)/yfinance | raw_shipping_data, raw_commodity_prices | shipping_data | No | YES — SSI sector impacts |
| consumer_intelligence.py | FRED API, yfinance | raw_macro_data | macro_series, payment_processor_signals | No | PARTIAL — indices not persisted |
| rates_credit_collector.py | FRED API, yfinance | raw_macro_data | rates_data, rates_signals | No | YES — position sizing modifier |
| commodity_collector.py | yfinance | raw_commodity_prices, commodity_lead_lag | commodity_prices | No | PARTIAL — sector impacts unclear |
| technology_intelligence.py | yfinance, BioPharmaCAT | tech_intelligence, fda_calendar, tech_knowledge_graph | No | No | DEAD END |
| government_data_collector.py | BLS API, Census API, USASpending API | bls_data, raw_government_contracts | No | No | DEAD END (mostly) |
| insider_transaction_collector.py | SEC EDGAR | No | No | insider_transactions | YES — score boosting |

## Continue to Part 7B: YES

**Remaining collectors (Part 7B):**
- advanced_news_intelligence.py
- alternative_quiver_collector.py
- geopolitical_collector.py
- geographic_intelligence.py
- job_postings_collector.py
- news_context_enricher.py
- openbb_collector.py
- polygon_collector.py
- quiver_collector.py
- regulatory_intelligence.py
- sec_fulltext_collector.py
- short_interest_collector.py
- simfin_collector.py
- social_influence_tracker.py
