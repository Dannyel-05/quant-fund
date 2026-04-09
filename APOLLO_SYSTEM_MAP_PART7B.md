# APOLLO SYSTEM MAP — PART 7B
## Second Half of Group 7 Collectors

Generated: 2026-04-09
Scope: data/collectors/ (second batch) + altdata/collector/ lunar and geomagnetic

---

## FILE 1: job_postings_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/job_postings_collector.py`

### A) PURPOSE
Tracks company job posting counts across two job boards (Adzuna, Reed UK) as a leading revenue indicator. Computes job growth rates and converts them to a scalar signal in [-1, +1].

### B) CLASSES AND METHODS

**Module-level helpers:**
- `_categorise_title(title: str) -> str` — Classifies a job title into "engineering", "sales", "admin", or "other" by keyword matching against `_ENGINEERING_KW`, `_SALES_KW`, `_ADMIN_KW` sets.

**Class: `JobPostingsCollector`**

- `__init__(config, db_path)` — Reads `api_keys.adzuna_app_id`, `api_keys.adzuna_app_key`, `api_keys.reed_api_key` from config. Calls `_ensure_table()`. Creates `requests.Session` if available.

- `_ensure_table()` — DB WRITE. Creates `job_postings` table in `closeloop/storage/closeloop.db` if not exists.

- `fetch_adzuna(company: str, ticker: str) -> Dict[str, int]` — EXTERNAL API. Calls `https://api.adzuna.com/v1/api/jobs/us/search/1` with `results_per_page=50`. Returns `{total, engineering, sales, admin, other}` counts. Returns zeros if keys not configured.

- `fetch_reed_uk(company: str, ticker: str) -> Dict[str, int]` — EXTERNAL API. Calls `https://www.reed.co.uk/api/1.0/search` with `resultsToTake=100`. Uses HTTP Basic Auth with reed key. Returns same dict shape. Only called if `reed_api_key` configured.

- `calculate_growth_rate(ticker: str) -> float` — DB READ from `job_postings`. Fetches last 2 rows ordered by `collection_date DESC`. Returns `(current - prior) / max(prior, 1.0)`.

- `job_growth_signal(ticker: str) -> float` — DB READ from `job_postings`. Fetches last 2 rows for `total_postings`, `engineering_count`, `sales_count`. Applies scoring rules. Returns float clamped to [-1, +1].

- `_store(ticker, company, counts, source) -> int` — DB WRITE. `INSERT OR IGNORE` into `job_postings`. Calls `calculate_growth_rate()` before storing so the stored `growth_rate` uses the prior row (not the current row being inserted). Returns `changes()`.

- `collect_ticker(ticker, company, market) -> int` — Calls `fetch_adzuna()` and optionally `fetch_reed_uk()`. Stores results. Returns rows stored.

- `collect(ticker_company_map: Dict[str, str]) -> int` — Iterates all tickers. Sleeps 0.5s every 10 tickers.

- `status() -> Dict` — DB READ. Returns `{total_rows, unique_tickers}`.

### C) MATHEMATICS

Growth rate (two locations in code):
```
growth_rate = (current - prior) / max(prior, 1.0)
```

Signal scoring (`job_growth_signal`):
```
total_gr = (current_total - prior_total) / max(prior_total, 1.0)
eng_gr   = (current_eng   - prior_eng)   / max(prior_eng,   1.0)
sales_gr = (current_sales - prior_sales) / max(prior_sales, 1.0)

if total_gr < -0.20:
    score = -0.40
else:
    if eng_gr   > 0.30: score += 0.30
    if sales_gr > 0.20: score += 0.20
    if total_gr > 0.20: score += 0.20

score = clamp(score, -1.0, +1.0)
```

### D) DATA FLOWS

**Inputs:** Adzuna API JSON (`results[].title`), Reed API JSON (`results[].jobTitle`).

**DB Table Written:** `closeloop/storage/closeloop.db` :: `job_postings`
- Columns: `id`, `ticker`, `company`, `collection_date`, `total_postings`, `engineering_count`, `sales_count`, `admin_count`, `growth_rate`, `source`, `stored_at`
- Constraint: `UNIQUE(ticker, collection_date, source)` — uses `INSERT OR IGNORE`

**DB Table Read:** Same `job_postings` — last 2 rows per ticker ordered by `collection_date DESC`.

### E) DEPENDENCIES

Internal: None.
External: `requests` (optional — degrades gracefully if missing), `sqlite3`, standard library.

### F) WIRING STATUS

The docstring states: `Wire: job_growth_rate → fundamental signal with weight 0.05`. The `job_growth_signal()` method is designed as a callable endpoint for a signal consumer. However, no import of `JobPostingsCollector` was found in signal layer files during this session. The collector writes to `closeloop.db` rather than `permanent_archive.db`, which is where most signal consumers read. **PARTIAL — signal method exists and documented as wired, but DB path isolation may prevent upstream consumption.**

### G) ISSUES FOUND

1. **`growth_rate` stored before current row exists.** `_store()` calls `calculate_growth_rate()` before the INSERT. At first collection for a ticker, the rate is 0.0 (correct). On second collection, `calculate_growth_rate()` fetches only the prior row (correct). However if called for a ticker that has existing data, the stored `growth_rate` column reflects the ratio of the two most recent existing rows — not the new row being added. The new row's absolute count is then stored, but the `growth_rate` written to that new row is actually the old row's rate vs its predecessor. This is a silent staleness bug: the `growth_rate` column always lags by one collection cycle.

2. **`job_growth_signal()` reads `engineering_count` and `sales_count` from rows that may have `admin_count` uninitialized if source="reed" (reed only stores categorised results same as adzuna, so not a data gap, but the `other` category is never stored to DB in `_store()` — it is silently dropped).**

3. **Score can reach +0.70** (engineering +0.30, sales +0.20, total +0.20 all simultaneously). This exceeds the docstring's stated cap of "+0.4" for aggressive hiring. The `clamp(score, -1, +1)` caps it to 1.0, but the docstring claim of "+0.2 to +0.4" is wrong.

4. **DB path hardcoded to `closeloop/storage/closeloop.db` (relative path)**. If the process is started from a different CWD, the DB will be created in the wrong location. The `db_path` parameter allows override but is not used by the scheduler unless explicitly passed.

5. **Reed UK is only triggered for tickers ending in `.L` or `market="uk"`.** US-listed ADRs of UK companies will never get Reed data.

---

## FILE 2: sec_fulltext_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/sec_fulltext_collector.py`

### A) PURPOSE
Scans SEC EDGAR full-text search (EFTS) for 19 crisis keywords and 10 opportunity keywords across all filings from the past 3 days. Stores alerts to `permanent_archive.db`. Matches results against the trading universe.

### B) CLASSES AND METHODS

**Class: `SECFullTextCollector`**

Constants:
- `BASE_URL = FALLBACK_URL = 'https://efts.sec.gov/LATEST/search-index'` — **Both URLs are identical (noted in code as "Fallback").**
- `CRISIS_KEYWORDS` — 19 entries with severities: CRITICAL/HIGH/MEDIUM.
- `OPPORTUNITY_KEYWORDS` — 10 entries with severities.

- `__init__()` — Calls `_ensure_db()`, `_load_universe()`.

- `_ensure_db()` — DB WRITE. Creates `sec_fulltext_alerts` table in `output/permanent_archive.db`. Sets WAL mode.

- `_load_universe()` — DB READ (indirect). Tries to import `data.universe.Universe` and call `u.get_us_tickers()`. Falls back to globbing `data/universe/*.csv` and `universe/*.csv`. Populates `self._universe_tickers`.

- `in_universe(ticker) -> bool` — Returns `ticker.upper() in self._universe_tickers`.

- `search_keyword(keyword, days_back=3, filing_types=None) -> List[Dict]` — EXTERNAL API. GET to `efts.sec.gov/LATEST/search-index` with date range `now - days_back` to `now`. Parses `hits.hits[]`. Extracts ticker from `display_names` field using `_SEC_TICKER_RE` regex. Returns list of alert dicts including `keyword`, `entity_name`, `ticker`, `filing_type`, `filing_date`, `accession_no`, `snippet`, `in_universe`, `fetched_at`. Handles 429 with 5s sleep and returns [].

- `_store_alerts(alerts, alert_type)` — DB WRITE. `INSERT` (not INSERT OR IGNORE) into `sec_fulltext_alerts`. Each alert stores: `keyword`, `entity_name`, `ticker`, `filing_type`, `filing_date`, `severity`, `alert_type`, `snippet`, `accession_no`, `in_universe`, `fetched_at`.

- `daily_crisis_scan() -> List[Dict]` — Iterates `CRISIS_KEYWORDS`, calls `search_keyword()` with `days_back=3`, attaches severity, calls `_store_alerts(alerts, 'CRISIS')`. Returns sorted by severity.

- `daily_opportunity_scan() -> List[Dict]` — Same pattern with `OPPORTUNITY_KEYWORDS` and `alert_type='OPPORTUNITY'`.

- `run_full_daily_scan() -> Dict` — Calls both scans, returns summary with counts and `crisis_alerts[:20]` / `opportunity_alerts[:20]`.

### C) MATHEMATICS
None. Pure keyword matching and counting.

### D) DATA FLOWS

**Inputs:** SEC EFTS REST API. Query parameters: `q` (quoted keyword), `dateRange=custom`, `startdt`, `enddt`, optional `forms`.

**DB Table Written:** `output/permanent_archive.db` :: `sec_fulltext_alerts`
- Columns: `id`, `keyword`, `entity_name`, `ticker`, `filing_type`, `filing_date`, `severity`, `alert_type`, `snippet`, `accession_no`, `in_universe`, `fetched_at`

**DB Table Read:** `output/permanent_archive.db` — indirectly via `_load_universe()` if Universe class used.

### E) DEPENDENCIES

Internal: `data.universe.Universe` (optional, with CSV fallback), `yaml`, `csv`, `glob`.
External: `requests`, `re`, `sqlite3`.

### F) WIRING STATUS

No direct import found in signal layer. `run_full_daily_scan()` and `daily_crisis_scan()` are designed as standalone batch jobs. Data stored in `sec_fulltext_alerts` table — no evidence this table is read by any signal generator in the codebase. **DATA DEAD END — alerts stored to DB but no downstream signal consumer documented.**

### G) ISSUES FOUND

1. **`BASE_URL` and `FALLBACK_URL` are identical strings.** The fallback is never different from the primary. There is no actual failover.

2. **`_store_alerts` uses plain INSERT (not INSERT OR IGNORE).** Every run of `daily_crisis_scan()` on the same day will duplicate all alerts. There is no deduplication on `(keyword, accession_no, filing_date)` or any other unique constraint.

3. **Ticker extraction regex `_SEC_TICKER_RE` may produce false positives.** Pattern `r'\(([A-Z][A-Z0-9\.,\s]{0,40}?)\)\s*\(CIK\s*\d'` is designed for the format `"COMPANY (TICK1, TICK2) (CIK ...)"`. If SEC changes display name format, all ticker extractions silently return None.

4. **`search_keyword` only uses `days_back=3` for both scan types.** Opportunity events (FDA approvals, merger announcements) may be filed and missed if the daily scan is skipped for 3+ days.

5. **`snippet` is capped at 200 chars from the first highlight field.** If no highlight is returned by EFTS, `snippet` is empty string — stored silently as blank.

6. **Universe loading falls back to relative path globs.** If working directory is not the project root, the CSV glob fails silently and `_universe_tickers` is empty. Result: all alerts are stored with `in_universe=0`.

---

## FILE 3: alternative_quiver_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/alternative_quiver_collector.py`

### A) PURPOSE
Free alternative to QuiverQuantitative. Fetches congressional trades (Form 4 search on EFTS), House financial disclosures, WSB mentions (Reddit public API), patent filings (PatentsView), and Form 4 insider trades from EDGAR. No API key required. No DB writes — returns raw Python lists.

### B) CLASSES AND METHODS

**Class: `AlternativeQuiverCollector`**

- `__init__(config: dict)` — Stores config. Sets `User-Agent` header.

- `get_senate_trades(days_back=30) -> List[Dict]` — EXTERNAL API. GET `efts.sec.gov/LATEST/search-index` with `q='"Senate" "financial disclosure"'`, `forms=4`, date range. Returns up to 20 hits as `{'source': _source_dict, 'type': 'senate'}`. No DB write.

- `get_house_trades(days_back=30) -> List[Dict]` — Same pattern with `q='"House" "financial disclosure" "stock"'`. Returns up to 20 hits. No DB write.

- `get_wsb_mentions(ticker: str) -> Dict` — EXTERNAL API. GET `reddit.com/r/wallstreetbets/search.json` with `q=ticker`, `sort=new`, `limit=25`. Returns `{'ticker', 'mention_count', 'source'}`. No DB write.

- `get_patent_filings(company_name: str) -> List[Dict]` — EXTERNAL API. GET `search.patentsview.org/api/v1/patent/` with text search on `patent_abstract`. Returns list of `{patent_number, patent_title, patent_date}`. No DB write.

- `get_form4_insider_trades(ticker: str, days_back=30) -> List[Dict]` — EXTERNAL API. GET `efts.sec.gov/LATEST/search-index` with `q='"ticker"'`, `forms=4`. Returns up to 20 hits as `{ticker, entity, date, form}`. No DB write.

### C) MATHEMATICS
None.

### D) DATA FLOWS

**Inputs:** SEC EFTS, Reddit public JSON API, PatentsView API.

**DB Table Written:** None. All methods return in-memory lists/dicts only.

**DB Table Read:** None.

### E) DEPENDENCIES

Internal: None.
External: `requests`, `datetime`.

### F) WIRING STATUS

No DB writes. No imports found linking this collector to any signal generator. **DATA DEAD END — all data returned as Python objects, never persisted, never consumed by signals.**

### G) ISSUES FOUND

1. **Congressional trade search is fundamentally broken.** The method searches for `"Senate" "financial disclosure"` in Form 4 filings. Form 4 filings are insider trades by corporate officers — not Senate financial disclosures. Senate disclosures are filed under eFD system, not EDGAR Form 4. This query will return near-zero relevant results.

2. **Same issue for `get_house_trades`.** House members file financial disclosures through the House Clerk system, not as EDGAR Form 4. The search will not find actual House stock trades.

3. **`get_wsb_mentions` User-Agent override** sets `'quant-fund/1.0'` inside the method but the class-level `self.headers` has `'quant-fund research@quantfund.com'`. The WSB call uses a merged dict with the method-level agent overriding the class-level — this is correct but the email-format user-agent in the class will be rejected by Reddit's API, making class-level headers useless for Reddit.

4. **`get_patent_filings` uses malformed JSON in query string.** The `q` parameter is a raw Python f-string inserted into a URL param: `f'{{"_text_any":{{"patent_abstract":"{company_name}"}}}}`. If `company_name` contains quotes or special chars, the JSON is invalid.

5. **No rate limiting anywhere in this collector.**

6. **No data persistence** — all results are ephemeral in memory and lost between runs.

---

## FILE 4: geopolitical_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/geopolitical_collector.py`

### A) PURPOSE
Collects geopolitical risk signals from GDELT (global event data), NewsAPI (15 high-impact keywords), and USGS earthquake feed. Classifies events by severity, maps them to affected economic sectors, generates signal multipliers, and provides a hardcoded historical crisis reference database (9 events, 1997–2023).

### B) CLASSES AND METHODS

**Module-level functions:**

- `_load_config() -> Dict` — Reads `config/settings.yaml`.
- `_get_conn() -> sqlite3.Connection` — Opens `output/permanent_archive.db` with WAL + busy_timeout=5000 + foreign_keys.
- `_init_db(conn)` — DB WRITE. `CREATE TABLE IF NOT EXISTS raw_geopolitical_events` with schema migration (ALTER TABLE ADD COLUMN for each missing column). Also creates `raw_articles` table if not exists.
- `_classify_severity(text: str) -> str` — Returns worst severity found via `_KEYWORD_SEVERITY` dict. Levels: LOW < MEDIUM < HIGH < CRITICAL.
- `_identify_sectors(text: str) -> List[str]` — Returns list of sectors from `_KEYWORD_SECTORS` dict based on keyword presence.
- `_fetch_gdelt(conn) -> List[GeopoliticalAlert]` — EXTERNAL API. 8 queries to `api.gdeltproject.org/api/v2/doc/doc`. 250 articles per query. Exponential backoff on 429 (5s, 10s, 20s, 40s). Full URL fetch for HIGH/CRITICAL articles, capped at 20 per query. DB WRITE to `raw_geopolitical_events`. Returns only HIGH/CRITICAL alerts.
- `_fetch_newsapi(conn, api_key) -> List[GeopoliticalAlert]` — EXTERNAL API. 15 keywords via `newsapi.org/v2/everything`. 100 articles each. 2.0s sleep between keywords. DB WRITE to `raw_articles`. Returns only HIGH/CRITICAL alerts.
- `_fetch_usgs(conn) -> List[GeopoliticalAlert]` — EXTERNAL API. GET `earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson`. Filters mag >= 3.0 for DB write; only mag >= 6.0 go into alerts. Checks populated region bounding boxes.
- `_magnitude_to_severity(mag) -> str` — `>= 8.0` → CRITICAL, `>= 7.0` → HIGH, `>= 6.0` → MEDIUM, else LOW.
- `_check_populated(lat, lon) -> Optional[Dict]` — Returns region+sectors if coords fall within 7 hardcoded bounding boxes (Japan, California, Indonesia, Turkey, Italy, Chile, Mexico).

**Dataclass: `GeopoliticalAlert`**
Fields: `severity`, `description`, `affected_sectors`, `affected_regions`, `signal_modifier`, `source`, `event_date`, `url`.
Method: `to_dict()`.

**Class: `GeopoliticalCollector`**

- `__init__(config)` — Reads config, gets `api_keys`.
- `collect(market=None) -> List[GeopoliticalAlert]` — Runs GDELT, NewsAPI, USGS in sequence. Stores `self._alerts`. Returns all alerts.
- `get_current_risk_level() -> str` — Returns severity of worst active alert.
- `get_sector_modifiers() -> Dict[str, float]` — Returns `{sector: product_of_modifiers}` — modifiers compound **multiplicatively** across alerts.
- `get_alerts(severity=None) -> List[GeopoliticalAlert]` — Filter by severity level.
- `get_historical_crisis(event_type=None) -> List[Dict]` — Returns from hardcoded `HISTORICAL_CRISES` list (9 entries).
- `get_summary() -> Dict` — Returns counts + sector modifiers.

### C) MATHEMATICS

Severity modifier mapping:
```
LOW      → 0.95
MEDIUM   → 0.85
HIGH     → 0.70
CRITICAL → 0.50
```

Sector modifier (multiplicative compounding):
```
modifiers[sector] = current_modifier * alert.signal_modifier
```
(starts at 1.0 per sector, each alert multiplies it down)

Hawkish/dovish scoring (NewsAPI path): uses `_classify_severity()` — pure keyword presence, no weighting.

### D) DATA FLOWS

**Inputs:** GDELT v2 Doc API, NewsAPI v2, USGS GeoJSON feed, `config/settings.yaml` for `api_keys.news_api`.

**DB Table Written:** `output/permanent_archive.db`
- `raw_geopolitical_events`: `id`, `source`, `event_date`, `title`, `description`, `url`, `goldstein_scale`, `magnitude`, `latitude`, `longitude`, `location`, `severity`, `affected_sectors`, `affected_regions`, `raw_json`, `collected_at`
- `raw_articles` (NewsAPI path): `id`, `url`, `fetch_date`, `source`, `ticker_context`, `full_text`, `word_count`, `title`, `author`, `publication_date`, `is_paywalled`, `fetch_method`, `all_tickers_mentioned`, `all_companies_mentioned`, `sentiment_score`, `article_type`

**DB Table Read:** None directly (the `_init_db` migration reads `PRAGMA table_info`).

### E) DEPENDENCIES

Internal: None.
External: `requests`, `yaml`, `sqlite3`, `json`, `dataclasses`.

### F) WIRING STATUS

`GeopoliticalCollector` is instantiated by external orchestrators. `get_sector_modifiers()` returns a dict that could be used to scale position sizes. No direct import in signal files was verified in this session. The data lands in `raw_geopolitical_events` and `raw_articles`. **PARTIAL — collector is functional and structured as a signal provider, but wiring to live trading path not confirmed.**

### G) ISSUES FOUND

1. **GDELT full-text URL fetch is unthrottled within each article loop** (only 20 per query due to the `url_fetches < 20` cap), but the 8 queries run sequentially with only 1s sleep between them — at 20 URL fetches each, this is 160 HTTP GETs to arbitrary third-party URLs with 5s timeout each. This can take 13+ minutes.

2. **`raw_articles` schema mismatch risk.** The `_init_db` CREATE TABLE for `raw_articles` defines `publication_date` but `SocialInfluenceTracker._init_db` creates `raw_articles` with a `published_at` column. Both modules attempt to create this same table. If `SocialInfluenceTracker` runs first, `geopolitical_collector` inserts into `raw_articles` using `publication_date` which does not exist in the other module's schema — causing a silent column-not-found error or mismatched data.

3. **`goldstein_scale` is always stored as `None`** from GDELT despite the column existing. The code does not extract `goldstein_scale` from the GDELT article JSON.

4. **No deduplication on `raw_geopolitical_events`** for GDELT/NewsAPI writes — `INSERT OR IGNORE` is used, but the table has no UNIQUE constraint. Every daily scan duplicates all events.

5. **`HISTORICAL_CRISES` is a static hardcoded list** — it contains no 2024 or 2025 events despite being used as a reference database.

6. **Signal modifier compounding** can reach near-zero quickly (e.g., 3 CRITICAL alerts on the same sector give `0.50 × 0.50 × 0.50 = 0.125`). No floor is applied. A single sector could be effectively suppressed to near-zero by a bad news day.

---

## FILE 5: altdata/collector/finnhub_collector.py

**File:** `/home/dannyelticala/quant-fund/altdata/collector/finnhub_collector.py`

### A) PURPOSE
Collects company news, analyst recommendations, and price targets from Finnhub API for all tickers. Returns standardised result dicts with `source`, `ticker`, `data_type`, `value`, `raw_data`, `timestamp`, `quality_score`. No DB writes.

### B) CLASSES AND METHODS

**Module-level function: `collect(tickers, market, config) -> list`**

For each ticker:
1. Company news — GET `finnhub.io/api/v1/company-news` for last 30 days. Stores up to 10 articles as `data_type="news"`, `value=0.0`, `quality_score=0.7`.
2. Analyst recommendations — GET `finnhub.io/api/v1/stock/recommendation`. Takes `recs[0]` (most recent). Computes `score = (buy - sell) / max(buy + sell + hold, 1)`. Stores as `data_type="analyst_rating"`, `quality_score=0.8`.
3. Price target — GET `finnhub.io/api/v1/stock/price-target`. Stores `targetMean` as `data_type="price_target"`, `quality_score=0.75`.

Each API call sleeps `_DELAY = 0.5s`. Strips `.L` from ticker for Finnhub symbol.

**Class: `FinnhubCollector`**

- `__init__(config)` — Stores config.
- `collect(tickers, market='US') -> list` — Delegates to module-level `collect()`.

### C) MATHEMATICS

Analyst rating score:
```
score = (buy - sell) / max(buy + sell + hold, 1)
```
Range: [-1.0, +1.0] approximately. Stored as `value` field.

### D) DATA FLOWS

**Inputs:** Finnhub REST API. Requires `config.api_keys.finnhub`.

**DB Table Written:** None. All data returned as list of dicts in memory.

**DB Table Read:** None.

### E) DEPENDENCIES

Internal: None.
External: `requests`, `datetime`.

### F) WIRING STATUS

Located in `altdata/collector/` (not `data/collectors/`). The module-level `collect()` function signature matches the altdata collector interface pattern used by other collectors in `altdata/collector/`. This pattern is consumed by an altdata aggregator. **WORKING — connected to altdata pipeline (not the main closeloop signal path). No DB persistence.**

### G) ISSUES FOUND

1. **`value=0.0` for news items.** News articles are fetched and stored with `value=0.0` — no sentiment analysis is applied. The raw article text is stored in `raw_data` but unused by any downstream consumer without additional processing.

2. **Only `recs[0]` is used for analyst rating.** If the most recent recommendation is stale (months old), the score reflects outdated consensus.

3. **UK tickers (`ticker.endswith('.L')`) strip `.L` for Finnhub** — Finnhub covers some LSE stocks but many will return empty results silently.

4. **Rate limit: 60 calls/min on free tier** with `_DELAY=0.5s`. For N tickers, 3 calls each = 3N calls. At 0.5s delay between calls, N=20 tickers = 60 calls in ~30s — acceptable. But N=100 tickers = 300 calls in ~150s — will hit rate limits.

5. **No error distinction between "no data" and "API error"** — both return empty result silently.

---

## FILE 6: data/collectors/advanced_news_intelligence.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/advanced_news_intelligence.py`

### A) PURPOSE
Full-article reading engine. Fetches article URLs from NewsAPI, parses full HTML content (BeautifulSoup preferred, regex fallback), extracts quantitative forward-looking claims (revenue, growth, margin, EPS, hiring, capex), tracks narrative sentiment shifts over 7/30/90-day windows, scores source credibility, and links related articles. Master class is `AdvancedNewsIntelligence`.

### B) CLASSES AND METHODS

**Class: `ArticleReader`**

- `__init__(conn)` — Stores DB connection.
- `_fetch_url(url) -> Optional[str]` — GET with browser User-Agent, 30s timeout.
- `_fetch_with_archive_fallback(url) -> Optional[str]` — Tries direct URL, then `web.archive.org/web/{url}`.
- `_parse_bs4(html, url) -> Dict` — BeautifulSoup: strips nav/footer/aside/script/style/header/form. Extracts title (og:title preferred), author, date (article:published_time), body text from `<article>` → `<main>` → `<body>`.
- `_parse_regex(html, url) -> Dict` — Strips all HTML tags with `re.sub`. Returns title from `<title>` tag, body capped at 50,000 chars.
- `_parse(html, url) -> Dict` — Uses BS4 if available, else regex.
- `_extract_domain(url) -> str` — `urlparse(url).netloc.lstrip("www.")`.
- `_store_article(url, title, author, pub_date, body, source_name, ticker) -> int` — DB WRITE to `raw_articles`. Returns `lastrowid`.
- `fetch_article(url, ticker) -> Optional[Dict]` — Skips `consent.yahoo.com`. Skips if body < 100 chars AND title < 20 chars. Calls `_store_article`. Returns `{article_id, url, domain, title, author, date, body}` or None.

**Class: `SourceCredibilityDB`**

- `__init__(conn)` — Default score 30.0, default tier 4.
- `get_score(url) -> float` — DB READ from `source_credibility` by domain. Returns 30.0 if not found.
- `update_accuracy(domain, was_accurate)` — DB READ + WRITE. EWA update: `new_score = 0.95 * current_score + 0.05 * signal` where signal = 100.0 if accurate else 0.0.

**Class: `NarrativeShiftTracker`**

- `__init__(conn, credibility_db)` — Stores conn and credibility reference.
- `_weighted_avg(rows) -> float` — Weighted average of sentiment scores by source credibility score. `total = sum(credibility * sentiment) / sum(credibility)`.
- `compute_shift(ticker_or_sector) -> Dict` — DB READ from `raw_articles` WHERE `all_tickers_mentioned LIKE %ticker%` OR `ticker_context LIKE %ticker%` AND `fetch_date >= 90 days ago`. Computes `sent_90`, `sent_30`, `sent_7`. DB WRITE to `narrative_shifts`. Returns shift analysis dict.

**Class: `QuantitativeClaimExtractor`**

- `__init__(conn, credibility_db)` — Stores refs.
- `_parse_value(raw) -> float` — `float(raw.replace(",", ""))`.
- `_resolve_unit(raw_value, unit_str) -> Tuple[float, str]` — Converts B→×1000, M→×1 in "$M" units.
- `extract_claims(text, source_domain, claim_date, article_id, ticker, company_name) -> List[Dict]` — Applies 6 compiled regex patterns. For each match: extracts context (80 chars each side), resolves value+unit, stores to DB. DB WRITE to `quantitative_claims`.

**Class: `RelatedArticleDiscovery`**

- `__init__(conn)`.
- `find_related(article_id, ticker, limit=5) -> List[Dict]` — DB READ from `raw_articles` WHERE ticker mentioned AND `fetch_date >= 7 days ago`. DB WRITE to `article_connections` with `connection_type="same_company"`, `connection_strength=0.7`.

**Class: `AdvancedNewsIntelligence`** (master orchestrator)

- `__init__()` — Loads config, opens DB, calls `_init_db`, instantiates all sub-classes.
- `_search_news_api(ticker) -> List[Dict]` — GET NewsAPI with `q=ticker`, 10 articles.
- `collect_and_analyse(tickers, news_api_key) -> Dict` — For each ticker: fetch NewsAPI articles, for each article: `fetch_article()` → `extract_claims()` → `find_related()`. Then `compute_shift()`. Returns summary counts.
- `get_narrative_shift(ticker) -> Dict` — Calls `narrative_tracker.compute_shift(ticker)`.
- `get_source_score(url) -> float` — Calls `credibility_db.get_score(url)`.

### C) MATHEMATICS

Source credibility EWA update:
```
new_score = 0.95 * current_score + 0.05 * signal
(signal = 100.0 if accurate, 0.0 if not)
```

Credibility-weighted sentiment average:
```
weighted_avg = sum(sentiment_i * credibility_i) / sum(credibility_i)
```

Narrative shift:
```
shift_score = sent_7d - sent_90d
velocity    = (sent_7d - sent_30d) * 4.0
is_significant = abs(shift_score) > 0.3
label: shift_score > 0.3 → TURNING_BULLISH
       shift_score < -0.3 → TURNING_BEARISH
       else → STABLE
```

Quantitative claim unit resolution:
```
B or billion → value * 1000, unit = "$M"
M or million → value * 1,   unit = "$M"
else         → value,        unit = "units"
```

### D) DATA FLOWS

**Inputs:** NewsAPI (requires `api_keys.news_api`), direct URL fetch for full articles, `archive.org` fallback.

**DB Tables Written:** `output/permanent_archive.db`
- `raw_articles`: `url`, `fetch_date`, `source`, `ticker_context`, `full_text`, `word_count`, `title`, `author`, `publication_date`, `is_paywalled=0`, `fetch_method='direct'`, `all_tickers_mentioned`, `sentiment_score=0.0`
- `source_credibility`: `domain`, `tier`, `base_score`, `current_score`, `n_verified`, `n_accurate`, `last_updated` — pre-populated with 18 known domains at init.
- `narrative_shifts`: `ticker_or_sector`, `date`, `sentiment_7d`, `sentiment_30d`, `sentiment_90d`, `narrative_shift_score`, `narrative_velocity`, `is_significant`, `calculated_at`
- `quantitative_claims`: `article_id`, `claim_text`, `claim_type`, `claimed_value`, `claimed_unit`, `claimed_timeframe`, `company_name`, `ticker`, `speaker`, `source_domain`, `credibility_score`, `claim_date`, `was_verified=0`, `actual_value=NULL`, `error_pct=NULL`, `extracted_at`
- `article_connections`: `article_id_a`, `article_id_b`, `connection_type`, `connection_strength`, `discovered_at`

**DB Tables Read:**
- `raw_articles` — for narrative shift computation (sentiment_score, source, fetch_date)
- `source_credibility` — for credibility weighting

### E) DEPENDENCIES

Internal: None.
External: `requests`, `yaml`, `sqlite3`, `re`, `json`, `bs4` (optional), `vaderSentiment` (via `news_context_enricher.py` pattern — not used here directly; sentiment_score stored as 0.0).

### F) WIRING STATUS

`get_narrative_shift()` is a clearly intended signal method. The `narrative_shifts` table is populated on each `collect_and_analyse()` call. No verified import chain to live trading signal consumer found in this session. `sentiment_score` stored in `raw_articles` is always `0.0` — the actual sentiment is never computed by this class, only by `NewsContextEnricher`. **PARTIAL — data collection works, narrative shift math is implemented, but `sentiment_score=0.0` renders the narrative shift metrics meaningless.**

### G) ISSUES FOUND

1. **`sentiment_score` is always written as `0.0` to `raw_articles`.** The `_store_article` call hardcodes `sentiment_score=0.0`. The narrative shift tracker then reads `sentiment_score` from `raw_articles` and computes credibility-weighted averages — all of which will be 0.0. `narrative_shift_score`, `sentiment_7d`, `sentiment_30d`, `sentiment_90d` are all mathematically guaranteed to be 0.0. The narrative shift system is non-functional.

2. **`all_tickers_mentioned` is written as the `ticker` parameter** (not a list of all tickers in the article body). The field name implies a list of all tickers mentioned, but the stored value is simply the single ticker being processed.

3. **`claimed_timeframe` is always stored as empty string `""`** — the `claimed_timeframe` field in the INSERT is hardcoded as `""`. The extractor never populates it.

4. **`article_connections.connection_strength` is hardcoded to `0.7`** for every connection regardless of actual textual similarity.

5. **No deduplication guard on `raw_articles`.** Multiple calls for the same URL (same ticker, different run) will create duplicate rows.

6. **`_fetch_with_archive_fallback` constructs the archive URL as `f"https://web.archive.org/web/{url}"`** — this is missing the timestamp component. Valid archive URLs require `https://web.archive.org/web/{timestamp}/{url}`. The fallback will hit the archive redirect system but may not find the correct snapshot.

---

## FILE 7: data/collectors/news_context_enricher.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/news_context_enricher.py`

### A) PURPOSE
Stateless enrichment utility. Attaches `sentiment_score`, `relevance_score`, `financial_context_score`, and `categories` to raw article dicts in-place. Used as a post-processor on article lists returned by other collectors.

### B) CLASSES AND METHODS

**Module-level helpers:**
- `_simple_sentiment(text) -> float` — Positive/negative word count: `(pos - neg) / (pos + neg)`. Range [-1, +1]. Falls back when VADER unavailable.
- `_vader_sentiment(text) -> float` — Tries `vaderSentiment.SentimentIntensityAnalyzer().polarity_scores(text)["compound"]`. Falls back to `_simple_sentiment` on ImportError.
- `_relevance(text, ticker) -> float` — Counts ticker name occurrences (×0.3 each) plus 9 financial keyword occurrences (×0.05 each). `min(1.0, result)`.
- `_detect_categories(text) -> List[str]` — Matches against 8 category keyword sets: earnings, guidance, m_and_a, dividend, insider, macro, legal, product.

**Class: `NewsContextEnricher`**

- `enrich(ticker, articles) -> List[dict]` — Calls `_enrich_one` for each article. Returns enriched list.
- `_enrich_one(ticker, article) -> dict` — Extracts text from title/summary/description/body/text/content fields. Computes `sentiment`, `relevance`, `categories`. Computes `composite`. Adds 5 keys to article dict in-place.
- `_extract_text(article) -> str` — Joins all non-empty string values from keys: `title`, `summary`, `description`, `body`, `text`, `content`.
- `score_for_ticker(ticker, articles) -> float` — Enriches all articles, then computes linearly-weighted average of `financial_context_score` (most recent article = highest weight `n`).

### C) MATHEMATICS

Simple sentiment:
```
pos = |words ∩ _POSITIVE_WORDS|
neg = |words ∩ _NEGATIVE_WORDS|
sentiment = (pos - neg) / (pos + neg)   [0.0 if pos+neg=0]
```

Relevance:
```
relevance = min(1.0, ticker_hits * 0.3 + keyword_hits * 0.05)
```

Composite (financial_context_score):
```
composite = sentiment * 0.6 + relevance * 0.4 * (1 if sentiment >= 0 else -1)
```

Recency-weighted average:
```
weights = [1, 2, 3, ..., n]   (most recent = weight n)
score = sum(score_i * weight_i) / sum(weights)
```

### D) DATA FLOWS

**Inputs:** List of article dicts (in-memory, from any collector).
**DB Table Written:** None.
**DB Table Read:** None.
**Outputs:** Enriched dicts with added keys `sentiment_score`, `relevance_score`, `financial_context_score`, `categories`, `_enriched`.

### E) DEPENDENCIES

Internal: None.
External: `vaderSentiment` (optional), `re`.

### F) WIRING STATUS

This is a utility class — callers must import it explicitly. It does not write to DB and returns enriched dicts for the caller to process. No direct DB persistence. **CONNECTED as a utility** — the class is designed to be imported by other collectors or signal generators. Whether it is actively called in the live path depends on importers.

### G) ISSUES FOUND

1. **`_simple_sentiment` uses a `set` for words** (`words = set(re.findall(...))`) — this means each unique word is counted at most once, even if it appears 10 times. A repeated negative word like "loss loss loss" scores the same as a single occurrence.

2. **Composite formula sign logic is non-standard:** when sentiment is negative, relevance is subtracted: `relevance * 0.4 * -1`. This means a highly relevant but negative article scores more negative than a less-relevant negative article, which is intentional — but the formula asymmetrically treats positive sentiment (add relevance) vs negative sentiment (subtract relevance), not a true composite.

3. **VADER instantiation inside `_vader_sentiment`** creates a new `SentimentIntensityAnalyzer` on every call. At 10 articles per ticker × N tickers, this is expensive. No caching.

4. **`_relevance` counts `ticker_name = ticker.split(".")[0].lower()`** — for a ticker like `AAPL`, it counts occurrences of `"aapl"` in the text. Most articles will spell out "Apple" not "AAPL", so relevance will consistently undercount.

---

## FILE 8: data/collectors/openbb_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/openbb_collector.py`

### A) PURPOSE
Wraps the OpenBB Platform SDK to fetch analyst consensus estimates, economic calendar events, and analyst price target history. Degrades gracefully if `openbb` is not installed.

### B) CLASSES AND METHODS

**Class: `OpenBBCollector`**

- `__init__(config)` — Tries `from openbb import obb`. Sets `self.enabled = True/False`.
- `get_analyst_estimates(ticker) -> Dict` — Calls `obb.equity.estimates.consensus(symbol=ticker, provider='yfinance')`. Returns `{mean_target, high_target, low_target, n_analysts, recommendation, source, fetched_at}` or `{}`.
- `get_economic_calendar(days_ahead=7) -> List[Dict]` — Calls `obb.economy.calendar(start_date, end_date)`. Returns list of `{date, event, country, importance, forecast, previous}`.
- `get_analyst_target_changes(ticker) -> List[Dict]` — Calls `obb.equity.estimates.price_target(symbol=ticker, provider='yfinance', limit=20)`. Returns list of `{date, analyst, firm, old_target, new_target, rating}`.

### C) MATHEMATICS
None. Pure data retrieval.

### D) DATA FLOWS

**Inputs:** OpenBB SDK (wraps yfinance for equity estimates).
**DB Table Written:** None.
**DB Table Read:** None.
**Outputs:** Python dicts/lists returned to caller.

### E) DEPENDENCIES

Internal: None.
External: `openbb` (optional), `datetime`.

### F) WIRING STATUS

No DB writes. Returns data to caller in memory. **DATA DEAD END — unless the caller persists the results, they are ephemeral. No confirmed wiring to live signal path.**

### G) ISSUES FOUND

1. **`openbb` package is optional but the module gracefully degrades.** If not installed, all methods return `{}` or `[]`. No warning is raised during actual method calls — callers receive empty data silently.

2. **`provider='yfinance'` for analyst estimates** — yfinance does not reliably provide analyst consensus data for all tickers, and its data coverage for UK tickers is sparse.

3. **`get_economic_calendar` returns events for all countries** but the caller receives no filtering mechanism by market/country.

---

## FILE 9: data/collectors/polygon_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/polygon_collector.py`

### A) PURPOSE
Fetches daily OHLCV bars and previous-close prices from Polygon.io (or Massive.com, which is checked first). Free tier only provides previous-day data.

### B) CLASSES AND METHODS

**Class: `PolygonCollector`**

- `__init__(config)` — Gets `api_keys.polygon`. Checks that key exists and doesn't contain "PASTE". Calls `_find_base()` if enabled.
- `_find_base() -> str` — Tries `https://api.massive.com` first, then `https://api.polygon.io`. Tests with a known AAPL date range. Returns first responding base URL.
- `get_daily_bars(ticker, days_back=30) -> pd.DataFrame` — GET `{base}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}`. Parses `results[]`. Renames columns `{o,h,l,c,v}` → `{open,high,low,close,volume}`. Sets date index from millisecond timestamp `t`. Returns empty DataFrame on error.
- `get_previous_close(ticker) -> Optional[float]` — GET `{base}/v2/aggs/ticker/{ticker}/prev`. Returns `results[0]['c']` as float.

### C) MATHEMATICS
None. Pure OHLCV retrieval.

`t_mean = (temp_max + temp_min) / 2.0` (note: this formula appears in geographic_intelligence.py, not polygon_collector.py — included there)

### D) DATA FLOWS

**Inputs:** Polygon.io or Massive.com REST API. Requires `api_keys.polygon`.
**DB Table Written:** None.
**DB Table Read:** None.
**Outputs:** Pandas DataFrame or float, returned in memory.

### E) DEPENDENCIES

Internal: None.
External: `requests`, `pandas`, `datetime`.

### F) WIRING STATUS

No DB writes. Returns DataFrames to caller. **DATA DEAD END at this level — depends entirely on callers persisting or consuming the data.**

### G) ISSUES FOUND

1. **`_find_base()` makes a live API call to AAPL in the constructor.** If Polygon API is down at startup, `_find_base` returns the fallback URL without verifying it works. Initialisation also incurs a network call for every restart.

2. **`api.massive.com` is checked before `api.polygon.io`** — if Massive.com returns HTTP 200 for any reason but with wrong data structure, it will be selected as base without validation of the response body.

3. **`get_daily_bars` does not handle the Polygon free-tier limitation** that only returns the previous day's data. Requesting 30 days back will return at most 1 day on a free key, silently.

---

## FILE 10: data/collectors/quiver_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/quiver_collector.py`

### A) PURPOSE
Fetches congressional trading data, government contracts, WallStreetBets mentions, lobbying data, and patent data from QuiverQuantitative API. Requires paid API key. Writes congressional trades and government contracts to `permanent_archive.db`.

### B) CLASSES AND METHODS

**Class: `QuiverCollector`**

- `__init__(config)` — Gets `api_keys.quiver_quant`. Enables only if key present and not containing "PASTE". Calls `_ensure_db()`.
- `_ensure_db()` — DB WRITE. Creates `raw_congressional_trades` and `raw_government_contracts` tables in `output/permanent_archive.db`.
- `get_congressional_trades(ticker=None) -> List[Dict]` — GET `beta/historical/congresstrading/{ticker}` or `beta/live/congresstrading`. Enriches with `days_to_disclose`, `urgency_flag` (True if < 10 days), `politician_score` (base 0.5, +0.2 if urgent). No DB write.
- `get_government_contracts(ticker=None) -> List[Dict]` — GET contracts endpoint. Uses yfinance for market cap lookup. Computes `contract_pct_of_mcap`. Assigns signal: VERY_STRONG (>50%), STRONG (>10%), MODERATE (>2%), WEAK (<2%). No DB write.
- `get_wsb_mentions(ticker=None) -> List[Dict]` — GET WSB data. Computes `mention_velocity = recent_avg_7d / hist_avg`. Classifies: EXTREME (>10), HIGH (>5), ELEVATED (>2), FADING (<0.5), NORMAL. No DB write.
- `get_lobbying(ticker=None) -> List[Dict]` — GET lobbying endpoint. Returns raw list. No DB write.
- `get_patents(ticker) -> List[Dict]` — GET patents endpoint. Returns raw list. No DB write.

### C) MATHEMATICS

Days-to-disclose urgency:
```
days_to_disclose = (disclosure_date - transaction_date).days
urgency = days_to_disclose < 10
politician_score = 0.5 + (0.2 if urgency else 0.0)
```

Contract signal thresholds:
```
pct = amount / market_cap
pct > 0.50 → VERY_STRONG, score=0.9
pct > 0.10 → STRONG,      score=0.7
pct > 0.02 → MODERATE,    score=0.4
else       → WEAK,         score=0.1
```

WSB velocity:
```
hist_avg   = mean(all_mentions)
recent_avg = mean(last_7_mentions)
velocity   = recent_avg / max(hist_avg, 1)
```

### D) DATA FLOWS

**Inputs:** QuiverQuantitative API (paid, `Authorization: Token {key}`).

**DB Tables Written:** `output/permanent_archive.db`
- `raw_congressional_trades`: `id`, `ticker`, `politician`, `transaction_date`, `disclosure_date`, `amount`, `transaction_type`, `days_to_disclose`, `urgency_flag`, `politician_score`, `fetched_at` — **created but NEVER written to by any method in this file.**
- `raw_government_contracts`: `id`, `ticker`, `amount`, `contract_pct_of_mcap`, `contract_signal`, `signal_score`, `description`, `award_date`, `fetched_at` — **created but NEVER written to by any method in this file.**

**DB Tables Read:** None directly. yfinance used for market cap.

### E) DEPENDENCIES

Internal: None.
External: `requests`, `sqlite3`, `pandas`, `yfinance` (in `get_government_contracts` for market cap), `datetime`.

### F) WIRING STATUS

All methods return in-memory lists. No method writes to the DB tables it creates. **DATA DEAD END — DB tables exist but are never populated. Data returned in memory only.**

### G) ISSUES FOUND

1. **DB tables are created in `_ensure_db()` but never written to by any method.** `get_congressional_trades()`, `get_government_contracts()`, etc., all return lists to the caller without persisting to DB. The schema exists but the tables remain empty.

2. **`yfinance` import is inside the method body** (`get_government_contracts`) — if yfinance is not installed, the market cap lookup silently defaults to `1e9` (1 billion). Every government contract will be evaluated against a $1B market cap if yfinance fails.

3. **`politician_score` is a flat 0.5 for non-urgent trades and 0.7 for urgent.** No differentiation by politician track record, committee membership, or trade size.

4. **`get_lobbying` and `get_patents` return raw lists** with zero enrichment. No signal computation is applied.

5. **Conflict with `social_influence_tracker.py`**: Both files create a `raw_congressional_trades` table but with **different schemas**. `quiver_collector.py` schema: `(ticker, politician, transaction_date, disclosure_date, amount, transaction_type, days_to_disclose, urgency_flag, politician_score, fetched_at)`. `social_influence_tracker.py` schema: `(member_name, ticker, transaction_date, transaction_type, amount_range, disclosure_date, sector, correlated_trade, raw_json, fetched_at)`. Both use `CREATE TABLE IF NOT EXISTS` — whichever runs first wins. The other will silently use the wrong schema.

---

## FILE 11: data/collectors/regulatory_intelligence.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/regulatory_intelligence.py`

### A) PURPOSE
Monitors four regulatory sources: SEC EDGAR comment letters, FDA warning letters RSS feed, EPA ECHO database violations, and OSHA violations (via NewsAPI search). Matches company names to trading universe tickers using fuzzy matching. Stores all alerts to `permanent_archive.db`. Exposes a `get_signal_modifier()` method returning a position multiplier.

### B) CLASSES AND METHODS

**Module-level helpers:**
- `_load_config()`, `_get_conn()`, `_init_db(conn)` — Standard pattern. Creates `regulatory_alerts` table and `idx_ra_ticker` index.
- `_load_universe_tickers() -> List[str]` — Reads from `data/universe_us_tier1.csv` (one ticker per line, no header).
- `_match_company_to_ticker(company_name, tickers) -> Optional[str]` — Two-pass fuzzy match: (1) check if any ticker is a substring of company_name (case-insensitive); (2) check if first 4 chars of company_name match start of any ticker.
- `_determine_signal(alert_type, alert_count=1) -> str` — Maps: SEC_COMMENT → WARNING, FDA_WARNING → SUPPRESS_LONG, EPA_VIOLATION → WARNING, OSHA_VIOLATION → WARNING. If `alert_count > 1` → STRONG_NEGATIVE.
- `_determine_severity(alert_type, context) -> str` — FDA_WARNING → HIGH. SEC_COMMENT → HIGH if going concern/internal control/related party in context, else MEDIUM. EPA_VIOLATION → HIGH if "multiple" in context, else MEDIUM. Default → MEDIUM.

**Class: `SECCommentLetterMonitor`**

- `__init__(conn, tickers)`.
- `collect() -> List[Dict]` — GET SEC EFTS for `"comment+letter"` forms=UPLOAD last 30 days. Fuzzy-matches company names. DB WRITE to `regulatory_alerts` with `alert_type="SEC_COMMENT"`. Returns matched alerts list.

**Class: `FDAWarningLetterMonitor`**

- `__init__(conn, tickers)`.
- `collect() -> List[Dict]` — GET FDA RSS `https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/warning-letters/rss.xml`. Parses XML. Fuzzy-matches item `<title>` as company name. DB WRITE to `regulatory_alerts` with `alert_type="FDA_WARNING"`, `signal_generated="SUPPRESS_LONG"`.

**Class: `EPAViolationMonitor`**

- `__init__(conn, tickers)`.
- `collect() -> List[Dict]` — GET EPA ECHO `echo.epa.gov/api/echo/cwa_rest_services` with `p_vio_flag=Y` and date cutoff 90 days. Parses facilities list. Fuzzy-matches `FacilityName`. DB WRITE to `regulatory_alerts`.

**Class: `OSHAViolationMonitor`**

- `__init__(conn, tickers, news_api_key)`.
- `_search_news(ticker) -> List[Dict]` — GET NewsAPI for `"{ticker} OSHA violation"`. 5 articles per ticker.
- `collect(tickers_to_check=None)` — Checks up to 50 tickers (cap to avoid rate limits). For each article: only stores if "osha" appears in title or description. DB WRITE to `regulatory_alerts` with `alert_type="OSHA_VIOLATION"`. 0.2s sleep between tickers.

**Class: `RegulatoryIntelligence`** (orchestrator)

- `__init__()` — Loads config, DB, universe tickers, creates all four monitors.
- `collect_all(tickers=None) -> Dict` — Runs all four monitors. Returns summary dict.
- `get_alerts_for_ticker(ticker) -> List[Dict]` — DB READ from `regulatory_alerts` WHERE ticker and `alert_date >= 90 days ago`.
- `get_signal_modifier(ticker) -> float` — Returns 0.0 if SUPPRESS_LONG or STRONG_NEGATIVE in last 90 days. Returns 0.7 if WARNING. Returns 1.0 if no alerts.

### C) MATHEMATICS

Signal modifier:
```
SUPPRESS_LONG or STRONG_NEGATIVE present → 0.0
WARNING present                          → 0.7
No alerts                                → 1.0
```

### D) DATA FLOWS

**Inputs:** SEC EFTS API, FDA RSS XML, EPA ECHO JSON API, NewsAPI (for OSHA).

**DB Table Written:** `output/permanent_archive.db` :: `regulatory_alerts`
- Columns: `id`, `company`, `ticker`, `alert_type`, `alert_date`, `alert_severity`, `description`, `source_url`, `signal_generated`, `fetched_at`
- Index: `idx_ra_ticker` on `(ticker, alert_date)`

**DB Table Read:** Same `regulatory_alerts` table in `get_alerts_for_ticker()` and `get_signal_modifier()`.

**Universe CSV Read:** `data/universe_us_tier1.csv` at init.

### E) DEPENDENCIES

Internal: None.
External: `requests`, `yaml`, `sqlite3`, `xml.etree.ElementTree`, `csv`.

### F) WIRING STATUS

`get_signal_modifier()` is a clear live-trading signal interface. The modifier (0.0/0.7/1.0) is designed to suppress positions in companies with active regulatory alerts. **PARTIALLY CONNECTED — signal interface exists and is well-structured, but whether it is called by the signal layer during live trading requires verification of the orchestrator import chain.**

### G) ISSUES FOUND

1. **`_match_company_to_ticker` first-pass check** tests if any ticker is a **substring** of the company name: `if t in name_upper`. For a ticker like "A" (Agilent Technologies), this will match virtually any company name containing the letter "A". False positive rate is very high for short tickers.

2. **FDA `<title>` is the company name (recipient)** — FDA warning letters' RSS `<title>` typically contains the recipient company name, which is often a subsidiary or facility name, not the parent company ticker. The fuzzy match will fail for most real alerts.

3. **SEC comment letter search uses `forms=UPLOAD`** — SEC UPLOAD form type is used for EDGAR submissions uploaded by staff, which includes comment letters but also many other documents. The results will include non-comment-letter documents.

4. **EPA ECHO API URL** includes hardcoded `p_st=US` — no UK coverage. EPA violations for UK-listed companies in the Apollo universe will never be found.

5. **OSHA monitor is limited to 50 tickers** per run (`check_list = tickers_to_check or self.tickers[:50]`) even if the universe is larger.

6. **No deduplication on `regulatory_alerts`** — the table has no UNIQUE constraint. Every `collect_all()` call can insert duplicate alert rows for the same event.

7. **`_determine_signal(alert_count)` parameter is only used if `alert_count > 1`**, but the callers always pass `alert_count=1` (the default). The STRONG_NEGATIVE path is never triggered during normal operation.

---

## FILE 12: data/collectors/short_interest_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/short_interest_collector.py`

### A) PURPOSE
Fetches biweekly FINRA short interest data from NASDAQ's public API. Computes a `short_squeeze_score` and stores results in `closeloop.db`. Provides methods to block shorts when days-to-cover exceeds a threshold.

### B) CLASSES AND METHODS

**Class: `ShortInterestCollector`**

- `__init__(db_path=_DB_PATH)` — Uses `closeloop/storage/closeloop.db`. Calls `_ensure_table()`.
- `_conn()` — Opens SQLite with WAL, busy_timeout=30000.
- `_ensure_table()` — DB WRITE. Creates `short_interest` table.
- `fetch_ticker(ticker) -> Optional[Dict]` — Uses `urllib.request` (not requests). GET `https://api.nasdaq.com/api/quote/{ticker}/short-interest` with browser-like headers. Parses `data.shortInterestTable.rows[0]` (most recent row). Extracts `interest` (short shares), `avgDailyShareVolume`, `daysToCover`. Returns dict or None.
- `_parse_num(s) -> float` — `float(str(s).replace(",","").replace("%","").strip())`.
- `collect(tickers, delay=1.0) -> int` — Iterates tickers with `delay` sleep. Returns count stored.
- `_store(record)` — DB WRITE. `INSERT OR REPLACE` into `short_interest`.
- `get_squeeze_score(ticker) -> Optional[float]` — DB READ. Returns most recent `short_squeeze_score`.
- `get_days_to_cover(ticker) -> Optional[float]` — DB READ. Returns most recent `days_to_cover`.
- `should_block_short(ticker, threshold=5.0) -> bool` — Returns `days_to_cover > threshold`.
- `status() -> Dict` — DB READ. Returns `{tickers_tracked, last_updated}`.

### C) MATHEMATICS

Short squeeze score as described in docstring:
```
short_squeeze_score = (short_interest / float_shares) * (1 / max(days_to_cover, 0.1))
```

**Actual code:**
```python
short_ratio  = 0.0          # ALWAYS zero — float_shares unavailable from NASDAQ API
float_shares = 0.0          # ALWAYS zero
squeeze_score = (short_ratio * (1.0 / max(days_cover, 0.1)))
# = 0.0 * anything = 0.0 ALWAYS
```

### D) DATA FLOWS

**Inputs:** NASDAQ public API (no key). Browser-like headers to avoid bot detection.

**DB Table Written:** `closeloop/storage/closeloop.db` :: `short_interest`
- Columns: `id`, `ticker`, `report_date`, `short_interest`, `float_shares`, `short_ratio`, `days_to_cover`, `short_squeeze_score`, `updated_at`
- Constraint: `UNIQUE(ticker, report_date)`

**DB Table Read:** Same `short_interest` in `get_squeeze_score()`, `get_days_to_cover()`, `status()`.

### E) DEPENDENCIES

Internal: None.
External: `urllib.request` (stdlib), `sqlite3`, `datetime`.

### F) WIRING STATUS

`should_block_short()` and `get_days_to_cover()` are designed as risk management signals. `closeloop.db` is the live trading database. **CONNECTED to live path via closeloop.db** — this is the live risk management DB.

### G) ISSUES FOUND

1. **`short_squeeze_score` is ALWAYS 0.0.** The docstring formula requires `short_interest / float_shares`, but the code explicitly sets `short_ratio = 0.0` and `float_shares = 0.0` with the comment "shortPercentOfFloat not in NASDAQ API — float_shares unavailable". The `squeeze_score = short_ratio * (1/max(days_cover, 0.1)) = 0.0 * anything = 0.0`. Every ticker stored has `short_squeeze_score = 0.0`. `get_squeeze_score()` always returns 0.0. This is a silent dead metric.

2. **The only functional metric is `days_to_cover`** — computed as `short_interest / avg_daily_volume` (fallback to API's `daysToCover` field). `should_block_short()` works correctly based on this.

3. **`.L` suffix stripping for UK tickers** — `ticker.replace(".L", "")` is applied before the NASDAQ API call. UK tickers will query NASDAQ which covers US stocks only. Results for UK tickers will be empty and silently skipped.

4. **NASDAQ API is not an official FINRA data endpoint** — it is a public-facing website API that may change structure or add bot detection without notice. The headers attempt to mimic a browser.

---

## FILE 13: data/collectors/simfin_collector.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/simfin_collector.py`

### A) PURPOSE
Fetches 10 years of annual fundamental data (income statements, balance sheets, cash flows) from SimFin's bulk CSV download. Calculates an earnings quality score from 4 metrics: accruals ratio, FCF conversion, revenue consistency, and gross margin trend. Stores results in `permanent_archive.db`.

### B) CLASSES AND METHODS

**Class: `SimFinCollector`**

- `__init__(config)` — Gets `api_keys.simfin`. Disabled if key missing or contains "PASTE". Calls `_init_simfin()` and `_ensure_db()`.
- `_init_simfin()` — Calls `sf.set_api_key()`, `sf.set_data_dir('/tmp/simfin_data')`. Downloads bulk data on first call.
- `_load_income(market='us') -> pd.DataFrame` — Lazy-loads and caches `sf.load_income(variant='annual', market=market)`.
- `_load_balance(market='us') -> pd.DataFrame` — Same pattern.
- `_load_cashflow(market='us') -> pd.DataFrame` — Same pattern.
- `_filter_ticker(df, ticker) -> pd.DataFrame` — Handles MultiIndex `(Ticker, Date)` or plain `Ticker` column. Returns filtered rows.
- `get_income_statement(ticker) -> pd.DataFrame` — Filter + return.
- `get_balance_sheet(ticker) -> pd.DataFrame` — Filter + return.
- `get_cashflow(ticker) -> pd.DataFrame` — Filter + return.
- `_ensure_db()` — DB WRITE. Creates `earnings_quality` table in `output/permanent_archive.db`.
- `calculate_earnings_quality(ticker) -> Dict` — Computes 4 component scores. DB WRITE. Returns `{earnings_quality_score, quality_tier, key_metrics, quality_reasoning}`.
- `bulk_quality_scan(tickers) -> Dict[str, Dict]` — Calls `calculate_earnings_quality` for each ticker.

### C) MATHEMATICS

Accruals ratio:
```
accruals = Net Income (latest) - Operating Cash Flow (latest)
accruals_ratio = |accruals| / max(|Total Assets (latest)|, 1)
accruals_score = 0.8 if ratio < 0.05 else (0.3 if ratio > 0.10 else 0.55)
```

FCF conversion:
```
fcf_conversion = Operating Cash Flow / max(|Net Income|, 1)
fcf_score = 0.9 if fcf > 1.0 else (0.3 if fcf < 0.5 else 0.6)
```

ROIC (note: uses Total Assets, not invested capital):
```
roic = Net Income / max(|Total Assets|, 1)
roic_score = 0.8 if roic > 0.15 else (0.3 if roic < 0.05 else 0.55)
```

Revenue consistency:
```
rev_std = std(pct_change(Revenue))   [requires >= 4 years of data]
rev_score = 0.8 if rev_std < 0.05 else (0.2 if rev_std > 0.20 else 0.5)
```

Gross margin trend:
```
margins = Gross Profit / Revenue  [array over time]
trend   = margins[-1] - mean(margins[:-1])
margin_score = 0.7 if trend > 0 else 0.3
```

Final earnings quality score (weighted sum):
```
eq_score = accruals_score * 0.30
         + fcf_score      * 0.25
         + rev_score      * 0.20
         + margin_score   * 0.15
         + roic_score     * 0.10

tier = 'HIGH' if eq_score > 0.7 else ('LOW' if eq_score < 0.4 else 'MEDIUM')
```

### D) DATA FLOWS

**Inputs:** SimFin bulk CSV download to `/tmp/simfin_data`. Requires `api_keys.simfin`.

**DB Table Written:** `output/permanent_archive.db` :: `earnings_quality`
- Columns: `id`, `ticker`, `quality_score`, `quality_tier`, `accruals_ratio`, `fcf_conversion`, `revenue_consistency`, `margin_trend`, `roic`, `calculated_at`
- Constraint: `UNIQUE(ticker)` with `INSERT OR REPLACE`

**DB Table Read:** None (reads from in-memory DataFrames loaded from SimFin bulk files).

### E) DEPENDENCIES

Internal: None.
External: `simfin` (optional), `pandas`, `sqlite3`, `warnings`.

### F) WIRING STATUS

`earnings_quality` table is written to `permanent_archive.db`. `calculate_earnings_quality()` is a clearly designed signal input. Whether any signal generator reads the `earnings_quality` table was not confirmed. **PARTIAL — data is persisted but downstream consumption unverified.**

### G) ISSUES FOUND

1. **"ROIC" is computed as `Net Income / Total Assets`** — this is actually Return on Assets (ROA), not Return on Invested Capital (ROIC). True ROIC = NOPAT / (Debt + Equity). The metric is mislabeled throughout (variable names, column names, docstring).

2. **`_load_income()` caches only the US market bulk file** in `self._income_us`. If called with `market='uk'`, the caching variable `self._income_us` stores UK data on first UK call and US data is then returned on next US call (or vice versa — the cache key is not market-aware).

3. **`bulk_quality_scan` triggers one bulk download per ticker** (via `_filter_ticker`) but the bulk DataFrames are cached at class level — this is actually efficient. However, `/tmp/simfin_data` is ephemeral on most systems and may be cleared between runs, forcing a full re-download.

4. **Revenue consistency requires >= 4 years of data** but uses `std(pct_change)` which requires at least 5 data points (4 pct_change values) for meaningful std. No guard against `pct_change()` returning NaN-heavy series.

5. **`accruals_ratio` uses `len(ni) > 0 and len(cf) > 0 and len(ta) > 0`** but takes only `iloc[-1]` (single year). The accruals calculation is a single-year snapshot, not an average — making it sensitive to outlier years.

---

## FILE 14: data/collectors/social_influence_tracker.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/social_influence_tracker.py`

### A) PURPOSE
Four sub-collectors in one file: Fed speech hawkish/dovish scorer (Federal Reserve RSS), Congressional trades monitor (House Stock Watcher S3), CEO forward-looking statement extractor (NewsAPI), and influencer mention tracker (Elon Musk via NewsAPI). Master orchestrator: `SocialInfluenceTracker`. All data stored in `permanent_archive.db`.

### B) CLASSES AND METHODS

**Module-level helpers:**
- `_load_config()`, `_get_conn()`, `_init_db(conn)` — Standard. Creates tables: `social_influence`, `ceo_credibility`, `raw_congressional_trades`, `raw_articles`.
- `_score_hawkish_dovish(text) -> Dict` — Counts hawkish keywords (7) and dovish keywords (7). Returns `{hawkish_score, dovish_score, net_score}`.
- `_extract_tickers(text) -> List[str]` — Regex `\b([A-Z]{1,5})\b` with stop-word filter of 21 common acronyms.
- `_extract_companies(text) -> List[str]` — Regex for capitalized multi-word phrases with optional corporate suffixes. Returns up to 20 unique matches > 3 chars.
- `_infer_sector_from_text(text) -> str` — Best-matching sector from 6 sector keyword sets. Returns "general" if no match.
- `_parse_rss(url) -> List[Dict]` — Fetches and parses RSS XML. Strips XML namespaces from tag names.
- `_fetch_news(api_key, query, days_back=7, page_size=100) -> List[Dict]` — NewsAPI with in-memory daily cache per query. Returns `[]` on 429.

**Class: `FedSpeechCollector`**

- `__init__(config)` — Gets `api_keys.news_api`. Defines 2 RSS feeds (Fed Board, NY Fed).
- `_get_full_text(url) -> str` — GET URL, strip HTML tags with regex. Cap at 20,000 chars.
- `_parse_speaker_from_rss(item) -> str` — Tries author/dc:creator/creator/title fields.
- `collect(conn) -> List[Dict]` — Fetches both RSS feeds. For each item: parses date (RFC 2822), fetches full text, scores hawkish/dovish, extracts tickers/companies. DB WRITE to `social_influence` with `INSERT OR IGNORE`.
- `get_fed_sentiment(conn) -> Dict` — DB READ last 20 Fed speeches. Computes `current_score`, `avg_7d`, `avg_prev_7d`, `wow_change`, `trend` label (HAWKISH/DOVISH/NEUTRAL + TIGHTENING/EASING modifier).

**Class: `CongressionalTradesMonitor`**

- `__init__(config)` — Gets `api_keys.news_api`. Sets `QUIVERQUANT_BASE` and `HOUSE_WATCHER_URL`.
- `_fetch_house_watcher() -> List[Dict]` — GET House Stock Watcher S3 JSON bulk file.
- `_fetch_quiverquant(ticker) -> List[Dict]` — GET QuiverQuant endpoint (handles 401/403 gracefully).
- `collect(conn) -> List[Dict]` — Fetches House Watcher data. For each trade: extracts member, ticker, dates, type, amount, sector. DB WRITE to `raw_congressional_trades` with `INSERT OR IGNORE`. Returns all stored rows.
- `get_recent_trades(conn, days=30)` — DB READ `raw_congressional_trades` last N days.
- `get_correlated_trades(conn)` — DB READ where `correlated_trade=1`.
- `flag_correlated_trades(conn) -> int` — Reads all unflagged trades. For each, checks `social_influence` table for same member (last-name match) making statements within 30 days. DB WRITE UPDATE if match found. Returns count flagged.

**Class: `CEOStatementTracker`**

- `__init__(config)` — Gets `api_keys.news_api`. Defines 5 search queries and 3 regex patterns.
- `_extract_claims(text) -> List[Dict]` — Applies `_REVENUE_PATTERN`, `_GROWTH_PATTERN`, `_VOLUME_PATTERN` regexes. Returns list of `{claim_type, claim_value, claim_text}`.
- `_extract_timeframe(text) -> str` — Regex for Q1-Q4/year, fiscal year, "next quarter/year". Returns "unspecified" if none found.
- `collect(conn) -> List[Dict]` — 5 NewsAPI queries. For each article: extracts company, claims, tickers, scores. DB WRITE to `social_influence` and `ceo_credibility`.
- `get_credibility_score(conn, company) -> Optional[float]` — DB READ `ceo_credibility` WHERE `was_accurate IS NOT NULL`. Returns `accurate_count / total_claims`.
- `get_ceo_signals(conn, ticker) -> Dict` — DB READ. Joins `ceo_credibility` and `social_influence`. Computes `accuracy_adjustment = 0.7 + 0.6 * credibility`.

**Class: `MuskMonitor`**

- `__init__(config)` — Defines 1 influencer (Elon Musk) with 3 search queries.
- `collect(conn) -> List[Dict]` — NewsAPI queries for Musk mentions. DB WRITE to `social_influence`.
- `get_recent_mentions(conn, person_name, days=7)` — DB READ `social_influence`.
- `get_impact_signal(conn, ticker, person_name) -> Optional[Dict]` — DB READ last 48h mentions where ticker appears in `tickers_mentioned`. Returns `{signal_strength = min(1.0, count * 0.2), direction}`.

**Class: `SocialInfluenceTracker`** (master orchestrator)

- `__init__()` — Creates all 4 sub-collectors.
- `collect() -> Dict` — Runs all 4 collectors. Returns summary with counts.
- `get_fed_sentiment() -> Dict`.
- `get_congressional_signals(ticker) -> Dict` — Counts buys/sells, lists correlated trades.
- `get_ceo_signals(ticker) -> Dict`.
- `get_musk_signal(ticker) -> Optional[Dict]` — Calls `get_impact_signal`.

### C) MATHEMATICS

Hawkish/dovish score:
```
hawkish_score = (sum of keyword occurrences in text) / total_words * 1000
dovish_score  = same for dovish keywords
net_score     = hawkish_score - dovish_score
```

Fed trend classification:
```
avg_7d > 2.0  → HAWKISH
avg_7d < -2.0 → DOVISH
else          → NEUTRAL
(appended with _TIGHTENING if wow_change > 1.0, _EASING if wow_change < -1.0)
```

CEO credibility:
```
credibility = accurate_count / total_verified_claims
accuracy_adjustment = 0.7 + 0.6 * credibility
```
(Range: 0.7 when credibility=0, 1.3 when credibility=1.0)

Musk signal strength:
```
signal_strength = min(1.0, mention_count_48h * 0.2)
```

### D) DATA FLOWS

**Inputs:** Federal Reserve RSS feeds (2 URLs), House Stock Watcher S3 bulk JSON, NewsAPI (for CEO/Musk), QuiverQuant (optional).

**DB Tables Written:** `output/permanent_archive.db`
- `social_influence`: `id`, `person_name`, `role`, `statement_date`, `statement_time`, `platform`, `full_text`, `url`, `source`, `tickers_mentioned`, `companies_mentioned`, `sentiment_score`, `hawkish_dovish_score`, `quantitative_claims`, `market_impact_observed`, `was_statement_accurate`, `fetched_at`. UNIQUE on `(person_name, url, statement_date)`.
- `ceo_credibility`: `id`, `company`, `ceo_name`, `claim_text`, `claim_type`, `claim_value`, `timeframe`, `source`, `claim_date`, `was_accurate`, `credibility_score`, `fetched_at`
- `raw_congressional_trades`: See schema note in ISSUES.
- `raw_articles`: Minimal schema — `id`, `source`, `article_type`, `title`, `description`, `url`, `published_at`, `full_text`, `raw_json`, `collected_at` — **DIFFERENT from other modules' `raw_articles` schema.**

**DB Tables Read:** `social_influence` (for Fed sentiment, correlation checks, CEO/Musk signals), `raw_congressional_trades` (for recent trades), `ceo_credibility` (for credibility scores).

### E) DEPENDENCIES

Internal: None.
External: `requests`, `yaml`, `sqlite3`, `re`, `json`, `xml.etree.ElementTree`.

### F) WIRING STATUS

`get_fed_sentiment()`, `get_congressional_signals()`, `get_ceo_signals()`, `get_musk_signal()` are all live-query methods. Fed hawkish/dovish scoring is a named signal in Apollo (rate sensitivity). **PARTIALLY CONNECTED — data is collected and persisted. Signal methods exist. Full wiring to live trading path not verified in this session.**

### G) ISSUES FOUND

1. **Schema collision on `raw_articles`.** This file's `_init_db` creates `raw_articles` with columns: `source`, `article_type`, `title`, `description`, `url`, `published_at`, `full_text`, `raw_json`, `collected_at`. Other modules (`geopolitical_collector.py`, `advanced_news_intelligence.py`, `news_context_enricher.py`) create `raw_articles` with different columns: `ticker_context`, `publication_date`, `word_count`, `author`, `is_paywalled`, `fetch_method`, `all_tickers_mentioned`, `sentiment_score`. Whichever module's `_init_db` runs first creates the table; subsequent INSERTs from other modules will fail on missing columns or silently insert NULLs into mismatched positions.

2. **Schema collision on `raw_congressional_trades`.** Documented above in quiver_collector.py issue #5.

3. **`sentiment_score` in `social_influence` table is stored as `scores["net_score"]`** (the hawkish/dovish net score) for Fed speeches, and also as `scores["net_score"]` for CEO/Musk articles. The column is named `sentiment_score` but contains a hawkish/dovish index — the two concepts are conflated.

4. **`ceo_name` column in `ceo_credibility` is never populated** — the INSERT omits it.

5. **`flag_correlated_trades` uses last-name matching**: `person_name LIKE f"%{member.split()[-1]}%"`. If a member's last name is "Brown" or "Smith", false positives will be high.

6. **`_NEWS_API_CACHE` and `_NEWS_API_LAST_CALL` are module-level dicts** — they persist for the process lifetime, not just per-call. This means repeated calls within the same day reuse cached results, which is intentional, but also means stale results if the process runs more than 24 hours.

7. **`collect()` in `CEOStatementTracker`** writes `sentiment_score=scores["net_score"]` into `social_influence` — but `net_score` is the hawkish/dovish difference, not a financial sentiment score. Articles about a CEO saying "inflation is elevated" would get a high hawkish score falsely interpreted as financial negativity.

---

## FILE 15: altdata/collector/lunar_collector.py

**File:** `/home/dannyelticala/quant-fund/altdata/collector/lunar_collector.py`

### A) PURPOSE
Computes lunar cycle metrics (phase illumination, days to full/new moon, moon distance, cyclical sin/cos encodings) using the `ephem` astronomical library. Generates a scaled lunar signal based on academic literature (3-4 bp/day effect). Produces the same signal value for all tickers on a given day. No API calls — pure computation.

### B) CLASSES AND METHODS

**Class: `LunarCollector`**

- `__init__(config)` — Stores config. Initialises `_cache: Dict[str, dict]` keyed by date string.
- `get_lunar_data(for_date: date) -> dict` — Checks cache. Computes via `ephem`. Returns full lunar metrics dict. On error: returns neutral defaults with `error` field.
- `precompute_range(start, end)` — Iterates `start` to `end` calling `get_lunar_data()` for each date. Intended for startup pre-warm.
- `collect(tickers, market='us') -> List[dict]` — Gets today's lunar data. Returns one result dict per ticker, all with identical `value=lunar_signal`.

### C) MATHEMATICS

Phase illumination: `phase_raw = moon.phase / 100.0` (ephem returns 0–100)

Cyclical encoding:
```
phase_angle = phase_raw * 2 * π
phase_sin   = sin(phase_angle)
phase_cos   = cos(phase_angle)
```

Lunar signal:
```
lunar_signal (raw)   = cos(phase_angle)
lunar_signal_scaled  = cos(phase_angle) * 0.1
```
Range: [-0.1, +0.1]. Peaks (+0.1) at new moon (phase=0, cos=1). Troughs (-0.1) at full moon (phase=1.0, cos(2π)=1 — wait, see issue #1).

Days calculations:
```
days_to_full    = float(ephem.next_full_moon(d) - d)
days_since_full = float(d - ephem.previous_full_moon(d))
days_from_full  = min(days_to_full, days_since_full)
days_from_new   = min(days_to_new, float(d - prev_new))
```

### D) DATA FLOWS

**Inputs:** `ephem` library (ephemeris computation, no network). No API keys.
**DB Table Written:** None.
**DB Table Read:** None.
**Outputs:** List of dicts with `source="lunar"`, `data_type="lunar_cycle"`, `value=lunar_signal_scaled`, `quality_score=0.3`.

### E) DEPENDENCIES

Internal: None.
External: `ephem` (required), `math`, `datetime`.

### F) WIRING STATUS

Located in `altdata/collector/` — uses the altdata collector interface (`collect(tickers, market)` returning a list of standardised dicts). `quality_score=0.3` marks it as speculative. **CONNECTED to altdata pipeline if the altdata aggregator includes it. No live trading path impact beyond that.**

### G) ISSUES FOUND

1. **The lunar signal formula is mathematically incorrect for its stated intent.** The docstring says "cos peaks at 0 (new moon) and troughs at pi (full moon)." At new moon: `phase_raw ≈ 0`, `phase_angle ≈ 0`, `cos(0) = 1.0` → signal = +0.1 ✓. At full moon: `phase_raw ≈ 1.0`, `phase_angle = 2π`, `cos(2π) = 1.0` → signal = +0.1 also. The full moon does NOT trough — it gives the same value as new moon. The formula `cos(phase_raw * 2π)` is symmetric around both 0 and 2π. To get a true trough at full moon, the formula should use `cos(phase_raw * π)`.

2. **`quality_score=0.3` is explicitly self-labelled as speculative.** The academic effect cited (3-4 bp/day) is not well-supported and the formula produces values up to ±0.1 (~10%), orders of magnitude larger than the cited effect.

3. **All tickers receive the same signal value.** Lunar cycle is not ticker-specific. The `collect()` loop is meaningless overhead — the result is the same dict repeated N times.

---

## FILE 16: altdata/collector/weather_collector.py

**File:** `/home/dannyelticala/quant-fund/altdata/collector/weather_collector.py`

This file is an **exact byte-for-byte duplicate** of `/home/dannyelticala/quant-fund/data/collectors/geographic_intelligence.py`'s `WeatherCollector` class extracted as a standalone module. It defines the same `WeatherCollector` class with identical logic. See `geographic_intelligence.py` section for full documentation.

**Key difference from `geographic_intelligence.py`:** This standalone version does NOT write to any DB and returns standardised altdata dicts. The `geographic_intelligence.py` version writes to `permanent_archive.db` and `historical_db.db`.

**Issues:** Same as documented under `geographic_intelligence.py`, plus: having two versions of the same code creates a maintenance split — changes to one are not reflected in the other.

---

## FILE 17: data/collectors/geographic_intelligence.py

**File:** `/home/dannyelticala/quant-fund/data/collectors/geographic_intelligence.py`

### A) PURPOSE
Full geographic data collection: weather (Open-Meteo historical archive, OWM current), pollen forecast (Open-Meteo air quality), AQI (WAQI), USGS earthquakes, FRED regional economic indicators (CFNAI, MANEMP, STLFSI4, KCFSI), and EIA electricity demand. Covers 13 US and 6 UK cities. Stores to two DBs: `permanent_archive.db` and `historical_db.db`.

### B) CLASSES AND METHODS

**Module-level:**
- `US_LOCATIONS`, `UK_LOCATIONS`, `ALL_LOCATIONS` — 19 city coordinate dicts.
- `_FRED_REGIONAL_SERIES` — 4 FRED series IDs.
- `_HIST_WEATHER_VARS` — 6 Open-Meteo daily variable names.
- `_POLLEN_VARS` — 6 pollen variable names.
- Helper functions: `_load_config()`, `_conn()`, `_init_perm_db()`, `_init_hist_db()`, `_get()`, `_zscore()`, `_mean()`, `_std()`.

**Class: `GeographicIntelligence`**

- `__init__(config)` — Reads `api_keys.openweathermap`, `api_keys.waqi`, `api_keys.fred`, `api_keys.eia`. Initialises `_baselines` cache.
- `_perm_conn()`, `_hist_conn()` — Lazy DB connections with table init.
- `_compute_baseline(location) -> Dict[str, Tuple[float, float]]` — DB READ from `raw_weather_data` WHERE source='OPEN_METEO_HIST' AND date 2010-2021. Computes (mean, std) for `temp_max`, `temp_min`, `temp_mean`, `precipitation`, `windspeed_max`. Cached in `self._baselines`.
- `get_weather_risk(location) -> Optional[float]` — DB READ most recent row. Combines anomalies.
- `_compute_risk_score(temp_anom, precip_anom, wind_z) -> float` — See math section.
- `collect_historical(start_date='2010-01-01') -> Dict` — Backfills all 19 locations from start_date to today via Open-Meteo archive.
- `collect_weather(days_back=7) -> Dict` — Fetches last N days for all 19 locations.
- `_fetch_and_store_meteo(location, country, lat, lon, start_date, end_date, source) -> int` — DB WRITE to both `raw_weather_data` (permanent) and `weather_data` (historical). Computes anomalies vs baseline. Marks `is_extreme=1` if any anomaly > 2.0 std devs.
- `_collect_owm() -> int` — OWM current conditions + forecast for all 19 cities. DB WRITE to `raw_weather_data`.
- `_collect_pollen() -> int` — Open-Meteo air quality API. Aggregates hourly pollen to daily totals (6 pollen types). Computes pollen z-score vs internal baseline. DB WRITE to `raw_weather_data` (source='OPEN_METEO_POLLEN').
- `get_pollen_forecast(location) -> List[Dict]` — DB READ last 14 pollen entries.
- `_collect_waqi() -> int` — WAQI API for 6 cities. Extracts AQI and PM2.5. DB WRITE to `raw_weather_data` (source='WAQI').
- `_collect_earthquakes() -> int` — USGS GeoJSON. Filters mag >= 3.0. Classifies severity: >= 6.5 CRITICAL, >= 5.5 HIGH, >= 4.5 MEDIUM, else LOW. DB WRITE to `raw_geopolitical_events`.
- `get_earthquake_alerts(min_magnitude=4.5) -> List[Dict]` — DB READ.
- `_collect_fred() -> int` — FRED API 4 series from 2000-01-01 to today. DB WRITE to `raw_macro_data`.
- `_collect_eia() -> int` — EIA v2 electricity demand API, last 7 days, length=5000. DB WRITE to `raw_macro_data`.
- `get_extreme_events(threshold=2.0) -> List[Dict]` — DB READ `raw_weather_data` WHERE `is_extreme=1` last 7 days.
- `collect() -> Dict` — Runs all 7 sub-collections. Returns summary.

### C) MATHEMATICS

Z-score:
```
z = (value - mean) / std    [returns None if std == 0]
```

Weather risk score:
```
raw  = |temp_anomaly| * 0.4 + max(precip_anomaly, 0) * 0.35 + max(wind_z, 0) * 0.25
risk = clamp(raw / 4.0, -1.0, +1.0)
```

Temperature mean:
```
t_mean = (temp_max + temp_min) / 2.0
```

Pollen z-score (internal — baseline computed from the same 7-day forecast window):
```
baseline_mean = mean(all 7-day pollen_totals)
baseline_std  = std(all 7-day pollen_totals)
pollen_z = (daily_total - baseline_mean) / baseline_std
```

Extreme event flag:
```
is_extreme = 1 if any(|anomaly| > 2.0 for anomaly in [temp_anom, precip_anom, wind_z])
```

### D) DATA FLOWS

**Inputs:** Open-Meteo archive API (free, no key), Open-Meteo forecast/air-quality API (free), OWM (requires `api_keys.openweathermap`), WAQI (requires `api_keys.waqi`), USGS GeoJSON (free), FRED API (requires `api_keys.fred`), EIA API (requires `api_keys.eia`).

**DB Tables Written:**

`output/permanent_archive.db` :: `raw_weather_data`
- Columns: `id`, `location`, `country`, `latitude`, `longitude`, `date`, `source`, `temp_max`, `temp_min`, `temp_mean`, `precipitation`, `rain`, `snowfall`, `windspeed_max`, `weathercode`, `sunshine_duration`, `temperature_anomaly`, `precip_anomaly`, `windspeed_zscore`, `weather_risk_score`, `is_extreme`, `pollen_total`, `aqi`, `pm25`, `raw_json`, `collected_at`

`output/permanent_archive.db` :: `raw_macro_data`
- Columns: `id`, `series_name`, `series_id`, `date`, `value`, `source`, `raw_json`, `collected_at`. UNIQUE on `(series_name, date)`.

`output/permanent_archive.db` :: `raw_geopolitical_events`
- Columns: `id`, `source`, `event_date`, `event_type`, `title`, `description`, `url`, `goldstein_scale`, `magnitude`, `latitude`, `longitude`, `location`, `severity`, `affected_sectors`, `affected_regions`, `raw_json`, `collected_at`

`output/historical_db.db` :: `weather_data`
- Columns: `id`, `location`, `country`, `date`, `source`, `temp_max`, `temp_min`, `temp_mean`, `precipitation`, `snowfall`, `windspeed_max`, `weathercode`, `temperature_anomaly`, `precip_anomaly`, `weather_risk_score`, `is_extreme`, `collected_at`. UNIQUE on `(location, date, source)`.

**DB Tables Read:** `raw_weather_data` (for baseline computation), `raw_geopolitical_events` (for earthquake alerts).

### E) DEPENDENCIES

Internal: None.
External: `requests`, `yaml`, `sqlite3`, `json`.

### F) WIRING STATUS

`get_weather_risk()` and `get_extreme_events()` are query interfaces. `raw_macro_data` stores FRED and EIA data usable for macro signal generation. `raw_geopolitical_events` is shared with `geopolitical_collector.py`. **PARTIALLY CONNECTED — data infrastructure is rich and well-designed, but downstream signal consumption not verified in this session.**

### G) ISSUES FOUND

1. **Pollen baseline is computed from the same 7-day forecast window** — the z-score baseline (`baseline_mean = mean(all 7-day totals)`) is computed from the exact values being z-scored. This means the z-score will always be near 0 — it is comparing each day's pollen to the mean of those same days, not to a multi-year seasonal baseline.

2. **Schema conflict on `raw_geopolitical_events`** with `geopolitical_collector.py`. `geographic_intelligence.py` adds `event_type` column in its `_init_perm_db`. `geopolitical_collector.py` uses `ALTER TABLE ADD COLUMN` migration for different columns. If schemas diverge at runtime, INSERTs from one module may fail silently on columns that exist in the other's schema but not both.

3. **`_collect_owm` forecast data** is fetched (`_get(forecast_url, ...)`) but the result is never used or stored — the return value is discarded with `_get(...)` result being ignored. Dead code.

4. **`collect_historical()` starting from 2010** on first run will download ~14 years × 19 locations of daily weather data — approximately 100,000+ rows. At 0.3s sleep per location, this takes ~6 seconds for the API calls but the data volume could be significant. No progress indicator or resume capability.

5. **`_collect_earthquakes` uses `INSERT OR IGNORE`** but the table has no UNIQUE constraint on `raw_geopolitical_events` — the IGNORE has no effect; every call will insert all earthquakes again as duplicates.

6. **`sunshine_duration` variable is listed in `_WEATHER_VARS`** but not in `_HIST_WEATHER_VARS`. The `_fetch_and_store_meteo` method uses `_HIST_WEATHER_VARS` which omits `sunshine_duration` — so `sun[i]` will always be `[None] * len(dates)`. The column exists in the table but is always NULL.

---

## SECTION 5 GATE PARTIAL (Second Batch of Collectors)

### Files Read

**Found and fully read:**
1. `/home/dannyelticala/quant-fund/data/collectors/job_postings_collector.py`
2. `/home/dannyelticala/quant-fund/data/collectors/sec_fulltext_collector.py`
3. `/home/dannyelticala/quant-fund/data/collectors/alternative_quiver_collector.py`
4. `/home/dannyelticala/quant-fund/data/collectors/geopolitical_collector.py`
5. `/home/dannyelticala/quant-fund/altdata/collector/finnhub_collector.py`
6. `/home/dannyelticala/quant-fund/data/collectors/advanced_news_intelligence.py`
7. `/home/dannyelticala/quant-fund/data/collectors/news_context_enricher.py`
8. `/home/dannyelticala/quant-fund/data/collectors/openbb_collector.py`
9. `/home/dannyelticala/quant-fund/data/collectors/polygon_collector.py`
10. `/home/dannyelticala/quant-fund/data/collectors/quiver_collector.py`
11. `/home/dannyelticala/quant-fund/data/collectors/regulatory_intelligence.py`
12. `/home/dannyelticala/quant-fund/data/collectors/short_interest_collector.py`
13. `/home/dannyelticala/quant-fund/data/collectors/simfin_collector.py`
14. `/home/dannyelticala/quant-fund/data/collectors/social_influence_tracker.py`
15. `/home/dannyelticala/quant-fund/altdata/collector/lunar_collector.py`
16. `/home/dannyelticala/quant-fund/altdata/collector/weather_collector.py` (duplicate of data/collectors WeatherCollector)
17. `/home/dannyelticala/quant-fund/data/collectors/geographic_intelligence.py`

**Not found (do not exist):**
- `/home/dannyelticala/quant-fund/data/collectors/usa_spending_collector.py` — MISSING
- `/home/dannyelticala/quant-fund/data/collectors/weather_collector.py` — NOT PRESENT (weather_collector is in altdata/collector/ only)

**Additional file found in altdata/collector/ (not on original list):**
- `/home/dannyelticala/quant-fund/altdata/collector/lunar_collector.py` — READ AND DOCUMENTED

---

### Key Findings

**DB path split:** Collectors divide into two DB destinations — `closeloop/storage/closeloop.db` (live trading: job_postings, short_interest) and `output/permanent_archive.db` (archive: everything else). These are separate SQLite files. Signals that should cross-reference both DBs must open both connections independently.

**Pervasive `sentiment_score = 0.0` bug:** `advanced_news_intelligence.py` stores `sentiment_score=0.0` for all articles in `raw_articles`. `narrative_shifts` computations that depend on these values return 0.0. The narrative shift tracker is non-functional.

**Three modules fight over `raw_articles` schema:** `geopolitical_collector.py`, `social_influence_tracker.py`, and `advanced_news_intelligence.py` all CREATE TABLE `raw_articles` with incompatible schemas. Whichever runs first creates the table. The other two fail silently on mismatched columns or insert into wrong positions.

**Two modules fight over `raw_congressional_trades` schema:** `quiver_collector.py` and `social_influence_tracker.py` create different schemas for the same table name. Unresolvable without a migration.

**`short_squeeze_score` is permanently 0.0:** Documented under `short_interest_collector.py` — the formula requires float_shares which is unavailable from the API, resulting in `0.0 * x = 0.0` always.

**`alternative_quiver_collector.py` congressional trade search is broken:** Queries Form 4 EDGAR for Senate/House disclosures — but congressional financial disclosures are not filed as EDGAR Form 4s. The search will not return real congressional trades.

**`quiver_collector.py` creates DB tables it never writes to.** All five data methods return in-memory lists only.

**`lunar_collector.py` signal formula is wrong:** `cos(phase_raw * 2π)` evaluates to the same value (+1.0 → signal +0.1) at both new moon AND full moon. The intended trough at full moon does not occur.

---

### Dead End Collectors

The following collectors in this batch collect data that is never persisted to DB and never consumed by any downstream signal:

- `alternative_quiver_collector.py` — no DB writes, no wiring
- `openbb_collector.py` — no DB writes, returns dicts only
- `polygon_collector.py` — no DB writes, returns DataFrames only
- `quiver_collector.py` — creates tables but never writes to them; returns lists only
- `news_context_enricher.py` — stateless utility; not a dead end per se but requires explicit caller to be useful

The following write to DB but have no verified downstream signal consumer:

- `sec_fulltext_collector.py` — writes `sec_fulltext_alerts` table; no known reader
- `advanced_news_intelligence.py` — writes to `narrative_shifts` but `sentiment_score=0.0` makes it useless

---

### Contradictions

1. **`short_interest_collector.py` docstring formula vs code:** Docstring states `(short_interest / float_shares) * (1 / max(days_to_cover, 0.1))`. Code computes `short_ratio * (1 / max(days_cover, 0.1))` where `short_ratio = 0.0`. Result is always 0.

2. **`job_postings_collector.py` docstring max signal vs code:** Docstring says "+0.2 to +0.4" for aggressive hiring. Code can produce +0.70 (all three conditions true simultaneously).

3. **`lunar_collector.py` comment vs formula:** Comment says "cos peaks at 0 (new moon) and troughs at pi (full moon)." Formula uses `cos(phase_raw * 2π)` which has the same value at both new moon and full moon.

4. **`simfin_collector.py` "ROIC" vs actual formula:** Labeled ROIC but computes `Net Income / Total Assets` = Return on Assets.

5. **`quiver_collector.py` creates `raw_congressional_trades` and `raw_government_contracts`** but never inserts into them. The `social_influence_tracker.py` creates `raw_congressional_trades` with a different schema and actually uses it. The tables created by `quiver_collector.py` are therefore ghost tables.

6. **`alternative_quiver_collector.py`** is explicitly described as "Free alternative to QuiverQuantitative using public data sources" but its congressional trade methods query the wrong data source (EDGAR Form 4 instead of congressional disclosure systems).

---

### Data Flows Summary

```
External APIs → Collectors → DB Tables → (potential signal consumers)

Adzuna/Reed → job_postings_collector → closeloop.db::job_postings → job_growth_signal()
NASDAQ API  → short_interest_collector → closeloop.db::short_interest → should_block_short()
SEC EFTS    → sec_fulltext_collector → permanent_archive.db::sec_fulltext_alerts → [no consumer]
GDELT/NewsAPI/USGS → geopolitical_collector → permanent_archive.db::raw_geopolitical_events → get_sector_modifiers()
Finnhub     → finnhub_collector → [memory only, altdata pipeline]
NewsAPI     → advanced_news_intelligence → permanent_archive.db::{raw_articles, narrative_shifts, quantitative_claims, article_connections, source_credibility} → [narrative_shift methods — broken due to sentiment=0]
SEC EFTS/FDA/EPA/NewsAPI → regulatory_intelligence → permanent_archive.db::regulatory_alerts → get_signal_modifier()
SimFin      → simfin_collector → permanent_archive.db::earnings_quality → [query methods]
NewsAPI/FedRSS/HouseWatcher → social_influence_tracker → permanent_archive.db::{social_influence, ceo_credibility, raw_congressional_trades} → get_fed_sentiment(), get_ceo_signals()
Open-Meteo/OWM/WAQI/USGS/FRED/EIA → geographic_intelligence → permanent_archive.db::{raw_weather_data, raw_macro_data, raw_geopolitical_events} + historical_db.db::weather_data → get_weather_risk(), get_extreme_events()
ephem       → lunar_collector → [memory only, altdata pipeline, signal formula broken]
```

---

### Continue to Part 8: YES

Part 8 should cover: signal generators, position sizing logic, the closeloop execution layer, risk management, and the scheduler/orchestration layer.
