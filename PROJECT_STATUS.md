# Quant Fund — Project Status

_Last updated: 2026-04-02_

---

## What's Built

### Macro Intelligence Layer — NEW 2026-04-02

| Module | Location | Status |
|---|---|---|
| `setup_permanent_archive.py` | `output/` | Creates permanent_archive.db with 11 tables + FTS5 |
| `shipping_intelligence.py` | `data/collectors/` | BDI + shipping stocks + SSI (4,086 rows) |
| `geographic_intelligence.py` | `data/collectors/` | Open-Meteo weather + OWM + WAQI + USGS quakes + EIA |
| `geopolitical_collector.py` | `data/collectors/` | GDELT + NewsAPI + USGS + crisis database |
| `consumer_intelligence.py` | `data/collectors/` | 26 FRED series + payment processor signals |
| `commodity_collector.py` | `data/collectors/` | 29 commodities back to 1983 (188K+ rows) |
| `rates_credit_collector.py` | `data/collectors/` | Yield curve + FRED credit (245K rows) |
| `macro_signal_engine.py` | `analysis/` | Regime classifier + sector modifiers + EarningsContextScore |

**Permanent Archive DB** (`output/permanent_archive.db`): 461K rows across 11 tables + FTS5 search
**Historical DB** (`output/historical_db.db`): 469K rows across 19 tables
**Optional APIs active**: EIA (electricity), OpenWeatherMap, WAQI, ESA Copernicus

**Current Macro State** (as of 2026-04-02):
- Regime: RISK_ON (62% confidence)
- PEAD multiplier: 1.00x
- Yield curve: +51bps (NORMAL), 10yr=4.30%, 2yr=3.79%
- HY spreads: 328bps (NORMAL)
- Shipping Stress Index: 1.35 (NEUTRAL, threshold=1.5)
- Consumer confidence: 56.6 (WEAK)
- Jobless claims: 210K
- Next Fed meeting: 2026-05-06 (34 days)
- Geopolitical risk: LOW

**Daily briefing**: `python3 -c "import yaml,sys; sys.path.insert(0,'.'); config=yaml.safe_load(open('config/settings.yaml')); from intelligence.daily_pipeline import DailyPipeline; print(DailyPipeline(config).run_macro_briefing())"`

### Config (`config/`)
- `settings.yaml` — full parameter file: universe filters, signal thresholds, backtest dates, cost models (US + UK), risk limits, paper trading schedule, full `altdata:` section

### Data layer (`data/`)
| Module | Description |
|---|---|
| `fetcher.py` | yfinance wrapper with batched downloads and pickle cache (TTL-based). Handles `.L` suffix and pence→GBP conversion for UK names |
| `cleaner.py` | OHLCV sanity checks: forward-fill gaps, outlier cap at ±50% daily, OHLC consistency enforcement |
| `earnings_calendar.py` | Fetches earnings history and surprise % via yfinance; upcoming earnings scanner |
| `universe.py` | Applies market-cap / volume / price filters from config; falls back to a built-in default ticker list for dev use |

### Signals (`signals/`)
| Module | Description |
|---|---|
| `pead_signal.py` | Post-Earnings Announcement Drift: long/short on surprise > ±5% with volume surge ≥ 1.5×; z-score normalised across rolling 60-event window |
| `anomaly_scanner.py` | Scans returns matrix for: day-of-week, month-of-year, momentum (5/10/20/60d), mean-reversion (1/3/5d). Filters by min Sharpe and observation count; deduplicates by cross-correlation |
| `signal_validator.py` | 70/15/15 train/val/test OOS validation; t-test for mean-return significance |
| `filters.py` | Earnings quality (revenue trend), sector contagion (sector ETF check), short availability (market-cap proxy) |
| `signal_registry.py` | JSON-persisted registry of candidate → validated → live signals; supports manual promotion (auto_promote=false by default) |

### Backtest (`backtest/`)
| Module | Description |
|---|---|
| `engine.py` | Vectorised engine: full cost model (commission, slippage, stamp duty 0.5% UK, borrow costs for shorts), ATR execution, per-trade P&L |
| `walk_forward.py` | Expanding-window walk-forward over N configurable OOS windows; aggregates Sharpe and return across windows |
| `monte_carlo.py` | Bootstrap resamples trade returns × 1000 sims; outputs final-value / max-drawdown / Sharpe percentiles + prob_profit / prob_ruin |

### Risk (`risk/`)
- `manager.py` — half-Kelly sizing, hard caps (5% per position, 25% per sector, 60% net exposure, 20 max positions), 15% drawdown halt, ATR stop calculation, correlation gate (ρ < 0.75)

### Execution (`execution/`)
| Module | Description |
|---|---|
| `broker_interface.py` | Abstract `BrokerInterface` + `PaperBroker` (FIFO avg-cost accounting, stamp duty, slippage, full trade log) |
| `paper_trader.py` | Scheduled scans via `schedule` library (US 09:45, UK 08:15); auto-exit by date; daily P&L log to `logs/paper_trading.jsonl` |

### Reporting (`reporting/`)
- `analytics.py` — full metric suite (total return, CAGR, Sharpe, Sortino, Calmar, max DD + duration, VaR/CVaR 95%, skew, kurtosis, alpha, beta, IR); equity curve + drawdown chart; trade analysis chart (return distribution, cumulative P&L, return vs surprise, holding period)

### Alternative Data (`altdata/`) — **COMPLETE**

#### Collectors (`altdata/collector/`)
| Module | Source | Interface |
|---|---|---|
| `reddit_collector.py` | Reddit (PRAW / public JSON fallback) | `collect(tickers, market, config)` |
| `stocktwits_collector.py` | StockTwits public API | `collect(tickers, market, config)` |
| `news_collector.py` | RSS/Atom feeds (Benzinga, Yahoo, Reuters) | `collect(tickers, market, config)` |
| `sec_edgar_collector.py` | SEC EDGAR full-text search API (8-K, 13F, Form 4) | `collect(tickers, market, config)` |
| `companies_house_collector.py` | UK Companies House API (free tier) | `collect(tickers, market, config)` |
| `fred_collector.py` | FRED economic data API (free) | `collect(tickers, market, config)` |
| `shipping_collector.py` | Baltic Dry Index + public shipping data | `ShippingCollector(config).collect(tickers, market)` |
| `jobs_collector.py` | US BLS + Indeed job trend scraping | `JobsCollector(config).collect(tickers, market)` |
| `wikipedia_collector.py` | Wikipedia pageview API | `WikipediaCollector(config).collect(tickers, market)` |
| `google_trends_collector.py` | pytrends (45–90s delays, 12h cache) | `GoogleTrendsCollector(config).collect(tickers, market)` |
| `weather_collector.py` | Open-Meteo API (free, no key) | `WeatherCollector(config).collect(tickers, market)` |
| `lunar_collector.py` | ephem library (local computation, no API) | `LunarCollector(config).collect(tickers, market)` |

#### Processing (`altdata/processing/`)
| Module | Description |
|---|---|
| `sentiment_engine.py` | VADER + TextBlob + FinBERT (ProsusAI) ensemble; source-weighted scoring |
| `nlp_processor.py` | Text cleaning, entity extraction, topic classification |
| `feature_engineer.py` | Builds feature vectors from raw collector output; rolling z-scores |
| `normaliser.py` | Cross-source normalisation; freshness decay `exp(-λ×hours)` |

#### Learning (`altdata/learning/`)
| Module | Description |
|---|---|
| `online_learner.py` | River AdaptiveRandomForest + LogisticRegression ensemble; ADWIN drift detection |
| `weekly_retrainer.py` | Weekly scikit-learn batch retraining; Deflated Sharpe Ratio evaluation |
| `model_registry.py` | Versioned model storage; symlink to `current`; full rollback support |
| `model_validator.py` | OOS validation, Benjamini-Hochberg FDR, Monte Carlo permutation test (top 5%) |
| `rollback_manager.py` | Auto-rollback on Sharpe drop >10%, accuracy drop >15%, 3 consecutive wrong signals |

#### Anomaly Detection (`altdata/anomaly/`)
| Module | Description |
|---|---|
| `nonsense_detector.py` | NonsenseScore = `1 / (economic_logic_score + 0.01)`; Information Cascade Detector |
| `statistical_validator.py` | Bonferroni + BH FDR corrections; regime-conditional significance |
| `custom_metrics.py` | AltDataConfluenceScore, SignalFreshnessDecay, macro regime multipliers |

#### Signals (`altdata/signals/`)
| Module | Description |
|---|---|
| `altdata_signal_engine.py` | Combines all sources into directional signals with provenance tracking |
| `signal_promoter.py` | Promotes candidates to live after validation; wires into main signal registry |

#### Storage (`altdata/storage/`)
- `altdata_store.py` — SQLite WAL-mode store. 8 tables: raw_data, sentiment_scores, features, model_versions, signals, anomaly_candidates, notifications, alt_data_pnl. Thread-safe with per-thread connections. Daily backup.

#### Notifications (`altdata/notifications/`)
- `notifier.py` — Three channels: terminal (rich ANSI), log file (`logs/alerts.log`), desktop (plyer). Trigger methods: `new_signal()`, `pead_abort()`, `nonsense_candidate()`, `model_rollback()`, `drawdown_halt()`, `unusual_activity()`, `weekly_summary()`, `source_failure()`.

#### Dashboard (`altdata/dashboard/`)
- `altdata_dashboard.py` — Writes `output/daily_dashboard.txt`. Sections: macro environment, top alt signals, anomaly watch, nonsense digest (Sundays only), model health, data source status.

### Entry point
- `main.py` — CLI with **eleven** subcommands (see below)

### Tests (`tests/`) — **55/55 passing**
| File | Coverage |
|---|---|
| `test_signals.py` | PEADSignal: direction, threshold, volume gate, exit date, z-score, empty inputs |
| `test_anomaly_scanner.py` | Scanner filters, sort order, deduplication; Validator splits, OOS pass/fail, t-test |
| `test_backtest.py` | Engine: keys, empty input, initial capital, metrics, drawdown ≤ 0, shorts, UK stamp duty, missing ticker; Monte Carlo: structure, prob bounds, percentile order; Walk-forward: keys, insufficient data |
| `test_risk.py` | Kelly sizing, position caps, drawdown halt, exposure halt, sector cap, max positions; ATR stop; correlation gate; PaperBroker: buy/sell/short/cover, rejections, stamp duty, trade log |

---

## What's Working End-to-End

```
config loaded → universe filtered → prices fetched (cached) →
earnings surprise calculated → PEAD signals generated →
pre-trade filters applied → backtest engine → Monte Carlo →
metrics computed → report printed → charts saved to output/

altdata collect → SQLite store → sentiment/feature pipeline →
online learner → signal engine → signal promoter → notifications →
daily dashboard
```

All modules import cleanly. Tests run in ~1.7 seconds (no network calls).

---

## What's Still Needed

### 1. Live broker integration (`execution/broker_interface.py`)
- IBKR via `ib_insync` or Alpaca via `alpaca-trade-api` — slot is ready, just subclass `BrokerInterface`

### 2. Universe scaling
- Current universe manager calls yfinance `.info` per ticker (slow at scale). For production, replace with a pre-screened ticker CSV or a proper data vendor screener.
- Suggested: `data/universe_us.csv` and `data/universe_uk.csv` refreshed weekly via a cron job.

### 3. Sentiment filter wiring
- `signals/filters.py:sentiment()` is a stub. Wire it to `altdata/signals/altdata_signal_engine.py` in `filters.run_all()`.

### 4. Walk-forward signal generator wiring
- `backtest/walk_forward.py` accepts any `signal_generator(price_data) -> signals_df` callable.
  Add a `--walk-forward` flag to `main.py backtest`.

### 5. FRED API key (optional)
- The FRED collector works without a key but is rate-limited. Set `FRED_API_KEY` env var for higher limits (free at fred.stlouisfed.org).

---

## Exact Commands to Run Next Time

```bash
# 0. From the project root
cd /home/dannyelticala/quant-fund

# 1. Install dependencies (once)
pip3 install -r requirements.txt --break-system-packages

# 2. Run all tests
python3 -m pytest tests/ -v

# 3. Backtest PEAD on US market (uses default ticker list)
python3 main.py backtest --market us

# 4. Backtest on UK market
python3 main.py backtest --market uk

# 5. Backtest both markets
python3 main.py backtest --market both

# 6. Backtest with your own ticker list
python3 main.py backtest --market us --tickers-file data/universe_us.csv

# 7. Scan for statistical anomalies
python3 main.py anomaly_scan --market us

# 8. View signal registry after a scan
#    (output/signal_registry.json is updated automatically)
cat output/signal_registry.json | python3 -m json.tool | head -60

# 9. Manually promote a validated signal to live
python3 main.py promote --signal momentum_us_002

# 10. Regenerate report from saved results (no re-download)
python3 main.py report --prefix backtest_us
python3 main.py report --prefix backtest_uk

# 11. Start paper trading loop (blocks; runs on schedule)
python3 main.py paper_trade

# 12. View paper trading log
tail -f logs/paper_trading.jsonl

# 13. Run alt-data collectors (fetches live data from all 12 sources)
python3 main.py altdata collect

# 14. Collect for specific tickers only
python3 main.py altdata collect --tickers AAPL MSFT NVDA

# 15. Generate alt-data signals from collected data
python3 main.py altdata signals

# 16. Show pipeline status summary
python3 main.py altdata status

# 17. Render the daily dashboard
python3 main.py altdata dashboard
cat output/daily_dashboard.txt

# 18. Run nonsense detector scan on anomaly candidates
python3 main.py altdata nonsense

# 19. Rollback model to a previous version
python3 main.py altdata rollback --to-version v20240115_143022

# 20. Run a single test module
python3 -m pytest tests/test_signals.py -v
python3 -m pytest tests/test_backtest.py -v
```

---

## Output Files

```
output/
  backtest_us_trades.csv          — per-trade record
  backtest_us_equity.csv          — daily equity curve
  backtest_us_metrics.json        — all performance metrics
  backtest_us_monte_carlo.json    — MC simulation results
  backtest_us_report.md           — printable text report
  pead_—_us.png                   — equity curve + drawdown chart
  backtest_us_trade_analysis.png  — 4-panel trade analysis chart
  signal_registry.json            — live signal registry
  altdata.db                      — SQLite database (all alt-data)
  daily_dashboard.txt             — latest alt-data dashboard
  backups/altdata_YYYYMMDD.db     — daily DB backup

logs/
  quant_fund.log                  — structured application log
  paper_trading.jsonl             — daily paper P&L log (one JSON per line)
  alerts.log                      — alt-data notifications (one JSON per line)

altdata/models/                   — versioned model snapshots
  v20240115_143022/
  current -> v20240115_143022     (symlink to active model)
```
