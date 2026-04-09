# APOLLO SYSTEM MAP — PART 5
## GROUP 5: Signal Aggregation, Macro Engine, Mathematical Signals, Bayesian Regime

Generated: 2026-04-08
Files read: 4 files, every line

---

## FILE 1: /home/dannyelticala/quant-fund/closeloop/integration/signal_aggregator.py

### A) PURPOSE

Combines signals from multiple sources (PEAD, frontier, context, altdata, intelligence) into a single confluence score and Kelly multiplier. Provides provenance tracking for post-trade attribution. Also contains `MultiFrequencyAggregator` for 4-horizon Multi-Frequency Scoring.

### B) CLASSES AND METHODS

#### Class: `SignalAggregator`
Aggregates signals from PEAD, frontier, context, and altdata sources into a confluence score and recommended Kelly multiplier.

**`__init__(self, store=None, config=None)`**
- Inputs: `store` (optional DB store object), `config` (optional dict)
- Reads: `config["closeloop"]["entry"]` if provided
- Output: None
- DB reads/writes: None at init

**`aggregate(self, ticker, pead_signal=None, frontier_signals=None, context_signals=None, altdata_signals=None, intelligence_signals=None)`**
- Inputs:
  - `ticker`: str
  - `pead_signal`: optional dict containing `surprise_zscore` (float), `signal` (int +1/-1)
  - `frontier_signals`: optional list of dicts with `signal_name`, `value` (float), `quality_score` (float), `source`
  - `context_signals`: optional list of dicts with `signal_name`, `strength` (float), `direction` (int), `quality_score`, `source`
  - `altdata_signals`: optional list of dicts with `signal_name`, `strength` (float), `direction` (int), `quality_score`, `source`
  - `intelligence_signals`: optional list of dicts with `signal_name`, `value` (float), `strength` (float), `quality_score`, `source`
- Output: dict with keys `ticker`, `confluence_score`, `confluence_level`, `kelly_multiplier`, `combined_direction`, `signal_count`, `active_signals`, `provenance`, `timestamp`
- DB reads/writes: None

**`_empty(self, ticker)`**
- Inputs: `ticker` str
- Output: empty result dict with all zeros/NONE defaults and current UTC timestamp
- DB reads/writes: None

**`get_multi_frequency_score(self, ticker, signals_dict)`**
- Inputs: `ticker` str, `signals_dict` dict
- Output: delegates to `MultiFrequencyAggregator(self.config).compute_mfs(ticker, signals_dict)`
- DB reads/writes: None

---

#### Class: `MultiFrequencyAggregator`
Combines signals from 4 time horizons into a single Multi-Frequency Score (MFS).

Static attribute `WEIGHTS`:
```
{"h1": 0.20, "h2": 0.35, "h3": 0.30, "h4": 0.15}
```

**`__init__(self, config=None)`**
- Inputs: optional config dict
- Output: None

**`compute_mfs(self, ticker, signals)`**
- Inputs:
  - `ticker`: str
  - `signals`: dict with optional keys `h1`, `h2`, `h3`, `h4` — each a dict of signal_name: float in [-1, +1]
    - `h1` (Intraday): `hmm_signal`, `options_flow`, `momentum_1d`
    - `h2` (Short-term): `pead_signal`, `pairs_signal`, `analyst_revision`, `calendar_modifier`
    - `h3` (Medium-term): `macro_regime`, `sector_rotation`, `narrative_shift`
    - `h4` (Long-term): `tech_theme`, `thematic_score`
- Output: dict with `ticker`, `mfs`, `h1_score`, `h2_score`, `h3_score`, `h4_score`, `size_multiplier`, `conflict_detected`, `label`, `computed_at`
- DB reads/writes: None
- Note: uses `datetime.utcnow()` (naive UTC) rather than `datetime.now(timezone.utc)` used elsewhere in this file — inconsistency

### C) MATHEMATICS

FORMULA [PEAD strength normalisation]:
  Variables:
    `surprise_zscore` — from pead_signal dict, float
  Calculation: `strength = min(abs(pead_signal.get("surprise_zscore", 1.0)) / 3.0, 1.0)`
  Output: PEAD signal strength [0, 1] for confluence weighting

FORMULA [Frontier signal strength]:
  Variables:
    `value` — from frontier_signal dict, float
    `quality` — from frontier_signal dict, float (default 0.5)
  Calculation: `strength = min(abs(sig.get("value", 0.0)), 1.0)` then `effective_strength = strength * quality` (used in confluence weighting)
  Output: Effective signal weight

FORMULA [Intelligence signal direction]:
  Variables:
    `raw_score` — from intelligence_signal["value"], float
  Calculation:
    `direction = 1 if raw_score > 0.05 else (-1 if raw_score < -0.05 else 0)`
  Output: int direction for confluence calculation

FORMULA [Confluence score]:
  Variables:
    `w` — per-signal weight: `w = sig["strength"] * sig["quality"]`
    `agrees` — bool: `sig["direction"] == primary_dir` (or True if no primary)
    `agreement_score` — sum of `w * (1.0 if agrees else 0.0)` across all signals
    `total_weight` — sum of all `w`
  Calculation: `confluence = agreement_score / total_weight if total_weight > 0 else 0.0`
  Output: float [0, 1] — fraction of weighted signals agreeing with primary direction

FORMULA [Confluence level thresholds]:
  STRONG   >= 0.70 → kelly_multiplier = 1.00
  MODERATE >= 0.50 → kelly_multiplier = 0.75
  WEAK     >= 0.30 → kelly_multiplier = 0.50
  NONE     <  0.30 → kelly_multiplier = 0.00

FORMULA [MFS per-horizon score]:
  Variables:
    `h_dict` — dict of signal values for one horizon
    `vals` — list of float values in h_dict (non-None)
  Calculation: `h_score = float(np.mean(vals)) if vals else 0.0`
  Output: float mean of all signals in the horizon

FORMULA [MFS weighted combination]:
  Variables:
    `h1`, `h2`, `h3`, `h4` — horizon scores (each is mean of their signals)
  Calculation:
    `mfs = h1 * 0.20 + h2 * 0.35 + h3 * 0.30 + h4 * 0.15`
    `mfs = float(np.clip(mfs, -1.0, 1.0))`
  Output: MFS float in [-1, +1]

FORMULA [MFS size multiplier]:
  Calculation:
    `if mfs > 0.7:   size_mult = 1.50`
    `elif mfs > 0.5: size_mult = 1.00`
    `elif mfs > 0.3: size_mult = 0.50`
    `else:           size_mult = 0.25`
  Conflict adjustment (h2 > 0.3 AND h3 < -0.2):
    `size_mult *= 0.55`
  Output: float multiplier for position sizing

FORMULA [MFS label]:
  `if mfs > 0.7:   label = "MAX_CONVICTION"`
  `elif mfs > 0.5: label = "STANDARD"`
  `elif mfs > 0.3: label = "REDUCED"`
  `else:           label = "MINIMAL"`

### D) DATA FLOWS

Input to `aggregate()`:
- `pead_signal` dict (from EarningsCalendar / PEAD system)
- `frontier_signals` list of dicts
- `context_signals` list of dicts
- `altdata_signals` list of dicts
- `intelligence_signals` list of dicts

Output from `aggregate()`:
```python
{
    "ticker": str,
    "confluence_score": float,      # [0, 1]
    "confluence_level": str,        # STRONG / MODERATE / WEAK / NONE
    "kelly_multiplier": float,      # 0.0, 0.50, 0.75, or 1.00
    "combined_direction": int,      # +1 / -1 / 0
    "signal_count": int,
    "active_signals": list[str],    # names of all signals present
    "provenance": list[dict],       # per-signal contribution records
    "timestamp": str,               # ISO8601 UTC
}
```

DB reads: None
DB writes: None

### E) DEPENDENCIES

Internal Apollo imports: None directly imported (used by main.py, paper_trader.py)
External libraries:
- `numpy` (np)
- `logging`
- `datetime` (datetime, timezone)
- `typing` (Dict, List, Optional)

### F) WIRING STATUS

**Connected to live trading path: YES**

Two wiring points:
1. `main.py` lines 326-337: `SignalAggregator.aggregate()` called before dispatching to paper_trader. If `confluence_level == "NONE"` the ticker is skipped.
2. `execution/paper_trader.py` lines 1320-1355: `SignalAggregator.aggregate()` called inside `_process()` — a separate instance cached on `self._signal_aggregator`. Only receives `pead_signal` and `context_signals` (derived from `_raw_signals`). If `kelly_multiplier == 0.0`, returns None (no trade).

`MultiFrequencyAggregator.compute_mfs()` is NOT called anywhere in the live path. It is defined and accessible via `get_multi_frequency_score()` but no caller exists in main.py or paper_trader.py.

### G) ISSUES FOUND

1. **Dual invocation with different inputs**: `aggregate()` is called in `main.py` with full signal sets (pead + potentially others), then called again in `paper_trader._process()` with only pead + context (no frontier, altdata, intelligence). The two calls can return different confluence scores for the same ticker on the same event.

2. **`MultiFrequencyAggregator` is dead code in live path**: `compute_mfs()` is never called from main.py or paper_trader.py. The MFS framework (h1–h4 horizons) is defined but not wired.

3. **`computed_at` uses `datetime.utcnow()` (naive)** in `compute_mfs()` while `aggregate()` uses `datetime.now(timezone.utc)` (aware) for `timestamp`. Inconsistent timezone handling.

4. **No try/except around signal processing loops**: If any signal dict is malformed (e.g., `direction` contains a non-castable value), `int()` will throw and the entire `aggregate()` call will fail silently wherever it is wrapped in try/except (as in paper_trader).

5. **`kelly_multiplier` is not applied to size in paper_trader**: `aggregate()` returns `kelly_multiplier` but paper_trader only checks if it equals 0.0 (block) or not (allow). It does NOT multiply `size_pct` by `kelly_multiplier`. The MODERATE (0.75x) and WEAK (0.50x) multipliers from the docstring are never applied.

6. **`store` parameter is accepted but never used** in `SignalAggregator.__init__()` — `self.store` is set but not read anywhere in the class.

---

## FILE 2: /home/dannyelticala/quant-fund/analysis/macro_signal_engine.py

### A) PURPOSE

Orchestrates all macro data sources (rates, credit, FRED, altdata, VIX) into a unified MacroState dataclass. Contains four classes: `MacroRegimeClassifier`, `SectorContextEngine`, `EarningsContextScore`, and `MacroSignalEngine` (main orchestrator). Used to gate PEAD trades, size positions, and provide earnings context scores. Labelled "STEP 9" in Apollo's pipeline.

### B) CLASSES AND METHODS

#### Dataclass: `MacroState`
Complete snapshot of all macro signals at a point in time.
Fields (all optional unless noted):
- `as_of`: str (UTC ISO, auto-set)
- `regime`: str (default "RISK_ON")
- `regime_confidence`: float (default 0.5)
- `pead_multiplier`: float (default 1.0)
- `position_size_multiplier`: float (default 1.0)
- `yield_curve_slope`, `is_inverted`, `inversion_weeks`, `yield_momentum_10yr`, `yields_rising_fast`, `rates_regime`, `breakeven_inflation`
- `hy_spread`, `ig_spread`, `credit_stress_z`, `ted_spread`
- `gdp_growth`, `inflation`, `unemployment`, `vix`, `consumer_confidence`, `consumer_confidence_change`
- `oil_price`, `oil_1m_change`, `copper_price`, `copper_1m_change`, `gold_price`, `gold_1m_change`, `oil_zscore`, `copper_zscore`, `gold_zscore`
- `shipping_stress`, `shipping_sector_impacts`
- `geopolitical_risk_level`, `active_alerts`, `geopolitical_crisis`
- `housing_health`, `inflation_pressure`, `consumer_health`
- `sector_modifiers`
- `days_to_fed_meeting`, `fed_position_multiplier`
- `upcoming_earnings_context`

---

#### Class: `MacroRegimeClassifier`
Classifies current macro regime from available data inputs. All inputs optional.

**`classify(self, gdp_growth, inflation, unemployment, unemployment_3m_change, vix, yield_curve_slope, hy_credit_spread, consumer_confidence, shipping_stress, geopolitical_crisis, inversion_weeks)`**
- Inputs: all optional floats/bool/int described above
- Output: dict with `regime` (str), `pead_multiplier` (float), `position_size_multiplier` (float), `preferred_sectors` (list), `avoid_sectors` (list), `confidence` (float), `matched_conditions` (list)
- DB reads/writes: None

---

#### Class: `SectorContextEngine`
Applies sector-specific multiplicative modifiers driven by macro conditions.

**`get_modifier(self, sector, macro_data)`**
- Inputs: `sector` str (snake_case), `macro_data` dict (matching MacroState fields)
- Output: float — multiplicative modifier (1.0 = neutral, > 1.0 = favourable, < 1.0 = unfavourable)
- DB reads/writes: None

**`get_all_modifiers(self, macro_data)`**
- Inputs: `macro_data` dict
- Output: dict of sector → float for all 27 known sectors
- DB reads/writes: None

---

#### Class: `EarningsContextScore`
Calculates composite context score (0–1) for an upcoming earnings event.

**`__init__(self, archive_db_path)`**
- Inputs: `archive_db_path` str (default "output/permanent_archive.db")
- DB writes: Creates `earnings_context_log` table in permanent_archive.db if not present
- Also attempts (and silently fails): `CREATE INDEX ON predictions_log(ticker_or_sector, prediction_date)` then falls back to `predictions_log(ticker, calc_timestamp)` — this index creation is on the WRONG table (`predictions_log` rather than `earnings_context_log`)

**`calculate(self, ticker, sector, macro_data)`**
- Inputs: `ticker` str, `sector` str, `macro_data` dict
- Output: dict with `ticker`, `sector`, `composite_score` (float), `label` (FAVOURABLE/NEUTRAL/UNFAVOURABLE), `components` (dict of 7 scores), `regime` (str)
- DB writes: INSERT into `earnings_context_log` in permanent_archive.db (all 7 component scores, composite, label, regime, macro_data JSON)

---

#### Class: `MacroSignalEngine`
Main macro signal orchestrator. Aggregates all collectors into a MacroState.

**`__init__(self, config_path)`**
- Inputs: `config_path` str (default "config/settings.yaml")
- Loads config from YAML. Instantiates `MacroRegimeClassifier`, `SectorContextEngine`, `EarningsContextScore`. Lazy-loads `RatesCreditCollector`.
- DB writes: triggers `EarningsContextScore.__init__()` → creates earnings_context_log table

**`_load_config(path)`** (static)
- Inputs: file path str
- Output: dict (YAML contents) or {} on failure

**`_get_rates_collector(self)`**
- Lazy-loads `RatesCreditCollector` from `data.collectors.rates_credit_collector`
- Returns None if unavailable

**`_gather_rates_data(self)`**
- Calls `RatesCreditCollector` methods: `get_yield_curve_status()`, `get_credit_conditions()`, `get_rates_regime()`, `get_breakeven_inflation()`, `days_to_next_fed_meeting()`, `get_position_size_multiplier()`
- Output: dict with yield curve, credit, Fed meeting data
- DB reads: indirectly via RatesCreditCollector (output/permanent_archive.db, output/historical_db.db)

**`_gather_fred_macro(self)`**
- DB reads: `output/historical_db.db` table `rates_data` — columns `series_id`, `value`, `obs_date`
  - Reads `UNRATE` (current + 3-period lag for 3m change), `CPIAUCSL` (latest value)
- Output: dict with `unemployment`, `unemployment_3m_change`, `inflation`

**`_gather_altdata_macro(self)`**
- DB reads: `altdata/storage/altdata.db`
  - Table `macro_indicators`, column `indicator='ShippingStressIndex'` → `value`
  - Table `geopolitical_alerts` → `level`, `alert_text`
  - Table `macro_indicators`, column `indicator='ConsumerConfidence'` → `value`
  - Table `commodity_prices` → `commodity`, `value`, `date` (for oil, copper, gold — last 22 rows)
- Output: dict with shipping_stress, geopolitical_risk_level, active_alerts, geopolitical_crisis, consumer_confidence, consumer_confidence_change, oil/copper/gold prices and 1m changes

**`_gather_vix(self)`**
- DB reads: `output/historical_db.db` table `macro_data` WHERE `symbol='vix'`
- Fallback: yfinance `^VIX` 5-day download
- Output: float or None

**`run_full_analysis(self)`**
- Inputs: None (uses instance state)
- Calls: `_gather_rates_data()`, `_gather_fred_macro()`, `_gather_altdata_macro()`, `_gather_vix()`
- Calls: `MacroRegimeClassifier.classify()` with all gathered inputs
- Output: populated `MacroState` object, cached as `self._state`
- Computes `position_size_multiplier = regime_mult * fed_mult * rates_mult`
- Computes sector modifiers and shipping sector impacts
- DB reads: all four gather methods above

**`get_pead_multiplier(self)`**
- Output: float from `self._state.pead_multiplier`, runs analysis if not cached

**`get_position_size_multiplier(self)`**
- Output: float from `self._state.position_size_multiplier`

**`get_earnings_context(self, ticker, sector)`**
- Calls `EarningsContextScore.calculate()` with current macro state as dict
- DB writes: INSERT into earnings_context_log

**`get_complete_briefing_data(self)`**
- Output: large dict with all MacroState fields flattened (regime, rates, credit, geopolitical, commodities, sector modifiers, Fed, fundamentals, earnings context)

---

#### Utility functions

**`_clamp(val, lo, hi)`**
- Inputs: float, float, float
- Output: float clamped to [lo, hi]

**`_state_to_dict(state)`**
- Inputs: Optional[MacroState]
- Output: flat dict of MacroState fields for use in sub-engine calls

### C) MATHEMATICS

FORMULA [Regime classification — CRISIS]:
  Variables:
    `vix` — float, VIX level
    `hy_credit_spread` — float, HY spread in bps
    `geopolitical_crisis` — bool
  Calculation:
    CRISIS if: `vix > 35` OR `hy_credit_spread > 500` OR `geopolitical_crisis == True`
  Output: regime string "CRISIS"

FORMULA [Regime classification — RECESSION_RISK]:
  Variables:
    `yield_curve_slope` — float (10Y-2Y)
    `inversion_weeks` — int
    `unemployment_3m_change` — float (ppt change over 3 months)
    `gdp_growth` — float (%)
  Calculation:
    RECESSION_RISK if NOT CRISIS AND:
    (`yield_curve_slope < 0` AND `inversion_weeks > 13`) OR
    (`unemployment_3m_change > 0.5`) OR
    (`gdp_growth < 0`)
  Output: regime string "RECESSION_RISK"

FORMULA [Regime classification — STAGFLATION]:
  Variables:
    `inflation` — float (%)
    `gdp_growth` — float (%)
  Calculation:
    STAGFLATION if: `inflation > 4.0` AND `gdp_growth < 1.0`
  Output: regime string "STAGFLATION"

FORMULA [Regime classification — RISK_OFF]:
  Calculation:
    RISK_OFF if: (`25 <= vix <= 35`) OR (`hy_credit_spread > 350`) OR (`consumer_confidence < 80`)
  Output: regime string "RISK_OFF"

FORMULA [Regime classification — GOLDILOCKS]:
  Calculation:
    GOLDILOCKS if: `gdp_growth > 2.0` AND `1.0 <= inflation <= 3.0` AND `unemployment < 5.0` AND `vix < 18`
  Output: regime string "GOLDILOCKS"

Default: "RISK_ON" if none of the above conditions are met.

FORMULA [Regime confidence]:
  Variables:
    `n_signals_available` — count of non-None inputs among {gdp_growth, inflation, unemployment, vix, yield_curve_slope, hy_credit_spread, consumer_confidence, shipping_stress}
    `matched_conditions` — list of triggered condition strings
  Calculation:
    `confidence = min(0.4 + (n_signals_available / 8) * 0.6, 1.0)`
    `if len(matched_conditions) >= 2: confidence = min(confidence + 0.1, 1.0)`
  Output: float [0.4, 1.0]

FORMULA [PEAD multiplier by regime]:
  CRISIS: 0.1 | RECESSION_RISK: 0.4 | STAGFLATION: 0.6 | RISK_OFF: 0.7 | GOLDILOCKS: 1.2 | RISK_ON: 1.0

FORMULA [Combined position size multiplier]:
  Variables:
    `regime_mult` — from REGIME_PEAD_MULTIPLIERS (same as pead_multiplier)
    `fed_mult` — from RatesCreditCollector.get_position_size_multiplier()
    `rates_mult` — lookup: {"CRISIS":0.2, "TIGHT":0.6, "NEUTRAL":1.0, "EASY":1.1}.get(rates_regime, 1.0)
  Calculation:
    `position_size_multiplier = round(regime_mult * fed_mult * rates_mult, 4)`
  Output: float — combined multiplier applied to position sizing

FORMULA [Sector modifier — additive delta]:
  Base: 1.0
  Shipping stress (shipping_stress > 1.5):
    retailers: -0.3, food_manufacturers: -0.2, domestic_producers: +0.3, air_freight: +0.2, shipping: +0.3
  Oil shock (oil_1m_change > 0.15):
    airlines: -0.4, trucking: -0.3, energy_producers: +0.4, energy: +0.3
  Copper surge (copper_1m_change > 0.10):
    industrials: +0.2, mining: +0.3
  Consumer confidence collapse (consumer_confidence_change < -10):
    restaurants: -0.3, entertainment: -0.3, luxury: -0.4, dollar_stores: +0.2, consumer_discretionary: -0.2
  Inverted yield (yield_curve_slope < -0.5):
    banks: -0.3, financials: -0.2, growth: -0.2, utilities: +0.1
  Geopolitical crisis:
    defence: +0.3, energy: +0.2, energy_producers: +0.2, all_others: -0.2
  Final: `modifier = round(1.0 + delta, 4)`

FORMULA [EarningsContextScore — macro_regime_score]:
  Variables:
    `pead_multiplier` — from MacroRegimeClassifier (range 0.1 to 1.2)
  Calculation: `macro_regime_score = min(pead_multiplier / 1.2, 1.0)`
  Output: float [0, 1]

FORMULA [EarningsContextScore — sector_health_score]:
  Variables:
    `sector_modifier` — from SectorContextEngine.get_modifier() (approx range [0.3, 1.7])
  Calculation: `sector_health_score = _clamp((sector_modifier - 0.3) / 1.4, 0.0, 1.0)`
  Output: float [0, 1]

FORMULA [EarningsContextScore — shipping_stress_score]:
  Variables:
    `shipping_stress` — float (None → 0.5 neutral)
  Calculation: `shipping_stress_score = _clamp(1.0 - (shipping_stress - 0.5) / 1.5, 0.0, 1.0)`
  Boundary: stress >= 2.0 → 0.0; stress <= 0.5 → 1.0
  Output: float [0, 1]

FORMULA [EarningsContextScore — consumer_confidence_score]:
  Variables:
    `consumer_confidence` — float (None → 0.5)
  Calculation: `consumer_confidence_score = _clamp((consumer_confidence - 60) / 50, 0.0, 1.0)`
  Boundary: confidence >= 110 → 1.0; confidence <= 60 → 0.0
  Output: float [0, 1]

FORMULA [EarningsContextScore — commodity_pressure_score]:
  Variables:
    `oil_zscore` — float (None → excluded)
    `copper_zscore` — float (None → excluded)
  Calculation:
    oil: `_clamp(0.5 - oil_zscore * 0.15, 0.0, 1.0)` (high oil z → lower score)
    copper: `_clamp(0.5 + copper_zscore * 0.10, 0.0, 1.0)` (high copper z → higher score)
    `commodity_pressure_score = float(np.mean(scores))`
  Output: float [0, 1]

FORMULA [EarningsContextScore — geopolitical_risk_score]:
  Lookup: LOW → 0.8, MEDIUM → 0.5, HIGH → 0.25, CRITICAL → 0.0 (unrecognised → 0.5)
  Output: float

FORMULA [EarningsContextScore — credit_conditions_score]:
  Variables:
    `credit_stress_z` — float (primary)
    `hy_spread` — float (fallback)
  Calculation (primary): `_clamp(0.5 - credit_stress_z * 0.2, 0.0, 1.0)`
  Calculation (fallback): `_clamp(1.0 - (hy_spread - 200) / 600, 0.0, 1.0)` (200bps→1.0, 800bps→0.0)
  Default: 0.5
  Output: float [0, 1]

FORMULA [EarningsContextScore — composite]:
  Weights:
    macro_regime_score: 0.25
    sector_health_score: 0.20
    consumer_confidence_score: 0.15
    shipping_stress_score: 0.10
    commodity_pressure_score: 0.10
    geopolitical_risk_score: 0.10
    credit_conditions_score: 0.10
  Calculation: `composite = sum(components[k] * COMPONENT_WEIGHTS[k] for k in COMPONENT_WEIGHTS)`
  Label: composite >= 0.7 → FAVOURABLE; >= 0.4 → NEUTRAL; < 0.4 → UNFAVOURABLE
  Output: float [0, 1]

### D) DATA FLOWS

Data enters from:
1. `output/historical_db.db` — tables: `rates_data` (FRED series), `macro_data` (VIX)
2. `output/permanent_archive.db` — read via RatesCreditCollector
3. `altdata/storage/altdata.db` — tables: `macro_indicators`, `geopolitical_alerts`, `commodity_prices`
4. yfinance — `^VIX` as fallback
5. `config/settings.yaml` — configuration

Data written to:
1. `output/permanent_archive.db` — table `earnings_context_log` (via EarningsContextScore.calculate())

### E) DEPENDENCIES

Internal Apollo imports:
- `data.collectors.rates_credit_collector.RatesCreditCollector` (lazy, graceful)

External libraries:
- `numpy` (np)
- `yaml`
- `sqlite3`
- `logging`
- `dataclasses` (dataclass, field)
- `datetime`
- `pathlib` (Path)
- `typing` (Any, Dict, List, Optional)
- `yfinance` (yf) — VIX fallback only
- `json` — in `EarningsContextScore.calculate()`

### F) WIRING STATUS

**Connected to live trading path: YES (via `_capture_context`)** and **indirectly via `pead_multiplier`**

`paper_trader._capture_context()` (line 876) instantiates `MacroSignalEngine` (cached as `self._macro_engine`) and calls `get_complete_briefing_data()`. The regime string is stored in `context_at_open["macro"]["regime"]` and then wired to `RiskManager.set_macro_regime()` (line 1023).

`paper_trader` also instantiates `MacroSignalEngine` in `_build_market_context()` (line 1723) for a lighter call.

`EarningsContextScore.calculate()` is called via `get_earnings_context()` which is available but how often it is called in the live path is not confirmed from this file alone.

`pead_multiplier` from `MacroState` is available but is NOT directly applied to `size_pct` in paper_trader — it is stored in context but the position sizing code does not fetch it from the engine at trade time.

### G) ISSUES FOUND

1. **`_ensure_predictions_log()` creates wrong index**: Lines 447-453 attempt `CREATE INDEX ON predictions_log(ticker_or_sector, ...)` then fall back to `predictions_log(ticker, ...)`. Neither references the actual table `earnings_context_log` that was just created. This is a silent error — the index silently fails, but the table creation itself succeeds.

2. **`gdp_growth` is never populated in `run_full_analysis()`**: `MacroState.gdp_growth` defaults to `None`. `_gather_fred_macro()` reads `CPIAUCSL` (inflation) and `UNRATE` (unemployment) but has no GDP read. When `MacroRegimeClassifier.classify()` is called with `gdp_growth=state.gdp_growth`, this is always `None`. The STAGFLATION and GOLDILOCKS regimes require `gdp_growth` — they can never trigger.

3. **`oil_zscore`, `copper_zscore`, `gold_zscore` are never populated in `run_full_analysis()`**: `_gather_altdata_macro()` only fetches prices and 1m changes, not z-scores. The `commodity_pressure_score` in `EarningsContextScore` uses these z-scores as primary inputs; they will always be `None`, causing the formula to default to 0.5.

4. **`housing_health`, `inflation_pressure`, `consumer_health` on MacroState are never written in `run_full_analysis()`**: These fields exist in the dataclass and appear in `get_complete_briefing_data()` output, but no gather method sets them. Always `None`.

5. **`position_size_multiplier` not applied to trades in paper_trader**: `MacroSignalEngine.get_position_size_multiplier()` returns the combined multiplier, but paper_trader does not call this at trade time. The macro sizing gate is only used for context capture and risk manager regime signalling, not for actual share quantity calculation.

6. **`upcoming_earnings_context` on MacroState is never populated**: Field exists with default `[]` but no method in `run_full_analysis()` fills it.

---

## FILE 3: /home/dannyelticala/quant-fund/analysis/mathematical_signals.py

### A) PURPOSE

Provides Hidden Markov Model (HMM) signals, autocorrelation-based signals (Ljung-Box, momentum, mean-reversion, spectral FFT), and Kalman filter utilities. Main orchestrator `MathematicalSignals` combines HMM and autocorrelation signals into a single float in [-1, +1] with 1-hour in-memory cache. Also contains `KalmanSignalSmoother` and `KalmanPairsTrader`.

### B) CLASSES AND METHODS

#### Class: `HMMSignals`
Fits a 3-state GaussianHMM (bull/neutral/bear) on ticker price history.

**`__init__(self)`**
- Maintains: `_models` dict (ticker→fitted HMM), `_state_map` dict (ticker→{bull,bear,neutral}), `_last_obs` dict (ticker→features array)

**`_build_features(self, price_df)`**
- Inputs: OHLCV DataFrame with columns Open, High, Low, Close, Volume; minimum 252 rows
- Output: (N, 4) numpy array or None
- Features computed:
  1. `log_ret = log(Close[t] / Close[t-1])`
  2. `vol_z = (Volume - 20d_rolling_mean) / 20d_rolling_std` (5-period min)
  3. `hl_range = (High - Low) / Close`
  4. `co_gap = (Close - Open) / Open`

**`fit(self, ticker, price_df)`**
- Inputs: ticker str, OHLCV DataFrame
- Fits GaussianHMM (n_components=3, covariance_type="diag", n_iter=100, random_state=42)
- Maps states to bull/neutral/bear by mean return of column 0 (ascending: bear=lowest, bull=highest)
- Validates: bull_mean must > 0, bear_mean must < 0 — fails and returns False if not
- Output: True on success, False on failure
- DB reads/writes: None

**`get_state(self, ticker)`**
- Inputs: ticker str (must have been fitted)
- Decodes last bar posterior probabilities using `model.predict_proba(obs)`[-1]
- Determines likely next state via `model.transmat_[current_state]`
- Output: dict with `current_state` (int), `state_label` (str), `state_probs` (list[float]), `likely_next_state` (int), `hmm_signal` (+1/0/-1)

**`get_signal_strength(self, ticker)`**
- Output: max probability of current state as float [0, 1]

---

#### Class: `FiveStateHMM`
5-state GaussianHMM (CRISIS, BEAR, NEUTRAL, BULL, EUPHORIA). Not a replacement for HMMSignals — listed as backwards compatible.

**`_regularize_model(model)`** (static)
- Fixes degenerate transmat_ rows (zero-sum) → uniform distribution

**`_build_features(self, price_df)`**
- (N, 5) feature matrix:
  1. `log_return = log(Close[t]/Close[t-1])`
  2. `realized_vol = 30d rolling std of log returns`
  3. `vol_ratio = Volume / 20d_avg_volume`
  4. `short_vol = 5d rolling std of log returns`
  5. `yield_curve_prx = (Close/Close.shift(10) - 1) - (Close/Close.shift(30) - 1)` (10d mom - 30d mom)

**`fit(self, price_df)`**
- Tries covariance_type "diag" then "full"
- Maps states by mean log return (ascending: CRISIS → BEAR → NEUTRAL → BULL → EUPHORIA)
- Returns True/False

**`get_current_label(self)`**
- Output: state label string (default "NEUTRAL" on failure)

**`get_regime_weights(self, state)`**
- Inputs: state label str
- Output: dict of signal multipliers for the state
  - BULL: {momentum: 2.0, mean_reversion: 0.5}
  - EUPHORIA: {momentum: 1.5, mean_reversion: 0.3, max_position_pct: 0.5}
  - NEUTRAL: {} (all × 1.0)
  - BEAR: {momentum: 0.3, mean_reversion: 2.0}
  - CRISIS: {all_longs: 0.0}

**`compare_aic_bic(self, price_df)`**
- Fits both 3-state and 5-state HMMs, computes AIC/BIC
- Output: dict with aic/bic for both, preferred model by BIC

**`log_transition_matrix(self)`**
- Logs formatted transition matrix to logger.info

**`partial_fit(self, new_price_df, lookback_days=252)`**
- Re-fits on last `lookback_days` rows — simply calls `fit()`

---

#### Class: `AutocorrelationSignals`
Autocorrelation-based signals.

**`ljung_box_test(self, returns_series)`**
- Inputs: pd.Series of returns
- Tests at lags [1, 2, 3, 5, 10]
- Uses statsmodels `acorr_ljungbox` if available; otherwise manual Q-statistic
- Output: list of dicts {lag, statistic, p_value, is_significant (p < 0.05)}

**`get_momentum_signal(self, ticker, price_df)`**
- Inputs: ticker str, OHLCV DataFrame (minimum 60 rows)
- Computes log returns, runs Ljung-Box test
- If significant autocorrelation at lag k: computes ACF, checks if |ACF| > 0.05
- Signal: if ACF > 0 (positive autocorrelation), follow recent direction; if ACF < 0, fade it
- Output: dict {signal (+1/-1/0), lag, acf_value, p_value, reason}

**`mean_reversion_halflife(self, price_series)`**
- Inputs: pd.Series of prices (minimum 30)
- OLS regression: `Δp_t = const + λ * p_{t-1} + ε_t`
- Returns None if λ >= 0 (not mean-reverting) or half_life > 365 or <= 0
- Output: float half-life in days, or None

**`get_mean_reversion_signal(self, ticker, price_df, halflife_threshold=10.0)`**
- Inputs: ticker, OHLCV DataFrame (minimum 40 rows), halflife threshold (default 10 days)
- Only generates signal if half_life < threshold
- z-score lookback = max(int(half_life * 3), 20) days
- Signal: zscore > 1.0 → -1 (short/fade); zscore < -1.0 → +1 (long/mean-revert)
- Output: dict {signal, zscore, halflife, reason}

**`spectral_dominant_cycle(self, price_series)`**
- Inputs: pd.Series of prices (minimum 60)
- Detrends by subtracting linear fit, applies Hanning window, rfft
- Excludes DC (freq=0) and cycles < 5 days (freq >= 0.2)
- Output: dominant cycle length in days (float) or None

---

#### Class: `MathematicalSignals`
Main orchestrator. Combines HMM + autocorrelation signals per ticker with 1-hour cache.

**`__init__(self, config_path)`**
- Instantiates `HMMSignals` as `self._hmm`, `AutocorrelationSignals` as `self._ac`
- In-memory cache: `self._cache` (dict ticker→results), `self._cache_ts` (dict ticker→timestamp)

**`_is_cached(self, ticker)`**
- Returns True if ticker in cache and age < 3600 seconds

**`_store_cache(self, ticker, result)`**
- Writes result to `self._cache[ticker]` with current time

**`run_all(self, tickers, price_data=None)`**
- Inputs: list of ticker strings, optional dict of ticker→OHLCV DataFrame
- If price_data is None, calls `_fetch_yfinance(tickers)` (2-year download)
- For each ticker: checks cache, calls `_analyse_ticker()`, stores result
- Output: dict ticker→result dict

**`_analyse_ticker(self, ticker, df)`**
- Calls: `HMMSignals.fit()`, `HMMSignals.get_state()`, `HMMSignals.get_signal_strength()`
- Calls: `AutocorrelationSignals.get_momentum_signal()`, `get_mean_reversion_signal()`, `spectral_dominant_cycle()`
- Calls: `_combine_signals()`
- Output: dict {ticker, hmm, momentum, mean_reversion, spectral_cycle_days, combined_signal, computed_at}

**`_combine_signals(res)`** (static)
- See MATHEMATICS section below
- Output: float in [-1, +1]

**`get_combined_signal(self, ticker)`**
- Returns cached combined_signal for ticker, or 0.0

**`analyse(self, tickers, price_data=None)`**
- Alias for `run_all()`. Accepts single ticker string or list.

**`_fetch_yfinance(tickers)`** (static)
- Downloads 2-year OHLCV via yfinance, flattens MultiIndex columns

---

#### Class: `KalmanSignalSmoother`
Applies Kalman filtering to raw signal scores to reduce noise.

State-space model:
- State: θ_t = θ_{t-1} + w_t (w_t ~ N(0, Q))
- Observation: y_t = θ_t + v_t (v_t ~ N(0, R))

**`__init__(self, transition_cov=1e-3, observation_cov=1e-1)`**
- Parameters: Q (process noise), R (observation noise)

**`smooth_score(self, scores, ticker="default")`**
- Inputs: list of float scores (most recent last), ticker str
- Falls back to `scores[-1]` if pykalman unavailable or < 3 points
- Output: Kalman-smoothed current score float

**`smooth_series(self, series)`**
- Inputs: numpy array
- Output: same-length smoothed array, or original on failure

---

#### Class: `KalmanPairsTrader`
Dynamic hedge ratio estimation for pairs trading.

State: x_t = [β_t, α_t]^T (hedge ratio + intercept)
Observation: y_t = H_t * x_t + v_t where H_t = [price_x_t, 1]

**`__init__(self, delta=1e-4)`**
- `_Vw = delta / (1.0 - delta)` — process noise variance

**`update(self, price_x, price_y)`**
- Prediction step: `P_pred = self._P + Q` where `Q = Vw * I`
- Innovation: `e = price_y - y_hat`
- Measurement noise: `S = H @ P_pred @ H.T + var(e_history[-50:])` (or 1.0 if < 10 obs)
- Kalman gain: `K = (P_pred @ H.T) / S`
- State update: `theta = theta + K * e`
- Covariance update: standard
- Entry signals: z-score < -2.0 → +1 (long spread), z-score > 2.0 → -1 (short spread)
- Output: dict {hedge_ratio, intercept, spread, spread_mean, spread_std, z_score, signal}

### C) MATHEMATICS

FORMULA [HMM feature: log daily return]:
  `log_ret = np.log(df["Close"] / df["Close"].shift(1)).fillna(0).values`

FORMULA [HMM feature: volume z-score]:
  `vol_mean = vol.rolling(20, min_periods=5).mean()`
  `vol_std = vol.rolling(20, min_periods=5).std().replace(0, np.nan)`
  `vol_z = ((vol - vol_mean) / vol_std).fillna(0).values`

FORMULA [HMM feature: high-low range]:
  `hl_range = ((df["High"] - df["Low"]) / df["Close"].replace(0, np.nan)).fillna(0).values`

FORMULA [HMM feature: close-open gap]:
  `co_gap = ((df["Close"] - df["Open"]) / df["Open"].replace(0, np.nan)).fillna(0).values`

FORMULA [FiveStateHMM feature: realized volatility]:
  `realized_vol = np.log(close/close.shift(1)).rolling(30, min_periods=10).std().fillna(0).values`

FORMULA [FiveStateHMM feature: yield curve proxy]:
  `mom10 = (close / close.shift(10) - 1).fillna(0).values`
  `mom30 = (close / close.shift(30) - 1).fillna(0).values`
  `yield_curve_prx = mom10 - mom30`

FORMULA [Manual Ljung-Box Q statistic (statsmodels fallback)]:
  `Q = n * (n + 2) * sum(acf[k]^2 / (n - k - 1) for k in range(lag))`
  where `n = len(arr)`, lags tested = [1, 2, 3, 5, 10]

FORMULA [Ornstein-Uhlenbeck mean-reversion half-life]:
  OLS: `Δp_t = const + λ * p_{t-1} + ε_t`
  Code: `X = np.column_stack([np.ones(len(lag)), lag])`
  `coef = np.linalg.lstsq(X, delta, rcond=None)`
  `lam = coef[1]`
  `half_life = float(-np.log(2) / lam)` (only if lam < 0)
  Output: days

FORMULA [Mean-reversion z-score]:
  `lookback = max(int(half_life * 3), 20)`
  `mu = window.mean()`
  `sigma = window.std()`
  `zscore = (prices.iloc[-1] - mu) / sigma`
  Signal: zscore > 1.0 → -1; zscore < -1.0 → +1

FORMULA [Spectral dominant cycle]:
  Detrend: `poly = np.polyfit(x, arr, 1); detrended = arr - np.polyval(poly, x)`
  Window: `windowed = detrended * np.hanning(len(detrended))`
  FFT: `fft_vals = np.fft.rfft(windowed); power = np.abs(fft_vals) ** 2`
  Frequency filter: `valid_mask = (freqs > 0) & (freqs < 0.2)` (> 5-day cycles)
  `dominant_freq = freqs[valid_mask][np.argmax(power[valid_mask])]`
  `cycle_days = 1.0 / dominant_freq`

FORMULA [MathematicalSignals combined signal]:
  Inputs:
    HMM: `sig * strength * 0.50` (weight 0.50 — only if hmm_signal is not None)
    Momentum: `mom_signal * 0.30` (weight 0.30 — only if signal != 0)
    Mean-reversion: `mr_signal * 0.20` (weight 0.20 — only if signal != 0)
  Calculation:
    `weighted_sum` accumulates each active component
    `total_weight` accumulates weights of active components only
    `combined = weighted_sum / total_weight`
    `combined = float(np.clip(combined, -1.0, 1.0))`
  Output: float [-1, +1]

FORMULA [AIC/BIC for HMM model comparison]:
  `k = (n_states - 1) + n_states * (n_states - 1) + n_states * n_feat * 2` (conservative param count)
  `aic = -2 * logL * n_obs + 2 * k`
  `bic = -2 * logL * n_obs + k * np.log(n_obs)`
  Preferred: lower BIC

FORMULA [Kalman pairs trader — process noise]:
  `Vw = delta / (1.0 - delta)` where default `delta = 1e-4` → `Vw ≈ 1.0001e-4`

FORMULA [Kalman pairs trader — measurement noise S]:
  `S = float(H @ P_pred @ H.T) + float(np.var(e_history[-50:]) if len(e_history) >= 10 else 1.0)`

### D) DATA FLOWS

Data enters from:
- OHLCV DataFrame (passed in by caller — must come from price fetcher)
- yfinance (optional fallback via `_fetch_yfinance()`, 2-year period)
- `config/settings.yaml` (loaded but minimal use; config values not currently read in signal logic)

Data leaves as:
- In-memory dict (ticker→result) returned by `run_all()` / `analyse()`
- In-memory cache (1-hour TTL, lost on restart)

DB reads: None
DB writes: None

### E) DEPENDENCIES

Internal Apollo imports: None (standalone analysis module)
External libraries:
- `numpy` (np)
- `pandas` (pd)
- `hmmlearn` — optional (`HMM_AVAILABLE` flag), `GaussianHMM`
- `scipy.stats` — optional (`SCIPY_AVAILABLE` flag), chi2 CDF
- `statsmodels.api` — optional (`STATSMODELS_AVAILABLE` flag), `acorr_ljungbox`
- `statsmodels.stats.diagnostic` — for Ljung-Box (imported inline)
- `pykalman` — optional (`PYKALMAN_AVAILABLE` flag), `KalmanFilter`
- `yfinance` — optional (standalone/test use)
- `yaml` — for config loading
- `logging`, `time`, `dataclasses`, `datetime`, `typing`

### F) WIRING STATUS

**`MathematicalSignals` — Connected to live trading path: YES (via `_capture_context`)**

`paper_trader.__init__()` (line 251-252) instantiates `MathematicalSignals` as `self.math_signals`.

`paper_trader._capture_context()` (lines 941-957) calls:
1. `ms.analyse([ticker])` — fits HMM and computes autocorrelation signals
2. `ms.get_combined_signal(ticker)` — retrieves cached combined float
3. Maps combined_signal to: > 0.20 → "BULL", < -0.20 → "BEAR", else → "NEUTRAL"
4. Stores as `context_at_open["hmm_state"]` (str) and `context_at_open["hmm_state_raw"]` (float)

This is stored in context only. The HMM state is NOT used to gate or size trades in paper_trader._process(). It is stored for attribution purposes only.

**`FiveStateHMM`, `KalmanSignalSmoother`, `KalmanPairsTrader`** — NOT connected to any live trading path. Defined and importable but no callers exist in main.py or paper_trader.py.

### G) ISSUES FOUND

1. **`HMMSignals.fit()` state validation can silently return False**: If the fitted HMM produces a bull state with mean return <= 0 (e.g., in a persistent bear market), the model is rejected and HMM signals are skipped silently. The caller in `_analyse_ticker()` proceeds with empty `hmm` dict; combined signal will not include HMM component. This is by design but means HMM often produces no signal in adverse markets.

2. **`_combine_signals()` normalises by active weights only**: If only HMM is active (momentum and mean-reversion both return 0), the combined signal = HMM_signal * strength (not divided by 0.50, divided by 0.50). This is correct in principle but can amplify the HMM signal relative to a full 3-signal scenario. A combined signal of +1 from HMM alone carries the same weight as +1 from all three agreeing.

3. **Cache is in-memory only**: 1-hour TTL but lost on process restart. HMM is re-fitted from scratch after every restart, which requires price downloads and 252+ bars.

4. **`FiveStateHMM._build_features()` slot 4 description mismatch**: Docstring says feature 4 is "vix_proxy — same as realized_vol", but the code computes `short_vol = 5d rolling std`. The description is wrong.

5. **`compare_aic_bic()` AIC/BIC formula**: Uses `logL * n_obs` where `logL = model.score(features)`. hmmlearn's `score()` returns the log-likelihood per observation (log-likelihood / n_obs). Multiplying by `n_obs` gives total log-likelihood, which is correct. This is fine but non-obvious.

6. **`KalmanPairsTrader` requires `pykalman`** but falls back to `signal: 0` if unavailable. No warning is logged in `update()` when pykalman is missing, only at module import.

---

## FILE 4: /home/dannyelticala/quant-fund/analysis/bayesian_regime.py

### A) PURPOSE

Probabilistic regime detector using a 4-component Gaussian Mixture Model (GMM) as a lightweight alternative to full Bayesian MCMC (PyMC). Classifies market into CRISIS/BEAR/NEUTRAL/BULL states with continuous probability estimates. Designed as a parallel comparator to the rule-based `RegimeDetector`. Self-contained — fetches its own data from yfinance.

### B) CLASSES AND METHODS

#### Module-level constants:
- `_STATE_LABELS = ["CRISIS", "BEAR", "NEUTRAL", "BULL"]`
- `_SIZE_MULTIPLIERS = {"BULL": 1.00, "NEUTRAL": 0.85, "BEAR": 0.60, "CRISIS": 0.30}`

#### Class: `BayesianRegimeDetector`

**`__init__(self)`**
- State: `_gmm` (sklearn GaussianMixture or None), `_fitted` (bool), `_last_probs` (dict, default 0.25 each), `_last_regime` (str, default "NEUTRAL"), `_comparison_log` (list, max 100 entries), `_component_order` (list, default [0,1,2,3])

**`_fetch_features(self, lookback=252)`**
- Fetches: SPY (pct_change), ^VIX (close), ^TNX and ^IRX (10Y-2Y yield curve slope)
- Aligns all series on common index
- If TNX/IRX unavailable: yield curve = zeros array
- Output: (N, 3) numpy array [spy_return, vix_level, yield_curve_slope] or None if < 30 observations

**`fit(self, features=None)`**
- Inputs: optional pre-computed (N, 3) feature array; if None, calls `_fetch_features()`
- Fits `sklearn.mixture.GaussianMixture(n_components=4, covariance_type="full", max_iter=200, random_state=42, n_init=5)`
- Sorts components by mean SPY return (ascending → CRISIS, BEAR, NEUTRAL, BULL)
- Stores `_component_order` as sorted index list
- Output: True on success, False on failure
- DB reads/writes: None

**`get_regime_probabilities(self, observation=None)`**
- Inputs: optional (1,3) observation array; if None, fetches last 5 days and uses most recent
- Auto-fits if not fitted
- Calls `gmm.predict_proba(observation)` → raw (4,) probabilities per component
- Reorders to CRISIS, BEAR, NEUTRAL, BULL order using `_component_order`
- Normalises: `ordered /= total`
- Stores result as `_last_probs` and updates `_last_regime`
- Output: dict {state: probability} summing to 1.0

**`get_regime(self)`**
- Output: str — argmax of `get_regime_probabilities()`

**`position_size_multiplier(self)`**
- Computes expected multiplier using probability weighting
- Output: float in [0, 1]

**`compare_with_rule_based(self)`**
- Imports and runs `analysis.regime_detector.RegimeDetector` (graceful on failure → "UNKNOWN")
- Compares `rule_based` vs `bayesian` regime
- Appends to `_comparison_log` (capped at 100 entries)
- Logs INFO if disagreement
- Output: dict {rule_based, bayesian, bayesian_probs, agree}

**`agreement_rate(self)`**
- Output: float — fraction of comparison_log entries where both detectors agreed

### C) MATHEMATICS

FORMULA [GMM feature matrix]:
  Variables:
    `spy_ret` — SPY daily pct_change
    `vix_cl` — VIX daily close level
    `yc` — yield curve slope: `^TNX_close - ^IRX_close` (or 0 if unavailable)
  Calculation: `features = np.column_stack([spy_ret, vix_cl, yc])`
  Output: (N, 3) array

FORMULA [GMM component ordering]:
  `order = np.argsort(gmm.means_[:, 0])` — sort by mean SPY return ascending
  Mapping: index 0 (lowest mean) → CRISIS, index 3 (highest mean) → BULL

FORMULA [Probability reordering]:
  `raw_probs = gmm.predict_proba(observation)[0]`  — (4,) per GMM component
  For each (label_idx, comp_idx) in enumerate(component_order):
    `ordered[label_idx] = raw_probs[comp_idx]`
  `ordered /= ordered.sum()`
  Output: normalised probability vector [p_CRISIS, p_BEAR, p_NEUTRAL, p_BULL]

FORMULA [Probability-weighted position size multiplier]:
  `E[multiplier] = sum(probs[s] * SIZE_MULTIPLIERS[s] for s in STATE_LABELS)`
  where SIZE_MULTIPLIERS = {BULL: 1.00, NEUTRAL: 0.85, BEAR: 0.60, CRISIS: 0.30}
  `output = float(np.clip(expected, 0.0, 1.0))`
  Output: float [0.30, 1.00] (weighted average of state multipliers)

### D) DATA FLOWS

Data enters from:
- yfinance: SPY (pct_change, lookback+30 days), ^VIX (close), ^TNX and ^IRX (close, for yield curve)

Data leaves as:
- In-memory dict `_last_probs` (dict of state→probability)
- In-memory str `_last_regime`
- In-memory list `_comparison_log`

DB reads: None
DB writes: None

### E) DEPENDENCIES

Internal Apollo imports:
- `analysis.regime_detector.RegimeDetector` — imported inline in `compare_with_rule_based()`, graceful on failure

External libraries:
- `sklearn.mixture.GaussianMixture` — optional (`GMM_AVAILABLE` flag)
- `yfinance` — for data fetching
- `pandas` — for index alignment
- `numpy` (np)
- `logging`
- `datetime` (date, timedelta)
- `typing` (Dict, Optional, Tuple)

### F) WIRING STATUS

**Connected to live trading path: NO**

`BayesianRegimeDetector` is not imported in `main.py`, `paper_trader.py`, or any live execution module. It is a standalone analysis class. The comment in the module docstring explicitly states it runs "in parallel with RegimeDetector" for monitoring — but no scheduling or calling mechanism exists in the live path.

It would need to be explicitly instantiated and called (e.g., in `_capture_context` or the main scan loop) to have any effect on live trading.

### G) ISSUES FOUND

1. **Not wired anywhere in live path**: Despite being described as a "parallel" monitoring component, `BayesianRegimeDetector` is never imported or called in any live module found in the codebase.

2. **`_fetch_features()` uses `date.today()` not UTC**: May produce different results depending on server timezone at midnight boundaries.

3. **`get_regime_probabilities()` with no observation fetches only 5 days**: `_fetch_features(lookback=5)` returns ~5 observations which is >= the 30-observation minimum only barely. If markets are closed for several days (holidays), this could return None and fall back to `_last_probs` (stale or default 0.25 each).

4. **`_component_order` default is `[0, 1, 2, 3]` (unfitted order)**: If `get_regime_probabilities()` is called and `fit()` fails, it falls back to default uniform probs. But if called with an observation BEFORE fitting successfully, the `_component_order` is the default `[0,1,2,3]` (identity), not sorted by mean return. The component labels CRISIS/BEAR/NEUTRAL/BULL would be randomly assigned.

5. **`compare_with_rule_based()` uses two separate GMM calls**: `compare_with_rule_based()` calls `get_regime()` which calls `get_regime_probabilities()` which fetches new data — separate from when `fit()` was last called. If market conditions changed between fit and comparison call, results may be inconsistent.

6. **No persistence**: GMM model is fit in-memory each run. Every new process start requires re-fitting (which needs yfinance data download).

---

# SECTION 3B — SIGNAL AGGREGATION MATHEMATICS

## How signals are combined (exact formula)

`aggregate()` computes a **weighted agreement score**:

```
For each signal s:
    w = s["strength"] * s["quality"]
    agrees = (s["direction"] == primary_dir) if primary_dir != 0 else True
    agreement_score += w * (1.0 if agrees else 0.0)
    total_weight += w

confluence = agreement_score / total_weight if total_weight > 0 else 0.0
```

The primary signal is the first signal with `role == "primary_trigger"` (i.e., the PEAD signal). All other signals are compared against the PEAD direction. If there is no primary signal, all signals are counted as agreeing.

## The exact weighting formula

Each signal's weight is: `w = strength * quality`

Per signal type, the inputs are:
- PEAD: `strength = min(abs(surprise_zscore) / 3.0, 1.0)`, `quality = 0.9` (hardcoded)
- Frontier: `strength = min(abs(value), 1.0)`, but confluenc weight = `strength * quality` (quality from dict, default 0.5)
- Context: `strength` from dict (default 0.3), `quality` from dict (default 0.6)
- Altdata: `strength` from dict (default 0.2), `quality` from dict (default 0.5)
- Intelligence: `strength` from dict or `min(abs(raw_score), 1.0)`, `quality` from dict (default 0.65)

## Whether signal weights are static or dynamic

**Partially dynamic, partially static:**
- The per-signal weights (`strength * quality`) are dynamic — they vary with signal values.
- The quality defaults are **static hardcoded values** within the `aggregate()` method body (0.9 for PEAD, 0.5 for frontier, 0.6 for context, 0.5 for altdata, 0.65 for intelligence).
- There is no external weight configuration file used.

## Where signal weights are stored

Weights are hardcoded in `signal_aggregator.py` within the `aggregate()` method body. No DB storage. No config file controls these weights. The `CONFLUENCE_THRESHOLDS` and `CONFLUENCE_KELLY_MULTIPLIERS` dictionaries at module level control level boundaries and Kelly outputs.

## What happens when a signal errors (try/except behaviour)

**There is NO try/except in `aggregate()` itself.** The method has no error handling. If any signal dict is malformed (e.g., `int()` conversion fails on direction), the entire method raises an exception.

In the callers:
- `paper_trader._process()` wraps the `aggregate()` call in a bare `except: pass` (line 1354) — fail-open, trade proceeds.
- `main.py` wraps it in `except Exception` (implied from pattern) — fail-open.

The fail-open means a crash in aggregation is silently ignored and the trade proceeds without confluence gating.

## Output format from aggregate()

```python
{
    "ticker": str,
    "confluence_score": float,      # [0.0, 1.0], rounded to 4 dp
    "confluence_level": str,        # "STRONG" | "MODERATE" | "WEAK" | "NONE"
    "kelly_multiplier": float,      # 1.00 | 0.75 | 0.50 | 0.00
    "combined_direction": int,      # +1 | -1 | 0
    "signal_count": int,            # total number of signals processed
    "active_signals": list[str],    # list of signal names present
    "provenance": list[dict],       # per-signal records for attribution
    "timestamp": str,               # ISO8601 timezone-aware UTC string
}
```

---

# SECTION 3C — CONTEXT_AT_OPEN: WHAT IS IN IT?

`context_at_open` is built in `paper_trader._capture_context()` (lines 851-1027) then enriched in `paper_trader._process()` (lines 1406-1438) before being passed to `record_trade()`.

| Key name | Written in | Value type | Persisted to DB | Recovered on restart |
|----------|-----------|------------|-----------------|----------------------|
| `timestamp` | `_capture_context:856` | str (ISO8601 UTC) | YES — via context_at_open JSON in `open_positions` | YES from DB |
| `pead` | `_capture_context:861` | dict {surprise_pct, surprise_zscore, quality, volume_surge} | YES | YES |
| `macro` | `_capture_context:911` | dict {regime, vix, yield_curve_slope_bps, hy_spread_bps} | YES | YES |
| `macro["regime"]` | `_capture_context:879` | str (regime string or None) | YES | YES |
| `macro["vix"]` | `_capture_context:894` | float or None | YES | YES |
| `macro["yield_curve_slope_bps"]` | `_capture_context:904` | float or None | YES | YES |
| `macro["hy_spread_bps"]` | `_capture_context:905` | float or None | YES | YES |
| `altdata` | `_capture_context:914` | dict {confluence_score, sentiment} | YES | YES |
| `altdata["confluence_score"]` | `_capture_context:917` | float or None | YES | YES |
| `altdata["sentiment"]` | `_capture_context:914` | None (never written post-init) | YES (as null) | YES (as null) |
| `shipping_stress_index` | `_capture_context:933` | float or None | YES | YES |
| `hmm_state` | `_capture_context:950-956` | str ("BULL"/"BEAR"/"NEUTRAL"/None) | YES | YES |
| `hmm_state_raw` | `_capture_context:957` | float or None | YES | YES |
| `earnings_quality` | `_capture_context:966-969` | dict {score, tier} or None | YES | YES |
| `has_sec_crisis_alert` | `_capture_context:981` | bool | YES | YES |
| `geopolitical_risk` | `_capture_context:998` | str ("LOW"/"MEDIUM"/"HIGH"/etc) | YES | YES |
| `signals_at_entry` | `_capture_context:1008` | dict {signal, strength, confidence, tier, strategy} | YES | YES |
| `active_signals` | `_process():1430` | list of dicts {name, strength, role} | YES | YES |
| `signals_at_entry["primary"]` | `_process():1434` | str (signal_type) | YES | YES |
| `signals_at_entry["signals_fired"]` | `_process():1435` | list[str] | YES | YES |
| `signals_at_entry["confluence_count"]` | `_process():1438` | int | YES | YES |

Notes:
- `context_at_open` is stored in `self.active[ticker]` (in-memory dict) and persisted to the `open_positions` table via `self._store.record_trade()` — recovery on restart YES for all keys that were persisted before the crash.
- If `_capture_context()` itself throws, `context_at_open` is `{}` (lines 1399-1403). In this case, the trade is entered but all context fields are missing. This is a silent data loss.
- `altdata["sentiment"]` is initialised to `None` in line 914 and never updated — it is always null.

---

# SECTION 3D — SIGNAL FLOW FROM COLLECTION TO TRADE

## What does aggregate() return exactly?

See SECTION 3B output format above. Key fields for trade gating: `kelly_multiplier` (0.0 = block trade), `confluence_level` (informational), `confluence_score` (float).

## How does paper_trader receive it?

`paper_trader._process()` calls aggregate at lines 1346-1350:

```python
agg_result = self._signal_aggregator.aggregate(
    ticker=ticker,
    pead_signal=pead_sig_dict,
    context_signals=_context_sigs if _context_sigs else None,
)
```

Note: **only `pead_signal` and `context_signals` are passed**. `frontier_signals`, `altdata_signals`, and `intelligence_signals` are NOT passed. This means `signal_count` will typically be 1 (just PEAD) unless there are context signals, and the confluence score measures agreement between PEAD and whatever context signals were built from `_raw_signals`.

## Key names paper_trader expects vs what aggregator produces

paper_trader checks: `agg_result.get("kelly_multiplier", 1.0) == 0.0`

If this is True, return None (block). Otherwise, trade proceeds. The `kelly_multiplier` value (0.75, 0.50 etc.) is NOT used to scale position size — only 0.0 is acted on.

The `agg_result` dict is NOT stored in `context_at_open` at all. The confluence score and kelly multiplier are lost after the gate check. Only the signals themselves (from `_raw_signals`) are written into context via the enrichment block at lines 1406-1438.

## Key/interface mismatches

1. **PEAD signal format mismatch**: `aggregate()` expects `pead_signal["surprise_zscore"]` and `pead_signal["signal"]`. `paper_trader` passes `pead_sig_dict = signal.to_dict() if hasattr(signal, 'to_dict') else dict(signal)`. The PEAD signal object uses `signal` field (int) per the docstring — but the actual signal object key may differ depending on which signal type triggered the trade (non-PEAD signal types also go through this path).

2. **`main.py` aggregator vs paper_trader aggregator**: Two separate `SignalAggregator` instances — one in `main.py` (ephemeral, per ticker scan), one cached on `paper_trader` as `self._signal_aggregator`. They may produce different results because main.py potentially passes more signal types.

3. **`kelly_multiplier` not applied to size**: The gating is binary (0 or not-0). The partial sizing signal (MODERATE=0.75x, WEAK=0.50x) is ignored.

4. **`_raw_signals` normalisation for aggregator**: The code at lines 1327-1344 normalises `_raw_signals` into context_signals format. The `quality_score` is hardcoded to `0.5` for all, and `strength` defaults to `0.3` for zero-value scores. This means any non-PEAD signal passing through this path has lower quality weighting than the `0.9` hardcoded for PEAD.

---

# SECTION 7A — REGIME DETECTION PIPELINE

## What inputs feed regime detection

`MacroRegimeClassifier.classify()` receives:
- `gdp_growth` — always None in live path (not gathered)
- `inflation` — from FRED `CPIAUCSL` via `output/historical_db.db`
- `unemployment` — from FRED `UNRATE` via `output/historical_db.db`
- `unemployment_3m_change` — computed from 3-period lag of UNRATE
- `vix` — from `output/historical_db.db` macro_data table or yfinance fallback
- `yield_curve_slope` — from RatesCreditCollector
- `hy_credit_spread` — from RatesCreditCollector
- `consumer_confidence` — from `altdata/storage/altdata.db`
- `shipping_stress` — from `altdata/storage/altdata.db`
- `geopolitical_crisis` — from `altdata/storage/altdata.db` geopolitical_alerts
- `inversion_weeks` — from RatesCreditCollector

## Exact formula/algorithm used

Rule-based priority ladder (first match wins):
1. CRISIS: VIX > 35 OR HY spread > 500bps OR geopolitical_crisis flag
2. RECESSION_RISK: yield inverted > 13 weeks OR unemployment rising > 0.5ppt OR GDP < 0
3. STAGFLATION: inflation > 4% AND GDP < 1% (GDP always None — **never triggers**)
4. RISK_OFF: 25 ≤ VIX ≤ 35 OR HY > 350bps OR consumer confidence < 80
5. GOLDILOCKS: GDP > 2% AND 1% ≤ inflation ≤ 3% AND unemployment < 5% AND VIX < 18 (GDP always None — **never triggers**)
6. Default: RISK_ON

## Output format

```python
{
    "regime": str,                    # one of 6 regime strings
    "pead_multiplier": float,         # 0.1 to 1.2
    "position_size_multiplier": float, # same as pead_multiplier
    "preferred_sectors": list[str],
    "avoid_sectors": list[str],
    "confidence": float,              # [0.4, 1.0]
    "matched_conditions": list[str],  # human-readable trigger descriptions
}
```

## How it reaches paper_trader._process()

It does NOT reach `_process()` directly. The regime is captured in `_capture_context()`:
1. `MacroSignalEngine.get_complete_briefing_data()` returns regime string
2. Stored as `context_at_open["macro"]["regime"]`
3. After context capture (lines 1019-1025): `self.risk.set_macro_regime(_REGIME_STR_TO_INT[regime])` — wired to risk manager

The regime does NOT gate or adjust `size_pct` inside `_process()` directly. The risk manager may apply it indirectly depending on its own logic.

## How it influences position sizing or trade decisions

Indirectly only. The `pead_multiplier` from `REGIME_PEAD_MULTIPLIERS` is stored in `MacroState.pead_multiplier` but is not read by `_process()` at trade time. The `position_size_multiplier` is similarly computed but not applied in `_process()`. The regime is wired to the risk manager (`set_macro_regime`) which may influence risk checks, but the direct size_pct calculation does not reference macro regime multipliers.

---

# SECTION 7B — MACRO SIGNAL ENGINE MATHEMATICS

All formulas extracted exactly as coded. See FILE 2, SECTION C above for complete list. Summary:

1. Regime classification — rule-based priority ladder (see above)
2. Confidence = `min(0.4 + (n_signals/8) * 0.6, 1.0)` + 0.1 bonus for ≥2 matched conditions
3. Position size multiplier = `regime_mult * fed_mult * rates_mult`
4. Sector modifier = `1.0 + sum_of_deltas` (additive per condition)
5. Macro regime score = `min(pead_multiplier / 1.2, 1.0)`
6. Sector health score = `clamp((sector_modifier - 0.3) / 1.4, 0, 1)`
7. Shipping stress score = `clamp(1.0 - (shipping_stress - 0.5) / 1.5, 0, 1)`
8. Consumer confidence score = `clamp((consumer_confidence - 60) / 50, 0, 1)`
9. Commodity pressure score (oil) = `clamp(0.5 - oil_zscore * 0.15, 0, 1)`
10. Commodity pressure score (copper) = `clamp(0.5 + copper_zscore * 0.10, 0, 1)`
11. Geopolitical risk score = lookup table {LOW:0.8, MEDIUM:0.5, HIGH:0.25, CRITICAL:0.0}
12. Credit conditions (z-score path) = `clamp(0.5 - credit_stress_z * 0.2, 0, 1)`
13. Credit conditions (HY path) = `clamp(1.0 - (hy_spread - 200) / 600, 0, 1)`
14. Composite context = weighted sum of 7 components (weights sum to 1.0)

---

# SECTION 7C — MATHEMATICAL SIGNALS

## What model is used?

Primary: 3-state `GaussianHMM` (hmmlearn) on 4-dimensional OHLCV features.
Secondary: 5-state `GaussianHMM` (hmmlearn) — exists but not used in live path.
Additionally: Ornstein-Uhlenbeck mean-reversion half-life via OLS, Ljung-Box autocorrelation test, FFT spectral cycle detection.
Smoothing (available but not called live): Kalman filter via pykalman.

## What does it output?

`MathematicalSignals.run_all()` returns per ticker:
```python
{
    "ticker": str,
    "hmm": {current_state, state_label, state_probs, likely_next_state, hmm_signal, signal_strength},
    "momentum": {signal, lag, acf_value, p_value, reason},
    "mean_reversion": {signal, zscore, halflife, reason},
    "spectral_cycle_days": float or None,
    "combined_signal": float in [-1, +1],
    "computed_at": str (UTC ISO),
}
```

## Is hmmlearn actually being used? Where?

YES, conditionally. `HMM_AVAILABLE` flag is set at import time. If hmmlearn is installed:
- `HMMSignals.fit()` is called in `_analyse_ticker()` for each ticker
- `HMMSignals.get_state()` retrieves decoded state and probabilities

In `paper_trader.__init__()`, `MathematicalSignals()` is instantiated. In `_capture_context()`, `ms.analyse([ticker])` is called. This will use hmmlearn if installed and if the ticker has 252+ bars of price history.

Whether hmmlearn is actually installed on the VPS is not confirmed from code alone.

## How does it connect to the regime or signal system?

**Only as context data.** The `combined_signal` float is mapped to "BULL"/"BEAR"/"NEUTRAL" and stored in `context_at_open["hmm_state"]`. It is not fed into the `SignalAggregator`. It is not used to gate trades. It is not used to adjust position size. It is stored for post-trade attribution analysis only.

There is no code path that reads `context_at_open["hmm_state"]` and acts on it in the trade entry or exit logic.

---

# SECTION 7D — BAYESIAN REGIME DETECTOR

## Document BayesianRegimeDetector in full

See FILE 4 documentation above for complete class documentation.

Summary:
- 4-component Gaussian Mixture Model on 3 features: [SPY daily return, VIX level, yield curve slope]
- GMM trained on 252-day lookback via yfinance download
- States: CRISIS (lowest mean return) → BEAR → NEUTRAL → BULL (highest mean return)
- Outputs: probability vector {CRISIS: float, BEAR: float, NEUTRAL: float, BULL: float}
- Position size multiplier: `sum(p(state) * size_mult(state))` = probability-weighted expected multiplier
  - BULL×1.00 + NEUTRAL×0.85 + BEAR×0.60 + CRISIS×0.30
- Comparison method `compare_with_rule_based()` can run both GMM and rule-based detectors and log agreement

## Is it connected to anything in the live path?

**NO.** `BayesianRegimeDetector` is not imported or called in any live trading module. It exists as a standalone analysis class with no wiring to main.py, paper_trader.py, or any scheduler.

## If not connected, what would it do if connected?

If connected (e.g., called in `_capture_context()` or the main scan loop):

1. On first call, it would auto-fit a 4-component GMM by downloading ~282 days of SPY, VIX, TNX, IRX data from yfinance. This download would add network latency to the first trade.

2. It would produce a probability vector over {CRISIS, BEAR, NEUTRAL, BULL}.

3. `position_size_multiplier()` would return a float in [0.30, 1.00] representing the probability-weighted expected position multiplier. This would provide a continuous sizing signal rather than the binary PEAD multiplier lookup used by MacroRegimeClassifier.

4. If wired to replace or blend with `MacroRegimeClassifier`, it would offer:
   - Continuous probability estimates (vs hard classification)
   - Data-driven state boundaries (vs hardcoded VIX/spread thresholds)
   - Only 3 input features vs MacroRegimeClassifier's 11 possible inputs

5. `compare_with_rule_based()` would log agreement rates between GMM and rule-based regime detectors, providing a monitoring signal for regime detection quality.

6. It would NOT solve the GDP data gap (MacroRegimeClassifier) — GMM does not use GDP as a feature.

7. The GMM model would be re-fitted from scratch on every process restart (no persistence).

---

# SECTION 5 GATE (GROUP 5)

## Files read:
1. `/home/dannyelticala/quant-fund/closeloop/integration/signal_aggregator.py` — 339 lines, read completely
2. `/home/dannyelticala/quant-fund/analysis/macro_signal_engine.py` — 1146 lines, read completely
3. `/home/dannyelticala/quant-fund/analysis/mathematical_signals.py` — ~1100+ lines, read completely
4. `/home/dannyelticala/quant-fund/analysis/bayesian_regime.py` — 261 lines, read completely

## Key findings:

1. **SignalAggregator is in the live path but only as a binary gate**: kelly_multiplier 0.75x and 0.50x are computed but never applied to position size. The gating is pass/fail only.

2. **SignalAggregator is called twice per trade (main.py + paper_trader) with different inputs**: Main.py may pass more signal types than paper_trader does. Two separate instances, potentially different confluence results for the same event.

3. **`MultiFrequencyAggregator` (4-horizon MFS) is completely dead code in the live path**: All h1-h4 horizon weighting logic exists but is never called.

4. **GDP data is never collected**: `gdp_growth` is always `None` in `MacroState`. STAGFLATION and GOLDILOCKS regimes can never trigger. The effective regime set is {CRISIS, RECESSION_RISK, RISK_OFF, RISK_ON}.

5. **oil_zscore, copper_zscore, gold_zscore are never populated**: `commodity_pressure_score` always falls back to 0.5 (neutral default).

6. **`BayesianRegimeDetector` is never called in the live path**: Zero wiring.

7. **`FiveStateHMM`, `KalmanSignalSmoother`, `KalmanPairsTrader` are never called in the live path**: Dead code.

8. **HMM signals are stored as context only**: They are in `context_at_open["hmm_state"]` but do not influence trade entry/exit decisions, position sizing, or the signal aggregator.

9. **`_capture_context()` failing silently produces empty context_at_open**: All context data is lost for that trade.

10. **`altdata["sentiment"]` is always null**: Initialized to None and never populated.

11. **`EarningsContextScore._ensure_predictions_log()` creates index on wrong table**: `predictions_log` instead of `earnings_context_log`. Silent failure.

12. **`position_size_multiplier` from MacroSignalEngine is not applied to trades**: The combined `regime_mult * fed_mult * rates_mult` multiplier is computed but not read at trade execution time.

13. **`macro_signal_engine.py` has housing_health, inflation_pressure, consumer_health fields that are never written**.

14. **The confidence formula baseline of 0.4 means even with zero inputs, confidence = 0.4**: Minimum possible confidence is 40% regardless of data availability.

15. **`FiveStateHMM._build_features()` docstring says feature 4 is "vix_proxy — same as realized_vol" but code computes 5d short_vol instead**: Documentation is wrong.

## Contradictions found:

1. **Signal aggregator docstring says WEAK = 0.50x Kelly**: The CONFLUENCE_KELLY_MULTIPLIERS dict confirms this, but paper_trader never applies the multiplier — only checks for 0.0.

2. **`MacroState.position_size_multiplier` is computed as `regime_mult * fed_mult * rates_mult`** but `regime_mult` and `pead_multiplier` are set to the same value (both from `REGIME_PEAD_MULTIPLIERS`). The `position_size_multiplier` name implies it is used for position sizing, but it is not applied in paper_trader.

3. **`BayesianRegimeDetector` docstring says it "runs in parallel with RegimeDetector"** — but it is not wired to run at all.

4. **`HMMSignals` is described as the primary HMM class** and `FiveStateHMM` as "backwards compatible with HMMSignals — does NOT replace it" — but only `HMMSignals` is used in the live path. `FiveStateHMM` is more sophisticated but unused.

5. **Module docstring for `mathematical_signals.py` mentions `yfinance` is "only used in `__main__` test block"** — but `MathematicalSignals._fetch_yfinance()` is used as a fallback when `price_data=None`, and `KalmanPairsTrader.update()` does not use yfinance. The docstring is partially incorrect.

## Formulas extracted:

1. PEAD strength normalisation: `min(abs(surprise_zscore) / 3.0, 1.0)`
2. Confluence score: `agreement_score / total_weight`
3. Signal weight: `w = strength * quality`
4. MFS horizon score: `np.mean(values in horizon dict)`
5. MFS combined: `h1*0.20 + h2*0.35 + h3*0.30 + h4*0.15` clipped to [-1,+1]
6. MFS conflict penalty: `size_mult *= 0.55` when h2 > 0.3 AND h3 < -0.2
7. Regime confidence: `min(0.4 + (n/8)*0.6, 1.0)` + 0.1 if ≥2 triggers
8. Position size multiplier: `regime_mult * fed_mult * rates_mult`
9. Sector modifier: `1.0 + additive_delta`
10. Macro regime score: `min(pead_multiplier / 1.2, 1.0)`
11. Sector health score: `clamp((sector_modifier - 0.3) / 1.4, 0, 1)`
12. Shipping stress score: `clamp(1.0 - (shipping_stress - 0.5) / 1.5, 0, 1)`
13. Consumer confidence score: `clamp((consumer_confidence - 60) / 50, 0, 1)`
14. Commodity oil score: `clamp(0.5 - oil_zscore * 0.15, 0, 1)`
15. Commodity copper score: `clamp(0.5 + copper_zscore * 0.10, 0, 1)`
16. Credit conditions (z): `clamp(0.5 - credit_stress_z * 0.2, 0, 1)`
17. Credit conditions (HY): `clamp(1.0 - (hy_spread - 200) / 600, 0, 1)`
18. Composite context score: weighted sum of 7 components
19. HMM feature — log return: `log(Close[t] / Close[t-1])`
20. HMM feature — volume z-score: `(vol - 20d_mean) / 20d_std`
21. HMM feature — HL range: `(High - Low) / Close`
22. HMM feature — CO gap: `(Close - Open) / Open`
23. 5-state HMM feature — realized vol: `30d rolling std of log returns`
24. 5-state HMM feature — yield curve proxy: `(Close/Close.shift(10)-1) - (Close/Close.shift(30)-1)`
25. OU half-life: `-ln(2) / λ` (where λ from OLS `Δp_t = const + λ*p_{t-1}`)
26. Mean-reversion z-score: `(price_last - mu) / sigma` (lookback = max(halflife*3, 20))
27. Spectral cycle: `1.0 / dominant_frequency` (from rfft with Hanning window, detrended)
28. MathematicalSignals combined: `clip((HMM*strength*0.5 + mom*0.3 + mr*0.2) / active_weight, -1, 1)`
29. AIC: `-2 * logL * n_obs + 2 * k`
30. BIC: `-2 * logL * n_obs + k * log(n_obs)`
31. GMM component order: `np.argsort(gmm.means_[:, 0])` ascending by SPY return
32. Bayesian position multiplier: `sum(p(state) * SIZE_MULTIPLIER[state])` for all states

## Data flows documented:

1. `signal_aggregator.aggregate()`: signal dicts IN → confluence score dict OUT — no DB
2. `MacroSignalEngine.run_full_analysis()`: 3 DBs IN (historical_db, altdata, permanent_archive via rates) + yfinance → MacroState OUT
3. `EarningsContextScore.calculate()`: macro_data dict + ticker/sector IN → composite score dict OUT + INSERT to earnings_context_log
4. `MathematicalSignals.run_all()`: OHLCV DataFrames IN → signal dict per ticker OUT — no DB, in-memory cache
5. `BayesianRegimeDetector.fit()`: yfinance download → GMM model IN MEMORY — no DB
6. `context_at_open`: assembled in _capture_context → enriched in _process → stored via record_trade to open_positions DB table
7. `paper_trader._process()` aggregator path: signal IN → aggregate() → kelly_multiplier check → proceed or block trade

## Proceed to GROUP 6: YES
