"""
STEP 9 — Macro Signal Engine
==============================
Orchestrates all macro data sources into a unified signal layer that gates
PEAD trades, sizes positions, and provides context scores for upcoming
earnings events.

Sub-engines:
  - MacroRegimeClassifier  — classifies the current macro regime
  - SectorContextEngine    — sector-level modifiers driven by macro data
  - EarningsContextScore   — per-ticker context score for upcoming earnings
  - MacroSignalEngine      — main orchestrator used by all strategy modules

Dependencies (all graceful on failure):
  - data/collectors/rates_credit_collector.py  (STEP 8)
  - altdata module (shipping, consumer confidence, geopolitical alerts)
  - output/permanent_archive.db
  - output/historical_db.db
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regime definitions (checked in priority order)
REGIME_PRIORITY = [
    "CRISIS",
    "RECESSION_RISK",
    "STAGFLATION",
    "RISK_OFF",
    "GOLDILOCKS",
    "RISK_ON",
]

REGIME_PEAD_MULTIPLIERS: Dict[str, float] = {
    "CRISIS": 0.1,
    "RECESSION_RISK": 0.4,
    "STAGFLATION": 0.6,
    "RISK_OFF": 0.7,
    "GOLDILOCKS": 1.2,
    "RISK_ON": 1.0,
}

REGIME_PREFERRED_SECTORS: Dict[str, List[str]] = {
    "CRISIS": ["utilities", "gold", "treasuries", "staples"],
    "RECESSION_RISK": ["utilities", "healthcare", "staples", "defence"],
    "STAGFLATION": ["energy", "commodities", "mining", "real_assets"],
    "RISK_OFF": ["utilities", "healthcare", "defence", "staples"],
    "GOLDILOCKS": ["technology", "consumer_discretionary", "industrials", "financials"],
    "RISK_ON": ["technology", "consumer_discretionary", "small_cap", "financials"],
}

REGIME_AVOID_SECTORS: Dict[str, List[str]] = {
    "CRISIS": ["financials", "consumer_discretionary", "real_estate", "airlines"],
    "RECESSION_RISK": ["consumer_discretionary", "industrials", "real_estate", "financials"],
    "STAGFLATION": ["growth", "technology", "consumer_discretionary", "financials"],
    "RISK_OFF": ["growth", "consumer_discretionary", "real_estate"],
    "GOLDILOCKS": ["utilities", "gold"],
    "RISK_ON": ["utilities"],
}


# ---------------------------------------------------------------------------
# MacroState dataclass
# ---------------------------------------------------------------------------

@dataclass
class MacroState:
    """Complete snapshot of all macro signals at a point in time."""

    # Timestamp
    as_of: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Regime
    regime: str = "RISK_ON"
    regime_confidence: float = 0.5
    pead_multiplier: float = 1.0
    position_size_multiplier: float = 1.0

    # Rates / yield curve
    yield_curve_slope: Optional[float] = None
    is_inverted: bool = False
    inversion_weeks: int = 0
    yield_momentum_10yr: Optional[float] = None
    yields_rising_fast: bool = False
    rates_regime: str = "NEUTRAL"
    breakeven_inflation: Optional[float] = None

    # Credit
    hy_spread: Optional[float] = None
    ig_spread: Optional[float] = None
    credit_stress_z: Optional[float] = None
    ted_spread: Optional[float] = None

    # Macro fundamentals (from altdata / FRED)
    gdp_growth: Optional[float] = None
    inflation: Optional[float] = None
    unemployment: Optional[float] = None
    vix: Optional[float] = None
    consumer_confidence: Optional[float] = None
    consumer_confidence_change: Optional[float] = None

    # Commodities
    oil_price: Optional[float] = None
    oil_1m_change: Optional[float] = None
    copper_price: Optional[float] = None
    copper_1m_change: Optional[float] = None
    gold_price: Optional[float] = None
    gold_1m_change: Optional[float] = None
    oil_zscore: Optional[float] = None
    copper_zscore: Optional[float] = None
    gold_zscore: Optional[float] = None

    # Shipping
    shipping_stress: Optional[float] = None
    shipping_sector_impacts: Dict[str, float] = field(default_factory=dict)

    # Geopolitical
    geopolitical_risk_level: str = "LOW"
    active_alerts: List[str] = field(default_factory=list)
    geopolitical_crisis: bool = False

    # Housing
    housing_health: Optional[float] = None
    inflation_pressure: Optional[float] = None
    consumer_health: Optional[float] = None

    # Sector modifiers
    sector_modifiers: Dict[str, float] = field(default_factory=dict)

    # Fed
    days_to_fed_meeting: int = 999
    fed_position_multiplier: float = 1.0

    # Upcoming earnings context
    upcoming_earnings_context: List[Dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# MacroRegimeClassifier
# ---------------------------------------------------------------------------

class MacroRegimeClassifier:
    """
    Classifies the current macro regime from available data inputs.

    All inputs are optional — sensible defaults are used when not available.
    Regimes are checked in priority order; the first matching wins.
    """

    def classify(
        self,
        gdp_growth: Optional[float] = None,
        inflation: Optional[float] = None,
        unemployment: Optional[float] = None,
        unemployment_3m_change: Optional[float] = None,
        vix: Optional[float] = None,
        yield_curve_slope: Optional[float] = None,
        hy_credit_spread: Optional[float] = None,
        consumer_confidence: Optional[float] = None,
        shipping_stress: Optional[float] = None,
        geopolitical_crisis: bool = False,
        inversion_weeks: int = 0,
    ) -> Dict[str, Any]:
        """
        Classify regime and return full regime dict.

        Returns:
            {
              'regime': str,
              'pead_multiplier': float,
              'position_size_multiplier': float,
              'preferred_sectors': list,
              'avoid_sectors': list,
              'confidence': float,
              'matched_conditions': list,
            }
        """
        matched_conditions: List[str] = []
        regime = "RISK_ON"

        # ── CRISIS ──────────────────────────────────────────────────────
        crisis_triggers = []
        if vix is not None and vix > 35:
            crisis_triggers.append(f"VIX={vix:.1f}>35")
        if hy_credit_spread is not None and hy_credit_spread > 500:
            crisis_triggers.append(f"HY_spread={hy_credit_spread:.0f}>500bps")
        if geopolitical_crisis:
            crisis_triggers.append("CRITICAL_geopolitical_alert")
        if crisis_triggers:
            regime = "CRISIS"
            matched_conditions.extend(crisis_triggers)

        # ── RECESSION_RISK ───────────────────────────────────────────────
        elif True:
            rec_triggers = []
            if yield_curve_slope is not None and yield_curve_slope < 0 and inversion_weeks > 13:
                rec_triggers.append(f"yield_curve_inverted_{inversion_weeks}wks")
            if unemployment_3m_change is not None and unemployment_3m_change > 0.5:
                rec_triggers.append(f"unemployment_rising_{unemployment_3m_change:.2f}ppt_3mo")
            if gdp_growth is not None and gdp_growth < 0:
                rec_triggers.append(f"GDP={gdp_growth:.1f}%")
            if rec_triggers:
                regime = "RECESSION_RISK"
                matched_conditions.extend(rec_triggers)

        if regime == "RISK_ON":
            # ── STAGFLATION ──────────────────────────────────────────────
            if (
                inflation is not None and inflation > 4.0 and
                gdp_growth is not None and gdp_growth < 1.0
            ):
                regime = "STAGFLATION"
                matched_conditions.append(f"inflation={inflation:.1f}%>4, GDP={gdp_growth:.1f}%<1")

            # ── RISK_OFF ─────────────────────────────────────────────────
            elif (
                (vix is not None and 25 <= vix <= 35) or
                (hy_credit_spread is not None and hy_credit_spread > 350) or
                (consumer_confidence is not None and consumer_confidence < 80)
            ):
                regime = "RISK_OFF"
                if vix is not None and 25 <= vix <= 35:
                    matched_conditions.append(f"VIX={vix:.1f} in [25,35]")
                if hy_credit_spread is not None and hy_credit_spread > 350:
                    matched_conditions.append(f"HY_spread={hy_credit_spread:.0f}>350bps")
                if consumer_confidence is not None and consumer_confidence < 80:
                    matched_conditions.append(f"consumer_confidence={consumer_confidence:.0f}<80")

            # ── GOLDILOCKS ───────────────────────────────────────────────
            elif (
                (gdp_growth is not None and gdp_growth > 2.0) and
                (inflation is not None and 1.0 <= inflation <= 3.0) and
                (unemployment is not None and unemployment < 5.0) and
                (vix is not None and vix < 18)
            ):
                regime = "GOLDILOCKS"
                matched_conditions.append(
                    f"GDP={gdp_growth:.1f}%>2, inf={inflation:.1f}% in [1,3], "
                    f"unemp={unemployment:.1f}%<5, VIX={vix:.1f}<18"
                )

        # ── Confidence calculation ────────────────────────────────────────
        n_signals_available = sum([
            gdp_growth is not None,
            inflation is not None,
            unemployment is not None,
            vix is not None,
            yield_curve_slope is not None,
            hy_credit_spread is not None,
            consumer_confidence is not None,
            shipping_stress is not None,
        ])
        confidence = min(0.4 + (n_signals_available / 8) * 0.6, 1.0)
        if len(matched_conditions) >= 2:
            confidence = min(confidence + 0.1, 1.0)

        multiplier = REGIME_PEAD_MULTIPLIERS.get(regime, 1.0)

        return {
            "regime": regime,
            "pead_multiplier": multiplier,
            "position_size_multiplier": multiplier,
            "preferred_sectors": REGIME_PREFERRED_SECTORS.get(regime, []),
            "avoid_sectors": REGIME_AVOID_SECTORS.get(regime, []),
            "confidence": round(confidence, 3),
            "matched_conditions": matched_conditions,
        }


# ---------------------------------------------------------------------------
# SectorContextEngine
# ---------------------------------------------------------------------------

class SectorContextEngine:
    """
    Applies sector-specific multipliers driven by macro conditions.

    All sector names use snake_case. Modifiers are additive deltas on a
    base of 1.0 (so 1.0 = no change, 0.7 = 30% reduction, 1.3 = 30% boost).
    """

    # Default sector list
    ALL_SECTORS = [
        "retailers", "food_manufacturers", "domestic_producers", "air_freight",
        "shipping", "airlines", "trucking", "energy_producers", "energy",
        "industrials", "mining", "restaurants", "entertainment", "luxury",
        "dollar_stores", "banks", "growth", "utilities", "defence",
        "healthcare", "technology", "consumer_discretionary", "real_estate",
        "staples", "materials", "financials", "small_cap",
    ]

    def get_modifier(self, sector: str, macro_data: Dict[str, Any]) -> float:
        """
        Return multiplicative modifier for a single sector.

        Args:
            sector: sector name (snake_case)
            macro_data: dict with keys matching MacroState fields

        Returns:
            float — 1.0 = neutral, >1.0 = favourable, <1.0 = unfavourable
        """
        delta = 0.0

        shipping_stress = macro_data.get("shipping_stress")
        oil_1m_change = macro_data.get("oil_1m_change")
        copper_1m_change = macro_data.get("copper_1m_change")
        consumer_confidence_change = macro_data.get("consumer_confidence_change")
        yield_curve_slope = macro_data.get("yield_curve_slope")
        geopolitical_crisis = macro_data.get("geopolitical_crisis", False)

        # ── Shipping stress ───────────────────────────────────────────────
        if shipping_stress is not None and shipping_stress > 1.5:
            impacts = {
                "retailers": -0.3,
                "food_manufacturers": -0.2,
                "domestic_producers": +0.3,
                "air_freight": +0.2,
                "shipping": +0.3,
            }
            delta += impacts.get(sector, 0.0)

        # ── Oil price shock ───────────────────────────────────────────────
        if oil_1m_change is not None and oil_1m_change > 0.15:
            impacts = {
                "airlines": -0.4,
                "trucking": -0.3,
                "energy_producers": +0.4,
                "energy": +0.3,
            }
            delta += impacts.get(sector, 0.0)

        # ── Copper demand surge ───────────────────────────────────────────
        if copper_1m_change is not None and copper_1m_change > 0.10:
            impacts = {
                "industrials": +0.2,
                "mining": +0.3,
            }
            delta += impacts.get(sector, 0.0)

        # ── Consumer confidence collapse ──────────────────────────────────
        if consumer_confidence_change is not None and consumer_confidence_change < -10:
            impacts = {
                "restaurants": -0.3,
                "entertainment": -0.3,
                "luxury": -0.4,
                "dollar_stores": +0.2,
                "consumer_discretionary": -0.2,
            }
            delta += impacts.get(sector, 0.0)

        # ── Inverted yield curve (banks hurt) ─────────────────────────────
        if yield_curve_slope is not None and yield_curve_slope < -0.5:
            impacts = {
                "banks": -0.3,
                "financials": -0.2,
                "growth": -0.2,
                "utilities": +0.1,
            }
            delta += impacts.get(sector, 0.0)

        # ── Geopolitical crisis ───────────────────────────────────────────
        if geopolitical_crisis:
            impacts = {
                "defence": +0.3,
                "energy": +0.2,
                "energy_producers": +0.2,
            }
            sector_delta = impacts.get(sector, -0.2)  # default: all sectors -0.2
            delta += sector_delta

        return round(1.0 + delta, 4)

    def get_all_modifiers(self, macro_data: Dict[str, Any]) -> Dict[str, float]:
        """Return a modifier dict for all known sectors."""
        return {sector: self.get_modifier(sector, macro_data) for sector in self.ALL_SECTORS}


# ---------------------------------------------------------------------------
# EarningsContextScore
# ---------------------------------------------------------------------------

class EarningsContextScore:
    """
    Calculates a composite context score (0-1) for an upcoming earnings event,
    incorporating macro regime, sector health, and various risk factors.

    Every calculation is stored permanently in output/permanent_archive.db.
    """

    COMPONENT_WEIGHTS = {
        "macro_regime_score": 0.25,
        "sector_health_score": 0.20,
        "shipping_stress_score": 0.10,
        "consumer_confidence_score": 0.15,
        "commodity_pressure_score": 0.10,
        "geopolitical_risk_score": 0.10,
        "credit_conditions_score": 0.10,
    }

    def __init__(self, archive_db_path: str = "output/permanent_archive.db"):
        self.archive_db_path = archive_db_path
        self._sector_engine = SectorContextEngine()
        self._regime_classifier = MacroRegimeClassifier()
        self._ensure_predictions_log()

    def _ensure_predictions_log(self) -> None:
        """Create predictions_log table if not present."""
        Path(self.archive_db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.archive_db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS earnings_context_log (
                    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                    calc_timestamp             TEXT NOT NULL,
                    ticker                     TEXT NOT NULL,
                    sector                     TEXT,
                    composite_score            REAL NOT NULL,
                    label                      TEXT NOT NULL,
                    macro_regime_score         REAL,
                    sector_health_score        REAL,
                    shipping_stress_score      REAL,
                    consumer_confidence_score  REAL,
                    commodity_pressure_score   REAL,
                    geopolitical_risk_score    REAL,
                    credit_conditions_score    REAL,
                    regime                     TEXT,
                    macro_data_json            TEXT
                )
            """)
            # Index on ticker_or_sector (canonical schema) or ticker (alt schema)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_pl_ticker ON predictions_log(ticker_or_sector, prediction_date)")
            except Exception:
                try:
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_pl_ticker ON predictions_log(ticker, calc_timestamp)")
                except Exception:
                    pass
            conn.commit()
        finally:
            conn.close()

    def calculate(
        self,
        ticker: str,
        sector: str,
        macro_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Calculate context score for a single ticker/earnings event.

        Returns:
            {
              'ticker': str,
              'sector': str,
              'composite_score': float,
              'label': str (FAVOURABLE/NEUTRAL/UNFAVOURABLE),
              'components': dict,
              'regime': str,
            }
        """
        import json

        # ── 1. Macro regime score ──────────────────────────────────────
        regime_result = self._regime_classifier.classify(
            gdp_growth=macro_data.get("gdp_growth"),
            inflation=macro_data.get("inflation"),
            unemployment=macro_data.get("unemployment"),
            vix=macro_data.get("vix"),
            yield_curve_slope=macro_data.get("yield_curve_slope"),
            hy_credit_spread=macro_data.get("hy_spread"),
            consumer_confidence=macro_data.get("consumer_confidence"),
            shipping_stress=macro_data.get("shipping_stress"),
            geopolitical_crisis=macro_data.get("geopolitical_crisis", False),
            inversion_weeks=macro_data.get("inversion_weeks", 0),
        )
        regime = regime_result["regime"]
        # Map pead_multiplier (0.1–1.2) → score (0–1)
        macro_regime_score = min(regime_result["pead_multiplier"] / 1.2, 1.0)

        # ── 2. Sector health score ─────────────────────────────────────
        sector_modifier = self._sector_engine.get_modifier(sector, macro_data)
        # modifier range is roughly [0.3, 1.7]; map to [0, 1]
        sector_health_score = _clamp((sector_modifier - 0.3) / 1.4, 0.0, 1.0)

        # ── 3. Shipping stress score ───────────────────────────────────
        shipping_stress = macro_data.get("shipping_stress")
        if shipping_stress is None:
            shipping_stress_score = 0.5  # neutral default
        else:
            # stress > 2.0 → 0.0; stress < 0.5 → 1.0
            shipping_stress_score = _clamp(1.0 - (shipping_stress - 0.5) / 1.5, 0.0, 1.0)

        # ── 4. Consumer confidence score ───────────────────────────────
        consumer_confidence = macro_data.get("consumer_confidence")
        if consumer_confidence is None:
            consumer_confidence_score = 0.5
        else:
            # 110+ → 1.0, 60 → 0.0
            consumer_confidence_score = _clamp((consumer_confidence - 60) / 50, 0.0, 1.0)

        # ── 5. Commodity pressure score ────────────────────────────────
        # High oil change is negative for most companies; use oil z-score
        oil_zscore = macro_data.get("oil_zscore")
        copper_zscore = macro_data.get("copper_zscore")
        if oil_zscore is None and copper_zscore is None:
            commodity_pressure_score = 0.5
        else:
            scores = []
            if oil_zscore is not None:
                # high oil z-score → commodity pressure → lower score
                scores.append(_clamp(0.5 - oil_zscore * 0.15, 0.0, 1.0))
            if copper_zscore is not None:
                # high copper often signals growth
                scores.append(_clamp(0.5 + copper_zscore * 0.10, 0.0, 1.0))
            commodity_pressure_score = float(np.mean(scores))

        # ── 6. Geopolitical risk score ─────────────────────────────────
        geo_level = macro_data.get("geopolitical_risk_level", "LOW")
        geopolitical_risk_score = {"LOW": 0.8, "MEDIUM": 0.5, "HIGH": 0.25, "CRITICAL": 0.0}.get(
            str(geo_level).upper(), 0.5
        )

        # ── 7. Credit conditions score ─────────────────────────────────
        credit_stress_z = macro_data.get("credit_stress_z")
        hy_spread = macro_data.get("hy_spread")
        if credit_stress_z is not None:
            # z-score > 2 → poor credit → 0.0; z < -1 → easy credit → 1.0
            credit_conditions_score = _clamp(0.5 - credit_stress_z * 0.2, 0.0, 1.0)
        elif hy_spread is not None:
            # hy_spread 200bps=1.0, 800bps=0.0
            credit_conditions_score = _clamp(1.0 - (hy_spread - 200) / 600, 0.0, 1.0)
        else:
            credit_conditions_score = 0.5

        # ── Weighted composite ─────────────────────────────────────────
        components = {
            "macro_regime_score": round(macro_regime_score, 4),
            "sector_health_score": round(sector_health_score, 4),
            "shipping_stress_score": round(shipping_stress_score, 4),
            "consumer_confidence_score": round(consumer_confidence_score, 4),
            "commodity_pressure_score": round(commodity_pressure_score, 4),
            "geopolitical_risk_score": round(geopolitical_risk_score, 4),
            "credit_conditions_score": round(credit_conditions_score, 4),
        }

        composite = sum(
            components[k] * self.COMPONENT_WEIGHTS[k]
            for k in self.COMPONENT_WEIGHTS
        )
        composite = round(composite, 4)

        if composite >= 0.7:
            label = "FAVOURABLE"
        elif composite >= 0.4:
            label = "NEUTRAL"
        else:
            label = "UNFAVOURABLE"

        result = {
            "ticker": ticker,
            "sector": sector,
            "composite_score": composite,
            "label": label,
            "components": components,
            "regime": regime,
        }

        # ── Persist to predictions_log ─────────────────────────────────
        try:
            conn = sqlite3.connect(self.archive_db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            try:
                conn.execute("""
                    INSERT INTO earnings_context_log (
                        calc_timestamp, ticker, sector, composite_score, label,
                        macro_regime_score, sector_health_score, shipping_stress_score,
                        consumer_confidence_score, commodity_pressure_score,
                        geopolitical_risk_score, credit_conditions_score,
                        regime, macro_data_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    datetime.utcnow().isoformat(),
                    ticker, sector, composite, label,
                    components["macro_regime_score"],
                    components["sector_health_score"],
                    components["shipping_stress_score"],
                    components["consumer_confidence_score"],
                    components["commodity_pressure_score"],
                    components["geopolitical_risk_score"],
                    components["credit_conditions_score"],
                    regime,
                    json.dumps({k: v for k, v in macro_data.items() if isinstance(v, (int, float, str, bool, type(None)))}),
                ))
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("Could not store prediction to predictions_log: %s", exc)

        return result


# ---------------------------------------------------------------------------
# MacroSignalEngine (main orchestrator)
# ---------------------------------------------------------------------------

class MacroSignalEngine:
    """
    Main macro signal orchestrator.

    Aggregates data from all available collectors into a unified MacroState
    that gates PEAD trades, sizes positions, and enriches earnings context.

    Usage:
        engine = MacroSignalEngine(config_path='config/settings.yaml')
        state  = engine.run_full_analysis()
        mult   = engine.get_pead_multiplier()
        brief  = engine.get_complete_briefing_data()
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config_path = config_path
        self.config = self._load_config(config_path)

        self._regime_classifier = MacroRegimeClassifier()
        self._sector_engine = SectorContextEngine()
        self._earnings_scorer = EarningsContextScore(
            archive_db_path="output/permanent_archive.db"
        )

        # Lazy-loaded rates collector
        self._rates_collector = None
        self._state: Optional[MacroState] = None

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(path: str) -> dict:
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning("Could not load config from %s: %s", path, exc)
            return {}

    # ------------------------------------------------------------------
    # Rates collector (lazy)
    # ------------------------------------------------------------------

    def _get_rates_collector(self):
        if self._rates_collector is None:
            try:
                from data.collectors.rates_credit_collector import RatesCreditCollector
                self._rates_collector = RatesCreditCollector(
                    config_path=self.config_path,
                    archive_db_path="output/permanent_archive.db",
                    historical_db_path="output/historical_db.db",
                )
            except Exception as exc:
                logger.warning("Could not initialise RatesCreditCollector: %s", exc)
        return self._rates_collector

    # ------------------------------------------------------------------
    # Data gathering helpers
    # ------------------------------------------------------------------

    def _gather_rates_data(self) -> Dict[str, Any]:
        """Pull rates/credit data from RatesCreditCollector."""
        rc = self._get_rates_collector()
        if rc is None:
            return {}
        try:
            yc = rc.get_yield_curve_status()
            cc = rc.get_credit_conditions()
            regime = rc.get_rates_regime()
            bei = rc.get_breakeven_inflation()
            days_fed = rc.days_to_next_fed_meeting()
            fed_mult = rc.get_position_size_multiplier()
            return {
                "yield_curve_slope": yc.get("slope"),
                "is_inverted": yc.get("is_inverted", False),
                "inversion_weeks": yc.get("inversion_weeks", 0),
                "yield_momentum_10yr": yc.get("momentum"),
                "yields_rising_fast": yc.get("yields_rising_fast", False),
                "rates_regime": regime,
                "breakeven_inflation": bei,
                "hy_spread": cc.get("hy_spread"),
                "ig_spread": cc.get("ig_spread"),
                "credit_stress_z": cc.get("credit_stress_z"),
                "ted_spread": cc.get("ted_spread"),
                "days_to_fed_meeting": days_fed,
                "fed_position_multiplier": fed_mult,
            }
        except Exception as exc:
            logger.warning("Rates data gather failed: %s", exc)
            return {}

    def _gather_fred_macro(self) -> Dict[str, Any]:
        """
        Pull GDP, CPI, UNRATE from historical_db.db (if previously collected).
        Returns empty dict on failure.
        """
        out = {}
        try:
            conn = sqlite3.connect("output/historical_db.db", timeout=10)
            try:
                def _latest(series_id: str) -> Optional[float]:
                    row = conn.execute(
                        "SELECT value FROM rates_data WHERE series_id=? AND value IS NOT NULL "
                        "ORDER BY obs_date DESC LIMIT 1",
                        (series_id,)
                    ).fetchone()
                    return row[0] if row else None

                def _prev(series_id: str, offset: int = 3) -> Optional[float]:
                    """Get value offset months ago (approximate)."""
                    rows = conn.execute(
                        "SELECT value FROM rates_data WHERE series_id=? AND value IS NOT NULL "
                        "ORDER BY obs_date DESC LIMIT ?",
                        (series_id, offset + 1)
                    ).fetchall()
                    if len(rows) > offset:
                        return rows[offset][0]
                    return None

                unrate_now = _latest("UNRATE")
                unrate_3m = _prev("UNRATE", 3)
                out["unemployment"] = unrate_now
                out["unemployment_3m_change"] = (
                    (unrate_now - unrate_3m) if (unrate_now and unrate_3m) else None
                )
                out["inflation"] = _latest("CPIAUCSL")
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("FRED macro gather failed: %s", exc)
        return out

    def _gather_altdata_macro(self) -> Dict[str, Any]:
        """
        Pull shipping stress, geopolitical, consumer confidence from altdata DB.
        All optional — returns empty dict on failure.
        """
        out: Dict[str, Any] = {}
        try:
            conn = sqlite3.connect("altdata/storage/altdata.db", timeout=10)
            try:
                # Shipping stress
                row = conn.execute(
                    "SELECT value FROM macro_indicators WHERE indicator='ShippingStressIndex' "
                    "ORDER BY date DESC LIMIT 1"
                ).fetchone()
                if row:
                    out["shipping_stress"] = row[0]

                # Geopolitical
                row = conn.execute(
                    "SELECT level, alert_text FROM geopolitical_alerts "
                    "ORDER BY created_at DESC LIMIT 5"
                ).fetchall()
                if row:
                    levels = [r[0] for r in row]
                    texts = [r[1] for r in row if r[1]]
                    out["geopolitical_risk_level"] = levels[0] if levels else "LOW"
                    out["active_alerts"] = texts[:5]
                    out["geopolitical_crisis"] = levels[0] in ("CRITICAL", "HIGH") if levels else False

                # Consumer confidence
                row = conn.execute(
                    "SELECT value FROM macro_indicators WHERE indicator='ConsumerConfidence' "
                    "ORDER BY date DESC LIMIT 2"
                ).fetchall()
                if row:
                    out["consumer_confidence"] = row[0][0]
                    if len(row) > 1 and row[1][0]:
                        out["consumer_confidence_change"] = row[0][0] - row[1][0]

                # Commodities (oil, copper, gold — from price table if present)
                for commodity, col in [("oil", "oil_price"), ("copper", "copper_price"), ("gold", "gold_price")]:
                    row = conn.execute(
                        f"SELECT value FROM commodity_prices WHERE commodity=? ORDER BY date DESC LIMIT 22",
                        (commodity,)
                    ).fetchall()
                    if row:
                        out[col] = row[0][0]
                        if len(row) >= 21:
                            old_price = row[20][0]
                            if old_price and old_price != 0:
                                out[f"{commodity}_1m_change"] = (row[0][0] - old_price) / old_price

            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Altdata gather failed (expected if altdata not yet collected): %s", exc)
        return out

    def _gather_vix(self) -> Optional[float]:
        """Pull latest VIX from historical_db.db or yfinance."""
        try:
            conn = sqlite3.connect("output/historical_db.db", timeout=10)
            try:
                row = conn.execute(
                    "SELECT value FROM macro_data WHERE symbol='vix' ORDER BY date DESC LIMIT 1"
                ).fetchone()
                if row:
                    return float(row[0])
            finally:
                conn.close()
        except Exception:
            pass

        try:
            import yfinance as yf
            ticker = yf.Ticker("^VIX")
            hist = ticker.history(period="5d")
            if not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception as exc:
            logger.debug("VIX fetch failed: %s", exc)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_full_analysis(self) -> MacroState:
        """
        Run all data gathering and analysis, returning a complete MacroState.
        All collectors are called with graceful fallback on failure.
        """
        logger.info("MacroSignalEngine.run_full_analysis() starting")
        state = MacroState()

        # Gather data from all sources
        rates = self._gather_rates_data()
        fred_macro = self._gather_fred_macro()
        altdata = self._gather_altdata_macro()
        vix = self._gather_vix()

        # Populate state fields
        state.yield_curve_slope = rates.get("yield_curve_slope")
        state.is_inverted = rates.get("is_inverted", False)
        state.inversion_weeks = rates.get("inversion_weeks", 0)
        state.yield_momentum_10yr = rates.get("yield_momentum_10yr")
        state.yields_rising_fast = rates.get("yields_rising_fast", False)
        state.rates_regime = rates.get("rates_regime", "NEUTRAL")
        state.breakeven_inflation = rates.get("breakeven_inflation")
        state.hy_spread = rates.get("hy_spread")
        state.ig_spread = rates.get("ig_spread")
        state.credit_stress_z = rates.get("credit_stress_z")
        state.ted_spread = rates.get("ted_spread")
        state.days_to_fed_meeting = rates.get("days_to_fed_meeting", 999)
        state.fed_position_multiplier = rates.get("fed_position_multiplier", 1.0)

        state.unemployment = fred_macro.get("unemployment")
        state.inflation = fred_macro.get("inflation")

        state.shipping_stress = altdata.get("shipping_stress")
        state.geopolitical_risk_level = altdata.get("geopolitical_risk_level", "LOW")
        state.active_alerts = altdata.get("active_alerts", [])
        state.geopolitical_crisis = altdata.get("geopolitical_crisis", False)
        state.consumer_confidence = altdata.get("consumer_confidence")
        state.consumer_confidence_change = altdata.get("consumer_confidence_change")
        state.oil_price = altdata.get("oil_price")
        state.oil_1m_change = altdata.get("oil_1m_change")
        state.copper_price = altdata.get("copper_price")
        state.copper_1m_change = altdata.get("copper_1m_change")
        state.gold_price = altdata.get("gold_price")
        state.gold_1m_change = altdata.get("gold_1m_change")

        state.vix = vix

        # Classify regime
        regime_result = self._regime_classifier.classify(
            gdp_growth=state.gdp_growth,
            inflation=state.inflation,
            unemployment=state.unemployment,
            unemployment_3m_change=fred_macro.get("unemployment_3m_change"),
            vix=state.vix,
            yield_curve_slope=state.yield_curve_slope,
            hy_credit_spread=state.hy_spread,
            consumer_confidence=state.consumer_confidence,
            shipping_stress=state.shipping_stress,
            geopolitical_crisis=state.geopolitical_crisis,
            inversion_weeks=state.inversion_weeks,
        )
        state.regime = regime_result["regime"]
        state.regime_confidence = regime_result["confidence"]
        state.pead_multiplier = regime_result["pead_multiplier"]

        # Combined position size multiplier
        regime_mult = regime_result["position_size_multiplier"]
        fed_mult = state.fed_position_multiplier
        rates_mult = {"CRISIS": 0.2, "TIGHT": 0.6, "NEUTRAL": 1.0, "EASY": 1.1}.get(
            state.rates_regime, 1.0
        )
        state.position_size_multiplier = round(regime_mult * fed_mult * rates_mult, 4)

        # Sector modifiers
        macro_dict = _state_to_dict(state)
        state.sector_modifiers = self._sector_engine.get_all_modifiers(macro_dict)
        state.shipping_sector_impacts = {
            k: v for k, v in state.sector_modifiers.items()
            if state.shipping_stress and state.shipping_stress > 1.5
        }

        self._state = state
        logger.info(
            "MacroSignalEngine analysis complete: regime=%s pead_mult=%.2f pos_size=%.2f",
            state.regime, state.pead_multiplier, state.position_size_multiplier,
        )
        return state

    def get_pead_multiplier(self) -> float:
        """Return current PEAD multiplier (run analysis first if not cached)."""
        if self._state is None:
            self.run_full_analysis()
        return self._state.pead_multiplier if self._state else 1.0

    def get_position_size_multiplier(self) -> float:
        """Return combined position size multiplier."""
        if self._state is None:
            self.run_full_analysis()
        return self._state.position_size_multiplier if self._state else 1.0

    def get_earnings_context(self, ticker: str, sector: str) -> Dict[str, Any]:
        """
        Return EarningsContextScore result for a specific ticker/sector.
        Uses current macro state if available.
        """
        if self._state is None:
            self.run_full_analysis()
        macro_dict = _state_to_dict(self._state) if self._state else {}
        return self._earnings_scorer.calculate(ticker, sector, macro_dict)

    def get_complete_briefing_data(self) -> Dict[str, Any]:
        """
        Return a comprehensive dict with ALL data needed for the daily briefing.
        Runs full analysis if not already cached.
        """
        if self._state is None:
            self.run_full_analysis()
        state = self._state

        if state is None:
            return {"error": "MacroState not available"}

        return {
            # Regime
            "regime": state.regime,
            "regime_confidence": state.regime_confidence,
            "pead_multiplier": state.pead_multiplier,

            # Shipping
            "shipping_stress": state.shipping_stress,
            "shipping_sector_impacts": state.shipping_sector_impacts,

            # Consumer / Housing / Inflation
            "consumer_health": state.consumer_health,
            "housing_health": state.housing_health,
            "inflation_pressure": state.inflation_pressure,
            "consumer_confidence": state.consumer_confidence,
            "consumer_confidence_change": state.consumer_confidence_change,

            # Rates / Yield curve
            "yield_curve_slope": state.yield_curve_slope,
            "is_inverted": state.is_inverted,
            "inversion_weeks": state.inversion_weeks,
            "yield_momentum_10yr": state.yield_momentum_10yr,
            "yields_rising_fast": state.yields_rising_fast,
            "rates_regime": state.rates_regime,
            "breakeven_inflation": state.breakeven_inflation,
            "credit_stress": state.credit_stress_z,
            "hy_spread": state.hy_spread,
            "ig_spread": state.ig_spread,
            "ted_spread": state.ted_spread,

            # Geopolitical
            "geopolitical_risk_level": state.geopolitical_risk_level,
            "active_alerts": state.active_alerts,
            "geopolitical_crisis": state.geopolitical_crisis,

            # Commodities
            "commodity_signals": {
                "oil": {
                    "price": state.oil_price,
                    "1m_change": state.oil_1m_change,
                    "zscore": state.oil_zscore,
                },
                "copper": {
                    "price": state.copper_price,
                    "1m_change": state.copper_1m_change,
                    "zscore": state.copper_zscore,
                },
                "gold": {
                    "price": state.gold_price,
                    "1m_change": state.gold_1m_change,
                    "zscore": state.gold_zscore,
                },
            },

            # Sector modifiers
            "sector_modifiers": state.sector_modifiers,

            # Fed
            "days_to_fed_meeting": state.days_to_fed_meeting,
            "fed_position_multiplier": state.fed_position_multiplier,
            "position_size_multiplier": state.position_size_multiplier,

            # Fundamentals
            "gdp_growth": state.gdp_growth,
            "inflation": state.inflation,
            "unemployment": state.unemployment,
            "vix": state.vix,

            # Upcoming earnings context
            "upcoming_earnings_context": state.upcoming_earnings_context,

            # Meta
            "as_of": state.as_of,
        }


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _clamp(val: float, lo: float, hi: float) -> float:
    """Clamp a float between lo and hi."""
    return max(lo, min(hi, val))


def _state_to_dict(state: Optional[MacroState]) -> Dict[str, Any]:
    """Convert MacroState to a flat dict for use as macro_data in sub-engines."""
    if state is None:
        return {}
    return {
        "gdp_growth": state.gdp_growth,
        "inflation": state.inflation,
        "unemployment": state.unemployment,
        "vix": state.vix,
        "yield_curve_slope": state.yield_curve_slope,
        "is_inverted": state.is_inverted,
        "inversion_weeks": state.inversion_weeks,
        "hy_spread": state.hy_spread,
        "ig_spread": state.ig_spread,
        "credit_stress_z": state.credit_stress_z,
        "ted_spread": state.ted_spread,
        "consumer_confidence": state.consumer_confidence,
        "consumer_confidence_change": state.consumer_confidence_change,
        "shipping_stress": state.shipping_stress,
        "oil_price": state.oil_price,
        "oil_1m_change": state.oil_1m_change,
        "copper_price": state.copper_price,
        "copper_1m_change": state.copper_1m_change,
        "gold_price": state.gold_price,
        "gold_1m_change": state.gold_1m_change,
        "oil_zscore": state.oil_zscore,
        "copper_zscore": state.copper_zscore,
        "gold_zscore": state.gold_zscore,
        "geopolitical_risk_level": state.geopolitical_risk_level,
        "active_alerts": state.active_alerts,
        "geopolitical_crisis": state.geopolitical_crisis,
        "regime": state.regime,
        "rates_regime": state.rates_regime,
        "breakeven_inflation": state.breakeven_inflation,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    os.chdir(Path(__file__).resolve().parents[1])  # cd to repo root

    print("\n=== MacroSignalEngine — Full Analysis Run ===\n")
    engine = MacroSignalEngine(config_path="config/settings.yaml")
    state = engine.run_full_analysis()

    print(f"Regime:               {state.regime} (confidence={state.regime_confidence:.2f})")
    print(f"PEAD multiplier:      {state.pead_multiplier:.2f}")
    print(f"Position size mult:   {state.position_size_multiplier:.4f}")
    print(f"VIX:                  {state.vix}")
    print(f"Yield curve slope:    {state.yield_curve_slope}")
    print(f"Is inverted:          {state.is_inverted}  ({state.inversion_weeks} weeks)")
    print(f"HY spread:            {state.hy_spread}")
    print(f"Credit stress z:      {state.credit_stress_z}")
    print(f"Rates regime:         {state.rates_regime}")
    print(f"Days to Fed meeting:  {state.days_to_fed_meeting}")
    print(f"Geopolitical level:   {state.geopolitical_risk_level}")
    print(f"Shipping stress:      {state.shipping_stress}")

    print("\n--- Sector Modifiers ---")
    for sector, mod in sorted(state.sector_modifiers.items()):
        indicator = "+" if mod > 1.0 else ("-" if mod < 1.0 else " ")
        print(f"  {sector:<30} {indicator}{mod:.4f}")

    print("\n--- Earnings Context Sample ---")
    sample_tickers = [
        ("AAPL", "technology"),
        ("XOM", "energy_producers"),
        ("UAL", "airlines"),
        ("WMT", "retailers"),
    ]
    for ticker, sector in sample_tickers:
        ctx = engine.get_earnings_context(ticker, sector)
        print(
            f"  {ticker:<6} [{sector:<20}]  score={ctx['composite_score']:.3f}  {ctx['label']}"
        )

    print("\n--- Complete Briefing Data Keys ---")
    briefing = engine.get_complete_briefing_data()
    for key in briefing:
        val = briefing[key]
        if isinstance(val, dict):
            print(f"  {key}: {{...{len(val)} keys}}")
        elif isinstance(val, list):
            print(f"  {key}: [{len(val)} items]")
        else:
            print(f"  {key}: {val}")
