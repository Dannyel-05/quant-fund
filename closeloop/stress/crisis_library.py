"""
Crisis Library — historical stress scenarios with conditions vectors and signal performance data.
Each scenario contains everything needed for both stress testing and scenario relevance matching.

PHILOSOPHY: These scenarios are diagnostic reference data, not predictions.
They capture what happened, what signals did, and what the lessons were.
"""

CRISIS_SCENARIOS = {
    "great_depression_1929": {
        "start": "1929-10-24",
        "trough": "1932-07-08",
        "end": "1937-01-01",
        "peak_drawdown": -0.89,
        "uk_premium": -0.15,           # UK fared slightly better than US
        "small_cap_premium": -0.20,    # small caps worse than large
        "sector_premiums": {
            "Financial Services": -0.95,
            "Industrials": -0.85,
            "Consumer Discretionary": -0.90,
        },
        "conditions_vector": {
            "vix_proxy": 80,
            "yield_curve": -0.5,
            "credit_spread": 5.0,
            "momentum": -0.8,
        },
        "signal_performance": {
            "momentum": -0.9,
            "mean_reversion": +0.2,
            "quality": +0.1,
        },
        "description": (
            "Wall Street Crash + Great Depression. 89% peak-to-trough. "
            "25% unemployment. Bank failures cascaded through the economy."
        ),
        "lessons": [
            "High leverage is fatal when forced liquidations cascade",
            "Momentum signals fail catastrophically in prolonged bear markets",
            "Quality defensive but not immune to 89% drawdowns",
        ],
        "duration_days": 2625,
        "recovery_days": 1900,
    },

    "black_monday_1987": {
        "start": "1987-10-14",
        "trough": "1987-10-19",
        "end": "1988-07-01",
        "peak_drawdown": -0.34,
        "uk_premium": -0.05,
        "small_cap_premium": -0.10,
        "sector_premiums": {
            "Financial Services": -0.35,
            "Technology": -0.40,
        },
        "conditions_vector": {
            "vix_proxy": 60,
            "yield_curve": 0.5,
            "credit_spread": 1.5,
            "momentum": -0.5,
        },
        "signal_performance": {
            "momentum": -0.6,
            "mean_reversion": +0.4,
            "pead": -0.3,
        },
        "description": (
            "Single-day 22% crash on 19 Oct 1987. Portfolio insurance "
            "feedback loop. Fast recovery — back near highs within a year."
        ),
        "lessons": [
            "Single-day crashes are survivable with diversification and no leverage",
            "Mean reversion strongest signal post-crash",
            "Portfolio insurance products amplified, not reduced, crash severity",
        ],
        "duration_days": 5,
        "recovery_days": 255,
    },

    "dot_com_crash_2000": {
        "start": "2000-03-10",
        "trough": "2002-10-09",
        "end": "2007-10-01",
        "peak_drawdown": -0.49,
        "uk_premium": +0.05,             # UK less exposed to NASDAQ tech
        "small_cap_premium": +0.10,      # small caps outperformed (less tech weight)
        "sector_premiums": {
            "Technology": -0.78,
            "Telecommunications": -0.65,
            "Consumer Staples": +0.05,
        },
        "conditions_vector": {
            "vix_proxy": 45,
            "yield_curve": 0.8,
            "credit_spread": 2.0,
            "momentum": -0.3,
        },
        "signal_performance": {
            "momentum": -0.4,
            "value": +0.3,
            "pead": -0.1,
            "short_squeeze": +0.2,
        },
        "description": (
            "Tech bubble burst. NASDAQ -78%. Small-cap and value outperformed. "
            "Slow 2.5-year decline — not a crash but a grind."
        ),
        "lessons": [
            "Sector concentration in tech is lethal",
            "Value and small-cap outperformed throughout the drawdown",
            "PEAD still worked in non-tech sectors",
            "Slow grinds are harder to hedge than sharp crashes",
        ],
        "duration_days": 943,
        "recovery_days": 1818,
    },

    "ltcm_1998": {
        "start": "1998-08-17",
        "trough": "1998-10-09",
        "end": "1999-03-01",
        "peak_drawdown": -0.20,
        "uk_premium": -0.05,
        "small_cap_premium": -0.15,
        "sector_premiums": {
            "Financial Services": -0.35,
        },
        "conditions_vector": {
            "vix_proxy": 50,
            "yield_curve": 1.0,
            "credit_spread": 3.5,
            "momentum": -0.4,
        },
        "signal_performance": {
            "momentum": -0.5,
            "short_squeeze": -0.8,
            "pead": -0.2,
        },
        "description": (
            "LTCM collapse following Russian default. Liquidity crisis. "
            "All correlations spiked to 1. Small-cap worst hit by forced redemptions."
        ),
        "lessons": [
            "Correlation assumptions break completely in liquidity crises",
            "Short squeeze signals are extremely dangerous when forced sellers dominate",
            "Small-cap liquidity dries up first in crises",
        ],
        "duration_days": 53,
        "recovery_days": 144,
    },

    "nine_eleven_2001": {
        "start": "2001-09-11",
        "trough": "2001-09-21",
        "end": "2001-11-01",
        "peak_drawdown": -0.12,
        "uk_premium": -0.03,
        "small_cap_premium": -0.05,
        "sector_premiums": {
            "Airlines": -0.40,
            "Defense": +0.20,
            "Insurance": -0.25,
        },
        "conditions_vector": {
            "vix_proxy": 45,
            "yield_curve": 2.0,
            "credit_spread": 2.0,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.4,
            "defensive": +0.2,
        },
        "description": (
            "Geopolitical shock. US markets closed 4 days. Fast partial recovery. "
            "Sector effects predictable and sharp."
        ),
        "lessons": [
            "Geopolitical shocks are sharp but fast — overreaction is common",
            "Diversification across sectors critical — airlines vs defense diverged 60%",
            "Defensive signals outperform in short geopolitical events",
        ],
        "duration_days": 10,
        "recovery_days": 51,
    },

    "global_financial_crisis_2008": {
        "start": "2007-10-09",
        "trough": "2009-03-09",
        "end": "2013-03-01",
        "peak_drawdown": -0.57,
        "uk_premium": -0.05,
        "small_cap_premium": -0.15,
        "sector_premiums": {
            "Financial Services": -0.80,
            "Real Estate": -0.70,
            "Consumer Discretionary": -0.55,
            "Technology": -0.40,
            "Consumer Staples": -0.15,
        },
        "conditions_vector": {
            "vix_proxy": 80,
            "yield_curve": -0.3,
            "credit_spread": 5.5,
            "momentum": -0.7,
        },
        "signal_performance": {
            "momentum": -0.6,
            "pead": -0.4,
            "short_squeeze": -0.9,
            "quality": +0.1,
            "value": -0.3,
        },
        "description": (
            "Global Financial Crisis. Lehman Brothers collapse Sep 2008. "
            "Credit markets frozen. 57% peak-to-trough over 17 months."
        ),
        "lessons": [
            "Short squeeze signals catastrophically fail — forced sellers overwhelm buyers",
            "Quality is defensive but not enough to avoid significant losses",
            "Momentum reverses violently when credit conditions tighten suddenly",
            "Financial sector leverage amplifies everything",
        ],
        "duration_days": 517,
        "recovery_days": 1482,
    },

    "flash_crash_2010": {
        "start": "2010-05-06",
        "trough": "2010-05-06",
        "end": "2010-05-06",
        "peak_drawdown": -0.09,
        "uk_premium": -0.02,
        "small_cap_premium": -0.05,
        "sector_premiums": {},
        "conditions_vector": {
            "vix_proxy": 40,
            "yield_curve": 2.5,
            "credit_spread": 1.5,
            "momentum": -0.2,
        },
        "signal_performance": {
            "pead": 0.0,
            "momentum": -0.3,
        },
        "description": (
            "Algorithmic trading flash crash. 9% intraday drop, full recovery same day. "
            "Triggered by large E-mini futures sell order and HFT feedback loops."
        ),
        "lessons": [
            "Stop-losses get triggered at worst prices in flash crashes",
            "Intraday signals unreliable during mechanically-driven moves",
            "Recovery within hours — patient holders unaffected",
        ],
        "duration_days": 1,
        "recovery_days": 1,
    },

    "quant_quake_2007": {
        "start": "2007-08-07",
        "trough": "2007-08-09",
        "end": "2007-08-15",
        "peak_drawdown": -0.10,
        "uk_premium": -0.02,
        "small_cap_premium": -0.08,
        "sector_premiums": {},
        "conditions_vector": {
            "vix_proxy": 30,
            "yield_curve": 1.0,
            "credit_spread": 2.0,
            "momentum": -0.6,
        },
        "signal_performance": {
            "momentum": -0.9,
            "mean_reversion": +0.5,
            "pead": -0.2,
        },
        "description": (
            "Quant deleveraging Aug 2007. Factor crowding caused simultaneous "
            "strategy failure across all quant funds. Market looked fine to non-quants."
        ),
        "lessons": [
            "Momentum factor most vulnerable to crowding unwinds",
            "Signal correlation spikes dramatically during quant deleveraging events",
            "Quant events are invisible to fundamental analysis — regime-based caps matter",
        ],
        "duration_days": 8,
        "recovery_days": 6,
    },

    "quant_unwind_2025": {
        "start": "2025-01-15",
        "trough": "2025-02-01",
        "end": "2025-03-01",
        "peak_drawdown": -0.08,
        "uk_premium": -0.01,
        "small_cap_premium": -0.06,
        "sector_premiums": {
            "Technology": -0.12,
        },
        "conditions_vector": {
            "vix_proxy": 25,
            "yield_curve": 0.5,
            "credit_spread": 1.8,
            "momentum": -0.5,
        },
        "signal_performance": {
            "momentum": -0.7,
            "pead": -0.1,
        },
        "description": (
            "AI-driven quant factor crowding unwind Jan-Feb 2025. "
            "Momentum worst hit. PEAD more resilient due to earnings-anchor."
        ),
        "lessons": [
            "AI-generated factor signals increasingly correlated — crowding risk rising",
            "PEAD more resilient than pure momentum due to fundamental earnings anchor",
            "Concentration in AI/momentum factors creates systemic fragility",
        ],
        "duration_days": 17,
        "recovery_days": 28,
    },

    "black_wednesday_1992": {
        "start": "1992-09-16",
        "trough": "1992-09-16",
        "end": "1993-06-01",
        "peak_drawdown": -0.08,
        "uk_premium": -0.15,       # UK-specific event, much worse
        "small_cap_premium": -0.10,
        "sector_premiums": {
            "Financial Services": -0.20,
            "Real Estate": -0.15,
        },
        "conditions_vector": {
            "vix_proxy": 35,
            "yield_curve": 0.5,
            "credit_spread": 2.0,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.3,
        },
        "description": (
            "UK forced out of ERM on Black Wednesday. GBP devaluation. "
            "UK-specific shock. FTSE companies with dollar revenues actually benefited."
        ),
        "lessons": [
            "Currency crises are UK-specific — US indices unaffected",
            "FTSE exporters with dollar revenues benefit from GBP devaluation",
            "BoE interest rate hikes to defend peg create domestic demand shock",
        ],
        "duration_days": 1,
        "recovery_days": 258,
    },

    "uk_brexit_vote_2016": {
        "start": "2016-06-24",
        "trough": "2016-06-27",
        "end": "2016-07-15",
        "peak_drawdown": -0.12,
        "uk_premium": -0.20,       # UK much worse than global
        "small_cap_premium": -0.15,
        "sector_premiums": {
            "Financial Services": -0.25,
            "Real Estate": -0.20,
            "Consumer Discretionary": -0.15,
        },
        "conditions_vector": {
            "vix_proxy": 26,
            "yield_curve": 1.5,
            "credit_spread": 1.5,
            "momentum": -0.4,
        },
        "signal_performance": {
            "pead": -0.2,
            "momentum": -0.4,
            "value": +0.1,
        },
        "description": (
            "Brexit referendum shock. UK assets sold off sharply. GBP -10% overnight. "
            "Fast partial recovery — FTSE 100 recovered within weeks (USD revenue effect)."
        ),
        "lessons": [
            "Political shocks hit UK domestic small-caps hardest",
            "Domestic UK revenue exposure amplifies downside in GBP crises",
            "FTSE 100 large-caps partially insulated by foreign earnings",
        ],
        "duration_days": 3,
        "recovery_days": 21,
    },

    "uk_mini_budget_2022": {
        "start": "2022-09-23",
        "trough": "2022-10-14",
        "end": "2022-11-01",
        "peak_drawdown": -0.08,
        "uk_premium": -0.12,
        "small_cap_premium": -0.10,
        "sector_premiums": {
            "Financial Services": -0.12,
            "Real Estate": -0.20,
        },
        "conditions_vector": {
            "vix_proxy": 35,
            "yield_curve": -0.5,
            "credit_spread": 2.5,
            "momentum": -0.4,
        },
        "signal_performance": {
            "pead": -0.15,
            "momentum": -0.35,
        },
        "description": (
            "UK Mini-Budget Gilt crisis. Kwarteng's unfunded tax cuts caused Gilt yields "
            "to spike. BoE emergency intervention. UK pension fund LDI strategies at risk. "
            "UK-specific event."
        ),
        "lessons": [
            "Fiscal policy surprises create UK-specific dislocations",
            "Gilt yields are a leading indicator for UK equity stress",
            "Pension fund LDI unwinds created secondary liquidity shock",
        ],
        "duration_days": 21,
        "recovery_days": 49,
    },

    "covid_crash_2020": {
        "start": "2020-02-19",
        "trough": "2020-03-23",
        "end": "2020-08-18",
        "peak_drawdown": -0.34,
        "uk_premium": -0.05,
        "small_cap_premium": -0.10,
        "sector_premiums": {
            "Airlines": -0.70,
            "Hotels": -0.60,
            "Energy": -0.50,
            "Technology": +0.30,
            "Healthcare": +0.10,
        },
        "conditions_vector": {
            "vix_proxy": 82,
            "yield_curve": 1.0,
            "credit_spread": 4.0,
            "momentum": -0.6,
        },
        "signal_performance": {
            "pead": -0.3,
            "momentum": -0.5,
            "short_squeeze": -0.8,
            "quality": +0.2,
        },
        "description": (
            "COVID-19 crash. Fastest 30%+ decline in history (33 calendar days). "
            "V-shaped recovery driven by unprecedented fiscal and monetary stimulus."
        ),
        "lessons": [
            "Sector dispersion extreme — airlines -70%, tech +30%",
            "Quality signals outperformed in fastest crash on record",
            "Short squeeze signals catastrophically dangerous in circuit-breaker conditions",
            "V-shaped recoveries reward patience over panic selling",
        ],
        "duration_days": 33,
        "recovery_days": 148,
    },

    "rate_shock_2022": {
        "start": "2022-01-03",
        "trough": "2022-10-12",
        "end": "2024-01-01",
        "peak_drawdown": -0.25,
        "uk_premium": -0.02,
        "small_cap_premium": -0.15,
        "sector_premiums": {
            "Technology": -0.35,
            "Real Estate": -0.30,
            "Utilities": -0.25,
            "Energy": +0.40,
            "Financials": +0.10,
        },
        "conditions_vector": {
            "vix_proxy": 35,
            "yield_curve": -1.5,
            "credit_spread": 2.0,
            "momentum": -0.4,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.4,
            "value": +0.2,
            "short_squeeze": -0.3,
        },
        "description": (
            "Fed rate shock. 40-year high inflation. 525bps of hikes. "
            "Growth stocks -35%. Energy outperformed. Yield curve inverted deeply."
        ),
        "lessons": [
            "Duration risk in growth/tech stocks — high P/E multiples compress fast",
            "Value and energy outperform in inflationary bear markets",
            "Small-cap worse than large-cap — credit-dependent business models vulnerable",
        ],
        "duration_days": 283,
        "recovery_days": 460,
    },

    "svb_banking_crisis_2023": {
        "start": "2023-03-08",
        "trough": "2023-03-14",
        "end": "2023-05-01",
        "peak_drawdown": -0.07,
        "uk_premium": -0.02,
        "small_cap_premium": -0.05,
        "sector_premiums": {
            "Financial Services": -0.25,
            "Real Estate": -0.10,
        },
        "conditions_vector": {
            "vix_proxy": 28,
            "yield_curve": -1.8,
            "credit_spread": 2.5,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.25,
            "short_squeeze": -0.4,
        },
        "description": (
            "SVB and Signature Bank failures. Regional banking contagion fears. "
            "FDIC intervention contained broader panic. CS/UBS merger forced."
        ),
        "lessons": [
            "Regional bank failures create sector-specific contagion",
            "Flight to quality brief but sharp — Treasuries rallied hard",
            "Contagion contained faster than GFC because leverage was lower system-wide",
        ],
        "duration_days": 6,
        "recovery_days": 48,
    },

    "gulf_war_1990": {
        "start": "1990-07-16",
        "trough": "1990-10-11",
        "end": "1991-02-01",
        "peak_drawdown": -0.20,
        "uk_premium": -0.03,
        "small_cap_premium": -0.08,
        "sector_premiums": {
            "Energy": +0.20,
            "Airlines": -0.35,
            "Consumer Discretionary": -0.20,
        },
        "conditions_vector": {
            "vix_proxy": 35,
            "yield_curve": 0.5,
            "credit_spread": 2.0,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.3,
        },
        "description": (
            "Gulf War I. Iraq invades Kuwait. Oil spike. US recession. "
            "Energy outperformed. Airlines crushed. Recovery on war resolution."
        ),
        "lessons": [
            "Oil shocks create predictable sector winners and losers",
            "Geopolitical events have consistent sector rotation effects",
            "Energy sector outperforms in supply-shock crises",
        ],
        "duration_days": 87,
        "recovery_days": 113,
    },

    "ukraine_invasion_2022": {
        "start": "2022-02-24",
        "trough": "2022-03-08",
        "end": "2022-04-01",
        "peak_drawdown": -0.10,
        "uk_premium": -0.02,
        "small_cap_premium": -0.05,
        "sector_premiums": {
            "Energy": +0.30,
            "Defense": +0.20,
            "Consumer Discretionary": -0.15,
        },
        "conditions_vector": {
            "vix_proxy": 38,
            "yield_curve": 0.5,
            "credit_spread": 2.0,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.1,
            "momentum": -0.25,
            "value": +0.1,
        },
        "description": (
            "Russia invades Ukraine 24 Feb 2022. Commodity/energy shock. "
            "Defense stocks re-rated. European energy crisis fears. "
            "Overlapped with 2022 rate shock — compounded volatility."
        ),
        "lessons": [
            "Commodity shocks are tradeable with sector knowledge — energy +30%",
            "Defense sector re-rating creates multi-month tailwind, not just a spike",
            "Geopolitical events in commodity exporters have outsized energy effects",
        ],
        "duration_days": 12,
        "recovery_days": 38,
    },

    "enron_worldcom_2001": {
        "start": "2001-10-16",
        "trough": "2002-07-19",
        "end": "2003-01-01",
        "peak_drawdown": -0.15,
        "uk_premium": 0.0,
        "small_cap_premium": -0.05,
        "sector_premiums": {
            "Telecommunications": -0.50,
            "Energy": -0.30,
        },
        "conditions_vector": {
            "vix_proxy": 40,
            "yield_curve": 1.0,
            "credit_spread": 2.5,
            "momentum": -0.3,
        },
        "signal_performance": {
            "pead": -0.2,
            "quality": +0.2,
        },
        "description": (
            "Enron (Oct 2001) and WorldCom (Jul 2002) accounting fraud scandals. "
            "Both companies had elevated Sloan accruals ratios and deflection scores "
            "in earnings calls before collapse."
        ),
        "lessons": [
            "Sloan accruals ratio predictive of accounting fraud pre-collapse",
            "Transcript deflection scores elevated in management calls before failure",
            "Quality signals correctly flagged both before market recognition",
        ],
        "duration_days": 276,
        "recovery_days": 165,
    },

    "tech_regulation_fears_2018": {
        "start": "2018-10-01",
        "trough": "2018-12-24",
        "end": "2019-04-01",
        "peak_drawdown": -0.20,
        "uk_premium": +0.03,
        "small_cap_premium": +0.05,   # small-cap outperformed large in this episode
        "sector_premiums": {
            "Technology": -0.25,
            "Consumer Discretionary": -0.20,
        },
        "conditions_vector": {
            "vix_proxy": 36,
            "yield_curve": 0.5,
            "credit_spread": 1.5,
            "momentum": -0.4,
        },
        "signal_performance": {
            "pead": -0.15,
            "momentum": -0.4,
            "value": +0.1,
        },
        "description": (
            "Trade war fears + tech regulation + Fed hiking = Q4 2018 sell-off. "
            "S&P -20%. Small-cap outperformed large-cap. UK less affected."
        ),
        "lessons": [
            "Tech sector vulnerable to regulation signals — watch political cycle",
            "Small-cap outperformed large in this correction — not all crises uniform",
            "Multiple simultaneous headwinds (trade + rates + regulation) compound",
        ],
        "duration_days": 84,
        "recovery_days": 98,
    },

    "crypto_winter_2022": {
        "start": "2022-11-08",
        "trough": "2022-11-22",
        "end": "2023-01-01",
        "peak_drawdown": -0.12,
        "uk_premium": -0.01,
        "small_cap_premium": -0.03,
        "sector_premiums": {
            "Financial Services": -0.15,
            "Technology": -0.10,
        },
        "conditions_vector": {
            "vix_proxy": 25,
            "yield_curve": -2.0,
            "credit_spread": 2.0,
            "momentum": -0.2,
        },
        "signal_performance": {
            "pead": -0.05,
            "momentum": -0.2,
        },
        "description": (
            "FTX collapse Nov 2022. Crypto contagion to fintech and exchange-adjacent "
            "stocks. Contained for traditional equities. Coincided with tail of rate shock."
        ),
        "lessons": [
            "Crypto-correlated stocks and fintechs highly vulnerable to exchange failures",
            "Contagion to traditional equities contained when macro backdrop is stable",
            "Crypto winter on its own insufficient to trigger broader equity bear market",
        ],
        "duration_days": 14,
        "recovery_days": 40,
    },
}


def get_scenario(name: str) -> dict:
    """Return a single scenario by name. Returns empty dict if not found."""
    return CRISIS_SCENARIOS.get(name, {})


def get_all_scenarios() -> dict:
    """Return the full CRISIS_SCENARIOS dict."""
    return CRISIS_SCENARIOS


def get_conditions_vector(scenario_name: str) -> dict:
    """Return the conditions_vector for a named scenario. Empty dict if not found."""
    return CRISIS_SCENARIOS.get(scenario_name, {}).get("conditions_vector", {})


def get_scenarios_by_severity(min_drawdown: float = -0.20) -> dict:
    """
    Return scenarios with peak_drawdown <= min_drawdown (i.e. more severe than threshold).
    min_drawdown should be negative, e.g. -0.20 = 20% drawdown threshold.
    """
    return {
        name: s for name, s in CRISIS_SCENARIOS.items()
        if s.get("peak_drawdown", 0) <= min_drawdown
    }


def get_uk_relevant_scenarios() -> dict:
    """Return scenarios where uk_premium <= -0.08 (UK-specific shock or severe UK impact)."""
    return {
        name: s for name, s in CRISIS_SCENARIOS.items()
        if s.get("uk_premium", 0) <= -0.08
    }


def get_scenario_signal_performance(scenario_name: str, signal_name: str) -> float:
    """
    Return historical signal performance in a scenario.
    Returns 0.0 if scenario or signal not found.
    Positive = signal outperformed. Negative = signal underperformed/failed.
    """
    scenario = CRISIS_SCENARIOS.get(scenario_name, {})
    return scenario.get("signal_performance", {}).get(signal_name, 0.0)


def list_scenario_names() -> list:
    """Return sorted list of all scenario names."""
    return sorted(CRISIS_SCENARIOS.keys())


class CrisisLibrary:
    """Class interface to the CRISIS_SCENARIOS data and helper functions."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.scenarios = CRISIS_SCENARIOS

    def get_scenario(self, name: str) -> dict:
        return get_scenario(name)

    def get_all_scenarios(self) -> dict:
        return get_all_scenarios()

    def list_scenario_names(self) -> list:
        return list_scenario_names()

    def get_scenarios_by_severity(self, min_drawdown: float = -0.20) -> dict:
        return get_scenarios_by_severity(min_drawdown)

    def get_uk_relevant_scenarios(self) -> dict:
        return get_uk_relevant_scenarios()

    def get_signal_performance(self, scenario_name: str, signal_name: str) -> float:
        return get_scenario_signal_performance(scenario_name, signal_name)
