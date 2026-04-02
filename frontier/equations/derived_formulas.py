"""
Frontier Intelligence — Novel Derived Formulas.

Each formula is an original mathematical construction for this system.
Docstrings explain BOTH the financial logic and the scientific mechanism,
not just the code.

References cited where published research underpins the formula.
Formulas marked [NOVEL] have no prior published financial application.
"""
import math
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. GRAI — GeomagnEtic Risk Aversion Index
# ---------------------------------------------------------------------------

def calc_grai(
    kp_readings: Dict[str, float],
    geographic_weights: Dict[str, float],
    session_overlaps: Dict[str, float],
    hours_since_storm_peak: float,
    current_vix: float,
    historical_vix_mean: float,
    decay_lambda: float = 0.15,
    volatility_amplifier: float = 0.3,
) -> float:
    """
    GeomagnEtic Risk Aversion Index (GRAI).

    Scientific basis
    ----------------
    Geomagnetic storms perturb the magnetosphere, which in turn reduces
    melatonin synthesis and alters serotonin levels in the brain
    (Burch et al. 1998). Lower serotonin is associated with elevated
    risk-aversion and depressed mood (Carver & Miller 2006).

    Financial basis
    ---------------
    Krivelyova & Robotti (2003) found that high geomagnetic activity
    precedes below-average stock returns by 1-4 days.  Kamstra, Kramer &
    Levi (2003) documented SAD-driven risk aversion effects.  The mechanism
    is behavioural: investors exposed to geomagnetic storms become
    systematically more risk-averse, suppressing demand for equities.

    Formula
    -------
    GRAI(t) = Σ_l [ Kp(t,l) × geo_weight(l) × session_overlap(t,l) ]
              × exp(−λ × hours_since_storm_peak)
              × (1 + α × VIX_current / VIX_mean)

    Where:
      Kp(t,l)             : NOAA planetary K-index at location l, 0–9 scale
      geo_weight(l)       : higher for locations closer to geomagnetic poles
                            (effect is stronger at high magnetic latitudes)
      session_overlap(t,l): 1.0 during trading hours, 0.3 overnight
                            (only trading investors matter)
      λ = 0.15            : exponential decay rate (Krivelyova & Robotti 2003
                            suggest ~6h half-life: ln(2)/0.15 ≈ 4.6h)
      α = 0.3             : amplifier — geomagnetic effect larger when
                            markets are already volatile (fear amplifies fear)

    Thresholds
    ----------
    GRAI < 1.0 : normal conditions, no adjustment
    GRAI > 3.0 : elevated risk aversion expected
                 → reduce new long exposure by GRAI / 10
    GRAI > 6.0 : significant geomagnetic storm
                 → reduce all new position sizes by 40%

    Parameter drifting
    ------------------
    decay_lambda and volatility_amplifier should be drifted by
    ParameterDrifter after ≥100 observations in your specific universe.
    Published values are the anchors; max drift ±30%.
    """
    if not kp_readings:
        return 0.0

    try:
        # Weighted sum across locations
        raw_sum = sum(
            kp_readings.get(loc, 0.0)
            * geographic_weights.get(loc, 1.0)
            * session_overlaps.get(loc, 0.5)
            for loc in geographic_weights
        )

        # Temporal decay: storm effect fades exponentially
        time_decay = math.exp(-decay_lambda * max(hours_since_storm_peak, 0.0))

        # Volatility amplifier: effect stronger in already-stressed markets
        vix_ratio = current_vix / max(historical_vix_mean, 1.0)
        vol_amp = 1.0 + volatility_amplifier * vix_ratio

        grai = raw_sum * time_decay * vol_amp
        return max(0.0, grai)
    except Exception as e:
        logger.warning(f"GRAI calculation failed: {e}")
        return 0.0


def grai_position_multiplier(grai: float) -> float:
    """
    Convert GRAI to a position-size multiplier.
    Returns 1.0 (no adjustment) down to 0.6 (40% reduction).
    """
    if grai < 1.0:
        return 1.0
    elif grai < 3.0:
        return 1.0 - (grai - 1.0) / 20.0   # linear from 1.0→0.9
    elif grai < 6.0:
        return 1.0 - grai / 10.0            # GRAI/10 reduction
    else:
        return 0.6                           # hard cap: 40% reduction


# ---------------------------------------------------------------------------
# 2. ASI — Attention Saturation Index   [NOVEL]
# ---------------------------------------------------------------------------

def calc_asi(topic_counts: Dict[str, int]) -> float:
    """
    Attention Saturation Index (ASI).  [NOVEL — no prior published version]

    Financial basis
    ---------------
    Investor attention is finite (Kahneman 1973, Hirshleifer & Teoh 2003).
    When attention is monopolised by one topic (e.g. a macro shock), other
    stocks are systematically under-attended, leading to delayed price
    discovery and subsequent mean-reversion once attention normalises.

    Formula
    -------
    ASI(t) = H(news_distribution(t)) / H_max

    Where:
      H(p) = −Σ p_i · log₂(p_i)   [Shannon entropy of topic distribution]
      H_max = log₂(n_topics)       [maximum entropy = uniform distribution]

    ASI near 0 : attention fully saturated on one topic
                 → maximum mispricings in un-covered stocks
    ASI near 1 : attention evenly distributed → normal market

    Parameters
    ----------
    topic_counts : {topic_label: article_count}
    """
    if not topic_counts:
        return 1.0  # no data → assume normal

    total = sum(topic_counts.values())
    if total == 0:
        return 1.0

    n = len(topic_counts)
    if n == 1:
        return 0.0  # one topic = fully saturated

    h_max = math.log2(n)
    if h_max == 0:
        return 1.0

    entropy = -sum(
        (c / total) * math.log2(c / total)
        for c in topic_counts.values()
        if c > 0
    )
    return min(1.0, max(0.0, entropy / h_max))


def calc_attention_mispricing_score(
    asi: float,
    stock_news_share: float,
    mean_reversion_strength: float,
) -> float:
    """
    AttentionMispricingScore (AMS).  [NOVEL]

    Measures how mis-priced a stock is *because* attention is elsewhere.

    Formula
    -------
    AMS(stock, t) = (1 − ASI(t)) × (1 − stock_news_share(t))
                   × mean_reversion_strength(stock)

    High AMS = stock being ignored during news saturation
             = likely to revert when attention normalises

    Parameters
    ----------
    asi                   : Attention Saturation Index (0=saturated, 1=normal)
    stock_news_share      : fraction of news this stock captures (0–1)
    mean_reversion_strength: historical half-life-based reversion score (0–1)
    """
    return (1.0 - asi) * (1.0 - stock_news_share) * mean_reversion_strength


def calc_revert_window_days(asi: float) -> float:
    """
    Predicted reversion window in trading days.

    More saturated attention → market takes longer to normalise → hold longer.

    RevertWindowDays = 5 + 15 × (1 − ASI)
    Range: 5 days (normal market) to 20 days (fully saturated).
    """
    return 5.0 + 15.0 * (1.0 - asi)


# ---------------------------------------------------------------------------
# 3. SCV — Social Contagion Velocity   [NOVEL]
# ---------------------------------------------------------------------------

def calc_scv(
    susceptible: float,
    infected: float,
    recovered: float,
    beta: float,
    gamma: float,
) -> Tuple[float, float]:
    """
    Social Contagion Velocity (SCV) — SIR epidemic model on investor attention.
    [NOVEL application to finance; SIR model from Kermack & McKendrick 1927]

    Financial basis
    ---------------
    Information spreads through social networks the same way diseases spread
    through populations.  The SIR model captures: who hasn't heard yet (S),
    who is actively discussing/trading (I), and who has already acted and
    moved on (R).  The rate of spread (SCV) predicts price momentum and its
    eventual exhaustion.

    SIR equations
    -------------
    dS/dt = −β·S·I / N
    dI/dt =  β·S·I / N − γ·I
    dR/dt =  γ·I

    SCV = dI/dt / max(I_total, 1)   [spread rate relative to current infected]

    R₀ = β / γ   [basic reproduction number]
      R₀ > 1 : contagion will spread (viral stock)
      R₀ < 1 : contagion will die out naturally

    Returns
    -------
    (scv, r0) : velocity and reproduction number
    """
    n = max(susceptible + infected + recovered, 1.0)
    di_dt = beta * susceptible * infected / n - gamma * infected
    scv = di_dt / max(infected, 1.0)
    r0 = beta / max(gamma, 1e-9)
    return float(scv), float(r0)


def scv_position_size_multiplier(r0: float) -> float:
    """
    Position sizing from R₀.

    Enter when SCV first turns positive AND R₀ > 1.
    Size = base_size × min(R₀, 2.0) / 2.0   (cap R₀ contribution at 2×)
    """
    return min(r0, 2.0) / 2.0


# ---------------------------------------------------------------------------
# 4. ODP — Obituary Drift Predictor   [NOVEL]
# ---------------------------------------------------------------------------

ROLE_WEIGHTS = {
    "founder": 1.0,
    "cto": 0.9,
    "chief_scientist": 0.9,
    "ceo": 0.85,
    "cfo": 0.6,
    "coo": 0.5,
    "cso": 0.5,
    "other_csuite": 0.4,
    "director": 0.2,
}


def calc_knowledge_loss_score(
    role: str,
    tenure_years: float,
    succession_signal: float,
    analyst_coverage: int,
) -> float:
    """
    KnowledgeLossScore — quantifies human-capital loss to a company.  [NOVEL]

    Financial basis
    ---------------
    Post-departure drift is analogous to PEAD: the market under-reacts to
    information embedded in a key person's tacit knowledge.  Small-caps are
    more affected because they are less covered and more dependent on
    individuals (Fama & French size effect analogue for human capital).

    Formula
    -------
    KLS(company, person) =
      role_weight(person)
      × tenure_years^0.5        [diminishing returns: sqrt scaling]
      × (1 − succession_signal) [0 if clear successor, 1 if vacuum]
      × (1 / analyst_coverage)  [less coverage → slower price discovery]

    Parameters
    ----------
    role              : lowercase role string (see ROLE_WEIGHTS)
    tenure_years      : years in role at company
    succession_signal : 0.0 = no successor named, 1.0 = clear internal successor
    analyst_coverage  : number of analysts covering the stock (min 1)
    """
    weight = ROLE_WEIGHTS.get(role.lower(), ROLE_WEIGHTS["other_csuite"])
    kls = (
        weight
        * math.sqrt(max(tenure_years, 0.0))
        * (1.0 - min(max(succession_signal, 0.0), 1.0))
        * (1.0 / max(analyst_coverage, 1))
    )
    return float(kls)


def calc_expected_drift(kls: float, market_cap_millions: float) -> float:
    """
    ExpectedDriftMagnitude for obituary / departure signal.

    Formula
    -------
    ExpectedDrift = −0.03 × KLS × (1 / √market_cap_millions)

    Negative because departure creates downward drift.
    Small-cap effect: company more dependent on individuals
    (analogous to size effect in Fama-French).
    """
    if market_cap_millions <= 0:
        return 0.0
    return -0.03 * kls * (1.0 / math.sqrt(market_cap_millions))


def calc_expected_drift_duration(kls: float) -> float:
    """
    ExpectedDriftDuration in trading days.

    Formula: 10 + 20 × KLS
    Range: 10 days (minor departure) to 30 days (founder death, no successor).
    Higher KLS → market takes longer to fully price the loss.
    """
    return 10.0 + 20.0 * kls


# ---------------------------------------------------------------------------
# 5. DLI — Divorce Lead Indicator   [NOVEL]
# ---------------------------------------------------------------------------

SECTOR_SENSITIVITIES = {
    "home_furnishings":    0.8,
    "consumer_electronics": 0.6,
    "legal_services":      1.0,
    "financial_advisory":  0.7,
    "rental_housing":      0.5,
    "grocery_retail":      0.3,
    "luxury_goods":       -0.4,
}


def calc_dli(
    divorce_rate: float,
    baseline_mean: float,
    baseline_std: float,
) -> float:
    """
    DivorceLeadIndicator (DLI).  [NOVEL]

    Financial basis
    ---------------
    Divorce rates are a leading indicator for specific consumer expenditure
    categories.  A household dissolving into two creates predictable spending:
    two sets of furniture, two rental properties, legal fees, financial advice.
    The effect leads actual consumer spending by 3–18 months depending on the
    sector (longer for housing, shorter for legal).

    Formula
    -------
    DLI(region, t) = (divorce_rate(region, t) − baseline_mean) / baseline_std

    Returns the standardised deviation from regional baseline.
    """
    if baseline_std == 0:
        return 0.0
    return (divorce_rate - baseline_mean) / baseline_std


def calc_sector_impact(
    dli: float,
    sector: str,
    income_level_weight: float,
    months_ahead: int,
    lag_decay: float = 0.1,
) -> float:
    """
    SectorImpact from divorce lead indicator.

    Formula
    -------
    Impact(sector, region, t) = DLI × sensitivity(sector)
                                × income_weight(region)
                                × exp(−lag_decay × months_ahead)

    income_level_weight : higher-income divorces have larger consumer impact
                          proxy = log(median_income / national_median + 1)
    lag_decay           : effect attenuates with forecast horizon (0.1 = ~10m decay)
    """
    sensitivity = SECTOR_SENSITIVITIES.get(sector, 0.0)
    return (
        dli
        * sensitivity
        * income_level_weight
        * math.exp(-lag_decay * months_ahead)
    )


# ---------------------------------------------------------------------------
# 6. LPAS — LLM Perplexity Anomaly Score   [NOVEL]
# ---------------------------------------------------------------------------

DOCUMENT_TYPE_WEIGHTS = {
    "sec_8k": 1.0,
    "earnings_release": 0.8,
    "press_release": 0.5,
    "ceo_letter": 0.9,
}


def calc_lpas(
    current_perplexity: float,
    rolling_mean: float,
    rolling_std: float,
) -> float:
    """
    LLM Perplexity Anomaly Score (LPAS).  [NOVEL — no prior financial application]

    Scientific basis
    ----------------
    Language models trained on large corpora assign probability scores to
    token sequences.  Perplexity = exp(−mean log-probability) measures how
    "surprising" a document is to the model.

    Financial basis
    ---------------
    Corporate communication follows genre conventions.  When management
    deviates from their own prior communication patterns — measured by
    elevated perplexity relative to a company-specific baseline — it signals
    a structural change: stress, attempted concealment, or major strategic shift.

    This is related to but distinct from tone analysis:
    - Tone captures what is said (sentiment direction)
    - LPAS captures how it is said (linguistic pattern deviation)

    Formula
    -------
    perplexity(d) = exp(−1/N · Σ log P(word_i | context_i))
    LPAS(c, t) = (perplexity(d_t) − μ_rolling) / σ_rolling

    LPAS > +2.0 : statistically unusual language for this company
                  → management under stress, information asymmetry, or major change
    LPAS < −2.0 : unusually formulaic/predictable language
                  → possible deliberate obfuscation via boilerplate
    """
    if rolling_std == 0:
        return 0.0
    return (current_perplexity - rolling_mean) / rolling_std


def calc_composite_lpas(
    lpas_by_type: Dict[str, float],
) -> float:
    """
    Weighted composite LPAS across document types.

    Higher weight on 8-K filings (most informative) and CEO letters
    (most discretionary).  Lower weight on press releases (PR-drafted).
    """
    total_weight = 0.0
    weighted_sum = 0.0
    for doc_type, lpas in lpas_by_type.items():
        w = DOCUMENT_TYPE_WEIGHTS.get(doc_type, 0.5)
        weighted_sum += lpas * w
        total_weight += w
    if total_weight == 0:
        return 0.0
    return weighted_sum / total_weight


def lpas_combined_signal(lpas: float, tone_shift: float) -> float:
    """
    Combined LPAS + transcript tone shift signal.

    If LPAS > 2.0 AND tone_shift > 0.15: strong anomaly
    combined = LPAS × tone_shift × 0.5

    Both signals must be elevated — reduces false positives.
    """
    if lpas > 2.0 and tone_shift > 0.15:
        return lpas * tone_shift * 0.5
    return 0.0


# ---------------------------------------------------------------------------
# 7. QTPI — Quantum Threat Proximity Index   [NOVEL]
# ---------------------------------------------------------------------------

QUANTUM_MILESTONES: Dict[str, Dict] = {
    "50_qubit_system":                  {"achieved": "2017-11", "weight": 0.10},
    "quantum_advantage_demonstrated":   {"achieved": "2019-10", "weight": 0.20},
    "100_logical_qubit":                {"achieved": None,       "weight": 0.30},
    "error_correction_at_scale":        {"achieved": None,       "weight": 0.40},
    "cryptographic_attack_demonstrated":{"achieved": None,       "weight": 0.80},
    "shors_algorithm_rsa_break":        {"achieved": None,       "weight": 1.00},
}


def calc_qtpi(
    arxiv_velocity: float,
    patent_velocity: float,
    achieved_milestones: List[str],
) -> float:
    """
    Quantum Threat Proximity Index (QTPI).  [NOVEL]

    Financial basis
    ---------------
    Quantum computing threatens RSA/ECC encryption used by banks, telecom,
    and government contractors.  The timing of this threat is uncertain but
    approaching.  QTPI provides an early-warning system based on observable
    leading indicators: academic paper velocity, patent filing velocity,
    and milestone achievements.

    This is a years-to-decades signal — not tradeable on daily basis.
    Update monthly.  The value is in positioning portfolios BEFORE the
    market prices this risk.

    Formula
    -------
    QTPI = 0.3 × ArXiv_velocity + 0.3 × Patent_velocity + 0.4 × Milestone_score

    Where:
      ArXiv_velocity = z-score of quantum paper publications (90-day window)
      Patent_velocity = z-score of quantum patent filings (180-day window)
      Milestone_score = Σ(milestone_weights × achieved_flags)

    Thresholds
    ----------
    QTPI < 0.3  : quantum threat distant, no portfolio adjustment
    0.3–0.6     : accelerating — begin reducing cybersecurity incumbents
    0.6–0.8     : significant — strong rotation signal
    > 0.8       : imminent disruption — maximum defensive positioning
    """
    milestone_score = sum(
        QUANTUM_MILESTONES[m]["weight"]
        for m in achieved_milestones
        if m in QUANTUM_MILESTONES
    )
    # Normalise milestone score to 0-1
    max_milestone = sum(v["weight"] for v in QUANTUM_MILESTONES.values())
    if max_milestone > 0:
        milestone_score = milestone_score / max_milestone

    qtpi = 0.3 * arxiv_velocity + 0.3 * patent_velocity + 0.4 * milestone_score
    return max(0.0, float(qtpi))


def qtpi_sector_multiplier(qtpi: float, sector_type: str) -> float:
    """
    Position multiplier for threatened vs opportunity sectors.

    sector_type: 'threatened' | 'opportunity' | 'neutral'
    """
    if qtpi < 0.3:
        return 1.0
    if sector_type == "threatened":
        if qtpi < 0.6:
            return max(0.7, 1.0 - qtpi * 0.5)
        elif qtpi < 0.8:
            return 0.4
        else:
            return 0.1
    elif sector_type == "opportunity":
        if qtpi < 0.6:
            return 1.0 + qtpi * 0.5
        else:
            return min(2.0, 1.0 + qtpi)
    return 1.0  # neutral


# ---------------------------------------------------------------------------
# 8. FSP — Frontier Signal Purity   [NOVEL meta-metric]
# ---------------------------------------------------------------------------

def calc_fsp(
    signal_returns: pd.Series,
    factor_returns: Dict[str, pd.Series],
) -> float:
    """
    Frontier Signal Purity (FSP).  [NOVEL meta-metric]

    Financial basis
    ---------------
    Known factor returns are widely traded; their alpha erodes quickly.
    A signal that is orthogonal to ALL known factors has two implications:
    1. It is genuinely novel — competitors cannot replicate it
    2. It is more likely to remain profitable (not arbitraged away)
    3. It is also more likely to be noise — hence full validation is critical

    FSP near 1.0 : signal unlike ANY known factor → most durable if validated
    FSP near 0.0 : signal highly correlated with known factors → less durable

    Formula
    -------
    FSP(s) = 1 − max|corr(signal_s, factor_f)| for f in known_factors

    FrontierValueScore = FSP × validated_Sharpe × evidence_tier_weight
    (used by FrontierSizer to allocate more capital to purer signals)

    Parameters
    ----------
    signal_returns : pd.Series of signal return time series
    factor_returns : {factor_name: pd.Series} of known factor return series
    """
    if signal_returns.empty or not factor_returns:
        return 1.0  # no comparison possible → assume pure

    max_corr = 0.0
    for name, factor_s in factor_returns.items():
        try:
            aligned = signal_returns.align(factor_s, join="inner")[0]
            f_aligned = signal_returns.align(factor_s, join="inner")[1]
            if len(aligned) < 20:
                continue
            corr = abs(float(aligned.corr(f_aligned)))
            if not math.isnan(corr):
                max_corr = max(max_corr, corr)
        except Exception:
            continue

    return max(0.0, 1.0 - max_corr)


def calc_frontier_value_score(
    fsp: float,
    validated_sharpe: float,
    evidence_tier: int,
) -> float:
    """
    FrontierValueScore = FSP × validated_Sharpe × evidence_tier_weight

    Tier weights: {1: 1.0, 2: 0.75, 3: 0.5, 4: 0.25, 5: 0.0}
    High score = rare, novel, validated signal = maximum research priority.
    """
    tier_weights = {1: 1.0, 2: 0.75, 3: 0.5, 4: 0.25, 5: 0.0}
    tw = tier_weights.get(evidence_tier, 0.0)
    return fsp * max(validated_sharpe, 0.0) * tw
