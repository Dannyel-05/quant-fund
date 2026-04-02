"""
Cross-signal interaction terms for the frontier intelligence module.

These are higher-order combinations of frontier signals that may have
emergent predictive power beyond any individual component.
All interactions are logged and validated before use.
"""
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def geomagnetic_expiry_divorce_trinity(
    grai: float,
    gamma_overhang_score: float,
    divorce_anomaly: float,
) -> float:
    """
    'GeomagnEticExpiryDivorceTrinity' interaction.

    Hypothesis: geomagnetic risk aversion + options gamma pressure +
    demographic stress simultaneously = unusual risk-off cascade.

    Economic story (weak but testable):
    - Geomagnetic storm → elevated risk aversion
    - Options expiry → forced market maker hedging = amplified moves
    - Divorce spike → financial distress, forced selling
    All three acting together could produce an unusual short-term
    market dislocation.

    This is a nonsense test — we include it because the cost is zero
    and it may reveal a genuine interaction.
    """
    return grai * gamma_overhang_score * divorce_anomaly


def schumann_reddit_lunar_combo(
    schumann_deviation: float,
    reddit_coordination_score: float,
    lunar_phase_angle_rad: float,
) -> float:
    """
    'SchumannRedditLunar' interaction.

    Completely speculative. Tests whether electromagnetic anomalies
    (Schumann) coincide with social media coordination (Reddit) near
    full moons (lunar_phase_angle near 0 or 2π = full moon).

    Economic story: none convincing. This is a pure statistical test.
    If it survives validation: name it, track it, size at Tier 5.
    """
    lunar_factor = math.cos(lunar_phase_angle_rad)  # 1.0 at full moon
    return schumann_deviation * reddit_coordination_score * lunar_factor


def church_congress_hiring_combo(
    church_attendance_signal: float,
    congressional_signal_strength: float,
    hiring_momentum: float,
) -> float:
    """
    'ChurchCongressHiring' interaction.

    Semi-plausible hypothesis: rising church attendance (growing regional
    confidence) + congressional smart-money buying + hiring surge =
    genuine fundamental improvement being noticed simultaneously by
    multiple independent proxies.

    This combination is interesting because each component is independently
    observable and potentially correlated with fundamentals.
    """
    return church_attendance_signal * congressional_signal_strength * hiring_momentum


def pollen_squeeze_satellite_combo(
    pollen_stress_index: float,
    short_squeeze_score: float,
    satellite_activity_drop: float,
) -> float:
    """
    'PollenSqueezeActivity' interaction.

    Hypothesis: high pollen → reduced outdoor activity → reduced retail footfall
    (satellite confirms) + heavily shorted retail stocks = potential catalyst.

    This tests whether seasonal biological signals (pollen) interact with
    market structure signals (short squeeze) in predictable ways.
    """
    return pollen_stress_index * short_squeeze_score * satellite_activity_drop


def canal_obituary_permit_combo(
    canal_congestion_index: float,
    obituary_impact_score: float,
    building_permit_inflection: float,
) -> float:
    """
    'CanalObituaryPermit' interaction.

    Pure statistical test. Three independently collected, non-overlapping
    signals from completely different domains (physical, social, financial).

    If this combination predicts returns: it is almost certainly spurious
    (overfitting). BUT: if it survives out-of-sample testing AND has a
    nonsense_score > 0.8 (very economically implausible), it may be
    genuinely exploitable precisely because it is so non-obvious.

    RenTec philosophy: test everything. The inexplicable pattern that
    passes rigorous testing is the most durable.
    """
    return canal_congestion_index * obituary_impact_score * building_permit_inflection


def electricity_citation_perplexity_combo(
    electricity_anomaly: float,
    academic_citation_velocity: float,
    lpas_mean: float,
) -> float:
    """
    'ElectricityCitationPerplexity' interaction.

    More plausible: electricity anomaly (industrial activity up) + academic
    citation acceleration (technology approaching commercialisation) +
    unusual management language (LPAS) for the same sector companies.

    Could indicate: a technology sector company is ramping production
    (electricity), their technology is being cited more (commercialising),
    and management is describing something unusual (new product/contract).

    Economic story: coherent. This is a Tier C candidate (novel, no
    prior literature, multi-source confirmation).
    """
    return electricity_anomaly * academic_citation_velocity * lpas_mean


def quantum_amr_contagion_combo(
    qtpi: float,
    amr_urgency: float,
    social_contagion_r0: float,
) -> float:
    """
    'QuantumAMRContagion' interaction.

    Tests whether quantum computing progress + antimicrobial resistance
    urgency + high social R₀ simultaneously predicts biotech/pharma
    sector rotation.

    Economic story: quantum accelerates drug discovery → AMR becomes
    solvable → biotech buzz spreads socially. This could be an early
    signal of the next biotech cycle if all three align.
    """
    return qtpi * amr_urgency * max(social_contagion_r0 - 1.0, 0.0)


def get_all_interactions(signals: Dict[str, float]) -> Dict[str, float]:
    """
    Calculate all cross-signal interaction terms from a signals dict.
    Returns dict of {interaction_name: value}.
    Missing signals default to 0.0.
    """
    g = signals.get

    return {
        "geomagnetic_expiry_divorce": geomagnetic_expiry_divorce_trinity(
            g("grai", 0.0), g("gamma_overhang_score", 0.0), g("divorce_anomaly", 0.0)
        ),
        "schumann_reddit_lunar": schumann_reddit_lunar_combo(
            g("schumann_deviation", 0.0),
            g("reddit_coordination_score", 0.0),
            g("lunar_phase_angle_rad", 0.0),
        ),
        "church_congress_hiring": church_congress_hiring_combo(
            g("church_attendance_signal", 0.0),
            g("congressional_signal_strength", 0.0),
            g("hiring_momentum", 0.0),
        ),
        "pollen_squeeze_satellite": pollen_squeeze_satellite_combo(
            g("pollen_stress_index", 0.0),
            g("short_squeeze_score", 0.0),
            g("satellite_activity_drop", 0.0),
        ),
        "canal_obituary_permit": canal_obituary_permit_combo(
            g("canal_congestion_index", 0.0),
            g("obituary_impact_score", 0.0),
            g("building_permit_inflection", 0.0),
        ),
        "electricity_citation_perplexity": electricity_citation_perplexity_combo(
            g("electricity_anomaly", 0.0),
            g("academic_citation_velocity", 0.0),
            g("lpas_mean", 0.0),
        ),
        "quantum_amr_contagion": quantum_amr_contagion_combo(
            g("qtpi", 0.0),
            g("amr_urgency", 0.0),
            g("social_contagion_r0", 1.0),
        ),
    }
