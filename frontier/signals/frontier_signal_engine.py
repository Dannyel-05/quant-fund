"""
Frontier Signal Engine.

Orchestrates all frontier collectors, computes UMCI, generates
directional signals with tiered position sizing.
"""
import importlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# (class_name, module_path)
COLLECTORS = [
    ("GeomagneticCollector",        "frontier.physical.geomagnetic_collector"),
    ("SchumannCollector",           "frontier.physical.schumann_collector"),
    ("PollenCollector",             "frontier.physical.pollen_collector"),
    ("ElectricityCollector",        "frontier.physical.electricity_collector"),
    ("CanalCongestionCollector",    "frontier.physical.canal_congestion_collector"),
    ("SatelliteImageryCollector",   "frontier.physical.satellite_imagery_collector"),
    ("AttentionEconomyCollector",   "frontier.social.attention_economy"),
    ("SocialContagionMapper",       "frontier.social.social_contagion_mapper"),
    ("ObituaryTracker",             "frontier.social.obituary_tracker"),
    ("ChurchAttendanceCollector",   "frontier.social.church_attendance"),
    ("DivorceFilingCollector",      "frontier.social.divorce_filing_collector"),
    ("HQTrafficMonitor",            "frontier.social.hq_traffic_monitor"),
    ("AcademicCitationTracker",     "frontier.scientific.academic_citation_tracker"),
    ("AMRResearchTracker",          "frontier.scientific.amr_research_tracker"),
    ("SoilHealthCollector",         "frontier.scientific.soil_health_collector"),
    ("QuantumReadinessTracker",     "frontier.scientific.quantum_readiness_tracker"),
    ("FoodSafetyCollector",         "frontier.scientific.food_safety_collector"),
    ("OptionExpiryOverhangCollector","frontier.financial_frontier.option_expiry_overhang"),
    ("BuildingPermitCollector",     "frontier.financial_frontier.building_permit_collector"),
    ("LLMPerplexityScorer",         "frontier.financial_frontier.llm_perplexity_scorer"),
]


class FrontierSignalEngine:
    """
    Collects all frontier signals, computes UMCI, and generates
    tiered directional signals for each requested ticker.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        self._store = store
        self._config = config or {}

    def _collect_all(self) -> Dict[str, Dict]:
        """Run all collectors; return {signal_name: result_dict}."""
        results = {}
        for cls_name, mod_path in COLLECTORS:
            try:
                mod = importlib.import_module(mod_path)
                cls = getattr(mod, cls_name)
                result = cls(self._config).collect()
                sig = result.get("signal_name", cls_name)
                results[sig] = result
            except Exception as e:
                logger.debug(f"Collector {cls_name} skipped: {e}")
        return results

    def _get_cached_or_collect(self) -> Dict[str, float]:
        """
        Try store first (last 24h), fall back to live collection,
        fall back to neutral defaults.
        """
        signals: Dict[str, float] = {}

        if self._store:
            try:
                for sig_name in [
                    "grai", "schumann_deviation", "pollen_stress_index",
                    "electricity_anomaly", "canal_congestion_index",
                    "satellite_activity_drop", "asi", "social_contagion_r0",
                    "obituary_impact_score", "church_attendance_signal",
                    "divorce_anomaly", "hq_traffic_index",
                    "academic_citation_velocity", "amr_urgency",
                    "soil_health_degradation", "qtpi", "food_safety_risk",
                    "lpas_mean", "gamma_overhang_score",
                    "building_permit_inflection",
                ]:
                    hist = self._store.get_raw_history(sig_name, days_back=1)
                    if hist:
                        signals[sig_name] = hist[-1]["value"]
            except Exception as e:
                logger.debug(f"Store fetch failed: {e}")

        # Live collection for anything missing
        if len(signals) < 5:
            collected = self._collect_all()
            for sig_name, result in collected.items():
                signals.setdefault(sig_name, result.get("value", 0.0))

        # Neutral defaults
        defaults = {
            "grai": 0.0, "schumann_deviation": 0.0, "pollen_stress_index": 0.0,
            "electricity_anomaly": 0.0, "canal_congestion_index": 0.0,
            "satellite_activity_drop": 0.0, "asi": 0.7,
            "social_contagion_r0": 1.0, "obituary_impact_score": 0.0,
            "church_attendance_signal": 0.0, "divorce_anomaly": 0.0,
            "hq_traffic_index": 0.0, "academic_citation_velocity": 0.3,
            "amr_urgency": 0.5, "soil_health_degradation": 0.2,
            "qtpi": 0.35, "food_safety_risk": 0.1, "lpas_mean": 0.0,
            "gamma_overhang_score": 0.0, "building_permit_inflection": 0.0,
        }
        for k, v in defaults.items():
            signals.setdefault(k, v)

        return signals

    def get_umci_snapshot(self) -> Dict:
        """Return full UMCI computation from current signals."""
        signals = self._get_cached_or_collect()
        try:
            from frontier.equations.unified_complexity_index import (
                calc_physical_complexity, calc_social_complexity,
                calc_scientific_complexity, calc_financial_frontier_complexity,
                calc_altdata_complexity, calc_umci,
            )
            pc  = calc_physical_complexity(
                signals["grai"], signals["schumann_deviation"],
                signals["pollen_stress_index"], signals["electricity_anomaly"],
                signals["canal_congestion_index"])
            sc  = calc_social_complexity(
                signals["asi"], signals["social_contagion_r0"],
                signals["divorce_anomaly"], signals["obituary_impact_score"],
                signals["church_attendance_signal"])
            sci = calc_scientific_complexity(
                signals["qtpi"], signals["amr_urgency"],
                signals["academic_citation_velocity"], signals["soil_health_degradation"])
            ffc = calc_financial_frontier_complexity(
                signals["lpas_mean"], signals["gamma_overhang_score"],
                signals["building_permit_inflection"], signals["food_safety_risk"])
            adc = calc_altdata_complexity(
                signals.get("reddit_coordination_score", 0.3), 0.2,
                signals["canal_congestion_index"], 0.3)
            umci, breakdown = calc_umci(pc, sc, sci, ffc, adc, self._config)
            return {"umci": umci, "breakdown": breakdown, "signals": signals}
        except Exception as e:
            logger.warning(f"UMCI computation failed: {e}")
            return {"umci": 25.0, "breakdown": {"level": "LOW", "position_multiplier": 1.0}, "signals": signals}

    def generate(self, tickers: List[str], market: str = "us") -> List[Dict]:
        """
        Generate frontier signals for each ticker.

        Returns list of signal dicts with directional recommendation,
        confidence, evidence tier, and full context.
        """
        snapshot = self.get_umci_snapshot()
        signals = snapshot["signals"]
        umci = snapshot["umci"]
        breakdown = snapshot["breakdown"]

        umci_multiplier = breakdown.get("position_multiplier", 1.0)
        umci_level = breakdown.get("level", "LOW")
        grai  = signals.get("grai", 0.0)
        asi   = signals.get("asi", 0.7)
        qtpi  = signals.get("qtpi", 0.35)
        lpas  = signals.get("lpas_mean", 0.0)

        # Mean quality across available collectors
        quality_scores = [
            result.get("quality_score", 0.5)
            for _, result in (self._collect_all().items() if len(signals) < 5 else {}.items())
        ]
        mean_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.6

        output = []
        for ticker in tickers:
            # Directional rules (Tier 3 — frontier, not yet validated)
            direction = 0
            reason = []

            if grai > 0.6:
                direction -= 1
                reason.append(f"GRAI={grai:.2f} (risk-off)")
            elif grai < 0.2:
                direction += 1
                reason.append(f"GRAI={grai:.2f} (calm geomagnetic)")

            if asi < 0.3:
                direction += 1
                reason.append(f"ASI={asi:.2f} (attention dispersed, contrarian buy)")
            elif asi > 0.8:
                direction -= 1
                reason.append(f"ASI={asi:.2f} (attention saturated, fade)")

            if abs(lpas) > 2.0:
                direction -= abs(direction) or 1  # amplify existing direction, or add caution
                reason.append(f"LPAS={lpas:.2f} (unusual language)")

            # No signal if UMCI is extreme
            if umci_multiplier == 0.0:
                direction = 0
                reason = ["UMCI UNPRECEDENTED — no new positions"]

            # Clamp direction
            direction = max(-1, min(1, direction))
            confidence = (1.0 - umci / 100.0) * max(0.3, mean_quality)

            output.append({
                "ticker": ticker,
                "direction": direction,
                "confidence": round(confidence, 4),
                "evidence_tier": 3,
                "signal_name": "frontier_composite",
                "umci": round(umci, 2),
                "umci_level": umci_level,
                "umci_multiplier": umci_multiplier,
                "grai": round(grai, 4),
                "asi": round(asi, 4),
                "qtpi": round(qtpi, 4),
                "lpas": round(lpas, 4),
                "reason": "; ".join(reason) or "No strong frontier signal",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_signals": list(signals.keys()),
            })

        return output
