"""
Stress tester: applies CRISIS_SCENARIOS to current portfolio and signals.

WeightedStressRisk = sum(scenario_loss * scenario_relevance)
scenario_relevance = cosine similarity of conditions vectors

Flags CRISIS_FRAGILE when weighted stress risk exceeds threshold.
"""
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_CRISIS_FRAGILE_THRESHOLD = 0.15  # 15% weighted stress risk


def _cosine_similarity(a: Dict[str, float], b: Dict[str, float]) -> float:
    """Cosine similarity between two sparse condition vectors (dict form)."""
    keys = set(a) | set(b)
    if not keys:
        return 0.0
    dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
    norm_a = math.sqrt(sum(v ** 2 for v in a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in b.values()))
    if norm_a < 1e-9 or norm_b < 1e-9:
        return 0.0
    return dot / (norm_a * norm_b)


class StressTester:
    """
    Applies scenarios from the crisis library to the current market conditions
    and portfolio signals.
    """

    def __init__(self, store=None):
        self.store = store
        self._scenarios = None

    def _load_scenarios(self) -> List[Dict]:
        if self._scenarios is None:
            try:
                from closeloop.stress.crisis_library import CRISIS_SCENARIOS
                # Support both dict-of-dicts and list formats
                if isinstance(CRISIS_SCENARIOS, dict):
                    scenarios = []
                    for key, val in CRISIS_SCENARIOS.items():
                        s = dict(val)
                        s.setdefault("name", key)
                        scenarios.append(s)
                    self._scenarios = scenarios
                else:
                    self._scenarios = list(CRISIS_SCENARIOS)
            except Exception as exc:
                logger.warning("StressTester: could not load crisis library: %s", exc)
                self._scenarios = []
        return self._scenarios

    def run(
        self,
        current_conditions: Optional[Dict[str, float]] = None,
        portfolio_signals: Optional[List[str]] = None,
        portfolio_value: float = 100_000.0,
    ) -> Dict:
        """
        Run stress tests against all scenarios.

        Parameters
        ----------
        current_conditions : dict mapping condition names to float values [0, 1].
            E.g., {"vix_elevated": 0.8, "credit_spread_wide": 0.6}
        portfolio_signals  : list of active signal names
        portfolio_value    : current portfolio value (for absolute loss estimates)

        Returns
        -------
        dict with:
            weighted_stress_risk : float — WeightedStressRisk
            crisis_fragile       : bool — True if above threshold
            top_scenarios        : list — top 5 most relevant scenarios
            scenario_results     : list — all scenario results
            summary              : str
        """
        scenarios = self._load_scenarios()
        if not scenarios:
            return self._empty()

        if current_conditions is None:
            current_conditions = self._estimate_current_conditions()

        portfolio_signals = portfolio_signals or []
        results = []

        for scenario in scenarios:
            try:
                cond_vec = scenario.get("conditions_vector", {})
                relevance = _cosine_similarity(current_conditions, cond_vec)

                # Base loss from scenario (use worst-case signal performance)
                sig_perf = scenario.get("signal_performance", {})
                base_loss = self._estimate_base_loss(sig_perf, portfolio_signals)

                weighted_loss = base_loss * relevance
                abs_loss = weighted_loss * portfolio_value

                results.append({
                    "scenario_name": scenario.get("name", "Unknown"),
                    "year": scenario.get("year"),
                    "relevance": round(relevance, 4),
                    "base_loss_pct": round(base_loss, 4),
                    "weighted_loss_pct": round(weighted_loss, 4),
                    "estimated_abs_loss": round(abs_loss, 2),
                    "affected_signals": [
                        s for s in portfolio_signals if s in sig_perf
                    ],
                })
            except Exception as exc:
                logger.debug("StressTester scenario error: %s", exc)
                continue

        if not results:
            return self._empty()

        weighted_stress_risk = sum(r["weighted_loss_pct"] for r in results)
        crisis_fragile = weighted_stress_risk >= _CRISIS_FRAGILE_THRESHOLD

        results.sort(key=lambda x: x["relevance"], reverse=True)
        top_5 = results[:5]

        summary = (
            f"WeightedStressRisk={weighted_stress_risk:.3f} "
            f"({'CRISIS_FRAGILE' if crisis_fragile else 'OK'}) | "
            f"Top scenario: {top_5[0]['scenario_name'] if top_5 else 'N/A'}"
        )

        # Persist to store
        if self.store:
            try:
                self.store.record_stress_outcome(
                    run_date=__import__("datetime").datetime.now().isoformat(),
                    scenario_name="aggregate",
                    weighted_stress_risk=weighted_stress_risk,
                    crisis_fragile=crisis_fragile,
                    top_scenario=top_5[0]["scenario_name"] if top_5 else "",
                    conditions=str(current_conditions),
                )
            except Exception as exc:
                logger.debug("StressTester store error: %s", exc)

        return {
            "weighted_stress_risk": round(weighted_stress_risk, 4),
            "crisis_fragile": crisis_fragile,
            "top_scenarios": top_5,
            "scenario_results": results,
            "summary": summary,
            "n_scenarios": len(scenarios),
        }

    def _estimate_current_conditions(self) -> Dict[str, float]:
        """Estimate current market conditions from live data."""
        conditions = {}
        try:
            import yfinance as yf
            vix = yf.Ticker("^VIX").fast_info.last_price or 20.0
            conditions["vix_elevated"] = min(1.0, max(0.0, (vix - 15) / 40))
            conditions["vix_extreme"] = min(1.0, max(0.0, (vix - 30) / 20))

            # 10Y-2Y spread (proxy for recession risk)
            try:
                t10 = yf.Ticker("^TNX").fast_info.last_price or 4.0
                t2 = yf.Ticker("^IRX").fast_info.last_price or 4.0
                spread = t10 - t2
                conditions["yield_curve_inverted"] = 1.0 if spread < 0 else 0.0
                conditions["credit_stress"] = min(1.0, max(0.0, -spread / 2))
            except Exception:
                pass
        except Exception as exc:
            logger.debug("StressTester.estimate_conditions: %s", exc)
        return conditions

    def _estimate_base_loss(
        self, signal_performance: Dict[str, float], active_signals: List[str]
    ) -> float:
        """
        Estimate portfolio loss fraction given scenario signal performance dict.
        signal_performance maps signal_name -> return_during_scenario (negative = loss).
        """
        if not signal_performance:
            return 0.05  # default 5% loss assumption

        relevant = [signal_performance[s] for s in active_signals if s in signal_performance]
        if not relevant:
            # Use mean of all scenario losses
            all_losses = list(signal_performance.values())
            relevant = all_losses

        mean_return = sum(relevant) / len(relevant)
        # Convert to loss (positive = bad)
        return max(0.0, -mean_return)

    def _empty(self) -> Dict:
        return {
            "weighted_stress_risk": 0.0,
            "crisis_fragile": False,
            "top_scenarios": [],
            "scenario_results": [],
            "summary": "No scenarios loaded",
            "n_scenarios": 0,
        }
