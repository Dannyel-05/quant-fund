"""
Monthly stress runner: scheduled and on-demand stress test execution.

Runs stress tests on a monthly schedule (first Monday) and whenever
triggered by UMCI spike or drawdown event.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_OUTPUT_DIR = Path("output/stress_reports")


class MonthlyStressRunner:
    """
    Orchestrates regular stress testing runs and saves reports to disk.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        self.store = store
        self.config = config or {}
        self._stress_tester = None
        self._last_run: Optional[str] = None

    def _get_tester(self):
        if self._stress_tester is None:
            try:
                from closeloop.stress.stress_tester import StressTester
                self._stress_tester = StressTester(store=self.store)
            except Exception as exc:
                logger.warning("MonthlyStressRunner: could not load StressTester: %s", exc)
        return self._stress_tester

    def run(
        self,
        portfolio_signals: Optional[List[str]] = None,
        portfolio_value: float = 100_000.0,
        current_conditions: Optional[Dict] = None,
        trigger: str = "scheduled",
    ) -> Dict:
        """
        Execute a full stress test run and save the report.

        Parameters
        ----------
        portfolio_signals : active signal names
        portfolio_value   : current fund value
        current_conditions: override auto-estimated conditions
        trigger           : "scheduled" | "umci_spike" | "drawdown" | "manual"

        Returns
        -------
        stress result dict + report_path
        """
        tester = self._get_tester()
        if not tester:
            return {"error": "StressTester unavailable", "trigger": trigger}

        timestamp = datetime.now(timezone.utc)
        result = tester.run(
            current_conditions=current_conditions,
            portfolio_signals=portfolio_signals,
            portfolio_value=portfolio_value,
        )
        result["trigger"] = trigger
        result["run_timestamp"] = timestamp.isoformat()
        self._last_run = timestamp.isoformat()

        # Save report
        report_path = self._save_report(result, timestamp, trigger)
        result["report_path"] = str(report_path)

        logger.info(
            "MonthlyStressRunner: run complete [%s] WeightedRisk=%.3f CrisisFragile=%s",
            trigger,
            result.get("weighted_stress_risk", 0.0),
            result.get("crisis_fragile", False),
        )
        return result

    def should_run_monthly(self) -> bool:
        """Check if a monthly run is due (first Monday of the month)."""
        now = datetime.now()
        if now.weekday() != 0:  # Monday
            return False
        if now.day > 7:  # Not first week
            return False
        if self._last_run:
            try:
                last = datetime.fromisoformat(self._last_run)
                if last.month == now.month and last.year == now.year:
                    return False  # Already ran this month
            except Exception:
                pass
        return True

    def run_if_due(self, **kwargs) -> Optional[Dict]:
        """Run only if monthly schedule requires it."""
        if self.should_run_monthly():
            logger.info("MonthlyStressRunner: monthly run triggered")
            return self.run(trigger="scheduled", **kwargs)
        return None

    def run_on_umci_spike(self, umci_score: float, threshold: float = 70.0, **kwargs) -> Optional[Dict]:
        """Trigger stress test if UMCI exceeds threshold."""
        if umci_score >= threshold:
            logger.warning(
                "MonthlyStressRunner: UMCI spike (%.1f >= %.1f) — triggering stress run",
                umci_score, threshold,
            )
            return self.run(trigger="umci_spike", **kwargs)
        return None

    def run_on_drawdown(self, drawdown_pct: float, threshold: float = 0.05, **kwargs) -> Optional[Dict]:
        """Trigger stress test if drawdown exceeds threshold."""
        if drawdown_pct >= threshold:
            logger.warning(
                "MonthlyStressRunner: drawdown trigger (%.1f%% >= %.1f%%)",
                drawdown_pct * 100, threshold * 100,
            )
            return self.run(trigger="drawdown", **kwargs)
        return None

    def _save_report(self, result: Dict, timestamp: datetime, trigger: str) -> Path:
        """Save stress report to output/stress_reports/."""
        _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        fname = f"stress_{trigger}_{timestamp.strftime('%Y%m%d_%H%M%S')}.txt"
        path = _OUTPUT_DIR / fname

        lines = [
            f"STRESS TEST REPORT",
            f"==================",
            f"Timestamp  : {timestamp.isoformat()}",
            f"Trigger    : {trigger}",
            f"",
            f"WeightedStressRisk : {result.get('weighted_stress_risk', 0.0):.4f}",
            f"CrisisFragile      : {result.get('crisis_fragile', False)}",
            f"Scenarios run      : {result.get('n_scenarios', 0)}",
            f"",
            f"Summary: {result.get('summary', 'N/A')}",
            f"",
            f"TOP SCENARIOS:",
        ]
        for i, sc in enumerate(result.get("top_scenarios", [])[:5], 1):
            lines.append(
                f"  {i}. {sc.get('scenario_name', '?'):30s} "
                f"relevance={sc.get('relevance', 0):.3f} "
                f"weighted_loss={sc.get('weighted_loss_pct', 0):.3f}"
            )

        path.write_text("\n".join(lines))
        return path
