"""
Closed-loop dashboard: 19-section status overview.

Writes output/closeloop_dashboard.txt and prints to stdout.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_OUTPUT_FILE = Path("output/closeloop_dashboard.txt")


def _bar(score: float, width: int = 20) -> str:
    """ASCII progress bar, score in [0, 1]."""
    filled = int(min(max(score, 0.0), 1.0) * width)
    return "█" * filled + "░" * (width - filled)


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


class ClosedLoopDashboard:
    """
    Assembles status from all closeloop sub-modules and renders the dashboard.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        self.store = store
        self.config = config or {}

    def render(self) -> str:
        """Render the full dashboard and return as string."""
        lines = []
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines += [
            "=" * 70,
            f"  CLOSED-LOOP FUND DASHBOARD   {ts}",
            "=" * 70,
            "",
        ]

        # 1. Store status
        lines += self._section_store_status()
        # 2. Signal weights
        lines += self._section_signal_weights()
        # 3. Recent trades
        lines += self._section_recent_trades()
        # 4. PnL attribution
        lines += self._section_pnl_attribution()
        # 5. Regime
        lines += self._section_regime()
        # 6. Entry quality
        lines += self._section_entry_quality()
        # 7. Scale-in status
        lines += self._section_scale_in()
        # 8. Peer influence
        lines += self._section_peer_influence()
        # 9. Analyst revisions
        lines += self._section_analyst_revisions()
        # 10. Academic tailwind
        lines += self._section_academic()
        # 11. Drawdown forensics
        lines += self._section_drawdown()
        # 12. Stress test
        lines += self._section_stress()
        # 13. Correlation regime
        lines += self._section_correlation()
        # 14. Market impact
        lines += self._section_market_impact()
        # 15. Tax summary
        lines += self._section_tax()
        # 16. Benchmark comparison
        lines += self._section_benchmark()
        # 17. Module wiring
        lines += self._section_wiring()
        # 18. Signal interactions
        lines += self._section_interactions()
        # 19. Weight history
        lines += self._section_weight_history()

        lines.append("=" * 70)

        dashboard = "\n".join(lines)

        # Write to file
        _OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        _OUTPUT_FILE.write_text(dashboard)
        logger.info("ClosedLoopDashboard: written to %s", _OUTPUT_FILE)

        return dashboard

    # ------------------------------------------------------------------
    # Sections
    # ------------------------------------------------------------------

    def _section_store_status(self) -> list:
        lines = ["--- 1. STORE STATUS " + "-" * 50]
        if not self.store:
            lines.append("  Store: NOT INITIALISED")
            lines.append("")
            return lines
        try:
            summary = self.store.status_summary()
            for table, count in summary.items():
                if isinstance(count, int):
                    lines.append(f"  {table:35s}: {count:6d} rows")
                else:
                    lines.append(f"  {table:35s}: {count}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_signal_weights(self) -> list:
        lines = ["--- 2. SIGNAL WEIGHTS " + "-" * 48]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_all_signal_weights()
            if not rows:
                lines.append("  No signal weights recorded yet")
            for r in rows[:10]:
                name = r.get("signal_name", "?")
                w = float(r.get("weight", 0.0))
                lines.append(f"  {name:30s} w={w:.4f}  {_bar(w)}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_recent_trades(self) -> list:
        lines = ["--- 3. RECENT TRADES (last 10) " + "-" * 39]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            trades = self.store.get_trades(limit=10)
            if not trades:
                lines.append("  No trades recorded yet")
            for t in trades:
                ticker = t.get("ticker", "?")
                pnl = float(t.get("net_pnl", 0.0))
                dt = t.get("exit_date", t.get("entry_date", "?"))
                sign = "+" if pnl >= 0 else ""
                lines.append(f"  {ticker:8s} {dt[:10]}  PnL={sign}{pnl:8.2f}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_pnl_attribution(self) -> list:
        lines = ["--- 4. PnL ATTRIBUTION " + "-" * 47]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            attrs = self.store.get_attributions(limit=5)
            if not attrs:
                lines.append("  No attribution data yet")
            for a in attrs:
                sig = a.get("signal_name", "?")
                att = float(a.get("attributed_pnl", 0.0))
                ctr = float(a.get("counterfactual_pnl", 0.0))
                lines.append(
                    f"  {sig:25s} attributed={att:+8.2f}  counterfactual={ctr:+8.2f}"
                )
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_regime(self) -> list:
        lines = ["--- 5. REGIME " + "-" * 56]
        try:
            from closeloop.learning.regime_tracker import RegimeTracker
            rt = RegimeTracker(store=self.store)
            regime = rt.current_regime()
            lines.append(f"  VIX regime      : {regime.get('vix_bucket', 'UNKNOWN')}")
            lines.append(f"  Macro regime    : {regime.get('macro_regime', 'UNKNOWN')}")
            lines.append(f"  VIX             : {regime.get('vix', 'N/A')}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_entry_quality(self) -> list:
        lines = ["--- 6. ENTRY QUALITY " + "-" * 49]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_entry_outcomes(limit=20)
            if not rows:
                lines.append("  No entry timing outcomes yet")
            else:
                entry_alphas = [float(r.get("entry_alpha", 0.0)) for r in rows]
                mean_alpha = sum(entry_alphas) / len(entry_alphas)
                positive = sum(1 for a in entry_alphas if a > 0)
                lines.append(f"  Mean entry alpha : {mean_alpha:+.4f}")
                lines.append(f"  Positive entries : {positive}/{len(rows)}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_scale_in(self) -> list:
        lines = ["--- 7. SCALE-IN STATUS " + "-" * 47]
        lines.append("  Scale-in: 3-tranche (33%/33%/34%)")
        lines.append("  Abort logic: connected via ModuleWirer #8")
        lines.append("")
        return lines

    def _section_peer_influence(self) -> list:
        lines = ["--- 8. PEER INFLUENCE " + "-" * 48]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_peer_outcomes(limit=10)
            if not rows:
                lines.append("  No peer influence outcomes yet")
            else:
                lines.append(f"  {len(rows)} peer influence outcomes recorded")
                pos = sum(1 for r in rows if float(r.get("pnl_contribution", 0)) > 0)
                lines.append(f"  Positive contributions: {pos}/{len(rows)}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_analyst_revisions(self) -> list:
        lines = ["--- 9. ANALYST REVISIONS " + "-" * 45]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_analyst_outcomes(limit=10)
            if not rows:
                lines.append("  No analyst revision outcomes yet")
            else:
                lines.append(f"  {len(rows)} analyst revision outcomes recorded")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_academic(self) -> list:
        lines = ["--- 10. ACADEMIC TAILWIND " + "-" * 44]
        try:
            from closeloop.context.academic_fundamental_bridge import AcademicFundamentalBridge
            bridge = AcademicFundamentalBridge(store=self.store)
            lines.append("  AcademicFundamentalBridge: available")
        except Exception:
            lines.append("  AcademicFundamentalBridge: not loaded")
        lines.append("")
        return lines

    def _section_drawdown(self) -> list:
        lines = ["--- 11. DRAWDOWN FORENSICS " + "-" * 43]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_drawdown_events(limit=5)
            if not rows:
                lines.append("  No drawdown events recorded")
            else:
                for r in rows:
                    dd = float(r.get("drawdown_pct", 0.0))
                    dtype = r.get("failure_type", "?")
                    dt = r.get("start_date", "?")[:10]
                    lines.append(f"  {dt}  DD={dd:.1%}  Type={dtype}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_stress(self) -> list:
        lines = ["--- 12. STRESS TEST " + "-" * 50]
        try:
            from closeloop.stress.stress_tester import StressTester
            tester = StressTester(store=self.store)
            result = tester.run()
            wsr = result.get("weighted_stress_risk", 0.0)
            cf = result.get("crisis_fragile", False)
            n = result.get("n_scenarios", 0)
            lines.append(f"  WeightedStressRisk : {wsr:.4f}  {_bar(min(wsr * 4, 1.0))}")
            lines.append(f"  CrisisFragile      : {'YES ⚠' if cf else 'NO'}")
            lines.append(f"  Scenarios evaluated: {n}")
            if result.get("top_scenarios"):
                top = result["top_scenarios"][0]
                lines.append(f"  Top scenario: {top.get('scenario_name','?')} (relevance={top.get('relevance',0):.3f})")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_correlation(self) -> list:
        lines = ["--- 13. CORRELATION REGIME " + "-" * 43]
        try:
            from closeloop.risk.correlation_regime import CorrelationRegimeDetector
            detector = CorrelationRegimeDetector(store=self.store)
            lines.append("  CorrelationRegimeDetector: available")
            lines.append("  Run: closeloop correlation (live data needed)")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_market_impact(self) -> list:
        lines = ["--- 14. MARKET IMPACT MODEL " + "-" * 42]
        try:
            from closeloop.risk.market_impact import MarketImpactModel
            lines.append("  MarketImpactModel: available")
            lines.append("  η(US)=0.15  η(UK)=0.20  Impact=η·σ·√(Q/V)")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_tax(self) -> list:
        lines = ["--- 15. TAX SUMMARY " + "-" * 50]
        try:
            from closeloop.risk.tax_manager import TaxManager
            tm = TaxManager(store=self.store)
            liability = tm.compute_annual_liability()
            tg = liability.get("taxable_gain", 0.0)
            tax_h = liability.get("estimated_tax_higher_rate", 0.0)
            lines.append(f"  Net gain (YTD)     : £{liability.get('net_gain', 0.0):.2f}")
            lines.append(f"  CGT allowance      : £{liability.get('annual_allowance', 3000):.0f}")
            lines.append(f"  Taxable gain       : £{tg:.2f}")
            lines.append(f"  Est. tax (higher)  : £{tax_h:.2f}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_benchmark(self) -> list:
        lines = ["--- 16. BENCHMARK COMPARISON " + "-" * 41]
        try:
            from closeloop.risk.benchmark_tracker import BenchmarkTracker
            bt = BenchmarkTracker(store=self.store)
            lines.append(bt.summary_text())
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_wiring(self) -> list:
        lines = ["--- 17. MODULE WIRING " + "-" * 48]
        try:
            from closeloop.integration.module_wirer import ModuleWirer
            wirer = ModuleWirer(store=self.store, config=self.config)
            result = wirer.wire_all()
            n_wired = result.get("n_wired", 0)
            n_total = result.get("n_total", 12)
            lines.append(f"  Wired: {n_wired}/{n_total}")
            for num, info in result.get("disconnects", {}).items():
                icon = "[OK]" if info["wired"] else "[--]"
                lines.append(f"    {icon} #{num:2d} {info['description'][:50]}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_interactions(self) -> list:
        lines = ["--- 18. SIGNAL INTERACTIONS " + "-" * 42]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_interactions(limit=10)
            if not rows:
                lines.append("  No signal interaction data yet")
            else:
                for r in rows:
                    combo = r.get("signal_combo", "?")
                    mult = float(r.get("multiplier", 1.0))
                    n = r.get("n_trades", 0)
                    lines.append(f"  {combo:35s} mult={mult:.3f}  n={n}")
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines

    def _section_weight_history(self) -> list:
        lines = ["--- 19. WEIGHT HISTORY (last 5 changes) " + "-" * 30]
        if not self.store:
            lines.append("  Store unavailable")
            lines.append("")
            return lines
        try:
            rows = self.store.get_weight_history(limit=5)
            if not rows:
                lines.append("  No weight history yet")
            else:
                for r in rows:
                    sig = r.get("signal_name", "?")
                    old_w = float(r.get("old_weight", 0.0))
                    new_w = float(r.get("new_weight", 0.0))
                    dt = r.get("updated_at", "?")[:16]
                    delta = new_w - old_w
                    lines.append(
                        f"  {dt}  {sig:25s}  {old_w:.4f} → {new_w:.4f}  ({delta:+.4f})"
                    )
        except Exception as exc:
            lines.append(f"  Error: {exc}")
        lines.append("")
        return lines


# Alias for backwards-compatible imports
CloseloopDashboard = ClosedLoopDashboard
