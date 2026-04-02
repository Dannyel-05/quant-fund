"""DeepDataDashboard — daily output to output/deepdata_dashboard.txt."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from rich.console import Console
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    Console = None
    HAS_RICH = False

OUTPUT_DIR = Path(__file__).resolve().parents[3] / "output"
OUTPUT_FILE = OUTPUT_DIR / "deepdata_dashboard.txt"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DeepDataDashboard:
    """Builds and renders the daily DeepData intelligence dashboard."""

    def __init__(self, config: dict):
        self.config = config or {}
        self.store = None
        self.output_path = Path(
            self.config.get("output_path", str(OUTPUT_FILE))
        )
        if HAS_RICH:
            self._console = Console(record=True)
        else:
            self._console = None

    def set_store(self, store) -> None:
        self.store = store

    # ------------------------------------------------------------------
    # Main render
    # ------------------------------------------------------------------

    def render(self, all_module_results: dict = None) -> str:
        """
        Build sections and write to output/deepdata_dashboard.txt.
        Returns full text string.
        """
        all_module_results = all_module_results or {}
        now = datetime.now(timezone.utc)

        lines = []
        lines += self._header(now)
        lines += self._section_options(all_module_results.get("options", []))
        lines += self._section_short_interest(all_module_results.get("short_interest", []))
        lines += self._section_transcripts(all_module_results.get("transcripts", []))
        lines += self._section_congressional(all_module_results.get("congressional", []))
        lines += self._section_supply_chain(all_module_results.get("supply_chain", []))
        lines += self._section_patents(all_module_results.get("patents", []))
        lines += self._section_patterns(
            all_module_results.get("patterns", []) + all_module_results.get("nonsense", [])
        )
        lines += self._section_factors(
            all_module_results.get("factors", []) + all_module_results.get("factor_signal", [])
        )
        lines.append("")
        lines.append("=" * 72)
        lines.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append("=" * 72)

        text = "\n".join(lines)

        # Write to file
        try:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.write_text(text, encoding="utf-8")
        except Exception as exc:
            logger.warning("Dashboard write failed: %s", exc)

        return text

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _header(self, now) -> list:
        lines = [
            "=" * 72,
            "  DEEPDATA INTELLIGENCE DASHBOARD",
            f"  {now.strftime('%A %d %B %Y  |  %H:%M UTC')}",
            "=" * 72,
            "",
        ]
        return lines

    # ------------------------------------------------------------------
    # Section renderers
    # ------------------------------------------------------------------

    def _section_options(self, results: list) -> list:
        """Top unusual options (by SMFI), sweep detections, IV rank extremes, dark pool alerts."""
        lines = [
            "─" * 72,
            "  1. OPTIONS FLOW TODAY",
            "─" * 72,
        ]
        if not results:
            lines.append("  No options data available.")
            lines.append("")
            return lines

        sweeps = [r for r in results if "sweep" in r.get("data_type", "").lower()]
        dark_pool = [r for r in results if "dark_pool" in r.get("data_type", "").lower()]
        iv_extremes = [r for r in results if "iv" in r.get("data_type", "").lower()]
        other = [r for r in results if r not in sweeps + dark_pool + iv_extremes]

        lines.append(f"  Total signals: {len(results)}")

        if sweeps:
            lines.append(f"\n  INSTITUTIONAL SWEEPS ({len(sweeps)}):")
            for r in sweeps[:10]:
                raw = r.get("raw_data", {}) or {}
                ticker = r.get("ticker", "?")
                val = r.get("value", 0.0)
                lines.append(f"    {ticker:8s}  SMFI={val:.2f}  {raw.get('direction','?')}")

        if dark_pool:
            lines.append(f"\n  DARK POOL ALERTS ({len(dark_pool)}):")
            for r in dark_pool[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(f"    {r.get('ticker','?'):8s}  value={r.get('value',0):.2f}")

        if iv_extremes:
            lines.append(f"\n  IV RANK EXTREMES ({len(iv_extremes)}):")
            for r in iv_extremes[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  IV_rank={r.get('value',0):.2f}")

        if other:
            lines.append(f"\n  OTHER OPTIONS SIGNALS ({len(other)}):")
            for r in other[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  {r.get('data_type','?')}  val={r.get('value',0):.3f}")

        lines.append("")
        return lines

    def _section_short_interest(self, results: list) -> list:
        """Stocks entering squeeze (score > 60), predicted squeezes, new patterns."""
        lines = [
            "─" * 72,
            "  2. SHORT INTEREST ALERTS",
            "─" * 72,
        ]
        if not results:
            lines.append("  No short interest data available.")
            lines.append("")
            return lines

        entering_squeeze = [r for r in results if float(r.get("value", 0)) > 60]
        lines.append(f"  Total alerts: {len(results)}")

        if entering_squeeze:
            lines.append(f"\n  ENTERING SQUEEZE TERRITORY (score > 60):")
            for r in sorted(entering_squeeze, key=lambda x: -float(x.get("value", 0)))[:10]:
                ticker = r.get("ticker", "?")
                score = float(r.get("value", 0))
                raw = r.get("raw_data", {}) or {}
                si_pct = raw.get("short_interest_pct", "?")
                lines.append(f"    {ticker:8s}  squeeze_score={score:.1f}  SI%={si_pct}")

        new_patterns = [r for r in results if "pattern" in r.get("data_type", "").lower()]
        if new_patterns:
            lines.append(f"\n  NEW SHORT PATTERNS ({len(new_patterns)}):")
            for r in new_patterns[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  {r.get('data_type','?')}")

        lines.append("")
        return lines

    def _section_transcripts(self, results: list) -> list:
        """Transcripts analysed, tone scores, deflection alerts, guidance."""
        lines = [
            "─" * 72,
            "  3. TRANSCRIPT ANALYSIS",
            "─" * 72,
        ]
        if not results:
            lines.append("  No transcript data available.")
            lines.append("")
            return lines

        deflection = [r for r in results if "deflect" in r.get("data_type", "").lower()]
        guidance = [r for r in results if "guidance" in r.get("data_type", "").lower()]
        tone = [r for r in results if "tone" in r.get("data_type", "").lower() or "sentiment" in r.get("data_type", "").lower()]
        other = [r for r in results if r not in deflection + guidance + tone]

        lines.append(f"  Transcripts processed: {len(results)}")

        if deflection:
            lines.append(f"\n  DEFLECTION ALERTS ({len(deflection)}):")
            for r in deflection[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  score={r.get('value',0):.2f}  "
                    f"topic={raw.get('topic', '?')}"
                )

        if tone:
            lines.append(f"\n  TONE ANALYSIS:")
            for r in sorted(tone, key=lambda x: abs(float(x.get("value", 0))), reverse=True)[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  tone_score={r.get('value',0):.3f}")

        if guidance:
            lines.append(f"\n  GUIDANCE SIGNALS ({len(guidance)}):")
            for r in guidance[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  val={r.get('value',0):.2f}  "
                    f"direction={raw.get('direction','?')}"
                )

        lines.append("")
        return lines

    def _section_congressional(self, results: list) -> list:
        """New disclosures, cluster detections, weekly leaderboard."""
        lines = [
            "─" * 72,
            "  4. CONGRESSIONAL ACTIVITY",
            "─" * 72,
        ]
        if not results:
            lines.append("  No congressional data available.")
            lines.append("")
            return lines

        clusters = [r for r in results if "cluster" in r.get("data_type", "").lower()]
        singles = [r for r in results if r not in clusters]
        lines.append(f"  New disclosures: {len(results)}  |  Clusters: {len(clusters)}")

        if clusters:
            lines.append(f"\n  CLUSTER DETECTIONS:")
            for r in clusters[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  members={raw.get('member_count','?')}  "
                    f"credibility={raw.get('avg_credibility','?')}"
                )

        if singles:
            lines.append(f"\n  INDIVIDUAL DISCLOSURES (top 5 by value):")
            for r in sorted(singles, key=lambda x: -float(x.get("value", 0)))[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  val={r.get('value',0):.2f}  "
                    f"member={raw.get('member_name','?')}"
                )

        lines.append("")
        return lines

    def _section_supply_chain(self, results: list) -> list:
        """Active readthroughs, supply chain risk changes."""
        lines = [
            "─" * 72,
            "  5. SUPPLY CHAIN RADAR",
            "─" * 72,
        ]
        if not results:
            lines.append("  No supply chain data available.")
            lines.append("")
            return lines

        readthroughs = [r for r in results if "readthrough" in r.get("data_type", "").lower()]
        risk_changes = [r for r in results if "risk" in r.get("data_type", "").lower()]

        lines.append(f"  Total signals: {len(results)}")

        if readthroughs:
            lines.append(f"\n  ACTIVE READTHROUGHS ({len(readthroughs)}):")
            for r in readthroughs[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  source={raw.get('source_ticker','?')}  "
                    f"impact={r.get('value',0):.3f}"
                )

        if risk_changes:
            lines.append(f"\n  SUPPLY CHAIN RISK CHANGES ({len(risk_changes)}):")
            for r in sorted(risk_changes, key=lambda x: abs(float(x.get("value", 0))), reverse=True)[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  risk_delta={r.get('value',0):.3f}")

        lines.append("")
        return lines

    def _section_patents(self, results: list) -> list:
        """Filing velocity changes, citation spikes, competitor overlap."""
        lines = [
            "─" * 72,
            "  6. PATENT SIGNALS",
            "─" * 72,
        ]
        if not results:
            lines.append("  No patent data available.")
            lines.append("")
            return lines

        velocity = [r for r in results if "velocity" in r.get("data_type", "").lower()]
        citations = [r for r in results if "citation" in r.get("data_type", "").lower()]
        overlap = [r for r in results if "overlap" in r.get("data_type", "").lower()]
        other = [r for r in results if r not in velocity + citations + overlap]

        lines.append(f"  Total patent signals: {len(results)}")

        if velocity:
            lines.append(f"\n  FILING VELOCITY CHANGES:")
            for r in sorted(velocity, key=lambda x: abs(float(x.get("value", 0))), reverse=True)[:5]:
                raw = r.get("raw_data", {}) or {}
                lines.append(
                    f"    {r.get('ticker','?'):8s}  velocity_z={r.get('value',0):.2f}  "
                    f"filed={raw.get('recent_filings','?')}"
                )

        if citations:
            lines.append(f"\n  CITATION SPIKES:")
            for r in citations[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  citation_score={r.get('value',0):.2f}")

        if overlap:
            lines.append(f"\n  COMPETITOR PATENT OVERLAP:")
            for r in overlap[:5]:
                lines.append(f"    {r.get('ticker','?'):8s}  overlap={r.get('value',0):.2f}")

        lines.append("")
        return lines

    def _section_patterns(self, results: list) -> list:
        """Active nonsense patterns, pattern performance this week."""
        lines = [
            "─" * 72,
            "  7. CROSS-MODULE PATTERNS",
            "─" * 72,
        ]
        if not results:
            lines.append("  No pattern data available.")
            lines.append("")
            return lines

        active = [r for r in results if r.get("quality_score", 0) > 0.5]
        lines.append(f"  Active patterns: {len(active)}  |  Total: {len(results)}")

        if active:
            lines.append(f"\n  TOP ACTIVE PATTERNS:")
            for r in sorted(active, key=lambda x: -float(x.get("value", 0)))[:8]:
                raw = r.get("raw_data", {}) or {}
                passed = raw.get("passed", False)
                sharpe = raw.get("sharpe", 0.0)
                perm_pct = raw.get("permutation_pct", 0.0)
                dsr = raw.get("dsr", 0.0)
                ns_score = raw.get("nonsense_score", 0.0)
                name = raw.get("name", r.get("data_type", "?"))
                lines.append(
                    f"    {r.get('ticker','ALL'):8s}  {name[:30]:30s}  "
                    f"SR={sharpe:.2f}  perm={perm_pct:.2f}  DSR={dsr:.2f}  "
                    f"nonsense={ns_score:.1f}"
                )

        lines.append("")
        return lines

    def _section_factors(self, results: list) -> list:
        """Portfolio factor tilt, unexpected factor concentration."""
        lines = [
            "─" * 72,
            "  8. FACTOR EXPOSURES",
            "─" * 72,
        ]
        if not results:
            lines.append("  No factor data available.")
            lines.append("")
            return lines

        # Aggregate factor exposures across all results
        factor_values = {}
        for r in results:
            raw = r.get("raw_data", {}) or {}
            factor_combo = raw.get("factor_combo", {})
            for fname, fval in factor_combo.items():
                if fname not in factor_values:
                    factor_values[fname] = []
                factor_values[fname].append(float(fval))

        lines.append(f"  Factor signals: {len(results)}")

        if factor_values:
            lines.append(f"\n  AVERAGE FACTOR LOADINGS (portfolio):")
            import numpy as np
            for fname, vals in sorted(factor_values.items()):
                avg = float(np.mean(vals))
                lines.append(f"    {fname:25s}  avg_loading={avg:+.3f}")

        # Mispricings
        mispricings = [r for r in results if r.get("data_type") == "factor_mispricing"]
        if mispricings:
            lines.append(f"\n  FACTOR MISPRICINGS ({len(mispricings)}):")
            for r in sorted(mispricings, key=lambda x: -float(x.get("value", 0)))[:8]:
                raw = r.get("raw_data", {}) or {}
                reason = raw.get("reason", "?")
                lines.append(
                    f"    {r.get('ticker','?'):8s}  {reason:40s}  "
                    f"strength={r.get('value',0):.3f}"
                )

        lines.append("")
        return lines

    # ------------------------------------------------------------------
    # Terminal output
    # ------------------------------------------------------------------

    def print_to_terminal(self, text: str) -> None:
        """Print using rich if available, else plain print."""
        try:
            if HAS_RICH and self._console is not None:
                self._console.print(text)
            else:
                print(text)
        except Exception as exc:
            logger.warning("print_to_terminal failed: %s", exc)
            print(text)
