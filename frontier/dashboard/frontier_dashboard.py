"""
Frontier Intelligence Dashboard.

Renders a full plain-text status panel covering:
  - UMCI score + breakdown
  - Active frontier signals
  - Watchlist candidates
  - Cross-signal interactions
  - Parameter drift summary
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path("output/frontier_dashboard.txt")


def _bar(score: float, width: int = 20) -> str:
    filled = int(max(0.0, min(1.0, score)) * width)
    return "█" * filled + "░" * (width - filled)


class FrontierDashboard:

    def __init__(self, store=None, registry=None, config: Optional[Dict] = None):
        self._store = store
        self._registry = registry
        self._config = config or {}

    def _get_umci(self) -> Dict:
        try:
            from frontier.signals.frontier_signal_engine import FrontierSignalEngine
            engine = FrontierSignalEngine(store=self._store, config=self._config)
            snapshot = engine.get_umci_snapshot()
            return snapshot.get("breakdown", {}), snapshot.get("umci", 0.0), snapshot.get("signals", {})
        except Exception as e:
            logger.debug(f"UMCI fetch failed: {e}")
            return {}, 0.0, {}

    def render(self) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = []
        W = 65

        def hr(char="="):
            lines.append(char * W)

        def section(title):
            lines.append("-" * W)
            lines.append(f"  {title}")
            lines.append("-" * W)

        hr()
        lines.append(f"  FRONTIER INTELLIGENCE DASHBOARD   {ts}")
        hr()

        # UMCI block
        breakdown, umci, signals = self._get_umci()
        if breakdown:
            level = breakdown.get("level", "?")
            pos_mult = breakdown.get("position_multiplier", 1.0)
            strategy = breakdown.get("preferred_strategy", "?")
            msg = breakdown.get("message", "")
            frontier_active = breakdown.get("frontier_signals_active", True)

            lines.append(f"\n  UMCI: {umci:.1f}/100  [{level}]  |  Position: {pos_mult:.0%} of normal")
            lines.append(f"  Strategy : {strategy.upper()}")
            lines.append(f"  Frontier : {'ACTIVE' if frontier_active else 'SUPPRESSED'}")
            lines.append(f"  {msg}")
            lines.append("")

            dims = breakdown.get("dimensions", {})
            if dims:
                lines.append("  Dimension Breakdown:")
                for dim, score in dims.items():
                    lines.append(f"    {dim:<12} {score:.3f}  {_bar(score)}")
        else:
            lines.append(f"\n  UMCI: not yet computed (run 'frontier umci')")

        lines.append("")

        # Active signals
        section(f"ACTIVE FRONTIER SIGNALS")
        try:
            from frontier.signals.frontier_signal_engine import FrontierSignalEngine
            engine = FrontierSignalEngine(store=self._store, config=self._config)
            cfg = self._config.get("frontier", {})
            tickers = cfg.get("default_tickers", ["AAPL", "MSFT", "AMZN", "GOOGL", "META"])
            sigs = engine.generate(tickers)
            if sigs:
                lines.append(f"  {'Ticker':<8} {'Dir':<5} {'Conf':<6} {'Tier':<6} {'UMCI':<6} Signal")
                for s in sigs:
                    d = "LONG" if s["direction"] > 0 else ("SHORT" if s["direction"] < 0 else "FLAT")
                    lines.append(
                        f"  {s['ticker']:<8} {d:<5} {s['confidence']:.2f}  "
                        f"T{s['evidence_tier']:<5} {s['umci']:<6.1f} {s['signal_name']}"
                    )
                    if s.get("reason"):
                        lines.append(f"           ↳ {s['reason']}")
            else:
                lines.append("  No frontier signals generated.")
        except Exception as e:
            lines.append(f"  Signal engine error: {e}")

        lines.append("")

        # Watchlist
        section("WATCHLIST CANDIDATES")
        try:
            if self._registry:
                watchlist = self._registry.get_watchlist(limit=10)
            else:
                from frontier.meta_learning.discovery_registry import DiscoveryRegistry
                watchlist = DiscoveryRegistry().get_watchlist(limit=10)

            if watchlist:
                lines.append(f"  {'Signal':<32} {'Status':<12} {'Grade':<6} {'Tier':<5} {'Nonsense'}")
                for w in watchlist:
                    lines.append(
                        f"  {w['signal_name']:<32} {w['status']:<12} "
                        f"{w.get('evidence_grade','?'):<6} "
                        f"T{w.get('evidence_tier',4):<4} "
                        f"{w.get('nonsense_score',0):.2f}"
                    )
            else:
                lines.append("  Watchlist empty — run 'frontier discover' to populate.")
        except Exception as e:
            lines.append(f"  Watchlist error: {e}")

        lines.append("")

        # Cross-signal interactions
        section("CROSS-SIGNAL INTERACTIONS  (top 5 by |value|)")
        try:
            from frontier.equations.cross_signal_interactions import get_all_interactions
            if signals:
                interactions = get_all_interactions(signals)
                sorted_ix = sorted(interactions.items(), key=lambda x: abs(x[1]), reverse=True)
                for name, val in sorted_ix[:5]:
                    lines.append(f"  {name:<40} {val:+.6f}")
            else:
                lines.append("  No signal data available.")
        except Exception as e:
            lines.append(f"  Interactions error: {e}")

        lines.append("")

        # Parameter drift
        section("PARAMETER DRIFT SUMMARY")
        try:
            from frontier.meta_learning.parameter_drifter import ParameterDrifter
            drifter = ParameterDrifter()
            summary = drifter.summary()
            if summary:
                lines.append(f"  {'Parameter':<35} {'Published':>10} {'Current':>10} {'Drift':>8}")
                for param, data in summary.items():
                    pub = data.get("published", 0)
                    cur = data.get("current", pub)
                    drift = cur - pub
                    lines.append(f"  {param:<35} {pub:>10.4f} {cur:>10.4f} {drift:>+8.4f}")
        except Exception as e:
            lines.append(f"  Parameter drifter error: {e}")

        lines.append("")
        hr()

        text = "\n".join(lines)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(text)

        # Rich output if available
        try:
            from rich.console import Console
            from rich.text import Text
            console = Console()
            console.print(text)
        except ImportError:
            print(text)

        return text
