"""
Daily text dashboard for the alt-data pipeline.

Writes output/daily_dashboard.txt and also prints to terminal.

Sections:
  1. Macro Environment          — current regime + FRED indicators
  2. Top Alt Signals            — highest-confidence signals from last 48h
  3. Anomaly Watch              — candidates near promotion threshold
  4. Nonsense Detector Update   — Sunday only: weekly nonsense digest
  5. Model Health               — active model accuracy + recent drift events
  6. Data Source Status         — last collect time + quality per source
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from rich.console import Console
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
    _RICH = True
except ImportError:
    _RICH = False


class AltDataDashboard:
    """Assembles and writes the daily alt-data dashboard."""

    def __init__(self, config: dict):
        cfg = config.get("altdata", {}).get("dashboard", {})
        self._out_path = cfg.get("output_path", "output/daily_dashboard.txt")
        self._store = None
        self._console = Console(record=True) if _RICH else None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def set_store(self, store) -> None:
        """Inject AltDataStore after construction (avoids circular import)."""
        self._store = store

    def render(
        self,
        macro_regime: Optional[dict] = None,
        model_health: Optional[dict] = None,
        source_status: Optional[dict] = None,
    ) -> str:
        """
        Build and save the dashboard. Returns the full text string.

        Args:
            macro_regime:  output of MacroRegimeClassifier.classify()
            model_health:  dict with keys: version, accuracy, sharpe, drift_events
            source_status: {source_name: {"last_collect": iso_ts, "quality": 0–1}}
        """
        now = datetime.now(timezone.utc)
        is_sunday = now.weekday() == 6

        lines: list[str] = []

        lines += self._header(now)
        lines += self._section_macro(macro_regime)
        lines += self._section_signals()
        lines += self._section_anomaly_watch()
        if is_sunday:
            lines += self._section_nonsense_digest()
        lines += self._section_model_health(model_health)
        lines += self._section_source_status(source_status)

        text = "\n".join(lines)

        Path(self._out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(self._out_path, "w") as f:
            f.write(text)
        logger.info(f"Dashboard written to {self._out_path}")
        return text

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------

    def _header(self, now: datetime) -> list[str]:
        stamp = now.strftime("%Y-%m-%d %H:%M UTC")
        return [
            "=" * 72,
            f"  QUANT FUND — ALT DATA DASHBOARD   {stamp}",
            "=" * 72,
            "",
        ]

    def _section_macro(self, macro: Optional[dict]) -> list[str]:
        lines = ["── MACRO ENVIRONMENT ──────────────────────────────────────────────", ""]
        if not macro:
            lines.append("  (no macro data available)")
        else:
            regime = macro.get("regime", "UNKNOWN")
            confidence = macro.get("confidence", 0.0)
            indicators = macro.get("indicators", {})
            lines.append(f"  Regime      : {regime}  (confidence {confidence:.0%})")
            for k, v in indicators.items():
                lines.append(f"  {k:<18}: {v}")
        lines.append("")
        return lines

    def _section_signals(self) -> list[str]:
        lines = ["── TOP ALT SIGNALS (last 48h) ─────────────────────────────────────", ""]
        if not self._store:
            lines.append("  (store not connected)")
            lines.append("")
            return lines

        signals = self._store.get_recent_signals(hours_back=48, limit=10)
        if not signals:
            lines.append("  No signals in the last 48 hours.")
        else:
            header = f"  {'Ticker':<8} {'Type':<20} {'Dir':<6} {'Conf':>6} {'Conf%':>7}  Sources"
            lines.append(header)
            lines.append("  " + "-" * 68)
            for s in signals:
                dir_str = "LONG " if s["direction"] > 0 else "SHORT"
                sources = s.get("sources") or []
                if isinstance(sources, str):
                    try:
                        sources = json.loads(sources)
                    except Exception:
                        sources = [sources]
                src_str = ", ".join(sources[:3])
                lines.append(
                    f"  {s['ticker']:<8} {s['signal_type']:<20} {dir_str:<6}"
                    f" {s['confidence']:>6.3f}"
                    f"  {src_str}"
                )
        lines.append("")
        return lines

    def _section_anomaly_watch(self) -> list[str]:
        lines = ["── ANOMALY WATCH ──────────────────────────────────────────────────", ""]
        if not self._store:
            lines.append("  (store not connected)")
            lines.append("")
            return lines

        candidates = self._store.get_anomaly_candidates(status="candidate")
        if not candidates:
            lines.append("  No active anomaly candidates.")
        else:
            lines.append(f"  {'Name':<30} {'Sharpe':>8} {'Nonsense':>9}  Status")
            lines.append("  " + "-" * 60)
            for c in candidates[:10]:
                lines.append(
                    f"  {c['name']:<30} {c['sharpe']:>8.2f}"
                    f" {(c.get('nonsense_score') or 0):>9.3f}  {c['status']}"
                )
        lines.append("")
        return lines

    def _section_nonsense_digest(self) -> list[str]:
        lines = [
            "── NONSENSE DETECTOR — WEEKLY DIGEST (Sunday) ────────────────────",
            "",
        ]
        if not self._store:
            lines.append("  (store not connected)")
            lines.append("")
            return lines

        candidates = self._store.get_anomaly_candidates()
        live = [c for c in candidates if c["status"] == "live"]
        rejected = [c for c in candidates if c["status"] == "rejected"]
        new_this_week = [c for c in candidates if c["status"] == "candidate"]

        lines.append(f"  Live anomalies     : {len(live)}")
        lines.append(f"  Rejected this cycle: {len(rejected)}")
        lines.append(f"  Pending review     : {len(new_this_week)}")
        if live:
            lines.append("")
            lines.append("  Active anomalies:")
            for c in live[:5]:
                lines.append(f"    • {c['name']}  sharpe={c['sharpe']:.2f}")
        lines.append("")
        return lines

    def _section_model_health(self, health: Optional[dict]) -> list[str]:
        lines = ["── MODEL HEALTH ───────────────────────────────────────────────────", ""]
        if not health:
            if self._store:
                model = self._store.get_active_model()
                if model:
                    health = {
                        "version": model.get("version"),
                        "accuracy": model.get("accuracy"),
                        "sharpe": model.get("sharpe"),
                        "drift_events": 0,
                    }
        if not health:
            lines.append("  (no model data available)")
        else:
            lines.append(f"  Active version : {health.get('version', 'n/a')}")
            acc = health.get("accuracy")
            shr = health.get("sharpe")
            lines.append(f"  Accuracy       : {acc:.1%}" if acc is not None else "  Accuracy       : n/a")
            lines.append(f"  Sharpe         : {shr:.2f}" if shr is not None else "  Sharpe         : n/a")
            lines.append(f"  Drift events   : {health.get('drift_events', 0)}")
        lines.append("")
        return lines

    def _section_source_status(self, status: Optional[dict]) -> list[str]:
        lines = ["── DATA SOURCE STATUS ─────────────────────────────────────────────", ""]
        if not status:
            lines.append("  (no source status data)")
            lines.append("")
            return lines

        lines.append(f"  {'Source':<25} {'Last Collect':<22} {'Quality':>8}")
        lines.append("  " + "-" * 58)
        for src, info in sorted(status.items()):
            last = info.get("last_collect", "never")
            qual = info.get("quality", 0.0)
            qual_str = f"{qual:.0%}" if isinstance(qual, float) else str(qual)
            flag = "  OK" if isinstance(qual, float) and qual >= 0.8 else " LOW"
            lines.append(f"  {src:<25} {last:<22} {qual_str:>8}{flag}")
        lines.append("")
        return lines

    # ------------------------------------------------------------------
    # Print to terminal
    # ------------------------------------------------------------------

    def print_to_terminal(self, text: str) -> None:
        if _RICH and self._console:
            # Re-render with rich formatting
            console = Console()
            for line in text.splitlines():
                if line.startswith("=") or line.startswith("──"):
                    console.rule(line.strip("─= "), style="bold cyan")
                else:
                    console.print(line)
        else:
            print(text)
