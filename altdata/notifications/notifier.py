"""
Notification dispatcher for the alt-data pipeline.

Three channels:
  terminal  — ANSI-coloured output via `rich`
  log       — structured append to logs/alerts.log
  desktop   — system notification via `plyer` (optional)

Trigger types:
  new_signal         — a new alt-data signal was generated
  pead_abort         — a PEAD signal was aborted due to adverse alt-data
  nonsense_candidate — a high-nonsense-score anomaly was found
  model_rollback     — model rolled back to prior version
  drawdown_halt      — drawdown halt triggered
  unusual_activity   — unusual volume / sentiment spike detected
  weekly_summary     — end-of-week performance digest
  source_failure     — data source failed or returned poor quality data
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import urllib.request
import urllib.parse

logger = logging.getLogger(__name__)

# Attempt optional imports
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text
    _RICH = True
except ImportError:
    _RICH = False

try:
    from plyer import notification as _plyer_notify
    _PLYER = True
except ImportError:
    _PLYER = False


_LEVEL_COLOURS = {
    "INFO":     "green",
    "WARNING":  "yellow",
    "ALERT":    "bold red",
    "CRITICAL": "bold white on red",
}

_LEVEL_ICONS = {
    "INFO":     "[i]",
    "WARNING":  "[!]",
    "ALERT":    "[!!]",
    "CRITICAL": "[!!!]",
}


class Notifier:
    """
    Sends notifications through configured channels.

    Usage:
        notifier = Notifier(config)
        notifier.send("new_signal", "AAPL long | confidence 0.82", level="INFO")
        notifier.new_signal("AAPL", direction=1, confidence=0.82, sources=["reddit","news"])
    """

    def __init__(self, config: dict):
        cfg = config.get("altdata", {}).get("notifications", {})
        self.channels = cfg.get("channels", ["terminal", "log"])
        log_path = cfg.get("log_path", "logs/alerts.log")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        self._log_path = log_path
        self._console = Console() if _RICH else None
        self._min_level = cfg.get("min_level", "INFO")
        self._level_order = ["INFO", "WARNING", "ALERT", "CRITICAL"]

        # Telegram — read from top-level notifications section or altdata sub-section
        tg_top = config.get("notifications", {}).get("telegram", {})
        tg_alt = cfg.get("telegram", {})
        tg_cfg = tg_top if tg_top.get("enabled") else tg_alt
        self._tg_enabled   = bool(tg_cfg.get("enabled", False))
        self._tg_bot_token = tg_cfg.get("bot_token", "")
        self._tg_chat_id   = str(tg_cfg.get("chat_id", ""))
        if self._tg_enabled and self._tg_bot_token and self._tg_chat_id:
            self.channels = list(set(self.channels) | {"telegram"})

    # ------------------------------------------------------------------
    # Core dispatch
    # ------------------------------------------------------------------

    def send(
        self,
        trigger: str,
        message: str,
        level: str = "INFO",
        details: Optional[dict] = None,
    ) -> None:
        if not self._should_send(level):
            return

        ts = datetime.now(timezone.utc).isoformat()
        title = f"{trigger.upper().replace('_', ' ')}"
        body = message

        if "terminal" in self.channels:
            self._terminal(level, title, body, details)
        if "log" in self.channels:
            self._log(ts, level, trigger, title, body, details)
        if "desktop" in self.channels:
            self._desktop(level, title, body)
        if "telegram" in self.channels:
            self._telegram(level, title, body, details)

    # ------------------------------------------------------------------
    # Convenience methods for each trigger type
    # ------------------------------------------------------------------

    def new_signal(
        self,
        ticker: str,
        direction: int,
        confidence: float,
        sources: Optional[list] = None,
        confluence: Optional[float] = None,
    ) -> None:
        dir_str = "LONG" if direction > 0 else "SHORT"
        src_str = ", ".join(sources) if sources else "n/a"
        msg = (
            f"{ticker} {dir_str} | confidence={confidence:.2f}"
            + (f" | confluence={confluence:.2f}" if confluence else "")
            + f" | sources=[{src_str}]"
        )
        self.send("new_signal", msg, level="INFO", details={"ticker": ticker, "direction": direction})

    def pead_abort(self, ticker: str, reason: str) -> None:
        self.send(
            "pead_abort",
            f"{ticker} — PEAD signal aborted: {reason}",
            level="WARNING",
            details={"ticker": ticker, "reason": reason},
        )

    def nonsense_candidate(self, name: str, nonsense_score: float, sharpe: float) -> None:
        self.send(
            "nonsense_candidate",
            f"{name} | nonsense_score={nonsense_score:.3f} | sharpe={sharpe:.2f}",
            level="INFO",
            details={"name": name, "nonsense_score": nonsense_score, "sharpe": sharpe},
        )

    def model_rollback(self, from_version: str, to_version: str, reason: str) -> None:
        self.send(
            "model_rollback",
            f"Rolled back {from_version} → {to_version}: {reason}",
            level="ALERT",
            details={"from": from_version, "to": to_version, "reason": reason},
        )

    def drawdown_halt(self, current_dd: float, limit: float) -> None:
        self.send(
            "drawdown_halt",
            f"Trading halted — drawdown {current_dd:.1%} exceeds limit {limit:.1%}",
            level="CRITICAL",
            details={"drawdown": current_dd, "limit": limit},
        )

    def unusual_activity(self, ticker: str, metric: str, value: float, z_score: float) -> None:
        self.send(
            "unusual_activity",
            f"{ticker} — {metric}={value:.3f} (z={z_score:.1f}σ)",
            level="WARNING" if abs(z_score) < 4 else "ALERT",
            details={"ticker": ticker, "metric": metric, "value": value, "z_score": z_score},
        )

    def weekly_summary(self, stats: dict) -> None:
        lines = [f"{k}: {v}" for k, v in stats.items()]
        self.send(
            "weekly_summary",
            " | ".join(lines),
            level="INFO",
            details=stats,
        )

    def source_failure(self, source: str, error: str) -> None:
        self.send(
            "source_failure",
            f"{source} — {error}",
            level="WARNING",
            details={"source": source, "error": error},
        )

    # ------------------------------------------------------------------
    # Channel implementations
    # ------------------------------------------------------------------

    def _terminal(self, level: str, title: str, body: str, details: Optional[dict]) -> None:
        if _RICH and self._console:
            colour = _LEVEL_COLOURS.get(level, "white")
            icon = _LEVEL_ICONS.get(level, "")
            text = Text()
            text.append(f"{icon} {title}: ", style=colour)
            text.append(body)
            if details:
                text.append(f"\n  {json.dumps(details)}", style="dim")
            self._console.print(text)
        else:
            icon = _LEVEL_ICONS.get(level, "")
            print(f"{icon} [{level}] {title}: {body}")

    def _log(
        self,
        ts: str,
        level: str,
        trigger: str,
        title: str,
        body: str,
        details: Optional[dict],
    ) -> None:
        record = {
            "ts": ts,
            "level": level,
            "trigger": trigger,
            "title": title,
            "body": body,
        }
        if details:
            record["details"] = details
        try:
            with open(self._log_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        except OSError as e:
            logger.warning(f"Notifier: could not write to {self._log_path}: {e}")

    def _telegram(
        self,
        level: str,
        title: str,
        body: str,
        details: Optional[dict],
    ) -> None:
        """Send a message to a Telegram chat via the Bot API (no third-party deps)."""
        if not (self._tg_bot_token and self._tg_chat_id):
            return
        icon = _LEVEL_ICONS.get(level, "")
        text = f"{icon} *{title}*\n{body}"
        if details:
            # Compact JSON snippet — trim to keep messages readable
            detail_str = json.dumps(details, default=str)
            if len(detail_str) > 300:
                detail_str = detail_str[:297] + "..."
            text += f"\n`{detail_str}`"
        try:
            url = (
                f"https://api.telegram.org/bot{self._tg_bot_token}/sendMessage"
            )
            payload = urllib.parse.urlencode({
                "chat_id":    self._tg_chat_id,
                "text":       text,
                "parse_mode": "Markdown",
            }).encode()
            req = urllib.request.Request(url, data=payload, method="POST")
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            logger.debug("Telegram notification failed: %s", e)

    def _desktop(self, level: str, title: str, body: str) -> None:
        if not _PLYER:
            return
        try:
            _plyer_notify.notify(
                title=f"[{level}] {title}",
                message=body[:200],
                app_name="QuantFund",
                timeout=10,
            )
        except Exception as e:
            logger.debug(f"Desktop notification failed: {e}")

    def _send_telegram(self, message: str) -> bool:
        """Send an arbitrary message via Telegram Bot API (HTML parse mode)."""
        if not (self._tg_enabled and self._tg_bot_token and self._tg_chat_id):
            return False
        try:
            import json as _json
            url = f"https://api.telegram.org/bot{self._tg_bot_token}/sendMessage"
            data = _json.dumps({
                "chat_id": self._tg_chat_id,
                "text": message,
                "parse_mode": "HTML",
            }).encode()
            req = urllib.request.Request(
                url, data=data,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception as exc:
            logger.warning("Telegram send failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Formatted Telegram convenience methods
    # ------------------------------------------------------------------

    def send_signal_telegram(
        self,
        ticker: str,
        direction: int,
        surprise_pct: float,
        confidence: float,
        size_pct: float,
        macro_regime: str,
        sector: str,
        mfs: float,
    ) -> bool:
        """Send a formatted PEAD/signal alert to Telegram."""
        icon = "🟢" if direction > 0 else "🔴"
        dir_text = "LONG" if direction > 0 else "SHORT"
        msg = (
            f"{icon} <b>{dir_text} SIGNAL — {ticker}</b>\n"
            f"Surprise: {surprise_pct:+.1f}% | Confidence: {confidence:.2f}\n"
            f"Size: {size_pct:.0f}% (obs mode) | MFS: {mfs:.2f}\n"
            f"Macro: {macro_regime} | Sector: {sector}\n"
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return self._send_telegram(msg)

    def send_trade_close_telegram(
        self,
        ticker: str,
        return_pct: float,
        holding_days: int,
        pnl_paper: float,
    ) -> bool:
        """Send a formatted trade-close notification to Telegram."""
        icon = "✅" if return_pct > 0 else "❌"
        msg = (
            f"{icon} <b>TRADE CLOSED — {ticker}</b>\n"
            f"Return: {return_pct:+.1f}% in {holding_days} days\n"
            f"Paper P&amp;L: {pnl_paper:+.0f}\n"
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return self._send_telegram(msg)

    def send_daily_summary_telegram(
        self,
        regime: str,
        vix: float,
        n_open: int,
        daily_pnl: float,
        n_signals: int,
    ) -> bool:
        """Send a daily portfolio summary to Telegram."""
        msg = (
            f"📊 <b>DAILY SUMMARY — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}</b>\n"
            f"Regime: {regime} | VIX: {vix:.1f}\n"
            f"Open positions: {n_open} | P&amp;L today: {daily_pnl:+.0f}\n"
            f"Signals today: {n_signals}"
        )
        return self._send_telegram(msg)

    def send_alert_telegram(
        self,
        alert_type: str,
        details: str,
        action: str = "",
    ) -> bool:
        """Send an alert message to Telegram."""
        msg = (
            f"⚠️ <b>ALERT — {alert_type}</b>\n"
            f"{details}\n"
            f"{'Action: ' + action if action else ''}"
        )
        return self._send_telegram(msg)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _should_send(self, level: str) -> bool:
        try:
            return self._level_order.index(level) >= self._level_order.index(self._min_level)
        except ValueError:
            return True
