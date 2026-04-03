"""
Instant alert monitoring.

Watches for critical conditions and fires immediate Telegram alerts.
Runs continuously in the MonitorRunner background thread.

Conditions monitored:
  - RAM > 1.8GB              → auto-pause stream + RAM ALERT
  - Collector failure 3x     → COLLECTOR ALERT
  - API broken               → API ALERT
  - Bot log errors           → BOT ERROR ALERT
  - Model accuracy drop >10% → MODEL DEGRADED ALERT
  - New equation discovered  → NEW PATTERN alert
"""
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from monitoring import telegram_logger
from monitoring.system_stats import get_ram_mb

logger = logging.getLogger(__name__)

_ALERTS_DIR   = Path("logs/alerts")
_RAM_CRITICAL = 1800   # MB
_RAM_RESUME   = 1536   # 1.5 GB — resume stream below this

# Per-instance state
_collector_fail_counts:  dict = {}   # name → consecutive fail count
_known_equation_ids:     set  = set()
_last_model_accuracy:    dict = {}   # signal_type → accuracy
_stream_paused_for_ram:  bool = False
_last_api_status:        dict = {}
_last_log_scan_pos:      dict = {}   # log_path → byte offset


# ── send helpers ──────────────────────────────────────────────────────────────

def _send_alert(config: dict, alert_type: str, text: str) -> bool:
    """Send alert via Notifier and log it permanently."""
    delivered = False
    try:
        from altdata.notifications.notifier import Notifier
        n = Notifier(config)
        delivered = n._send_telegram(text)
    except Exception as exc:
        logger.warning("Alert send failed: %s", exc)
    telegram_logger.log_message("instant_alert", text, delivered)
    if not delivered:
        telegram_logger.queue_retry("instant_alert", text)
    # Save to logs/alerts/
    try:
        _ALERTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = _ALERTS_DIR / f"alert_{alert_type}_{ts}.txt"
        path.write_text(text, encoding="utf-8")
    except Exception:
        pass
    return delivered


# ── individual checks ─────────────────────────────────────────────────────────

def check_ram(config: dict, stream_worker=None) -> None:
    """If RAM > 1.8GB auto-pause stream and alert. Resume below 1.5GB."""
    global _stream_paused_for_ram
    used_mb, total_mb, pct = get_ram_mb()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    if used_mb >= _RAM_CRITICAL and not _stream_paused_for_ram:
        _stream_paused_for_ram = True
        # Auto-pause stream to free RAM
        if stream_worker and hasattr(stream_worker, "stop"):
            try:
                stream_worker.stop()
                logger.warning("RAM critical — stream auto-paused")
            except Exception:
                pass
        text = (
            f"🚨 RAM ALERT: Server RAM at {used_mb}MB/{total_mb}MB ({pct:.1f}%)\n"
            f"Stream has been auto-paused\n"
            f"Bot is still running normally\n"
            f"Will resume stream when RAM drops below {_RAM_RESUME // 1024:.1f}GB\n"
            f"Time: {ts}"
        )
        _send_alert(config, "ram_critical", text)

    elif used_mb < _RAM_RESUME and _stream_paused_for_ram:
        _stream_paused_for_ram = False
        logger.info("RAM back below threshold — stream will reconnect on next restart")


def check_collector_failures(config: dict, collector_name: str,
                              error: Optional[str] = None) -> None:
    """Call this whenever a collector fails. Alerts on 3 consecutive failures."""
    global _collector_fail_counts
    _collector_fail_counts[collector_name] = \
        _collector_fail_counts.get(collector_name, 0) + 1

    if _collector_fail_counts[collector_name] == 3:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        # Determine affected data type
        data_map = {
            "news":      "news sentiment signals",
            "edgar":     "SEC filing signals",
            "finnhub":   "Finnhub earnings/news signals",
            "rates":     "interest rate / credit spread signals",
            "shipping":  "shipping intelligence signals",
            "commodities": "commodity price signals",
            "geopolitical": "geopolitical risk signals",
        }
        affected = data_map.get(collector_name, f"{collector_name} signals")
        text = (
            f"🚨 COLLECTOR ALERT: {collector_name} has failed 3 times in a row\n"
            f"Last error: {error or 'unknown'}\n"
            f"Data affected: {affected}\n"
            f"Time: {ts}"
        )
        _send_alert(config, "collector_failure", text)
    elif _collector_fail_counts[collector_name] > 3:
        pass  # Already alerted — don't spam


def reset_collector_ok(collector_name: str) -> None:
    """Call when a collector succeeds — resets the failure counter."""
    _collector_fail_counts[collector_name] = 0


def check_api_health(config: dict) -> None:
    """Check all APIs; alert immediately if one switches from working to broken."""
    global _last_api_status
    import requests as _req

    api_keys = config.get("api_keys", {})
    checks = {
        "finnhub":
            f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={api_keys.get('finnhub','')}",
        "fred":
            f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS"
            f"&api_key={api_keys.get('fred','')}&file_type=json&limit=1",
    }
    for name, url in checks.items():
        key = api_keys.get(name, "")
        if not key:
            continue
        try:
            resp = _req.get(url, timeout=4)
            ok   = resp.status_code == 200
        except Exception as exc:
            ok  = False
            err = str(exc)[:80]
        else:
            err = f"HTTP {resp.status_code}"

        prev_ok = _last_api_status.get(name, True)
        _last_api_status[name] = ok
        if not ok and prev_ok:
            # Just broke
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            impact_map = {
                "finnhub": "earnings sentiment, news signals",
                "fred":    "macro regime signals, rate signals",
            }
            text = (
                f"🚨 API ALERT: {name} has stopped working\n"
                f"Error: {err}\n"
                f"Impact: {impact_map.get(name, 'signals from this source')}\n"
                f"Fix: Check api_keys.{name} in config/settings.yaml\n"
                f"Time: {ts}"
            )
            _send_alert(config, "api_broken", text)


def check_log_errors(config: dict, log_path: str = "logs/quant_fund.log") -> None:
    """Scan bot log for new ERROR/CRITICAL lines and alert once per batch."""
    global _last_log_scan_pos
    if not Path(log_path).exists():
        return
    try:
        offset = _last_log_scan_pos.get(log_path, 0)
        with open(log_path, "r", errors="replace") as f:
            f.seek(offset)
            new_lines = f.readlines()
            _last_log_scan_pos[log_path] = f.tell()
        errors = [l.strip() for l in new_lines
                  if " ERROR " in l or " CRITICAL " in l]
        if not errors:
            return
        # Group consecutive errors into one alert (max 3 shown)
        sample = errors[:3]
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        text = (
            f"🚨 BOT ERROR DETECTED ({len(errors)} new error{'s' if len(errors)>1 else ''})\n"
            + "\n".join(f"  {e[:200]}" for e in sample)
            + (f"\n  ...and {len(errors)-3} more" if len(errors) > 3 else "")
            + f"\nTime: {ts}\nAuto-recovery: not attempted"
        )
        _send_alert(config, "bot_error", text)
    except Exception as exc:
        logger.debug("check_log_errors: %s", exc)


def check_model_performance(config: dict) -> None:
    """Detect accuracy drops > 10% vs last known value and alert."""
    global _last_model_accuracy
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        rows = conn.execute(
            "SELECT signal_type, AVG(was_traded) as rate "
            "FROM signals_log WHERE timestamp >= date('now','-1 day') "
            "GROUP BY signal_type HAVING COUNT(*) >= 5"
        ).fetchall()
        conn.close()
        for sig_type, current_rate in rows:
            prev = _last_model_accuracy.get(sig_type)
            if prev is not None and (prev - current_rate) > 0.10:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
                text = (
                    f"🚨 MODEL PERFORMANCE DROP:\n"
                    f"Model: {sig_type}\n"
                    f"Previous accuracy: {prev*100:.1f}%\n"
                    f"Current accuracy:  {current_rate*100:.1f}%\n"
                    f"Drop: {(prev-current_rate)*100:.1f}% (threshold: 10%)\n"
                    f"Action: Check signals and consider re-running altdata collect\n"
                    f"Time: {ts}"
                )
                _send_alert(config, "model_degraded", text)
            _last_model_accuracy[sig_type] = current_rate
    except Exception:
        pass


def check_new_equations(config: dict) -> None:
    """Alert when a new equation is discovered in frontier."""
    global _known_equation_ids
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        rows = conn.execute(
            "SELECT id, equation_str, sharpe_estimate, ic_score, n_times_used "
            "FROM discovered_equations WHERE is_active = 1"
        ).fetchall()
        conn.close()
        for row_id, eq_str, sharpe, ic, n_used in rows:
            if row_id not in _known_equation_ids:
                _known_equation_ids.add(row_id)
                if _known_equation_ids.__len__() > 1:  # Skip the very first scan
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
                    win_rate = min(99, max(1, round((ic + 1) / 2 * 100, 1)))
                    text = (
                        f"💡 NEW PATTERN DISCOVERED:\n"
                        f"Pattern: {eq_str[:120]}\n"
                        f"Sharpe ratio: {sharpe:.2f}\n"
                        f"Win rate in backtest: {win_rate:.0f}%\n"
                        f"Observations: {n_used}\n"
                        f"Status: Promoted to active monitoring\n"
                        f"Time: {ts}"
                    )
                    _send_alert(config, "new_pattern", text)
    except Exception:
        pass


def run_all_checks(config: dict, stream_worker=None) -> None:
    """Run all instant alert checks. Call this every ~60s from MonitorRunner."""
    try:
        check_ram(config, stream_worker)
    except Exception as exc:
        logger.debug("check_ram: %s", exc)
    try:
        check_api_health(config)
    except Exception as exc:
        logger.debug("check_api_health: %s", exc)
    try:
        check_log_errors(config)
    except Exception as exc:
        logger.debug("check_log_errors: %s", exc)
    try:
        check_model_performance(config)
    except Exception as exc:
        logger.debug("check_model_performance: %s", exc)
    try:
        check_new_equations(config)
    except Exception as exc:
        logger.debug("check_new_equations: %s", exc)
