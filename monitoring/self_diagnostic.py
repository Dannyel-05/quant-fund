"""
6-hour silent self-diagnostic.

Runs all checks silently and only sends a Telegram alert if any fail.
Saves diagnostic files to logs/diagnostics/ permanently.
"""
import importlib
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

from monitoring import telegram_logger
from monitoring.system_stats import (
    get_ram_mb, get_cpu_pct, get_disk_gb,
    is_pm2_running, get_log_last_write,
)

logger = logging.getLogger(__name__)

_DIAG_DIR      = Path("logs/diagnostics")
_RAM_LIMIT_MB  = 1800
_CPU_LIMIT_PCT = 80.0
_DISK_LIMIT_PCT = 80.0
_LOG_STALE_SEC  = 3600    # alert if a key log hasn't been written in 1h

# Checks that are always sent even during quiet hours (22:00-07:00 UTC)
_CRITICAL_CHECK_NAMES = {"All databases accessible", "PM2 process running"}


def _any_market_open() -> bool:
    """Returns True if US or UK market is currently open (UTC hours, weekday check)."""
    try:
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:   # weekend
            return False
        h, m = now.hour, now.minute
        now_mins = h * 60 + m
        us_open = (14 * 60 + 30) <= now_mins < (21 * 60)
        uk_open = (8  * 60     ) <= now_mins < (16 * 60 + 30)
        return us_open or uk_open
    except Exception:
        return True  # assume open if unsure (fail safe)


def _core_logs_ok():
    """Like _logs_being_written but only checks quant_fund.log (market-closed variant)."""
    stale = []
    for log_path in ["logs/quant_fund.log"]:
        age = get_log_last_write(log_path)
        if age > _LOG_STALE_SEC:
            stale.append(f"{log_path} (last write: {age/3600:.1f}h ago)")
    if stale:
        return "Stale logs: " + ", ".join(stale)

_COLLECTOR_CLASSES = [
    ("data.collectors.rates_credit_collector",    "RatesCreditCollector"),
    ("data.collectors.consumer_intelligence",     "ConsumerIntelligence"),
    ("data.collectors.geopolitical_collector",    "GeopoliticalCollector"),
    ("data.collectors.commodity_collector",       "CommodityCollector"),
    ("data.collectors.technology_intelligence",   "TechnologyIntelligence"),
    ("data.collectors.government_data_collector", "BLSCollector"),
    ("data.collectors.government_data_collector", "USASpendingCollector"),
    ("data.collectors.shipping_intelligence",     "ShippingIntelligence"),
    ("data.collectors.sec_fulltext_collector",    "SECFullTextCollector"),
    ("data.collectors.alternative_quiver_collector", "AlternativeQuiverCollector"),
    ("altdata.collector.news_collector",          "NewsCollector"),
    ("altdata.collector.sec_edgar_collector",     "SECEdgarCollector"),
    ("altdata.collector.finnhub_collector",       "FinnhubCollector"),
]

_DBS = [
    "output/permanent_archive.db",
    "closeloop/storage/closeloop.db",
    "output/historical_db.db",
    "output/altdata.db",
]

_KEY_LOGS = [
    "logs/quant_fund.log",
    "logs/alpaca_stream.log",
]


# ── individual checks ─────────────────────────────────────────────────────────

def _check(name: str, fn) -> Tuple[str, bool, str]:
    """Run *fn*, return (name, passed, detail)."""
    try:
        result = fn()
        if result is None or result is True:
            return name, True, "ok"
        return name, False, str(result)
    except Exception as exc:
        return name, False, str(exc)[:200]


def _collectors_ok():
    failed = []
    for mod_path, cls_name in _COLLECTOR_CLASSES:
        try:
            mod = importlib.import_module(mod_path)
            getattr(mod, cls_name)
        except Exception as exc:
            failed.append(f"{cls_name}:{exc!s:.40}")
    if failed:
        return "Failed: " + ", ".join(failed)


def _ram_ok():
    used_mb, total_mb, pct = get_ram_mb()
    if used_mb >= _RAM_LIMIT_MB:
        return f"{used_mb}MB / {total_mb}MB ({pct:.1f}%) — OVER {_RAM_LIMIT_MB}MB limit"


def _cpu_ok():
    cpu = get_cpu_pct()
    if cpu > _CPU_LIMIT_PCT:
        return f"{cpu:.1f}% — OVER {_CPU_LIMIT_PCT}% limit"


def _disk_ok():
    used, total, pct = get_disk_gb()
    if pct > _DISK_LIMIT_PCT:
        return f"{used:.1f}/{total:.0f}GB ({pct:.1f}%) — OVER {_DISK_LIMIT_PCT}% limit"


def _stream_ok():
    try:
        from execution.alpaca_stream import get_stream_cache
        if not get_stream_cache().is_connected():
            return "Alpaca websocket DISCONNECTED"
    except Exception as exc:
        return f"Stream module error: {exc!s:.60}"


def _databases_ok():
    failed = []
    for db in _DBS:
        if not Path(db).exists():
            failed.append(f"{db} MISSING")
            continue
        try:
            conn = sqlite3.connect(db, timeout=3)
            conn.execute("PRAGMA integrity_check").fetchone()
            conn.close()
        except Exception as exc:
            failed.append(f"{db}: {exc!s:.50}")
    if failed:
        return "DB errors: " + "; ".join(failed)


def _pm2_ok():
    if not is_pm2_running():
        return "PM2 process not found — bot may not be managed by PM2"


def _no_recent_errors():
    """Check bot log for ERROR/CRITICAL lines in last 6 hours."""
    log = Path("logs/quant_fund.log")
    if not log.exists():
        return None
    try:
        now = time.time()
        six_hours_ago = now - 21600
        errors = []
        with open(log, "r", errors="replace") as f:
            for line in f:
                if " ERROR " in line or " CRITICAL " in line:
                    errors.append(line.strip()[:120])
        if len(errors) > 20:
            return f"{len(errors)} error lines found in bot log"
    except Exception:
        pass


def _logs_being_written():
    stale = []
    for log_path in _KEY_LOGS:
        age = get_log_last_write(log_path)
        if age > _LOG_STALE_SEC:
            stale.append(f"{log_path} (last write: {age/3600:.1f}h ago)")
    if stale:
        return "Stale logs: " + ", ".join(stale)


def _apis_responding():
    """Quick check — only test one API to keep it lightweight."""
    try:
        import urllib.request
        with urllib.request.urlopen(
            "https://finnhub.io/api/v1/status", timeout=4
        ) as r:
            if r.status >= 500:
                return f"Finnhub server error HTTP {r.status}"
    except Exception as exc:
        return f"API connectivity: {exc!s:.60}"


def _alpaca_api_ok(config: dict):
    """
    Direct Alpaca API health check — GET /v2/account.
    Saves last successful ping to output/alpaca_status.json.
    Returns error string on failure, None on success.
    """
    try:
        import json, requests as _req
        api_keys  = config.get("api_keys", {})
        alpaca_cfg = config.get("alpaca", {})
        key    = api_keys.get("alpaca_api_key", "")
        secret = api_keys.get("alpaca_secret_key", "")
        base   = alpaca_cfg.get("base_url", "https://paper-api.alpaca.markets")
        if not key or "PASTE" in key:
            return None  # not configured — skip
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        resp = _req.get(f"{base}/v2/account", headers=headers, timeout=6)
        if resp.status_code == 200:
            status = {
                "connected": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "equity":    float(resp.json().get("portfolio_value", 0)),
            }
            try:
                Path("output/alpaca_status.json").write_text(
                    json.dumps(status, indent=2)
                )
            except Exception:
                pass
            return None  # OK
        return f"Alpaca API HTTP {resp.status_code}"
    except Exception as exc:
        return f"Alpaca API: {exc!s:.80}"


# ── main entry point ──────────────────────────────────────────────────────────

def run_diagnostic(config: dict, quiet_hours: bool = False) -> dict:
    """
    Run all checks, save to file, and send Telegram alert only if any fail.

    quiet_hours=True  → only CRITICAL alerts (databases, PM2) are sent via Telegram;
                        non-critical failures are saved to file but not notified.
    Market-closed     → skips stream connectivity and alpaca_stream.log staleness.

    Returns {"passed": bool, "results": [(name, ok, detail)], "path": str}.
    """
    _DIAG_DIR.mkdir(parents=True, exist_ok=True)

    mkt_open = _any_market_open()

    check_fns = [
        ("All 13 collectors responding",        _collectors_ok),
        ("RAM under 1.8GB",                     _ram_ok),
        ("CPU under 80%",                       _cpu_ok),
        ("Disk under 80%",                      _disk_ok),
        ("All databases accessible",            _databases_ok),
        ("PM2 process running",                 _pm2_ok),
        ("No excessive errors last 6h",         _no_recent_errors),
        ("External API connectivity",           _apis_responding),
        ("Alpaca API responding",               lambda: _alpaca_api_ok(config)),
    ]

    if mkt_open:
        # Include stream + full log checks only when markets are open
        check_fns.insert(4, ("Websocket stream connected", _stream_ok))
        check_fns.append(("Log files being written", _logs_being_written))
    else:
        # Market closed: only check core bot log (skip alpaca_stream.log)
        check_fns.append(("Core log being written", _core_logs_ok))

    results = [_check(name, fn) for name, fn in check_fns]
    passed_all = all(r[1] for r in results)
    ts_now  = datetime.now()
    ts_str  = ts_now.strftime("%Y-%m-%d %H:%M")

    # ── save diagnostic file ──────────────────────────────────────────────
    report_lines = [
        f"QUANT FUND SELF-DIAGNOSTIC",
        f"Time: {ts_now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Result: {'ALL CHECKS PASSED' if passed_all else 'FAILURES DETECTED'}",
        "",
    ]
    for name, ok, detail in results:
        icon = "✅" if ok else "❌"
        report_lines.append(f"{icon} {name}: {detail if not ok else 'ok'}")

    report_text = "\n".join(report_lines)
    fname = f"diagnostic_{ts_now.strftime('%Y-%m-%d_%H-%M')}.txt"
    path  = _DIAG_DIR / fname
    try:
        path.write_text(report_text, encoding="utf-8")
        logger.debug("Diagnostic saved: %s", path)
    except Exception as exc:
        logger.warning("Could not save diagnostic: %s", exc)

    # ── alert if any check failed ─────────────────────────────────────────
    if not passed_all:
        all_failures = [(n, d) for n, ok, d in results if not ok]
        passing      = [n      for n, ok, d in results if ok]

        # Quiet hours (22:00-07:00 UTC): only send CRITICAL alerts
        if quiet_hours:
            failures = [(n, d) for n, d in all_failures if n in _CRITICAL_CHECK_NAMES]
            if not failures:
                logger.info(
                    "Quiet hours — suppressing %d non-critical diagnostic alert(s): %s",
                    len(all_failures),
                    ", ".join(n for n, _ in all_failures),
                )
                return {"passed": passed_all, "results": results, "path": str(path)}
        else:
            failures = all_failures

        first_name, first_detail = failures[0]
        other_count = len(failures) - 1

        fix_map = {
            "All 13 collectors responding":
                "Run: source venv/bin/activate && pip install -r requirements.txt",
            "RAM under 1.8GB":
                "Restart bot: pm2 restart quant-fund  (or reduce universe size)",
            "CPU under 80%":
                "Check for stuck processes: top -bn1 | head -20",
            "Disk under 80%":
                "Clear old cache files: rm data/cache/*.pkl",
            "Websocket stream connected":
                "Stream auto-reconnects in 30s; if persists check Alpaca API key",
            "All databases accessible":
                "Run: sqlite3 <db> 'PRAGMA integrity_check'",
            "PM2 process running":
                "Start with: pm2 start ecosystem.config.js",
            "No excessive errors last 6h":
                "Check logs/quant_fund.log for the root cause error",
            "Log files being written":
                "Ensure the bot process is running: pm2 status",
            "External API connectivity":
                "Check server network connectivity and DNS resolution",
        }
        fix = fix_map.get(first_name, "Investigate the relevant module")

        alert_text = (
            f"🚨 SELF DIAGNOSIS FAILED\n"
            f"Time: {ts_now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"Failed check: {first_name}\n"
            f"Details: {first_detail}\n"
            f"Suggested fix: {fix}\n"
        )
        if other_count > 0:
            extra_names = ", ".join(n for n, d in failures[1:])
            alert_text += f"Other failures ({other_count}): {extra_names}\n"
        alert_text += f"All other checks: ✅ passing ({len(passing)}/{len(results)})"

        delivered = False
        try:
            from altdata.notifications.notifier import Notifier
            n = Notifier(config)
            delivered = n._send_telegram(alert_text)
        except Exception as exc:
            logger.warning("Diagnostic alert send failed: %s", exc)
        telegram_logger.log_message("diagnostic", alert_text, delivered)
        if not delivered:
            telegram_logger.queue_retry("diagnostic", alert_text)

    logger.info(
        "Diagnostic complete: %s/%s passed", sum(r[1] for r in results), len(results)
    )
    return {"passed": passed_all, "results": results, "path": str(path)}
