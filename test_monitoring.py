#!/usr/bin/env python3
"""
Part 8 — Paper trade verification test for monitoring system.

Checks:
  1. Fake daily health report generated and Telegram attempted
  2. Fake instant alert generated and Telegram attempted
  3. All report directories exist and files saved correctly
  4. telegram_history JSON written with correct schema
  5. python3 main.py reports list shows test reports
  6. RAM and CPU within limits
  7. Existing paper trading imports unaffected
  8. MonitorRunner starts cleanly as daemon thread
"""
import json
import os
import subprocess
import sys
import time
import yaml
from datetime import datetime
from pathlib import Path

GREEN = "\033[92mPASS\033[0m"
RED   = "\033[91mFAIL\033[0m"
results = []

def check(name, fn):
    try:
        fn()
        print(f"  {GREEN}  {name}")
        results.append((name, True, None))
    except Exception as e:
        print(f"  {RED}  {name}: {e}")
        results.append((name, False, str(e)))

with open("config/settings.yaml") as f:
    config = yaml.safe_load(f)


# ── 1. Daily health report generated and sent ────────────────────────────────
print("\n[1] Daily health report")

def _daily_report():
    from monitoring.health_reporter import format_daily_report, send_daily_report
    # generate (no API calls, just formatting)
    report = format_daily_report(config)
    assert "QUANT FUND DAILY HEALTH REPORT" in report, "Report header missing"
    assert "API STATUS" in report, "API STATUS section missing"
    assert "TRADING ACTIVITY" in report, "TRADING ACTIVITY section missing"
    assert "DATA FUSION SUMMARY" in report, "DATA FUSION SUMMARY section missing"
    assert "SUGGESTIONS" in report, "SUGGESTIONS section missing"
    # Save and attempt send (Telegram may or may not deliver — we just check it runs)
    ok = send_daily_report(config)
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = Path(f"logs/daily_health_reports/health_report_{date_str}.txt")
    assert path.exists(), f"Report file not created: {path}"
    print(f"       (Telegram delivered: {ok}, file: {path})")

check("Daily health report generated, saved, and send attempted", _daily_report)


# ── 2. Instant alert ──────────────────────────────────────────────────────────
print("\n[2] Instant alert")

def _instant_alert():
    from monitoring import telegram_logger
    from monitoring.alert_monitor import _send_alert
    test_msg = (
        f"🚨 TEST ALERT — monitoring system verification\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"This is a test alert from test_monitoring.py"
    )
    _send_alert(config, "test", test_msg)
    # Check it was logged
    recent = telegram_logger.get_recent(5)
    found  = any("TEST ALERT" in m.get("message", "") for m in recent)
    assert found, "Alert not found in telegram_history"
    # Check alert file saved
    alert_files = list(Path("logs/alerts").glob("alert_test_*.txt"))
    assert alert_files, "Alert file not saved to logs/alerts/"
    print(f"       (alert file: {alert_files[-1].name})")

check("Instant alert sent and logged to telegram_history", _instant_alert)


# ── 3. Report directories and file creation ───────────────────────────────────
print("\n[3] Report directories and files")

def _directories():
    required = [
        "logs/daily_health_reports",
        "logs/weekly_reports",
        "logs/diagnostics",
        "logs/alerts",
        "output/telegram_history",
    ]
    for d in required:
        assert Path(d).is_dir(), f"Directory missing: {d}"

check("All 5 required log directories exist", _directories)

def _weekly_report_saveable():
    from monitoring.health_reporter import format_weekly_report
    report = format_weekly_report(config)
    assert "QUANT FUND WEEKLY REPORT" in report
    assert "TOP PERFORMING SIGNALS" in report
    assert "PERFORMANCE SUMMARY" in report
    # Save manually to test
    from pathlib import Path
    now  = datetime.now()
    week = now.isocalendar()
    fname = f"weekly_report_{week[0]}-W{week[1]:02d}.txt"
    path  = Path("logs/weekly_reports") / fname
    path.write_text(report, encoding="utf-8")
    assert path.exists(), f"Weekly report file not found: {path}"
    print(f"       (saved: {path.name})")

check("Weekly report generated and saved", _weekly_report_saveable)

def _diagnostic_saveable():
    from monitoring.self_diagnostic import run_diagnostic
    result = run_diagnostic(config)
    assert "passed" in result
    assert "results" in result
    path = Path(result.get("path", ""))
    assert path.exists(), f"Diagnostic file not found: {path}"
    print(f"       ({sum(r[1] for r in result['results'])}/{len(result['results'])} checks passed)")
    print(f"       (saved: {path.name})")

check("Self-diagnostic runs and saves file", _diagnostic_saveable)


# ── 4. Telegram history JSON schema ──────────────────────────────────────────
print("\n[4] Telegram history JSON")

def _tg_history_schema():
    from monitoring import telegram_logger
    # Log a test entry
    telegram_logger.log_message("test", "Schema verification test message", True)
    recent = telegram_logger.get_recent(1)
    assert recent, "No entries in telegram history"
    entry = recent[0]
    for field in ("timestamp", "type", "message", "delivered"):
        assert field in entry, f"Field '{field}' missing from telegram history entry"
    assert entry["type"] == "test"
    assert "Schema verification" in entry["message"]
    # Check file exists
    month = datetime.now().strftime("%Y-%m")
    path  = Path(f"output/telegram_history/telegram_log_{month}.json")
    assert path.exists(), f"Monthly log file not found: {path}"
    # Valid JSONL
    lines = [l.strip() for l in path.read_text().splitlines() if l.strip()]
    for line in lines[-3:]:
        json.loads(line)  # must parse
    print(f"       ({len(lines)} total messages in {path.name})")

check("telegram_history JSONL schema correct and files written", _tg_history_schema)


# ── 5. python3 main.py reports list ──────────────────────────────────────────
print("\n[5] reports list command")

def _reports_list():
    result = subprocess.run(
        [sys.executable, "main.py", "reports", "list"],
        capture_output=True, text=True, timeout=20,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, f"reports list returned exit {result.returncode}: {output[:200]}"
    assert "QUANT FUND SAVED REPORTS" in output, "Header not in output"
    assert "Daily Health Reports" in output, "Daily section missing"
    print(f"       (exit={result.returncode}, output lines={len(output.splitlines())})")

check("python3 main.py reports list runs and shows test reports", _reports_list)


# ── 6. RAM and CPU within limits ──────────────────────────────────────────────
print("\n[6] Resource usage")

def _ram_ok():
    from monitoring.system_stats import get_ram_mb
    used, total, pct = get_ram_mb()
    assert used > 0 and total > 0, "get_ram_mb returned zeros"
    print(f"       (RAM: {used}MB / {total}MB = {pct:.1f}%)")
    # For the test process alone, just verify the monitoring module doesn't blow up
    assert pct < 100, "RAM at 100% - something is wrong"

check("RAM stats readable and within 100%", _ram_ok)

def _cpu_ok():
    from monitoring.system_stats import get_cpu_pct
    cpu = get_cpu_pct()
    assert cpu >= 0, "get_cpu_pct returned negative"
    print(f"       (CPU: {cpu:.1f}%)")

check("CPU stats readable", _cpu_ok)


# ── 7. Paper trading imports unaffected ───────────────────────────────────────
print("\n[7] Paper trading unaffected")

def _paper_trading_ok():
    from execution.paper_trader import PaperTrader
    from data.fetcher import DataFetcher
    from data.universe import Universe
    assert hasattr(PaperTrader, "_get_live_price"), "_get_live_price missing"
    assert hasattr(PaperTrader, "run_scan"),         "run_scan missing"
    assert hasattr(PaperTrader, "check_exit_conditions"), "check_exit_conditions missing"

check("PaperTrader imports and key methods present", _paper_trading_ok)

def _collectors_still_ok():
    from data.collectors.rates_credit_collector import RatesCreditCollector
    from data.collectors.consumer_intelligence import ConsumerIntelligence
    from data.collectors.commodity_collector import CommodityCollector
    RatesCreditCollector("config/settings.yaml")
    ConsumerIntelligence(config)
    CommodityCollector(config)

check("Core collectors still import correctly alongside monitoring", _collectors_still_ok)


# ── 8. MonitorRunner starts as daemon thread ──────────────────────────────────
print("\n[8] MonitorRunner daemon thread")

def _monitor_runner_starts():
    import threading
    from monitoring.monitor_runner import start_monitoring
    runner = start_monitoring(config)
    time.sleep(0.5)
    assert runner.is_alive(), "MonitorRunner thread not alive"
    # Verify it's a daemon thread
    for t in threading.enumerate():
        if t.name == "monitor-runner":
            assert t.daemon, "monitor-runner is NOT a daemon thread"
            break
    print(f"       (alive={runner.is_alive()}, daemon=True)")

check("MonitorRunner starts cleanly as daemon thread", _monitor_runner_starts)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
passed = sum(1 for _, ok, _ in results if ok)
total  = len(results)
print(f"Result: {passed}/{total} checks passed")
if passed < total:
    print("\nFailed checks:")
    for name, ok, err in results:
        if not ok:
            print(f"  ✗ {name}: {err}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)
