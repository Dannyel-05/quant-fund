"""
Apollo Pre-Flight Check
Run before market open to confirm all systems ready.
Outputs a clear PASS/FAIL status for each system.

Usage: python3 scripts/preflight_check.py
"""

import sqlite3
import subprocess
import os
import sys
import glob
from datetime import datetime, timezone

QUANT_DIR = '/home/dannyelticala/quant-fund'
CHECKS = []


def check(name, fn):
    try:
        result = fn()
        status = "PASS" if result else "FAIL"
        CHECKS.append((name, status, ""))
    except Exception as e:
        CHECKS.append((name, "FAIL", str(e)))


def bot_is_running():
    result = subprocess.run(
        ['ps', 'aux'],
        capture_output=True, text=True
    )
    return 'python3 main.py' in result.stdout


def all_dbs_accessible():
    dbs = [
        'closeloop/storage/closeloop.db',
        'output/historical_db.db',
        'output/permanent_archive.db',
        'frontier/storage/frontier.db',
        'deepdata/storage/deepdata.db',
    ]
    for db in dbs:
        path = os.path.join(QUANT_DIR, db)
        if not os.path.exists(path):
            return False
        conn = sqlite3.connect(path)
        conn.execute("SELECT 1")
        conn.close()
    return True


def simulation_db_ready():
    path = os.path.join(QUANT_DIR, 'simulations', 'simulation.db')
    if not os.path.exists(path):
        return False
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='simulation_runs'")
    result = cursor.fetchone()
    conn.close()
    return result is not None


def shadow_db_ready():
    path = os.path.join(QUANT_DIR, 'simulations', 'shadow.db')
    if not os.path.exists(path):
        return False
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='model_registry'")
    result = cursor.fetchone()
    conn.close()
    return result is not None


def retraining_controller_ok():
    try:
        sys.path.insert(0, QUANT_DIR)
        from core.retraining_controller import RetrainingController
        rc = RetrainingController()
        dormant, _ = rc.check_dormancy()
        return True  # Import and dormancy check succeeded
    except Exception:
        return False


def log_file_recent():
    pattern = os.path.join(QUANT_DIR, 'logs', 'bot_*.log')
    logs = glob.glob(pattern)
    if not logs:
        return False
    latest = sorted(logs)[-1]
    mtime = os.path.getmtime(latest)
    age_minutes = (datetime.now().timestamp() - mtime) / 60
    return age_minutes < 120  # Log updated within last 2 hours


def config_key_ok():
    """Confirm OWM key has been updated (not the old key)."""
    config_path = os.path.join(QUANT_DIR, 'config', 'settings.yaml')
    with open(config_path) as f:
        content = f.read()
    return 'ab94a3ece8f2c0de634e41ccc1d3561f' not in content


check("Bot process running", bot_is_running)
check("All databases accessible", all_dbs_accessible)
check("Simulation DB ready", simulation_db_ready)
check("Shadow DB ready", shadow_db_ready)
check("Retraining controller OK", retraining_controller_ok)
check("Log file recent (< 2h)", log_file_recent)
check("OWM API key updated", config_key_ok)

print(f"\n{'='*55}")
print(f"APOLLO PRE-FLIGHT CHECK — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print(f"{'='*55}")
for name, status, err in CHECKS:
    marker = "OK" if status == "PASS" else "!!"
    print(f"[{marker}] {status:<4}  {name}")
    if err:
        print(f"           Error: {err}")
fails = [c for c in CHECKS if c[1] == "FAIL"]
print(f"{'='*55}")
if fails:
    print(f"[!!] {len(fails)} check(s) failed — resolve before market open")
    sys.exit(1)
else:
    print("[OK] All systems ready for Monday April 7 14:30 UTC")
    sys.exit(0)
