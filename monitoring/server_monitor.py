"""
ServerMonitor — checks disk, RAM, Alpaca balance, PM2/process health.
Runs periodic checks and sends Telegram alerts when thresholds are breached.
"""
from __future__ import annotations

import gc
import logging
import os
import shutil
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)


class ServerMonitor:
    """
    Lightweight server health monitor.  Call start() to run in background.
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._srv = config.get("server", {})
        self._ram_warn   = self._srv.get("ram_warning_gb",  1.5)
        self._ram_crit   = self._srv.get("ram_critical_gb", 1.8)
        self._disk_warn  = self._srv.get("disk_warning_gb", 5.0)
        self._disk_crit  = self._srv.get("disk_critical_gb", 2.0)
        self._min_bal    = self._srv.get("alpaca_balance_min_usd", 5000.0)
        self._interval   = self._srv.get("check_interval_seconds", 300)
        self._alpaca_api = None   # injected externally after init
        self._running    = False
        self._thread: threading.Thread | None = None
        self._alerts_sent: set[str] = set()  # debounce repeated alerts

    # ── external injection ────────────────────────────────────────────────────

    def set_alpaca(self, api) -> None:
        self._alpaca_api = api

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ServerMonitor")
        self._thread.start()
        logger.info("ServerMonitor started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._running = False

    # ── main loop ─────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            try:
                self._check_all()
            except Exception as exc:
                logger.exception("ServerMonitor check error: %s", exc)
            time.sleep(self._interval)

    def _check_all(self) -> dict:
        report: dict[str, Any] = {}

        # RAM
        ram = self._check_ram()
        report["ram"] = ram
        ram_gb = ram.get("used_gb", 0)
        if ram_gb >= self._ram_crit:
            self._alert("ram_critical", f"RAM CRITICAL: {ram_gb:.2f} GB used — triggering gc.collect()")
            gc.collect()
        elif ram_gb >= self._ram_warn:
            self._alert("ram_warning", f"RAM WARNING: {ram_gb:.2f} GB used")

        # Disk
        disk = self._check_disk()
        report["disk"] = disk
        disk_gb = disk.get("free_gb", 999)
        if disk_gb <= self._disk_crit:
            self._alert("disk_critical", f"DISK CRITICAL: only {disk_gb:.1f} GB free")
        elif disk_gb <= self._disk_warn:
            self._alert("disk_warning", f"DISK WARNING: {disk_gb:.1f} GB free")

        # Alpaca balance
        bal = self._check_alpaca_balance()
        report["alpaca_balance"] = bal
        if bal is not None and bal < self._min_bal:
            self._alert("balance_low", f"Alpaca balance LOW: ${bal:,.2f} (min ${self._min_bal:,.0f})")

        # Bot process alive
        pid_ok = self._check_pid()
        report["pid_alive"] = pid_ok
        if not pid_ok:
            self._alert("pid_dead", "Bot PID not found in apollo.pid — bot may have crashed")

        logger.debug("ServerMonitor: %s", report)
        return report

    # ── individual checks ─────────────────────────────────────────────────────

    def _check_ram(self) -> dict:
        try:
            import psutil
            vm = psutil.virtual_memory()
            return {
                "total_gb": round(vm.total / 1e9, 3),
                "used_gb":  round(vm.used  / 1e9, 3),
                "pct":      vm.percent,
            }
        except ImportError:
            # fallback: /proc/meminfo
            try:
                info: dict[str, int] = {}
                with open("/proc/meminfo") as fh:
                    for line in fh:
                        parts = line.split()
                        if len(parts) >= 2:
                            info[parts[0].rstrip(":")] = int(parts[1])
                total = info.get("MemTotal", 0) * 1024
                avail = info.get("MemAvailable", 0) * 1024
                used  = total - avail
                return {
                    "total_gb": round(total / 1e9, 3),
                    "used_gb":  round(used  / 1e9, 3),
                    "pct":      round(used / total * 100, 1) if total else 0,
                }
            except Exception:
                return {}
        except Exception as exc:
            logger.debug("RAM check error: %s", exc)
            return {}

    def _check_disk(self) -> dict:
        try:
            usage = shutil.disk_usage("/")
            return {
                "total_gb": round(usage.total / 1e9, 1),
                "free_gb":  round(usage.free  / 1e9, 1),
                "used_pct": round((usage.used / usage.total) * 100, 1),
            }
        except Exception as exc:
            logger.debug("Disk check error: %s", exc)
            return {}

    def _check_alpaca_balance(self) -> float | None:
        if self._alpaca_api is None:
            return None
        try:
            acct = self._alpaca_api.get_account()
            return float(acct.equity)
        except Exception as exc:
            logger.debug("Alpaca balance check error: %s", exc)
            return None

    def _check_pid(self) -> bool:
        pid_file = "output/apollo.pid"
        try:
            if not os.path.exists(pid_file):
                return False
            with open(pid_file) as fh:
                pid = int(fh.read().strip())
            return os.path.exists(f"/proc/{pid}")
        except Exception:
            return False

    # ── alerting ──────────────────────────────────────────────────────────────

    def _alert(self, key: str, msg: str) -> None:
        if key in self._alerts_sent:
            return
        self._alerts_sent.add(key)
        logger.warning("ServerMonitor ALERT [%s]: %s", key, msg)
        self._send_telegram(f"[ServerMonitor]\n{msg}")

    def _send_telegram(self, text: str) -> None:
        try:
            telegram_cfg = self._config.get("notifications", {}).get("telegram", {})
            token   = telegram_cfg.get("bot_token", "")
            chat_id = telegram_cfg.get("chat_id", "")
            if not token or not chat_id:
                return
            import requests
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
        except Exception:
            pass

    def status(self) -> dict:
        return self._check_all()
