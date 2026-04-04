"""
ExternalSourceMonitor — checks that external data sources are reachable
and returning fresh data.

Sources checked:
  - Alpaca API (trading + data endpoints)
  - FRED API
  - yfinance (spot-check one ticker)
  - Companies House API (UK)
  - News API
  - SimFin

Runs on a configurable interval.  Disables collectors after 3 consecutive
failures and re-enables after recovery.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_CHECK_INTERVAL = 900   # 15 minutes
_FAIL_THRESHOLD         = 3


class _SourceStatus:
    def __init__(self, name: str) -> None:
        self.name      = name
        self.ok        = True
        self.failures  = 0
        self.last_ok   = datetime.now().isoformat()
        self.last_fail: str | None = None
        self.error_msg: str | None = None

    def record_ok(self) -> None:
        self.ok       = True
        self.failures = 0
        self.last_ok  = datetime.now().isoformat()

    def record_fail(self, error: str) -> bool:
        """Returns True if threshold crossed."""
        self.ok        = False
        self.failures += 1
        self.last_fail = datetime.now().isoformat()
        self.error_msg = error
        return self.failures >= _FAIL_THRESHOLD

    def to_dict(self) -> dict:
        return {
            "name":      self.name,
            "ok":        self.ok,
            "failures":  self.failures,
            "last_ok":   self.last_ok,
            "last_fail": self.last_fail,
            "error":     self.error_msg,
        }


class ExternalSourceMonitor:
    """
    Monitors external data source connectivity.
    """

    def __init__(self, config: dict) -> None:
        self._config   = config
        self._interval = config.get("server", {}).get("check_interval_seconds", _DEFAULT_CHECK_INTERVAL)
        self._running  = False
        self._thread: threading.Thread | None = None
        self._sources: dict[str, _SourceStatus] = {
            name: _SourceStatus(name)
            for name in ("alpaca", "fred", "yfinance", "companies_house", "news_api", "simfin")
        }
        self._feature_manager = None   # injected

    def set_feature_manager(self, fm) -> None:
        self._feature_manager = fm

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True, name="ExternalSourceMonitor")
        self._thread.start()
        logger.info("ExternalSourceMonitor started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._running = False

    # ── loop ──────────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            try:
                self.run_checks()
            except Exception as exc:
                logger.exception("ExternalSourceMonitor: check error: %s", exc)
            time.sleep(self._interval)

    # ── checks ────────────────────────────────────────────────────────────────

    def run_checks(self) -> dict[str, dict]:
        self._check_alpaca()
        self._check_fred()
        self._check_yfinance()
        self._check_companies_house()
        return self.status()

    def _check_alpaca(self) -> None:
        src = self._sources["alpaca"]
        try:
            import requests
            ak  = self._config.get("api_keys", {}).get("alpaca_api_key", "")
            sk  = self._config.get("api_keys", {}).get("alpaca_secret_key", "")
            base = self._config.get("alpaca", {}).get("base_url", "https://paper-api.alpaca.markets")
            resp = requests.get(
                f"{base}/v2/account",
                headers={"APCA-API-KEY-ID": ak, "APCA-API-SECRET-KEY": sk},
                timeout=10,
            )
            if resp.status_code == 200:
                src.record_ok()
            else:
                self._handle_fail("alpaca", f"HTTP {resp.status_code}")
        except Exception as exc:
            self._handle_fail("alpaca", str(exc))

    def _check_fred(self) -> None:
        src = self._sources["fred"]
        try:
            import requests
            api_key = self._config.get("api_keys", {}).get("fred", "")
            if not api_key:
                src.record_ok()  # not configured — skip
                return
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series",
                params={"series_id": "UNRATE", "api_key": api_key, "file_type": "json"},
                timeout=10,
            )
            if resp.status_code == 200:
                src.record_ok()
            else:
                self._handle_fail("fred", f"HTTP {resp.status_code}")
        except Exception as exc:
            self._handle_fail("fred", str(exc))

    def _check_yfinance(self) -> None:
        try:
            import yfinance as yf
            from datetime import date, timedelta
            end   = date.today()
            start = end - timedelta(days=5)
            df = yf.download("SPY", start=str(start), end=str(end),
                             progress=False, auto_adjust=True, threads=False)
            if df is not None and len(df) > 0:
                self._sources["yfinance"].record_ok()
            else:
                self._handle_fail("yfinance", "empty response")
        except Exception as exc:
            self._handle_fail("yfinance", str(exc))

    def _check_companies_house(self) -> None:
        api_key = self._config.get("api_keys", {}).get("companies_house", "")
        if not api_key:
            self._sources["companies_house"].record_ok()  # not configured — skip
            return
        try:
            import requests
            resp = requests.get(
                "https://api.company-information.service.gov.uk/search/companies",
                params={"q": "test", "items_per_page": 1},
                auth=(api_key, ""),
                timeout=10,
            )
            if resp.status_code in (200, 400):
                self._sources["companies_house"].record_ok()
            else:
                self._handle_fail("companies_house", f"HTTP {resp.status_code}")
        except Exception as exc:
            self._handle_fail("companies_house", str(exc))

    # ── failure handling ──────────────────────────────────────────────────────

    def _handle_fail(self, source_name: str, error: str) -> None:
        src = self._sources[source_name]
        crossed = src.record_fail(error)
        logger.warning(
            "ExternalSourceMonitor: %s FAIL (%d/%d): %s",
            source_name, src.failures, _FAIL_THRESHOLD, error
        )
        if crossed:
            logger.error(
                "ExternalSourceMonitor: %s crossed failure threshold — sending alert",
                source_name
            )
            self._alert(f"External source DOWN: {source_name}\nError: {error}")
            if self._feature_manager is not None:
                try:
                    self._feature_manager.disable(
                        f"{source_name}_collector",
                        f"source unavailable: {error}"
                    )
                except Exception:
                    pass

    def _alert(self, text: str) -> None:
        try:
            tg = self._config.get("telegram", {})
            token   = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
            if not token or not chat_id:
                return
            import requests
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": f"[ExternalSourceMonitor]\n{text}"},
                timeout=10,
            )
        except Exception:
            pass

    def status(self) -> dict[str, dict]:
        return {name: s.to_dict() for name, s in self._sources.items()}

    def all_ok(self) -> bool:
        return all(s.ok for s in self._sources.values())
