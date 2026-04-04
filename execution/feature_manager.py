"""
FeatureManager — singleton that wraps optional features with automatic
disable-after-3-failures and Telegram alerting.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_INSTANCE: "FeatureManager | None" = None


class FeatureManager:
    """
    Singleton manager for optional bot features.

    Usage
    -----
    fm = FeatureManager.get(config)
    result = fm.wrap("sector_rotation", _do_sector_rotation, arg1, arg2)
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._flags: dict[str, bool] = {}          # True = enabled
        self._failures: dict[str, int] = defaultdict(int)
        self._disabled_at: dict[str, float] = {}
        self._lock = threading.Lock()
        self._telegram = None
        self._load_flags()

    # ── singleton ─────────────────────────────────────────────────────────────

    @classmethod
    def get(cls, config: dict | None = None) -> "FeatureManager":
        global _INSTANCE
        with _LOCK:
            if _INSTANCE is None:
                if config is None:
                    raise RuntimeError("FeatureManager not yet initialised — pass config on first call")
                _INSTANCE = cls(config)
            return _INSTANCE

    @classmethod
    def reset(cls) -> None:
        """For testing only."""
        global _INSTANCE
        with _LOCK:
            _INSTANCE = None

    # ── flag loading ──────────────────────────────────────────────────────────

    def _load_flags(self) -> None:
        raw: dict = self._config.get("feature_flags", {})
        for name, enabled in raw.items():
            self._flags[name] = bool(enabled)
        logger.debug("FeatureManager loaded %d flags: %s", len(self._flags), list(self._flags))

    def is_enabled(self, name: str) -> bool:
        with self._lock:
            return self._flags.get(name, True)  # default ON if not listed

    def disable(self, name: str, reason: str = "") -> None:
        with self._lock:
            self._flags[name] = False
            self._disabled_at[name] = time.time()
        logger.warning("FeatureManager: DISABLED feature '%s' — %s", name, reason)
        self._alert(f"Feature DISABLED: {name}\nReason: {reason}")

    def enable(self, name: str) -> None:
        with self._lock:
            self._flags[name] = True
            self._failures[name] = 0
        logger.info("FeatureManager: RE-ENABLED feature '%s'", name)

    # ── wrap ──────────────────────────────────────────────────────────────────

    def wrap(self, name: str, fn: Callable, *args: Any, default: Any = None, **kwargs: Any) -> Any:
        """
        Call fn(*args, **kwargs) if feature is enabled.
        On exception: increment failure counter; disable after 3 failures.
        Returns default on failure or when disabled.
        """
        if not self.is_enabled(name):
            return default
        try:
            result = fn(*args, **kwargs)
            # reset failure count on success
            with self._lock:
                self._failures[name] = 0
            return result
        except Exception as exc:
            with self._lock:
                self._failures[name] += 1
                count = self._failures[name]
            logger.exception("FeatureManager: '%s' failed (%d/3): %s", name, count, exc)
            self._log_feature_error(name, exc)
            if count >= 3:
                self.disable(name, f"{type(exc).__name__}: {exc}")
            return default

    # ── decorator ─────────────────────────────────────────────────────────────

    def guarded(self, name: str, default: Any = None):
        """Decorator — @fm.guarded('feature_name')"""
        def decorator(fn: Callable) -> Callable:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                return self.wrap(name, fn, *args, default=default, **kwargs)
            return wrapper
        return decorator

    # ── helpers ───────────────────────────────────────────────────────────────

    def _log_feature_error(self, name: str, exc: Exception) -> None:
        try:
            with open("logs/feature_errors.log", "a") as fh:
                import datetime
                ts = datetime.datetime.now().isoformat()
                fh.write(f"{ts}  [{name}]  {type(exc).__name__}: {exc}\n")
        except Exception:
            pass

    def _alert(self, msg: str) -> None:
        try:
            telegram_cfg = self._config.get("telegram", {})
            token = telegram_cfg.get("bot_token", "")
            chat_id = telegram_cfg.get("chat_id", "")
            if not token or not chat_id:
                return
            import requests
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": f"[FeatureManager]\n{msg}"},
                timeout=10,
            )
        except Exception:
            pass

    def status_dict(self) -> dict:
        with self._lock:
            return {
                "flags": dict(self._flags),
                "failures": dict(self._failures),
                "disabled_at": {k: time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(v))
                                for k, v in self._disabled_at.items()},
            }
