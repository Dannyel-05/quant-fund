"""
Shared TTL cache for all new Apollo ecosystem modules.
Deterministic key generation from function name + arguments.
"""
import time
import hashlib
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# TTL constants (seconds)
TTL_PRICE = 30
TTL_NEWS = 300          # 5 minutes
TTL_MACRO = 1800        # 30 minutes
TTL_REGIME = 600        # 10 minutes
TTL_CHART = 300         # 5 minutes (store path, not bytes)
TTL_LLM = 600           # 10 minutes


class TTLCache:
    """
    In-memory TTL cache with deterministic key generation.
    Thread-safe via simple dict operations (GIL-protected for CPython).
    """

    def __init__(self):
        self._store: dict[str, tuple[Any, float]] = {}  # key -> (value, expiry)

    def _make_key(self, *args, **kwargs) -> str:
        """Generate a deterministic cache key from arbitrary arguments."""
        try:
            raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        except Exception:
            raw = str(args) + str(kwargs)
        return hashlib.md5(raw.encode()).hexdigest()

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Store a value with a TTL in seconds."""
        expiry = time.monotonic() + ttl
        self._store[key] = (value, expiry)

    def get(self, key: str) -> Optional[Any]:
        """Return value if present and not expired, else None."""
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if time.monotonic() > expiry:
            del self._store[key]
            return None
        return value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        now = time.monotonic()
        expired = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in expired:
            del self._store[k]
        return len(expired)

    def cache_key_for(self, fn_name: str, *args, **kwargs) -> str:
        """Generate a cache key namespaced by function name."""
        return f"{fn_name}:{self._make_key(*args, **kwargs)}"

    def get_or_set(self, key: str, value_fn, ttl: int) -> Any:
        """
        Return cached value if present, otherwise call value_fn(), store, and return.
        value_fn must be a synchronous callable.
        """
        cached = self.get(key)
        if cached is not None:
            return cached
        value = value_fn()
        if value is not None:
            self.set(key, value, ttl)
        return value

    def hash_query(self, context_str: str, question: Optional[str] = None) -> str:
        """Generate a cache key for LLM responses based on content hash."""
        raw = context_str + (question or "")
        return "llm:" + hashlib.sha256(raw.encode()).hexdigest()[:32]


# Module-level shared instance
_shared_cache = TTLCache()


def get_shared_cache() -> TTLCache:
    """Return the module-level shared cache instance."""
    return _shared_cache
