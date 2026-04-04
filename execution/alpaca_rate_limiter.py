"""
AlpacaRateLimiter — token-bucket rate limiter for Alpaca API calls.

Alpaca paper trading limits:
  - 200 requests/minute for data endpoints
  - 60 requests/minute for trading endpoints

Usage
-----
rl = AlpacaRateLimiter()
rl.acquire("data")      # blocks until a token is available
rl.acquire("trading")   # blocks until a token is available

Or as a context manager:
    with rl.limit("trading"):
        broker.place_order(...)
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Iterator

logger = logging.getLogger(__name__)

_DEFAULT_LIMITS = {
    "data":    (200, 60.0),   # (tokens, refill_period_seconds)
    "trading": (60,  60.0),
    "account": (50,  60.0),
    "default": (100, 60.0),
}


class _TokenBucket:
    """Thread-safe token bucket."""

    def __init__(self, capacity: int, refill_period: float) -> None:
        self._capacity    = capacity
        self._tokens      = float(capacity)
        self._refill_rate = capacity / refill_period   # tokens/second
        self._last_refill = time.monotonic()
        self._lock        = threading.Lock()

    def _refill(self) -> None:
        now     = time.monotonic()
        elapsed = now - self._last_refill
        gained  = elapsed * self._refill_rate
        self._tokens      = min(self._capacity, self._tokens + gained)
        self._last_refill = now

    def acquire(self, tokens: int = 1, timeout: float = 60.0) -> bool:
        """
        Acquire `tokens` from the bucket.  Blocks up to `timeout` seconds.
        Returns True if acquired, False if timed out.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                wait = (tokens - self._tokens) / self._refill_rate
            sleep_time = min(wait, deadline - time.monotonic())
            if sleep_time <= 0:
                return False
            time.sleep(sleep_time)

    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class AlpacaRateLimiter:
    """
    Multi-bucket rate limiter for Alpaca API endpoints.
    """

    def __init__(self, limits: dict[str, tuple[int, float]] | None = None) -> None:
        cfg = limits or _DEFAULT_LIMITS
        self._buckets: dict[str, _TokenBucket] = {
            name: _TokenBucket(cap, period)
            for name, (cap, period) in cfg.items()
        }
        self._call_counts: dict[str, int] = {name: 0 for name in self._buckets}
        self._lock = threading.Lock()

    def acquire(self, endpoint: str = "default", tokens: int = 1, timeout: float = 30.0) -> bool:
        """
        Acquire rate limit token for the given endpoint category.
        Blocks until available or timeout.
        Returns True if acquired.
        """
        bucket_name = endpoint if endpoint in self._buckets else "default"
        bucket = self._buckets[bucket_name]
        acquired = bucket.acquire(tokens=tokens, timeout=timeout)
        if acquired:
            with self._lock:
                self._call_counts[bucket_name] = self._call_counts.get(bucket_name, 0) + tokens
        else:
            logger.warning(
                "AlpacaRateLimiter: TIMEOUT acquiring token for '%s' (waited %.1fs)",
                endpoint, timeout
            )
        return acquired

    @contextmanager
    def limit(self, endpoint: str = "default", timeout: float = 30.0) -> Iterator[None]:
        """Context manager: acquires a token before entering the block."""
        self.acquire(endpoint, timeout=timeout)
        yield

    def available(self, endpoint: str = "default") -> float:
        bucket_name = endpoint if endpoint in self._buckets else "default"
        return self._buckets[bucket_name].available()

    def status(self) -> dict:
        return {
            name: {
                "available": round(bucket.available(), 1),
                "capacity":  bucket._capacity,
                "calls_made": self._call_counts.get(name, 0),
            }
            for name, bucket in self._buckets.items()
        }

    def reset_counts(self) -> None:
        with self._lock:
            self._call_counts = {name: 0 for name in self._buckets}
