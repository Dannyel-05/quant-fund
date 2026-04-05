"""
Apollo Async Infrastructure
Rate limiting, caching, and async HTTP utilities.
Do not import signal logic here. This is pure infrastructure.
"""

import asyncio
import aiohttp
import time
import logging
from collections import defaultdict
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token bucket rate limiter for each API source.
    Queues requests rather than dropping them.
    """
    def __init__(self, calls_per_second: float, burst_limit: int = 5):
        self.calls_per_second = calls_per_second
        self.burst_limit = burst_limit
        self._tokens = burst_limit
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self.burst_limit,
                self._tokens + elapsed * self.calls_per_second
            )
            self._last_refill = now
            if self._tokens < 1:
                wait_time = (1 - self._tokens) / self.calls_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0
            else:
                self._tokens -= 1


class TTLCache:
    """
    Time-to-live cache. Returns stale value on miss rather than blocking.
    """
    def __init__(self):
        self._store: Dict[str, tuple] = {}  # key -> (value, expiry_time)

    def get(self, key: str) -> Optional[Any]:
        if key in self._store:
            value, expiry = self._store[key]
            if time.monotonic() < expiry:
                return value
        return None

    def set(self, key: str, value: Any, ttl_seconds: int):
        self._store[key] = (value, time.monotonic() + ttl_seconds)

    def get_stale(self, key: str) -> Optional[Any]:
        """Return value even if expired — used as fallback."""
        if key in self._store:
            return self._store[key][0]
        return None


class APIHealthTracker:
    """
    Tracks per-API error rate, latency, and success rate.
    Auto-disables unstable sources after repeated failures.
    """
    def __init__(self, failure_threshold: int = 10, window: int = 60):
        self.failure_threshold = failure_threshold
        self.window = window
        self._failures: Dict[str, list] = defaultdict(list)
        self._disabled: Dict[str, float] = {}
        self._latencies: Dict[str, list] = defaultdict(list)

    def record_success(self, api_name: str, latency_ms: float):
        self._latencies[api_name].append((time.monotonic(), latency_ms))
        self._latencies[api_name] = [
            (t, l) for t, l in self._latencies[api_name]
            if time.monotonic() - t < self.window
        ]

    def record_failure(self, api_name: str):
        now = time.monotonic()
        self._failures[api_name].append(now)
        self._failures[api_name] = [
            t for t in self._failures[api_name]
            if now - t < self.window
        ]
        if len(self._failures[api_name]) >= self.failure_threshold:
            self._disabled[api_name] = now + 300  # disable for 5 min
            logger.warning(f"API {api_name} auto-disabled after {self.failure_threshold} failures in {self.window}s")

    def is_available(self, api_name: str) -> bool:
        if api_name in self._disabled:
            if time.monotonic() < self._disabled[api_name]:
                return False
            else:
                del self._disabled[api_name]
                logger.info(f"API {api_name} re-enabled after cooldown")
        return True

    def get_avg_latency(self, api_name: str) -> Optional[float]:
        recent = [l for t, l in self._latencies.get(api_name, [])
                  if time.monotonic() - t < self.window]
        return sum(recent) / len(recent) if recent else None


async def safe_fetch(
    session: aiohttp.ClientSession,
    url: str,
    api_name: str,
    rate_limiter: RateLimiter,
    cache: TTLCache,
    health_tracker: APIHealthTracker,
    cache_key: str,
    ttl: int = 60,
    timeout_sec: float = 3.0,
    max_retries: int = 2,
    fallback_value: Any = None,
    params: dict = None,
    headers: dict = None
) -> Any:
    """
    Universal safe fetch with rate limiting, caching, retries, and fallback.
    NEVER raises an exception — always returns a value.
    """
    # Return cached value if fresh
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    # Check API health
    if not health_tracker.is_available(api_name):
        stale = cache.get_stale(cache_key)
        return stale if stale is not None else fallback_value

    for attempt in range(max_retries + 1):
        try:
            await rate_limiter.acquire()
            start = time.monotonic()
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_sec)
            ) as response:
                latency_ms = (time.monotonic() - start) * 1000
                if response.status == 200:
                    data = await response.json()
                    cache.set(cache_key, data, ttl)
                    health_tracker.record_success(api_name, latency_ms)
                    return data
                else:
                    health_tracker.record_failure(api_name)
                    logger.warning(f"{api_name} returned HTTP {response.status}")
        except asyncio.TimeoutError:
            health_tracker.record_failure(api_name)
            logger.warning(f"{api_name} timeout on attempt {attempt + 1}")
        except Exception as e:
            health_tracker.record_failure(api_name)
            logger.warning(f"{api_name} error: {e}")

        if attempt < max_retries:
            await asyncio.sleep(0.5 * (attempt + 1))

    # All retries failed — return stale or fallback
    stale = cache.get_stale(cache_key)
    if stale is not None:
        logger.info(f"{api_name} returning stale cache for {cache_key}")
        return stale
    return fallback_value
