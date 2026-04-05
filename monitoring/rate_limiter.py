"""
Shared rate limiter for all new Apollo ecosystem modules.
Token bucket algorithm with exponential backoff and graceful fallback.
"""
import asyncio
import time
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

API_LIMITS = {
    "telegram":    {"calls_per_minute": 30,  "timeout_sec": 10, "max_retries": 2},
    "finnhub":     {"calls_per_minute": 60,  "timeout_sec": 5,  "max_retries": 2},
    "newsapi":     {"calls_per_minute": 20,  "timeout_sec": 8,  "max_retries": 2},
    "fred":        {"calls_per_minute": 30,  "timeout_sec": 10, "max_retries": 1},
    "simfin":      {"calls_per_minute": 10,  "timeout_sec": 15, "max_retries": 1},
    "alpaca":      {"calls_per_minute": 200, "timeout_sec": 5,  "max_retries": 3},
    "yfinance":    {"calls_per_minute": 60,  "timeout_sec": 8,  "max_retries": 2},
    "anthropic":   {"calls_per_minute": 20,  "timeout_sec": 30, "max_retries": 1},
    "default":     {"calls_per_minute": 30,  "timeout_sec": 10, "max_retries": 2},
}


class _TokenBucket:
    """Thread-safe token bucket for a single API."""

    def __init__(self, calls_per_minute: int):
        self.rate = calls_per_minute / 60.0  # tokens per second
        self.capacity = float(calls_per_minute)
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        """Acquire one token. Returns seconds to wait (0 if immediately available)."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            else:
                wait = (1.0 - self.tokens) / self.rate
                return wait


class RateLimiter:
    """
    Global rate limiter for all Apollo ecosystem API calls.
    Uses token bucket per API with exponential backoff on limit hits.
    """

    def __init__(self):
        self._buckets: dict[str, _TokenBucket] = {}
        for api, cfg in API_LIMITS.items():
            self._buckets[api] = _TokenBucket(cfg["calls_per_minute"])
        # Track 5xx errors per API for sustained failure detection
        self._error_timestamps: dict[str, list[float]] = {}

    def _get_config(self, api: str) -> dict:
        return API_LIMITS.get(api, API_LIMITS["default"])

    def _get_bucket(self, api: str) -> _TokenBucket:
        if api not in self._buckets:
            cfg = self._get_config(api)
            self._buckets[api] = _TokenBucket(cfg["calls_per_minute"])
        return self._buckets[api]

    async def acquire(self, api: str) -> None:
        """Wait until a token is available for the given API."""
        bucket = self._get_bucket(api)
        wait = await bucket.acquire()
        if wait > 0:
            logger.debug(f"RateLimiter: {api} throttled, waiting {wait:.2f}s")
            await asyncio.sleep(wait)

    async def call_with_retry(
        self,
        api: str,
        coro_fn,
        *args,
        cached_value: Any = None,
        endpoint: str = "unknown",
        **kwargs,
    ) -> Any:
        """
        Execute an async coroutine with rate limiting and exponential backoff.

        - On rate limit hit: queue and retry after delay
        - On timeout: return cached_value if available, else None (logs WARNING)
        - On max retries exceeded: log ERROR, return None gracefully
        - On HTTP 5xx: exponential backoff, alert only if sustained > 5 minutes
        """
        cfg = self._get_config(api)
        timeout_sec = cfg["timeout_sec"]
        max_retries = cfg["max_retries"]

        for attempt in range(max_retries + 1):
            await self.acquire(api)
            try:
                result = await asyncio.wait_for(
                    coro_fn(*args, **kwargs),
                    timeout=timeout_sec,
                )
                # Clear error timestamps on success
                self._error_timestamps.pop(api, None)
                return result

            except asyncio.TimeoutError:
                logger.warning(
                    f"RateLimiter: {api}/{endpoint} timed out after {timeout_sec}s "
                    f"(attempt {attempt + 1}/{max_retries + 1})"
                )
                if attempt == max_retries:
                    if cached_value is not None:
                        logger.warning(f"RateLimiter: returning cached value for {api}/{endpoint}")
                        return cached_value
                    return None

            except Exception as e:
                err_str = str(e)
                # Check for 5xx-style errors
                is_server_error = any(
                    code in err_str for code in ["500", "502", "503", "504", "5xx"]
                )
                if is_server_error:
                    now = time.monotonic()
                    self._error_timestamps.setdefault(api, []).append(now)
                    # Prune old entries
                    self._error_timestamps[api] = [
                        t for t in self._error_timestamps[api] if now - t < 310
                    ]
                    if len(self._error_timestamps[api]) > 1:
                        duration = now - self._error_timestamps[api][0]
                        if duration > 300:
                            logger.error(
                                f"RateLimiter: {api}/{endpoint} returning 5xx for "
                                f"{duration:.0f}s — sustained failure"
                            )

                if attempt < max_retries:
                    delay = min(2 ** attempt, 60)
                    logger.warning(
                        f"RateLimiter: {api}/{endpoint} error on attempt {attempt + 1}: "
                        f"{e} — retrying in {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"RateLimiter: {api}/{endpoint} failed after {max_retries + 1} attempts: {e}"
                    )
                    return None

        return None
