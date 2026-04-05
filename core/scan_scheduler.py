"""
Apollo 3-Tier Scan Scheduler
Tier 1: 10-30s  — price, volume, volatility
Tier 2: 1-5min  — news, earnings, macro
Tier 3: 5-15min — alt data, long-cycle macro

Each tier runs independently. A slow Tier 3 never blocks Tier 1.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class TieredScanScheduler:
    def __init__(self, tier1_interval=20, tier2_interval=120, tier3_interval=600):
        self.tier1_interval = tier1_interval
        self.tier2_interval = tier2_interval
        self.tier3_interval = tier3_interval
        self._running = False
        self._tier1_collectors = []
        self._tier2_collectors = []
        self._tier3_collectors = []
        self._scan_count = 0
        self._last_full_cycle = None

    def register_collector(self, collector_fn, tier: int):
        """Register a collector coroutine function to a tier."""
        if tier == 1:
            self._tier1_collectors.append(collector_fn)
        elif tier == 2:
            self._tier2_collectors.append(collector_fn)
        elif tier == 3:
            self._tier3_collectors.append(collector_fn)

    async def _run_tier(self, tier_num: int, collectors: list, interval: int):
        """Run a single tier in its own loop."""
        tier_names = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}
        while self._running:
            start = time.monotonic()
            if collectors:
                results = await asyncio.gather(
                    *[c() for c in collectors],
                    return_exceptions=True
                )
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.warning(
                            f"Tier {tier_num} [{tier_names[tier_num]}] "
                            f"collector {i} failed: {result}"
                        )
            elapsed = time.monotonic() - start
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def start(self):
        self._running = True
        logger.info("TieredScanScheduler starting all tiers")
        await asyncio.gather(
            self._run_tier(1, self._tier1_collectors, self.tier1_interval),
            self._run_tier(2, self._tier2_collectors, self.tier2_interval),
            self._run_tier(3, self._tier3_collectors, self.tier3_interval),
        )

    def stop(self):
        self._running = False
        logger.info("TieredScanScheduler stopped")
