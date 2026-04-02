"""
EarningsCache — bulk-fetch earnings data once per day via Finnhub.

Instead of yf.Ticker(t).earnings_history per ticker (7s each),
this fetches the Finnhub earnings calendar in a single API call
(covering all tickers for the past 3 years), caches to JSON,
and serves lookups in <1ms.

Cache is rebuilt once per day on first use.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml

logger = logging.getLogger(__name__)

CACHE_FILE = "output/earnings_cache.json"


class EarningsCache:
    def __init__(self, config: dict):
        self.config = config
        self.cache: Dict[str, List[dict]] = {}
        self.cache_date: Optional[str] = None
        self._load_cache()

    # ------------------------------------------------------------------
    def _load_cache(self) -> None:
        if Path(CACHE_FILE).exists():
            try:
                with open(CACHE_FILE) as f:
                    data = json.load(f)
                self.cache_date = data.get("date")
                self.cache = data.get("earnings", {})
                logger.info("EarningsCache loaded: %d tickers (date=%s)",
                            len(self.cache), self.cache_date)
            except Exception as e:
                logger.warning("EarningsCache load failed: %s", e)
                self.cache = {}

    def _save_cache(self) -> None:
        os.makedirs("output", exist_ok=True)
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump({"date": datetime.now().strftime("%Y-%m-%d"),
                           "earnings": self.cache}, f)
            logger.info("EarningsCache saved: %d tickers", len(self.cache))
        except Exception as e:
            logger.warning("EarningsCache save failed: %s", e)

    def needs_refresh(self) -> bool:
        today = datetime.now().strftime("%Y-%m-%d")
        # Rebuild if: new day OR cache is completely empty (< 10 tickers with data)
        # Don't rebuild mid-day just for low coverage — Finnhub free tier caps at 1500 events
        covered = sum(1 for v in self.cache.values() if v)
        return self.cache_date != today or covered < 10

    def get_earnings(self, ticker: str) -> List[dict]:
        return self.cache.get(ticker, [])

    # ------------------------------------------------------------------
    def bulk_fetch(self, tickers: List[str]) -> Dict[str, List[dict]]:
        """
        Rebuild cache via two methods:
        1. Finnhub earnings calendar (one API call, covers all tickers)
        2. yfinance fallback for any tickers still missing (max 200)
        """
        logger.info("EarningsCache.bulk_fetch: building cache for %d tickers", len(tickers))
        print(f"Building earnings cache for {len(tickers)} tickers...")

        api_key = self.config.get("api_keys", {}).get("finnhub", "")
        start = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
        end   = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")

        # ── Method 1: Finnhub bulk calendar ──────────────────────────
        finnhub_ok = False
        if api_key:
            try:
                r = requests.get(
                    "https://finnhub.io/api/v1/calendar/earnings",
                    params={"from": start, "to": end, "token": api_key},
                    timeout=30,
                )
                if r.status_code == 200:
                    events = r.json().get("earningsCalendar", [])
                    for ev in events:
                        sym = ev.get("symbol", "")
                        if not sym:
                            continue
                        if sym not in self.cache:
                            self.cache[sym] = []
                        eps_actual   = ev.get("epsActual")
                        eps_estimate = ev.get("epsEstimate")
                        if eps_actual is None or eps_estimate is None:
                            continue
                        self.cache[sym].append({
                            "date": ev.get("date", ""),
                            "epsActual": float(eps_actual),
                            "epsEstimate": float(eps_estimate),
                            "revenueActual": ev.get("revenueActual"),
                            "revenueEstimate": ev.get("revenueEstimate"),
                            "source": "finnhub_bulk",
                        })
                    finnhub_ok = True
                    print(f"  Finnhub bulk: {len(events)} events → "
                          f"{len(self.cache)} tickers covered")
                else:
                    logger.warning("Finnhub calendar HTTP %d", r.status_code)
            except Exception as e:
                logger.warning("Finnhub bulk fetch failed: %s", e)

        # ── Method 2: yfinance for tickers with signals but no cache ──
        missing = [t for t in tickers if not self.cache.get(t)]
        priority = missing[:200]
        if priority:
            print(f"  yfinance fallback for {len(priority)} uncached tickers...")
            import yfinance as yf
            for i, ticker in enumerate(priority):
                try:
                    hist = yf.Ticker(ticker).earnings_history
                    if hist is not None and not hist.empty:
                        records = []
                        for _, row in hist.iterrows():
                            try:
                                ea = float(row.get("epsActual", 0) or 0)
                                ee = float(row.get("epsEstimate", 0) or 0)
                                records.append({
                                    "date": str(row.name.date()) if hasattr(row.name, "date") else str(row.name),
                                    "epsActual": ea,
                                    "epsEstimate": ee,
                                    "source": "yfinance",
                                })
                            except Exception:
                                pass
                        self.cache[ticker] = records
                    else:
                        self.cache[ticker] = []
                    time.sleep(0.05)
                except Exception:
                    self.cache[ticker] = []
                if i % 50 == 49:
                    print(f"    yfinance: {i+1}/{len(priority)} done")

        self.cache_date = datetime.now().strftime("%Y-%m-%d")
        self._save_cache()
        covered = sum(1 for v in self.cache.values() if v)
        print(f"EarningsCache ready: {len(self.cache)} tickers, "
              f"{covered} with data")
        return self.cache
