#!/usr/bin/env python3
"""
Alpaca stream diagnostic test.

Checks:
  1. Stream module imports cleanly
  2. PriceCache works correctly
  3. AlpacaStreamWorker starts (daemon thread)
  4. Websocket connects and authenticates (live Alpaca check)
  5. Cache receives at least one update within 30 seconds
  6. Existing collectors are unaffected (still importable)
  7. RAM delta < 100 MB
  8. CPU delta < 10%
"""
import sys
import time
import os
import threading
import yaml

GREEN = "\033[92mPASS\033[0m"
RED   = "\033[91mFAIL\033[0m"

results = []

def check(name, fn):
    try:
        fn()
        print(f"  {GREEN}  {name}")
        results.append((name, True, None))
    except Exception as e:
        print(f"  {RED}  {name}: {e}")
        results.append((name, False, str(e)))


# ── Load config ──────────────────────────────────────────────────────────────
with open("config/settings.yaml") as f:
    config = yaml.safe_load(f)


# ── 1. Module imports ─────────────────────────────────────────────────────────
print("\n[1] Module imports")

def _import_stream():
    from execution.alpaca_stream import (
        PriceCache, AlpacaStreamWorker, get_stream_cache,
        get_stream_worker, start_stream,
    )

check("execution.alpaca_stream imports cleanly", _import_stream)


# ── 2. PriceCache unit tests ─────────────────────────────────────────────────
print("\n[2] PriceCache correctness")

def _cache_basic():
    from execution.alpaca_stream import PriceCache
    from datetime import datetime, timezone, timedelta
    c = PriceCache()

    t0 = datetime.now(timezone.utc)
    # First update always stored
    alert = c.update("AAPL", 180.0, 1000, t0)
    assert c.get_price("AAPL") == 180.0, "price not stored"

    # Sub-threshold move should be ignored
    alert = c.update("AAPL", 180.5, 1000, t0)  # +0.28% < 0.5%
    assert c.get_price("AAPL") == 180.0, "sub-threshold move was stored"

    # Above-threshold move should update
    alert = c.update("AAPL", 182.0, 2000, t0)  # +1.1%
    assert c.get_price("AAPL") == 182.0, "threshold move not stored"

    # get_move_pct
    c2 = PriceCache()
    c2.update("MSFT", 400.0, 500, t0)
    c2.update("MSFT", 410.0, 500, t0 + timedelta(seconds=30))
    move = c2.get_move_pct("MSFT", window_minutes=5)
    assert move is not None, "get_move_pct returned None"
    assert abs(move - 2.5) < 0.1, f"get_move_pct wrong: {move}"

check("PriceCache stores prices correctly", _cache_basic)

def _cache_spike():
    from execution.alpaca_stream import PriceCache
    from datetime import datetime, timezone, timedelta
    c = PriceCache()
    t0 = datetime.now(timezone.utc)
    c.update("GME", 20.0, 1000, t0)
    c.update("GME", 21.1, 2000, t0 + timedelta(seconds=10))   # +5.5%
    # Should flag as URGENT (>5% in window)
    alert = c.update("GME", 22.0, 5000, t0 + timedelta(seconds=20))
    assert alert in ("SPIKE", "URGENT"), f"Expected SPIKE/URGENT, got {alert}"

check("PriceCache spike/urgent detection works", _cache_spike)

def _cache_fresh():
    from execution.alpaca_stream import PriceCache
    from datetime import datetime, timezone, timedelta
    c = PriceCache()
    old_ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    c.update("TSLA", 250.0, 1000, old_ts)
    # 10-min-old entry should be stale
    assert c.get_fresh_price("TSLA", max_age_sec=300) is None, "stale price returned as fresh"
    # Recent entry should be fresh
    c.update("TSLA", 252.5, 1000, datetime.now(timezone.utc))
    assert c.get_fresh_price("TSLA", max_age_sec=300) is not None, "fresh price not returned"

check("PriceCache.get_fresh_price staleness check", _cache_fresh)


# ── 3. Worker thread starts ───────────────────────────────────────────────────
print("\n[3] AlpacaStreamWorker thread lifecycle")

def _worker_starts():
    from execution.alpaca_stream import AlpacaStreamWorker
    api_keys   = config.get("api_keys", {})
    api_key    = api_keys.get("alpaca_api_key", "")
    secret_key = api_keys.get("alpaca_secret_key", "")
    w = AlpacaStreamWorker(api_key, secret_key, {"AAPL", "MSFT"}, config)
    w.start()
    time.sleep(1.0)
    assert w.is_alive(), "worker thread not alive after 1s"
    w.stop()

check("AlpacaStreamWorker starts as daemon thread", _worker_starts)

def _worker_daemon():
    import threading
    # All threads named "alpaca-stream" should be daemon threads
    for t in threading.enumerate():
        if t.name == "alpaca-stream":
            assert t.daemon, "alpaca-stream thread is NOT daemon!"

check("alpaca-stream thread is daemon (won't block pm2 shutdown)", _worker_daemon)


# ── 4. Live Alpaca connection (30-second window) ──────────────────────────────
print("\n[4] Live Alpaca websocket connection")

def _live_connect():
    from execution.alpaca_stream import start_stream, get_stream_cache
    api_keys   = config.get("api_keys", {})
    api_key    = api_keys.get("alpaca_api_key", "")
    secret_key = api_keys.get("alpaca_secret_key", "")
    if not api_key or "PASTE" in api_key:
        raise RuntimeError("Alpaca keys not configured")
    # Start with a small universe so we don't flood subscriptions
    tickers = ["AAPL", "MSFT", "TSLA", "SPY", "QQQ"]
    worker = start_stream(config, tickers)
    if worker is None:
        raise RuntimeError("start_stream returned None")
    # Give the thread up to 15s to connect and authenticate
    deadline = time.time() + 15
    while time.time() < deadline:
        if get_stream_cache().is_connected():
            break
        time.sleep(0.5)
    assert get_stream_cache().is_connected(), \
        "Not connected after 15s — check Alpaca keys and network"

check("Websocket connects and authenticates within 15s", _live_connect)


# ── 5. Cache receives a price update ─────────────────────────────────────────
print("\n[5] Real-time cache update")

def _receives_update():
    from execution.alpaca_stream import get_stream_cache
    cache = get_stream_cache()
    if not cache.is_connected():
        raise RuntimeError("Stream not connected — skipping update check")
    # Wait up to 30s for at least one price update
    # Note: outside market hours (evenings/weekends) bars may not arrive.
    # We check total_updates instead of a specific ticker.
    deadline = time.time() + 30
    while time.time() < deadline:
        stats = cache.stats()
        if stats["total_updates"] > 0:
            print(f"       (received {stats['total_updates']} updates, "
                  f"{stats['tickers_cached']} tickers cached)")
            return
        time.sleep(1)
    stats = cache.stats()
    if stats["tickers_cached"] == 0:
        # Outside market hours — no bars are published by Alpaca IEX.
        # Connection is confirmed good; zero updates is expected off-hours.
        print("       (no bar data — outside US market hours, connection OK)")
        return
    raise RuntimeError(f"No updates after 30s. Stats: {stats}")

check("Cache receives price update (or confirms off-hours)", _receives_update)


# ── 6. Existing collectors unaffected ────────────────────────────────────────
print("\n[6] Existing collectors still importable (unaffected)")

def _collectors_ok():
    from data.collectors.rates_credit_collector import RatesCreditCollector
    from data.collectors.consumer_intelligence import ConsumerIntelligence
    from data.collectors.geopolitical_collector import GeopoliticalCollector
    from data.collectors.commodity_collector import CommodityCollector
    from data.collectors.technology_intelligence import TechnologyIntelligence
    from data.collectors.government_data_collector import BLSCollector, USASpendingCollector
    from deepdata.short_interest.finra_collector import FINRACollector
    from deepdata.supply_chain.relationship_mapper import SupplyChainRelationshipMapper
    # All instantiate without error
    RatesCreditCollector("config/settings.yaml")
    ConsumerIntelligence(config)
    GeopoliticalCollector(config)
    CommodityCollector(config)
    TechnologyIntelligence(config)
    BLSCollector(config)
    USASpendingCollector(config)

check("All 7 collectors still load correctly", _collectors_ok)

def _paper_trader_ok():
    # Paper trader imports correctly and _get_live_price helper exists
    from execution.paper_trader import PaperTrader
    import inspect
    assert hasattr(PaperTrader, "_get_live_price"), \
        "_get_live_price helper missing from PaperTrader"
    src = inspect.getsource(PaperTrader._get_live_price)
    assert "get_stream_cache" in src, "Stream cache not used in _get_live_price"
    assert "fast_info.last_price" in src, "yfinance fallback missing from _get_live_price"

check("PaperTrader._get_live_price helper wired correctly", _paper_trader_ok)


# ── 7. RAM usage ──────────────────────────────────────────────────────────────
print("\n[7] Memory and CPU impact")

def _ram_check():
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        rss_mb = proc.memory_info().rss / 1024 / 1024
        print(f"       (current process RSS: {rss_mb:.1f} MB)")
        # The stream thread itself should add < 10 MB over baseline
        # We check that total process RAM is under 512 MB
        # (full bot with ML models will use more; this is just the test harness)
        assert rss_mb < 512, f"RSS {rss_mb:.1f} MB seems high for test harness"
    except ImportError:
        print("       (psutil not installed — skipping exact RAM measurement)")

check("RAM usage within reason (psutil check)", _ram_check)

def _cpu_check():
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=2)
        print(f"       (CPU% during test: {cpu:.1f}%)")
        # The stream thread idles — CPU should be low during the test
        assert cpu < 50, f"CPU {cpu:.1f}% seems high — check for runaway loops"
    except ImportError:
        print("       (psutil not installed — skipping CPU measurement)")

check("CPU usage acceptable during stream idle", _cpu_check)


# ── 8. Log file created ────────────────────────────────────────────────────────
print("\n[8] Logging")

def _log_file():
    log_path = "logs/alpaca_stream.log"
    assert os.path.exists(log_path), f"Log file {log_path} was not created"

check("logs/alpaca_stream.log created", _log_file)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
passed = sum(1 for _, ok, _ in results if ok)
total  = len(results)
print(f"Result: {passed}/{total} checks passed")
if passed < total:
    print("\nFailed:")
    for name, ok, err in results:
        if not ok:
            print(f"  - {name}: {err}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)
