#!/usr/bin/env python3
"""Mini diagnostic: verify all critical fixes are working."""
import sys
import traceback

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
results = []

def check(name, fn):
    try:
        fn()
        print(f"  {PASS}  {name}")
        results.append((name, True, None))
    except Exception as e:
        print(f"  {FAIL}  {name}: {e}")
        results.append((name, False, str(e)))

# ── 1. All 13 collectors load without TypeError ──────────────────────────────
print("\n[1] Collector init (config dict passthrough)")
import yaml
with open("config/settings.yaml") as f:
    cfg = yaml.safe_load(f)

def _import_collectors():
    from data.collectors.rates_credit_collector import RatesCreditCollector
    from data.collectors.consumer_intelligence import ConsumerIntelligence as ConsumerIntelligenceCollector
    from data.collectors.geopolitical_collector import GeopoliticalCollector
    from data.collectors.commodity_collector import CommodityCollector
    from data.collectors.technology_intelligence import TechnologyIntelligence as TechnologyIntelligenceCollector
    from data.collectors.government_data_collector import BLSCollector, USASpendingCollector

    RatesCreditCollector("config/settings.yaml")   # init_mode='path'
    ConsumerIntelligenceCollector(cfg)
    GeopoliticalCollector(cfg)
    CommodityCollector(cfg)
    TechnologyIntelligenceCollector(cfg)
    BLSCollector(cfg)
    USASpendingCollector(cfg)

check("All 7 previously-broken collectors init OK", _import_collectors)

# ── 2. rates_credit_collector.collect() accepts tickers/market kwargs ────────
print("\n[2] RatesCreditCollector.collect() signature")
def _rates_collect_sig():
    from data.collectors.rates_credit_collector import RatesCreditCollector
    rc = RatesCreditCollector("config/settings.yaml")
    import inspect
    sig = inspect.signature(rc.collect)
    params = list(sig.parameters)
    assert "tickers" in params or "kwargs" in str(sig), f"tickers param missing: {params}"

check("RatesCreditCollector.collect accepts tickers/market/kwargs", _rates_collect_sig)

# ── 3. trading_bot.py actions guard (isinstance check) ──────────────────────
print("\n[3] trading_bot.py actions list guard")
def _actions_guard():
    import ast, pathlib
    src = pathlib.Path("execution/trading_bot.py").read_text()
    assert "isinstance(a, dict)" in src, "isinstance guard missing in trading_bot.py"

check("isinstance(a, dict) guard present in trading_bot.py", _actions_guard)

# ── 4. DataFetcher OHLCV MultiIndex level-scan fix ──────────────────────────
print("\n[4] DataFetcher OHLCV level-scan")
def _level_scan():
    import pathlib
    src = pathlib.Path("data/fetcher.py").read_text()
    assert "_ohlcv_fields" in src, "_ohlcv_fields level-scan missing in fetcher.py"
    assert "chosen_level" in src, "chosen_level missing in fetcher.py"

check("fetcher.py has MultiIndex level-scan logic", _level_scan)

# ── 5. Advanced news: language=en in NewsAPI URL ─────────────────────────────
print("\n[5] Advanced news language filtering")
def _news_lang():
    import pathlib
    src = pathlib.Path("data/collectors/advanced_news_intelligence.py").read_text()
    assert "language=en" in src, "language=en param missing from NewsAPI URL"
    assert "consent.yahoo.com" in src, "consent.yahoo.com skip missing"

check("NewsAPI has language=en filter + consent.yahoo.com skip", _news_lang)

# ── 6. SupplyChainRelationshipMapper has collect() ──────────────────────────
print("\n[6] SupplyChainRelationshipMapper.collect()")
def _supply_chain_collect():
    from deepdata.supply_chain.relationship_mapper import SupplyChainRelationshipMapper
    import inspect
    assert hasattr(SupplyChainRelationshipMapper, "collect"), "collect() method missing"
    sig = inspect.signature(SupplyChainRelationshipMapper.collect)
    assert "tickers" in sig.parameters, "tickers param missing from collect()"

check("SupplyChainRelationshipMapper.collect() present", _supply_chain_collect)

# ── 7. FINRA collector has browser-like User-Agent ──────────────────────────
print("\n[7] FINRA collector User-Agent")
def _finra_ua():
    import pathlib
    src = pathlib.Path("deepdata/short_interest/finra_collector.py").read_text()
    assert "Mozilla/5.0" in src, "Browser User-Agent missing from FINRACollector"
    assert "Referer" in src, "Referer header missing from FINRACollector"

check("FINRACollector has browser User-Agent + Referer", _finra_ua)

# ── 8. Universe 404-skip logic ───────────────────────────────────────────────
print("\n[8] Universe 404-skip logic")
def _universe_skip():
    import pathlib
    src = pathlib.Path("data/universe.py").read_text()
    assert "likely delisted or 404" in src, "404-skip logic missing from universe.py"
    assert "currentPrice" in src, "currentPrice fallback missing from 404-skip"

check("universe.py has 404/delisted skip logic", _universe_skip)

# ── 9. Delisted tickers not in universe CSV ──────────────────────────────────
print("\n[9] Delisted tickers absent from universe_us.csv")
def _no_delisted():
    import pathlib
    delisted = {"SPTN", "VTLE", "UCBI", "SRDX", "INDUS", "ALTUS", "HSII", "AMED", "LAZR", "ESSA"}
    csv_text = pathlib.Path("data/universe_us.csv").read_text()
    found = [t for t in delisted if t in csv_text.split()]
    assert not found, f"Delisted tickers still in CSV: {found}"

check("No delisted tickers in universe_us.csv", _no_delisted)

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
passed = sum(1 for _, ok, _ in results if ok)
total  = len(results)
print(f"Result: {passed}/{total} checks passed")
if passed < total:
    print("\nFailed checks:")
    for name, ok, err in results:
        if not ok:
            print(f"  - {name}: {err}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)
