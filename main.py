"""
Quant Fund — entry point.

Commands
--------
  python main.py backtest      [--market us|uk|both] [--tickers-file PATH]
  python main.py anomaly_scan  [--market us|uk|both]
  python main.py paper_trade
  python main.py report        [--prefix backtest_us]
  python main.py promote       --signal SIGNAL_NAME
  python main.py validate

  python main.py altdata collect   [--tickers AAPL MSFT ...]
  python main.py altdata signals   [--tickers AAPL MSFT ...]
  python main.py altdata dashboard
  python main.py altdata nonsense
  python main.py altdata rollback  --to-version VERSION
  python main.py altdata status

  python main.py deepdata status
  python main.py deepdata collect   [--tickers AAPL MSFT ...]
  python main.py deepdata dashboard
  python main.py deepdata options   --ticker TICKER
  python main.py deepdata squeeze   --ticker TICKER
  python main.py deepdata congress  [--days 30]
  python main.py deepdata patterns
  python main.py deepdata transcript --ticker TICKER

  python main.py frontier status
  python main.py frontier collect  [--tickers AAPL MSFT ...]
  python main.py frontier umci
  python main.py frontier dashboard
  python main.py frontier discover
  python main.py frontier watchlist
  python main.py frontier validate  --signal SIGNAL_NAME
  python main.py frontier geomagnetic
  python main.py frontier attention
  python main.py frontier quantum

  python main.py historical collect [--tickers HRMY METC SHEN] [--start 2010-01-01] [--phases prices financials edgar macro enrich news delisted]
  python main.py historical status
  python main.py historical delisted

  python main.py intelligence run
  python main.py intelligence report
  python main.py intelligence score SHEN
  python main.py intelligence morning
  python main.py intelligence close
  python main.py intelligence weekly
  python main.py intelligence status
  python main.py intelligence readthrough [--tickers HRMY METC SHEN] [--days 14]
"""
import argparse
import logging
import sys
from pathlib import Path

import yaml


def load_config(path: str = "config/settings.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def setup_logging() -> None:
    Path("logs").mkdir(exist_ok=True)
    Path("output").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/quant_fund.log"),
        ],
    )


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def _load_macro_regime(config: dict) -> int:
    """
    Load the latest macro regime from the altdata store.
    Returns regime int (0=RISK_ON … 4=RECESSION_RISK); defaults to 0 (RISK_ON).
    """
    try:
        from altdata.storage.altdata_store import AltDataStore
        store = AltDataStore(config)
        rows = store.get_raw(source="fred", ticker="MACRO", hours_back=24 * 7)  # last week
        import json as _json
        for row in rows:
            if row.get("data_type") != "macro_regime":
                continue
            raw = row.get("raw_data", {})
            if isinstance(raw, str):
                try:
                    raw = _json.loads(raw)
                except Exception:
                    continue
            if isinstance(raw, dict) and raw.get("regime_code") is not None:
                return int(raw["regime_code"])
    except Exception:
        pass
    return 0  # RISK_ON default


def _get_altdata_confluence(store, ticker: str) -> float:
    """Return recent altdata sentiment for ticker, or 0.0 if none."""
    try:
        rows = store.get_sentiment(ticker, hours_back=24 * 7)
        if rows:
            scores = [float(r.get("score", 0)) for r in rows if r.get("score") is not None]
            return sum(scores) / len(scores) if scores else 0.0
    except Exception:
        pass
    return 0.0


def cmd_backtest(config: dict, market: str, tickers_file: str = None, max_tickers: int = None) -> None:
    import pandas as pd

    from data.fetcher import DataFetcher
    from data.cleaner import DataCleaner
    from data.earnings_calendar import EarningsCalendar
    from data.universe import UniverseManager
    from signals.pead_signal import PEADSignal
    from signals.filters import SignalFilters
    from backtest.engine import BacktestEngine
    from backtest.monte_carlo import MonteCarloSimulator
    from reporting.analytics import Analytics
    from risk.manager import RiskManager

    log = logging.getLogger(__name__)
    log.info("=== Backtest [%s] ===", market.upper())

    fetcher = DataFetcher(config)
    cleaner = DataCleaner()
    universe = UniverseManager(config, fetcher)
    pead = PEADSignal(config)
    filters = SignalFilters(config)
    engine = BacktestEngine(config)
    mc_sim = MonteCarloSimulator(config)
    analytics = Analytics(config)
    cal = EarningsCalendar(config, fetcher)
    risk = RiskManager(config)

    # ── Load macro regime and wire into risk manager ──────────────────
    macro_regime = _load_macro_regime(config)
    risk.set_macro_regime(macro_regime)
    log.info("Macro regime: %d", macro_regime)

    # ── Load altdata store for confluence filtering ───────────────────
    altdata_store = None
    altdata_available = False
    try:
        from altdata.storage.altdata_store import AltDataStore
        altdata_store = AltDataStore(config)
        altdata_available = True
    except Exception as e:
        log.debug("AltDataStore unavailable: %s", e)

    # Altdata confluence threshold: only filter if store has data
    CONFLUENCE_THRESHOLD = config.get("signal", {}).get("pead", {}).get(
        "altdata_confluence_threshold", 0.0
    )  # 0.0 = disabled; set to e.g. -0.3 to block strong contra signals

    bt = config["backtest"]
    tickers = universe.get_universe(market, tickers_file=tickers_file)
    log.info("Universe: %d tickers", len(tickers))

    # Phase 12: Survivorship bias correction — include delisted companies
    if market == "us":
        try:
            from data.delisted_universe import get_delisted_tickers
            delisted = get_delisted_tickers()
            pre_len  = len(tickers)
            tickers  = list(dict.fromkeys(tickers + delisted))  # dedup, preserve order
            log.info(
                "Survivorship bias fix: added %d delisted tickers (total %d)",
                len(tickers) - pre_len, len(tickers)
            )
        except Exception as e:
            log.warning("Could not load delisted universe (survivorship fix skipped): %s", e)

    # Apply --max-tickers cap BEFORE fetching price data
    if max_tickers and len(tickers) > max_tickers:
        tickers = tickers[:max_tickers]
        log.info("Universe capped to %d tickers (--max-tickers)", max_tickers)

    price_data = fetcher.fetch_universe_data(
        tickers, bt["start_date"], bt["end_date"], market=market
    )
    log.info("Price data loaded for %d tickers", len(price_data))

    # Large cap readthrough signals (fail-open)
    readthrough_signals = {}
    try:
        from data.large_cap_influence import LargeCapInfluenceEngine
        lci = LargeCapInfluenceEngine()
        rt_results = lci.get_readthrough_signals(tickers, days_lookback=21)
        for sig in rt_results:
            peer_ticker = sig.get("peer_ticker")
            if peer_ticker:
                readthrough_signals[peer_ticker] = sig
        if readthrough_signals:
            log.info("Large-cap readthrough: %d signals", len(readthrough_signals))
    except Exception as e:
        log.debug("large_cap_influence failed: %s", e)

    all_signals = []
    n_tickers = len([t for t in tickers if t in price_data and not price_data[t].empty])
    for _bt_i, ticker in enumerate(tickers):
        if ticker not in price_data or price_data[ticker].empty:
            continue
        if _bt_i > 0 and (_bt_i % 50 == 0):
            log.info("Signal scan progress: %d/%d tickers processed", _bt_i, len(tickers))
        history = cal.get_earnings_surprise(ticker)
        if history.empty:
            continue
        signals = pead.generate(ticker, price_data[ticker], history)
        if signals.empty:
            continue

        # Apply readthrough multiplier if available
        if ticker in readthrough_signals:
            rt = readthrough_signals[ticker]
            rt_mult = float(rt.get("readthrough_score", 0.0))
            if rt_mult != 0.0:
                log.debug("%s: readthrough multiplier %.2f", ticker, rt_mult)
                # Boost surprise_pct proportionally (readthrough_score in [-1,+1])
                # Only apply if readthrough agrees with signal direction
                signals = signals.copy()
                # No modification needed for backtest — just informational

        # Filter each signal individually using its entry_date so that
        # sector_contagion and earnings_quality use historical data at
        # the time of the signal, not live data today.
        passing = []
        for _, sig_row in signals.iterrows():
            # Use earnings_date (not entry_date) so sector_contagion checks
            # the 5 trading days BEFORE the earnings announcement.
            sig_date = sig_row.get("earnings_date")
            filt = filters.run_all(ticker, market, price_data[ticker],
                                   signal_date=sig_date)
            if not filt["passed"]:
                log.debug("Filtered %s @ %s: %s", ticker, sig_date, filt["failures"])
                continue

            # ── Altdata confluence gate ───────────────────────────────
            if altdata_available and CONFLUENCE_THRESHOLD != 0.0:
                confluence = _get_altdata_confluence(altdata_store, ticker)
                direction = int(sig_row.get("signal", 1))
                # Block if confluence is strongly against signal direction
                if confluence * direction < CONFLUENCE_THRESHOLD:
                    log.debug(
                        "Altdata confluence gate blocked %s: direction=%+d confluence=%.3f",
                        ticker, direction, confluence,
                    )
                    continue

            # ── SignalAggregator check (fail-open) ────────────────────
            try:
                from closeloop.integration.signal_aggregator import SignalAggregator
                agg = SignalAggregator(config=config)
                pead_sig_dict = sig_row.to_dict()
                agg_result = agg.aggregate(
                    ticker=ticker,
                    pead_signal=pead_sig_dict,
                )
                if agg_result.get("confluence_level") == "NONE" and agg_result.get("signal_count", 0) > 0:
                    log.debug(
                        "%s: SignalAggregator confluence=NONE, skipping",
                        ticker,
                    )
                    # Only block if there are other non-PEAD signals contradicting;
                    # with only a PEAD signal the aggregator always returns STRONG.
                    # So this effectively only blocks when other sources conflict.
            except Exception:
                pass  # fail open, use original signal

            passing.append(sig_row)
        if passing:
            all_signals.append(pd.DataFrame(passing))

    if not all_signals:
        log.warning("No signals generated — check universe and earnings data")
        return

    signals_df = pd.concat(all_signals, ignore_index=True)
    log.info("Total signals: %d", len(signals_df))

    results = engine.run(signals_df, price_data, market=market)

    if not results["trades"].empty:
        mc_out = mc_sim.run(results["trades"]["return"])
        results["monte_carlo"] = mc_out
        log.info(
            "Monte Carlo: prob_profit=%.1f%% prob_ruin=%.1f%%",
            mc_out.get("prob_profit", 0) * 100,
            mc_out.get("prob_ruin", 0) * 100,
        )

    # Optionally compare against benchmark
    benchmark = None
    bm_ticker = bt.get(f"benchmark_{market}")
    if bm_ticker:
        bm_data = fetcher.fetch_ohlcv(bm_ticker, bt["start_date"], bt["end_date"])
        if not bm_data.empty:
            benchmark = bm_data["close"]

    metrics = analytics.compute_metrics(
        results["equity_curve"], results["trades"], benchmark
    )
    results["metrics"] = metrics

    prefix = f"backtest_{market}"
    report = analytics.generate_report(results, f"{prefix}_report.md")
    print(report)

    # ── Sub-period analysis ────────────────────────────────────────────
    try:
        from backtest.subperiod_analysis import SubperiodAnalyser
        analyser = SubperiodAnalyser()
        sp_result = analyser.analyse(results["equity_curve"], results["trades"])
        print("\n" + analyser.format_report(sp_result))
        import json as _json
        sp_path = Path(f"output/{prefix}_subperiods.json")
        sp_path.write_text(_json.dumps(
            {p.label: p.__dict__ for p in sp_result["periods"]}, indent=2
        ))
        log.info("Sub-period analysis saved to %s", sp_path)
    except Exception as _sp_exc:
        log.debug("Sub-period analysis skipped: %s", _sp_exc)

    analytics.save_results(results, prefix=prefix)
    analytics.plot_equity_curve(
        results["equity_curve"], benchmark, title=f"PEAD — {market.upper()}"
    )
    analytics.plot_trade_analysis(results["trades"], prefix=prefix)


def cmd_anomaly_scan(config: dict, market: str) -> None:
    import pandas as pd

    from data.fetcher import DataFetcher
    from data.cleaner import DataCleaner
    from data.universe import UniverseManager
    from signals.anomaly_scanner import AnomalyScanner
    from signals.signal_validator import SignalValidator
    from signals.signal_registry import SignalRegistry

    log = logging.getLogger(__name__)
    log.info("=== Anomaly Scan [%s] ===", market.upper())

    fetcher = DataFetcher(config)
    cleaner = DataCleaner()
    universe = UniverseManager(config, fetcher)
    scanner = AnomalyScanner(config)
    validator = SignalValidator(config)
    registry = SignalRegistry(config)

    bt = config["backtest"]
    tickers = universe.get_universe(market)
    price_data = fetcher.fetch_universe_data(
        tickers, bt["start_date"], bt["end_date"], market=market
    )

    returns = pd.DataFrame(
        {t: cleaner.compute_returns(df) for t, df in price_data.items() if not df.empty}
    ).dropna(how="all")

    log.info("Returns matrix: %d dates × %d tickers", *returns.shape)

    anomalies = scanner.scan(returns)
    anomalies = scanner.deduplicate(anomalies)

    promoted = 0
    for i, anomaly in enumerate(anomalies):
        name = f"{anomaly['type']}_{market}_{i:03d}"
        val = validator.validate(anomaly["returns_series"])
        meta = {
            "sharpe": anomaly["sharpe"],
            "n_obs": anomaly["n_obs"],
            "mean_return": anomaly["mean_return"],
            "params": anomaly["params"],
            "validation": val,
        }
        registry.register(name, anomaly["type"], anomaly["params"], meta)

        if val["passed"]:
            registry.promote(name, val)
            promoted += 1
            log.info(
                "  [PASS] %s  sharpe=%.2f  val_sharpe=%.2f",
                name, anomaly["sharpe"], val["val_sharpe"],
            )
        else:
            log.info(
                "  [FAIL] %s  sharpe=%.2f  reason=%s",
                name, anomaly["sharpe"], val.get("reason"),
            )

    summary = registry.list_all()
    if not summary.empty:
        print("\n" + summary[["name", "type", "status", "sharpe", "n_obs"]].to_string(index=False))

    log.info("Scan complete: %d candidates, %d validated/promoted", len(anomalies), promoted)


def cmd_paper_trade(config: dict) -> None:
    from data.fetcher import DataFetcher
    from signals.pead_signal import PEADSignal
    from risk.manager import RiskManager
    from execution.broker_interface import PaperBroker
    from execution.paper_trader import PaperTrader

    fetcher = DataFetcher(config)
    risk = RiskManager(config)
    broker = PaperBroker(config["backtest"]["initial_capital"], config)
    generators = {"pead": PEADSignal(config)}

    trader = PaperTrader(config, fetcher, generators, risk, broker)
    trader.run()


def cmd_paper_trade_once(config: dict) -> None:
    """Run one US scan immediately, print results, and exit."""
    import json as _json
    from data.fetcher import DataFetcher
    from signals.pead_signal import PEADSignal
    from risk.manager import RiskManager
    from execution.broker_interface import PaperBroker
    from execution.paper_trader import PaperTrader

    log = logging.getLogger(__name__)
    log.info("=== paper_trade --once: running one US scan ===")

    fetcher = DataFetcher(config)
    risk = RiskManager(config)
    broker = PaperBroker(config["backtest"]["initial_capital"], config)
    # --once uses fast local generators only (PEAD requires per-ticker API calls; use bot for full scan)
    generators = {}

    trader = PaperTrader(config, fetcher, generators, risk, broker)
    # Cap at 100 tickers; skip INSIDER_MOM (SEC API per ticker); use bot for full scan
    actions = trader.run_scan(market="us", limit=100, skip_slow_generators=True)

    print("\n" + "=" * 70)
    print("  PAPER TRADE SCAN — ONE-SHOT RESULTS  (200-ticker sample)")
    print("=" * 70)

    # Signal type breakdown
    by_type: dict = {}
    by_signal_type: dict = {}
    for action in actions:
        atype = action.get("type", "?")
        if atype == "trade_open":
            label = "OPENED"
        elif atype == "observed_not_traded":
            label = "SKIPPED"
        else:
            label = atype.upper()
        by_type[label] = by_type.get(label, 0) + 1
        sig_type = action.get("signal_type", action.get("signal", {}).get("signal_type", "PEAD"))
        by_signal_type[sig_type] = by_signal_type.get(sig_type, 0) + 1

    if not actions:
        print("  No signals generated in this scan.")
    else:
        summary = "  Actions:  " + "  |  ".join(f"{k}: {v}" for k, v in sorted(by_type.items()))
        sig_summary = "  By type:  " + "  |  ".join(f"{k}: {v}" for k, v in sorted(by_signal_type.items()))
        try:
            phase_info = trader.sizer.get_phase_summary() if trader.sizer else "N/A"
            print(f"  Phase:    {phase_info}")
        except Exception:
            pass
        print(sig_summary)
        print(summary)
        print("  " + "-" * 66)
        for action in actions:
            atype = action.get("type", "?")
            ticker = action.get("ticker", "?")
            if atype == "trade_open":
                print(
                    f"  OPEN   {ticker:<8} direction={action.get('direction','?'):<5} "
                    f"price={action.get('price', 0):.4f}  shares={action.get('shares', 0):.2f}"
                )
            elif atype == "observed_not_traded":
                print(
                    f"  SKIP   {ticker:<8} confidence={action.get('confidence', 0):.3f} "
                    f"(threshold={action.get('min_confidence', 0):.2f})"
                )
            else:
                print(f"  {atype.upper():<7} {ticker:<8} {_json.dumps({k: v for k, v in action.items() if k not in ('type','ticker')})}")
    print("=" * 70 + "\n")


def cmd_paper_trade_status(config: dict) -> None:
    """Show open positions, macro regime, signal type breakdown, and phase status."""
    import json as _json
    log = logging.getLogger(__name__)
    print("\n" + "=" * 70)
    print("  PAPER TRADE — STATUS")
    print("=" * 70)

    # ── Macro regime ────────────────────────────────────────────────────
    try:
        from analysis.macro_signal_engine import MacroSignalEngine
        mse = MacroSignalEngine(config_path="config/settings.yaml")
        briefing = mse.get_complete_briefing_data()
        regime_name = briefing.get("regime", "UNKNOWN")
        regime_conf = briefing.get("regime_confidence", 0.0)
        pead_mult = briefing.get("pead_multiplier", 1.0)
        print(f"  Macro Regime:  {regime_name}  (confidence={regime_conf:.0%}  PEAD_mult={pead_mult:.2f}x)")
    except Exception as e:
        log.debug("Macro regime unavailable: %s", e)
        print("  Macro Regime:  unavailable")

    # ── Phase status (signal counts from recent log) ────────────────────
    log_path = Path("logs/paper_trading.jsonl")
    sig_counts: dict = {}
    total_opens = 0
    total_skips = 0
    if log_path.exists():
        try:
            with open(log_path) as f:
                for line in f:
                    try:
                        entry = _json.loads(line.strip())
                        etype = entry.get("type", "")
                        if etype == "trade_open":
                            total_opens += 1
                            stype = entry.get("signal_type", "pead")
                            sig_counts[stype] = sig_counts.get(stype, 0) + 1
                        elif etype == "observed_not_traded":
                            total_skips += 1
                    except Exception:
                        pass
        except Exception:
            pass
    print(f"  Trades opened: {total_opens}  |  Skipped (low conf): {total_skips}")
    if sig_counts:
        print("  Signals by type: " + "  |  ".join(f"{k.upper()}: {v}" for k, v in sorted(sig_counts.items())))

    # ── Open positions ──────────────────────────────────────────────────
    print()
    print("  OPEN POSITIONS")
    print("  " + "-" * 65)
    try:
        from closeloop.storage.closeloop_store import ClosedLoopStore
        store = ClosedLoopStore(config)
        trades = store.get_trades(n=500)
        open_trades = [t for t in trades if not t.get("exit_date")]
        if not open_trades:
            print("  No open positions found in closeloop_store.")
        else:
            print(f"  {'Ticker':<10} {'Market':<6} {'Direction':<10} {'Entry Price':>12} {'Entry Date':<22}")
            print("  " + "-" * 65)
            for t in open_trades:
                direction_str = "LONG" if (t.get("direction", 1) or 1) > 0 else "SHORT"
                print(
                    f"  {t.get('ticker','?'):<10} {t.get('market','?'):<6} {direction_str:<10} "
                    f"{(t.get('entry_price') or 0):>12.4f} {str(t.get('entry_date','?'))[:19]:<22}"
                )
    except Exception as e:
        log.warning("Could not read from closeloop_store: %s", e)
        print(f"  Error reading positions: {e}")
    print("=" * 70 + "\n")


def cmd_paper_trade_history(config: dict) -> None:
    """Show last 20 closed trades from paper_trading.jsonl."""
    import json as _json
    log = logging.getLogger(__name__)
    log_path = Path("logs/paper_trading.jsonl")
    print("\n" + "=" * 70)
    print("  PAPER TRADE — LAST 20 CLOSED TRADES")
    print("=" * 70)
    if not log_path.exists():
        print("  No paper trading log found (logs/paper_trading.jsonl).")
        print("=" * 70 + "\n")
        return
    try:
        records = []
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = _json.loads(line)
                    if entry.get("type") == "trade_close":
                        records.append(entry)
                except Exception:
                    pass
        closes = records[-20:]
        if not closes:
            print("  No closed trades found in log.")
        else:
            print(f"  {'Ticker':<8} {'Entry':>10} {'Exit':>10} {'Return%':>8} {'Days':>5} {'Reason':<16} {'Exit Date':<22}")
            print("  " + "-" * 80)
            for t in closes:
                ret_pct = (t.get("return_pct", 0) or 0) * 100
                print(
                    f"  {t.get('ticker','?'):<8} "
                    f"{(t.get('entry_price') or 0):>10.4f} "
                    f"{(t.get('exit_price') or 0):>10.4f} "
                    f"{ret_pct:>8.2f}% "
                    f"{(t.get('holding_days') or 0):>5} "
                    f"{str(t.get('exit_reason','?')):<16} "
                    f"{str(t.get('exit_date','?'))[:19]:<22}"
                )
    except Exception as e:
        log.warning("Error reading paper trading log: %s", e)
        print(f"  Error: {e}")
    print("=" * 70 + "\n")


def cmd_report(config: dict, prefix: str) -> None:
    import pandas as pd
    from reporting.analytics import Analytics

    log = logging.getLogger(__name__)
    analytics = Analytics(config)

    equity_path = Path(f"output/{prefix}_equity.csv")
    trades_path = Path(f"output/{prefix}_trades.csv")
    mc_path = Path(f"output/{prefix}_monte_carlo.json")

    if not equity_path.exists():
        log.error("Equity file not found: %s", equity_path)
        return

    equity = pd.read_csv(equity_path, index_col=0, parse_dates=True).squeeze()
    trades = pd.read_csv(trades_path) if trades_path.exists() else pd.DataFrame()

    results: dict = {"equity_curve": equity, "trades": trades, "metrics": {}}

    if mc_path.exists():
        import json
        results["monte_carlo"] = json.loads(mc_path.read_text())

    metrics = analytics.compute_metrics(equity, trades if not trades.empty else None)
    results["metrics"] = metrics

    report = analytics.generate_report(results)
    print(report)

    analytics.plot_equity_curve(equity, title=f"Strategy — {prefix}")
    if not trades.empty:
        analytics.plot_trade_analysis(trades, prefix=prefix)


def cmd_status(config: dict) -> None:
    """Show system health: API keys, collectors, DB counts, upcoming earnings, health score."""
    import json, sqlite3, os, time, requests
    log = logging.getLogger(__name__)

    print("\n" + "=" * 66)
    print("  QUANT FUND — SYSTEM STATUS")
    print("=" * 66)

    # ── 1. API Key Health ─────────────────────────────────────────────
    api_keys = config.get("api_keys", {})
    key_tests = {
        "finnhub":       ("https://finnhub.io/api/v1/quote?symbol=AAPL&token={key}", "c", None),
        "fred":          ("https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={key}&file_type=json&limit=1", None, "observations"),
        "alpha_vantage": ("https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={key}", "Global Quote", None),
        "news_api":      ("https://newsapi.org/v2/everything?q=stocks&apiKey={key}&pageSize=1", None, "articles"),
        "marketstack":   ("http://api.marketstack.com/v1/eod?access_key={key}&symbols=AAPL&limit=1", None, "data"),
    }
    key_limits = {
        "finnhub":       "60 calls/min",
        "fred":          "120 calls/min",
        "alpha_vantage": "25 calls/day",
        "news_api":      "100 calls/day",
        "marketstack":   "100 calls/month",
    }

    print("\n[1] API KEY STATUS")
    print(f"  {'Key':<18} {'Status':<14} {'Limit':<20} {'Detail'}")
    print(f"  {'-'*18} {'-'*14} {'-'*20} {'-'*20}")
    api_health_score = 0
    for key_name, (url_tpl, field_direct, field_list) in key_tests.items():
        key_val = api_keys.get(key_name, "")
        if not key_val:
            print(f"  {key_name:<18} {'MISSING':<14} {key_limits.get(key_name,''):<20} no key in config")
            continue
        try:
            url = url_tpl.replace("{key}", key_val)
            r = requests.get(url, timeout=8)
            data = r.json()
            if r.status_code == 200:
                if field_direct and data.get(field_direct):
                    status = "WORKING"
                    api_health_score += 20
                elif field_list and data.get(field_list) is not None:
                    status = "WORKING"
                    api_health_score += 20
                elif "Note" in data or "Information" in data:
                    status = "RATE LIMITED"
                    api_health_score += 10
                else:
                    status = "BAD RESPONSE"
            elif r.status_code == 401:
                status = "WRONG KEY"
            elif r.status_code == 429:
                status = "RATE LIMITED"
                api_health_score += 10
            else:
                status = f"HTTP {r.status_code}"
            detail = str(data)[:50] if status not in ("WORKING","RATE LIMITED") else ""
        except Exception as e:
            status = "ERROR"
            detail = str(e)[:50]
        print(f"  {key_name:<18} {status:<14} {key_limits.get(key_name,''):<20} {detail}")

    # ── 2. Database Health ────────────────────────────────────────────
    print("\n[2] DATABASE STATUS")
    print(f"  {'Database':<36} {'Size':>8} {'Tables':>8} {'Rows':>10}")
    print(f"  {'-'*36} {'-'*8} {'-'*8} {'-'*10}")
    db_list = [
        "output/altdata.db",
        "output/historical_db.db",
        "output/earnings.db",
        "deepdata/storage/deepdata.db",
        "frontier/storage/frontier.db",
        "closeloop/storage/closeloop.db",
        "output/permanent_log.db",
    ]
    db_health_score = 0
    for db_path in db_list:
        if not os.path.exists(db_path):
            print(f"  {db_path:<36} {'MISSING':>8}")
            continue
        size_kb = os.path.getsize(db_path) // 1024
        try:
            conn = sqlite3.connect(db_path)
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            total_rows = 0
            for (t,) in tables:
                try:
                    total_rows += conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]
                except Exception:
                    pass
            conn.close()
            if total_rows > 0:
                db_health_score += 10
            print(f"  {db_path:<36} {size_kb:>7}KB {len(tables):>8} {total_rows:>10,}")
        except Exception as e:
            print(f"  {db_path:<36} ERROR: {e}")

    # ── 3. Collector status (last 24h from DB) ────────────────────────
    print("\n[3] COLLECTOR STATUS (last 24h in DB)")
    collector_health = 0
    try:
        import sqlite3 as _sq
        conn = _sq.connect("output/altdata.db")
        rows = conn.execute(
            "SELECT source, COUNT(*) as n, MAX(collected_at) as last "
            "FROM raw_data GROUP BY source ORDER BY n DESC"
        ).fetchall()
        conn.close()
        if rows:
            print(f"  {'Source':<22} {'Records':>8} {'Last Seen':<24}")
            print(f"  {'-'*22} {'-'*8} {'-'*24}")
            for src, n, last in rows:
                print(f"  {src:<22} {n:>8} {str(last)[:19] if last else 'never'}")
                collector_health += min(10, n)
        else:
            print("  altdata.db raw_data is empty — run: python3 main.py altdata collect")
    except Exception as e:
        print(f"  ERROR reading altdata.db: {e}")

    # ── 4. Real-time stream status ────────────────────────────────────
    print("\n[4] REAL-TIME PRICE STREAM")
    try:
        stream_status = {}
        bot_status_path = "output/bot_status.json"
        if os.path.exists(bot_status_path):
            with open(bot_status_path) as _f:
                _bst = json.load(_f)
                stream_status = _bst.get("stream", {})
        connected      = stream_status.get("connected", False)
        tickers_cached = stream_status.get("tickers_cached", 0)
        total_updates  = stream_status.get("total_updates", 0)
        spike_flags    = stream_status.get("spike_flags", 0)
        last_update    = stream_status.get("last_update") or "never"
        status_label   = "\033[92mLIVE\033[0m" if connected else "\033[91mDISCONNECTED\033[0m"
        print(f"  Stream:       {status_label}")
        print(f"  Tickers:      {tickers_cached} cached")
        print(f"  Updates:      {total_updates:,} total")
        print(f"  Spike flags:  {spike_flags} active")
        print(f"  Last update:  {str(last_update)[:19]}")
        if not connected:
            print("  Fallback:     yfinance 15-min polling (normal when bot not running)")
    except Exception as _e:
        print(f"  Stream status unavailable: {_e}")

    # ── 5. Upcoming earnings (next 7 days, sample from universe) ─────
    print("\n[5] UPCOMING EARNINGS (next 7 days — sample 20 tickers)")
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        from data.universe import UniverseManager
        from data.fetcher import DataFetcher
        fetcher_q = DataFetcher(config)
        um = UniverseManager(config, fetcher_q)
        sample = um._default_tickers("us")[:20]
        today = datetime.now().date()
        upcoming = []
        for t in sample:
            try:
                cal = yf.Ticker(t).calendar
                if cal is None:
                    continue
                dates = []
                if isinstance(cal, dict):
                    dates = cal.get("Earnings Date", [])
                elif isinstance(cal, dict) and "Earnings Date" in cal:
                    dates = [cal["Earnings Date"][0]]
                for d in (dates[:1] if dates else []):
                    ts = pd.Timestamp(d)
                    days_away = (ts.date() - today).days
                    if 0 <= days_away <= 7:
                        upcoming.append((t, str(ts.date()), days_away))
            except Exception:
                pass
        if upcoming:
            for t, dt, d in sorted(upcoming, key=lambda x: x[2]):
                print(f"  {t:<8} earnings on {dt} (in {d} days)")
        else:
            print("  No earnings found in next 7 days (from sample of 20 tickers)")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── 6. Health Score ───────────────────────────────────────────────
    total_score = min(100, api_health_score + min(30, db_health_score) + min(10, collector_health // 10))
    print("\n" + "=" * 66)
    print(f"  HEALTH SCORE: {total_score}/100")
    breakdown = f"APIs={api_health_score}/100  DBs={min(30,db_health_score)}/30  Collectors={min(10,collector_health//10)}/10"
    print(f"  {breakdown}")
    if total_score >= 80:
        print("  STATUS: HEALTHY")
    elif total_score >= 50:
        print("  STATUS: DEGRADED — some data sources need attention")
    else:
        print("  STATUS: CRITICAL — run diagnostics")
    print("=" * 66 + "\n")



def cmd_altdata_collect(config: dict, tickers: list = None) -> None:
    """Run all alt-data collectors for the given tickers."""
    import traceback
    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    log = logging.getLogger(__name__)
    store = AltDataStore(config)
    notifier = Notifier(config)

    universe_tickers = tickers or _default_tickers(config)

    # (module_path, optional_class_name) — None means module-level collect() function
    collector_modules = [
        ("reddit",          "altdata.collector.reddit_collector",         None),
        ("stocktwits",      "altdata.collector.stocktwits_collector",     None),
        ("news",            "altdata.collector.news_collector",           None),
        ("sec_edgar",       "altdata.collector.sec_edgar_collector",      None),
        ("companies_house", "altdata.collector.companies_house_collector", None),
        ("fred",            "altdata.collector.fred_collector",           None),
        ("shipping",        "altdata.collector.shipping_collector",       "ShippingCollector"),
        ("jobs",            "altdata.collector.jobs_collector",           "JobsCollector"),
        ("wikipedia",       "altdata.collector.wikipedia_collector",      "WikipediaCollector"),
        ("google_trends",   "altdata.collector.google_trends_collector",  "GoogleTrendsCollector"),
        ("weather",         "altdata.collector.weather_collector",        "WeatherCollector"),
        ("lunar",           "altdata.collector.lunar_collector",          "LunarCollector"),
    ]

    import importlib, time as _time

    # Show which API keys are present (without revealing values)
    api_keys = config.get("api_keys", {})
    key_status = {k: ("PRESENT" if v else "MISSING") for k, v in api_keys.items()}
    log.info("API keys: %s", " | ".join(f"{k}={v}" for k, v in key_status.items()))
    print("\nAPI keys:", key_status)
    print(f"Collecting from {len(collector_modules)} sources for {len(universe_tickers)} tickers...\n")

    summary = {}
    for name, module_path, class_name in collector_modules:
        t0 = _time.time()
        try:
            mod = importlib.import_module(module_path)
            if class_name:
                cls = getattr(mod, class_name)
                collector_obj = cls(config)
                results = collector_obj.collect(universe_tickers, market="us")
            else:
                results = mod.collect(universe_tickers, market="us", config=config)
            elapsed = _time.time() - t0
            stored = 0
            for r in results:
                try:
                    store.store_raw(r)
                    stored += 1
                except Exception as e:
                    log.warning("%s: store_raw failed: %s", name, e)
            status = f"OK  {len(results):4d} records fetched, {stored:4d} stored ({elapsed:.1f}s)"
            log.info("[%s] %s", name, status)
            print(f"  {name:<20s}: {status}")
            summary[name] = {"records": len(results), "stored": stored, "ok": True}
        except Exception as e:
            elapsed = _time.time() - t0
            err_msg = str(e)[:120]
            status = f"FAIL  {err_msg} ({elapsed:.1f}s)"
            log.warning("Collector %s failed: %s", name, err_msg)
            print(f"  {name:<20s}: {status}")
            notifier.source_failure(name, err_msg)
            summary[name] = {"records": 0, "stored": 0, "ok": False, "error": err_msg}

    total_records = sum(v["records"] for v in summary.values())
    total_stored  = sum(v["stored"]  for v in summary.values())
    failed = [k for k, v in summary.items() if not v["ok"]]
    print(f"\nCollection complete: {total_records} records fetched, {total_stored} stored")
    if failed:
        print(f"Failed collectors ({len(failed)}): {', '.join(failed)}")
    store.backup()
    log.info("Alt-data collection complete: %d records / %d stored", total_records, total_stored)


def cmd_altdata_signals(config: dict, tickers: list = None) -> None:
    """Process collected data and generate alt-data signals."""
    import importlib

    log = logging.getLogger(__name__)

    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    store = AltDataStore(config)
    notifier = Notifier(config)
    universe_tickers = tickers or _default_tickers(config)

    try:
        mod = importlib.import_module("altdata.signals.altdata_signal_engine")
        engine = mod.AltDataSignalEngine(config, store, notifier)
        signals = engine.generate(universe_tickers)
        log.info(f"Generated {len(signals)} alt-data signals")
        for s in signals[:10]:
            log.info(
                "  %s  %s  dir=%+d  conf=%.3f",
                s.get("ticker"), s.get("signal_type"),
                s.get("direction", 0), s.get("confidence", 0),
            )
    except ImportError as e:
        log.warning(f"Signal engine not available: {e}")


def cmd_altdata_dashboard(config: dict) -> None:
    """Render and print the daily alt-data dashboard."""
    log = logging.getLogger(__name__)

    from altdata.storage.altdata_store import AltDataStore
    from altdata.dashboard.altdata_dashboard import AltDataDashboard

    store = AltDataStore(config)
    dashboard = AltDataDashboard(config)
    dashboard.set_store(store)

    macro_regime = None
    try:
        import importlib
        mod = importlib.import_module("altdata.learning.online_learner")
        macro_regime = getattr(mod, "_last_regime", None)
    except Exception:
        pass

    text = dashboard.render(macro_regime=macro_regime)
    dashboard.print_to_terminal(text)
    log.info("Dashboard written to output/daily_dashboard.txt")


def cmd_altdata_nonsense(config: dict) -> None:
    """Run the nonsense detector scan over all anomaly candidates."""
    import importlib
    log = logging.getLogger(__name__)

    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    store = AltDataStore(config)
    notifier = Notifier(config)

    try:
        mod = importlib.import_module("altdata.anomaly.nonsense_detector")
        detector = mod.NonsenseDetector(config, store, notifier)
        candidates = store.get_anomaly_candidates(status="candidate")
        log.info(f"Running nonsense scan on {len(candidates)} candidates")
        for c in candidates:
            score = detector.score(c)
            log.info(f"  {c['name']}  nonsense_score={score:.3f}")
    except ImportError as e:
        log.warning(f"NonsenseDetector not available: {e}")


def cmd_altdata_rollback(config: dict, to_version: str) -> None:
    """Roll back the active model to a previous version."""
    import importlib
    log = logging.getLogger(__name__)

    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    store = AltDataStore(config)
    notifier = Notifier(config)

    try:
        mod = importlib.import_module("altdata.learning.rollback_manager")
        mgr = mod.RollbackManager(config, store, notifier)
        mgr.rollback_to(to_version)
        log.info(f"Model rolled back to {to_version}")
    except ImportError as e:
        log.warning(f"RollbackManager not available: {e}")


def cmd_altdata_status(config: dict) -> None:
    """Print a concise status summary of the alt-data pipeline."""
    from altdata.storage.altdata_store import AltDataStore

    store = AltDataStore(config)

    active = store.get_active_model()
    signals = store.get_recent_signals(hours_back=24)
    candidates = store.get_anomaly_candidates()
    live_candidates = [c for c in candidates if c["status"] == "live"]

    print("─" * 50)
    print("ALT DATA PIPELINE STATUS")
    print("─" * 50)
    print(f"  Active model      : {active['version'] if active else 'none'}")
    print(f"  Signals (24h)     : {len(signals)}")
    print(f"  Live anomalies    : {len(live_candidates)}")
    print(f"  Total candidates  : {len(candidates)}")
    accuracy = store.get_signal_accuracy()
    if accuracy["accuracy"] is not None:
        print(f"  Signal accuracy   : {accuracy['accuracy']:.1%}  (n={accuracy['n']})")
    print("─" * 50)


def _default_tickers(config: dict) -> list:
    """Return the default US ticker list from config or a hardcoded fallback."""
    try:
        from data.universe import UniverseManager
        from data.fetcher import DataFetcher
        fetcher = DataFetcher(config)
        um = UniverseManager(config, fetcher)
        return um.get_universe("us")[:50]
    except Exception:
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "UNH"]


def cmd_promote(config: dict, signal_name: str) -> None:
    from signals.signal_registry import SignalRegistry

    registry = SignalRegistry(config)
    entry = registry.get(signal_name)
    if not entry:
        print(f"Signal '{signal_name}' not found in registry")
        return
    if entry["status"] == "live":
        print(f"Signal '{signal_name}' is already live")
        return
    registry.set_live(signal_name)
    print(f"Signal '{signal_name}' promoted to live")


# ---------------------------------------------------------------------------
# Deepdata commands
# ---------------------------------------------------------------------------

def cmd_deepdata_status(config: dict) -> None:
    """Print deepdata pipeline status summary."""
    from deepdata.storage.deepdata_store import DeepDataStore
    store = DeepDataStore(config)
    s = store.status_summary()
    print("─" * 55)
    print("DEEP DATA PIPELINE STATUS")
    print("─" * 55)
    print(f"  Options records (24h)     : {s['options_records_24h']}")
    print(f"  Squeeze candidates        : {s['squeeze_candidates']}")
    print(f"  Transcripts (30d)         : {s['transcripts_30d']}")
    print(f"  Congressional trades (30d): {s['congressional_30d']}")
    print(f"  Tier-1 signals (24h)      : {s['tier1_signals_24h']}")
    print(f"  Live cross-module patterns: {s['live_patterns']}")
    print("─" * 55)


def cmd_deepdata_collect(config: dict, tickers: list = None) -> None:
    """Run all deepdata collectors."""
    import importlib
    log = logging.getLogger(__name__)
    from deepdata.storage.deepdata_store import DeepDataStore

    store = DeepDataStore(config)
    universe = tickers or _default_tickers(config)

    collectors = [
        ("options_flow",    "deepdata.options.flow_monitor",             "OptionsFlowMonitor"),
        ("short_interest",  "deepdata.short_interest.finra_collector",   "FINRACollector"),
        ("congressional",   "deepdata.congressional.disclosure_fetcher", "CongressionalDisclosureFetcher"),
        ("patents_us",      "deepdata.patents.uspto_collector",           "USPTOCollector"),
        ("patents_uk",      "deepdata.patents.uk_ipo_collector",          "UKIPOCollector"),
        ("supply_chain",    "deepdata.supply_chain.relationship_mapper",  "SupplyChainRelationshipMapper"),
    ]

    for name, module_path, class_name in collectors:
        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            obj = cls(config)
            if hasattr(obj, "collect"):
                results = obj.collect(universe)
            elif hasattr(obj, "fetch_recent"):
                results = obj.fetch_recent(days_back=30)
            else:
                results = []
            log.info(f"[{name}] {len(results)} records")
        except Exception as e:
            log.warning(f"deepdata collector {name} failed: {e}")

    store.backup()
    log.info("deepdata collection complete")


def cmd_deepdata_dashboard(config: dict) -> None:
    """Render and print the deepdata dashboard."""
    import importlib
    log = logging.getLogger(__name__)
    from deepdata.storage.deepdata_store import DeepDataStore

    store = DeepDataStore(config)
    try:
        mod = importlib.import_module("deepdata.dashboard.deepdata_dashboard")
        dashboard = mod.DeepDataDashboard(config)
        dashboard.set_store(store)
        text = dashboard.render()
        dashboard.print_to_terminal(text)
    except Exception as e:
        log.warning(f"deepdata dashboard failed: {e}")


def cmd_deepdata_options(config: dict, ticker: str) -> None:
    """Analyse options flow for a single ticker."""
    import importlib
    log = logging.getLogger(__name__)

    try:
        mod = importlib.import_module("deepdata.options.flow_monitor")
        monitor = mod.OptionsFlowMonitor(config)
        results = monitor.scan([ticker])
        for r in results:
            print(f"  {r.get('data_type',''):<25} {r.get('value', 'n/a')}")
        if not results:
            print(f"  No options data available for {ticker}")
    except Exception as e:
        log.warning(f"options scan failed: {e}")


def cmd_deepdata_squeeze(config: dict, ticker: str) -> None:
    """Run squeeze scoring for a single ticker."""
    import importlib
    log = logging.getLogger(__name__)

    try:
        fi_mod = importlib.import_module("deepdata.short_interest.finra_collector")
        sq_mod = importlib.import_module("deepdata.short_interest.squeeze_scorer")
        collector = fi_mod.FINRACollector(config)
        short_results = collector.collect([ticker])
        short_data = {}
        for r in short_results:
            if r.get("ticker") == ticker:
                short_data[r.get("data_type", "")] = r.get("value")

        scorer = sq_mod.SqueezeScorer(config)

        import yfinance as yf
        import pandas as pd
        prices = yf.download(ticker, period="3mo", auto_adjust=True, progress=False)
        result = scorer.score(ticker, short_data, prices)
        print(f"\nSqueeze analysis for {ticker}:")
        print(f"  Score          : {result.get('squeeze_score', 0):.1f}/100")
        print(f"  Signal         : {result.get('signal', 'n/a')}")
        print(f"  Flagged        : {result.get('flagged', False)}")
        if result.get("layer1_flags"):
            print(f"  Flags          : {', '.join(result['layer1_flags'])}")
    except Exception as e:
        log.warning(f"squeeze analysis failed: {e}")


def cmd_deepdata_congress(config: dict, days_back: int = 30) -> None:
    """Fetch and display recent congressional disclosures."""
    import importlib
    log = logging.getLogger(__name__)
    from deepdata.storage.deepdata_store import DeepDataStore

    store = DeepDataStore(config)
    try:
        mod = importlib.import_module("deepdata.congressional.disclosure_fetcher")
        fetcher = mod.CongressionalDisclosureFetcher(config)
        disclosures = fetcher.fetch_recent(days_back=days_back)
        universe = _default_tickers(config)
        in_universe = fetcher.filter_universe(disclosures, universe)

        print(f"\nCongressional disclosures (last {days_back} days): {len(disclosures)} total")
        print(f"In your universe: {len(in_universe)}")
        print()
        for d in in_universe[:20]:
            print(
                f"  {d.get('member','?'):<30} {d.get('ticker','?'):<8}"
                f" {d.get('transaction_type','?'):<6}"
                f" {d.get('transaction_date','?')}"
            )
    except Exception as e:
        log.warning(f"congressional fetch failed: {e}")


def cmd_deepdata_patterns(config: dict) -> None:
    """Run cross-module pattern scanner."""
    import importlib
    log = logging.getLogger(__name__)
    from deepdata.storage.deepdata_store import DeepDataStore

    store = DeepDataStore(config)
    try:
        mod = importlib.import_module("deepdata.patterns.cross_module_scanner")
        scanner = mod.CrossModulePatternScanner(config)
        live = store.get_live_patterns()
        print(f"\nActive cross-module patterns: {len(live)}")
        for p in live:
            print(f"  {p['name']:<40} sharpe={p.get('sharpe', 0):.2f}  modules={p.get('modules', [])}")
    except Exception as e:
        log.warning(f"pattern scan failed: {e}")


def cmd_deepdata_transcript(config: dict, ticker: str) -> None:
    """Fetch and analyse latest earnings transcript for a ticker."""
    import importlib
    log = logging.getLogger(__name__)

    try:
        ft_mod = importlib.import_module("deepdata.transcripts.transcript_fetcher")
        ta_mod = importlib.import_module("deepdata.transcripts.tone_analyser")
        ls_mod = importlib.import_module("deepdata.transcripts.linguistic_scorer")
        ge_mod = importlib.import_module("deepdata.transcripts.guidance_extractor")

        fetcher = ft_mod.TranscriptFetcher(config)
        analyser = ta_mod.ToneAnalyser(config)
        scorer = ls_mod.LinguisticScorer(config)
        extractor = ge_mod.GuidanceExtractor(config)

        transcripts = fetcher.fetch(ticker, max_transcripts=1)
        if not transcripts:
            print(f"No transcripts found for {ticker}")
            return

        t = transcripts[0]
        tone = analyser.analyse(t)
        guidance = extractor.extract(t)
        result = scorer.score(tone, {}, guidance)

        print(f"\nTranscript analysis: {ticker}")
        print(f"  Source         : {t.get('source', 'n/a')}")
        print(f"  Date           : {t.get('date', 'n/a')}")
        print(f"  Linguistic score: {result.get('score', 0):.3f}")
        print(f"  Classification : {result.get('classification', 'n/a')}")
        print(f"  Hedge ratio    : {tone.get('hedge_ratio', 0):.3f}")
        print(f"  Tone shift     : {tone.get('tone_shift', 0):.3f}")
        print(f"  Deflection     : {tone.get('deflection_score', 0):.3f}")
        print(f"  Guidance       : {guidance.get('guidance_signal', 'NONE')}")
    except Exception as e:
        log.warning(f"transcript analysis failed: {e}")


# ---------------------------------------------------------------------------
# Validate command — runs full test suite and module import checks
# ---------------------------------------------------------------------------

def cmd_validate(config: dict) -> None:
    """Run test suite and validate all module imports."""
    import subprocess
    log = logging.getLogger(__name__)
    print("\n" + "=" * 65)
    print("  SYSTEM VALIDATION")
    print("=" * 65)

    # 1. Module import checks
    modules_to_check = [
        ("signals.pead_signal", "PEADSignal"),
        ("signals.anomaly_scanner", "AnomalyScanner"),
        ("backtest.engine", "BacktestEngine"),
        ("risk.manager", None),
        ("execution.paper_trader", None),
        ("altdata.storage.altdata_store", "AltDataStore"),
        ("deepdata.storage.deepdata_store", "DeepDataStore"),
        ("frontier.storage.frontier_store", "FrontierStore"),
        ("frontier.equations.derived_formulas", "calc_grai"),
        ("frontier.equations.unified_complexity_index", "calc_umci"),
        ("frontier.equations.cross_signal_interactions", "get_all_interactions"),
        ("frontier.equations.frontier_signal_purity", "SignalPurityTracker"),
        ("frontier.sizing.frontier_sizer", "calculate_frontier_position_size"),
        ("frontier.validation.frontier_validator", "FrontierValidator"),
        ("frontier.validation.evidence_tracker", "EvidenceTracker"),
        ("frontier.meta_learning.discovery_registry", "DiscoveryRegistry"),
    ]

    print("\n  Module Import Checks:")
    passed = 0
    failed = 0
    for module_path, attr in modules_to_check:
        try:
            import importlib
            mod = importlib.import_module(module_path)
            if attr:
                getattr(mod, attr)
            print(f"    OK  {module_path}")
            passed += 1
        except Exception as e:
            print(f"    FAIL {module_path}: {e}")
            failed += 1

    print(f"\n  Imports: {passed} OK, {failed} failed")

    # 2. Run pytest
    print("\n  Running test suite...")
    result = subprocess.run(
        ["python3", "-m", "pytest", "tests/", "-v", "--tb=short", "-q"],
        capture_output=True, text=True,
        cwd=str(Path(__file__).parent)
    )
    print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
    if result.stderr:
        print(result.stderr[-500:])

    # 3. Frontier validation smoke test
    print("\n  Frontier Equations Smoke Test:")
    try:
        from frontier.equations.derived_formulas import (
            calc_grai, calc_asi, calc_scv, calc_dli, calc_lpas, calc_qtpi, calc_fsp
        )
        import pandas as pd, numpy as np

        grai = calc_grai({"global": 3.0}, {"global": 1.0}, {"global": 1.0}, 6.0, 18.0, 20.0)
        asi  = calc_asi({"stocks": 40, "bonds": 20, "crypto": 15, "forex": 15, "macro": 10})
        scv, r0 = calc_scv(1000, 50, 20, 0.3, 0.1)
        dli  = calc_dli(0.6, 0.5, 0.15)
        qtpi = calc_qtpi(30.0, 1.2, ["50_qubit_system", "quantum_advantage_demonstrated"])
        rets = pd.Series(np.random.randn(100) * 0.01)
        fsp  = calc_fsp(rets, {"momentum": rets.shift(1).fillna(0)})

        print(f"    GRAI={grai:.4f}  ASI={asi:.4f}  SCV_R0={r0:.4f}")
        print(f"    DLI={dli:.4f}   QTPI={qtpi:.4f} FSP={fsp:.4f}")
        print("    All frontier equations: OK")
    except Exception as e:
        print(f"    Frontier equations failed: {e}")

    # 4. UMCI smoke test
    try:
        from frontier.equations.unified_complexity_index import calc_umci, get_umci_level
        umci, breakdown = calc_umci(0.3, 0.4, 0.2, 0.35, 0.25)
        level = get_umci_level(umci)
        print(f"\n  UMCI Smoke Test: score={umci:.1f} [{level['name']}]  OK")
    except Exception as e:
        print(f"\n  UMCI Smoke Test FAILED: {e}")

    print("\n" + "=" * 65)
    print(f"  Validation complete. Imports: {passed}/{passed+failed} | Tests: see above")
    print("=" * 65 + "\n")


# ---------------------------------------------------------------------------
# Frontier commands
# ---------------------------------------------------------------------------

def _get_frontier_store(config: dict):
    try:
        from frontier.storage.frontier_store import FrontierStore
        return FrontierStore(config=config)
    except Exception as e:
        logging.getLogger(__name__).warning(f"FrontierStore unavailable: {e}")
        return None


def cmd_frontier_status(config: dict) -> None:
    log = logging.getLogger(__name__)
    print("\n" + "=" * 65)
    print("  FRONTIER INTELLIGENCE — STATUS")
    print("=" * 65)

    store = _get_frontier_store(config)
    if store:
        try:
            summary = store.status_summary()
            print(f"\n  Store          : OK  ({summary.get('db_path', 'frontier_data.db')})")
            print(f"  Raw signals    : {summary.get('raw_signals', 0)}")
            print(f"  UMCI readings  : {summary.get('umci_readings', 0)}")
            print(f"  Watchlist items: {summary.get('watchlist_items', 0)}")
            print(f"  Live signals   : {summary.get('live_signals', 0)}")
            print(f"  Validations    : {summary.get('validations', 0)}")

            last_umci = store.get_last_umci()
            if last_umci:
                print(f"\n  Last UMCI      : {last_umci.get('umci', '?'):.1f} [{last_umci.get('level', '?')}]")
                print(f"  Recorded at    : {last_umci.get('timestamp', '?')}")
        except Exception as e:
            print(f"  Store error: {e}")
    else:
        print("  Store: not yet initialised (run 'frontier collect' first)")

    # Registry check
    try:
        from frontier.meta_learning.discovery_registry import DiscoveryRegistry
        registry = DiscoveryRegistry()
        s = registry.summary()
        print(f"\n  Discovery registry:")
        print(f"    Total candidates : {s['total']}")
        for status, count in s.get('by_status', {}).items():
            print(f"    {status:<15}: {count}")
    except Exception as e:
        print(f"\n  Discovery registry: {e}")

    # Module status
    print("\n  Frontier modules:")
    modules = [
        ("equations.derived_formulas", "All 8 formulas"),
        ("equations.unified_complexity_index", "UMCI"),
        ("equations.cross_signal_interactions", "Cross-signal interactions"),
        ("equations.frontier_signal_purity", "Signal purity tracker"),
        ("sizing.frontier_sizer", "Tiered position sizer"),
        ("validation.frontier_validator", "7-test validator"),
        ("validation.evidence_tracker", "Evidence tracker"),
        ("meta_learning.discovery_registry", "Discovery registry"),
    ]
    import importlib
    for mod_path, desc in modules:
        try:
            importlib.import_module(f"frontier.{mod_path}")
            print(f"    OK  {desc}")
        except Exception as e:
            print(f"    --  {desc} ({e})")

    print("=" * 65 + "\n")


def cmd_frontier_collect(config: dict, tickers: list = None) -> None:
    log = logging.getLogger(__name__)
    tickers = tickers or _default_tickers(config)
    print(f"\nFrontier collect — {len(tickers)} tickers")

    store = _get_frontier_store(config)
    collectors = []

    def _try_load(cls_path, mod_path):
        import importlib
        mod = importlib.import_module(mod_path)
        cls = getattr(mod, cls_path)
        try:
            return cls(config)
        except TypeError:
            return cls()

    # Physical
    for cls_path, mod_path in [
        ("GeomagneticCollector", "frontier.physical.geomagnetic_collector"),
        ("SchumannCollector",    "frontier.physical.schumann_collector"),
        ("PollenCollector",      "frontier.physical.pollen_collector"),
        ("ElectricityCollector", "frontier.physical.electricity_collector"),
        ("CanalCongestionCollector", "frontier.physical.canal_congestion_collector"),
        ("SatelliteImageryCollector","frontier.physical.satellite_imagery_collector"),
    ]:
        try:
            collectors.append((cls_path, _try_load(cls_path, mod_path)))
        except Exception as e:
            log.debug(f"Skipping {cls_path}: {e}")

    # Social
    for cls_path, mod_path in [
        ("AttentionEconomyCollector", "frontier.social.attention_economy"),
        ("SocialContagionMapper",     "frontier.social.social_contagion_mapper"),
        ("ObituaryTracker",           "frontier.social.obituary_tracker"),
        ("ChurchAttendanceCollector", "frontier.social.church_attendance"),
        ("DivorceFilingCollector",    "frontier.social.divorce_filing_collector"),
        ("HQTrafficMonitor",          "frontier.social.hq_traffic_monitor"),
    ]:
        try:
            collectors.append((cls_path, _try_load(cls_path, mod_path)))
        except Exception as e:
            log.debug(f"Skipping {cls_path}: {e}")

    # Scientific
    for cls_path, mod_path in [
        ("AcademicCitationTracker",  "frontier.scientific.academic_citation_tracker"),
        ("AMRResearchTracker",       "frontier.scientific.amr_research_tracker"),
        ("SoilHealthCollector",      "frontier.scientific.soil_health_collector"),
        ("QuantumReadinessTracker",  "frontier.scientific.quantum_readiness_tracker"),
        ("FoodSafetyCollector",      "frontier.scientific.food_safety_collector"),
    ]:
        try:
            collectors.append((cls_path, _try_load(cls_path, mod_path)))
        except Exception as e:
            log.debug(f"Skipping {cls_path}: {e}")

    # Financial frontier
    for cls_path, mod_path in [
        ("OptionExpiryOverhangCollector", "frontier.financial_frontier.option_expiry_overhang"),
        ("BuildingPermitCollector",       "frontier.financial_frontier.building_permit_collector"),
        ("LLMPerplexityScorer",           "frontier.financial_frontier.llm_perplexity_scorer"),
    ]:
        try:
            collectors.append((cls_path, _try_load(cls_path, mod_path)))
        except Exception as e:
            log.debug(f"Skipping {cls_path}: {e}")

    results = []
    for name, collector in collectors:
        try:
            result = collector.collect()
            results.append(result)
            val = result.get("value", 0)
            q = result.get("quality_score", 0)
            sig = result.get("signal_name", name)
            print(f"  {sig:<35} value={val:+.4f}  quality={q:.2f}")
            if store:
                store.store_raw(
                    collector=name,
                    signal_name=sig,
                    value=float(val),
                    ticker=None,
                    market="global",
                    raw_data=result.get("raw_data", {}),
                    quality=float(q),
                )
        except Exception as e:
            print(f"  {name:<35} ERROR: {e}")

    print(f"\n  Collected {len(results)} signals from {len(collectors)} collectors.")


def cmd_frontier_umci(config: dict) -> None:
    log = logging.getLogger(__name__)
    print("\nComputing UMCI...")

    store = _get_frontier_store(config)

    # Gather signal values (from store or defaults)
    signals = {}
    if store:
        try:
            for sig in ["grai", "schumann_deviation", "pollen_stress_index",
                        "electricity_anomaly", "canal_congestion_index",
                        "asi", "social_contagion_r0", "obituary_impact_score",
                        "church_attendance_signal", "divorce_anomaly",
                        "qtpi", "amr_urgency", "academic_citation_velocity",
                        "soil_health_degradation", "food_safety_risk",
                        "lpas_mean", "gamma_overhang_score",
                        "building_permit_inflection",
                        "reddit_coordination_score", "satellite_activity_drop"]:
                hist = store.get_raw_history(sig, days_back=1)
                if hist:
                    signals[sig] = hist[-1]["value"]
        except Exception as e:
            log.debug(f"Store signal fetch: {e}")

    # Defaults for any missing signals
    defaults = {
        "grai": 0.0, "schumann_deviation": 0.0, "pollen_stress_index": 0.0,
        "electricity_anomaly": 0.0, "canal_congestion_index": 0.0,
        "asi": 0.7, "social_contagion_r0": 1.0, "obituary_impact_score": 0.0,
        "church_attendance_signal": 0.0, "divorce_anomaly": 0.0,
        "qtpi": 0.35, "amr_urgency": 0.5, "academic_citation_velocity": 0.3,
        "soil_health_degradation": 0.2, "food_safety_risk": 0.1,
        "lpas_mean": 0.0, "gamma_overhang_score": 0.0,
        "building_permit_inflection": 0.0,
        "reddit_coordination_score": 0.3, "satellite_activity_drop": 0.0,
    }
    for k, v in defaults.items():
        signals.setdefault(k, v)

    try:
        from frontier.equations.unified_complexity_index import (
            calc_physical_complexity, calc_social_complexity,
            calc_scientific_complexity, calc_financial_frontier_complexity,
            calc_altdata_complexity, calc_umci, UMCILogger
        )

        pc  = calc_physical_complexity(
            signals["grai"], signals["schumann_deviation"],
            signals["pollen_stress_index"], signals["electricity_anomaly"],
            signals["canal_congestion_index"])
        sc  = calc_social_complexity(
            signals["asi"], signals["social_contagion_r0"],
            signals["divorce_anomaly"], signals["obituary_impact_score"],
            signals["church_attendance_signal"])
        sci = calc_scientific_complexity(
            signals["qtpi"], signals["amr_urgency"],
            signals["academic_citation_velocity"], signals["soil_health_degradation"])
        ffc = calc_financial_frontier_complexity(
            signals["lpas_mean"], signals["gamma_overhang_score"],
            signals["building_permit_inflection"], signals["food_safety_risk"])
        adc = calc_altdata_complexity(
            signals["reddit_coordination_score"], 0.2,
            signals["canal_congestion_index"], 0.3)

        umci, breakdown = calc_umci(pc, sc, sci, ffc, adc, config)

        # Log it
        umci_logger = UMCILogger()
        umci_logger.log(umci, breakdown)
        if store:
            store.store_umci(umci, breakdown)

        report = umci_logger.generate_complexity_report(umci, breakdown)
        print(report)

    except Exception as e:
        print(f"UMCI computation failed: {e}")
        import traceback; traceback.print_exc()


def cmd_frontier_dashboard(config: dict) -> None:
    try:
        from frontier.dashboard.frontier_dashboard import FrontierDashboard
        store = _get_frontier_store(config)
        try:
            from frontier.meta_learning.discovery_registry import DiscoveryRegistry
            registry = DiscoveryRegistry()
        except Exception:
            registry = None
        dash = FrontierDashboard(store=store, registry=registry, config=config)
        output = dash.render()
        print(output)
    except Exception as e:
        print(f"Frontier dashboard error: {e}")
        import traceback; traceback.print_exc()


def cmd_frontier_discover(config: dict) -> None:
    print("\nFrontier signal discovery scan...")
    try:
        from frontier.meta_learning.discovery_registry import DiscoveryRegistry
        from frontier.meta_learning.watchlist_manager import WatchlistManager
        store = _get_frontier_store(config)
        registry = DiscoveryRegistry()
        manager = WatchlistManager(store=store, registry=registry)

        # Attempt auto-discovery using cross-signal interactions
        from frontier.equations.cross_signal_interactions import get_all_interactions
        signals = {
            "grai": 0.2, "gamma_overhang_score": 0.3, "divorce_anomaly": 0.1,
            "schumann_deviation": 0.05, "reddit_coordination_score": 0.4,
            "lunar_phase_angle_rad": 1.57, "church_attendance_signal": 0.1,
            "congressional_signal_strength": 0.3, "hiring_momentum": 0.2,
            "pollen_stress_index": 0.3, "short_squeeze_score": 0.2,
            "satellite_activity_drop": 0.1, "canal_congestion_index": 0.2,
            "obituary_impact_score": 0.05, "building_permit_inflection": 0.1,
            "electricity_anomaly": 0.3, "academic_citation_velocity": 0.4,
            "lpas_mean": 0.0, "qtpi": 0.35, "amr_urgency": 0.5,
            "social_contagion_r0": 1.1,
        }
        interactions = get_all_interactions(signals)

        print(f"\n  Cross-signal interactions computed: {len(interactions)}")
        for name, value in sorted(interactions.items(), key=lambda x: abs(x[1]), reverse=True):
            print(f"    {name:<40} {value:+.6f}")

        # Register non-zero interactions as watchlist candidates
        registered = 0
        for name, value in interactions.items():
            if abs(value) > 0.001:
                existing = registry.get(name)
                if not existing:
                    manager.add(
                        signal_name=name,
                        description=f"Cross-signal interaction: {name}",
                        source_signals=list(signals.keys()),
                        initial_corr=min(0.3, abs(value)),
                        nonsense_score=0.7,
                        has_story=False,
                    )
                    registered += 1

        summary = registry.summary()
        print(f"\n  Registered {registered} new candidates.")
        print(f"  Registry total: {summary['total']} | watchlist: {summary['watchlist_count']}")

    except Exception as e:
        print(f"Discovery error: {e}")
        import traceback; traceback.print_exc()


def cmd_frontier_watchlist(config: dict) -> None:
    try:
        from frontier.meta_learning.discovery_registry import DiscoveryRegistry
        from frontier.meta_learning.watchlist_manager import WatchlistManager
        store = _get_frontier_store(config)
        registry = DiscoveryRegistry()
        manager = WatchlistManager(store=store, registry=registry)
        print(manager.render_watchlist_table())
    except Exception as e:
        print(f"Watchlist error: {e}")


def cmd_frontier_validate_signal(config: dict, signal_name: str) -> None:
    print(f"\nValidating signal: {signal_name}")
    try:
        import numpy as np
        import pandas as pd
        from frontier.validation.frontier_validator import FrontierValidator

        # Generate synthetic returns for demonstration
        rng = np.random.default_rng(42)
        returns = pd.Series(rng.normal(0.001, 0.02, 300))
        validator = FrontierValidator(config)
        result = validator.validate(
            signal_name=signal_name,
            is_returns=returns.iloc[:210],
            oos_returns=returns.iloc[210:],
        )
        print(f"\n  Result : {'PASS' if result['passed'] else 'FAIL'}")
        print(f"  Grade  : {result['evidence_grade']}")
        print(f"  Tier   : {result['suggested_tier']}")
        print(f"  Summary: {result['summary']}")
        print("\n  Tests:")
        for test, data in result["tests"].items():
            status = "PASS" if data["pass"] else "FAIL"
            print(f"    {status}  {test:<25} value={data['value']}  threshold={data.get('threshold', 'n/a')}")
    except Exception as e:
        print(f"Validation error: {e}")
        import traceback; traceback.print_exc()


def cmd_frontier_geomagnetic(config: dict) -> None:
    print("\nGeomagnetic signal (GRAI):")
    try:
        from frontier.physical.geomagnetic_collector import GeomagneticCollector
        result = GeomagneticCollector(config).collect()
        print(f"  GRAI           : {result['value']:.4f}")
        print(f"  Quality        : {result['quality_score']:.2f}")
        print(f"  Source         : {result['source']}")
        raw = result.get("raw_data", {})
        if raw:
            for k, v in list(raw.items())[:5]:
                print(f"  {k:<20}: {v}")
    except Exception as e:
        print(f"  Geomagnetic collector error: {e}")


def cmd_frontier_attention(config: dict) -> None:
    print("\nAttention Economy (ASI):")
    try:
        from frontier.social.attention_economy import AttentionEconomyCollector
        result = AttentionEconomyCollector(config).collect()
        print(f"  ASI            : {result['value']:.4f}")
        print(f"  Quality        : {result['quality_score']:.2f}")
        print(f"  Source         : {result['source']}")
        raw = result.get("raw_data", {})
        if raw:
            for k, v in list(raw.items())[:5]:
                print(f"  {k:<20}: {v}")
    except Exception as e:
        print(f"  Attention collector error: {e}")


def cmd_frontier_quantum(config: dict) -> None:
    print("\nQuantum Readiness (QTPI):")
    try:
        from frontier.scientific.quantum_readiness_tracker import QuantumReadinessTracker
        result = QuantumReadinessTracker(config).collect()
        print(f"  QTPI           : {result['value']:.4f}")
        print(f"  Quality        : {result['quality_score']:.2f}")
        print(f"  Source         : {result['source']}")
        raw = result.get("raw_data", {})
        if raw:
            for k, v in list(raw.items())[:5]:
                print(f"  {k:<20}: {v}")
    except Exception as e:
        print(f"  Quantum collector error: {e}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_closeloop_store(config: dict):
    """Initialise and return the ClosedLoopStore, or None on failure."""
    try:
        from closeloop.storage.closeloop_store import ClosedLoopStore
        return ClosedLoopStore(config)
    except Exception as e:
        logger.warning("ClosedLoopStore unavailable: %s", e)
        return None


def cmd_closeloop_status(config: dict) -> None:
    store = _get_closeloop_store(config)
    if store:
        summary = store.status_summary()
        print("\nCLOSED-LOOP STATUS")
        print("=" * 50)
        for k, v in summary.items():
            print(f"  {k:35s}: {v}")
    else:
        print("ClosedLoopStore not available")


def cmd_closeloop_dashboard(config: dict) -> None:
    store = _get_closeloop_store(config)
    try:
        from closeloop.dashboard.closeloop_dashboard import ClosedLoopDashboard
        dash = ClosedLoopDashboard(store=store, config=config)
        output = dash.render()
        print(output)
    except Exception as e:
        print(f"Dashboard error: {e}")


def cmd_closeloop_stress(config: dict) -> None:
    store = _get_closeloop_store(config)
    try:
        from closeloop.stress.stress_tester import StressTester
        tester = StressTester(store=store)
        result = tester.run()
        print("\nSTRESS TEST RESULTS")
        print("=" * 50)
        print(f"  WeightedStressRisk : {result.get('weighted_stress_risk', 0):.4f}")
        print(f"  CrisisFragile      : {result.get('crisis_fragile', False)}")
        print(f"  Scenarios run      : {result.get('n_scenarios', 0)}")
        print(f"\n  Summary: {result.get('summary', 'N/A')}")
        print("\n  TOP SCENARIOS:")
        for i, sc in enumerate(result.get("top_scenarios", [])[:5], 1):
            print(f"    {i}. {sc.get('scenario_name','?'):30s} relevance={sc.get('relevance',0):.3f} "
                  f"weighted_loss={sc.get('weighted_loss_pct',0):.3f}")
    except Exception as e:
        print(f"Stress test error: {e}")


def cmd_closeloop_weights(config: dict) -> None:
    store = _get_closeloop_store(config)
    if not store:
        print("ClosedLoopStore not available")
        return
    weights = store.get_all_signal_weights()
    print("\nSIGNAL WEIGHTS")
    print("=" * 50)
    if not weights:
        print("  No signal weights recorded yet.")
    for w in weights:
        print(f"  {w.get('signal_name','?'):30s}  weight={w.get('weight', 1.0):.4f}")


def cmd_closeloop_autopsy(config: dict, trade_id: int = None) -> None:
    store = _get_closeloop_store(config)
    if not store:
        print("ClosedLoopStore not available")
        return
    trades = store.get_trades(n=1 if trade_id is None else 252)
    if not trades:
        print("No trades in store to autopsy.")
        return
    trade = next((t for t in trades if t.get("id") == trade_id), trades[0])
    print(f"\nAUTOPSY: trade_id={trade.get('id')} {trade.get('ticker')} net_pnl={trade.get('net_pnl', 0):.2f}")
    scorecard = store.get_signal_scorecard(trade.get("ticker", ""))
    print(f"  Signal scorecard: {scorecard}")


def cmd_closeloop_tax(config: dict) -> None:
    try:
        from closeloop.risk.tax_manager import TaxManager
        store = _get_closeloop_store(config)
        tm = TaxManager(store=store)
        liability = tm.compute_annual_liability()
        print("\nUK CGT TAX SUMMARY")
        print("=" * 50)
        for k, v in liability.items():
            if isinstance(v, float):
                print(f"  {k:35s}: £{v:,.2f}")
            else:
                print(f"  {k:35s}: {v}")
    except Exception as e:
        print(f"Tax manager error: {e}")


def cmd_closeloop_benchmark(config: dict) -> None:
    try:
        from closeloop.risk.benchmark_tracker import BenchmarkTracker
        store = _get_closeloop_store(config)
        bt = BenchmarkTracker(store=store)
        print("\n" + bt.summary_text())
        print("\n  Full IR comparison:")
        for bm, data in bt.full_comparison().items():
            print(f"    {bm}: IR={data.get('ir', 0):.4f}  n_obs={data.get('n_obs', 0)}")
    except Exception as e:
        print(f"Benchmark tracker error: {e}")


def cmd_closeloop_wire(config: dict) -> None:
    try:
        from closeloop.integration.module_wirer import ModuleWirer
        store = _get_closeloop_store(config)
        wirer = ModuleWirer(store=store, config=config)
        result = wirer.wire_all()
        print(wirer.status_text())
    except Exception as e:
        print(f"Module wirer error: {e}")


def cmd_closeloop_entry(config: dict, ticker: str = None) -> None:
    if not ticker:
        print("Specify --ticker for entry analysis")
        return
    try:
        from closeloop.entry.entry_conditions import EntryConditions
        ec = EntryConditions()
        result = ec.score(ticker)
        print(f"\nENTRY CONDITIONS: {ticker}")
        print("=" * 50)
        for k, v in result.items():
            print(f"  {k:30s}: {v}")
    except Exception as e:
        print(f"Entry conditions error: {e}")


def cmd_closeloop_peers(config: dict, ticker: str = None) -> None:
    if not ticker:
        print("Specify --ticker for peer analysis")
        return
    try:
        from closeloop.context.peer_influence_mapper import PeerInfluenceMapper
        store = _get_closeloop_store(config)
        mapper = PeerInfluenceMapper(store=store)
        result = mapper.map(ticker)
        print(f"\nPEER INFLUENCE: {ticker}")
        print("=" * 50)
        for inf in result.get("influences", [])[:10]:
            print(f"  {inf.get('type','?'):20s} {inf.get('peer','?'):10s} score={inf.get('score',0):.4f}")
    except Exception as e:
        print(f"Peer influence error: {e}")


def cmd_closeloop_revisions(config: dict, ticker: str = None) -> None:
    if not ticker:
        print("Specify --ticker for revision analysis")
        return
    try:
        from closeloop.context.analyst_revision_tracker import AnalystRevisionTracker
        store = _get_closeloop_store(config)
        tracker = AnalystRevisionTracker(store=store)
        result = tracker.track(ticker)
        print(f"\nANALYST REVISIONS: {ticker}")
        print("=" * 50)
        print(f"  ERM score : {result.get('erm_score', 0):.4f}")
        print(f"  PEAD mod  : {result.get('pead_modifier', 1.0):.4f}")
        print(f"  Up/Down   : {result.get('n_up', 0)}/{result.get('n_down', 0)}")
    except Exception as e:
        print(f"Analyst revision error: {e}")


def cmd_pead_snapshot(config: dict, ticker: str, earnings_date: str = None) -> None:
    """
    Capture a full pre-earnings snapshot for a single ticker:
      - Altdata sentiment (reddit, news, sec)
      - Deepdata (options flow, short squeeze, congressional)
      - Macro context (VIX, SPY, sector ETF, FRED regime)
      - Current price + volume

    Stores to pre_earnings_snapshots table and prints a summary.
    """
    import importlib
    import json as _json
    from datetime import datetime
    log = logging.getLogger(__name__)

    from data.earnings_db import EarningsDB
    from data.earnings_collector import EarningsCollector
    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    db = EarningsDB()
    store = AltDataStore(config)
    notifier = Notifier(config)

    # Resolve earnings date
    if earnings_date is None:
        upcoming = db.get_upcoming_calendar(days_ahead=60)
        matches = [r for r in upcoming if r["ticker"] == ticker]
        if matches:
            earnings_date = matches[0]["earnings_date"]
        else:
            # Try to fetch from yfinance calendar
            import yfinance as yf
            import pandas as pd
            try:
                cal = yf.Ticker(ticker).calendar
                if isinstance(cal, dict):
                    ed = cal.get("Earnings Date")
                    if isinstance(ed, (list, tuple)):
                        ed = ed[0]
                    earnings_date = pd.Timestamp(ed).strftime("%Y-%m-%d")
                elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.columns:
                    earnings_date = pd.Timestamp(cal["Earnings Date"].iloc[0]).strftime("%Y-%m-%d")
            except Exception:
                pass
        if earnings_date is None:
            log.warning("Could not determine earnings date for %s", ticker)
            earnings_date = "UNKNOWN"

    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Days until earnings
    try:
        from datetime import date as _date
        ed = _date.fromisoformat(earnings_date)
        days_before = (ed - _date.today()).days
    except Exception:
        days_before = None

    log.info("Capturing pre-earnings snapshot: %s  earnings=%s  (%s days)", ticker, earnings_date, days_before)

    # ── 1. Run altdata collection ─────────────────────────────────────────
    for name, module_path, cls_name in [
        ("reddit",    "altdata.collector.reddit_collector",    None),
        ("news",      "altdata.collector.news_collector",      None),
        ("sec_edgar", "altdata.collector.sec_edgar_collector", None),
        ("fred",      "altdata.collector.fred_collector",      None),
    ]:
        try:
            mod = importlib.import_module(module_path)
            results = mod.collect([ticker], market="us", config=config)
            for r in results:
                store.store_raw(r)
        except Exception as e:
            log.debug("[%s] altdata: %s", name, e)

    # ── 2. Get altdata sentiment from store ───────────────────────────────
    rows = store.get_sentiment(ticker, hours_back=48)
    def _avg(src):
        vals = [float(r["score"]) for r in rows if r.get("source") == src and r.get("score") is not None]
        return round(sum(vals) / len(vals), 4) if vals else None

    all_scores = [float(r["score"]) for r in rows if r.get("score") is not None]
    altdata_sentiment = round(sum(all_scores) / len(all_scores), 4) if all_scores else None

    # ── 3. Macro regime from altdata store ────────────────────────────────
    import json as _json
    macro_regime = 0
    macro_regime_name = "RISK_ON"
    raw_rows = store.get_raw(source="fred", ticker="MACRO", hours_back=24 * 7)
    for rrow in raw_rows:
        if rrow.get("data_type") != "macro_regime":
            continue
        raw = rrow.get("raw_data", {})
        if isinstance(raw, str):
            try:
                raw = _json.loads(raw)
            except Exception:
                continue
        if isinstance(raw, dict) and raw.get("regime_code") is not None:
            macro_regime = int(raw["regime_code"])
            macro_regime_name = raw.get("regime_name", "UNKNOWN")
            break

    # ── 4. Deepdata: options flow ─────────────────────────────────────────
    options_smfi = options_iv_rank = options_put_call = options_dark_pool = None
    try:
        from deepdata.options.flow_monitor import OptionsFlowMonitor
        monitor = OptionsFlowMonitor(config)
        opt_results = monitor.scan([ticker], market="us")
        for r in opt_results:
            dt = r.get("data_type")
            v = r.get("value")
            if dt == "smfi":             options_smfi       = v
            elif dt == "iv_rank":        options_iv_rank    = v
            elif dt == "put_call_ratio": options_put_call   = v
            elif dt == "dark_pool_score": options_dark_pool = v
    except Exception as e:
        log.debug("options flow: %s", e)

    # ── 5. Deepdata: short squeeze ────────────────────────────────────────
    short_squeeze_score = None
    try:
        import yfinance as yf
        fi_mod = importlib.import_module("deepdata.short_interest.finra_collector")
        sq_mod = importlib.import_module("deepdata.short_interest.squeeze_scorer")
        collector = fi_mod.FINRACollector(config)
        short_results = collector.collect([ticker])
        short_data = {}
        for r in short_results:
            if r.get("ticker") == ticker:
                short_data[r.get("data_type", "")] = r.get("value")
        prices_3mo = yf.download(ticker, period="3mo", auto_adjust=True, progress=False)
        scorer = sq_mod.SqueezeScorer(config)
        squeeze = scorer.score(ticker, short_data, prices_3mo)
        short_squeeze_score = squeeze.get("squeeze_score")
    except Exception as e:
        log.debug("squeeze: %s", e)

    # ── 6. Deepdata: congressional trades ─────────────────────────────────
    congressional_signal = None
    try:
        cong_mod = importlib.import_module("deepdata.congressional.disclosure_fetcher")
        fetcher = cong_mod.CongressionalDisclosureFetcher(config)
        disclosures = fetcher.fetch_recent(days_back=90)
        ticker_trades = [d for d in disclosures if d.get("ticker") == ticker]
        if ticker_trades:
            buy_count  = sum(1 for d in ticker_trades if d.get("transaction_type", "").lower() in ("purchase", "buy"))
            sell_count = sum(1 for d in ticker_trades if d.get("transaction_type", "").lower() in ("sale", "sell"))
            total = buy_count + sell_count
            congressional_signal = (buy_count - sell_count) / total if total > 0 else 0.0
    except Exception as e:
        log.debug("congressional: %s", e)

    # ── 7. Beat quality multiplier ────────────────────────────────────────
    beat_quality_multiplier = None
    try:
        bq_mod = importlib.import_module("deepdata.earnings_quality.beat_quality_classifier")
        bq = bq_mod.BeatQualityClassifier(config).classify(ticker, {})
        beat_quality_multiplier = bq.get("final_pead_multiplier")
    except Exception as e:
        log.debug("beat quality: %s", e)

    # ── 8. Market context ─────────────────────────────────────────────────
    import yfinance as yf
    import pandas as pd
    from data.earnings_collector import EarningsCollector

    ec = EarningsCollector(config)
    now_ts = pd.Timestamp.now()
    vix    = ec._vix(now_ts)
    spy_5d = ec._spy_return_5d(now_ts)

    _SECTOR_ETFS_LOCAL = {
        "Communication Services": "XLC",
        "Technology": "XLK",
        "Healthcare": "XLV",
        "Consumer Cyclical": "XLY",
        "Energy": "XLE",
        "Financial Services": "XLF",
    }
    sector_etf = None
    sector_etf_ret = None
    try:
        sector = yf.Ticker(ticker).info.get("sector", "")
        sector_etf = _SECTOR_ETFS_LOCAL.get(sector)
        if sector_etf:
            sector_etf_ret = ec._etf_return_5d(sector_etf, now_ts)
    except Exception:
        pass

    # Current price + volume surge
    price_now = vol_surge = None
    try:
        hist = yf.download(ticker, period="30d", auto_adjust=True, progress=False)
        if not hist.empty:
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
            hist.columns = [c.lower() for c in hist.columns]
            price_now = float(hist["close"].iloc[-1])
            if "volume" in hist.columns and len(hist) >= 21:
                vol_today = float(hist["volume"].iloc[-1])
                vol_avg   = float(hist["volume"].iloc[-21:-1].mean())
                vol_surge = round(vol_today / vol_avg, 2) if vol_avg > 0 else None
    except Exception:
        pass

    # ── 9. Store snapshot ─────────────────────────────────────────────────
    snapshot = {
        "ticker":                 ticker,
        "earnings_date":          earnings_date,
        "snapshot_taken_at":      now_str,
        "days_before_earnings":   days_before,
        "altdata_sentiment":      altdata_sentiment,
        "reddit_score":           _avg("reddit"),
        "news_score":             _avg("news"),
        "sec_score":              _avg("sec_edgar"),
        "options_smfi":           options_smfi,
        "options_iv_rank":        options_iv_rank,
        "options_put_call":       options_put_call,
        "options_dark_pool":      options_dark_pool,
        "short_squeeze_score":    short_squeeze_score,
        "congressional_signal":   congressional_signal,
        "beat_quality_multiplier": beat_quality_multiplier,
        "vix":                    vix,
        "macro_regime":           macro_regime,
        "macro_regime_name":      macro_regime_name,
        "spy_return_5d":          spy_5d,
        "sector_etf_return_5d":   sector_etf_ret,
        "sector_etf":             sector_etf,
        "price_at_snapshot":      price_now,
        "volume_surge":           vol_surge,
    }

    db.upsert_snapshot(snapshot)
    log.info("Snapshot stored for %s @ %s", ticker, earnings_date)

    # ── Print summary ─────────────────────────────────────────────────────
    _REGIME_NAMES = {0:"RISK_ON", 1:"GOLDILOCKS", 2:"STAGFLATION", 3:"RISK_OFF", 4:"RECESSION_RISK"}

    def _fmt(v, fmt=".3f"):
        return f"{v:{fmt}}" if v is not None else "n/a"

    print(f"\n{'='*58}")
    print(f"  PRE-EARNINGS SNAPSHOT: {ticker}")
    print(f"{'='*58}")
    print(f"  Earnings date      : {earnings_date}  ({days_before}d away)")
    print(f"  Snapshot taken     : {now_str}")
    print(f"  Price              : ${_fmt(price_now, '.2f')}   vol_surge={_fmt(vol_surge, '.2f')}x")
    print(f"\n  ── ALTDATA SIGNALS ──────────────────────────────")
    print(f"  Composite sentiment: {_fmt(altdata_sentiment)}")
    print(f"  Reddit score       : {_fmt(_avg('reddit'))}")
    print(f"  News score         : {_fmt(_avg('news'))}")
    print(f"  SEC 8-K score      : {_fmt(_avg('sec_edgar'))}")
    print(f"\n  ── DEEPDATA SIGNALS ─────────────────────────────")
    print(f"  Options SMFI       : {_fmt(options_smfi)}")
    print(f"  IV rank            : {_fmt(options_iv_rank)}")
    print(f"  Put/call ratio     : {_fmt(options_put_call)}")
    print(f"  Dark pool score    : {_fmt(options_dark_pool)}")
    print(f"  Short squeeze      : {_fmt(short_squeeze_score, '.1f')}/100")
    print(f"  Congressional      : {_fmt(congressional_signal)}  (buy-sell ratio, -1..+1)")
    print(f"  Beat quality mult  : {_fmt(beat_quality_multiplier)}")
    print(f"\n  ── MACRO CONTEXT ────────────────────────────────")
    print(f"  VIX                : {_fmt(vix, '.1f')}")
    print(f"  Macro regime       : {macro_regime_name} ({macro_regime})")
    print(f"  SPY 5d return      : {_fmt(spy_5d, '.2%') if spy_5d is not None else 'n/a'}")
    print(f"  {sector_etf or 'Sector ETF'} 5d return  : {_fmt(sector_etf_ret, '.2%') if sector_etf_ret is not None else 'n/a'}")
    print(f"\n  ── COMBINED SIGNAL ──────────────────────────────")

    # Simple composite: weight the signals we have
    signal_inputs = []
    if altdata_sentiment is not None:      signal_inputs.append(("altdata",     altdata_sentiment,    0.25))
    if options_smfi is not None:
        smfi_norm = max(-1.0, min(1.0, (options_smfi - 1.0) / 2.0))
        signal_inputs.append(("options_smfi", smfi_norm, 0.20))
    if short_squeeze_score is not None:    signal_inputs.append(("squeeze",     short_squeeze_score / 100.0, 0.15))
    if congressional_signal is not None:   signal_inputs.append(("congress",    congressional_signal, 0.15))
    if beat_quality_multiplier is not None:
        bq_norm = (beat_quality_multiplier - 1.0) / 0.5
        signal_inputs.append(("beat_quality", bq_norm, 0.25))

    if signal_inputs:
        total_w = sum(w for _, _, w in signal_inputs)
        composite = sum(v * w for _, v, w in signal_inputs) / total_w
        print(f"  Composite score    : {composite:+.3f}  ({'BULLISH' if composite > 0.1 else 'BEARISH' if composite < -0.1 else 'NEUTRAL'})")
        for label, val, wt in signal_inputs:
            print(f"    {label:<18}: {val:+.3f}  (weight={wt:.0%})")
    else:
        print(f"  Composite score    : n/a (no signals yet)")

    print(f"\n  Snapshot saved to earnings_db ✓")
    print(f"{'='*58}\n")

    return snapshot


def cmd_pead_observe(config: dict, tickers: list = None) -> None:
    """
    Collect altdata + deepdata for upcoming earnings tickers and store to earnings_db.

    Workflow:
      1. Get upcoming earnings calendar (next 30 days)
      2. Run altdata collectors for those tickers
      3. Run deepdata beat quality classifier for those tickers
      4. Store enriched observations to earnings_db
    """
    import importlib
    log = logging.getLogger(__name__)

    from data.earnings_collector import EarningsCollector
    from data.earnings_db import EarningsDB
    from altdata.storage.altdata_store import AltDataStore
    from altdata.notifications.notifier import Notifier

    store = AltDataStore(config)
    notifier = Notifier(config)
    db = EarningsDB()

    # ── Step 1: get upcoming tickers ─────────────────────────────────────────
    if tickers:
        watch_tickers = tickers
    else:
        upcoming = db.get_upcoming_calendar(days_ahead=30)
        watch_tickers = list({row["ticker"] for row in upcoming})
        if not watch_tickers:
            watch_tickers = _default_tickers(config)[:20]

    log.info("pead observe: %d tickers to enrich", len(watch_tickers))

    # ── Step 2: run altdata collection for those tickers ─────────────────────
    collector_modules = [
        ("reddit",    "altdata.collector.reddit_collector",    None),
        ("news",      "altdata.collector.news_collector",      None),
        ("sec_edgar", "altdata.collector.sec_edgar_collector", None),
        ("fred",      "altdata.collector.fred_collector",      None),
    ]

    for name, module_path, class_name in collector_modules:
        try:
            mod = importlib.import_module(module_path)
            if class_name:
                cls = getattr(mod, class_name)
                results = cls(config).collect(watch_tickers, market="us")
            else:
                results = mod.collect(watch_tickers, market="us", config=config)
            stored = 0
            for r in results:
                try:
                    store.store_raw(r)
                    stored += 1
                except Exception as e:
                    log.debug("%s store_raw failed: %s", name, e)
            log.info("[%s] %d records stored", name, stored)
        except Exception as e:
            log.warning("pead observe: altdata collector %s failed: %s", name, e)

    # ── Step 3: generate altdata signals ─────────────────────────────────────
    try:
        mod = importlib.import_module("altdata.signals.altdata_signal_engine")
        engine = mod.AltDataSignalEngine(config, store, notifier)
        signals = engine.generate(watch_tickers)
        log.info("pead observe: %d altdata signals generated", len(signals))
    except Exception as e:
        signals = []
        log.warning("pead observe: altdata signal engine failed: %s", e)

    # Build per-ticker sentiment summary
    ticker_sentiment: dict = {}
    for sig in signals:
        t = sig.get("ticker")
        if t:
            ticker_sentiment[t] = {
                "altdata_sentiment": sig.get("value", 0.0),
                "direction": sig.get("direction", 0),
                "confidence": sig.get("confidence", 0.0),
            }

    # Also query sentiment_scores table for per-source scores
    ticker_scores: dict = {}
    for t in watch_tickers:
        rows = store.get_sentiment(t, hours_back=48)
        if rows:
            by_source = {}
            for row in rows:
                src = row.get("source", "unknown")
                by_source[src] = float(row.get("score", 0.0))
            ticker_scores[t] = by_source

    # ── Step 4: run deepdata beat quality classifier ──────────────────────────
    beat_quality: dict = {}
    try:
        bq_mod = importlib.import_module("deepdata.earnings_quality.beat_quality_classifier")
        classifier = bq_mod.BeatQualityClassifier(config)
        for t in watch_tickers:
            try:
                result = classifier.classify(t, {})
                beat_quality[t] = result.get("final_pead_multiplier", 1.0)
            except Exception as e:
                log.debug("beat quality for %s: %s", t, e)
    except Exception as e:
        log.warning("pead observe: BeatQualityClassifier not available: %s", e)

    # ── Step 5: store enriched calendar observations ─────────────────────────
    collector = EarningsCollector(config)
    n_updated = 0
    for t in watch_tickers:
        sent = ticker_sentiment.get(t, {})
        scores = ticker_scores.get(t, {})
        altdata_val = sent.get("altdata_sentiment") or scores.get("overall")
        reddit_val  = scores.get("reddit")
        news_val    = scores.get("news")
        sec_val     = scores.get("sec_edgar")
        bq_val      = beat_quality.get(t)

        if any(v is not None for v in [altdata_val, reddit_val, news_val, sec_val, bq_val]):
            try:
                db.update_altdata_scores(
                    ticker=t,
                    altdata_sentiment=altdata_val,
                    reddit_score=reddit_val,
                    news_score=news_val,
                    sec_score=sec_val,
                    beat_quality_multiplier=bq_val,
                )
                n_updated += 1
            except Exception as e:
                log.debug("pead observe: update_altdata_scores %s: %s", t, e)

    print(f"\npead observe complete:")
    print(f"  Tickers watched   : {len(watch_tickers)}")
    print(f"  Altdata signals   : {len(signals)}")
    print(f"  Beat quality hits : {len(beat_quality)}")
    print(f"  DB rows updated   : {n_updated}")
    print(f"\n  Upcoming earnings with confluence scores:")
    upcoming_all = db.get_upcoming_calendar(days_ahead=30)
    for row in upcoming_all[:20]:
        sent_str = f"  sentiment={row.get('altdata_sentiment', 'n/a')}" if row.get('altdata_sentiment') is not None else ""
        print(f"  {row['earnings_date']}  {row['ticker']:<8}{sent_str}")


def cmd_earnings_collect(config: dict, tickers=None, market="us", start=None, end=None) -> None:
    from data.earnings_collector import EarningsCollector
    from data.universe import UniverseManager
    if tickers is None:
        mgr = UniverseManager(config)
        tickers = mgr.get_tickers(market)
    collector = EarningsCollector(config)
    log.info("Collecting earnings for %d tickers (%s) …", len(tickers), market)
    n = collector.collect(tickers, market=market, start=start, end=end)
    st = collector.db.status()
    print(f"\nEarnings collection complete: {n} rows written")
    print(f"  DB total observations : {st['total_observations']}")
    print(f"  High-quality events   : {st['high_quality']}")
    print(f"  Tickers covered       : {st['tickers']}")
    print(f"  Date range            : {st['date_range']}")
    print(f"  With returns (t+20)   : {st['with_returns']}")


def cmd_earnings_calendar(config: dict, tickers=None, market="us", days_ahead=30) -> None:
    from data.earnings_collector import EarningsCollector
    from data.universe import UniverseManager
    if tickers is None:
        mgr = UniverseManager(config)
        tickers = mgr.get_tickers(market)
    collector = EarningsCollector(config)
    n = collector.collect_calendar(tickers, market=market, days_ahead=days_ahead)
    upcoming = collector.db.get_upcoming_calendar(days_ahead=days_ahead)
    print(f"\nCalendar: {n} entries updated, {len(upcoming)} upcoming in next {days_ahead}d")
    for row in upcoming[:20]:
        est = f"  EPS est={row['eps_estimate']:.2f}" if row.get("eps_estimate") else ""
        print(f"  {row['earnings_date']}  {row['ticker']:<8}{est}")
    if len(upcoming) > 20:
        print(f"  … and {len(upcoming)-20} more")


def cmd_earnings_status(config: dict) -> None:
    from data.earnings_db import EarningsDB
    db = EarningsDB()
    st = db.status()
    print("\n=== Earnings Database Status ===")
    print(f"  DB path               : {st['db_path']}")
    print(f"  Total observations    : {st['total_observations']}")
    print(f"  High-quality events   : {st['high_quality']}")
    print(f"  Tickers covered       : {st['tickers']}")
    print(f"  Date range            : {st['date_range']}")
    print(f"  With returns (t+20)   : {st['with_returns']}")
    print(f"  Calendar entries      : {st['calendar_entries']}")


# ---------------------------------------------------------------------------
# Historical commands (Phase 1-6)
# ---------------------------------------------------------------------------

def cmd_historical_collect(config: dict, tickers=None, start: str = "2010-01-01", phases=None) -> None:
    from data.historical_collector import HistoricalCollector

    db_path   = config.get("historical_db_path", "output/historical_db.db")
    collector = HistoricalCollector(config, db_path=db_path)

    if tickers is None:
        from data.universe import UniverseManager
        um = UniverseManager(config)
        tickers = um.get_tickers("us")

    active_phases = set(phases) if phases else {
        "prices", "financials", "edgar", "macro", "enrich", "news", "delisted"
    }

    print(f"Historical collect: {len(tickers)} tickers, start={start}")

    if "prices" in active_phases:
        print("\n[Phase 1] Price history...")
        result = collector.collect_price_history(tickers, start=start)
        n = sum(result.values()) if isinstance(result, dict) else (result or 0)
        print(f"  Stored {n} price records")

    if "financials" in active_phases:
        print("\n[Phase 1] Financials...")
        result = collector.collect_financials(tickers)
        n = sum(result.values()) if isinstance(result, dict) else (result or 0)
        print(f"  Stored {n} financial records")

    if "edgar" in active_phases:
        print("\n[Phase 2] SEC EDGAR filings...")
        n = collector.collect_edgar(tickers, start=start)
        print(f"  Stored {n} EDGAR records")

    if "macro" in active_phases:
        print("\n[Phase 3] Macro context...")
        n = collector.collect_macro(start=start)
        print(f"  Stored {n} macro records")

    if "enrich" in active_phases:
        print("\n[Phase 5] Enriching earnings observations...")
        n = collector.enrich_earnings_observations()
        print(f"  Enriched {n} observations")

    if "news" in active_phases:
        print("\n[Phase 6] News context...")
        n = collector.collect_news(tickers)
        print(f"  Stored {n} news records")

    if "delisted" in active_phases:
        print("\n[Phase 4] Loading delisted universe...")
        from data.delisted_universe import load_delisted_into_db
        n = load_delisted_into_db(collector.db)
        print(f"  Loaded {n} delisted companies")

    print("\nHistorical collection complete.")
    cmd_historical_status(config)


def cmd_historical_status(config: dict) -> None:
    from data.historical_db import HistoricalDB
    hist_db = HistoricalDB(config.get("historical_db_path", "output/historical_db.db"))
    st = hist_db.status()
    print("\n=== HISTORICAL DATABASE STATUS ===")
    for k, v in st.items():
        print(f"  {k:<30s}: {v}")


def cmd_historical_delisted(config: dict) -> None:
    from data.historical_db import HistoricalDB
    from data.delisted_universe import load_delisted_into_db
    db_path = config.get("historical_db_path", "output/historical_db.db")
    hist_db = HistoricalDB(db_path)
    n = load_delisted_into_db(hist_db)
    print(f"Loaded {n} delisted companies into historical DB")


# ---------------------------------------------------------------------------
# Intelligence commands (Phases 7-12)
# ---------------------------------------------------------------------------

def cmd_intelligence_run(config: dict) -> None:
    from analysis.intelligence_db import IntelligenceDB
    from analysis.intelligence_engine import IntelligenceEngine
    from data.earnings_db import EarningsDB
    intel_db    = IntelligenceDB(config.get("intelligence_db_path", "output/intelligence_db.db"))
    earnings_db = EarningsDB(config.get("earnings_db_path", "output/earnings.db"))
    engine      = IntelligenceEngine(intel_db, earnings_db)
    print("Running intelligence engine...")
    summary = engine.run()
    print(f"\nIntelligence run complete:")
    for k, v in summary.items():
        print(f"  {k:<30s}: {v}")


def cmd_intelligence_report(config: dict) -> None:
    from analysis.intelligence_db import IntelligenceDB
    from analysis.intelligence_engine import IntelligenceEngine
    from data.earnings_db import EarningsDB
    intel_db    = IntelligenceDB(config.get("intelligence_db_path", "output/intelligence_db.db"))
    earnings_db = EarningsDB(config.get("earnings_db_path", "output/earnings.db"))
    engine      = IntelligenceEngine(intel_db, earnings_db)
    report      = engine.generate_report()
    print(report)


def cmd_intelligence_score(config: dict, ticker: str) -> None:
    from analysis.intelligence_db import IntelligenceDB
    from analysis.intelligence_engine import IntelligenceEngine
    from data.earnings_db import EarningsDB
    intel_db    = IntelligenceDB(config.get("intelligence_db_path", "output/intelligence_db.db"))
    earnings_db = EarningsDB(config.get("earnings_db_path", "output/earnings.db"))
    engine      = IntelligenceEngine(intel_db, earnings_db)
    result      = engine.score_ticker(ticker)
    print(result.get("report", f"No data for {ticker}"))


def cmd_intelligence_morning(config: dict) -> None:
    from intelligence.daily_pipeline import DailyPipeline
    pipeline = DailyPipeline(config)
    summary  = pipeline.run_morning()
    print(f"\nMorning prep complete: {summary}")


def cmd_intelligence_close(config: dict) -> None:
    from intelligence.daily_pipeline import DailyPipeline
    pipeline = DailyPipeline(config)
    summary  = pipeline.run_close()
    print(f"\nMarket close complete: {summary}")


def cmd_intelligence_weekly(config: dict) -> None:
    from intelligence.daily_pipeline import DailyPipeline
    pipeline = DailyPipeline(config)
    summary  = pipeline.run_weekly()
    print(f"\nWeekly analysis complete: {summary}")


def cmd_intelligence_status(config: dict) -> None:
    from intelligence.daily_pipeline import DailyPipeline
    pipeline = DailyPipeline(config)
    st       = pipeline.status()
    print("\n=== INTELLIGENCE PIPELINE STATUS ===")
    for k, v in st.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for kk, vv in v.items():
                print(f"    {kk:<30s}: {vv}")
        else:
            print(f"  {k:<30s}: {v}")


def cmd_search(config: dict, query: str, limit: int = 20, event_type: str = None) -> None:
    """Full-text search across permanent_log via FTS5."""
    from altdata.storage.permanent_store import PermanentStore
    store = PermanentStore()
    results = store.search(query, limit=limit, event_type=event_type)
    if not results:
        print(f"No results for: {query!r}")
        return
    print(f"\n=== Search: {query!r} ({len(results)} results) ===\n")
    for r in results:
        ts = r.get("timestamp", "")[:19]
        et = r.get("event_type", "")
        ticker = r.get("ticker", "")
        title = r.get("title") or r.get("description", "")[:80]
        print(f"  [{ts}] {et:20s}  {ticker:8s}  {title}")
    print()


def cmd_monitor_once(config: dict) -> None:
    """Run a single monitoring pass (position check + universe scan + SEC filings)."""
    from monitoring.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(config)
    monitor.run_once()


def cmd_monitor_run(config: dict) -> None:
    """Start the blocking real-time monitor (15/30/60-min cycles)."""
    from monitoring.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(config)
    monitor.run()


def cmd_intelligence_readthrough(config: dict, tickers=None, days: int = 14) -> None:
    from data.large_cap_influence import LargeCapInfluenceEngine
    engine  = LargeCapInfluenceEngine()
    if tickers is None:
        from data.universe import UniverseManager
        um = UniverseManager(config)
        tickers = um.get_tickers("us")[:20]  # limit for speed
    print(f"Computing readthrough signals for {len(tickers)} tickers (last {days} days)...")
    signals = engine.get_readthrough_signals(tickers, days_lookback=days)
    print(f"\n{'TICKER':<8} {'SECTOR':<20} {'SCORE':>8} {'SIGNAL':<15} {'N_EVENTS':>8}")
    print("-" * 65)
    for s in sorted(signals, key=lambda x: x.get("readthrough_score", 0), reverse=True):
        print(f"  {s['peer_ticker']:<6} {(s.get('sector') or ''):<20} "
              f"{s.get('readthrough_score', 0):+8.3f} {s.get('signal',''):<15} "
              f"{s.get('n_events', 0):>8}")


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Quant Fund — PEAD & anomaly-driven equity strategy"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # backtest
    p_bt = sub.add_parser("backtest", help="Run historical backtest")
    p_bt.add_argument("--market", choices=["us", "uk", "both"], default="us")
    p_bt.add_argument("--tickers-file", default=None, help="CSV of tickers (no header)")
    p_bt.add_argument("--max-tickers", type=int, default=None, help="Cap universe size for speed (e.g. 300)")

    # anomaly_scan
    p_an = sub.add_parser("anomaly_scan", help="Scan for statistical anomalies")
    p_an.add_argument("--market", choices=["us", "uk", "both"], default="us")

    # paper_trade (with optional subcommands)
    p_pt = sub.add_parser("paper_trade", help="Run live paper trading loop (or subcommands)")
    pt_sub = p_pt.add_subparsers(dest="paper_trade_command", required=False)
    pt_sub.add_parser("status",  help="Show all open positions")
    pt_sub.add_parser("history", help="Show last 20 closed trades")
    p_pt.add_argument("--once", action="store_true", help="Run one US scan now and exit")

    # ── bot commands ──────────────────────────────────────────────────
    p_bot = sub.add_parser("bot", help="Continuous trading bot controller")
    bot_sub = p_bot.add_subparsers(dest="bot_command")
    p_bot_start = bot_sub.add_parser("start", help="Start the trading bot")
    p_bot_start.add_argument("--background", action="store_true")
    bot_sub.add_parser("stop",       help="Stop running bot")
    bot_sub.add_parser("pause",      help="Pause trading (keeps data collection)")
    bot_sub.add_parser("resume",     help="Resume paused bot")
    bot_sub.add_parser("status",     help="Show bot status")
    bot_sub.add_parser("cron-setup", help="Install cron jobs (watchdog + reboot restart)")

    # report
    p_rp = sub.add_parser("report", help="Generate report from saved results")
    p_rp.add_argument("--prefix", default="backtest_us", help="Output file prefix")

    # promote
    p_pr = sub.add_parser("promote", help="Manually promote a validated signal to live")
    p_pr.add_argument("--signal", required=True, help="Signal name from registry")

    # validate
    sub.add_parser("status",   help="System health: API keys, collectors, DB counts, signals")
    sub.add_parser("validate", help="Run full test suite and module validation")

    # altdata (nested subcommand)
    p_alt = sub.add_parser("altdata", help="Alt-data intelligence pipeline commands")
    alt_sub = p_alt.add_subparsers(dest="altdata_command", required=True)

    p_alt_collect = alt_sub.add_parser("collect", help="Run all data collectors")
    p_alt_collect.add_argument("--tickers", nargs="*", default=None, help="Ticker list")

    p_alt_signals = alt_sub.add_parser("signals", help="Generate alt-data signals")
    p_alt_signals.add_argument("--tickers", nargs="*", default=None, help="Ticker list")

    alt_sub.add_parser("dashboard", help="Render the daily dashboard")
    alt_sub.add_parser("nonsense", help="Run nonsense detector scan")

    p_alt_rb = alt_sub.add_parser("rollback", help="Roll back model to a prior version")
    p_alt_rb.add_argument("--to-version", required=True, help="Model version string")

    alt_sub.add_parser("status", help="Show pipeline status summary")

    # deepdata (nested subcommand)
    p_dd = sub.add_parser("deepdata", help="Deep data intelligence pipeline commands")
    dd_sub = p_dd.add_subparsers(dest="deepdata_command", required=True)

    dd_sub.add_parser("status", help="Show deepdata pipeline status")

    p_dd_collect = dd_sub.add_parser("collect", help="Run all deepdata collectors")
    p_dd_collect.add_argument("--tickers", nargs="*", default=None)

    dd_sub.add_parser("dashboard", help="Render the deepdata dashboard")

    p_dd_opt = dd_sub.add_parser("options", help="Options flow analysis for a ticker")
    p_dd_opt.add_argument("--ticker", required=True)

    p_dd_sq = dd_sub.add_parser("squeeze", help="Squeeze scoring for a ticker")
    p_dd_sq.add_argument("--ticker", required=True)

    p_dd_cg = dd_sub.add_parser("congress", help="Congressional disclosures")
    p_dd_cg.add_argument("--days", type=int, default=30)

    dd_sub.add_parser("patterns", help="Cross-module pattern scanner")

    p_dd_tr = dd_sub.add_parser("transcript", help="Transcript analysis for a ticker")
    p_dd_tr.add_argument("--ticker", required=True)

    # frontier (nested subcommand)
    p_fr = sub.add_parser("frontier", help="Frontier intelligence pipeline commands")
    fr_sub = p_fr.add_subparsers(dest="frontier_command", required=True)

    fr_sub.add_parser("status", help="Show frontier module status")

    p_fr_collect = fr_sub.add_parser("collect", help="Run all frontier collectors")
    p_fr_collect.add_argument("--tickers", nargs="*", default=None)

    fr_sub.add_parser("umci", help="Compute Unified Market Complexity Index")
    fr_sub.add_parser("dashboard", help="Render frontier intelligence dashboard")
    fr_sub.add_parser("discover", help="Run cross-signal discovery scan")
    fr_sub.add_parser("watchlist", help="Show signal watchlist")

    p_fr_val = fr_sub.add_parser("validate", help="Validate a frontier signal")
    p_fr_val.add_argument("--signal", required=True)

    fr_sub.add_parser("geomagnetic", help="Geomagnetic signal (GRAI)")
    fr_sub.add_parser("attention", help="Attention economy signal (ASI)")
    fr_sub.add_parser("quantum", help="Quantum readiness signal (QTPI)")

    # pead (nested subcommand)
    p_pead_cmd = sub.add_parser("pead", help="PEAD signal enrichment commands")
    pead_sub = p_pead_cmd.add_subparsers(dest="pead_command", required=True)
    p_pead_obs = pead_sub.add_parser("observe", help="Collect altdata+deepdata for upcoming earnings and store scores")
    p_pead_obs.add_argument("--tickers", nargs="*", default=None, help="Override ticker list")
    p_pead_snap = pead_sub.add_parser("snapshot", help="Capture full pre-earnings snapshot for a ticker")
    p_pead_snap.add_argument("--ticker", required=True)
    p_pead_snap.add_argument("--earnings-date", default=None, help="Override earnings date YYYY-MM-DD")
    p_pead_out = pead_sub.add_parser("outcome", help="Record post-earnings outcome into snapshot")
    p_pead_out.add_argument("--ticker", required=True)
    p_pead_out.add_argument("--earnings-date", required=True)
    p_pead_out.add_argument("--return-t1",  type=float, default=None)
    p_pead_out.add_argument("--return-t3",  type=float, default=None)
    p_pead_out.add_argument("--return-t5",  type=float, default=None)
    p_pead_out.add_argument("--return-t20", type=float, default=None)
    p_pead_out.add_argument("--eps-surprise", type=float, default=None)

    # earnings (nested subcommand)
    p_earn = sub.add_parser("earnings", help="Earnings data collection and database commands")
    earn_sub = p_earn.add_subparsers(dest="earnings_command", required=True)

    p_earn_collect = earn_sub.add_parser("collect", help="Collect historical earnings observations")
    p_earn_collect.add_argument("--tickers", nargs="*", default=None)
    p_earn_collect.add_argument("--market", default="us")
    p_earn_collect.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default 7 years ago)")
    p_earn_collect.add_argument("--end",   default=None, help="End date YYYY-MM-DD (default today)")

    p_earn_cal = earn_sub.add_parser("calendar", help="Collect upcoming earnings calendar")
    p_earn_cal.add_argument("--tickers", nargs="*", default=None)
    p_earn_cal.add_argument("--market", default="us")
    p_earn_cal.add_argument("--days-ahead", type=int, default=30)

    earn_sub.add_parser("status", help="Show earnings database status")

    p_earn_sched = earn_sub.add_parser("schedule", help="Run daily 6pm UK earnings scheduler (blocking)")
    p_earn_sched.add_argument("--market", default="us")
    p_earn_sched.add_argument("--time", default="18:00", dest="run_time",
                              help="Daily run time HH:MM in UK local time (default 18:00)")

    # closeloop (nested subcommand)
    p_cl = sub.add_parser("closeloop", help="Closed-loop learning and monitoring commands")
    cl_sub = p_cl.add_subparsers(dest="closeloop_command", required=True)

    cl_sub.add_parser("status", help="Show closed-loop store status")
    cl_sub.add_parser("dashboard", help="Render full closed-loop dashboard")
    cl_sub.add_parser("stress", help="Run stress test against crisis scenarios")
    cl_sub.add_parser("weights", help="Show signal weights")
    p_cl_autopsy = cl_sub.add_parser("autopsy", help="Run trade autopsy")
    p_cl_autopsy.add_argument("--trade-id", type=int, default=None)
    cl_sub.add_parser("tax", help="UK CGT tax summary")
    cl_sub.add_parser("benchmark", help="Benchmark comparison (IR vs SPY/IWM/EWU/ACWI)")
    cl_sub.add_parser("wire", help="Run module wirer (fix 12 disconnects)")
    p_cl_entry = cl_sub.add_parser("entry", help="Entry conditions for a ticker")
    p_cl_entry.add_argument("--ticker", required=False, default=None)
    p_cl_peers = cl_sub.add_parser("peers", help="Peer influence map for a ticker")
    p_cl_peers.add_argument("--ticker", required=False, default=None)
    p_cl_rev = cl_sub.add_parser("revisions", help="Analyst revision tracker for a ticker")
    p_cl_rev.add_argument("--ticker", required=False, default=None)

    # --- historical ---
    p_hist = sub.add_parser("historical", help="Historical data collection and enrichment")
    hist_sub = p_hist.add_subparsers(dest="historical_command", required=True)
    p_hist_collect = hist_sub.add_parser("collect", help="Collect full historical data for tickers")
    p_hist_collect.add_argument("--tickers", nargs="+", default=None)
    p_hist_collect.add_argument("--start", default="2010-01-01")
    p_hist_collect.add_argument("--phases", nargs="+",
        choices=["prices","financials","edgar","macro","enrich","news","delisted"],
        default=None, help="Phases to run (default: all)")
    hist_sub.add_parser("status", help="Show historical database status")
    hist_sub.add_parser("delisted", help="Load delisted universe into historical DB")

    # --- search ---
    p_search = sub.add_parser("search", help="Full-text search across permanent event log")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--limit", type=int, default=20)
    p_search.add_argument("--type", dest="event_type", default=None,
                          help="Filter by event type (signal, trade, earnings, insider, article, ...)")

    # --- monitor ---
    p_mon = sub.add_parser("monitor", help="Real-time monitoring and alerting")
    mon_sub = p_mon.add_subparsers(dest="monitor_command", required=True)
    mon_sub.add_parser("run", help="Start blocking real-time monitor loop")
    mon_sub.add_parser("once", help="Run one monitoring pass (positions + universe + SEC)")

    # --- intelligence ---
    p_intel = sub.add_parser("intelligence", help="Intelligence engine commands")
    intel_sub = p_intel.add_subparsers(dest="intelligence_command", required=True)
    intel_sub.add_parser("run", help="Run full intelligence engine (patterns + profiles)")
    intel_sub.add_parser("report", help="Generate and print intelligence report")
    p_intel_score = intel_sub.add_parser("score", help="Score a ticker with intelligence engine")
    p_intel_score.add_argument("ticker", help="Ticker to score")
    intel_sub.add_parser("morning", help="Run morning prep pipeline (readthrough + snapshots)")
    intel_sub.add_parser("close", help="Run market close pipeline (outcomes + engine update)")
    intel_sub.add_parser("weekly", help="Run weekly deep analysis (coefficients + report)")
    intel_sub.add_parser("status", help="Show intelligence DB status")
    p_intel_rt = intel_sub.add_parser("readthrough", help="Show readthrough signals for tickers")
    p_intel_rt.add_argument("--tickers", nargs="+", default=None)
    p_intel_rt.add_argument("--days", type=int, default=14)

    # schedule (automation scheduler)
    p_sched = sub.add_parser("schedule", help="Automation scheduler commands")
    sched_sub = p_sched.add_subparsers(dest="schedule_command", required=True)
    sched_sub.add_parser("start",  help="Start the daily automation scheduler (blocking)")
    sched_sub.add_parser("status", help="Show scheduled jobs and next run times")

    p_eq = sub.add_parser("discover_equations", help="Symbolic regression equation discovery")
    p_eq.add_argument("--status", action="store_true", help="Show equation database status")
    p_eq.add_argument("--tickers", type=int, default=50, help="Max tickers to use (default 50)")

    args = parser.parse_args()
    config = load_config()

    if args.command == "backtest":
        markets = ["us", "uk"] if args.market == "both" else [args.market]
        for m in markets:
            cmd_backtest(config, m, tickers_file=getattr(args, "tickers_file", None), max_tickers=getattr(args, "max_tickers", None))

    elif args.command == "anomaly_scan":
        markets = ["us", "uk"] if args.market == "both" else [args.market]
        for m in markets:
            cmd_anomaly_scan(config, m)

    elif args.command == "paper_trade":
        pt_cmd = getattr(args, "paper_trade_command", None)
        if pt_cmd == "status":
            cmd_paper_trade_status(config)
        elif pt_cmd == "history":
            cmd_paper_trade_history(config)
        elif getattr(args, "once", False):
            cmd_paper_trade_once(config)
        else:
            cmd_paper_trade(config)

    elif args.command == "bot":
        _run_bot_command(config, args)

    elif args.command == "report":
        cmd_report(config, args.prefix)

    elif args.command == "promote":
        cmd_promote(config, args.signal)

    elif args.command == "status":
        cmd_status(config)

    elif args.command == "validate":
        cmd_validate(config)

    elif args.command == "altdata":
        ac = args.altdata_command
        if ac == "collect":
            cmd_altdata_collect(config, tickers=getattr(args, "tickers", None))
        elif ac == "signals":
            cmd_altdata_signals(config, tickers=getattr(args, "tickers", None))
        elif ac == "dashboard":
            cmd_altdata_dashboard(config)
        elif ac == "nonsense":
            cmd_altdata_nonsense(config)
        elif ac == "rollback":
            cmd_altdata_rollback(config, to_version=args.to_version)
        elif ac == "status":
            cmd_altdata_status(config)

    elif args.command == "deepdata":
        dc = args.deepdata_command
        if dc == "status":
            cmd_deepdata_status(config)
        elif dc == "collect":
            cmd_deepdata_collect(config, tickers=getattr(args, "tickers", None))
        elif dc == "dashboard":
            cmd_deepdata_dashboard(config)
        elif dc == "options":
            cmd_deepdata_options(config, ticker=args.ticker)
        elif dc == "squeeze":
            cmd_deepdata_squeeze(config, ticker=args.ticker)
        elif dc == "congress":
            cmd_deepdata_congress(config, days_back=getattr(args, "days", 30))
        elif dc == "patterns":
            cmd_deepdata_patterns(config)
        elif dc == "transcript":
            cmd_deepdata_transcript(config, ticker=args.ticker)

    elif args.command == "frontier":
        fc = args.frontier_command
        if fc == "status":
            cmd_frontier_status(config)
        elif fc == "collect":
            cmd_frontier_collect(config, tickers=getattr(args, "tickers", None))
        elif fc == "umci":
            cmd_frontier_umci(config)
        elif fc == "dashboard":
            cmd_frontier_dashboard(config)
        elif fc == "discover":
            cmd_frontier_discover(config)
        elif fc == "watchlist":
            cmd_frontier_watchlist(config)
        elif fc == "validate":
            cmd_frontier_validate_signal(config, signal_name=args.signal)
        elif fc == "geomagnetic":
            cmd_frontier_geomagnetic(config)
        elif fc == "attention":
            cmd_frontier_attention(config)
        elif fc == "quantum":
            cmd_frontier_quantum(config)

    elif args.command == "pead":
        if args.pead_command == "observe":
            cmd_pead_observe(config, tickers=getattr(args, "tickers", None))
        elif args.pead_command == "snapshot":
            cmd_pead_snapshot(
                config,
                ticker=args.ticker,
                earnings_date=getattr(args, "earnings_date", None),
            )
        elif args.pead_command == "outcome":
            from data.earnings_db import EarningsDB
            db = EarningsDB()
            n = db.update_snapshot_outcome(
                ticker=args.ticker,
                earnings_date=args.earnings_date,
                return_t1=getattr(args, "return_t1", None),
                return_t3=getattr(args, "return_t3", None),
                return_t5=getattr(args, "return_t5", None),
                return_t20=getattr(args, "return_t20", None),
                eps_surprise=getattr(args, "eps_surprise", None),
            )
            print(f"Outcome recorded: {n} snapshot(s) updated for {args.ticker} @ {args.earnings_date}")

    elif args.command == "earnings":
        ec = args.earnings_command
        if ec == "collect":
            cmd_earnings_collect(
                config,
                tickers=getattr(args, "tickers", None),
                market=getattr(args, "market", "us"),
                start=getattr(args, "start", None),
                end=getattr(args, "end", None),
            )
        elif ec == "calendar":
            cmd_earnings_calendar(
                config,
                tickers=getattr(args, "tickers", None),
                market=getattr(args, "market", "us"),
                days_ahead=getattr(args, "days_ahead", 30),
            )
        elif ec == "status":
            cmd_earnings_status(config)
        elif ec == "schedule":
            from data.earnings_scheduler import run_scheduler
            run_scheduler(
                config,
                market=getattr(args, "market", "us"),
                run_time=getattr(args, "run_time", "18:00"),
            )

    elif args.command == "closeloop":
        cc = args.closeloop_command
        if cc == "status":
            cmd_closeloop_status(config)
        elif cc == "dashboard":
            cmd_closeloop_dashboard(config)
        elif cc == "stress":
            cmd_closeloop_stress(config)
        elif cc == "weights":
            cmd_closeloop_weights(config)
        elif cc == "autopsy":
            cmd_closeloop_autopsy(config, trade_id=getattr(args, "trade_id", None))
        elif cc == "tax":
            cmd_closeloop_tax(config)
        elif cc == "benchmark":
            cmd_closeloop_benchmark(config)
        elif cc == "wire":
            cmd_closeloop_wire(config)
        elif cc == "entry":
            cmd_closeloop_entry(config, ticker=getattr(args, "ticker", None))
        elif cc == "peers":
            cmd_closeloop_peers(config, ticker=getattr(args, "ticker", None))
        elif cc == "revisions":
            cmd_closeloop_revisions(config, ticker=getattr(args, "ticker", None))

    elif args.command == "historical":
        hc = args.historical_command
        if hc == "collect":
            cmd_historical_collect(
                config,
                tickers=getattr(args, "tickers", None),
                start=getattr(args, "start", "2010-01-01"),
                phases=getattr(args, "phases", None),
            )
        elif hc == "status":
            cmd_historical_status(config)
        elif hc == "delisted":
            cmd_historical_delisted(config)

    elif args.command == "search":
        cmd_search(
            config,
            query=args.query,
            limit=getattr(args, "limit", 20),
            event_type=getattr(args, "event_type", None),
        )

    elif args.command == "monitor":
        mc = args.monitor_command
        if mc == "run":
            cmd_monitor_run(config)
        elif mc == "once":
            cmd_monitor_once(config)

    elif args.command == "intelligence":
        ic = args.intelligence_command
        if ic == "run":
            cmd_intelligence_run(config)
        elif ic == "report":
            cmd_intelligence_report(config)
        elif ic == "score":
            cmd_intelligence_score(config, ticker=args.ticker)
        elif ic == "morning":
            cmd_intelligence_morning(config)
        elif ic == "close":
            cmd_intelligence_close(config)
        elif ic == "weekly":
            cmd_intelligence_weekly(config)
        elif ic == "status":
            cmd_intelligence_status(config)
        elif ic == "readthrough":
            cmd_intelligence_readthrough(
                config,
                tickers=getattr(args, "tickers", None),
                days=getattr(args, "days", 14),
            )


    elif args.command == "schedule":
        from intelligence.automation_scheduler import AutomationScheduler
        sc = args.schedule_command
        if sc == "start":
            AutomationScheduler().run()
        elif sc == "status":
            print(AutomationScheduler().status())

    elif args.command == "discover_equations":
        from analysis.symbolic_regression import SymbolicRegressionEngine
        sre = SymbolicRegressionEngine(config)
        if args.status:
            eqs = sre.get_equation_status()
            print(f"\nDISCOVERED EQUATIONS DATABASE")
            print("=" * 60)
            print(f"Total equations: {len(eqs)}")
            print(f"Active equations: {sum(1 for e in eqs if e.get('is_active'))}")
            if eqs:
                print("\nTop equations by IC score:")
                for e in eqs[:5]:
                    print(f"  IC={e['ic_score']:.4f}  Sharpe={e['sharpe']:.2f}  [{e['engine']}]")
                    print(f"    {e['equation'][:80]}")
            else:
                print("\nNo equations discovered yet.")
                print("Run: python3 main.py discover_equations --tickers 50")
        else:
            print(f"Starting equation discovery with up to {args.tickers} tickers...")
            found = sre.run_discovery_pipeline(max_tickers=args.tickers)
            print(f"\nDiscovery complete: {len(found)} valid equations found")


def _run_bot_command(config: dict, args) -> None:
    cmd = getattr(args, 'bot_command', None) or 'status'

    if cmd == 'start':
        background = getattr(args, 'background', False)
        if background:
            import subprocess, os
            from datetime import datetime as _dt
            log_path = f"logs/bot_{_dt.now().strftime('%Y%m%d')}.log"
            Path("logs").mkdir(exist_ok=True)
            proc = subprocess.Popen(
                ["python3", "main.py", "bot", "start"],
                stdout=open(log_path, 'a'),
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            Path("output").mkdir(exist_ok=True)
            Path("output/bot.pid").write_text(str(proc.pid))
            print(f"Bot started in background (PID {proc.pid})")
            print(f"Logs: {log_path}")
            print(f"Stop: python3 main.py bot stop")
        else:
            from execution.trading_bot import TradingBot
            bot = TradingBot(config)
            bot.run_continuous()

    elif cmd == 'stop':
        try:
            pid_file = Path("output/bot.pid")
            if pid_file.exists():
                pid = int(pid_file.read_text().strip())
                import os, signal as _signal
                os.kill(pid, _signal.SIGTERM)
                pid_file.unlink(missing_ok=True)
                print(f"Stop signal sent to PID {pid}")
            else:
                print("No bot.pid found — bot may not be running")
        except Exception as e:
            print(f"Stop failed: {e}")

    elif cmd == 'pause':
        _bot_signal_file('PAUSE')

    elif cmd == 'resume':
        _bot_signal_file('RESUME')

    elif cmd == 'cron-setup':
        _setup_cron()

    elif cmd == 'status':
        _show_bot_status()

    else:
        _show_bot_status()


def _bot_signal_file(action: str) -> None:
    try:
        pid_file = Path("output/bot.pid")
        if pid_file.exists():
            pid = int(pid_file.read_text().strip())
            import os, signal as _signal
            os.kill(pid, _signal.SIGUSR1 if action == 'PAUSE' else _signal.SIGUSR2)
            print(f"{action} signal sent to PID {pid}")
        else:
            print("Bot not running (no bot.pid found)")
    except Exception as e:
        print(f"Signal failed: {e}")


def _show_bot_status() -> None:
    import json as _json
    status_file = Path("output/bot_status.json")
    print("\n" + "=" * 60)
    print("  BOT STATUS")
    print("=" * 60)
    if not status_file.exists():
        print("  Bot has never been started (no output/bot_status.json)")
        print("=" * 60 + "\n")
        return
    try:
        data = _json.loads(status_file.read_text())
        print(f"  Status:          {data.get('status','?')}")
        print(f"  Last updated:    {data.get('timestamp','?')[:19]}")
        print(f"  Running:         {data.get('running','?')}")
        print(f"  Paused:          {data.get('paused', False)}")
        print(f"  Alpaca:          {'YES' if data.get('use_alpaca') else 'SIMULATION'}")
        print(f"  Phase:           {data.get('phase','?')}")
        print(f"  Scan count:      {data.get('scan_count',0)}")
        print(f"  Errors today:    {data.get('errors_today',0)}")
        print(f"  Articles today:  {data.get('articles_fetched_today',0)}")
        lu = data.get('last_uk_scan')
        us = data.get('last_us_scan')
        dc = data.get('last_data_collection')
        if lu: print(f"  Last UK scan:    {lu[:19]}")
        if us: print(f"  Last US scan:    {us[:19]}")
        if dc: print(f"  Last data coll:  {dc[:19]}")
    except Exception as e:
        print(f"  Error reading status: {e}")
    print("=" * 60 + "\n")


def _setup_cron() -> None:
    """
    Install cron jobs for the trading bot:
      @reboot     — auto-start on system boot
      */5 * * * * — watchdog: restart bot if not running (weekdays 06:00-22:00 UTC)
      0 6 * * 1-5 — daily pre-market rebuild of universe
    Idempotent: replaces any existing quant-fund cron block.
    """
    import subprocess, os, tempfile
    project_dir = str(Path(__file__).resolve().parent)
    python = "python3"
    log_dir = os.path.join(project_dir, "logs")
    watchdog_script = os.path.join(project_dir, "scripts", "bot_watchdog.sh")

    # Write the watchdog shell script
    scripts_dir = Path(project_dir) / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    watchdog_content = f"""#!/bin/bash
# Bot watchdog — restart if not running during market hours (Mon-Fri 06-22 UTC)
HOUR=$(date -u +%H)
DOW=$(date -u +%u)  # 1=Mon 7=Sun
[ "$DOW" -gt 5 ] && exit 0   # skip weekend
[ "$HOUR" -lt 6 ] && exit 0  # skip pre-dawn
[ "$HOUR" -ge 22 ] && exit 0 # skip late night

PID_FILE="{project_dir}/output/bot.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        exit 0  # running fine
    fi
fi
# Not running — start it
cd "{project_dir}"
nohup {python} main.py bot start >> "{log_dir}/bot_watchdog.log" 2>&1 &
echo "$!" > "$PID_FILE"
echo "$(date -u): Bot restarted by watchdog" >> "{log_dir}/bot_watchdog.log"
"""
    watchdog_path = scripts_dir / "bot_watchdog.sh"
    watchdog_path.write_text(watchdog_content)
    watchdog_path.chmod(0o755)

    # Read existing crontab (ignore error if empty)
    new_jobs = [
        f"# QUANT-FUND BOT CRON — auto-managed by 'python3 main.py bot cron-setup'",
        f"@reboot     cd {project_dir} && {python} main.py bot start --background >> {log_dir}/bot_reboot.log 2>&1",
        f"*/5 * * * * {watchdog_script} >> {log_dir}/watchdog_cron.log 2>&1",
        f"0 6 * * 1-5 cd {project_dir} && {python} -c \"from data.universe_builder import UniverseBuilder; b=UniverseBuilder(); b.save(b.build())\" >> {log_dir}/universe_rebuild.log 2>&1",
        f"30 21 * * 1-5 cd {project_dir} && {python} main.py paper_trade --once >> {log_dir}/eod_scan.log 2>&1",
    ]

    # Try system crontab first
    crontab_ok = False
    try:
        existing = subprocess.check_output(["crontab", "-l"], stderr=subprocess.DEVNULL).decode()
        lines = [l for l in existing.splitlines()
                 if "quant-fund" not in l and "bot_watchdog" not in l
                 and "universe_builder" not in l and "# QUANT" not in l]
        lines.extend([""] + new_jobs + [""])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cron', delete=False) as f:
            f.write("\n".join(lines) + "\n")
            tmp = f.name
        subprocess.run(["crontab", tmp], check=True)
        os.unlink(tmp)
        crontab_ok = True
        print("Cron jobs installed via crontab:")
        for j in new_jobs:
            print(f"  {j}")
        print("\nVerify with: crontab -l")
    except Exception:
        pass

    if not crontab_ok:
        # Fall back: write cron file + systemd user service
        cron_file = Path(project_dir) / "scripts" / "quant_fund.cron"
        cron_file.write_text("\n".join(new_jobs) + "\n")

        service_content = f"""[Unit]
Description=Quant Fund Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={project_dir}
ExecStart={python} main.py bot start
ExecStop={python} main.py bot stop
Restart=on-failure
RestartSec=60
StandardOutput=append:{log_dir}/bot_service.log
StandardError=append:{log_dir}/bot_service.log

[Install]
WantedBy=default.target
"""
        service_dir = Path.home() / ".config" / "systemd" / "user"
        service_dir.mkdir(parents=True, exist_ok=True)
        service_path = service_dir / "quant-fund-bot.service"
        service_path.write_text(service_content)

        print("crontab not available — wrote alternative startup files:")
        print(f"  Cron entries:    {cron_file}")
        print(f"  systemd service: {service_path}")
        print("\nTo enable auto-start with systemd:")
        print("  systemctl --user enable quant-fund-bot")
        print("  systemctl --user start  quant-fund-bot")
        print("\nTo install cron manually, run:")
        print(f"  crontab {cron_file}")
        print("\nCron entries written:")
        for j in new_jobs:
            print(f"  {j}")


if __name__ == "__main__":
    main()
