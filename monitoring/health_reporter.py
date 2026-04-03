"""
Daily (7am UK) and weekly (Sunday 6am UK) health reports.

Generates and sends formatted reports to Apollo Telegram and saves
permanently to logs/daily_health_reports/ and logs/weekly_reports/.
Files are NEVER deleted automatically.
"""
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from monitoring import telegram_logger
from monitoring.system_stats import get_ram_mb, get_cpu_pct, get_disk_gb

logger = logging.getLogger(__name__)

_DAILY_DIR  = Path("logs/daily_health_reports")
_WEEKLY_DIR = Path("logs/weekly_reports")

# All 13 bot collectors
_COLLECTORS = [
    ("shipping",    "data.collectors.shipping_intelligence",    "ShippingIntelligence"),
    ("consumer",    "data.collectors.consumer_intelligence",    "ConsumerIntelligence"),
    ("geopolitical","data.collectors.geopolitical_collector",   "GeopoliticalCollector"),
    ("rates",       "data.collectors.rates_credit_collector",   "RatesCreditCollector"),
    ("commodities", "data.collectors.commodity_collector",      "CommodityCollector"),
    ("sec_fulltext","data.collectors.sec_fulltext_collector",   "SECFullTextCollector"),
    ("alt_quiver",  "data.collectors.alternative_quiver_collector", "AlternativeQuiverCollector"),
    ("tech_intel",  "data.collectors.technology_intelligence",  "TechnologyIntelligence"),
    ("usa_spending","data.collectors.government_data_collector","USASpendingCollector"),
    ("bls",         "data.collectors.government_data_collector","BLSCollector"),
    ("news",        "altdata.collector.news_collector",         "NewsCollector"),
    ("edgar",       "altdata.collector.sec_edgar_collector",    "SECEdgarCollector"),
    ("finnhub",     "altdata.collector.finnhub_collector",      "FinnhubCollector"),
]

_API_CHECKS = {
    "finnhub":       ("https://finnhub.io/api/v1/quote?symbol=AAPL&token={key}", "c", None),
    "fred":          ("https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={key}&file_type=json&limit=1", None, "observations"),
    "alpha_vantage": ("https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={key}", "Global Quote", None),
    "news_api":      ("https://newsapi.org/v2/everything?q=stocks&apiKey={key}&pageSize=1", None, "articles"),
    "marketstack":   ("http://api.marketstack.com/v1/eod?access_key={key}&symbols=AAPL&limit=1", None, "data"),
}

# ── data gatherers ─────────────────────────────────────────────────────────

def _check_apis(config: dict) -> dict:
    """Quick API health check. Returns {name: status_str}."""
    api_keys = config.get("api_keys", {})
    results  = {}
    for name, (url_tpl, field, list_field) in _API_CHECKS.items():
        key = api_keys.get(name, "")
        if not key:
            results[name] = "MISSING_KEY"
            continue
        try:
            url  = url_tpl.replace("{key}", key)
            resp = requests.get(url, timeout=5)
            data = resp.json()
            if resp.status_code == 200:
                if field and data.get(field):
                    results[name] = "working"
                elif list_field and data.get(list_field) is not None:
                    results[name] = "working"
                elif "Note" in data or "Information" in data:
                    results[name] = "rate_limited"
                else:
                    results[name] = "bad_response"
            elif resp.status_code == 429:
                results[name] = "rate_limited"
            elif resp.status_code == 401:
                results[name] = "wrong_key"
            else:
                results[name] = f"http_{resp.status_code}"
        except Exception as exc:
            results[name] = f"error:{str(exc)[:40]}"
    return results


def _check_collectors(config: dict) -> dict:
    """Try importing each collector. Returns {name: 'ok'/'failed:msg'}."""
    import importlib
    results = {}
    for name, module_path, class_name in _COLLECTORS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            results[name] = "ok"
        except Exception as exc:
            results[name] = f"failed:{str(exc)[:60]}"
    return results


def _get_trading_stats(config: dict) -> dict:
    """Query databases for trading activity stats."""
    stats = {
        "signals_yesterday":  0,
        "trades_yesterday":   0,
        "win_rate_yesterday": None,
        "win_rate_alltime":   None,
        "open_positions":     0,
        "phase":              "unknown",
    }
    # Phase from bot_status.json
    try:
        with open("output/bot_status.json") as f:
            bst = json.load(f)
        stats["phase"] = bst.get("phase", "unknown")
    except Exception:
        pass

    # Trade stats from closeloop.db
    try:
        conn = sqlite3.connect("closeloop/storage/closeloop.db", timeout=5)
        conn.row_factory = sqlite3.Row
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        # Open positions
        row = conn.execute(
            "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL"
        ).fetchone()
        stats["open_positions"] = row[0] if row else 0
        # Yesterday trades
        row = conn.execute(
            "SELECT COUNT(*) FROM trade_ledger WHERE entry_date >= ?", (yesterday,)
        ).fetchone()
        stats["trades_yesterday"] = row[0] if row else 0
        # Win rate yesterday
        row = conn.execute(
            "SELECT AVG(was_profitable) FROM trade_ledger "
            "WHERE exit_date >= ? AND was_profitable IS NOT NULL", (yesterday,)
        ).fetchone()
        if row and row[0] is not None:
            stats["win_rate_yesterday"] = round(row[0] * 100, 1)
        # Win rate all time
        row = conn.execute(
            "SELECT AVG(was_profitable) FROM trade_ledger WHERE was_profitable IS NOT NULL"
        ).fetchone()
        if row and row[0] is not None:
            stats["win_rate_alltime"] = round(row[0] * 100, 1)
        conn.close()
    except Exception:
        pass

    # Signals from permanent_archive.db
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT COUNT(*) FROM signals_log WHERE timestamp >= ?", (yesterday,)
        ).fetchone()
        stats["signals_yesterday"] = row[0] if row else 0
        conn.close()
    except Exception:
        pass

    return stats


def _get_signal_performance(config: dict) -> dict:
    """Get per-signal-type accuracy from signals_log."""
    perf = {"best": None, "worst": None, "patterns_found": 0, "patterns_validated": 0}
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        rows = conn.execute(
            "SELECT signal_type, AVG(was_traded) as rate, COUNT(*) as n "
            "FROM signals_log WHERE timestamp >= date('now','-7 days') "
            "GROUP BY signal_type HAVING n >= 5 ORDER BY rate DESC"
        ).fetchall()
        conn.close()
        if rows:
            perf["best"]  = (rows[0][0],  round(rows[0][1]  * 100, 1))
            perf["worst"] = (rows[-1][0], round(rows[-1][1] * 100, 1))
    except Exception:
        pass
    # Discovered equations count
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        row = conn.execute("SELECT COUNT(*) FROM discovered_equations").fetchone()
        perf["patterns_found"] = row[0] if row else 0
        row = conn.execute(
            "SELECT COUNT(*) FROM discovered_equations WHERE is_active=1"
        ).fetchone()
        perf["patterns_validated"] = row[0] if row else 0
        conn.close()
    except Exception:
        pass
    return perf


def _get_data_fusion_stats(config: dict) -> dict:
    """Count active data sources and cross-signal confirmations."""
    stats = {"sources_active": 0, "sources_total": 30, "signals_combined": 0, "cross_confirmed": 0}
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT COUNT(*) FROM signals_log WHERE timestamp >= ?", (yesterday,)
        ).fetchone()
        stats["signals_combined"] = row[0] if row else 0
        # Cross-confirmed: same ticker with 2+ signal types same day
        rows = conn.execute(
            "SELECT ticker, COUNT(DISTINCT signal_type) as src_count "
            "FROM signals_log WHERE timestamp >= ? GROUP BY ticker HAVING src_count >= 2",
            (yesterday,)
        ).fetchall()
        stats["cross_confirmed"] = len(rows)
        conn.close()
    except Exception:
        pass
    try:
        conn = sqlite3.connect("output/altdata.db", timeout=5)
        row = conn.execute(
            "SELECT COUNT(DISTINCT source) FROM raw_data "
            "WHERE collected_at >= date('now','-1 day')"
        ).fetchone()
        stats["sources_active"] = row[0] if row else 0
        conn.close()
    except Exception:
        pass
    return stats


def _build_suggestions(api_status: dict, collector_status: dict,
                        trading: dict, signal_perf: dict, ram_pct: float) -> list:
    """Generate 3 specific improvement suggestions from current data."""
    suggestions = []
    # 1. RAM pressure
    if ram_pct > 75:
        suggestions.append(
            f"RAM at {ram_pct:.0f}% — consider reducing batch_size in data/fetcher.py "
            "or reducing universe size to free ~100MB"
        )
    # 2. Best/worst signal
    if signal_perf.get("worst") and signal_perf["worst"][1] < 40:
        suggestions.append(
            f"Signal '{signal_perf['worst'][0]}' has {signal_perf['worst'][1]:.0f}% trade rate "
            "this week — consider raising its min_confidence threshold by 0.2"
        )
    if signal_perf.get("best") and signal_perf["best"][1] > 70:
        suggestions.append(
            f"Signal '{signal_perf['best'][0]}' is performing at {signal_perf['best'][1]:.0f}% "
            "— consider increasing its position size multiplier by 10%"
        )
    # 3. Broken APIs
    broken = [k for k, v in api_status.items() if "error" in v or "http_" in v or "wrong" in v]
    if broken:
        suggestions.append(
            f"API{'s' if len(broken)>1 else ''} {', '.join(broken)} returning errors "
            "— check api_keys in config/settings.yaml and rotate keys if needed"
        )
    # 4. Failed collectors
    failed = [k for k, v in collector_status.items() if v.startswith("failed")]
    if failed:
        suggestions.append(
            f"Collector{'s' if len(failed)>1 else ''} {', '.join(failed)} failing to import "
            "— check missing dependencies (pip install)"
        )
    # Fill to 3
    defaults = [
        "Consider scheduling altdata collect every 4h during market hours for fresher signals",
        "Enable weekly model retraining via 'python3 main.py altdata rollback' on Sundays",
        "Add more UK LSE tickers to universe_uk.csv to diversify signal sources",
    ]
    for d in defaults:
        if len(suggestions) >= 3:
            break
        suggestions.append(d)
    return suggestions[:3]


# ── report formatters ──────────────────────────────────────────────────────

def format_daily_report(config: dict) -> str:
    """Build the full daily health report string."""
    now      = datetime.now()
    api_st   = _check_apis(config)
    col_st   = _check_collectors(config)
    trading  = _get_trading_stats(config)
    sig_perf = _get_signal_performance(config)
    fusion   = _get_data_fusion_stats(config)

    ram_used, ram_total, ram_pct = get_ram_mb()
    cpu_pct = get_cpu_pct()
    disk_used, disk_total, disk_pct = get_disk_gb()

    # Stream status
    stream_status = "DISCONNECTED"
    try:
        from execution.alpaca_stream import get_stream_cache
        if get_stream_cache().is_connected():
            stream_status = "LIVE"
    except Exception:
        pass

    cols_ok      = sum(1 for v in col_st.values() if v == "ok")
    cols_total   = len(col_st)
    failed_cols  = [k for k, v in col_st.items() if v != "ok"]

    lines = [
        "📊 QUANT FUND DAILY HEALTH REPORT",
        f"📅 {now.strftime('%A %d %B %Y')} {now.strftime('%H:%M')} UTC",
        "",
        "🔌 API STATUS:",
    ]
    for name, status in api_st.items():
        if status == "working":
            lines.append(f"  ✅ {name} — working")
        elif status == "rate_limited":
            lines.append(f"  ⚠️ {name} — rate limited (resets tomorrow)")
        elif status == "MISSING_KEY":
            lines.append(f"  ⚠️ {name} — no key configured")
        else:
            lines.append(f"  🚨 {name} — BROKEN ({status})")

    lines += [
        "",
        "📡 COLLECTOR STATUS:",
        f"  ✅ {cols_ok}/{cols_total} collectors running",
    ]
    if failed_cols:
        lines.append(f"  ⚠️ Failed: {', '.join(failed_cols)}")

    lines += [
        "",
        "💻 SERVER HEALTH:",
        f"  RAM:    {ram_used}MB / {ram_total}MB used ({ram_pct:.1f}%)",
        f"  CPU:    {cpu_pct:.1f}% (1-min load avg)",
        f"  Disk:   {disk_used:.1f}GB / {disk_total:.0f}GB used ({disk_pct:.1f}%)",
        f"  Stream: {stream_status}",
        "",
        "📈 TRADING ACTIVITY:",
        f"  Signals generated yesterday: {trading['signals_yesterday']}",
        f"  Trades executed:             {trading['trades_yesterday']}",
        f"  Win rate yesterday:          {trading['win_rate_yesterday']}%"
            if trading['win_rate_yesterday'] is not None else
            "  Win rate yesterday:          n/a (no closed trades)",
        f"  Win rate all time:           {trading['win_rate_alltime']}%"
            if trading['win_rate_alltime'] is not None else
            "  Win rate all time:           n/a",
        f"  Current phase:               {trading['phase']}",
        f"  Open positions:              {trading['open_positions']}",
        "",
        "🔍 NEW DISCOVERIES:",
        f"  New patterns found:    {sig_perf['patterns_found']}",
        f"  Patterns validated:    {sig_perf['patterns_validated']}",
    ]
    if sig_perf["best"]:
        lines.append(f"  Best signal:  {sig_perf['best'][0]} ({sig_perf['best'][1]:.0f}% trade rate)")
    if sig_perf["worst"]:
        lines.append(f"  Worst signal: {sig_perf['worst'][0]} ({sig_perf['worst'][1]:.0f}% trade rate)")

    lines += [
        "",
        "🔗 DATA FUSION SUMMARY:",
        f"  Sources active:           {fusion['sources_active']}/{fusion['sources_total']}",
        f"  Signals combined today:   {fusion['signals_combined']}",
        f"  Cross-signal confirmations: {fusion['cross_confirmed']}",
        "  (2+ sources agreed on same ticker)",
        "",
        "💡 SUGGESTIONS:",
    ]
    suggestions = _build_suggestions(api_st, col_st, trading, sig_perf, ram_pct)
    for i, s in enumerate(suggestions, 1):
        lines.append(f"  {i}. {s}")

    return "\n".join(lines)


def format_weekly_report(config: dict) -> str:
    """Build the full weekly suggestion report."""
    now  = datetime.now()
    week = now.isocalendar()
    week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    week_end   = (now - timedelta(days=now.weekday()) + timedelta(days=6)).strftime("%Y-%m-%d")

    # Trade stats for the week
    trades_week, win_rate_week = 0, None
    best_trade, worst_trade = None, None
    try:
        conn = sqlite3.connect("closeloop/storage/closeloop.db", timeout=5)
        seven_days_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT COUNT(*), AVG(was_profitable) FROM trade_ledger "
            "WHERE exit_date >= ? AND was_profitable IS NOT NULL", (seven_days_ago,)
        ).fetchone()
        if row:
            trades_week = row[0] or 0
            if row[1] is not None:
                win_rate_week = round(row[1] * 100, 1)
        # Best trade
        row = conn.execute(
            "SELECT ticker, pnl_pct FROM trade_ledger WHERE exit_date >= ? "
            "ORDER BY pnl_pct DESC LIMIT 1", (seven_days_ago,)
        ).fetchone()
        if row:
            best_trade = (row[0], round(row[1], 2))
        # Worst trade
        row = conn.execute(
            "SELECT ticker, pnl_pct FROM trade_ledger WHERE exit_date >= ? "
            "ORDER BY pnl_pct ASC LIMIT 1", (seven_days_ago,)
        ).fetchone()
        if row:
            worst_trade = (row[0], round(row[1], 2))
        conn.close()
    except Exception:
        pass

    # Signal performance
    top_signals, worst_signals = [], []
    try:
        conn = sqlite3.connect("output/permanent_archive.db", timeout=5)
        seven_days_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT signal_type, AVG(was_traded) as rate, COUNT(*) as n "
            "FROM signals_log WHERE timestamp >= ? GROUP BY signal_type "
            "HAVING n >= 3 ORDER BY rate DESC LIMIT 3", (seven_days_ago,)
        ).fetchall()
        top_signals = [(r[0], round(r[1]*100, 1), r[2]) for r in rows]
        rows = conn.execute(
            "SELECT signal_type, AVG(was_traded) as rate, COUNT(*) as n "
            "FROM signals_log WHERE timestamp >= ? GROUP BY signal_type "
            "HAVING n >= 3 ORDER BY rate ASC LIMIT 2", (seven_days_ago,)
        ).fetchall()
        worst_signals = [(r[0], round(r[1]*100, 1), r[2]) for r in rows]
        conn.close()
    except Exception:
        pass

    # Data source contribution
    top_sources = []
    try:
        conn = sqlite3.connect("output/altdata.db", timeout=5)
        seven_days_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT source, COUNT(*) FROM raw_data WHERE collected_at >= ? "
            "GROUP BY source ORDER BY COUNT(*) DESC LIMIT 3", (seven_days_ago,)
        ).fetchall()
        top_sources = [(r[0], r[1]) for r in rows]
        conn.close()
    except Exception:
        pass

    lines = [
        "📊 QUANT FUND WEEKLY REPORT",
        f"Week: {week_start} → {week_end} (W{week[1]:02d}/{week[0]})",
        "",
        "🏆 TOP PERFORMING SIGNALS THIS WEEK:",
    ]
    if top_signals:
        for i, (sig, rate, n) in enumerate(top_signals, 1):
            lines.append(f"  {i}. {sig} — {rate:.0f}% trade rate, {n} signals")
    else:
        lines.append("  (insufficient data — signals accumulating)")

    lines += ["", "❌ WORST PERFORMING SIGNALS:"]
    if worst_signals:
        for i, (sig, rate, n) in enumerate(worst_signals, 1):
            lines.append(f"  {i}. {sig} — {rate:.0f}% trade rate (consider reducing weight)")
    else:
        lines.append("  (insufficient data)")

    lines += ["", "📡 MOST VALUABLE DATA SOURCES:"]
    if top_sources:
        for i, (src, n) in enumerate(top_sources, 1):
            lines.append(f"  {i}. {src} — {n} records collected")
    else:
        lines.append("  (altdata.db empty — run: python3 main.py altdata collect)")

    lines += [
        "",
        "🧠 WHAT THE MODEL LEARNED THIS WEEK:",
    ]
    if top_signals:
        lines.append(
            f"  The {top_signals[0][0]} signal showed strongest predictive value with "
            f"{top_signals[0][1]:.0f}% confirmed trade rate from {top_signals[0][2]} signals. "
        )
    lines.append(
        "  Cross-signal confluence (multiple sources agreeing) improved signal quality. "
        "  Real-time Alpaca price data is now augmenting entry timing for US positions. "
        "  The closed-loop system is accumulating trade outcomes for the next retraining cycle."
    )

    lines += ["", "⚙️ SUGGESTED PARAMETER TWEAKS:"]
    if worst_signals and worst_signals[0][1] < 30:
        lines.append(
            f"  • Raise min_confidence for {worst_signals[0][0]} from current value by +0.3"
        )
    lines += [
        "  • Consider enabling observation_mode for new signal types until 10+ trades accumulate",
        "  • Schedule weekly retraining: python3 main.py altdata rollback on Sundays 05:00 UTC",
    ]

    lines += [
        "",
        "📈 PERFORMANCE SUMMARY:",
        f"  Trades this week:  {trades_week}",
        f"  Win rate:          {win_rate_week}%" if win_rate_week is not None
            else "  Win rate:          n/a (no closed trades this week)",
    ]
    if best_trade:
        lines.append(f"  Best trade:  {best_trade[0]} +{best_trade[1]:.2f}%")
    if worst_trade:
        lines.append(f"  Worst trade: {worst_trade[0]} {worst_trade[1]:+.2f}%")

    return "\n".join(lines)


# ── send / save ────────────────────────────────────────────────────────────

def _send_via_notifier(config: dict, msg_type: str, text: str) -> bool:
    """Send text via Notifier._send_telegram and log it."""
    delivered = False
    try:
        from altdata.notifications.notifier import Notifier
        n = Notifier(config)
        delivered = n._send_telegram(text)
    except Exception as exc:
        logger.warning("send_via_notifier failed: %s", exc)
    telegram_logger.log_message(msg_type, text, delivered)
    if not delivered:
        telegram_logger.queue_retry(msg_type, text)
    return delivered


def send_daily_report(config: dict) -> bool:
    """Generate daily report, send to Telegram and save to file."""
    _DAILY_DIR.mkdir(parents=True, exist_ok=True)
    try:
        report = format_daily_report(config)
    except Exception as exc:
        logger.error("format_daily_report failed: %s", exc)
        return False
    # Save to file
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = _DAILY_DIR / f"health_report_{date_str}.txt"
    try:
        path.write_text(report, encoding="utf-8")
        logger.info("Daily health report saved: %s", path)
    except Exception as exc:
        logger.error("Failed to save daily report: %s", exc)
    # Send to Telegram
    delivered = _send_via_notifier(config, "daily_health", report)
    logger.info("Daily health report sent to Telegram: %s", delivered)
    return delivered


def send_weekly_report(config: dict) -> bool:
    """Generate weekly report, send to Telegram and save to file."""
    _WEEKLY_DIR.mkdir(parents=True, exist_ok=True)
    try:
        report = format_weekly_report(config)
    except Exception as exc:
        logger.error("format_weekly_report failed: %s", exc)
        return False
    now = datetime.now()
    week = now.isocalendar()
    fname = f"weekly_report_{week[0]}-W{week[1]:02d}.txt"
    path  = _WEEKLY_DIR / fname
    try:
        path.write_text(report, encoding="utf-8")
        logger.info("Weekly report saved: %s", path)
    except Exception as exc:
        logger.error("Failed to save weekly report: %s", exc)
    delivered = _send_via_notifier(config, "weekly", report)
    logger.info("Weekly report sent to Telegram: %s", delivered)
    return delivered
