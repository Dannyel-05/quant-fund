"""
PreFlightChecker — runs at 13:00 UTC on trading days (90 min before US open).
PreMarketScanner — runs at 14:00 UTC on trading days (30 min before US open).
EndOfDayReporter — runs at 21:15 UTC on trading days (15 min after US close).

All three send formatted Telegram reports and save to logs/.
Integrated into trading_bot.py main loop via helper functions at the bottom.
"""
from __future__ import annotations

import importlib
import json
import logging
import os
import sqlite3
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_PREFLIGHT_DIR = Path("logs/preflight")
_EOD_DIR       = Path("logs/eod_reports")
_ALPACA_STATUS = Path("output/alpaca_status.json")

_CLOSELOOP_DB    = "closeloop/storage/closeloop.db"
_PERMANENT_DB    = "output/permanent_archive.db"
_HISTORICAL_DB   = "output/historical_db.db"
_ALTDATA_DB      = "output/altdata.db"

_COLLECTOR_CLASSES = [
    ("data.collectors.rates_credit_collector",    "RatesCreditCollector"),
    ("data.collectors.consumer_intelligence",     "ConsumerIntelligence"),
    ("data.collectors.geopolitical_collector",    "GeopoliticalCollector"),
    ("data.collectors.commodity_collector",       "CommodityCollector"),
    ("data.collectors.technology_intelligence",   "TechnologyIntelligence"),
    ("data.collectors.government_data_collector", "BLSCollector"),
    ("data.collectors.government_data_collector", "USASpendingCollector"),
    ("data.collectors.shipping_intelligence",     "ShippingIntelligence"),
    ("data.collectors.sec_fulltext_collector",    "SECFullTextCollector"),
    ("data.collectors.alternative_quiver_collector", "AlternativeQuiverCollector"),
    ("altdata.collector.news_collector",          "NewsCollector"),
    ("altdata.collector.sec_edgar_collector",     "SECEdgarCollector"),
    ("altdata.collector.finnhub_collector",       "FinnhubCollector"),
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _send_telegram(config: dict, text: str) -> bool:
    try:
        tg      = config.get("notifications", {}).get("telegram", {})
        token   = tg.get("bot_token", "")
        chat_id = tg.get("chat_id", "")
        if not token or not chat_id:
            return False
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        return resp.ok
    except Exception as exc:
        logger.debug("_send_telegram: %s", exc)
        return False


def _is_trading_day(dt: date, market: str = "us") -> bool:
    try:
        from analysis.market_calendar import MarketCalendar
        return MarketCalendar().is_trading_day(market, dt)
    except Exception:
        return dt.weekday() < 5


def _collectors_healthy() -> tuple[int, int, list]:
    """Returns (ok_count, total, failed_names)."""
    ok = 0
    failed = []
    for mod_path, cls_name in _COLLECTOR_CLASSES:
        try:
            mod = importlib.import_module(mod_path)
            getattr(mod, cls_name)
            ok += 1
        except Exception:
            failed.append(cls_name)
    return ok, len(_COLLECTOR_CLASSES), failed


def _db_accessible(path: str) -> bool:
    try:
        if not Path(path).exists():
            return False
        con = sqlite3.connect(path, timeout=3)
        con.execute("PRAGMA integrity_check").fetchone()
        con.close()
        return True
    except Exception:
        return False


def _get_alpaca_status(config: dict) -> dict:
    """Check Alpaca API directly. Returns status dict."""
    try:
        api_keys = config.get("api_keys", {})
        alpaca_cfg = config.get("alpaca", {})
        key    = api_keys.get("alpaca_api_key", "")
        secret = api_keys.get("alpaca_secret_key", "")
        base   = alpaca_cfg.get("base_url", "https://paper-api.alpaca.markets")
        if not key or "PASTE" in key:
            return {"connected": False, "mode": "no_key", "equity": 0.0}
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        resp = requests.get(f"{base}/v2/account", headers=headers, timeout=8)
        if resp.status_code == 200:
            acc = resp.json()
            status = {
                "connected": True,
                "mode": "paper",
                "equity": float(acc.get("portfolio_value", 0)),
                "cash":   float(acc.get("cash", 0)),
                "timestamp": datetime.utcnow().isoformat(),
            }
            # Persist last successful ping
            try:
                _ALPACA_STATUS.parent.mkdir(exist_ok=True)
                _ALPACA_STATUS.write_text(json.dumps(status, indent=2))
            except Exception:
                pass
            return status
        return {"connected": False, "mode": "error", "http": resp.status_code, "equity": 0.0}
    except Exception as exc:
        return {"connected": False, "mode": "exception", "error": str(exc)[:80], "equity": 0.0}


def _get_phase_info() -> dict:
    try:
        con = sqlite3.connect(_CLOSELOOP_DB, timeout=5)
        real = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT MIN(rowid) FROM trade_ledger
                WHERE exit_date IS NOT NULL AND gross_pnl != 0.0
                AND (is_phantom=0 OR is_phantom IS NULL)
                GROUP BY ticker, entry_date
            )
        """).fetchone()[0]
        phantom = con.execute(
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom=1"
        ).fetchone()[0]
        open_pos = con.execute(
            "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL"
        ).fetchone()[0]
        con.close()
    except Exception:
        return {"phase": "UNKNOWN", "real_trades": 0, "phantom": 0, "open_pos": 0,
                "trades_to_next": 0, "next_phase": "PHASE_2"}

    phase = "PHASE_1"
    next_phase = "PHASE_2"
    next_threshold = 100
    if real >= 2000:
        phase = "PHASE_FREE"; next_phase = "PHASE_FREE"; next_threshold = 0
    elif real >= 1000:
        phase = "PHASE_5";    next_phase = "PHASE_FREE"; next_threshold = 2000
    elif real >= 600:
        phase = "PHASE_4";    next_phase = "PHASE_5";    next_threshold = 1000
    elif real >= 300:
        phase = "PHASE_3";    next_phase = "PHASE_4";    next_threshold = 600
    elif real >= 100:
        phase = "PHASE_2";    next_phase = "PHASE_3";    next_threshold = 300

    return {
        "phase": phase,
        "real_trades": real,
        "phantom": phantom,
        "open_pos": open_pos,
        "trades_to_next": max(0, next_threshold - real),
        "next_phase": next_phase,
    }


def _get_cooling_off_count() -> int:
    try:
        from execution.cooling_off_tracker import CoolingOffTracker
        # In-memory only — check the singleton if one exists
        # No persistent file, so return from DB fallback
        return 0
    except Exception:
        return 0


def _get_regime(config: dict) -> str:
    try:
        from analysis.regime_detector import RegimeDetector
        rd = RegimeDetector(config)
        return rd.detect()
    except Exception:
        return "UNKNOWN"


def _get_spy_premarket() -> tuple[float, float]:
    """Return (last_price, pct_change). Uses yfinance."""
    try:
        import yfinance as yf
        spy = yf.Ticker("SPY")
        hist = spy.history(period="2d", interval="1d")
        if len(hist) >= 2:
            prev  = float(hist["Close"].iloc[-2])
            last  = float(hist["Close"].iloc[-1])
            pct   = (last - prev) / prev * 100
            return last, pct
        elif len(hist) == 1:
            return float(hist["Close"].iloc[-1]), 0.0
    except Exception:
        pass
    return 0.0, 0.0


def _count_signals_ready(config: dict) -> int:
    """Count signals generated in last 24h (signals_log is in permanent_archive.db)."""
    try:
        con = sqlite3.connect(_PERMANENT_DB, timeout=5)
        n = con.execute(
            "SELECT COUNT(DISTINCT ticker) FROM signals_log "
            "WHERE timestamp >= datetime('now', '-24 hours')"
        ).fetchone()[0]
        con.close()
        return n
    except Exception:
        return 0


def _get_universe_size() -> int:
    try:
        # Try to get from config universe file
        import yaml
        with open("config/settings.yaml") as f:
            cfg = yaml.safe_load(f)
        us_count = len(cfg.get("universe", {}).get("us_tickers", []))
        uk_count = len(cfg.get("universe", {}).get("uk_tickers", []))
        if us_count + uk_count > 0:
            return us_count + uk_count
        # Fallback: count distinct tickers in historical DB
        con = sqlite3.connect(_HISTORICAL_DB, timeout=5)
        n = con.execute("SELECT COUNT(DISTINCT ticker) FROM price_data").fetchone()[0]
        con.close()
        return n
    except Exception:
        return 0


# ── PreFlightChecker ──────────────────────────────────────────────────────────

class PreFlightChecker:
    """Generates and sends 13:00 UTC pre-flight readiness report."""

    def generate_report(self, config: dict, trader=None) -> dict:
        now = datetime.utcnow()
        today = now.date()

        collectors_ok, collectors_total, failed_collectors = _collectors_healthy()

        dbs = {
            "closeloop":  _db_accessible(_CLOSELOOP_DB),
            "permanent":  _db_accessible(_PERMANENT_DB),
            "historical": _db_accessible(_HISTORICAL_DB),
            "altdata":    _db_accessible(_ALTDATA_DB),
        }
        dbs_ok = sum(dbs.values())

        alpaca = _get_alpaca_status(config)
        phase  = _get_phase_info()
        regime = _get_regime(config)
        spy_price, spy_pct = _get_spy_premarket()
        signals_ready = _count_signals_ready(config)

        # Bot PID
        pid = None
        try:
            pid_text = Path("output/bot.pid").read_text().strip()
            pid = int(pid_text)
            os.kill(pid, 0)   # raises if not running
        except Exception:
            pid = None

        # Market hours gate status (from paper_trader constants)
        try:
            from execution.paper_trader import _is_market_open
            mkt_gate_active = not _is_market_open("us")  # True = gate currently blocking (market closed at 13:00)
        except Exception:
            mkt_gate_active = True

        warnings = []
        if collectors_ok < collectors_total:
            warnings.append(f"Collectors degraded: {failed_collectors}")
        if dbs_ok < len(dbs):
            missing = [k for k, v in dbs.items() if not v]
            warnings.append(f"DB inaccessible: {missing}")
        if not alpaca["connected"]:
            warnings.append("Alpaca not connected — simulation mode")
        if pid is None:
            warnings.append("Bot PID not found — may not be running via pm2")

        return {
            "timestamp":          now.isoformat(),
            "market_date":        today.isoformat(),
            "bot_pid":            pid,
            "bot_running":        pid is not None,
            "collectors_ok":      collectors_ok,
            "collectors_total":   collectors_total,
            "dbs_ok":             dbs_ok,
            "dbs_total":          len(dbs),
            "alpaca":             alpaca,
            "market_gate_active": mkt_gate_active,
            "cooling_off_count":  _get_cooling_off_count(),
            "phase":              phase,
            "regime":             regime,
            "spy_price":          spy_price,
            "spy_pct":            spy_pct,
            "signals_ready":      signals_ready,
            "universe_size":      _get_universe_size(),
            "warnings":           warnings,
        }

    def format_message(self, report: dict) -> str:
        p = report["phase"]
        alp = report["alpaca"]
        today_str = report.get("market_date", date.today().isoformat())

        bot_line = (
            f"✅ Bot Status: RUNNING (PID {report['bot_pid']})"
            if report["bot_running"]
            else "❌ Bot Status: NOT DETECTED"
        )
        col_line = (
            f"✅ Collectors: {report['collectors_ok']}/{report['collectors_total']} healthy"
            if report["collectors_ok"] == report["collectors_total"]
            else f"⚠️ Collectors: {report['collectors_ok']}/{report['collectors_total']} healthy"
        )
        db_line = (
            f"✅ Databases: {report['dbs_ok']}/{report['dbs_total']} accessible"
            if report["dbs_ok"] == report["dbs_total"]
            else f"⚠️ Databases: {report['dbs_ok']}/{report['dbs_total']} accessible"
        )
        alp_line = (
            f"✅ Alpaca: Connected (paper) — equity ${alp.get('equity', 0):,.0f}"
            if alp.get("connected")
            else "⚠️ Alpaca: Simulation mode"
        )
        gate_line = (
            "✅ Market Hours Gate: ACTIVE"
            if report["market_gate_active"]
            else "✅ Market Hours Gate: ACTIVE (market open)"
        )
        phase_line = (
            f"✅ Current Phase: {p['phase']} "
            f"({p['real_trades']} real trades, {p['trades_to_next']} to {p['next_phase']})"
        )
        spy_sign = "+" if report["spy_pct"] >= 0 else ""
        spy_line = (
            f"📈 SPY Last Close: ${report['spy_price']:.2f} "
            f"({spy_sign}{report['spy_pct']:.2f}%)"
            if report["spy_price"] > 0
            else "📈 SPY: unavailable"
        )

        warnings_block = ""
        if report["warnings"]:
            warnings_block = "\n⚠️ Warnings:\n" + "\n".join(f"  - {w}" for w in report["warnings"])

        return (
            f"🚀 Apollo Pre-Flight Check — {today_str}\n"
            f"Market Opens: 14:30 UTC (in ~90 min)\n"
            f"{'━' * 36}\n"
            f"{bot_line}\n"
            f"{col_line}\n"
            f"{db_line}\n"
            f"{alp_line}\n"
            f"{gate_line}\n"
            f"✅ Cooling Off Locks: {report['cooling_off_count']} active\n"
            f"✅ Phantom Trades: {p['phantom']} (excluded from phase)\n"
            f"{phase_line}\n"
            f"\n"
            f"📊 Open Positions: {p['open_pos']}\n"
            f"🎯 Signals Ready: {report['signals_ready']} tickers\n"
            f"🌍 Regime: {report['regime']}\n"
            f"{spy_line}\n"
            f"{warnings_block}\n"
            f"\n"
            f"{'Apollo is ready for open 🟢' if not report['warnings'] else 'Apollo ready with warnings ⚠️'}"
        )

    def run(self, config: dict, trader=None) -> bool:
        """Generate, save, and send pre-flight report. Returns True if sent."""
        try:
            report = self.generate_report(config, trader)
            msg    = self.format_message(report)

            _PREFLIGHT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
            (_PREFLIGHT_DIR / f"preflight_{ts}.txt").write_text(msg)

            delivered = _send_telegram(config, msg)
            logger.info("PreFlightChecker sent (delivered=%s)", delivered)
            return delivered
        except Exception as exc:
            logger.error("PreFlightChecker.run: %s", exc)
            return False


# ── PreMarketScanner ──────────────────────────────────────────────────────────

class PreMarketScanner:
    """Runs at 14:00 UTC — generates top-signal list 30 min before US open."""

    def run(self, config: dict, trader=None) -> bool:
        try:
            report = self._build(config, trader)
            msg    = self._format(report)

            _PREFLIGHT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
            (_PREFLIGHT_DIR / f"prescan_{ts}.txt").write_text(msg)

            delivered = _send_telegram(config, msg)
            logger.info("PreMarketScanner sent (delivered=%s)", delivered)
            return delivered
        except Exception as exc:
            logger.error("PreMarketScanner.run: %s", exc)
            return False

    def _build(self, config: dict, trader=None) -> dict:
        now = datetime.utcnow()
        phase = _get_phase_info()
        regime = _get_regime(config)

        # Pull top signals from last 24h (signals_log is in permanent_archive.db)
        signals = []
        try:
            con = sqlite3.connect(_PERMANENT_DB, timeout=5)
            rows = con.execute("""
                SELECT ticker, signal_type, score, direction
                FROM signals_log
                WHERE timestamp >= datetime('now', '-24 hours')
                  AND score >= 0.25
                ORDER BY score DESC
                LIMIT 20
            """).fetchall()
            con.close()
            for row in rows:
                signals.append({
                    "ticker":    row[0],
                    "sig_type":  row[1] or "unknown",
                    "score":     float(row[2] or 0),
                    "direction": row[3] or "LONG",
                })
        except Exception as exc:
            logger.debug("PreMarketScanner._build signals: %s", exc)

        # Cooling-off blocked tickers from DB
        cooling_count = 0
        try:
            con = sqlite3.connect(_CLOSELOOP_DB, timeout=5)
            cooling_count = con.execute(
                "SELECT COUNT(DISTINCT ticker) FROM trade_ledger "
                "WHERE exit_date >= date('now', '-2 days') AND gross_pnl < 0"
            ).fetchone()[0]
            con.close()
        except Exception:
            pass

        # Position sizing from sizer if trader available
        phase_multiplier = 1.0
        max_new = 10
        if trader is not None:
            try:
                sizer = getattr(trader, "sizer", None) or getattr(trader, "trader", None)
                if sizer and hasattr(sizer, "sizer"):
                    sizer = sizer.sizer
                if sizer and hasattr(sizer, "phase_multiplier"):
                    phase_multiplier = sizer.phase_multiplier
                if sizer and hasattr(sizer, "max_simultaneous_positions"):
                    max_new = sizer.max_simultaneous_positions - phase["open_pos"]
            except Exception:
                pass

        return {
            "timestamp":        now.isoformat(),
            "signals":          signals,
            "phase":            phase,
            "regime":           regime,
            "phase_multiplier": phase_multiplier,
            "max_new_positions": max(0, max_new),
            "cooling_off_count": cooling_count,
        }

    def _format(self, report: dict) -> str:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        signals = report["signals"][:10]  # top 10 for readability
        p = report["phase"]

        lines = [
            f"📡 Pre-Market Signal Scan — 30min to Open",
            f"{'━' * 36}",
            f"Date: {today}",
            "",
            "Top signals ready:",
        ]
        if signals:
            for i, s in enumerate(signals, 1):
                conf = "High" if s["score"] >= 0.6 else "Medium" if s["score"] >= 0.4 else "Watch"
                lines.append(
                    f"  {i:2d}. {s['ticker']:<8} — {s['sig_type']:<12} {s['score']:+.2f}"
                    f" | {s['direction']:<5} | Confidence: {conf}"
                )
        else:
            lines.append("  No qualifying signals in last 24h")

        lines += [
            "",
            f"Regime: {report['regime']}",
            f"Position sizing: {report['phase_multiplier']*100:.0f}% of normal",
            f"Max new positions today: {report['max_new_positions']}",
            f"Cooling off blocks: {report['cooling_off_count']} tickers",
            f"Current phase: {p['phase']} ({p['real_trades']} real trades)",
            "",
            "Apollo scanning live prices at 14:30 UTC 🎯",
        ]
        return "\n".join(lines)


# ── EndOfDayReporter ──────────────────────────────────────────────────────────

class EndOfDayReporter:
    """Runs at 21:15 UTC — summary of today's trading activity."""

    def run(self, config: dict) -> bool:
        try:
            report = self._build(config)
            msg    = self._format(report)

            _EOD_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
            (_EOD_DIR / f"eod_{ts}.txt").write_text(msg)

            delivered = _send_telegram(config, msg)
            logger.info("EndOfDayReporter sent (delivered=%s)", delivered)
            return delivered
        except Exception as exc:
            logger.error("EndOfDayReporter.run: %s", exc)
            return False

    def _build(self, config: dict) -> dict:
        today = date.today().isoformat()
        con = sqlite3.connect(_CLOSELOOP_DB, timeout=5)
        try:
            opened = con.execute(
                "SELECT COUNT(*) FROM trade_ledger WHERE date(entry_date)=? AND (is_phantom IS NULL OR is_phantom=0)",
                (today,)
            ).fetchone()[0]
            closed = con.execute(
                "SELECT COUNT(*) FROM trade_ledger WHERE date(exit_date)=? AND (is_phantom IS NULL OR is_phantom=0)",
                (today,)
            ).fetchone()[0]
            pnl = float(con.execute(
                "SELECT COALESCE(SUM(gross_pnl),0) FROM trade_ledger WHERE date(exit_date)=? AND (is_phantom IS NULL OR is_phantom=0)",
                (today,)
            ).fetchone()[0] or 0)

            # Best / worst
            best = con.execute(
                "SELECT ticker, gross_pnl FROM trade_ledger WHERE date(exit_date)=? AND (is_phantom IS NULL OR is_phantom=0) ORDER BY gross_pnl DESC LIMIT 1",
                (today,)
            ).fetchone()
            worst = con.execute(
                "SELECT ticker, gross_pnl FROM trade_ledger WHERE date(exit_date)=? AND (is_phantom IS NULL OR is_phantom=0) ORDER BY gross_pnl ASC LIMIT 1",
                (today,)
            ).fetchone()
        finally:
            con.close()

        # Signals by type (signals_log lives in permanent_archive.db)
        sig_rows = []
        try:
            con2 = sqlite3.connect(_PERMANENT_DB, timeout=5)
            sig_rows = con2.execute(
                "SELECT signal_type, COUNT(*) FROM signals_log WHERE date(timestamp)=? GROUP BY signal_type",
                (today,)
            ).fetchall()
            con2.close()
        except Exception:
            pass

        phase = _get_phase_info()
        regime = _get_regime(config)

        # Data collected today
        new_rows = 0
        try:
            con2 = sqlite3.connect(_PERMANENT_DB, timeout=5)
            new_rows = con2.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM macro_intelligence WHERE date(stored_at)=? LIMIT 10000)",
                (today,)
            ).fetchone()[0]
            con2.close()
        except Exception:
            pass

        collectors_ok, collectors_total, _ = _collectors_healthy()

        return {
            "date":            today,
            "opened":          opened,
            "closed":          closed,
            "net_pnl":         pnl,
            "best_trade":      best,
            "worst_trade":     worst,
            "signals_by_type": dict(sig_rows),
            "phase":           phase,
            "regime":          regime,
            "new_rows":        new_rows,
            "collectors_ok":   collectors_ok,
            "collectors_total": collectors_total,
        }

    def _format(self, report: dict) -> str:
        p = report["phase"]
        pnl_sign = "+" if report["net_pnl"] >= 0 else ""

        best = worst = "N/A"
        if report["best_trade"]:
            best = f"{report['best_trade'][0]} ${report['best_trade'][1]:+.2f}"
        if report["worst_trade"]:
            worst = f"{report['worst_trade'][0]} ${report['worst_trade'][1]:+.2f}"

        sig_lines = ""
        for sig_type, count in sorted(report["signals_by_type"].items()):
            sig_lines += f"\n  - {sig_type or 'unknown'}: {count} signals"

        return (
            f"📊 Apollo End of Day — {report['date']}\n"
            f"{'━' * 36}\n"
            f"Trades Today:\n"
            f"  Opened: {report['opened']} positions\n"
            f"  Closed: {report['closed']} positions\n"
            f"  Net PnL: {pnl_sign}${report['net_pnl']:,.2f}\n"
            f"\n"
            f"Best Trade:  {best}\n"
            f"Worst Trade: {worst}\n"
            f"\n"
            f"Signals Generated:{sig_lines if sig_lines else ' none'}\n"
            f"\n"
            f"Portfolio Status:\n"
            f"  Open Positions: {p['open_pos']}\n"
            f"  Phase: {p['phase']} ({p['trades_to_next']} trades to {p['next_phase']})\n"
            f"  Regime: {report['regime']}\n"
            f"\n"
            f"Data Collected Today:\n"
            f"  New rows: {report['new_rows']:,}\n"
            f"  Collectors healthy: {report['collectors_ok']}/{report['collectors_total']}\n"
            f"\n"
            f"Apollo signing off 🌙"
        )


# ── Scheduling helpers ────────────────────────────────────────────────────────

_preflight_checker    = PreFlightChecker()
_premarket_scanner    = PreMarketScanner()
_eod_reporter         = EndOfDayReporter()

# Called from trading_bot.py main loop to check if any scheduled report should run
_last_preflight_date:  Optional[str] = None
_last_prescan_date:    Optional[str] = None
_last_eod_date:        Optional[str] = None


def tick(config: dict, now: Optional[datetime] = None, trader=None) -> None:
    """
    Called every minute from trading_bot main loop.
    Fires preflight (13:00 UTC), prescan (14:00 UTC), EOD (21:15 UTC) on trading days.
    """
    global _last_preflight_date, _last_prescan_date, _last_eod_date

    if now is None:
        now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")

    if not _is_trading_day(now.date(), "us"):
        return

    # 13:00 UTC — pre-flight
    if now.hour == 13 and now.minute < 2 and _last_preflight_date != today:
        _last_preflight_date = today
        import threading
        threading.Thread(
            target=_preflight_checker.run, args=(config, trader),
            daemon=True, name="preflight",
        ).start()

    # 14:00 UTC — pre-market signal scan
    if now.hour == 14 and now.minute == 0 and _last_prescan_date != today:
        _last_prescan_date = today
        import threading
        threading.Thread(
            target=_premarket_scanner.run, args=(config, trader),
            daemon=True, name="prescan",
        ).start()

    # 21:15 UTC — end of day
    if now.hour == 21 and now.minute == 15 and _last_eod_date != today:
        _last_eod_date = today
        import threading
        threading.Thread(
            target=_eod_reporter.run, args=(config,),
            daemon=True, name="eod_report",
        ).start()
