"""
Apollo Private Bot — Telegram inline menu, NLP routing, and trade explanation engine.
Runs as a daemon thread using long-polling (getUpdates).
All data access is strictly read-only from SQLite databases.
"""
import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]

DB_CLOSELOOP = str(_ROOT / "closeloop" / "storage" / "closeloop.db")
DB_CLOSELOOP_DATA = str(_ROOT / "closeloop_data.db")
DB_HISTORICAL = str(_ROOT / "output" / "historical_db.db")
DB_FRONTIER = str(_ROOT / "frontier" / "storage" / "frontier.db")
DB_DEEPDATA = str(_ROOT / "deepdata" / "storage" / "deepdata.db")
DB_INTELLIGENCE = str(_ROOT / "output" / "intelligence_db.db")

START_DATE = "2026-04-03"
PRIVATE_CHAT_ID = "8508697534"


def _load_config() -> dict:
    try:
        return yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())
    except Exception:
        return {}


def _db_path() -> str:
    try:
        if Path(DB_CLOSELOOP).stat().st_size > 100:
            return DB_CLOSELOOP
    except Exception:
        pass
    return DB_CLOSELOOP_DATA


def _safe_query(db_path: str, sql: str, params=()) -> list:
    try:
        conn = sqlite3.connect(db_path, timeout=8)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"private_bot: DB query failed: {e}")
        return []


def _safe_scalar(db_path: str, sql: str, params=(), default=None):
    rows = _safe_query(db_path, sql, params)
    if rows:
        return list(rows[0].values())[0]
    return default


# ── Telegram API helpers ─────────────────────────────────────────────────────

def _tg_request(token: str, method: str, data: dict, timeout: int = 15) -> Optional[dict]:
    """Make a Telegram API call and return the JSON response."""
    try:
        url = f"https://api.telegram.org/bot{token}/{method}"
        payload = json.dumps(data).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.debug(f"Telegram API error ({method}): {e}")
        return None


def _send_message(token: str, chat_id: str, text: str, reply_markup: dict = None,
                   parse_mode: str = "Markdown") -> Optional[dict]:
    """Send a text message, truncating if over 4096 chars."""
    if len(text) > 4096:
        text = text[:4092] + "\n..."
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return _tg_request(token, "sendMessage", data)


def _answer_callback(token: str, callback_query_id: str, text: str = "") -> None:
    _tg_request(token, "answerCallbackQuery",
                {"callback_query_id": callback_query_id, "text": text})


def _send_photo(token: str, chat_id: str, photo_path: str, caption: str = "") -> bool:
    """Send a photo via multipart upload."""
    try:
        boundary = b"----TgBoundary"
        with open(photo_path, "rb") as f:
            photo_bytes = f.read()
        filename = Path(photo_path).name
        body = b""
        body += b"--" + boundary + b"\r\n"
        body += b'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
        body += chat_id.encode() + b"\r\n"
        if caption:
            body += b"--" + boundary + b"\r\n"
            body += b'Content-Disposition: form-data; name="caption"\r\n\r\n'
            body += caption[:1024].encode() + b"\r\n"
        body += b"--" + boundary + b"\r\n"
        body += f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'.encode()
        body += b"Content-Type: image/png\r\n\r\n"
        body += photo_bytes + b"\r\n"
        body += b"--" + boundary + b"--\r\n"
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary.decode()}"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error(f"private_bot: photo send failed: {e}")
        return False


# ── Inline menu builder ──────────────────────────────────────────────────────

def _build_menu_keyboard() -> dict:
    """Build the 9-row, 2-column inline keyboard."""
    rows = [
        [("📊 Positions", "btn_positions"), ("💹 Performance", "btn_performance")],
        [("🔧 System Health", "btn_health"), ("📡 Collectors", "btn_collectors")],
        [("🧠 Intelligence", "btn_intelligence"), ("⚠️ Errors", "btn_errors")],
        [("🌍 Regime", "btn_regime"), ("📈 Weekly Report", "btn_weekly_report")],
        [("🔬 Pairs Trading", "btn_pairs"), ("🧮 Factor Model", "btn_factors")],
        [("📊 Options Flow", "btn_options"), ("👤 Insider Data", "btn_insider")],
        [("〰️ Kalman", "btn_kalman"), ("🌊 Wavelet", "btn_wavelet")],
        [("🚢 Shipping", "btn_shipping"), ("📦 Commodities", "btn_commodities")],
        [("🗄️ DB Stats", "btn_db_stats"), ("🔄 Force Report", "btn_force_report")],
    ]
    keyboard = []
    for row in rows:
        keyboard.append([{"text": label, "callback_data": cb} for label, cb in row])
    return {"inline_keyboard": keyboard}


# ── Menu header ──────────────────────────────────────────────────────────────

def _build_menu_header() -> str:
    """Build the status header for the main menu."""
    try:
        import subprocess
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, timeout=5
        )
        pid = "?"
        for line in result.stdout.splitlines():
            if "python3 main.py" in line and "grep" not in line:
                parts = line.split()
                if len(parts) > 1:
                    pid = parts[1]
                    break
    except Exception:
        pid = "?"

    regime = "NEUTRAL"
    confidence = 0.0
    try:
        row = _safe_query(DB_FRONTIER,
            "SELECT level, umci FROM umci_history ORDER BY recorded_at DESC LIMIT 1")
        if row:
            regime = row[0].get("level", "NEUTRAL")
            confidence = row[0].get("umci", 0.0) or 0.0
    except Exception:
        pass

    phase = "1"
    real_trades = 0
    trades_to_next = "?"
    try:
        status_file = _ROOT / "output" / "bot_status.json"
        if status_file.exists():
            data = json.loads(status_file.read_text())
            raw_phase = str(data.get("phase", "1"))
            # Extract just the number (could be "1", "PHASE_1", or full sizer summary)
            import re as _re
            m = _re.search(r"(\d)", raw_phase)
            phase = m.group(1) if m else "1"
    except Exception:
        pass

    try:
        real_trades = _safe_scalar(_db_path(),
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom=0 AND ABS(net_pnl)>0.01",
            default=0)
        phase_thresholds = {"1": 10, "2": 25, "3": 50, "4": 100}
        try:
            pn = int(phase)
            next_t = phase_thresholds.get(str(pn + 1), 100)
            trades_to_next = max(0, next_t - (real_trades or 0))
        except Exception:
            trades_to_next = "?"
    except Exception:
        pass

    open_pos = _safe_scalar(_db_path(),
        "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL AND is_phantom=0",
        default=0)

    last_cycle = "?"
    try:
        status_file = _ROOT / "output" / "bot_status.json"
        if status_file.exists():
            data = json.loads(status_file.read_text())
            last_cycle = data.get("timestamp", "?")
            if last_cycle and len(last_cycle) > 19:
                last_cycle = last_cycle[:19]
    except Exception:
        pass

    return (
        f"🤖 *Apollo Control Centre*\n\n"
        f"Bot Status: RUNNING (PID {pid})\n"
        f"Regime: {regime} ({confidence:.0f}% confidence)\n"
        f"Phase: PHASE-{phase} | {real_trades} real trades ({trades_to_next} to next)\n"
        f"Open Positions: {open_pos}\n"
        f"Last Cycle: {last_cycle}"
    )


# ── Button response handlers ─────────────────────────────────────────────────

def _handle_positions(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(_db_path(),
            "SELECT ticker, entry_date, entry_price, exit_price, pnl_pct, net_pnl, "
            "position_size, signals_at_entry, holding_days, sector, direction, macro_regime "
            "FROM trade_ledger WHERE exit_date IS NULL AND is_phantom=0 "
            "ORDER BY pnl_pct DESC")

        if not rows:
            _send_message(token, chat_id, "📊 *Positions*\n\nNo open positions currently.")
            return

        # Fetch current prices from historical db
        lines = ["📊 *Open Positions*\n"]
        lines.append("```")
        lines.append(f"{'Ticker':<8} {'Entry':>8} {'PnL%':>7} {'PnL£':>8} {'Size':>5} {'Signal':<15} {'Days':>5} {'Sector':<10}")
        lines.append("-" * 75)

        total_pnl = 0.0
        for r in rows:
            ticker = (r.get("ticker") or "?")[:7]
            entry_p = r.get("entry_price", 0) or 0
            pnl_pct = r.get("pnl_pct", 0) or 0
            net_pnl = r.get("net_pnl", 0) or 0
            size = r.get("position_size", 0) or 0
            signal = (r.get("signals_at_entry") or "?")[:14]
            days = r.get("holding_days", 0) or 0
            sector = (r.get("sector") or "?")[:10]
            total_pnl += net_pnl
            sign = "+" if pnl_pct >= 0 else ""
            lines.append(
                f"{ticker:<8} {entry_p:>8.2f} {sign}{pnl_pct:>6.1f}% {net_pnl:>8.0f} "
                f"{size:>5.0f} {signal:<15} {days:>5} {sector:<10}"
            )

        lines.append("-" * 75)
        lines.append(f"{'TOTAL PnL':>50} {total_pnl:>8.0f}")
        lines.append("```")
        text = "\n".join(lines)
        _send_message(token, chat_id, text, parse_mode="Markdown")
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Positions error: {e}")


def _handle_performance(token: str, chat_id: str) -> None:
    try:
        db = _db_path()
        all_trades = _safe_query(db,
            "SELECT * FROM trade_ledger WHERE is_phantom=0 AND entry_date >= ?",
            (START_DATE,))

        total_trades = len(all_trades)
        closed = [t for t in all_trades if t.get("exit_date")]
        wins = [t for t in closed if (t.get("net_pnl", 0) or 0) > 0]
        total_pnl = sum(t.get("net_pnl", 0) or 0 for t in all_trades)
        win_rate = len(wins) / len(closed) if closed else 0.0

        import math
        daily_pnl: dict[str, float] = {}
        for t in closed:
            day = (t.get("exit_date") or "")[:10]
            if day:
                daily_pnl[day] = daily_pnl.get(day, 0.0) + (t.get("net_pnl", 0) or 0)
        returns = list(daily_pnl.values())
        if len(returns) >= 2:
            n = len(returns)
            mean = sum(returns) / n
            var = sum((r - mean) ** 2 for r in returns) / (n - 1)
            std = math.sqrt(var) if var > 0 else 0.0
            sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
        else:
            sharpe = 0.0

        pnl_series = []
        cum = 0.0
        for t in sorted(closed, key=lambda x: x.get("exit_date") or ""):
            cum += t.get("net_pnl", 0) or 0
            pnl_series.append(cum)

        max_dd = 0.0
        if pnl_series:
            peak = pnl_series[0]
            for v in pnl_series:
                if v > peak:
                    peak = v
                dd = (peak - v) / (abs(peak) + 1e-9)
                if dd > max_dd:
                    max_dd = dd

        avg_win = sum(t.get("net_pnl", 0) or 0 for t in wins) / len(wins) if wins else 0
        losses = [t for t in closed if (t.get("net_pnl", 0) or 0) <= 0]
        avg_loss = sum(t.get("net_pnl", 0) or 0 for t in losses) / len(losses) if losses else 0
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)

        # Signal breakdown
        sig_stats: dict[str, dict] = {}
        for t in closed:
            sig = (t.get("signals_at_entry") or "unknown")[:20]
            if sig not in sig_stats:
                sig_stats[sig] = {"wins": 0, "total": 0, "pnl": 0.0}
            sig_stats[sig]["total"] += 1
            sig_stats[sig]["pnl"] += t.get("net_pnl", 0) or 0
            if (t.get("net_pnl", 0) or 0) > 0:
                sig_stats[sig]["wins"] += 1

        lines = [
            "💹 *Performance Summary*\n",
            f"📊 Total trades: {total_trades} | Closed: {len(closed)}",
            f"✅ Win rate: {win_rate:.1%} ({len(wins)}/{len(closed)})",
            f"💰 All-time PnL: £{total_pnl:+,.0f}",
            f"📐 Sharpe: {sharpe:.2f}",
            f"📉 Max drawdown: {max_dd:.2%}",
            f"📊 Avg win: £{avg_win:,.0f} | Avg loss: £{avg_loss:,.0f}",
            f"🎯 Expectancy per trade: £{expectancy:+,.0f}",
            "",
            "*Signal Breakdown:*",
        ]
        for sig, stats in sorted(sig_stats.items(), key=lambda x: -x[1]["pnl"])[:8]:
            wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0
            lines.append(f"  — {sig}: {stats['total']} trades | {wr:.0%} win | £{stats['pnl']:+,.0f}")

        _send_message(token, chat_id, "\n".join(lines))

        # Send equity curve chart
        try:
            from monitoring.chart_generator import _build_equity_curve
            path = _build_equity_curve()
            if path and os.path.exists(path):
                _send_photo(token, chat_id, path, "Apollo Fund — Equity Curve")
        except Exception as e:
            logger.debug(f"equity curve chart: {e}")

    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Performance error: {e}")


def _handle_health(token: str, chat_id: str) -> None:
    try:
        import subprocess

        # Get live PID
        pid = "?"
        uptime = "?"
        try:
            result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
            for line in result.stdout.splitlines():
                if "python3 main.py" in line and "grep" not in line:
                    parts = line.split()
                    if len(parts) > 1:
                        pid = parts[1]
                        if len(parts) > 9:
                            uptime = parts[9]
                        break
        except Exception:
            pass

        # RAM usage
        ram_used = "?"
        if pid != "?":
            try:
                with open(f"/proc/{pid}/status") as f:
                    for line in f:
                        if "VmRSS:" in line:
                            kb = int(line.split()[1])
                            ram_used = f"{kb / 1024:.0f} MB"
                            break
            except Exception:
                pass

        # Disk usage
        disk_pct = "?"
        try:
            import shutil
            du = shutil.disk_usage("/")
            disk_pct = f"{du.used / du.total:.0%} used ({du.free / 1e9:.0f} GB free)"
        except Exception:
            pass

        # CPU load
        cpu_load = "?"
        try:
            with open("/proc/loadavg") as f:
                cpu_load = f.read().split()[0]
        except Exception:
            pass

        # DB sizes
        db_sizes = []
        for name, path in [
            ("closeloop", _db_path()),
            ("historical", DB_HISTORICAL),
            ("frontier", DB_FRONTIER),
            ("deepdata", DB_DEEPDATA),
        ]:
            try:
                sz = os.path.getsize(path) / 1e6
                db_sizes.append(f"  — {name}: {sz:.1f} MB")
            except Exception:
                db_sizes.append(f"  — {name}: unavailable")

        # Last errors
        last_err = "?"
        log_path = _ROOT / "logs" / "quant_fund.log"
        try:
            with open(log_path, "r", errors="replace") as f:
                lines_all = f.readlines()
            errors = [l.strip() for l in lines_all[-2000:] if "ERROR" in l or "CRITICAL" in l]
            if errors:
                last_err = errors[-1][:100]
            else:
                last_err = "None"
        except Exception:
            pass

        text = (
            f"🔧 *System Health*\n\n"
            f"🤖 Bot PID: `{pid}` | Uptime: {uptime}\n"
            f"💾 RAM: {ram_used}\n"
            f"⚡ CPU Load (1m): {cpu_load}\n"
            f"💿 Disk: {disk_pct}\n\n"
            f"*Database sizes:*\n" + "\n".join(db_sizes) + "\n\n"
            f"*Last error:* `{last_err}`"
        )
        _send_message(token, chat_id, text)
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Health error: {e}")


def _handle_collectors(token: str, chat_id: str) -> None:
    try:
        log_path = _ROOT / "logs" / "quant_fund.log"
        collector_status: dict[str, dict] = {}

        # Parse recent log for collector statuses
        collector_names = [
            "CommodityCollector", "SECFullTextCollector", "TechnologyIntelligence",
            "GovernmentDataCollector", "HealthDashboard", "FinnhubCollector",
            "RedditCollector", "SocialInfluenceTracker", "ShippingCollector",
            "WeatherRiskCollector", "JobPostingsCollector", "OptionsFlowCollector",
            "InsiderCollector",
        ]

        try:
            with open(log_path, "r", errors="replace") as f:
                last_lines = f.readlines()[-3000:]

            for line in reversed(last_lines):
                for cn in collector_names:
                    if cn not in collector_status and cn in line:
                        ts = line[:19] if len(line) > 19 else "?"
                        is_error = "ERROR" in line or "CRITICAL" in line
                        collector_status[cn] = {
                            "last_seen": ts,
                            "status": "🔴 error" if is_error else "🟢 OK",
                            "last_line": line.strip()[:80],
                        }
        except Exception:
            pass

        lines = ["📡 *Collectors Status*\n"]
        for cn in collector_names:
            info = collector_status.get(cn, {"last_seen": "never", "status": "⚪ no data"})
            lines.append(f"{info['status']} *{cn}*\n  Last: {info['last_seen']}")

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Collectors error: {e}")


def _handle_intelligence(token: str, chat_id: str) -> None:
    try:
        umci_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1")

        lines = ["🧠 *Intelligence & Regime*\n"]

        if umci_rows:
            r = umci_rows[0]
            level = r.get("level", "NEUTRAL")
            umci = r.get("umci", 0)
            halt = r.get("halt", False)
            mult = r.get("position_mult", 1.0)
            breakdown = r.get("full_breakdown", "{}")
            try:
                if isinstance(breakdown, str):
                    bd = json.loads(breakdown)
                else:
                    bd = {}
            except Exception:
                bd = {}

            lines += [
                f"🌍 Current regime: *{level}* | UMCI: {umci:.2f}",
                f"📐 Sizing multiplier: {mult}x",
                f"🛑 Halt: {'YES ⚠️' if halt else 'No'}",
                "",
            ]

            if bd:
                lines.append("*HMM Probability Vector:*")
                for k, v in bd.items():
                    if k.startswith("p_"):
                        regime_name = k.replace("p_", "").upper()
                        bar_len = int((v or 0) * 20)
                        bar = "█" * bar_len + "░" * (20 - bar_len)
                        lines.append(f"  {regime_name:<10} [{bar}] {(v or 0):.1%}")
            else:
                lines += [
                    f"  Physical:   {r.get('physical', 0):.2f}",
                    f"  Social:     {r.get('social', 0):.2f}",
                    f"  Scientific: {r.get('scientific', 0):.2f}",
                    f"  Financial:  {r.get('financial', 0):.2f}",
                    f"  AltData:    {r.get('altdata', 0):.2f}",
                ]
        else:
            lines.append("⚪ No regime data available")

        _send_message(token, chat_id, "\n".join(lines))

        # Send regime chart
        try:
            from monitoring.chart_generator import _build_regime_chart
            path = _build_regime_chart()
            if path and os.path.exists(path):
                _send_photo(token, chat_id, path, "Apollo Regime Probabilities")
        except Exception as e:
            logger.debug(f"regime chart: {e}")

    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Intelligence error: {e}")


def _handle_errors(token: str, chat_id: str) -> None:
    try:
        log_path = _ROOT / "logs" / "quant_fund.log"
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        errors = []
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    if today_str in line and ("ERROR" in line or "CRITICAL" in line):
                        errors.append(line.strip()[:120])
        except Exception:
            pass

        if not errors:
            _send_message(token, chat_id, "✅ No errors in today's log")
            return

        last_20 = errors[-20:]
        lines = [f"⚠️ *Last {len(last_20)} Errors Today*\n"]
        for e in last_20:
            lines.append(f"`{e}`")
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Error handler failed: {e}")


def _handle_regime(token: str, chat_id: str) -> None:
    try:
        from intelligence.reasoning_engine import ReasoningEngine
        re_engine = ReasoningEngine()

        umci_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1")
        regime = "NEUTRAL"
        confidence = 0.5
        if umci_rows:
            regime = umci_rows[0].get("level", "NEUTRAL")
            confidence = min((umci_rows[0].get("umci", 0.5) or 0.5) / 10.0, 1.0)

        interp = re_engine.interpret_regime(regime, confidence)
        lines = [
            "🌍 *Regime Intelligence*\n",
            f"Current state: *{regime}* ({confidence:.0%} confidence)",
            f"📋 {interp['plain_english']}",
            f"📐 Sizing: {interp['sizing_impact']}",
            "",
            f"*Active signals:* {', '.join(interp['active_signals']) or 'none'}",
            f"*Suppressed:* {', '.join(interp['suppressed_signals']) or 'none'}",
        ]

        # Historical regime log (last 7 days)
        recent = _safe_query(DB_FRONTIER,
            "SELECT level, recorded_at FROM umci_history "
            "WHERE recorded_at >= date('now', '-7 days') ORDER BY recorded_at ASC LIMIT 50")
        if recent:
            lines.append("\n*Regime log (7d):*")
            daily: dict[str, str] = {}
            for r in recent:
                day = (r.get("recorded_at") or "")[:10]
                daily[day] = r.get("level", "?")
            for day in sorted(daily.keys()):
                icon = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪", "CRISIS": "💀", "EUPHORIA": "🌟"}.get(daily[day], "⚪")
                lines.append(f"  {day}: {icon} {daily[day]}")

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Regime error: {e}")


def _handle_pairs(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT * FROM pairs_signals ORDER BY updated_at DESC LIMIT 20")
        if not rows:
            rows = _safe_query(_db_path(),
                "SELECT * FROM pairs_signals ORDER BY signal_date DESC LIMIT 10")

        lines = ["🔬 *Pairs Trading Status*\n"]
        if not rows:
            lines.append("⚪ No pairs data available")
        else:
            lines.append("```")
            lines.append(f"{'Pair':<16} {'Z-Score':>8} {'Half-Life':>10} {'p-val':>8} {'Signal':<10}")
            lines.append("-" * 56)
            for r in rows[:10]:
                a = r.get("ticker_a", "?")
                b = r.get("ticker_b", "?")
                pair = f"{a}/{b}"[:15]
                zscore = r.get("zscore", r.get("z_score", 0.0)) or 0.0
                hl = r.get("half_life", r.get("half_life_days", 0)) or 0
                pval = r.get("p_value", 0.0) or 0.0
                sig = r.get("signal_strength", r.get("signal", ""))[:9] if r.get("signal_strength") or r.get("signal") else "⚪ flat"
                lines.append(f"{pair:<16} {zscore:>8.2f} {hl:>10.1f} {pval:>8.4f} {sig:<10}")
            lines.append("```")
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Pairs error: {e}")


def _handle_factors(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(_db_path(),
            "SELECT * FROM factor_exposures ORDER BY run_date DESC LIMIT 20")
        lines = ["🧮 *Fama-French 6-Factor Exposures*\n"]
        if not rows:
            lines.append("⚪ No factor model data available")
        else:
            lines.append("```")
            lines.append(f"{'Ticker':<8} {'β_mkt':>7} {'SMB':>7} {'HML':>7} {'MOM':>7} {'RMW':>7} {'CMA':>7} {'α':>7} {'R²':>6}")
            lines.append("-" * 63)
            seen = set()
            for r in rows:
                t = r.get("ticker", "?")
                if t in seen:
                    continue
                seen.add(t)
                lines.append(
                    f"{t:<8} {r.get('beta_mkt',0):>7.3f} {r.get('beta_smb',0):>7.3f} "
                    f"{r.get('beta_hml',0):>7.3f} {r.get('beta_mom',0):>7.3f} "
                    f"{r.get('beta_rmw',0):>7.3f} {r.get('beta_cma',0):>7.3f} "
                    f"{r.get('alpha',0):>7.4f} {r.get('r_squared',0):>6.2f}"
                )
            lines.append("```")
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Factor model error: {e}")


def _handle_options(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_DEEPDATA,
            "SELECT * FROM options_flow ORDER BY collected_at DESC LIMIT 20")
        lines = ["📊 *Options Flow — Top Positions*\n"]
        if not rows:
            lines.append("⚪ No options flow data available")
        else:
            lines.append("```")
            lines.append(f"{'Ticker':<8} {'PCR':>6} {'IV%ile':>8} {'Unusual':>8} {'Score':>8}")
            lines.append("-" * 42)
            for r in rows[:20]:
                t = (r.get("ticker") or r.get("symbol", "?"))[:7]
                pcr = r.get("put_call_ratio", r.get("pcr", 0)) or 0
                iv = r.get("iv_percentile", r.get("iv_rank", 0)) or 0
                unusual = "YES ⚡" if r.get("unusual_activity") or r.get("is_unusual") else "no"
                score = r.get("sentiment_score", r.get("options_smfi", 0)) or 0
                lines.append(f"{t:<8} {pcr:>6.2f} {iv:>8.1f}% {unusual:>8} {score:>8.3f}")
            lines.append("```")

        _send_message(token, chat_id, "\n".join(lines))

        try:
            from monitoring.chart_generator import _build_signal_performance_chart
            path = _build_signal_performance_chart()
            if path and os.path.exists(path):
                _send_photo(token, chat_id, path, "Signal Performance")
        except Exception:
            pass

    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Options error: {e}")


def _handle_insider(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT * FROM insider_transactions ORDER BY transaction_date DESC LIMIT 20")
        if not rows:
            rows = _safe_query(_db_path(),
                "SELECT * FROM insider_transactions ORDER BY stored_at DESC LIMIT 20")

        lines = ["👤 *Insider Transactions (Last 20)*\n"]
        if not rows:
            lines.append("⚪ No insider transaction data available")
        else:
            lines.append("```")
            lines.append(f"{'Ticker':<8} {'Insider':<18} {'Title':<12} {'Type':<5} {'Shares':>10} {'Value':>12} {'Date':<12}")
            lines.append("-" * 80)
            for r in rows:
                t = (r.get("ticker", "?"))[:7]
                name = (r.get("reporter_name") or r.get("insider_name") or "?")[:17]
                title = (r.get("reporter_title") or r.get("title") or "?")[:11]
                ttype = (r.get("transaction_type", "?"))[:5]
                shares = r.get("shares", 0) or 0
                value = r.get("total_value") or r.get("value_usd") or 0
                date = (r.get("transaction_date", "?"))[:10]
                lines.append(f"{t:<8} {name:<18} {title:<12} {ttype:<5} {shares:>10,.0f} {value:>12,.0f} {date:<12}")
            lines.append("```")
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Insider error: {e}")


def _handle_kalman(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT close, date FROM price_history WHERE ticker='SPY' ORDER BY date DESC LIMIT 60")
        lines = ["〰️ *Kalman Filter — SPY*\n"]
        if len(rows) < 10:
            lines.append("⚪ Insufficient SPY data for Kalman filter")
            _send_message(token, chat_id, "\n".join(lines))
            return

        import numpy as np
        closes = [float(r.get("close") or r.get("adj_close") or 0) for r in reversed(rows)]
        closes = [c for c in closes if c > 0]

        if len(closes) < 5:
            lines.append("⚪ No valid price data")
            _send_message(token, chat_id, "\n".join(lines))
            return

        # Simple Kalman filter
        Q = 1e-5  # process noise
        R = 1e-2  # measurement noise
        x = closes[0]
        P = 1.0
        smoothed = []
        innovations = []
        for z in closes:
            x_pred = x
            P_pred = P + Q
            K = P_pred / (P_pred + R)
            innov = z - x_pred
            x = x_pred + K * innov
            P = (1 - K) * P_pred
            smoothed.append(x)
            innovations.append(innov)

        raw_now = closes[-1]
        smooth_now = smoothed[-1]
        innov_now = innovations[-1]
        innov_avg = sum(innovations[-5:]) / 5

        trend = "🟢 Bullish" if innov_avg > 0 else ("🔴 Bearish" if innov_avg < 0 else "⚪ Neutral")
        lines += [
            f"Raw price:    ${raw_now:.2f}",
            f"Smoothed:     ${smooth_now:.2f}",
            f"Innovation:   {innov_now:+.3f} (residual from prediction)",
            f"5d avg innov: {innov_avg:+.3f}",
            f"Trend bias:   {trend}",
            f"Filter variance: P={P:.5f}",
        ]
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Kalman error: {e}")


def _handle_wavelet(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT close, date FROM price_history WHERE ticker='SPY' ORDER BY date DESC LIMIT 128")
        lines = ["🌊 *Wavelet Analysis — SPY*\n"]
        if len(rows) < 32:
            lines.append("⚪ Insufficient data for wavelet analysis")
            _send_message(token, chat_id, "\n".join(lines))
            return

        closes = [float(r.get("close") or 0) for r in reversed(rows)]
        closes = [c for c in closes if c > 0]

        if len(closes) < 32:
            lines.append("⚪ No valid price data")
            _send_message(token, chat_id, "\n".join(lines))
            return

        try:
            import pywt
            coeffs = pywt.wavedec(closes, "db4", level=4)
            cA4, cD4, cD3, cD2, cD1 = coeffs[0], coeffs[1], coeffs[2], coeffs[3], coeffs[4]
            a4_trend = "🟢 Up" if cA4[-1] > cA4[-2] else "🔴 Down"
            d3_pos = "overbought" if cD3[-1] > 0.5 else ("oversold" if cD3[-1] < -0.5 else "neutral")
            d4_mom = "positive" if cD4[-1] > 0 else "negative"
            dom_period = 2 ** 3  # D3 = ~8 day cycle
            lines += [
                f"Dominant cycle: ~{dom_period} day period",
                f"A4 trend (long-term): {a4_trend}",
                f"D3 swing position: {d3_pos} ({cD3[-1]:+.3f})",
                f"D4 medium momentum: {d4_mom} ({cD4[-1]:+.3f})",
                f"Raw price: ${closes[-1]:.2f}",
                f"Denoised (A4 only): reconstructing...",
            ]
        except ImportError:
            # Fallback: simple moving average as trend proxy
            ma5 = sum(closes[-5:]) / 5
            ma20 = sum(closes[-20:]) / 20
            trend = "🟢 Bullish" if ma5 > ma20 else "🔴 Bearish"
            lines += [
                f"SPY price: ${closes[-1]:.2f}",
                f"5d MA: ${ma5:.2f} | 20d MA: ${ma20:.2f}",
                f"Trend: {trend}",
                "(pywt not installed — simplified analysis)",
            ]

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Wavelet error: {e}")


def _handle_shipping(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT * FROM shipping_data ORDER BY date DESC LIMIT 1")
        lines = ["🚢 *Shipping Intelligence*\n"]
        if rows:
            r = rows[0]
            bdi = r.get("bdi_value", "N/A")
            stress = r.get("shipping_stress_index", "N/A")
            regime = r.get("stress_regime", "N/A")
            zscore = r.get("bdi_zscore_252", "N/A")
            lines += [
                f"BDI Value:            {bdi}",
                f"BDI Z-Score (252d):   {zscore:.2f}" if isinstance(zscore, float) else f"BDI Z-Score:          {zscore}",
                f"Shipping Stress Index: {stress:.2f}" if isinstance(stress, float) else f"Stress Index:         {stress}",
                f"Stress Regime:        {regime}",
                f"Data date:            {r.get('date', '?')}",
            ]
        else:
            lines.append("⚪ No shipping data available")

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Shipping error: {e}")


def _handle_commodities(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT * FROM commodity_prices ORDER BY date DESC LIMIT 100")
        lines = ["📦 *Commodities*\n"]
        if not rows:
            lines.append("⚪ No commodity data available")
        else:
            lines.append("```")
            lines.append(f"{'Symbol':<8} {'Price':>10} {'Date':<12}")
            lines.append("-" * 32)
            seen = set()
            for r in rows[:20]:
                sym = r.get("symbol", "?")
                if sym in seen:
                    continue
                seen.add(sym)
                price = r.get("close") or r.get("adj_close") or 0
                date = r.get("date", "?")[:10]
                lines.append(f"{sym:<8} {price:>10.2f} {date:<12}")
            lines.append("```")

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Commodities error: {e}")


def _handle_db_stats(token: str, chat_id: str) -> None:
    try:
        dbs = [
            ("closeloop", _db_path()),
            ("historical", DB_HISTORICAL),
            ("frontier", DB_FRONTIER),
            ("deepdata", DB_DEEPDATA),
            ("intelligence", DB_INTELLIGENCE),
        ]
        lines = ["🗄️ *Database Statistics*\n", "```"]
        lines.append(f"{'DB':<14} {'Tables':>7} {'Size MB':>9} {'Modified':<20}")
        lines.append("-" * 52)
        for name, path in dbs:
            try:
                conn = sqlite3.connect(path, timeout=5)
                tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                conn.close()
                sz = os.path.getsize(path) / 1e6
                mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
                lines.append(f"{name:<14} {len(tables):>7} {sz:>9.1f} {mtime:<20}")
            except Exception as e:
                lines.append(f"{name:<14} error: {str(e)[:25]}")
        lines.append("```")
        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ DB stats error: {e}")


def _handle_force_report(token: str, chat_id: str) -> None:
    _send_message(token, chat_id, "✅ Generating weekly report now...")
    try:
        from monitoring.weekly_report import WeeklyReportGenerator
        reporter = WeeklyReportGenerator()
        reporter.send_weekly_report()
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Report generation failed: {e}")


def _handle_weekly_report(token: str, chat_id: str) -> None:
    _handle_force_report(token, chat_id)


def _handle_news(token: str, chat_id: str) -> None:
    try:
        rows = _safe_query(DB_HISTORICAL,
            "SELECT ticker, headline, published_date, sentiment_raw FROM news_context "
            "ORDER BY published_date DESC LIMIT 20")
        lines = ["📰 *Latest News Articles*\n"]
        if not rows:
            lines.append("⚪ No news data available")
        else:
            for r in rows:
                sentiment = r.get("sentiment_raw", 0) or 0
                icon = "🟢" if sentiment > 0.1 else ("🔴" if sentiment < -0.1 else "⚪")
                lines.append(
                    f"{icon} *{r.get('ticker','?')}* — {r.get('headline','?')[:80]}\n"
                    f"  _{r.get('published_date','?')[:10]}_ | sentiment: {sentiment:.2f}"
                )
        _send_message(token, chat_id, "\n".join(lines[:30]))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ News error: {e}")


def _handle_risk(token: str, chat_id: str) -> None:
    try:
        positions = _safe_query(_db_path(),
            "SELECT ticker, sector, position_size FROM trade_ledger "
            "WHERE exit_date IS NULL AND is_phantom=0")
        lines = ["⚠️ *Portfolio Risk & Concentration*\n"]
        if not positions:
            lines.append("⚪ No open positions")
        else:
            total = len(positions)
            sector_counts: dict[str, int] = {}
            for p in positions:
                s = p.get("sector") or "Unknown"
                sector_counts[s] = sector_counts.get(s, 0) + 1

            lines.append(f"Total positions: {total}\n")
            lines.append("*Sector concentration:*")
            for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
                pct = count / total * 100
                warn = " ⚠️ HIGH" if pct > 15 else ""
                lines.append(f"  — {sector}: {count} ({pct:.0f}%){warn}")

        _send_message(token, chat_id, "\n".join(lines))
    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Risk error: {e}")


def _handle_trade_explanation(token: str, chat_id: str, ticker: str) -> None:
    """Explain why a specific trade was made using the reasoning engine."""
    try:
        ticker = ticker.upper().strip()

        # Find trade
        trade = _safe_query(_db_path(),
            "SELECT * FROM trade_ledger WHERE ticker=? ORDER BY entry_date DESC LIMIT 1",
            (ticker,))
        if not trade:
            # Try case-insensitive
            all_trades = _safe_query(_db_path(),
                "SELECT DISTINCT ticker FROM trade_ledger WHERE is_phantom=0")
            tickers = [t.get("ticker", "").upper() for t in all_trades]
            if ticker not in tickers:
                _send_message(token, chat_id,
                    f"❌ No trade data found for *{ticker}*. Apollo does not hold and has not traded this ticker.")
                return

        td = trade[0]
        entry_date = td.get("entry_date", "?")
        entry_price = td.get("entry_price", 0) or 0
        exit_price = td.get("exit_price") or 0
        pnl_pct = td.get("pnl_pct", 0) or 0
        holding_days = td.get("holding_days", 0) or 0
        macro_regime = td.get("macro_regime", "?")
        signals_raw = td.get("signals_at_entry", "{}")
        vix = td.get("vix_level", "?")

        # Parse signals
        signals = {}
        try:
            if isinstance(signals_raw, str):
                s = json.loads(signals_raw)
                if isinstance(s, dict):
                    signals = {k: float(v) for k, v in s.items() if isinstance(v, (int, float))}
        except Exception:
            if signals_raw and signals_raw != "{}":
                signals = {"primary": 0.5}

        # Get current price
        current_price = entry_price
        try:
            price_row = _safe_query(DB_HISTORICAL,
                "SELECT close FROM price_history WHERE ticker=? ORDER BY date DESC LIMIT 1",
                (ticker,))
            if price_row:
                current_price = price_row[0].get("close") or entry_price
        except Exception:
            pass

        # Get attribution data
        attr_rows = _safe_query(_db_path(),
            "SELECT * FROM pnl_attribution WHERE trade_id=?",
            (td.get("id", 0),))

        # Build context via reasoning engine
        from intelligence.reasoning_engine import ReasoningEngine
        re_engine = ReasoningEngine()

        trade_data = {
            "entry_date": entry_date,
            "entry_price": entry_price,
            "current_price": current_price,
            "pnl_pct": pnl_pct,
            "holding_days": holding_days,
            "macro_regime": macro_regime,
            "vix_level": vix,
            "regime_confidence": 0.6,
            "sector": td.get("sector", "?"),
            "direction": td.get("direction", "?"),
        }

        # Get news for this ticker
        news_rows = _safe_query(DB_HISTORICAL,
            "SELECT headline, sentiment_raw FROM news_context WHERE ticker=? "
            "ORDER BY published_date DESC LIMIT 3",
            (ticker,))

        ctx = re_engine.build_trade_context(ticker, trade_data, signals, macro_regime, news_rows)

        current_p_display = f"${current_price:.2f}" if current_price else "N/A"
        sign = "+" if pnl_pct >= 0 else ""

        lines = [
            f"🧠 *Trade Analysis — {ticker}*\n",
            f"Entry: {entry_date} @ ${entry_price:.2f}",
            f"Current: {current_p_display} | PnL: {sign}{pnl_pct:.1f}%",
            "",
            "📊 *Signal Stack at Entry:*",
        ]

        for sig_entry in ctx.get("signal_stack", []):
            lines.append(
                f"  — *{sig_entry['signal'].upper()}*: {sig_entry['score']:.2f} "
                f"({sig_entry['strength']}) — {sig_entry['label']}"
            )

        if attr_rows:
            lines.append("")
            lines.append("📋 *Attribution Data:*")
            for a in attr_rows[:5]:
                correct = "✅" if a.get("was_signal_correct") else "❌"
                lines.append(
                    f"  {correct} {a.get('signal_name','?')}: "
                    f"strength {a.get('signal_strength_at_entry',0):.2f} → "
                    f"attributed £{a.get('attributed_pnl',0):+.0f}"
                )

        conflicts = ctx.get("conflicts_detected", [])
        if conflicts:
            lines.append("")
            lines.append("⚠️ *Conflicts at Entry:*")
            for c in conflicts:
                lines.append(f"  — {c}")

        regime_interp = ctx.get("regime", {}).get("interpretation", {})
        lines += [
            "",
            f"🌍 Regime at Entry: *{macro_regime}*",
            f"📋 {regime_interp.get('plain_english', '')}",
        ]

        # Layer 2: LLM explanation (async, run in thread)
        llm_text = re_engine.build_deterministic_summary(ctx)
        try:
            loop = asyncio.new_event_loop()
            llm_text = loop.run_until_complete(
                re_engine.llm_explain(ctx, f"Explain this {ticker} trade in plain English")
            )
            loop.close()
        except Exception:
            pass

        lines += ["", "🧠 *AI Explanation:*", llm_text[:500]]

        _send_message(token, chat_id, "\n".join(lines))

    except Exception as e:
        _send_message(token, chat_id, f"⚠️ Trade explanation error for {ticker}: {e}")


# ── NLP keyword routing ──────────────────────────────────────────────────────

_KEYWORD_ROUTES = [
    (["errors", "what broke", "crash", "exception"], "btn_errors"),
    (["positions", "show positions", "portfolio", "holdings"], "btn_positions"),
    (["collectors", "collection", "data health"], "btn_collectors"),
    (["regime", "market regime", "bull", "bear", "neutral", "crisis"], "btn_regime"),
    (["pairs", "cointegration", "pairs trading"], "btn_pairs"),
    (["kalman", "kalman filter"], "btn_kalman"),
    (["wavelet", "cycle"], "btn_wavelet"),
    (["factors", "factor", "ff6", "fama", "french"], "btn_factors"),
    (["options", "flow", "pcr", "options flow"], "btn_options"),
    (["insider", "insider data", "insider buying"], "btn_insider"),
    (["weekly report", "report", "generate report"], "btn_force_report"),
    (["health", "status", "system", "system health"], "btn_health"),
    (["shipping", "bdi", "freight"], "btn_shipping"),
    (["commodities", "oil", "gold", "copper", "commodity"], "btn_commodities"),
    (["database", "db stats", "databases"], "btn_db_stats"),
    (["news", "articles", "headlines"], "nlp_news"),
    (["why no trades", "why not trading", "no signals", "no trades"], "nlp_no_trades"),
    (["biggest risk", "risk", "concentration"], "nlp_risk"),
    (["performance", "pnl", "sharpe", "returns"], "btn_performance"),
    (["intelligence", "hmm", "regime probability"], "btn_intelligence"),
]


def _route_nlp(text: str) -> Optional[str]:
    """Detect keyword match and return action code. Case-insensitive."""
    lower = text.lower()
    for keywords, action in _KEYWORD_ROUTES:
        if any(kw in lower for kw in keywords):
            return action
    return None


def _is_trade_explanation(text: str) -> Optional[str]:
    """Detect 'why TICKER', 'explain TICKER', etc. Returns ticker or None."""
    patterns = [
        r"WHY DID YOU (?:BUY|SELL) ([A-Z]{1,6}(?:\.[A-Z]+)?)",
        r"TRADE ANALYSIS\s+([A-Z]{1,6}(?:\.[A-Z]+)?)",
        r"(?:WHY|EXPLAIN|ANALYSE|ANALYZE|ANALYSIS)\s+([A-Z]{1,6}(?:\.[A-Z]+)?)",
    ]
    upper_text = text.upper()
    for pat in patterns:
        m = re.search(pat, upper_text)
        if m:
            return m.group(1)
    return None


def _dispatch_action(token: str, chat_id: str, action: str) -> None:
    """Dispatch an action code to the appropriate handler."""
    handlers = {
        "btn_positions": _handle_positions,
        "btn_performance": _handle_performance,
        "btn_health": _handle_health,
        "btn_collectors": _handle_collectors,
        "btn_intelligence": _handle_intelligence,
        "btn_errors": _handle_errors,
        "btn_regime": _handle_regime,
        "btn_weekly_report": _handle_weekly_report,
        "btn_pairs": _handle_pairs,
        "btn_factors": _handle_factors,
        "btn_options": _handle_options,
        "btn_insider": _handle_insider,
        "btn_kalman": _handle_kalman,
        "btn_wavelet": _handle_wavelet,
        "btn_shipping": _handle_shipping,
        "btn_commodities": _handle_commodities,
        "btn_db_stats": _handle_db_stats,
        "btn_force_report": _handle_force_report,
        "nlp_news": _handle_news,
        "nlp_risk": _handle_risk,
        "nlp_no_trades": lambda t, c: _send_message(t, c,
            "⚠️ No trade explanation data available — scan results not cached yet"),
    }
    handler = handlers.get(action)
    if handler:
        handler(token, chat_id)
    else:
        _send_message(token, chat_id, f"⚠️ Unknown action: {action}")


# ── Main polling bot ─────────────────────────────────────────────────────────

class PrivateBot:
    """
    Telegram private bot using long-polling (getUpdates).
    Runs as a daemon thread. Does not block the main process.
    """

    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = str(chat_id)
        self._offset = 0
        self._running = False

    def _get_updates(self, timeout: int = 20) -> list:
        try:
            data = {
                "offset": self._offset,
                "timeout": timeout,
                "allowed_updates": ["message", "callback_query"],
            }
            resp = _tg_request(self._token, "getUpdates", data, timeout=timeout + 5)
            if resp and resp.get("ok"):
                return resp.get("result", [])
        except Exception as e:
            logger.debug(f"PrivateBot getUpdates error: {e}")
        return []

    def _handle_update(self, update: dict) -> None:
        """Route a single update to the correct handler."""
        update_id = update.get("update_id", 0)
        self._offset = update_id + 1

        # Callback query (button press)
        if "callback_query" in update:
            cq = update["callback_query"]
            cq_id = cq.get("id")
            cq_chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))
            data = cq.get("data", "")

            # Only respond to our private chat
            if cq_chat_id != self._chat_id:
                return

            _answer_callback(self._token, cq_id)

            def _run():
                try:
                    _dispatch_action(self._token, self._chat_id, data)
                except Exception as e:
                    logger.error(f"PrivateBot callback handler error: {e}")

            threading.Thread(target=_run, daemon=True).start()
            return

        # Text message
        if "message" not in update:
            return

        msg = update["message"]
        msg_chat_id = str(msg.get("chat", {}).get("id", ""))
        text = msg.get("text", "").strip()

        # Only respond to our private chat
        if msg_chat_id != self._chat_id:
            return

        if not text:
            return

        def _run():
            try:
                lower = text.lower()

                # Menu triggers
                if lower in ("hi", "hello", "menu", "/start"):
                    header = _build_menu_header()
                    keyboard = _build_menu_keyboard()
                    _send_message(self._token, self._chat_id, header, reply_markup=keyboard)
                    return

                # Trade explanation
                ticker = _is_trade_explanation(text)
                if ticker:
                    _handle_trade_explanation(self._token, self._chat_id, ticker)
                    return

                # NLP routing
                action = _route_nlp(text)
                if action:
                    _dispatch_action(self._token, self._chat_id, action)
                    return

                # Default: show menu
                header = _build_menu_header()
                keyboard = _build_menu_keyboard()
                _send_message(self._token, self._chat_id, header, reply_markup=keyboard)

            except Exception as e:
                logger.error(f"PrivateBot message handler error: {e}")

        threading.Thread(target=_run, daemon=True).start()

    def run(self) -> None:
        """Main polling loop. Runs until self._running is False."""
        self._running = True
        logger.info(f"PrivateBot: starting long-polling for chat_id={self._chat_id}")

        # Send startup message
        try:
            _send_message(
                self._token, self._chat_id,
                "🤖 *Apollo Private Bot Online*\nSend `hi` or `menu` for the control panel."
            )
        except Exception:
            pass

        consecutive_errors = 0
        while self._running:
            try:
                updates = self._get_updates(timeout=20)
                consecutive_errors = 0
                for update in updates:
                    try:
                        self._handle_update(update)
                    except Exception as e:
                        logger.error(f"PrivateBot update handler error: {e}")
            except Exception as e:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 60)
                logger.error(f"PrivateBot polling error (retry in {wait}s): {e}")
                time.sleep(wait)

    def stop(self) -> None:
        self._running = False


def start_private_bot(config: dict = None) -> Optional[PrivateBot]:
    """
    Start the private bot as a daemon thread.
    Called from main.py after bot initialisation.
    Returns the PrivateBot instance.
    """
    if config is None:
        config = _load_config()
    tg = config.get("notifications", {}).get("telegram", {})
    token = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", PRIVATE_CHAT_ID))

    if not token:
        logger.warning("PrivateBot: no Telegram token found — private bot not started")
        return None

    bot = PrivateBot(token, chat_id)
    t = threading.Thread(target=bot.run, daemon=True, name="apollo-private-bot")
    t.start()
    logger.info("PrivateBot: daemon thread started")
    return bot
