"""
Apollo Weekly Report Generator
Sends an 8-section weekly report to Telegram every Sunday at 09:00 UTC.
All database access is strictly read-only (no INSERT/UPDATE/DELETE).
"""
import json
import logging
import math
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# ── Database paths ──────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[1]

DB_CLOSELOOP = str(_ROOT / "closeloop" / "storage" / "closeloop.db")
DB_CLOSELOOP_DATA = str(_ROOT / "closeloop_data.db")
DB_HISTORICAL = str(_ROOT / "output" / "historical_db.db")
DB_PERMANENT = str(_ROOT / "output" / "permanent_log.db")
DB_FRONTIER = str(_ROOT / "frontier" / "storage" / "frontier.db")
DB_DEEPDATA = str(_ROOT / "deepdata" / "storage" / "deepdata.db")
DB_INTELLIGENCE = str(_ROOT / "output" / "intelligence_db.db")

START_DATE = "2026-04-03"

SIGNAL_TYPES = [
    "pead", "momentum", "mean_reversion", "gap", "pairs",
    "options_flow", "insider", "wavelet", "kalman",
]


def _esc(text) -> str:
    """Escape underscores for Telegram Markdown v1 (prevents italic parsing)."""
    return str(text).replace("_", r"\_")


def _open_db(path: str) -> sqlite3.Connection:
    """Open a SQLite connection in WAL mode (read-only safe)."""
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


def _safe_query(path: str, sql: str, params=()) -> list:
    """Execute a read-only query; return [] on any error."""
    try:
        conn = _open_db(path)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"DB query failed on {path}: {e}")
        return []


def _safe_scalar(path: str, sql: str, params=(), default=None):
    """Return the first column of the first row, or default on error."""
    rows = _safe_query(path, sql, params)
    if rows:
        return list(rows[0].values())[0]
    return default


def _week_bounds():
    """Return (week_start ISO, week_end ISO) for the last 7 days."""
    now = datetime.now(timezone.utc)
    week_end = now.strftime("%Y-%m-%d")
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    return week_start, week_end


def _sharpe(returns: list[float]) -> float:
    """Annualised Sharpe from a list of daily returns."""
    if len(returns) < 2:
        return 0.0
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0.0
    if std == 0:
        return 0.0
    return (mean / std) * math.sqrt(252)


def _max_drawdown(cum_pnl: list[float]) -> float:
    """Max drawdown from a cumulative PnL series."""
    if not cum_pnl:
        return 0.0
    peak = cum_pnl[0]
    max_dd = 0.0
    for v in cum_pnl:
        if v > peak:
            peak = v
        dd = (peak - v) / (abs(peak) + 1e-9)
        if dd > max_dd:
            max_dd = dd
    return max_dd


# ── Telegram sender ─────────────────────────────────────────────────────────────

def _load_config() -> dict:
    try:
        return yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())
    except Exception:
        return {}


def _send_telegram(token: str, chat_id: str, text: str) -> bool:
    """Send a Telegram message, truncating if over 4000 chars."""
    if len(text) > 4000:
        text = text[:3997] + "..."
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# ── Section builders ─────────────────────────────────────────────────────────────

def _section_1_performance(week_start: str, week_end: str) -> str:
    """Section 1: Performance Summary"""
    try:
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA

        # Week trades
        week_trades = _safe_query(db,
            "SELECT * FROM trade_ledger WHERE exit_date BETWEEN ? AND ? AND is_phantom = 0",
            (week_start, week_end))

        # All-time trades since START_DATE
        all_trades = _safe_query(db,
            "SELECT * FROM trade_ledger WHERE entry_date >= ? AND is_phantom = 0",
            (START_DATE,))

        # Weekly stats
        total_pnl_week = sum(t.get("net_pnl", 0) or 0 for t in week_trades)
        wins = [t for t in week_trades if (t.get("net_pnl", 0) or 0) > 0]
        win_rate = len(wins) / len(week_trades) if week_trades else 0.0
        avg_hold = (sum(t.get("holding_days", 0) or 0 for t in week_trades) / len(week_trades)) if week_trades else 0.0

        # Best / worst trade this week
        if week_trades:
            best = max(week_trades, key=lambda t: t.get("pnl_pct", 0) or 0)
            worst = min(week_trades, key=lambda t: t.get("pnl_pct", 0) or 0)
        else:
            best = worst = {}

        # Daily returns for Sharpe
        daily_pnl: dict[str, float] = {}
        for t in week_trades:
            day = (t.get("exit_date") or "")[:10]
            if day:
                daily_pnl[day] = daily_pnl.get(day, 0.0) + (t.get("net_pnl", 0) or 0)
        daily_returns = list(daily_pnl.values())
        sharpe = _sharpe(daily_returns)

        # Cumulative PnL for max drawdown
        cum = 0.0
        cum_series = []
        for t in sorted(week_trades, key=lambda x: x.get("exit_date") or ""):
            cum += t.get("net_pnl", 0) or 0
            cum_series.append(cum)
        max_dd = _max_drawdown(cum_series)

        # All-time PnL
        all_time_pnl = sum(t.get("net_pnl", 0) or 0 for t in all_trades)

        # SPY comparison (skip if yfinance slow)
        spy_weekly = 0.0
        try:
            import yfinance as yf
            spy = yf.download("SPY", start=week_start, end=week_end, progress=False)
            if not spy.empty and len(spy) >= 2:
                spy_weekly = float((spy["Close"].iloc[-1] - spy["Close"].iloc[0]) / spy["Close"].iloc[0] * 100)
        except Exception:
            pass

        pnl_pct_week = (total_pnl_week / 100000) * 100  # assume £100k base

        lines = [
            "📊 *Apollo Weekly Report 1/8 — Performance Summary*",
            "",
            f"🗓 Week: {week_start} → {week_end}",
            "",
            f"{'🟢' if total_pnl_week >= 0 else '🔴'} Weekly PnL: £{total_pnl_week:+,.0f} ({pnl_pct_week:+.2f}%)",
            f"📈 All-time PnL (since Apr 3 2026): £{all_time_pnl:+,.0f}",
            "",
            f"📊 Trades this week: {len(week_trades)}",
            f"✅ Win rate: {win_rate:.1%}",
            f"⏱ Avg hold time: {avg_hold:.1f} days",
            f"📐 Weekly Sharpe: {sharpe:.2f}",
            f"📉 Max drawdown: {max_dd:.2%}",
        ]

        if spy_weekly != 0.0:
            outperform = pnl_pct_week - spy_weekly
            lines.append(f"🌍 SPY this week: {spy_weekly:+.2f}% | {'🟢 Outperformed' if outperform > 0 else '🔴 Underperformed'} by {outperform:+.2f}%")

        if best:
            lines += [
                "",
                f"🏆 Best trade: *{_esc(best.get('ticker','?'))}* {best.get('pnl_pct',0):+.1f}% "
                f"| Signal: {_esc(best.get('signals_at_entry','?')[:30])} | {best.get('holding_days','?')}d hold",
            ]
        if worst:
            lines += [
                f"💀 Worst trade: *{_esc(worst.get('ticker','?'))}* {worst.get('pnl_pct',0):+.1f}% "
                f"| Signal: {_esc(worst.get('signals_at_entry','?')[:30])} | {worst.get('holding_days','?')}d hold",
            ]

        return "\n".join(lines)
    except Exception as e:
        return f"📊 *Apollo Weekly Report 1/8 — Performance Summary*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_2_signal_performance(week_start: str, week_end: str) -> str:
    """Section 2: Signal Performance"""
    try:
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA

        lines = ["📡 *Apollo Weekly Report 2/8 — Signal Performance*", ""]

        # Get signal performance from pnl_attribution or signal_regime_performance
        srp_rows = _safe_query(db,
            "SELECT signal_name, n_trades, win_rate, mean_pnl, sharpe, best_trade_pnl, worst_trade_pnl "
            "FROM signal_regime_performance WHERE last_updated >= ?",
            (week_start,))

        signal_stats: dict[str, dict] = {}
        for row in srp_rows:
            name = row.get("signal_name", "").lower()
            signal_stats[name] = row

        # Also pull from pnl_attribution for this week's data
        attr_rows = _safe_query(db,
            """SELECT pa.signal_name, pa.was_signal_correct, pa.attributed_pnl
               FROM pnl_attribution pa
               JOIN trade_ledger tl ON pa.trade_id = tl.id
               WHERE tl.exit_date BETWEEN ? AND ? AND tl.is_phantom = 0""",
            (week_start, week_end))

        weekly_signal: dict[str, dict] = {}
        for r in attr_rows:
            name = (r.get("signal_name") or "unknown").lower()
            if name not in weekly_signal:
                weekly_signal[name] = {"fires": 0, "wins": 0, "total_pnl": 0.0}
            weekly_signal[name]["fires"] += 1
            if r.get("was_signal_correct"):
                weekly_signal[name]["wins"] += 1
            weekly_signal[name]["total_pnl"] += r.get("attributed_pnl", 0) or 0

        any_signal = False
        for sig in SIGNAL_TYPES:
            fires = weekly_signal.get(sig, {}).get("fires", 0)
            wins = weekly_signal.get(sig, {}).get("wins", 0)
            total_pnl = weekly_signal.get(sig, {}).get("total_pnl", 0.0)
            win_rate = wins / fires if fires > 0 else 0.0
            avg_return = total_pnl / fires if fires > 0 else 0.0

            icon = "⚠️ POSSIBLY BROKEN" if fires == 0 else ("🟢" if win_rate >= 0.5 else "🔴")
            any_signal = True
            lines.append(
                f"*{sig.upper().replace('_', ' ')}*: {fires} fires | "
                f"win rate {win_rate:.0%} | avg £{avg_return:+.0f} {icon}"
            )

        if not any_signal:
            lines.append("⚠️ No signal attribution data available for this week")

        # Best/worst signal
        if weekly_signal:
            sorted_sigs = sorted(
                weekly_signal.items(),
                key=lambda x: x[1]["wins"] / max(x[1]["fires"], 1),
                reverse=True,
            )
            best_sig = sorted_sigs[0]
            worst_sig = sorted_sigs[-1]
            lines += [
                "",
                f"🏆 Best signal: *{best_sig[0].upper()}* — "
                f"{best_sig[1]['wins']}/{best_sig[1]['fires']} wins",
                f"💀 Worst signal: *{worst_sig[0].upper()}* — "
                f"{worst_sig[1]['wins']}/{worst_sig[1]['fires']} wins",
            ]

        return "\n".join(lines)
    except Exception as e:
        return f"📡 *Apollo Weekly Report 2/8 — Signal Performance*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_3_data_health() -> str:
    """Section 3: Data Collection Health"""
    try:
        lines = ["🔧 *Apollo Weekly Report 3/8 — Data Collection Health*", ""]

        # Check each database
        # (db_name, db_path, table, timestamp_column)
        db_checks = [
            ("closeloop", DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA, "trade_ledger", "entry_date"),
            ("historical", DB_HISTORICAL, "price_history", "date"),
            ("frontier", DB_FRONTIER, "raw_signals", "collected_at"),
            ("deepdata", DB_DEEPDATA, "options_flow", "collected_at"),
        ]

        for db_name, db_path, table, ts_col in db_checks:
            try:
                row_count = _safe_scalar(db_path, f"SELECT COUNT(*) FROM {table}")
                # Rows added today
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                try:
                    today_rows = _safe_scalar(db_path,
                        f"SELECT COUNT(*) FROM {table} WHERE {ts_col} >= ?",
                        (today,))
                except Exception:
                    today_rows = "?"
                lines.append(f"🟢 *{db_name}*: {row_count:,} total rows | {today_rows} today")
            except Exception as e:
                lines.append(f"🔴 *{db_name}*: ⚠️ error — {e}")

        # Check frontier signals
        try:
            frontier_count = _safe_scalar(DB_FRONTIER,
                "SELECT COUNT(*) FROM raw_signals WHERE collected_at >= date('now', '-7 days')")
            lines.append(f"\n📡 *Frontier signals (7d)*: {frontier_count:,} collected")
        except Exception:
            pass

        # Check log for rate limit events
        log_path = _ROOT / "logs" / "quant_fund.log"
        rate_limit_events = 0
        errors_today = 0
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if log_path.exists():
            try:
                with open(log_path, "r", errors="replace") as f:
                    for line in f:
                        if today_str in line:
                            if "rate limit" in line.lower() or "429" in line:
                                rate_limit_events += 1
                            if "ERROR" in line or "CRITICAL" in line:
                                errors_today += 1
            except Exception:
                pass

        lines += [
            "",
            f"⚡ Rate limit events today: {rate_limit_events}",
            f"❌ Errors today: {errors_today}",
        ]

        return "\n".join(lines)
    except Exception as e:
        return f"🔧 *Apollo Weekly Report 3/8 — Data Collection Health*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_4_phase_progress(week_start: str, week_end: str) -> str:
    """Section 4: Phase Progress"""
    try:
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA

        # Read current phase from bot_status.json
        status_file = _ROOT / "output" / "bot_status.json"
        phase = "?"
        try:
            if status_file.exists():
                data = json.loads(status_file.read_text())
                phase = str(data.get("phase", "?"))
        except Exception:
            pass

        # Real trades (non-phantom, non-zero PnL, during market hours)
        real_trades = _safe_scalar(db,
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom = 0 AND ABS(net_pnl) > 0.01",
            default=0)

        # Phantom count
        phantom_count = _safe_scalar(db,
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom = 1",
            default=0)

        # Weekly real trade count
        week_real = _safe_scalar(db,
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom = 0 AND ABS(net_pnl) > 0.01 "
            "AND entry_date >= ?", (week_start,), default=0)

        # Phase thresholds (typical)
        phase_thresholds = {
            "1": 10, "2": 25, "3": 50, "4": 100
        }

        # Determine current phase number
        try:
            current_phase_num = int(str(phase).replace("PHASE_", "").strip())
        except Exception:
            current_phase_num = 1

        next_phase = current_phase_num + 1
        next_threshold = phase_thresholds.get(str(next_phase), 100)
        trades_to_next = max(0, next_threshold - (real_trades or 0))

        # Estimate time to next phase
        daily_avg = week_real / 7 if week_real else 0
        days_to_next = (trades_to_next / daily_avg) if daily_avg > 0 else float("inf")

        lines = [
            "📈 *Apollo Weekly Report 4/8 — Phase Progress*",
            "",
            f"🎯 Current phase: *PHASE\_{current_phase_num}*",
            f"✅ Real trades (non-phantom, non-zero PnL): *{real_trades}*",
            f"👻 Phantom trades: {phantom_count} ⚠️ (these are paper-only, not counted)",
            f"📊 Trades this week: {week_real}",
            "",
            f"🏁 Next phase: PHASE\_{next_phase} (threshold: {next_threshold} real trades)",
            f"📌 Trades to next phase: {trades_to_next}",
        ]

        if daily_avg > 0 and days_to_next < 365:
            lines.append(f"⏱ Estimated time to PHASE\_{next_phase}: ~{days_to_next:.0f} days "
                         f"(at {daily_avg:.1f} trades/day)")
        else:
            lines.append(f"⏱ Estimated time to PHASE\_{next_phase}: insufficient data")

        lines += [
            "",
            f"🔭 Distance to PHASE\_4: {max(0, 100 - (real_trades or 0))} trades",
        ]

        phase_notes = {
            2: "Phase 2 unlocks: increased position sizing and additional signal types",
            3: "Phase 3 unlocks: full capital deployment and live execution",
            4: "Phase 4: full live trading, all systems active",
        }
        if next_phase in phase_notes:
            lines.append(f"ℹ️ What changes at Phase {next_phase}: {phase_notes[next_phase]}")

        return "\n".join(lines)
    except Exception as e:
        return f"📈 *Apollo Weekly Report 4/8 — Phase Progress*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_5_weaknesses(week_start: str, week_end: str) -> str:
    """Section 5: Weaknesses Detected"""
    try:
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA

        losers = _safe_query(db,
            "SELECT * FROM trade_ledger WHERE exit_date BETWEEN ? AND ? "
            "AND is_phantom = 0 AND (net_pnl < 0 OR pnl_pct < 0)",
            (week_start, week_end))

        all_trades = _safe_query(db,
            "SELECT * FROM trade_ledger WHERE exit_date BETWEEN ? AND ? AND is_phantom = 0",
            (week_start, week_end))

        lines = ["⚠️ *Apollo Weekly Report 5/8 — Weaknesses Detected*", ""]

        if not all_trades:
            lines.append("⚪ No closed trades this week — no weakness analysis available")
            return "\n".join(lines)

        # Signal type win rates
        signal_results: dict[str, dict] = {}
        for t in all_trades:
            sig = (t.get("signals_at_entry") or "unknown")[:20]
            if sig not in signal_results:
                signal_results[sig] = {"wins": 0, "total": 0}
            signal_results[sig]["total"] += 1
            if (t.get("net_pnl", 0) or 0) > 0:
                signal_results[sig]["wins"] += 1

        flagged_signals = []
        for sig, stats in signal_results.items():
            wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0
            if wr < 0.4:
                flagged_signals.append((sig, wr, stats["total"]))

        if flagged_signals:
            lines.append("🔴 *Signals with win rate < 40%:*")
            for sig, wr, n in flagged_signals:
                lines.append(f"  — {sig}: {wr:.0%} win rate over {n} trades")
        else:
            lines.append("🟢 No signals with win rate < 40% this week")

        # Sector analysis
        sector_losses: dict[str, float] = {}
        sector_counts: dict[str, int] = {}
        for t in losers:
            sector = t.get("sector") or "Unknown"
            sector_losses[sector] = sector_losses.get(sector, 0.0) + abs(t.get("net_pnl", 0) or 0)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        if sector_losses:
            worst_sector = max(sector_losses, key=sector_losses.get)
            lines += [
                "",
                f"📊 *Worst sector this week:* {worst_sector} — "
                f"£{sector_losses[worst_sector]:,.0f} losses in {sector_counts[worst_sector]} trades",
            ]

        # Long vs short win rates
        longs = [t for t in all_trades if (t.get("direction") or "").lower() in ("long", "buy", "1")]
        shorts = [t for t in all_trades if (t.get("direction") or "").lower() in ("short", "sell", "-1")]
        long_wr = len([t for t in longs if (t.get("net_pnl", 0) or 0) > 0]) / len(longs) if longs else 0
        short_wr = len([t for t in shorts if (t.get("net_pnl", 0) or 0) > 0]) / len(shorts) if shorts else 0

        lines += [
            "",
            f"📊 Long win rate: {long_wr:.0%} ({len(longs)} trades)",
            f"📊 Short win rate: {short_wr:.0%} ({len(shorts)} trades)",
        ]

        # Regime mismatches
        regime_losses: dict[str, int] = {}
        for t in losers:
            regime = t.get("macro_regime") or "Unknown"
            regime_losses[regime] = regime_losses.get(regime, 0) + 1

        if regime_losses:
            worst_regime = max(regime_losses, key=regime_losses.get)
            lines += [
                "",
                f"🌍 *Most losses by regime:* {worst_regime} "
                f"({regime_losses[worst_regime]} losing trades)",
                "  → Consider reviewing signal filters for this regime",
            ]

        return "\n".join(lines)
    except Exception as e:
        return f"⚠️ *Apollo Weekly Report 5/8 — Weaknesses Detected*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_6_regime_intelligence(week_start: str, week_end: str) -> str:
    """Section 6: Regime Intelligence"""
    try:
        lines = ["🌍 *Apollo Weekly Report 6/8 — Regime Intelligence*", ""]

        # UMCI / regime history from frontier
        umci_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM umci_history WHERE recorded_at >= ? ORDER BY recorded_at ASC",
            (week_start,))

        if umci_rows:
            lines.append(f"📅 Regime log ({len(umci_rows)} readings this week):")
            # Show daily summary
            daily_regimes: dict[str, str] = {}
            for r in umci_rows:
                day = (r.get("recorded_at") or "")[:10]
                daily_regimes[day] = r.get("level", "NEUTRAL")

            for day in sorted(daily_regimes.keys()):
                regime = daily_regimes[day]
                icon = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪", "CRISIS": "🔴", "EUPHORIA": "🟢"}.get(regime, "⚪")
                lines.append(f"  {day}: {icon} {regime}")

            # Count transitions
            regimes_list = [daily_regimes[d] for d in sorted(daily_regimes.keys())]
            transitions = sum(1 for i in range(1, len(regimes_list)) if regimes_list[i] != regimes_list[i-1])
            lines.append(f"\n🔄 Regime transitions this week: {transitions}")

            # Latest regime
            latest = umci_rows[-1]
            latest_level = latest.get("level", "NEUTRAL")
            latest_mult = latest.get("position_mult", 1.0)
            lines += [
                "",
                f"📊 Current regime: *{latest_level}*",
                f"📐 Current sizing multiplier: {latest_mult}x",
                f"🛑 Halt active: {'YES ⚠️' if latest.get('halt') else 'No'}",
            ]
        else:
            lines.append("⚪ No regime data available for this week")

        # Wavelet / macro from historical
        try:
            rates_row = _safe_query(DB_HISTORICAL,
                "SELECT * FROM rates_signals ORDER BY calc_date DESC LIMIT 1")
            if rates_row:
                r = rates_row[0]
                lines += [
                    "",
                    f"📈 Yield curve slope: {r.get('yield_curve_slope', 'N/A')}",
                    f"⚡ Rates regime: {r.get('rates_regime', 'N/A')}",
                    f"💳 HY spread: {r.get('hy_spread', 'N/A')}",
                ]
        except Exception:
            pass

        return "\n".join(lines)
    except Exception as e:
        return f"🌍 *Apollo Weekly Report 6/8 — Regime Intelligence*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_7_opportunities(week_start: str, week_end: str) -> str:
    """Section 7: Top 5 Opportunities"""
    try:
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA

        lines = ["🔬 *Apollo Weekly Report 7/8 — Top 5 Opportunities*", ""]

        # Best setups from signal log
        signal_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM signal_log WHERE generated_at >= ? ORDER BY confidence DESC LIMIT 10",
            (week_start,))

        if signal_rows:
            lines.append("*Best signals identified this week:*")
            for i, sig in enumerate(signal_rows[:5], 1):
                ticker = sig.get("ticker", "?")
                sig_name = sig.get("signal_name", "?")
                conf = sig.get("confidence", 0.0) or 0.0
                outcome = sig.get("outcome_return")
                outcome_str = f"{outcome:+.2f}%" if outcome is not None else "pending"
                lines.append(
                    f"{i}. *{_esc(ticker)}* — {_esc(sig_name)} | conf {conf:.2f} | outcome: {outcome_str}"
                )
        else:
            # Fallback: use trade ledger
            top_trades = _safe_query(db,
                "SELECT ticker, signals_at_entry, pnl_pct, macro_regime FROM trade_ledger "
                "WHERE entry_date >= ? AND is_phantom = 0 ORDER BY pnl_pct DESC LIMIT 5",
                (week_start,))

            if top_trades:
                lines.append("*Top performing setups this week:*")
                for i, t in enumerate(top_trades, 1):
                    lines.append(
                        f"{i}. *{_esc(t.get('ticker','?'))}* | {t.get('pnl_pct',0):+.1f}% | "
                        f"Signal: {_esc((t.get('signals_at_entry') or '?')[:25])} | "
                        f"Regime: {t.get('macro_regime','?')}"
                    )
            else:
                lines.append("⚪ No opportunity data available this week")

        return "\n".join(lines)
    except Exception as e:
        return f"🔬 *Apollo Weekly Report 7/8 — Top 5 Opportunities*\n\n⚠️ Data unavailable — {type(e).__name__}"


def _section_8_outlook() -> str:
    """Section 8: Next Week Outlook"""
    try:
        lines = ["🔭 *Apollo Weekly Report 8/8 — Next Week Outlook*", ""]

        # Current regime
        umci_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1")
        current_regime = "NEUTRAL"
        if umci_rows:
            current_regime = umci_rows[0].get("level", "NEUTRAL")

        regime_implications = {
            "BULL": "Bullish — full signal set active, standard sizing",
            "BEAR": "Bearish — defensive posture, short signals prioritised, long sizing reduced",
            "NEUTRAL": "Neutral — balanced exposure, standard rules apply",
            "CRISIS": "Crisis — trading halted or minimal, capital preservation mode",
            "EUPHORIA": "Euphoria — caution warranted, mean reversion signals elevated",
        }
        lines += [
            f"🌍 Current regime: *{current_regime}*",
            f"📋 Implication: {regime_implications.get(current_regime, 'Unknown regime')}",
            "",
        ]

        # Open positions count
        db = DB_CLOSELOOP if Path(DB_CLOSELOOP).stat().st_size > 0 else DB_CLOSELOOP_DATA
        open_pos = _safe_scalar(db,
            "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL AND is_phantom = 0",
            default=0)

        # Get sector distribution
        sector_rows = _safe_query(db,
            "SELECT sector, COUNT(*) as n FROM trade_ledger "
            "WHERE exit_date IS NULL AND is_phantom = 0 GROUP BY sector ORDER BY n DESC")

        lines.append(f"📊 Open positions: {open_pos}")

        if sector_rows and open_pos > 0:
            lines.append("*Sector distribution:*")
            for row in sector_rows[:8]:
                sector = row.get("sector") or "Unknown"
                n = row.get("n", 0)
                pct = n / open_pos * 100 if open_pos > 0 else 0
                warn = " ⚠️ CONCENTRATION RISK" if pct > 15 else ""
                lines.append(f"  — {sector}: {n} positions ({pct:.0f}%){warn}")

        # Upcoming macro events from rates
        try:
            macro_row = _safe_query(DB_HISTORICAL,
                "SELECT * FROM macro_context ORDER BY date DESC LIMIT 1")
            if macro_row:
                m = macro_row[0]
                vix = m.get("vix")
                if vix:
                    vix_note = "elevated" if vix > 25 else "normal"
                    lines.append(f"\n⚡ VIX: {vix:.1f} ({vix_note})")
        except Exception:
            pass

        lines += [
            "",
            "Apollo Weekly Report Complete — Next report Sunday 09:00 UTC 🤖",
        ]

        return "\n".join(lines)
    except Exception as e:
        return f"🔭 *Apollo Weekly Report 8/8 — Next Week Outlook*\n\n⚠️ Data unavailable — {type(e).__name__}\n\nApollo Weekly Report Complete — Next report Sunday 09:00 UTC 🤖"


# ── Main class ───────────────────────────────────────────────────────────────────

class WeeklyReportGenerator:
    """
    Generates and sends the 8-section Apollo weekly report.
    All data is pulled read-only from SQLite databases.
    """

    def __init__(self):
        cfg = _load_config()
        tg = cfg.get("notifications", {}).get("telegram", {})
        self._token = tg.get("bot_token", "")
        self._chat_id = str(tg.get("chat_id", "8508697534"))

    def generate_report(self) -> list[str]:
        """Generate all 8 sections. Returns list of strings."""
        week_start, week_end = _week_bounds()
        sections = [
            _section_1_performance(week_start, week_end),
            _section_2_signal_performance(week_start, week_end),
            _section_3_data_health(),
            _section_4_phase_progress(week_start, week_end),
            _section_5_weaknesses(week_start, week_end),
            _section_6_regime_intelligence(week_start, week_end),
            _section_7_opportunities(week_start, week_end),
            _section_8_outlook(),
        ]
        return sections

    def send_weekly_report(self) -> None:
        """Generate and send all 8 sections to Telegram. Called by scheduler."""
        logger.info("WeeklyReportGenerator: starting weekly report send")
        sections = self.generate_report()
        success_count = 0
        for i, section in enumerate(sections, 1):
            try:
                ok = _send_telegram(self._token, self._chat_id, section)
                if ok:
                    success_count += 1
                    logger.info(f"Weekly report section {i}/8 sent")
                else:
                    logger.error(f"Weekly report section {i}/8 send failed")
            except Exception as e:
                logger.error(f"Weekly report section {i}/8 exception: {e}")
        logger.info(f"WeeklyReportGenerator: {success_count}/8 sections sent")
