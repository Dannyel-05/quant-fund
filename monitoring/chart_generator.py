"""
Apollo Chart Generator
Generates 4 chart types and delivers them to Telegram as photo messages.
Uses Agg backend (non-interactive), all rendering in thread executor.
"""
import asyncio
import io
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import yaml

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]

DB_CLOSELOOP = str(_ROOT / "closeloop" / "storage" / "closeloop.db")
DB_CLOSELOOP_DATA = str(_ROOT / "closeloop_data.db")
DB_HISTORICAL = str(_ROOT / "output" / "historical_db.db")
DB_FRONTIER = str(_ROOT / "frontier" / "storage" / "frontier.db")
DB_DEEPDATA = str(_ROOT / "deepdata" / "storage" / "deepdata.db")

START_DATE = "2026-04-03"

DARK_BG = "#1a1a2e"
CARD_BG = "#16213e"
ACCENT = "#0f3460"
HIGHLIGHT = "#e94560"
GREEN = "#00c853"
RED = "#ff1744"
AMBER = "#ffc107"
GRAY = "#9e9e9e"

CHART_DPI = 100
CHART_W = 12  # inches → 1200px at 100dpi
CHART_H = 7


def _load_config() -> dict:
    try:
        return yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())
    except Exception:
        return {}


def _get_tg_credentials() -> tuple[str, str]:
    cfg = _load_config()
    tg = cfg.get("notifications", {}).get("telegram", {})
    return tg.get("bot_token", ""), str(tg.get("chat_id", "8508697534"))


def _safe_db_query(db_path: str, sql: str, params=()):
    import sqlite3
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"chart_generator: DB query failed on {db_path}: {e}")
        return []


def _db_path():
    """Return the closeloop DB path that has data."""
    if Path(DB_CLOSELOOP).stat().st_size > 100:
        return DB_CLOSELOOP
    return DB_CLOSELOOP_DATA


def _save_chart(fig: plt.Figure, name: str) -> str:
    """Save figure to /tmp and return path."""
    ts = int(time.time())
    path = f"/tmp/apollo_chart_{name}_{ts}.png"
    fig.savefig(path, dpi=CHART_DPI, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    return path


def _send_photo_to_telegram(photo_path: str, caption: str = "") -> bool:
    """Send a photo file to Telegram via multipart form upload."""
    token, chat_id = _get_tg_credentials()
    if not token or not chat_id:
        return False
    try:
        import mimetypes
        boundary = b"----ApolloBoundary"
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
            body += caption.encode() + b"\r\n"
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
        logger.error(f"chart_generator: Telegram photo send failed: {e}")
        return False


def _placeholder_chart(name: str, reason: str) -> str:
    """Generate a placeholder chart with an error message."""
    fig = plt.figure(figsize=(CHART_W, CHART_H), facecolor=DARK_BG)
    ax = fig.add_subplot(111)
    ax.set_facecolor(DARK_BG)
    ax.text(
        0.5, 0.5,
        f"Data not yet available\n{reason}",
        transform=ax.transAxes,
        ha="center", va="center",
        fontsize=16, color=AMBER,
        fontweight="bold",
    )
    ax.axis("off")
    return _save_chart(fig, name)


# ── Chart implementations ──────────────────────────────────────────────────────

def _build_position_chart(ticker: str) -> str:
    """60-day candlestick with entry, ATR stop, RSI, and volume."""
    try:
        # Fetch OHLCV from historical.db first
        rows = _safe_db_query(DB_HISTORICAL,
            "SELECT date, open, high, low, close, adj_close, volume FROM price_history "
            "WHERE ticker = ? ORDER BY date DESC LIMIT 70",
            (ticker,))

        import pandas as pd
        if len(rows) < 10:
            # Fallback to yfinance
            try:
                import yfinance as yf
                df = yf.download(ticker, period="65d", progress=False)
                if df.empty:
                    return _placeholder_chart(f"position_{ticker}", f"No price data for {ticker}")
                df = df.tail(60)
            except Exception as e:
                return _placeholder_chart(f"position_{ticker}", f"yfinance failed: {e}")
        else:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
            df.rename(columns={"adj_close": "Adj Close", "open": "Open", "high": "High",
                                "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
            df = df.tail(60)

        # Get trade data for this ticker
        trade_row = _safe_db_query(_db_path(),
            "SELECT * FROM trade_ledger WHERE ticker = ? AND exit_date IS NULL "
            "AND is_phantom = 0 ORDER BY entry_date DESC LIMIT 1",
            (ticker,))
        if not trade_row:
            trade_row = _safe_db_query(_db_path(),
                "SELECT * FROM trade_ledger WHERE ticker = ? AND is_phantom = 0 "
                "ORDER BY entry_date DESC LIMIT 1",
                (ticker,))

        entry_date = None
        entry_price = None
        atr_stop = None

        if trade_row:
            td = trade_row[0]
            entry_date = td.get("entry_date")
            entry_price = td.get("entry_price")
            atr_stop_raw = td.get("atr_at_entry")
            if atr_stop_raw and entry_price:
                atr_stop = entry_price - 2.0 * float(atr_stop_raw)

        # Use mplfinance for candlestick
        import mplfinance as mpf

        # Ensure DataFrame has proper columns for mplfinance
        if "Open" not in df.columns:
            df.rename(columns={k: k.capitalize() for k in df.columns}, inplace=True)

        needed = ["Open", "High", "Low", "Close"]
        for col in needed:
            if col not in df.columns:
                return _placeholder_chart(f"position_{ticker}", f"Missing column {col}")

        # Calculate RSI
        close = df["Close"].astype(float)
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(span=14, adjust=False).mean()
        avg_loss = loss.ewm(span=14, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))

        # Build addplots
        addplots = []
        if entry_price:
            addplots.append(mpf.make_addplot(
                [entry_price] * len(df),
                color=GREEN, linestyle="--", width=1.2, panel=0,
            ))
        if atr_stop:
            addplots.append(mpf.make_addplot(
                [atr_stop] * len(df),
                color=RED, linestyle="--", width=1.2, panel=0,
            ))
        addplots.append(mpf.make_addplot(
            rsi, panel=1, color=ACCENT, width=1.5,
            ylabel="RSI",
        ))
        addplots.append(mpf.make_addplot(
            [70] * len(df), panel=1, color=RED, linestyle="--", width=0.8,
        ))
        addplots.append(mpf.make_addplot(
            [30] * len(df), panel=1, color=GREEN, linestyle="--", width=0.8,
        ))

        mc = mpf.make_marketcolors(up=GREEN, down=RED, volume="in", inherit=True)
        s = mpf.make_mpf_style(
            marketcolors=mc,
            base_mpf_style="nightclouds",
            figcolor=DARK_BG,
            facecolor=DARK_BG,
            edgecolor=DARK_BG,
            gridcolor=ACCENT,
        )

        # Build title
        last_close = float(close.iloc[-1])
        if entry_price:
            pnl_pct = (last_close - entry_price) / entry_price * 100
            title = f"Apollo Position: {ticker} | Entry: ${entry_price:.2f} | PnL: {pnl_pct:+.1f}%"
        else:
            title = f"Apollo — {ticker} | Last: ${last_close:.2f}"

        fig, axes = mpf.plot(
            df,
            type="candle",
            style=s,
            title=title,
            volume=True,
            addplot=addplots,
            figsize=(CHART_W, CHART_H),
            returnfig=True,
            tight_layout=True,
            panel_ratios=(3, 1, 1),
        )
        fig.patch.set_facecolor(DARK_BG)

        ts = int(time.time())
        path = f"/tmp/apollo_chart_position_{ticker}_{ts}.png"
        fig.savefig(path, dpi=CHART_DPI, bbox_inches="tight", facecolor=DARK_BG)
        plt.close(fig)
        return path

    except Exception as e:
        logger.error(f"chart_generator: position chart failed for {ticker}: {e}")
        return _placeholder_chart(f"position_{ticker}", str(e))


def _build_equity_curve() -> str:
    """Cumulative PnL equity curve from Apr 3 2026."""
    try:
        trades = _safe_db_query(_db_path(),
            "SELECT exit_date, net_pnl FROM trade_ledger "
            "WHERE exit_date IS NOT NULL AND entry_date >= ? AND is_phantom = 0 "
            "ORDER BY exit_date ASC",
            (START_DATE,))

        import pandas as pd
        if not trades:
            return _placeholder_chart("equity_curve", "No closed trades since Apr 3 2026")

        df = pd.DataFrame(trades)
        df["exit_date"] = pd.to_datetime(df["exit_date"])
        df["net_pnl"] = pd.to_numeric(df["net_pnl"], errors="coerce").fillna(0)
        df = df.groupby("exit_date")["net_pnl"].sum().reset_index()
        df = df.set_index("exit_date").sort_index()
        df["cum_pnl"] = df["net_pnl"].cumsum()

        # Drawdown
        rolling_max = df["cum_pnl"].cummax()
        drawdown = df["cum_pnl"] - rolling_max

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(CHART_W, CHART_H),
                                        facecolor=DARK_BG, gridspec_kw={"height_ratios": [3, 1]})
        ax1.set_facecolor(DARK_BG)
        ax2.set_facecolor(DARK_BG)

        dates = df.index
        cum_pnl = df["cum_pnl"].values

        # Green above zero, red below
        ax1.plot(dates, cum_pnl, color=GREEN, linewidth=1.5, zorder=3)
        ax1.axhline(0, color=GRAY, linewidth=0.8, linestyle="--", alpha=0.5)
        ax1.fill_between(dates, cum_pnl, 0,
                         where=[v >= 0 for v in cum_pnl], color=GREEN, alpha=0.2)
        ax1.fill_between(dates, cum_pnl, 0,
                         where=[v < 0 for v in cum_pnl], color=RED, alpha=0.2)

        ax1.set_title(f"Apollo Fund — Equity Curve | Since Apr 3 2026",
                      color="white", fontsize=13, pad=10)
        ax1.set_ylabel("Cumulative PnL (£)", color=GRAY)
        ax1.tick_params(colors=GRAY)
        ax1.spines["bottom"].set_color(ACCENT)
        ax1.spines["left"].set_color(ACCENT)
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)
        ax1.grid(True, color=ACCENT, alpha=0.3)

        # Drawdown subplot
        ax2.fill_between(dates, drawdown.values, 0, color=RED, alpha=0.6)
        ax2.set_ylabel("Drawdown (£)", color=GRAY, fontsize=9)
        ax2.tick_params(colors=GRAY, labelsize=8)
        ax2.spines["bottom"].set_color(ACCENT)
        ax2.spines["left"].set_color(ACCENT)
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)
        ax2.grid(True, color=ACCENT, alpha=0.3)

        fig.tight_layout()
        return _save_chart(fig, "equity_curve")

    except Exception as e:
        logger.error(f"chart_generator: equity curve failed: {e}")
        return _placeholder_chart("equity_curve", str(e))


def _build_regime_chart() -> str:
    """Pie chart of current HMM regime probability vector."""
    try:
        umci_rows = _safe_db_query(DB_FRONTIER,
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1")

        regime_colours = {
            "crisis": RED, "bear": "#ff6b6b", "neutral": GRAY,
            "bull": "#00e5ff", "euphoria": GREEN,
        }
        regime_labels = ["crisis", "bear", "neutral", "bull", "euphoria"]
        regime_names = ["Crisis", "Bear", "Neutral", "Bull", "Euphoria"]

        if umci_rows:
            row = umci_rows[0]
            current_regime = row.get("level", "NEUTRAL")
            confidence = 0.6  # default
            # Try to extract probabilities from full_breakdown JSON
            probs = [0.05, 0.10, 0.20, 0.55, 0.10]  # defaults
            try:
                breakdown = row.get("full_breakdown", "{}")
                if isinstance(breakdown, str):
                    bd = __import__("json").loads(breakdown)
                    probs = [
                        bd.get("p_crisis", 0.05),
                        bd.get("p_bear", 0.10),
                        bd.get("p_neutral", 0.20),
                        bd.get("p_bull", 0.55),
                        bd.get("p_euphoria", 0.10),
                    ]
            except Exception:
                pass
        else:
            current_regime = "NEUTRAL"
            confidence = 0.5
            probs = [0.05, 0.10, 0.40, 0.35, 0.10]

        colours = [regime_colours[k] for k in regime_labels]

        fig, ax = plt.subplots(figsize=(CHART_W, CHART_H), facecolor=DARK_BG)
        ax.set_facecolor(DARK_BG)

        wedges, texts, autotexts = ax.pie(
            probs,
            labels=[f"{n}\n{p:.0%}" for n, p in zip(regime_names, probs)],
            colors=colours,
            autopct="%.0f%%",
            startangle=90,
            textprops={"color": "white", "fontsize": 11},
            wedgeprops={"linewidth": 1.5, "edgecolor": DARK_BG},
        )
        for at in autotexts:
            at.set_color("white")
            at.set_fontsize(9)

        conf_display = f"{confidence:.0%}" if confidence < 1 else f"{confidence:.0%}"
        ax.set_title(
            f"Apollo Regime Probabilities | Current: {current_regime} ({conf_display})",
            color="white", fontsize=13, pad=15,
        )

        return _save_chart(fig, "regime")

    except Exception as e:
        logger.error(f"chart_generator: regime chart failed: {e}")
        return _placeholder_chart("regime", str(e))


def _build_signal_performance_chart() -> str:
    """Horizontal bar chart of signal win rates."""
    try:
        rows = _safe_db_query(_db_path(),
            "SELECT signal_name, win_rate, n_trades FROM signal_regime_performance "
            "ORDER BY win_rate DESC")

        if not rows:
            # Fallback: build from pnl_attribution
            rows = _safe_db_query(_db_path(),
                "SELECT signal_name, "
                "AVG(CASE WHEN was_signal_correct THEN 1.0 ELSE 0.0 END) as win_rate, "
                "COUNT(*) as n_trades "
                "FROM pnl_attribution GROUP BY signal_name ORDER BY win_rate DESC")

        if not rows:
            return _placeholder_chart("signal_perf", "No signal performance data available")

        # Deduplicate by signal name
        seen = {}
        for r in rows:
            name = (r.get("signal_name") or "unknown").lower()
            if name not in seen:
                seen[name] = r
        rows = list(seen.values())
        rows.sort(key=lambda x: x.get("win_rate", 0) or 0, reverse=True)

        names = [r.get("signal_name", "?").replace("_", " ").title() for r in rows]
        win_rates = [(r.get("win_rate", 0) or 0) * 100 for r in rows]
        colours = [GREEN if wr >= 50 else RED for wr in win_rates]

        fig, ax = plt.subplots(figsize=(CHART_W, CHART_H), facecolor=DARK_BG)
        ax.set_facecolor(DARK_BG)

        bars = ax.barh(names, win_rates, color=colours, alpha=0.85, edgecolor=DARK_BG, height=0.6)

        ax.axvline(50, color=AMBER, linestyle="--", linewidth=1.2, alpha=0.8, label="50% threshold")
        ax.set_xlim(0, 100)
        ax.set_xlabel("Win Rate (%)", color=GRAY)
        ax.set_title("Apollo Signal Performance | This Week", color="white", fontsize=13, pad=10)
        ax.tick_params(colors=GRAY)
        ax.spines["bottom"].set_color(ACCENT)
        ax.spines["left"].set_color(ACCENT)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, axis="x", color=ACCENT, alpha=0.3)

        # Add value labels
        for bar, wr in zip(bars, win_rates):
            ax.text(min(wr + 1, 95), bar.get_y() + bar.get_height() / 2,
                    f"{wr:.0f}%", va="center", color="white", fontsize=9)

        ax.legend(loc="lower right", facecolor=CARD_BG, labelcolor="white")
        fig.tight_layout()
        return _save_chart(fig, "signal_perf")

    except Exception as e:
        logger.error(f"chart_generator: signal performance chart failed: {e}")
        return _placeholder_chart("signal_perf", str(e))


# ── Main class ──────────────────────────────────────────────────────────────────

class ChartGenerator:
    """
    Generates 4 chart types, saves to /tmp, sends to Telegram.
    All chart generation runs in thread executor to avoid blocking the event loop.
    """

    def __init__(self):
        token, chat_id = _get_tg_credentials()
        self._token = token
        self._chat_id = chat_id

    async def _run_in_executor(self, fn, *args):
        """Run a blocking function in a thread executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, fn, *args)

    async def generate_position_chart(self, ticker: str) -> str:
        path = await self._run_in_executor(_build_position_chart, ticker)
        if path and os.path.exists(path):
            await self._run_in_executor(
                _send_photo_to_telegram, path,
                f"Apollo Position Chart: {ticker}"
            )
        return path

    async def generate_equity_curve(self) -> str:
        path = await self._run_in_executor(_build_equity_curve)
        if path and os.path.exists(path):
            await self._run_in_executor(
                _send_photo_to_telegram, path,
                "Apollo Fund — Equity Curve"
            )
        return path

    async def generate_regime_chart(self) -> str:
        path = await self._run_in_executor(_build_regime_chart)
        if path and os.path.exists(path):
            await self._run_in_executor(
                _send_photo_to_telegram, path,
                "Apollo Regime Probabilities"
            )
        return path

    async def generate_signal_performance_chart(self) -> str:
        path = await self._run_in_executor(_build_signal_performance_chart)
        if path and os.path.exists(path):
            await self._run_in_executor(
                _send_photo_to_telegram, path,
                "Apollo Signal Performance"
            )
        return path
