"""
StockTwits alternative data collector.

Calls the free (unauthenticated) StockTwits public stream API to compute
a bull/bear sentiment ratio per ticker, with 30-day baseline comparison
via a local SQLite store.
"""

import logging
import sqlite3
import os
import time
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "cache", "stocktwits_baseline.db"
)

# ── SQLite helpers ─────────────────────────────────────────────────────────────

def _ensure_db() -> sqlite3.Connection:
    """Open (and if needed initialise) the baseline SQLite database."""
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS baseline (
            ticker      TEXT NOT NULL,
            date        TEXT NOT NULL,
            bull_ratio  REAL,
            watcher_count INTEGER,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.commit()
    return conn


def _store_daily(conn: sqlite3.Connection, ticker: str, bull_ratio: float, watcher_count: int) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO baseline (ticker, date, bull_ratio, watcher_count)
            VALUES (?, ?, ?, ?)
            """,
            (ticker, today, bull_ratio, watcher_count),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("stocktwits DB write error for %s: %s", ticker, exc)


def _get_30d_baseline(conn: sqlite3.Connection, ticker: str) -> float | None:
    """Return average bull_ratio over the past 30 days, or None if not enough data."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()
    try:
        cursor = conn.execute(
            "SELECT AVG(bull_ratio) FROM baseline WHERE ticker=? AND date>=?",
            (ticker, cutoff),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception as exc:
        logger.warning("stocktwits DB read error for %s: %s", ticker, exc)
    return None


# ── parsing helpers ────────────────────────────────────────────────────────────

def _parse_created_at(ts_str: str) -> datetime | None:
    """Parse StockTwits ISO timestamp (may end with 'Z')."""
    if not ts_str:
        return None
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str)
    except Exception:
        return None


def _quality_score(bulls: int, bears: int, watcher_count: int, baseline_diff: float | None) -> float:
    """Heuristic quality signal [0, 1]."""
    total = bulls + bears
    base = 0.4
    if total >= 20:
        base += 0.2
    if total >= 50:
        base += 0.1
    if watcher_count > 10_000:
        base += 0.1
    if baseline_diff is not None and abs(baseline_diff) > 0.1:
        base += 0.2  # significant deviation from baseline boosts quality
    return min(base, 1.0)


# ── main collector ─────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect StockTwits sentiment signals for the given tickers.

    Parameters
    ----------
    tickers : list of str
    market  : str
    config  : dict  (optional — not required for StockTwits free tier)

    Returns
    -------
    list of result dicts.
    """
    if config is None:
        config = {}

    results: list = []
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)

    try:
        conn = _ensure_db()
    except Exception as exc:
        logger.warning("stocktwits: could not open baseline DB: %s", exc)
        conn = None

    session = requests.Session()
    session.headers.update({"User-Agent": "quant-fund/1.0"})

    for ticker in tickers:
        time.sleep(1)  # rate-limit courtesy

        url = _BASE_URL.format(ticker=ticker)
        try:
            resp = session.get(url, timeout=10)
        except requests.RequestException as exc:
            logger.warning("stocktwits: network error for %s: %s", ticker, exc)
            continue

        if resp.status_code == 429:
            logger.warning("stocktwits: rate limited (429) for %s, skipping", ticker)
            continue

        if resp.status_code != 200:
            logger.warning(
                "stocktwits: HTTP %s for %s, skipping", resp.status_code, ticker
            )
            continue

        try:
            payload = resp.json()
        except Exception as exc:
            logger.warning("stocktwits: JSON parse error for %s: %s", ticker, exc)
            continue

        # ── extract messages ─────────────────────────────────────────────────
        messages = payload.get("messages", [])
        symbol_info = payload.get("symbol", {})
        watcher_count = int(symbol_info.get("watchlist_count", 0) or 0)

        bulls = 0
        bears = 0
        recent_messages: list = []

        for msg in messages:
            created_raw = msg.get("created_at", "")
            created_dt = _parse_created_at(created_raw)
            if created_dt is None or created_dt < cutoff_24h:
                continue

            sentiment = None
            if msg.get("entities", {}).get("sentiment"):
                sentiment = msg["entities"]["sentiment"].get("basic")

            if sentiment == "Bullish":
                bulls += 1
            elif sentiment == "Bearish":
                bears += 1

            recent_messages.append({
                "id": msg.get("id"),
                "body": (msg.get("body") or "")[:200],
                "sentiment": sentiment,
                "created_at": created_raw,
            })

        total = bulls + bears
        bull_ratio = bulls / total if total > 0 else 0.5

        # StockTwitsScore in [-1, +1]
        st_score = (bull_ratio - 0.5) * 2.0

        # ── 30-day baseline comparison ────────────────────────────────────────
        baseline: float | None = None
        baseline_diff: float | None = None
        if conn is not None:
            baseline = _get_30d_baseline(conn, ticker)
            if baseline is not None:
                baseline_diff = bull_ratio - baseline
            _store_daily(conn, ticker, bull_ratio, watcher_count)

        qs = _quality_score(bulls, bears, watcher_count, baseline_diff)

        results.append({
            "source": "stocktwits",
            "ticker": ticker,
            "market": market,
            "data_type": "social_sentiment",
            "value": round(st_score, 6),
            "raw_data": {
                "bulls": bulls,
                "bears": bears,
                "total_with_sentiment": total,
                "bull_ratio": round(bull_ratio, 4),
                "watcher_count": watcher_count,
                "baseline_30d": round(baseline, 4) if baseline is not None else None,
                "baseline_diff": round(baseline_diff, 4) if baseline_diff is not None else None,
                "message_count_24h": len(recent_messages),
                "sample_messages": recent_messages[:5],
            },
            "timestamp": datetime.now().isoformat(),
            "quality_score": round(qs, 4),
        })

    if conn is not None:
        conn.close()

    logger.info("stocktwits_collector: returned %d signals", len(results))
    return results
