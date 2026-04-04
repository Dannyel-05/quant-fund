"""
ShortInterestCollector — fetches biweekly FINRA short interest data.

Data source: NASDAQ API (uses FINRA data)
  https://api.nasdaq.com/api/quote/{ticker}/short-interest

Stores in closeloop.db:
  Table: short_interest
  Columns: ticker, report_date, short_interest, float_shares, short_ratio,
           days_to_cover, short_squeeze_score, updated_at

short_squeeze_score = (short_interest / float_shares) * (1 / max(days_to_cover, 0.1))
High score = high short interest + fast turnover = squeeze risk
"""
from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DB_PATH    = "closeloop/storage/closeloop.db"
_API_BASE   = "https://api.nasdaq.com/api/quote/{ticker}/short-interest"
_HEADERS    = {
    "User-Agent": "Mozilla/5.0 (compatible; QuantBot/1.0)",
    "Accept": "application/json",
}


class ShortInterestCollector:
    """
    Fetches FINRA biweekly short interest data from NASDAQ API.
    Stores results in closeloop.db::short_interest table.
    """

    def __init__(self, db_path: str = _DB_PATH) -> None:
        self._db = db_path
        self._ensure_table()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _ensure_table(self) -> None:
        conn = self._conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS short_interest (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                report_date TEXT,
                short_interest REAL,
                float_shares REAL,
                short_ratio REAL,
                days_to_cover REAL,
                short_squeeze_score REAL,
                updated_at TEXT,
                UNIQUE(ticker, report_date)
            )
        """)
        conn.commit()
        conn.close()

    def fetch_ticker(self, ticker: str) -> Optional[Dict]:
        """
        Fetch short interest for a single ticker from NASDAQ API.
        Returns dict or None on failure.
        """
        import urllib.request, urllib.error, json
        url = _API_BASE.format(ticker=ticker.replace(".L", ""))
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            rows = (data.get("data", {}) or {}).get("shortInterestTable", {}).get("rows", [])
            if not rows:
                return None

            # Most recent row
            row = rows[0]
            short_int  = self._parse_num(row.get("shortInterest", "0"))
            avg_vol    = self._parse_num(row.get("averageDailyShareVolume", "1"))
            days_cover = short_int / max(avg_vol, 1)

            # float_shares not always available — estimate from short ratio
            short_ratio = self._parse_num(row.get("shortPercentOfFloat", "0")) / 100.0
            float_shares = short_int / max(short_ratio, 1e-6) if short_ratio > 0 else 0.0

            squeeze_score = (short_ratio * (1.0 / max(days_cover, 0.1)))

            return {
                "ticker":              ticker,
                "report_date":         str(row.get("settlementDate", date.today())),
                "short_interest":      short_int,
                "float_shares":        float_shares,
                "short_ratio":         short_ratio,
                "days_to_cover":       days_cover,
                "short_squeeze_score": squeeze_score,
            }
        except (urllib.error.HTTPError, urllib.error.URLError, Exception) as exc:
            logger.debug("ShortInterest fetch failed for %s: %s", ticker, exc)
            return None

    def _parse_num(self, s: str) -> float:
        try:
            return float(str(s).replace(",", "").replace("%", "").strip())
        except Exception:
            return 0.0

    def collect(self, tickers: List[str], delay: float = 1.0) -> int:
        """
        Collect short interest for a list of tickers.
        Returns number of tickers successfully stored.
        """
        stored = 0
        for ticker in tickers:
            result = self.fetch_ticker(ticker)
            if result:
                self._store(result)
                stored += 1
            time.sleep(delay)
        logger.info("ShortInterestCollector: stored %d / %d tickers", stored, len(tickers))
        return stored

    def _store(self, record: Dict) -> None:
        conn = self._conn()
        now  = datetime.utcnow().isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO short_interest
            (ticker, report_date, short_interest, float_shares, short_ratio,
             days_to_cover, short_squeeze_score, updated_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            record["ticker"], record["report_date"], record["short_interest"],
            record["float_shares"], record["short_ratio"], record["days_to_cover"],
            record["short_squeeze_score"], now,
        ))
        conn.commit()
        conn.close()

    def get_squeeze_score(self, ticker: str) -> Optional[float]:
        """Return the most recent short squeeze score for a ticker, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT short_squeeze_score FROM short_interest "
            "WHERE ticker=? ORDER BY report_date DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        conn.close()
        return float(row[0]) if row else None

    def get_days_to_cover(self, ticker: str) -> Optional[float]:
        """Return the most recent days-to-cover for a ticker, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT days_to_cover FROM short_interest "
            "WHERE ticker=? ORDER BY report_date DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        conn.close()
        return float(row[0]) if row else None

    def should_block_short(self, ticker: str, days_to_cover_threshold: float = 5.0) -> bool:
        """Return True if days-to-cover > threshold (too risky to short)."""
        dtc = self.get_days_to_cover(ticker)
        if dtc is None:
            return False
        return dtc > days_to_cover_threshold

    def status(self) -> Dict:
        conn = self._conn()
        n = conn.execute("SELECT COUNT(DISTINCT ticker) FROM short_interest").fetchone()[0]
        latest = conn.execute("SELECT MAX(updated_at) FROM short_interest").fetchone()[0]
        conn.close()
        return {"tickers_tracked": n, "last_updated": latest}
