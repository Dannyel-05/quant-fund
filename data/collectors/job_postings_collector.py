"""
JobPostingsCollector — tracks company hiring trends as a leading revenue indicator.

Data sources:
  1. Indeed RSS (free, no key): https://www.indeed.com/rss?q={company}&l=&sort=date&limit=50
  2. Reed.co.uk API (UK, free tier): https://www.reed.co.uk/api/1.0/search?keywords={company}
     Requires reed_api_key in config.api_keys (optional — skipped if not configured)

Signal:
  job_growth_rate = (current_90d - prior_90d) / max(prior_90d, 1)
  > +20%  → aggressive hiring = revenue growth signal (+0.2 to +0.4)
  > +30% engineering specifically → product expansion (+0.3)
  > +20% sales → revenue push (+0.2)
  < -20%  → job cuts = distress signal (-0.4)

Wire: job_growth_rate → fundamental signal with weight 0.05
"""
from __future__ import annotations

import email.utils
import logging
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

# Keyword patterns for categorisation
_ENGINEERING_KW = {"engineer", "developer", "software", "data scientist", "ml", "ai",
                   "backend", "frontend", "devops", "sre", "infrastructure"}
_SALES_KW       = {"sales", "account executive", "business development", "revenue",
                   "account manager", "sales manager"}
_ADMIN_KW       = {"admin", "support", "operations", "coordinator", "assistant",
                   "hr", "finance", "accounting"}

_INDEED_RSS = "https://www.indeed.com/rss"
_REED_API   = "https://www.reed.co.uk/api/1.0/search"


def _categorise_title(title: str) -> str:
    t = title.lower()
    if any(k in t for k in _ENGINEERING_KW):
        return "engineering"
    if any(k in t for k in _SALES_KW):
        return "sales"
    if any(k in t for k in _ADMIN_KW):
        return "admin"
    return "other"


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse RFC 2822 or ISO date strings."""
    try:
        return datetime(*email.utils.parsedate(date_str)[:6])
    except Exception:
        pass
    try:
        return datetime.fromisoformat(date_str[:10])
    except Exception:
        return None


class JobPostingsCollector:
    """
    Collects job posting counts for companies in the trading universe.
    Calculates growth rates as leading revenue indicator.
    """

    DB_PATH = "closeloop/storage/closeloop.db"

    def __init__(
        self,
        config: Optional[Dict] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self._config  = config or {}
        self._db_path = db_path or self.DB_PATH
        self._reed_key = (
            self._config.get("api_keys", {}).get("reed_api_key", "") or ""
        )
        self._ensure_table()
        try:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Apollo-Quant/1.0 research@apollo-quant.com",
            })
        except ImportError:
            self._session = None  # type: ignore

    # ── DB setup ──────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            con.execute("""
                CREATE TABLE IF NOT EXISTS job_postings (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker            TEXT    NOT NULL,
                    company           TEXT,
                    collection_date   TEXT    NOT NULL,
                    total_postings    INTEGER DEFAULT 0,
                    engineering_count INTEGER DEFAULT 0,
                    sales_count       INTEGER DEFAULT 0,
                    admin_count       INTEGER DEFAULT 0,
                    growth_rate       REAL    DEFAULT 0.0,
                    source            TEXT,
                    stored_at         TEXT    DEFAULT (datetime('now')),
                    UNIQUE(ticker, collection_date, source)
                )
            """)
            con.commit()
            con.close()
        except Exception as exc:
            logger.warning("JobPostingsCollector._ensure_table: %s", exc)

    # ── Indeed RSS ────────────────────────────────────────────────────────

    def fetch_indeed_rss(self, company: str, ticker: str) -> Dict[str, int]:
        """
        Fetch job postings from Indeed RSS for past 90 days.
        Returns categorised counts.
        """
        if self._session is None:
            return {}
        cutoff = datetime.utcnow() - timedelta(days=90)
        counts = {"total": 0, "engineering": 0, "sales": 0, "admin": 0, "other": 0}
        try:
            resp = self._session.get(
                _INDEED_RSS,
                params={"q": company, "l": "", "sort": "date", "limit": 50},
                timeout=15,
            )
            if resp.status_code != 200:
                return counts
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                pub_date = item.findtext("pubDate", "")
                title    = item.findtext("title", "")
                dt = _parse_date(pub_date)
                if dt is None or dt < cutoff:
                    continue
                cat = _categorise_title(title)
                counts["total"] += 1
                counts[cat] += 1
        except Exception as exc:
            logger.debug("fetch_indeed_rss %s: %s", ticker, exc)
        return counts

    # ── Reed UK ───────────────────────────────────────────────────────────

    def fetch_reed_uk(self, company: str, ticker: str) -> Dict[str, int]:
        """
        Fetch UK job postings from Reed.co.uk API (if key configured).
        Returns categorised counts.
        """
        counts = {"total": 0, "engineering": 0, "sales": 0, "admin": 0, "other": 0}
        if not self._reed_key or self._session is None:
            return counts
        try:
            resp = self._session.get(
                _REED_API,
                params={"keywords": company, "resultsToTake": 100},
                auth=(self._reed_key, ""),
                timeout=15,
            )
            if resp.status_code != 200:
                return counts
            data = resp.json()
            for job in data.get("results", []):
                title = job.get("jobTitle", "")
                cat   = _categorise_title(title)
                counts["total"] += 1
                counts[cat] += 1
        except Exception as exc:
            logger.debug("fetch_reed_uk %s: %s", ticker, exc)
        return counts

    # ── growth rate ───────────────────────────────────────────────────────

    def calculate_growth_rate(self, ticker: str) -> float:
        """
        (current_90d_postings - prior_90d_postings) / max(prior_90d_postings, 1)
        Uses stored DB data — needs at least 2 collection cycles.
        """
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            rows = con.execute("""
                SELECT total_postings, collection_date
                FROM job_postings
                WHERE ticker = ?
                ORDER BY collection_date DESC
                LIMIT 2
            """, (ticker,)).fetchall()
            con.close()
            if len(rows) < 2:
                return 0.0
            current = float(rows[0][0] or 0)
            prior   = float(rows[1][0] or 0)
            return (current - prior) / max(prior, 1.0)
        except Exception as exc:
            logger.debug("calculate_growth_rate %s: %s", ticker, exc)
            return 0.0

    # ── signal ────────────────────────────────────────────────────────────

    def job_growth_signal(self, ticker: str) -> float:
        """
        Returns float in [-1, +1] based on job posting growth rate.

          Engineering hiring > 30%  → +0.3
          Sales hiring > 20%        → +0.2
          Total growth > 20%        → +0.2 (additive, capped)
          Job cuts > 20%            → -0.4
        """
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            rows = con.execute("""
                SELECT total_postings, engineering_count, sales_count, collection_date
                FROM job_postings
                WHERE ticker = ?
                ORDER BY collection_date DESC
                LIMIT 2
            """, (ticker,)).fetchall()
            con.close()
        except Exception as exc:
            logger.debug("job_growth_signal %s: %s", ticker, exc)
            return 0.0

        if len(rows) < 2:
            return 0.0

        current_total = float(rows[0][0] or 0)
        prior_total   = float(rows[1][0] or 0)
        current_eng   = float(rows[0][1] or 0)
        prior_eng     = float(rows[1][1] or 0)
        current_sales = float(rows[0][2] or 0)
        prior_sales   = float(rows[1][2] or 0)

        def _growth(cur, prev):
            return (cur - prev) / max(prev, 1.0)

        total_gr = _growth(current_total, prior_total)
        eng_gr   = _growth(current_eng,   prior_eng)
        sales_gr = _growth(current_sales, prior_sales)

        score = 0.0
        if total_gr < -0.20:
            score -= 0.40  # job cuts → distress
        else:
            if eng_gr > 0.30:
                score += 0.30
            if sales_gr > 0.20:
                score += 0.20
            if total_gr > 0.20:
                score += 0.20

        return float(max(-1.0, min(1.0, score)))

    # ── collection ────────────────────────────────────────────────────────

    def _store(self, ticker: str, company: str, counts: Dict[str, int], source: str) -> int:
        today = datetime.utcnow().date().isoformat()
        gr = self.calculate_growth_rate(ticker)
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            con.execute("""
                INSERT OR IGNORE INTO job_postings
                (ticker, company, collection_date, total_postings, engineering_count,
                 sales_count, admin_count, growth_rate, source)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                ticker, company, today,
                counts.get("total", 0),
                counts.get("engineering", 0),
                counts.get("sales", 0),
                counts.get("admin", 0),
                gr, source,
            ))
            stored = con.execute("SELECT changes()").fetchone()[0]
            con.commit()
            con.close()
            return stored
        except Exception as exc:
            logger.debug("_store %s: %s", ticker, exc)
            return 0

    def collect_ticker(self, ticker: str, company: str, market: str = "us") -> int:
        """Collect job postings for one ticker. Returns rows stored."""
        stored = 0
        indeed_counts = self.fetch_indeed_rss(company, ticker)
        if indeed_counts.get("total", 0) > 0:
            stored += self._store(ticker, company, indeed_counts, "indeed")

        if market == "uk" or ticker.endswith(".L"):
            reed_counts = self.fetch_reed_uk(company, ticker)
            if reed_counts.get("total", 0) > 0:
                stored += self._store(ticker, company, reed_counts, "reed")

        return stored

    def collect(self, ticker_company_map: Dict[str, str]) -> int:
        """
        Collect for multiple tickers. ticker_company_map = {ticker: company_name}.
        Returns total rows stored.
        """
        total = 0
        for i, (ticker, company) in enumerate(ticker_company_map.items()):
            market = "uk" if ticker.endswith(".L") else "us"
            total += self.collect_ticker(ticker, company, market)
            if i % 10 == 9:
                time.sleep(0.5)  # light rate limiting
        logger.info("JobPostingsCollector: collected %d tickers, %d rows stored", len(ticker_company_map), total)
        return total

    def status(self) -> Dict[str, Any]:
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            total   = con.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]
            tickers = con.execute("SELECT COUNT(DISTINCT ticker) FROM job_postings").fetchone()[0]
            con.close()
            return {"total_rows": total, "unique_tickers": tickers}
        except Exception:
            return {"total_rows": 0, "unique_tickers": 0}
