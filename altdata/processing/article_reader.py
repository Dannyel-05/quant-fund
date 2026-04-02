"""
Article Reader — full text extraction with paywall handling and NLP.

For every URL collected from any source, attempts to fetch complete article text:
  1. Direct fetch (requests + BeautifulSoup)
  2. Google Cache fallback
  3. Archive.org fallback
  4. Store whatever is accessible

NLP on full text:
  - VADER + simple FinBERT-like sentiment
  - Extract all mentioned company names / tickers
  - Identify article type
  - Cross-ticker relevance detection

Permanent storage in article_store SQLite table (FTS5 full-text search).

Usage:
    reader = ArticleReader()
    result = reader.fetch_and_store("https://...", ticker_context="SHEN")
    results = reader.search("SHEN guidance")
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _VADER = SentimentIntensityAnalyzer()
    HAS_VADER = True
except Exception:
    _VADER = None
    HAS_VADER = False

_ARTICLE_DB_PATH = "output/article_store.db"
_REQUEST_DELAY   = 1.5
_TIMEOUT         = 20

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Article type keywords
_ARTICLE_TYPE_PATTERNS = {
    "earnings":    re.compile(r"\b(earnings|EPS|revenue|quarterly|Q[1-4] \d{4}|beat|miss|guidance)\b", re.I),
    "merger_ma":   re.compile(r"\b(acqui|merger|takeover|buyout|deal|bid|offer)\b", re.I),
    "analyst":     re.compile(r"\b(upgrade|downgrade|price target|initiat|reiterate|outperform|underperform)\b", re.I),
    "regulatory":  re.compile(r"\b(FDA|SEC|FTC|DOJ|regulatory|approval|rejection|investigation)\b", re.I),
    "insider":     re.compile(r"\b(insider|Form 4|CEO|CFO|officer|director|purchased|sold shares)\b", re.I),
    "macro":       re.compile(r"\b(fed|interest rate|inflation|GDP|recession|tariff|trade war)\b", re.I),
    "opinion":     re.compile(r"\b(opinion|commentary|analysis|perspective|view|think|believe)\b", re.I),
}

# Forward-looking language
_FORWARD_LOOKING = re.compile(
    r"\b(expect|forecast|project|anticipate|guidance|outlook|target|will|plan to|intend)\b", re.I
)

# Risk language
_RISK_LANGUAGE = re.compile(
    r"\b(risk|uncertainty|headwind|challenge|concern|decline|decrease|miss|shortfall|loss)\b", re.I
)

# Simple ticker pattern: $TICKER or standalone 2-5 uppercase letters
_TICKER_RE = re.compile(r"\$([A-Z]{2,5})\b|\b([A-Z]{2,5})\b")

# Common English words to filter from ticker extraction
_COMMON_WORDS = frozenset({
    "A", "AN", "THE", "AND", "OR", "BUT", "IN", "ON", "AT", "TO", "FOR",
    "OF", "IS", "ARE", "WAS", "BE", "BY", "AS", "IT", "WE", "HE", "SHE",
    "IF", "NOT", "ALL", "CAN", "HAS", "HAD", "DO", "DID", "SO", "UP",
    "OUT", "US", "MY", "NO", "GO", "NEW", "INC", "LLC", "ETF", "CEO",
    "CFO", "COO", "IPO", "M&A", "AI", "IT", "UK", "EU", "US", "NY",
    "EPS", "ROI", "ROE", "YTD", "Q1", "Q2", "Q3", "Q4", "FY", "YOY",
    "QOQ", "PE", "PB", "PS", "PG", "GM", "MS", "DB", "GS", "JP",
    "SEC", "FDA", "FTC", "DOJ", "IRS", "ESG", "SaaS", "API",
})

_local = threading.local()

_DDL = """
CREATE TABLE IF NOT EXISTS article_store (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url_hash        TEXT UNIQUE NOT NULL,
    url             TEXT NOT NULL,
    ticker_context  TEXT,
    fetch_date      TEXT NOT NULL,
    source          TEXT,
    title           TEXT,
    full_text       TEXT,
    word_count      INTEGER,
    is_paywalled    INTEGER DEFAULT 0,
    fetch_method    TEXT,   -- 'direct' | 'google_cache' | 'archive_org' | 'partial'
    sentiment_vader REAL,
    article_type    TEXT,
    tickers_mentioned TEXT,          -- JSON list
    cross_ticker_relevance TEXT,     -- JSON dict
    forward_looking_count INTEGER,
    risk_language_count INTEGER,
    stored_at       TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS article_fts USING fts5(
    url,
    ticker_context,
    title,
    full_text,
    source,
    content='article_store',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS article_ai AFTER INSERT ON article_store BEGIN
    INSERT INTO article_fts(rowid, url, ticker_context, title, full_text, source)
    VALUES (new.id, new.url, new.ticker_context, new.title, new.full_text, new.source);
END;

CREATE TRIGGER IF NOT EXISTS article_au AFTER UPDATE ON article_store BEGIN
    INSERT INTO article_fts(article_fts, rowid, url, ticker_context, title, full_text, source)
    VALUES ('delete', old.id, old.url, old.ticker_context, old.title, old.full_text, old.source);
    INSERT INTO article_fts(rowid, url, ticker_context, title, full_text, source)
    VALUES (new.id, new.url, new.ticker_context, new.title, new.full_text, new.source);
END;
"""

_PREDICTION_DDL = """
CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    source_url      TEXT,
    predicted_at    TEXT NOT NULL,
    prediction_type TEXT,   -- 'direction' | 'price_target' | 'event'
    direction       INTEGER,
    price_target    REAL,
    confidence      REAL,
    horizon_days    INTEGER,
    supporting_data TEXT,   -- JSON
    outcome_return  REAL,
    outcome_date    TEXT,
    was_correct     INTEGER,
    resolved_at     TEXT
);
"""


class ArticleReader:
    """
    Fetches, stores, and analyses full article text from URLs.
    Uses SQLite with FTS5 for permanent, searchable storage.
    """

    def __init__(self, db_path: str = _ARTICLE_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_request = 0.0
        self._init_db()

    # ------------------------------------------------------------------
    # DB Management
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if not getattr(_local, "article_conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            _local.article_conn = conn
        return _local.article_conn

    @contextmanager
    def _cursor(self):
        conn = self._connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _init_db(self) -> None:
        conn = self._connect()
        conn.executescript(_DDL)
        conn.executescript(_PREDICTION_DDL)
        conn.commit()

    # ------------------------------------------------------------------
    # HTTP Fetching
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < _REQUEST_DELAY:
            time.sleep(_REQUEST_DELAY - elapsed)
        self._last_request = time.monotonic()

    def _fetch_direct(self, url: str) -> Optional[Tuple[str, str]]:
        """Returns (html, method) or None."""
        if not url or not url.startswith("http"):
            return None
        self._rate_limit()
        try:
            resp = self._session.get(url, timeout=_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text, "direct"
            elif resp.status_code in (403, 429, 503):
                logger.debug("article_reader: blocked (%d) for %s", resp.status_code, url)
                return None
        except Exception as e:
            logger.debug("article_reader: direct fetch error %s: %s", url, e)
        return None

    def _fetch_google_cache(self, url: str) -> Optional[Tuple[str, str]]:
        """Try Google Cache as paywall bypass."""
        cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}"
        self._rate_limit()
        try:
            resp = self._session.get(cache_url, timeout=_TIMEOUT)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text, "google_cache"
        except Exception as e:
            logger.debug("article_reader: google cache error %s: %s", url, e)
        return None

    def _fetch_archive_org(self, url: str) -> Optional[Tuple[str, str]]:
        """Try Wayback Machine as last resort."""
        avail_url = f"https://archive.org/wayback/available?url={url}"
        self._rate_limit()
        try:
            resp = self._session.get(avail_url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                closest = data.get("archived_snapshots", {}).get("closest", {})
                archive_url = closest.get("url")
                if archive_url:
                    self._rate_limit()
                    resp2 = self._session.get(archive_url, timeout=_TIMEOUT)
                    if resp2.status_code == 200 and len(resp2.text) > 500:
                        return resp2.text, "archive_org"
        except Exception as e:
            logger.debug("article_reader: archive.org error %s: %s", url, e)
        return None

    # ------------------------------------------------------------------
    # Text Extraction
    # ------------------------------------------------------------------

    def _extract_text(self, html: str) -> Tuple[str, str]:
        """Extract title and article body text from HTML."""
        if not HAS_BS4:
            return "", ""
        soup = BeautifulSoup(html, "lxml")

        # Title
        title = ""
        title_tag = soup.find("title") or soup.find("h1")
        if title_tag:
            title = title_tag.get_text(strip=True)

        # Remove noise elements
        for tag in soup.find_all(["nav", "header", "footer", "script", "style",
                                   "aside", "form", "iframe", "noscript",
                                   "ins", "figure"]):
            tag.decompose()

        # Try structured selectors first
        article_body = None
        for selector in [
            "article", "[role='main']", "main",
            ".article-body", ".post-body", ".entry-content",
            ".story-body", ".article__body", "#article-body",
            ".article-content", ".content-body", ".post-content",
        ]:
            el = soup.select_one(selector)
            if el and len(el.get_text(strip=True)) > 200:
                article_body = el
                break

        if article_body is None:
            article_body = soup.find("body") or soup

        # Extract all paragraph text
        paragraphs = []
        for p in article_body.find_all(["p", "li", "blockquote", "h2", "h3"]):
            text = p.get_text(separator=" ", strip=True)
            if len(text) > 30:  # filter out nav links and short fragments
                paragraphs.append(text)

        full_text = "\n\n".join(paragraphs)

        # Fallback: just get all text
        if len(full_text) < 200:
            full_text = article_body.get_text(separator=" ", strip=True)

        return title, full_text

    def _is_paywalled(self, text: str, url: str) -> bool:
        """Heuristic paywall detection."""
        low = text.lower()
        paywall_phrases = [
            "subscribe to read", "subscription required", "sign in to read",
            "members only", "premium content", "paywall", "subscribe now",
            "create a free account", "already a subscriber",
        ]
        return any(p in low for p in paywall_phrases) or len(text.split()) < 100

    # ------------------------------------------------------------------
    # NLP Analysis
    # ------------------------------------------------------------------

    def _analyse_text(self, text: str, ticker_context: str = "") -> Dict:
        """Run NLP on full article text."""
        result = {
            "sentiment_vader":       0.0,
            "article_type":          "unknown",
            "tickers_mentioned":     [],
            "cross_ticker_relevance": {},
            "forward_looking_count": 0,
            "risk_language_count":   0,
        }

        if not text:
            return result

        # Sentiment
        if HAS_VADER and _VADER:
            scores = _VADER.polarity_scores(text[:5000])  # cap for speed
            result["sentiment_vader"] = scores.get("compound", 0.0)

        # Article type classification
        for atype, pattern in _ARTICLE_TYPE_PATTERNS.items():
            if pattern.search(text):
                result["article_type"] = atype
                break

        # Forward-looking and risk language counts
        result["forward_looking_count"] = len(_FORWARD_LOOKING.findall(text))
        result["risk_language_count"]   = len(_RISK_LANGUAGE.findall(text))

        # Ticker extraction
        found_tickers = set()
        for m in _TICKER_RE.finditer(text):
            t = m.group(1) or m.group(2)
            if t and t not in _COMMON_WORDS and len(t) >= 2:
                found_tickers.add(t)

        result["tickers_mentioned"] = sorted(found_tickers)

        # Cross-ticker relevance: flag tickers in our universe
        relevance = {}
        if ticker_context:
            for t in found_tickers:
                if t != ticker_context:
                    relevance[t] = {
                        "mentioned_with": ticker_context,
                        "article_sentiment": result["sentiment_vader"],
                    }
        result["cross_ticker_relevance"] = relevance

        return result

    # ------------------------------------------------------------------
    # Main Interface
    # ------------------------------------------------------------------

    def fetch_and_store(
        self,
        url: str,
        ticker_context: str = "",
        source: str = "",
    ) -> Optional[Dict]:
        """
        Fetch a URL, extract full text, run NLP, store permanently.
        Returns the stored record dict or None on failure.
        """
        if not url or not url.startswith("http"):
            return None

        url_hash = hashlib.md5(url.encode()).hexdigest()

        # Check if already stored
        with self._cursor() as cur:
            cur.execute("SELECT * FROM article_store WHERE url_hash=?", [url_hash])
            existing = cur.fetchone()
            if existing:
                return dict(existing)

        # Try fetch methods in order
        html_result = (
            self._fetch_direct(url)
            or self._fetch_google_cache(url)
            or self._fetch_archive_org(url)
        )

        if not html_result:
            logger.warning("article_reader: all fetch methods failed for %s", url)
            # Store a minimal record so we don't retry indefinitely
            self._store_record({
                "url_hash": url_hash,
                "url": url,
                "ticker_context": ticker_context,
                "fetch_date": datetime.now(timezone.utc).isoformat(),
                "source": source,
                "title": "",
                "full_text": "",
                "word_count": 0,
                "is_paywalled": 1,
                "fetch_method": "failed",
                "sentiment_vader": 0.0,
                "article_type": "unknown",
                "tickers_mentioned": "[]",
                "cross_ticker_relevance": "{}",
                "forward_looking_count": 0,
                "risk_language_count": 0,
            })
            return None

        html, method = html_result
        title, full_text = self._extract_text(html)
        paywalled = self._is_paywalled(full_text, url)
        word_count = len(full_text.split())

        if paywalled or word_count < 50:
            method = "partial"

        nlp = self._analyse_text(full_text, ticker_context)

        record = {
            "url_hash":               url_hash,
            "url":                    url,
            "ticker_context":         ticker_context,
            "fetch_date":             datetime.now(timezone.utc).isoformat(),
            "source":                 source,
            "title":                  title[:500] if title else "",
            "full_text":              full_text,  # never truncated
            "word_count":             word_count,
            "is_paywalled":           1 if paywalled else 0,
            "fetch_method":           method,
            "sentiment_vader":        nlp["sentiment_vader"],
            "article_type":           nlp["article_type"],
            "tickers_mentioned":      json.dumps(nlp["tickers_mentioned"]),
            "cross_ticker_relevance": json.dumps(nlp["cross_ticker_relevance"]),
            "forward_looking_count":  nlp["forward_looking_count"],
            "risk_language_count":    nlp["risk_language_count"],
        }

        self._store_record(record)
        logger.info(
            "article_reader: stored %s [%s] %d words%s",
            url[:60], method, word_count, " (paywalled)" if paywalled else ""
        )
        return record

    def _store_record(self, record: dict) -> None:
        record["stored_at"] = datetime.now(timezone.utc).isoformat()
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO article_store ({col_str}) VALUES ({placeholders})"
        with self._cursor() as cur:
            cur.execute(sql, list(record.values()))

    def fetch_many(
        self,
        urls: List[str],
        ticker_context: str = "",
        source: str = "",
        limit: int = 10,
    ) -> List[Dict]:
        """Fetch multiple URLs. Respects rate limits."""
        results = []
        for url in urls[:limit]:
            r = self.fetch_and_store(url, ticker_context, source)
            if r:
                results.append(r)
        return results

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, query: str, limit: int = 20) -> List[Dict]:
        """Full-text search across all stored articles using FTS5."""
        try:
            with self._cursor() as cur:
                cur.execute(
                    """SELECT a.* FROM article_store a
                       JOIN article_fts f ON a.id = f.rowid
                       WHERE article_fts MATCH ?
                       ORDER BY rank LIMIT ?""",
                    [query, limit]
                )
                rows = cur.fetchall()
                results = []
                for row in rows:
                    d = dict(row)
                    for f in ("tickers_mentioned", "cross_ticker_relevance"):
                        if d.get(f) and isinstance(d[f], str):
                            try:
                                d[f] = json.loads(d[f])
                            except Exception:
                                pass
                    results.append(d)
                return results
        except Exception as e:
            logger.warning("article_reader: search error: %s", e)
            return []

    def get_by_ticker(self, ticker: str, limit: int = 50) -> List[Dict]:
        """Get all articles collected for a specific ticker."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM article_store WHERE ticker_context=? ORDER BY fetch_date DESC LIMIT ?",
                [ticker, limit]
            )
            return [dict(row) for row in cur.fetchall()]

    def status(self) -> Dict:
        """Return summary statistics."""
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM article_store")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM article_store WHERE is_paywalled=0")
            accessible = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT ticker_context) FROM article_store")
            tickers = cur.fetchone()[0]
            cur.execute("SELECT SUM(word_count) FROM article_store")
            total_words = cur.fetchone()[0] or 0
        return {
            "total_articles": total,
            "accessible": accessible,
            "paywalled": total - accessible,
            "unique_tickers": tickers,
            "total_words_stored": total_words,
            "db_path": str(self.db_path),
        }
