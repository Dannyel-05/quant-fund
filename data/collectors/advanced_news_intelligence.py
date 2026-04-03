"""
Advanced News Intelligence
===========================
Full-article reading engine with source credibility scoring,
narrative shift detection, quantitative claim extraction, and
related article discovery.

Classes:
  ArticleReader          — fetches and parses full articles
  SourceCredibilityDB    — manages source credibility scores
  NarrativeShiftTracker  — tracks sentiment over 7/30/90 day windows
  QuantitativeClaimExtractor — extracts forward-looking numeric claims
  RelatedArticleDiscovery — finds confirming/contradicting articles
  AdvancedNewsIntelligence — master orchestrator
"""

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import yaml

# ── Optional BeautifulSoup ─────────────────────────────────────────────────────
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "settings.yaml"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"

# ── Regex patterns for quantitative claim extraction (compiled at module level) ─
_RE_REVENUE = re.compile(
    r'(?:expects?|guides?|targets?|anticipates?)\s+revenue\s+of\s+\$?([\d,.]+)\s*(B|M|billion|million)?',
    re.IGNORECASE,
)
_RE_GROWTH = re.compile(
    r'(?:expects?|anticipates?|targets?)\s+([\d.]+)%?\s+(?:growth|increase|rise)',
    re.IGNORECASE,
)
_RE_MARGIN = re.compile(
    r'(?:expects?|targets?)\s+(?:gross\s+)?margin\s+of\s+([\d.]+)%',
    re.IGNORECASE,
)
_RE_EPS = re.compile(
    r'(?:guides?|estimates?\s+EPS|expects?\s+EPS)\s+(?:of\s+)?\$?([\d.]+)',
    re.IGNORECASE,
)
_RE_HIRING = re.compile(
    r'(?:hiring|adding)\s+([\d,]+)\s+(?:employees?|jobs?|workers?)',
    re.IGNORECASE,
)
_RE_CAPEX = re.compile(
    r'(?:investing|spending)\s+\$?([\d,.]+)\s*(B|M|billion|million)?\s+(?:in|on)',
    re.IGNORECASE,
)

_CLAIM_PATTERNS: List[Tuple[str, Any]] = [
    ("revenue", _RE_REVENUE),
    ("growth", _RE_GROWTH),
    ("margin", _RE_MARGIN),
    ("eps", _RE_EPS),
    ("hiring", _RE_HIRING),
    ("capex", _RE_CAPEX),
]

# ── Config ────────────────────────────────────────────────────────────────────

def _load_config() -> Dict[str, Any]:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as exc:
        logger.warning("Could not load settings.yaml: %s", exc)
        return {}


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    _PERM_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_PERM_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS source_credibility (
            domain       TEXT PRIMARY KEY,
            tier         INTEGER,
            base_score   REAL,
            current_score REAL,
            n_verified   INTEGER DEFAULT 0,
            n_accurate   INTEGER DEFAULT 0,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS narrative_shifts (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_or_sector      TEXT NOT NULL,
            date                  TEXT NOT NULL,
            sentiment_7d          REAL,
            sentiment_30d         REAL,
            sentiment_90d         REAL,
            narrative_shift_score REAL,
            narrative_velocity    REAL,
            is_significant        BOOLEAN DEFAULT 0,
            calculated_at         TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ns_ticker
            ON narrative_shifts(ticker_or_sector, date);

        CREATE TABLE IF NOT EXISTS quantitative_claims (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id       INTEGER,
            claim_text       TEXT,
            claim_type       TEXT,
            claimed_value    REAL,
            claimed_unit     TEXT,
            claimed_timeframe TEXT,
            company_name     TEXT,
            ticker           TEXT,
            speaker          TEXT,
            source_domain    TEXT,
            credibility_score REAL,
            claim_date       TEXT,
            was_verified     BOOLEAN DEFAULT 0,
            actual_value     REAL,
            error_pct        REAL,
            extracted_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS article_connections (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id_a      INTEGER,
            article_id_b      INTEGER,
            connection_type   TEXT,
            connection_strength REAL,
            discovered_at     TEXT
        );
    """)

    # Pre-populate source_credibility with known domains
    _KNOWN_SOURCES = [
        ("wsj.com",             1, 95.0),
        ("ft.com",              1, 95.0),
        ("bloomberg.com",       1, 93.0),
        ("reuters.com",         1, 92.0),
        ("apnews.com",          1, 90.0),
        ("sec.gov",             1, 100.0),
        ("federalreserve.gov",  1, 100.0),
        ("cnbc.com",            2, 75.0),
        ("marketwatch.com",     2, 72.0),
        ("barrons.com",         2, 80.0),
        ("thestreet.com",       2, 70.0),
        ("benzinga.com",        2, 68.0),
        ("seekingalpha.com",    2, 65.0),
        ("motleyfool.com",      2, 63.0),
        ("reddit.com",          3, 45.0),
        ("stocktwits.com",      3, 42.0),
        ("substack.com",        3, 50.0),
        ("medium.com",          3, 48.0),
    ]
    now = datetime.utcnow().isoformat()
    conn.executemany(
        """INSERT OR IGNORE INTO source_credibility
               (domain, tier, base_score, current_score, n_verified, n_accurate, last_updated)
           VALUES (?, ?, ?, ?, 0, 0, ?)""",
        [(d, t, s, s, now) for d, t, s in _KNOWN_SOURCES],
    )
    conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# ArticleReader
# ═══════════════════════════════════════════════════════════════════════════════

class ArticleReader:
    """Fetches and parses full article content from a URL."""

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # ── Fetch ─────────────────────────────────────────────────────────────────

    def _fetch_url(self, url: str) -> Optional[str]:
        try:
            resp = requests.get(url, headers=self._HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.text
            logger.debug("HTTP %s for %s", resp.status_code, url)
        except Exception as exc:
            logger.debug("Fetch error for %s: %s", url, exc)
        return None

    def _fetch_with_archive_fallback(self, url: str) -> Optional[str]:
        html = self._fetch_url(url)
        if html:
            return html
        # Archive.org fallback
        archive_url = f"https://web.archive.org/web/{url}"
        logger.info("Trying Archive.org fallback for %s", url)
        return self._fetch_url(archive_url)

    # ── Parse ─────────────────────────────────────────────────────────────────

    def _parse_bs4(self, html: str, url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        # Remove noise tags
        for tag in soup(["nav", "footer", "aside", "script", "style", "header", "form"]):
            tag.decompose()

        title = ""
        if soup.title:
            title = soup.title.get_text(strip=True)
        # Try og:title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            title = og_title["content"]

        author = ""
        author_meta = soup.find("meta", {"name": "author"}) or soup.find("meta", {"property": "article:author"})
        if author_meta and author_meta.get("content"):
            author = author_meta["content"]

        date = ""
        date_meta = (
            soup.find("meta", {"property": "article:published_time"})
            or soup.find("meta", {"name": "publishdate"})
            or soup.find("time")
        )
        if date_meta:
            date = date_meta.get("content") or date_meta.get("datetime") or date_meta.get_text(strip=True)

        # Body text: prefer article tag, then main, then body
        body_tag = soup.find("article") or soup.find("main") or soup.body
        body = body_tag.get_text(separator=" ", strip=True) if body_tag else ""

        return {"title": title, "author": author, "date": date, "body": body}

    def _parse_regex(self, html: str, url: str) -> Dict[str, Any]:
        # Strip all tags
        clean = re.sub(r"<[^>]+>", " ", html)
        clean = re.sub(r"\s+", " ", clean).strip()

        title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""

        return {"title": title, "author": "", "date": "", "body": clean[:50000]}

    def _parse(self, html: str, url: str) -> Dict[str, Any]:
        if BS4_AVAILABLE:
            try:
                return self._parse_bs4(html, url)
            except Exception as exc:
                logger.debug("BS4 parse error: %s", exc)
        return self._parse_regex(html, url)

    # ── Domain helper ─────────────────────────────────────────────────────────

    @staticmethod
    def _extract_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lstrip("www.")
        except Exception:
            return ""

    # ── Store ─────────────────────────────────────────────────────────────────

    def _store_article(
        self,
        url: str,
        title: str,
        author: str,
        pub_date: str,
        body: str,
        source_name: str,
        ticker: str = "",
    ) -> int:
        now = datetime.utcnow().isoformat()
        word_count = len(body.split()) if body else 0
        cur = self.conn.execute(
            """INSERT INTO raw_articles
                   (url, fetch_date, source, ticker_context, full_text, word_count,
                    title, author, publication_date, is_paywalled, fetch_method,
                    all_tickers_mentioned, sentiment_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'direct', ?, 0.0)""",
            (url, now, source_name, ticker, body, word_count,
             title, author, pub_date, ticker),
        )
        self.conn.commit()
        return cur.lastrowid

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch_article(self, url: str, ticker: str = "") -> Optional[Dict[str, Any]]:
        """Fetch, parse and store a single article. Returns metadata dict or None."""
        # Skip Yahoo consent redirect pages
        if "consent.yahoo.com" in url:
            return None
        domain = self._extract_domain(url)
        html = self._fetch_with_archive_fallback(url)
        if not html:
            logger.warning("Could not fetch article: %s", url)
            return None

        parsed = self._parse(html, url)
        title = parsed.get("title", "")
        author = parsed.get("author", "")
        date = parsed.get("date", "")
        body = parsed.get("body", "")

        # Skip articles with insufficient content (consent pages, paywalls, etc.)
        if len(body) < 100 and len(title) < 20:
            return None

        article_id = self._store_article(url, title, author, date, body, domain, ticker)
        logger.info("Stored article id=%s title=%r domain=%s", article_id, title[:60], domain)

        return {
            "article_id": article_id,
            "url": url,
            "domain": domain,
            "title": title,
            "author": author,
            "date": date,
            "body": body,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SourceCredibilityDB
# ═══════════════════════════════════════════════════════════════════════════════

class SourceCredibilityDB:
    """Manages source credibility scores stored in permanent_archive.db."""

    _DEFAULT_SCORE = 30.0
    _DEFAULT_TIER = 4

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    @staticmethod
    def _extract_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lstrip("www.")
        except Exception:
            return url

    def get_score(self, url: str) -> float:
        domain = self._extract_domain(url)
        row = self.conn.execute(
            "SELECT current_score FROM source_credibility WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row:
            return row[0]
        return self._DEFAULT_SCORE

    def update_accuracy(self, domain: str, was_accurate: bool) -> None:
        signal = 100.0 if was_accurate else 0.0
        row = self.conn.execute(
            "SELECT current_score, n_verified, n_accurate FROM source_credibility WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row:
            current_score, n_verified, n_accurate = row
            new_score = 0.95 * current_score + 0.05 * signal
            self.conn.execute(
                """UPDATE source_credibility
                   SET current_score = ?, n_verified = ?, n_accurate = ?, last_updated = ?
                   WHERE domain = ?""",
                (
                    new_score,
                    n_verified + 1,
                    n_accurate + (1 if was_accurate else 0),
                    datetime.utcnow().isoformat(),
                    domain,
                ),
            )
        else:
            now = datetime.utcnow().isoformat()
            new_score = 0.95 * self._DEFAULT_SCORE + 0.05 * signal
            self.conn.execute(
                """INSERT INTO source_credibility
                       (domain, tier, base_score, current_score, n_verified, n_accurate, last_updated)
                   VALUES (?, ?, ?, ?, 1, ?, ?)""",
                (
                    domain,
                    self._DEFAULT_TIER,
                    self._DEFAULT_SCORE,
                    new_score,
                    1 if was_accurate else 0,
                    now,
                ),
            )
        self.conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# NarrativeShiftTracker
# ═══════════════════════════════════════════════════════════════════════════════

class NarrativeShiftTracker:
    """Tracks sentiment windows and detects narrative shifts."""

    def __init__(self, conn: sqlite3.Connection, credibility_db: SourceCredibilityDB) -> None:
        self.conn = conn
        self.credibility_db = credibility_db

    def _weighted_avg(self, rows: List[Tuple]) -> float:
        """rows = [(sentiment_score, source_domain), ...]"""
        total_weight = 0.0
        weighted_sum = 0.0
        for sentiment, domain in rows:
            if sentiment is None:
                continue
            score = self.credibility_db.get_score(f"http://{domain}") if domain else 30.0
            weighted_sum += sentiment * score
            total_weight += score
        if total_weight == 0:
            return 0.0
        return weighted_sum / total_weight

    def compute_shift(self, ticker_or_sector: str) -> Dict[str, Any]:
        now = datetime.utcnow()
        cutoff_90 = (now - timedelta(days=90)).isoformat()
        cutoff_30 = (now - timedelta(days=30)).isoformat()
        cutoff_7 = (now - timedelta(days=7)).isoformat()

        # Fetch articles from last 90 days mentioning this ticker
        rows = self.conn.execute(
            """SELECT sentiment_score, source, fetch_date
               FROM raw_articles
               WHERE (all_tickers_mentioned LIKE ? OR ticker_context LIKE ?)
                 AND fetch_date >= ?
               ORDER BY fetch_date DESC""",
            (f"%{ticker_or_sector}%", f"%{ticker_or_sector}%", cutoff_90),
        ).fetchall()

        rows_90 = [(r[0], r[1]) for r in rows]
        rows_30 = [(r[0], r[1]) for r in rows if r[2] >= cutoff_30]
        rows_7  = [(r[0], r[1]) for r in rows if r[2] >= cutoff_7]

        sent_90 = self._weighted_avg(rows_90)
        sent_30 = self._weighted_avg(rows_30)
        sent_7  = self._weighted_avg(rows_7)

        shift_score = sent_7 - sent_90
        velocity = (sent_7 - sent_30) * 4.0
        is_significant = abs(shift_score) > 0.3

        if shift_score > 0.3:
            label = "TURNING_BULLISH"
        elif shift_score < -0.3:
            label = "TURNING_BEARISH"
        else:
            label = "STABLE"

        today = now.date().isoformat()
        calc_at = now.isoformat()

        self.conn.execute(
            """INSERT INTO narrative_shifts
                   (ticker_or_sector, date, sentiment_7d, sentiment_30d, sentiment_90d,
                    narrative_shift_score, narrative_velocity, is_significant, calculated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker_or_sector, today, sent_7, sent_30, sent_90,
             shift_score, velocity, int(is_significant), calc_at),
        )
        self.conn.commit()

        return {
            "ticker": ticker_or_sector,
            "shift_score": shift_score,
            "velocity": velocity,
            "sentiment_7d": sent_7,
            "sentiment_30d": sent_30,
            "sentiment_90d": sent_90,
            "label": label,
            "is_significant": is_significant,
            "article_count_90d": len(rows_90),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# QuantitativeClaimExtractor
# ═══════════════════════════════════════════════════════════════════════════════

class QuantitativeClaimExtractor:
    """Extracts forward-looking numeric claims from article text."""

    def __init__(self, conn: sqlite3.Connection, credibility_db: SourceCredibilityDB) -> None:
        self.conn = conn
        self.credibility_db = credibility_db

    @staticmethod
    def _parse_value(raw: str) -> float:
        try:
            return float(raw.replace(",", ""))
        except (ValueError, AttributeError):
            return 0.0

    @staticmethod
    def _resolve_unit(raw_value: str, unit_str: Optional[str]) -> Tuple[float, str]:
        val = float(raw_value.replace(",", ""))
        if unit_str:
            unit_lower = unit_str.lower()
            if unit_lower in ("b", "billion"):
                return val * 1_000, "$M"
            if unit_lower in ("m", "million"):
                return val, "$M"
        return val, "units"

    def extract_claims(
        self,
        text: str,
        source_domain: str,
        claim_date: str,
        article_id: Optional[int] = None,
        ticker: str = "",
        company_name: str = "",
    ) -> List[Dict[str, Any]]:
        results = []
        cred_score = self.credibility_db.get_score(f"http://{source_domain}")
        now = datetime.utcnow().isoformat()

        for claim_type, pattern in _CLAIM_PATTERNS:
            for m in pattern.finditer(text):
                raw_val = m.group(1)
                # Some patterns have a unit group (group 2)
                unit_raw = None
                try:
                    unit_raw = m.group(2)
                except IndexError:
                    pass

                # Context window: 80 chars before and after match
                start = max(0, m.start() - 80)
                end = min(len(text), m.end() + 80)
                claim_text = text[start:end].strip()

                if claim_type in ("revenue", "capex") and unit_raw:
                    claimed_value, claimed_unit = self._resolve_unit(raw_val, unit_raw)
                elif claim_type in ("growth", "margin"):
                    claimed_value = self._parse_value(raw_val)
                    claimed_unit = "%"
                elif claim_type == "eps":
                    claimed_value = self._parse_value(raw_val)
                    claimed_unit = "$"
                else:
                    claimed_value = self._parse_value(raw_val)
                    claimed_unit = "units"

                claim = {
                    "article_id": article_id,
                    "claim_text": claim_text,
                    "claim_type": claim_type,
                    "claimed_value": claimed_value,
                    "claimed_unit": claimed_unit,
                    "claimed_timeframe": "",
                    "company_name": company_name,
                    "ticker": ticker,
                    "speaker": "",
                    "source_domain": source_domain,
                    "credibility_score": cred_score,
                    "claim_date": claim_date,
                    "extracted_at": now,
                }
                results.append(claim)

                try:
                    self.conn.execute(
                        """INSERT INTO quantitative_claims
                               (article_id, claim_text, claim_type, claimed_value, claimed_unit,
                                claimed_timeframe, company_name, ticker, speaker, source_domain,
                                credibility_score, claim_date, was_verified, actual_value,
                                error_pct, extracted_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?)""",
                        (
                            article_id, claim_text, claim_type, claimed_value, claimed_unit,
                            "", company_name, ticker, "", source_domain,
                            cred_score, claim_date, now,
                        ),
                    )
                except Exception as exc:
                    logger.debug("Could not store claim: %s", exc)

        self.conn.commit()
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# RelatedArticleDiscovery
# ═══════════════════════════════════════════════════════════════════════════════

class RelatedArticleDiscovery:
    """Finds related articles in the database and stores connections."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def find_related(
        self,
        article_id: int,
        ticker: Optional[str] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
        now = datetime.utcnow().isoformat()

        if ticker:
            rows = self.conn.execute(
                """SELECT id, title, source FROM raw_articles
                   WHERE id != ?
                     AND (all_tickers_mentioned LIKE ? OR ticker_context LIKE ?)
                     AND fetch_date >= ?
                   ORDER BY fetch_date DESC
                   LIMIT ?""",
                (article_id, f"%{ticker}%", f"%{ticker}%", cutoff, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT id, title, source FROM raw_articles
                   WHERE id != ?
                     AND fetch_date >= ?
                   ORDER BY fetch_date DESC
                   LIMIT ?""",
                (article_id, cutoff, limit),
            ).fetchall()

        related = []
        for row in rows:
            rel_id, rel_title, rel_source = row
            related.append({"article_id": rel_id, "title": rel_title, "source": rel_source})
            try:
                self.conn.execute(
                    """INSERT INTO article_connections
                           (article_id_a, article_id_b, connection_type, connection_strength, discovered_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (article_id, rel_id, "same_company", 0.7, now),
                )
            except Exception as exc:
                logger.debug("Could not store connection: %s", exc)

        self.conn.commit()
        return related


# ═══════════════════════════════════════════════════════════════════════════════
# AdvancedNewsIntelligence — Master Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

class AdvancedNewsIntelligence:
    """Master orchestrator for advanced news intelligence collection."""

    def __init__(self) -> None:
        self.config = _load_config()
        self.conn = _get_conn()
        _init_db(self.conn)

        self.article_reader = ArticleReader(self.conn)
        self.credibility_db = SourceCredibilityDB(self.conn)
        self.narrative_tracker = NarrativeShiftTracker(self.conn, self.credibility_db)
        self.claim_extractor = QuantitativeClaimExtractor(self.conn, self.credibility_db)
        self.related_discovery = RelatedArticleDiscovery(self.conn)

        api_keys = self.config.get("api_keys", {})
        self.news_api_key: Optional[str] = api_keys.get("news_api") or None

    # ── News API ──────────────────────────────────────────────────────────────

    def _search_news_api(self, ticker: str) -> List[Dict[str, Any]]:
        if not self.news_api_key:
            logger.info("No News API key — skipping API search for %s", ticker)
            return []
        try:
            url = (
                f"https://newsapi.org/v2/everything"
                f"?q={ticker}&sortBy=publishedAt&pageSize=10&language=en&apiKey={self.news_api_key}"
            )
            resp = requests.get(url, timeout=15)
            data = resp.json()
            articles = data.get("articles", [])
            logger.info("News API returned %d articles for %s", len(articles), ticker)
            return articles
        except Exception as exc:
            logger.warning("News API error for %s: %s", ticker, exc)
            return []

    # ── Collect and Analyse ───────────────────────────────────────────────────

    def collect_and_analyse(
        self,
        tickers: List[str],
        news_api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if news_api_key:
            self.news_api_key = news_api_key

        summary: Dict[str, Any] = {
            "tickers": tickers,
            "articles_fetched": 0,
            "claims_extracted": 0,
            "connections_found": 0,
            "narrative_shifts": {},
        }

        for ticker in tickers:
            logger.info("Processing ticker: %s", ticker)
            api_articles = self._search_news_api(ticker)

            for art in api_articles:
                url = art.get("url")
                if not url:
                    continue
                # Skip Yahoo consent redirect pages — they contain no article content
                if "consent.yahoo.com" in url:
                    continue
                # Skip if article language is explicitly non-English
                if art.get("language") and art.get("language") != "en":
                    continue
                try:
                    result = self.article_reader.fetch_article(url, ticker=ticker)
                    if not result:
                        continue
                    summary["articles_fetched"] += 1

                    article_id = result["article_id"]
                    body = result.get("body", "")
                    domain = result.get("domain", "")
                    date = result.get("date", datetime.utcnow().date().isoformat())

                    # Extract quantitative claims
                    claims = self.claim_extractor.extract_claims(
                        body, domain, date,
                        article_id=article_id,
                        ticker=ticker,
                    )
                    summary["claims_extracted"] += len(claims)

                    # Find related articles
                    related = self.related_discovery.find_related(article_id, ticker=ticker)
                    summary["connections_found"] += len(related)

                    time.sleep(0.5)  # polite rate limiting

                except Exception as exc:
                    logger.warning("Error processing article %s: %s", url, exc)

            # Compute narrative shift for this ticker
            try:
                shift = self.narrative_tracker.compute_shift(ticker)
                summary["narrative_shifts"][ticker] = shift
            except Exception as exc:
                logger.warning("Narrative shift error for %s: %s", ticker, exc)

        logger.info(
            "collect_and_analyse complete — articles=%d claims=%d connections=%d",
            summary["articles_fetched"],
            summary["claims_extracted"],
            summary["connections_found"],
        )
        return summary

    def get_narrative_shift(self, ticker: str) -> Dict[str, Any]:
        return self.narrative_tracker.compute_shift(ticker)

    def get_source_score(self, url: str) -> float:
        return self.credibility_db.get_score(url)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ani = AdvancedNewsIntelligence()
    result = ani.collect_and_analyse(["NVDA", "AAPL"])
    print(json.dumps(result, indent=2, default=str))
