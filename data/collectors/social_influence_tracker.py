"""
Social Influence and Key Person Statement Tracker

Tracks:
  - Fed speeches (hawkish/dovish scoring)
  - Congressional trades (QuiverQuant + House Stock Watcher)
  - CEO forward-looking statements (News API + regex extraction)
  - Influencer mentions — Musk and general (News API)

All data stored permanently in output/permanent_archive.db.
"""

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

import requests
import yaml

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

# ── Hawkish / Dovish keyword lists ────────────────────────────────────────────
_HAWKISH_KEYWORDS = [
    "inflation", "restrictive", "elevated", "persistent",
    "tighten", "above target", "more work to do",
]
_DOVISH_KEYWORDS = [
    "moderate", "cooling", "progress", "balanced",
    "appropriate", "easing", "patient",
]

# ── Common sectors for correlation ───────────────────────────────────────────
_SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "tech": ["technology", "software", "semiconductor", "cloud", "ai", "chip"],
    "energy": ["oil", "gas", "energy", "petroleum", "lng", "refin"],
    "defense": ["defense", "military", "weapon", "contract", "pentagon"],
    "healthcare": ["pharma", "drug", "biotech", "health", "medical", "vaccine"],
    "finance": ["bank", "finance", "financial", "insurance", "lending"],
    "agriculture": ["farm", "crop", "agriculture", "food", "grain"],
}

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
        CREATE TABLE IF NOT EXISTS social_influence (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            person_name             TEXT NOT NULL,
            role                    TEXT,
            statement_date          TEXT,
            statement_time          TEXT,
            platform                TEXT,
            full_text               TEXT,
            url                     TEXT,
            source                  TEXT,
            tickers_mentioned       TEXT,
            companies_mentioned     TEXT,
            sentiment_score         REAL,
            hawkish_dovish_score    REAL,
            quantitative_claims     TEXT,
            market_impact_observed  TEXT,
            was_statement_accurate  INTEGER,
            fetched_at              TEXT DEFAULT (datetime('now')),
            UNIQUE(person_name, url, statement_date)
        );

        CREATE TABLE IF NOT EXISTS ceo_credibility (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            company             TEXT NOT NULL,
            ceo_name            TEXT,
            claim_text          TEXT,
            claim_type          TEXT,
            claim_value         TEXT,
            timeframe           TEXT,
            source              TEXT,
            claim_date          TEXT,
            was_accurate        INTEGER,
            credibility_score   REAL,
            fetched_at          TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS raw_congressional_trades (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            member_name         TEXT,
            ticker              TEXT,
            transaction_date    TEXT,
            transaction_type    TEXT,
            amount_range        TEXT,
            disclosure_date     TEXT,
            sector              TEXT,
            correlated_trade    INTEGER DEFAULT 0,
            raw_json            TEXT,
            fetched_at          TEXT DEFAULT (datetime('now')),
            UNIQUE(member_name, ticker, transaction_date, transaction_type)
        );

        CREATE TABLE IF NOT EXISTS raw_articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT,
            article_type    TEXT,
            title           TEXT,
            description     TEXT,
            url             TEXT,
            published_at    TEXT,
            full_text       TEXT,
            raw_json        TEXT,
            collected_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _score_hawkish_dovish(text: str) -> Dict[str, float]:
    """Compute hawkish/dovish scores for text."""
    if not text:
        return {"hawkish_score": 0.0, "dovish_score": 0.0, "net_score": 0.0}

    lower_text = text.lower()
    words = lower_text.split()
    total_words = len(words) if words else 1

    hawkish_count = sum(
        lower_text.count(kw) for kw in _HAWKISH_KEYWORDS
    )
    dovish_count = sum(
        lower_text.count(kw) for kw in _DOVISH_KEYWORDS
    )

    hawkish_score = hawkish_count / total_words * 1000
    dovish_score = dovish_count / total_words * 1000
    net_score = hawkish_score - dovish_score

    return {
        "hawkish_score": round(hawkish_score, 4),
        "dovish_score": round(dovish_score, 4),
        "net_score": round(net_score, 4),
    }


def _extract_tickers(text: str) -> List[str]:
    """Extract likely ticker symbols from text (1-5 uppercase letters in context)."""
    pattern = r'\b([A-Z]{1,5})\b'
    candidates = re.findall(pattern, text)
    # Filter out common English words that look like tickers
    _STOP = {
        "I", "A", "AN", "THE", "IN", "OF", "AND", "OR", "AT", "BY", "TO",
        "IS", "IT", "AS", "ON", "BE", "US", "UK", "EU", "FED", "CEO", "CFO",
        "CTO", "GDP", "IPO", "IMF", "ECB", "SEC", "FDA", "AI", "EV",
    }
    return [c for c in candidates if c not in _STOP]


def _extract_companies(text: str) -> List[str]:
    """Extract company-like mentions using basic capitalization patterns."""
    pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:\s+(?:Inc|Corp|Ltd|LLC|Co|Group|Holdings|Technologies|Energy|Capital|Partners))?)\b'
    matches = re.findall(pattern, text)
    return list(set(m for m in matches if len(m) > 3))[:20]


def _infer_sector_from_text(text: str) -> str:
    """Return the most likely sector based on keyword presence."""
    lower = text.lower()
    best_sector = "general"
    best_count = 0
    for sector, keywords in _SECTOR_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw in lower)
        if count > best_count:
            best_count = count
            best_sector = sector
    return best_sector


# ── RSS parsing helper ────────────────────────────────────────────────────────

def _parse_rss(url: str, timeout: int = 20) -> List[Dict[str, str]]:
    """Fetch and parse an RSS feed. Returns list of item dicts."""
    try:
        headers = {"User-Agent": "quant-fund-research/1.0"}
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)

        items = []
        for item in root.iter("item"):
            entry: Dict[str, str] = {}
            for child in item:
                tag = child.tag.split("}")[-1]  # strip namespace
                entry[tag] = (child.text or "").strip()
            if entry:
                items.append(entry)
        return items
    except Exception as exc:
        logger.warning("RSS fetch failed for %s: %s", url, exc)
        return []


# ── News API helper ───────────────────────────────────────────────────────────

_NEWS_API_BASE = "https://newsapi.org/v2/everything"
_NEWS_API_CACHE: Dict[str, Any] = {}  # Simple in-memory cache keyed by query
_NEWS_API_LAST_CALL: Dict[str, str] = {}  # query -> date of last call


def _fetch_news(
    api_key: str,
    query: str,
    days_back: int = 7,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    Fetch News API articles. Respects 100/day free limit by caching results
    per query per calendar day.
    """
    if not api_key:
        logger.warning("News API key not configured — skipping query: %s", query)
        return []

    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"{query}::{today}"

    if cache_key in _NEWS_API_CACHE:
        logger.debug("News API cache hit for query: %s", query)
        return _NEWS_API_CACHE[cache_key]

    from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    params = {
        "q": query,
        "apiKey": api_key,
        "from": from_date,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": min(page_size, 100),
    }
    try:
        resp = requests.get(_NEWS_API_BASE, params=params, timeout=20)
        if resp.status_code == 429:
            logger.warning("News API rate limit hit for query '%s' — caching empty result until tomorrow.", query)
            _NEWS_API_CACHE[cache_key] = []
            return []
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        _NEWS_API_CACHE[cache_key] = articles
        return articles
    except Exception as exc:
        logger.error("News API fetch failed for '%s': %s", query, exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# FedSpeechCollector
# ═══════════════════════════════════════════════════════════════════════════════

class FedSpeechCollector:
    """
    Fetches Federal Reserve speeches from public RSS feeds.
    Scores each speech for hawkish/dovish language.
    Tracks week-over-week change in net_score.
    """

    FED_RSS_FEEDS = [
        "https://www.federalreserve.gov/feeds/speeches.xml",
        "https://www.newyorkfed.org/newsevents/speeches.rss",
    ]

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._news_api_key: str = config.get("api_keys", {}).get("news_api", "")

    def _get_full_text(self, url: str) -> str:
        """Attempt to fetch the full text of a speech page."""
        if not url:
            return ""
        try:
            headers = {"User-Agent": "quant-fund-research/1.0"}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            # Strip HTML tags with a basic regex
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:20000]  # cap at 20k chars
        except Exception as exc:
            logger.debug("Could not fetch full text for %s: %s", url, exc)
            return ""

    def _parse_speaker_from_rss(self, item: Dict[str, str]) -> str:
        """Try to extract speaker name from RSS item."""
        for field in ("author", "dc:creator", "creator", "title"):
            val = item.get(field, "")
            if val and len(val) < 80:
                return val
        return "Unknown Fed Official"

    def collect(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Fetch all speeches from RSS feeds, score them, store permanently."""
        speeches = []

        for feed_url in self.FED_RSS_FEEDS:
            logger.info("Fetching Fed RSS: %s", feed_url)
            items = _parse_rss(feed_url)

            for item in items:
                try:
                    title = item.get("title", "")
                    url = item.get("link", item.get("guid", ""))
                    pub_date = item.get("pubDate", item.get("dc:date", ""))
                    speaker = self._parse_speaker_from_rss(item)
                    description = item.get("description", "")

                    # Parse date
                    statement_date = ""
                    statement_time = ""
                    if pub_date:
                        try:
                            # RFC 2822 format common in RSS
                            dt = datetime.strptime(pub_date[:25], "%a, %d %b %Y %H:%M:%S")
                            statement_date = dt.strftime("%Y-%m-%d")
                            statement_time = dt.strftime("%H:%M:%S")
                        except Exception:
                            statement_date = pub_date[:10]

                    # Fetch full text
                    full_text = self._get_full_text(url) or description

                    # Score
                    scores = _score_hawkish_dovish(full_text or title)
                    tickers = _extract_tickers(full_text)
                    companies = _extract_companies(full_text)

                    row = {
                        "person_name": speaker,
                        "role": "Federal Reserve Official",
                        "statement_date": statement_date,
                        "statement_time": statement_time,
                        "platform": "Federal Reserve Speech",
                        "full_text": full_text,
                        "url": url,
                        "source": feed_url,
                        "tickers_mentioned": json.dumps(tickers),
                        "companies_mentioned": json.dumps(companies),
                        "sentiment_score": 0.0,
                        "hawkish_dovish_score": scores["net_score"],
                        "quantitative_claims": json.dumps([]),
                        "market_impact_observed": None,
                        "was_statement_accurate": None,
                    }

                    try:
                        conn.execute(
                            """INSERT OR IGNORE INTO social_influence
                               (person_name, role, statement_date, statement_time,
                                platform, full_text, url, source, tickers_mentioned,
                                companies_mentioned, sentiment_score, hawkish_dovish_score,
                                quantitative_claims, market_impact_observed, was_statement_accurate)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                row["person_name"], row["role"], row["statement_date"],
                                row["statement_time"], row["platform"], row["full_text"],
                                row["url"], row["source"], row["tickers_mentioned"],
                                row["companies_mentioned"], row["sentiment_score"],
                                row["hawkish_dovish_score"], row["quantitative_claims"],
                                row["market_impact_observed"], row["was_statement_accurate"],
                            ),
                        )
                        conn.commit()
                    except Exception as db_exc:
                        logger.debug("DB insert skipped (duplicate?): %s", db_exc)

                    speeches.append(row)
                    logger.info(
                        "Fed speech: %s | %s | net_score=%.2f",
                        speaker, statement_date, scores["net_score"],
                    )

                except Exception as exc:
                    logger.error("Error processing Fed RSS item: %s", exc)

        return speeches

    def get_fed_sentiment(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Returns current hawkish_dovish_score, its 7-day average,
        and week-over-week change.
        """
        rows = conn.execute(
            """SELECT statement_date, hawkish_dovish_score
               FROM social_influence
               WHERE role='Federal Reserve Official'
               ORDER BY statement_date DESC
               LIMIT 20"""
        ).fetchall()

        if not rows:
            return {"current_score": None, "avg_7d": None, "wow_change": None, "trend": "NO_DATA"}

        today = datetime.utcnow().date()
        week_ago = today - timedelta(days=7)
        two_weeks_ago = today - timedelta(days=14)

        this_week = [r[1] for r in rows if r[0] >= str(week_ago)]
        last_week = [r[1] for r in rows if str(two_weeks_ago) <= r[0] < str(week_ago)]

        current_score = rows[0][1] if rows else None
        avg_7d = sum(this_week) / len(this_week) if this_week else None
        avg_prev_7d = sum(last_week) / len(last_week) if last_week else None

        wow_change = None
        if avg_7d is not None and avg_prev_7d is not None:
            wow_change = round(avg_7d - avg_prev_7d, 4)

        trend = "NEUTRAL"
        if avg_7d is not None:
            if avg_7d > 2.0:
                trend = "HAWKISH"
            elif avg_7d < -2.0:
                trend = "DOVISH"
            if wow_change is not None:
                if wow_change > 1.0:
                    trend += "_TIGHTENING"
                elif wow_change < -1.0:
                    trend += "_EASING"

        return {
            "current_score": current_score,
            "avg_7d": round(avg_7d, 4) if avg_7d is not None else None,
            "avg_prev_7d": round(avg_prev_7d, 4) if avg_prev_7d is not None else None,
            "wow_change": wow_change,
            "trend": trend,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CongressionalTradesMonitor
# ═══════════════════════════════════════════════════════════════════════════════

class CongressionalTradesMonitor:
    """
    Fetches congressional trades from QuiverQuant and House Stock Watcher.
    Cross-references with sector news to flag correlated trades.
    """

    QUIVERQUANT_BASE = "https://api.quiverquant.com/beta/historical/congresstrading"
    HOUSE_WATCHER_URL = (
        "https://house-stock-watcher-data.s3-us-east-2.amazonaws.com"
        "/data/all_transactions.json"
    )

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._news_api_key: str = config.get("api_keys", {}).get("news_api", "")
        self._trades_cache: List[Dict[str, Any]] = []

    def _fetch_house_watcher(self) -> List[Dict[str, Any]]:
        """Fetch bulk transaction data from House Stock Watcher S3."""
        try:
            headers = {"User-Agent": "quant-fund-research/1.0"}
            resp = requests.get(self.HOUSE_WATCHER_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("House Stock Watcher fetch failed: %s", exc)
            return []

    def _fetch_quiverquant(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch QuiverQuant congressional trading history for a ticker."""
        try:
            url = f"{self.QUIVERQUANT_BASE}/{ticker}"
            headers = {
                "User-Agent": "quant-fund-research/1.0",
                "Accept": "application/json",
            }
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code in (401, 403):
                logger.debug("QuiverQuant requires auth for %s — skipping", ticker)
                return []
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("QuiverQuant fetch failed for %s: %s", ticker, exc)
            return []

    def _infer_sector(self, ticker: str, description: str = "") -> str:
        combined = f"{ticker} {description}".lower()
        return _infer_sector_from_text(combined)

    def collect(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Fetch congressional trades and store permanently."""
        logger.info("Fetching congressional trades from House Stock Watcher...")
        raw_trades = self._fetch_house_watcher()

        stored = []
        for trade in raw_trades:
            try:
                member = trade.get("representative", trade.get("senator", "Unknown"))
                ticker = trade.get("ticker", "")
                txn_date = trade.get("transaction_date", "")
                txn_type = trade.get("type", trade.get("transaction_type", ""))
                amount = trade.get("amount", "")
                disclosure = trade.get("disclosure_date", "")
                description = trade.get("description", "")

                sector = self._infer_sector(ticker, description)

                row_data = {
                    "member_name": str(member),
                    "ticker": str(ticker),
                    "transaction_date": str(txn_date)[:10],
                    "transaction_type": str(txn_type),
                    "amount_range": str(amount),
                    "disclosure_date": str(disclosure)[:10],
                    "sector": sector,
                    "correlated_trade": 0,
                    "raw_json": json.dumps(trade),
                }

                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO raw_congressional_trades
                           (member_name, ticker, transaction_date, transaction_type,
                            amount_range, disclosure_date, sector, correlated_trade, raw_json)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            row_data["member_name"], row_data["ticker"],
                            row_data["transaction_date"], row_data["transaction_type"],
                            row_data["amount_range"], row_data["disclosure_date"],
                            row_data["sector"], row_data["correlated_trade"],
                            row_data["raw_json"],
                        ),
                    )
                except Exception as db_exc:
                    logger.debug("DB insert skipped: %s", db_exc)

                stored.append(row_data)
            except Exception as exc:
                logger.error("Error processing congressional trade: %s", exc)

        conn.commit()
        logger.info("Congressional trades stored: %d", len(stored))
        self._trades_cache = stored
        return stored

    def get_recent_trades(self, conn: sqlite3.Connection, days: int = 30) -> List[Dict[str, Any]]:
        """Returns list of recent congressional trades within the last N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """SELECT member_name, ticker, transaction_date, transaction_type,
                      amount_range, disclosure_date, sector, correlated_trade
               FROM raw_congressional_trades
               WHERE transaction_date >= ?
               ORDER BY transaction_date DESC""",
            (cutoff,),
        ).fetchall()

        return [
            {
                "member_name": r[0], "ticker": r[1], "transaction_date": r[2],
                "transaction_type": r[3], "amount_range": r[4],
                "disclosure_date": r[5], "sector": r[6], "correlated_trade": bool(r[7]),
            }
            for r in rows
        ]

    def get_correlated_trades(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Returns trades flagged as CORRELATED_TRADE (member statement + trade in same sector)."""
        rows = conn.execute(
            """SELECT member_name, ticker, transaction_date, transaction_type,
                      amount_range, sector
               FROM raw_congressional_trades
               WHERE correlated_trade=1
               ORDER BY transaction_date DESC"""
        ).fetchall()

        return [
            {
                "member_name": r[0], "ticker": r[1], "transaction_date": r[2],
                "transaction_type": r[3], "amount_range": r[4], "sector": r[5],
            }
            for r in rows
        ]

    def flag_correlated_trades(self, conn: sqlite3.Connection) -> int:
        """
        Cross-reference trades with social_influence table.
        Flag trades where same member made a statement about same sector within 30 days.
        Returns count of newly flagged trades.
        """
        trades = conn.execute(
            """SELECT id, member_name, ticker, transaction_date, sector
               FROM raw_congressional_trades WHERE correlated_trade=0"""
        ).fetchall()

        flagged_count = 0
        for trade_id, member, ticker, txn_date, sector in trades:
            if not txn_date:
                continue
            try:
                txn_dt = datetime.strptime(txn_date[:10], "%Y-%m-%d")
                window_start = (txn_dt - timedelta(days=30)).strftime("%Y-%m-%d")
                window_end = txn_date[:10]

                # Check if member mentioned in social_influence in same window
                match = conn.execute(
                    """SELECT COUNT(*) FROM social_influence
                       WHERE person_name LIKE ?
                       AND statement_date BETWEEN ? AND ?""",
                    (f"%{member.split()[-1]}%", window_start, window_end),
                ).fetchone()

                if match and match[0] > 0:
                    conn.execute(
                        "UPDATE raw_congressional_trades SET correlated_trade=1 WHERE id=?",
                        (trade_id,),
                    )
                    flagged_count += 1
            except Exception as exc:
                logger.debug("Correlation check failed for trade %s: %s", trade_id, exc)

        conn.commit()
        logger.info("Flagged %d correlated congressional trades", flagged_count)
        return flagged_count


# ═══════════════════════════════════════════════════════════════════════════════
# CEOStatementTracker
# ═══════════════════════════════════════════════════════════════════════════════

class CEOStatementTracker:
    """
    Monitors News API for CEO forward-looking statements.
    Extracts quantitative claims and scores CEO credibility over time.
    """

    _SEARCH_QUERIES = [
        '"CEO said"',
        '"Chief Executive"',
        '"management guidance"',
        '"earnings call"',
        '"investor day"',
    ]

    # Regex patterns for quantitative claims
    _REVENUE_PATTERN = re.compile(r'revenue.{0,30}(\$[\d.]+[BMK]|\d+%)', re.IGNORECASE)
    _GROWTH_PATTERN = re.compile(r'grow.{0,20}(\d+)%', re.IGNORECASE)
    _VOLUME_PATTERN = re.compile(
        r'ship.{0,20}(\d[\d,]+)\s*(units|vehicles|devices)', re.IGNORECASE
    )

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._news_api_key: str = config.get("api_keys", {}).get("news_api", "")

    def _extract_claims(self, text: str) -> List[Dict[str, str]]:
        """Extract all quantitative forward-looking claims from text."""
        claims = []
        if not text:
            return claims

        for match in self._REVENUE_PATTERN.finditer(text):
            claims.append({
                "claim_type": "revenue",
                "claim_value": match.group(1),
                "claim_text": text[max(0, match.start()-30):match.end()+30].strip(),
            })

        for match in self._GROWTH_PATTERN.finditer(text):
            claims.append({
                "claim_type": "growth",
                "claim_value": f"{match.group(1)}%",
                "claim_text": text[max(0, match.start()-30):match.end()+30].strip(),
            })

        for match in self._VOLUME_PATTERN.finditer(text):
            claims.append({
                "claim_type": "volume",
                "claim_value": f"{match.group(1)} {match.group(2)}",
                "claim_text": text[max(0, match.start()-30):match.end()+30].strip(),
            })

        return claims

    def _extract_timeframe(self, text: str) -> str:
        """Extract timeframe mention from claim context."""
        patterns = [
            r'(Q[1-4]\s*\d{4})',
            r'(fiscal\s+\d{4})',
            r'(full[- ]year\s+\d{4})',
            r'(next\s+(?:quarter|year|fiscal year))',
            r'(\d{4})',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(1)
        return "unspecified"

    def collect(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Fetch CEO statements from News API, extract claims, store permanently."""
        all_results = []

        for query in self._SEARCH_QUERIES:
            logger.info("Searching News API for: %s", query)
            articles = _fetch_news(self._news_api_key, query, days_back=7)

            for article in articles:
                try:
                    title = article.get("title", "") or ""
                    description = article.get("description", "") or ""
                    url = article.get("url", "")
                    published_at = article.get("publishedAt", "")[:10]
                    source_name = (article.get("source", {}) or {}).get("name", "")
                    content = article.get("content", "") or ""
                    full_text = f"{title}. {description}. {content}"

                    # Extract company from source/title
                    companies = _extract_companies(full_text)
                    company = companies[0] if companies else "Unknown"

                    # Extract claims
                    claims = self._extract_claims(full_text)
                    tickers = _extract_tickers(full_text)

                    # Sentiment (simple)
                    scores = _score_hawkish_dovish(full_text)

                    # Store in social_influence
                    try:
                        conn.execute(
                            """INSERT OR IGNORE INTO social_influence
                               (person_name, role, statement_date, platform,
                                full_text, url, source, tickers_mentioned,
                                companies_mentioned, sentiment_score,
                                hawkish_dovish_score, quantitative_claims)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                f"CEO of {company}", "CEO",
                                published_at, "News/Earnings Call",
                                full_text, url, source_name,
                                json.dumps(tickers), json.dumps(companies),
                                scores["net_score"], 0.0,
                                json.dumps([c["claim_text"] for c in claims]),
                            ),
                        )
                    except Exception as db_exc:
                        logger.debug("Social influence insert skipped: %s", db_exc)

                    # Store each claim in ceo_credibility
                    for claim in claims:
                        timeframe = self._extract_timeframe(
                            claim.get("claim_text", "")
                        )
                        try:
                            conn.execute(
                                """INSERT INTO ceo_credibility
                                   (company, claim_text, claim_type, claim_value,
                                    timeframe, source, claim_date)
                                   VALUES (?,?,?,?,?,?,?)""",
                                (
                                    company,
                                    claim["claim_text"],
                                    claim["claim_type"],
                                    claim["claim_value"],
                                    timeframe,
                                    source_name,
                                    published_at,
                                ),
                            )
                        except Exception as db_exc:
                            logger.debug("CEO credibility insert skipped: %s", db_exc)

                    all_results.append({
                        "company": company,
                        "url": url,
                        "published_at": published_at,
                        "claims_count": len(claims),
                        "tickers": tickers,
                    })

                except Exception as exc:
                    logger.error("Error processing CEO article: %s", exc)

            conn.commit()

        logger.info("CEO statements collected: %d articles processed", len(all_results))
        return all_results

    def get_credibility_score(self, conn: sqlite3.Connection, company: str) -> Optional[float]:
        """
        Returns CEO credibility score for a company.
        credibility_score = sum(was_accurate) / total_claims where was_accurate is set.
        """
        rows = conn.execute(
            """SELECT was_accurate FROM ceo_credibility
               WHERE company=? AND was_accurate IS NOT NULL""",
            (company,),
        ).fetchall()

        if not rows:
            return None

        accurate_count = sum(1 for r in rows if r[0] == 1)
        return round(accurate_count / len(rows), 4)

    def get_ceo_signals(self, conn: sqlite3.Connection, ticker: str) -> Dict[str, Any]:
        """Returns recent CEO statements and credibility score for a ticker."""
        rows = conn.execute(
            """SELECT company, claim_text, claim_type, claim_value, claim_date, credibility_score
               FROM ceo_credibility
               WHERE company LIKE ? OR (SELECT tickers_mentioned FROM social_influence
                    WHERE social_influence.url = ceo_credibility.source LIMIT 1) LIKE ?
               ORDER BY claim_date DESC LIMIT 10""",
            (f"%{ticker}%", f"%{ticker}%"),
        ).fetchall()

        # Also search social_influence for ticker mentions
        si_rows = conn.execute(
            """SELECT person_name, statement_date, hawkish_dovish_score, tickers_mentioned
               FROM social_influence
               WHERE role='CEO' AND tickers_mentioned LIKE ?
               ORDER BY statement_date DESC LIMIT 5""",
            (f"%{ticker}%",),
        ).fetchall()

        # Compute credibility from any company matching the ticker
        credibility = None
        if rows:
            company = rows[0][0]
            credibility = self.get_credibility_score(conn, company)

        accuracy_adjustment = None
        if credibility is not None:
            accuracy_adjustment = round(0.7 + 0.6 * credibility, 4)

        return {
            "ticker": ticker,
            "recent_claims": [
                {
                    "claim_text": r[1], "claim_type": r[2],
                    "claim_value": r[3], "claim_date": r[4],
                }
                for r in rows[:5]
            ],
            "credibility_score": credibility,
            "ceo_accuracy_adjustment": accuracy_adjustment,
            "recent_statements": len(si_rows),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# MuskMonitor
# ═══════════════════════════════════════════════════════════════════════════════

class MuskMonitor:
    """
    Monitors news for Elon Musk mentions and extracts company/ticker references.
    General influencer monitor — easily extensible to other names.
    """

    _INFLUENCERS = [
        {"name": "Elon Musk", "queries": ['"Elon Musk"', '"Musk said"', '"Tesla CEO"'], "role": "Influencer/CEO"},
    ]

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._news_api_key: str = config.get("api_keys", {}).get("news_api", "")

    def collect(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Fetch and store all influencer mentions."""
        all_results = []

        for influencer in self._INFLUENCERS:
            name = influencer["name"]
            role = influencer["role"]
            logger.info("Monitoring influencer: %s", name)

            for query in influencer["queries"]:
                articles = _fetch_news(self._news_api_key, query, days_back=7)

                for article in articles:
                    try:
                        title = article.get("title", "") or ""
                        description = article.get("description", "") or ""
                        url = article.get("url", "")
                        published_at = article.get("publishedAt", "")
                        source_name = (article.get("source", {}) or {}).get("name", "")
                        content = article.get("content", "") or ""
                        full_text = f"{title}. {description}. {content}"

                        tickers = _extract_tickers(full_text)
                        companies = _extract_companies(full_text)
                        scores = _score_hawkish_dovish(full_text)

                        statement_date = published_at[:10] if published_at else ""
                        statement_time = published_at[11:19] if len(published_at) > 10 else ""

                        try:
                            conn.execute(
                                """INSERT OR IGNORE INTO social_influence
                                   (person_name, role, statement_date, statement_time,
                                    platform, full_text, url, source, tickers_mentioned,
                                    companies_mentioned, sentiment_score, hawkish_dovish_score,
                                    quantitative_claims)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (
                                    name, role, statement_date, statement_time,
                                    "News", full_text, url, source_name,
                                    json.dumps(tickers), json.dumps(companies),
                                    scores["net_score"], 0.0, json.dumps([]),
                                ),
                            )
                        except Exception as db_exc:
                            logger.debug("Musk mention insert skipped: %s", db_exc)

                        all_results.append({
                            "person_name": name,
                            "statement_date": statement_date,
                            "url": url,
                            "tickers": tickers,
                            "companies": companies[:5],
                        })

                    except Exception as exc:
                        logger.error("Error processing influencer article: %s", exc)

            conn.commit()

        logger.info("Influencer mentions stored: %d", len(all_results))
        return all_results

    def get_recent_mentions(
        self, conn: sqlite3.Connection, person_name: str = "Elon Musk", days: int = 7
    ) -> List[Dict[str, Any]]:
        """Returns recent mentions for an influencer with context."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """SELECT statement_date, statement_time, platform, url,
                      tickers_mentioned, companies_mentioned, full_text
               FROM social_influence
               WHERE person_name=? AND statement_date >= ?
               ORDER BY statement_date DESC, statement_time DESC""",
            (person_name, cutoff),
        ).fetchall()

        return [
            {
                "statement_date": r[0],
                "statement_time": r[1],
                "platform": r[2],
                "url": r[3],
                "tickers_mentioned": json.loads(r[4] or "[]"),
                "companies_mentioned": json.loads(r[5] or "[]"),
                "snippet": (r[6] or "")[:200],
            }
            for r in rows
        ]

    def get_impact_signal(
        self, conn: sqlite3.Connection, ticker: str, person_name: str = "Elon Musk"
    ) -> Optional[Dict[str, Any]]:
        """
        If ticker was mentioned by the influencer in the last 48 hours,
        return a signal dict; otherwise None.
        """
        cutoff = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute(
            """SELECT statement_date, statement_time, url, tickers_mentioned, sentiment_score
               FROM social_influence
               WHERE person_name=? AND tickers_mentioned LIKE ?
               AND datetime(statement_date || ' ' || COALESCE(statement_time,'00:00:00')) >= ?
               ORDER BY statement_date DESC, statement_time DESC
               LIMIT 5""",
            (person_name, f"%{ticker}%", cutoff),
        ).fetchall()

        if not rows:
            return None

        return {
            "ticker": ticker,
            "influencer": person_name,
            "mention_count_48h": len(rows),
            "latest_date": rows[0][0],
            "latest_url": rows[0][2],
            "signal_strength": min(1.0, len(rows) * 0.2),
            "direction": "POSITIVE" if (rows[0][4] or 0) >= 0 else "NEGATIVE",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SocialInfluenceTracker  (main orchestrator)
# ═══════════════════════════════════════════════════════════════════════════════

class SocialInfluenceTracker:
    """
    Main orchestrator. Runs all sub-collectors and provides unified query interface.
    """

    def __init__(self):
        self._config = _load_config()
        self._fed_collector = FedSpeechCollector(self._config)
        self._congress_monitor = CongressionalTradesMonitor(self._config)
        self._ceo_tracker = CEOStatementTracker(self._config)
        self._musk_monitor = MuskMonitor(self._config)

    def collect(self) -> Dict[str, Any]:
        """
        Runs all collectors, stores everything permanently.
        Returns summary dict.
        """
        summary: Dict[str, Any] = {
            "fed_speeches": 0,
            "congressional_trades": 0,
            "ceo_statements": 0,
            "influencer_mentions": 0,
            "errors": [],
            "collected_at": datetime.utcnow().isoformat(),
        }

        conn = _get_conn()
        try:
            _init_db(conn)

            # Fed speeches
            try:
                fed_speeches = self._fed_collector.collect(conn)
                summary["fed_speeches"] = len(fed_speeches)
                logger.info("Fed speeches collected: %d", len(fed_speeches))
            except Exception as exc:
                logger.error("FedSpeechCollector failed: %s", exc)
                summary["errors"].append(f"fed_speeches: {exc}")

            # Congressional trades
            try:
                trades = self._congress_monitor.collect(conn)
                summary["congressional_trades"] = len(trades)
                correlated = self._congress_monitor.flag_correlated_trades(conn)
                summary["correlated_trades_flagged"] = correlated
            except Exception as exc:
                logger.error("CongressionalTradesMonitor failed: %s", exc)
                summary["errors"].append(f"congressional_trades: {exc}")

            # CEO statements
            try:
                ceo_results = self._ceo_tracker.collect(conn)
                summary["ceo_statements"] = len(ceo_results)
            except Exception as exc:
                logger.error("CEOStatementTracker failed: %s", exc)
                summary["errors"].append(f"ceo_statements: {exc}")

            # Musk / influencer monitor
            try:
                musk_results = self._musk_monitor.collect(conn)
                summary["influencer_mentions"] = len(musk_results)
            except Exception as exc:
                logger.error("MuskMonitor failed: %s", exc)
                summary["errors"].append(f"influencer_mentions: {exc}")

            # Fed sentiment summary
            try:
                summary["fed_sentiment"] = self._fed_collector.get_fed_sentiment(conn)
            except Exception as exc:
                logger.error("Could not compute Fed sentiment: %s", exc)

        finally:
            conn.close()

        return summary

    def get_fed_sentiment(self) -> Dict[str, Any]:
        """Returns current hawkish_dovish_score and trend."""
        conn = _get_conn()
        try:
            _init_db(conn)
            return self._fed_collector.get_fed_sentiment(conn)
        finally:
            conn.close()

    def get_congressional_signals(self, ticker: str) -> Dict[str, Any]:
        """Returns recent congressional trades for a ticker."""
        conn = _get_conn()
        try:
            _init_db(conn)
            recent = self._congress_monitor.get_recent_trades(conn, days=30)
            ticker_trades = [t for t in recent if t.get("ticker", "").upper() == ticker.upper()]
            correlated = [t for t in ticker_trades if t.get("correlated_trade")]
            return {
                "ticker": ticker,
                "recent_trades_30d": ticker_trades,
                "correlated_trades": correlated,
                "total_trades": len(ticker_trades),
                "buy_count": sum(1 for t in ticker_trades if "purchase" in (t.get("transaction_type") or "").lower()),
                "sell_count": sum(1 for t in ticker_trades if "sale" in (t.get("transaction_type") or "").lower()),
            }
        finally:
            conn.close()

    def get_ceo_signals(self, ticker: str) -> Dict[str, Any]:
        """Returns recent CEO statements and credibility score for a ticker."""
        conn = _get_conn()
        try:
            _init_db(conn)
            return self._ceo_tracker.get_ceo_signals(conn, ticker)
        finally:
            conn.close()

    def get_musk_signals(self) -> Dict[str, Any]:
        """Returns any recent Musk mentions."""
        conn = _get_conn()
        try:
            _init_db(conn)
            mentions = self._musk_monitor.get_recent_mentions(conn, "Elon Musk", days=7)
            return {
                "influencer": "Elon Musk",
                "mentions_7d": len(mentions),
                "recent_mentions": mentions[:10],
                "tickers_mentioned": list(
                    set(t for m in mentions for t in m.get("tickers_mentioned", []))
                ),
            }
        finally:
            conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint

    tracker = SocialInfluenceTracker()

    print("\n" + "=" * 70)
    print("SOCIAL INFLUENCE TRACKER")
    print("=" * 70)

    summary = tracker.collect()
    print("\n--- Collection Summary ---")
    pprint.pprint({k: v for k, v in summary.items() if k != "errors"})

    if summary.get("errors"):
        print(f"\n--- {len(summary['errors'])} Errors ---")
        for e in summary["errors"][:10]:
            print(f"  {e}")

    print("\n--- Fed Sentiment ---")
    fed = tracker.get_fed_sentiment()
    pprint.pprint(fed)

    print("\n--- Musk Signals (last 7 days) ---")
    musk = tracker.get_musk_signals()
    print(f"  Mentions: {musk['mentions_7d']}")
    print(f"  Tickers referenced: {musk['tickers_mentioned'][:10]}")

    print(f"\nPermanent archive: {_PERM_DB}")
    print("=" * 70)
