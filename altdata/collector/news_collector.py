"""
News alternative data collector.

Combines RSS feed scraping + Finnhub free-tier company news.
Computes composite sentiment (VADER + TextBlob + keyword scoring)
with source credibility weighting.
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

# ── optional deps ─────────────────────────────────────────────────────────────
try:
    import feedparser
    _FEEDPARSER_AVAILABLE = True
except ImportError:
    _FEEDPARSER_AVAILABLE = False
    logger.warning("feedparser not installed — RSS collection disabled")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _vader = SentimentIntensityAnalyzer()
    _VADER_AVAILABLE = True
except ImportError:
    _vader = None
    _VADER_AVAILABLE = False
    logger.warning("vaderSentiment not installed — VADER sentiment defaults to 0")

try:
    from textblob import TextBlob
    _TEXTBLOB_AVAILABLE = True
except ImportError:
    TextBlob = None
    _TEXTBLOB_AVAILABLE = False
    logger.warning("textblob not installed — TextBlob sentiment defaults to 0")

# ── constants ─────────────────────────────────────────────────────────────────

_TICKER_RE1 = re.compile(r'\b([A-Z]{2,5})\b')
_TICKER_RE2 = re.compile(r'\$([A-Z]{2,5})')

_LEADERSHIP_RE = re.compile(
    r'\b(CEO|CFO|Chairman|Chief Executive|Chief Financial)\b.{0,60}'
    r'\b(resign|resigns|resigned|appoint|appointed|replace|replaced|step down|steps down)\b',
    re.IGNORECASE,
)

_KEYWORD_SCORES: list = [
    # (pattern, score)
    (re.compile(r'\b(record earnings|beat expectations|raised guidance|buyback|dividend increase)\b', re.I), 1.0),
    (re.compile(r'\b(growth|expansion|profit|revenue increase|new contract)\b', re.I), 0.5),
    (re.compile(r'\b(miss|below expectations|loss|lawsuit|recall|investigation)\b', re.I), -0.5),
    (re.compile(r'\b(bankruptcy|fraud|SEC investigation|guidance cut|profit warning)\b', re.I), -1.0),
]

_SOURCE_CREDIBILITY: dict = {
    "ft.com": 1.0,
    "bbc": 0.9,
    "yahoo": 0.7,
}
_DEFAULT_CREDIBILITY = 0.6

_FINNHUB_BASE = "https://finnhub.io/api/v1/company-news"

# ── helpers ───────────────────────────────────────────────────────────────────

def _vader_compound(text: str) -> float:
    if not _VADER_AVAILABLE or not text:
        return 0.0
    return _vader.polarity_scores(text)["compound"]


def _textblob_polarity(text: str) -> float:
    if not _TEXTBLOB_AVAILABLE or not text:
        return 0.0
    try:
        return TextBlob(text).sentiment.polarity
    except Exception:
        return 0.0


def _keyword_score(text: str) -> float:
    if not text:
        return 0.0
    score = 0.0
    for pattern, weight in _KEYWORD_SCORES:
        if pattern.search(text):
            score += weight
    return max(-1.0, min(1.0, score))


def _credibility(url: str) -> float:
    url_lower = (url or "").lower()
    for domain, weight in _SOURCE_CREDIBILITY.items():
        if domain in url_lower:
            return weight
    return _DEFAULT_CREDIBILITY


def _extract_tickers(text: str, universe: set) -> set:
    found: set = set()
    for m in _TICKER_RE1.finditer(text):
        t = m.group(1)
        if t in universe:
            found.add(t)
    for m in _TICKER_RE2.finditer(text):
        t = m.group(1)
        if t in universe:
            found.add(t)
    return found


def _composite_score(title: str, summary: str) -> float:
    v = _vader_compound(title)
    tb = _textblob_polarity(summary)
    kw = _keyword_score(f"{title} {summary}")
    return v * 0.35 + tb * 0.25 + kw * 0.40


def _leadership_change(text: str) -> bool:
    return bool(_Leadership_RE_match(text))


def _Leadership_RE_match(text: str) -> bool:
    return bool(_LEADERSHIP_RE.search(text or ""))


def _parse_published(entry) -> datetime | None:
    """Try to extract a datetime from a feedparser entry."""
    try:
        import email.utils
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        if hasattr(entry, "updated_parsed") and entry.updated_parsed:
            return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _build_result(
    ticker: str,
    market: str,
    title: str,
    summary: str,
    url: str,
    published_dt: datetime | None,
    source_label: str,
    extra_raw: dict = None,
) -> dict:
    cred = _credibility(url)
    base_score = _composite_score(title, summary)
    final_score = base_score * cred
    final_score = max(-1.0, min(1.0, final_score))

    leadership_flag = _Leadership_RE_match(f"{title} {summary}")

    quality = 0.5
    if abs(base_score) > 0.3:
        quality += 0.2
    if leadership_flag:
        quality += 0.1
    quality = min(quality, 1.0)

    raw: dict = {
        "title": title[:300],
        "summary": (summary or "")[:500],
        "url": url,
        "source_label": source_label,
        "credibility_weight": cred,
        "vader_compound": _vader_compound(title),
        "textblob_polarity": _textblob_polarity(summary),
        "keyword_score": _keyword_score(f"{title} {summary}"),
        "composite_score": base_score,
        "leadership_change_flag": leadership_flag,
        "published": published_dt.isoformat() if published_dt else None,
    }
    if extra_raw:
        raw.update(extra_raw)

    return {
        "source": source_label,
        "ticker": ticker,
        "market": market,
        "data_type": "news_sentiment",
        "value": round(final_score, 6),
        "raw_data": raw,
        "timestamp": datetime.now().isoformat(),
        "quality_score": round(quality, 4),
    }


# ── RSS collection ─────────────────────────────────────────────────────────────

def _collect_rss(rss_feeds: list, universe: set, market: str) -> list:
    if not _FEEDPARSER_AVAILABLE:
        return []

    results: list = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    for feed_url in rss_feeds:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as exc:
            logger.warning("RSS parse error for %s: %s", feed_url, exc)
            continue

        for entry in feed.entries:
            title = getattr(entry, "title", "") or ""
            summary = getattr(entry, "summary", "") or ""
            link = getattr(entry, "link", "") or feed_url

            pub_dt = _parse_published(entry)
            if pub_dt and pub_dt < cutoff:
                continue

            full_text = f"{title} {summary}"
            matched = _extract_tickers(full_text, universe)
            if not matched:
                continue

            for ticker in matched:
                results.append(
                    _build_result(
                        ticker=ticker,
                        market=market,
                        title=title,
                        summary=summary,
                        url=link,
                        published_dt=pub_dt,
                        source_label=feed_url,
                    )
                )

    return results


# ── Finnhub collection ─────────────────────────────────────────────────────────

def _collect_finnhub(tickers: list, universe: set, market: str, api_key: str = "") -> list:
    results: list = []
    session = requests.Session()
    session.headers.update({"User-Agent": "quant-fund/1.0"})

    today = datetime.now(timezone.utc).date()
    from_date = (today - timedelta(days=7)).isoformat()
    to_date = today.isoformat()

    for ticker in tickers:
        time.sleep(0.5)
        params = {
            "symbol": ticker,
            "from": from_date,
            "to": to_date,
        }
        if api_key:
            params["token"] = api_key

        try:
            resp = session.get(_FINNHUB_BASE, params=params, timeout=10)
        except requests.RequestException as exc:
            logger.warning("finnhub: network error for %s: %s", ticker, exc)
            continue

        if resp.status_code == 429:
            logger.warning("finnhub: rate limited (429) for %s, skipping", ticker)
            continue

        if resp.status_code != 200:
            logger.warning("finnhub: HTTP %s for %s", resp.status_code, ticker)
            continue

        try:
            articles = resp.json()
        except Exception as exc:
            logger.warning("finnhub: JSON parse error for %s: %s", ticker, exc)
            continue

        if not isinstance(articles, list):
            continue

        for article in articles:
            headline = article.get("headline", "") or ""
            summary = article.get("summary", "") or ""
            url = article.get("url", "") or ""
            source = article.get("source", "finnhub")
            timestamp_unix = article.get("datetime")

            pub_dt = None
            if timestamp_unix:
                try:
                    pub_dt = datetime.fromtimestamp(int(timestamp_unix), tz=timezone.utc)
                except Exception:
                    pass

            results.append(
                _build_result(
                    ticker=ticker,
                    market=market,
                    title=headline,
                    summary=summary,
                    url=url,
                    published_dt=pub_dt,
                    source_label=f"finnhub/{source}",
                )
            )

    return results


# ── NewsAPI collection ─────────────────────────────────────────────────────────

def _collect_newsapi(tickers: list, universe: set, market: str, api_key: str = "") -> list:
    results: list = []
    session = requests.Session()
    session.headers.update({"User-Agent": "quant-fund/1.0"})

    for ticker in tickers:
        time.sleep(0.5)
        # fetch from newsapi.org/v2/everything?q=TICKER&apiKey=KEY
        url = (
            f"https://newsapi.org/v2/everything"
            f"?q={ticker}&language=en&sortBy=publishedAt&pageSize=20&apiKey={api_key}"
        )
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException as exc:
            logger.warning("newsapi: network error for %s: %s", ticker, exc)
            continue

        if resp.status_code == 429:
            logger.warning("newsapi: rate limited (429) for %s, skipping", ticker)
            continue
        if resp.status_code != 200:
            logger.debug("newsapi: HTTP %s for %s", resp.status_code, ticker)
            continue

        try:
            data = resp.json()
        except Exception as exc:
            logger.warning("newsapi: JSON parse error for %s: %s", ticker, exc)
            continue

        articles = data.get("articles", [])
        if not isinstance(articles, list):
            continue

        for article in articles:
            title   = article.get("title", "") or ""
            summary = article.get("description", "") or ""
            art_url = article.get("url", "") or ""
            source  = (article.get("source") or {}).get("name", "newsapi")
            pub_str = article.get("publishedAt", "")

            pub_dt = None
            if pub_str:
                try:
                    pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                except Exception:
                    pass

            results.append(
                _build_result(
                    ticker=ticker,
                    market=market,
                    title=title,
                    summary=summary,
                    url=art_url,
                    published_dt=pub_dt,
                    source_label=f"newsapi/{source}",
                )
            )

    return results


# ── main collector ─────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect news sentiment signals for the given tickers.

    Parameters
    ----------
    tickers : list of str
    market  : str
    config  : dict

    Returns
    -------
    list of result dicts with data_type="news_sentiment"
    """
    if config is None:
        config = {}

    news_cfg = (
        config
        .get("altdata", {})
        .get("collectors", {})
        .get("news", {})
    )

    rss_feeds: list = news_cfg.get("rss_feeds", [])
    # Read Finnhub key from top-level api_keys, fall back to legacy location
    finnhub_api_key: str = (config.get("api_keys") or {}).get("finnhub", "") \
        or news_cfg.get("finnhub_api_key", "")
    # Read NewsAPI key from top-level api_keys
    news_api_key: str = (config.get("api_keys") or {}).get("news_api", "")

    universe = set(tickers)
    results: list = []

    # ── RSS ───────────────────────────────────────────────────────────────────
    if rss_feeds:
        try:
            rss_results = _collect_rss(rss_feeds, universe, market)
            results.extend(rss_results)
            logger.info("news_collector RSS: %d articles", len(rss_results))
        except Exception as exc:
            logger.warning("news_collector RSS section failed: %s", exc)

    # ── Finnhub ───────────────────────────────────────────────────────────────
    try:
        fh_results = _collect_finnhub(tickers, universe, market, api_key=finnhub_api_key)
        results.extend(fh_results)
        logger.info("news_collector Finnhub: %d articles", len(fh_results))
    except Exception as exc:
        logger.warning("news_collector Finnhub section failed: %s", exc)

    # ── NewsAPI ───────────────────────────────────────────────────────────────
    if news_api_key:
        try:
            newsapi_results = _collect_newsapi(tickers, universe, market, api_key=news_api_key)
            results.extend(newsapi_results)
            logger.info("news_collector NewsAPI: %d articles", len(newsapi_results))
        except Exception as exc:
            logger.warning("news_collector NewsAPI section failed: %s", exc)

    logger.info("news_collector: returned %d total signals", len(results))
    return results


class NewsCollector:
    """Class wrapper around the module-level collect() function."""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def collect(self, tickers: list, market: str = 'US') -> list:
        return collect(tickers, market, self.config)
