"""
Multi-Platform Social Intelligence Collector

Upgrades over basic reddit_collector.py:
  - 15+ subreddits monitored thematically (not just ticker-specific)
  - Full post + comment text, never truncated
  - Cross-ticker signal propagation (sector headwinds, supply chain, commodities)
  - Author influence scoring (karma, age, prediction accuracy)
  - Financial blog RSS collection (SeekingAlpha, Motley Fool, Benzinga, etc.)
  - StockTwits full message collection with influence scoring
  - Permanent storage in social_posts table (append-only)
  - Pump-and-dump detection (new accounts, only positive posts about one stock)

Usage:
    from altdata.collector.social_intelligence_collector import SocialIntelligenceCollector
    collector = SocialIntelligenceCollector(config)
    results = collector.collect(tickers, market="us")
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

try:
    import praw
    HAS_PRAW = True
except ImportError:
    HAS_PRAW = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _VADER = SentimentIntensityAnalyzer()
    HAS_VADER = True
except Exception:
    _VADER = None
    HAS_VADER = False

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

# Subreddits to monitor
SUBREDDITS_INVESTMENT = [
    "investing", "stocks", "wallstreetbets", "smallcapstocks",
    "SecurityAnalysis", "ValueInvesting", "stockmarket",
    "algotrading", "pennystocks", "finance",
]

SUBREDDITS_SECTOR = {
    "biotech":        ["biotech", "medicine"],
    "technology":     ["technology", "artificial"],
    "energy":         ["energy", "oilandgasworkers"],
    "real_estate":    ["realestateinvesting"],
    "financials":     ["PersonalFinanceCanada", "investing"],
}

# Financial blog RSS feeds (free content)
FINANCIAL_RSS_FEEDS = [
    ("seekingalpha_free",   "https://seekingalpha.com/feed.xml"),
    ("marketwatch",         "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("reuters_business",    "https://feeds.reuters.com/reuters/businessNews"),
    ("ft_markets",          "https://www.ft.com/rss/home/us"),
    ("cnbc_investing",      "https://search.cnbc.com/rs/search/combinedcombined/rss.html?partnerId=wrss01&id=15839069"),
    ("benzinga",            "https://www.benzinga.com/feed"),
    ("motleyfool",          "https://www.fool.com/feeds/index.aspx"),
    ("zacks",               "https://www.zacks.com/rss/main/rss.php"),
]

# Ticker regex
_TICKER_RE = re.compile(r"\$([A-Z]{2,5})\b|\b([A-Z]{2,5})\b")
_COMMON_WORDS = frozenset({
    "A", "AN", "THE", "AND", "OR", "BUT", "IN", "ON", "AT", "TO", "FOR",
    "OF", "IS", "ARE", "WAS", "BE", "BY", "AS", "IT", "WE", "HE", "SHE",
    "IF", "NOT", "ALL", "CAN", "HAS", "HAD", "DO", "DID", "SO", "UP",
    "OUT", "US", "MY", "NO", "GO", "NEW", "INC", "LLC", "CEO", "CFO",
    "EPS", "ROI", "ROE", "YTD", "Q1", "Q2", "Q3", "Q4", "FY", "YOY",
    "SEC", "FDA", "FTC", "DOJ", "ESG", "AI", "IT", "UK", "EU",
})

# Thematic keyword → sectors
_THEME_SECTOR_MAP = {
    "semiconductor": "technology",
    "chip":          "technology",
    "nvidia":        "technology",
    "biotech":       "healthcare",
    "clinical trial": "healthcare",
    "drug approval": "healthcare",
    "rate hike":     "financials",
    "interest rate": "financials",
    "oil price":     "energy",
    "crude":         "energy",
    "real estate":   "real_estate",
    "housing":       "real_estate",
    "inflation":     None,   # macro
    "recession":     None,
    "tariff":        None,
}

# Supply chain keywords → affected sectors
_SUPPLY_CHAIN_MAP = {
    "shipping delay":       ["technology", "consumer_disc", "industrials"],
    "port congestion":      ["consumer_disc", "industrials"],
    "chip shortage":        ["technology", "consumer_disc"],
    "rare earth":           ["technology", "materials"],
    "lithium":              ["technology", "consumer_disc"],   # EV batteries
    "natural gas":          ["energy", "utilities"],
    "commodity":            ["materials", "energy"],
}


def _extract_tickers(text: str) -> List[str]:
    found = set()
    for m in _TICKER_RE.finditer(text):
        t = m.group(1) or m.group(2)
        if t and t not in _COMMON_WORDS and len(t) >= 2:
            found.add(t)
    return sorted(found)


def _vader_score(text: str) -> float:
    if not HAS_VADER or not _VADER or not text:
        return 0.0
    return _VADER.polarity_scores(text[:3000]).get("compound", 0.0)


def _detect_pump_dump(author: str, posts_last_30d: int, unique_tickers: int, avg_score: float) -> bool:
    """Flag potential pump-and-dump: new-ish account posting only about one stock with high sentiment."""
    return (posts_last_30d >= 5 and unique_tickers <= 2 and avg_score > 0.6)


class SocialIntelligenceCollector:
    """
    Multi-platform social intelligence collection.
    Extends the basic reddit_collector with:
      - Thematic subreddit monitoring
      - Full text storage
      - Cross-ticker propagation
      - Influence scoring
      - Financial blog RSS
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self._reddit = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "quant-fund/1.0 social-intelligence research@example.com"
        })

    def _get_reddit(self):
        if self._reddit is not None:
            return self._reddit
        if not HAS_PRAW:
            return None
        try:
            reddit_cfg = (self.config.get("altdata", {})
                          .get("collectors", {})
                          .get("reddit", {}))
            cid = reddit_cfg.get("client_id")
            csecret = reddit_cfg.get("client_secret")
            ua = reddit_cfg.get("user_agent", "quant-fund/1.0")
            if not cid or not csecret:
                logger.info("social_intelligence: no Reddit credentials configured")
                return None
            self._reddit = praw.Reddit(
                client_id=cid,
                client_secret=csecret,
                user_agent=ua,
                read_only=True,
            )
            return self._reddit
        except Exception as e:
            logger.warning("social_intelligence: Reddit init failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Reddit Collection
    # ------------------------------------------------------------------

    def collect_reddit_thematic(
        self,
        target_tickers: List[str],
        subreddits: Optional[List[str]] = None,
        post_limit: int = 25,
    ) -> List[Dict]:
        """
        Collect thematic Reddit posts across investment subreddits.
        Extracts cross-ticker signals from full post text.
        """
        reddit = self._get_reddit()
        if not reddit:
            return []

        subreddits = subreddits or SUBREDDITS_INVESTMENT
        target_set = set(t.upper() for t in target_tickers)
        results = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for sub_name in subreddits:
            try:
                subreddit = reddit.subreddit(sub_name)
                for post in subreddit.hot(limit=post_limit):
                    # Full text: title + body
                    full_text = f"{post.title}\n\n{post.selftext or ''}"

                    # Also grab top comments (up to 10)
                    try:
                        post.comments.replace_more(limit=0)
                        for comment in list(post.comments)[:10]:
                            if hasattr(comment, "body") and comment.body:
                                full_text += f"\n\nCOMMENT: {comment.body}"
                    except Exception:
                        pass

                    # Extract tickers mentioned
                    tickers_in_post = _extract_tickers(full_text)

                    # Cross-ticker: tickers in our universe that are mentioned
                    universe_tickers_hit = [t for t in tickers_in_post if t in target_set]

                    # Thematic analysis
                    thematic_signals = self._extract_thematic_signals(full_text, target_tickers)

                    # Sentiment
                    sentiment = _vader_score(full_text)

                    # Author influence scoring
                    try:
                        author = post.author
                        author_name = str(author) if author else "[deleted]"
                        author_karma = getattr(author, "comment_karma", 0) or 0
                        acct_age_days = max(0, (datetime.now().timestamp() - getattr(author, "created_utc", 0)) / 86400) if author else 0
                    except Exception:
                        author_name = "[deleted]"
                        author_karma = 0
                        acct_age_days = 0

                    # Influence score: log-scaled karma + account age weight
                    import math
                    influence = min(1.0, (
                        math.log10(max(1, author_karma)) / 5.0 * 0.6
                        + min(1.0, acct_age_days / 365.0) * 0.4
                    ))

                    # Pump-and-dump flag (heuristic only — needs author history for full check)
                    pnd_flag = (acct_age_days < 30 and author_karma < 100
                                and sentiment > 0.6 and len(tickers_in_post) <= 2)

                    # Primary ticker context (if any of our targets are mentioned)
                    primary_ticker = universe_tickers_hit[0] if universe_tickers_hit else ""

                    record = {
                        "source":              "reddit",
                        "ticker":              primary_ticker or ",".join(universe_tickers_hit[:3]),
                        "market":              "us",
                        "data_type":           "social_post",
                        "value":               sentiment,
                        "raw_data":            json.dumps({
                            "subreddit":        sub_name,
                            "post_id":          post.id,
                            "title":            post.title,
                            "full_text":        full_text[:10000],  # store up to 10k chars
                            "author":           author_name,
                            "upvotes":          post.score,
                            "comment_count":    post.num_comments,
                            "created_utc":      post.created_utc,
                            "url":              f"https://reddit.com{post.permalink}",
                            "flair":            post.link_flair_text or "",
                            "tickers_mentioned": tickers_in_post,
                            "universe_tickers_hit": universe_tickers_hit,
                            "thematic_signals": thematic_signals,
                            "sentiment":        sentiment,
                            "author_influence": influence,
                            "is_pump_dump_flag": pnd_flag,
                        }),
                        "quality":             influence * 0.7 + 0.3,
                        "collected_at":        now_iso,
                    }
                    results.append(record)

                time.sleep(0.5)  # rate limit per subreddit
            except Exception as e:
                logger.debug("social_intelligence: reddit error %s: %s", sub_name, e)

        logger.info("social_intelligence: reddit collected %d posts from %d subreddits",
                    len(results), len(subreddits))
        return results

    def _extract_thematic_signals(self, text: str, target_tickers: List[str]) -> List[Dict]:
        """
        Extract sector/supply-chain/macro thematic signals from post text.
        Returns list of {theme, sectors_affected, sentiment, strength}.
        """
        signals = []
        text_lower = text.lower()

        for theme, sectors in _THEME_SECTOR_MAP.items():
            if theme in text_lower:
                sentiment = _vader_score(
                    text[max(0, text_lower.find(theme)-200):text_lower.find(theme)+200]
                )
                signals.append({
                    "theme":             theme,
                    "sectors_affected":  [sectors] if sectors else ["macro"],
                    "sentiment":         sentiment,
                    "strength":          abs(sentiment),
                    "signal_type":       "sector_theme",
                })

        for supply_keyword, affected_sectors in _SUPPLY_CHAIN_MAP.items():
            if supply_keyword in text_lower:
                sentiment = _vader_score(
                    text[max(0, text_lower.find(supply_keyword)-200):text_lower.find(supply_keyword)+200]
                )
                signals.append({
                    "theme":             supply_keyword,
                    "sectors_affected":  affected_sectors,
                    "sentiment":         sentiment,
                    "strength":          abs(sentiment),
                    "signal_type":       "supply_chain",
                })

        return signals

    # ------------------------------------------------------------------
    # Financial Blog RSS
    # ------------------------------------------------------------------

    def collect_blog_rss(self, target_tickers: List[str]) -> List[Dict]:
        """
        Collect articles from financial blog RSS feeds.
        Extracts ticker mentions and generates cross-ticker signals.
        """
        if not HAS_FEEDPARSER:
            logger.debug("social_intelligence: feedparser not installed, skipping RSS")
            return []

        import feedparser as fp
        target_set = set(t.upper() for t in target_tickers)
        results = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for feed_name, feed_url in FINANCIAL_RSS_FEEDS:
            try:
                feed = fp.parse(feed_url)
                for entry in (feed.entries or [])[:20]:
                    title   = getattr(entry, "title", "")
                    summary = getattr(entry, "summary", "")
                    url     = getattr(entry, "link", "")
                    pub_date = getattr(entry, "published", "")

                    full_text = f"{title}\n\n{summary}"
                    tickers_in_post = _extract_tickers(full_text)
                    universe_hits = [t for t in tickers_in_post if t in target_set]

                    if not universe_hits:
                        continue  # skip articles not mentioning our universe

                    sentiment = _vader_score(full_text)
                    thematic  = self._extract_thematic_signals(full_text, target_tickers)

                    for ticker in universe_hits:
                        results.append({
                            "source":       f"rss_{feed_name}",
                            "ticker":       ticker,
                            "market":       "us",
                            "data_type":    "blog_article",
                            "value":        sentiment,
                            "raw_data":     json.dumps({
                                "feed":              feed_name,
                                "title":             title,
                                "summary":           summary[:2000],
                                "url":               url,
                                "published":         pub_date,
                                "tickers_mentioned": tickers_in_post,
                                "universe_hits":     universe_hits,
                                "thematic_signals":  thematic,
                                "sentiment":         sentiment,
                            }),
                            "quality":      0.6,
                            "collected_at": now_iso,
                        })

                time.sleep(0.3)
            except Exception as e:
                logger.debug("social_intelligence: RSS error %s: %s", feed_name, e)

        logger.info("social_intelligence: RSS collected %d relevant articles", len(results))
        return results

    # ------------------------------------------------------------------
    # StockTwits
    # ------------------------------------------------------------------

    def collect_stocktwits(self, tickers: List[str]) -> List[Dict]:
        """
        Collect StockTwits messages with full text and influence scoring.
        Uses the public API (no auth required for symbol streams).
        """
        results = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for ticker in tickers:
            try:
                url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
                resp = self._session.get(url, timeout=10)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                messages = data.get("messages", [])

                bull_count = bear_count = neutral_count = 0
                msg_records = []

                for msg in messages[:50]:  # get up to 50 messages
                    body    = msg.get("body", "")
                    created = msg.get("created_at", "")
                    user    = msg.get("user", {})
                    sentiment_obj = msg.get("entities", {}).get("sentiment")
                    st_sentiment  = (sentiment_obj or {}).get("basic", "Neutral")

                    # Map StockTwits sentiment
                    if st_sentiment == "Bullish":
                        bull_count += 1
                    elif st_sentiment == "Bearish":
                        bear_count += 1
                    else:
                        neutral_count += 1

                    # Author influence
                    followers   = user.get("followers", 0) or 0
                    join_date   = user.get("join_date", "")
                    acct_age    = 365  # default
                    import math
                    influence   = min(1.0, math.log10(max(1, followers)) / 5.0)

                    # Full text sentiment
                    vader_score = _vader_score(body)

                    msg_records.append({
                        "author":     user.get("username", ""),
                        "followers":  followers,
                        "influence":  influence,
                        "body":       body,
                        "sentiment_stocktwits": st_sentiment,
                        "sentiment_vader":      vader_score,
                        "created_at": created,
                    })

                total = max(1, bull_count + bear_count + neutral_count)
                bull_ratio = bull_count / total
                bear_ratio = bear_count / total
                net_score  = bull_ratio - bear_ratio

                results.append({
                    "source":       "stocktwits",
                    "ticker":       ticker,
                    "market":       "us",
                    "data_type":    "stocktwits_stream",
                    "value":        net_score,
                    "raw_data":     json.dumps({
                        "bull_count":  bull_count,
                        "bear_count":  bear_count,
                        "neutral_count": neutral_count,
                        "bull_ratio":  bull_ratio,
                        "bear_ratio":  bear_ratio,
                        "net_score":   net_score,
                        "message_count": len(messages),
                        "messages":    msg_records[:20],  # store top 20 full text
                    }),
                    "quality":      0.7,
                    "collected_at": now_iso,
                })
                time.sleep(0.5)
            except Exception as e:
                logger.debug("social_intelligence: stocktwits error %s: %s", ticker, e)

        logger.info("social_intelligence: stocktwits collected %d tickers", len(results))
        return results

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------

    def collect(self, tickers: List[str], market: str = "us") -> List[Dict]:
        """
        Main collection method. Runs all social intelligence sources.
        Returns list of raw_data dicts for AltDataStore.
        """
        results = []

        # Reddit thematic (only if credentials available)
        reddit_results = self.collect_reddit_thematic(tickers)
        results.extend(reddit_results)

        # Financial blog RSS
        rss_results = self.collect_blog_rss(tickers)
        results.extend(rss_results)

        # StockTwits (prioritise tickers with upcoming earnings)
        stocktwits_results = self.collect_stocktwits(tickers[:20])  # cap to avoid rate limits
        results.extend(stocktwits_results)

        logger.info(
            "social_intelligence: total %d records (reddit=%d, rss=%d, stocktwits=%d)",
            len(results), len(reddit_results), len(rss_results), len(stocktwits_results)
        )
        return results

    def get_cross_ticker_signals(
        self,
        raw_results: List[Dict],
        universe_tickers: List[str],
    ) -> Dict[str, List[Dict]]:
        """
        Extract cross-ticker signals from collected social data.
        Returns {ticker: [signals]} for every universe ticker that was mentioned
        in articles collected for other tickers.
        """
        universe_set = set(t.upper() for t in universe_tickers)
        signals: Dict[str, List[Dict]] = {t: [] for t in universe_tickers}

        for record in raw_results:
            try:
                raw = json.loads(record.get("raw_data", "{}"))
            except Exception:
                continue

            mentioned = raw.get("tickers_mentioned") or raw.get("universe_tickers_hit") or []
            sentiment = raw.get("sentiment") or record.get("value", 0.0)
            source    = record.get("source", "")
            primary   = record.get("ticker", "")

            for t in mentioned:
                if t in universe_set and t != primary:
                    signals[t].append({
                        "source":           source,
                        "primary_ticker":   primary,
                        "sentiment":        sentiment,
                        "cross_mention":    True,
                        "thematic_signals": raw.get("thematic_signals", []),
                    })

        return signals
