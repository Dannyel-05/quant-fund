"""
Reddit alternative data collector.

Scans configured subreddits for ticker mentions, computes VADER sentiment,
upvote momentum, and detects possible coordination signals.
"""

import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Lazy imports so the module loads even if optional deps are missing
try:
    import praw
    _PRAW_AVAILABLE = True
except ImportError:
    _PRAW_AVAILABLE = False
    logger.warning("praw not installed — reddit_collector will be a no-op")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _vader = SentimentIntensityAnalyzer()
    _VADER_AVAILABLE = True
except ImportError:
    _vader = None
    _VADER_AVAILABLE = False
    logger.warning("vaderSentiment not installed — sentiment will default to 0")

# ── helpers ──────────────────────────────────────────────────────────────────

_TICKER_RE1 = re.compile(r'\b([A-Z]{2,5})\b')
_TICKER_RE2 = re.compile(r'\$([A-Z]{2,5})')

_CACHE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "cache", "reddit_seen.json"
)

_SEEN_IDS: set = set()
_SEEN_LOADED = False


def _load_seen() -> None:
    global _SEEN_IDS, _SEEN_LOADED
    if _SEEN_LOADED:
        return
    _SEEN_LOADED = True
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, "r") as fh:
                data = json.load(fh)
            _SEEN_IDS = set(data.get("seen", []))
    except Exception as exc:
        logger.warning("Could not load reddit_seen.json: %s", exc)
        _SEEN_IDS = set()


def _save_seen() -> None:
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        with open(_CACHE_FILE, "w") as fh:
            json.dump({"seen": list(_SEEN_IDS)}, fh)
    except Exception as exc:
        logger.warning("Could not save reddit_seen.json: %s", exc)


def _vader_compound(text: str) -> float:
    if not _VADER_AVAILABLE or not text:
        return 0.0
    return _vader.polarity_scores(text)["compound"]


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


def _tanh(x: float) -> float:
    import math
    return math.tanh(x)


def _quality_score(post) -> float:
    """Simple heuristic — capped at 1.0."""
    score = 0.5
    if getattr(post, "upvote_ratio", 0) > 0.8:
        score += 0.2
    if getattr(post, "num_comments", 0) > 50:
        score += 0.2
    if getattr(post, "score", 0) > 100:
        score += 0.1
    return min(score, 1.0)


# ── main collector ────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect Reddit sentiment signals for the given tickers.

    Parameters
    ----------
    tickers : list of str
        Ticker symbols to track.
    market  : str
        Market identifier (e.g. 'US', 'UK').
    config  : dict
        Full application config. Expected key path:
        config["altdata"]["collectors"]["reddit"]

    Returns
    -------
    list of dicts with keys:
        source, ticker, market, data_type, value, raw_data,
        timestamp, quality_score
    """
    if config is None:
        config = {}

    reddit_cfg = (
        config
        .get("altdata", {})
        .get("collectors", {})
        .get("reddit", {})
    )

    # Read Reddit credentials from top-level api_keys first, fall back to collector config
    client_id = (config.get("api_keys") or {}).get("reddit_client_id", "") \
        or reddit_cfg.get("client_id", "")
    client_secret = (config.get("api_keys") or {}).get("reddit_client_secret", "") \
        or reddit_cfg.get("client_secret", "")
    user_agent = reddit_cfg.get("user_agent", "quant-fund/1.0")
    subreddits = reddit_cfg.get("subreddits", ["wallstreetbets", "stocks", "investing"])
    post_limit = int(reddit_cfg.get("post_limit", 50))

    # ── setup check ──────────────────────────────────────────────────────────
    if not client_id:
        print(
            "\n[reddit_collector] First-run setup instructions:\n"
            "  1. Go to https://www.reddit.com/prefs/apps and create a 'script' app.\n"
            "  2. Copy the client_id (under the app name) and client_secret.\n"
            "  3. Add to your config:\n"
            "       altdata.collectors.reddit.client_id = '<your_id>'\n"
            "       altdata.collectors.reddit.client_secret = '<your_secret>'\n"
            "       altdata.collectors.reddit.user_agent = 'quant-fund/1.0 by u/<username>'\n"
        )
        return []

    if not _PRAW_AVAILABLE:
        logger.warning("praw not installed; cannot collect Reddit data")
        return []

    _load_seen()

    # ── connect ───────────────────────────────────────────────────────────────
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        # Verify credentials with a cheap call
        _ = reddit.user.me()
    except Exception as exc:
        logger.warning("Reddit auth failed: %s", exc)
        return []

    universe = set(tickers)
    results: list = []

    # post_time_map[ticker] -> list of created_utc floats (for coordination detection)
    post_time_map: dict = defaultdict(list)

    # Accumulate per-ticker data before building result dicts
    ticker_posts: dict = defaultdict(list)

    for sub_name in subreddits:
        try:
            subreddit = reddit.subreddit(sub_name)
            feeds = [
                subreddit.hot(limit=post_limit),
                subreddit.new(limit=post_limit),
            ]
            for feed in feeds:
                for post in feed:
                    if post.id in _SEEN_IDS:
                        continue
                    _SEEN_IDS.add(post.id)

                    full_text = f"{post.title} {post.selftext or ''}"
                    matched = _extract_tickers(full_text, universe)
                    if not matched:
                        continue

                    compound = _vader_compound(full_text)
                    upvote_ratio = getattr(post, "upvote_ratio", 0.5) or 0.5
                    num_comments = getattr(post, "num_comments", 0) or 0
                    created_utc = getattr(post, "created_utc", 0.0)
                    flair = getattr(post, "link_flair_text", "") or ""
                    is_dd = "DD" in flair.upper()

                    # RedditMomentumScore (before DD multiplier)
                    raw_score = upvote_ratio * compound * min(num_comments / 10.0, 1.0)
                    if is_dd:
                        raw_score *= 2.0

                    for ticker in matched:
                        post_time_map[ticker].append(created_utc)
                        ticker_posts[ticker].append({
                            "post_id": post.id,
                            "subreddit": sub_name,
                            "title": post.title[:200],
                            "vader_compound": compound,
                            "upvote_ratio": upvote_ratio,
                            "num_comments": num_comments,
                            "created_utc": created_utc,
                            "is_dd": is_dd,
                            "raw_score": raw_score,
                            "quality": _quality_score(post),
                        })
        except Exception as exc:
            logger.warning("Error scanning r/%s: %s", sub_name, exc)
            continue

    # ── build result dicts ────────────────────────────────────────────────────
    now_iso = datetime.now().isoformat()

    for ticker, posts in ticker_posts.items():
        if not posts:
            continue

        # Coordination detection: >3 posts about same ticker within any 2-hour window
        times = sorted(post_time_map[ticker])
        coordination_flag = False
        for i, t in enumerate(times):
            window = [x for x in times if t <= x <= t + 7200]
            if len(window) > 3:
                coordination_flag = True
                break

        # Aggregate: average of tanh-normalised individual scores
        individual_scores = [_tanh(p["raw_score"] * 3.0) for p in posts]
        agg_value = sum(individual_scores) / len(individual_scores)
        # Clamp to [-1, 1]
        agg_value = max(-1.0, min(1.0, agg_value))

        avg_quality = sum(p["quality"] for p in posts) / len(posts)
        if coordination_flag:
            avg_quality = min(avg_quality * 0.7, 1.0)

        results.append({
            "source": "reddit",
            "ticker": ticker,
            "market": market,
            "data_type": "social_sentiment",
            "value": round(agg_value, 6),
            "raw_data": {
                "post_count": len(posts),
                "coordination_flag": coordination_flag,
                "subreddits_scanned": subreddits,
                "posts": posts[:10],  # store up to 10 for audit
            },
            "timestamp": now_iso,
            "quality_score": round(avg_quality, 4),
        })

    _save_seen()
    logger.info("reddit_collector: returned %d signals", len(results))
    return results


class RedditCollector:
    """Class wrapper around the module-level collect() function."""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def collect(self, tickers: list, market: str = 'US') -> list:
        return collect(tickers, market, self.config)
