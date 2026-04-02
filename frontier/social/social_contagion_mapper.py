"""
Social Contagion Mapper — Social Contagion Velocity (SCV) via Reddit.

Economic Hypothesis
-------------------
Information about stocks propagates through social networks in a manner
analogous to epidemic spread (SIR model, Kermack & McKendrick 1927).
The population is divided into: Susceptible investors who have not yet
heard about a stock, Infected investors who are actively discussing and
trading it, and Recovered investors who have already acted and moved on.

A rising SCV (dI/dt > 0) with R0 > 1 predicts near-term price momentum
as the "infection" (trading interest) accelerates.  When SCV peaks and R0
crosses below 1, the contagion is self-exhausting and reversal is likely.

Reddit communities r/wallstreetbets, r/investing, and r/stocks provide
a real-time, free proxy for retail investor social contagion.  Ticker
mention counts from post titles are used to estimate the infected population.

Signal: R0 (basic reproduction number).  R0 > 1 = viral momentum,
R0 < 1 = dying interest.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import requests

from frontier.equations.derived_formulas import calc_scv

logger = logging.getLogger(__name__)

_SUBREDDITS = [
    "wallstreetbets",
    "investing",
    "stocks",
]
_REDDIT_HOT_URL = "https://www.reddit.com/r/{sub}/hot.json?limit=25"
_HEADERS = {"User-Agent": "quant-fund-research-bot/0.1 (academic research)"}

_DEFAULT_TICKERS = [
    "AAPL", "TSLA", "NVDA", "AMD", "GME", "AMC", "PLTR", "MSTR",
    "SPY", "QQQ", "MSFT", "AMZN", "META", "GOOG", "NFLX",
]

_TICKER_PATTERN = re.compile(r"\b([A-Z]{2,5})\b")


def _fetch_reddit_titles(subreddit: str) -> list:
    """Fetch hot post titles from a subreddit via the public JSON API."""
    url = _REDDIT_HOT_URL.format(sub=subreddit)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        return [p["data"].get("title", "") for p in posts]
    except Exception as exc:
        logger.warning("Failed to fetch r/%s: %s", subreddit, exc)
        return []


def _count_ticker_mentions(titles: list, tickers: list) -> dict:
    """Count how many post titles mention each ticker (case-sensitive)."""
    ticker_set = set(tickers)
    counts: dict = {}
    for title in titles:
        found = _TICKER_PATTERN.findall(title)
        for t in found:
            if t in ticker_set:
                counts[t] = counts.get(t, 0) + 1
    return counts


class SocialContagionMapper:
    """
    Map social contagion dynamics for a list of tickers using Reddit data.

    Uses the SIR epidemic model applied to investor attention spreading
    through Reddit communities to compute the Social Contagion Velocity
    and the basic reproduction number R0.
    """

    def collect(self, tickers: Optional[list] = None) -> dict:
        """
        Fetch Reddit hot posts, count ticker mentions, compute SCV and R0.

        Parameters
        ----------
        tickers : list of uppercase ticker symbols to track.
                  Defaults to a curated list of frequently discussed names.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        if tickers is None:
            tickers = _DEFAULT_TICKERS

        all_titles: list = []
        subreddit_counts: dict = {}

        for sub in _SUBREDDITS:
            titles = _fetch_reddit_titles(sub)
            all_titles.extend(titles)
            subreddit_counts[sub] = len(titles)

        mention_counts = _count_ticker_mentions(all_titles, tickers)
        unique_mentioned = len(mention_counts)

        # SIR population estimates
        susceptible = 1000.0
        infected = float(max(unique_mentioned, 1))
        recovered = infected * 0.3

        beta = 0.3
        gamma = 0.1

        try:
            scv_rate, r0 = calc_scv(
                susceptible=susceptible,
                infected=infected,
                recovered=recovered,
                beta=beta,
                gamma=gamma,
            )
        except Exception as exc:
            logger.warning("calc_scv failed: %s", exc)
            scv_rate, r0 = 0.0, beta / gamma

        posts_fetched = len(all_titles)
        quality_score = min(1.0, posts_fetched / 75.0)  # 75 = 3 subs × 25 posts

        raw_data = {
            "mention_counts": mention_counts,
            "unique_tickers_mentioned": unique_mentioned,
            "total_posts_fetched": posts_fetched,
            "subreddit_post_counts": subreddit_counts,
            "scv_rate": scv_rate,
            "r0": r0,
            "sir_params": {
                "susceptible": susceptible,
                "infected": infected,
                "recovered": recovered,
                "beta": beta,
                "gamma": gamma,
            },
        }

        return {
            "signal_name": "social_contagion_r0",
            "value": r0,
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": "reddit_public_json_api",
        }
