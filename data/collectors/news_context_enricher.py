"""
NewsContextEnricher — attaches financial-context scores to raw news articles.

Enriches each article dict with:
  - sentiment_score : VADER compound (-1 to +1)
  - relevance_score : keyword-based relevance to the ticker
  - financial_context_score : weighted composite of sentiment + relevance
  - categories : detected article categories (earnings, guidance, m&a, etc.)

Usage
-----
enricher = NewsContextEnricher()
articles = enricher.enrich(ticker, raw_articles)
"""
from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── keyword mappings ──────────────────────────────────────────────────────────

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "earnings":  ["earnings", "eps", "revenue", "profit", "loss", "beat", "miss",
                  "quarterly", "annual results", "full year"],
    "guidance":  ["guidance", "outlook", "forecast", "raised", "lowered", "warned",
                  "expects", "expects to", "full-year"],
    "m_and_a":   ["merger", "acquisition", "takeover", "bid", "deal", "buyout",
                  "acquire", "bought", "agreed to buy"],
    "dividend":  ["dividend", "yield", "payout", "special dividend", "buyback"],
    "insider":   ["insider", "director", "ceo", "cfo", "bought shares", "sold shares",
                  "form 4", "significant control"],
    "macro":     ["federal reserve", "fed", "interest rate", "inflation", "gdp",
                  "bank of england", "ecb", "treasury"],
    "legal":     ["lawsuit", "investigation", "sec", "fca", "regulatory", "fine",
                  "settlement", "fraud"],
    "product":   ["launch", "product", "fda", "approval", "patent", "clinical",
                  "trial", "pipeline"],
}

_POSITIVE_WORDS = {
    "beat", "record", "strong", "growth", "raised", "upgraded", "buy",
    "outperform", "upbeat", "surge", "soar", "rally", "approval", "wins",
    "breakthrough", "boost", "exceed", "positive",
}

_NEGATIVE_WORDS = {
    "miss", "loss", "warn", "downgrade", "sell", "underperform", "slump",
    "crash", "investigation", "fraud", "cut", "lower", "concern", "decline",
    "disappoints", "weak", "delay", "rejected", "recall",
}


def _simple_sentiment(text: str) -> float:
    """
    Fallback sentiment: count positive/negative words, return [-1, 1].
    """
    words = set(re.findall(r"\b\w+\b", text.lower()))
    pos = len(words & _POSITIVE_WORDS)
    neg = len(words & _NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)


def _vader_sentiment(text: str) -> float:
    """
    Use VADER if available, else fall back to simple keyword sentiment.
    """
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(text)
        return round(scores["compound"], 4)
    except ImportError:
        return _simple_sentiment(text)
    except Exception as exc:
        logger.debug("VADER error: %s", exc)
        return _simple_sentiment(text)


def _relevance(text: str, ticker: str) -> float:
    """
    Keyword relevance: how many times the ticker and related financial keywords
    appear in the text.  Returns 0.0–1.0.
    """
    text_lower = text.lower()
    # Ticker mentions
    ticker_name = ticker.split(".")[0].lower()
    ticker_hits = text_lower.count(ticker_name)

    # Financial keyword density
    fin_keywords = ["earnings", "revenue", "profit", "guidance", "dividend",
                    "acquisition", "shares", "stock", "market cap"]
    keyword_hits = sum(text_lower.count(kw) for kw in fin_keywords)

    relevance = min(1.0, ticker_hits * 0.3 + keyword_hits * 0.05)
    return round(relevance, 4)


def _detect_categories(text: str) -> list[str]:
    """Return list of matching article category labels."""
    text_lower = text.lower()
    found = []
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(category)
    return found


class NewsContextEnricher:
    """
    Enriches news article dicts with financial context scores.
    """

    def enrich(self, ticker: str, articles: list[dict]) -> list[dict]:
        """
        Enrich a list of article dicts in place and return them.
        Each article should have at least one of: 'title', 'summary', 'body', 'text'.
        """
        enriched: list[dict] = []
        for article in articles:
            try:
                enriched.append(self._enrich_one(ticker, article))
            except Exception as exc:
                logger.debug("NewsContextEnricher: error on article: %s", exc)
                enriched.append(article)
        return enriched

    def _enrich_one(self, ticker: str, article: dict) -> dict:
        text = self._extract_text(article)

        sentiment = _vader_sentiment(text)
        relevance = _relevance(text, ticker)
        categories = _detect_categories(text)

        # Composite: 60% sentiment magnitude (abs), 40% relevance
        # Signed: use sentiment direction
        composite = round(sentiment * 0.6 + relevance * 0.4 * (1 if sentiment >= 0 else -1), 4)

        article["sentiment_score"]         = sentiment
        article["relevance_score"]         = relevance
        article["financial_context_score"] = composite
        article["categories"]              = categories
        article["_enriched"]               = True
        return article

    @staticmethod
    def _extract_text(article: dict) -> str:
        """Concatenate title + summary/body for scoring."""
        parts = []
        for key in ("title", "summary", "description", "body", "text", "content"):
            val = article.get(key, "")
            if isinstance(val, str) and val:
                parts.append(val)
        return " ".join(parts)

    def score_for_ticker(self, ticker: str, articles: list[dict]) -> float:
        """
        Return a single aggregate financial_context_score for all articles.
        Uses a recency-weighted average (most recent article gets highest weight).
        """
        enriched = self.enrich(ticker, articles)
        if not enriched:
            return 0.0
        scores = [a.get("financial_context_score", 0.0) for a in enriched]
        # Linearly weight: most recent (last in list) gets max weight
        n = len(scores)
        weights = [i + 1 for i in range(n)]
        total_w = sum(weights)
        weighted = sum(s * w for s, w in zip(scores, weights))
        return round(weighted / total_w, 4) if total_w > 0 else 0.0
