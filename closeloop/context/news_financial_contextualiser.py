"""
News financial contextualiser: builds financial profile + sentiment multiplier.

Fetches recent news for a ticker, scores sentiment, combines with fundamental
metrics to produce a contextual signal modifier.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_POSITIVE_WORDS = {
    "beat", "beats", "exceeds", "exceeded", "record", "surge", "growth",
    "raises", "raised", "guidance", "upbeat", "positive", "bullish", "upgrade",
    "buy", "strong", "outperform", "momentum", "win", "wins", "profit",
}
_NEGATIVE_WORDS = {
    "miss", "misses", "missed", "disappoints", "disappointed", "loss", "losses",
    "cut", "cuts", "lowered", "guidance", "downgrade", "sell", "weak", "risk",
    "concern", "decline", "drop", "drops", "fell", "falls", "underperform",
    "layoff", "layoffs", "recall", "fraud", "investigation", "lawsuit",
}


class NewsFinancialContextualiser:
    """
    Fetches and scores financial news headlines for a ticker.
    Produces a sentiment_multiplier in [0.5, 1.5] used to scale PEAD signal strength.
    """

    def __init__(self, store=None):
        self.store = store

    def contextualise(self, ticker: str) -> Dict:
        """
        Returns:
            sentiment_score  : float in [-1, 1]
            sentiment_multiplier : float in [0.5, 1.5]
            headline_count   : int
            financial_profile: dict (P/E, beta, sector)
            headlines_sample : list of str (up to 5)
        """
        headlines = self._fetch_headlines(ticker)
        sentiment = self._score_headlines(headlines)
        profile = self._fetch_financial_profile(ticker)
        multiplier = self._compute_multiplier(sentiment, profile)

        return {
            "ticker": ticker,
            "sentiment_score": round(sentiment, 4),
            "sentiment_multiplier": round(multiplier, 4),
            "headline_count": len(headlines),
            "financial_profile": profile,
            "headlines_sample": headlines[:5],
        }

    def _fetch_headlines(self, ticker: str) -> List[str]:
        headlines = []
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            news = t.news or []
            for item in news:
                title = (
                    item.get("title")
                    or (item.get("content") or {}).get("title", "")
                    or ""
                )
                title = title.strip()
                if title:
                    headlines.append(title)
        except Exception as exc:
            logger.debug("NewsFinancialContextualiser.fetch(%s): %s", ticker, exc)
        return headlines

    def _score_headlines(self, headlines: List[str]) -> float:
        """Return sentiment score in [-1, 1]."""
        if not headlines:
            return 0.0
        scores = []
        for h in headlines:
            words = set(h.lower().split())
            pos = len(words & _POSITIVE_WORDS)
            neg = len(words & _NEGATIVE_WORDS)
            total = pos + neg
            if total > 0:
                scores.append((pos - neg) / total)
        return float(sum(scores) / len(scores)) if scores else 0.0

    def _fetch_financial_profile(self, ticker: str) -> Dict:
        profile = {"pe_ratio": None, "beta": None, "sector": None, "market_cap": None}
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info or {}
            profile["pe_ratio"] = info.get("trailingPE")
            profile["beta"] = info.get("beta")
            profile["sector"] = info.get("sector")
            profile["market_cap"] = info.get("marketCap")
        except Exception as exc:
            logger.debug("NewsFinancialContextualiser.profile(%s): %s", ticker, exc)
        return profile

    def _compute_multiplier(self, sentiment: float, profile: Dict) -> float:
        """
        Base multiplier from sentiment, adjusted by beta if available.
        Range: [0.5, 1.5]
        """
        # sentiment [-1, 1] → multiplier [0.7, 1.3]
        base = 1.0 + 0.3 * sentiment

        # Beta adjustment: high-beta stocks get amplified signals
        beta = profile.get("beta")
        if beta is not None:
            try:
                beta = float(beta)
                if beta > 1.5:
                    base *= 1.1
                elif beta < 0.5:
                    base *= 0.9
            except (TypeError, ValueError):
                pass

        return max(0.5, min(1.5, base))
