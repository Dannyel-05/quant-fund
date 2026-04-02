import logging
import re
from typing import Set, Tuple

logger = logging.getLogger(__name__)


class NLPProcessor:
    """
    Financial NLP utilities: VADER, TextBlob, keyword scoring,
    leadership change detection, regulatory detection, M&A detection,
    and ticker extraction.
    """

    VERY_POSITIVE = [
        "record earnings", "beat expectations", "raised guidance", "buyback",
        "dividend increase", "acquisition", "major contract", "partnership",
        "market share gain",
    ]
    POSITIVE = [
        "growth", "expansion", "profit", "revenue increase", "new contract",
        "award", "upgrade", "outperform", "beat", "exceeded",
    ]
    NEGATIVE = [
        "miss", "below expectations", "loss", "debt", "lawsuit", "recall",
        "investigation", "downgrade", "underperform", "disappoints", "warning",
    ]
    VERY_NEGATIVE = [
        "bankruptcy", "fraud", "SEC investigation", "class action",
        "guidance cut", "CEO resignation", "profit warning", "going concern",
        "default", "delisted",
    ]

    LEADERSHIP_TERMS = [
        "CEO", "CFO", "CTO", "chairman", "chief executive",
        "chief financial", "managing director",
    ]
    LEADERSHIP_EVENTS = [
        "resign", "step down", "depart", "retire", "fired",
        "terminated", "appointed", "joins as", "named",
    ]

    REGULATORY_BODIES = ["SEC", "FCA", "DOJ", "HMRC", "OFCOM", "CMA", "FDA", "FTC"]

    MA_TERMS = [
        "merger", "acquisition", "takeover", "buyout", "bid", "offer",
        "combine", "merge", "acquire",
    ]

    # Stopwords for ticker extraction
    _TICKER_STOPWORDS: Set[str] = {
        "I", "A", "THE", "CEO", "CFO", "CTO", "IPO", "ETF", "NYSE", "SEC",
        "FDA", "UK", "US", "EU", "AND", "OR", "FOR", "IN", "ON", "AT",
        "BY", "TO", "OF", "IS", "IT", "BE", "DO", "GO",
    }

    def __init__(self):
        self.vader = None  # lazy load

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _get_vader(self):
        if self.vader is None:
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                self.vader = SentimentIntensityAnalyzer()
            except ImportError:
                logger.warning("vaderSentiment not installed; VADER scoring unavailable.")
                self.vader = None
        return self.vader

    def vader_score(self, text: str) -> float:
        """Return VADER compound score in [-1, +1]. Returns 0.0 if unavailable."""
        analyzer = self._get_vader()
        if analyzer is None:
            return 0.0
        try:
            return float(analyzer.polarity_scores(text)["compound"])
        except Exception as exc:
            logger.warning("VADER scoring failed: %s", exc)
            return 0.0

    def textblob_score(self, text: str) -> Tuple[float, float]:
        """
        Return (polarity, subjectivity) via TextBlob.
        Falls back to (0.0, 0.5) on any error.
        """
        try:
            from textblob import TextBlob
            tb = TextBlob(text)
            return float(tb.sentiment.polarity), float(tb.sentiment.subjectivity)
        except ImportError:
            logger.warning("textblob not installed; TextBlob scoring unavailable.")
            return 0.0, 0.5
        except Exception as exc:
            logger.warning("TextBlob scoring failed: %s", exc)
            return 0.0, 0.5

    def keyword_score(self, text: str) -> float:
        """
        Rule-based keyword score in [-1, +1].
        Checks VERY_POSITIVE/POSITIVE/NEGATIVE/VERY_NEGATIVE lists,
        with simple negation handling.
        """
        text_lower = text.lower()
        score = 0.0

        for phrase in self.VERY_POSITIVE:
            if phrase in text_lower:
                score += 1.0
        for phrase in self.POSITIVE:
            if phrase in text_lower:
                score += 0.5
        for phrase in self.NEGATIVE:
            if phrase in text_lower:
                score -= 0.5
        for phrase in self.VERY_NEGATIVE:
            if phrase in text_lower:
                score -= 1.0

        # Negation handling: "not profitable" etc. reduce absolute score 30%
        negations = re.findall(
            r"\b(not|no|never|neither|nor|don't|doesn't|didn't|isn't|aren't|wasn't)\s+\w+",
            text_lower,
        )
        if negations:
            score *= 0.7

        return max(-1.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Event detection
    # ------------------------------------------------------------------

    def detect_leadership_change(self, text: str) -> bool:
        """Return True if text mentions a leadership change event."""
        text_lower = text.lower()
        has_leader = any(t.lower() in text_lower for t in self.LEADERSHIP_TERMS)
        has_event = any(e in text_lower for e in self.LEADERSHIP_EVENTS)
        return has_leader and has_event

    def detect_regulatory(self, text: str) -> bool:
        """Return True if any regulatory body is mentioned."""
        return any(rb in text for rb in self.REGULATORY_BODIES)

    def detect_ma(self, text: str) -> float:
        """
        Return M&A probability estimate in [0.0, 1.0]
        based on density of M&A-related terms.
        """
        text_lower = text.lower()
        hits = sum(1 for term in self.MA_TERMS if term in text_lower)
        return min(hits / 3.0, 1.0)

    # ------------------------------------------------------------------
    # Ticker extraction
    # ------------------------------------------------------------------

    def extract_tickers(self, text: str) -> Set[str]:
        """
        Extract potential ticker symbols from text.
        Recognises both $AAPL-style and bare ALL-CAPS 2-5 letter tokens.
        Filters common non-ticker words.
        """
        dollar = set(re.findall(r"\$([A-Z]{2,5})\b", text))
        bare = set(re.findall(r"\b([A-Z]{2,5})\b", text))
        return (dollar | bare) - self._TICKER_STOPWORDS
