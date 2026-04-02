import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .nlp_processor import NLPProcessor

logger = logging.getLogger(__name__)


class SentimentEngine:
    """
    Multi-level sentiment aggregation across all data sources.
    Supports VADER, TextBlob, keyword scoring, and optional FinBERT.
    Applies source weights and recency multipliers when aggregating.
    """

    SOURCE_WEIGHTS: Dict[str, float] = {
        "sec_edgar":   0.35,
        "news":        0.25,
        "stocktwits":  0.20,
        "reddit_dd":   0.15,
        "reddit":      0.05,
    }

    RECENCY_MULTIPLIER_6H = 3.0
    RECENCY_MULTIPLIER_24H = 1.5
    RECENCY_MULTIPLIER_OLD = 1.0

    def __init__(self, config: dict, store=None):
        self.config = config
        self.store = store  # AltDataStore instance, optional
        self.nlp = NLPProcessor()
        self._finbert = None  # lazy load

    # ------------------------------------------------------------------
    # Single-text scoring
    # ------------------------------------------------------------------

    def score_text(self, text: str, priority: str = "normal") -> dict:
        """
        Score text with all available methods.

        priority:
          "normal"  — VADER + TextBlob + keywords
          "high"    — also attempts FinBERT; re-weights composite if available

        Returns dict containing individual scores and a composite score in [-1, +1].
        """
        vader = self.nlp.vader_score(text)
        tb_polarity, tb_subjectivity = self.nlp.textblob_score(text)
        keyword = self.nlp.keyword_score(text)

        composite = vader * 0.40 + tb_polarity * 0.25 + keyword * 0.35

        result = {
            "vader": vader,
            "textblob_polarity": tb_polarity,
            "textblob_subjectivity": tb_subjectivity,
            "keyword": keyword,
            "composite": composite,
            "finbert": None,
            "leadership_change": self.nlp.detect_leadership_change(text),
            "regulatory_flag": self.nlp.detect_regulatory(text),
            "ma_probability": self.nlp.detect_ma(text),
        }

        if priority == "high":
            finbert_score = self._run_finbert(text)
            if finbert_score is not None:
                result["finbert"] = finbert_score
                # Re-weight composite to include FinBERT
                composite = (
                    vader * 0.25
                    + tb_polarity * 0.15
                    + keyword * 0.25
                    + finbert_score * 0.35
                )
                result["composite"] = composite

        return result

    def _run_finbert(self, text: str) -> Optional[float]:
        """
        Run ProsusAI/finbert sentiment analysis.
        Lazy-loads the model on first call.
        Returns None if transformers is unavailable or on any error.
        """
        try:
            if self._finbert is None:
                from transformers import pipeline
                self._finbert = pipeline(
                    "sentiment-analysis",
                    model="ProsusAI/finbert",
                    truncation=True,
                    max_length=512,
                )
            result = self._finbert(text[:512])[0]
            label_map = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}
            return label_map.get(result["label"], 0.0) * float(result["score"])
        except ImportError:
            logger.debug("transformers not installed; FinBERT unavailable.")
            return None
        except Exception as exc:
            logger.debug("FinBERT inference failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Multi-source aggregation
    # ------------------------------------------------------------------

    def aggregate_ticker_sentiment(
        self,
        ticker: str,
        scores_by_source: Dict[str, List[Tuple[float, str, float]]],
        hours_back: int = 24,
    ) -> dict:
        """
        Aggregate sentiment scores from multiple sources with recency
        and source weighting.

        Parameters
        ----------
        ticker : str
            Ticker symbol (used for logging only).
        scores_by_source : dict
            Mapping of source_name -> list of (score, timestamp_iso, confidence).
            score: float in [-1, +1]
            timestamp_iso: ISO 8601 string
            confidence: float in [0, 1]
        hours_back : int
            Maximum age of entries to include (default 24 hours).

        Returns
        -------
        dict with keys:
          composite  : float in [-1, +1]
          confidence : float in [0, 1]
          n_sources  : int (total qualifying entries across sources)
        """
        now = datetime.now()

        weighted_sum = 0.0
        total_weight = 0.0
        n_sources = 0

        for source, entries in scores_by_source.items():
            source_weight = self.SOURCE_WEIGHTS.get(source, 0.1)

            for item in entries:
                try:
                    score, ts_iso, confidence = item
                    ts = datetime.fromisoformat(ts_iso)
                    # Strip timezone info for naive comparison
                    if ts.tzinfo is not None:
                        ts = ts.replace(tzinfo=None)
                    hours_ago = (now - ts).total_seconds() / 3600.0

                    if hours_ago > hours_back:
                        continue

                    if hours_ago < 6:
                        recency = self.RECENCY_MULTIPLIER_6H
                    elif hours_ago < 24:
                        recency = self.RECENCY_MULTIPLIER_24H
                    else:
                        recency = self.RECENCY_MULTIPLIER_OLD

                    w = source_weight * recency * float(confidence)
                    weighted_sum += float(score) * w
                    total_weight += w
                    n_sources += 1

                except Exception as exc:
                    logger.warning(
                        "aggregate_ticker_sentiment: skipping bad entry for %s/%s: %s",
                        ticker, source, exc,
                    )
                    continue

        if total_weight == 0.0:
            return {"composite": 0.0, "confidence": 0.0, "n_sources": 0}

        composite = weighted_sum / total_weight
        composite = max(-1.0, min(1.0, composite))

        # Confidence scales with number of agreeing sources (saturates at 5)
        confidence = min(n_sources / 5.0, 1.0)

        return {
            "composite": composite,
            "confidence": confidence,
            "n_sources": n_sources,
        }
