"""
Attention Economy Collector — Attention Saturation Index (ASI).

Economic Hypothesis
-------------------
Investor attention is a finite cognitive resource (Kahneman 1973,
Hirshleifer & Teoh 2003).  When the news cycle is saturated by a single
dominant macro theme (e.g., Fed decisions, inflation prints, recession fear),
other securities receive systematically less attention.  This under-coverage
leads to delayed price discovery and predictable mean-reversion once the
saturating topic fades.

Google Trends provides a real-time proxy for public attention across financial
topics.  When one topic monopolises search volume the ASI approaches 0,
signalling peak mis-pricing opportunity in uncovered stocks.  The complementary
Attention Mispricing Score (AMS) weights the signal by a stock's individual
news share and its historical mean-reversion strength.

Signal: ASI (0 = fully saturated, 1 = evenly distributed attention).
High-frequency pytrends requests are rate-limited; a randomised 45-90 second
sleep is applied before each call.
"""

import logging
import math
import random
import time
from datetime import datetime, timezone

from frontier.equations.derived_formulas import (
    calc_asi,
    calc_attention_mispricing_score,
)

logger = logging.getLogger(__name__)

_TOPICS = [
    "stock market",
    "interest rates",
    "inflation",
    "recession",
    "fed reserve",
]


class AttentionEconomyCollector:
    """Collect Google Trends data to compute the Attention Saturation Index."""

    def collect(self) -> dict:
        """
        Fetch Google Trends interest for key financial topics, compute ASI
        and AMS, and return a standardised signal dict.

        Returns
        -------
        dict with keys: signal_name, value, raw_data, quality_score,
                        timestamp, source
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            from pytrends.request import TrendReq  # type: ignore

            pytrends = TrendReq(hl="en-US", tz=360)

            sleep_secs = random.uniform(45, 90)
            logger.debug(
                "Sleeping %.1f seconds before pytrends call to avoid rate limits.",
                sleep_secs,
            )
            time.sleep(sleep_secs)

            pytrends.build_payload(_TOPICS, timeframe="today 1-m")
            iot = pytrends.interest_over_time()

            if iot is None or iot.empty:
                raise ValueError("pytrends returned empty DataFrame")

            topic_counts: dict = {}
            for topic in _TOPICS:
                if topic in iot.columns:
                    topic_counts[topic] = float(iot[topic].iloc[-1])
                else:
                    topic_counts[topic] = 50.0  # neutral fallback

            asi = calc_asi(topic_counts)

            stock_news_share = topic_counts.get("stock market", 50.0) / 100.0
            ams = calc_attention_mispricing_score(
                asi=asi,
                stock_news_share=stock_news_share,
                mean_reversion_strength=0.5,
            )

            raw_data = {
                "topic_counts": topic_counts,
                "ams": ams,
                "n_topics": len(topic_counts),
            }

            return {
                "signal_name": "asi",
                "value": asi,
                "raw_data": raw_data,
                "quality_score": 1.0,
                "timestamp": timestamp,
                "source": "google_trends_pytrends",
            }

        except Exception as exc:
            logger.warning("AttentionEconomyCollector failed: %s", exc)
            return {
                "signal_name": "asi",
                "value": 0.5,
                "raw_data": {"error": str(exc), "ams": None},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": "google_trends_pytrends",
            }
