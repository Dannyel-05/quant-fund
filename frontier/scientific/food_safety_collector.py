"""
Food Safety Collector — food_safety_risk Signal.

Monitors the FDA's food recall RSS feed to derive a rolling measure of
food-safety system stress in the United States.

Economic hypothesis
-------------------
FDA food recall frequency is a leading indicator for several investable
themes:

  1. Food & beverage company liability risk: surges in recalls in a given
     sub-category (e.g. frozen produce, deli meats) signal elevated
     litigation and remediation costs for named brands.

  2. Retail food chain disruption: high recall frequency causes shelf
     clearances, supply gaps, and accelerated private-label switching.

  3. Regulatory and inspection cycle intensity: the FDA recall cadence
     also tracks resource utilisation — more recalls mean more inspector
     time, which in turn affects FDA review timelines for new product
     approvals in adjacent categories.

  4. Macro food inflation: recall-driven supply removals tighten available
     supply, contributing to short-term food-CPI spikes in affected
     categories.

A value of 1.0 (20 or more recalls in 30 days) represents a period of
elevated systemic food-safety stress.  The 20-recall baseline was chosen
as approximately the 90th percentile of historical monthly recall counts
in the 2020–2024 period.

Update frequency: daily.

Data source
-----------
FDA Food Recalls RSS feed (free, no key):
https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-recalls/rss.xml
"""

import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)

_FDA_RSS_URL = (
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/"
    "rss-feeds/food-recalls/rss.xml"
)
_MAX_RISK_RECALLS = 20     # recalls per 30 days = maximum risk (score = 1.0)
_LOOKBACK_DAYS = 30
_REQUEST_TIMEOUT = 15


class FoodSafetyCollector:
    """
    Parses the FDA food recall RSS feed and computes a normalised
    food-safety risk score based on the count of recalls in the
    most recent 30 days.
    """

    def _fetch_rss(self) -> str:
        """
        Download the FDA food recall RSS XML.

        Returns the raw XML text, or an empty string on error.
        """
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; QuantFundFoodSafetyBot/1.0)"
                )
            }
            resp = requests.get(
                _FDA_RSS_URL, headers=headers, timeout=_REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.Timeout:
            logger.warning("FoodSafetyCollector: FDA RSS request timed out.")
        except requests.exceptions.RequestException as exc:
            logger.warning("FoodSafetyCollector: network error: %s", exc)
        except Exception as exc:
            logger.warning("FoodSafetyCollector: unexpected error: %s", exc)
        return ""

    def _parse_items(self, xml_text: str) -> list[dict]:
        """
        Parse RSS <item> elements from the XML, extracting title and pubDate.

        Returns a list of dicts: [{title, pub_date_str, pub_dt}, ...].
        Silently skips malformed items.
        """
        items = []
        if not xml_text:
            return items
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("FoodSafetyCollector: XML parse error: %s", exc)
            return items

        # RSS namespace-agnostic search
        for item in root.iter("item"):
            title_el = item.find("title")
            pubdate_el = item.find("pubDate")

            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            pub_date_str = (
                pubdate_el.text.strip()
                if pubdate_el is not None and pubdate_el.text
                else ""
            )

            pub_dt = None
            if pub_date_str:
                try:
                    pub_dt = parsedate_to_datetime(pub_date_str)
                    # Ensure timezone-aware
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            items.append({
                "title": title,
                "pub_date_str": pub_date_str,
                "pub_dt": pub_dt,
            })

        return items

    def _count_recent_recalls(self, items: list[dict]) -> int:
        """Count recalls published within the last LOOKBACK_DAYS days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
        count = 0
        for item in items:
            if item["pub_dt"] is not None and item["pub_dt"] >= cutoff:
                count += 1
        return count

    def collect(self) -> dict:
        """
        Fetch the FDA food recall RSS and return the food_safety_risk signal.

        Returns
        -------
        dict with keys:
            signal_name   : "food_safety_risk"
            value         : float in [0, 1]
            raw_data      : dict — recall count, titles of recent recalls
            quality_score : 1.0 if RSS fetched successfully, 0.0 on error
            timestamp     : ISO-8601 UTC string
            source        : FDA RSS URL
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        xml_text = self._fetch_rss()
        quality_score = 1.0 if xml_text else 0.0

        if not xml_text:
            return {
                "signal_name": "food_safety_risk",
                "value": 0.0,
                "raw_data": {"error": "Failed to fetch FDA RSS feed."},
                "quality_score": 0.0,
                "timestamp": timestamp,
                "source": _FDA_RSS_URL,
            }

        items = self._parse_items(xml_text)
        recall_count = self._count_recent_recalls(items)

        # food_safety_risk = min(1.0, recall_count / 20)
        food_safety_risk = min(1.0, recall_count / _MAX_RISK_RECALLS)

        # Collect titles of recent recalls for raw_data
        recent_titles = [
            it["title"]
            for it in items
            if it["pub_dt"] is not None
            and it["pub_dt"]
            >= datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
        ]

        raw_data = {
            "total_items_in_feed": len(items),
            "recalls_last_30d": recall_count,
            "max_risk_threshold": _MAX_RISK_RECALLS,
            "recent_recall_titles": recent_titles[:10],  # cap for readability
        }

        return {
            "signal_name": "food_safety_risk",
            "value": float(food_safety_risk),
            "raw_data": raw_data,
            "quality_score": quality_score,
            "timestamp": timestamp,
            "source": _FDA_RSS_URL,
        }
