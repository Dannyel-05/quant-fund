"""
Finnhub collector: company news, earnings surprises, analyst ratings,
insider transactions, ESG scores.
Requires finnhub key in config.api_keys.finnhub
"""
import logging
import time
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_BASE = "https://finnhub.io/api/v1"
_DELAY = 0.5  # 60 calls/min free tier

def collect(tickers: list, market: str, config: dict = None) -> list:
    config = config or {}
    api_key = (config.get("api_keys") or {}).get("finnhub", "")
    if not api_key:
        logger.warning("finnhub_collector: no API key configured")
        return []

    session = requests.Session()
    results = []

    for ticker in tickers:
        t_symbol = ticker.replace(".L", "")  # Finnhub uses symbol without .L

        # Company news (last 30 days)
        try:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            to_date = datetime.now().strftime("%Y-%m-%d")
            resp = session.get(
                f"{_BASE}/company-news",
                params={"symbol": t_symbol, "from": from_date, "to": to_date, "token": api_key},
                timeout=15,
            )
            if resp.status_code == 200:
                articles = resp.json()
                for art in articles[:10]:
                    results.append({
                        "source": "finnhub",
                        "ticker": ticker,
                        "market": market,
                        "data_type": "news",
                        "value": 0.0,
                        "raw_data": {
                            "headline": art.get("headline", ""),
                            "summary": art.get("summary", ""),
                            "url": art.get("url", ""),
                            "source": art.get("source", ""),
                            "datetime": art.get("datetime", 0),
                        },
                        "timestamp": datetime.now().isoformat(),
                        "quality_score": 0.7,
                    })
            time.sleep(_DELAY)
        except Exception as e:
            logger.debug("finnhub news failed for %s: %s", ticker, e)

        # Analyst recommendations
        try:
            resp = session.get(
                f"{_BASE}/stock/recommendation",
                params={"symbol": t_symbol, "token": api_key},
                timeout=15,
            )
            if resp.status_code == 200:
                recs = resp.json()
                if recs:
                    latest = recs[0]
                    buy = latest.get("buy", 0)
                    sell = latest.get("sell", 0)
                    hold = latest.get("hold", 0)
                    total = max(buy + sell + hold, 1)
                    score = (buy - sell) / total
                    results.append({
                        "source": "finnhub",
                        "ticker": ticker,
                        "market": market,
                        "data_type": "analyst_rating",
                        "value": round(score, 4),
                        "raw_data": latest,
                        "timestamp": datetime.now().isoformat(),
                        "quality_score": 0.8,
                    })
            time.sleep(_DELAY)
        except Exception as e:
            logger.debug("finnhub recommendations failed for %s: %s", ticker, e)

        # Price target
        try:
            resp = session.get(
                f"{_BASE}/stock/price-target",
                params={"symbol": t_symbol, "token": api_key},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("targetMean"):
                    results.append({
                        "source": "finnhub",
                        "ticker": ticker,
                        "market": market,
                        "data_type": "price_target",
                        "value": round(float(data.get("targetMean", 0)), 4),
                        "raw_data": data,
                        "timestamp": datetime.now().isoformat(),
                        "quality_score": 0.75,
                    })
            time.sleep(_DELAY)
        except Exception as e:
            logger.debug("finnhub price target failed for %s: %s", ticker, e)

    logger.info("finnhub_collector: returned %d signals", len(results))
    return results


class FinnhubCollector:
    """Class wrapper around the module-level collect() function."""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def collect(self, tickers: list, market: str = 'US') -> list:
        return collect(tickers, market, self.config)
