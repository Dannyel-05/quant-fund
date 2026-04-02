"""
Merger spillover detector.

Scans yfinance news for M&A-related keywords affecting sector peers,
estimates spillover premium, and generates directional signals.

MergerSpilloverScore = base_premium × sector_relevance × size_similarity × 0.3
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_MA_KEYWORDS = {
    "acquires", "acquired", "acquisition", "merger", "merges", "takeover",
    "buyout", "bid", "offer", "deal", "buys", "purchase", "purchased",
    "private equity", "pe firm", "leverage", "lbo",
}

# Sector relevance weights for spillover (same sector = 1.0, adjacent = 0.5)
_SECTOR_RELEVANCE = {
    "same": 1.0,
    "adjacent": 0.5,
    "unrelated": 0.1,
}

_BASE_PREMIUM = 0.25  # Typical M&A premium is ~25%


class MergerSpilloverDetector:
    """
    Detects M&A activity in the news that may create spillover signals
    for peer companies in the same sector.
    """

    def __init__(self, store=None):
        self.store = store

    def detect(self, ticker: str, market: str = "us") -> Dict:
        """
        Detect merger spillover signals for a ticker.

        Returns:
            spillover_score     : float [0, 1]
            signal_direction    : int +1 (peer buyout candidate) / 0
            ma_events_found     : list of dicts with {headline, acquiree, score}
            notes               : str
        """
        profile = self._fetch_profile(ticker)
        if not profile:
            return self._neutral(ticker)

        sector = profile.get("sector", "Unknown")
        market_cap = profile.get("market_cap", 0)

        # Scan news for M&A events in same sector
        ma_events = self._scan_ma_news(ticker, sector)

        if not ma_events:
            return self._neutral(ticker)

        # Compute spillover score from strongest event
        best_score = 0.0
        for event in ma_events:
            score = self._compute_spillover(
                event=event,
                ticker_sector=sector,
                ticker_cap=market_cap,
            )
            event["spillover_contribution"] = round(score, 4)
            best_score = max(best_score, score)

        direction = 1 if best_score >= 0.10 else 0

        return {
            "ticker": ticker,
            "spillover_score": round(best_score, 4),
            "signal_direction": direction,
            "ma_events_found": ma_events[:3],
            "sector": sector,
            "notes": f"{len(ma_events)} M&A event(s) detected in sector",
        }

    def batch_detect(self, tickers: List[str], market: str = "us") -> List[Dict]:
        results = []
        for ticker in tickers:
            try:
                results.append(self.detect(ticker, market))
            except Exception as exc:
                logger.warning("MergerSpilloverDetector.batch(%s): %s", ticker, exc)
        return results

    def _scan_ma_news(self, ticker: str, sector: str) -> List[Dict]:
        """Scan recent news headlines for M&A keywords."""
        events = []
        try:
            import yfinance as yf
            # Scan ETF proxies for the sector as well as the ticker itself
            tickers_to_scan = [ticker, "SPY"]  # Extend with sector ETFs if available
            for sym in tickers_to_scan:
                try:
                    news = yf.Ticker(sym).news or []
                    for item in news:
                        title = (
                            item.get("title")
                            or (item.get("content") or {}).get("title", "")
                            or ""
                        ).lower()
                        if any(kw in title for kw in _MA_KEYWORDS):
                            events.append({
                                "headline": title,
                                "source_ticker": sym,
                                "sector_match": True,
                            })
                except Exception:
                    continue
        except ImportError:
            pass
        return events

    def _compute_spillover(
        self, event: Dict, ticker_sector: str, ticker_cap: float
    ) -> float:
        """
        MergerSpilloverScore = base_premium × sector_relevance × size_similarity × 0.3
        """
        sector_rel = _SECTOR_RELEVANCE["same"]  # assume same sector for now
        # Size similarity: small-mid caps ($50M-$2B) are most likely targets
        if ticker_cap and 50_000_000 <= ticker_cap <= 2_000_000_000:
            size_sim = 1.0
        elif ticker_cap and ticker_cap < 50_000_000:
            size_sim = 0.3
        else:
            size_sim = 0.5

        score = _BASE_PREMIUM * sector_rel * size_sim * 0.3
        return min(score, 1.0)

    def _fetch_profile(self, ticker: str) -> Optional[Dict]:
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info or {}
            return {
                "sector": info.get("sector", "Unknown"),
                "market_cap": info.get("marketCap", 0),
            }
        except Exception as exc:
            logger.debug("MergerSpilloverDetector.fetch(%s): %s", ticker, exc)
            return None

    def _neutral(self, ticker: str) -> Dict:
        return {
            "ticker": ticker,
            "spillover_score": 0.0,
            "signal_direction": 0,
            "ma_events_found": [],
            "notes": "No M&A events detected",
        }
