"""Capitol Trades / Congressional trading data collector.

Uses SEC EDGAR Form 4 as a fallback data source since Capitol Trades
requires a paid API key. The CongressionalDisclosureFetcher in this
package provides the primary source.
"""
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)


class CapitolTradesCollector:
    """Collector for congressional stock trades via Capitol Trades API
    with fallback to SEC EDGAR Form 4 filings."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.headers = {'User-Agent': 'quant-fund research@quantfund.com'}
        # Try to load the CongressionalDisclosureFetcher as primary source
        self._fetcher = None
        try:
            from deepdata.congressional.disclosure_fetcher import CongressionalDisclosureFetcher
            self._fetcher = CongressionalDisclosureFetcher()
            logger.info('CapitolTradesCollector: using CongressionalDisclosureFetcher')
        except Exception as e:
            logger.info('CapitolTradesCollector: falling back to SEC EDGAR (%s)', e)

    def get_recent_trades(self, days_back: int = 30) -> List[Dict]:
        """Get recent congressional trades."""
        if self._fetcher is not None:
            try:
                return self._fetcher.fetch_recent(days_back=days_back)
            except Exception as e:
                logger.debug('CapitolTradesCollector fetcher error: %s', e)
        return self._fetch_from_edgar(days_back)

    def _fetch_from_edgar(self, days_back: int) -> List[Dict]:
        """Fallback: pull Form 4 filings from SEC EDGAR full-text search."""
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={
                    'q': 'congress OR senator OR representative',
                    'forms': '4',
                    'dateRange': 'custom',
                    'startdt': from_date,
                    'enddt': datetime.now().strftime('%Y-%m-%d'),
                },
                headers=self.headers,
                timeout=15,
            )
            if r.status_code == 200:
                hits = r.json().get('hits', {}).get('hits', [])
                return [h.get('_source', {}) for h in hits[:50]]
        except Exception as e:
            logger.debug('CapitolTradesCollector._fetch_from_edgar: %s', e)
        return []

    def get_ticker_trades(self, ticker: str, days_back: int = 90) -> List[Dict]:
        """Get congressional trades for a specific ticker."""
        if self._fetcher is not None:
            try:
                return self._fetcher.fetch_for_ticker(ticker, days_back=days_back)
            except Exception:
                pass
        return []

    def get_signal_for_ticker(self, ticker: str) -> Dict:
        """Return a simple signal dict if congressional buying detected."""
        trades = self.get_ticker_trades(ticker, days_back=60)
        buys = [t for t in trades if str(t.get('transaction_type', '')).upper() in ('P', 'BUY', 'PURCHASE')]
        if buys:
            return {'ticker': ticker, 'direction': 'LONG',
                    'score': min(len(buys) / 5.0, 1.0),
                    'congressional_buys_60d': len(buys),
                    'signal_type': 'CONGRESSIONAL'}
        return {}
