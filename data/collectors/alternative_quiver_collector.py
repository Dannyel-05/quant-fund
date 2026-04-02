"""
Alternative Quiver data collector using free public sources.
Congressional trading from SEC EDGAR Form 4, PatentsView API, etc.
No API key required for basic functionality.
"""
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class AlternativeQuiverCollector:
    """Free alternative to QuiverQuantitative using public data sources."""

    def __init__(self, config: dict):
        self.config = config
        self.headers = {'User-Agent': 'quant-fund research@quantfund.com'}
        logger.info('AlternativeQuiverCollector: READY (free public sources)')

    def get_senate_trades(self, days_back: int = 30) -> List[Dict]:
        """Get Senate financial disclosures from efts.sec.gov."""
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={
                    'q': '"Senate" "financial disclosure"',
                    'dateRange': 'custom',
                    'startdt': from_date,
                    'enddt': datetime.now().strftime('%Y-%m-%d'),
                    'forms': '4'
                },
                headers=self.headers, timeout=15)
            if r.status_code == 200:
                hits = r.json().get('hits', {}).get('hits', [])
                return [{'source': h.get('_source', {}), 'type': 'senate'} for h in hits[:20]]
        except Exception as e:
            logger.debug('get_senate_trades: %s', e)
        return []

    def get_house_trades(self, days_back: int = 30) -> List[Dict]:
        """Get House financial disclosures."""
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={
                    'q': '"House" "financial disclosure" "stock"',
                    'dateRange': 'custom',
                    'startdt': from_date,
                    'enddt': datetime.now().strftime('%Y-%m-%d'),
                    'forms': '4'
                },
                headers=self.headers, timeout=15)
            if r.status_code == 200:
                hits = r.json().get('hits', {}).get('hits', [])
                return [{'source': h.get('_source', {}), 'type': 'house'} for h in hits[:20]]
        except Exception as e:
            logger.debug('get_house_trades: %s', e)
        return []

    def get_wsb_mentions(self, ticker: str) -> Dict:
        """Estimate WSB mentions via Reddit public API."""
        try:
            r = requests.get(
                'https://www.reddit.com/r/wallstreetbets/search.json',
                params={'q': ticker, 'sort': 'new', 'limit': 25, 'restrict_sr': 1},
                headers={**self.headers, 'User-Agent': 'quant-fund/1.0'},
                timeout=10)
            if r.status_code == 200:
                posts = r.json().get('data', {}).get('children', [])
                return {'ticker': ticker, 'mention_count': len(posts), 'source': 'reddit_public'}
        except Exception as e:
            logger.debug('get_wsb_mentions %s: %s', ticker, e)
        return {'ticker': ticker, 'mention_count': 0, 'source': 'unavailable'}

    def get_patent_filings(self, company_name: str) -> List[Dict]:
        """Get recent patent filings from PatentsView API."""
        try:
            r = requests.get(
                'https://search.patentsview.org/api/v1/patent/',
                params={
                    'q': f'{{"_text_any":{{"patent_abstract":"{company_name}"}}}}',
                    'f': '["patent_number","patent_title","patent_date"]',
                    'o': '{"per_page":10}'
                },
                headers=self.headers, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return data.get('patents', []) or []
        except Exception as e:
            logger.debug('get_patent_filings %s: %s', company_name, e)
        return []

    def get_form4_insider_trades(self, ticker: str, days_back: int = 30) -> List[Dict]:
        """Get Form 4 insider trades from SEC EDGAR for a ticker."""
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={
                    'q': f'"{ticker}"',
                    'forms': '4',
                    'dateRange': 'custom',
                    'startdt': from_date,
                    'enddt': datetime.now().strftime('%Y-%m-%d'),
                },
                headers=self.headers, timeout=15)
            if r.status_code == 200:
                hits = r.json().get('hits', {}).get('hits', [])
                return [{
                    'ticker': ticker,
                    'entity': h.get('_source', {}).get('entity_name', ''),
                    'date': h.get('_source', {}).get('file_date', ''),
                    'form': '4'
                } for h in hits[:20]]
        except Exception as e:
            logger.debug('get_form4_insider_trades %s: %s', ticker, e)
        return []
