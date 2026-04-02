"""
Polygon.io / Massive.com data collector.
Free tier: previous day OHLCV data.
"""
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import pandas as pd

logger = logging.getLogger(__name__)

class PolygonCollector:
    def __init__(self, config: dict):
        self.api_key = config.get('api_keys', {}).get('polygon', '')
        self.enabled = bool(self.api_key) and 'PASTE' not in self.api_key
        self._base = None
        if self.enabled:
            self._base = self._find_base()
        if not self.enabled:
            logger.info('PolygonCollector: NO KEY')
        else:
            logger.info('PolygonCollector: ENABLED (%s)', self._base)

    def _find_base(self) -> str:
        for base in ['https://api.massive.com', 'https://api.polygon.io']:
            try:
                r = requests.get(
                    f'{base}/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-05',
                    params={'apiKey': self.api_key}, timeout=10)
                if r.status_code == 200:
                    return base
            except Exception:
                continue
        return 'https://api.polygon.io'

    def get_daily_bars(self, ticker: str, days_back: int = 30) -> pd.DataFrame:
        if not self.enabled:
            return pd.DataFrame()
        try:
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                f'{self._base}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}',
                params={'apiKey': self.api_key, 'adjusted': 'true', 'sort': 'asc'},
                timeout=15)
            if r.status_code == 200:
                results = r.json().get('results', [])
                if results:
                    df = pd.DataFrame(results)
                    df['date'] = pd.to_datetime(df['t'], unit='ms')
                    df = df.rename(columns={
                        'o': 'open', 'h': 'high', 'l': 'low',
                        'c': 'close', 'v': 'volume'
                    })
                    return df.set_index('date')
        except Exception as e:
            logger.debug('PolygonCollector.get_daily_bars %s: %s', ticker, e)
        return pd.DataFrame()

    def get_previous_close(self, ticker: str) -> Optional[float]:
        if not self.enabled:
            return None
        try:
            r = requests.get(
                f'{self._base}/v2/aggs/ticker/{ticker}/prev',
                params={'apiKey': self.api_key}, timeout=10)
            if r.status_code == 200:
                results = r.json().get('results', [])
                if results:
                    return float(results[0].get('c', 0))
        except Exception as e:
            logger.debug('PolygonCollector.get_previous_close %s: %s', ticker, e)
        return None
