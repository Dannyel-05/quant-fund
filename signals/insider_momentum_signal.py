"""Insider momentum signal based on SEC Form 4 filings."""
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd

logger = logging.getLogger(__name__)


class InsiderMomentumSignal:
    def __init__(self, config: dict):
        self.config = config
        self.headers = {'User-Agent': 'quant-fund research@quantfund.com'}

    def get_insider_trades(self, ticker: str, days_back: int = 90) -> List[Dict]:
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            r = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={'q': f'"{ticker}"', 'forms': '4', 'dateRange': 'custom',
                        'startdt': from_date, 'enddt': datetime.now().strftime('%Y-%m-%d')},
                headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json().get('hits', {}).get('hits', [])
        except Exception as e:
            logger.debug('InsiderMomentumSignal.get_insider_trades %s: %s', ticker, e)
        return []

    def generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]:
        signals = []
        try:
            trades = self.get_insider_trades(ticker, days_back=60)
            if len(trades) >= 2:
                # Multiple Form 4 filings = insider activity
                signals.append({
                    'ticker': ticker, 'direction': 'LONG',
                    'score': min(len(trades) / 10.0, 1.0),
                    'insider_filings_60d': len(trades),
                    'signal_type': 'INSIDER_MOMENTUM'
                })
        except Exception as e:
            logger.debug('InsiderMomentumSignal.generate %s: %s', ticker, e)
        return signals
