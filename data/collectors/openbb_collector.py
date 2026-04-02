"""
OpenBB Platform collector — free Bloomberg Terminal alternative.
Aggregates analyst estimates, price targets, economic calendar.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)


class OpenBBCollector:
    def __init__(self, config: dict):
        self.config = config
        try:
            from openbb import obb
            self.obb = obb
            self.enabled = True
            logger.info('OpenBBCollector: READY')
        except ImportError:
            self.obb = None
            self.enabled = False
            logger.info('OpenBBCollector: openbb not installed')
        except Exception as e:
            self.obb = None
            self.enabled = False
            logger.info('OpenBBCollector: init failed - %s', e)

    def get_analyst_estimates(self, ticker: str) -> Dict:
        if not self.enabled:
            return {}
        try:
            data = self.obb.equity.estimates.consensus(
                symbol=ticker, provider='yfinance')
            if data and data.results:
                r = data.results[0]
                return {
                    'mean_target': getattr(r, 'target_price_mean', None),
                    'high_target': getattr(r, 'target_price_high', None),
                    'low_target': getattr(r, 'target_price_low', None),
                    'n_analysts': getattr(r, 'number_of_analysts', 0),
                    'recommendation': getattr(r, 'recommendation', ''),
                    'source': 'openbb_yfinance',
                    'fetched_at': datetime.now().isoformat()
                }
        except Exception as e:
            logger.debug('OpenBBCollector.get_analyst_estimates %s: %s', ticker, e)
        return {}

    def get_economic_calendar(self, days_ahead: int = 7) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            start = datetime.now().strftime('%Y-%m-%d')
            end = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
            data = self.obb.economy.calendar(start_date=start, end_date=end)
            if data and data.results:
                return [{
                    'date': str(getattr(r, 'date', '')),
                    'event': getattr(r, 'event', ''),
                    'country': getattr(r, 'country', ''),
                    'importance': str(getattr(r, 'importance', '')),
                    'forecast': getattr(r, 'forecast', None),
                    'previous': getattr(r, 'previous', None),
                } for r in data.results]
        except Exception as e:
            logger.debug('OpenBBCollector.get_economic_calendar: %s', e)
        return []

    def get_analyst_target_changes(self, ticker: str) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            data = self.obb.equity.estimates.price_target(
                symbol=ticker, provider='yfinance', limit=20)
            if data and data.results:
                return [{
                    'date': str(getattr(r, 'published_date', '')),
                    'analyst': getattr(r, 'analyst', ''),
                    'firm': getattr(r, 'analyst_company', ''),
                    'old_target': getattr(r, 'price_target_previous', None),
                    'new_target': getattr(r, 'price_target', None),
                    'rating': getattr(r, 'rating', ''),
                } for r in data.results]
        except Exception as e:
            logger.debug('OpenBBCollector.get_analyst_target_changes %s: %s', ticker, e)
        return []
