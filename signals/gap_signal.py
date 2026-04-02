"""Gap trading signal - detects and trades significant price gaps."""
import logging
import pandas as pd
from typing import List, Dict

logger = logging.getLogger(__name__)


class GapSignal:
    def __init__(self, config: dict):
        self.config = config
        self.min_gap_pct = 0.02  # 2% minimum gap

    def generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]:
        signals = []
        try:
            if price_data is None or len(price_data) < 5:
                return []
            open_col = 'open' if 'open' in price_data.columns else price_data.columns[0]
            close_col = 'close' if 'close' in price_data.columns else price_data.columns[3]
            prev_close = float(price_data[close_col].iloc[-2])
            today_open = float(price_data[open_col].iloc[-1])
            gap_pct = (today_open - prev_close) / prev_close if prev_close > 0 else 0
            if abs(gap_pct) > self.min_gap_pct:
                # Gap fill trade (fade the gap)
                direction = 'SHORT' if gap_pct > 0 else 'LONG'
                signals.append({
                    'ticker': ticker, 'direction': direction,
                    'score': min(abs(gap_pct) * 10, 1.0),
                    'gap_pct': gap_pct, 'signal_type': 'GAP'
                })
        except Exception as e:
            logger.debug('GapSignal %s: %s', ticker, e)
        return signals
