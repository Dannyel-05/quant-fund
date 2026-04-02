"""Mean reversion signal based on Bollinger Bands and RSI."""
import logging
import numpy as np
import pandas as pd
from typing import List, Dict

logger = logging.getLogger(__name__)


class MeanReversionSignal:
    def __init__(self, config: dict):
        self.config = config

    def generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]:
        signals = []
        try:
            if price_data is None or len(price_data) < 30:
                return []
            close = price_data['close'] if 'close' in price_data.columns else price_data.iloc[:, 3]
            # Bollinger Band position
            rolling_mean = close.rolling(20).mean()
            rolling_std = close.rolling(20).std()
            current = float(close.iloc[-1])
            mean = float(rolling_mean.iloc[-1])
            std = float(rolling_std.iloc[-1]) if float(rolling_std.iloc[-1]) > 0 else 1
            zscore = (current - mean) / std
            # RSI
            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss.replace(0, 1e-10)
            rsi = float(100 - 100 / (1 + rs.iloc[-1]))
            # Signal when price is extended and RSI confirms
            if zscore < -2.0 and rsi < 35:
                signals.append({'ticker': ticker, 'direction': 'LONG',
                    'score': min(abs(zscore) / 3, 1.0),
                    'zscore': zscore, 'rsi': rsi, 'signal_type': 'MEAN_REVERSION'})
            elif zscore > 2.0 and rsi > 65:
                signals.append({'ticker': ticker, 'direction': 'SHORT',
                    'score': min(abs(zscore) / 3, 1.0),
                    'zscore': zscore, 'rsi': rsi, 'signal_type': 'MEAN_REVERSION'})
        except Exception as e:
            logger.debug('MeanReversionSignal %s: %s', ticker, e)
        return signals
