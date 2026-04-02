"""Momentum signal based on price momentum over multiple timeframes."""
import logging
import numpy as np
import pandas as pd
from typing import List, Dict

logger = logging.getLogger(__name__)


class MomentumSignal:
    def __init__(self, config: dict):
        self.config = config

    def generate(self, ticker: str, price_data: pd.DataFrame) -> List[Dict]:
        signals = []
        try:
            if price_data is None or len(price_data) < 60:
                return []
            close = price_data['close'] if 'close' in price_data.columns else price_data.iloc[:, 3]
            # Multi-timeframe momentum
            r1m = float(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else 0
            r3m = float(close.iloc[-1] / close.iloc[-63] - 1) if len(close) >= 63 else 0
            r6m = float(close.iloc[-1] / close.iloc[-126] - 1) if len(close) >= 126 else 0
            # Composite momentum score (weight recent more)
            score = r1m * 0.5 + r3m * 0.3 + r6m * 0.2
            if abs(score) > 0.05:
                direction = 'LONG' if score > 0 else 'SHORT'
                signals.append({
                    'ticker': ticker, 'direction': direction,
                    'score': min(abs(score) * 3, 1.0),
                    'r1m': r1m, 'r3m': r3m, 'r6m': r6m,
                    'signal_type': 'MOMENTUM'
                })
        except Exception as e:
            logger.debug('MomentumSignal %s: %s', ticker, e)
        return signals
