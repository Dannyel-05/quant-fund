"""Factor arbitrage - long/short factor-based strategies."""
import logging
import pandas as pd
import numpy as np
from typing import Dict, List

logger = logging.getLogger(__name__)


class FactorArbitrage:
    def __init__(self, config: dict):
        self.config = config

    def calculate_factor_scores(self, tickers: List[str]) -> Dict[str, float]:
        """Calculate composite factor scores for tickers."""
        scores = {}
        try:
            import yfinance as yf
            for ticker in tickers[:50]:  # limit for speed
                try:
                    t = yf.Ticker(ticker)
                    info = t.fast_info
                    # Value factor (P/E proxy via market cap / earnings)
                    pe = getattr(info, 'pe_ratio', None) or 20
                    value_score = 1 / max(pe, 1) * 20  # normalize
                    # Momentum factor
                    hist = t.history(period='6mo', auto_adjust=True)
                    if len(hist) >= 60:
                        mom = float(hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1)
                    else:
                        mom = 0
                    scores[ticker] = value_score * 0.4 + mom * 0.6
                except Exception:
                    scores[ticker] = 0
        except Exception as e:
            logger.warning('FactorArbitrage: %s', e)
        return scores

    def get_long_short_pairs(self, tickers: List[str]) -> List[Dict]:
        """Return ranked long/short pairs by factor spread."""
        scores = self.calculate_factor_scores(tickers)
        if not scores:
            return []
        sorted_scores = sorted(scores.items(), key=lambda x: x[1])
        n = max(1, len(sorted_scores) // 5)
        shorts = sorted_scores[:n]    # lowest factor scores
        longs = sorted_scores[-n:]    # highest factor scores
        pairs = []
        for (l_tick, l_score), (s_tick, s_score) in zip(longs, shorts):
            pairs.append({'long': l_tick, 'short': s_tick,
                          'spread': l_score - s_score})
        return sorted(pairs, key=lambda x: x['spread'], reverse=True)
