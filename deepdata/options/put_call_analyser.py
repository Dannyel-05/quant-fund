"""Put/call ratio analysis and smart money positioning classification."""

import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata/options")


def _make_result(
    source: str,
    ticker: str,
    market: str,
    data_type: str,
    value,
    raw_data=None,
    quality_score: float = 1.0,
) -> dict:
    return {
        "source": source,
        "ticker": ticker,
        "market": market,
        "data_type": data_type,
        "value": value,
        "raw_data": raw_data or {},
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
    }


class PutCallAnalyser:
    """Analyse put/call ratios, rolling z-scores, and positioning classification."""

    def __init__(self, config: dict):
        self.config = config
        self.uk_confidence_weight = config.get("uk_confidence_weight", 0.6)
        self.rate_limit_sleep = config.get("rate_limit_sleep", 0.5)
        self.zscore_window = config.get("pc_zscore_window", 30)
        self.rolling_days = config.get("pc_rolling_days", 5)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def analyse(self, tickers: list) -> list:
        """Return CollectorResult list with put/call analysis per ticker."""
        results = []
        if yf is None:
            logger.warning("yfinance not installed; cannot run PutCallAnalyser")
            return results

        for ticker in tickers:
            market = "uk" if ticker.endswith(".L") else "us"
            confidence = self.uk_confidence_weight if market == "uk" else 1.0
            try:
                t = yf.Ticker(ticker)
                expiries = t.options
                if not expiries:
                    time.sleep(self.rate_limit_sleep)
                    continue

                all_calls, all_puts = [], []
                for expiry in expiries[:4]:
                    try:
                        chain = t.option_chain(expiry)
                        all_calls.append(chain.calls)
                        all_puts.append(chain.puts)
                        time.sleep(self.rate_limit_sleep)
                    except Exception as exc:
                        logger.warning("option_chain(%s, %s): %s", ticker, expiry, exc)

                calls_df = pd.concat(all_calls, ignore_index=True) if all_calls else pd.DataFrame()
                puts_df = pd.concat(all_puts, ignore_index=True) if all_puts else pd.DataFrame()

                call_vol = float(calls_df["volume"].fillna(0).sum()) if not calls_df.empty and "volume" in calls_df.columns else 0.0
                put_vol = float(puts_df["volume"].fillna(0).sum()) if not puts_df.empty and "volume" in puts_df.columns else 0.0
                pc_ratio = put_vol / call_vol if call_vol > 1 else float("nan")

                # Build a minimal history proxy (single point - real system would cache)
                history = [pc_ratio] if not np.isnan(pc_ratio) else []
                zscore = self.calc_rolling_pc_zscore(ticker, history)
                positioning = self.classify_positioning(pc_ratio, zscore) if not np.isnan(pc_ratio) else "NEUTRAL"

                # Price trend (20d return)
                try:
                    hist = t.history(period="30d")
                    if not hist.empty and len(hist) >= 20:
                        price_trend = float((hist["Close"].iloc[-1] / hist["Close"].iloc[-20]) - 1)
                    else:
                        price_trend = 0.0
                except Exception:
                    price_trend = 0.0

                divergence = self.divergence_signal(positioning, price_trend)

                raw = {
                    "pc_ratio": pc_ratio if not np.isnan(pc_ratio) else None,
                    "zscore": zscore,
                    "positioning": positioning,
                    "divergence": divergence,
                    "price_trend_20d": price_trend,
                    "call_volume": call_vol,
                    "put_volume": put_vol,
                }

                results.append(_make_result(
                    source="put_call_analyser",
                    ticker=ticker,
                    market=market,
                    data_type="put_call_analysis",
                    value=pc_ratio if not np.isnan(pc_ratio) else 0.0,
                    raw_data=raw,
                    quality_score=confidence,
                ))

            except Exception as exc:
                logger.warning("PutCallAnalyser.analyse(%s): %s", ticker, exc)

        return results

    def calc_rolling_pc_zscore(self, ticker: str, history: list) -> float:
        """5-day rolling put/call ratio z-score over last 30 days history."""
        if not history or len(history) < 2:
            return 0.0
        arr = np.array([x for x in history if x is not None and not np.isnan(x)], dtype=float)
        if len(arr) < 2:
            return 0.0
        mean = np.mean(arr)
        std = np.std(arr)
        if std < 1e-9:
            return 0.0
        return float((arr[-1] - mean) / std)

    def classify_positioning(self, pc_ratio: float, zscore: float) -> str:
        """Return 'BEARISH_SMART', 'BULLISH_SMART', 'NEUTRAL', 'RETAIL_NOISE'"""
        if np.isnan(pc_ratio):
            return "NEUTRAL"

        # High put/call + extreme z-score = smart money bearish
        if pc_ratio > 1.5 and zscore > 1.5:
            return "BEARISH_SMART"
        # Low put/call + negative z-score = smart money bullish (call buying)
        if pc_ratio < 0.5 and zscore < -1.5:
            return "BULLISH_SMART"
        # Very high z-score with moderate pc = retail chasing
        if abs(zscore) > 2.5:
            return "RETAIL_NOISE"
        return "NEUTRAL"

    def divergence_signal(self, pc_sentiment: str, price_trend: float) -> str:
        """Detect put/call vs price divergence. Returns 'DISTRIBUTION', 'ACCUMULATION', 'NONE'"""
        # Distribution: price going up but smart money buying puts (bearish)
        if pc_sentiment == "BEARISH_SMART" and price_trend > 0.02:
            return "DISTRIBUTION"
        # Accumulation: price falling but smart money buying calls (bullish)
        if pc_sentiment == "BULLISH_SMART" and price_trend < -0.02:
            return "ACCUMULATION"
        return "NONE"
