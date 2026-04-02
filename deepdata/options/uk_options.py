"""UK options data collection via yfinance with reduced confidence weighting."""

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


def _suffix_uk(ticker: str) -> str:
    """Ensure UK ticker has .L suffix."""
    return ticker if ticker.endswith(".L") else ticker + ".L"


def _strip_suffix(ticker: str) -> str:
    return ticker.replace(".L", "")


class UKOptionsCollector:
    """Collect UK options data via yfinance, applying 0.6x confidence weighting."""

    def __init__(self, config: dict):
        self.config = config
        self.confidence = config.get("uk_confidence_weight", 0.6)
        self.rate_limit_sleep = config.get("rate_limit_sleep", 0.5)
        self.block_threshold = config.get("block_threshold", 500)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def collect(self, tickers: list) -> list:
        """Collect options data for UK (.L) tickers via yfinance. Apply 0.6x confidence."""
        results = []
        if yf is None:
            logger.warning("yfinance not installed; cannot collect UK options")
            return results

        for ticker in tickers:
            yf_ticker = _suffix_uk(ticker)
            base_ticker = _strip_suffix(ticker)
            try:
                t = yf.Ticker(yf_ticker)
                expiries = t.options
                if not expiries:
                    logger.warning("No options available for UK ticker %s", yf_ticker)
                    time.sleep(self.rate_limit_sleep)
                    continue

                all_calls, all_puts = [], []
                for expiry in expiries[:4]:
                    try:
                        chain = t.option_chain(expiry)
                        c = chain.calls.copy(); c["expiry"] = expiry
                        p = chain.puts.copy(); p["expiry"] = expiry
                        all_calls.append(c)
                        all_puts.append(p)
                        time.sleep(self.rate_limit_sleep)
                    except Exception as exc:
                        logger.warning("UK option_chain(%s, %s): %s", yf_ticker, expiry, exc)

                calls_df = pd.concat(all_calls, ignore_index=True) if all_calls else pd.DataFrame()
                puts_df = pd.concat(all_puts, ignore_index=True) if all_puts else pd.DataFrame()

                call_vol = float(calls_df["volume"].fillna(0).sum()) if not calls_df.empty and "volume" in calls_df.columns else 0.0
                put_vol = float(puts_df["volume"].fillna(0).sum()) if not puts_df.empty and "volume" in puts_df.columns else 0.0
                pc_ratio = put_vol / call_vol if call_vol > 0 else float("nan")

                avg_call_iv = float(calls_df["impliedVolatility"].mean()) if not calls_df.empty and "impliedVolatility" in calls_df.columns else 0.0
                avg_put_iv = float(puts_df["impliedVolatility"].mean()) if not puts_df.empty and "impliedVolatility" in puts_df.columns else 0.0

                raw = {
                    "call_volume": call_vol,
                    "put_volume": put_vol,
                    "pc_ratio": None if np.isnan(pc_ratio) else pc_ratio,
                    "avg_call_iv": avg_call_iv,
                    "avg_put_iv": avg_put_iv,
                    "expiries_available": list(expiries[:4]),
                    "note": "UK options via yfinance; liquidity typically lower than US",
                }

                results.append(_make_result(
                    source="uk_options",
                    ticker=base_ticker,
                    market="uk",
                    data_type="uk_options_summary",
                    value=pc_ratio if not np.isnan(pc_ratio) else 0.0,
                    raw_data=raw,
                    quality_score=self.confidence,
                ))

            except Exception as exc:
                logger.warning("UKOptionsCollector.collect(%s): %s", ticker, exc)

        return results

    def compare_us_uk_flow(self, us_results: list, uk_results: list) -> list:
        """Flag when UK options contradict US sector options direction."""
        flags = []
        if not us_results or not uk_results:
            return flags

        # Build lookup: ticker -> pc_ratio
        def _build_lookup(results_list):
            lookup = {}
            for r in results_list:
                ticker = r.get("ticker", "")
                raw = r.get("raw_data", {})
                pc = raw.get("pc_ratio", None) or r.get("value", None)
                if pc is not None and not (isinstance(pc, float) and np.isnan(pc)):
                    lookup[ticker] = float(pc)
            return lookup

        us_lookup = _build_lookup(us_results)
        uk_lookup = _build_lookup(uk_results)

        # US market average pc
        us_vals = list(us_lookup.values())
        us_avg = float(np.mean(us_vals)) if us_vals else 1.0

        for uk_ticker, uk_pc in uk_lookup.items():
            base = _strip_suffix(uk_ticker)
            us_pc = us_lookup.get(base, us_avg)

            us_sentiment = "bearish" if us_pc > 1.2 else ("bullish" if us_pc < 0.8 else "neutral")
            uk_sentiment = "bearish" if uk_pc > 1.2 else ("bullish" if uk_pc < 0.8 else "neutral")

            contradiction = (
                (us_sentiment == "bullish" and uk_sentiment == "bearish") or
                (us_sentiment == "bearish" and uk_sentiment == "bullish")
            )

            if contradiction:
                flags.append(_make_result(
                    source="uk_options",
                    ticker=base,
                    market="cross",
                    data_type="us_uk_flow_divergence",
                    value=uk_pc - us_pc,
                    raw_data={
                        "us_pc_ratio": us_pc,
                        "uk_pc_ratio": uk_pc,
                        "us_sentiment": us_sentiment,
                        "uk_sentiment": uk_sentiment,
                        "contradiction": contradiction,
                    },
                    quality_score=self.confidence,
                ))

        return flags
