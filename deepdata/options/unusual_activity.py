"""Unusual options activity detection."""

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


class UnusualActivityDetector:
    """Detect unusual options activity: sweeps, block trades, expiry clustering."""

    def __init__(self, config: dict):
        self.config = config
        self.block_threshold = config.get("block_threshold", 500)
        self.sweep_vol_oi_ratio = config.get("sweep_vol_oi_ratio", 2.0)
        self.uk_confidence_weight = config.get("uk_confidence_weight", 0.6)
        self.rate_limit_sleep = config.get("rate_limit_sleep", 0.5)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def scan(self, tickers: list, market: str = "us") -> list:
        """Scan for unusual options activity across tickers."""
        if yf is None:
            logger.warning("yfinance not installed; cannot scan unusual activity")
            return []

        results = []
        confidence = self.uk_confidence_weight if market == "uk" else 1.0

        for ticker in tickers:
            yf_ticker = ticker + ".L" if market == "uk" and not ticker.endswith(".L") else ticker
            try:
                t = yf.Ticker(yf_ticker)
                expiries = t.options
                if not expiries:
                    logger.warning("No options for %s", yf_ticker)
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
                        logger.warning("option_chain(%s, %s): %s", yf_ticker, expiry, exc)

                chain_dict = {
                    "calls": pd.concat(all_calls, ignore_index=True) if all_calls else pd.DataFrame(),
                    "puts": pd.concat(all_puts, ignore_index=True) if all_puts else pd.DataFrame(),
                }

                sweeps = self.detect_sweeps(chain_dict, ticker)
                blocks = self.detect_block_trades(chain_dict)
                clustering = self.detect_expiry_clustering(chain_dict)
                events = sweeps + blocks

                score_dict = self.score_activity(ticker, events)
                score_val = score_dict.get("score", 0.0)

                raw = {
                    "sweeps": sweeps,
                    "blocks": blocks,
                    "expiry_clustering": clustering,
                    "score_detail": score_dict,
                }
                results.append(_make_result(
                    source="unusual_activity",
                    ticker=ticker,
                    market=market,
                    data_type="unusual_activity_score",
                    value=score_val,
                    raw_data=raw,
                    quality_score=confidence,
                ))

            except Exception as exc:
                logger.warning("scan unusual_activity for %s: %s", ticker, exc)

        return results

    def detect_sweeps(self, chain: dict, ticker: str) -> list:
        """Detect sweep orders: high volume relative to OI, spread across multiple strikes/expiries."""
        sweeps = []
        for side, df in [("call", chain.get("calls", pd.DataFrame())), ("put", chain.get("puts", pd.DataFrame()))]:
            if df.empty or "volume" not in df.columns or "openInterest" not in df.columns:
                continue
            df = df.copy().fillna(0)
            if "expiry" not in df.columns:
                continue

            # Group by expiry, find high volume relative to OI
            for expiry, grp in df.groupby("expiry"):
                oi_sum = grp["openInterest"].sum()
                vol_sum = grp["volume"].sum()
                if oi_sum <= 0:
                    continue
                ratio = vol_sum / oi_sum
                if ratio >= self.sweep_vol_oi_ratio:
                    strikes_hit = grp.loc[grp["volume"] > 0, "strike"].nunique() if "strike" in grp.columns else 0
                    if strikes_hit >= 2:
                        sweeps.append({
                            "type": "sweep",
                            "side": side,
                            "ticker": ticker,
                            "expiry": expiry,
                            "volume": float(vol_sum),
                            "open_interest": float(oi_sum),
                            "vol_oi_ratio": float(ratio),
                            "strikes_hit": int(strikes_hit),
                        })
        return sweeps

    def detect_block_trades(self, chain: dict) -> list:
        """Flag single orders > block_threshold contracts."""
        blocks = []
        for side, df in [("call", chain.get("calls", pd.DataFrame())), ("put", chain.get("puts", pd.DataFrame()))]:
            if df.empty or "volume" not in df.columns:
                continue
            df = df.copy().fillna(0)
            big = df[df["volume"] >= self.block_threshold]
            for _, row in big.iterrows():
                blocks.append({
                    "type": "block",
                    "side": side,
                    "strike": float(row.get("strike", 0)),
                    "expiry": row.get("expiry", None),
                    "volume": float(row.get("volume", 0)),
                    "openInterest": float(row.get("openInterest", 0)),
                    "impliedVolatility": float(row.get("impliedVolatility", 0)),
                })
        return blocks

    def detect_expiry_clustering(self, chain: dict) -> dict:
        """Detect when volume is concentrated at one specific expiry (event-driven bet)."""
        result = {"clustering_detected": False, "dominant_expiry": None, "concentration_ratio": 0.0, "detail": {}}
        all_dfs = []
        for df in [chain.get("calls", pd.DataFrame()), chain.get("puts", pd.DataFrame())]:
            if not df.empty and "volume" in df.columns and "expiry" in df.columns:
                all_dfs.append(df[["expiry", "volume"]].copy().fillna(0))
        if not all_dfs:
            return result

        combined = pd.concat(all_dfs, ignore_index=True)
        by_expiry = combined.groupby("expiry")["volume"].sum()
        total_vol = by_expiry.sum()
        if total_vol <= 0:
            return result

        dominant_expiry = by_expiry.idxmax()
        concentration = float(by_expiry[dominant_expiry] / total_vol)
        clustered = concentration >= 0.6  # >60% of volume at one expiry

        result.update({
            "clustering_detected": bool(clustered),
            "dominant_expiry": str(dominant_expiry),
            "concentration_ratio": concentration,
            "detail": by_expiry.to_dict(),
        })
        return result

    def score_activity(self, ticker: str, events: list) -> dict:
        """Aggregate all unusual events into an UnusualActivityScore 0-1."""
        if not events:
            return {"ticker": ticker, "score": 0.0, "event_count": 0, "sweep_count": 0, "block_count": 0}

        sweep_count = sum(1 for e in events if e.get("type") == "sweep")
        block_count = sum(1 for e in events if e.get("type") == "block")
        call_events = sum(1 for e in events if e.get("side") == "call")
        put_events = sum(1 for e in events if e.get("side") == "put")

        # Weighted score
        sweep_score = min(sweep_count / 5.0, 1.0) * 0.4
        block_score = min(block_count / 10.0, 1.0) * 0.4
        directional_score = (1.0 if call_events > put_events else 0.5) * 0.2

        score = sweep_score + block_score + directional_score

        return {
            "ticker": ticker,
            "score": float(np.clip(score, 0.0, 1.0)),
            "event_count": len(events),
            "sweep_count": sweep_count,
            "block_count": block_count,
            "call_events": call_events,
            "put_events": put_events,
        }
