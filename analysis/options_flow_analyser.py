"""
OptionsFlowAnalyser — options market sentiment signal generator.

Aggregates put/call ratio, IV percentile, and unusual activity into a
single options_sentiment_score ranging from -1 (very bearish) to +1 (very bullish).

Wires into signal generation:
  options_sentiment_score ± 0.05-0.10 added to combined_score
  position_size_adjustment() reduces size up to 30% when options very bearish on a long
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

# Thresholds
_PCR_BEARISH = 1.5      # PCR > 1.5 = bearish
_PCR_BULLISH = 0.5      # PCR < 0.5 = bullish
_IV_HIGH_PCT = 80.0     # IV > 80th pct = expensive options (mean reversion expected)
_IV_LOW_PCT  = 20.0     # IV < 20th pct = cheap options (expansion expected)
_UNUSUAL_VOI = 10.0     # volume > 10× open_interest = unusual

# Position size reduction cap when options very bearish on long
_MAX_SIZE_REDUCTION = 0.30


class OptionsFlowAnalyser:
    """
    Options market sentiment analyser.

    Uses yfinance options chains (free, no API key required).
    Gracefully returns 0.0 / None if data unavailable.
    """

    def __init__(self, config: Optional[Dict] = None) -> None:
        self._config = config or {}
        # Cache: {ticker: {pcr, iv_pct, unusual, score, timestamp}}
        self._cache: Dict[str, Dict] = {}

    # ── put/call ratio ────────────────────────────────────────────────────

    def get_put_call_ratio(self, ticker: str) -> Optional[float]:
        """
        Returns put/call volume ratio for the nearest expiration.
        Returns None if data unavailable.
        """
        try:
            import yfinance as yf
            tk = yf.Ticker(ticker)
            exps = tk.options
            if not exps:
                return None
            chain = tk.option_chain(exps[0])
            calls_vol = float(chain.calls["volume"].sum() or 0)
            puts_vol  = float(chain.puts["volume"].sum() or 0)
            if calls_vol <= 0:
                return None
            return puts_vol / calls_vol
        except Exception as exc:
            logger.debug("get_put_call_ratio %s: %s", ticker, exc)
            return None

    # ── IV percentile ─────────────────────────────────────────────────────

    def get_iv_percentile(self, ticker: str) -> Optional[float]:
        """
        Returns current ATM IV as a percentile (0-100) of observed IVs
        across all available option expirations.
        Returns None if unavailable.
        """
        try:
            import yfinance as yf
            tk = yf.Ticker(ticker)
            exps = tk.options
            if not exps:
                return None

            all_ivs: List[float] = []
            for exp in exps[:6]:  # first 6 expirations
                try:
                    chain = tk.option_chain(exp)
                    # ATM = strike closest to current price
                    info = tk.fast_info
                    spot = getattr(info, "last_price", None) or getattr(info, "regularMarketPrice", None)
                    if spot is None:
                        continue
                    calls = chain.calls.copy()
                    calls["dist"] = (calls["strike"] - spot).abs()
                    atm_row = calls.nsmallest(1, "dist")
                    if not atm_row.empty:
                        iv = atm_row["impliedVolatility"].iloc[0]
                        if iv and iv > 0:
                            all_ivs.append(float(iv))
                except Exception:
                    continue

            if len(all_ivs) < 2:
                return None

            current_iv = all_ivs[0]
            # Percentile of current_iv within observed distribution
            pct = float(np.mean(np.array(all_ivs) <= current_iv) * 100)
            return pct

        except Exception as exc:
            logger.debug("get_iv_percentile %s: %s", ticker, exc)
            return None

    # ── unusual activity ──────────────────────────────────────────────────

    def detect_unusual_activity(self, ticker: str) -> bool:
        """
        Returns True if any single option contract has volume > 10× open interest.
        Signals informed/institutional flow.
        """
        try:
            import yfinance as yf
            tk = yf.Ticker(ticker)
            exps = tk.options
            if not exps:
                return False
            chain = tk.option_chain(exps[0])
            for df in (chain.calls, chain.puts):
                df = df.copy()
                df["vol"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
                df["oi"]  = pd.to_numeric(df.get("openInterest", 0), errors="coerce").fillna(0)
                df["oi"]  = df["oi"].replace(0, np.nan)
                ratio = df["vol"] / df["oi"]
                if ratio.dropna().gt(_UNUSUAL_VOI).any():
                    return True
        except Exception as exc:
            logger.debug("detect_unusual_activity %s: %s", ticker, exc)
        return False

    # ── combined sentiment score ──────────────────────────────────────────

    def options_sentiment_score(self, ticker: str) -> float:
        """
        Combines PCR + IV percentile + unusual activity into a single score.

        Returns float in [-1, +1]:
          PCR > 1.5  → bearish component (-0.3 to -0.5)
          PCR < 0.5  → bullish component (+0.3 to +0.5)
          IV > 80th  → mean reversion signal (+0.1)
          IV < 20th  → expansion expected (0.0, neutral — avoid selling premium)
          Unusual activity → amplify current direction × 1.5 (capped)
        """
        score = 0.0

        # PCR component
        pcr = self.get_put_call_ratio(ticker)
        if pcr is not None:
            if pcr > _PCR_BEARISH:
                bearish_strength = min((pcr - _PCR_BEARISH) / 2.0, 1.0)
                score -= 0.3 + 0.2 * bearish_strength   # -0.3 to -0.5
            elif pcr < _PCR_BULLISH:
                bullish_strength = min((_PCR_BULLISH - pcr) / 0.5, 1.0)
                score += 0.3 + 0.2 * bullish_strength   # +0.3 to +0.5

        # IV percentile component
        iv_pct = self.get_iv_percentile(ticker)
        if iv_pct is not None:
            if iv_pct > _IV_HIGH_PCT:
                score += 0.1   # expensive IV → mean reversion likely → mild bullish
            # IV < 20th pct: neutral (don't add anything — signals uncertainty)

        # Unusual activity: amplify by 1.5× (capped at ±1)
        if self.detect_unusual_activity(ticker):
            score = np.clip(score * 1.5, -1.0, 1.0)

        return float(np.clip(score, -1.0, 1.0))

    # ── position size adjustment ──────────────────────────────────────────

    def position_size_adjustment(self, ticker: str, base_size: float) -> float:
        """
        Reduce position size by up to 30% when options market is very bearish
        on a long position.

        Returns adjusted size (never below base_size * 0.70).
        """
        score = self.options_sentiment_score(ticker)
        if score < -0.5:
            # Very bearish options → reduce by up to 30%
            reduction = _MAX_SIZE_REDUCTION * min(abs(score + 0.5) / 0.5, 1.0)
            adjusted = base_size * (1.0 - reduction)
            logger.debug(
                "OptionsFlowAnalyser %s: bearish options (score=%.2f) → size %.2f→%.2f",
                ticker, score, base_size, adjusted,
            )
            return max(adjusted, base_size * (1.0 - _MAX_SIZE_REDUCTION))
        return base_size

    # ── signal output for combined_score ─────────────────────────────────

    def apply_to_signal(
        self, ticker: str, combined_score: float, weight: float = 0.075
    ) -> float:
        """
        Add options_sentiment_score × weight to combined_score.
        Default weight = 0.075 (between 0.05 and 0.10).
        """
        opt_score = self.options_sentiment_score(ticker)
        return float(np.clip(combined_score + opt_score * weight, -1.0, 1.0))

    def status(self) -> Dict[str, Any]:
        return {
            "cached_tickers": list(self._cache.keys()),
            "thresholds": {
                "pcr_bearish": _PCR_BEARISH,
                "pcr_bullish": _PCR_BULLISH,
                "iv_high_pct": _IV_HIGH_PCT,
                "iv_low_pct":  _IV_LOW_PCT,
                "unusual_voi": _UNUSUAL_VOI,
            },
        }


# pandas needed for detect_unusual_activity — lazy import
try:
    import pandas as pd
except ImportError:
    pass
