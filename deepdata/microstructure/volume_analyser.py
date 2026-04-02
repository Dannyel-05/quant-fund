"""
Volume and Liquidity Analyser (Refinement 6)

Provides:
  Volume Pattern Analysis:
    - Unusual volume detection (2x, 3x, 5x averages)
    - Volume classification: ACCUMULATION, DISTRIBUTION, CHURNING, CLIMAX, DRY_UP
    - On-Balance Volume (OBV) with trend and divergence detection
    - VWAP (daily and weekly)
    - Amihud illiquidity ratio
    - Dark pool proxy flags

  Enhanced Liquidity Score 0-100:
    - Combines: spread proxy, volume, Amihud, market cap
    - Bands: 0-30 dangerous, 30-60 acceptable, 60-80 good, 80-100 excellent

Usage:
    from deepdata.microstructure.volume_analyser import VolumeAnalyser
    va = VolumeAnalyser(config)
    result = va.analyse(ticker, price_data)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False


# Volume pattern classifications
VOL_ACCUMULATION = "ACCUMULATION"   # price up + high volume
VOL_DISTRIBUTION = "DISTRIBUTION"   # price down + high volume
VOL_CHURNING     = "CHURNING"       # high volume + flat price
VOL_CLIMAX       = "CLIMAX"         # extreme volume spike (possible reversal)
VOL_DRY_UP       = "DRY_UP"        # very low volume (consolidation)
VOL_NORMAL       = "NORMAL"

# Thresholds
UNUSUAL_VOL_MILD   = 2.0
UNUSUAL_VOL_HIGH   = 3.0
UNUSUAL_VOL_EXTREME = 5.0
DRY_UP_THRESHOLD   = 0.50  # < 50% of avg = dry up


class VolumeAnalyser:
    """
    Comprehensive volume and liquidity analysis for a single ticker.
    """

    def __init__(self, config: dict = None):
        self.config = config or {}

    def analyse(
        self,
        ticker: str,
        price_data=None,
        market_cap_usd: float = 0.0,
    ) -> Dict:
        """
        Run full volume and liquidity analysis.
        price_data: pandas DataFrame with columns [open, high, low, close, volume]
                    or yfinance-style.
        Returns a comprehensive analysis dict.
        """
        if not HAS_PANDAS:
            return self._empty(ticker)

        # Load price data if not provided
        if price_data is None or (hasattr(price_data, "empty") and price_data.empty):
            price_data = self._fetch_price_data(ticker)

        if price_data is None or (hasattr(price_data, "empty") and price_data.empty):
            return self._empty(ticker)

        df = self._normalise_df(price_data)
        if df is None or len(df) < 5:
            return self._empty(ticker)

        result = {
            "ticker":           ticker,
            "analysed_at":      datetime.now(timezone.utc).isoformat(),
            "n_days":           len(df),
        }

        # --- Volume pattern analysis ---
        result.update(self._volume_patterns(df))

        # --- OBV ---
        result.update(self._on_balance_volume(df))

        # --- VWAP ---
        result.update(self._vwap(df))

        # --- Amihud illiquidity ---
        result.update(self._amihud(df))

        # --- Dark pool proxy ---
        result.update(self._dark_pool_proxy(df))

        # --- Composite liquidity score ---
        result["liquidity_score"] = self._liquidity_score(result, market_cap_usd, df)
        result["liquidity_band"]  = self._liquidity_band(result["liquidity_score"])

        return result

    # ------------------------------------------------------------------
    # Volume Patterns
    # ------------------------------------------------------------------

    def _volume_patterns(self, df) -> Dict:
        vol = df["volume"].values
        close = df["close"].values
        open_ = df["open"].values if "open" in df.columns else close

        # Averages
        avg5   = float(np.mean(vol[-5:]))   if len(vol) >= 5  else float(np.mean(vol))
        avg20  = float(np.mean(vol[-20:]))  if len(vol) >= 20 else float(np.mean(vol))
        avg90  = float(np.mean(vol[-90:]))  if len(vol) >= 90 else float(np.mean(vol))
        today_vol = float(vol[-1]) if len(vol) > 0 else 0.0

        rel5   = today_vol / max(1, avg5)
        rel20  = today_vol / max(1, avg20)
        rel90  = today_vol / max(1, avg90)

        # Price change today
        if len(close) >= 2:
            price_chg = (close[-1] / max(1e-9, close[-2])) - 1
        else:
            price_chg = 0.0

        # Volume surge flags
        vol_surge_2x  = rel20 >= UNUSUAL_VOL_MILD
        vol_surge_3x  = rel20 >= UNUSUAL_VOL_HIGH
        vol_surge_5x  = rel20 >= UNUSUAL_VOL_EXTREME
        vol_dry_up    = rel20 <= DRY_UP_THRESHOLD

        # Classification
        pattern = VOL_NORMAL
        if rel20 >= UNUSUAL_VOL_EXTREME:
            pattern = VOL_CLIMAX
        elif rel20 >= UNUSUAL_VOL_HIGH:
            if price_chg > 0.01:
                pattern = VOL_ACCUMULATION
            elif price_chg < -0.01:
                pattern = VOL_DISTRIBUTION
            else:
                pattern = VOL_CHURNING
        elif rel20 >= UNUSUAL_VOL_MILD:
            if price_chg > 0.005:
                pattern = VOL_ACCUMULATION
            elif price_chg < -0.005:
                pattern = VOL_DISTRIBUTION
            else:
                pattern = VOL_CHURNING
        elif vol_dry_up:
            pattern = VOL_DRY_UP

        # Volume trend: is volume rising or falling over last 10 days?
        if len(vol) >= 10:
            vol10 = vol[-10:]
            x = np.arange(len(vol10), dtype=float)
            slope = np.polyfit(x, vol10, 1)[0]
            vol_trend = "RISING" if slope > 0 else "FALLING"
        else:
            vol_trend = "UNKNOWN"

        return {
            "volume_today":      today_vol,
            "volume_avg_5d":     avg5,
            "volume_avg_20d":    avg20,
            "volume_avg_90d":    avg90,
            "vol_rel_to_20d":    round(rel20, 3),
            "vol_rel_to_5d":     round(rel5, 3),
            "vol_rel_to_90d":    round(rel90, 3),
            "vol_surge_2x":      int(vol_surge_2x),
            "vol_surge_3x":      int(vol_surge_3x),
            "vol_surge_5x":      int(vol_surge_5x),
            "vol_dry_up":        int(vol_dry_up),
            "volume_pattern":    pattern,
            "volume_trend":      vol_trend,
            "price_change_pct":  round(price_chg, 5),
        }

    # ------------------------------------------------------------------
    # On-Balance Volume
    # ------------------------------------------------------------------

    def _on_balance_volume(self, df) -> Dict:
        close = df["close"].values
        vol   = df["volume"].values

        if len(close) < 2:
            return {"obv": 0.0, "obv_trend": "UNKNOWN", "obv_divergence": False}

        obv = np.zeros(len(close))
        for i in range(1, len(close)):
            if close[i] > close[i-1]:
                obv[i] = obv[i-1] + vol[i]
            elif close[i] < close[i-1]:
                obv[i] = obv[i-1] - vol[i]
            else:
                obv[i] = obv[i-1]

        # OBV trend (10-day slope)
        if len(obv) >= 10:
            x = np.arange(10, dtype=float)
            obv_slope  = np.polyfit(x, obv[-10:], 1)[0]
            obv_trend  = "RISING" if obv_slope > 0 else "FALLING"
            price_slope = np.polyfit(x, close[-10:], 1)[0]
        else:
            obv_slope  = 0.0
            obv_trend  = "UNKNOWN"
            price_slope = 0.0

        # Divergence: OBV and price moving in opposite directions
        obv_divergence = False
        if len(obv) >= 10:
            obv_divergence = (obv_slope > 0 and price_slope < 0) or \
                             (obv_slope < 0 and price_slope > 0)

        return {
            "obv_current":    float(obv[-1]),
            "obv_trend":      obv_trend,
            "obv_divergence": obv_divergence,
            "obv_slope_10d":  float(obv_slope),
        }

    # ------------------------------------------------------------------
    # VWAP
    # ------------------------------------------------------------------

    def _vwap(self, df) -> Dict:
        close  = df["close"].values
        vol    = df["volume"].values
        high   = df["high"].values  if "high"  in df.columns else close
        low    = df["low"].values   if "low"   in df.columns else close

        # Typical price
        tp = (high + low + close) / 3.0

        # Daily VWAP (last trading day)
        daily_vwap = float(tp[-1]) if len(tp) > 0 else 0.0  # simplified

        # Weekly VWAP (last 5 days)
        if len(tp) >= 5 and len(vol) >= 5:
            w_tp  = tp[-5:]
            w_vol = vol[-5:]
            total_vol = float(np.sum(w_vol))
            weekly_vwap = float(np.sum(w_tp * w_vol) / max(1, total_vol))
        else:
            weekly_vwap = float(np.mean(close[-5:])) if len(close) >= 5 else float(close[-1]) if len(close) > 0 else 0.0

        current_price = float(close[-1]) if len(close) > 0 else 0.0
        above_daily_vwap  = current_price > daily_vwap  if daily_vwap > 0 else None
        above_weekly_vwap = current_price > weekly_vwap if weekly_vwap > 0 else None

        return {
            "daily_vwap":        round(daily_vwap, 4),
            "weekly_vwap":       round(weekly_vwap, 4),
            "price_above_daily_vwap":  int(above_daily_vwap)  if above_daily_vwap  is not None else None,
            "price_above_weekly_vwap": int(above_weekly_vwap) if above_weekly_vwap is not None else None,
            "pct_above_daily_vwap":   round((current_price / daily_vwap - 1), 5) if daily_vwap > 0 else None,
        }

    # ------------------------------------------------------------------
    # Amihud Illiquidity Ratio
    # ------------------------------------------------------------------

    def _amihud(self, df) -> Dict:
        close   = df["close"].values
        vol     = df["volume"].values

        if len(close) < 2:
            return {"amihud_ratio": 0.0, "amihud_percentile": 0.5}

        returns = np.abs(np.diff(close) / np.maximum(close[:-1], 1e-9))
        dollar_vols = vol[1:] * close[1:]

        illiq_days = []
        for r, dv in zip(returns, dollar_vols):
            if dv > 0:
                illiq_days.append(r / dv * 1e6)  # scale to readable numbers

        if not illiq_days:
            return {"amihud_ratio": 0.0, "amihud_percentile": 0.5}

        amihud = float(np.mean(illiq_days))
        amihud_20d = float(np.mean(illiq_days[-20:])) if len(illiq_days) >= 20 else amihud

        # Heuristic percentile (0=very liquid, 1=very illiquid)
        # Typical range: 0.001 (mega-cap) to 100+ (micro-cap)
        amihud_score = min(1.0, math.log10(max(0.001, amihud_20d) + 1) / 3.0)

        return {
            "amihud_ratio":        round(amihud_20d, 6),
            "amihud_illiquidity":  round(amihud_score, 4),  # 0=liquid, 1=illiquid
        }

    # ------------------------------------------------------------------
    # Dark Pool Proxy
    # ------------------------------------------------------------------

    def _dark_pool_proxy(self, df) -> Dict:
        """
        When reported volume significantly exceeds expected on no-news days,
        flag as possible dark pool prints becoming visible.
        """
        vol = df["volume"].values
        if len(vol) < 20:
            return {"dark_pool_flag": False, "dark_pool_days": 0}

        avg20 = float(np.mean(vol[-20:]))
        std20 = float(np.std(vol[-20:]))

        # Count days in last 5 where volume > mean + 2.5 std
        dark_pool_days = 0
        for v in vol[-5:]:
            if std20 > 0 and (v - avg20) / std20 > 2.5:
                dark_pool_days += 1

        return {
            "dark_pool_flag": dark_pool_days >= 2,
            "dark_pool_days": dark_pool_days,
        }

    # ------------------------------------------------------------------
    # Composite Liquidity Score
    # ------------------------------------------------------------------

    def _liquidity_score(self, result: Dict, market_cap_usd: float, df) -> float:
        """
        Compute a 0-100 liquidity score combining:
          - Volume (40%): avg daily dollar volume
          - Amihud (30%): illiquidity ratio (inverted)
          - Spread proxy (20%): intraday high-low range
          - Market cap (10%): size proxy
        """
        score = 0.0

        # Volume component: avg dollar volume score
        avg_vol_usd = result.get("volume_avg_20d", 0) * result.get("daily_vwap", 1.0)
        if avg_vol_usd >= 100_000_000:   vol_score = 100
        elif avg_vol_usd >= 10_000_000:  vol_score = 80
        elif avg_vol_usd >= 1_000_000:   vol_score = 60
        elif avg_vol_usd >= 100_000:     vol_score = 40
        elif avg_vol_usd >= 10_000:      vol_score = 20
        else:                            vol_score = 5
        score += vol_score * 0.40

        # Amihud component (inverted)
        amihud_illiq = result.get("amihud_illiquidity", 0.5)
        amihud_score = (1.0 - amihud_illiq) * 100
        score += amihud_score * 0.30

        # Spread proxy: avg (high-low)/close over last 20 days
        if HAS_PANDAS and len(df) >= 5:
            if "high" in df.columns and "low" in df.columns and "close" in df.columns:
                spreads = ((df["high"] - df["low"]) / df["close"].clip(lower=1e-9)).values[-20:]
                avg_spread = float(np.mean(spreads))
                # < 0.5% = excellent, 0.5-2% = good, 2-5% = fair, > 5% = poor
                if avg_spread < 0.005:   spread_score = 100
                elif avg_spread < 0.02:  spread_score = 75
                elif avg_spread < 0.05:  spread_score = 50
                elif avg_spread < 0.10:  spread_score = 25
                else:                    spread_score = 10
            else:
                spread_score = 50
        else:
            spread_score = 50
        score += spread_score * 0.20

        # Market cap component
        if market_cap_usd >= 2_000_000_000:    cap_score = 100
        elif market_cap_usd >= 500_000_000:    cap_score = 80
        elif market_cap_usd >= 100_000_000:    cap_score = 60
        elif market_cap_usd >= 50_000_000:     cap_score = 40
        elif market_cap_usd >= 10_000_000:     cap_score = 20
        else:                                  cap_score = 5
        score += cap_score * 0.10

        return round(min(100.0, max(0.0, score)), 1)

    def _liquidity_band(self, score: float) -> str:
        if score >= 80:   return "EXCELLENT"
        elif score >= 60: return "GOOD"
        elif score >= 30: return "ACCEPTABLE"
        else:             return "DANGEROUS"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_price_data(self, ticker: str):
        if not HAS_YF:
            return None
        try:
            df = yf.download(ticker, period="1y", auto_adjust=True, progress=False)
            if df is not None and not df.empty:
                return df
        except Exception:
            pass
        return None

    def _normalise_df(self, df):
        """Normalise various DataFrame formats to standard columns."""
        if not HAS_PANDAS:
            return None
        try:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [str(c).lower().strip() for c in df.columns]

            needed = {"close", "volume"}
            if not needed.issubset(set(df.columns)):
                # Try uppercase
                df.columns = [c.capitalize() for c in df.columns]
                df.columns = [str(c).lower() for c in df.columns]

            if "close" not in df.columns:
                return None

            # Fill volume if missing
            if "volume" not in df.columns:
                df["volume"] = 0

            return df.dropna(subset=["close"]).copy()
        except Exception as e:
            logger.debug("volume_analyser: normalise error: %s", e)
            return None

    def _empty(self, ticker: str) -> Dict:
        return {
            "ticker":           ticker,
            "analysed_at":      datetime.now(timezone.utc).isoformat(),
            "n_days":           0,
            "volume_pattern":   "UNKNOWN",
            "obv_trend":        "UNKNOWN",
            "obv_divergence":   False,
            "amihud_ratio":     0.0,
            "amihud_illiquidity": 0.5,
            "liquidity_score":  0.0,
            "liquidity_band":   "DANGEROUS",
        }

    def score_for_signal(self, ticker: str, price_data=None, market_cap: float = 0) -> Dict:
        """
        Convenience method returning key metrics for signal generation.
        """
        full = self.analyse(ticker, price_data, market_cap)
        return {
            "ticker":           ticker,
            "volume_pattern":   full.get("volume_pattern", "NORMAL"),
            "vol_surge":        full.get("vol_surge_2x", 0),
            "vol_dry_up":       full.get("vol_dry_up", 0),
            "obv_trend":        full.get("obv_trend", "UNKNOWN"),
            "obv_divergence":   full.get("obv_divergence", False),
            "above_vwap":       full.get("price_above_daily_vwap"),
            "amihud":           full.get("amihud_illiquidity", 0.5),
            "liquidity_score":  full.get("liquidity_score", 0.0),
            "liquidity_band":   full.get("liquidity_band", "DANGEROUS"),
            "dark_pool_flag":   full.get("dark_pool_flag", False),
        }
