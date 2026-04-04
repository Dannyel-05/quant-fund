"""
Gap trading signal — detects significant overnight price gaps and generates
regime-conditioned entries.

Behaviour by regime:
  NEUTRAL / BEAR  → fade the gap (original behaviour)
  BULL            → buy continuation of up-gaps; fade down-gaps
  CRISIS          → skip all gap signals

Additional filters:
  - Gap size:  only act on gaps 2-8% (abs(gap_pct) between 0.02 and 0.08)
  - Volume:    skip if opening volume > 1.5x 20-day average (high-volume gaps tend to continue)
  - Time:      within_open_window flag (first 30 min) respected when passed
  - Sector:    if whole sector gaps same direction → skip fade (continuation more likely)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Gap size bounds
MIN_GAP_PCT = 0.02   # 2%
MAX_GAP_PCT = 0.08   # 8%

# Volume filter
MAX_VOLUME_RATIO = 1.5   # skip if open volume > 1.5x 20d avg

# Sector ETF proxies for common sectors
_SECTOR_ETFS = {
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE",
    "XLC", "SMH", "IBB", "KRE", "XBI",
}


class GapSignal:
    def __init__(self, config: dict) -> None:
        self.config = config
        self._regime_detector = None
        self._regime_cache: Optional[str] = None
        # Maps sector ETF → last gap direction (+1 / -1 / 0)
        self._sector_gap_cache: Dict[str, int] = {}

    # ── regime helpers ────────────────────────────────────────────────────

    def _get_regime(self) -> str:
        if self._regime_detector is None:
            try:
                from analysis.regime_detector import RegimeDetector
                self._regime_detector = RegimeDetector()
            except Exception:
                return "NEUTRAL"
        try:
            return self._regime_detector.detect()
        except Exception:
            return "NEUTRAL"

    # ── sector direction helper ───────────────────────────────────────────

    def update_sector_gap(self, sector_etf: str, gap_direction: int) -> None:
        """Call once per cycle with the sector ETF's own gap direction (+1 up / -1 down)."""
        if sector_etf in _SECTOR_ETFS:
            self._sector_gap_cache[sector_etf] = gap_direction

    def _sector_aligned(self, ticker: str, gap_direction: int) -> bool:
        """Return True if a sector ETF is gapping the same direction (→ continuation likely)."""
        for etf, etf_dir in self._sector_gap_cache.items():
            if etf_dir == gap_direction:
                return True
        return False

    # ── main signal generation ────────────────────────────────────────────

    def generate(
        self,
        ticker: str,
        price_data: pd.DataFrame,
        within_open_window: bool = True,
        opening_volume: Optional[float] = None,
    ) -> List[Dict]:
        """
        Generate gap signals for *ticker*.

        Parameters
        ----------
        ticker            : ticker symbol
        price_data        : OHLCV DataFrame, last row = today
        within_open_window: True when called in first 30 min of session
        opening_volume    : today's opening volume (to check volume filter)
        """
        signals: List[Dict] = []
        try:
            if price_data is None or len(price_data) < 5:
                return []

            open_col  = "open"  if "open"  in price_data.columns else price_data.columns[0]
            close_col = "close" if "close" in price_data.columns else price_data.columns[3]
            vol_col   = "volume" if "volume" in price_data.columns else None

            prev_close  = float(price_data[close_col].iloc[-2])
            today_open  = float(price_data[open_col].iloc[-1])

            if prev_close <= 0:
                return []

            gap_pct = (today_open - prev_close) / prev_close
            abs_gap = abs(gap_pct)

            # ── gap size filter ────────────────────────────────────────────
            if abs_gap < MIN_GAP_PCT or abs_gap > MAX_GAP_PCT:
                logger.debug("GapSignal %s: gap %.2f%% outside [2%%,8%%] — skip", ticker, gap_pct * 100)
                return []

            # ── time filter ────────────────────────────────────────────────
            if not within_open_window:
                logger.debug("GapSignal %s: outside open window — skip", ticker)
                return []

            # ── volume filter ──────────────────────────────────────────────
            if vol_col is not None and opening_volume is not None:
                vol_series = price_data[vol_col].astype(float)
                avg_vol_20d = vol_series.iloc[-21:-1].mean()
                if avg_vol_20d > 0:
                    vol_ratio = opening_volume / avg_vol_20d
                    if vol_ratio > MAX_VOLUME_RATIO:
                        logger.debug(
                            "GapSignal %s: vol_ratio=%.2f > %.1f — high volume gap, skip fade",
                            ticker, vol_ratio, MAX_VOLUME_RATIO,
                        )
                        return []

            # ── regime gate ────────────────────────────────────────────────
            regime = self._get_regime()
            gap_direction = 1 if gap_pct > 0 else -1

            if regime == "CRISIS":
                logger.debug("GapSignal %s: CRISIS regime — skip all gap signals", ticker)
                return []

            # ── sector alignment check (suppress fade when sector confirms gap) ──
            if self._sector_aligned(ticker, gap_direction):
                logger.debug(
                    "GapSignal %s: sector gapping same direction (%.2f%%) — skip fade",
                    ticker, gap_pct * 100,
                )
                return []

            # ── direction logic ────────────────────────────────────────────
            if regime == "BULL":
                # In BULL: ride continuation of up-gaps; still fade down-gaps
                if gap_pct > 0:
                    direction = "LONG"    # continuation
                    signal_subtype = "GAP_CONTINUATION"
                    score = min(abs_gap * 8, 0.7)   # lower confidence than fade
                else:
                    direction = "LONG"    # fade down-gap (stock fell, buy dip)
                    signal_subtype = "GAP_FADE"
                    score = min(abs_gap * 10, 1.0)
            else:
                # NEUTRAL / BEAR: fade the gap
                direction = "SHORT" if gap_pct > 0 else "LONG"
                signal_subtype = "GAP_FADE"
                score = min(abs_gap * 10, 1.0)

            gap_context = {
                "regime":         regime,
                "gap_pct":        round(gap_pct, 4),
                "abs_gap_pct":    round(abs_gap, 4),
                "gap_direction":  gap_direction,
                "sector_aligned": False,
                "action":         signal_subtype,
            }

            signals.append({
                "ticker":       ticker,
                "direction":    direction,
                "score":        score,
                "gap_pct":      gap_pct,
                "signal_type":  "GAP",
                "signal_subtype": signal_subtype,
                "gap_context":  gap_context,
            })

            logger.debug(
                "GapSignal %s: %.2f%% gap → %s %s (regime=%s, score=%.2f)",
                ticker, gap_pct * 100, direction, signal_subtype, regime, score,
            )

        except Exception as exc:
            logger.debug("GapSignal %s: %s", ticker, exc)

        return signals
