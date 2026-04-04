"""
RegimeDetector — classifies current market into 4 states.

States:  BULL | NEUTRAL | BEAR | CRISIS

Detection criteria:
  CRISIS  : VIX > 35  OR  SPY < MA200 * 0.92
  BEAR    : SPY < MA200  AND  VIX > 25
  BULL    : SPY > MA200  AND  VIX < 20
  NEUTRAL : everything else

Credit-spread confirmation (optional): if credit_spread_bps > 400 →
pushes result toward CRISIS/BEAR.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_REGIMES = ("BULL", "NEUTRAL", "BEAR", "CRISIS")

# ── thresholds ────────────────────────────────────────────────────────────────
_VIX_CRISIS   = 35.0
_VIX_BEAR     = 25.0
_VIX_BULL     = 20.0
_SPY_CRISIS_DISCOUNT = 0.92   # SPY < MA200 * 0.92 → CRISIS
_CREDIT_SPREAD_CRISIS_BPS = 400


def _fetch_spy_vix(lookback: int = 210) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (spy_close, spy_ma200, vix_close).  Any element can be None.
    """
    try:
        import yfinance as yf
        from datetime import date, timedelta
        end   = date.today()
        start = end - timedelta(days=lookback + 20)

        spy_df = yf.download("SPY", start=str(start), end=str(end),
                             progress=False, auto_adjust=True, threads=False)
        vix_df = yf.download("^VIX", start=str(start), end=str(end),
                             progress=False, auto_adjust=True, threads=False)

        spy_close: Optional[float] = None
        spy_ma200: Optional[float] = None
        vix_close: Optional[float] = None

        if spy_df is not None and len(spy_df) > 0:
            closes = spy_df["Close"].dropna()
            spy_close = float(closes.iloc[-1])
            if len(closes) >= 200:
                spy_ma200 = float(closes.iloc[-200:].mean())

        if vix_df is not None and len(vix_df) > 0:
            vix_close = float(vix_df["Close"].dropna().iloc[-1])

        return spy_close, spy_ma200, vix_close
    except Exception as exc:
        logger.debug("RegimeDetector: SPY/VIX fetch error: %s", exc)
        return None, None, None


class RegimeDetector:
    """
    Detects current market regime.  State is cached for the trading day.
    """

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}
        self._current_regime: str = "NEUTRAL"
        self._last_detected: Optional[date] = None
        self._last_inputs: dict = {}

    # ── main method ───────────────────────────────────────────────────────────

    def detect(
        self,
        force: bool = False,
        credit_spread_bps: float | None = None,
    ) -> str:
        """
        Return current regime string.  Uses cached value if called today
        and force=False.
        """
        today = date.today()
        if not force and self._last_detected == today:
            return self._current_regime

        spy_close, spy_ma200, vix_close = _fetch_spy_vix()
        regime = self._classify(spy_close, spy_ma200, vix_close, credit_spread_bps)

        self._current_regime = regime
        self._last_detected  = today
        self._last_inputs    = {
            "spy_close":          spy_close,
            "spy_ma200":          spy_ma200,
            "vix_close":          vix_close,
            "credit_spread_bps":  credit_spread_bps,
        }

        logger.info(
            "RegimeDetector: %s (SPY=%.2f MA200=%.2f VIX=%.1f)",
            regime,
            spy_close or 0,
            spy_ma200 or 0,
            vix_close or 0,
        )
        return regime

    def _classify(
        self,
        spy_close: Optional[float],
        spy_ma200: Optional[float],
        vix_close: Optional[float],
        credit_spread_bps: Optional[float],
    ) -> str:
        # Default NEUTRAL when data missing
        if spy_close is None and vix_close is None:
            return "NEUTRAL"

        vix = vix_close or 20.0
        spy = spy_close or 0.0
        ma200 = spy_ma200 or spy  # if no MA, treat as neutral relative

        # CRISIS
        if vix >= _VIX_CRISIS:
            return "CRISIS"
        if ma200 > 0 and spy < ma200 * _SPY_CRISIS_DISCOUNT:
            return "CRISIS"
        if credit_spread_bps is not None and credit_spread_bps > _CREDIT_SPREAD_CRISIS_BPS:
            return "CRISIS"

        # BEAR
        if spy < ma200 and vix > _VIX_BEAR:
            return "BEAR"

        # BULL
        if spy > ma200 and vix < _VIX_BULL:
            return "BULL"

        return "NEUTRAL"

    # ── convenience ───────────────────────────────────────────────────────────

    def current(self) -> str:
        """Return last detected regime without re-fetching."""
        return self._current_regime

    def is_bull(self)   -> bool: return self._current_regime == "BULL"
    def is_bear(self)   -> bool: return self._current_regime == "BEAR"
    def is_crisis(self) -> bool: return self._current_regime == "CRISIS"
    def is_neutral(self)-> bool: return self._current_regime == "NEUTRAL"

    def position_size_multiplier(self) -> float:
        """
        Returns position size multiplier based on regime.
          BULL    → 1.0
          NEUTRAL → 0.85
          BEAR    → 0.60
          CRISIS  → 0.30
        """
        return {
            "BULL":    1.00,
            "NEUTRAL": 0.85,
            "BEAR":    0.60,
            "CRISIS":  0.30,
        }.get(self._current_regime, 0.85)

    def status(self) -> dict:
        return {
            "regime":   self._current_regime,
            "detected": str(self._last_detected),
            "inputs":   self._last_inputs,
            "pos_size_multiplier": self.position_size_multiplier(),
        }
