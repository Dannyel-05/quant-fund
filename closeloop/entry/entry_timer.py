"""
EntryTimer — finds optimal entry points for PEAD and all other signals.
Philosophy: 'enter at next open' is lazy. Better entries = better returns
with identical signals.
"""
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
    logger.warning("EntryTimer: pandas not available — some functionality limited")


class EntryTimer:
    """
    Evaluates whether to enter a position immediately or wait for a better entry point.
    Runs 5 entry conditions: extension, momentum_confirmation, volume_confirmation,
    spread, time_of_day. Computes a composite entry timing score.
    """

    def __init__(self, store=None, config=None):
        cfg = (config or {}).get("closeloop", {}).get("entry", {})
        self.max_wait_days = cfg.get("max_wait_days", 5)
        self.pullback_threshold = cfg.get("pullback_threshold_pct", 0.02)
        self.extension_threshold_atr = cfg.get("extension_threshold_atr", 2.0)
        self._store = store

    def evaluate(self, ticker: str, direction: int, market: str = "us",
                 signal_context: dict = None) -> Dict:
        """
        Fetches recent price/volume data via yfinance.
        Runs all 5 entry conditions.
        Returns:
          {
            should_enter_now: bool,
            entry_timing_score: float,
            conditions: List[EntryConditionResult],
            recommended_size_multiplier: float,  # 0.5-1.0
            wait_reason: str or None,
            entry_method: str  # "immediate", "pullback_wait", "momentum_wait"
          }
        Graceful fallback: if yfinance fails, return should_enter_now=True, score=0.5
        """
        try:
            import yfinance as yf
        except ImportError:
            logger.warning(f"EntryTimer.evaluate({ticker}): yfinance not available")
            return self._fallback_entry()

        try:
            hist = yf.download(ticker, period="20d", interval="1d", progress=False)
            if hist is None or (hasattr(hist, "empty") and hist.empty):
                logger.warning(f"EntryTimer.evaluate({ticker}): empty data from yfinance")
                return self._fallback_entry()

            # Extract closes and volumes
            try:
                if hasattr(hist, "columns") and "Close" in hist.columns:
                    closes = hist["Close"].dropna()
                else:
                    closes = hist.iloc[:, 0].dropna()

                if hasattr(hist, "columns") and "Volume" in hist.columns:
                    volumes = hist["Volume"].dropna()
                else:
                    volumes = None
            except Exception as e:
                logger.warning(f"EntryTimer.evaluate({ticker}): column extraction error: {e}")
                return self._fallback_entry()

            if len(closes) < 3:
                logger.warning(f"EntryTimer.evaluate({ticker}): too few price observations")
                return self._fallback_entry()

            # Compute ATR
            atr = self._calc_atr(hist)
            if atr <= 0:
                atr = float(closes.std()) if hasattr(closes, "std") else 1.0

            # Compute baseline volume
            try:
                baseline_volume = float(volumes.mean()) if volumes is not None and len(volumes) > 0 else 1.0
            except Exception:
                baseline_volume = 1.0

            # Import condition checkers
            try:
                from closeloop.entry.entry_conditions import (
                    check_extension,
                    check_momentum_confirmation,
                    check_volume_confirmation,
                    check_spread,
                    check_time_of_day,
                    calc_entry_timing_score,
                    EntryConditionResult,
                )
            except ImportError:
                try:
                    from entry_conditions import (
                        check_extension,
                        check_momentum_confirmation,
                        check_volume_confirmation,
                        check_spread,
                        check_time_of_day,
                        calc_entry_timing_score,
                        EntryConditionResult,
                    )
                except ImportError:
                    logger.warning(f"EntryTimer.evaluate({ticker}): entry_conditions import failed")
                    return self._fallback_entry()

            # Run all 5 condition checks
            conditions = []

            try:
                ext_result = check_extension(closes, atr, self.extension_threshold_atr)
                conditions.append(ext_result)
            except Exception as e:
                logger.warning(f"EntryTimer: check_extension failed: {e}")

            try:
                mom_result = check_momentum_confirmation(closes, direction)
                conditions.append(mom_result)
            except Exception as e:
                logger.warning(f"EntryTimer: check_momentum_confirmation failed: {e}")

            if volumes is not None:
                try:
                    vol_result = check_volume_confirmation(volumes, baseline_volume)
                    conditions.append(vol_result)
                except Exception as e:
                    logger.warning(f"EntryTimer: check_volume_confirmation failed: {e}")

            # Spread check — estimate from signal_context or use a neutral default
            try:
                current_spread_pct = (signal_context or {}).get("current_spread_pct", 0.001)
                baseline_spread_pct = (signal_context or {}).get("baseline_spread_pct", 0.001)
                spread_result = check_spread(current_spread_pct, baseline_spread_pct)
                conditions.append(spread_result)
            except Exception as e:
                logger.warning(f"EntryTimer: check_spread failed: {e}")

            try:
                tod_result = check_time_of_day(market)
                conditions.append(tod_result)
            except Exception as e:
                logger.warning(f"EntryTimer: check_time_of_day failed: {e}")

            if not conditions:
                return self._fallback_entry()

            # Compute composite timing score
            entry_timing_score = calc_entry_timing_score(conditions)

            # Determine recommended_size_multiplier: scale between 0.5 and 1.0
            recommended_size_multiplier = 0.5 + 0.5 * entry_timing_score

            # Determine should_enter_now and entry_method
            wait_reasons = []
            reduce_size_reasons = []

            for cond in conditions:
                if cond.action == "wait":
                    wait_reasons.append(f"{cond.condition_name}: {cond.reason}")
                elif cond.action == "reduce_size":
                    reduce_size_reasons.append(f"{cond.condition_name}: {cond.reason}")

            # Blocking wait conditions
            blocking_waits = [c for c in conditions if c.action == "wait" and c.score < 0.2]

            if blocking_waits:
                should_enter_now = False
                wait_reason = "; ".join(wait_reasons)
                # Determine entry method based on what is blocking
                blocking_names = [c.condition_name for c in blocking_waits]
                if "extension" in blocking_names:
                    entry_method = "pullback_wait"
                elif "momentum_confirmation" in blocking_names:
                    entry_method = "momentum_wait"
                else:
                    entry_method = "pullback_wait"
            elif entry_timing_score >= 0.6:
                should_enter_now = True
                wait_reason = None
                entry_method = "immediate"
            else:
                should_enter_now = True
                wait_reason = None
                entry_method = "immediate"
                # Lower size multiplier if score is mediocre
                if entry_timing_score < 0.4:
                    recommended_size_multiplier = max(0.5, recommended_size_multiplier - 0.1)

            logger.info(
                f"EntryTimer.evaluate({ticker}): score={entry_timing_score:.3f}, "
                f"enter_now={should_enter_now}, method={entry_method}, "
                f"size_mult={recommended_size_multiplier:.2f}"
            )

            return {
                "should_enter_now": should_enter_now,
                "entry_timing_score": entry_timing_score,
                "conditions": conditions,
                "recommended_size_multiplier": recommended_size_multiplier,
                "wait_reason": wait_reason,
                "entry_method": entry_method,
            }

        except Exception as e:
            logger.warning(f"EntryTimer.evaluate({ticker}): {e}")
            return self._fallback_entry()

    def wait_for_pullback(self, ticker: str, direction: int,
                          post_earnings_high: float,
                          days_waited: int = 0) -> bool:
        """
        Check if price has pulled back pullback_threshold from post_earnings_high.
        Returns True if pullback occurred (time to enter).
        Also returns True if days_waited >= max_wait_days (don't miss the trade).

        A pullback for a long (direction=1) is price dropping pullback_threshold below
        post_earnings_high. For a short (direction=-1) it is price rising
        pullback_threshold above post_earnings_high (the post-earnings low).
        """
        try:
            if days_waited >= self.max_wait_days:
                logger.info(
                    f"wait_for_pullback({ticker}): max_wait_days={self.max_wait_days} reached — entering regardless"
                )
                return True

            try:
                import yfinance as yf
            except ImportError:
                logger.warning(f"wait_for_pullback({ticker}): yfinance not available — returning True")
                return True

            hist = yf.download(ticker, period="2d", interval="1d", progress=False)
            if hist is None or (hasattr(hist, "empty") and hist.empty):
                logger.warning(f"wait_for_pullback({ticker}): no data — returning True")
                return True

            try:
                if "Close" in hist.columns:
                    current_price = float(hist["Close"].dropna().iloc[-1])
                else:
                    current_price = float(hist.iloc[-1, 0])
            except Exception as e:
                logger.warning(f"wait_for_pullback({ticker}): price extraction error: {e}")
                return True

            if direction > 0:
                # Long: pullback = price dropped from high by threshold
                pullback_level = post_earnings_high * (1.0 - self.pullback_threshold)
                pulled_back = current_price <= pullback_level
            else:
                # Short: pullback = price recovered from low by threshold
                pullback_level = post_earnings_high * (1.0 + self.pullback_threshold)
                pulled_back = current_price >= pullback_level

            if pulled_back:
                logger.info(
                    f"wait_for_pullback({ticker}): pullback confirmed. "
                    f"current={current_price:.4f}, level={pullback_level:.4f}, direction={direction}"
                )
            else:
                logger.debug(
                    f"wait_for_pullback({ticker}): no pullback yet. "
                    f"current={current_price:.4f}, level={pullback_level:.4f}"
                )

            return pulled_back

        except Exception as e:
            logger.warning(f"wait_for_pullback({ticker}): unexpected error: {e} — returning True")
            return True

    def _calc_atr(self, hist, periods: int = 14) -> float:
        """
        Average True Range over last `periods` days.
        TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        Falls back to close-based volatility estimate if H/L/C columns missing.
        """
        try:
            if not _PANDAS_AVAILABLE:
                return 0.0

            if hist is None or (hasattr(hist, "empty") and hist.empty):
                return 0.0

            required = {"High", "Low", "Close"}
            if not required.issubset(set(hist.columns)):
                logger.debug("_calc_atr: H/L/C columns not available, falling back to close std")
                try:
                    closes = hist["Close"].dropna() if "Close" in hist.columns else hist.iloc[:, 0].dropna()
                    if len(closes) < 2:
                        return 0.0
                    return float(closes.diff().abs().mean())
                except Exception:
                    return 0.0

            df = hist[["High", "Low", "Close"]].dropna().tail(periods + 1)
            if len(df) < 2:
                return 0.0

            high = df["High"]
            low = df["Low"]
            prev_close = df["Close"].shift(1)

            tr = pd.DataFrame({
                "hl": high - low,
                "hpc": (high - prev_close).abs(),
                "lpc": (low - prev_close).abs(),
            }).max(axis=1)

            atr = float(tr.dropna().tail(periods).mean())
            return atr if atr > 0 else 0.0

        except Exception as e:
            logger.warning(f"_calc_atr: unexpected error: {e}")
            return 0.0

    def _fallback_entry(self) -> Dict:
        """Return permissive entry decision when data unavailable."""
        return {
            "should_enter_now": True,
            "entry_timing_score": 0.5,
            "conditions": [],
            "recommended_size_multiplier": 1.0,
            "wait_reason": None,
            "entry_method": "immediate_fallback",
        }
