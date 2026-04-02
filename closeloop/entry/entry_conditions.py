"""Entry condition checkers for EntryTimer."""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class EntryConditionResult:
    condition_name: str
    passed: bool
    score: float          # 0-1
    reason: str
    action: str           # "enter", "wait", "reduce_size"


def check_extension(price_history, atr: float, threshold_atr: float = 2.0) -> EntryConditionResult:
    """
    Has price already moved > threshold_atr * ATR from pre-earnings close?
    Uses last 2 prices from price_history (pd.Series or list).
    If extended: action="wait", score=0.3
    If not extended: action="enter", score=1.0
    """
    try:
        prices = list(price_history)
        if len(prices) < 2:
            logger.warning("check_extension: insufficient price history, defaulting to not extended")
            return EntryConditionResult(
                condition_name="extension",
                passed=True,
                score=1.0,
                reason="Insufficient price history — defaulting to not extended",
                action="enter",
            )

        pre_earnings_close = float(prices[-2])
        latest_price = float(prices[-1])

        if atr <= 0:
            logger.warning("check_extension: ATR <= 0, defaulting to not extended")
            return EntryConditionResult(
                condition_name="extension",
                passed=True,
                score=1.0,
                reason="ATR <= 0 — cannot assess extension",
                action="enter",
            )

        move = abs(latest_price - pre_earnings_close)
        threshold = threshold_atr * atr
        extended = move > threshold

        if extended:
            return EntryConditionResult(
                condition_name="extension",
                passed=False,
                score=0.3,
                reason=f"Price moved {move:.4f} > {threshold:.4f} ({threshold_atr}x ATR) — extended",
                action="wait",
            )
        else:
            return EntryConditionResult(
                condition_name="extension",
                passed=True,
                score=1.0,
                reason=f"Price move {move:.4f} within {threshold:.4f} ({threshold_atr}x ATR) — not extended",
                action="enter",
            )
    except Exception as e:
        logger.warning(f"check_extension: unexpected error: {e}")
        return EntryConditionResult(
            condition_name="extension",
            passed=True,
            score=1.0,
            reason=f"Error in check_extension ({e}) — defaulting to enter",
            action="enter",
        )


def check_momentum_confirmation(price_history, direction: int, confirmation_days: int = 3) -> EntryConditionResult:
    """
    Has price moved in signal direction for confirmation_days?
    Returns score proportional to fraction of days confirming.
    score = confirming_days / confirmation_days
    action="enter" if score >= 0.67 else "wait"
    """
    try:
        prices = list(price_history)
        if len(prices) < confirmation_days + 1:
            logger.warning("check_momentum_confirmation: insufficient price history")
            return EntryConditionResult(
                condition_name="momentum_confirmation",
                passed=False,
                score=0.5,
                reason=f"Insufficient price history (need {confirmation_days + 1} days)",
                action="wait",
            )

        recent_prices = prices[-(confirmation_days + 1):]
        confirming = 0
        for i in range(1, len(recent_prices)):
            daily_move = recent_prices[i] - recent_prices[i - 1]
            if direction > 0 and daily_move > 0:
                confirming += 1
            elif direction < 0 and daily_move < 0:
                confirming += 1

        score = confirming / confirmation_days
        passed = score >= 0.67
        action = "enter" if passed else "wait"

        return EntryConditionResult(
            condition_name="momentum_confirmation",
            passed=passed,
            score=score,
            reason=(
                f"{confirming}/{confirmation_days} days confirmed momentum in direction={direction}"
            ),
            action=action,
        )
    except Exception as e:
        logger.warning(f"check_momentum_confirmation: unexpected error: {e}")
        return EntryConditionResult(
            condition_name="momentum_confirmation",
            passed=False,
            score=0.5,
            reason=f"Error in check_momentum_confirmation ({e}) — defaulting to wait",
            action="wait",
        )


def check_volume_confirmation(volume_history, baseline_avg_volume: float, threshold: float = 1.2) -> EntryConditionResult:
    """
    Is latest volume > threshold * baseline_avg_volume?
    score = min(1.0, latest_volume / (threshold * baseline_avg_volume))
    action="enter" if volume elevated, "reduce_size" if not
    """
    try:
        volumes = list(volume_history)
        if not volumes:
            logger.warning("check_volume_confirmation: empty volume history")
            return EntryConditionResult(
                condition_name="volume_confirmation",
                passed=False,
                score=0.5,
                reason="Empty volume history — defaulting to reduce_size",
                action="reduce_size",
            )

        if baseline_avg_volume <= 0:
            logger.warning("check_volume_confirmation: baseline_avg_volume <= 0")
            return EntryConditionResult(
                condition_name="volume_confirmation",
                passed=False,
                score=0.5,
                reason="baseline_avg_volume <= 0 — cannot assess volume",
                action="reduce_size",
            )

        latest_volume = float(volumes[-1])
        elevated_threshold = threshold * baseline_avg_volume
        score = min(1.0, latest_volume / elevated_threshold)
        elevated = latest_volume > elevated_threshold
        action = "enter" if elevated else "reduce_size"

        return EntryConditionResult(
            condition_name="volume_confirmation",
            passed=elevated,
            score=score,
            reason=(
                f"Latest volume {latest_volume:.0f} vs threshold {elevated_threshold:.0f} "
                f"({threshold}x baseline {baseline_avg_volume:.0f})"
            ),
            action=action,
        )
    except Exception as e:
        logger.warning(f"check_volume_confirmation: unexpected error: {e}")
        return EntryConditionResult(
            condition_name="volume_confirmation",
            passed=False,
            score=0.5,
            reason=f"Error in check_volume_confirmation ({e}) — defaulting to reduce_size",
            action="reduce_size",
        )


def check_spread(current_spread_pct: float, baseline_spread_pct: float, max_multiple: float = 2.0) -> EntryConditionResult:
    """
    Is current spread < max_multiple * baseline?
    score = 1 - min(1, current/baseline / max_multiple)
    action="wait" if spread too wide, "enter" if acceptable
    """
    try:
        if baseline_spread_pct <= 0:
            logger.warning("check_spread: baseline_spread_pct <= 0")
            return EntryConditionResult(
                condition_name="spread",
                passed=True,
                score=1.0,
                reason="baseline_spread_pct <= 0 — cannot assess spread, defaulting to enter",
                action="enter",
            )

        ratio = current_spread_pct / baseline_spread_pct
        score = 1.0 - min(1.0, ratio / max_multiple)
        too_wide = current_spread_pct >= max_multiple * baseline_spread_pct
        action = "wait" if too_wide else "enter"

        return EntryConditionResult(
            condition_name="spread",
            passed=not too_wide,
            score=score,
            reason=(
                f"Spread {current_spread_pct:.4f} vs baseline {baseline_spread_pct:.4f} "
                f"(ratio={ratio:.2f}, max_multiple={max_multiple})"
            ),
            action=action,
        )
    except Exception as e:
        logger.warning(f"check_spread: unexpected error: {e}")
        return EntryConditionResult(
            condition_name="spread",
            passed=True,
            score=1.0,
            reason=f"Error in check_spread ({e}) — defaulting to enter",
            action="enter",
        )


def check_time_of_day(market: str = "us") -> EntryConditionResult:
    """
    US: prefer 10:30-11:30 ET, never first 15 min
    UK: prefer 9:15-10:00 BST
    Uses datetime.now() — score=1.0 in ideal window, 0.5 outside, 0.0 in first 15 min
    """
    try:
        from datetime import datetime, timezone, timedelta

        now_utc = datetime.now(timezone.utc)

        if market.lower() == "us":
            # ET = UTC-5 (EST) or UTC-4 (EDT). Use UTC-5 as conservative base.
            # Market opens 9:30 ET. First 15 min = 9:30-9:45 ET.
            # Ideal window: 10:30-11:30 ET.
            try:
                import zoneinfo
                tz = zoneinfo.ZoneInfo("America/New_York")
            except Exception:
                tz = timezone(timedelta(hours=-5))

            now_local = now_utc.astimezone(tz)
            h, m = now_local.hour, now_local.minute
            minutes_since_open = (h - 9) * 60 + m - 30  # minutes since 9:30

            if minutes_since_open < 15:
                # First 15 minutes — never trade
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=False,
                    score=0.0,
                    reason=f"US market first 15 min blackout ({now_local.strftime('%H:%M')} ET)",
                    action="wait",
                )
            elif (h == 10 and m >= 30) or (h == 11 and m < 30):
                # 10:30-11:30 ET — ideal window
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=True,
                    score=1.0,
                    reason=f"US ideal entry window 10:30-11:30 ET ({now_local.strftime('%H:%M')} ET)",
                    action="enter",
                )
            elif 9 <= h <= 15:
                # Market hours but outside ideal window
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=True,
                    score=0.5,
                    reason=f"US market hours but outside ideal window ({now_local.strftime('%H:%M')} ET)",
                    action="enter",
                )
            else:
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=False,
                    score=0.0,
                    reason=f"US market closed ({now_local.strftime('%H:%M')} ET)",
                    action="wait",
                )

        elif market.lower() == "uk":
            # BST = UTC+1, GMT = UTC+0
            try:
                import zoneinfo
                tz = zoneinfo.ZoneInfo("Europe/London")
            except Exception:
                tz = timezone(timedelta(hours=1))

            now_local = now_utc.astimezone(tz)
            h, m = now_local.hour, now_local.minute
            minutes_since_open = (h - 8) * 60 + m  # minutes since 8:00 BST (UK market opens 8:00)

            if minutes_since_open < 15:
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=False,
                    score=0.0,
                    reason=f"UK market first 15 min blackout ({now_local.strftime('%H:%M')} BST/GMT)",
                    action="wait",
                )
            elif (h == 9 and m >= 15) or (h == 10 and m < 0):
                # Ideal window 9:15-10:00 BST
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=True,
                    score=1.0,
                    reason=f"UK ideal entry window 9:15-10:00 ({now_local.strftime('%H:%M')} BST/GMT)",
                    action="enter",
                )
            elif h == 9 and m < 60:
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=True,
                    score=1.0,
                    reason=f"UK ideal entry window ({now_local.strftime('%H:%M')} BST/GMT)",
                    action="enter",
                )
            elif 8 <= h < 16 and m < 30 if h == 16 else 8 <= h < 16:
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=True,
                    score=0.5,
                    reason=f"UK market hours but outside ideal window ({now_local.strftime('%H:%M')} BST/GMT)",
                    action="enter",
                )
            else:
                return EntryConditionResult(
                    condition_name="time_of_day",
                    passed=False,
                    score=0.0,
                    reason=f"UK market closed ({now_local.strftime('%H:%M')} BST/GMT)",
                    action="wait",
                )
        else:
            logger.warning(f"check_time_of_day: unknown market '{market}', defaulting to score=0.5")
            return EntryConditionResult(
                condition_name="time_of_day",
                passed=True,
                score=0.5,
                reason=f"Unknown market '{market}' — defaulting to neutral score",
                action="enter",
            )
    except Exception as e:
        logger.warning(f"check_time_of_day: unexpected error: {e}")
        return EntryConditionResult(
            condition_name="time_of_day",
            passed=True,
            score=0.5,
            reason=f"Error in check_time_of_day ({e}) — defaulting to neutral",
            action="enter",
        )


def calc_entry_timing_score(results: List[EntryConditionResult]) -> float:
    """
    Weighted average of all condition scores.
    Returns 0-1.
    Weights: extension=0.30, momentum_confirmation=0.25, volume_confirmation=0.20,
             spread=0.15, time_of_day=0.10. Equal weight if condition_name not matched.
    """
    try:
        if not results:
            logger.warning("calc_entry_timing_score: empty results list, returning 0.5")
            return 0.5

        weights = {
            "extension": 0.30,
            "momentum_confirmation": 0.25,
            "volume_confirmation": 0.20,
            "spread": 0.15,
            "time_of_day": 0.10,
        }

        total_weight = 0.0
        weighted_sum = 0.0

        for result in results:
            w = weights.get(result.condition_name, None)
            if w is None:
                # Equal weight for unknown condition names
                w = 1.0 / len(results)
                logger.debug(
                    f"calc_entry_timing_score: unknown condition '{result.condition_name}' — using equal weight {w:.4f}"
                )
            score = max(0.0, min(1.0, result.score))
            weighted_sum += w * score
            total_weight += w

        if total_weight == 0:
            logger.warning("calc_entry_timing_score: total weight is zero, returning 0.5")
            return 0.5

        final_score = weighted_sum / total_weight
        return max(0.0, min(1.0, final_score))

    except Exception as e:
        logger.warning(f"calc_entry_timing_score: unexpected error: {e}")
        return 0.5
