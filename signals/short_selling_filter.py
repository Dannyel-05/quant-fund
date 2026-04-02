"""
Short Selling Filter
=====================
Filters short signals based on borrowability, short interest,
squeeze risk, and estimated borrow cost.

Classes:
  ShortSellingFilter  — validates and enriches short signals
"""
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Squeeze risk thresholds (days to cover)
_SQUEEZE_LOW_DTC = 3.0
_SQUEEZE_HIGH_DTC = 7.0


def get_short_interest(ticker: str) -> Optional[float]:
    """
    Fetch short interest as a percentage of float.

    Returns: float (0-100) or None on error / unavailable.
    """
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info
        raw = info.get("shortPercentOfFloat")
        if raw is None:
            return None
        return float(raw) * 100.0
    except Exception as exc:
        logger.debug("%s: get_short_interest failed: %s", ticker, exc)
        return None


def get_days_to_cover(ticker: str) -> Optional[float]:
    """
    Fetch days-to-cover (short ratio).

    Prefers the pre-computed shortRatio field; falls back to
    sharesShort / averageVolume calculation.

    Returns: float or None on error / unavailable.
    """
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info

        # Prefer shortRatio (already shares_short / avg_daily_volume)
        short_ratio = info.get("shortRatio")
        if short_ratio is not None:
            return float(short_ratio)

        shares_short = info.get("sharesShort")
        avg_vol = info.get("averageVolume") or info.get("averageDailyVolume10Day")
        if shares_short and avg_vol and avg_vol > 0:
            return float(shares_short) / float(avg_vol)

        return None
    except Exception as exc:
        logger.debug("%s: get_days_to_cover failed: %s", ticker, exc)
        return None


def estimate_borrow_cost_annual(short_interest_pct: float) -> float:
    """
    Estimate annual borrow cost based on short interest level.

    Parameters
    ----------
    short_interest_pct : float, short interest as % of float (0-100)

    Returns
    -------
    float : estimated annual borrow rate (e.g. 0.06 = 6%)
    """
    if short_interest_pct < 10:
        return 0.02
    if short_interest_pct < 20:
        return 0.06
    if short_interest_pct < 30:
        return 0.15
    return 0.40


def get_market_cap(ticker: str) -> Optional[float]:
    """
    Fetch market cap in USD.

    Returns: float or None on error / unavailable.
    """
    try:
        import yfinance as yf

        cap = yf.Ticker(ticker).info.get("marketCap")
        return float(cap) if cap is not None else None
    except Exception as exc:
        logger.debug("%s: get_market_cap failed: %s", ticker, exc)
        return None


def _squeeze_risk_label(days_to_cover: float) -> str:
    """Map days-to-cover to a squeeze risk label."""
    if days_to_cover <= _SQUEEZE_LOW_DTC:
        return "LOW"
    if days_to_cover <= _SQUEEZE_HIGH_DTC:
        return "MEDIUM"
    return "HIGH"


def validate_short(
    ticker: str,
    expected_return_pct: float,
    holding_days: int = 20,
) -> Dict:
    """
    Validate whether a short position is feasible given borrowability,
    squeeze risk, and borrow costs.

    Checks (applied in order, return early on first failure):
      1. market_cap < $50M  → reject (too small to locate)
      2. market_cap < $100M → reject (below institutional threshold)
      3. short_interest > 30% → reject (crowded short, squeeze risk)
      4. days_to_cover > 10  → reject (too hard to cover quickly)
      5. annualised borrow cost > expected holding return → reject

    Parameters
    ----------
    ticker              : ticker symbol
    expected_return_pct : expected absolute return over holding period (e.g. 0.05 = 5%)
    holding_days        : planned holding period in trading days (default 20)

    Returns
    -------
    dict with keys:
      shortable (bool), and either reason (str) or full metrics.
    """
    # --- Market cap check ---
    mkt_cap = get_market_cap(ticker)
    if mkt_cap is not None:
        if mkt_cap < 50_000_000:
            logger.debug("%s: short rejected — market_cap_too_small (%.0f)", ticker, mkt_cap)
            return {"shortable": False, "reason": "market_cap_too_small", "market_cap": mkt_cap}
        if mkt_cap < 100_000_000:
            logger.debug("%s: short rejected — market_cap_below_threshold (%.0f)", ticker, mkt_cap)
            return {
                "shortable": False,
                "reason": "market_cap_below_threshold",
                "market_cap": mkt_cap,
            }

    # --- Short interest check ---
    si_pct = get_short_interest(ticker)
    if si_pct is not None and si_pct > 30:
        logger.debug(
            "%s: short rejected — short_interest_too_high (%.1f%%)", ticker, si_pct
        )
        return {
            "shortable": False,
            "reason": "short_interest_too_high",
            "short_interest_pct": si_pct,
        }

    # --- Days to cover check ---
    dtc = get_days_to_cover(ticker)
    if dtc is not None and dtc > 10:
        logger.debug(
            "%s: short rejected — days_to_cover_too_high (%.1f)", ticker, dtc
        )
        return {
            "shortable": False,
            "reason": "days_to_cover_too_high",
            "days_to_cover": dtc,
        }

    # --- Borrow cost check ---
    # Use short_interest_pct; if unavailable use a default of 5%
    si_for_cost = si_pct if si_pct is not None else 5.0
    annual_borrow = estimate_borrow_cost_annual(si_for_cost)
    # Annualise expected_return_pct to same holding period fraction
    # borrow_cost_period = annual_borrow * (holding_days / 252)
    borrow_cost_period = annual_borrow * (holding_days / 252.0)
    expected_holding_return = abs(expected_return_pct)

    if borrow_cost_period > expected_holding_return:
        logger.debug(
            "%s: short rejected — borrow_cost_exceeds_expected_return "
            "(borrow=%.3f > expected=%.3f)",
            ticker, borrow_cost_period, expected_holding_return,
        )
        return {
            "shortable": False,
            "reason": "borrow_cost_exceeds_expected_return",
            "estimated_borrow_cost_period": round(borrow_cost_period, 6),
            "expected_return_pct": expected_return_pct,
        }

    # --- All checks passed ---
    squeeze_risk = _squeeze_risk_label(dtc) if dtc is not None else "UNKNOWN"
    net_expected = expected_return_pct - borrow_cost_period

    result = {
        "shortable": True,
        "short_interest_pct": si_pct,
        "days_to_cover": dtc,
        "estimated_borrow_cost_annual": annual_borrow,
        "estimated_borrow_cost_period": round(borrow_cost_period, 6),
        "squeeze_risk": squeeze_risk,
        "net_expected_return": round(net_expected, 6),
        "market_cap": mkt_cap,
    }

    logger.info(
        "%s: short APPROVED — si=%.1f%%, dtc=%.1f, borrow_period=%.3f, net_ret=%.3f, squeeze=%s",
        ticker,
        si_pct or 0,
        dtc or 0,
        borrow_cost_period,
        net_expected,
        squeeze_risk,
    )
    return result


class ShortSellingFilter:
    """
    Validates and enriches short signals based on borrowability,
    short interest, squeeze risk, and estimated borrow cost.
    """

    def __init__(self, config: dict):
        self.config = config

    def filter(
        self,
        ticker: str,
        expected_return_pct: float,
        holding_days: int = 20,
    ) -> Dict:
        """
        Run all short selling validation checks.

        Parameters
        ----------
        ticker              : ticker symbol
        expected_return_pct : expected absolute return over holding period
        holding_days        : planned holding period in trading days

        Returns
        -------
        dict from validate_short()
        """
        try:
            return validate_short(ticker, expected_return_pct, holding_days)
        except Exception as exc:
            logger.warning(
                "%s: ShortSellingFilter.filter unexpected error: %s", ticker, exc
            )
            # Fail open — do not block due to data errors
            return {
                "shortable": True,
                "reason": "data_error_fail_open",
                "short_interest_pct": None,
                "days_to_cover": None,
                "estimated_borrow_cost_annual": 0.02,
                "estimated_borrow_cost_period": 0.0,
                "squeeze_risk": "UNKNOWN",
                "net_expected_return": expected_return_pct,
            }
