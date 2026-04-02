"""
Options Earnings Signal
========================
Detects earnings straddle opportunities (cheap IV before earnings)
and PEAD confirmation through options flow analysis.

Uses yfinance options chain data (free, no API key needed).

Classes:
  OptionsEarningsSignal  — main signal class
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def get_options_data(ticker: str) -> Optional[Dict]:
    """
    Fetch options chain via yfinance for the nearest expiry.

    Returns: {"calls": df, "puts": df, "expiry": str} or None on error.
    """
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker)
        expirations = tk.options
        if not expirations:
            logger.debug("%s: no option expiries found", ticker)
            return None

        expiry = expirations[0]  # nearest expiry
        chain = tk.option_chain(expiry)
        calls = chain.calls
        puts = chain.puts
        return {"calls": calls, "puts": puts, "expiry": expiry}
    except Exception as exc:
        logger.warning("%s: get_options_data failed: %s", ticker, exc)
        return None


def implied_move(ticker: str) -> Optional[float]:
    """
    Estimate market's implied earnings move using ATM straddle pricing.

    ATM = strike closest to current price.
    implied_move_pct = (call_ask + put_ask) / current_price * 100

    Returns: float (percentage) or None on error.
    """
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker)
        info = tk.info
        current_price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not current_price:
            hist = tk.history(period="2d")
            if hist.empty:
                return None
            current_price = float(hist["Close"].iloc[-1])

        opts = get_options_data(ticker)
        if opts is None:
            return None

        calls = opts["calls"]
        puts = opts["puts"]

        if calls.empty or puts.empty:
            return None

        # Find ATM call strike (closest to current price)
        calls = calls.copy()
        calls["dist"] = (calls["strike"] - current_price).abs()
        atm_strike = calls.loc[calls["dist"].idxmin(), "strike"]

        # Get ATM call and put ask prices
        atm_call = calls[calls["strike"] == atm_strike]
        atm_put = puts[puts["strike"] == atm_strike]

        if atm_call.empty or atm_put.empty:
            return None

        call_ask = float(atm_call["ask"].iloc[0])
        put_ask = float(atm_put["ask"].iloc[0])

        if call_ask <= 0 or put_ask <= 0:
            # Fall back to lastPrice if ask is zero/stale
            call_ask = float(atm_call["lastPrice"].iloc[0])
            put_ask = float(atm_put["lastPrice"].iloc[0])

        move_pct = (call_ask + put_ask) / current_price * 100
        logger.debug(
            "%s: implied_move=%.2f%% (call_ask=%.2f, put_ask=%.2f, price=%.2f)",
            ticker, move_pct, call_ask, put_ask, current_price,
        )
        return move_pct
    except Exception as exc:
        logger.warning("%s: implied_move failed: %s", ticker, exc)
        return None


def historical_earnings_move(ticker: str, n_quarters: int = 8) -> Optional[float]:
    """
    Average absolute % move on the day after earnings over the last n_quarters.

    Uses yfinance earnings history + price data.
    Returns: float (percentage) or None if insufficient data.
    """
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker)

        # Get earnings dates
        try:
            earnings_dates = tk.earnings_dates
        except Exception:
            earnings_dates = None

        if earnings_dates is None or earnings_dates.empty:
            logger.debug("%s: no earnings_dates available", ticker)
            return None

        # Filter to past earnings dates only
        now = pd.Timestamp.now(tz="UTC")
        past_dates = earnings_dates[earnings_dates.index < now].head(n_quarters)
        if past_dates.empty:
            return None

        # Download price history covering all past earnings
        oldest = past_dates.index[-1]
        start_str = (oldest - timedelta(days=5)).strftime("%Y-%m-%d")
        hist = yf.download(ticker, start=start_str, progress=False, auto_adjust=True)

        if hist.empty:
            return None

        close = hist["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]

        moves = []
        for earn_dt in past_dates.index:
            earn_date = earn_dt.date() if hasattr(earn_dt, "date") else earn_dt
            # Find price on earnings day and next trading day
            date_index = close.index
            try:
                # Convert index to date for comparison
                dates_only = pd.Index([d.date() if hasattr(d, "date") else d for d in date_index])
                loc = dates_only.get_loc(earn_date)
                if isinstance(loc, slice):
                    loc = loc.start
                if loc + 1 < len(close):
                    p0 = float(close.iloc[loc])
                    p1 = float(close.iloc[loc + 1])
                    if p0 > 0:
                        move = abs(p1 / p0 - 1) * 100
                        moves.append(move)
            except (KeyError, TypeError):
                continue

        if not moves:
            return None

        avg_move = float(np.mean(moves))
        logger.debug("%s: historical_earnings_move=%.2f%% (n=%d)", ticker, avg_move, len(moves))
        return avg_move
    except Exception as exc:
        logger.warning("%s: historical_earnings_move failed: %s", ticker, exc)
        return None


def iv_rank(ticker: str) -> float:
    """
    Estimate IV rank (0-100) using ATM call implied volatility.

    Proxy formula: iv_rank = min(100, max(0, (iv_current - 0.15) / 0.50 * 100))
    Returns: float 0-100 (defaults to 50 on error)
    """
    try:
        opts = get_options_data(ticker)
        if opts is None:
            return 50.0

        calls = opts["calls"]
        if calls.empty or "impliedVolatility" not in calls.columns:
            return 50.0

        import yfinance as yf

        tk = yf.Ticker(ticker)
        info = tk.info
        current_price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not current_price:
            hist = tk.history(period="2d")
            current_price = float(hist["Close"].iloc[-1]) if not hist.empty else None

        if not current_price:
            return 50.0

        calls = calls.copy()
        calls["dist"] = (calls["strike"] - current_price).abs()
        atm_idx = calls["dist"].idxmin()
        iv_current = float(calls.loc[atm_idx, "impliedVolatility"])

        rank = min(100.0, max(0.0, (iv_current - 0.15) / 0.50 * 100.0))
        logger.debug("%s: iv_current=%.3f → iv_rank=%.1f", ticker, iv_current, rank)
        return rank
    except Exception as exc:
        logger.warning("%s: iv_rank failed: %s", ticker, exc)
        return 50.0


def generate_straddle_signal(ticker: str, days_to_earnings: int) -> Dict:
    """
    Generate pre-earnings straddle signal.

    Conditions for BUY_STRADDLE:
      - days_to_earnings in [3, 5]
      - iv_rank < 50
      - historical_earnings_move > implied_move * 1.1

    Returns: {
      "signal": "BUY_STRADDLE" | "SELL_IV_CRUSH" | "NONE",
      "implied_move": float,
      "hist_move": float,
      "iv_rank": float
    }
    """
    result = {
        "signal": "NONE",
        "implied_move": None,
        "hist_move": None,
        "iv_rank": 50.0,
    }

    try:
        iv_r = iv_rank(ticker)
        result["iv_rank"] = iv_r

        impl_move = implied_move(ticker)
        result["implied_move"] = impl_move

        hist_move = historical_earnings_move(ticker)
        result["hist_move"] = hist_move

        if impl_move is None or hist_move is None:
            return result

        in_window = 3 <= days_to_earnings <= 5
        iv_cheap = iv_r < 50
        hist_exceeds = hist_move > impl_move * 1.1

        if in_window and iv_cheap and hist_exceeds:
            result["signal"] = "BUY_STRADDLE"
        elif in_window and iv_r >= 70:
            # High IV going into earnings → sell premium
            result["signal"] = "SELL_IV_CRUSH"

        logger.info(
            "%s: straddle_signal=%s (dte=%d, iv_rank=%.1f, impl=%.2f%%, hist=%.2f%%)",
            ticker, result["signal"], days_to_earnings, iv_r,
            impl_move or 0, hist_move or 0,
        )
    except Exception as exc:
        logger.warning("%s: generate_straddle_signal failed: %s", ticker, exc)

    return result


def generate_pead_options_signal(
    ticker: str, direction: int, days_after_earnings: int
) -> Dict:
    """
    Generate post-earnings options signal for PEAD confirmation.

    After earnings (days_after_earnings in [1, 3]):
      direction=+1: BUY_OTM_CALLS (2 weeks out, ~5% OTM)
      direction=-1: BUY_OTM_PUTS  (2 weeks out, ~5% OTM)
    sizing_fraction = 0.10 (10% of stock position notional)

    Returns: {"signal": str, "sizing_fraction": float}
    """
    result = {"signal": "NONE", "sizing_fraction": 0.10}

    try:
        if not (1 <= days_after_earnings <= 3):
            return result

        if direction == 1:
            result["signal"] = "BUY_OTM_CALLS"
        elif direction == -1:
            result["signal"] = "BUY_OTM_PUTS"

        logger.debug(
            "%s: pead_options_signal=%s (day_after=%d)",
            ticker, result["signal"], days_after_earnings,
        )
    except Exception as exc:
        logger.warning("%s: generate_pead_options_signal failed: %s", ticker, exc)

    return result


def get_pead_confidence_modifier(ticker: str, pead_direction: int) -> float:
    """
    Adjust PEAD confidence based on unusual options flow.

    Logic:
      - call_volume / put_volume > 2 AND direction=+1 → return 1.25 (confirming flow)
      - put_volume / call_volume > 2 AND direction=+1 → return 0.75 (opposing flow)
      - Symmetric for direction=-1

    Returns: float (default 1.0 on any error)
    """
    try:
        opts = get_options_data(ticker)
        if opts is None:
            return 1.0

        calls = opts["calls"]
        puts = opts["puts"]

        if calls.empty or puts.empty:
            return 1.0

        total_call_vol = float(calls["volume"].fillna(0).sum())
        total_put_vol = float(puts["volume"].fillna(0).sum())

        if total_put_vol <= 0:
            return 1.0

        call_put_ratio = total_call_vol / total_put_vol

        if pead_direction == 1:
            if call_put_ratio > 2.0:
                logger.debug(
                    "%s: unusual call flow (C/P=%.2f) → confidence +25%%", ticker, call_put_ratio
                )
                return 1.25
            if call_put_ratio < 0.5:
                logger.debug(
                    "%s: unusual put flow (C/P=%.2f) → confidence -25%%", ticker, call_put_ratio
                )
                return 0.75
        elif pead_direction == -1:
            if call_put_ratio < 0.5:
                logger.debug(
                    "%s: unusual put flow (C/P=%.2f) → confidence +25%%", ticker, call_put_ratio
                )
                return 1.25
            if call_put_ratio > 2.0:
                logger.debug(
                    "%s: unusual call flow (C/P=%.2f) → confidence -25%%", ticker, call_put_ratio
                )
                return 0.75

        return 1.0
    except Exception as exc:
        logger.warning("%s: get_pead_confidence_modifier failed: %s", ticker, exc)
        return 1.0


class OptionsEarningsSignal:
    """
    Main options earnings signal class.

    Combines pre-earnings straddle analysis with post-earnings PEAD
    options overlay and flow-based confidence modifiers.
    """

    def __init__(self, config: dict):
        self.config = config

    def analyse(
        self,
        ticker: str,
        pead_direction: Optional[int] = None,
        days_to_earnings: Optional[int] = None,
        days_after_earnings: Optional[int] = None,
    ) -> Dict:
        """
        Run all options checks for a given ticker.

        Parameters
        ----------
        ticker            : stock ticker symbol
        pead_direction    : +1 (long PEAD) or -1 (short PEAD), or None
        days_to_earnings  : integer days until next earnings date, or None
        days_after_earnings: integer days since last earnings date, or None

        Returns
        -------
        {
          "straddle"            : straddle signal dict,
          "pead_options"        : pead options signal dict,
          "confidence_modifier" : float
        }
        """
        straddle_sig = {"signal": "NONE", "implied_move": None, "hist_move": None, "iv_rank": 50.0}
        pead_opts_sig = {"signal": "NONE", "sizing_fraction": 0.10}
        conf_mod = 1.0

        try:
            if days_to_earnings is not None:
                straddle_sig = generate_straddle_signal(ticker, days_to_earnings)

            if pead_direction is not None and days_after_earnings is not None:
                pead_opts_sig = generate_pead_options_signal(
                    ticker, pead_direction, days_after_earnings
                )

            if pead_direction is not None:
                conf_mod = get_pead_confidence_modifier(ticker, pead_direction)

        except Exception as exc:
            logger.warning("%s: OptionsEarningsSignal.analyse failed: %s", ticker, exc)

        return {
            "straddle": straddle_sig,
            "pead_options": pead_opts_sig,
            "confidence_modifier": conf_mod,
        }
