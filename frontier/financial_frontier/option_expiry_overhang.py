"""
Option Expiry Overhang Collector — gamma_overhang_score Signal.

Computes the aggregate Gamma Exposure (GEX) across major equity index ETFs
to measure the degree to which market maker delta-hedging is likely to
amplify or dampen near-term price moves.

Economic hypothesis
-------------------
When options market makers (dealers) are net short gamma, they must hedge
dynamically by buying into rising markets and selling into falling ones,
which AMPLIFIES price moves — a "negative gamma" regime.  Conversely,
when dealers are net long gamma (short from customer side), they hedge by
selling into rallies and buying dips, which DAMPENS volatility — a
"positive gamma" regime.

The Gamma Exposure (GEX) metric, developed by practitioners at Squeezemetrics
and popularised by Cem Karsan, quantifies this:

  GEX = Σ (gamma × open_interest × 100 × S²)

Normalised by price squared and total open interest, this produces a
directional signal:
  - Positive GEX: dealer long gamma → volatility suppression → mean-reversion
    favoured → reduce trend-following exposure.
  - Negative GEX: dealer short gamma → volatility amplification → trend
    continuation more likely → increase trend-following exposure.

This signal is most powerful at major option expiries (monthly/quarterly),
when gamma concentrations unwind and price dynamics often "reset."

Implementation note
-------------------
yfinance provides options chains with gamma and open_interest fields.
The GEX formula is:
  gex_ticker = Σ(gamma_i × open_interest_i × 100) / price²

We sum calls as positive gamma exposure and puts as negative (from dealer
perspective, assuming dealers are short the options customers buy), then
normalise the aggregate across tickers.

Data source
-----------
yfinance (free, no key): options chain data for SPY, QQQ, IWM.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_TICKERS = ["SPY", "QQQ", "IWM"]
_NUM_EXPIRIES = 3   # nearest N expiries to include
_SOURCE = "yfinance options chain"


class OptionExpiryOverhangCollector:
    """
    Fetches options chains for the specified tickers, computes Gamma
    Exposure (GEX) for each, and returns a normalised overhang score.

    The GEX formula applied here:
        gex = Σ(gamma × open_interest × 100) / price²

    Calls contribute positive GEX (dealers short calls → long gamma on calls).
    Puts contribute negative GEX (dealers short puts → short gamma on puts).
    Combined GEX captures net dealer gamma exposure.
    """

    def _compute_ticker_gex(self, ticker_symbol: str) -> tuple[float, dict]:
        """
        Compute normalised GEX for a single ticker across its nearest
        _NUM_EXPIRIES expiry dates.

        Returns (normalised_gex, debug_info).  Returns (0.0, {}) on any error.
        """
        try:
            import yfinance as yf

            ticker = yf.Ticker(ticker_symbol)

            # Current price
            hist = ticker.history(period="1d")
            if hist.empty or "Close" not in hist.columns:
                logger.warning(
                    "OptionExpiryOverhangCollector: no price data for %s.",
                    ticker_symbol,
                )
                return 0.0, {}

            price = float(hist["Close"].iloc[-1])
            if price <= 0:
                return 0.0, {}

            price_sq = price ** 2

            # Get available expiry dates
            expiry_dates = ticker.options
            if not expiry_dates:
                logger.warning(
                    "OptionExpiryOverhangCollector: no options data for %s.",
                    ticker_symbol,
                )
                return 0.0, {}

            # Use nearest _NUM_EXPIRIES expirations
            selected_expiries = list(expiry_dates[:_NUM_EXPIRIES])

            total_gex = 0.0
            total_oi = 0
            expiry_details = []

            for exp_date in selected_expiries:
                try:
                    chain = ticker.option_chain(exp_date)
                    calls = chain.calls
                    puts = chain.puts

                    exp_gex = 0.0

                    # Calls: dealers are net short → positive gamma exposure
                    if calls is not None and not calls.empty:
                        for _, row in calls.iterrows():
                            gamma = float(row.get("gamma") or 0.0)
                            oi = float(row.get("openInterest") or 0.0)
                            exp_gex += gamma * oi * 100.0

                    # Puts: dealers are net short → negative gamma exposure
                    if puts is not None and not puts.empty:
                        for _, row in puts.iterrows():
                            gamma = float(row.get("gamma") or 0.0)
                            oi = float(row.get("openInterest") or 0.0)
                            exp_gex -= gamma * oi * 100.0

                    calls_oi = int(calls["openInterest"].sum()) if calls is not None and not calls.empty else 0
                    puts_oi = int(puts["openInterest"].sum()) if puts is not None and not puts.empty else 0

                    expiry_details.append({
                        "expiry": exp_date,
                        "gex": round(exp_gex, 2),
                        "calls_oi": calls_oi,
                        "puts_oi": puts_oi,
                    })

                    total_gex += exp_gex
                    total_oi += calls_oi + puts_oi

                except Exception as exc:
                    logger.warning(
                        "OptionExpiryOverhangCollector: error processing "
                        "expiry %s for %s: %s",
                        exp_date,
                        ticker_symbol,
                        exc,
                    )
                    continue

            # Normalise by price squared
            normalised_gex = total_gex / price_sq if price_sq > 0 else 0.0

            debug_info = {
                "ticker": ticker_symbol,
                "price": round(price, 2),
                "total_raw_gex": round(total_gex, 2),
                "normalised_gex": round(normalised_gex, 6),
                "total_open_interest": total_oi,
                "expiries": expiry_details,
            }

            return float(normalised_gex), debug_info

        except ImportError:
            logger.warning(
                "OptionExpiryOverhangCollector: yfinance not installed."
            )
        except Exception as exc:
            logger.warning(
                "OptionExpiryOverhangCollector: unexpected error for %s: %s",
                ticker_symbol,
                exc,
            )
        return 0.0, {}

    def collect(self, tickers: Optional[list] = None) -> dict:
        """
        Compute GEX across all tickers and return the signal dict.

        The gamma_overhang_score is the mean of normalised GEX values across
        tickers, clamped to [-1, 1].  Positive = volatility suppression
        regime; negative = volatility amplification regime.

        Parameters
        ----------
        tickers : list of str, optional
            Equity tickers to include.  Defaults to ["SPY", "QQQ", "IWM"].

        Returns
        -------
        dict with keys:
            signal_name   : "gamma_overhang_score"
            value         : float in [-1, 1]
            raw_data      : dict — per-ticker GEX details
            quality_score : fraction of tickers with valid data
            timestamp     : ISO-8601 UTC string
            source        : "yfinance options chain"
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        if tickers is None:
            tickers = _DEFAULT_TICKERS

        gex_values = []
        ticker_details = []
        successful = 0

        for ticker_symbol in tickers:
            gex, debug_info = self._compute_ticker_gex(ticker_symbol)
            ticker_details.append(debug_info if debug_info else {"ticker": ticker_symbol, "error": "no data"})
            if debug_info:
                gex_values.append(gex)
                successful += 1

        if not gex_values:
            logger.warning(
                "OptionExpiryOverhangCollector: no GEX data for any ticker."
            )
            option_overhang = 0.0
            quality_score = 0.0
        else:
            mean_gex = sum(gex_values) / len(gex_values)
            # Clamp to [-1, 1]
            option_overhang = max(-1.0, min(1.0, mean_gex))
            quality_score = successful / len(tickers)

        raw_data = {
            "tickers_requested": tickers,
            "tickers_successful": successful,
            "individual_gex": gex_values,
            "mean_gex_pre_clamp": round(sum(gex_values) / len(gex_values), 6) if gex_values else 0.0,
            "ticker_details": ticker_details,
        }

        return {
            "signal_name": "gamma_overhang_score",
            "value": float(option_overhang),
            "raw_data": raw_data,
            "quality_score": float(quality_score),
            "timestamp": timestamp,
            "source": _SOURCE,
        }
