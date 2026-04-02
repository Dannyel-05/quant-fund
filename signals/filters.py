"""
Pre-trade signal filters: earnings quality, sector contagion, short availability,
swing entry confirmation, earnings proximity, ex-dividend proximity, broad market trend.
Each filter fails open (returns True) if the required data is unavailable.
"""
import logging
from datetime import timedelta
from typing import Dict, Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_SECTOR_ETFS = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Basic Materials": "XLB",
    "Industrials": "XLI",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


class SignalFilters:
    def __init__(self, config: dict):
        self.config = config
        self.cfg = config["filters"]
        # Cache ETF OHLCV data keyed by (etf, start_str) to avoid redundant
        # downloads when many signals land in the same sector and date range.
        self._etf_cache: dict = {}
        self._spy_cache: Optional[pd.DataFrame] = None

    def run_all(
        self,
        ticker: str,
        market: str = "us",
        price_data: pd.DataFrame = None,
        signal_date: Optional[pd.Timestamp] = None,
        direction: int = 1,  # +1 long, -1 short
    ) -> Dict:
        failures = []

        if self.cfg.get("earnings_quality"):
            if not self.earnings_quality(ticker, signal_date=signal_date):
                failures.append("earnings_quality")

        if self.cfg.get("sector_contagion_check"):
            if not self.sector_contagion(ticker, signal_date=signal_date):
                failures.append("sector_contagion")

        if market == "uk" and self.cfg.get("short_availability_check"):
            if not self.short_availability(ticker):
                failures.append("short_availability")

        # Swing-specific filters (only run when enabled in config)
        if self.cfg.get("swing_filters", False):
            if not self.swing_entry_confirmation(ticker, direction, price_data):
                failures.append("swing_entry_confirmation")

            if not self.near_earnings(ticker, days=self.cfg.get("earnings_buffer_days", 5)):
                failures.append("near_earnings")

            if not self.near_ex_dividend(ticker, days=self.cfg.get("exdiv_buffer_days", 3)):
                failures.append("near_ex_dividend")

            if self.cfg.get("broad_market_filter", True):
                if not self.broad_market_trend(direction):
                    failures.append("broad_market_trend")

        return {"ticker": ticker, "passed": len(failures) == 0, "failures": failures}

    # ------------------------------------------------------------------
    # Existing filters
    # ------------------------------------------------------------------

    def earnings_quality(
        self, ticker: str, signal_date: Optional[pd.Timestamp] = None
    ) -> bool:
        """
        Flag if quarterly revenue fell >20% in the quarter preceding signal_date
        (possible accrual-driven beat).  Uses historical financials when signal_date
        is provided; falls back to live data for live trading.
        """
        try:
            t = yf.Ticker(ticker)
            fin = t.quarterly_financials
            if fin is None or fin.empty or "Total Revenue" not in fin.index:
                return True
            rev = fin.loc["Total Revenue"].dropna()
            if len(rev) < 2:
                return True
            # When backtesting, restrict to quarters before the signal date
            if signal_date is not None:
                rev = rev[rev.index <= signal_date]
                if len(rev) < 2:
                    return True
            if rev.iloc[0] < rev.iloc[1] * 0.80:
                logger.debug("%s: revenue down >20%% — quality flag", ticker)
                return False
            return True
        except Exception:
            return True

    def sector_contagion(
        self, ticker: str, signal_date: Optional[pd.Timestamp] = None
    ) -> bool:
        """
        Reject if the sector ETF was down >3% in the 5 trading days ending on
        signal_date (the earnings date).  When signal_date is None (live trading)
        uses the last 7 calendar days.  ETF data is cached per (etf, start_date)
        to avoid redundant downloads across many signals in the same sector.
        """
        try:
            sector = yf.Ticker(ticker).info.get("sector", "")
            etf = _SECTOR_ETFS.get(sector)
            if not etf:
                return True

            if signal_date is not None:
                # Fetch 14 calendar days ending on signal_date to guarantee ≥5
                # trading days even around holidays.
                start = (signal_date - timedelta(days=14)).strftime("%Y-%m-%d")
                end   = (signal_date + timedelta(days=1)).strftime("%Y-%m-%d")
                cache_key = (etf, start)
                if cache_key not in self._etf_cache:
                    self._etf_cache[cache_key] = yf.download(
                        etf, start=start, end=end, progress=False, auto_adjust=True
                    )
                data = self._etf_cache[cache_key]
            else:
                data = yf.download(etf, period="7d", progress=False, auto_adjust=True)

            if data.empty or len(data) < 2:
                return True

            # Extract Close — yfinance returns MultiIndex (field, ticker) for
            # single-ticker downloads too. Flatten to a plain Series.
            raw_close = data["Close"]
            if isinstance(raw_close, pd.DataFrame):
                close = raw_close.iloc[:, 0]
            else:
                close = raw_close

            # 5 trading days ending at or before signal_date
            window = close.tail(5)
            if len(window) < 2:
                return True
            sector_return = float(window.iloc[-1]) / float(window.iloc[0]) - 1
            passed = sector_return > -0.03
            logger.debug(
                "sector_contagion %s/%s @ %s: 5d_return=%.2f%% → %s",
                ticker, etf, signal_date, sector_return * 100,
                "PASS" if passed else "FAIL",
            )
            return passed
        except Exception:
            return True

    def short_availability(self, ticker: str) -> bool:
        """
        Simplified short-availability check for UK names.
        In production: query broker's locate API.
        """
        try:
            cap = yf.Ticker(ticker).info.get("marketCap") or 0
            return cap > 30_000_000  # very small caps are hard to borrow
        except Exception:
            return True

    def sentiment(self, ticker: str) -> bool:
        """Placeholder — connect a news/sentiment API in production."""
        return True

    # ------------------------------------------------------------------
    # Swing trading filters (Refinement 8)
    # ------------------------------------------------------------------

    def _compute_rsi(self, close: pd.Series, window: int = 14) -> float:
        """Compute RSI for the last data point."""
        if len(close) < window + 1:
            return 50.0
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(window).mean()
        loss = (-delta.clip(upper=0)).rolling(window).mean()
        rs = gain / loss.replace(0, float("nan"))
        rsi = 100.0 - 100.0 / (1.0 + rs)
        val = rsi.iloc[-1]
        return float(val) if pd.notna(val) else 50.0

    def swing_entry_confirmation(
        self,
        ticker: str,
        direction: int,
        price_data: pd.DataFrame = None,
    ) -> bool:
        """
        Technical confirmation filters for swing trade entry:
        - Long: price must be above 20-day MA; RSI not overbought (≤70)
        - Short: price must be below 20-day MA; RSI not oversold (≥30)
        - Volume on entry day must be ≥80% of 20-day average
        Fails open when data is unavailable.
        """
        try:
            if price_data is not None and not price_data.empty and len(price_data) >= 21:
                data = price_data
            else:
                data = yf.download(ticker, period="60d", progress=False, auto_adjust=True)

            if data is None or data.empty or len(data) < 21:
                return True

            close = data["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]

            volume = data["Volume"]
            if isinstance(volume, pd.DataFrame):
                volume = volume.iloc[:, 0]

            last_close = float(close.iloc[-1])
            ma20 = float(close.rolling(20).mean().iloc[-1])

            # Price vs 20-day MA
            if direction > 0 and last_close < ma20:
                logger.debug(
                    "%s: swing long rejected — price %.2f below MA20 %.2f",
                    ticker, last_close, ma20,
                )
                return False
            if direction < 0 and last_close > ma20:
                logger.debug(
                    "%s: swing short rejected — price %.2f above MA20 %.2f",
                    ticker, last_close, ma20,
                )
                return False

            # RSI guard — reject extreme readings at entry
            rsi = self._compute_rsi(close)
            if direction > 0 and rsi > 70:
                logger.debug("%s: swing long rejected — RSI %.1f overbought", ticker, rsi)
                return False
            if direction < 0 and rsi < 30:
                logger.debug("%s: swing short rejected — RSI %.1f oversold", ticker, rsi)
                return False

            # Volume confirmation: entry day volume ≥ 80% of 20-day average
            avg_vol = float(volume.rolling(20).mean().iloc[-1])
            last_vol = float(volume.iloc[-1])
            if avg_vol > 0 and last_vol < avg_vol * 0.80:
                logger.debug(
                    "%s: swing entry rejected — low volume %.0f vs avg %.0f (%.0f%%)",
                    ticker, last_vol, avg_vol, 100 * last_vol / avg_vol,
                )
                return False

            return True
        except Exception:
            return True

    def near_earnings(self, ticker: str, days: int = 5) -> bool:
        """
        Return False (block entry) if next earnings is within `days` calendar days.
        Fails open if earnings date cannot be determined.
        """
        try:
            cal = yf.Ticker(ticker).calendar
            if cal is None:
                return True

            next_earnings = None
            if isinstance(cal, dict):
                dates = cal.get("Earnings Date", [])
                if dates:
                    next_earnings = pd.Timestamp(dates[0])
            elif isinstance(cal, pd.DataFrame):
                if "Earnings Date" in cal.columns:
                    next_earnings = pd.Timestamp(cal["Earnings Date"].iloc[0])
                elif "Earnings Date" in cal.index:
                    next_earnings = pd.Timestamp(cal.loc["Earnings Date"].iloc[0])

            if next_earnings is None:
                return True

            today = pd.Timestamp.now().normalize()
            days_until = (next_earnings.normalize() - today).days
            if 0 <= days_until <= days:
                logger.debug("%s: blocked — earnings in %d days", ticker, days_until)
                return False
            return True
        except Exception:
            return True

    def near_ex_dividend(self, ticker: str, days: int = 3) -> bool:
        """
        Return False (block entry) if ex-dividend date is within `days` calendar days.
        Fails open if ex-div date unavailable.
        """
        try:
            info = yf.Ticker(ticker).info
            ex_div = info.get("exDividendDate")
            if not ex_div:
                return True

            if isinstance(ex_div, (int, float)):
                ex_div_ts = pd.Timestamp(ex_div, unit="s")
            else:
                ex_div_ts = pd.Timestamp(ex_div)

            today = pd.Timestamp.now().normalize()
            days_until = (ex_div_ts.normalize() - today).days
            if 0 <= days_until <= days:
                logger.debug("%s: blocked — ex-dividend in %d days", ticker, days_until)
                return False
            return True
        except Exception:
            return True

    def broad_market_trend(self, direction: int) -> bool:
        """
        Check SPY vs its 50-day MA to confirm broad market regime.
        - Long trades: SPY must be above 50d MA (bull regime)
        - Short trades: SPY must be below 50d MA (bear regime)
        Fails open if data unavailable. Result is cached within the instance.
        """
        try:
            if self._spy_cache is None or self._spy_cache.empty:
                self._spy_cache = yf.download(
                    "SPY", period="90d", progress=False, auto_adjust=True
                )

            spy = self._spy_cache
            if spy is None or spy.empty or len(spy) < 51:
                return True

            close = spy["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]

            ma50 = float(close.rolling(50).mean().iloc[-1])
            last = float(close.iloc[-1])
            spy_above_ma = last > ma50

            if direction > 0 and not spy_above_ma:
                logger.debug(
                    "broad_market: SPY %.2f below MA50 %.2f — long rejected", last, ma50
                )
                return False
            if direction < 0 and spy_above_ma:
                logger.debug(
                    "broad_market: SPY %.2f above MA50 %.2f — short rejected", last, ma50
                )
                return False
            return True
        except Exception:
            return True

    def invalidate_spy_cache(self) -> None:
        """Call once per session to force fresh SPY data."""
        self._spy_cache = None
