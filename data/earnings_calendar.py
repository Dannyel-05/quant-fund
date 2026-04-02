import logging
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class EarningsCalendar:
    def __init__(self, config: dict, fetcher):
        self.config = config
        self.fetcher = fetcher

    def get_earnings_surprise(self, ticker: str) -> pd.DataFrame:
        """
        Returns DataFrame indexed by date with columns:
        [epsEstimate, epsActual, epsDifference, surprisePercent]

        surprisePercent is always stored as a FRACTION (e.g. 0.1676 for a 16.76% beat).

        Source strategy — MERGE all available sources for maximum coverage:
          1. Finnhub (primary — best small-cap coverage, real-time API)
          2. Alpha Vantage (secondary — supplementary quarterly data)
          3. yfinance earnings_dates (tertiary — 25+ quarters, best for backtest depth)
          4. yfinance earnings_history (last resort — 4 quarters only)

        Sources are merged by fiscal quarter; the first source with a non-null
        epsActual for that quarter wins.
        """
        frames = []
        sources_used = []

        # 1. Finnhub (primary)
        try:
            finnhub_key = (self.config.get("api_keys") or {}).get("finnhub", "")
            if finnhub_key:
                fh_df = self._fetch_finnhub_earnings(ticker, self.config)
                if fh_df is not None and not fh_df.empty:
                    frames.append(fh_df)
                    sources_used.append("finnhub")
        except Exception as e:
            logger.debug("Finnhub earnings failed for %s: %s", ticker, e)

        # 2. Alpha Vantage (secondary)
        try:
            av_key = (self.config.get("api_keys") or {}).get("alpha_vantage", "")
            if av_key:
                av_df = self._fetch_alpha_vantage_earnings(ticker, self.config)
                if av_df is not None and not av_df.empty:
                    frames.append(av_df)
                    sources_used.append("alpha_vantage")
        except Exception as e:
            logger.debug("Alpha Vantage earnings failed for %s: %s", ticker, e)

        # 3. yfinance earnings_dates (tertiary — richest historical depth)
        try:
            t = yf.Ticker(ticker)
            ed = t.earnings_dates
            if ed is not None and not ed.empty:
                ed = ed[ed["Reported EPS"].notna()].copy()
                if not ed.empty:
                    ed.index = pd.to_datetime(ed.index).tz_localize(None)
                    ed = ed.rename(columns={
                        "EPS Estimate": "epsEstimate",
                        "Reported EPS": "epsActual",
                        "Surprise(%)": "surprisePercent",
                    })
                    ed["surprisePercent"] = ed["surprisePercent"] / 100.0
                    ed["epsDifference"] = ed["epsActual"] - ed["epsEstimate"].fillna(0)
                    frames.append(ed.sort_index())
                    sources_used.append("yfinance_dates")
        except Exception as e:
            logger.debug("yfinance earnings_dates failed for %s: %s", ticker, e)

        # Merge: combine frames, keeping first non-null epsActual per date bucket
        if frames:
            COLS = ["epsActual", "epsEstimate", "epsDifference", "surprisePercent"]
            # Round dates to quarter to align across sources
            merged = pd.concat(frames)
            merged.index = pd.to_datetime(merged.index)
            # De-duplicate by keeping first occurrence (Finnhub/AV take precedence
            # when their data is more recent, yfinance fills older history)
            merged = merged[~merged.index.duplicated(keep="first")]
            merged = merged.sort_index()
            for col in COLS:
                if col not in merged.columns:
                    merged[col] = None
            logger.debug("%s: earnings merged from %s (%d rows)",
                         ticker, "+".join(sources_used), len(merged))
            return merged[COLS]

        # 4. Last resort: yfinance earnings_history (4 quarters only)
        try:
            t = yf.Ticker(ticker)
            history = t.earnings_history
            if history is not None and not history.empty:
                history.index = pd.to_datetime(history.index)
                return history.sort_index()
            return self._fallback_earnings(t)
        except Exception as e:
            logger.error("get_earnings_surprise(%s): %s", ticker, e)
            return pd.DataFrame()

    def _fetch_finnhub_earnings(self, ticker: str, config: dict) -> Optional[pd.DataFrame]:
        """
        Fetch earnings history from Finnhub earnings calendar API.
        Returns DataFrame with surprisePercent as a FRACTION (e.g. 0.1676).
        """
        import requests
        api_key = (config.get("api_keys") or {}).get("finnhub", "")
        if not api_key:
            return None

        t_symbol = ticker.replace(".L", "")
        url = f"https://finnhub.io/api/v1/stock/earnings"
        try:
            resp = requests.get(
                url,
                params={"symbol": t_symbol, "token": api_key},
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, list) or not data:
                return None

            rows = []
            for item in data:
                date_str = item.get("period", "")
                if not date_str:
                    continue
                eps_actual   = item.get("actual")
                eps_estimate = item.get("estimate")
                surprise_pct = item.get("surprisePercent")  # Finnhub returns as percentage
                try:
                    date = pd.to_datetime(date_str)
                    row = {
                        "epsActual":      float(eps_actual)   if eps_actual   is not None else None,
                        "epsEstimate":    float(eps_estimate) if eps_estimate is not None else None,
                        # Finnhub surprisePercent is a percentage (e.g. 16.76) — convert to fraction
                        "surprisePercent": float(surprise_pct) / 100.0 if surprise_pct is not None else None,
                    }
                    row["epsDifference"] = (
                        row["epsActual"] - (row["epsEstimate"] or 0)
                        if row["epsActual"] is not None else None
                    )
                    rows.append((date, row))
                except Exception:
                    continue

            if not rows:
                return None

            idx = pd.DatetimeIndex([r[0] for r in rows])
            df = pd.DataFrame([r[1] for r in rows], index=idx)
            return df.sort_index()

        except Exception as e:
            logger.debug("_fetch_finnhub_earnings(%s): %s", ticker, e)
            return None

    def _fetch_alpha_vantage_earnings(self, ticker: str, config: dict) -> Optional[pd.DataFrame]:
        """
        Fetch earnings history from Alpha Vantage EARNINGS function.
        Returns DataFrame with surprisePercent as a FRACTION (e.g. 0.1676).
        """
        import requests
        api_key = (config.get("api_keys") or {}).get("alpha_vantage", "")
        if not api_key:
            return None

        url = "https://www.alphavantage.co/query"
        try:
            resp = requests.get(
                url,
                params={"function": "EARNINGS", "symbol": ticker, "apikey": api_key},
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            quarterly = data.get("quarterlyEarnings", [])
            if not quarterly:
                return None

            rows = []
            for item in quarterly:
                date_str     = item.get("fiscalDateEnding", "")
                eps_actual   = item.get("reportedEPS")
                eps_estimate = item.get("estimatedEPS")
                surprise_pct = item.get("surprisePercentage")  # AV returns as percentage

                if not date_str:
                    continue
                try:
                    date = pd.to_datetime(date_str)
                    row = {
                        "epsActual":      float(eps_actual)   if eps_actual   not in (None, "None", "") else None,
                        "epsEstimate":    float(eps_estimate) if eps_estimate not in (None, "None", "") else None,
                        # Alpha Vantage surprisePercentage is a percentage — convert to fraction
                        "surprisePercent": float(surprise_pct) / 100.0 if surprise_pct not in (None, "None", "") else None,
                    }
                    row["epsDifference"] = (
                        row["epsActual"] - (row["epsEstimate"] or 0)
                        if row["epsActual"] is not None else None
                    )
                    rows.append((date, row))
                except Exception:
                    continue

            if not rows:
                return None

            idx = pd.DatetimeIndex([r[0] for r in rows])
            df = pd.DataFrame([r[1] for r in rows], index=idx)
            return df.sort_index()

        except Exception as e:
            logger.debug("_fetch_alpha_vantage_earnings(%s): %s", ticker, e)
            return None

    def get_upcoming_earnings(self, tickers: List[str], days_ahead: int = 30) -> pd.DataFrame:
        """Scan tickers for upcoming earnings within N days."""
        cutoff = datetime.now() + timedelta(days=days_ahead)
        results = []

        for ticker in tickers:
            try:
                t = yf.Ticker(ticker)
                cal = t.earnings_dates
                if cal is None or cal.empty:
                    continue

                upcoming = cal[
                    (cal.index > datetime.now()) & (cal.index <= cutoff)
                ]
                for date, row in upcoming.iterrows():
                    results.append(
                        {
                            "ticker": ticker,
                            "earnings_date": date,
                            "eps_estimate": row.get("EPS Estimate"),
                        }
                    )
            except Exception:
                continue

        if not results:
            return pd.DataFrame()
        return pd.DataFrame(results).sort_values("earnings_date").reset_index(drop=True)

    def get_earnings_dates(self, ticker: str) -> pd.DatetimeIndex:
        try:
            t = yf.Ticker(ticker)
            dates = t.earnings_dates
            if dates is None or dates.empty:
                return pd.DatetimeIndex([])
            return pd.DatetimeIndex(dates.index)
        except Exception as e:
            logger.error(f"get_earnings_dates({ticker}): {e}")
            return pd.DatetimeIndex([])

    def _fallback_earnings(self, ticker_obj) -> pd.DataFrame:
        try:
            earnings = ticker_obj.quarterly_earnings
            if earnings is None or earnings.empty:
                return pd.DataFrame()
            earnings.index = pd.to_datetime(earnings.index)
            # Rename to match expected schema
            earnings = earnings.rename(
                columns={"Earnings": "epsActual", "Revenue": "revenue"}
            )
            earnings["epsEstimate"] = None
            earnings["surprisePercent"] = None
            return earnings.sort_index()
        except Exception:
            return pd.DataFrame()
