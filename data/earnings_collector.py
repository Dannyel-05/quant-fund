"""
Daily earnings data collector.

Collects for each ticker × earnings event:
  - EPS actuals, estimates (yfinance + SEC EDGAR 8-K fallback)
  - Price captures at t+0/1/3/5/10/20 trading days
  - Alt-data signals (sentiment from altdata module when available)
  - Market context: VIX, SPY return, sector ETF return

Stores results in EarningsDB (SQLite WAL).

Usage:
    from data.earnings_collector import EarningsCollector
    collector = EarningsCollector(config)
    collector.collect(tickers, market="us")
    collector.collect_calendar(tickers, market="us")
"""
import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf

from data.earnings_db import EarningsDB

logger = logging.getLogger(__name__)

# Sector → ETF mapping (same as filters.py)
_SECTOR_ETFS = {
    "Technology":             "XLK",
    "Financial Services":     "XLF",
    "Healthcare":             "XLV",
    "Consumer Cyclical":      "XLY",
    "Consumer Defensive":     "XLP",
    "Energy":                 "XLE",
    "Utilities":              "XLU",
    "Basic Materials":        "XLB",
    "Industrials":            "XLI",
    "Real Estate":            "XLRE",
    "Communication Services": "XLC",
}

# How many trading-day offsets to capture
_PRICE_OFFSETS = [0, 1, 3, 5, 10, 20]


class EarningsCollector:
    def __init__(self, config: dict, db_path: str = "output/earnings.db"):
        self.config = config
        self.db = EarningsDB(db_path)
        self._price_cache: dict = {}      # (ticker, start, end) → DataFrame
        self._vix_cache:   dict = {}      # date_str → float
        self._spy_cache:   dict = {}      # date_str → float (5d return)
        self._etf_cache:   dict = {}      # (etf, start_str) → DataFrame
        self._info_cache:  dict = {}      # ticker → info dict
        self._altdata_store = None        # lazy-loaded AltDataStore

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def collect(
        self,
        tickers: List[str],
        market: str = "us",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> int:
        """
        Collect historical earnings observations for all tickers.
        Returns total rows written.
        """
        if start is None:
            start = (datetime.now() - timedelta(days=365 * 7)).strftime("%Y-%m-%d")
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        total = 0
        for i, ticker in enumerate(tickers):
            try:
                n = self._collect_ticker(ticker, market, start, end)
                total += n
                if n:
                    logger.info("%s: %d observations written", ticker, n)
            except Exception as e:
                logger.error("%s: collection failed — %s", ticker, e)
            # Gentle rate-limiting
            if i % 20 == 19:
                time.sleep(1.0)

        logger.info("Collection complete: %d total observations written", total)
        return total

    def collect_calendar(
        self,
        tickers: List[str],
        market: str = "us",
        days_ahead: int = 30,
    ) -> int:
        """
        Collect upcoming earnings calendar for tickers.
        Returns number of calendar entries written.
        """
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        written = 0
        for ticker in tickers:
            try:
                t = yf.Ticker(ticker)
                cal = t.calendar
                if cal is None:
                    continue
                # yfinance calendar has varying formats across versions
                if isinstance(cal, dict):
                    earnings_date = cal.get("Earnings Date")
                    if earnings_date is None:
                        continue
                    if isinstance(earnings_date, (list, tuple)):
                        earnings_date = earnings_date[0]
                    date_str = pd.Timestamp(earnings_date).strftime("%Y-%m-%d")
                elif isinstance(cal, pd.DataFrame):
                    if "Earnings Date" not in cal.columns:
                        continue
                    date_str = pd.Timestamp(cal["Earnings Date"].iloc[0]).strftime("%Y-%m-%d")
                else:
                    continue

                # Only store if within days_ahead
                cutoff = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
                if date_str > cutoff:
                    continue

                # Fetch estimate if available
                eps_est = None
                try:
                    if isinstance(cal, dict):
                        eps_est = float(cal.get("EPS Estimate", {}) or 0) or None
                    elif isinstance(cal, pd.DataFrame) and "EPS Estimate" in cal.columns:
                        v = cal["EPS Estimate"].iloc[0]
                        eps_est = float(v) if pd.notna(v) else None
                except Exception:
                    pass

                self.db.upsert_calendar({
                    "ticker":        ticker,
                    "earnings_date": date_str,
                    "market":        market,
                    "eps_estimate":  eps_est,
                    "added_at":      now_str,
                })
                written += 1
            except Exception as e:
                logger.debug("%s: calendar fetch failed — %s", ticker, e)

        logger.info("Calendar: %d entries written for %d tickers", written, len(tickers))
        return written

    # ------------------------------------------------------------------
    # Per-ticker collection
    # ------------------------------------------------------------------

    def _collect_ticker(
        self, ticker: str, market: str, start: str, end: str
    ) -> int:
        t = yf.Ticker(ticker)
        earnings_hist = self._fetch_earnings_history(t, ticker)
        if earnings_hist is None or earnings_hist.empty:
            return 0

        # Fetch price data for the full range + 30 extra days for return windows
        fetch_start = (pd.Timestamp(start) - pd.Timedelta(days=40)).strftime("%Y-%m-%d")
        fetch_end   = (pd.Timestamp(end)   + pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        prices = self._fetch_prices(ticker, fetch_start, fetch_end)
        if prices.empty:
            return 0

        # Sector ETF for this ticker
        sector_etf = self._get_sector_etf(ticker, t)

        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        records = []

        for date, row in earnings_hist.iterrows():
            # Only collect events within requested range
            if not (pd.Timestamp(start) <= date <= pd.Timestamp(end)):
                continue

            record = self._build_record(
                ticker, market, date, row, prices, sector_etf, now_str
            )
            if record:
                records.append(record)

        return self.db.upsert_observations_batch(records)

    def _build_record(
        self,
        ticker: str,
        market: str,
        date: pd.Timestamp,
        row: pd.Series,
        prices: pd.DataFrame,
        sector_etf: Optional[str],
        now_str: str,
    ) -> Optional[dict]:
        def _f(key):
            v = row.get(key)
            try:
                f = float(v)
                return None if pd.isna(f) else f
            except (TypeError, ValueError):
                return None

        actual   = _f("epsActual")
        estimate = _f("epsEstimate")
        diff     = _f("epsDifference")
        pct_yf   = _f("surprisePercent")

        # Compute surprise_pct and quality
        if actual is not None and estimate is not None and estimate != 0:
            surprise_pct  = (actual - estimate) / abs(estimate)
            data_quality  = "high"
        elif pct_yf is not None and actual is not None and estimate is not None:
            surprise_pct  = pct_yf / 100.0
            data_quality  = "high"
        elif diff is not None and actual is not None:
            surprise_pct  = None   # epsDiff-only — no meaningful % without estimate
            data_quality  = "low"
        else:
            surprise_pct  = None
            data_quality  = "missing"

        # Price captures
        date_str = date.strftime("%Y-%m-%d")
        p = self._price_captures(prices, date)
        if p["t0"] is None:
            return None   # no price data for this date — skip

        # Returns relative to t0
        def ret(tx_key):
            tx = p.get(tx_key)
            if tx is None or p["t0"] is None:
                return None
            return tx / p["t0"] - 1

        # Volume context
        vol_t0, vol_avg = self._volume_context(prices, date)

        # Market context
        vix      = self._vix(date)
        spy_ret  = self._spy_return_5d(date)
        etf_ret  = self._etf_return_5d(sector_etf, date) if sector_etf else None

        return {
            "ticker":              ticker,
            "earnings_date":       date_str,
            "market":              market,
            "eps_actual":          actual,
            "eps_estimate":        estimate,
            "eps_difference":      diff,
            "surprise_pct":        surprise_pct,
            "surprise_percent_yf": pct_yf,
            "data_quality":        data_quality,
            "price_t0":            p["t0"],
            "price_t1":            p["t1"],
            "price_t3":            p["t3"],
            "price_t5":            p["t5"],
            "price_t10":           p["t10"],
            "price_t20":           p["t20"],
            "return_t1":           ret("t1"),
            "return_t3":           ret("t3"),
            "return_t5":           ret("t5"),
            "return_t10":          ret("t10"),
            "return_t20":          ret("t20"),
            "volume_t0":           vol_t0,
            "volume_avg_20d":      vol_avg,
            "volume_surge":        (vol_t0 / vol_avg) if (vol_t0 and vol_avg) else None,
            "vix_t0":              vix,
            "spy_return_5d":       spy_ret,
            "sector_etf_return_5d": etf_ret,
            "sector_etf_ticker":   sector_etf,
            "altdata_sentiment":   self._get_altdata_sentiment(ticker),
            "reddit_score":        self._get_source_score(ticker, "reddit"),
            "news_score":          self._get_source_score(ticker, "news"),
            "sec_score":           self._get_source_score(ticker, "sec_edgar"),
            "collected_at":        now_str,
            "source":              "yfinance",
        }

    # ------------------------------------------------------------------
    # Data helpers
    # ------------------------------------------------------------------

    def _fetch_earnings_history(self, t: yf.Ticker, ticker: str) -> Optional[pd.DataFrame]:
        try:
            hist = t.earnings_history
            if hist is None or hist.empty:
                return None
            hist.index = pd.to_datetime(hist.index)
            return hist.sort_index()
        except Exception as e:
            logger.debug("%s: earnings_history failed — %s", ticker, e)
            return None

    def _fetch_prices(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        cache_key = (ticker, start, end)
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        try:
            raw = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            if raw.empty:
                return pd.DataFrame()
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).lower().strip() for c in raw.columns]
            raw.index = pd.to_datetime(raw.index)
            df = raw[[c for c in ["open", "high", "low", "close", "volume"] if c in raw.columns]]
            self._price_cache[cache_key] = df
            return df
        except Exception as e:
            logger.debug("Price fetch failed for %s: %s", ticker, e)
            return pd.DataFrame()

    def _price_captures(self, prices: pd.DataFrame, date: pd.Timestamp) -> dict:
        idx = prices.index
        result = {}
        # t0: closest trading day on or before earnings date
        t0_dates = idx[idx <= date]
        if len(t0_dates) == 0:
            return {"t0": None}
        t0 = t0_dates[-1]
        result["t0"] = float(prices.loc[t0, "close"]) if "close" in prices.columns else None

        # t1, t3, t5, t10, t20: Nth trading day strictly after date
        future = idx[idx > date]
        for offset_key, offset_n in [("t1", 1), ("t3", 3), ("t5", 5), ("t10", 10), ("t20", 20)]:
            if len(future) >= offset_n:
                result[offset_key] = float(prices.loc[future[offset_n - 1], "close"])
            else:
                result[offset_key] = None
        return result

    def _volume_context(self, prices: pd.DataFrame, date: pd.Timestamp):
        if "volume" not in prices.columns:
            return None, None
        try:
            window = prices.loc[:date]["volume"]
            if len(window) == 0:
                return None, None
            vol_t0  = float(window.iloc[-1])
            vol_avg = float(window.iloc[:-1].tail(20).mean()) if len(window) > 1 else None
            return vol_t0, vol_avg
        except Exception:
            return None, None

    def _get_sector_etf(self, ticker: str, t: yf.Ticker) -> Optional[str]:
        if ticker not in self._info_cache:
            try:
                self._info_cache[ticker] = t.info
            except Exception:
                self._info_cache[ticker] = {}
        sector = self._info_cache[ticker].get("sector", "")
        return _SECTOR_ETFS.get(sector)

    def _vix(self, date: pd.Timestamp) -> Optional[float]:
        date_str = date.strftime("%Y-%m-%d")
        if date_str in self._vix_cache:
            return self._vix_cache[date_str]
        try:
            start = (date - timedelta(days=5)).strftime("%Y-%m-%d")
            end   = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            raw = yf.download("^VIX", start=start, end=end, auto_adjust=True, progress=False)
            if raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).lower() for c in raw.columns]
            # closest available date on or before earnings
            available = raw.index[raw.index <= date]
            if len(available) == 0:
                return None
            val = float(raw.loc[available[-1], "close"])
            self._vix_cache[date_str] = val
            return val
        except Exception:
            return None

    def _spy_return_5d(self, date: pd.Timestamp) -> Optional[float]:
        date_str = date.strftime("%Y-%m-%d")
        if date_str in self._spy_cache:
            return self._spy_cache[date_str]
        try:
            start = (date - timedelta(days=14)).strftime("%Y-%m-%d")
            end   = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            raw = yf.download("SPY", start=start, end=end, auto_adjust=True, progress=False)
            if raw.empty or len(raw) < 2:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).lower() for c in raw.columns]
            available = raw.index[raw.index <= date]
            window = available[-5:] if len(available) >= 5 else available
            if len(window) < 2:
                return None
            val = float(raw.loc[window[-1], "close"]) / float(raw.loc[window[0], "close"]) - 1
            self._spy_cache[date_str] = val
            return val
        except Exception:
            return None

    def _altdata_store_instance(self):
        if self._altdata_store is None:
            try:
                from altdata.storage.altdata_store import AltDataStore
                self._altdata_store = AltDataStore(self.config)
            except Exception:
                self._altdata_store = False  # mark as unavailable
        return self._altdata_store if self._altdata_store else None

    def _get_altdata_sentiment(self, ticker: str) -> Optional[float]:
        store = self._altdata_store_instance()
        if store is None:
            return None
        try:
            rows = store.get_sentiment(ticker, hours_back=48)
            if not rows:
                return None
            scores = [float(r.get("score", 0)) for r in rows if r.get("score") is not None]
            return round(sum(scores) / len(scores), 4) if scores else None
        except Exception:
            return None

    def _get_source_score(self, ticker: str, source: str) -> Optional[float]:
        store = self._altdata_store_instance()
        if store is None:
            return None
        try:
            rows = store.get_sentiment(ticker, hours_back=48)
            src_rows = [r for r in rows if r.get("source") == source]
            if not src_rows:
                return None
            return round(float(src_rows[0]["score"]), 4)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Intelligence engine integration (Phase 10)
    # ------------------------------------------------------------------

    def run_intelligence_update(self, intel_db=None, hist_db=None) -> dict:
        """
        Trigger an intelligence engine run after earnings collection.
        Updates company profiles, patterns, and signal effectiveness in intelligence_db.
        Safe to call even if intelligence_db is unavailable.
        """
        try:
            if intel_db is None:
                from analysis.intelligence_db import IntelligenceDB
                intel_db = IntelligenceDB()
            from analysis.intelligence_engine import IntelligenceEngine
            engine = IntelligenceEngine(intel_db, self.db, hist_db)
            summary = engine.run()
            logger.info("Intelligence update complete: %s", summary)
            return summary
        except Exception as e:
            logger.warning("Intelligence update failed (non-fatal): %s", e)
            return {}

    def get_intelligence_signals(self, ticker: str) -> list:
        """
        Retrieve intelligence signals for a ticker to pass into SignalAggregator.
        Returns list of signal dicts with signal_name, value, strength, source.
        """
        signals = []
        try:
            from analysis.intelligence_db import IntelligenceDB
            from analysis.intelligence_engine import IntelligenceEngine
            intel_db = IntelligenceDB()
            engine   = IntelligenceEngine(intel_db, self.db)

            # Composite profile score
            profile = intel_db.get_profile(ticker)
            if profile:
                beat_rate = profile.get("beat_rate") or 0.5
                avg_pead  = profile.get("avg_pead_return") or 0.0
                profile_score = (beat_rate - 0.5) * 2.0 + avg_pead * 5.0
                signals.append({
                    "signal_name": "intelligence_profile",
                    "value":       round(max(-1, min(1, profile_score)), 4),
                    "strength":    min(abs(profile_score), 1.0),
                    "quality_score": 0.7,
                    "source":      "intelligence_engine:profile",
                })

            # Readthrough signal
            if engine.influence_engine:
                rt = engine.influence_engine.score_peer(ticker, days_lookback=14)
                if rt and rt.get("score") is not None:
                    signals.append({
                        "signal_name": "readthrough",
                        "value":       round(rt["score"], 4),
                        "strength":    min(abs(rt["score"]), 1.0),
                        "quality_score": 0.65,
                        "source":      "intelligence_engine:readthrough",
                    })
        except Exception as e:
            logger.debug("get_intelligence_signals failed for %s: %s", ticker, e)

        return signals

    def _etf_return_5d(self, etf: str, date: pd.Timestamp) -> Optional[float]:
        date_str = date.strftime("%Y-%m-%d")
        cache_key = (etf, date_str)
        if cache_key in self._etf_cache:
            return self._etf_cache[cache_key]
        try:
            start = (date - timedelta(days=14)).strftime("%Y-%m-%d")
            end   = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            raw = yf.download(etf, start=start, end=end, auto_adjust=True, progress=False)
            if raw.empty or len(raw) < 2:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).lower() for c in raw.columns]
            available = raw.index[raw.index <= date]
            window = available[-5:] if len(available) >= 5 else available
            if len(window) < 2:
                return None
            val = float(raw.loc[window[-1], "close"]) / float(raw.loc[window[0], "close"]) - 1
            self._etf_cache[cache_key] = val
            return val
        except Exception:
            return None
