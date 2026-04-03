import pickle
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, config: dict, cache_dir: str = "data/cache"):
        self.config = config
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_hours = config.get("cache_ttl_hours", 24)

    def fetch_ohlcv(
        self,
        ticker: str,
        start: str,
        end: str,
        market: str = "us",
        use_cache: bool = True,
    ) -> pd.DataFrame:
        cache_key = f"ohlcv_{ticker}_{start}_{end}"
        if use_cache:
            cached = self._load_from_cache(cache_key)
            if cached is not None:
                return cached

        try:
            raw = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            if raw.empty:
                logger.warning(f"No data for {ticker}")
                return pd.DataFrame()

            # yfinance returns MultiIndex columns for index tickers (e.g. ^GSPC)
            # when downloaded as a single ticker. Flatten before normalising.
            if isinstance(raw.columns, pd.MultiIndex):
                extracted = self._extract_ticker(raw, ticker)
                if extracted is not None and not extracted.empty:
                    df = extracted
                else:
                    # Find the level that contains OHLCV field names and use it.
                    # yfinance column order varies by version: (field, ticker) or (ticker, field).
                    _ohlcv_fields = {'open', 'high', 'low', 'close', 'volume', 'adj close'}
                    chosen_level = 0
                    for _lvl in range(raw.columns.nlevels):
                        _vals = {str(v).lower() for v in raw.columns.get_level_values(_lvl).unique()}
                        if _vals & _ohlcv_fields:
                            chosen_level = _lvl
                            break
                    raw.columns = raw.columns.get_level_values(chosen_level)
                    df = self._normalize_columns(raw)
            else:
                df = self._normalize_columns(raw)

            cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            if not cols:
                logger.error(f"Failed to fetch {ticker}: no recognised OHLCV columns after flatten")
                return pd.DataFrame()
            df = df[cols]

            if market == "uk":
                df = self._apply_uk_adjustments(df)

            if use_cache:
                self._save_to_cache(cache_key, df)
            return df

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            return pd.DataFrame()

    def fetch_universe_data(
        self,
        tickers: List[str],
        start: str,
        end: str,
        market: str = "us",
        use_cache: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        results = {}
        batch_size = 50

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            batch_key = f"batch_{hashlib.md5(' '.join(batch).encode()).hexdigest()}_{start}_{end}_{market}"

            if use_cache:
                cached = self._load_from_cache(batch_key)
                if cached is not None:
                    results.update(cached)
                    continue

            try:
                raw = yf.download(
                    batch,
                    start=start,
                    end=end,
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                )

                batch_results = {}
                if len(batch) == 1:
                    df = self._normalize_columns(raw)
                    df = df[["open", "high", "low", "close", "volume"]].dropna(how="all")
                    if market == "uk":
                        df = self._apply_uk_adjustments(df)
                    if not df.empty:
                        batch_results[batch[0]] = df
                else:
                    for ticker in batch:
                        df = self._extract_ticker(raw, ticker)
                        if df is None or df.empty:
                            continue
                        if market == "uk":
                            df = self._apply_uk_adjustments(df)
                        batch_results[ticker] = df

                if use_cache:
                    self._save_to_cache(batch_key, batch_results)
                results.update(batch_results)

            except Exception as e:
                logger.error(f"Batch download failed: {e}. Falling back to individual.")
                for ticker in batch:
                    df = self.fetch_ohlcv(ticker, start, end, market, use_cache)
                    if not df.empty:
                        results[ticker] = df

        return results

    def fetch_ticker_info(self, ticker: str) -> dict:
        cached = self._load_from_cache(f"info_{ticker}", ttl_hours=168)
        if cached is not None:
            return cached
        try:
            info = yf.Ticker(ticker).info
            self._save_to_cache(f"info_{ticker}", info)
            return info
        except Exception as e:
            logger.error(f"Failed to fetch info for {ticker}: {e}")
            return {}

    def _extract_ticker(self, raw: pd.DataFrame, ticker: str) -> Optional[pd.DataFrame]:
        """Extract single ticker from a multi-ticker download result."""
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0).unique().tolist()
                lvl1 = raw.columns.get_level_values(1).unique().tolist()
                if ticker in lvl0:
                    df = raw[ticker].copy()
                elif ticker in lvl1:
                    df = raw.xs(ticker, axis=1, level=1).copy()
                else:
                    return None
            else:
                return None

            df = self._normalize_columns(df)
            cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            return df[cols].dropna(how="all") if cols else None
        except Exception:
            return None

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).lower().strip() for c in df.columns]
        df.index = pd.to_datetime(df.index)
        return df

    def _apply_uk_adjustments(self, df: pd.DataFrame) -> pd.DataFrame:
        divisor = self.config.get("markets", {}).get("uk", {}).get("price_divisor", 100)
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col] / divisor
        return df

    def _load_from_cache(self, key: str, ttl_hours: int = None) -> object:
        ttl = ttl_hours or self.cache_ttl_hours
        path = self.cache_dir / f"{key}.pkl"
        if not path.exists():
            return None
        age_hours = (datetime.now().timestamp() - path.stat().st_mtime) / 3600
        if age_hours > ttl:
            return None
        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None

    def _save_to_cache(self, key: str, data: object) -> None:
        path = self.cache_dir / f"{key}.pkl"
        try:
            with open(path, "wb") as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.warning(f"Cache write failed for {key}: {e}")
