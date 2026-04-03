"""FINRA short interest data collector with FCA UK short disclosure fallback."""

import io
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import requests
except ImportError:
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/finra")
DEEPDATA_CACHE = Path("data/cache/deepdata")

FINRA_SHORT_VOL_BASE = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"
FCA_SHORTS_URL = "https://www.fca.org.uk/markets/short-selling/notification-and-disclosure-net-short-positions"


def _make_result(
    source: str,
    ticker: str,
    market: str,
    data_type: str,
    value,
    raw_data=None,
    quality_score: float = 1.0,
) -> dict:
    return {
        "source": source,
        "ticker": ticker,
        "market": market,
        "data_type": data_type,
        "value": value,
        "raw_data": raw_data or {},
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
    }


class FINRACollector:
    """Collect short interest data from FINRA (US) and FCA (UK)."""

    def __init__(self, config: dict):
        self.config = config
        self.uk_confidence_weight = config.get("uk_confidence_weight", 0.6)
        self.cache_dir = Path(config.get("finra_cache_dir", str(CACHE_DIR)))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        DEEPDATA_CACHE.mkdir(parents=True, exist_ok=True)
        self.request_sleep = config.get("request_sleep", 1.5)
        self._session = None

    def _get_session(self):
        if requests is None:
            return None
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.finra.org/",
            })
        return self._session

    def collect(self, tickers: list, market: str = "us") -> list:
        """Fetch short interest data. Returns CollectorResult list."""
        results = []
        for ticker in tickers:
            is_uk = market == "uk" or ticker.endswith(".L")
            base_ticker = ticker.replace(".L", "") if is_uk else ticker
            confidence = self.uk_confidence_weight if is_uk else 1.0

            try:
                if is_uk:
                    short_data = self.fetch_fca_shorts(base_ticker)
                    for item in short_data.get("disclosed_shorts", []):
                        results.append(_make_result(
                            source="fca_shorts",
                            ticker=base_ticker,
                            market="uk",
                            data_type="fca_disclosed_short",
                            value=item.get("pct_capital", 0.0),
                            raw_data=item,
                            quality_score=confidence,
                        ))
                    if not short_data.get("disclosed_shorts"):
                        results.append(_make_result(
                            source="fca_shorts",
                            ticker=base_ticker,
                            market="uk",
                            data_type="short_float_pct",
                            value=0.0,
                            raw_data=short_data,
                            quality_score=confidence * 0.5,
                        ))
                else:
                    short_data = self.fetch_finra_short_data(ticker)
                    if short_data:
                        for dt in ["short_ratio", "short_float_pct", "days_to_cover"]:
                            val = short_data.get(dt)
                            if val is not None:
                                results.append(_make_result(
                                    source="finra",
                                    ticker=ticker,
                                    market="us",
                                    data_type=dt,
                                    value=val,
                                    raw_data=short_data,
                                    quality_score=confidence,
                                ))

            except Exception as exc:
                logger.warning("FINRACollector.collect(%s): %s", ticker, exc)

        return results

    def fetch_finra_short_data(self, ticker: str) -> dict:
        """
        Fetch FINRA daily short volume. Falls back to yfinance info if unavailable.
        Returns: {short_interest, float_pct, days_to_cover, change_from_prev, trend_3period}

        Fix 2026: cdn.finra.org returns 403 for weekend dates (no trading) and for
        the current/previous calendar day before FINRA publishes (~T+1).
        We iterate backwards over business days only, up to 10 calendar days back,
        which guarantees we reach the most recent published file.
        URL format unchanged: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt
        """
        # Try FINRA daily short volume file — business days only, up to 10 days back
        result = {}
        for days_back in range(1, 11):
            dt = datetime.utcnow() - timedelta(days=days_back)
            if dt.weekday() >= 5:          # Skip Saturday (5) and Sunday (6)
                continue
            date = dt.strftime("%Y%m%d")
            cache_path = self.cache_dir / f"CNMSshvol{date}.txt"

            df = None
            if cache_path.exists():
                try:
                    df = pd.read_csv(cache_path, sep="|", dtype=str)
                    logger.debug("Loaded FINRA file from cache: %s", cache_path)
                except Exception as exc:
                    logger.warning("Failed to read cached FINRA file %s: %s", cache_path, exc)

            if df is None:
                url = FINRA_SHORT_VOL_BASE.format(date=date)
                session = self._get_session()
                if session is None:
                    break
                try:
                    resp = session.get(url, timeout=15)
                    if resp.status_code == 200 and len(resp.content) > 100:
                        cache_path.write_bytes(resp.content)
                        df = pd.read_csv(io.StringIO(resp.text), sep="|", dtype=str)
                        logger.debug("Downloaded FINRA file for %s", date)
                    else:
                        logger.warning("FINRA file %s: HTTP %s", url, resp.status_code)
                    time.sleep(self.request_sleep)
                except Exception as exc:
                    logger.warning("FINRA download failed for %s: %s", date, exc)

            if df is not None and not df.empty:
                df.columns = [c.strip() for c in df.columns]
                if "Symbol" not in df.columns:
                    continue
                row = df[df["Symbol"].str.strip().str.upper() == ticker.upper()]
                if not row.empty:
                    row = row.iloc[0]
                    try:
                        short_vol = float(row.get("ShortVolume", 0) or 0)
                        total_vol = float(row.get("TotalVolume", 0) or 0)
                        short_ratio = short_vol / total_vol if total_vol > 0 else 0.0
                        result = {
                            "short_ratio": short_ratio,
                            "short_interest": short_vol,
                            "total_volume": total_vol,
                            "short_float_pct": None,
                            "days_to_cover": None,
                            "change_from_prev": None,
                            "trend_3period": "FLAT",
                            "source": "finra_daily_file",
                            "date": date,
                        }
                        return result
                    except Exception as exc:
                        logger.warning("FINRA parse error for %s: %s", ticker, exc)

        # Fallback to yfinance
        if yf is not None:
            try:
                info = yf.Ticker(ticker).info
                short_ratio = info.get("shortRatio", None)
                float_pct = info.get("shortPercentOfFloat", None)
                shares_short = info.get("sharesShort", None)
                avg_volume = info.get("averageVolume", None)
                dtc = None
                if shares_short and avg_volume and avg_volume > 0:
                    dtc = shares_short / avg_volume

                if float_pct or short_ratio:
                    result = {
                        "short_ratio": float(short_ratio) if short_ratio else None,
                        "short_interest": float(shares_short) if shares_short else None,
                        "short_float_pct": float(float_pct) if float_pct else None,
                        "days_to_cover": float(dtc) if dtc else None,
                        "change_from_prev": None,
                        "trend_3period": "FLAT",
                        "source": "yfinance_fallback",
                    }
                    return result
            except Exception as exc:
                logger.warning("yfinance short info fallback for %s: %s", ticker, exc)

        return {}

    def fetch_fca_shorts(self, ticker: str) -> dict:
        """
        Fetch FCA short position disclosures for UK tickers.
        Returns: {disclosed_shorts: list of {holder, pct_capital, date}}
        """
        result = {"disclosed_shorts": [], "source": "fca", "ticker": ticker}

        if requests is None or BeautifulSoup is None:
            logger.warning("requests or BeautifulSoup not installed; cannot fetch FCA shorts")
            return result

        session = self._get_session()
        try:
            resp = session.get(FCA_SHORTS_URL, timeout=20)
            time.sleep(self.request_sleep)
            if resp.status_code != 200:
                logger.warning("FCA shorts page returned HTTP %s", resp.status_code)
                return result

            soup = BeautifulSoup(resp.text, "lxml")
            tables = soup.find_all("table")
            disclosed = []
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if not cells:
                        continue
                    row_text = " ".join(cells).upper()
                    if ticker.upper() in row_text:
                        entry = {
                            "holder": cells[0] if len(cells) > 0 else "",
                            "issuer": cells[1] if len(cells) > 1 else "",
                            "pct_capital": _parse_float(cells[2]) if len(cells) > 2 else None,
                            "date": cells[3] if len(cells) > 3 else None,
                            "raw": cells,
                        }
                        disclosed.append(entry)

            result["disclosed_shorts"] = disclosed
        except Exception as exc:
            logger.warning("FCA shorts scrape failed for %s: %s", ticker, exc)

        return result

    def calc_trend(self, history: list) -> str:
        """3-period moving average direction: 'INCREASING', 'DECREASING', 'FLAT'"""
        if not history or len(history) < 2:
            return "FLAT"
        vals = [v for v in history if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if len(vals) < 2:
            return "FLAT"
        ma3 = np.convolve(vals, np.ones(min(3, len(vals))) / min(3, len(vals)), mode="valid")
        if len(ma3) < 2:
            return "FLAT"
        diff = ma3[-1] - ma3[-2]
        threshold = abs(ma3[-1]) * 0.02  # 2% change threshold
        if diff > threshold:
            return "INCREASING"
        if diff < -threshold:
            return "DECREASING"
        return "FLAT"


def _parse_float(s: str) -> float:
    """Parse a percentage string to float."""
    try:
        return float(s.replace("%", "").replace(",", "").strip())
    except Exception:
        return 0.0


# Alias for backwards-compatible imports
FinraCollector = FINRACollector
