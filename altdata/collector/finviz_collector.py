"""
Finviz collector — free tier (scraping, no API key required).

Scrapes https://finviz.com/quote.ashx?t=TICKER and collects:
  Technical data:   price, change%, volume, 52wk high/low, RSI, ATR, MAs,
                    short float, short ratio, beta
  Fundamental data: P/E, P/S, P/B, EPS estimates, growth rates, margins,
                    D/E, current ratio, analyst rec, price target
  News headlines:   last 20 with date, time, source, headline, URL
  Insider activity: last 20 transactions (name, title, date, type, shares, price, value)
  Analyst ratings:  last 10 rating changes (firm, date, change, ratings, target)

Rate limit: 1 request per 2 seconds.
Storage: raw_data table in altdata_store.db + dedicated finviz_data table (via migration).
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    logger.warning("finviz_collector: beautifulsoup4 not installed — collector disabled")

_FINVIZ_BASE = "https://finviz.com/quote.ashx?t={ticker}&p=d"
_REQUEST_DELAY = 2.0  # seconds between requests (be polite)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://finviz.com/",
    "DNT": "1",
    "Connection": "keep-alive",
}

# Mapping from Finviz label to our column name
_TECH_FIELDS = {
    "Price": "price",
    "Change": "change_pct",
    "Volume": "volume",
    "Avg Volume": "avg_volume",
    "Rel Volume": "rel_volume",
    "52W High": "week52_high",
    "52W Low": "week52_low",
    "RSI (14)": "rsi14",
    "ATR (14)": "atr14",
    "SMA20": "sma20",
    "SMA50": "sma50",
    "SMA200": "sma200",
    "20-Day Simple Moving Average": "sma20",
    "50-Day Simple Moving Average": "sma50",
    "200-Day Simple Moving Average": "sma200",
    "Short Float": "short_float_pct",
    "Short Ratio": "short_ratio",
    "Beta": "beta",
    "Volatility": "volatility",
    "Perf Week": "perf_week",
    "Perf Month": "perf_month",
    "Perf Quart": "perf_quarter",
    "Perf Half Y": "perf_half_year",
    "Perf Year": "perf_year",
    "Perf YTD": "perf_ytd",
    "52W Range": "week52_range",
}

_FUND_FIELDS = {
    "P/E": "pe_ratio",
    "Forward P/E": "pe_forward",
    "PEG": "peg_ratio",
    "P/S": "ps_ratio",
    "P/B": "pb_ratio",
    "P/C": "pc_ratio",
    "P/FCF": "pfcf_ratio",
    "EPS (ttm)": "eps_ttm",
    "EPS next Y": "eps_next_year",
    "EPS next Q": "eps_next_quarter",
    "EPS this Y": "eps_growth_this_year",
    "EPS next 5Y": "eps_growth_next_5y",
    "EPS past 5Y": "eps_growth_past_5y",
    "Sales past 5Y": "sales_growth_past_5y",
    "Sales Q/Q": "sales_growth_qoq",
    "EPS Q/Q": "eps_growth_qoq",
    "ROA": "roa",
    "ROE": "roe",
    "ROI": "roi",
    "Gross Margin": "gross_margin",
    "Oper. Margin": "oper_margin",
    "Profit Margin": "profit_margin",
    "Debt/Eq": "debt_to_equity",
    "LT Debt/Eq": "lt_debt_to_equity",
    "Current Ratio": "current_ratio",
    "Quick Ratio": "quick_ratio",
    "Recom": "analyst_rec",
    "Target Price": "analyst_target",
    "Avg Volume": "avg_volume",
    "Market Cap": "market_cap",
    "Income": "net_income",
    "Sales": "revenue",
    "Book/sh": "book_per_share",
    "Cash/sh": "cash_per_share",
    "Dividend": "dividend",
    "Dividend %": "dividend_yield",
    "Employees": "employees",
    "Optionable": "optionable",
    "Shortable": "shortable",
    "Index": "index_membership",
    "Earnings": "earnings_date",
    "No. Analysts": "analyst_count",
}


def _parse_number(s: str) -> Optional[float]:
    """Convert Finviz string like '12.5%', '1.23B', '-' to float."""
    if not s or s.strip() in ("-", "N/A", "n/a", "", "nan"):
        return None
    s = s.strip()
    multiplier = 1.0
    if s.endswith("%"):
        s = s[:-1]
        multiplier = 0.01
    elif s.endswith("B"):
        s = s[:-1]
        multiplier = 1e9
    elif s.endswith("M"):
        s = s[:-1]
        multiplier = 1e6
    elif s.endswith("K"):
        s = s[:-1]
        multiplier = 1e3
    try:
        return float(s.replace(",", "")) * multiplier
    except (ValueError, TypeError):
        return None


class FinvizCollector:
    """
    Scrapes Finviz for technical, fundamental, news, insider, and analyst data.
    One instance per run; uses a requests.Session for connection pooling.
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_request = 0.0

    def _get(self, url: str) -> Optional[str]:
        """Rate-limited GET with error handling."""
        elapsed = time.monotonic() - self._last_request
        if elapsed < _REQUEST_DELAY:
            time.sleep(_REQUEST_DELAY - elapsed)
        try:
            resp = self._session.get(url, timeout=15)
            self._last_request = time.monotonic()
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 404:
                logger.debug("finviz: 404 for %s", url)
            else:
                logger.warning("finviz: HTTP %s for %s", resp.status_code, url)
        except Exception as e:
            logger.warning("finviz: request error %s: %s", url, e)
        return None

    def _parse_quote_page(self, ticker: str, html: str) -> Dict[str, Any]:
        """Parse a Finviz quote page HTML into structured data."""
        if not HAS_BS4:
            return {}

        soup = BeautifulSoup(html, "lxml")
        result: Dict[str, Any] = {
            "ticker": ticker,
            "technical": {},
            "fundamental": {},
            "news": [],
            "insiders": [],
            "analyst_ratings": [],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # ── Snapshot table (key-value pairs) ──────────────────────────
        # Finviz uses a table with alternating label/value cells
        snapshot_table = soup.find("table", class_="snapshot-table2") or \
                         soup.find("table", attrs={"class": lambda c: c and "snapshot" in c})

        if snapshot_table is None:
            # Try newer layout
            cells = soup.select("td.snapshot-td2-cp, td.snapshot-td2")
            pairs = [(cells[i].get_text(strip=True), cells[i+1].get_text(strip=True))
                     for i in range(0, len(cells)-1, 2)]
        else:
            cells = snapshot_table.find_all("td")
            pairs = [(cells[i].get_text(strip=True), cells[i+1].get_text(strip=True))
                     for i in range(0, len(cells)-1, 2)]

        for label, value in pairs:
            label = label.strip()
            value = value.strip()
            if label in _TECH_FIELDS:
                col = _TECH_FIELDS[label]
                parsed = _parse_number(value)
                result["technical"][col] = parsed if parsed is not None else value
            if label in _FUND_FIELDS:
                col = _FUND_FIELDS[label]
                parsed = _parse_number(value)
                result["fundamental"][col] = parsed if parsed is not None else value

        # Compute derived technical fields
        tech = result["technical"]
        price = tech.get("price")
        if isinstance(price, str):
            price = _parse_number(price)
            tech["price"] = price

        w52_high = tech.get("week52_high")
        w52_low  = tech.get("week52_low")
        if price and w52_high and isinstance(w52_high, float) and w52_high > 0:
            tech["dist_from_52w_high_pct"] = (price / w52_high) - 1
        if price and w52_low and isinstance(w52_low, float) and w52_low > 0:
            tech["dist_from_52w_low_pct"] = (price / w52_low) - 1

        sma20  = tech.get("sma20")
        sma50  = tech.get("sma50")
        sma200 = tech.get("sma200")
        if price and isinstance(price, float):
            tech["above_sma20"]  = 1 if (isinstance(sma20,  float) and price > sma20)  else 0
            tech["above_sma50"]  = 1 if (isinstance(sma50,  float) and price > sma50)  else 0
            tech["above_sma200"] = 1 if (isinstance(sma200, float) and price > sma200) else 0

        # Analyst target distance
        target = result["fundamental"].get("analyst_target")
        if price and target and isinstance(target, float) and isinstance(price, float) and price > 0:
            result["fundamental"]["target_upside_pct"] = (target / price) - 1

        # ── News headlines ─────────────────────────────────────────────
        news_table = soup.find("table", id="news-table")
        if news_table:
            current_date = ""
            for row in news_table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    date_cell = cells[0].get_text(strip=True)
                    link_cell = cells[1]
                    a_tag = link_cell.find("a")
                    if a_tag:
                        headline = a_tag.get_text(strip=True)
                        url = a_tag.get("href", "")
                        source_span = link_cell.find("span", class_="news-link-right")
                        source = source_span.get_text(strip=True) if source_span else ""
                        # Date format: "Jan-01-25 09:00AM" or "09:00AM" (continuation)
                        parts = date_cell.split()
                        if len(parts) >= 2:
                            current_date = parts[0]
                            time_str = parts[1]
                        else:
                            time_str = date_cell
                        result["news"].append({
                            "date": current_date,
                            "time": time_str,
                            "source": source,
                            "headline": headline,
                            "url": url,
                        })
                        if len(result["news"]) >= 20:
                            break

        # ── Insider transactions ───────────────────────────────────────
        insider_table = soup.find("table", class_="body-table")
        if insider_table is None:
            # Try finding by header
            for tbl in soup.find_all("table"):
                header = tbl.find("th")
                if header and "Insider" in (header.get_text() or ""):
                    insider_table = tbl
                    break

        if insider_table:
            rows = insider_table.find_all("tr")[1:]  # skip header
            for row in rows[:20]:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) >= 8:
                    result["insiders"].append({
                        "insider_name":  cols[0],
                        "relationship":  cols[1],
                        "date":          cols[2],
                        "transaction":   cols[3],
                        "cost":          _parse_number(cols[4]),
                        "shares":        _parse_number(cols[5]),
                        "value":         _parse_number(cols[6]),
                        "shares_total":  _parse_number(cols[7]),
                        "is_option":     "option" in cols[3].lower() or "exercise" in cols[3].lower(),
                        "is_open_market_buy": cols[3].strip() == "Buy",
                        "is_open_market_sell": cols[3].strip() == "Sale",
                    })

        # ── Analyst ratings ────────────────────────────────────────────
        # Ratings appear in a ratings table below the snapshot
        ratings_tables = soup.find_all("table")
        for tbl in ratings_tables:
            ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
            if "Date" in ths and any(w in ths for w in ("Action", "Rating", "Analyst")):
                rows = tbl.find_all("tr")[1:]
                for row in rows[:10]:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cols) >= 4:
                        result["analyst_ratings"].append({
                            "date":           cols[0],
                            "action":         cols[1] if len(cols) > 1 else "",
                            "firm":           cols[2] if len(cols) > 2 else "",
                            "from_rating":    cols[3] if len(cols) > 3 else "",
                            "to_rating":      cols[4] if len(cols) > 4 else "",
                            "target_from":    _parse_number(cols[5]) if len(cols) > 5 else None,
                            "target_to":      _parse_number(cols[6]) if len(cols) > 6 else None,
                        })
                break

        return result

    def collect_ticker(self, ticker: str) -> Optional[Dict]:
        """Fetch and parse data for a single ticker. Returns structured dict."""
        if not HAS_BS4:
            return None
        url = _FINVIZ_BASE.format(ticker=ticker.upper())
        html = self._get(url)
        if not html:
            return None
        try:
            return self._parse_quote_page(ticker, html)
        except Exception as e:
            logger.warning("finviz: parse error for %s: %s", ticker, e)
            return None

    def collect(self, tickers: List[str], market: str = "us") -> List[Dict]:
        """
        Main entry point. Collects data for all tickers.
        Returns list of signal dicts compatible with AltDataStore.store_raw().
        """
        if not HAS_BS4:
            logger.warning("finviz_collector: bs4 not available, returning []")
            return []

        results = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for ticker in tickers:
            data = self.collect_ticker(ticker)
            if not data:
                continue

            # Technical signal score: above all 3 MAs + RSI healthy + low short float
            tech = data.get("technical", {})
            fund = data.get("fundamental", {})
            rsi  = tech.get("rsi14")
            sf   = tech.get("short_float_pct")
            a20  = tech.get("above_sma20", 0)
            a50  = tech.get("above_sma50", 0)
            a200 = tech.get("above_sma200", 0)

            tech_score = 0.0
            if isinstance(rsi, float):
                # RSI 40-60 is neutral; >60 slightly bullish; <40 slightly bearish
                tech_score += (rsi - 50) / 50.0 * 0.3
            tech_score += (a20 + a50 + a200) / 3.0 * 0.4
            if isinstance(sf, float):
                tech_score -= sf * 2.0 * 0.3  # high short float = bearish pressure

            tech_score = max(-1.0, min(1.0, tech_score))

            # Analyst score: based on recommendation (1=Strong Buy ... 5=Strong Sell)
            rec = fund.get("analyst_rec")
            analyst_score = 0.0
            if isinstance(rec, float) and rec > 0:
                analyst_score = (3.0 - rec) / 2.0  # 1=1.0, 3=0.0, 5=-1.0

            composite_score = tech_score * 0.6 + analyst_score * 0.4

            results.append({
                "source":       "finviz",
                "ticker":       ticker,
                "market":       market,
                "data_type":    "finviz_snapshot",
                "value":        composite_score,
                "raw_data":     json.dumps({
                    "technical":        data.get("technical", {}),
                    "fundamental":      data.get("fundamental", {}),
                    "news":             data.get("news", []),
                    "insiders":         data.get("insiders", []),
                    "analyst_ratings":  data.get("analyst_ratings", []),
                    "tech_score":       tech_score,
                    "analyst_score":    analyst_score,
                }, default=str),
                "quality":      0.8,
                "collected_at": now_iso,
            })

            logger.debug("finviz: %s tech_score=%.3f analyst_score=%.3f",
                         ticker, tech_score, analyst_score)

        logger.info("finviz_collector: %d/%d tickers collected", len(results), len(tickers))
        return results

    def get_news_urls(self, ticker: str) -> List[str]:
        """Return list of news URLs for a ticker (for article_reader)."""
        data = self.collect_ticker(ticker)
        if not data:
            return []
        return [n["url"] for n in data.get("news", []) if n.get("url")]

    def get_insider_transactions(self, ticker: str) -> List[Dict]:
        """Return parsed insider transactions for a ticker."""
        data = self.collect_ticker(ticker)
        if not data:
            return []
        return data.get("insiders", [])
