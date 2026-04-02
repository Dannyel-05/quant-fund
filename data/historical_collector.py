"""
Comprehensive historical data collector.

Phases 1-3: Price history, financials, EDGAR filings, macro context.
Phase 5: Enrich earnings_observations with derived features.
Phase 6: Historical news from EDGAR press releases + Google News RSS.

Data sources (all free, no paid API required):
  - yfinance: price, financials, balance sheet, cash flow, institutional holders
  - SEC EDGAR: filings index, XBRL facts, insider transactions (Form 4)
  - Google News RSS: headline feeds
  - Open-Meteo archive: already used in weather_collector
"""
import logging
import time
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np
import yfinance as yf

from data.historical_db import HistoricalDB

logger = logging.getLogger(__name__)

# ── EDGAR constants ───────────────────────────────────────────────────────────
EDGAR_BASE    = "https://data.sec.gov"
EDGAR_SEARCH  = "https://efts.sec.gov/LATEST/search-index"
EDGAR_HEADERS = {
    "User-Agent": "quant-fund-research research@quantfund.local",
    "Accept-Encoding": "gzip, deflate",
}
EDGAR_RATE    = 0.12   # seconds between requests (SEC allows ~10/s)

# ── Google News RSS ───────────────────────────────────────────────────────────
GNEWS_RSS = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

# ── Macro symbols for yfinance ────────────────────────────────────────────────
MACRO_SYMBOLS = {
    "spx_close":  "^GSPC",
    "ndx_close":  "^NDX",
    "rut_close":  "^RUT",
    "ftse_close": "^FTSE",
    "vix":        "^VIX",
    "tnx":        "^TNX",
    "tyx":        "^TYX",
    "irx":        "^IRX",
    "dxy":        "DX-Y.NYB",
    "gbpusd":     "GBPUSD=X",
    "eurusd":     "EURUSD=X",
    "cl1_oil":    "CL=F",
    "gc1_gold":   "GC=F",
    "hg1_copper": "HG=F",
    "xlk":        "XLK",  "xlv": "XLV",  "xlf": "XLF",  "xly": "XLY",
    "xlp":        "XLP",  "xle": "XLE",  "xlu": "XLU",  "xlb": "XLB",
    "xli":        "XLI",  "xlre":"XLRE", "xlc": "XLC",
}


class HistoricalCollector:
    """Collect and store full historical data into HistoricalDB."""

    def __init__(self, config: dict, db_path: str = "output/historical_db.db"):
        self.config = config
        self.db = HistoricalDB(db_path)
        self._cik_cache: Dict[str, str] = {}  # ticker → CIK
        self._session = requests.Session()
        self._session.headers.update(EDGAR_HEADERS)

    # ────────────────────────────────────────────────────────────────────────
    # Phase 1: Price history + financials
    # ────────────────────────────────────────────────────────────────────────

    def collect_price_history(
        self,
        tickers: List[str],
        start: str = "2010-01-01",
        delisted: bool = False,
    ) -> Dict[str, int]:
        """Download daily OHLCV for each ticker via yfinance. Returns {ticker: rows}."""
        results = {}
        # Batch download (much faster than per-ticker)
        batch_size = 20
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            try:
                raw = yf.download(
                    batch,
                    start=start,
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                if raw.empty:
                    continue

                # yfinance returns MultiIndex when multiple tickers
                if isinstance(raw.columns, pd.MultiIndex):
                    for ticker in batch:
                        try:
                            df = raw.xs(ticker, axis=1, level=1).dropna(how="all")
                            if df.empty:
                                continue
                            df.columns = [c.lower() for c in df.columns]
                            df.index = pd.to_datetime(df.index)
                            records = [
                                {
                                    "ticker":    ticker,
                                    "date":      d.strftime("%Y-%m-%d"),
                                    "open":      _safe_float(row.get("open")),
                                    "high":      _safe_float(row.get("high")),
                                    "low":       _safe_float(row.get("low")),
                                    "close":     _safe_float(row.get("close")),
                                    "adj_close": _safe_float(row.get("close")),
                                    "volume":    _safe_float(row.get("volume")),
                                    "delisted":  1 if delisted else 0,
                                }
                                for d, row in df.iterrows()
                            ]
                            n = self.db.upsert_prices(records)
                            results[ticker] = n
                        except Exception as e:
                            logger.debug("price %s: %s", ticker, e)
                else:
                    # Single ticker
                    ticker = batch[0]
                    df = raw.dropna(how="all")
                    df.columns = [c.lower() for c in df.columns]
                    records = [
                        {
                            "ticker":    ticker,
                            "date":      d.strftime("%Y-%m-%d"),
                            "open":      _safe_float(row.get("open")),
                            "high":      _safe_float(row.get("high")),
                            "low":       _safe_float(row.get("low")),
                            "close":     _safe_float(row.get("close")),
                            "adj_close": _safe_float(row.get("close")),
                            "volume":    _safe_float(row.get("volume")),
                            "delisted":  1 if delisted else 0,
                        }
                        for d, row in df.iterrows()
                    ]
                    n = self.db.upsert_prices(records)
                    results[ticker] = n
            except Exception as e:
                logger.warning("batch price download %s: %s", batch, e)

            time.sleep(0.5)

        return results

    def collect_financials(
        self, tickers: List[str], start: str = "2010-01-01"
    ) -> Dict[str, int]:
        """Collect quarterly and annual financials, balance sheet, cash flow."""
        results = {}
        for ticker in tickers:
            try:
                n = self._collect_ticker_financials(ticker)
                results[ticker] = n
            except Exception as e:
                logger.warning("financials %s: %s", ticker, e)
            time.sleep(0.3)
        return results

    def _collect_ticker_financials(self, ticker: str) -> int:
        t = yf.Ticker(ticker)
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        total = 0

        # ── Income statement (quarterly + annual) ────────────────────────
        for period_type, attr in [("quarterly", "quarterly_income_stmt"),
                                    ("annual",    "income_stmt")]:
            try:
                df = getattr(t, attr)
                if df is None or df.empty:
                    continue
                df.columns = pd.to_datetime(df.columns)
                records = []
                prev_rev = None
                for col in sorted(df.columns):
                    def _g(key):
                        for k in df.index:
                            if key.lower() in str(k).lower():
                                v = df.loc[k, col]
                                return _safe_float(v)
                        return None

                    rev = _g("total revenue") or _g("revenue")
                    gp  = _g("gross profit")
                    oi  = _g("operating income") or _g("ebit")
                    ni  = _g("net income")
                    ebt = _g("ebitda")
                    eps = _g("diluted eps") or _g("basic eps")
                    eps_basic = _g("basic eps")
                    shares = _g("diluted average shares") or _g("basic average shares")

                    gm = (gp / rev) if (gp and rev and rev != 0) else None
                    om = (oi / rev) if (oi and rev and rev != 0) else None
                    nm = (ni / rev) if (ni and rev and rev != 0) else None
                    rev_growth = ((rev - prev_rev) / abs(prev_rev)) if (rev and prev_rev and prev_rev != 0) else None

                    records.append({
                        "ticker":            ticker,
                        "period":            col.strftime("%Y-%m-%d"),
                        "period_type":       period_type,
                        "revenue":           rev,
                        "gross_profit":      gp,
                        "operating_income":  oi,
                        "net_income":        ni,
                        "ebitda":            ebt,
                        "eps_basic":         eps_basic,
                        "eps_diluted":       eps,
                        "shares_outstanding": shares,
                        "gross_margin":      gm,
                        "operating_margin":  om,
                        "net_margin":        nm,
                        "revenue_growth_yoy": rev_growth,
                        "source":            "yfinance",
                    })
                    prev_rev = rev
                total += self.db.upsert_financials(records)
            except Exception as e:
                logger.debug("income %s %s: %s", ticker, period_type, e)

        # ── Balance sheet ─────────────────────────────────────────────────
        for period_type, attr in [("quarterly", "quarterly_balance_sheet"),
                                    ("annual",    "balance_sheet")]:
            try:
                df = getattr(t, attr)
                if df is None or df.empty:
                    continue
                df.columns = pd.to_datetime(df.columns)
                records = []
                for col in df.columns:
                    def _g(key):
                        for k in df.index:
                            if key.lower() in str(k).lower():
                                return _safe_float(df.loc[k, col])
                        return None

                    ta  = _g("total assets")
                    tl  = _g("total liabilities")
                    eq  = _g("stockholders equity") or _g("total equity")
                    ca  = _g("current assets") or _g("total current assets")
                    cl  = _g("current liabilities") or _g("total current liabilities")
                    cash = _g("cash and cash equivalents") or _g("cash")
                    debt = _g("total debt") or _g("long term debt")
                    nd   = (debt - cash) if (debt and cash) else None
                    cr   = (ca / cl) if (ca and cl and cl != 0) else None
                    de   = (debt / eq) if (debt and eq and eq != 0) else None
                    shares = _g("shares issued") or _g("ordinary shares number")
                    bvps  = (eq / shares) if (eq and shares and shares != 0) else None

                    records.append({
                        "ticker":            ticker,
                        "period":            col.strftime("%Y-%m-%d"),
                        "period_type":       period_type,
                        "total_assets":      ta,
                        "total_liabilities": tl,
                        "total_equity":      eq,
                        "cash_and_equiv":    cash,
                        "total_debt":        debt,
                        "net_debt":          nd,
                        "current_assets":    ca,
                        "current_liabilities": cl,
                        "current_ratio":     cr,
                        "debt_to_equity":    de,
                        "book_value_per_share": bvps,
                        "source":            "yfinance",
                    })
                total += self.db.upsert_balance_sheet(records)
            except Exception as e:
                logger.debug("balance %s %s: %s", ticker, period_type, e)

        # ── Cash flow ─────────────────────────────────────────────────────
        for period_type, attr in [("quarterly", "quarterly_cashflow"),
                                    ("annual",    "cashflow")]:
            try:
                df = getattr(t, attr)
                if df is None or df.empty:
                    continue
                df.columns = pd.to_datetime(df.columns)
                records = []
                for col in df.columns:
                    def _g(key):
                        for k in df.index:
                            if key.lower() in str(k).lower():
                                return _safe_float(df.loc[k, col])
                        return None

                    ocf  = _g("operating cash flow") or _g("cash from operations")
                    icf  = _g("investing cash flow") or _g("investing activities")
                    fcf_stmt = _g("financing cash flow") or _g("financing activities")
                    capex = _g("capital expenditure") or _g("purchase of property")
                    fcf   = (ocf + capex) if (ocf and capex) else (ocf if ocf else None)
                    divs  = _g("common stock dividend")
                    buyb  = _g("repurchase of capital stock") or _g("common stock repurchase")

                    records.append({
                        "ticker":          ticker,
                        "period":          col.strftime("%Y-%m-%d"),
                        "period_type":     period_type,
                        "operating_cf":    ocf,
                        "investing_cf":    icf,
                        "financing_cf":    fcf_stmt,
                        "capex":           capex,
                        "free_cash_flow":  fcf,
                        "dividends_paid":  divs,
                        "buybacks":        buyb,
                        "source":          "yfinance",
                    })
                total += self.db.upsert_cash_flow(records)
            except Exception as e:
                logger.debug("cashflow %s %s: %s", ticker, period_type, e)

        return total

    # ────────────────────────────────────────────────────────────────────────
    # Phase 2: SEC EDGAR filings
    # ────────────────────────────────────────────────────────────────────────

    def collect_edgar(
        self,
        tickers: List[str],
        start: str = "2010-01-01",
        forms: List[str] = None,
    ) -> Dict[str, int]:
        """Collect EDGAR filing metadata for all tickers."""
        if forms is None:
            forms = ["8-K", "10-K", "10-Q", "4", "DEF 14A", "SC 13G", "SC 13G/A"]
        results = {}
        for ticker in tickers:
            try:
                n = self._collect_edgar_ticker(ticker, start, forms)
                results[ticker] = n
                logger.debug("EDGAR %s: %d records", ticker, n)
            except Exception as e:
                logger.warning("EDGAR %s: %s", ticker, e)
            time.sleep(EDGAR_RATE * 3)
        return results

    def _get_cik(self, ticker: str) -> Optional[str]:
        if ticker in self._cik_cache:
            return self._cik_cache[ticker]
        try:
            url = f"{EDGAR_BASE}/cgi-bin/browse-edgar?company=&CIK={ticker}&type=&dateb=&owner=include&count=1&search_text=&action=getcompany&output=atom"
            resp = self._session.get(url, timeout=15)
            if resp.status_code == 200:
                # Parse atom feed for CIK
                m = re.search(r'CIK=(\d{10})', resp.text)
                if m:
                    cik = m.group(1).lstrip("0")
                    self._cik_cache[ticker] = cik
                    return cik
        except Exception:
            pass

        # Try company_tickers.json
        try:
            resp = self._session.get(
                f"{EDGAR_BASE}/files/company_tickers.json", timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                for entry in data.values():
                    if entry.get("ticker", "").upper() == ticker.upper():
                        cik = str(entry["cik_str"])
                        self._cik_cache[ticker] = cik
                        return cik
        except Exception:
            pass
        return None

    def _collect_edgar_ticker(
        self, ticker: str, start: str, forms: List[str]
    ) -> int:
        cik = self._get_cik(ticker)
        if not cik:
            logger.debug("EDGAR: no CIK for %s", ticker)
            return 0

        cik_padded = cik.zfill(10)
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Fetch submissions
        try:
            url = f"{EDGAR_BASE}/submissions/CIK{cik_padded}.json"
            resp = self._session.get(url, timeout=30)
            if resp.status_code != 200:
                return 0
            sub = resp.json()
        except Exception as e:
            logger.debug("EDGAR submissions %s: %s", ticker, e)
            return 0

        time.sleep(EDGAR_RATE)

        filings_raw = sub.get("filings", {}).get("recent", {})
        f_dates   = filings_raw.get("filingDate", [])
        f_forms   = filings_raw.get("form", [])
        f_accnos  = filings_raw.get("accessionNumber", [])
        f_periods = filings_raw.get("reportDate", [])
        f_descs   = filings_raw.get("primaryDocument", [])

        edgar_records = []
        insider_records = []

        for i, (fd, ftype, acn, period, pdoc) in enumerate(
            zip(f_dates, f_forms, f_accnos, f_periods, f_descs)
        ):
            if fd < start[:10]:
                continue
            if ftype not in forms:
                continue

            acn_clean = acn.replace("-", "")
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{acn_clean}/{pdoc}"
            )

            edgar_records.append({
                "ticker":          ticker,
                "cik":             cik,
                "form_type":       ftype,
                "filed_date":      fd,
                "period_of_report": period or "",
                "accession_number": acn,
                "url":             filing_url,
                "description":     pdoc or "",
                "collected_at":    now,
            })

            # For Form 4 (insider transactions), extract transaction data
            if ftype == "4" and len(edgar_records) <= 500:
                txns = self._parse_form4(ticker, cik, acn_clean, pdoc, fd, now)
                insider_records.extend(txns)
                if txns:
                    time.sleep(EDGAR_RATE)

        n_filings  = self.db.upsert_filings(edgar_records)
        n_insiders = self.db.upsert_insider_txns(insider_records)
        logger.debug("EDGAR %s: %d filings, %d insider txns", ticker, n_filings, n_insiders)

        # Institutional ownership from yfinance (simpler than 13F parsing)
        self._collect_institutional(ticker, cik, now)

        return n_filings + n_insiders

    def _parse_form4(
        self, ticker: str, cik: str, acn_clean: str, pdoc: str, filed_date: str, now: str
    ) -> List[dict]:
        """Parse Form 4 XML to extract insider transactions."""
        records = []
        try:
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acn_clean}/{pdoc}"
            resp = self._session.get(url, timeout=15)
            if resp.status_code != 200 or not pdoc.endswith(".xml"):
                # Try to find the XML in the filing index
                idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acn_clean}/0001"
                return records
            time.sleep(EDGAR_RATE)

            root = ET.fromstring(resp.content)
            ns = {"": ""}

            reporter_name  = _xml_text(root, ".//reportingOwner/reportingOwnerId/rptOwnerName") or ""
            reporter_title = _xml_text(root, ".//reportingOwnerRelationship/officerTitle") or ""

            for txn in root.findall(".//nonDerivativeTransaction"):
                txn_date = _xml_text(txn, "transactionDate/value") or filed_date
                code = (_xml_text(txn, "transactionCoding/transactionCode") or "").upper()
                shares = _safe_float(_xml_text(txn, "transactionAmounts/transactionShares/value"))
                price  = _safe_float(_xml_text(txn, "transactionAmounts/transactionPricePerShare/value"))
                owned  = _safe_float(_xml_text(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
                acq_disp = _xml_text(txn, "transactionAmounts/transactionAcquiredDisposedCode/value") or ""

                txn_type = "buy" if acq_disp.upper() == "A" else "sell" if acq_disp.upper() == "D" else code.lower()
                total_val = (shares * price) if (shares and price) else None

                records.append({
                    "ticker":           ticker,
                    "cik":              cik,
                    "reporter_name":    reporter_name,
                    "reporter_title":   reporter_title,
                    "transaction_date": txn_date[:10] if txn_date else filed_date,
                    "transaction_type": txn_type,
                    "shares":           shares,
                    "price_per_share":  price,
                    "total_value":      total_val,
                    "shares_owned_after": owned,
                    "form_type":        "4",
                    "accession_number": acn_clean,
                    "collected_at":     now,
                })
        except Exception as e:
            logger.debug("Form4 parse %s %s: %s", ticker, acn_clean, e)
        return records

    def _collect_institutional(self, ticker: str, cik: str, now: str) -> None:
        """Collect institutional ownership from yfinance (easier than 13F parsing)."""
        try:
            t = yf.Ticker(ticker)
            holders = t.institutional_holders
            if holders is None or holders.empty:
                return

            total_shares = _safe_float(holders["Shares"].sum()) if "Shares" in holders.columns else None
            count = len(holders)
            top_holder = str(holders.iloc[0]["Holder"]) if len(holders) > 0 and "Holder" in holders.columns else None
            top_pct = _safe_float(holders.iloc[0].get("% Out")) if len(holders) > 0 else None

            # Get total shares outstanding for ownership pct
            info = t.fast_info
            float_shares = getattr(info, "shares", None)
            own_pct = (total_shares / float_shares) if (total_shares and float_shares and float_shares > 0) else None

            quarter_end = date.today().replace(day=1) - timedelta(days=1)
            quarter_str = quarter_end.strftime("%Y-%m-%d")

            self.db.upsert_institutional([{
                "ticker":             ticker,
                "period":             quarter_str,
                "total_shares_held":  total_shares,
                "institutions_count": count,
                "ownership_pct":      own_pct,
                "top_holder":         top_holder,
                "top_holder_pct":     top_pct,
                "qoq_change_pct":     None,
                "collected_at":       now,
            }])
        except Exception as e:
            logger.debug("institutional %s: %s", ticker, e)

    # ────────────────────────────────────────────────────────────────────────
    # Phase 3: Macro context
    # ────────────────────────────────────────────────────────────────────────

    def collect_macro(self, start: str = "2010-01-01") -> int:
        """Download all macro series and store daily records."""
        logger.info("Collecting macro context since %s", start)

        symbols = list(MACRO_SYMBOLS.values())
        col_map  = {v: k for k, v in MACRO_SYMBOLS.items()}

        try:
            raw = yf.download(symbols, start=start, auto_adjust=True, progress=False, threads=True)
            if raw.empty:
                logger.warning("macro: yfinance returned empty")
                return 0

            if isinstance(raw.columns, pd.MultiIndex):
                close = raw.xs("Close", axis=1, level=0)
            else:
                close = raw[["Close"]] if "Close" in raw.columns else raw

            close.index = pd.to_datetime(close.index)
            records = []
            now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            for dt, row in close.iterrows():
                rec: dict = {"date": dt.strftime("%Y-%m-%d"), "collected_at": now}
                for sym, field in col_map.items():
                    v = row.get(sym)
                    rec[field] = _safe_float(v)
                # Derived: yield spread
                tnx = rec.get("tnx")
                irx = rec.get("irx")
                if tnx and irx:
                    rec["t10y2y"] = round(tnx - irx, 4)
                records.append(rec)

            n = self.db.upsert_macro(records)
            logger.info("Macro: %d daily records stored", n)
            return n
        except Exception as e:
            logger.warning("macro collection failed: %s", e)
            return 0

    # ────────────────────────────────────────────────────────────────────────
    # Phase 5: Enrich earnings_observations
    # ────────────────────────────────────────────────────────────────────────

    def enrich_earnings_observations(self, tickers: List[str] = None) -> int:
        """
        For each row in earnings_observations, compute derived features
        from the historical DB and store in earnings_enriched.
        """
        from data.earnings_db import EarningsDB
        edb = EarningsDB()

        if tickers:
            obs_all = []
            for t in tickers:
                obs_all.extend(edb.get_observations(ticker=t, limit=500))
        else:
            obs_all = edb.get_observations(limit=10000)

        if not obs_all:
            logger.info("enrich: no observations to enrich")
            return 0

        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        enriched = []

        for obs in obs_all:
            ticker = obs["ticker"]
            ed     = obs["earnings_date"]
            try:
                rec = self._build_enriched_record(ticker, ed, obs, now)
                if rec:
                    enriched.append(rec)
            except Exception as e:
                logger.debug("enrich %s @ %s: %s", ticker, ed, e)

        n = self.db.upsert_enriched(enriched)
        logger.info("Enriched %d earnings observations", n)
        return n

    def _build_enriched_record(
        self, ticker: str, earnings_date: str, obs: dict, now: str
    ) -> Optional[dict]:
        ed = earnings_date

        # ── Revenue trend ─────────────────────────────────────────────────
        fin_rows = [
            r for r in self._get_recent_financials(ticker, ed, periods=5)
            if r.get("revenue") is not None
        ]
        rev_growth_1q = rev_growth_4q = rev_accel = None
        gross_margin_trend = op_margin_trend = None
        if len(fin_rows) >= 2:
            rev_curr  = fin_rows[0].get("revenue")
            rev_prev1 = fin_rows[1].get("revenue")
            if rev_curr and rev_prev1 and rev_prev1 != 0:
                rev_growth_1q = (rev_curr - rev_prev1) / abs(rev_prev1)
        if len(fin_rows) >= 5:
            rev_prev4 = fin_rows[4].get("revenue")
            if fin_rows[0].get("revenue") and rev_prev4 and rev_prev4 != 0:
                rev_growth_4q = (fin_rows[0]["revenue"] - rev_prev4) / abs(rev_prev4)
            if rev_growth_1q is not None and len(fin_rows) >= 3 and fin_rows[1].get("revenue") and fin_rows[2].get("revenue"):
                prev_growth = (fin_rows[1]["revenue"] - fin_rows[2]["revenue"]) / abs(fin_rows[2]["revenue"])
                rev_accel = rev_growth_1q - prev_growth
        if len(fin_rows) >= 3:
            gm_vals = [r.get("gross_margin") for r in fin_rows[:3] if r.get("gross_margin") is not None]
            om_vals = [r.get("operating_margin") for r in fin_rows[:3] if r.get("operating_margin") is not None]
            if len(gm_vals) >= 2:
                gross_margin_trend = gm_vals[0] - gm_vals[-1]
            if len(om_vals) >= 2:
                op_margin_trend = om_vals[0] - om_vals[-1]

        # ── CF quality ────────────────────────────────────────────────────
        cf_rows = self._get_recent_cf(ticker, ed, periods=2)
        cf_ratio = None
        if cf_rows and fin_rows:
            ocf = cf_rows[0].get("operating_cf")
            ni  = fin_rows[0].get("net_income")
            if ocf and ni and ni != 0:
                cf_ratio = ocf / abs(ni)

        # ── Balance sheet ─────────────────────────────────────────────────
        bs_rows = self._get_recent_bs(ticker, ed, periods=1)
        current_ratio = debt_to_equity = None
        if bs_rows:
            current_ratio  = bs_rows[0].get("current_ratio")
            debt_to_equity = bs_rows[0].get("debt_to_equity")

        # ── Insider activity (90d before event) ───────────────────────────
        insider_net = insider_count = None
        since_90d = (pd.Timestamp(ed) - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
        insider_rows = self._get_insider_txns(ticker, since_90d, ed)
        if insider_rows:
            insider_count = len(insider_rows)
            buys  = sum(r.get("shares", 0) or 0 for r in insider_rows if r.get("transaction_type") == "buy")
            sells = sum(r.get("shares", 0) or 0 for r in insider_rows if r.get("transaction_type") == "sell")
            insider_net = buys - sells

        # ── 8-K flags (30d before event) ─────────────────────────────────
        since_30d = (pd.Timestamp(ed) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        eightk_rows = self._get_filings(ticker, since_30d, ed, "8-K")
        eightk_count = len(eightk_rows)
        eightk_pos = eightk_neg = 0  # placeholder — would need NLP

        # ── Macro at event ────────────────────────────────────────────────
        macro = self.db.get_macro_at(ed)
        vix_at = macro.get("vix") if macro else obs.get("vix_t0")
        yc_at  = macro.get("t10y2y") if macro else None
        spx_20d = self._spx_return_nd(ed, 20)

        # Determine sector ETF from observation
        sec_etf  = obs.get("sector_etf_ticker")
        sec_ret  = self._etf_return_nd(sec_etf, ed, 20) if sec_etf else None
        regime   = self._classify_regime_from_macro(macro) if macro else None

        # ── Institutional trend ───────────────────────────────────────────
        inst_rows = self._get_institutional(ticker, ed)
        inst_change = None
        if len(inst_rows) >= 2:
            curr = inst_rows[0].get("ownership_pct")
            prev = inst_rows[1].get("ownership_pct")
            if curr and prev and prev != 0:
                inst_change = curr - prev

        # ── News sentiment ────────────────────────────────────────────────
        news_rows = self.db.get_news(ticker, since_30d, ed)
        news_sent = None
        if news_rows:
            sentiments = [r.get("sentiment_raw", 0.0) or 0.0 for r in news_rows]
            news_sent = sum(sentiments) / len(sentiments)

        return {
            "ticker":                ticker,
            "earnings_date":         ed,
            "revenue_growth_1q":     rev_growth_1q,
            "revenue_growth_4q":     rev_growth_4q,
            "revenue_acceleration":  rev_accel,
            "gross_margin_trend":    gross_margin_trend,
            "operating_margin_trend": op_margin_trend,
            "cf_to_earnings_ratio":  cf_ratio,
            "current_ratio":         current_ratio,
            "debt_to_equity":        debt_to_equity,
            "insider_net_shares":    insider_net,
            "insider_txn_count":     insider_count,
            "eightk_count_30d":      eightk_count,
            "eightk_positive_flags": eightk_pos,
            "eightk_negative_flags": eightk_neg,
            "vix_at_event":          vix_at,
            "yield_spread_at_event": yc_at,
            "spx_return_20d":        spx_20d,
            "sector_return_20d":     sec_ret,
            "macro_regime":          regime,
            "inst_ownership_change": inst_change,
            "news_sentiment_30d":    news_sent,
            "news_count_30d":        len(news_rows),
            "sector_peer_surprise_avg": None,  # filled by Phase 7
            "readthrough_signal":    None,
            "enriched_at":           now,
        }

    # ────────────────────────────────────────────────────────────────────────
    # Phase 6: News context
    # ────────────────────────────────────────────────────────────────────────

    def collect_news(
        self, tickers: List[str], days_back: int = 365 * 5
    ) -> Dict[str, int]:
        """Collect historical news from Google News RSS (free, no key)."""
        results = {}
        for ticker in tickers:
            try:
                n = self._collect_ticker_news(ticker, days_back)
                results[ticker] = n
            except Exception as e:
                logger.warning("news %s: %s", ticker, e)
            time.sleep(1.0)  # polite rate limiting for Google
        return results

    def _collect_ticker_news(self, ticker: str, days_back: int) -> int:
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        records = []

        # Google News RSS
        try:
            url = GNEWS_RSS.format(q=f"{ticker}+earnings+stock")
            resp = requests.get(url, timeout=15,
                                headers={"User-Agent": "Mozilla/5.0 quant-fund"})
            if resp.status_code == 200:
                try:
                    root = ET.fromstring(resp.content)
                    for item in root.findall(".//item")[:50]:
                        title = (item.findtext("title") or "").strip()
                        link  = item.findtext("link") or ""
                        pub   = item.findtext("pubDate") or ""
                        try:
                            pub_dt = pd.to_datetime(pub, utc=True).strftime("%Y-%m-%d")
                        except Exception:
                            pub_dt = date.today().isoformat()
                        if pub_dt < cutoff:
                            continue
                        records.append({
                            "ticker":         ticker,
                            "published_date": pub_dt,
                            "headline":       title[:500],
                            "source":         "google_news",
                            "url":            link[:500],
                            "sentiment_raw":  _simple_sentiment(title),
                            "is_press_release": 0,
                            "collected_at":   now,
                        })
                except ET.ParseError:
                    pass
        except Exception as e:
            logger.debug("gnews %s: %s", ticker, e)

        # EDGAR press releases (8-K item 8.01 — earnings releases)
        filing_rows = self._get_filings_db(ticker, form_type="8-K", limit=200)
        for f in filing_rows:
            fd = f.get("filed_date", "")
            if fd < cutoff:
                continue
            desc = f.get("description", "")
            records.append({
                "ticker":           ticker,
                "published_date":   fd,
                "headline":         f"SEC 8-K: {desc[:200]}",
                "source":           "edgar_8k",
                "url":              f.get("url", ""),
                "sentiment_raw":    0.0,
                "is_press_release": 1,
                "edgar_form":       "8-K",
                "collected_at":     now,
            })

        return self.db.upsert_news(records)

    # ────────────────────────────────────────────────────────────────────────
    # Helper queries
    # ────────────────────────────────────────────────────────────────────────

    def _get_recent_financials(self, ticker: str, before_date: str, periods: int = 5) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM quarterly_financials WHERE ticker=? AND period<=? AND period_type='quarterly' ORDER BY period DESC LIMIT ?",
                [ticker, before_date, periods],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_recent_bs(self, ticker: str, before_date: str, periods: int = 1) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM balance_sheet WHERE ticker=? AND period<=? AND period_type='quarterly' ORDER BY period DESC LIMIT ?",
                [ticker, before_date, periods],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_recent_cf(self, ticker: str, before_date: str, periods: int = 2) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM cash_flow WHERE ticker=? AND period<=? AND period_type='quarterly' ORDER BY period DESC LIMIT ?",
                [ticker, before_date, periods],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_insider_txns(self, ticker: str, since: str, until: str) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM insider_transactions WHERE ticker=? AND transaction_date>=? AND transaction_date<=? ORDER BY transaction_date DESC",
                [ticker, since, until],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_filings(self, ticker: str, since: str, until: str, form_type: str) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM edgar_filings WHERE ticker=? AND form_type=? AND filed_date>=? AND filed_date<=? ORDER BY filed_date DESC",
                [ticker, form_type, since, until],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_filings_db(self, ticker: str, form_type: str, limit: int = 100) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM edgar_filings WHERE ticker=? AND form_type=? ORDER BY filed_date DESC LIMIT ?",
                [ticker, form_type, limit],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_institutional(self, ticker: str, before_date: str) -> List[dict]:
        with self.db._cursor() as cur:
            cur.execute(
                "SELECT * FROM institutional_ownership WHERE ticker=? AND period<=? ORDER BY period DESC LIMIT 4",
                [ticker, before_date],
            )
            return [dict(r) for r in cur.fetchall()]

    def _spx_return_nd(self, date_str: str, n: int) -> Optional[float]:
        try:
            with self.db._cursor() as cur:
                cur.execute(
                    "SELECT spx_close, date FROM macro_context WHERE date<=? ORDER BY date DESC LIMIT ?",
                    [date_str, n + 1],
                )
                rows = cur.fetchall()
            if len(rows) < 2:
                return None
            return (rows[0]["spx_close"] / rows[-1]["spx_close"] - 1) if rows[-1]["spx_close"] else None
        except Exception:
            return None

    def _etf_return_nd(self, etf_col: str, date_str: str, n: int) -> Optional[float]:
        col = etf_col.lower().replace("^", "")
        try:
            with self.db._cursor() as cur:
                cur.execute(
                    f"SELECT {col}, date FROM macro_context WHERE date<=? AND {col} IS NOT NULL ORDER BY date DESC LIMIT ?",
                    [date_str, n + 1],
                )
                rows = cur.fetchall()
            if len(rows) < 2:
                return None
            return (rows[0][col] / rows[-1][col] - 1) if rows[-1][col] else None
        except Exception:
            return None

    def _classify_regime_from_macro(self, macro: dict) -> Optional[int]:
        vix = macro.get("vix", 20.0) or 20.0
        yc  = macro.get("t10y2y", 0.5) or 0.5
        if yc < 0 and vix > 25:
            return 4  # RECESSION_RISK
        if vix > 20 and yc < 0:
            return 3  # RISK_OFF
        if vix < 15 and yc > 0.5:
            return 1  # GOLDILOCKS
        return 0  # RISK_ON

    # ────────────────────────────────────────────────────────────────────────
    # Full collection runner
    # ────────────────────────────────────────────────────────────────────────

    def collect_all(
        self,
        tickers: List[str],
        start: str = "2010-01-01",
        include_macro: bool = True,
        include_edgar: bool = True,
        include_news: bool = True,
    ) -> dict:
        """Run all collection phases for a list of tickers."""
        results = {}

        logger.info("=== Historical collect: %d tickers, start=%s ===", len(tickers), start)

        # Phase 1a: Price history
        logger.info("Phase 1: Price history")
        price_res = self.collect_price_history(tickers, start=start)
        results["price_rows"] = sum(price_res.values())

        # Phase 1b: Financials
        logger.info("Phase 1: Financials")
        fin_res = self.collect_financials(tickers, start=start)
        results["financial_rows"] = sum(fin_res.values())

        # Phase 2: EDGAR
        if include_edgar:
            logger.info("Phase 2: EDGAR filings")
            edgar_res = self.collect_edgar(tickers, start=start)
            results["edgar_rows"] = sum(edgar_res.values())

        # Phase 3: Macro context
        if include_macro:
            logger.info("Phase 3: Macro context")
            n_macro = self.collect_macro(start=start)
            results["macro_rows"] = n_macro

        # Phase 5: Enrich
        logger.info("Phase 5: Enriching earnings observations")
        n_enriched = self.enrich_earnings_observations(tickers)
        results["enriched_rows"] = n_enriched

        # Phase 6: News
        if include_news:
            logger.info("Phase 6: News context")
            news_res = self.collect_news(tickers, days_back=365 * 5)
            results["news_rows"] = sum(news_res.values())

        logger.info("Collection complete: %s", results)
        return results


# ── Utilities ─────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, 6)
    except (TypeError, ValueError):
        return None


def _xml_text(root: ET.Element, path: str) -> Optional[str]:
    el = root.find(path)
    return el.text.strip() if el is not None and el.text else None


def _simple_sentiment(text: str) -> float:
    """Naive keyword-based sentiment for news headlines."""
    text_lower = text.lower()
    positive = ["beat", "exceed", "raise", "growth", "strong", "record", "profit",
                 "upgrade", "buy", "positive", "outperform", "surpass", "guidance raised"]
    negative = ["miss", "lower", "cut", "loss", "weak", "decline", "disappoint",
                 "downgrade", "sell", "negative", "underperform", "warning", "guidance cut"]
    pos_score = sum(1 for w in positive if w in text_lower)
    neg_score = sum(1 for w in negative if w in text_lower)
    total = pos_score + neg_score
    if total == 0:
        return 0.0
    return round((pos_score - neg_score) / total, 3)
