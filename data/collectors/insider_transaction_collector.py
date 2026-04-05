"""
InsiderTransactionCollector — fetches and parses SEC EDGAR Form 4 filings.

Data flow:
  1. Search EDGAR full-text search for recent Form 4 filings
  2. Fetch each filing's primary XML document
  3. Parse transaction details: ticker, insider, title, date, shares, price, type
  4. Store in closeloop.db insider_transactions table
  5. Generate insider_cluster_signal: cluster buying within 30 days

Signal scoring:
  - 3+ insiders buying same stock within 30 days = strong buy (0.8)
  - CEO/CFO purchase weighted 3× vs other insiders
  - Open market purchase (P) weighted 2× vs option exercise (A)
  - Net score = (weighted_buys - weighted_sells) / (weighted_buys + weighted_sells + 1)
  - Adds +0.10 to combined_score when cluster_buy detected
"""
from __future__ import annotations

import logging
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_BASE   = "https://www.sec.gov"

_CEO_CFO_TITLES = {"ceo", "chief executive", "cfo", "chief financial", "president"}

# Transaction type codes
_BUY_CODES  = {"P"}   # open-market purchase
_EXER_CODES = {"A", "M"}  # option exercises, other acquisitions (lower weight)
_SELL_CODES = {"S", "D"}  # open-market sale, disposition


def _is_ceo_cfo(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in _CEO_CFO_TITLES)


class InsiderTransactionCollector:
    """Collect and store SEC Form 4 insider transactions."""

    DB_PATH = "closeloop/storage/closeloop.db"

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = db_path or self.DB_PATH
        self._ensure_table()
        try:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Apollo-Quant research@apollo-quant.com",
                "Accept-Encoding": "gzip, deflate",
            })
        except ImportError:
            self._session = None  # type: ignore

    # ── DB setup ──────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            con.execute("""
                CREATE TABLE IF NOT EXISTS insider_transactions (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker            TEXT    NOT NULL,
                    insider_name      TEXT,
                    title             TEXT,
                    transaction_date  TEXT,
                    shares            REAL,
                    price_per_share   REAL,
                    transaction_type  TEXT,
                    value_usd         REAL,
                    is_ceo_cfo        INTEGER DEFAULT 0,
                    filing_url        TEXT,
                    stored_at         TEXT DEFAULT (datetime('now')),
                    UNIQUE(ticker, insider_name, transaction_date, transaction_type)
                )
            """)
            con.commit()
            con.close()
        except Exception as exc:
            logger.warning("InsiderTransactionCollector._ensure_table: %s", exc)

    # ── EDGAR search ──────────────────────────────────────────────────────

    def fetch_recent_form4(self, days_back: int = 1) -> List[Dict]:
        """Search EDGAR for recent Form 4 filings. Returns list of filing metadata."""
        if self._session is None:
            return []
        end_dt   = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days_back)
        try:
            resp = self._session.get(
                _EDGAR_SEARCH,
                params={
                    "forms":      "4",
                    "dateRange":  "custom",
                    "startdt":    start_dt.strftime("%Y-%m-%d"),
                    "enddt":      end_dt.strftime("%Y-%m-%d"),
                    "_source":    "period_of_report,file_date,entity_name,file_num",
                    "from":       0,
                    "size":       100,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug("EDGAR search HTTP %d", resp.status_code)
                return []
            hits = resp.json().get("hits", {}).get("hits", [])
            results = []
            for h in hits:
                src = h.get("_source", {})
                results.append({
                    "entity_name": src.get("entity_name", ""),
                    "file_date":   src.get("file_date", ""),
                    "filing_id":   h.get("_id", ""),
                })
            return results
        except Exception as exc:
            logger.debug("EDGAR search error: %s", exc)
            return []

    # ── XML parser ────────────────────────────────────────────────────────

    def parse_form4_xml(self, filing_url: str) -> Optional[Dict]:
        """Fetch and parse Form 4 XML from EDGAR. Returns structured dict or None."""
        if self._session is None:
            return None
        try:
            resp = self._session.get(filing_url, timeout=15)
            if resp.status_code != 200:
                return None
            root = ET.fromstring(resp.text)
            ns = ""  # Form 4 XML uses no namespace in most modern filings

            def _text(tag: str) -> str:
                el = root.find(f".//{tag}")
                return el.text.strip() if el is not None and el.text else ""

            ticker   = _text("issuerTradingSymbol")
            owner    = _text("rptOwnerName")
            title    = _text("officerTitle")

            transactions = []
            for tx in root.findall(".//nonDerivativeTransaction"):
                try:
                    tx_date   = tx.findtext(".//transactionDate/value") or ""
                    tx_shares = tx.findtext(".//transactionShares/value") or "0"
                    tx_price  = tx.findtext(".//transactionPricePerShare/value") or "0"
                    tx_code   = tx.findtext(".//transactionCode") or "?"
                    shares = float(tx_shares)
                    price  = float(tx_price)
                    transactions.append({
                        "ticker":           ticker,
                        "insider_name":     owner,
                        "title":            title,
                        "transaction_date": tx_date,
                        "shares":           shares,
                        "price_per_share":  price,
                        "transaction_type": tx_code,
                        "value_usd":        shares * price,
                        "is_ceo_cfo":       int(_is_ceo_cfo(title)),
                        "filing_url":       filing_url,
                    })
                except Exception:
                    continue
            return {"ticker": ticker, "transactions": transactions} if ticker else None
        except Exception as exc:
            logger.debug("parse_form4_xml %s: %s", filing_url, exc)
            return None

    # ── collection ────────────────────────────────────────────────────────

    def _store_transactions(self, txs: List[Dict]) -> int:
        stored = 0
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            for tx in txs:
                try:
                    con.execute("""
                        INSERT OR IGNORE INTO insider_transactions
                        (ticker, insider_name, title, transaction_date, shares,
                         price_per_share, transaction_type, value_usd, is_ceo_cfo, filing_url)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (
                        tx.get("ticker"), tx.get("insider_name"), tx.get("title"),
                        tx.get("transaction_date"), tx.get("shares"),
                        tx.get("price_per_share"), tx.get("transaction_type"),
                        tx.get("value_usd"), tx.get("is_ceo_cfo", 0),
                        tx.get("filing_url"),
                    ))
                    stored += con.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    pass
            con.commit()
            con.close()
        except Exception as exc:
            logger.warning("_store_transactions: %s", exc)
        return stored

    def collect(self, days_back: int = 1, max_filings: int = 50) -> int:
        """
        Fetch recent Form 4 filings, parse XML, store rows.
        Returns total rows stored.
        """
        filings = self.fetch_recent_form4(days_back=days_back)
        total_stored = 0
        for i, filing_meta in enumerate(filings[:max_filings]):
            filing_id = filing_meta.get("filing_id", "")
            if not filing_id:
                continue
            # filing_id format from EDGAR search: "XXXXXXXXXX-YY-NNNNNN:document.xml"
            id_parts = filing_id.split(":")
            accession_dashes = id_parts[0]         # e.g. "0001840706-26-000084"
            doc_name = id_parts[1] if len(id_parts) > 1 else ""
            accession_nodash = accession_dashes.replace("-", "")  # "000184070626000084"
            cik = accession_dashes.split("-")[0]   # e.g. "0001840706"
            xml_url = (
                f"{_EDGAR_BASE}/Archives/edgar/data/"
                f"{cik}/{accession_nodash}/{doc_name}"
            )
            parsed = self.parse_form4_xml(xml_url)
            if parsed and parsed.get("transactions"):
                total_stored += self._store_transactions(parsed["transactions"])
            # Rate-limit: 10 req/s SEC EDGAR limit
            if i % 10 == 9:
                time.sleep(1.0)
        logger.info("InsiderTransactionCollector: stored %d new rows", total_stored)
        return total_stored

    # ── signal generation ─────────────────────────────────────────────────

    def generate_insider_signal(self, ticker: str, lookback_days: int = 30) -> float:
        """
        Returns float 0.0-1.0 representing insider buying strength.
        Cluster buying (3+ insiders buying within lookback_days) → strong signal.
        CEO/CFO buys weighted 3×; open-market buys weighted 2× vs option exercises.
        """
        try:
            since = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            con = sqlite3.connect(self._db_path, timeout=10)
            rows = con.execute("""
                SELECT insider_name, transaction_type, shares, is_ceo_cfo
                FROM insider_transactions
                WHERE ticker = ? AND transaction_date >= ?
            """, (ticker, since)).fetchall()
            con.close()
        except Exception as exc:
            logger.debug("generate_insider_signal %s: %s", ticker, exc)
            return 0.0

        if not rows:
            return 0.0

        weighted_buys  = 0.0
        weighted_sells = 0.0
        buy_insiders: set = set()

        for insider_name, tx_type, shares, is_ceo_cfo in rows:
            shares = float(shares or 0)
            ceo_mult = 3.0 if is_ceo_cfo else 1.0
            if tx_type in _BUY_CODES:
                w = shares * ceo_mult * 2.0  # open-market 2× bonus
                weighted_buys += w
                buy_insiders.add(insider_name)
            elif tx_type in _EXER_CODES:
                w = shares * ceo_mult * 1.0
                weighted_buys += w
                buy_insiders.add(insider_name)
            elif tx_type in _SELL_CODES:
                w = shares * ceo_mult
                weighted_sells += w

        cluster_buy = len(buy_insiders) >= 3
        net = (weighted_buys - weighted_sells) / (weighted_buys + weighted_sells + 1e-9)
        # Boost cluster buys to 0.8 floor
        if cluster_buy and net > 0:
            score = max(net, 0.8)
        else:
            score = max(0.0, net)
        return min(score, 1.0)

    def should_boost_signal(self, ticker: str, current_score: float, boost: float = 0.10) -> float:
        """
        If insider cluster buy detected, add +boost to current_score.
        Returns updated score clamped to [0, 1].
        """
        insider_score = self.generate_insider_signal(ticker)
        if insider_score >= 0.5:
            return min(1.0, current_score + boost)
        return current_score

    def status(self) -> Dict[str, Any]:
        try:
            con = sqlite3.connect(self._db_path, timeout=10)
            total = con.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0]
            tickers = con.execute("SELECT COUNT(DISTINCT ticker) FROM insider_transactions").fetchone()[0]
            con.close()
            return {"total_rows": total, "unique_tickers": tickers}
        except Exception:
            return {"total_rows": 0, "unique_tickers": 0}
