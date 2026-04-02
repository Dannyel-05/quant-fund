"""
SQLite storage layer for the comprehensive historical intelligence database.

Tables:
  price_history             — daily OHLCV back to 2010
  quarterly_financials      — revenue, EPS, margins per quarter
  balance_sheet             — assets, liabilities, equity per quarter
  cash_flow                 — operating/investing/financing CF per quarter
  edgar_filings             — all SEC filing metadata (8-K, 10-K, 10-Q, etc.)
  insider_transactions      — Form 4 insider buy/sell records
  proxy_data                — DEF 14A compensation and governance
  institutional_ownership   — 13F institutional holdings
  macro_context             — daily macro: indices, rates, commodities, FX
  news_context              — press releases + RSS news headlines
  earnings_enriched         — pre-computed enrichment features for each earnings event
  delisted_companies        — known delisted small-caps with data status
"""
import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_local = threading.local()

DB_PATH = "output/historical_db.db"

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    adj_close   REAL,
    volume      REAL,
    source      TEXT DEFAULT 'yfinance',
    delisted    INTEGER DEFAULT 0,
    UNIQUE(ticker, date)
);

CREATE TABLE IF NOT EXISTS quarterly_financials (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    period              TEXT NOT NULL,   -- YYYY-MM-DD (period end)
    period_type         TEXT DEFAULT 'quarterly',
    revenue             REAL,
    gross_profit        REAL,
    operating_income    REAL,
    net_income          REAL,
    ebitda              REAL,
    eps_basic           REAL,
    eps_diluted         REAL,
    shares_outstanding  REAL,
    gross_margin        REAL,
    operating_margin    REAL,
    net_margin          REAL,
    revenue_growth_yoy  REAL,
    source              TEXT DEFAULT 'yfinance',
    UNIQUE(ticker, period, period_type)
);

CREATE TABLE IF NOT EXISTS balance_sheet (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    period              TEXT NOT NULL,
    period_type         TEXT DEFAULT 'quarterly',
    total_assets        REAL,
    total_liabilities   REAL,
    total_equity        REAL,
    cash_and_equiv      REAL,
    total_debt          REAL,
    net_debt            REAL,
    current_assets      REAL,
    current_liabilities REAL,
    current_ratio       REAL,
    debt_to_equity      REAL,
    book_value_per_share REAL,
    source              TEXT DEFAULT 'yfinance',
    UNIQUE(ticker, period, period_type)
);

CREATE TABLE IF NOT EXISTS cash_flow (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    period              TEXT NOT NULL,
    period_type         TEXT DEFAULT 'quarterly',
    operating_cf        REAL,
    investing_cf        REAL,
    financing_cf        REAL,
    capex               REAL,
    free_cash_flow      REAL,
    dividends_paid      REAL,
    buybacks            REAL,
    source              TEXT DEFAULT 'yfinance',
    UNIQUE(ticker, period, period_type)
);

CREATE TABLE IF NOT EXISTS edgar_filings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    cik             TEXT,
    form_type       TEXT NOT NULL,
    filed_date      TEXT NOT NULL,
    period_of_report TEXT,
    accession_number TEXT,
    url             TEXT,
    description     TEXT,
    size_bytes      INTEGER,
    collected_at    TEXT NOT NULL,
    UNIQUE(ticker, accession_number)
);

CREATE TABLE IF NOT EXISTS insider_transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    cik             TEXT,
    reporter_name   TEXT,
    reporter_title  TEXT,
    transaction_date TEXT NOT NULL,
    transaction_type TEXT,   -- 'buy', 'sell', 'option_exercise'
    shares          REAL,
    price_per_share REAL,
    total_value     REAL,
    shares_owned_after REAL,
    form_type       TEXT DEFAULT '4',
    accession_number TEXT,
    collected_at    TEXT NOT NULL,
    UNIQUE(ticker, accession_number, transaction_date, shares)
);

CREATE TABLE IF NOT EXISTS proxy_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    cik             TEXT,
    filed_date      TEXT NOT NULL,
    fiscal_year     TEXT,
    ceo_total_comp  REAL,
    cfo_total_comp  REAL,
    median_employee_pay REAL,
    pay_ratio       REAL,
    board_size      INTEGER,
    independent_directors INTEGER,
    new_directors   INTEGER,
    accession_number TEXT,
    collected_at    TEXT NOT NULL,
    UNIQUE(ticker, filed_date)
);

CREATE TABLE IF NOT EXISTS institutional_ownership (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    period          TEXT NOT NULL,   -- quarter end YYYY-MM-DD
    total_shares_held REAL,
    institutions_count INTEGER,
    ownership_pct   REAL,
    top_holder      TEXT,
    top_holder_pct  REAL,
    qoq_change_pct  REAL,
    collected_at    TEXT NOT NULL,
    UNIQUE(ticker, period)
);

CREATE TABLE IF NOT EXISTS macro_context (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    -- Equity indices
    spx_close       REAL,   -- S&P 500
    ndx_close       REAL,   -- Nasdaq 100
    rut_close       REAL,   -- Russell 2000
    ftse_close      REAL,   -- FTSE 100
    -- Volatility
    vix             REAL,
    -- Rates
    tnx             REAL,   -- 10Y Treasury
    tyx             REAL,   -- 30Y Treasury
    irx             REAL,   -- 3M T-bill
    t10y2y          REAL,   -- Yield spread
    -- FX
    dxy             REAL,   -- Dollar index
    gbpusd          REAL,
    eurusd          REAL,
    -- Commodities
    cl1_oil         REAL,   -- Crude oil
    gc1_gold        REAL,   -- Gold
    hg1_copper      REAL,   -- Copper
    -- Sector ETFs
    xlk             REAL, xlv REAL, xlf REAL, xly REAL,
    xlp REAL, xle REAL, xlu REAL, xlb REAL, xli REAL, xlre REAL, xlc REAL,
    -- Economic (FRED)
    unemployment    REAL,
    cpi_yoy         REAL,
    collected_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS news_context (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    published_date  TEXT NOT NULL,
    headline        TEXT NOT NULL,
    source          TEXT,
    url             TEXT,
    sentiment_raw   REAL,
    is_press_release INTEGER DEFAULT 0,
    edgar_form      TEXT,
    collected_at    TEXT NOT NULL,
    UNIQUE(ticker, published_date, headline)
);

CREATE TABLE IF NOT EXISTS earnings_enriched (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT NOT NULL,
    earnings_date           TEXT NOT NULL,
    -- Revenue trend
    revenue_growth_1q       REAL,
    revenue_growth_4q       REAL,
    revenue_acceleration    REAL,
    -- Margin trend
    gross_margin_trend      REAL,
    operating_margin_trend  REAL,
    -- Cash flow quality
    cf_to_earnings_ratio    REAL,
    -- Balance sheet health
    current_ratio           REAL,
    debt_to_equity          REAL,
    -- Insider activity (90d before earnings)
    insider_net_shares      REAL,  -- positive=net buy, negative=net sell
    insider_txn_count       INTEGER,
    -- 8-K flags (30d before earnings)
    eightk_count_30d        INTEGER,
    eightk_positive_flags   INTEGER,
    eightk_negative_flags   INTEGER,
    -- Macro context at event
    vix_at_event            REAL,
    yield_spread_at_event   REAL,
    spx_return_20d          REAL,
    sector_return_20d       REAL,
    macro_regime            INTEGER,
    -- Institutional trend
    inst_ownership_change   REAL,
    -- News sentiment (30d before)
    news_sentiment_30d      REAL,
    news_count_30d          INTEGER,
    -- Large-cap readthrough
    sector_peer_surprise_avg REAL,
    readthrough_signal      REAL,
    enriched_at             TEXT NOT NULL,
    UNIQUE(ticker, earnings_date)
);

CREATE TABLE IF NOT EXISTS delisted_companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL UNIQUE,
    company_name    TEXT,
    sector          TEXT,
    delisted_date   TEXT,
    delisted_reason TEXT,  -- 'acquired', 'bankrupt', 'voluntary', 'compliance'
    acquiring_company TEXT,
    data_available  INTEGER DEFAULT 0,
    price_rows      INTEGER DEFAULT 0,
    financial_rows  INTEGER DEFAULT 0,
    last_attempted  TEXT
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ph_ticker_date   ON price_history (ticker, date)",
    "CREATE INDEX IF NOT EXISTS idx_qf_ticker_period ON quarterly_financials (ticker, period)",
    "CREATE INDEX IF NOT EXISTS idx_bs_ticker_period ON balance_sheet (ticker, period)",
    "CREATE INDEX IF NOT EXISTS idx_cf_ticker_period ON cash_flow (ticker, period)",
    "CREATE INDEX IF NOT EXISTS idx_ef_ticker_form   ON edgar_filings (ticker, form_type, filed_date)",
    "CREATE INDEX IF NOT EXISTS idx_it_ticker_date   ON insider_transactions (ticker, transaction_date)",
    "CREATE INDEX IF NOT EXISTS idx_mc_date          ON macro_context (date)",
    "CREATE INDEX IF NOT EXISTS idx_nc_ticker_date   ON news_context (ticker, published_date)",
    "CREATE INDEX IF NOT EXISTS idx_ee_ticker_date   ON earnings_enriched (ticker, earnings_date)",
]


class HistoricalDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ── Connection ───────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        if not getattr(_local, "hist_conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.row_factory = sqlite3.Row
            _local.hist_conn = conn
        return _local.hist_conn

    @contextmanager
    def _cursor(self):
        conn = self._conn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _init_schema(self):
        conn = self._conn()
        conn.executescript(_DDL)
        for idx in _INDEXES:
            try:
                conn.execute(idx)
            except Exception:
                pass
        conn.commit()

    # ── Generic upsert ───────────────────────────────────────────────────────

    def _upsert(self, table: str, record: dict, conflict_cols: List[str]) -> None:
        if not record:
            return
        cols = list(record.keys())
        ph = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        update_str = ", ".join(
            f"{c}=excluded.{c}" for c in cols if c not in conflict_cols
        )
        sql = f"""
            INSERT INTO {table} ({col_str}) VALUES ({ph})
            ON CONFLICT({', '.join(conflict_cols)}) DO UPDATE SET {update_str}
        """
        with self._cursor() as cur:
            cur.execute(sql, [record.get(c) for c in cols])

    def _upsert_many(self, table: str, records: List[dict], conflict_cols: List[str]) -> int:
        if not records:
            return 0
        n = 0
        for r in records:
            try:
                self._upsert(table, r, conflict_cols)
                n += 1
            except Exception as e:
                logger.debug("upsert %s failed: %s", table, e)
        return n

    # ── Price history ────────────────────────────────────────────────────────

    def upsert_prices(self, records: List[dict]) -> int:
        return self._upsert_many("price_history", records, ["ticker", "date"])

    def get_prices(self, ticker: str, since: str = "2010-01-01") -> List[dict]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM price_history WHERE ticker=? AND date>=? ORDER BY date",
                [ticker, since],
            )
            return [dict(r) for r in cur.fetchall()]

    # ── Financials ───────────────────────────────────────────────────────────

    def upsert_financials(self, records: List[dict]) -> int:
        return self._upsert_many("quarterly_financials", records, ["ticker", "period", "period_type"])

    def upsert_balance_sheet(self, records: List[dict]) -> int:
        return self._upsert_many("balance_sheet", records, ["ticker", "period", "period_type"])

    def upsert_cash_flow(self, records: List[dict]) -> int:
        return self._upsert_many("cash_flow", records, ["ticker", "period", "period_type"])

    # ── EDGAR ────────────────────────────────────────────────────────────────

    def upsert_filings(self, records: List[dict]) -> int:
        return self._upsert_many("edgar_filings", records, ["ticker", "accession_number"])

    def upsert_insider_txns(self, records: List[dict]) -> int:
        return self._upsert_many("insider_transactions", records,
                                  ["ticker", "accession_number", "transaction_date", "shares"])

    def upsert_proxy(self, records: List[dict]) -> int:
        return self._upsert_many("proxy_data", records, ["ticker", "filed_date"])

    def upsert_institutional(self, records: List[dict]) -> int:
        return self._upsert_many("institutional_ownership", records, ["ticker", "period"])

    # ── Macro ────────────────────────────────────────────────────────────────

    def upsert_macro(self, records: List[dict]) -> int:
        return self._upsert_many("macro_context", records, ["date"])

    def get_macro_at(self, date: str) -> Optional[dict]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM macro_context WHERE date<=? ORDER BY date DESC LIMIT 1",
                [date],
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── News ─────────────────────────────────────────────────────────────────

    def upsert_news(self, records: List[dict]) -> int:
        return self._upsert_many("news_context", records, ["ticker", "published_date", "headline"])

    def get_news(self, ticker: str, since: str, until: str) -> List[dict]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM news_context WHERE ticker=? AND published_date>=? AND published_date<=? ORDER BY published_date DESC",
                [ticker, since, until],
            )
            return [dict(r) for r in cur.fetchall()]

    # ── Enriched ─────────────────────────────────────────────────────────────

    def upsert_enriched(self, records: List[dict]) -> int:
        return self._upsert_many("earnings_enriched", records, ["ticker", "earnings_date"])

    def get_enriched(self, ticker: str = None) -> List[dict]:
        with self._cursor() as cur:
            if ticker:
                cur.execute(
                    "SELECT * FROM earnings_enriched WHERE ticker=? ORDER BY earnings_date DESC",
                    [ticker],
                )
            else:
                cur.execute("SELECT * FROM earnings_enriched ORDER BY earnings_date DESC LIMIT 10000")
            return [dict(r) for r in cur.fetchall()]

    # ── Delisted ─────────────────────────────────────────────────────────────

    def upsert_delisted(self, records: List[dict]) -> int:
        return self._upsert_many("delisted_companies", records, ["ticker"])

    def get_delisted(self) -> List[dict]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM delisted_companies ORDER BY ticker")
            return [dict(r) for r in cur.fetchall()]

    # ── Status ───────────────────────────────────────────────────────────────

    def status(self) -> dict:
        counts = {}
        tables = [
            "price_history", "quarterly_financials", "balance_sheet", "cash_flow",
            "edgar_filings", "insider_transactions", "proxy_data", "institutional_ownership",
            "macro_context", "news_context", "earnings_enriched", "delisted_companies",
        ]
        with self._cursor() as cur:
            for t in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    counts[t] = cur.fetchone()[0]
                except Exception:
                    counts[t] = -1
            try:
                cur.execute("SELECT COUNT(DISTINCT ticker) FROM price_history")
                counts["tickers_with_price"] = cur.fetchone()[0]
            except Exception:
                counts["tickers_with_price"] = 0
            try:
                cur.execute("SELECT MIN(date), MAX(date) FROM price_history")
                row = cur.fetchone()
                counts["price_date_range"] = f"{row[0]} → {row[1]}"
            except Exception:
                counts["price_date_range"] = "n/a"
        counts["db_path"] = str(self.db_path)
        return counts
