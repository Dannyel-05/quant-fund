"""
Sophisticated Insider Transaction Analysis Engine (upgraded).

NOISE EXCLUSION: TAX_WITHHOLDING (code F), OPTION_EXERCISE_SAME_DAY_SALE,
  SCHEDULED_10b51 (10b5-1 footnote), COMPENSATION_VEST (A/M), GIFT_TRANSFER (G)

SIGNAL SCORING: InsiderSignalScore 0-100 for OPEN_MARKET_BUY
  Base 50 + role_score + size_score + price_context_score + cluster_context_score
  + track_record_score + timing_score + first_purchase_bonus

CLUSTER ANALYSIS: ClusterBuyScore, ClusterSellScore, NetClusterScore -100/+100
  DIP_BUY_CLUSTER flag: stock down >20% last 30d AND ClusterBuyScore>20 AND NetCluster>+15
  POST_EARNINGS_DIP_BUY flag: earnings in last 14d AND stock down >10% AND any buy since

PEAD WIRING: get_pead_multiplier(ticker, earnings_date, price_change_30d)
  Returns (multiplier: float, reason: str)
  score>70 in 30d: 1.4x
  DIP_BUY_CLUSTER: 1.6x + INSIDER_CLUSTER_LONG signal
  POST_EARNINGS_DIP_BUY: 1.8x + HIGH_CONVICTION log
  NetClusterScore < -30: 0.0 (suppress long) + INSIDER_CLUSTER_SHORT signal

INDIVIDUAL TRACKING: InsiderTrackRecord per insider
  buy_accuracy_90d: % of buys where stock up >5% at 90d
  sell_accuracy_90d: % of sells where stock down >5% at 90d
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_local = threading.local()

# Transaction type codes
SIGNAL_STRONG_BULLISH = "STRONG_BULLISH"
SIGNAL_WEAK_BULLISH   = "WEAK_BULLISH"
SIGNAL_NEUTRAL        = "NEUTRAL"
SIGNAL_BEARISH        = "BEARISH"

# Cluster thresholds
CLUSTER_MEANINGFUL = 0.30   # 30%+ of insiders buying
CLUSTER_STRONG     = 0.50   # 50%+ of insiders buying

# Compensation-relative thresholds
COMP_RELATIVE_SIGNIFICANT   = 0.50   # > 50% of annual comp = significant
COMP_RELATIVE_EXTRAORDINARY = 1.00   # > 100% = extraordinary

_DDL = """
CREATE TABLE IF NOT EXISTS insider_transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    filing_date         TEXT NOT NULL,
    transaction_date    TEXT,
    insider_name        TEXT,
    insider_title       TEXT,
    insider_role        TEXT,
    transaction_code    TEXT,
    shares              REAL,
    price_per_share     REAL,
    value_usd           REAL,
    shares_total        REAL,
    is_10b51            INTEGER DEFAULT 0,
    classification      TEXT,
    signal_type         TEXT,
    signal_strength     REAL,
    cluster_score       REAL,
    comp_ratio          REAL,
    price_t0            REAL,
    price_t30           REAL,
    price_t60           REAL,
    price_t90           REAL,
    return_t30          REAL,
    return_t60          REAL,
    return_t90          REAL,
    was_predictive      INTEGER,
    stored_at           TEXT NOT NULL,
    is_noise            INTEGER DEFAULT 0,
    noise_type          TEXT,
    insider_signal_score INTEGER,
    role_score          INTEGER,
    size_score          INTEGER,
    price_context_score  INTEGER,
    cluster_context_score INTEGER,
    track_record_score  INTEGER,
    timing_score        INTEGER,
    first_purchase_flag INTEGER DEFAULT 0,
    price_30d_change    REAL,
    price_52w_position  REAL,
    UNIQUE (ticker, filing_date, insider_name, transaction_code, shares)
);

CREATE TABLE IF NOT EXISTS insider_track_records (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT NOT NULL,
    insider_name            TEXT NOT NULL,
    n_buys                  INTEGER DEFAULT 0,
    n_sells                 INTEGER DEFAULT 0,
    buy_accuracy            REAL,
    avg_return_60d          REAL,
    last_updated            TEXT,
    buy_accuracy_90d        REAL,
    avg_return_90d          REAL,
    sell_accuracy_90d       REAL,
    avg_return_90d_after_sell REAL,
    UNIQUE (ticker, insider_name)
);

CREATE TABLE IF NOT EXISTS insider_cluster_state (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,
    cluster_buy_score   REAL,
    cluster_sell_score  REAL,
    net_cluster_score   REAL,
    n_buyers            INTEGER,
    n_sellers           INTEGER,
    dip_buy_cluster     INTEGER DEFAULT 0,
    post_earnings_dip_buy INTEGER DEFAULT 0,
    computed_at         TEXT,
    UNIQUE (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_ins_ticker ON insider_transactions (ticker);
CREATE INDEX IF NOT EXISTS idx_ins_date   ON insider_transactions (filing_date);
CREATE INDEX IF NOT EXISTS idx_ins_class  ON insider_transactions (classification);
"""


class InsiderAnalyser:
    """
    Analyses Form 4 insider transactions with sophisticated classification.
    Backward-compatible with previous API; adds scoring, noise filtering,
    cluster flags, PEAD multiplier, and earnings_db integration.
    """

    def __init__(self, db_path: str = "output/insider_analysis.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._migrate_schema()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if not getattr(_local, "insider_conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            _local.insider_conn = conn
        return _local.insider_conn

    @contextmanager
    def _cursor(self):
        conn = self._connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _init_db(self) -> None:
        conn = self._connect()
        conn.executescript(_DDL)
        conn.commit()

    def _migrate_schema(self) -> None:
        """Safely add new columns to existing tables via ALTER TABLE."""
        new_columns = [
            # insider_transactions extras
            ("insider_transactions", "is_noise",              "INTEGER DEFAULT 0"),
            ("insider_transactions", "noise_type",            "TEXT"),
            ("insider_transactions", "insider_signal_score",  "INTEGER"),
            ("insider_transactions", "role_score",            "INTEGER"),
            ("insider_transactions", "size_score",            "INTEGER"),
            ("insider_transactions", "price_context_score",   "INTEGER"),
            ("insider_transactions", "cluster_context_score", "INTEGER"),
            ("insider_transactions", "track_record_score",    "INTEGER"),
            ("insider_transactions", "timing_score",          "INTEGER"),
            ("insider_transactions", "first_purchase_flag",   "INTEGER DEFAULT 0"),
            ("insider_transactions", "price_30d_change",      "REAL"),
            ("insider_transactions", "price_52w_position",    "REAL"),
            # insider_track_records extras
            ("insider_track_records", "buy_accuracy_90d",           "REAL"),
            ("insider_track_records", "avg_return_90d",             "REAL"),
            ("insider_track_records", "n_sells",                    "INTEGER DEFAULT 0"),
            ("insider_track_records", "sell_accuracy_90d",          "REAL"),
            ("insider_track_records", "avg_return_90d_after_sell",  "REAL"),
        ]
        conn = self._connect()
        for table, column, col_type in new_columns:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                conn.commit()
                logger.debug("Migration: added %s.%s", table, column)
            except sqlite3.OperationalError:
                pass  # column already exists

    # ------------------------------------------------------------------
    # Noise Detection
    # ------------------------------------------------------------------

    def _is_noise_transaction(
        self, txn: dict, all_txns_today: List[dict] = None
    ) -> Tuple[bool, str]:
        """
        Returns (is_noise, noise_type).
        Detects TAX_WITHHOLDING, SAME_DAY_EXERCISE_SALE, SCHEDULED_10b51,
        COMPENSATION_VEST, GIFT_TRANSFER.
        """
        code = txn.get("transaction_code", "").upper()
        is_10b51 = bool(txn.get("is_10b51"))

        if code == "F":
            return True, "TAX_WITHHOLDING"

        if code in ("A", "M", "J"):
            if all_txns_today:
                same_day_sales = [
                    t for t in all_txns_today
                    if t.get("transaction_code", "").upper() == "S"
                    and t.get("date") == txn.get("date")
                ]
                if same_day_sales:
                    return True, "OPTION_EXERCISE_SAME_DAY_SALE"
            return True, "COMPENSATION_VEST"

        if code == "G":
            return True, "GIFT_TRANSFER"

        if code == "S" and is_10b51:
            return True, "SCHEDULED_10b51"

        # Check for same-day exercise + sale
        if code == "S" and all_txns_today:
            same_day_exercises = [
                t for t in all_txns_today
                if t.get("transaction_code", "").upper() in ("M", "A")
                and t.get("date") == txn.get("date")
            ]
            if same_day_exercises:
                return True, "OPTION_EXERCISE_SAME_DAY_SALE"

        return False, ""

    # ------------------------------------------------------------------
    # Score Components
    # ------------------------------------------------------------------

    def _role_score(self, title: str) -> int:
        """Return role score component for InsiderSignalScore."""
        import re
        t = (title or "").upper()
        if any(x in t for x in ("CHIEF EXECUTIVE", "PRESIDENT")) or re.search(r'\bCEO\b', t):
            return 20
        if "CHIEF FINANCIAL" in t or re.search(r'\bCFO\b', t):
            return 18
        if "CHIEF OPERATING" in t or re.search(r'\bCOO\b', t):
            return 15
        if "CHIEF TECHNOLOGY" in t or "CHIEF SCIENTIST" in t or re.search(r'\bCTO\b', t):
            return 15
        if "DIRECTOR" in t:
            return 8
        if "VICE PRESIDENT" in t or re.search(r'\bVP\b', t):
            return 5
        return 2

    def _size_score(
        self, value_usd: float, comp_annual_usd: Optional[float]
    ) -> int:
        """Return size-relative-to-compensation score component."""
        if not comp_annual_usd or comp_annual_usd <= 0:
            if value_usd >= 1_000_000:
                return 25
            if value_usd >= 500_000:
                return 20
            if value_usd >= 200_000:
                return 15
            if value_usd >= 100_000:
                return 10
            if value_usd >= 50_000:
                return 5
            return 0
        ratio = value_usd / comp_annual_usd
        if ratio >= 1.0:
            return 25
        if ratio >= 0.5:
            return 20
        if ratio >= 0.2:
            return 15
        if ratio >= 0.1:
            return 10
        if ratio >= 0.05:
            return 5
        return 0

    def _price_context_score(self, ticker: str, txn_date_str: str) -> int:
        """
        Return price context score using yfinance 52-week range and 30d return.
        Near 52w low (<10% above low): +15
        Down >20% in 30d: +15 (additive)
        Near 52w high (>90% of range): -3
        At ATH: -5
        Fail open (return 0) if data unavailable.
        """
        try:
            import yfinance as yf
            import pandas as pd

            hist = yf.download(ticker, period="1y", auto_adjust=True, progress=False)
            if hist is None or hist.empty:
                return 0

            # Handle MultiIndex columns from yfinance
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = [c[0].lower() for c in hist.columns]
            else:
                hist.columns = [c.lower() for c in hist.columns]

            close_col = "close" if "close" in hist.columns else hist.columns[0]
            closes = hist[close_col].dropna()
            if len(closes) < 20:
                return 0

            week52_low  = float(closes.min())
            week52_high = float(closes.max())
            current     = float(closes.iloc[-1])

            score = 0

            # 30-day return
            if len(closes) >= 22:
                price_30d_ago = float(closes.iloc[-22])
                ret_30d = (current - price_30d_ago) / price_30d_ago if price_30d_ago > 0 else 0.0
                if ret_30d <= -0.20:
                    score += 15
            else:
                ret_30d = 0.0

            # 52-week position
            price_range = week52_high - week52_low
            if price_range > 0:
                pos_in_range = (current - week52_low) / price_range
                pct_above_low = (current - week52_low) / week52_low if week52_low > 0 else 1.0

                if pct_above_low < 0.10:
                    score += 15
                elif pos_in_range > 0.98:
                    score -= 5  # ATH
                elif pos_in_range > 0.90:
                    score -= 3  # near 52w high

            return score
        except Exception as exc:
            logger.debug("_price_context_score failed for %s: %s", ticker, exc)
            return 0

    def _cluster_context_score(
        self, ticker: str, txn_date_str: str, other_txns_30d: List[dict]
    ) -> int:
        """
        Return cluster context score component.
        3+ different insiders buying in 30d: +20
        2 insiders: +10
        This insider also bought in last 30d: +5
        """
        try:
            if not other_txns_30d:
                return 0

            buyers_30d = set(
                t.get("owner_name") or t.get("insider_name") or ""
                for t in other_txns_30d
                if (t.get("transaction_code") or "").upper() == "P"
            )
            buyers_30d.discard("")
            n_buyers = len(buyers_30d)

            score = 0
            if n_buyers >= 3:
                score += 20
            elif n_buyers >= 2:
                score += 10

            # Extra: check DB for prior buys from this insider
            try:
                cutoff = (
                    datetime.strptime(txn_date_str[:10], "%Y-%m-%d") - timedelta(days=30)
                ).strftime("%Y-%m-%d")
                with self._cursor() as cur:
                    cur.execute(
                        """SELECT COUNT(*) FROM insider_transactions
                           WHERE ticker=? AND filing_date >= ? AND transaction_code='P'
                             AND is_noise=0""",
                        [ticker, cutoff]
                    )
                    prior_count = cur.fetchone()[0] or 0
                if prior_count > 0:
                    score += 5
            except Exception:
                pass

            return score
        except Exception as exc:
            logger.debug("_cluster_context_score failed: %s", exc)
            return 0

    def _track_record_score(self, ticker: str, insider_name: str) -> int:
        """
        Return track record score component based on historical accuracy.
        accuracy>70%: +10. 50-70%: +5. <50%: -5. n_buys<3: 0
        """
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT n_buys, buy_accuracy FROM insider_track_records "
                    "WHERE ticker=? AND insider_name=?",
                    [ticker, insider_name]
                )
                row = cur.fetchone()
            if not row:
                return 0
            n_buys = row["n_buys"] or 0
            accuracy = row["buy_accuracy"] or 0.0
            if n_buys < 3:
                return 0
            if accuracy > 0.70:
                return 10
            if accuracy >= 0.50:
                return 5
            return -5
        except Exception as exc:
            logger.debug("_track_record_score failed: %s", exc)
            return 0

    def _timing_score(self, ticker: str, txn_date_str: str) -> int:
        """
        Return timing score: earnings proximity penalty/bonus.
        Within 7d of next earnings: -10
        Within 30d: -5
        >60d away: +5
        Fail open (0) if data unavailable.
        """
        try:
            import yfinance as yf
            tkr = yf.Ticker(ticker)
            cal = tkr.calendar
            if cal is None:
                return 0

            # calendar is a dict with 'Earnings Date' as a list or single value
            earnings_dates = None
            if isinstance(cal, dict):
                earnings_dates = cal.get("Earnings Date") or cal.get("earningsDate")
            elif hasattr(cal, "get"):
                earnings_dates = cal.get("Earnings Date")

            if not earnings_dates:
                return 0

            if not isinstance(earnings_dates, (list, tuple)):
                earnings_dates = [earnings_dates]

            txn_dt = datetime.strptime(txn_date_str[:10], "%Y-%m-%d")
            future_dates = []
            for ed in earnings_dates:
                try:
                    if hasattr(ed, "date"):
                        ed_dt = datetime.combine(ed.date(), datetime.min.time())
                    else:
                        ed_dt = datetime.strptime(str(ed)[:10], "%Y-%m-%d")
                    if ed_dt >= txn_dt:
                        future_dates.append(ed_dt)
                except Exception:
                    continue

            if not future_dates:
                return 0

            nearest = min(future_dates)
            days_to_earnings = (nearest - txn_dt).days

            if days_to_earnings <= 7:
                return -10
            if days_to_earnings <= 30:
                return -5
            if days_to_earnings > 60:
                return 5
            return 0
        except Exception as exc:
            logger.debug("_timing_score failed for %s: %s", ticker, exc)
            return 0

    def _is_first_purchase(self, ticker: str, insider_name: str, before_date: str) -> bool:
        """Returns True if this is the first open-market buy for this insider."""
        try:
            with self._cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM insider_transactions
                       WHERE ticker=? AND insider_name=?
                         AND transaction_code='P' AND is_noise=0
                         AND filing_date < ?""",
                    [ticker, insider_name, before_date]
                )
                count = cur.fetchone()[0] or 0
            return count == 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Main Signal Score
    # ------------------------------------------------------------------

    def compute_insider_signal_score(
        self,
        txn: dict,
        ticker: str,
        other_txns_30d: List[dict] = None,
        comp_annual_usd: Optional[float] = None,
    ) -> int:
        """Compute InsiderSignalScore 0-100 for an OPEN_MARKET_BUY transaction."""
        try:
            title      = txn.get("insider_title") or txn.get("owner_title") or txn.get("role") or ""
            value_usd  = float(txn.get("value_usd") or txn.get("value") or 0)
            filing_date = (txn.get("filing_date") or txn.get("date") or "")[:10]
            insider_name = txn.get("insider_name") or txn.get("owner_name") or ""

            rs  = self._role_score(title)
            ss  = self._size_score(value_usd, comp_annual_usd)
            pcs = self._price_context_score(ticker, filing_date)
            ccs = self._cluster_context_score(ticker, filing_date, other_txns_30d or [])
            trs = self._track_record_score(ticker, insider_name)
            ts  = self._timing_score(ticker, filing_date)
            fp  = 10 if self._is_first_purchase(ticker, insider_name, filing_date) else 0

            raw = 50 + rs + ss + pcs + ccs + trs + ts + fp
            return max(0, min(100, raw))
        except Exception as exc:
            logger.debug("compute_insider_signal_score failed: %s", exc)
            return 50

    # ------------------------------------------------------------------
    # Cluster Scoring (new numeric 0-100 style)
    # ------------------------------------------------------------------

    def get_net_cluster_score(self, ticker: str, window_days: int = 30) -> float:
        """
        Returns NetClusterScore in [-100, +100]. Positive = net buying.
        ClusterBuyScore  = n_unique_buyers  / total_insiders * 100
        ClusterSellScore = n_unique_sellers / total_insiders * 100
        NetClusterScore  = ClusterBuyScore - ClusterSellScore
        """
        try:
            cutoff = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
            with self._cursor() as cur:
                cur.execute(
                    """SELECT insider_name, transaction_code
                       FROM insider_transactions
                       WHERE ticker=? AND filing_date >= ? AND is_noise=0""",
                    [ticker, cutoff]
                )
                rows = cur.fetchall()

            if not rows:
                return 0.0

            buyers  = {r["insider_name"] for r in rows if (r["transaction_code"] or "").upper() == "P"}
            sellers = {r["insider_name"] for r in rows if (r["transaction_code"] or "").upper() == "S"}
            total_insiders = max(1, len(buyers | sellers))

            buy_score  = len(buyers)  / total_insiders * 100.0
            sell_score = len(sellers) / total_insiders * 100.0
            net = buy_score - sell_score

            return max(-100.0, min(100.0, round(net, 2)))
        except Exception as exc:
            logger.debug("get_net_cluster_score failed: %s", exc)
            return 0.0

    def get_cluster_flags(
        self,
        ticker: str,
        price_change_30d: float = 0.0,
        earnings_date_recent: Optional[str] = None,
        price_change_since_earnings: float = 0.0,
    ) -> dict:
        """
        Returns dict with:
          dip_buy_cluster, post_earnings_dip_buy,
          net_cluster_score, cluster_buy_score, cluster_sell_score
        """
        try:
            cutoff_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            with self._cursor() as cur:
                cur.execute(
                    """SELECT insider_name, transaction_code
                       FROM insider_transactions
                       WHERE ticker=? AND filing_date >= ? AND is_noise=0""",
                    [ticker, cutoff_30d]
                )
                rows = cur.fetchall()

            buyers_30d  = {r["insider_name"] for r in rows if (r["transaction_code"] or "").upper() == "P"}
            sellers_30d = {r["insider_name"] for r in rows if (r["transaction_code"] or "").upper() == "S"}
            total_insiders = max(1, len(buyers_30d | sellers_30d))

            cluster_buy_score  = round(len(buyers_30d)  / total_insiders * 100.0, 2)
            cluster_sell_score = round(len(sellers_30d) / total_insiders * 100.0, 2)
            net_cluster_score  = round(cluster_buy_score - cluster_sell_score, 2)

            # DIP_BUY_CLUSTER
            dip_buy_cluster = (
                price_change_30d < -0.20
                and cluster_buy_score > 20
                and net_cluster_score > 15
            )

            # POST_EARNINGS_DIP_BUY
            post_earnings_dip_buy = False
            if earnings_date_recent:
                try:
                    ed_dt = datetime.strptime(earnings_date_recent[:10], "%Y-%m-%d")
                    days_since = (datetime.now() - ed_dt).days
                    if days_since <= 14 and price_change_since_earnings < -0.10:
                        # Check for any buy since earnings date
                        ed_str = earnings_date_recent[:10]
                        with self._cursor() as cur:
                            cur.execute(
                                """SELECT COUNT(*) FROM insider_transactions
                                   WHERE ticker=? AND filing_date >= ?
                                     AND transaction_code='P' AND is_noise=0""",
                                [ticker, ed_str]
                            )
                            buy_count = cur.fetchone()[0] or 0
                        post_earnings_dip_buy = buy_count > 0
                except Exception:
                    pass

            return {
                "dip_buy_cluster":       dip_buy_cluster,
                "post_earnings_dip_buy": post_earnings_dip_buy,
                "net_cluster_score":     net_cluster_score,
                "cluster_buy_score":     cluster_buy_score,
                "cluster_sell_score":    cluster_sell_score,
            }
        except Exception as exc:
            logger.debug("get_cluster_flags failed: %s", exc)
            return {
                "dip_buy_cluster":       False,
                "post_earnings_dip_buy": False,
                "net_cluster_score":     0.0,
                "cluster_buy_score":     0.0,
                "cluster_sell_score":    0.0,
            }

    # ------------------------------------------------------------------
    # PEAD Multiplier
    # ------------------------------------------------------------------

    def get_pead_multiplier(
        self,
        ticker: str,
        earnings_date: str = None,
        price_change_30d: float = 0.0,
    ) -> Tuple[float, str, List[str]]:
        """
        Returns (multiplier, primary_reason, list_of_signals_to_generate).
        Call this from PEAD signal generator.

        Rules (highest wins, cap 1.8x):
          NetClusterScore < -30  → 0.0x, CLUSTER_SELL_SUPPRESSION, [INSIDER_CLUSTER_SHORT]
          POST_EARNINGS_DIP_BUY  → 1.8x, POST_EARNINGS_DIP_BUY, [HIGH_CONVICTION]
          DIP_BUY_CLUSTER        → 1.6x, DIP_BUY_CLUSTER, [INSIDER_CLUSTER_LONG]
          max signal_score > 70  → 1.4x, HIGH_SCORE_BUY, []
          default                → 1.0x, NO_INSIDER_SIGNAL, []
        """
        try:
            flags = self.get_cluster_flags(
                ticker,
                price_change_30d=price_change_30d,
                earnings_date_recent=earnings_date,
                price_change_since_earnings=price_change_30d,
            )
            net_cluster = flags.get("net_cluster_score", 0.0)

            # Suppress if cluster selling
            if net_cluster < -30:
                logger.info(
                    "%s: PEAD suppressed — NetClusterScore=%.1f (cluster selling)",
                    ticker, net_cluster
                )
                return 0.0, "CLUSTER_SELL_SUPPRESSION", ["INSIDER_CLUSTER_SHORT"]

            # POST_EARNINGS_DIP_BUY → highest bullish multiplier
            if flags.get("post_earnings_dip_buy"):
                logger.info("%s: POST_EARNINGS_DIP_BUY signal → 1.8x multiplier", ticker)
                return 1.8, "POST_EARNINGS_DIP_BUY", ["HIGH_CONVICTION"]

            # DIP_BUY_CLUSTER
            if flags.get("dip_buy_cluster"):
                logger.info("%s: DIP_BUY_CLUSTER signal → 1.6x multiplier", ticker)
                return 1.6, "DIP_BUY_CLUSTER", ["INSIDER_CLUSTER_LONG"]

            # High individual signal score in last 90d
            try:
                cutoff_90d = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                with self._cursor() as cur:
                    cur.execute(
                        """SELECT MAX(insider_signal_score) FROM insider_transactions
                           WHERE ticker=? AND filing_date >= ?
                             AND transaction_code='P' AND is_noise=0""",
                        [ticker, cutoff_90d]
                    )
                    row = cur.fetchone()
                    max_score = row[0] if row and row[0] is not None else 0
                if max_score > 70:
                    logger.info("%s: high insider score %d → 1.4x multiplier", ticker, max_score)
                    return 1.4, "HIGH_SCORE_BUY", []
            except Exception:
                pass

            return 1.0, "NO_INSIDER_SIGNAL", []
        except Exception as exc:
            logger.debug("get_pead_multiplier failed for %s: %s", ticker, exc)
            return 1.0, "ERROR_FALLBACK", []

    # ------------------------------------------------------------------
    # Earnings DB Fields
    # ------------------------------------------------------------------

    def get_earnings_db_fields(self, ticker: str, as_of_date: str = None) -> dict:
        """
        Returns dict of insider fields to write into earnings_observations:
          insider_signal_score_90d, insider_cluster_buy_score,
          insider_cluster_sell_score, dip_buy_cluster_flag,
          post_earnings_dip_buy_flag, n_insiders_bought_90d,
          n_insiders_sold_90d (excl noise), ceo_bought_90d, cfo_bought_90d
        """
        try:
            cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

            with self._cursor() as cur:
                # Max signal score in 90d
                cur.execute(
                    """SELECT MAX(insider_signal_score) FROM insider_transactions
                       WHERE ticker=? AND filing_date >= ?
                         AND transaction_code='P' AND is_noise=0""",
                    [ticker, cutoff]
                )
                row = cur.fetchone()
                max_score_90d = int(row[0]) if row and row[0] is not None else 0

                # Buyers / sellers in 90d
                cur.execute(
                    """SELECT transaction_code, insider_title, COUNT(DISTINCT insider_name) as n
                       FROM insider_transactions
                       WHERE ticker=? AND filing_date >= ? AND is_noise=0
                       GROUP BY transaction_code""",
                    [ticker, cutoff]
                )
                code_rows = cur.fetchall()

            n_bought = 0
            n_sold   = 0
            for r in code_rows:
                c = (r["transaction_code"] or "").upper()
                if c == "P":
                    n_bought = r["n"]
                elif c == "S":
                    n_sold = r["n"]

            # CEO / CFO bought in 90d
            try:
                with self._cursor() as cur:
                    cur.execute(
                        """SELECT insider_title FROM insider_transactions
                           WHERE ticker=? AND filing_date >= ?
                             AND transaction_code='P' AND is_noise=0""",
                        [ticker, cutoff]
                    )
                    title_rows = cur.fetchall()

                ceo_bought = int(any(
                    any(x in (r["insider_title"] or "").upper() for x in ("CEO", "CHIEF EXECUTIVE", "PRESIDENT"))
                    for r in title_rows
                ))
                cfo_bought = int(any(
                    any(x in (r["insider_title"] or "").upper() for x in ("CFO", "CHIEF FINANCIAL"))
                    for r in title_rows
                ))
            except Exception:
                ceo_bought = 0
                cfo_bought = 0

            flags = self.get_cluster_flags(ticker)

            return {
                "insider_signal_score_90d":   max_score_90d,
                "insider_cluster_buy_score":  flags.get("cluster_buy_score", 0.0),
                "insider_cluster_sell_score": flags.get("cluster_sell_score", 0.0),
                "dip_buy_cluster_flag":       int(flags.get("dip_buy_cluster", False)),
                "post_earnings_dip_buy_flag": int(flags.get("post_earnings_dip_buy", False)),
                "n_insiders_bought_90d":      n_bought,
                "n_insiders_sold_90d":        n_sold,
                "ceo_bought_90d":             ceo_bought,
                "cfo_bought_90d":             cfo_bought,
            }
        except Exception as exc:
            logger.debug("get_earnings_db_fields failed for %s: %s", ticker, exc)
            return {}

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def display_analysis(self, ticker: str, days: int = 90) -> None:
        """Print formatted insider analysis for a ticker to stdout."""
        try:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            with self._cursor() as cur:
                cur.execute(
                    """SELECT filing_date, insider_name, insider_title,
                              transaction_code, shares, price_per_share, value_usd,
                              classification, signal_type, insider_signal_score,
                              is_noise, noise_type
                       FROM insider_transactions
                       WHERE ticker=? AND filing_date >= ?
                       ORDER BY filing_date DESC""",
                    [ticker, cutoff]
                )
                rows = [dict(r) for r in cur.fetchall()]

            print(f"\n{'='*72}")
            print(f"  INSIDER ANALYSIS: {ticker}  (last {days} days)")
            print(f"{'='*72}")

            if not rows:
                print("  No transactions found in this window.")
            else:
                hdr = f"{'Date':<12} {'Insider':<24} {'Code':<5} {'$Value':>10} {'Type':<22} {'Score':>5} {'Noise':<6}"
                print(hdr)
                print("-" * 72)
                for r in rows:
                    v = r.get("value_usd") or 0
                    v_str = f"${v:,.0f}" if v else "-"
                    noise_flag = "NOISE" if r.get("is_noise") else ""
                    score_str = str(r.get("insider_signal_score") or "-")
                    name = (r.get("insider_name") or "")[:23]
                    sig  = (r.get("signal_type") or r.get("classification") or "")[:21]
                    print(
                        f"{(r.get('filing_date') or ''):<12} {name:<24} "
                        f"{(r.get('transaction_code') or ''):<5} {v_str:>10} "
                        f"{sig:<22} {score_str:>5} {noise_flag:<6}"
                    )

            # Cluster summary
            cluster = self.get_cluster_flags(ticker)
            net = cluster.get("net_cluster_score", 0.0)
            print(f"\n  Cluster Buy Score : {cluster.get('cluster_buy_score', 0.0):.1f}")
            print(f"  Cluster Sell Score: {cluster.get('cluster_sell_score', 0.0):.1f}")
            print(f"  Net Cluster Score : {net:.1f}")
            if cluster.get("dip_buy_cluster"):
                print("  *** DIP_BUY_CLUSTER FLAG ACTIVE ***")
            if cluster.get("post_earnings_dip_buy"):
                print("  *** POST_EARNINGS_DIP_BUY FLAG ACTIVE ***")

            # Track records
            try:
                with self._cursor() as cur:
                    cur.execute(
                        """SELECT insider_name, n_buys, buy_accuracy, avg_return_60d
                           FROM insider_track_records WHERE ticker=?
                           ORDER BY n_buys DESC""",
                        [ticker]
                    )
                    tr_rows = [dict(r) for r in cur.fetchall()]
                if tr_rows:
                    print(f"\n  Track Records:")
                    for tr in tr_rows:
                        acc = tr.get("buy_accuracy")
                        acc_str = f"{acc*100:.0f}%" if acc is not None else "n/a"
                        print(
                            f"    {(tr.get('insider_name') or ''):<30} "
                            f"buys={tr.get('n_buys', 0):>3}  acc={acc_str}"
                        )
            except Exception:
                pass

            print(f"{'='*72}\n")
        except Exception as exc:
            logger.warning("display_analysis failed for %s: %s", ticker, exc)

    # ------------------------------------------------------------------
    # Transaction Classification  (UNCHANGED — backward compat)
    # ------------------------------------------------------------------

    def classify_transaction(
        self,
        txn: dict,
        ticker_price_change_30d: float = 0.0,
        other_txns_30d: List[dict] = None,
        total_insiders: int = 5,
        comp_annual_usd: Optional[float] = None,
    ) -> Tuple[str, str, float]:
        """
        Classify a single Form 4 transaction.
        Returns: (classification, signal_type, signal_strength)
        """
        code     = txn.get("transaction_code", "").upper()
        is_10b51 = bool(txn.get("is_10b51"))
        value    = float(txn.get("value_usd") or txn.get("value") or 0)
        shares   = float(txn.get("shares") or 0)
        shares_total = float(txn.get("shares_total") or 0)
        role     = (txn.get("role") or txn.get("relationship") or "").upper()

        other_txns_30d = other_txns_30d or []

        if code in ("A", "M", "J"):
            return SIGNAL_NEUTRAL, "OPTION_AWARD", 0.0
        if code == "F":
            return SIGNAL_NEUTRAL, "TAX_WITHHOLDING_SALE", 0.0
        if code == "G":
            return SIGNAL_NEUTRAL, "GIFT", 0.0
        if code == "S" and is_10b51:
            return SIGNAL_NEUTRAL, "SCHEDULED_10b51_SALE", 0.0

        if code == "P":
            signal_type = "OPEN_MARKET_BUY"
            strength    = 0.5

            if ticker_price_change_30d <= -0.15:
                return SIGNAL_STRONG_BULLISH, "DIP_BUY", 0.9

            unique_buyers_30d = len(set(
                t.get("owner_name", t.get("insider_name", "")) or ""
                for t in other_txns_30d
                if (t.get("transaction_code") or "").upper() == "P"
            ))
            if unique_buyers_30d >= 3:
                cluster_ratio = unique_buyers_30d / max(1, total_insiders)
                return SIGNAL_STRONG_BULLISH, "CLUSTER_BUY", min(1.0, 0.7 + cluster_ratio * 0.3)

            if comp_annual_usd and comp_annual_usd > 0:
                comp_ratio = value / comp_annual_usd
                if comp_ratio >= COMP_RELATIVE_EXTRAORDINARY:
                    return SIGNAL_STRONG_BULLISH, "LARGE_BUY_EXTRAORDINARY", 1.0
                elif comp_ratio >= COMP_RELATIVE_SIGNIFICANT:
                    return SIGNAL_STRONG_BULLISH, "LARGE_BUY", 0.85
                elif comp_ratio >= 0.10:
                    return SIGNAL_STRONG_BULLISH, "OPEN_MARKET_BUY", 0.7
                else:
                    return SIGNAL_WEAK_BULLISH, "SMALL_OPEN_MARKET_BUY", 0.3

            if value >= 500_000:
                return SIGNAL_STRONG_BULLISH, signal_type, 0.85
            elif value >= 100_000:
                return SIGNAL_STRONG_BULLISH, signal_type, 0.70
            elif value >= 10_000:
                return SIGNAL_STRONG_BULLISH, signal_type, 0.5
            else:
                return SIGNAL_WEAK_BULLISH, "SMALL_OPEN_MARKET_BUY", 0.2

        if code == "D" and shares > 0 and value > 0:
            return SIGNAL_WEAK_BULLISH, "AUTOMATIC_PLAN_BUY", 0.3

        if code == "S" and not is_10b51:
            if shares_total > 0 and shares > 0:
                pct_sold = shares / (shares + shares_total)
            else:
                pct_sold = 0.0

            unique_sellers_30d = len(set(
                t.get("owner_name", t.get("insider_name", "")) or ""
                for t in other_txns_30d
                if (t.get("transaction_code") or "").upper() == "S"
                and not t.get("is_10b51")
            ))
            if unique_sellers_30d >= 3:
                return SIGNAL_BEARISH, "CLUSTER_SELL", 0.7
            if pct_sold >= 0.20 or value >= 1_000_000:
                return SIGNAL_BEARISH, "OPEN_MARKET_SALE_LARGE", 0.6
            if any(r in role for r in ("CEO", "CFO", "PRESIDENT")):
                return SIGNAL_BEARISH, "EXECUTIVE_SALE", 0.5
            return SIGNAL_NEUTRAL, "ROUTINE_SALE", 0.1

        return SIGNAL_NEUTRAL, f"CODE_{code}", 0.0

    # ------------------------------------------------------------------
    # Cluster Analysis  (UNCHANGED — backward compat)
    # ------------------------------------------------------------------

    def get_cluster_score(
        self,
        ticker: str,
        window_days: int = 90,
    ) -> Dict:
        """
        Compute ClusterScore for a ticker over the last window_days.
        Returns dict with buy_cluster, sell_cluster, net_cluster.
        (Original method — kept for backward compatibility.)
        """
        cutoff = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute(
                """SELECT DISTINCT insider_name, classification
                   FROM insider_transactions
                   WHERE ticker=? AND filing_date >= ?
                     AND classification != ?""",
                [ticker, cutoff, SIGNAL_NEUTRAL]
            )
            rows = cur.fetchall()

        if not rows:
            return {"buy_cluster": 0.0, "sell_cluster": 0.0, "net_cluster": 0.0, "n_insiders": 0}

        buyers  = {r["insider_name"] for r in rows if r["classification"] in (SIGNAL_STRONG_BULLISH, SIGNAL_WEAK_BULLISH)}
        sellers = {r["insider_name"] for r in rows if r["classification"] == SIGNAL_BEARISH}
        all_insiders = buyers | sellers

        n = max(1, len(all_insiders))
        buy_cluster  = len(buyers)  / n
        sell_cluster = len(sellers) / n

        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT insider_name) FROM insider_transactions WHERE ticker=?",
                [ticker]
            )
            total_known = cur.fetchone()[0] or 1

        buy_cluster_adj = len(buyers) / max(1, total_known)

        return {
            "buy_cluster":          round(buy_cluster, 4),
            "sell_cluster":         round(sell_cluster, 4),
            "net_cluster":          round(buy_cluster - sell_cluster, 4),
            "adjusted_buy_cluster": round(buy_cluster_adj, 4),
            "n_buyers":             len(buyers),
            "n_sellers":            len(sellers),
            "n_insiders":           len(all_insiders),
            "cluster_signal":       (
                "STRONG_CLUSTER_BUY"  if buy_cluster_adj >= CLUSTER_STRONG else
                "CLUSTER_BUY"         if buy_cluster_adj >= CLUSTER_MEANINGFUL else
                "CLUSTER_SELL"        if sell_cluster    >= CLUSTER_MEANINGFUL else
                "NO_CLUSTER"
            ),
        }

    # ------------------------------------------------------------------
    # Main Analysis Entry Point  (UPDATED — stores noise + score)
    # ------------------------------------------------------------------

    def analyse(
        self,
        ticker: str,
        transactions: List[dict],
        price_change_30d: float = 0.0,
        total_company_insiders: int = 5,
        comp_data: Optional[Dict[str, float]] = None,
    ) -> List[Dict]:
        """
        Analyse a batch of Form 4 transactions for a ticker.
        Stores classified transactions in DB, including noise flags and signal scores.
        Returns list of classified transaction dicts.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        results = []

        for txn in transactions:
            filing_date = txn.get("date") or txn.get("filing_date") or now_iso[:10]

            try:
                fd = datetime.strptime(filing_date[:10], "%Y-%m-%d")
                window_start = (fd - timedelta(days=30)).strftime("%Y-%m-%d")
                window_end   = (fd + timedelta(days=30)).strftime("%Y-%m-%d")
            except Exception:
                window_start = window_end = filing_date[:10]

            nearby_txns = [
                t for t in transactions
                if window_start <= (t.get("date") or t.get("filing_date") or "")[:10] <= window_end
                and t is not txn
            ]

            # Noise detection
            is_noise, noise_type = self._is_noise_transaction(txn, all_txns_today=transactions)

            insider_name = txn.get("owner_name") or txn.get("insider_name") or ""
            comp_annual  = None
            if comp_data:
                comp_annual = comp_data.get(insider_name) or comp_data.get("default")

            # Legacy classification
            classification, signal_type, strength = self.classify_transaction(
                txn,
                ticker_price_change_30d=price_change_30d,
                other_txns_30d=nearby_txns,
                total_insiders=total_company_insiders,
                comp_annual_usd=comp_annual,
            )

            # New signal score (only for open market buys that are not noise)
            insider_signal_score = None
            rs = ss = pcs = ccs = trs = ts = 0
            first_purchase_flag = 0

            code = (txn.get("transaction_code") or "").upper()
            if code == "P" and not is_noise:
                txn_enriched = dict(txn)
                txn_enriched["insider_name"]  = insider_name
                txn_enriched["insider_title"] = txn.get("owner_title") or txn.get("relationship") or ""
                txn_enriched["filing_date"]   = filing_date[:10]

                rs  = self._role_score(txn_enriched.get("insider_title", ""))
                ss  = self._size_score(float(txn.get("value_usd") or txn.get("value") or 0), comp_annual)
                pcs = self._price_context_score(ticker, filing_date[:10])
                ccs = self._cluster_context_score(ticker, filing_date[:10], nearby_txns)
                trs = self._track_record_score(ticker, insider_name)
                ts  = self._timing_score(ticker, filing_date[:10])
                fp_bool = self._is_first_purchase(ticker, insider_name, filing_date[:10])
                first_purchase_flag = 1 if fp_bool else 0
                fp_bonus = 10 if fp_bool else 0
                insider_signal_score = max(0, min(100, 50 + rs + ss + pcs + ccs + trs + ts + fp_bonus))

            # Cluster score at time of filing
            cluster = self.get_cluster_score(ticker, window_days=30)

            record = {
                "ticker":                 ticker,
                "filing_date":            filing_date[:10] if len(filing_date) >= 10 else now_iso[:10],
                "transaction_date":       txn.get("date") or filing_date[:10],
                "insider_name":           insider_name,
                "insider_title":          txn.get("owner_title") or txn.get("relationship") or "",
                "insider_role":           txn.get("role") or "",
                "transaction_code":       txn.get("transaction_code") or "",
                "shares":                 float(txn.get("shares") or 0),
                "price_per_share":        float(txn.get("price_per_share") or txn.get("cost") or 0),
                "value_usd":              float(txn.get("value_usd") or txn.get("value") or 0),
                "shares_total":           float(txn.get("shares_total") or 0),
                "is_10b51":               1 if txn.get("is_10b51") else 0,
                "classification":         classification,
                "signal_type":            signal_type,
                "signal_strength":        strength,
                "cluster_score":          cluster.get("adjusted_buy_cluster", 0.0),
                "comp_ratio":             None,
                "stored_at":              now_iso,
                "is_noise":               1 if is_noise else 0,
                "noise_type":             noise_type or None,
                "insider_signal_score":   insider_signal_score,
                "role_score":             rs if insider_signal_score is not None else None,
                "size_score":             ss if insider_signal_score is not None else None,
                "price_context_score":    pcs if insider_signal_score is not None else None,
                "cluster_context_score":  ccs if insider_signal_score is not None else None,
                "track_record_score":     trs if insider_signal_score is not None else None,
                "timing_score":           ts if insider_signal_score is not None else None,
                "first_purchase_flag":    first_purchase_flag,
                "price_30d_change":       price_change_30d if price_change_30d != 0.0 else None,
                "price_52w_position":     None,
            }

            try:
                with self._cursor() as cur:
                    cols   = list(record.keys())
                    ph     = ", ".join("?" * len(cols))
                    col_str = ", ".join(cols)
                    cur.execute(
                        f"INSERT OR IGNORE INTO insider_transactions ({col_str}) VALUES ({ph})",
                        list(record.values())
                    )
            except Exception as e:
                logger.debug("insider store failed: %s", e)

            results.append({
                **record,
                "cluster_analysis": cluster,
            })

        return results

    # ------------------------------------------------------------------
    # Summarise  (UNCHANGED — backward compat)
    # ------------------------------------------------------------------

    def summarise(self, ticker: str, days: int = 90) -> Dict:
        """Summarise insider activity for a ticker over the last N days."""
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute(
                """SELECT classification, signal_type, COUNT(*) as n,
                          SUM(value_usd) as total_value, AVG(signal_strength) as avg_strength
                   FROM insider_transactions
                   WHERE ticker=? AND filing_date >= ?
                   GROUP BY classification, signal_type
                   ORDER BY total_value DESC""",
                [ticker, cutoff]
            )
            rows = [dict(r) for r in cur.fetchall()]

        cluster = self.get_cluster_score(ticker, window_days=days)

        total_buy_value  = sum(r["total_value"] or 0 for r in rows if r["classification"] in (SIGNAL_STRONG_BULLISH, SIGNAL_WEAK_BULLISH))
        total_sell_value = sum(r["total_value"] or 0 for r in rows if r["classification"] == SIGNAL_BEARISH)

        net = 0.0
        if total_buy_value + total_sell_value > 0:
            net = (total_buy_value - total_sell_value) / (total_buy_value + total_sell_value)

        strong_bull = any(r["classification"] == SIGNAL_STRONG_BULLISH for r in rows)

        return {
            "ticker":             ticker,
            "period_days":        days,
            "total_buy_value":    total_buy_value,
            "total_sell_value":   total_sell_value,
            "net_insider_signal": round(net, 4),
            "has_strong_bullish": strong_bull,
            "cluster_analysis":   cluster,
            "breakdown":          rows,
            "overall_bias":       (
                "STRONG_BULLISH" if (net > 0.5 and strong_bull) else
                "BULLISH"        if net > 0.2 else
                "BEARISH"        if net < -0.2 else
                "NEUTRAL"
            ),
        }

    # ------------------------------------------------------------------
    # Track Record  (UNCHANGED — backward compat)
    # ------------------------------------------------------------------

    def update_track_record(
        self, ticker: str, insider_name: str, return_60d: float
    ) -> None:
        """Update historical accuracy for a specific insider's trades."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM insider_track_records WHERE ticker=? AND insider_name=?",
                [ticker, insider_name]
            )
            existing = cur.fetchone()
            if existing:
                n_buys    = (existing["n_buys"] or 0) + 1
                was_correct = 1 if return_60d > 0 else 0
                prev_acc  = existing["buy_accuracy"] or 0.5
                new_acc   = (prev_acc * (n_buys - 1) + was_correct) / n_buys
                avg_ret   = ((existing["avg_return_60d"] or 0) * (n_buys - 1) + return_60d) / n_buys
                cur.execute(
                    """UPDATE insider_track_records
                       SET n_buys=?, buy_accuracy=?, avg_return_60d=?, last_updated=?
                       WHERE ticker=? AND insider_name=?""",
                    [n_buys, new_acc, avg_ret, now, ticker, insider_name]
                )
            else:
                was_correct = 1 if return_60d > 0 else 0
                cur.execute(
                    """INSERT INTO insider_track_records
                       (ticker, insider_name, n_buys, buy_accuracy, avg_return_60d, last_updated)
                       VALUES (?,?,1,?,?,?)""",
                    [ticker, insider_name, float(was_correct), return_60d, now]
                )
