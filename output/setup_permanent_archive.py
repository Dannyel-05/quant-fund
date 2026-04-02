"""
STEP 2 — Permanent Archive DB Setup
====================================
Creates output/permanent_archive.db with all required tables, FTS5 indexes,
triggers, and WAL mode. Also exposes the PermanentArchive class for inserting
and querying data throughout the pipeline.

Run directly to initialise (or re-initialise) the database:
    python output/setup_permanent_archive.py
"""

import sqlite3
import os
import datetime
import json
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(_HERE, "permanent_archive.db")


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DDL_TABLES = """
-- ── Raw content tables ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_articles (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    url                     TEXT,
    fetch_date              TEXT,
    source                  TEXT,
    ticker_context          TEXT,
    full_text               TEXT,
    word_count              INTEGER,
    title                   TEXT,
    author                  TEXT,
    publication_date        TEXT,
    is_paywalled            INTEGER,
    fetch_method            TEXT,
    all_tickers_mentioned   TEXT,
    all_companies_mentioned TEXT,
    sentiment_score         REAL,
    article_type            TEXT
);

CREATE TABLE IF NOT EXISTS raw_filings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT,
    filing_type       TEXT,
    filing_date       TEXT,
    accession_number  TEXT,
    full_text         TEXT,
    url               TEXT,
    items_reported    TEXT,
    classified_as     TEXT
);

CREATE TABLE IF NOT EXISTS raw_social_posts (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    platform              TEXT,
    post_id               TEXT,
    author                TEXT,
    date                  TEXT,
    full_text             TEXT,
    ticker_context        TEXT,
    upvotes               INTEGER,
    all_tickers_mentioned TEXT,
    sentiment             REAL
);

CREATE TABLE IF NOT EXISTS raw_macro_data (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    series_name TEXT,
    series_id   TEXT,
    date        TEXT,
    value       REAL,
    source      TEXT,
    fetched_at  TEXT
);

CREATE TABLE IF NOT EXISTS raw_shipping_data (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name TEXT,
    date       TEXT,
    value      REAL,
    source     TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS raw_weather_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    location        TEXT,
    date            TEXT,
    temperature_max REAL,
    temperature_min REAL,
    precipitation   REAL,
    windspeed       REAL,
    weathercode     INTEGER,
    snowfall        REAL,
    pollen_total    REAL,
    source          TEXT,
    fetched_at      TEXT
);

CREATE TABLE IF NOT EXISTS raw_geopolitical_events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date           TEXT,
    event_type           TEXT,
    description          TEXT,
    location             TEXT,
    severity             TEXT,
    affected_sectors     TEXT,
    affected_regions     TEXT,
    source               TEXT,
    goldstein_scale      REAL,
    expected_direction   TEXT,
    actual_direction     TEXT,
    prediction_was_correct INTEGER,
    fetched_at           TEXT
);

CREATE TABLE IF NOT EXISTS raw_commodity_prices (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity  TEXT,
    symbol     TEXT,
    date       TEXT,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    source     TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS raw_insider_transactions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker           TEXT,
    insider_name     TEXT,
    title            TEXT,
    transaction_date TEXT,
    transaction_type TEXT,
    shares           REAL,
    price            REAL,
    value            REAL,
    form4_text       TEXT,
    classification   TEXT,
    signal_score     REAL,
    fetched_at       TEXT
);

CREATE TABLE IF NOT EXISTS raw_congressional_trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    member_name             TEXT,
    ticker                  TEXT,
    transaction_date        TEXT,
    transaction_type        TEXT,
    amount_range            TEXT,
    disclosure_date         TEXT,
    committee_memberships   TEXT,
    member_credibility_score REAL,
    fetched_at              TEXT
);

CREATE TABLE IF NOT EXISTS predictions_log (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_date             TEXT,
    prediction_type             TEXT,
    ticker_or_sector            TEXT,
    predicted_direction         TEXT,
    predicted_magnitude         REAL,
    confidence                  REAL,
    actual_direction            TEXT,
    actual_magnitude            REAL,
    was_correct                 INTEGER,
    data_that_led_to_prediction TEXT,
    outcome_date                TEXT
);
"""

DDL_FTS = """
-- ── FTS5 virtual tables ──────────────────────────────────────────────────────

CREATE VIRTUAL TABLE IF NOT EXISTS raw_articles_fts
    USING fts5(
        title,
        full_text,
        all_tickers_mentioned,
        all_companies_mentioned,
        content='raw_articles',
        content_rowid='id'
    );

CREATE VIRTUAL TABLE IF NOT EXISTS raw_filings_fts
    USING fts5(
        full_text,
        ticker,
        content='raw_filings',
        content_rowid='id'
    );

CREATE VIRTUAL TABLE IF NOT EXISTS raw_social_posts_fts
    USING fts5(
        full_text,
        all_tickers_mentioned,
        content='raw_social_posts',
        content_rowid='id'
    );

CREATE VIRTUAL TABLE IF NOT EXISTS raw_geopolitical_events_fts
    USING fts5(
        description,
        location,
        affected_sectors,
        content='raw_geopolitical_events',
        content_rowid='id'
    );
"""

DDL_INDEXES = """
-- ── Performance indexes ──────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_articles_fetch_date    ON raw_articles(fetch_date);
CREATE INDEX IF NOT EXISTS idx_articles_source        ON raw_articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_ticker        ON raw_articles(ticker_context);
CREATE INDEX IF NOT EXISTS idx_articles_pub_date      ON raw_articles(publication_date);

CREATE INDEX IF NOT EXISTS idx_filings_ticker         ON raw_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_filings_date           ON raw_filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_filings_type           ON raw_filings(filing_type);

CREATE INDEX IF NOT EXISTS idx_social_platform        ON raw_social_posts(platform);
CREATE INDEX IF NOT EXISTS idx_social_date            ON raw_social_posts(date);
CREATE INDEX IF NOT EXISTS idx_social_ticker          ON raw_social_posts(ticker_context);

CREATE INDEX IF NOT EXISTS idx_macro_series_id        ON raw_macro_data(series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date             ON raw_macro_data(date);
CREATE INDEX IF NOT EXISTS idx_macro_source           ON raw_macro_data(source);

CREATE INDEX IF NOT EXISTS idx_shipping_index         ON raw_shipping_data(index_name);
CREATE INDEX IF NOT EXISTS idx_shipping_date          ON raw_shipping_data(date);
CREATE INDEX IF NOT EXISTS idx_shipping_source        ON raw_shipping_data(source);

CREATE INDEX IF NOT EXISTS idx_weather_location       ON raw_weather_data(location);
CREATE INDEX IF NOT EXISTS idx_weather_date           ON raw_weather_data(date);

CREATE INDEX IF NOT EXISTS idx_geo_date               ON raw_geopolitical_events(event_date);
CREATE INDEX IF NOT EXISTS idx_geo_type               ON raw_geopolitical_events(event_type);
CREATE INDEX IF NOT EXISTS idx_geo_severity           ON raw_geopolitical_events(severity);

CREATE INDEX IF NOT EXISTS idx_commodity_symbol       ON raw_commodity_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_commodity_date         ON raw_commodity_prices(date);
CREATE INDEX IF NOT EXISTS idx_commodity_source       ON raw_commodity_prices(source);

CREATE INDEX IF NOT EXISTS idx_insider_ticker         ON raw_insider_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_insider_date           ON raw_insider_transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_congress_ticker        ON raw_congressional_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_congress_date          ON raw_congressional_trades(transaction_date);

CREATE INDEX IF NOT EXISTS idx_pred_date              ON predictions_log(prediction_date);
CREATE INDEX IF NOT EXISTS idx_pred_type              ON predictions_log(prediction_type);
CREATE INDEX IF NOT EXISTS idx_pred_ticker            ON predictions_log(ticker_or_sector);
"""

DDL_TRIGGERS = """
-- ── FTS auto-sync triggers ────────────────────────────────────────────────────

-- raw_articles → raw_articles_fts
CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON raw_articles BEGIN
    INSERT INTO raw_articles_fts(rowid, title, full_text, all_tickers_mentioned, all_companies_mentioned)
    VALUES (new.id, new.title, new.full_text, new.all_tickers_mentioned, new.all_companies_mentioned);
END;

CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON raw_articles BEGIN
    INSERT INTO raw_articles_fts(raw_articles_fts, rowid, title, full_text, all_tickers_mentioned, all_companies_mentioned)
    VALUES ('delete', old.id, old.title, old.full_text, old.all_tickers_mentioned, old.all_companies_mentioned);
END;

CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON raw_articles BEGIN
    INSERT INTO raw_articles_fts(raw_articles_fts, rowid, title, full_text, all_tickers_mentioned, all_companies_mentioned)
    VALUES ('delete', old.id, old.title, old.full_text, old.all_tickers_mentioned, old.all_companies_mentioned);
    INSERT INTO raw_articles_fts(rowid, title, full_text, all_tickers_mentioned, all_companies_mentioned)
    VALUES (new.id, new.title, new.full_text, new.all_tickers_mentioned, new.all_companies_mentioned);
END;

-- raw_filings → raw_filings_fts
CREATE TRIGGER IF NOT EXISTS filings_ai AFTER INSERT ON raw_filings BEGIN
    INSERT INTO raw_filings_fts(rowid, full_text, ticker)
    VALUES (new.id, new.full_text, new.ticker);
END;

CREATE TRIGGER IF NOT EXISTS filings_ad AFTER DELETE ON raw_filings BEGIN
    INSERT INTO raw_filings_fts(raw_filings_fts, rowid, full_text, ticker)
    VALUES ('delete', old.id, old.full_text, old.ticker);
END;

CREATE TRIGGER IF NOT EXISTS filings_au AFTER UPDATE ON raw_filings BEGIN
    INSERT INTO raw_filings_fts(raw_filings_fts, rowid, full_text, ticker)
    VALUES ('delete', old.id, old.full_text, old.ticker);
    INSERT INTO raw_filings_fts(rowid, full_text, ticker)
    VALUES (new.id, new.full_text, new.ticker);
END;

-- raw_social_posts → raw_social_posts_fts
CREATE TRIGGER IF NOT EXISTS social_ai AFTER INSERT ON raw_social_posts BEGIN
    INSERT INTO raw_social_posts_fts(rowid, full_text, all_tickers_mentioned)
    VALUES (new.id, new.full_text, new.all_tickers_mentioned);
END;

CREATE TRIGGER IF NOT EXISTS social_ad AFTER DELETE ON raw_social_posts BEGIN
    INSERT INTO raw_social_posts_fts(raw_social_posts_fts, rowid, full_text, all_tickers_mentioned)
    VALUES ('delete', old.id, old.full_text, old.all_tickers_mentioned);
END;

CREATE TRIGGER IF NOT EXISTS social_au AFTER UPDATE ON raw_social_posts BEGIN
    INSERT INTO raw_social_posts_fts(raw_social_posts_fts, rowid, full_text, all_tickers_mentioned)
    VALUES ('delete', old.id, old.full_text, old.all_tickers_mentioned);
    INSERT INTO raw_social_posts_fts(rowid, full_text, all_tickers_mentioned)
    VALUES (new.id, new.full_text, new.all_tickers_mentioned);
END;

-- raw_geopolitical_events → raw_geopolitical_events_fts
CREATE TRIGGER IF NOT EXISTS geo_ai AFTER INSERT ON raw_geopolitical_events BEGIN
    INSERT INTO raw_geopolitical_events_fts(rowid, description, location, affected_sectors)
    VALUES (new.id, new.description, new.location, new.affected_sectors);
END;

CREATE TRIGGER IF NOT EXISTS geo_ad AFTER DELETE ON raw_geopolitical_events BEGIN
    INSERT INTO raw_geopolitical_events_fts(raw_geopolitical_events_fts, rowid, description, location, affected_sectors)
    VALUES ('delete', old.id, old.description, old.location, old.affected_sectors);
END;

CREATE TRIGGER IF NOT EXISTS geo_au AFTER UPDATE ON raw_geopolitical_events BEGIN
    INSERT INTO raw_geopolitical_events_fts(raw_geopolitical_events_fts, rowid, description, location, affected_sectors)
    VALUES ('delete', old.id, old.description, old.location, old.affected_sectors);
    INSERT INTO raw_geopolitical_events_fts(rowid, description, location, affected_sectors)
    VALUES (new.id, new.description, new.location, new.affected_sectors);
END;
"""


# ---------------------------------------------------------------------------
# Database initialisation function
# ---------------------------------------------------------------------------

def setup_database(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Create (or open) the permanent archive database, apply all DDL, and return
    an open connection.  Safe to call repeatedly — all statements use
    IF NOT EXISTS.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)

    # PRAGMAs must be run before executescript (which issues an implicit COMMIT)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-32000")   # 32 MB page cache
    conn.commit()

    # executescript handles multi-statement SQL including BEGIN…END trigger bodies
    full_ddl = "\n".join([DDL_TABLES, DDL_FTS, DDL_INDEXES, DDL_TRIGGERS])
    conn.executescript(full_ddl)

    # Re-assert WAL after executescript (it issues a COMMIT internally)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn


def _split_statements(sql: str) -> List[str]:
    """
    Split a multi-statement SQL string into individual executable statements.

    Handles:
    - Single-line and block comments (skipped)
    - Regular statements ending with ';'
    - CREATE TRIGGER blocks (BEGIN…END;) which contain internal semicolons
    """
    results = []
    current: List[str] = []
    in_trigger = False   # True while inside a BEGIN…END trigger block
    begin_depth = 0      # nesting depth for BEGIN/END

    for line in sql.splitlines():
        stripped = line.strip()

        # Skip pure comment lines and blank lines when not building a statement
        if not current and (stripped.startswith("--") or stripped == ""):
            continue

        current.append(line)
        upper = stripped.upper()

        # Track BEGIN/END nesting for trigger bodies
        if upper.startswith("BEGIN"):
            in_trigger = True
            begin_depth += 1
        if upper == "END;" or upper.startswith("END;"):
            begin_depth = max(begin_depth - 1, 0)
            if begin_depth == 0:
                in_trigger = False

        # A statement is complete when we hit ';' and we are not inside a trigger
        if stripped.endswith(";") and not in_trigger:
            stmt = "\n".join(current).strip()
            # Strip trailing inline comment before appending
            if stmt:
                results.append(stmt)
            current = []
            begin_depth = 0

    # Catch any unterminated statement (shouldn't happen with well-formed DDL)
    if current:
        stmt = "\n".join(current).strip()
        if stmt:
            results.append(stmt)

    return results


# ---------------------------------------------------------------------------
# PermanentArchive class
# ---------------------------------------------------------------------------

class PermanentArchive:
    """
    High-level interface to the permanent_archive.db SQLite database.

    Usage
    -----
    archive = PermanentArchive()                         # default path
    archive = PermanentArchive("/path/to/archive.db")   # custom path
    archive.insert_article(...)
    results = archive.search("Baltic Dry retail")
    stats   = archive.get_stats()
    """

    # Tables that have a 'fetched_at' column — auto-populated on insert
    _TIMESTAMPED_TABLES = {
        "raw_macro_data",
        "raw_shipping_data",
        "raw_weather_data",
        "raw_geopolitical_events",
        "raw_commodity_prices",
        "raw_insider_transactions",
        "raw_congressional_trades",
    }

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ── Connection management ────────────────────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = setup_database(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _now(self) -> str:
        return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    def _execute(self, sql: str, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    # ── Insert methods ───────────────────────────────────────────────────────

    def insert_article(
        self,
        url: str,
        fetch_date: str,
        source: str,
        ticker_context: str,
        full_text: str,
        title: str = "",
        author: str = "",
        publication_date: str = "",
        all_tickers_mentioned: str = "",
        sentiment_score: Optional[float] = None,
        article_type: str = "",
        *,
        word_count: Optional[int] = None,
        is_paywalled: int = 0,
        fetch_method: str = "requests",
        all_companies_mentioned: str = "",
    ) -> int:
        """Insert a news article into raw_articles (FTS updated via trigger)."""
        wc = word_count if word_count is not None else len(full_text.split())
        cur = self._execute(
            """
            INSERT INTO raw_articles
                (url, fetch_date, source, ticker_context, full_text, word_count,
                 title, author, publication_date, is_paywalled, fetch_method,
                 all_tickers_mentioned, all_companies_mentioned,
                 sentiment_score, article_type)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                url, fetch_date, source, ticker_context, full_text, wc,
                title, author, publication_date, is_paywalled, fetch_method,
                all_tickers_mentioned, all_companies_mentioned,
                sentiment_score, article_type,
            ),
        )
        return cur.lastrowid

    def insert_macro_data(
        self,
        series_name: str,
        series_id: str,
        date: str,
        value: float,
        source: str,
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO raw_macro_data
                (series_name, series_id, date, value, source, fetched_at)
            VALUES (?,?,?,?,?,?)
            """,
            (series_name, series_id, date, value, source, self._now()),
        )
        return cur.lastrowid

    def insert_shipping(
        self,
        index_name: str,
        date: str,
        value: float,
        source: str,
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO raw_shipping_data
                (index_name, date, value, source, fetched_at)
            VALUES (?,?,?,?,?)
            """,
            (index_name, date, value, source, self._now()),
        )
        return cur.lastrowid

    def insert_weather(
        self,
        location: str,
        date: str,
        temperature_max: float,
        temperature_min: float,
        precipitation: float,
        windspeed: float,
        weathercode: int,
        snowfall: float,
        pollen_total: float,
        source: str,
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO raw_weather_data
                (location, date, temperature_max, temperature_min,
                 precipitation, windspeed, weathercode, snowfall,
                 pollen_total, source, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                location, date, temperature_max, temperature_min,
                precipitation, windspeed, weathercode, snowfall,
                pollen_total, source, self._now(),
            ),
        )
        return cur.lastrowid

    def insert_geopolitical(
        self,
        event_date: str,
        event_type: str,
        description: str,
        location: str,
        severity: str,
        affected_sectors: str,
        affected_regions: str,
        source: str,
        goldstein_scale: float = 0.0,
        *,
        expected_direction: str = "",
        actual_direction: str = "",
        prediction_was_correct: Optional[int] = None,
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO raw_geopolitical_events
                (event_date, event_type, description, location, severity,
                 affected_sectors, affected_regions, source, goldstein_scale,
                 expected_direction, actual_direction, prediction_was_correct,
                 fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_date, event_type, description, location, severity,
                affected_sectors, affected_regions, source, goldstein_scale,
                expected_direction, actual_direction, prediction_was_correct,
                self._now(),
            ),
        )
        return cur.lastrowid

    def insert_commodity(
        self,
        commodity: str,
        symbol: str,
        date: str,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        source: str,
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO raw_commodity_prices
                (commodity, symbol, date, open, high, low, close, volume,
                 source, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (commodity, symbol, date, open, high, low, close, volume, source, self._now()),
        )
        return cur.lastrowid

    def insert_prediction(
        self,
        prediction_date: str,
        prediction_type: str,
        ticker_or_sector: str,
        predicted_direction: str,
        predicted_magnitude: float,
        confidence: float,
        outcome_date: str,
        *,
        actual_direction: str = "",
        actual_magnitude: Optional[float] = None,
        was_correct: Optional[int] = None,
        data_that_led_to_prediction: str = "",
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO predictions_log
                (prediction_date, prediction_type, ticker_or_sector,
                 predicted_direction, predicted_magnitude, confidence,
                 actual_direction, actual_magnitude, was_correct,
                 data_that_led_to_prediction, outcome_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                prediction_date, prediction_type, ticker_or_sector,
                predicted_direction, predicted_magnitude, confidence,
                actual_direction, actual_magnitude, was_correct,
                data_that_led_to_prediction, outcome_date,
            ),
        )
        return cur.lastrowid

    # ── Search ───────────────────────────────────────────────────────────────

    def search(self, query: str) -> Dict[str, List[Dict]]:
        """
        Full-text search across all FTS tables.  Returns a dict keyed by
        table name with a list of matching row dicts (up to 20 per table).

        Example
        -------
        results = archive.search("Baltic Dry wheat supply chain")
        for table, rows in results.items():
            print(table, len(rows))
        """
        results: Dict[str, List[Dict]] = {}
        fts_queries = {
            "raw_articles": (
                "raw_articles_fts",
                "SELECT a.* FROM raw_articles a "
                "JOIN raw_articles_fts f ON a.id = f.rowid "
                "WHERE raw_articles_fts MATCH ? LIMIT 20",
            ),
            "raw_filings": (
                "raw_filings_fts",
                "SELECT f.* FROM raw_filings f "
                "JOIN raw_filings_fts fts ON f.id = fts.rowid "
                "WHERE raw_filings_fts MATCH ? LIMIT 20",
            ),
            "raw_social_posts": (
                "raw_social_posts_fts",
                "SELECT s.* FROM raw_social_posts s "
                "JOIN raw_social_posts_fts fts ON s.id = fts.rowid "
                "WHERE raw_social_posts_fts MATCH ? LIMIT 20",
            ),
            "raw_geopolitical_events": (
                "raw_geopolitical_events_fts",
                "SELECT g.* FROM raw_geopolitical_events g "
                "JOIN raw_geopolitical_events_fts fts ON g.id = fts.rowid "
                "WHERE raw_geopolitical_events_fts MATCH ? LIMIT 20",
            ),
        }
        for table_name, (_, sql) in fts_queries.items():
            try:
                rows = self.conn.execute(sql, (query,)).fetchall()
                results[table_name] = [dict(r) for r in rows]
            except sqlite3.OperationalError as exc:
                logger.warning("FTS search failed on %s: %s", table_name, exc)
                results[table_name] = []
        return results

    # ── Stats ────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, int]:
        """Return row counts for each content table."""
        tables = [
            "raw_articles",
            "raw_filings",
            "raw_social_posts",
            "raw_macro_data",
            "raw_shipping_data",
            "raw_weather_data",
            "raw_geopolitical_events",
            "raw_commodity_prices",
            "raw_insider_transactions",
            "raw_congressional_trades",
            "predictions_log",
        ]
        stats: Dict[str, int] = {}
        for t in tables:
            try:
                (count,) = self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                stats[t] = count
            except sqlite3.OperationalError as exc:
                logger.warning("Could not count %s: %s", t, exc)
                stats[t] = -1
        return stats


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Initialise the permanent archive database.")
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to database file (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(f"\nSetting up permanent archive database at: {args.db}\n")

    conn = setup_database(args.db)

    # Report what was created
    tables = conn.execute(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table','index','trigger') "
        "ORDER BY type, name"
    ).fetchall()

    by_type: Dict[str, List[str]] = {}
    for name, kind in tables:
        by_type.setdefault(kind, []).append(name)

    for kind in ("table", "index", "trigger"):
        items = by_type.get(kind, [])
        print(f"  {kind.upper()}S ({len(items)}):")
        for item in items:
            print(f"    - {item}")
        print()

    # Verify WAL mode
    (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
    print(f"  Journal mode : {journal_mode.upper()}")

    # Quick stats via PermanentArchive
    archive = PermanentArchive(args.db)
    stats = archive.get_stats()
    print("\n  Row counts (all zero on fresh DB):")
    for table, count in stats.items():
        print(f"    {table:<35} {count:>8}")

    conn.close()
    print("\nDone. Database is ready.\n")


if __name__ == "__main__":
    main()
