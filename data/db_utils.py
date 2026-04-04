"""
Shared SQLite connection utility.

Every database connection in this codebase should go through
`open_db()` (or the monkey-patch applied in main.py) so that
WAL mode and busy_timeout are ALWAYS set.

WAL mode  — multiple readers + 1 writer without blocking each other.
            Survives restarts; the journal file is reused automatically.
busy_timeout — SQLite waits up to N ms before raising "database is locked"
               instead of failing immediately.  30 s is safe for the
               longest expected write transaction.
"""
import sqlite3
import logging

logger = logging.getLogger(__name__)

_WAL_TIMEOUT_MS = 30_000  # 30 seconds


def open_db(path, **kwargs) -> sqlite3.Connection:
    """
    Open a SQLite connection with WAL journal mode and 30 s busy timeout.
    Drop-in replacement for sqlite3.connect().
    """
    conn = sqlite3.connect(str(path), **kwargs)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={_WAL_TIMEOUT_MS}")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-32000")    # 32 MB page cache
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception as exc:                        # noqa: BLE001
        logger.warning("db_utils.open_db pragma error for %s: %s", path, exc)
    return conn


def patch_sqlite3() -> None:
    """
    Monkey-patch sqlite3.connect so that every call anywhere in the process
    automatically gets WAL mode + busy_timeout.  Call once at process startup
    (before any imports that open databases).
    """
    _original_connect = sqlite3.connect

    def _patched_connect(database, *args, **kwargs):
        conn = _original_connect(database, *args, **kwargs)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA busy_timeout={_WAL_TIMEOUT_MS}")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-32000")
            conn.execute("PRAGMA foreign_keys=ON")
        except Exception:   # noqa: BLE001
            pass            # :memory: DBs ignore WAL silently
        return conn

    sqlite3.connect = _patched_connect
    logger.info("db_utils: sqlite3.connect patched — WAL + 30 s timeout on all connections")
