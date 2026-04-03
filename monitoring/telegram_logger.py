"""
Permanent storage for every Telegram message ever sent.

Writes to output/telegram_history/telegram_log_YYYY-MM.json (JSONL format).
Files are NEVER deleted automatically.
Thread-safe for concurrent writes.
"""
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional

_DIR  = Path("output/telegram_history")
_lock = threading.Lock()

# In-memory retry queue: list of (msg_type, message, first_fail_ts)
_retry_queue: list = []
_retry_lock  = threading.Lock()


def log_message(msg_type: str, message: str, delivered: bool) -> None:
    """Append one entry to the monthly JSONL file (thread-safe, non-blocking)."""
    try:
        _DIR.mkdir(parents=True, exist_ok=True)
        month = datetime.now().strftime("%Y-%m")
        path  = _DIR / f"telegram_log_{month}.json"
        entry = {
            "timestamp": datetime.now().isoformat(),
            "type":      msg_type,
            "message":   message,
            "delivered": delivered,
        }
        with _lock:
            with open(path, "a") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # never crash the caller


def get_recent(n: int = 10) -> List[dict]:
    """Return the last *n* messages across monthly files, newest first."""
    _DIR.mkdir(parents=True, exist_ok=True)
    entries: List[dict] = []
    for path in sorted(_DIR.glob("telegram_log_*.json"), reverse=True):
        try:
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            for line in reversed(lines):
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
                if len(entries) >= n:
                    return entries
        except Exception:
            continue
    return entries


def queue_retry(msg_type: str, message: str) -> None:
    """Add a failed message to the retry queue."""
    with _retry_lock:
        _retry_queue.append({
            "type":    msg_type,
            "message": message,
            "queued":  datetime.now().isoformat(),
        })


def pop_retry_queue() -> list:
    """Drain and return all pending retry messages."""
    with _retry_lock:
        items = list(_retry_queue)
        _retry_queue.clear()
    return items
