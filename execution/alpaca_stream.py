"""
Alpaca real-time websocket price stream.

Runs in a background daemon thread with its own asyncio event loop.
Never blocks the main trading loop, data collectors, scanner, or any other system.

Usage:
    from execution.alpaca_stream import start_stream, get_stream_cache

    # At bot startup:
    worker = start_stream(config, list_of_tickers)

    # Anywhere in the bot:
    cache = get_stream_cache()
    price = cache.get_price("AAPL")         # real-time or None
    move  = cache.get_move_pct("AAPL", 5)  # % move over last 5 minutes
"""

import asyncio
import json
import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger("alpaca_stream")

# ── Alpaca IEX free-tier websocket endpoint ────────────────────────────────
_STREAM_URL      = "wss://stream.data.alpaca.markets/v2/iex"
_RECONNECT_DELAY = 30        # seconds between reconnect attempts
_MAX_RECONNECTS  = 99999     # effectively unlimited restarts
_CACHE_THRESHOLD = 0.005     # 0.5% min move to update cache
_SPIKE_PCT       = 2.0       # % in 5 min  → SPIKE
_URGENT_PCT      = 5.0       # % in 10 min → URGENT + Telegram
_SPIKE_WINDOW    = timedelta(minutes=5)
_URGENT_WINDOW   = timedelta(minutes=10)
_HISTORY_LEN     = 30        # price history points per ticker
_CACHE_STALE_SEC = 300       # cached price older than 5 min treated as stale

# Module-level singleton references
_cache: Optional["PriceCache"] = None
_stream_worker: Optional["AlpacaStreamWorker"] = None


# ── Thread-safe price cache ────────────────────────────────────────────────

class PriceCache:
    """
    Thread-safe in-memory real-time price cache.

    Designed to use < 100 MB for up to 10,000 tickers with 30-point history.
    Rough overhead: 10,000 × (200 bytes data + 30 × 32 bytes history) ≈ 11 MB.
    """

    def __init__(self):
        self._lock         = threading.RLock()
        self._data:    Dict[str, dict]           = {}
        self._history: Dict[str, deque]          = defaultdict(lambda: deque(maxlen=_HISTORY_LEN))
        self._spike_flags: Dict[str, str]        = {}
        self._connected    = False
        self._total_updates = 0
        self._last_update: Optional[datetime]    = None

    # ── writes ─────────────────────────────────────────────────────────────

    def update(self, ticker: str, price: float, volume: float,
               ts: datetime) -> Optional[str]:
        """
        Store a new price point.
        Skips moves < 0.5% to keep update rate low.
        Returns "SPIKE" | "URGENT" | None.
        """
        with self._lock:
            prev_price = self._data.get(ticker, {}).get("price")
            pct = 0.0
            if prev_price and prev_price > 0:
                pct = (price - prev_price) / prev_price
                if abs(pct) < _CACHE_THRESHOLD:
                    return None
            pct_pct = pct * 100.0
            self._data[ticker] = {
                "price":         price,
                "volume":        volume,
                "timestamp":     ts,
                "pct_from_prev": pct_pct,
            }
            self._history[ticker].append((ts, price))
            self._total_updates += 1
            self._last_update    = ts
            alert = self._check_alert(ticker, price, ts)
            if alert:
                self._spike_flags[ticker] = alert
            return alert

    def _check_alert(self, ticker: str, now_price: float,
                     now: datetime) -> Optional[str]:
        history = list(self._history[ticker])
        if len(history) < 2:
            return None
        now_utc = _ensure_utc(now)
        for window, threshold, label in (
            (_SPIKE_WINDOW,  _SPIKE_PCT,  "SPIKE"),
            (_URGENT_WINDOW, _URGENT_PCT, "URGENT"),
        ):
            cutoff = now_utc - window
            for ts, px in history:
                if _ensure_utc(ts) >= cutoff and px > 0:
                    if abs((now_price - px) / px * 100) >= threshold:
                        return label
        return None

    def set_connected(self, v: bool) -> None:
        with self._lock:
            self._connected = v

    def clear_spike_flag(self, ticker: str) -> None:
        with self._lock:
            self._spike_flags.pop(ticker, None)

    # ── reads ───────────────────────────────────────────────────────────────

    def get_price(self, ticker: str) -> Optional[float]:
        """Return the latest cached price, or None if not available."""
        with self._lock:
            return self._data.get(ticker, {}).get("price")

    def get_fresh_price(self, ticker: str, max_age_sec: int = _CACHE_STALE_SEC) -> Optional[float]:
        """
        Return the cached price only if it is newer than *max_age_sec*.
        Returns None if the entry is missing or stale — caller should fall
        back to yfinance in that case.
        """
        with self._lock:
            entry = self._data.get(ticker)
            if not entry:
                return None
            ts = entry.get("timestamp")
            if ts is None:
                return None
            age = (datetime.now(timezone.utc) - _ensure_utc(ts)).total_seconds()
            if age > max_age_sec:
                return None
            return entry["price"]

    def get(self, ticker: str) -> Optional[dict]:
        with self._lock:
            d = self._data.get(ticker)
            return dict(d) if d else None

    def get_move_pct(self, ticker: str, window_minutes: int = 5) -> Optional[float]:
        """
        % price change over the last *window_minutes* from history.
        Returns None if insufficient history.
        """
        with self._lock:
            history = list(self._history.get(ticker, []))
        if not history:
            return None
        now     = datetime.now(timezone.utc)
        cutoff  = now - timedelta(minutes=window_minutes)
        baseline = None
        for ts, px in history:
            if _ensure_utc(ts) >= cutoff:
                baseline = px
                break
        if baseline is None or baseline == 0:
            return None
        current = history[-1][1]
        return (current - baseline) / baseline * 100.0

    def get_realtime_volatility(self, tickers: List[str],
                                window_minutes: int = 10) -> float:
        """
        Returns mean absolute % move across *tickers* over *window_minutes*.
        Useful for the frontier complexity index.
        """
        moves = []
        for t in tickers:
            m = self.get_move_pct(t, window_minutes)
            if m is not None:
                moves.append(abs(m))
        return float(sum(moves) / len(moves)) if moves else 0.0

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    def get_spike_flags(self) -> dict:
        with self._lock:
            return dict(self._spike_flags)

    def stats(self) -> dict:
        with self._lock:
            return {
                "connected":      self._connected,
                "tickers_cached": len(self._data),
                "spike_flags":    len(self._spike_flags),
                "total_updates":  self._total_updates,
                "last_update":    (self._last_update.isoformat()
                                   if self._last_update else None),
            }


def _ensure_utc(dt: datetime) -> datetime:
    """Attach UTC timezone if naive."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ── Singleton accessor ─────────────────────────────────────────────────────

def get_stream_cache() -> "PriceCache":
    """Return the module-level PriceCache singleton (always available)."""
    global _cache
    if _cache is None:
        _cache = PriceCache()
    return _cache


def get_stream_worker() -> Optional["AlpacaStreamWorker"]:
    """Return the running stream worker, or None if not started."""
    return _stream_worker


# ── Stream worker ──────────────────────────────────────────────────────────

class AlpacaStreamWorker:
    """
    Background daemon thread running an asyncio event loop with an
    aiohttp WebSocket connected to Alpaca's IEX stream.

    Auto-reconnects within RECONNECT_DELAY seconds.
    Falls back gracefully if aiohttp is missing (shouldn't happen — it's
    already in requirements.txt).
    """

    def __init__(self, api_key: str, secret_key: str,
                 universe: Set[str], config: dict):
        self._api_key    = api_key
        self._secret_key = secret_key
        self._universe   = universe
        self._config     = config
        self._stop       = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._notifier   = None
        self._reconnects = 0
        self._setup_log()
        self._init_notifier()
        get_stream_cache()  # ensure singleton exists

    # ── setup ───────────────────────────────────────────────────────────────

    def _setup_log(self) -> None:
        log_path = Path("logs/alpaca_stream.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if not logger.handlers:
            h = RotatingFileHandler(
                log_path, maxBytes=5 * 1024 * 1024, backupCount=3,
            )
            h.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)-8s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            logger.addHandler(h)
            logger.setLevel(logging.INFO)
            logger.propagate = False   # don't bleed into the main bot log

    def _init_notifier(self) -> None:
        try:
            from altdata.notifications.notifier import Notifier
            self._notifier = Notifier(self._config)
        except Exception as exc:
            logger.warning("Notifier unavailable (no URGENT Telegram alerts): %s", exc)

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="alpaca-stream", daemon=True,
        )
        self._thread.start()
        logger.info(
            "AlpacaStreamWorker started — universe: %d US tickers",
            len(self._universe),
        )

    def stop(self) -> None:
        self._stop.set()
        logger.info("AlpacaStreamWorker stop requested.")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def update_universe(self, tickers: List[str]) -> None:
        """Swap in a new ticker universe (thread-safe)."""
        self._universe = {t for t in tickers if not t.endswith(".L")}
        logger.info("Stream universe updated: %d tickers", len(self._universe))

    # ── thread entry ────────────────────────────────────────────────────────

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while not self._stop.is_set():
                try:
                    loop.run_until_complete(self._session())
                except Exception as exc:
                    logger.error("Stream session crashed: %s", exc)
                finally:
                    get_stream_cache().set_connected(False)

                if self._stop.is_set():
                    break
                self._reconnects += 1
                if self._reconnects > _MAX_RECONNECTS:
                    logger.error("Max reconnect attempts reached — stream disabled")
                    break
                logger.info(
                    "Reconnecting in %ds (attempt %d)…",
                    _RECONNECT_DELAY, self._reconnects,
                )
                self._stop.wait(timeout=_RECONNECT_DELAY)
        finally:
            loop.close()
            logger.info("AlpacaStreamWorker thread exited.")

    # ── websocket session ───────────────────────────────────────────────────

    async def _session(self) -> None:
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(
                _STREAM_URL,
                heartbeat=30,
                max_msg_size=4 * 1024 * 1024,
            ) as ws:
                # 1. Connected handshake
                raw = await ws.receive()
                _log_ws_event("connected handshake", raw.data)

                # 2. Authenticate
                await ws.send_json({
                    "action": "auth",
                    "key":    self._api_key,
                    "secret": self._secret_key,
                })
                raw = await ws.receive()
                msgs = _parse_ws(raw.data)
                if not any(m.get("msg") == "authenticated" for m in msgs):
                    logger.error("Alpaca auth failed: %s", msgs)
                    return   # triggers reconnect

                # 3. Subscribe to ALL bars (filter locally by universe)
                await ws.send_json({"action": "subscribe", "bars": ["*"]})
                raw = await ws.receive()
                logger.info("Subscribed. Response: %s", str(raw.data)[:200])

                get_stream_cache().set_connected(True)
                self._reconnects = 0
                logger.info(
                    "Stream LIVE — watching %d universe tickers",
                    len(self._universe),
                )

                # 4. Message loop
                async for raw_msg in ws:
                    if self._stop.is_set():
                        break
                    import aiohttp as _ah
                    if raw_msg.type in (_ah.WSMsgType.CLOSE, _ah.WSMsgType.ERROR):
                        logger.warning("WS closed/errored: %s", raw_msg.data)
                        break
                    if raw_msg.type not in (_ah.WSMsgType.TEXT, _ah.WSMsgType.BINARY):
                        continue
                    try:
                        for item in _parse_ws(raw_msg.data):
                            if item.get("T") == "b":
                                self._on_bar(item)
                    except Exception as exc:
                        logger.debug("Msg parse error: %s", exc)

    # ── bar handler ─────────────────────────────────────────────────────────

    def _on_bar(self, item: dict) -> None:
        ticker = item.get("S", "")
        if ticker not in self._universe:
            return  # not in our universe → discard

        try:
            price  = float(item.get("c") or item.get("vw") or 0)
            volume = float(item.get("v") or 0)
            ts_raw = item.get("t", "")
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)

            if price <= 0:
                return

            alert = get_stream_cache().update(ticker, price, volume, ts)

            if alert in ("SPIKE", "URGENT"):
                entry     = get_stream_cache().get(ticker) or {}
                pct       = entry.get("pct_from_prev", 0.0)
                direction = "UP" if pct >= 0 else "DOWN"
                if alert == "URGENT":
                    logger.warning(
                        "URGENT: %s %+.2f%% %s @ %.4f vol=%,.0f",
                        ticker, pct, direction, price, volume,
                    )
                    self._send_urgent_alert(ticker, pct, price, volume, direction, ts)
                    get_stream_cache().clear_spike_flag(ticker)
                else:
                    logger.info(
                        "SPIKE: %s %+.2f%% %s @ %.4f",
                        ticker, pct, direction, price,
                    )
        except Exception as exc:
            logger.debug("_on_bar %s: %s", item.get("S"), exc)

    def _send_urgent_alert(self, ticker: str, pct: float, price: float,
                            volume: float, direction: str,
                            ts: datetime) -> None:
        msg = (
            f"\U0001f6a8 URGENT ALERT: {ticker}\n"
            f"Move: {pct:+.2f}% {direction} in <10min\n"
            f"Price: ${price:.4f}\n"
            f"Volume: {volume:,.0f}\n"
            f"Time: {ts.strftime('%H:%M:%S UTC')}"
        )
        logger.warning("URGENT_TELEGRAM: %s", msg.replace("\n", " | "))
        if self._notifier:
            try:
                self._notifier.send(
                    trigger="alpaca_stream_urgent",
                    message=msg,
                    level="CRITICAL",
                )
            except Exception as exc:
                logger.warning("Telegram send failed: %s", exc)


# ── helpers ────────────────────────────────────────────────────────────────

def _parse_ws(data) -> list:
    """Parse a raw WS message into a list of dicts."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")
    try:
        obj = json.loads(data)
        return obj if isinstance(obj, list) else [obj]
    except Exception:
        return []


def _log_ws_event(label: str, data) -> None:
    logger.debug("WS %s: %s", label, str(data)[:200])


# ── public entry point ─────────────────────────────────────────────────────

def start_stream(config: dict, tickers: List[str]) -> Optional[AlpacaStreamWorker]:
    """
    Initialise and start the AlpacaStreamWorker in a background daemon thread.

    - Uses api_keys.alpaca_api_key / alpaca_secret_key from config.
    - Streams US tickers only (filters out .L tickers).
    - Returns the worker on success, None if keys are missing or stream disabled.
    - Safe to call multiple times — only starts once.
    """
    global _stream_worker

    # Guard: already running
    if _stream_worker is not None and _stream_worker.is_alive():
        logger.debug("Stream already running — skipping start_stream")
        return _stream_worker

    api_keys   = config.get("api_keys", {})
    api_key    = api_keys.get("alpaca_api_key", "")
    secret_key = api_keys.get("alpaca_secret_key", "")

    if not api_key or "PASTE" in api_key or not secret_key:
        logger.warning("Alpaca stream disabled: keys not configured")
        return None

    us_tickers = {t for t in tickers if not t.endswith(".L")}
    if not us_tickers:
        logger.warning("Alpaca stream disabled: no US tickers provided")
        return None

    try:
        import aiohttp  # noqa — confirms it's available before starting thread
    except ImportError:
        logger.error("aiohttp not installed — run: pip install aiohttp>=3.8.0")
        return None

    worker = AlpacaStreamWorker(api_key, secret_key, us_tickers, config)
    worker.start()
    _stream_worker = worker
    return worker
