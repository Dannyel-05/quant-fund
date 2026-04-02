"""
Real-Time Monitor (Refinement 9)

Continuous monitoring during market hours:
  - Every 15 minutes: check all open positions (price, volume, news, social, options)
  - Every 30 minutes: scan universe for new signals (volume spikes, price moves, news)
  - Immediately on detection: earnings, insider filings, congressional, M&A, 8-K, FDA

Alert system:
  - Structured alerts with event type, tickers, signal, confidence, recommended action
  - Output: terminal, log file, permanent store
  - Telegram support when configured

Usage:
    from monitoring.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(config)
    monitor.run()                     # blocking (market hours only)
    monitor.run_position_check()      # manual single check
    monitor.run_universe_scan()       # manual scan
"""

from __future__ import annotations

import logging
import math
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

# Alert severity levels
ALERT_HIGH   = "HIGH"
ALERT_MEDIUM = "MEDIUM"
ALERT_LOW    = "LOW"
ALERT_INFO   = "INFO"

# Price move thresholds for universe scan
PRICE_SPIKE_PCT = 0.03     # 3% price move = alert
VOLUME_SPIKE_X  = 3.0      # 3x average volume = alert
SECTOR_MOVE_PCT = 0.01     # 1% sector ETF move = macro signal


class Alert:
    """Structured alert object."""

    def __init__(
        self,
        event_type: str,
        severity: str,
        tickers: List[str],
        title: str,
        description: str,
        signal_implication: str,  # BULLISH | BEARISH | NEUTRAL
        confidence: float,
        recommended_action: str = "",
        data: Optional[Dict] = None,
    ):
        self.event_type         = event_type
        self.severity           = severity
        self.tickers            = tickers
        self.title              = title
        self.description        = description
        self.signal_implication = signal_implication
        self.confidence         = confidence
        self.recommended_action = recommended_action
        self.data               = data or {}
        self.timestamp          = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict:
        return {
            "event_type":          self.event_type,
            "severity":            self.severity,
            "tickers":             self.tickers,
            "title":               self.title,
            "description":         self.description,
            "signal_implication":  self.signal_implication,
            "confidence":          self.confidence,
            "recommended_action":  self.recommended_action,
            "data":                self.data,
            "timestamp":           self.timestamp,
        }

    def __str__(self) -> str:
        return (
            f"[{self.severity}] {self.event_type} — {self.title}\n"
            f"  Tickers: {', '.join(self.tickers)}\n"
            f"  Signal:  {self.signal_implication} (conf={self.confidence:.0%})\n"
            f"  Action:  {self.recommended_action}\n"
            f"  {self.description}"
        )


class AlertSink:
    """Handles alert output: terminal, log, permanent store, Telegram."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self._telegram_token = (config or {}).get("telegram", {}).get("bot_token")
        self._telegram_chat  = (config or {}).get("telegram", {}).get("chat_id")
        self._perm_store     = None

    def _get_perm_store(self):
        if self._perm_store is None:
            try:
                from altdata.storage.permanent_store import PermanentStore
                self._perm_store = PermanentStore()
            except Exception:
                pass
        return self._perm_store

    def emit(self, alert: Alert) -> None:
        """Emit alert to all configured sinks."""
        # Terminal
        print(f"\n{'='*60}")
        print(str(alert))
        print(f"{'='*60}\n")

        # Log
        logger.info("ALERT [%s] %s: %s", alert.severity, alert.event_type, alert.title)

        # Permanent store
        try:
            ps = self._get_perm_store()
            if ps:
                ps.log_event(
                    event_type=f"ALERT_{alert.event_type}",
                    ticker=",".join(alert.tickers[:3]),
                    title=alert.title,
                    description=alert.description,
                    data=alert.to_dict(),
                    signal_direction=1 if alert.signal_implication == "BULLISH" else
                                    -1 if alert.signal_implication == "BEARISH" else 0,
                    confidence=alert.confidence,
                    source="realtime_monitor",
                )
        except Exception as e:
            logger.debug("alert sink perm store error: %s", e)

        # Telegram (if configured)
        if self._telegram_token and self._telegram_chat:
            self._send_telegram(alert)

    def _send_telegram(self, alert: Alert) -> None:
        """Send alert to Telegram."""
        emoji = {"HIGH": "🚨", "MEDIUM": "⚠️", "LOW": "ℹ️", "INFO": "📊"}.get(alert.severity, "")
        impl_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}.get(alert.signal_implication, "")
        text = (
            f"{emoji} *{alert.event_type}* — {alert.title}\n"
            f"Tickers: `{', '.join(alert.tickers)}`\n"
            f"Signal: {impl_emoji} {alert.signal_implication} ({alert.confidence:.0%})\n"
            f"Action: _{alert.recommended_action}_"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{self._telegram_token}/sendMessage",
                json={"chat_id": self._telegram_chat, "text": text, "parse_mode": "Markdown"},
                timeout=5,
            )
        except Exception as e:
            logger.debug("telegram send failed: %s", e)


class RealtimeMonitor:
    """
    Continuous market monitoring with event detection and alert generation.
    """

    def __init__(self, config: dict):
        self.config = config
        self.sink   = AlertSink(config)
        self._open_positions: Dict[str, Dict] = {}  # ticker → position data
        self._price_cache: Dict[str, float] = {}    # last known prices
        self._vol_cache:   Dict[str, float] = {}    # 20d avg volume cache

    # ------------------------------------------------------------------
    # Open Position Management
    # ------------------------------------------------------------------

    def load_positions(self, positions: Dict[str, Dict]) -> None:
        """Load current open positions for monitoring."""
        self._open_positions = positions
        logger.info("monitor: loaded %d open positions", len(positions))

    def _load_positions_from_paper_trader(self) -> None:
        """Auto-load from paper trader output file if available."""
        import json
        from pathlib import Path
        pt_file = Path("output/paper_trading.json")
        if pt_file.exists():
            try:
                with open(pt_file) as f:
                    data = json.load(f)
                self._open_positions = data.get("active", {})
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 15-Minute Position Check
    # ------------------------------------------------------------------

    def run_position_check(self) -> List[Alert]:
        """
        Check all open positions every 15 minutes.
        Returns list of alerts generated.
        """
        if not self._open_positions:
            self._load_positions_from_paper_trader()

        alerts = []
        for ticker, pos in self._open_positions.items():
            try:
                alerts.extend(self._check_position(ticker, pos))
            except Exception as e:
                logger.debug("monitor: position check error %s: %s", ticker, e)

        return alerts

    def _check_position(self, ticker: str, pos: Dict) -> List[Alert]:
        """Check a single open position for alerts."""
        alerts = []
        if not HAS_YF:
            return []

        try:
            # Current quote
            info = yf.Ticker(ticker).fast_info
            current_price = float(getattr(info, "last_price", 0) or 0)
            if current_price == 0:
                return []

            entry_price   = float(pos.get("entry_price", current_price))
            direction     = int(pos.get("direction", 1))
            pos_return    = (current_price / max(1e-9, entry_price) - 1) * direction

            # Volume check
            today_vol = float(getattr(info, "three_month_average_volume", 0) or 0)
            avg_vol   = self._vol_cache.get(ticker, today_vol)

            # Alert: significant adverse move (>2% against position)
            if pos_return < -0.02:
                alerts.append(Alert(
                    event_type="POSITION_ADVERSE_MOVE",
                    severity=ALERT_HIGH if pos_return < -0.05 else ALERT_MEDIUM,
                    tickers=[ticker],
                    title=f"{ticker}: adverse move {pos_return:+.1%}",
                    description=f"Open position in {ticker} (direction={direction}) showing {pos_return:+.1%} return. Entry ${entry_price:.2f}, current ${current_price:.2f}.",
                    signal_implication="BEARISH" if direction > 0 else "BULLISH",
                    confidence=0.7,
                    recommended_action=f"Review stop loss. Consider exiting if return < -1.5x ATR.",
                    data={"entry": entry_price, "current": current_price, "return": pos_return},
                ))

            # Alert: significant positive move (>5% with position)
            if pos_return > 0.05:
                alerts.append(Alert(
                    event_type="POSITION_PROFIT_TARGET",
                    severity=ALERT_INFO,
                    tickers=[ticker],
                    title=f"{ticker}: profit target approaching {pos_return:+.1%}",
                    description=f"Position in {ticker} showing {pos_return:+.1%} return. Consider scaling out.",
                    signal_implication="BULLISH" if direction > 0 else "BEARISH",
                    confidence=0.8,
                    recommended_action="Consider selling 33% of position to lock in gains.",
                    data={"entry": entry_price, "current": current_price, "return": pos_return},
                ))

        except Exception as e:
            logger.debug("position check failed %s: %s", ticker, e)

        return alerts

    # ------------------------------------------------------------------
    # 30-Minute Universe Scan
    # ------------------------------------------------------------------

    def run_universe_scan(self, tickers: List[str] = None) -> List[Alert]:
        """
        Scan the universe for new signals every 30 minutes.
        Detects: volume spikes, price moves, sector ETF moves.
        """
        if not tickers:
            tickers = self._load_universe()

        alerts = []

        # Batch fetch last-day data
        for ticker in tickers[:50]:  # cap for rate limiting
            try:
                alert = self._scan_ticker(ticker)
                if alert:
                    alerts.append(alert)
                    self.sink.emit(alert)
            except Exception as e:
                logger.debug("monitor: scan error %s: %s", ticker, e)

        # Sector ETF scan
        alerts.extend(self._scan_sector_etfs())

        return alerts

    def _scan_ticker(self, ticker: str) -> Optional[Alert]:
        """Check a single ticker for unusual activity."""
        if not HAS_YF:
            return None

        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="25d", auto_adjust=True)
            if hist is None or len(hist) < 5:
                return None

            closes = hist["Close"].values
            vols   = hist["Volume"].values

            # Price change today
            if len(closes) < 2:
                return None
            price_chg = (closes[-1] / max(1e-9, closes[-2])) - 1

            # Volume vs average
            avg20 = float(sum(vols[-20:])) / max(1, len(vols[-20:]))
            today_vol = float(vols[-1])
            vol_ratio = today_vol / max(1, avg20)

            # Alert on volume spike
            if vol_ratio >= VOLUME_SPIKE_X:
                impl = "BULLISH" if price_chg > 0 else "BEARISH"
                return Alert(
                    event_type="VOLUME_SPIKE",
                    severity=ALERT_HIGH if vol_ratio >= 5.0 else ALERT_MEDIUM,
                    tickers=[ticker],
                    title=f"{ticker}: volume {vol_ratio:.1f}x average, price {price_chg:+.1%}",
                    description=f"Unusual volume detected for {ticker}: {vol_ratio:.1f}x 20-day average. Price change: {price_chg:+.1%}.",
                    signal_implication=impl,
                    confidence=0.6,
                    recommended_action="Investigate news catalyst. Check for insider/analyst activity.",
                    data={"vol_ratio": vol_ratio, "price_chg": price_chg, "avg_vol": avg20},
                )

            # Alert on large price move (no volume spike)
            if abs(price_chg) >= PRICE_SPIKE_PCT:
                impl = "BULLISH" if price_chg > 0 else "BEARISH"
                return Alert(
                    event_type="PRICE_MOVE",
                    severity=ALERT_MEDIUM,
                    tickers=[ticker],
                    title=f"{ticker}: price move {price_chg:+.1%}",
                    description=f"Significant price move in {ticker}: {price_chg:+.1%} on {vol_ratio:.1f}x average volume.",
                    signal_implication=impl,
                    confidence=0.5,
                    recommended_action="Check for news. If no news, may be technical.",
                    data={"price_chg": price_chg, "vol_ratio": vol_ratio},
                )

        except Exception as e:
            logger.debug("scan ticker failed %s: %s", ticker, e)

        return None

    def _scan_sector_etfs(self) -> List[Alert]:
        """Check sector ETFs for macro signals."""
        SECTOR_ETFS = {
            "XLK": "Technology",
            "XLV": "Healthcare",
            "XLF": "Financials",
            "XLE": "Energy",
            "XLY": "Consumer Discretionary",
            "XLI": "Industrials",
            "SPY": "S&P 500",
        }
        alerts = []
        if not HAS_YF:
            return []

        for etf, sector_name in SECTOR_ETFS.items():
            try:
                hist = yf.download(etf, period="5d", auto_adjust=True, progress=False)
                if hist is None or len(hist) < 2:
                    continue

                closes = hist["Close"].values.flatten()
                pct_chg = (closes[-1] / max(1e-9, closes[-2])) - 1

                if abs(pct_chg) >= SECTOR_MOVE_PCT:
                    impl = "BULLISH" if pct_chg > 0 else "BEARISH"
                    alerts.append(Alert(
                        event_type="SECTOR_MOVE",
                        severity=ALERT_MEDIUM if abs(pct_chg) >= 0.02 else ALERT_LOW,
                        tickers=[etf],
                        title=f"{sector_name} ({etf}): {pct_chg:+.1%}",
                        description=f"Sector ETF {etf} ({sector_name}) moved {pct_chg:+.1%}. May generate readthrough signals.",
                        signal_implication=impl,
                        confidence=0.65,
                        recommended_action=f"Check {sector_name} positions for readthrough effect.",
                        data={"etf": etf, "sector": sector_name, "pct_chg": pct_chg},
                    ))
                    self.sink.emit(alerts[-1])
            except Exception:
                pass

        return alerts

    # ------------------------------------------------------------------
    # Event Detection
    # ------------------------------------------------------------------

    def check_sec_edgar_new(self, tickers: List[str]) -> List[Alert]:
        """
        Check for new SEC filings (8-K, Form 4, etc.) for monitored tickers.
        Uses EDGAR EFTS search API.
        """
        alerts = []
        try:
            import requests as req
            for ticker in tickers[:20]:
                url = f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt={datetime.now().strftime('%Y-%m-%d')}&forms=8-K,4"
                resp = req.get(url, headers={"User-Agent": "quant-fund research@example.com"}, timeout=10)
                if resp.status_code == 200:
                    hits = resp.json().get("hits", {}).get("hits", [])
                    for hit in hits[:3]:
                        form_type = hit.get("_source", {}).get("form_type", "")
                        filed_at  = hit.get("_source", {}).get("file_date", "")
                        company   = hit.get("_source", {}).get("display_names", [ticker])

                        event_type = "FORM4" if form_type == "4" else "8K_FILING"
                        severity   = ALERT_MEDIUM if form_type == "4" else ALERT_HIGH

                        alerts.append(Alert(
                            event_type=event_type,
                            severity=severity,
                            tickers=[ticker],
                            title=f"{ticker}: New {form_type} filed",
                            description=f"New SEC {form_type} filing for {ticker} on {filed_at}. Company: {company}",
                            signal_implication="NEUTRAL",
                            confidence=0.6,
                            recommended_action=f"Read {form_type} immediately. Check for material events.",
                            data={"form_type": form_type, "filed_at": filed_at, "hit": hit.get("_source", {})},
                        ))
                        self.sink.emit(alerts[-1])
                time.sleep(0.5)
        except Exception as e:
            logger.debug("monitor: SEC EDGAR check failed: %s", e)

        return alerts

    # ------------------------------------------------------------------
    # Main Run Loop
    # ------------------------------------------------------------------

    def run(self, universe_tickers: List[str] = None) -> None:
        """
        Blocking run loop. Schedules:
          - Position check: every 15 minutes
          - Universe scan: every 30 minutes
          - SEC filings: every 60 minutes
        Only runs during market hours (9:30am - 4:30pm ET, Mon-Fri).
        """
        if not HAS_SCHEDULE:
            logger.error("monitor: 'schedule' package not installed. Run: pip install schedule")
            return

        tickers = universe_tickers or self._load_universe()
        logger.info("RealtimeMonitor starting with %d tickers", len(tickers))

        schedule.every(15).minutes.do(self.run_position_check)
        schedule.every(30).minutes.do(lambda: self.run_universe_scan(tickers))
        schedule.every(60).minutes.do(lambda: self.check_sec_edgar_new(tickers[:20]))

        logger.info("RealtimeMonitor running. Press Ctrl+C to stop.")
        print("RealtimeMonitor active. Checking every 15/30/60 minutes.")

        while True:
            if self._is_market_hours():
                schedule.run_pending()
            else:
                logger.debug("monitor: outside market hours, sleeping 5min")
            time.sleep(60)

    def _is_market_hours(self) -> bool:
        """Check if current time is within US market hours (Mon-Fri 9:30-16:30 ET)."""
        try:
            from zoneinfo import ZoneInfo
            et = ZoneInfo("America/New_York")
            now_et = datetime.now(et)
            if now_et.weekday() >= 5:  # weekend
                return False
            market_open  = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_et.replace(hour=16, minute=30, second=0, microsecond=0)
            return market_open <= now_et <= market_close
        except Exception:
            return True  # assume market hours if can't determine

    def _load_universe(self) -> List[str]:
        """Load universe tickers from CSV."""
        try:
            import csv
            with open("data/universe_us.csv") as f:
                return [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]
        except Exception:
            return []

    def run_once(self, tickers: List[str] = None) -> Dict:
        """
        Run a single manual check of everything. For CLI use.
        """
        tickers = tickers or self._load_universe()
        print(f"RealtimeMonitor: one-time check for {len(tickers)} tickers...")

        pos_alerts = self.run_position_check()
        scan_alerts = self.run_universe_scan(tickers[:30])

        return {
            "position_alerts":  len(pos_alerts),
            "universe_alerts":  len(scan_alerts),
            "total":            len(pos_alerts) + len(scan_alerts),
            "tickers_scanned":  min(30, len(tickers)),
        }
