"""
Paper trading loop with scheduled market scans.

Runs indefinitely; signals are generated fresh each scan window.
Exits are managed via:
  - Time-based (original exit_date from signal)
  - Scale-out: 33% at 50% of target, 33% at 100% of target, 34% runs to exit_date
  - Dynamic stop: -1.5× ATR from entry
  - Volume dry-up exit: volume < 30% of 20-day average for 2 consecutive days
  - Signal reversal: new opposite signal generated for same ticker
Correlation management: max 3 positions in same sector; max 2 with corr > 0.6.

OBSERVATION MODE: all new positions sized at obs_size_fraction (25%) of normal Kelly.
Below-threshold signals are logged as OBSERVED_NOT_TRADED.
Full context is captured at trade open and stored in closeloop_store.
Trade autopsy is run automatically on every position close.
"""
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import schedule
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Market hours constants (UTC) ─────────────────────────────────────────────
# US NYSE/NASDAQ: 09:30-16:00 ET = 14:30-21:00 UTC
_US_OPEN_UTC  = (14, 30)   # (hour, minute)
_US_CLOSE_UTC = (21,  0)
# UK LSE: 08:00-16:30 UTC
_UK_OPEN_UTC  = ( 8,  0)
_UK_CLOSE_UTC = (16, 30)


def _is_market_open(market: str) -> bool:
    """
    Returns True if the specified market (us/uk) is currently open.
    Uses MarketCalendar for holiday detection + UTC hour/minute check.
    Data collection is NOT gated by this — only trade evaluation.
    """
    try:
        from analysis.market_calendar import MarketCalendar
        cal = MarketCalendar()
        now = datetime.now(timezone.utc)
        today = now.date()
        if not cal.is_trading_day(market, today):
            return False
        h, m = now.hour, now.minute
        now_mins = h * 60 + m
        if market == "us":
            open_mins  = _US_OPEN_UTC[0]  * 60 + _US_OPEN_UTC[1]
            close_mins = _US_CLOSE_UTC[0] * 60 + _US_CLOSE_UTC[1]
        else:
            open_mins  = _UK_OPEN_UTC[0]  * 60 + _UK_OPEN_UTC[1]
            close_mins = _UK_CLOSE_UTC[0] * 60 + _UK_CLOSE_UTC[1]
        return open_mins <= now_mins < close_mins
    except Exception:
        return False


def _next_market_open_str(market: str) -> str:
    """Human-readable string of next market open, e.g. 'Monday 14:30 UTC'."""
    try:
        from analysis.market_calendar import MarketCalendar
        from datetime import date, timedelta
        cal = MarketCalendar()
        today = date.today()
        candidate = today
        for _ in range(10):
            if cal.is_trading_day(market, candidate):
                break
            candidate += timedelta(days=1)
        day_name = candidate.strftime("%A")
        h, m = (_US_OPEN_UTC if market == "us" else _UK_OPEN_UTC)
        return f"{day_name} {h:02d}:{m:02d} UTC"
    except Exception:
        return "next trading day"

# Scale-out fractions: [first_exit_pct_of_target, second_exit_pct_of_target]
_SCALE_OUT_LEVELS = [0.50, 1.00]          # take partial at 50% and 100% of target
_SCALE_OUT_FRACTIONS = [0.33, 0.33]       # 33% off at each level; 34% runs free
_ATR_STOP_MULTIPLIER = 1.5                # stop = entry ± 1.5 × ATR
_VOL_DRY_UP_THRESHOLD = 0.30             # volume < 30% of avg = dry-up
_VOL_DRY_UP_DAYS = 2                      # consecutive dry-up days to trigger exit
_CORRELATION_MAX_SAME_SECTOR = 3          # max open positions in same sector
_CORRELATION_MAX_HIGH_CORR = 2            # max open positions with corr > threshold
_CORRELATION_HIGH_THRESHOLD = 0.60        # correlation threshold

_LOG_PATH = Path("logs/paper_trading.jsonl")


def _append_log(entry: dict) -> None:
    """Append a JSON entry to the paper trading log (append mode)."""
    try:
        _LOG_PATH.parent.mkdir(exist_ok=True)
        with open(_LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.warning("Failed to append paper trading log: %s", e)


class PaperTrader:
    def __init__(
        self,
        config: dict,
        fetcher=None,
        signal_generators: Dict = None,
        risk_manager=None,
        broker=None,
    ):
        self.config = config
        self.pt = config.get("paper_trading", {})

        # ── Detect Alpaca ─────────────────────────────────────────────
        _alpaca_cfg = config.get('alpaca', {})
        _api_key = config.get('api_keys', {}).get('alpaca_api_key', '')
        self.use_alpaca = bool(_alpaca_cfg.get('enabled') and _api_key and 'PASTE' not in _api_key)

        # ── ClosedLoopStore (adaptive sizer depends on this) ───────────
        try:
            from closeloop.storage.closeloop_store import ClosedLoopStore
            self.closeloop = ClosedLoopStore(config)
        except Exception as e:
            logger.warning("ClosedLoopStore init failed: %s", e)
            self.closeloop = None

        # ── Adaptive position sizer ────────────────────────────────────
        try:
            from execution.adaptive_position_sizer import AdaptivePositionSizer
            self.sizer = AdaptivePositionSizer(config, self.closeloop)
        except Exception as e:
            logger.warning("AdaptivePositionSizer init failed: %s", e)
            self.sizer = None

        # ── Universe ──────────────────────────────────────────────────
        try:
            from data.universe import Universe
            self.universe = Universe(config)
        except Exception as e:
            logger.warning("Universe init failed: %s", e)
            self.universe = None

        # ── Auto-init components when not supplied ────────────────────
        if fetcher is None:
            try:
                from data.fetcher import DataFetcher
                fetcher = DataFetcher(config)
            except Exception as e:
                logger.warning("DataFetcher auto-init failed: %s", e)
        self.fetcher = fetcher

        if broker is None:
            try:
                if self.use_alpaca:
                    from execution.broker_interface import AlpacaPaperBroker
                    broker = AlpacaPaperBroker(config)
                else:
                    from execution.broker_interface import PaperBroker
                    broker = PaperBroker(
                        config.get('backtest', {}).get('initial_capital', 100000), config)
            except Exception as e:
                logger.warning("Broker auto-init failed: %s", e)
        self.broker = broker

        if risk_manager is None:
            try:
                from risk.manager import RiskManager
                risk_manager = RiskManager(config)
            except Exception as e:
                logger.warning("RiskManager auto-init failed: %s", e)
        self.risk = risk_manager

        if signal_generators is None:
            try:
                from signals.pead_signal import PEADSignal
                signal_generators = {'pead': PEADSignal(config)}
            except Exception as e:
                logger.warning("Default signal generators failed: %s", e)
                signal_generators = {}
        self.generators = signal_generators

        # Observation mode: reads from config paper_trading.observation_mode
        self.observation_mode = self.pt.get("observation_mode", False)
        self.obs_size_fraction = 0.25

        # ticker → {exit_date, direction, entry_price, atr, target_price,
        #            scale_out_done, sector, shares, dry_up_days,
        #            entry_date, trade_id, market, context_at_open}
        self.active: Dict = {}

        # Internal store reference (lazy-loaded inside run_scan)
        self._store = None

        # ── Extended signal generators ────────────────────────────────
        try:
            from signals.momentum_signal import MomentumSignal
            self.momentum = MomentumSignal(config)
        except Exception:
            self.momentum = None

        try:
            from signals.mean_reversion_signal import MeanReversionSignal
            self.mean_rev = MeanReversionSignal(config)
        except Exception:
            self.mean_rev = None

        try:
            from signals.gap_signal import GapSignal
            self.gap = GapSignal(config)
        except Exception:
            self.gap = None

        try:
            from signals.insider_momentum_signal import InsiderMomentumSignal
            self.insider_mom = InsiderMomentumSignal(config)
        except Exception:
            self.insider_mom = None

        try:
            from signals.calendar_effects_signal import CalendarEffectsSignal
            self.calendar = CalendarEffectsSignal(config)
        except Exception:
            self.calendar = None

        try:
            from signals.options_earnings_signal import OptionsEarningsSignal
            self.options_sig = OptionsEarningsSignal(config)
        except Exception:
            self.options_sig = None

        try:
            from signals.short_selling_filter import ShortSellingFilter
            self.short_filter = ShortSellingFilter(config)
        except Exception:
            self.short_filter = None

        # ── Mathematical / analysis models ────────────────────────────
        try:
            from analysis.mathematical_signals import MathematicalSignals
            self.math_signals = MathematicalSignals()
        except Exception:
            self.math_signals = None

        try:
            from analysis.crowding_detector import CrowdingDetector
            self.crowding = CrowdingDetector(config)
        except Exception:
            self.crowding = None

        try:
            from signals.sector_rotation_signal import SectorRotationSignal
            self.sector_rotation = SectorRotationSignal(config)
        except Exception:
            self.sector_rotation = None

        # ── Alt-data collectors ───────────────────────────────────────
        try:
            from data.collectors.simfin_collector import SimFinCollector
            self.simfin = SimFinCollector(config)
        except Exception:
            self.simfin = None

        try:
            from data.collectors.sec_fulltext_collector import SECFullTextCollector
            self.sec_fulltext = SECFullTextCollector()
        except Exception:
            self.sec_fulltext = None

        try:
            from data.collectors.alternative_quiver_collector import AlternativeQuiverCollector
            self.alt_quiver = AlternativeQuiverCollector(config)
        except Exception:
            self.alt_quiver = None

        # ── Adaptive position sizer ───────────────────────────────────
        try:
            from execution.adaptive_position_sizer import AdaptivePositionSizer
            self.adaptive_sizer = AdaptivePositionSizer(config)
        except Exception:
            self.adaptive_sizer = None

        # ── Trailing stop manager ─────────────────────────────────────
        try:
            from execution.trailing_stops import TrailingStopManager
            self._trailing_stops = TrailingStopManager()
        except Exception as e:
            logger.warning("TrailingStopManager init failed: %s", e)
            self._trailing_stops = None

        # ── Cooling-off tracker ────────────────────────────────────────
        try:
            from execution.cooling_off_tracker import StockCoolingOffTracker
            self._cooling_off = StockCoolingOffTracker(cooling_days=5)
        except Exception as e:
            logger.warning("CoolingOffTracker init failed: %s", e)
            self._cooling_off = None

        # ── Ensure signals_log table exists ──────────────────────────
        import sqlite3, os as _os
        _os.makedirs('output', exist_ok=True)
        try:
            _conn = sqlite3.connect('output/permanent_archive.db')
            _conn.execute('''CREATE TABLE IF NOT EXISTS signals_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT, signal_type TEXT, signal_score REAL,
                direction TEXT, was_traded INTEGER DEFAULT 0,
                trade_id TEXT, macro_regime TEXT, vix REAL,
                yield_curve REAL, shipping_stress REAL,
                consumer_health REAL, crowding_risk REAL,
                hmm_state TEXT, math_composite REAL,
                earnings_quality REAL, calendar_modifier REAL,
                has_crisis_filing INTEGER, all_context_json TEXT,
                timestamp TEXT)''')
            _conn.commit()
            _conn.close()
        except Exception:
            pass

        # ── Earnings cache (bulk-fetched once per day) ──────────────
        try:
            from data.earnings_cache import EarningsCache
            self.earnings_cache = EarningsCache(config)
        except Exception as e:
            logger.warning("EarningsCache init failed: %s", e)
            self.earnings_cache = None

        # Load existing open positions to avoid re-entering on restart
        self._load_existing_positions()

    # ------------------------------------------------------------------
    # Position restoration
    # ------------------------------------------------------------------

    def _load_existing_positions(self) -> None:
        """Load already-open positions from Alpaca + closeloop to prevent re-entry."""
        loaded = 0
        # From Alpaca
        if self.use_alpaca and self.broker is not None:
            try:
                from execution.broker_interface import AlpacaPaperBroker
                if isinstance(self.broker, AlpacaPaperBroker) and self.broker._connected:
                    positions = self.broker.get_positions()
                    for p in (positions or []):
                        sym = p.get("symbol", "")
                        if sym and sym not in self.active:
                            self.active[sym] = {
                                "direction": 1 if float(p.get("qty", 0)) > 0 else -1,
                                "entry_price": float(p.get("avg_entry_price", 0)),
                                "exit_date": None,
                                "atr": 0, "target_price": 0, "target_delta": 0,
                                "scale_out_done": [], "sector": "UNKNOWN",
                                "shares": abs(float(p.get("qty", 0))),
                                "dry_up_days": 0, "market": "us",
                                "context_at_open": {}, "signal_type": "restored",
                                "tier": "TIER_1_SMALLCAP",
                                "entry_date": datetime.utcnow().isoformat(),
                            }
                            loaded += 1
            except Exception as e:
                logger.debug("_load_existing_positions Alpaca: %s", e)
        # From closeloop
        if self.closeloop is not None:
            try:
                open_pos = self.closeloop.get_open_positions()
                for p in (open_pos or []):
                    sym = p.get("ticker", "")
                    if sym and sym not in self.active:
                        self.active[sym] = {
                            "direction": int(p.get("direction", 1)),
                            "entry_price": float(p.get("entry_price", 0)),
                            "exit_date": None,
                            "atr": 0, "target_price": 0, "target_delta": 0,
                            "scale_out_done": [], "sector": "UNKNOWN",
                            "shares": float(p.get("position_size", 0)),
                            "dry_up_days": 0, "market": "us",
                            "context_at_open": {}, "signal_type": p.get("signal_type", "restored"),
                            "tier": "TIER_1_SMALLCAP",
                            "entry_date": p.get("entry_date", datetime.utcnow().isoformat()),
                        }
                        loaded += 1
            except Exception as e:
                logger.debug("_load_existing_positions closeloop: %s", e)
        if loaded:
            logger.info("Restored %d existing open positions", loaded)

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("Paper trader started")
        schedule.every().day.at("07:00").do(self._morning_earnings_check)
        schedule.every().day.at("08:15").do(self.scan_uk)
        schedule.every().day.at("14:45").do(self.scan_us)
        schedule.every().day.at("21:30").do(self.run_eod)

        # Legacy intraday exit checks every 30 minutes during market hours
        for hh in range(10, 17):
            for mm in ("00", "30"):
                schedule.every().day.at(f"{hh:02d}:{mm}").do(
                    lambda m="us": self._check_exits(m)
                )
        schedule.every().day.at("16:30").do(lambda: self._check_exits("us"))
        schedule.every().day.at("16:45").do(lambda: self._check_exits("uk"))

        while True:
            schedule.run_pending()
            time.sleep(30)

    def scan_us(self) -> None:
        self.run_scan(market="us")

    def scan_uk(self) -> None:
        self.run_scan(market="uk")

    # ------------------------------------------------------------------
    # Real-time price helper (stream cache → yfinance fallback)
    # ------------------------------------------------------------------

    def _get_live_price(self, ticker: str,
                        fallback: Optional[float] = None) -> Optional[float]:
        """
        Return the current price for *ticker*.

        1. Tries the Alpaca real-time stream cache (stale threshold: 5 min).
        2. Falls back to yf.Ticker.fast_info.last_price if cache miss / stale.
        3. Returns *fallback* if both fail.

        UK tickers (.L) always use yfinance — they are not streamed.
        """
        if not ticker.endswith(".L"):
            try:
                from execution.alpaca_stream import get_stream_cache
                cached = get_stream_cache().get_fresh_price(ticker)
                if cached:
                    return cached
            except Exception:
                pass
        # yfinance fallback
        try:
            price = yf.Ticker(ticker).fast_info.last_price
            if price:
                return float(price)
        except Exception:
            pass
        return fallback

    # ------------------------------------------------------------------
    # Public API (used by CLI and scheduler)
    # ------------------------------------------------------------------

    def run_scan(self, market: str = "us", limit: int = 0, skip_slow_generators: bool = False) -> List[dict]:
        """
        Run a full signal scan for the given market.
        Returns a list of action dicts (for --once mode printing).
        """
        # Import closeloop_store here to avoid circular imports at module level
        try:
            from closeloop.storage.closeloop_store import ClosedLoopStore
            if self._store is None:
                self._store = ClosedLoopStore(self.config)
        except Exception as e:
            logger.warning("ClosedLoopStore unavailable: %s — continuing without persistence", e)

        logger.info("[%s] Running %s scan", datetime.utcnow().strftime("%H:%M UTC"), market.upper())
        actions = []
        today = pd.Timestamp.now().normalize()
        end = today.strftime("%Y-%m-%d")
        start = (today - timedelta(days=600)).strftime("%Y-%m-%d")

        if not self.fetcher:
            logger.warning("No fetcher configured — scan aborted")
            return actions

        # ── Fetch universe + price data once (shared across all generators) ──
        tickers: List[str] = []
        price_data: Dict = {}
        try:
            from data.universe import UniverseManager
            universe = UniverseManager(self.config, self.fetcher)
            tickers = universe.get_universe(market)
            fetch_tickers = tickers[:limit] if limit > 0 else tickers
            price_data = self.fetcher.fetch_universe_data(fetch_tickers, start, end, market)
            if limit > 0:
                tickers = tickers[:limit]
            # Cache prices on instance so _passes_correlation_check can use them
            self._scan_price_cache = price_data
            self._cached_portfolio = None  # reset per scan
            self._cached_equity = None
            self._cal_mod_cache = None     # reset calendar modifier per scan
            self._macro_regime_cache = None   # reset macro regime per scan
            self._crisis_tickers_cache = None  # reset SEC crisis tickers per scan
            logger.info(
                "[%s] Universe: %d tickers, price data: %d loaded",
                market, len(tickers), len(price_data),
            )
        except Exception as e:
            logger.warning("[%s] Universe/price fetch failed: %s", market, e)

        # Refresh earnings cache once per day (bulk Finnhub fetch)
        if self.earnings_cache is not None and self.earnings_cache.needs_refresh():
            try:
                logger.info("Refreshing earnings cache...")
                self.earnings_cache.bulk_fetch(tickers)
            except Exception as e:
                logger.warning("Earnings cache refresh failed: %s", e)

        # ── PEAD signal generators ──────────────────────────────────────
        for gen_name, gen in self.generators.items():
            try:
                from data.earnings_calendar import EarningsCalendar
                cal = EarningsCalendar(self.config, self.fetcher)

                all_signals = []
                for ticker in tickers:
                    if ticker not in price_data:
                        continue
                    # Use cached earnings if available (avoids 1s per-ticker yfinance call)
                    # When earnings_cache is loaded, it is authoritative — skip live API
                    # for uncached tickers to prevent 1774× yfinance calls per scan.
                    if self.earnings_cache is not None:
                        cached_earnings = self.earnings_cache.get_earnings(ticker)
                        if not cached_earnings:
                            continue  # No cached earnings = no PEAD signal; skip live API
                        signals = gen.generate(
                            ticker, price_data[ticker],
                            earnings_data=cached_earnings,
                        )
                    else:
                        hist = cal.get_earnings_surprise(ticker)
                        signals = gen.generate(ticker, price_data[ticker], hist)
                    if not signals.empty:
                        all_signals.append(signals)

                if not all_signals:
                    continue

                signals_df = pd.concat(all_signals, ignore_index=True)
                todays = signals_df[signals_df["entry_date"] >= today]
                logger.info("[%s] %s: %d signals today", market, gen_name, len(todays))

                for _, row in todays.iterrows():
                    tkr = row.get("ticker", "")
                    pdf = price_data.get(tkr)
                    action = self._process(row, market, price_df=pdf)
                    if action:
                        actions.append(action)

            except Exception as e:
                logger.error("[%s] PEAD scan failed for %s: %s", market, gen_name, e, exc_info=True)

        # ── Extended signal generators ──────────────────────────────────
        # Momentum, mean-reversion, gap, insider — directional signals
        # INSIDER_MOM makes per-ticker SEC API calls (~1s each); skip in limit/quick scans
        _ext_gens = [
            ("MOMENTUM",    self.momentum),
            ("MEAN_REV",    self.mean_rev),
            ("GAP",         self.gap),
        ]
        if not skip_slow_generators and limit == 0:
            _ext_gens.append(("INSIDER_MOM", self.insider_mom))
        for gen_name, gen in _ext_gens:
            if gen is None:
                continue
            gen_actions = 0
            try:
                for ticker in tickers:
                    if ticker not in price_data:
                        continue
                    pdf = price_data[ticker]
                    raw_signals = gen.generate(ticker, pdf)
                    for sig in raw_signals:
                        normalized = self._normalize_alt_signal(sig, gen_name)
                        action = self._process(normalized, market, price_df=pdf)
                        if action:
                            actions.append(action)
                            gen_actions += 1
            except Exception as e:
                logger.error("[%s] Extended scan failed for %s: %s", market, gen_name, e, exc_info=True)
            if gen_actions:
                logger.info("[%s] %s: %d actions", market, gen_name, gen_actions)

        # Track signal tickers for article prioritisation
        signal_tickers = list({a.get('ticker') for a in actions if a.get('ticker')})

        # Fallback: if no signal tickers (weekend / pre-open), use top 20 open positions
        if not signal_tickers:
            try:
                open_rows = self.store.conn.execute(
                    "SELECT DISTINCT ticker FROM trade_ledger WHERE exit_date IS NULL LIMIT 20"
                ).fetchall()
                signal_tickers = [r[0] for r in open_rows]
                if signal_tickers:
                    logger.info("Article fallback: using %d open positions for news fetch", len(signal_tickers))
            except Exception:
                pass

        # Wire article reading: fetch + VADER sentiment for signal/position tickers
        if signal_tickers:
            try:
                from data.collectors.advanced_news_intelligence import AdvancedNewsIntelligence
                ani = AdvancedNewsIntelligence()
                art_results = ani.collect_and_analyse(signal_tickers)
                narrative_shifts = art_results.get('narrative_shifts', {})

                # VADER sentiment scoring on narrative shift text
                vader_scores: dict = {}
                try:
                    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                    _sia = SentimentIntensityAnalyzer()
                    for ticker, shift in narrative_shifts.items():
                        text = shift.get("text", "") if isinstance(shift, dict) else str(shift)
                        if text:
                            vs = _sia.polarity_scores(text)
                            vader_scores[ticker] = vs.get("compound", 0.0)
                except Exception:
                    pass

                # Attach narrative shift + sentiment to matching actions
                for action in actions:
                    ticker = action.get('ticker')
                    if ticker and isinstance(action, dict):
                        if ticker in narrative_shifts:
                            action['article_narrative_shift'] = narrative_shifts[ticker]
                        if ticker in vader_scores:
                            action['news_sentiment'] = vader_scores[ticker]
                            # Negative sentiment on a LONG position → add exit pressure
                            if vader_scores[ticker] < -0.3 and action.get('direction') == 'LONG':
                                action['sentiment_exit_pressure'] = True

                logger.info(
                    "Article intelligence: %d articles, %d claims, %d connections, %d tickers, %d sentiment scores",
                    art_results.get('articles_fetched', 0),
                    art_results.get('claims_extracted', 0),
                    art_results.get('connections_found', 0),
                    len(signal_tickers),
                    len(vader_scores),
                )
            except Exception as e:
                logger.debug("Article reading skipped: %s", e)

        return actions

    def check_open_positions(self) -> None:
        """Mid-day position check — update exits for all markets."""
        self._check_exits("us")
        self._check_exits("uk")

    def run_eod(self) -> None:
        """End-of-day: check exits, update equity curve, log performance."""
        logger.info("=== EOD check ===")
        self._check_exits("us")
        self._check_exits("uk")
        self._log_performance()
        self._signal_decay_check()

    # ------------------------------------------------------------------
    # Signal decay check
    # ------------------------------------------------------------------

    def _signal_decay_check(self) -> None:
        """Log a decay check summary for all open positions."""
        if not self.active:
            return
        for ticker, pos in list(self.active.items()):
            entry = pos.get("entry_price", 0)
            direction = pos.get("direction", 1)
            try:
                price = self._get_live_price(ticker, fallback=entry) or entry
                pnl_pct = direction * (price - entry) / entry * 100 if entry else 0
                days_held = (datetime.utcnow() - datetime.fromisoformat(
                    pos.get("entry_date", datetime.utcnow().isoformat())
                )).days
                logger.info(
                    "SIGNAL DECAY CHECK: %s | held=%dd | pnl=%.2f%%", ticker, days_held, pnl_pct
                )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Morning earnings check
    # ------------------------------------------------------------------

    def _morning_earnings_check(self) -> None:
        """07:00 UTC: check earnings calendar for next 7 days and log."""
        logger.info("=== Morning earnings check (next 7 days) ===")
        try:
            from data.universe import UniverseManager
            from data.fetcher import DataFetcher
            fetcher = self.fetcher or DataFetcher(self.config)
            um = UniverseManager(self.config, fetcher)
            tickers = um.get_universe("us")[:50]
            today = datetime.utcnow().date()
            upcoming = []
            for t in tickers:
                try:
                    cal = yf.Ticker(t).calendar
                    if cal is None:
                        continue
                    dates = []
                    if isinstance(cal, dict):
                        dates = cal.get("Earnings Date", [])
                    for d in (dates[:1] if dates else []):
                        ts = pd.Timestamp(d)
                        days_away = (ts.date() - today).days
                        if 0 <= days_away <= 7:
                            upcoming.append({"ticker": t, "date": str(ts.date()), "days_away": days_away})
                except Exception:
                    pass
            if upcoming:
                for item in sorted(upcoming, key=lambda x: x["days_away"]):
                    logger.info("UPCOMING EARNINGS: %s on %s (in %d days)",
                                item["ticker"], item["date"], item["days_away"])
                _append_log({"type": "earnings_check", "timestamp": datetime.utcnow().isoformat(),
                             "upcoming": upcoming})
            else:
                logger.info("No earnings found in next 7 days (sample 50 tickers)")
        except Exception as e:
            logger.warning("Morning earnings check failed: %s", e)

    # ------------------------------------------------------------------
    # Context capture
    # ------------------------------------------------------------------

    def _capture_context(self, ticker: str, signal) -> dict:
        """
        Capture full market context at the exact moment of trade open.
        All sections are wrapped in try/except — failures are logged and skipped.
        """
        ctx = {"timestamp": datetime.utcnow().isoformat()}

        # ── PEAD context ──────────────────────────────────────────────
        try:
            sig_dict = signal.to_dict() if hasattr(signal, "to_dict") else dict(signal)
            ctx["pead"] = {
                "surprise_pct": sig_dict.get("surprise_pct"),
                "surprise_zscore": sig_dict.get("surprise_zscore"),
                "quality": sig_dict.get("quality"),
                "volume_surge": sig_dict.get("volume_surge"),
            }
        except Exception as e:
            logger.debug("Context: PEAD capture failed: %s", e)
            ctx["pead"] = {}

        # ── Macro context ─────────────────────────────────────────────
        # All macro data is cached on the instance to avoid per-trade API calls
        macro_ctx = {}
        try:
            if not hasattr(self, '_macro_engine'):
                from intelligence.macro_signal_engine import MacroSignalEngine
                self._macro_engine = MacroSignalEngine(config_path='config/settings.yaml')
            macro_ctx["regime"] = self._macro_engine.get_current_regime()
        except Exception as e:
            logger.debug("Context: MacroSignalEngine unavailable: %s", e)
            macro_ctx["regime"] = None

        try:
            if not hasattr(self, '_vix_cache'):
                vix_data = yf.download("^VIX", period="2d", progress=False, auto_adjust=True)
                if vix_data is not None and not vix_data.empty:
                    close = vix_data["Close"]
                    if isinstance(close, pd.DataFrame):
                        close = close.iloc[:, 0]
                    self._vix_cache = float(close.iloc[-1])
                else:
                    self._vix_cache = None
            macro_ctx["vix"] = self._vix_cache
        except Exception as e:
            logger.debug("Context: VIX fetch failed: %s", e)
            macro_ctx["vix"] = None

        try:
            if not hasattr(self, '_rates_collector'):
                from data.collectors.rates_credit_collector import RatesCreditCollector
                self._rates_collector = RatesCreditCollector('config/settings.yaml')
            rates = self._rates_collector.get_latest()
            macro_ctx["yield_curve_slope_bps"] = rates.get("yield_curve_slope_bps")
            macro_ctx["hy_spread_bps"] = rates.get("hy_spread_bps")
        except Exception as e:
            logger.debug("Context: RatesCreditCollector unavailable: %s", e)
            macro_ctx["yield_curve_slope_bps"] = None
            macro_ctx["hy_spread_bps"] = None

        ctx["macro"] = macro_ctx

        # ── Altdata context ───────────────────────────────────────────
        altdata_ctx = {"confluence_score": None, "sentiment": None}
        try:
            if self._store is not None and hasattr(self._store, "get_altdata_confluence"):
                altdata_ctx["confluence_score"] = self._store.get_altdata_confluence(ticker)
        except Exception as e:
            logger.debug("Context: altdata confluence failed: %s", e)

        ctx["altdata"] = altdata_ctx

        # ── Shipping stress index ─────────────────────────────────────
        try:
            import sqlite3
            db_path = "output/historical_db.db"
            if Path(db_path).exists():
                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT * FROM shipping_data ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
                conn.close()
                ctx["shipping_stress_index"] = row[1] if row and len(row) > 1 else None
            else:
                ctx["shipping_stress_index"] = None
        except Exception as e:
            logger.debug("Context: shipping_data read failed: %s", e)
            ctx["shipping_stress_index"] = None

        # ── HMM state / mathematical signals ─────────────────────────
        try:
            ms = self.math_signals
            if ms is None:
                from analysis.mathematical_signals import MathematicalSignals
                ms = MathematicalSignals()
            ms.analyse([ticker])
            ctx["hmm_state"] = ms.get_combined_signal(ticker)
        except Exception as e:
            logger.debug("Context: HMM state unavailable: %s", e)
            ctx["hmm_state"] = None

        # ── SimFin earnings quality ────────────────────────────────────
        try:
            if self.simfin is not None and self.simfin.enabled:
                eq = self.simfin.calculate_earnings_quality(ticker)
                ctx["earnings_quality"] = {
                    "score": eq.get("earnings_quality_score"),
                    "tier":  eq.get("quality_tier"),
                }
            else:
                ctx["earnings_quality"] = None
        except Exception as e:
            logger.debug("Context: SimFin earnings quality unavailable: %s", e)
            ctx["earnings_quality"] = None

        # ── SEC full-text crisis alerts ────────────────────────────────
        try:
            if self.sec_fulltext is not None:
                crisis_hits = self.sec_fulltext.search_keyword("going concern", days_back=7)
                crisis_tickers = {h.get("ticker") for h in crisis_hits if h.get("ticker")}
                ctx["has_sec_crisis_alert"] = ticker.upper() in crisis_tickers
            else:
                ctx["has_sec_crisis_alert"] = False
        except Exception as e:
            logger.debug("Context: SEC fulltext unavailable: %s", e)
            ctx["has_sec_crisis_alert"] = False

        # ── Geopolitical risk ─────────────────────────────────────────
        try:
            import sqlite3
            db_path = "output/permanent_log.db"
            if Path(db_path).exists():
                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT geopolitical_risk FROM macro_context ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
                conn.close()
                ctx["geopolitical_risk"] = row[0] if row else "LOW"
            else:
                ctx["geopolitical_risk"] = "LOW"
        except Exception as e:
            logger.debug("Context: geopolitical_risk read failed: %s", e)
            ctx["geopolitical_risk"] = "LOW"

        return ctx

    # ------------------------------------------------------------------
    # Signals log persistence
    # ------------------------------------------------------------------

    def _log_to_signals_db(
        self,
        ticker: str,
        signal,
        confidence: float,
        direction: int,
        was_traded: bool,
        context: dict = None,
        trade_id: str = None,
    ) -> None:
        """Persist every evaluated signal to signals_log for learning loop."""
        try:
            import sqlite3, json as _json
            ctx = context or {}
            macro = ctx.get('macro', {}) or {}
            eq = ctx.get('earnings_quality', {}) or {}
            eq_score = float(eq.get('score', 0.5) if isinstance(eq, dict) else eq or 0.5)
            sig_dict = signal.to_dict() if hasattr(signal, 'to_dict') else dict(signal)
            conn = sqlite3.connect('output/permanent_archive.db')
            conn.execute(
                '''INSERT INTO signals_log
                   (ticker, signal_type, signal_score, direction, was_traded, trade_id,
                    macro_regime, vix, yield_curve, shipping_stress, consumer_health,
                    crowding_risk, hmm_state, math_composite, earnings_quality,
                    calendar_modifier, has_crisis_filing, all_context_json, timestamp)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    ticker,
                    str(sig_dict.get('signal_type', 'unknown')),
                    float(confidence),
                    int(direction),
                    1 if was_traded else 0,
                    trade_id,
                    str(macro.get('regime', '')),
                    float(macro.get('vix', 0) or 0),
                    float(ctx.get('yield_curve_slope_bps', 0) or 0),
                    float(ctx.get('shipping_stress_index', 0) or 0),
                    float(ctx.get('consumer_health', 0) or 0),
                    float(ctx.get('crowding_risk', 0) or 0),
                    str(ctx.get('hmm_state', '')),
                    float(ctx.get('math_composite', 0) or 0),
                    eq_score,
                    float(ctx.get('calendar_modifier', 0) or 0),
                    1 if ctx.get('has_sec_crisis_alert') else 0,
                    _json.dumps(ctx),
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug('signals_log INSERT failed: %s', e)

    # ------------------------------------------------------------------
    # Process a signal
    # ------------------------------------------------------------------

    def _process(self, signal: pd.Series, market: str, price_df: pd.DataFrame = None) -> Optional[dict]:
        ticker = signal["ticker"]
        direction = int(signal["signal"])
        confidence = float(signal.get("surprise_zscore", 0.0))
        min_confidence = self.pt.get("min_confidence", 1.0)

        # ── Real-time signal boost ─────────────────────────────────────
        # If the stream confirms the price is already moving in the same
        # direction as our signal, boost confidence by up to +0.5.
        try:
            from execution.alpaca_stream import get_stream_cache
            rt_move = get_stream_cache().get_move_pct(ticker, window_minutes=5)
            if rt_move is not None:
                # Signal aligned with real-time price direction → boost
                if direction * rt_move > 2.0:
                    boost = min(0.5, abs(rt_move) / 20.0)
                    confidence += boost
                    logger.debug(
                        "RT boost: %s rt_move=%.2f%% boost=+%.3f → conf=%.3f",
                        ticker, rt_move, boost, confidence,
                    )
                # Signal opposite to current real-time move → slight penalty
                elif direction * rt_move < -3.0:
                    confidence *= 0.85
        except Exception:
            pass

        # Skip if already in position
        if ticker in self.active:
            if self.active[ticker]["direction"] != direction:
                logger.info("%s: reversal signal — closing existing position", ticker)
                self._close_position(ticker, reason="signal_reversal")
            else:
                return None

        # Signals below confidence threshold: log as OBSERVED_NOT_TRADED
        if abs(confidence) < min_confidence:
            sig_dict = signal.to_dict() if hasattr(signal, "to_dict") else dict(signal)
            obs_entry = {
                "type": "observed_not_traded",
                "timestamp": datetime.utcnow().isoformat(),
                "ticker": ticker,
                "market": market,
                "direction": direction,
                "confidence": confidence,
                "min_confidence": min_confidence,
                "reason": f"confidence {confidence:.3f} < threshold {min_confidence}",
                "signal": sig_dict,
            }
            _append_log(obs_entry)
            self._log_to_signals_db(ticker, signal, confidence, direction, was_traded=False)
            logger.info("OBSERVED_NOT_TRADED: %s (confidence=%.3f < %.3f)", ticker, confidence, min_confidence)
            return obs_entry

        # Use pre-fetched price if available (avoids per-ticker API call).
        # Real-time cache is checked as a secondary source when price_df is stale.
        try:
            if price_df is not None and not price_df.empty:
                close_col = 'close' if 'close' in price_df.columns else price_df.columns[-1]
                price = float(price_df[close_col].iloc[-1])
                # Overlay with real-time cache if a fresher price exists
                rt_price = self._get_live_price(ticker)
                if rt_price:
                    price = rt_price
            else:
                price = self._get_live_price(ticker)
            if not price:
                return None
        except Exception:
            return None

        # Correlation / concentration checks before opening
        sector = self._get_sector(ticker)
        if not self._passes_correlation_check(ticker, direction, sector):
            logger.info("%s: blocked by correlation management", ticker)
            return None

        # Cache portfolio state + account equity for scan duration to avoid per-ticker API calls
        if not hasattr(self, '_cached_portfolio') or self._cached_portfolio is None:
            self._cached_portfolio = self._portfolio_state()
            self._cached_equity = self._cached_portfolio.get('account_value', 0)
        portfolio = self._cached_portfolio
        size_pct = self.risk.size_position(
            ticker=ticker,
            signal_strength=abs(signal.get("surprise_zscore", 1.0)),
            portfolio=portfolio,
            price_data=pd.DataFrame(),
        )
        if size_pct <= 0:
            return None

        # Observation mode: scale size to 25% of normal Kelly
        if self.observation_mode:
            size_pct = size_pct * self.obs_size_fraction

        # Apply sector rotation modifier (-0.15 to +0.15) — pass pre-fetched sector
        try:
            if self.sector_rotation is not None:
                sr_mod = self.sector_rotation.get_modifier(ticker, sector=sector)
                size_pct = size_pct * (1.0 + sr_mod)
        except Exception:
            pass

        # Apply calendar effects modifier (-0.40 to +0.30) — cached per scan day
        try:
            if self.calendar is not None:
                if not hasattr(self, '_cal_mod_cache'):
                    self._cal_mod_cache = None
                if self._cal_mod_cache is None:
                    cal_result = self.calendar.get_composite_modifier(store_to_db=False)
                    self._cal_mod_cache = float(cal_result.get("total_modifier", 0.0))
                size_pct = size_pct * (1.0 + self._cal_mod_cache)
        except Exception:
            pass

        # Apply tier-based size multiplier
        try:
            from data.universe import classify_tier, get_tier_size_multiplier
            ticker_info = {}
            if self.fetcher:
                ticker_info = self.fetcher.fetch_ticker_info(ticker) or {}
            cap = float(ticker_info.get("marketCap") or 0)
            tier = classify_tier(cap)
            tier_mult = get_tier_size_multiplier(tier)
            size_pct = size_pct * tier_mult
            signal = signal.copy()
            signal["_tier"] = tier
        except Exception:
            pass

        # SignalAggregator check (fail-open) — cached instance to avoid per-ticker re-init
        try:
            if not hasattr(self, '_signal_aggregator'):
                from closeloop.integration.signal_aggregator import SignalAggregator
                self._signal_aggregator = SignalAggregator(config=self.config)
            pead_sig_dict = signal.to_dict() if hasattr(signal, "to_dict") else dict(signal)
            agg_result = self._signal_aggregator.aggregate(ticker=ticker, pead_signal=pead_sig_dict)
            if agg_result.get("kelly_multiplier", 1.0) == 0.0:
                logger.info("%s: blocked by SignalAggregator (kelly_multiplier=0)", ticker)
                return None
        except Exception:
            pass  # fail open

        # Cooling-off check — block re-entry after a losing trade
        if self._cooling_off is not None:
            try:
                if self._cooling_off.is_cooling_off(ticker):
                    days_left = self._cooling_off.days_remaining(ticker)
                    logger.info("%s: blocked by cooling-off (%d days remaining)", ticker, days_left)
                    return None
            except Exception:
                pass

        equity = getattr(self, '_cached_equity', None) or self.broker.get_account_value()
        value = equity * size_pct
        shares = value / price
        order_dir = "buy" if direction > 0 else "short"

        # Route to correct broker signature:
        #   AlpacaPaperBroker: place_order(ticker, qty, side, ...)
        #   PaperBroker:       place_order(ticker, quantity, direction, fill_price, ...)
        try:
            from execution.broker_interface import AlpacaPaperBroker as _AlpacaBroker
            if isinstance(self.broker, _AlpacaBroker):
                _side = 'buy' if direction > 0 else 'sell'
                result = self.broker.place_order(ticker, shares, _side,
                                                  direction=order_dir, fill_price=price)
            else:
                result = self.broker.place_order(ticker=ticker, quantity=shares,
                                                  direction=order_dir, fill_price=price)
        except Exception as _e:
            logger.debug("place_order routing error, falling back: %s", _e)
            result = {'status': 'error'}
        _order_status = result.get("status", "error")
        _is_filled = _order_status == "filled"
        _is_submitted = _order_status == "submitted"
        if _is_filled or _is_submitted:
            self._cached_portfolio = None  # invalidate so next check reflects new position
            self._cached_equity = None
            atr = self._compute_atr(ticker, price_df=price_df)
            surprise_pct = abs(signal.get("surprise_pct", 0.05))
            target_delta = max(surprise_pct * price, atr)
            target_price = price + direction * target_delta

            # Capture full context at open
            context_at_open = {}
            try:
                context_at_open = self._capture_context(ticker, signal)
            except Exception as e:
                logger.warning("Context capture failed for %s: %s", ticker, e)

            entry_date = datetime.utcnow().isoformat()

            self.active[ticker] = {
                "exit_date": signal.get("exit_date"),
                "direction": direction,
                "entry_price": price,
                "entry_date": entry_date,
                "atr": atr,
                "target_price": target_price,
                "target_delta": target_delta,
                "scale_out_done": [],
                "sector": sector,
                "shares": shares,
                "dry_up_days": 0,
                "market": market,
                "context_at_open": context_at_open,
                "signal_type": str(signal.get("signal_type", "pead")).lower(),
                "tier": str(signal.get("_tier", "TIER_1_SMALLCAP")),
                "is_submitted_only": not _is_filled,
            }

            # Register with trailing stop manager
            if self._trailing_stops is not None:
                try:
                    self._trailing_stops.add_position(ticker, entry_price=price, current_price=price)
                except Exception:
                    pass

            # Persist open position in closeloop_store — only for confirmed fills
            try:
                if self._store is not None and _is_filled:
                    trade_record = {
                        "ticker": ticker,
                        "market": market,
                        "direction": direction,
                        "entry_date": entry_date,
                        "entry_price": price,
                        "position_size": shares,
                        "order_status": _order_status,
                        "is_phantom": 0,
                    }
                    trade_id = self._store.record_trade(trade_record, context_at_open)
                    self.active[ticker]["trade_id"] = trade_id
                elif _is_submitted:
                    logger.info(
                        "%s: order submitted (not filled) — tracking in memory only", ticker
                    )
            except Exception as e:
                logger.warning("Failed to persist trade open in closeloop_store: %s", e)

            _append_log({
                "type": "trade_open",
                "timestamp": entry_date,
                "ticker": ticker,
                "market": market,
                "direction": direction,
                "entry_price": price,
                "shares": shares,
                "size_pct": size_pct,
                "signal_type": str(signal.get("signal_type", "pead")).lower(),
                "observation_mode": self.observation_mode,
                "context_at_open": context_at_open,
                "order_status": _order_status,
            })

            if _is_filled:
                self._log_to_signals_db(
                    ticker, signal, confidence, direction,
                    was_traded=True,
                    context=context_at_open,
                    trade_id=self.active[ticker].get("trade_id"),
                )
            logger.info(
                "Opened %s %s @ %.4f | target=%.4f | stop=%.4f | ATR=%.4f | obs_mode=%s",
                order_dir.upper(), ticker, price, target_price,
                price - direction * _ATR_STOP_MULTIPLIER * atr, atr,
                self.observation_mode,
            )
            return {"type": "trade_open", "ticker": ticker, "direction": order_dir,
                    "price": price, "shares": shares}

        return None

    def run_all_signals(
        self, ticker: str, price_data, context: Dict
    ) -> List[Dict]:
        """
        Run every loaded signal generator for a single ticker.
        Returns a unified list of signal dicts with keys:
          ticker, direction, score, signal_type
        """
        all_sigs: List[Dict] = []

        gen_pairs = [
            ('MOMENTUM',    self.momentum),
            ('MEAN_REV',    self.mean_rev),
            ('GAP',         self.gap),
            ('INSIDER_MOM', self.insider_mom),
        ]
        for gen_name, gen in gen_pairs:
            if gen is None:
                continue
            try:
                sigs = gen.generate(ticker, price_data)
                for s in sigs:
                    s['signal_type'] = gen_name
                all_sigs.extend(sigs)
            except Exception:
                pass

        # PEAD signal if generators loaded
        for gen_name, gen in self.generators.items():
            try:
                from data.earnings_calendar import EarningsCalendar
                cal = EarningsCalendar(self.config, self.fetcher)
                hist = cal.get_earnings_surprise(ticker)
                df = gen.generate(ticker, price_data, hist)
                if not df.empty:
                    for _, row in df.iterrows():
                        all_sigs.append({
                            'ticker': ticker,
                            'direction': 'LONG' if row.get('signal', 1) > 0 else 'SHORT',
                            'score': abs(float(row.get('surprise_zscore', 0.5))),
                            'signal_type': gen_name.upper(),
                        })
            except Exception:
                pass

        # ── Phase 10: signal contradiction scoring ────────────────────────────
        if all_sigs:
            all_sigs = self._apply_contradiction_scores(all_sigs)

        return all_sigs

    @staticmethod
    def _apply_contradiction_scores(signals: List[Dict]) -> List[Dict]:
        """
        Compute contradiction score for each signal based on buy/sell consensus.

        contradiction_score:
          0.0 = full consensus (all same direction)
          1.0 = maximum contradiction (50/50 split with equal weights)

        Effect:
          - consensus (contradiction < 0.2)  → score * 1.15 boost
          - strong contradiction (> 0.6)     → score * 0.75 reduction
          - moderate (0.2–0.6)               → no adjustment
        """
        longs  = [s for s in signals if s.get("direction", "LONG") == "LONG"]
        shorts = [s for s in signals if s.get("direction", "LONG") == "SHORT"]
        total  = len(signals)

        if total == 0:
            return signals

        long_wt  = sum(float(s.get("score", 0.5)) for s in longs)
        short_wt = sum(float(s.get("score", 0.5)) for s in shorts)
        total_wt = long_wt + short_wt

        if total_wt < 1e-9:
            contradiction_score = 0.0
        else:
            minority_wt = min(long_wt, short_wt)
            contradiction_score = 2.0 * minority_wt / total_wt  # 0=consensus, 1=50-50

        for s in signals:
            s["contradiction_score"] = round(contradiction_score, 4)
            base_score = float(s.get("score", 0.5))
            if contradiction_score < 0.2:
                s["score"] = min(base_score * 1.15, 5.0)  # consensus boost
            elif contradiction_score > 0.6:
                s["score"] = base_score * 0.75            # disagreement penalty

        return signals

    def pre_short_checklist(self, ticker: str, context: Dict, price_data) -> Dict:
        """
        Phase 11: Run 8 checks before allowing a SHORT position.
        Returns dict with 'passed' bool and 'checks' detail dict.
        All 8 must pass for a SHORT to be allowed.
        """
        checks: Dict[str, bool] = {}

        # Check 0: Regime gate — no shorts in BULL
        try:
            from analysis.regime_detector import RegimeDetector
            if not hasattr(self, '_regime_detector'):
                self._regime_detector = RegimeDetector()
            regime = self._regime_detector.get_current_regime()
            if regime == "BULL":
                checks["regime_allows_short"] = False
                logger.info("SHORT_REJECTED: pre_short_checklist: %s BLOCKED — BULL regime (no shorts)", ticker)
                return {"passed": False, "checks": checks, "reason": "bull_regime"}
            checks["regime_allows_short"] = True
        except Exception:
            checks["regime_allows_short"] = True  # fail open

        # RSI + momentum filter for short entries
        try:
            if price_data is not None and not price_data.empty and len(price_data) >= 25:
                from analysis.technical_indicators import TechnicalIndicatorCalculator
                rsi = TechnicalIndicatorCalculator.rsi(price_data)
                # Only short if RSI > 65 (overbought) AND 20d momentum is negative
                closes = price_data['close'].values if 'close' in price_data.columns else price_data.iloc[:, 3].values
                mom_20d = (closes[-1] - closes[-21]) / closes[-21] if len(closes) >= 21 else 0
                checks["short_momentum_ok"] = bool(rsi is not None and rsi > 65 and mom_20d < 0)
                if not checks["short_momentum_ok"]:
                    logger.info(
                        "SHORT_REJECTED: pre_short_checklist: %s BLOCKED — RSI=%.1f mom20d=%.1f%% (need RSI>65 + neg momentum)",
                        ticker, rsi or 0, mom_20d * 100
                    )
            else:
                checks["short_momentum_ok"] = True  # no data, fail open
        except Exception:
            checks["short_momentum_ok"] = True

        # 1. Short availability (borrow exists)
        checks["borrow_available"] = context.get("short_available", True)

        # 2. Not in squeeze territory (days-to-cover < 10)
        dtc = context.get("days_to_cover")
        checks["days_to_cover_ok"] = (dtc is None or dtc < 10.0)

        # 3. Short float below 30% (avoid short-squeeze risk)
        short_float = context.get("short_float_pct", 0.0)
        checks["short_float_ok"] = short_float < 0.30

        # 4. Not near earnings (within 5 days)
        days_to_earn = context.get("days_to_earnings")
        checks["earnings_clear"] = (days_to_earn is None or days_to_earn > 5)

        # 5. Not a biotech with binary catalyst pending
        sector = context.get("sector", "")
        has_catalyst = context.get("binary_catalyst_pending", False)
        checks["biotech_catalyst_clear"] = not (sector in ("Healthcare", "Biotechnology") and has_catalyst)

        # 6. Macro regime not CRISIS (shorts get squeezed in panic)
        macro_regime = context.get("macro_regime", "NEUTRAL")
        checks["macro_regime_ok"] = macro_regime != "CRISIS"

        # 7. Volume confirms — need > 0.5× average (not illiquid)
        try:
            if price_data is not None and "volume" in price_data.columns:
                vol_series = price_data["volume"].dropna()
                if len(vol_series) >= 10:
                    avg_vol = float(vol_series.iloc[-10:].mean())
                    last_vol = float(vol_series.iloc[-1])
                    checks["volume_ok"] = (avg_vol <= 0 or last_vol > avg_vol * 0.5)
                else:
                    checks["volume_ok"] = True
            else:
                checks["volume_ok"] = True
        except Exception:
            checks["volume_ok"] = True

        # 8. Not a UK penny stock (< 10p) — hard to borrow
        checks["uk_penny_ok"] = True
        if ticker.endswith(".L"):
            try:
                last_price = float(price_data.iloc[-1]["close"] if "close" in price_data.columns
                                   else price_data.iloc[-1, 3])
                checks["uk_penny_ok"] = last_price >= 0.10
            except Exception:
                pass

        passed = all(checks.values())
        return {"passed": passed, "checks": checks}

    def build_full_context(self, ticker: str) -> Dict:
        """Build full market context for a ticker (lighter version of _capture_context)."""
        ctx: Dict = {'ticker': ticker, 'timestamp': datetime.utcnow().isoformat()}

        # Macro — cached per scan to avoid creating MacroSignalEngine per ticker
        try:
            if not hasattr(self, '_macro_regime_cache') or self._macro_regime_cache is None:
                from analysis.macro_signal_engine import MacroSignalEngine
                mse = MacroSignalEngine()
                briefing = mse.get_complete_briefing_data()
                self._macro_regime_cache = briefing.get('regime', 'UNKNOWN')
            ctx['macro_regime'] = self._macro_regime_cache
            ctx['macro'] = {'regime': self._macro_regime_cache}
        except Exception:
            ctx['macro_regime'] = 'UNKNOWN'

        # HMM state
        try:
            if self.math_signals is not None:
                self.math_signals.analyse([ticker])
                hmm_val = self.math_signals.get_combined_signal(ticker)
                ctx['hmm_state'] = 'BULL' if hmm_val > 0.1 else ('BEAR' if hmm_val < -0.1 else 'NEUTRAL')
                ctx['math_composite'] = hmm_val
        except Exception:
            ctx['hmm_state'] = None
            ctx['math_composite'] = 0

        # Earnings quality
        try:
            if self.simfin is not None and self.simfin.enabled:
                eq = self.simfin.calculate_earnings_quality(ticker)
                ctx['earnings_quality_score'] = eq.get('earnings_quality_score', 0.5)
                ctx['earnings_quality'] = eq
        except Exception:
            ctx['earnings_quality_score'] = 0.5

        # SEC crisis — cached per scan (same result for all tickers)
        try:
            if self.sec_fulltext is not None:
                if not hasattr(self, '_crisis_tickers_cache') or self._crisis_tickers_cache is None:
                    hits = self.sec_fulltext.search_keyword('going concern', days_back=7)
                    self._crisis_tickers_cache = {h.get('ticker') for h in hits if h.get('ticker')}
                ctx['has_crisis_filing'] = ticker.upper() in self._crisis_tickers_cache
        except Exception:
            ctx['has_crisis_filing'] = False

        # Sector — use static map first (fast); yfinance only as last resort
        ctx['sector'] = self._get_sector(ticker)

        # Crowding
        try:
            if self.crowding is not None:
                ctx['crowding_risk'] = self.crowding.get_current_crowding_risk([ticker]).get('overall_score', 0.1)
        except Exception:
            ctx['crowding_risk'] = 0.1

        return ctx

    def check_exit_conditions(self, positions: List[Dict], equity: float) -> None:
        """Check exit conditions for a list of open positions."""
        try:
            for pos in positions:
                ticker = pos.get('ticker') or pos.get('symbol', '')
                if not ticker:
                    continue
                market = 'uk' if ticker.endswith('.L') else 'us'
                self._check_exits(market)
        except Exception as e:
            logger.warning('check_exit_conditions: %s', e)

    @staticmethod
    def _normalize_alt_signal(sig: dict, gen_name: str) -> pd.Series:
        """
        Convert a non-PEAD signal dict (direction: 'LONG'/'SHORT', score: float)
        into a pd.Series compatible with _process().
        Exit date is a pd.Timestamp (for exit logic) but also stored as ISO string.
        """
        direction_str = sig.get("direction", "LONG")
        direction_int = 1 if direction_str == "LONG" else -1
        score = float(sig.get("score", 0.5))
        # Scale score to a zscore-like range [0.5, 3.0] so confidence gates work
        zscore_equiv = score * 3.0
        holding_days = {"MOMENTUM": 20, "MEAN_REV": 10, "GAP": 5, "INSIDER_MOM": 30}.get(gen_name, 15)
        now = pd.Timestamp.now()
        exit_ts = now + pd.Timedelta(days=holding_days)
        return pd.Series({
            "ticker":          sig["ticker"],
            "signal":          direction_int,
            "surprise_zscore": zscore_equiv,
            "surprise_pct":    score * 0.10,
            "exit_date":       exit_ts.isoformat(),
            "quality":         "UNKNOWN",
            "volume_surge":    1.0,
            "signal_type":     gen_name,
            "entry_date":      now.isoformat(),
        })

    def _check_exits(self, market: str) -> None:
        # ── Market hours gate ─────────────────────────────────────────────
        if not _is_market_open(market):
            logger.debug(
                "Market closed — skipping exit evaluation for %s (opens %s)",
                market.upper(), _next_market_open_str(market),
            )
            return

        today = pd.Timestamp.now().normalize()
        for ticker in list(self.active.keys()):
            is_uk = ticker.endswith(".L")
            if (market == "uk") != is_uk:
                continue
            pos = self.active[ticker]

            try:
                price = self._get_live_price(ticker)
                if not price:
                    continue
            except Exception:
                continue

            direction = pos["direction"]
            entry = pos["entry_price"]
            atr = pos.get("atr", 0.0)
            target_price = pos.get("target_price", entry)
            target_delta = pos.get("target_delta", abs(target_price - entry))

            # ----------------------------------------------------------
            # 1. ATR stop loss
            # ----------------------------------------------------------
            # Tighter stop for shorts (1.0x ATR) vs longs (1.5x ATR)
            atr_mult = 1.0 if direction < 0 else _ATR_STOP_MULTIPLIER
            stop = entry - direction * atr_mult * atr
            hit_stop = (direction > 0 and price <= stop) or (direction < 0 and price >= stop)
            if hit_stop:
                logger.info("%s: ATR stop hit @ %.4f (stop=%.4f)", ticker, price, stop)
                self._close_position(ticker, reason="atr_stop", price=price)
                continue

            # ----------------------------------------------------------
            # 1b. Trailing stop (tiered — activates after 5% gain)
            # ----------------------------------------------------------
            if self._trailing_stops is not None:
                try:
                    old_tier = self._trailing_stops.tier(ticker) if hasattr(self._trailing_stops, 'tier') else None
                    self._trailing_stops.observe(ticker, price, entry_price=entry)
                    new_tier = self._trailing_stops.tier(ticker) if hasattr(self._trailing_stops, 'tier') else None
                    # Alert on tier graduation
                    if old_tier is not None and new_tier is not None and new_tier > old_tier:
                        gain_pct = (price - entry) / entry * 100 if entry else 0
                        _tier_msg = (
                            f"TRAILING STOP TIER UP: {ticker} "
                            f"Tier {old_tier}\u2192{new_tier} "
                            f"(gain={gain_pct:.1f}%, stop={self._trailing_stops.stop_price(ticker):.4f})"
                            if hasattr(self._trailing_stops, 'stop_price') else
                            f"TRAILING STOP TIER UP: {ticker} Tier {old_tier}\u2192{new_tier} (gain={gain_pct:.1f}%)"
                        )
                        logger.info(_tier_msg)
                        try:
                            from altdata.notifications.notifier import Notifier
                            Notifier(self.config)._send_telegram(_tier_msg)
                        except Exception:
                            pass
                    if self._trailing_stops.should_exit(ticker, price):
                        logger.info("%s: trailing stop triggered @ %.4f", ticker, price)
                        self._close_position(ticker, reason="trailing_stop", price=price)
                        continue
                except Exception as _te:
                    logger.debug("Trailing stop error for %s: %s", ticker, _te)

            # ----------------------------------------------------------
            # 2. Scale-out at 50% and 100% of target delta
            # ----------------------------------------------------------
            pnl_pct = direction * (price - entry) / entry if entry else 0.0
            target_pct = target_delta / entry if entry else 0.05

            for level, fraction in zip(_SCALE_OUT_LEVELS, _SCALE_OUT_FRACTIONS):
                if level in pos["scale_out_done"]:
                    continue
                if pnl_pct >= level * target_pct:
                    shares_out = pos["shares"] * fraction
                    if shares_out > 0.01:
                        close_dir = "sell" if direction > 0 else "cover"
                        res = self.broker.place_order(ticker, shares_out, close_dir, price)
                        if res.get("status") in ("filled", "submitted"):
                            pos["shares"] -= shares_out
                            pos["scale_out_done"].append(level)
                            logger.info(
                                "%s: scale-out %.0f%% at level %.0f%% target @ %.4f",
                                ticker, fraction * 100, level * 100, price,
                            )

            # ----------------------------------------------------------
            # 3. Volume dry-up exit
            # ----------------------------------------------------------
            vol_data = self._get_recent_volume(ticker, days=25)
            if vol_data is not None and len(vol_data) >= 21:
                avg_vol = float(vol_data.iloc[-21:-1].mean())
                last_vol = float(vol_data.iloc[-1])
                if avg_vol > 0 and last_vol < avg_vol * _VOL_DRY_UP_THRESHOLD:
                    pos["dry_up_days"] = pos.get("dry_up_days", 0) + 1
                    if pos["dry_up_days"] >= _VOL_DRY_UP_DAYS:
                        logger.info(
                            "%s: volume dry-up exit (%.0f vs avg %.0f)",
                            ticker, last_vol, avg_vol,
                        )
                        self._close_position(ticker, reason="volume_dry_up", price=price)
                        continue
                else:
                    pos["dry_up_days"] = 0

            # ----------------------------------------------------------
            # 4. Time-based exit (original exit_date)
            # ----------------------------------------------------------
            exit_date = pos.get("exit_date")
            if exit_date and pd.Timestamp(exit_date) <= today:
                logger.info("%s: time-based exit @ %.4f", ticker, price)
                self._close_position(ticker, reason="time_exit", price=price)

    def _close_position(
        self,
        ticker: str,
        reason: str = "unknown",
        price: Optional[float] = None,
    ) -> None:
        pos = self.active.get(ticker)
        if pos is None:
            return
        try:
            if price is None:
                price = self._get_live_price(ticker)
            positions = self.broker.get_positions()
            if isinstance(positions, list):
                # AlpacaPaperBroker returns a list of dicts with 'symbol' and 'qty'
                shares = 0.0
                for p in positions:
                    if p.get("symbol") == ticker:
                        qty = float(p.get("qty", 0))
                        shares = qty if p.get("side") == "long" else -qty
                        break
            else:
                shares = positions.get(ticker, 0)
            if shares == 0:
                del self.active[ticker]
                return
            close_dir = "sell" if pos["direction"] > 0 else "cover"
            self.broker.place_order(ticker, abs(shares), close_dir, price)
            entry = pos.get("entry_price", price)
            pnl_pct = pos["direction"] * (price - entry) / entry * 100 if entry else 0
            exit_date = datetime.utcnow().isoformat()
            entry_date = pos.get("entry_date", exit_date)
            context_at_open = pos.get("context_at_open", {})
            signal_type = pos.get("signal_type", "pead")

            # Calculate holding days
            try:
                holding_days = (datetime.fromisoformat(exit_date) -
                                datetime.fromisoformat(entry_date)).days
            except Exception:
                holding_days = 0

            return_pct = pnl_pct / 100.0

            # ── Phantom trade detection ───────────────────────────────────
            # A trade is phantom if: closed outside market hours AND
            # (pnl == 0 OR held < 30 minutes).  Phantom trades must never
            # count toward phase progression or trigger cooling-off.
            _market = "uk" if ticker.endswith(".L") else "us"
            _market_was_open = _is_market_open(_market)
            try:
                _held_minutes = (
                    datetime.fromisoformat(exit_date) -
                    datetime.fromisoformat(entry_date)
                ).total_seconds() / 60.0
            except Exception:
                _held_minutes = 0.0
            _is_phantom = (
                not _market_was_open
                and (abs(pnl_pct) < 0.001 or _held_minutes < 30)
            )
            if _is_phantom:
                logger.debug(
                    "Phantom trade detected: %s pnl=%.4f%% held=%.1fmin market_open=%s — "
                    "skipping cooling-off, marking phantom",
                    ticker, pnl_pct, _held_minutes, _market_was_open,
                )

            # Register with cooling-off tracker (skip for phantom trades)
            if self._cooling_off is not None and not _is_phantom:
                try:
                    from datetime import date as _date
                    self._cooling_off.register_exit(
                        ticker=ticker,
                        exit_date=_date.today(),
                        exit_price=price,
                        pnl_pct=return_pct,
                    )
                    if return_pct < 0:
                        _cool_msg = f"COOLING OFF: {ticker} locked 5 days (loss={pnl_pct:.1f}%)"
                        logger.info(_cool_msg)
                        try:
                            from altdata.notifications.notifier import Notifier
                            Notifier(self.config)._send_telegram(_cool_msg)
                        except Exception:
                            pass
                except Exception as _coe:
                    logger.debug("CoolingOff register error: %s", _coe)

            # Record trade close in closeloop_store
            try:
                if self._store is not None:
                    closed_trade = {
                        "ticker": ticker,
                        "market": pos.get("market", "us"),
                        "direction": pos["direction"],
                        "entry_date": entry_date,
                        "exit_date": exit_date,
                        "entry_price": entry,
                        "exit_price": price,
                        "position_size": abs(shares),
                        "gross_pnl": (price - entry) * abs(shares) * pos["direction"],
                        "net_pnl": (price - entry) * abs(shares) * pos["direction"],
                        "holding_days": holding_days,
                        "exit_reason": reason,
                        "sector": pos.get("sector", "Unknown"),
                        "is_phantom": 1 if _is_phantom else 0,
                    }
                    self._store.record_trade(closed_trade, context_at_open)
            except Exception as e:
                logger.warning("Failed to record trade close in closeloop_store: %s", e)

            # Run trade autopsy
            try:
                from closeloop.autopsy.trade_autopsy import TradeAutopsy
                autopsy = TradeAutopsy(store=self._store, config=self.config)
                closed_trade_for_autopsy = {
                    "ticker": ticker,
                    "market": pos.get("market", "us"),
                    "direction": pos["direction"],
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_price": entry,
                    "exit_price": price,
                    "position_size": abs(shares),
                    "gross_pnl": (price - entry) * abs(shares) * pos["direction"],
                    "net_pnl": (price - entry) * abs(shares) * pos["direction"],
                    "holding_days": holding_days,
                    "exit_reason": reason,
                    "sector": pos.get("sector", "Unknown"),
                }
                autopsy.run(closed_trade_for_autopsy, context_at_open)
            except Exception as e:
                logger.warning("Trade autopsy failed for %s: %s", ticker, e)

            # Log trade close to paper_trading.jsonl
            _append_log({
                "type": "trade_close",
                "timestamp": exit_date,
                "ticker": ticker,
                "market": pos.get("market", "us"),
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": entry,
                "exit_price": price,
                "return_pct": return_pct,
                "holding_days": holding_days,
                "signal_type": signal_type,
                "exit_reason": reason,
                "context_at_open": context_at_open,
            })

            logger.info(
                "Closed %s @ %.4f | reason=%s | P&L=%.2f%%", ticker, price, reason, pnl_pct
            )
            del self.active[ticker]
            if self._trailing_stops is not None:
                try:
                    self._trailing_stops.remove_position(ticker)
                except Exception:
                    pass
        except Exception as e:
            logger.error("Close failed for %s: %s", ticker, e)

    # ------------------------------------------------------------------
    # ATR & volume helpers
    # ------------------------------------------------------------------

    def _compute_atr(self, ticker: str, window: int = 14, price_df: pd.DataFrame = None) -> float:
        """Compute Average True Range over `window` days. Uses pre-fetched data if available."""
        try:
            if price_df is not None and not price_df.empty and len(price_df) >= window + 1:
                data = price_df
                # Normalise column names
                col_map = {c.lower(): c for c in data.columns}
                high = data[col_map.get('high', data.columns[1])]
                low  = data[col_map.get('low',  data.columns[2])]
                close= data[col_map.get('close',data.columns[3])]
            else:
                data = yf.download(ticker, period="40d", progress=False, auto_adjust=True)
                if data is None or data.empty or len(data) < window + 1:
                    return 0.0
                high = data["High"]
                low = data["Low"]
                close = data["Close"]
                if isinstance(high, pd.DataFrame):
                    high, low, close = high.iloc[:, 0], low.iloc[:, 0], close.iloc[:, 0]

            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low - close.shift()).abs(),
            ], axis=1).max(axis=1)
            atr = float(tr.rolling(window).mean().iloc[-1])
            return atr if pd.notna(atr) else 0.0
        except Exception:
            return 0.0

    def _get_recent_volume(self, ticker: str, days: int = 25) -> Optional[pd.Series]:
        """Fetch recent volume series."""
        try:
            data = yf.download(ticker, period=f"{days + 5}d", progress=False, auto_adjust=True)
            if data is None or data.empty:
                return None
            vol = data["Volume"]
            if isinstance(vol, pd.DataFrame):
                vol = vol.iloc[:, 0]
            return vol.tail(days)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Correlation management
    # ------------------------------------------------------------------

    def _get_sector(self, ticker: str) -> str:
        """Get sector — uses static map first (fast), fetcher cache second, yfinance last."""
        try:
            from signals.sector_rotation_signal import TICKER_TO_SECTOR
            sector = TICKER_TO_SECTOR.get(ticker.upper())
            if sector:
                return sector
        except Exception:
            pass
        try:
            if self.fetcher:
                info = self.fetcher.fetch_ticker_info(ticker) or {}
                return info.get("sector", "Unknown") or "Unknown"
            return yf.Ticker(ticker).info.get("sector", "Unknown") or "Unknown"
        except Exception:
            return "Unknown"

    def _passes_correlation_check(
        self, ticker: str, direction: int, sector: str
    ) -> bool:
        """
        Returns False if adding this position would violate:
        - Max 3 positions in same sector
        - Max 2 positions with Pearson correlation > 0.6 (vs same direction)
        """
        same_sector = [
            t for t, p in self.active.items()
            if p.get("sector") == sector
        ]
        if len(same_sector) >= _CORRELATION_MAX_SAME_SECTOR:
            logger.debug(
                "%s: sector concentration block — %d positions in %s",
                ticker, len(same_sector), sector,
            )
            return False

        same_dir = [t for t, p in self.active.items() if p.get("direction") == direction]
        if not same_dir:
            return True

        high_corr_count = 0
        try:
            tickers_to_check = same_dir[:10]
            cached = getattr(self, '_scan_price_cache', None)
            corr_matrix = self._compute_correlation_matrix([ticker] + tickers_to_check, cached_prices=cached)
            if corr_matrix is not None and ticker in corr_matrix.columns:
                for other in tickers_to_check:
                    if other in corr_matrix.index:
                        c = corr_matrix.loc[other, ticker]
                        if pd.notna(c) and abs(c) > _CORRELATION_HIGH_THRESHOLD:
                            high_corr_count += 1
        except Exception:
            pass  # fail open

        if high_corr_count >= _CORRELATION_MAX_HIGH_CORR:
            logger.debug(
                "%s: correlation block — %d highly correlated positions (>%.0f%%)",
                ticker, high_corr_count, _CORRELATION_HIGH_THRESHOLD * 100,
            )
            return False

        return True

    def _compute_correlation_matrix(
        self, tickers: List[str], period: str = "60d",
        cached_prices: Dict = None,
    ) -> Optional[pd.DataFrame]:
        """Compute correlation matrix from pre-fetched price data (or download as fallback)."""
        try:
            if len(tickers) < 2:
                return None

            closes = {}
            if cached_prices:
                for t in tickers:
                    df = cached_prices.get(t)
                    if df is not None and not df.empty:
                        col = 'close' if 'close' in df.columns else df.columns[-1]
                        closes[t] = df[col].tail(60)
            if len(closes) < 2:
                data = yf.download(
                    tickers, period=period, progress=False, auto_adjust=True, group_by="ticker"
                )
                if data.empty:
                    return None
                for t in tickers:
                    try:
                        if t in data.columns.get_level_values(0):
                            closes[t] = data[t]["Close"]
                        elif "Close" in data.columns:
                            closes[t] = data["Close"]
                    except Exception:
                        pass

            if len(closes) < 2:
                return None

            df = pd.DataFrame(closes).dropna()
            return df.pct_change().dropna().corr()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Performance logging
    # ------------------------------------------------------------------

    def _log_performance(self) -> None:
        try:
            raw_positions = self.broker.get_positions()
            if isinstance(raw_positions, list):
                tickers = [p.get("symbol", "") for p in raw_positions if isinstance(p, dict)]
            else:
                tickers = list(raw_positions.keys())
            positions = raw_positions
            prices = {}
            for t in tickers:
                try:
                    prices[t] = yf.Ticker(t).fast_info.last_price
                except Exception:
                    pass
            value = self.broker.get_account_value(prices)
            pnl = self.broker.get_pnl(prices)
            entry = {
                "type": "performance",
                "date": datetime.utcnow().isoformat(),
                "account_value": value,
                "pnl": pnl,
                "n_positions": len(positions),
            }
            logger.info("Performance: value=%.2f pnl=%.2f positions=%d", value, pnl, len(positions))
            _append_log(entry)
        except Exception as e:
            logger.error("Performance log failed: %s", e)

    def _portfolio_state(self) -> Dict:
        raw_positions = self.broker.get_positions()
        # Normalise: AlpacaPaperBroker returns a list of dicts; PaperBroker returns dict
        if isinstance(raw_positions, list):
            positions = {
                p.get('symbol', p.get('ticker', '')): float(p.get('market_value', 0))
                for p in raw_positions if isinstance(p, dict)
            }
        else:
            positions = raw_positions or {}
        value = self.broker.get_account_value()
        long_v = sum(v for v in positions.values() if v > 0)
        short_v = sum(abs(v) for v in positions.values() if v < 0)
        net_exp = (long_v - short_v) / max(value, 1)
        initial = getattr(self.broker, "initial_capital", value)
        dd = (value - initial) / initial if initial > 0 else 0

        sector_counts: Dict[str, int] = {}
        for pos in self.active.values():
            s = pos.get("sector", "Unknown")
            sector_counts[s] = sector_counts.get(s, 0) + 1

        return {
            "positions": positions,
            "account_value": value,
            "capital": value,
            "net_exposure": net_exp,
            "drawdown": dd,
            "sectors": sector_counts,
            "sector_exposures": sector_counts,
        }

    # ------------------------------------------------------------------
    # Status helpers (for CLI)
    # ------------------------------------------------------------------

    def get_open_positions_table(self) -> str:
        """Return a formatted table of open positions."""
        if not self.active:
            return "  No open positions."
        lines = [
            f"  {'Ticker':<10} {'Market':<6} {'Direction':<10} {'Entry':>10} "
            f"{'Target':>10} {'Entry Date':<22} {'Sector':<20}"
        ]
        lines.append("  " + "-" * 90)
        for ticker, pos in self.active.items():
            direction_str = "LONG" if pos.get("direction", 1) > 0 else "SHORT"
            lines.append(
                f"  {ticker:<10} {pos.get('market','?'):<6} {direction_str:<10} "
                f"{pos.get('entry_price',0):>10.4f} {pos.get('target_price',0):>10.4f} "
                f"{str(pos.get('entry_date','?'))[:19]:<22} {pos.get('sector','?'):<20}"
            )
        return "\n".join(lines)
