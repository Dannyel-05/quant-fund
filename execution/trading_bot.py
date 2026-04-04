"""
TradingBot — master continuous trading controller.

Runs UK market scans, US market scans, off-hours data collection,
and morning intelligence briefings on a scheduled basis.
Can be paused, resumed, and stopped cleanly.

Usage:
    python3 main.py bot start              # foreground
    python3 main.py bot start --background # background with logs
    python3 main.py bot stop               # send stop signal
    python3 main.py bot status             # show status
    python3 main.py bot pause / resume     # pause trading
"""
import json
import logging
import os
import signal
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Public-holiday tables (extend annually) ────────────────────────────────
# NYSE closes on these US dates. UK LSE closes on UK dates.
_US_HOLIDAYS_2026 = {
    date(2026,  1,  1),  # New Year's Day
    date(2026,  1, 19),  # MLK Day
    date(2026,  2, 16),  # Presidents' Day
    date(2026,  4,  3),  # Good Friday (NYSE closes)
    date(2026,  5, 25),  # Memorial Day
    date(2026,  6, 19),  # Juneteenth
    date(2026,  7,  3),  # Independence Day (observed, Jul 4 = Saturday)
    date(2026,  9,  7),  # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
}
_UK_HOLIDAYS_2026 = {
    date(2026,  1,  1),  # New Year's Day
    date(2026,  4,  3),  # Good Friday
    date(2026,  4,  6),  # Easter Monday
    date(2026,  5,  4),  # Early May bank holiday
    date(2026,  5, 25),  # Spring bank holiday
    date(2026,  8, 31),  # Summer bank holiday
    date(2026, 12, 25),  # Christmas Day
    date(2026, 12, 28),  # Boxing Day (observed)
}
_MARKET_HOLIDAYS = {'UK': _UK_HOLIDAYS_2026, 'US': _US_HOLIDAYS_2026}


class TradingBot:
    """
    Master trading loop controller with market-hour awareness,
    chunked scanning, and continuous data collection.
    """

    MARKET_SCHEDULE = {
        'UK': {
            'open_hour_gmt': 8, 'open_minute': 0,
            'close_hour_gmt': 16, 'close_minute': 30,
            'scan_times_gmt': [
                (8, 15), (9, 0), (10, 0), (11, 0),
                (12, 0), (13, 0), (14, 0), (15, 0), (16, 15),
            ],
            'currency': 'GBP', 'ticker_suffix': '.L',
        },
        'US': {
            'open_hour_gmt': 14, 'open_minute': 30,
            'close_hour_gmt': 21, 'close_minute': 0,
            'scan_times_gmt': [
                (14, 45), (15, 30), (16, 0), (17, 0),
                (18, 0), (19, 0), (20, 0), (20, 45),
            ],
            'currency': 'USD', 'ticker_suffix': '',
        },
    }

    DATA_COLLECTION_INTERVAL_MINUTES = 30

    def __init__(self, config: dict):
        self.config = config
        self.running = False
        self.paused = False
        self.status_file = Path('output/bot_status.json')
        self.pid_file = Path('output/bot.pid')

        Path('output').mkdir(exist_ok=True)
        Path('logs').mkdir(exist_ok=True)

        # ── Paper trader (self-initialising) ──────────────────────────
        from execution.paper_trader import PaperTrader
        self.trader = PaperTrader(config)

        # ── Data collectors ───────────────────────────────────────────
        self.collectors: Dict = {}
        self._init_collectors()

        # ── Intelligence pipeline ─────────────────────────────────────
        self.pipeline = None
        try:
            from intelligence.daily_pipeline import DailyPipeline
            self.pipeline = DailyPipeline(config)
        except Exception as e:
            logger.warning('DailyPipeline unavailable: %s', e)

        # ── Real-time price stream (Alpaca websocket) ─────────────────
        self.stream_worker = None
        try:
            from execution.alpaca_stream import start_stream
            from data.universe import UniverseManager
            from data.fetcher import DataFetcher
            _fetcher = DataFetcher(config)
            _um = UniverseManager(config, _fetcher)
            _us_tickers = _um._default_tickers('us')
            self.stream_worker = start_stream(config, _us_tickers)
        except Exception as _e:
            logger.warning('AlpacaStream start failed: %s', _e)

        # ── Self-monitoring and health reporting ───────────────────────
        self.monitor_runner = None
        try:
            from monitoring.monitor_runner import start_monitoring
            self.monitor_runner = start_monitoring(config, self.stream_worker)
        except Exception as _e:
            logger.warning('MonitorRunner start failed: %s', _e)

        # ── State tracking ────────────────────────────────────────────
        self.last_uk_scan: Optional[datetime] = None
        self.last_us_scan: Optional[datetime] = None
        self.last_data_collection: Optional[datetime] = None
        self.last_morning_briefing: Optional[datetime] = None
        self.scan_count = 0
        self.errors_today = 0
        self.articles_fetched_today = 0
        self.signals_tickers_today: List[str] = []

        self._save_status('INITIALISED')

    def _init_collectors(self) -> None:
        """Initialise all data collectors, gracefully skipping unavailable ones."""
        # (name, module_path, class_name, init_mode)
        # init_mode: 'config' = pass config dict, 'path' = pass config_path str, 'none' = no args
        collector_specs = [
            ('shipping',    'data.collectors.shipping_intelligence', 'ShippingIntelligence', 'config'),
            ('consumer',    'data.collectors.consumer_intelligence', 'ConsumerIntelligence', 'config'),
            ('geopolitical','data.collectors.geopolitical_collector', 'GeopoliticalCollector', 'config'),
            ('rates',       'data.collectors.rates_credit_collector', 'RatesCreditCollector', 'path'),
            ('commodities', 'data.collectors.commodity_collector', 'CommodityCollector', 'config'),
            ('sec_fulltext','data.collectors.sec_fulltext_collector', 'SECFullTextCollector', 'none'),
            ('alt_quiver',  'data.collectors.alternative_quiver_collector', 'AlternativeQuiverCollector', 'config'),
            ('tech_intel',  'data.collectors.technology_intelligence', 'TechnologyIntelligence', 'config'),
            ('usa_spending','data.collectors.government_data_collector', 'USASpendingCollector', 'config'),
            ('bls',         'data.collectors.government_data_collector', 'BLSCollector', 'config'),
            ('news',        'altdata.collector.news_collector', 'NewsCollector', 'config'),
            ('edgar',       'altdata.collector.sec_edgar_collector', 'SECEdgarCollector', 'config'),
            ('finnhub',     'altdata.collector.finnhub_collector', 'FinnhubCollector', 'config'),
        ]
        import importlib
        for name, module_path, class_name, init_mode in collector_specs:
            try:
                mod = importlib.import_module(module_path)
                cls = getattr(mod, class_name)
                if init_mode == 'config':
                    self.collectors[name] = cls(self.config)
                elif init_mode == 'path':
                    self.collectors[name] = cls('config/settings.yaml')
                else:
                    self.collectors[name] = cls()
            except Exception as e:
                logger.debug('Collector %s unavailable: %s', name, e)

        logger.info('TradingBot: %d/%d collectors loaded', len(self.collectors), len(collector_specs))

        # Startup check: warn about any newly-delisted tickers in active lists
        # so they're caught immediately instead of silently failing each cycle.
        try:
            from data.collectors.technology_intelligence import check_for_delisted
            _watch_lists = {
                'consumer_payment': ['V', 'MA', 'AXP'],
                'tech_reits':       ['EQIX', 'DLR', 'AMT', 'COR'],
                'tech_infra':       ['VRT', 'SMCI', 'IREN'],
                'ev_battery':       ['ALB', 'LAC', 'SQM'],
                'shipping_stocks':  ['BDRY', 'ZIM', 'MATX', 'SBLK', 'EGLE', 'DSX', 'NMM', 'GNK', 'SB'],
            }
            for list_name, tickers in _watch_lists.items():
                check_for_delisted(tickers, label=list_name)
        except Exception as _e:
            logger.debug('check_for_delisted startup scan failed: %s', _e)

    # ------------------------------------------------------------------
    def _save_status(self, status: str, extra: Dict = None) -> None:
        """Persist bot status to output/bot_status.json."""
        try:
            phase_summary = 'unknown'
            if self.trader.sizer:
                phase_summary = self.trader.sizer.get_phase_summary()
            stream_stats = {}
            try:
                from execution.alpaca_stream import get_stream_cache
                stream_stats = get_stream_cache().stats()
            except Exception:
                pass
            data = {
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'running': self.running,
                'paused': self.paused,
                'scan_count': self.scan_count,
                'errors_today': self.errors_today,
                'articles_fetched_today': self.articles_fetched_today,
                'last_uk_scan': self.last_uk_scan.isoformat() if self.last_uk_scan else None,
                'last_us_scan': self.last_us_scan.isoformat() if self.last_us_scan else None,
                'last_data_collection': self.last_data_collection.isoformat() if self.last_data_collection else None,
                'phase': phase_summary,
                'use_alpaca': self.trader.use_alpaca,
                'stream': stream_stats,
            }
            if extra:
                data.update(extra)
            with open(self.status_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.debug('_save_status failed: %s', e)

    def _save_pid(self) -> None:
        try:
            with open(self.pid_file, 'w') as f:
                f.write(str(os.getpid()))
        except Exception:
            pass

    # ------------------------------------------------------------------
    def is_market_open(self, market: str) -> bool:
        now = datetime.utcnow()
        if now.weekday() >= 5:  # weekend
            return False
        today = now.date()
        if today in _MARKET_HOLIDAYS.get(market, set()):
            return False  # public holiday
        sched = self.MARKET_SCHEDULE[market]
        open_t  = now.replace(hour=sched['open_hour_gmt'],  minute=sched['open_minute'],  second=0, microsecond=0)
        close_t = now.replace(hour=sched['close_hour_gmt'], minute=sched['close_minute'], second=0, microsecond=0)
        return open_t <= now <= close_t

    def is_scan_time(self, market: str) -> bool:
        now = datetime.utcnow()
        for hour, minute in self.MARKET_SCHEDULE[market]['scan_times_gmt']:
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if abs((now - target).total_seconds()) <= 90:
                return True
        return False

    def should_run_morning_briefing(self) -> bool:
        now = datetime.utcnow()
        if now.hour == 6 and now.minute < 5:
            if (self.last_morning_briefing is None or
                    (now - self.last_morning_briefing).days >= 1):
                return True
        return False

    def should_collect_data(self) -> bool:
        if self.last_data_collection is None:
            return True
        elapsed = (datetime.now() - self.last_data_collection).total_seconds() / 60
        return elapsed >= self.DATA_COLLECTION_INTERVAL_MINUTES

    # ------------------------------------------------------------------
    def run_data_collection(self) -> Dict:
        """Run all data collectors and fetch articles for signal tickers."""
        print(f'[{datetime.now().strftime("%H:%M")}] Data collection starting '
              f'({len(self.collectors)} collectors)...')
        collected: Dict = {}

        # Collectors that are ticker-based (called in _fetch_articles_for_tickers, not here)
        _TICKER_BASED = {'news', 'edgar', 'finnhub'}

        for name, collector in self.collectors.items():
            try:
                if name == 'sec_fulltext':
                    result = collector.run_full_daily_scan()
                    collected[name] = (result.get('total_crisis', 0) + result.get('total_opportunity', 0)
                                       if isinstance(result, dict) else 1)
                elif name in _TICKER_BASED:
                    # These run per-ticker in _fetch_articles_for_tickers; skip bulk call
                    collected[name] = 0
                elif name == 'tech_intel' and hasattr(collector, 'collect_all'):
                    result = collector.collect_all()
                    total = sum(v.get('rows', 0) for v in result.values() if isinstance(v, dict))
                    collected[name] = total
                elif name == 'bls' and hasattr(collector, 'collect_all_series'):
                    result = collector.collect_all_series()
                    collected[name] = len(result) if result else 0
                elif name == 'usa_spending' and hasattr(collector, 'get_recent_all_awards'):
                    result = collector.get_recent_all_awards()
                    collected[name] = len(result) if result else 0
                elif name == 'alt_quiver' and hasattr(collector, 'get_senate_trades'):
                    senate = collector.get_senate_trades(days_back=30)
                    house = collector.get_house_trades(days_back=30)
                    collected[name] = len(senate) + len(house)
                elif hasattr(collector, 'collect'):
                    result = collector.collect(market='us')
                    collected[name] = len(result) if result else 0
                elif hasattr(collector, 'run'):
                    collector.run()
                    collected[name] = 1
                else:
                    collected[name] = 0
            except Exception as e:
                collected[name] = f'ERR:{str(e)[:30]}'
                try:
                    from monitoring.alert_monitor import check_collector_failures
                    check_collector_failures(self.config, name, str(e)[:120])
                except Exception:
                    pass
            else:
                try:
                    from monitoring.alert_monitor import reset_collector_ok
                    reset_collector_ok(name)
                except Exception:
                    pass

        # ── Article collection for signal tickers ────────────────────
        articles_fetched = 0
        if self.signals_tickers_today:
            priority_tickers = list(set(self.signals_tickers_today))[:100]
            articles_fetched = self._fetch_articles_for_tickers(priority_tickers)
            self.articles_fetched_today += articles_fetched

        self.last_data_collection = datetime.now()
        ok_count = sum(1 for v in collected.values() if isinstance(v, int))
        total_records = sum(v for v in collected.values() if isinstance(v, int))
        print(f'  Collectors: {ok_count}/{len(collected)} OK | '
              f'Records: {total_records} | '
              f'Articles fetched: {articles_fetched}')
        return collected

    def _fetch_articles_for_tickers(self, tickers: List[str]) -> int:
        """Fetch news articles for a list of tickers. Returns article count."""
        fetched = 0
        news_col = self.collectors.get('news')
        finnhub_col = self.collectors.get('finnhub')
        edgar_col = self.collectors.get('edgar')

        for ticker in tickers[:50]:  # cap at 50 to avoid slowdown
            try:
                if news_col and hasattr(news_col, 'collect'):
                    result = news_col.collect([ticker], 'us', self.config)
                    if result:
                        fetched += len(result) if isinstance(result, list) else 1
            except Exception:
                pass
            try:
                if finnhub_col and hasattr(finnhub_col, 'get_company_news'):
                    news = finnhub_col.get_company_news(ticker)
                    if news:
                        fetched += len(news)
            except Exception:
                pass
            try:
                if edgar_col and hasattr(edgar_col, 'collect'):
                    filings = edgar_col.collect([ticker], 'us', self.config)
                    if filings:
                        fetched += len(filings) if isinstance(filings, list) else 1
            except Exception:
                pass

        return fetched

    def run_morning_briefing(self) -> None:
        """Run the morning intelligence briefing."""
        print(f'[{datetime.now().strftime("%H:%M")}] Morning briefing...')
        try:
            if self.pipeline:
                self.pipeline.run_macro_briefing()
            self.last_morning_briefing = datetime.now()
            print('  Morning briefing complete')
        except Exception as e:
            print(f'  Briefing failed: {e}')

    # ------------------------------------------------------------------
    def run_market_scan(self, market: str) -> Dict:
        """Run a full chunked market scan."""
        if self.paused:
            print(f'Bot paused — skipping {market} scan')
            return {}

        print(f'\n{"="*55}')
        print(f'[{datetime.now().strftime("%H:%M:%S")}] '
              f'{market.upper()} SCAN #{self.scan_count + 1}')
        if self.trader.sizer:
            print(f'Phase: {self.trader.sizer.get_phase_summary()}')
        print(f'{"="*55}')

        result = {}
        try:
            actions = self.trader.run_scan(market=market.lower())
            self.scan_count += 1

            if market.upper() == 'UK':
                self.last_uk_scan = datetime.now()
            else:
                self.last_us_scan = datetime.now()

            # Summarise result for status/logging (guard against non-dict items)
            opened = sum(1 for a in actions if isinstance(a, dict) and a.get('type') == 'trade_open')
            signal_tickers = list({a.get('ticker') for a in actions if isinstance(a, dict) and a.get('ticker')})
            result = {'positions_opened': opened, 'signal_tickers': signal_tickers, 'total_actions': len(actions)}

            if signal_tickers:
                self.signals_tickers_today.extend(signal_tickers)
                self.signals_tickers_today = self.signals_tickers_today[-500:]

            # Auto-trigger symbolic regression at milestones
            if self.trader.sizer:
                try:
                    self.trader.sizer.auto_trigger_discovery()
                except Exception:
                    pass

            self._save_status('RUNNING', {'last_scan_market': market,
                                           'last_scan_positions_opened': opened})

        except Exception as e:
            self.errors_today += 1
            print(f'  Scan error: {e}')
            import traceback
            traceback.print_exc()

        return result

    # ------------------------------------------------------------------
    def _check_open_positions(self) -> None:
        """Periodic open position check."""
        try:
            if self.trader.closeloop:
                positions = self.trader.closeloop.get_open_positions()
            elif self.trader.broker:
                positions = self.trader.broker.get_positions()
                positions = [{'ticker': k, 'shares': v} for k, v in positions.items()]
            else:
                positions = []
            if positions:
                equity = (self.trader.closeloop.get_paper_equity()
                          if self.trader.closeloop else 100000)
                logger.info('[%s] Open positions: %d',
                            datetime.now().strftime('%H:%M'), len(positions))
                self.trader.check_exit_conditions(positions, equity)
        except Exception as e:
            logger.debug('_check_open_positions: %s', e)

    # ------------------------------------------------------------------
    def run_continuous(self) -> None:
        """Main loop — runs until stopped or interrupted."""
        self.running = True
        self._save_pid()
        self._save_status('STARTING')

        print('=' * 60)
        print('TRADING BOT STARTING')
        print('=' * 60)
        sizer_summary = self.trader.sizer.get_phase_summary() if self.trader.sizer else 'no sizer'
        stream_live = self.stream_worker is not None and self.stream_worker.is_alive()
        print(f'Phase:  {sizer_summary}')
        print(f'Alpaca: {"CONNECTED" if self.trader.use_alpaca else "SIMULATION"}')
        print(f'Stream: {"LIVE (background thread)" if stream_live else "DISCONNECTED (using yfinance fallback)"}')
        print(f'Collectors: {len(self.collectors)} loaded')
        print(f'Press Ctrl+C to stop cleanly')
        print('=' * 60)

        def _handle_sigterm(signum, frame):
            print('\nSIGTERM received — stopping cleanly...')
            self.running = False

        try:
            signal.signal(signal.SIGTERM, _handle_sigterm)
        except Exception:
            pass

        last_check_minute = -1

        while self.running:
            try:
                now = datetime.utcnow()
                current_minute = now.minute

                if current_minute == last_check_minute:
                    time.sleep(5)
                    continue
                last_check_minute = current_minute

                # Morning briefing 06:00 GMT
                if self.should_run_morning_briefing():
                    self.run_morning_briefing()

                # UK market scans
                if self.is_market_open('UK') and self.is_scan_time('UK'):
                    self.run_market_scan('UK')

                # US market scans
                if self.is_market_open('US') and self.is_scan_time('US'):
                    self.run_market_scan('US')

                # Data collection every 30 min
                if self.should_collect_data():
                    self.run_data_collection()

                # Position check every 15 min
                if now.minute % 15 == 0:
                    self._check_open_positions()

                # Reset daily counters at midnight
                if now.hour == 0 and now.minute == 0:
                    self.errors_today = 0
                    self.articles_fetched_today = 0
                    self.signals_tickers_today = []

                time.sleep(10)

            except KeyboardInterrupt:
                print('\nCtrl+C — stopping cleanly...')
                self.running = False
                break
            except Exception as e:
                print(f'Bot loop error: {e}')
                self.errors_today += 1
                time.sleep(30)

        self._save_status('STOPPED')
        print('Bot stopped.')

    # ------------------------------------------------------------------
    def pause(self) -> None:
        self.paused = True
        self._save_status('PAUSED')
        print('Bot paused. Data collection continues.')

    def resume(self) -> None:
        self.paused = False
        self._save_status('RUNNING')
        print('Bot resumed.')

    def stop(self) -> None:
        self.running = False
        self._save_status('STOPPED')
        print('Bot stopping...')
