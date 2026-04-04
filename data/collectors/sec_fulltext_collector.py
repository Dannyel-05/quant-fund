"""
SEC EDGAR Full Text Search collector.
Searches all SEC filings simultaneously for crisis/opportunity keywords.
No API key required.
"""
import logging
import re
import sqlite3
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests

# SEC EFTS display_names format: "COMPANY NAME  (TICK1, TICK2, ...)  (CIK 0000123456)"
# The ticker group appears immediately before the "(CIK ..." parenthetical.
_SEC_TICKER_RE = re.compile(r'\(([A-Z][A-Z0-9\.,\s]{0,40}?)\)\s*\(CIK\s*\d')

logger = logging.getLogger(__name__)

class SECFullTextCollector:
    BASE_URL = 'https://efts.sec.gov/LATEST/search-index'
    # Fallback: use the public EDGAR full-text search if EFTS is unavailable
    FALLBACK_URL = 'https://efts.sec.gov/LATEST/search-index'
    DB_PATH = 'output/permanent_archive.db'
    HEADERS = {
        'User-Agent': 'quant-fund research@quantfund.com',
        'Accept-Encoding': 'gzip, deflate',
    }

    CRISIS_KEYWORDS = {
        'going concern': 'CRITICAL',
        'material weakness': 'HIGH',
        'substantial doubt': 'HIGH',
        'liquidity concerns': 'HIGH',
        'covenant violation': 'HIGH',
        'debt acceleration': 'CRITICAL',
        'unable to continue': 'CRITICAL',
        'supply shortage': 'MEDIUM',
        'factory closure': 'HIGH',
        'regulatory investigation': 'HIGH',
        'SEC investigation': 'CRITICAL',
        'DOJ investigation': 'CRITICAL',
        'class action lawsuit': 'HIGH',
        'accounting restatement': 'CRITICAL',
        'data breach': 'HIGH',
        'product recall': 'HIGH',
        'FDA warning letter': 'HIGH',
        'impairment charge': 'MEDIUM',
        'goodwill impairment': 'HIGH',
        'contract termination': 'HIGH',
    }

    OPPORTUNITY_KEYWORDS = {
        'government contract award': 'HIGH',
        'patent approved': 'MEDIUM',
        'FDA approval': 'CRITICAL',
        'FDA clearance': 'HIGH',
        'partnership agreement': 'MEDIUM',
        'merger agreement': 'HIGH',
        'share repurchase program': 'MEDIUM',
        'special cash dividend': 'HIGH',
        'going private transaction': 'CRITICAL',
        'strategic alternatives': 'MEDIUM',
    }

    def __init__(self):
        self._ensure_db()
        self._universe_tickers = set()
        self._load_universe()

    def _ensure_db(self):
        try:
            os.makedirs('output', exist_ok=True)
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute('''CREATE TABLE IF NOT EXISTS sec_fulltext_alerts
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 keyword TEXT, entity_name TEXT, ticker TEXT,
                 filing_type TEXT, filing_date TEXT,
                 severity TEXT, alert_type TEXT,
                 snippet TEXT, accession_no TEXT,
                 in_universe INTEGER, fetched_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('SECFullTextCollector DB init: %s', e)

    def _load_universe(self):
        # Primary: load from Universe class (covers both US and UK tickers)
        try:
            from data.universe import Universe
            import yaml
            import os
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'settings.yaml')
            if os.path.exists(config_path):
                config = yaml.safe_load(open(config_path))
            else:
                config = {}
            u = Universe(config)
            for t in u.get_us_tickers():
                self._universe_tickers.add(t.upper())
            logger.info('SECFullTextCollector: loaded %d US tickers from Universe', len(self._universe_tickers))
            return
        except Exception as e:
            logger.debug('SECFullTextCollector: Universe class load failed: %s', e)
        # Fallback: CSV files
        try:
            import glob
            import csv
            for f in glob.glob('data/universe/*.csv') + glob.glob('universe/*.csv'):
                with open(f) as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        t = row.get('ticker', row.get('Ticker', ''))
                        if t:
                            self._universe_tickers.add(t.upper())
        except Exception:
            pass

    def in_universe(self, ticker: Optional[str]) -> bool:
        if not ticker:
            return False
        return ticker.upper() in self._universe_tickers

    def search_keyword(self, keyword: str, days_back: int = 3,
                       filing_types: Optional[List[str]] = None) -> List[Dict]:
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            to_date = datetime.now().strftime('%Y-%m-%d')
            params = {
                'q': f'"{keyword}"',
                'dateRange': 'custom',
                'startdt': from_date,
                'enddt': to_date,
            }
            if filing_types:
                params['forms'] = ','.join(filing_types)
            r = requests.get(self.BASE_URL, params=params, headers=self.HEADERS, timeout=30)
            if r.status_code == 429:
                logger.warning('SECFullTextCollector rate-limited (429) for "%s"', keyword)
                time.sleep(5)
                return []
            if r.status_code != 200:
                logger.warning('SECFullTextCollector HTTP %d for "%s"', r.status_code, keyword)
                return []
            hits = r.json().get('hits', {}).get('hits', [])
            results = []
            _debug_sample_logged = False
            for hit in hits:
                src = hit.get('_source', {})
                display_names = src.get('display_names', [])
                entity = display_names[0] if display_names else ''

                # SEC EFTS does not return a dedicated 'ticker' field.
                # Extract ticker(s) from display_names: "COMPANY (TICK1, TICK2) (CIK ...)"
                ticker = None
                extracted_tickers = []
                for dn in display_names:
                    m = _SEC_TICKER_RE.search(dn)
                    if m:
                        for part in m.group(1).split(','):
                            t = part.strip()
                            if re.match(r'^[A-Z][A-Z0-9\.]{0,5}$', t):
                                extracted_tickers.append(t)
                # Use first extracted ticker as primary; check all against universe
                if extracted_tickers:
                    ticker = extracted_tickers[0]
                in_uni = any(self.in_universe(t) for t in extracted_tickers)

                # Debug log once per scan to show sample tickers for mismatch diagnosis
                if not _debug_sample_logged and extracted_tickers:
                    sample_uni = list(self._universe_tickers)[:3]
                    logger.debug(
                        'SEC ticker sample: extracted=%s universe_sample=%s in_universe=%s',
                        extracted_tickers[:3], sample_uni, in_uni,
                    )
                    _debug_sample_logged = True

                snippet = ''
                highlight = hit.get('highlight', {})
                if highlight:
                    for v in highlight.values():
                        if v:
                            snippet = v[0][:200]
                            break
                results.append({
                    'keyword': keyword,
                    'entity_name': entity,
                    'ticker': ticker,
                    'filing_type': src.get('file_type', src.get('form', '')),
                    'filing_date': src.get('file_date', ''),
                    'accession_no': src.get('adsh', ''),
                    'snippet': snippet,
                    'in_universe': in_uni,
                    'fetched_at': datetime.now().isoformat(),
                })
            return results
        except Exception as e:
            logger.warning('SECFullTextCollector.search_keyword "%s": %s', keyword, e)
            return []

    def _store_alerts(self, alerts: List[Dict], alert_type: str):
        try:
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            for a in alerts:
                conn.execute('''INSERT INTO sec_fulltext_alerts
                    (keyword, entity_name, ticker, filing_type, filing_date,
                     severity, alert_type, snippet, accession_no, in_universe, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                    (a.get('keyword', ''), a.get('entity_name', ''),
                     a.get('ticker', ''), a.get('filing_type', ''),
                     a.get('filing_date', ''), a.get('severity', ''),
                     alert_type, a.get('snippet', ''),
                     a.get('accession_no', ''), int(a.get('in_universe', False)),
                     a.get('fetched_at', '')))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('SECFullTextCollector._store_alerts: %s', e)

    def daily_crisis_scan(self) -> List[Dict]:
        alerts = []
        for keyword, severity in self.CRISIS_KEYWORDS.items():
            try:
                results = self.search_keyword(keyword, days_back=3)
                for r in results:
                    r['severity'] = severity
                    alerts.append(r)
            except Exception:
                continue
        self._store_alerts(alerts, 'CRISIS')
        return sorted(alerts, key=lambda x: {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2}.get(x.get('severity', 'MEDIUM'), 2))

    def daily_opportunity_scan(self) -> List[Dict]:
        alerts = []
        for keyword, severity in self.OPPORTUNITY_KEYWORDS.items():
            try:
                results = self.search_keyword(keyword, days_back=3)
                for r in results:
                    r['severity'] = severity
                    alerts.append(r)
            except Exception:
                continue
        self._store_alerts(alerts, 'OPPORTUNITY')
        return alerts

    def run_full_daily_scan(self) -> Dict:
        logger.info('SECFullTextCollector: running full daily scan...')
        crisis = self.daily_crisis_scan()
        opportunity = self.daily_opportunity_scan()
        universe_crisis = [a for a in crisis if a.get('in_universe')]
        universe_opp = [a for a in opportunity if a.get('in_universe')]
        result = {
            'total_crisis': len(crisis),
            'total_opportunity': len(opportunity),
            'universe_crisis': len(universe_crisis),
            'universe_opportunity': len(universe_opp),
            'crisis_alerts': crisis[:20],
            'opportunity_alerts': opportunity[:20],
        }
        logger.info(
            'SECFullTextCollector: crisis=%d opp=%d in_universe_crisis=%d in_universe_opp=%d',
            len(crisis), len(opportunity), len(universe_crisis), len(universe_opp),
        )
        return result
