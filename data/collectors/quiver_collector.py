"""
QuiverQuantitative data collector.
Congressional trading, government contracts, lobbying, WSB mentions, patents.
Falls back gracefully if no API key.
"""
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
import pandas as pd

logger = logging.getLogger(__name__)

class QuiverCollector:
    BASE = 'https://api.quiverquant.com/beta'
    DB_PATH = 'output/permanent_archive.db'

    def __init__(self, config: dict):
        self.api_key = config.get('api_keys', {}).get('quiver_quant', '')
        self.enabled = bool(self.api_key) and 'PASTE' not in self.api_key
        self.headers = {'Authorization': f'Token {self.api_key}'}
        if not self.enabled:
            logger.info('QuiverCollector: NO KEY - congressional signals disabled')
        else:
            logger.info('QuiverCollector: ENABLED')
        self._ensure_db()

    def _ensure_db(self):
        try:
            import os
            os.makedirs('output', exist_ok=True)
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''CREATE TABLE IF NOT EXISTS raw_congressional_trades
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ticker TEXT, politician TEXT, transaction_date TEXT,
                 disclosure_date TEXT, amount TEXT, transaction_type TEXT,
                 days_to_disclose INTEGER, urgency_flag INTEGER,
                 politician_score REAL, fetched_at TEXT)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS raw_government_contracts
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ticker TEXT, amount REAL, contract_pct_of_mcap REAL,
                 contract_signal TEXT, signal_score REAL, description TEXT,
                 award_date TEXT, fetched_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('QuiverCollector DB init: %s', e)

    def get_congressional_trades(self, ticker: Optional[str] = None) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            if ticker:
                url = f'{self.BASE}/historical/congresstrading/{ticker}'
            else:
                url = f'{self.BASE}/live/congresstrading'
            r = requests.get(url, headers=self.headers, timeout=30)
            if r.status_code != 200:
                return []
            trades = r.json() if isinstance(r.json(), list) else []
            enriched = []
            for t in trades:
                disc = t.get('DisclosureDate', '') or ''
                txn = t.get('TransactionDate', '') or ''
                days = 999
                urgency = False
                try:
                    d1 = datetime.strptime(disc[:10], '%Y-%m-%d')
                    d2 = datetime.strptime(txn[:10], '%Y-%m-%d')
                    days = (d1 - d2).days
                    urgency = days < 10
                except Exception:
                    pass
                score = 0.5
                if urgency:
                    score += 0.2
                enriched.append({**t, 'days_to_disclose': days,
                                 'urgency_flag': urgency,
                                 'politician_score': score})
            return enriched
        except Exception as e:
            logger.warning('QuiverCollector.get_congressional_trades: %s', e)
            return []

    def get_government_contracts(self, ticker: Optional[str] = None) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            if ticker:
                url = f'{self.BASE}/historical/govcontracts/{ticker}'
            else:
                url = f'{self.BASE}/live/govcontracts'
            r = requests.get(url, headers=self.headers, timeout=30)
            if r.status_code != 200:
                return []
            contracts = r.json() if isinstance(r.json(), list) else []
            result = []
            for c in contracts:
                amount = float(c.get('Amount', 0) or 0)
                mcap = 1e9  # default
                try:
                    import yfinance as yf
                    tkr = c.get('Ticker', '')
                    if tkr:
                        info = yf.Ticker(tkr).fast_info
                        mcap = getattr(info, 'market_cap', 1e9) or 1e9
                except Exception:
                    pass
                pct = amount / mcap if mcap > 0 else 0
                if pct > 0.5:
                    sig, score = 'VERY_STRONG', 0.9
                elif pct > 0.1:
                    sig, score = 'STRONG', 0.7
                elif pct > 0.02:
                    sig, score = 'MODERATE', 0.4
                else:
                    sig, score = 'WEAK', 0.1
                result.append({**c, 'contract_pct_of_mcap': pct,
                               'ContractSignal': sig, 'signal_score': score})
            return result
        except Exception as e:
            logger.warning('QuiverCollector.get_government_contracts: %s', e)
            return []

    def get_wsb_mentions(self, ticker: Optional[str] = None) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            if ticker:
                url = f'{self.BASE}/historical/wallstreetbets/{ticker}'
            else:
                url = f'{self.BASE}/live/wallstreetbets'
            r = requests.get(url, headers=self.headers, timeout=30)
            if r.status_code != 200:
                return []
            data = r.json() if isinstance(r.json(), list) else []
            if not data:
                return []
            mentions = [float(d.get('Mentions', 0) or 0) for d in data]
            recent = mentions[-7:] if len(mentions) >= 7 else mentions
            hist_avg = sum(mentions) / len(mentions) if mentions else 1
            recent_avg = sum(recent) / len(recent) if recent else 0
            velocity = recent_avg / max(hist_avg, 1)
            if velocity > 10:
                mtype = 'EXTREME'
            elif velocity > 5:
                mtype = 'HIGH'
            elif velocity > 2:
                mtype = 'ELEVATED'
            elif velocity < 0.5:
                mtype = 'FADING'
            else:
                mtype = 'NORMAL'
            return [{**d, 'mention_velocity': velocity,
                    'MomentumType': mtype} for d in data]
        except Exception as e:
            logger.warning('QuiverCollector.get_wsb_mentions: %s', e)
            return []

    def get_lobbying(self, ticker: Optional[str] = None) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            url = (f'{self.BASE}/historical/lobbying/{ticker}'
                   if ticker else f'{self.BASE}/live/lobbying')
            r = requests.get(url, headers=self.headers, timeout=30)
            return r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
        except Exception as e:
            logger.warning('QuiverCollector.get_lobbying: %s', e)
            return []

    def get_patents(self, ticker: str) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            r = requests.get(f'{self.BASE}/historical/patents/{ticker}',
                           headers=self.headers, timeout=30)
            return r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
        except Exception as e:
            logger.warning('QuiverCollector.get_patents: %s', e)
            return []
