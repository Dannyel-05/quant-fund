"""
SimFin fundamental data collector.
10 years of income statements, balance sheets, cash flows.
Uses the simfin Python library (bulk CSV download) for free tier access.
Used to calculate earnings quality metrics for PEAD signal filtering.
"""
import logging
import sqlite3
import os
from datetime import datetime
from typing import Dict, Optional
import warnings
import pandas as pd

logger = logging.getLogger(__name__)

_SIMFIN_DATA_DIR = '/tmp/simfin_data'


class SimFinCollector:
    DB_PATH = 'output/permanent_archive.db'

    def __init__(self, config: dict):
        self.api_key = config.get('api_keys', {}).get('simfin', '')
        self.enabled = bool(self.api_key) and 'PASTE' not in self.api_key
        self._sf = None
        self._income_us = None
        self._balance_us = None
        self._cashflow_us = None
        if not self.enabled:
            logger.info('SimFinCollector: NO KEY - earnings quality disabled')
        else:
            self._init_simfin()
        self._ensure_db()

    def _init_simfin(self):
        """Initialize simfin library and pre-load bulk datasets."""
        try:
            import simfin as sf
            sf.set_api_key(self.api_key)
            sf.set_data_dir(_SIMFIN_DATA_DIR)
            self._sf = sf
            logger.info('SimFinCollector: ENABLED (loading bulk data...)')
        except ImportError:
            logger.warning('SimFinCollector: simfin package not installed')
            self.enabled = False
        except Exception as e:
            logger.warning('SimFinCollector: init failed: %s', e)
            self.enabled = False

    def _load_income(self, market: str = 'us') -> pd.DataFrame:
        if self._income_us is not None:
            return self._income_us
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                df = self._sf.load_income(variant='annual', market=market)
            self._income_us = df
            return df
        except Exception as e:
            logger.warning('SimFinCollector._load_income: %s', e)
            return pd.DataFrame()

    def _load_balance(self, market: str = 'us') -> pd.DataFrame:
        if self._balance_us is not None:
            return self._balance_us
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                df = self._sf.load_balance(variant='annual', market=market)
            self._balance_us = df
            return df
        except Exception as e:
            logger.warning('SimFinCollector._load_balance: %s', e)
            return pd.DataFrame()

    def _load_cashflow(self, market: str = 'us') -> pd.DataFrame:
        if self._cashflow_us is not None:
            return self._cashflow_us
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                df = self._sf.load_cashflow(variant='annual', market=market)
            self._cashflow_us = df
            return df
        except Exception as e:
            logger.warning('SimFinCollector._load_cashflow: %s', e)
            return pd.DataFrame()

    def _filter_ticker(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter bulk DataFrame for a single ticker."""
        if df.empty:
            return df
        try:
            # SimFin uses MultiIndex (Ticker, Date) or has a Ticker column
            if isinstance(df.index, pd.MultiIndex):
                if ticker in df.index.get_level_values(0):
                    return df.xs(ticker, level=0)
                return pd.DataFrame()
            elif 'Ticker' in df.columns:
                return df[df['Ticker'] == ticker].reset_index(drop=True)
            elif 'Ticker' in df.index.names:
                if ticker in df.index.get_level_values('Ticker'):
                    return df.xs(ticker, level='Ticker')
            return pd.DataFrame()
        except Exception as e:
            logger.debug('SimFinCollector._filter_ticker %s: %s', ticker, e)
            return pd.DataFrame()

    def get_income_statement(self, ticker: str) -> pd.DataFrame:
        if not self.enabled or self._sf is None:
            return pd.DataFrame()
        return self._filter_ticker(self._load_income(), ticker)

    def get_balance_sheet(self, ticker: str) -> pd.DataFrame:
        if not self.enabled or self._sf is None:
            return pd.DataFrame()
        return self._filter_ticker(self._load_balance(), ticker)

    def get_cashflow(self, ticker: str) -> pd.DataFrame:
        if not self.enabled or self._sf is None:
            return pd.DataFrame()
        return self._filter_ticker(self._load_cashflow(), ticker)

    def _ensure_db(self):
        try:
            os.makedirs('output', exist_ok=True)
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''CREATE TABLE IF NOT EXISTS earnings_quality
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ticker TEXT UNIQUE, quality_score REAL, quality_tier TEXT,
                 accruals_ratio REAL, fcf_conversion REAL,
                 revenue_consistency REAL, margin_trend REAL, roic REAL,
                 calculated_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('SimFinCollector DB init: %s', e)

    def calculate_earnings_quality(self, ticker: str) -> Dict:
        default = {'earnings_quality_score': 0.5, 'quality_tier': 'UNKNOWN',
                   'key_metrics': {}, 'quality_reasoning': 'No SimFin data'}
        if not self.enabled or self._sf is None:
            return default
        try:
            income = self.get_income_statement(ticker)
            balance = self.get_balance_sheet(ticker)
            cashflow = self.get_cashflow(ticker)
            if income.empty or balance.empty or cashflow.empty:
                return default

            scores = {}

            # 1. Accruals ratio
            try:
                ni_col = next((c for c in income.columns if 'Net Income' in c), None)
                op_col = next((c for c in cashflow.columns if 'Operating' in c and 'Cash' in c), None)
                ta_col = next((c for c in balance.columns if 'Total Assets' in c), None)
                if ni_col and op_col and ta_col:
                    ni = pd.to_numeric(income[ni_col], errors='coerce').dropna()
                    cf = pd.to_numeric(cashflow[op_col], errors='coerce').dropna()
                    ta = pd.to_numeric(balance[ta_col], errors='coerce').dropna()
                    if len(ni) > 0 and len(cf) > 0 and len(ta) > 0:
                        accruals = float(ni.iloc[-1]) - float(cf.iloc[-1])
                        ratio = abs(accruals) / max(abs(float(ta.iloc[-1])), 1)
                        scores['accruals_ratio'] = ratio
                        scores['accruals_score'] = 0.8 if ratio < 0.05 else (0.3 if ratio > 0.10 else 0.55)
                        scores['fcf_conversion'] = float(cf.iloc[-1]) / max(abs(float(ni.iloc[-1])), 1) if float(ni.iloc[-1]) != 0 else 0
                        fcf = scores['fcf_conversion']
                        scores['fcf_score'] = 0.9 if fcf > 1.0 else (0.3 if fcf < 0.5 else 0.6)
                        scores['roic'] = float(ni.iloc[-1]) / max(abs(float(ta.iloc[-1])), 1)
                        scores['roic_score'] = 0.8 if scores['roic'] > 0.15 else (0.3 if scores['roic'] < 0.05 else 0.55)
                    else:
                        scores['accruals_score'] = scores['fcf_score'] = scores['roic_score'] = 0.5
                else:
                    scores['accruals_score'] = scores['fcf_score'] = scores['roic_score'] = 0.5
            except Exception:
                scores['accruals_score'] = scores['fcf_score'] = scores['roic_score'] = 0.5

            # 3. Revenue consistency
            try:
                rev_col = next((c for c in income.columns if c == 'Revenue' or c.startswith('Revenue')), None)
                if rev_col:
                    rev = pd.to_numeric(income[rev_col], errors='coerce').dropna()
                    if len(rev) >= 4:
                        rev_std = float(rev.pct_change().dropna().std())
                        scores['revenue_consistency'] = rev_std
                        scores['rev_score'] = 0.8 if rev_std < 0.05 else (0.2 if rev_std > 0.20 else 0.5)
                    else:
                        scores['rev_score'] = 0.5
                else:
                    scores['rev_score'] = 0.5
            except Exception:
                scores['rev_score'] = 0.5

            # 4. Gross margin trend
            try:
                gp_col = next((c for c in income.columns if 'Gross Profit' in c), None)
                rev_col2 = next((c for c in income.columns if c == 'Revenue'), None)
                if gp_col and rev_col2:
                    gp = pd.to_numeric(income[gp_col], errors='coerce').dropna()
                    rv = pd.to_numeric(income[rev_col2], errors='coerce').dropna()
                    n = min(len(gp), len(rv))
                    if n >= 2:
                        margins = gp.values[-n:] / rv.values[-n:]
                        trend = margins[-1] - margins[:-1].mean()
                        scores['margin_trend'] = float(trend)
                        scores['margin_score'] = 0.7 if trend > 0 else 0.3
                    else:
                        scores['margin_score'] = 0.5
                else:
                    scores['margin_score'] = 0.5
            except Exception:
                scores['margin_score'] = 0.5

            eq_score = (
                scores.get('accruals_score', 0.5) * 0.30 +
                scores.get('fcf_score', 0.5) * 0.25 +
                scores.get('rev_score', 0.5) * 0.20 +
                scores.get('margin_score', 0.5) * 0.15 +
                scores.get('roic_score', 0.5) * 0.10
            )

            tier = 'HIGH' if eq_score > 0.7 else ('LOW' if eq_score < 0.4 else 'MEDIUM')

            try:
                conn = sqlite3.connect(self.DB_PATH)
                conn.execute('''INSERT OR REPLACE INTO earnings_quality
                    (ticker, quality_score, quality_tier, accruals_ratio,
                     fcf_conversion, revenue_consistency, margin_trend, roic, calculated_at)
                    VALUES (?,?,?,?,?,?,?,?,?)''',
                    (ticker, eq_score, tier,
                     scores.get('accruals_ratio', 0), scores.get('fcf_conversion', 0),
                     scores.get('revenue_consistency', 0), scores.get('margin_trend', 0),
                     scores.get('roic', 0), datetime.now().isoformat()))
                conn.commit()
                conn.close()
            except Exception:
                pass

            return {
                'earnings_quality_score': round(eq_score, 4),
                'quality_tier': tier,
                'key_metrics': scores,
                'quality_reasoning': (
                    f'Accruals:{scores.get("accruals_score",0.5):.2f} '
                    f'FCF:{scores.get("fcf_score",0.5):.2f} '
                    f'Rev:{scores.get("rev_score",0.5):.2f} '
                    f'ROIC:{scores.get("roic",0):.3f}'
                )
            }
        except Exception as e:
            logger.warning('SimFinCollector.calculate_earnings_quality %s: %s', ticker, e)
            return default

    def bulk_quality_scan(self, tickers: list) -> Dict[str, Dict]:
        results = {}
        for ticker in tickers:
            try:
                results[ticker] = self.calculate_earnings_quality(ticker)
            except Exception:
                results[ticker] = {'earnings_quality_score': 0.5, 'quality_tier': 'UNKNOWN'}
        return results
