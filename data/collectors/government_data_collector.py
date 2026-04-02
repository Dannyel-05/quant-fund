"""
Government data collectors: BLS, Census Bureau, USASpending.gov
All free, no API key required.
"""
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class BLSCollector:
    BASE = 'https://api.bls.gov/publicAPI/v2'
    DB_PATH = 'output/permanent_archive.db'

    CPI_SERIES = {
        'CUUR0000SA0': 'CPI All Items',
        'CUUR0000SAF1': 'CPI Food at Home',
        'CUUR0000SAE': 'CPI Energy',
        'CUUR0000SAH1': 'CPI Shelter',
    }
    PPI_SERIES = {
        'WPS00000000': 'PPI All Commodities',
        'WPS0561': 'PPI Crude Oil',
        'WPS1017': 'PPI Iron and Steel',
    }
    EMPLOYMENT_SERIES = {
        'CES0000000001': 'Total Nonfarm Payrolls',
        'CES3000000001': 'Manufacturing Employment',
    }

    def __init__(self):
        os.makedirs('output', exist_ok=True)
        self._ensure_db()

    def _ensure_db(self):
        try:
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''CREATE TABLE IF NOT EXISTS bls_data
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 series_id TEXT, series_name TEXT, date TEXT,
                 value REAL, yoy_change REAL, fetched_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('BLSCollector DB init: %s', e)

    def fetch_series(self, series_ids: List[str], years_back: int = 5) -> Dict:
        start_year = str(datetime.now().year - years_back)
        end_year = str(datetime.now().year)
        results = {}
        # batch 25 at a time
        for i in range(0, len(series_ids), 25):
            batch = series_ids[i:i+25]
            try:
                r = requests.post(
                    f'{self.BASE}/timeseries/data/',
                    json={'seriesid': batch, 'startyear': start_year,
                          'endyear': end_year},
                    timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if data.get('status') == 'REQUEST_SUCCEEDED':
                        for series in data.get('Results', {}).get('series', []):
                            results[series['seriesID']] = series.get('data', [])
            except Exception as e:
                logger.warning('BLSCollector.fetch_series: %s', e)
        return results

    def collect_all_series(self) -> Dict:
        all_ids = (list(self.CPI_SERIES.keys()) +
                   list(self.PPI_SERIES.keys()) +
                   list(self.EMPLOYMENT_SERIES.keys()))
        all_names = {**self.CPI_SERIES, **self.PPI_SERIES, **self.EMPLOYMENT_SERIES}
        logger.info('BLSCollector: fetching %d series', len(all_ids))
        data = self.fetch_series(all_ids)
        try:
            conn = sqlite3.connect(self.DB_PATH)
            for sid, obs_list in data.items():
                name = all_names.get(sid, sid)
                for obs in obs_list:
                    try:
                        date_str = f"{obs.get('year', '')}-{obs.get('period', 'M01').replace('M','').zfill(2)}-01"
                        val = float(obs.get('value', 0))
                        conn.execute('''INSERT OR REPLACE INTO bls_data
                            (series_id, series_name, date, value, fetched_at)
                            VALUES (?,?,?,?,?)''',
                            (sid, name, date_str, val, datetime.now().isoformat()))
                    except Exception:
                        continue
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('BLSCollector.collect_all_series store: %s', e)
        return data


class CensusCollector:
    DB_PATH = 'output/permanent_archive.db'

    def __init__(self):
        os.makedirs('output', exist_ok=True)

    def get_building_permits(self) -> pd.DataFrame:
        try:
            r = requests.get(
                'https://api.census.gov/data/timeseries/eits/bps'
                '?get=cell_value,time_slot_id,seasonally_adj,error_data'
                '&for=us:*&time=from+2022-01',
                timeout=30)
            if r.status_code == 200:
                data = r.json()
                if len(data) > 1:
                    cols = data[0]
                    rows = data[1:]
                    return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            logger.warning('CensusCollector.get_building_permits: %s', e)
        return pd.DataFrame()

    def get_retail_sales(self) -> pd.DataFrame:
        try:
            r = requests.get(
                'https://api.census.gov/data/timeseries/eits/mrts'
                '?get=cell_value,time_slot_id,seasonally_adj,category_code'
                '&for=us:*&time=from+2022-01',
                timeout=30)
            if r.status_code == 200:
                data = r.json()
                if len(data) > 1:
                    return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            logger.warning('CensusCollector.get_retail_sales: %s', e)
        return pd.DataFrame()


class USASpendingCollector:
    BASE = 'https://api.usaspending.gov/api/v2'
    DB_PATH = 'output/permanent_archive.db'

    def __init__(self):
        os.makedirs('output', exist_ok=True)

    def get_recent_all_awards(self, min_amount: float = 5_000_000) -> List[Dict]:
        try:
            start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            end = datetime.now().strftime('%Y-%m-%d')
            r = requests.post(
                f'{self.BASE}/search/spending_by_award/',
                json={
                    'filters': {
                        'award_type_codes': ['A', 'B', 'C', 'D'],
                        'award_amounts': [{'lower_bound': min_amount, 'upper_bound': 10_000_000_000}],
                        'time_period': [{'start_date': start, 'end_date': end}]
                    },
                    'fields': ['Recipient Name', 'Award Amount', 'Awarding Agency', 'Start Date', 'Description'],
                    'sort': 'Award Amount',
                    'order': 'desc',
                    'limit': 100
                }, timeout=30)
            if r.status_code == 200:
                return r.json().get('results', [])
        except Exception as e:
            logger.warning('USASpendingCollector.get_recent_all_awards: %s', e)
        return []

    def search_contracts(self, company_name: str,
                         start_date: str = '2023-01-01') -> List[Dict]:
        try:
            r = requests.post(
                f'{self.BASE}/search/spending_by_award/',
                json={
                    'filters': {
                        'award_type_codes': ['A', 'B', 'C', 'D'],
                        'recipient_search_text': [company_name],
                        'time_period': [{'start_date': start_date,
                                         'end_date': datetime.now().strftime('%Y-%m-%d')}]
                    },
                    'fields': ['Award ID', 'Recipient Name', 'Award Amount',
                               'Awarding Agency', 'Start Date', 'Description'],
                    'sort': 'Award Amount',
                    'order': 'desc',
                    'limit': 100
                }, timeout=30)
            return r.json().get('results', []) if r.status_code == 200 else []
        except Exception as e:
            logger.warning('USASpendingCollector.search_contracts: %s', e)
            return []
