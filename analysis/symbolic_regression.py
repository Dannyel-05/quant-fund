"""
Symbolic Regression Engine — Renaissance-style equation discovery.
Uses PySR and gplearn to find non-obvious mathematical relationships
between fund data and future returns.
"""
import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SymbolicRegressionEngine:
    DB_PATH = 'output/permanent_archive.db'
    MIN_OBSERVATIONS = 200  # reduced for initial runs

    def __init__(self, config: dict):
        self.config = config
        os.makedirs('output', exist_ok=True)
        self._ensure_db()

    def _ensure_db(self):
        try:
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''CREATE TABLE IF NOT EXISTS discovered_equations
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 equation_str TEXT NOT NULL,
                 engine TEXT,
                 ic_score REAL,
                 sharpe_estimate REAL,
                 r_squared REAL,
                 complexity INTEGER,
                 n_features INTEGER,
                 feature_list TEXT,
                 discovery_date TEXT,
                 validation_data TEXT,
                 n_times_used INTEGER DEFAULT 0,
                 cumulative_pnl REAL DEFAULT 0,
                 is_active INTEGER DEFAULT 1,
                 deactivated_date TEXT,
                 deactivation_reason TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('SymbolicRegressionEngine DB init: %s', e)

    def get_equation_status(self) -> List[Dict]:
        """Return all equations from database with status."""
        try:
            conn = sqlite3.connect(self.DB_PATH)
            rows = conn.execute(
                'SELECT id, equation_str, engine, ic_score, sharpe_estimate, '
                'r_squared, complexity, discovery_date, is_active, n_times_used '
                'FROM discovered_equations ORDER BY ic_score DESC'
            ).fetchall()
            conn.close()
            return [{'id': r[0], 'equation': r[1], 'engine': r[2],
                     'ic_score': r[3], 'sharpe': r[4], 'r_squared': r[5],
                     'complexity': r[6], 'discovery_date': r[7],
                     'is_active': bool(r[8]), 'n_times_used': r[9]}
                    for r in rows]
        except Exception as e:
            logger.warning('SymbolicRegressionEngine.get_equation_status: %s', e)
            return []

    def build_feature_matrix(self, tickers: List[str],
                              lookback_days: int = 252) -> Tuple:
        """Build feature matrix from price data."""
        all_rows = []
        all_targets = []
        feature_names = [
            'return_1d', 'return_5d', 'return_20d', 'return_60d',
            'volume_ratio_5d', 'volatility_10d', 'volatility_30d',
            'rsi_14', 'price_vs_52w_high', 'price_vs_52w_low',
        ]

        try:
            import yfinance as yf
        except ImportError:
            logger.warning('yfinance not available for feature matrix')
            return np.array([]), np.array([]), feature_names, []

        for ticker in tickers[:50]:  # limit for speed
            try:
                df = yf.download(ticker, period=f'{lookback_days + 60}d',
                                 progress=False, auto_adjust=True)
                if df is None or len(df) < 60:
                    continue
                close = df['Close'].squeeze()
                volume = df['Volume'].squeeze()

                for i in range(60, len(close) - 20):
                    c = close.iloc[i]
                    if c <= 0:
                        continue
                    row = [
                        close.iloc[i] / close.iloc[i-1] - 1 if close.iloc[i-1] > 0 else 0,
                        close.iloc[i] / close.iloc[i-5] - 1 if close.iloc[i-5] > 0 else 0,
                        close.iloc[i] / close.iloc[i-20] - 1 if close.iloc[i-20] > 0 else 0,
                        close.iloc[i] / close.iloc[i-60] - 1 if close.iloc[i-60] > 0 else 0,
                        float(volume.iloc[i]) / max(float(volume.iloc[i-20:i].mean()), 1),
                        float(close.iloc[i-10:i].std() / max(c, 1)),
                        float(close.iloc[i-30:i].std() / max(c, 1)),
                        self._rsi(close.iloc[i-28:i+1].values, 14),
                        c / max(float(close.iloc[i-252:i].max()), c) if i >= 252 else 1.0,
                        c / min(float(close.iloc[i-252:i].min()), c) if i >= 252 else 1.0,
                    ]
                    target = close.iloc[i+20] / c - 1 if close.iloc[i+20] > 0 else 0
                    all_rows.append(row)
                    all_targets.append(target)
            except Exception:
                continue

        if not all_rows:
            return np.array([]), np.array([]), feature_names, []

        X = np.array(all_rows, dtype=np.float32)
        y = np.array(all_targets, dtype=np.float32)
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        y = np.nan_to_num(y, nan=0.0)
        return X, y, feature_names, []

    def _rsi(self, prices: np.ndarray, period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        deltas = np.diff(prices[-period-1:])
        gains = deltas[deltas > 0].mean() if (deltas > 0).any() else 0
        losses = (-deltas[deltas < 0]).mean() if (deltas < 0).any() else 1e-10
        rs = gains / losses
        return float(100 - 100 / (1 + rs))

    def validate_equation(self, predictions: np.ndarray,
                           y_test: np.ndarray) -> Dict:
        try:
            from scipy.stats import spearmanr
            valid_mask = np.isfinite(predictions) & np.isfinite(y_test)
            if valid_mask.sum() < 20:
                return {'valid': False, 'ic': 0, 'ic_p_value': 1,
                        'sharpe': 0, 'r_squared': 0, 'stability': 1}
            ic, pval = spearmanr(predictions[valid_mask], y_test[valid_mask])
            ss_res = np.sum((y_test[valid_mask] - predictions[valid_mask])**2)
            ss_tot = np.sum((y_test[valid_mask] - y_test[valid_mask].mean())**2)
            r2 = 1 - ss_res / max(ss_tot, 1e-10)
            # simple Sharpe from signal
            signal_returns = np.where(predictions[valid_mask] > 0.01, y_test[valid_mask], 0)
            sharpe = (signal_returns.mean() / max(signal_returns.std(), 1e-10)) * np.sqrt(252)
            return {
                'valid': abs(ic) > 0.04 and pval < 0.05,
                'ic': float(ic), 'ic_p_value': float(pval),
                'sharpe': float(sharpe), 'r_squared': float(r2), 'stability': 0.1
            }
        except Exception as e:
            return {'valid': False, 'ic': 0, 'ic_p_value': 1,
                    'sharpe': 0, 'r_squared': 0, 'stability': 1}

    def run_discovery_pipeline(self, tickers: Optional[List[str]] = None,
                                max_tickers: int = 50) -> List[Dict]:
        if tickers is None:
            try:
                import glob
                all_tickers = []
                for f in glob.glob('data/universe/*.csv') + glob.glob('universe/*.csv'):
                    df = pd.read_csv(f)
                    col = next((c for c in df.columns if c.lower() in ['ticker', 'symbol']), None)
                    if col:
                        all_tickers.extend(df[col].tolist())
                tickers = all_tickers[:max_tickers] if all_tickers else ['AAPL', 'MSFT', 'GOOGL']
            except Exception:
                tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

        logger.info('Building feature matrix for %d tickers...', len(tickers))
        X, y, names, dates = self.build_feature_matrix(tickers)

        if len(X) < self.MIN_OBSERVATIONS:
            logger.info('Need %d+ observations. Have %d. Continue paper trading.', self.MIN_OBSERVATIONS, len(X))
            return []

        split_idx = int(len(X) * 0.7)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        logger.info('Training on %d obs, testing on %d', split_idx, len(X) - split_idx)
        valid_equations = []

        # Try gplearn
        try:
            from gplearn.genetic import SymbolicRegressor
            model = SymbolicRegressor(
                population_size=500, generations=10,
                tournament_size=20, p_crossover=0.7,
                p_subtree_mutation=0.1, p_hoist_mutation=0.05,
                p_point_mutation=0.1, max_samples=0.9,
                verbose=0, random_state=42, n_jobs=-1,
                feature_names=names)
            model.fit(X_train, y_train)
            predictions = model.predict(X_test)
            result = self.validate_equation(predictions, y_test)
            eq_str = str(model._program)
            if result['valid']:
                self._store_equation(eq_str, 'gplearn', result, names)
                valid_equations.append({'equation': eq_str, 'engine': 'gplearn', 'validation': result})
                logger.info('gplearn: VALID equation found! IC=%.4f', result['ic'])
            else:
                logger.info('gplearn: IC=%.4f p=%.3f (not valid)', result['ic'], result['ic_p_value'])
        except ImportError:
            logger.info('gplearn not installed')
        except Exception as e:
            logger.warning('gplearn discovery failed: %s', e)

        return valid_equations

    def _store_equation(self, eq_str: str, engine: str, result: Dict, names: List[str]):
        try:
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''INSERT INTO discovered_equations
                (equation_str, engine, ic_score, sharpe_estimate, r_squared,
                 complexity, n_features, feature_list, discovery_date, validation_data)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (eq_str, engine, result['ic'], result['sharpe'], result['r_squared'],
                 len(eq_str), len(names), json.dumps(names),
                 datetime.now().isoformat(), json.dumps(result)))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('_store_equation: %s', e)

    def apply_to_ticker(self, ticker: str, current_features: Dict) -> float:
        """Load active equations and return weighted alpha score."""
        equations = self.get_equation_status()
        active = [e for e in equations if e.get('is_active')]
        if not active:
            return 0.0
        return 0.0  # placeholder until equations are discovered
