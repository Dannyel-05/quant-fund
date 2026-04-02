"""Threshold optimizer - learns optimal signal thresholds from trade outcomes."""
import logging
import sqlite3
import os
from typing import Dict, List

logger = logging.getLogger(__name__)


class ThresholdOptimizer:
    DB_PATH = 'output/permanent_archive.db'

    def __init__(self, config: dict):
        self.config = config
        self._ensure_db()

    def _ensure_db(self):
        try:
            os.makedirs('output', exist_ok=True)
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''CREATE TABLE IF NOT EXISTS threshold_history
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 signal_type TEXT, threshold REAL, win_rate REAL,
                 avg_return REAL, n_trades INTEGER, updated_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('ThresholdOptimizer DB: %s', e)

    def get_optimal_threshold(self, signal_type: str) -> float:
        """Return learned optimal threshold for signal type."""
        try:
            conn = sqlite3.connect(self.DB_PATH)
            row = conn.execute(
                'SELECT threshold FROM threshold_history WHERE signal_type=? '
                'ORDER BY updated_at DESC LIMIT 1', (signal_type,)).fetchone()
            conn.close()
            if row:
                return float(row[0])
        except Exception:
            pass
        # Defaults
        defaults = {
            'PEAD': 0.08, 'MOMENTUM': 0.05, 'MEAN_REVERSION': 0.15,
            'GAP': 0.02, 'MATHEMATICAL': 0.30, 'INSIDER_MOMENTUM': 0.5
        }
        return defaults.get(signal_type, 0.1)

    def update_threshold(self, signal_type: str, outcomes: List[Dict]):
        """Update threshold based on recent trade outcomes."""
        if not outcomes:
            return
        try:
            from datetime import datetime
            wins = sum(1 for o in outcomes if o.get('pnl', 0) > 0)
            win_rate = wins / len(outcomes)
            avg_return = sum(o.get('pnl', 0) for o in outcomes) / len(outcomes)
            # Simple optimization: tighten threshold if win rate < 50%
            current = self.get_optimal_threshold(signal_type)
            if win_rate < 0.45:
                new_threshold = min(current * 1.1, 0.95)
            elif win_rate > 0.65:
                new_threshold = max(current * 0.95, 0.01)
            else:
                new_threshold = current
            conn = sqlite3.connect(self.DB_PATH)
            conn.execute('''INSERT INTO threshold_history
                (signal_type, threshold, win_rate, avg_return, n_trades, updated_at)
                VALUES (?,?,?,?,?,?)''',
                (signal_type, new_threshold, win_rate, avg_return,
                 len(outcomes), datetime.now().isoformat()))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('ThresholdOptimizer.update: %s', e)
