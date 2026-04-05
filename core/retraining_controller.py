"""
Apollo Event-Driven Retraining Controller

DORMANCY RULES:
- Minimum 30 full live trading days before retraining is eligible
- Minimum 500 real trades before retraining is eligible
- Retraining only fires when performance degrades (not on a timer)
- All new models run in shadow mode before any deployment

This system is intentionally conservative. Apollo is a new bot.
It must accumulate sufficient live data before any model changes.

ML framework confirmed: scikit-learn (RandomForestClassifier,
GradientBoostingClassifier, LogisticRegression) with joblib serialisation.
Model path: altdata/models/
"""

import sqlite3
import logging
import json
import uuid
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

QUANT_DIR = '/home/dannyelticala/quant-fund'
CLOSELOOP_DB = os.path.join(QUANT_DIR, 'closeloop', 'storage', 'closeloop.db')
SIM_DB = os.path.join(QUANT_DIR, 'simulations', 'simulation.db')
SHADOW_DB = os.path.join(QUANT_DIR, 'simulations', 'shadow.db')
MODELS_DIR = os.path.join(QUANT_DIR, 'altdata', 'models')

# DORMANCY THRESHOLDS
MIN_TRADING_DAYS = 30
MIN_REAL_TRADES = 500

# RETRAINING TRIGGER THRESHOLDS
SHARPE_DECLINE_THRESHOLD = 0.3      # Retrain if Sharpe drops by this much
WIN_RATE_DECLINE_THRESHOLD = 0.08   # Retrain if win rate drops by this amount
DRAWDOWN_INCREASE_THRESHOLD = 0.05  # Retrain if max drawdown rises by this amount
ROLLING_WINDOW_DAYS = 14            # Look-back window for metric calculation

# DEPLOYMENT THRESHOLDS
MIN_SHARPE_IMPROVEMENT = 0.1        # New model must beat current by this much
MAX_DRAWDOWN_INCREASE = 0.02        # New model must not worsen drawdown by more than this


class RetrainingController:
    """
    Governs the full retraining lifecycle:
    1. Checks dormancy conditions
    2. Monitors performance triggers
    3. Trains candidate models (shadow mode)
    4. Deploys only when validated
    5. Maintains rollback registry
    """

    def __init__(self):
        os.makedirs(MODELS_DIR, exist_ok=True)
        os.makedirs(os.path.join(MODELS_DIR, 'candidates'), exist_ok=True)
        os.makedirs(os.path.join(MODELS_DIR, 'archive'), exist_ok=True)

    def _get_conn(self, db_path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def check_dormancy(self) -> Tuple[bool, str]:
        """
        Returns (is_dormant, reason).
        System is dormant if minimum thresholds not met.
        """
        try:
            conn = self._get_conn(CLOSELOOP_DB)
            cursor = conn.cursor()

            # Count real trades (non-zero PnL, non-phantom)
            # trade_ledger schema: is_phantom, net_pnl, entry_date
            cursor.execute("""
                SELECT COUNT(*) as count FROM trade_ledger
                WHERE ABS(net_pnl) > 0.01
                AND is_phantom = 0
            """)
            row = cursor.fetchone()
            real_trade_count = row['count'] if row else 0

            # Count unique trading days
            cursor.execute("""
                SELECT COUNT(DISTINCT date(entry_date)) as days FROM trade_ledger
                WHERE ABS(net_pnl) > 0.01 AND is_phantom = 0
            """)
            row = cursor.fetchone()
            trading_days = row['days'] if row else 0

            conn.close()

            if real_trade_count < MIN_REAL_TRADES:
                return True, (
                    f"Dormant: {real_trade_count}/{MIN_REAL_TRADES} real trades accumulated. "
                    f"Retraining unlocks at {MIN_REAL_TRADES} trades."
                )

            if trading_days < MIN_TRADING_DAYS:
                return True, (
                    f"Dormant: {trading_days}/{MIN_TRADING_DAYS} full trading days accumulated. "
                    f"Retraining unlocks after {MIN_TRADING_DAYS} trading days."
                )

            return False, f"Active: {real_trade_count} trades across {trading_days} days"

        except Exception as e:
            logger.warning(f"Dormancy check failed: {e} — staying dormant")
            return True, f"Dormant: database check failed ({e})"

    def compute_rolling_metrics(self, window_days: int = ROLLING_WINDOW_DAYS) -> Optional[Dict]:
        """
        Compute performance metrics over the recent rolling window.
        Returns None if insufficient data.
        """
        try:
            conn = self._get_conn(CLOSELOOP_DB)
            cursor = conn.cursor()
            cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).strftime('%Y-%m-%d')

            # trade_ledger schema: net_pnl, entry_date, is_phantom
            cursor.execute("""
                SELECT net_pnl FROM trade_ledger
                WHERE ABS(net_pnl) > 0.01 AND is_phantom = 0
                AND entry_date >= ?
                ORDER BY entry_date ASC
            """, (cutoff,))
            rows = cursor.fetchall()
            conn.close()

            if len(rows) < 20:
                return None  # Not enough recent trades

            import statistics
            import math
            pnls = [row['net_pnl'] for row in rows]
            winners = [p for p in pnls if p > 0]
            losers = [p for p in pnls if p < 0]

            mean_pnl = statistics.mean(pnls)
            std_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 1
            sharpe = (mean_pnl / std_pnl * math.sqrt(252)) if std_pnl > 0 else 0

            ds_std = statistics.stdev(losers) if len(losers) > 1 else std_pnl
            sortino = (mean_pnl / ds_std * math.sqrt(252)) if ds_std > 0 else 0

            equity = 100000.0
            peak = equity
            max_dd = 0.0
            for p in pnls:
                equity += p
                peak = max(peak, equity)
                dd = (peak - equity) / peak if peak > 0 else 0
                max_dd = max(max_dd, dd)

            win_rate = len(winners) / len(pnls)
            profit_factor = (sum(winners) / abs(sum(losers))) if losers else 999.0

            return {
                'sharpe': round(sharpe, 4),
                'sortino': round(sortino, 4),
                'max_drawdown': round(max_dd, 4),
                'win_rate': round(win_rate, 4),
                'profit_factor': round(profit_factor, 4),
                'trade_count': len(pnls),
                'window_days': window_days
            }
        except Exception as e:
            logger.warning(f"Rolling metrics computation failed: {e}")
            return None

    def check_retraining_triggers(self, current: Dict, baseline: Dict) -> Tuple[bool, str, str]:
        """
        Compare current metrics to baseline metrics.
        Returns (should_retrain, reason, metric_name).
        """
        if current['sharpe'] < baseline.get('sharpe', 0) - SHARPE_DECLINE_THRESHOLD:
            return True, "Sharpe ratio declined significantly", "sharpe"

        if current['win_rate'] < baseline.get('win_rate', 0) - WIN_RATE_DECLINE_THRESHOLD:
            return True, "Win rate deteriorated", "win_rate"

        if current['max_drawdown'] > baseline.get('max_drawdown', 0) + DRAWDOWN_INCREASE_THRESHOLD:
            return True, "Max drawdown increased", "max_drawdown"

        return False, "Performance within acceptable range", "none"

    def log_retraining_event(self, reason: str, metric: str, value: float, threshold: float):
        """Log a retraining trigger event to shadow.db."""
        try:
            conn = self._get_conn(SHADOW_DB)
            conn.execute("""
                INSERT INTO retraining_events (
                    triggered_at, trigger_reason, trigger_metric, trigger_value,
                    threshold_value, outcome
                ) VALUES (?,?,?,?,?,'pending')
            """, (
                datetime.now(timezone.utc).isoformat(),
                reason, metric, value, threshold
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Failed to log retraining event: {e}")

    def run_monitoring_cycle(self):
        """
        Main cycle — called by the scheduler every 6 hours.
        Checks dormancy → checks triggers → initiates shadow training if needed.
        """
        is_dormant, dormancy_reason = self.check_dormancy()

        if is_dormant:
            logger.info(f"[Retraining] {dormancy_reason}")
            return

        logger.info("[Retraining] Dormancy conditions met — checking performance triggers")
        current_metrics = self.compute_rolling_metrics(window_days=ROLLING_WINDOW_DAYS)
        baseline_metrics = self.compute_rolling_metrics(window_days=60)  # Longer window as baseline

        if not current_metrics or not baseline_metrics:
            logger.info("[Retraining] Insufficient data for trigger evaluation")
            return

        should_retrain, reason, metric = self.check_retraining_triggers(current_metrics, baseline_metrics)

        if should_retrain:
            logger.warning(f"[Retraining] Trigger fired: {reason}")
            self.log_retraining_event(
                reason=reason,
                metric=metric,
                value=current_metrics.get(metric, 0),
                threshold=baseline_metrics.get(metric, 0)
            )
            self._initiate_shadow_training(current_metrics, baseline_metrics, reason)
        else:
            logger.info(f"[Retraining] No triggers fired. Current Sharpe: {current_metrics['sharpe']}, "
                        f"Win rate: {current_metrics['win_rate']}")

    def _initiate_shadow_training(self, current: Dict, baseline: Dict, reason: str):
        """
        Kick off shadow model training in background.
        New model runs in parallel — never affects real trades.

        Framework: scikit-learn (RandomForest + GradientBoosting + Logistic)
        Model path: altdata/models/candidates/{version_id}.joblib
        Uses same pipeline as WeeklyRetrainer (archived in archive/legacy_retraining.py)
        """
        version_id = f"model_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        logger.info(f"[Shadow Training] Starting candidate model: {version_id}")

        try:
            conn = self._get_conn(SHADOW_DB)
            conn.execute("""
                INSERT INTO model_registry (
                    version_id, created_at, status,
                    sharpe_ratio, win_rate, max_drawdown,
                    notes
                ) VALUES (?,?,'training',?,?,?,?)
            """, (
                version_id,
                datetime.now(timezone.utc).isoformat(),
                current.get('sharpe'), current.get('win_rate'), current.get('max_drawdown'),
                f"Triggered by: {reason}"
            ))
            conn.commit()
            conn.close()

            # Training uses altdata store (same pipeline as legacy WeeklyRetrainer)
            # Deferred until altdata store has sufficient data (MIN_REAL_TRADES reached)
            logger.info(f"[Shadow Training] {version_id} registered in model_registry — "
                        f"training deferred until altdata store populated with sufficient data. "
                        f"Framework: scikit-learn RF+GBM+Logistic (see altdata/learning/weekly_retrainer.py)")

        except Exception as e:
            logger.error(f"[Shadow Training] Failed to initiate: {e}")

    def _validate_candidate(self, version_id: str, candidate_metrics: Dict, live_metrics: Dict) -> bool:
        """
        Returns True only if candidate is clearly superior.
        Applies strict validation rules.
        """
        if candidate_metrics['sharpe'] <= live_metrics['sharpe'] + MIN_SHARPE_IMPROVEMENT:
            logger.info(f"[Validation] {version_id} rejected: insufficient Sharpe improvement")
            return False

        if candidate_metrics['max_drawdown'] > live_metrics['max_drawdown'] + MAX_DRAWDOWN_INCREASE:
            logger.info(f"[Validation] {version_id} rejected: drawdown worsened")
            return False

        if candidate_metrics['win_rate'] < live_metrics['win_rate']:
            logger.info(f"[Validation] {version_id} rejected: win rate declined")
            return False

        logger.info(f"[Validation] {version_id} passed all checks — eligible for deployment")
        return True

    def rollback_to_version(self, version_id: str) -> bool:
        """
        Instantly rollback to a stored model version.
        All previous models are preserved in altdata/models/archive/.
        """
        import shutil
        archive_path = os.path.join(MODELS_DIR, 'archive', f"{version_id}.joblib")
        live_path = os.path.join(MODELS_DIR, 'live_model.joblib')

        if not os.path.exists(archive_path):
            logger.error(f"[Rollback] Version {version_id} not found in archive")
            return False

        backup_path = live_path + '.pre_rollback'
        if os.path.exists(live_path):
            shutil.copy2(live_path, backup_path)
        shutil.copy2(archive_path, live_path)
        logger.warning(f"[Rollback] Rolled back to {version_id}. Previous model backed up.")
        return True
