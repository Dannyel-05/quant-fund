"""
ARCHIVED — Legacy time-based retraining logic.
Replaced by event-driven system in Section 5 of the April 2026 build.

Reason: time-based retraining (Sunday 03:00 UTC, regardless of performance)
causes unnecessary model churn and instability on a system with insufficient
live data. Apollo had 0 full live trading days when this was replaced.

Two components are archived here:
  1. BatchRetrainer (closeloop/learning/batch_retrainer.py)
     - Optimises signal weights via WeightUpdater.batch_update()
     - Computes per-signal Sharpe, entry timing alpha, peer/analyst alpha
     - Valuable logic preserved here in case weight update cycle is reinstated

  2. WeeklyRetrainer (altdata/learning/weekly_retrainer.py)
     - sklearn ensemble (RF + GBM + Logistic) for altdata signal models
     - Sharpe-weighted ensemble with 80/20 time-series train/val split
     - Promotes only if new model >= 90% of current Sharpe

Both are preserved for reference only. Do not import.
"""

# ─── BatchRetrainer (closeloop/learning/batch_retrainer.py) ─────────────────
# See closeloop/learning/batch_retrainer.py for full source.
# Called by: intelligence/automation_scheduler.py job_weekly() Sunday 03:00 UTC
# Replaced by: core/retraining_controller.py RetrainingController.run_monitoring_cycle()
# Reason: dormancy-unaware — fires regardless of whether Apollo has sufficient data.

# ─── WeeklyRetrainer (altdata/learning/weekly_retrainer.py) ─────────────────
# See altdata/learning/weekly_retrainer.py for full source.
# Framework: scikit-learn (RandomForestClassifier, GradientBoostingClassifier,
#            LogisticRegression), serialised with joblib.
# Replaced by: core/retraining_controller.py — event-driven, performance-triggered.
# Reason: fires on a 7-day timer regardless of live data volume or performance.
#
# Key parameters preserved for reference:
#   lookback_days = 365
#   validation_split = 0.2
#   model_path = altdata/models/
#   trigger: every Sunday 02:00 UTC
#   promote threshold: new_sharpe >= current_sharpe * 0.9
#   ensemble weights: softmax over validation Sharpe values
