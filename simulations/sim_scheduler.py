"""
Runs daily simulation at 21:30 UTC after market close.
Plugs into Apollo's existing scheduling infrastructure via automation_scheduler.py.
"""

import logging
from datetime import datetime, timezone, timedelta
from simulations.simulation_engine import SimulationEngine

logger = logging.getLogger(__name__)


def run_daily_simulation():
    """Triggered at 21:30 UTC. Simulates the day that just ended."""
    today = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d')
    logger.info(f"Daily simulation starting for {today}")
    try:
        engine = SimulationEngine()
        result = engine.run_simulation(sim_date=today, market='US')
        logger.info(f"Daily simulation complete: {result.get('status')} | {result.get('metrics', {})}")
    except Exception as e:
        logger.error(f"Daily simulation failed: {e}")
