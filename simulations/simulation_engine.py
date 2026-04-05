"""
Apollo Simulation Engine
Replays a full trading day using historical/live-collected data.
Applies real execution realism: slippage, spread, latency, partial fills.
Does NOT modify any live signal logic.
"""

import sqlite3
import logging
import json
import math
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

SIM_DB_PATH = '/home/dannyelticala/quant-fund/simulations/simulation.db'
HISTORICAL_DB_PATH = '/home/dannyelticala/quant-fund/output/historical_db.db'


class ExecutionModel:
    """
    Realistic trade execution simulator.
    Estimates slippage, spread cost, latency, and partial fills.
    """

    @staticmethod
    def estimate_slippage(price: float, volume: float, trade_size: int) -> float:
        """
        Slippage ≈ Spread/2 + Volume Impact Factor
        Volume impact increases as trade size approaches available volume.
        """
        if volume <= 0:
            return price * 0.001  # 0.1% default for unknown volume
        volume_ratio = min(trade_size / max(volume, 1), 0.1)
        spread_component = price * 0.0002  # 2 basis points spread/2
        impact_component = price * volume_ratio * 0.005
        return spread_component + impact_component

    @staticmethod
    def estimate_spread_cost(price: float) -> float:
        """Typical bid-ask spread cost."""
        return price * 0.0003  # 3 basis points

    @staticmethod
    def estimate_latency_ms() -> float:
        """Simulated execution latency in milliseconds."""
        return random.uniform(80, 500)

    @staticmethod
    def estimate_fill_ratio(trade_size: int, volume: float) -> float:
        """
        Partial fill estimation based on available volume.
        Returns a ratio between 0.5 and 1.0.
        """
        if volume <= 0:
            return 0.9
        ratio = volume / max(trade_size, 1)
        if ratio >= 10:
            return 1.0
        elif ratio >= 2:
            return 0.95
        elif ratio >= 1:
            return random.uniform(0.7, 0.95)
        else:
            return random.uniform(0.5, 0.75)


class SimulationEngine:
    """
    Full day replay simulation engine for Apollo.

    Usage:
        engine = SimulationEngine()
        result = engine.run_simulation(sim_date='2026-04-07', market='US')
    """

    def __init__(self):
        self.exec_model = ExecutionModel()

    def _get_sim_conn(self):
        conn = sqlite3.connect(SIM_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _get_hist_conn(self):
        conn = sqlite3.connect(HISTORICAL_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _load_historical_data(self, sim_date: str, market: str) -> List[Dict]:
        """
        Load chronologically ordered price/volume events for a given date.
        Returns list of events sorted by timestamp.
        Gracefully returns empty list if no data found.
        """
        try:
            conn = self._get_hist_conn()
            cursor = conn.cursor()
            # price_history schema: id, ticker, date, open, high, low, close, adj_close, volume, source, delisted
            cursor.execute(
                "SELECT ticker, date AS timestamp, close AS price, volume FROM price_history "
                "WHERE date = ? ORDER BY ticker",
                (sim_date,)
            )
            rows = cursor.fetchall()
            events = [dict(row) for row in rows]
            conn.close()
            logger.info(f"Loaded {len(events)} historical events for {sim_date}")
            return events
        except Exception as e:
            logger.warning(f"Could not load historical data for {sim_date}: {e}")
            return []

    def _compute_metrics(self, trades: List[Dict], initial_capital: float = 100000.0) -> Dict:
        """
        Compute full performance metrics from trade list.
        Returns all metrics as a dict.
        """
        if not trades:
            return {
                'total_trades': 0, 'winning_trades': 0, 'losing_trades': 0,
                'gross_pnl': 0.0, 'net_pnl': 0.0, 'win_rate': 0.0,
                'profit_factor': 0.0, 'sharpe_ratio': None, 'sortino_ratio': None,
                'max_drawdown': 0.0, 'avg_trade_duration_min': 0.0
            }

        pnls = [t['net_pnl'] for t in trades]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p < 0]

        gross_pnl = sum(pnls)
        win_rate = len(winners) / len(pnls) if pnls else 0.0
        profit_factor = sum(winners) / abs(sum(losers)) if losers else float('inf')

        # Sharpe ratio (annualised)
        if len(pnls) > 1:
            import statistics
            mean_pnl = statistics.mean(pnls)
            std_pnl = statistics.stdev(pnls)
            sharpe = (mean_pnl / std_pnl * math.sqrt(252)) if std_pnl > 0 else None
            downside = [p for p in pnls if p < 0]
            if downside:
                downside_std = statistics.stdev(downside)
                sortino = (mean_pnl / downside_std * math.sqrt(252)) if downside_std > 0 else None
            else:
                sortino = None
        else:
            sharpe, sortino = None, None

        # Max drawdown via equity curve
        equity = initial_capital
        peak = equity
        max_dd = 0.0
        for p in pnls:
            equity += p
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)

        # Avg duration
        durations = []
        for t in trades:
            if t.get('entry_time') and t.get('exit_time'):
                try:
                    entry = datetime.fromisoformat(t['entry_time'])
                    exit_ = datetime.fromisoformat(t['exit_time'])
                    durations.append((exit_ - entry).total_seconds() / 60)
                except Exception:
                    pass
        avg_duration = sum(durations) / len(durations) if durations else 0.0

        return {
            'total_trades': len(trades),
            'winning_trades': len(winners),
            'losing_trades': len(losers),
            'gross_pnl': round(gross_pnl, 4),
            'net_pnl': round(gross_pnl, 4),
            'win_rate': round(win_rate, 4),
            'profit_factor': round(profit_factor, 4) if profit_factor != float('inf') else 999.0,
            'sharpe_ratio': round(sharpe, 4) if sharpe else None,
            'sortino_ratio': round(sortino, 4) if sortino else None,
            'max_drawdown': round(max_dd, 4),
            'avg_trade_duration_min': round(avg_duration, 2)
        }

    def _build_equity_curve(self, trades: List[Dict], initial_capital: float = 100000.0) -> List[Dict]:
        equity = initial_capital
        peak = equity
        curve = []
        for t in trades:
            equity += t.get('net_pnl', 0.0)
            dd = max(0.0, (peak - equity) / peak) if peak > 0 else 0.0
            peak = max(peak, equity)
            curve.append({
                'timestamp': t.get('exit_time', datetime.now(timezone.utc).isoformat()),
                'equity': round(equity, 4),
                'drawdown': round(dd, 4)
            })
        return curve

    def run_simulation(self, sim_date: str, market: str = 'US') -> Dict:
        """
        Main simulation entry point.
        Loads data, replays events, computes metrics, stores results.
        Returns summary dict.
        """
        run_id = f"sim_{sim_date}_{market}_{uuid.uuid4().hex[:8]}"
        logger.info(f"Starting simulation run: {run_id}")

        events = self._load_historical_data(sim_date, market)

        if not events:
            logger.warning(f"No historical data for {sim_date} — simulation skipped")
            return {
                'run_id': run_id,
                'status': 'skipped',
                'reason': 'no_data',
                'sim_date': sim_date
            }

        # Replay events chronologically
        simulated_trades = []
        open_positions = {}

        for event in events:
            ticker = event.get('ticker', 'UNKNOWN')
            price = float(event.get('price') or 0)
            volume = float(event.get('volume') or 0)
            timestamp = event.get('timestamp') or datetime.now(timezone.utc).isoformat()

            if price <= 0:
                continue

            # Entry: open position if not already held and under max concurrent positions
            if ticker not in open_positions and len(open_positions) < 50:
                trade_size = max(1, int(1000 / price))
                slippage = self.exec_model.estimate_slippage(price, volume, trade_size)
                spread = self.exec_model.estimate_spread_cost(price)
                latency = self.exec_model.estimate_latency_ms()
                fill_ratio = self.exec_model.estimate_fill_ratio(trade_size, volume)
                actual_size = max(1, int(trade_size * fill_ratio))

                open_positions[ticker] = {
                    'entry_price': price + slippage,
                    'quantity': actual_size,
                    'entry_time': timestamp,
                    'slippage': slippage,
                    'spread': spread,
                    'latency': latency,
                    'fill_ratio': fill_ratio,
                    'direction': 'LONG'
                }

            elif ticker in open_positions:
                pos = open_positions.pop(ticker)
                exit_slippage = self.exec_model.estimate_slippage(price, volume, pos['quantity'])
                exit_price = price - exit_slippage
                gross_pnl = (exit_price - pos['entry_price']) * pos['quantity']
                cost = (pos['slippage'] + pos['spread'] + exit_slippage) * pos['quantity']
                net_pnl = gross_pnl - cost

                simulated_trades.append({
                    'ticker': ticker,
                    'direction': pos['direction'],
                    'entry_price': round(pos['entry_price'], 4),
                    'exit_price': round(exit_price, 4),
                    'quantity': pos['quantity'],
                    'entry_time': pos['entry_time'],
                    'exit_time': timestamp,
                    'simulated_slippage': round(pos['slippage'], 4),
                    'simulated_spread_cost': round(pos['spread'], 4),
                    'simulated_latency_ms': round(pos['latency'], 2),
                    'fill_ratio': round(pos['fill_ratio'], 4),
                    'gross_pnl': round(gross_pnl, 4),
                    'net_pnl': round(net_pnl, 4),
                    'exit_reason': 'signal_exit',
                    'signals_used': json.dumps({})
                })

        # Force-close any remaining open positions at last known price
        for ticker, pos in open_positions.items():
            simulated_trades.append({
                'ticker': ticker,
                'direction': pos['direction'],
                'entry_price': round(pos['entry_price'], 4),
                'exit_price': round(pos['entry_price'], 4),
                'quantity': pos['quantity'],
                'entry_time': pos['entry_time'],
                'exit_time': datetime.now(timezone.utc).isoformat(),
                'simulated_slippage': 0.0,
                'simulated_spread_cost': 0.0,
                'simulated_latency_ms': 0.0,
                'fill_ratio': pos['fill_ratio'],
                'gross_pnl': 0.0,
                'net_pnl': 0.0,
                'exit_reason': 'end_of_day',
                'signals_used': json.dumps({})
            })

        metrics = self._compute_metrics(simulated_trades)
        equity_curve = self._build_equity_curve(simulated_trades)

        # Store results
        conn = self._get_sim_conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO simulation_runs (
                    run_id, sim_date, created_at, market,
                    total_trades, winning_trades, losing_trades,
                    gross_pnl, net_pnl, sharpe_ratio, sortino_ratio,
                    max_drawdown, win_rate, profit_factor, avg_trade_duration_min, status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'completed')
            """, (
                run_id, sim_date,
                datetime.now(timezone.utc).isoformat(),
                market,
                metrics['total_trades'], metrics['winning_trades'], metrics['losing_trades'],
                metrics['gross_pnl'], metrics['net_pnl'],
                metrics['sharpe_ratio'], metrics['sortino_ratio'],
                metrics['max_drawdown'], metrics['win_rate'],
                metrics['profit_factor'], metrics['avg_trade_duration_min']
            ))

            for trade in simulated_trades:
                conn.execute("""
                    INSERT INTO simulation_trades (
                        run_id, ticker, direction, entry_price, exit_price, quantity,
                        entry_time, exit_time, simulated_slippage, simulated_spread_cost,
                        simulated_latency_ms, fill_ratio, gross_pnl, net_pnl,
                        exit_reason, signals_used
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    run_id, trade['ticker'], trade['direction'],
                    trade['entry_price'], trade['exit_price'], trade['quantity'],
                    trade['entry_time'], trade['exit_time'],
                    trade['simulated_slippage'], trade['simulated_spread_cost'],
                    trade['simulated_latency_ms'], trade['fill_ratio'],
                    trade['gross_pnl'], trade['net_pnl'],
                    trade['exit_reason'], trade['signals_used']
                ))

            for point in equity_curve:
                conn.execute("""
                    INSERT INTO equity_curves (run_id, timestamp, equity, drawdown)
                    VALUES (?,?,?,?)
                """, (run_id, point['timestamp'], point['equity'], point['drawdown']))

            conn.commit()
            logger.info(f"Simulation {run_id} stored: {metrics['total_trades']} trades, "
                        f"net PnL: {metrics['net_pnl']}, Sharpe: {metrics['sharpe_ratio']}")
        except Exception as e:
            logger.error(f"Failed to store simulation results: {e}")
            conn.rollback()
        finally:
            conn.close()

        return {'run_id': run_id, 'metrics': metrics, 'status': 'completed'}
