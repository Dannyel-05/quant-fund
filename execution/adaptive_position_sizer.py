"""
Adaptive Position Sizer — 5-phase + PHASE_FREE system.

Phase 1: 0.15%–0.40% base/max — gather data on everything
Phase 2: 0.30%–0.80% — early patterns emerging
Phase 3: 0.50%–1.20% — clear patterns, growing confidence
Phase 4: 0.70%–1.80% — strong patterns, meaningful sizing
Phase 5: 1.00%–2.50% — optimised, algorithm driving
PHASE_FREE (2000+ trades): fully autonomous Kelly sizing

Dynamic scaling: within each phase, position sizes scale
up to the phase maximum when multiple signals agree,
macro regime is favourable, and HMM state aligns.
"""
import logging
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


PHASES = {
    'PHASE_1': {
        'min_trades': 0,
        'max_trades': 100,
        'base_pct': 0.0015,
        'max_pct': 0.004,
        'min_signal': 0.15,
        'max_positions': 200,
        'stop_loss': 0.20,
        'take_profit': 0.30,
        'max_hold_days': 15,
        'description': 'Gathering data — trade everything small',
    },
    'PHASE_2': {
        'min_trades': 100,
        'max_trades': 300,
        'base_pct': 0.003,
        'max_pct': 0.008,
        'min_signal': 0.22,
        'max_positions': 150,
        'stop_loss': 0.18,
        'take_profit': 0.25,
        'max_hold_days': 18,
        'description': 'Early patterns — slightly selective',
    },
    'PHASE_3': {
        'min_trades': 300,
        'max_trades': 600,
        'base_pct': 0.005,
        'max_pct': 0.012,
        'min_signal': 0.30,
        'max_positions': 120,
        'stop_loss': 0.15,
        'take_profit': 0.22,
        'max_hold_days': 20,
        'description': 'Clear patterns — growing confidence',
    },
    'PHASE_4': {
        'min_trades': 600,
        'max_trades': 1000,
        'base_pct': 0.007,
        'max_pct': 0.018,
        'min_signal': 0.38,
        'max_positions': 100,
        'stop_loss': 0.13,
        'take_profit': 0.20,
        'max_hold_days': 22,
        'description': 'Strong patterns — meaningful sizing',
    },
    'PHASE_5': {
        'min_trades': 1000,
        'max_trades': 2000,
        'base_pct': 0.010,
        'max_pct': 0.025,
        'min_signal': 0.45,
        'max_positions': 75,
        'stop_loss': 0.12,
        'take_profit': 0.18,
        'max_hold_days': 25,
        'description': 'Optimised — algorithm drives decisions',
    },
    'PHASE_FREE': {
        'min_trades': 2000,
        'max_trades': 9_999_999,
        'base_pct': None,
        'max_pct': None,
        'min_signal': None,
        'max_positions': None,
        'stop_loss': None,
        'take_profit': None,
        'max_hold_days': None,
        'description': 'Full autonomy — algorithm drives everything',
    },
}

ABSOLUTE_LIMITS = {
    'never_exceed_pct': 0.030,
    'never_below_pct':  0.001,
    'account_halt_pct': 0.15,
    'sector_max_pct':   0.20,
    'market_max_pct':   0.60,
}


class AdaptivePositionSizer:
    """
    Adaptive position sizer with phase-based and fully-autonomous sizing.
    """

    def __init__(self, config: dict, closeloop_store=None):
        self.config = config
        self.store = closeloop_store
        self.signal_performance_cache: Dict[str, Dict] = {}
        self.last_performance_update: Optional[datetime] = None
        self._ensure_db()

    # ------------------------------------------------------------------
    def _ensure_db(self) -> None:
        try:
            os.makedirs('output', exist_ok=True)
            conn = sqlite3.connect('output/permanent_archive.db')
            conn.execute('''CREATE TABLE IF NOT EXISTS phase_history
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 phase TEXT, reason TEXT, n_trades INTEGER,
                 changed_at TEXT)''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning('AdaptivePositionSizer DB: %s', e)

    # ------------------------------------------------------------------
    def get_current_phase(self) -> Dict[str, Any]:
        """Return current phase dict based on completed trade count."""
        n = 0
        if self.store is not None:
            try:
                n = self.store.count_completed_trades()
            except Exception:
                pass

        for name in ('PHASE_FREE', 'PHASE_5', 'PHASE_4', 'PHASE_3', 'PHASE_2', 'PHASE_1'):
            phase = PHASES[name]
            if n >= phase['min_trades']:
                max_t = phase['max_trades']
                min_t = phase['min_trades']
                progress = ((n - min_t) / max(max_t - min_t, 1) * 100) if max_t < 9_000_000 else 100
                return {
                    **phase,
                    'name': name,
                    'n_trades': n,
                    'progress_pct': round(progress, 1),
                }
        return {**PHASES['PHASE_1'], 'name': 'PHASE_1', 'n_trades': 0, 'progress_pct': 0.0}

    # ------------------------------------------------------------------
    def get_signal_type_performance(self, signal_type: str) -> Dict:
        """Return cached performance for a signal type, refreshed hourly."""
        if (self.last_performance_update is None or
                (datetime.now() - self.last_performance_update).total_seconds() > 3600):
            if self.store is not None:
                try:
                    self.signal_performance_cache = self.store.get_signal_performance_by_type()
                    self.last_performance_update = datetime.now()
                except Exception:
                    pass
        return self.signal_performance_cache.get(signal_type, {})

    # ------------------------------------------------------------------
    def calculate_confluence_multiplier(
        self,
        signal_type: str,
        signal_score: float,
        context: Dict,
        all_signals_for_ticker: List[Dict],
    ) -> float:
        """
        Dynamic scaling: returns a multiplier [0.3, 3.0] based on
        signal confluence, macro regime, HMM state, earnings quality,
        crowding, and historical performance.
        """
        multiplier = 1.0

        # ── Signal confluence ─────────────────────────────────────────
        if all_signals_for_ticker:
            ref_dir = all_signals_for_ticker[0].get('direction', 'LONG')
            agreeing_types = set()
            for sig in all_signals_for_ticker:
                if (sig.get('direction') == ref_dir and
                        float(sig.get('score', 0)) > 0.2):
                    agreeing_types.add(sig.get('signal_type', ''))
            n_agree = len(agreeing_types)
            if n_agree >= 4:
                multiplier *= 2.0
            elif n_agree == 3:
                multiplier *= 1.6
            elif n_agree == 2:
                multiplier *= 1.3

        # ── Macro regime ──────────────────────────────────────────────
        regime = context.get('macro_regime') or context.get('macro', {}).get('regime', 'UNKNOWN')
        if isinstance(regime, dict):
            regime = regime.get('regime', 'UNKNOWN')
        regime_mults = {
            'GOLDILOCKS': 1.30, 'RISK_ON': 1.10,
            'RISK_OFF': 0.70, 'STAGFLATION': 0.60,
            'RECESSION_RISK': 0.35, 'CRISIS': 0.20,
        }
        multiplier *= regime_mults.get(str(regime).upper(), 1.0)

        # ── HMM state ─────────────────────────────────────────────────
        hmm = context.get('hmm_state')
        direction = (all_signals_for_ticker[0].get('direction', 'LONG')
                     if all_signals_for_ticker else 'LONG')
        if hmm == 'BULL' and direction == 'LONG':
            multiplier *= 1.20
        elif hmm == 'BEAR' and direction == 'SHORT':
            multiplier *= 1.20
        elif hmm == 'BEAR' and direction == 'LONG':
            multiplier *= 0.60
        elif hmm == 'BULL' and direction == 'SHORT':
            multiplier *= 0.60

        # ── Earnings quality (SimFin) ─────────────────────────────────
        eq = context.get('earnings_quality_score', 0.5)
        if isinstance(eq, dict):
            eq = float(eq.get('score') or 0.5)
        eq = float(eq or 0.5)
        if eq > 0.7:
            multiplier *= 1.20
        elif eq < 0.3:
            multiplier *= 0.70

        # ── Crowding ──────────────────────────────────────────────────
        crowding = float(context.get('crowding_risk', 0.1) or 0.1)
        if crowding > 0.7:
            multiplier *= 0.50
        elif crowding > 0.5:
            multiplier *= 0.75

        # ── Mathematical composite ────────────────────────────────────
        math_comp = float(context.get('math_composite', 0) or 0)
        if abs(math_comp) > 0.6:
            multiplier *= 1.15

        # ── Sector rotation ───────────────────────────────────────────
        sr_score = float(context.get('sector_rotation_score', 0) or 0)
        if sr_score > 0.5 and direction == 'LONG':
            multiplier *= 1.10
        elif sr_score < -0.5 and direction == 'SHORT':
            multiplier *= 1.10

        # ── Historical win-rate bonus ──────────────────────────────────
        perf = self.get_signal_type_performance(signal_type)
        if perf and perf.get('n_trades', 0) >= 20:
            wr = perf.get('win_rate', 0.5)
            if wr > 0.60:
                multiplier *= 1.25
            elif wr > 0.55:
                multiplier *= 1.10
            elif wr < 0.40:
                multiplier *= 0.60
            elif wr < 0.45:
                multiplier *= 0.75

        return float(max(0.3, min(3.0, multiplier)))

    # ------------------------------------------------------------------
    def size_position(
        self,
        signal_score: float,
        signal_type: str,
        account_equity: float,
        ticker: str,
        context: Dict,
        all_signals: List[Dict],
    ) -> Dict:
        """
        Calculate position size. Routes to autonomous sizing in PHASE_FREE.
        Returns dict with position_pct, position_value, phase, scaling_reason.
        """
        phase = self.get_current_phase()

        if phase['name'] == 'PHASE_FREE':
            return self.size_position_autonomous(
                signal_type, signal_score, context,
                all_signals, account_equity, ticker)

        base_pct = phase['base_pct']
        max_pct  = phase['max_pct']

        score_mult = float(max(0.5, min(1.5, 0.7 + signal_score * 0.6)))
        confluence_mult = self.calculate_confluence_multiplier(
            signal_type, signal_score, context, all_signals)

        raw_pct = base_pct * score_mult * confluence_mult
        clipped_pct = float(max(base_pct * 0.5, min(max_pct, raw_pct)))
        final_pct = float(max(
            ABSOLUTE_LIMITS['never_below_pct'],
            min(ABSOLUTE_LIMITS['never_exceed_pct'], clipped_pct)
        ))
        final_value = round(account_equity * final_pct, 2)

        n_agree = (len(set(s.get('signal_type') for s in all_signals
                           if s.get('direction') == (all_signals[0].get('direction') if all_signals else 'LONG')))
                   if all_signals else 1)

        return {
            'position_pct': final_pct,
            'position_value': final_value,
            'phase': phase['name'],
            'base_pct': base_pct,
            'confluence_multiplier': confluence_mult,
            'n_agreeing_signals': n_agree,
            'n_completed_trades': phase['n_trades'],
            'scaling_reason': (
                f"Phase={phase['name']} "
                f"base={base_pct * 100:.2f}% "
                f"x score={score_mult:.2f} "
                f"x confluence={confluence_mult:.2f} "
                f"({n_agree} sigs agree) "
                f"= {final_pct * 100:.3f}% (${final_value:.0f})"
            ),
        }

    # ------------------------------------------------------------------
    def size_position_autonomous(
        self,
        signal_type: str,
        signal_score: float,
        context: Dict,
        all_signals: List[Dict],
        account_equity: float,
        ticker: str,
    ) -> Dict:
        """
        PHASE_FREE: fully autonomous Kelly-based sizing.
        Only absolute safety limits apply.
        """
        perf = self.get_signal_type_performance(signal_type)

        fractional_kelly = PHASES['PHASE_5']['base_pct']  # fallback
        if perf and perf.get('n_trades', 0) >= 50:
            win_rate = float(perf.get('win_rate', 0.5))
            avg_win  = float(perf.get('avg_win_pct', 0.05))
            avg_loss = abs(float(perf.get('avg_loss_pct', -0.03)))
            p, q = win_rate, 1.0 - win_rate
            b = avg_win / max(avg_loss, 0.001)
            kelly = (p * b - q) / max(b, 0.001)
            fractional_kelly = max(
                ABSOLUTE_LIMITS['never_below_pct'],
                min(ABSOLUTE_LIMITS['never_exceed_pct'], kelly * 0.25)
            )

        confluence_mult = self.calculate_confluence_multiplier(
            signal_type, signal_score, context, all_signals)

        regime = context.get('macro_regime') or context.get('macro', {}).get('regime', 'RISK_ON')
        if isinstance(regime, dict):
            regime = regime.get('regime', 'RISK_ON')
        regime_mult = {
            'GOLDILOCKS': 1.3, 'RISK_ON': 1.0, 'RISK_OFF': 0.6,
            'STAGFLATION': 0.5, 'RECESSION_RISK': 0.3, 'CRISIS': 0.1,
        }.get(str(regime).upper(), 1.0)

        decay_mult = 1.0
        try:
            from analysis.signal_decay_monitor import SignalDecayMonitor
            dm = SignalDecayMonitor(self.config)
            status = dm.get_signal_status(signal_type)
            if status == 'SEVERELY_DEGRADED':
                decay_mult = 0.1
            elif status == 'DECAYING':
                decay_mult = 0.5
        except Exception:
            pass

        raw_pct = fractional_kelly * confluence_mult * regime_mult * decay_mult
        final_pct = float(max(
            ABSOLUTE_LIMITS['never_below_pct'],
            min(ABSOLUTE_LIMITS['never_exceed_pct'], raw_pct)
        ))
        position_value = round(account_equity * final_pct, 2)

        return {
            'position_pct': final_pct,
            'position_value': position_value,
            'phase': 'PHASE_FREE',
            'kelly_fraction': fractional_kelly,
            'confluence_multiplier': confluence_mult,
            'regime_multiplier': regime_mult,
            'decay_multiplier': decay_mult,
            'n_completed_trades': self.store.count_completed_trades() if self.store else 0,
            'scaling_reason': (
                f"AUTONOMOUS Kelly={fractional_kelly * 100:.2f}% "
                f"x confluence={confluence_mult:.2f} "
                f"x regime={regime_mult:.2f} "
                f"x decay={decay_mult:.2f} "
                f"= {final_pct * 100:.3f}% (${position_value:.0f})"
            ),
        }

    # ------------------------------------------------------------------
    def should_trade(
        self,
        signal_score: float,
        signal_type: str = None,
        context: Dict = None,
    ) -> bool:
        """Return True if signal_score meets the current phase threshold."""
        phase = self.get_current_phase()

        if phase['name'] == 'PHASE_FREE':
            perf = self.get_signal_type_performance(signal_type or 'UNKNOWN')
            threshold = float(perf.get('min_profitable_score', 0.35)) if perf and perf.get('n_trades', 0) >= 20 else 0.35
            regime = (context or {}).get('macro_regime', 'RISK_ON')
            if isinstance(regime, dict):
                regime = regime.get('regime', 'RISK_ON')
            if str(regime).upper() in ('CRISIS', 'RECESSION_RISK'):
                threshold *= 1.5
            elif str(regime).upper() == 'GOLDILOCKS':
                threshold *= 0.8
            return signal_score >= threshold

        min_score = phase['min_signal']
        if context:
            regime = context.get('macro_regime', '')
            if isinstance(regime, dict):
                regime = regime.get('regime', '')
            if str(regime).upper() == 'CRISIS':
                min_score *= 2.0
        return signal_score >= min_score

    # ------------------------------------------------------------------
    def should_halt(self, account_equity: float, starting_equity: float = 100000.0) -> bool:
        """Return True if drawdown exceeds halt threshold."""
        dd = (starting_equity - account_equity) / max(starting_equity, 1)
        return dd > ABSOLUTE_LIMITS['account_halt_pct']

    # ------------------------------------------------------------------
    def max_new_positions(self, current_open: int, account_equity: float = 100000.0) -> int:
        """Return maximum number of new positions allowed."""
        phase = self.get_current_phase()
        if phase['name'] == 'PHASE_FREE':
            available_pct = 0.80 - current_open * 0.01
            estimated = int(available_pct / 0.01)
            return max(0, min(200, estimated))
        max_pos = phase.get('max_positions', 100) or 100
        return max(0, max_pos - current_open)

    # ------------------------------------------------------------------
    def get_phase_summary(self) -> str:
        """Return human-readable phase summary string."""
        phase = self.get_current_phase()
        name = phase['name']
        n = phase['n_trades']

        if name == 'PHASE_FREE':
            return (
                f"PHASE_FREE | {n} trades | "
                f"AUTONOMOUS Kelly sizing | "
                f"Safety limit: {ABSOLUTE_LIMITS['never_exceed_pct']*100:.0f}% max per trade"
            )

        remaining = max(0, phase['max_trades'] - n)
        return (
            f"{name} | {n} trades | "
            f"{phase['base_pct']*100:.2f}%–{phase['max_pct']*100:.2f}% per position | "
            f"max {phase['max_positions']} positions | "
            f"{remaining} trades to next phase"
        )

    # ------------------------------------------------------------------
    def auto_trigger_discovery(self) -> None:
        """Trigger symbolic regression discovery at key trade milestones."""
        n = 0
        if self.store is not None:
            try:
                n = self.store.count_completed_trades()
            except Exception:
                pass
        for tp in (200, 400, 600, 800, 1000, 1500, 2000):
            if tp - 3 <= n <= tp + 3:
                logger.info('Auto-triggering symbolic regression at %d trades', n)
                try:
                    from analysis.symbolic_regression import SymbolicRegressionEngine
                    import yaml
                    cfg = yaml.safe_load(open('config/settings.yaml'))
                    sre = SymbolicRegressionEngine(cfg)
                    sre.run_discovery_pipeline()
                except Exception as e:
                    logger.warning('Discovery failed: %s', e)
                break

    # ------------------------------------------------------------------
    # Backward-compat helpers
    # ------------------------------------------------------------------

    def get_current_phase_number(self) -> int:
        name = self.get_current_phase()['name']
        mapping = {'PHASE_1': 1, 'PHASE_2': 2, 'PHASE_3': 3,
                   'PHASE_4': 4, 'PHASE_5': 5, 'PHASE_FREE': 6}
        return mapping.get(name, 1)
