"""
CorrelationRegimeDetector — monitors eigenvalue structure of return correlations.
Classifies: NORMAL / ELEVATED / CRISIS / UNUSUAL_LOW.

The eigenvalue ratio lambda_1 / mean(lambda_2..lambda_n) measures how much
of the variance is explained by a single market factor. High ratios indicate
stocks are moving together (crisis correlation); low ratios indicate unusual
dispersion.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False
    logger.warning("CorrelationRegimeDetector: numpy not available — eigenvalue analysis disabled")


REGIMES = {
    "NORMAL":      {"eigenvalue_ratio": (1.0, 3.0),  "action": "none",      "pos_multiplier": 1.0},
    "ELEVATED":    {"eigenvalue_ratio": (3.0, 6.0),  "action": "alert",     "pos_multiplier": 0.85},
    "CRISIS":      {"eigenvalue_ratio": (6.0, 999),  "action": "reduce_30", "pos_multiplier": 0.70},
    "UNUSUAL_LOW": {"eigenvalue_ratio": (0.0, 1.0),  "action": "alert",     "pos_multiplier": 1.0},
}


class CorrelationRegimeDetector:
    """
    Tracks eigenvalue structure of cross-asset return correlations over time.
    Raises critical alert when CRISIS regime is detected.
    Maintains rolling history for charting and downstream use.
    """

    def __init__(self, store=None, config=None):
        self._history: List[Dict] = []
        self._store = store
        self._config = config or {}

    def update(self, returns_matrix) -> Dict:
        """
        returns_matrix: pd.DataFrame (dates x tickers) or np.ndarray
        Compute correlation matrix.
        Eigenvalue decomposition.
        eigenvalue_ratio = lambda_1 / mean(lambda_2...lambda_n)
        Classify regime.
        Alert if CRISIS: log.critical + return alert=True
        Store result in self._history.
        Return {regime, eigenvalue_ratio, pos_multiplier, alert}
        """
        fallback = {
            "regime": "NORMAL",
            "eigenvalue_ratio": 1.0,
            "pos_multiplier": 1.0,
            "alert": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if not _NUMPY_AVAILABLE:
            logger.warning("update: numpy not available — returning NORMAL fallback")
            return fallback

        try:
            # Convert to numpy array
            try:
                import pandas as pd
                if isinstance(returns_matrix, pd.DataFrame):
                    arr = returns_matrix.values.astype(float)
                else:
                    arr = np.array(returns_matrix, dtype=float)
            except ImportError:
                arr = np.array(returns_matrix, dtype=float)

            # Remove any rows with NaN
            arr = arr[~np.isnan(arr).any(axis=1)]

            if arr.shape[0] < 5 or arr.shape[1] < 2:
                logger.warning(
                    f"update: insufficient data shape {arr.shape} — returning NORMAL fallback"
                )
                return fallback

            # Compute correlation matrix
            corr = np.corrcoef(arr.T)  # shape (n_tickers, n_tickers)

            # Eigenvalue decomposition (symmetric matrix, use eigh for speed/stability)
            eigenvalues = np.linalg.eigvalsh(corr)
            eigenvalues = np.sort(eigenvalues)[::-1]  # descending

            lambda_1 = float(eigenvalues[0])
            rest = eigenvalues[1:]

            if len(rest) == 0 or float(np.mean(rest)) == 0:
                logger.warning("update: only one eigenvalue or mean of rest is zero")
                return fallback

            eigenvalue_ratio = lambda_1 / float(np.mean(rest))

            # Classify regime
            regime = self._classify_regime(eigenvalue_ratio)
            regime_cfg = REGIMES[regime]
            pos_multiplier = regime_cfg["pos_multiplier"]
            alert = regime == "CRISIS" or regime == "ELEVATED"

            if regime == "CRISIS":
                logger.critical(
                    f"CORRELATION CRISIS DETECTED: eigenvalue_ratio={eigenvalue_ratio:.3f} "
                    f"(threshold=6.0) — reducing positions to {pos_multiplier:.0%}"
                )
            elif regime == "ELEVATED":
                logger.warning(
                    f"ELEVATED correlation regime: eigenvalue_ratio={eigenvalue_ratio:.3f} "
                    f"— position multiplier={pos_multiplier:.0%}"
                )
            elif regime == "UNUSUAL_LOW":
                logger.warning(
                    f"UNUSUAL_LOW correlation regime: eigenvalue_ratio={eigenvalue_ratio:.3f} "
                    f"— unusual market dispersion"
                )
            else:
                logger.debug(
                    f"update: NORMAL regime, eigenvalue_ratio={eigenvalue_ratio:.3f}"
                )

            result = {
                "regime": regime,
                "eigenvalue_ratio": eigenvalue_ratio,
                "pos_multiplier": pos_multiplier,
                "alert": alert,
                "action": regime_cfg["action"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "n_tickers": arr.shape[1],
                "n_observations": arr.shape[0],
            }

            self._history.append(result)

            if self._store is not None:
                try:
                    self._store.log_correlation_regime(result)
                except Exception as e:
                    logger.warning(f"update: store log failed: {e}")

            return result

        except Exception as e:
            logger.warning(f"update: unexpected error: {e} — returning NORMAL fallback")
            return fallback

    def _classify_regime(self, eigenvalue_ratio: float) -> str:
        """Map eigenvalue_ratio to regime string."""
        if eigenvalue_ratio < 1.0:
            return "UNUSUAL_LOW"
        elif eigenvalue_ratio < 3.0:
            return "NORMAL"
        elif eigenvalue_ratio < 6.0:
            return "ELEVATED"
        else:
            return "CRISIS"

    def current_regime(self) -> str:
        """Return latest regime classification or 'NORMAL' if no history."""
        try:
            if not self._history:
                return "NORMAL"
            return self._history[-1].get("regime", "NORMAL")
        except Exception as e:
            logger.warning(f"current_regime: {e}")
            return "NORMAL"

    def get_position_multiplier(self) -> float:
        """Return position size multiplier for current regime."""
        try:
            if not self._history:
                return 1.0
            regime = self._history[-1].get("regime", "NORMAL")
            return REGIMES.get(regime, REGIMES["NORMAL"])["pos_multiplier"]
        except Exception as e:
            logger.warning(f"get_position_multiplier: {e}")
            return 1.0

    def get_90day_history(self) -> List[Dict]:
        """Return last 90 days of regime classifications."""
        try:
            # If history items have timestamps, filter by date
            if not self._history:
                return []
            from datetime import timedelta
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            recent = []
            for item in self._history:
                ts = item.get("timestamp")
                if ts is None:
                    recent.append(item)
                    continue
                try:
                    dt = datetime.fromisoformat(ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt >= cutoff:
                        recent.append(item)
                except Exception:
                    recent.append(item)
            return recent
        except Exception as e:
            logger.warning(f"get_90day_history: {e}")
            return list(self._history)

    def render_regime_chart(self) -> str:
        """ASCII chart of eigenvalue ratio over last 90 days."""
        try:
            history = self.get_90day_history()
            if not history:
                return "CorrelationRegimeDetector: no history to chart."

            ratios = [h.get("eigenvalue_ratio", 1.0) for h in history]
            max_ratio = max(ratios) if ratios else 1.0
            min_ratio = min(ratios) if ratios else 1.0
            chart_height = 10
            chart_width = min(len(ratios), 60)

            # Sample to fit width
            step = max(1, len(ratios) // chart_width)
            sampled = ratios[::step][-chart_width:]

            lines = []
            lines.append("=" * 64)
            lines.append("CORRELATION REGIME — Eigenvalue Ratio (last 90 days)")
            lines.append(f"  Range: {min_ratio:.2f} – {max_ratio:.2f}  |  "
                         f"Thresholds: ELEVATED>3.0, CRISIS>6.0")
            lines.append("-" * 64)

            for row in range(chart_height, 0, -1):
                threshold = min_ratio + (max_ratio - min_ratio) * row / chart_height
                row_str = ""
                for val in sampled:
                    if val >= threshold:
                        row_str += "█"
                    else:
                        row_str += " "
                label = f"{threshold:5.2f} |"
                lines.append(f"{label}{row_str}")

            lines.append("      +" + "-" * len(sampled))

            regime_line = ""
            for val in sampled:
                r = self._classify_regime(val)
                if r == "CRISIS":
                    regime_line += "C"
                elif r == "ELEVATED":
                    regime_line += "E"
                elif r == "UNUSUAL_LOW":
                    regime_line += "L"
                else:
                    regime_line += "."
            lines.append(f"Regime|{regime_line}  (C=CRISIS E=ELEVATED L=LOW .=NORMAL)")
            lines.append("=" * 64)
            lines.append(f"Current: {self.current_regime()} | Pos multiplier: {self.get_position_multiplier():.0%}")

            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"render_regime_chart: {e}")
            return f"CorrelationRegimeDetector chart error: {e}"

    def as_feature(self) -> float:
        """Return eigenvalue_ratio as a float feature for StressLearner."""
        try:
            if not self._history:
                return 1.0
            return float(self._history[-1].get("eigenvalue_ratio", 1.0))
        except Exception as e:
            logger.warning(f"as_feature: {e}")
            return 1.0


# Alias for backwards-compatible imports
CorrelationRegime = CorrelationRegimeDetector
