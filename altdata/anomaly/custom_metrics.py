import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import math
import logging

logger = logging.getLogger(__name__)


class CustomMetrics:
    """
    Original composite formulas for multi-source altdata signal combination.

    Provides:
      - Weighted confluence scoring across data sources
      - Exponential freshness decay per source
      - Information cascade stage detection
      - Cross-market divergence scoring (US vs UK)
      - Nonsense-signal purity (factor-unexplained variance)
      - Optimal lead-lag finder
    """

    # Source weights reflect expected information content / reliability
    SOURCE_WEIGHTS: Dict[str, float] = {
        "reddit": 0.08,
        "stocktwits": 0.12,
        "news": 0.18,
        "sec_edgar": 0.25,
        "shipping": 0.10,
        "jobs": 0.10,
        "wikipedia": 0.07,
        "google": 0.10,
    }

    # Decay rate lambda for exp(-lambda * hours).  Higher = faster decay.
    FRESHNESS_LAMBDAS: Dict[str, float] = {
        "news": 0.5,         # half-life ~1.4 hours
        "sec_edgar": 0.05,   # half-life ~14 hours
        "reddit": 0.3,
        "stocktwits": 0.4,
        "shipping": 0.01,    # half-life ~69 hours
        "jobs": 0.005,
        "wikipedia": 0.02,
        "google": 0.008,
        "lunar": 0.001,      # persists for days
        "weather": 0.02,
    }

    # ------------------------------------------------------------------
    # Confluence
    # ------------------------------------------------------------------

    def altdata_confluence_score(
        self,
        source_signals: Dict[str, float],
        source_confidences: Dict[str, float] = None,
    ) -> float:
        """
        Weighted average of source signals, adjusted by optional per-source
        confidence estimates.

        Returns score in [-1, +1].  |score| > 0.6 indicates strong confluence.
        """
        total_weight = 0.0
        weighted_sum = 0.0

        for source, signal in source_signals.items():
            w = self.SOURCE_WEIGHTS.get(source, 0.05)
            conf = (source_confidences or {}).get(source, 1.0)
            weighted_sum += signal * w * conf
            total_weight += w * conf

        if total_weight == 0:
            return 0.0

        score = weighted_sum / total_weight
        return float(max(-1.0, min(1.0, score)))

    # ------------------------------------------------------------------
    # Freshness decay
    # ------------------------------------------------------------------

    def signal_freshness_decay(
        self,
        raw_signal: float,
        source: str,
        hours_since_signal: float,
    ) -> float:
        """
        Apply exponential decay based on source type.

        Returns decayed signal value.
        """
        lam = self.FRESHNESS_LAMBDAS.get(source, 0.05)
        freshness = math.exp(-lam * hours_since_signal)
        decayed = raw_signal * freshness
        return float(decayed)

    # ------------------------------------------------------------------
    # Information cascade stage
    # ------------------------------------------------------------------

    def information_cascade_stage(
        self,
        source_activation_times: Dict[str, Optional[str]],
    ) -> float:
        """
        Returns cascade stage in [0, 1].
          0 = very early (smart money / insiders just acted)
          1 = very late  (retail already knows and has priced it in)

        A low stage means better entry timing.
        """
        # Ordered from smart-money (earliest) to retail (latest)
        smart_to_dumb = [
            "sec_edgar",
            "companies_house",
            "news",
            "wikipedia",
            "google",
            "stocktwits",
            "reddit",
        ]

        activated: Dict[str, datetime] = {}
        for source, ts in source_activation_times.items():
            if ts is None:
                continue
            try:
                activated[source] = datetime.fromisoformat(ts)
            except Exception:
                pass

        if not activated:
            return 0.5  # unknown; assume mid-cascade

        # Return position of earliest activated smart-money source
        for i, source in enumerate(smart_to_dumb):
            if source in activated:
                return float(i / len(smart_to_dumb))

        return 1.0  # only retail sources activated

    # ------------------------------------------------------------------
    # Cross-market divergence
    # ------------------------------------------------------------------

    def cross_market_divergence_score(
        self,
        us_sector_return: float,
        uk_sector_return: float,
        us_vol: float,
        uk_vol: float,
    ) -> float:
        """
        Measures divergence between US and UK small-caps in the same sector.

        Positive score => UK underperforming US (UK may be cheap).
        High absolute score => potential mispricing.

        Returns z-score clipped to [-3, +3].
        """
        pooled_vol = np.sqrt((us_vol ** 2 + uk_vol ** 2) / 2) + 1e-8
        divergence = (uk_sector_return - us_sector_return) / pooled_vol
        return float(np.clip(divergence, -3.0, 3.0))

    # ------------------------------------------------------------------
    # Nonsense signal purity
    # ------------------------------------------------------------------

    def nonsense_signal_purity(
        self,
        signal_returns: pd.Series,
        factor_returns: Dict[str, pd.Series],
    ) -> float:
        """
        Measures the fraction of signal return that is unexplained by known
        risk factors (market, value, momentum, etc.).

        Higher purity => more novel alpha => harder for competitors to replicate.

        Returns value in [0, 1].
        """
        if not factor_returns or len(signal_returns) < 30:
            return 0.5

        factor_df = pd.DataFrame(factor_returns).dropna()
        aligned = signal_returns.reindex(factor_df.index).dropna()
        factor_df = factor_df.reindex(aligned.index)

        if len(aligned) < 30:
            return 0.5

        from numpy.linalg import lstsq

        X = np.column_stack([np.ones(len(aligned)), factor_df.values])
        beta, _, _, _ = lstsq(X, aligned.values, rcond=None)
        fitted = X @ beta
        residual = aligned.values - fitted

        total_var = float(np.var(aligned.values))
        if total_var == 0:
            return 0.5

        explained_var = total_var - float(np.var(residual))
        purity = 1.0 - (explained_var / total_var)
        return float(np.clip(purity, 0.0, 1.0))

    # ------------------------------------------------------------------
    # Lead-lag optimiser
    # ------------------------------------------------------------------

    def altdata_lead_lag_optimiser(
        self,
        alt_signal: pd.Series,
        forward_returns: pd.Series,
        max_lag: int = 90,
    ) -> Dict:
        """
        Find the optimal lag (0 to max_lag days) that maximises the absolute
        Pearson correlation between the alt signal and forward returns.

        Uses a coarse grid search (step=5) followed by fine-grained refinement
        (±4 days around the coarse optimum) for speed.

        Returns dict: {optimal_lag, correlation, p_value}
        """
        from scipy import stats

        best_lag = 0
        best_corr = 0.0
        best_p = 1.0

        # Coarse pass
        for lag in range(0, max_lag + 1, 5):
            shifted = alt_signal.shift(lag)
            aligned = pd.concat([shifted, forward_returns], axis=1).dropna()
            if len(aligned) < 30:
                continue
            corr, p = stats.pearsonr(aligned.iloc[:, 0], aligned.iloc[:, 1])
            if abs(corr) > abs(best_corr) and p < 0.1:
                best_lag, best_corr, best_p = lag, corr, p

        # Fine-grained refinement around best_lag ±4 days
        for lag in range(max(0, best_lag - 4), min(max_lag + 1, best_lag + 5)):
            shifted = alt_signal.shift(lag)
            aligned = pd.concat([shifted, forward_returns], axis=1).dropna()
            if len(aligned) < 30:
                continue
            corr, p = stats.pearsonr(aligned.iloc[:, 0], aligned.iloc[:, 1])
            if abs(corr) > abs(best_corr):
                best_lag, best_corr, best_p = lag, corr, p

        logger.debug(
            "Lead-lag optimiser: best_lag=%d corr=%.4f p=%.4f",
            best_lag,
            best_corr,
            best_p,
        )
        return {
            "optimal_lag": best_lag,
            "correlation": float(best_corr),
            "p_value": float(best_p),
        }
