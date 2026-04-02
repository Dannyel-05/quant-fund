"""
Correlation Discoverer — automatic detection of new frontier signal candidates.

Scans all available frontier signals against returns to find statistically
significant correlations that pass a Monte Carlo permutation test.  Only
signals with an economic plausibility check (abs correlation in usable range,
sufficient observations, and beating the 95th percentile of shuffled
correlations) are surfaced as candidates.

Interaction discovery extends the search to pairwise products of signals,
enabling detection of emergent predictive power not visible in individual
components.

Every candidate returned here should be manually reviewed before registering
to the DiscoveryRegistry.  The nonsense_score and has_story fields should be
filled in by a human analyst.
"""
import logging
import random
from itertools import combinations
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    logger.warning("numpy not available — CorrelationDiscoverer will use fallback stats")

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False
    logger.warning("pandas not available — CorrelationDiscoverer disabled")


def _rolling_corr_mean(signal: "pd.Series", returns: "pd.Series", window: int = 20) -> float:
    """Compute mean of rolling correlation between signal and returns."""
    if not _HAS_PANDAS:
        return 0.0
    try:
        aligned_sig, aligned_ret = signal.align(returns, join="inner")
        if len(aligned_sig) < window:
            return float("nan")
        rc = aligned_sig.rolling(window).corr(aligned_ret)
        return float(rc.mean())
    except Exception as exc:
        logger.debug(f"rolling_corr_mean failed: {exc}")
        return float("nan")


def _lag_corr(signal: "pd.Series", returns: "pd.Series", lag: int) -> float:
    """Pearson correlation of signal (shifted by lag) with returns."""
    if not _HAS_PANDAS:
        return 0.0
    try:
        shifted = signal.shift(lag)
        aligned_sig, aligned_ret = shifted.align(returns, join="inner")
        mask = aligned_sig.notna() & aligned_ret.notna()
        s = aligned_sig[mask]
        r = aligned_ret[mask]
        if len(s) < 10:
            return float("nan")
        if not _HAS_NUMPY:
            # fallback: manual Pearson
            n = len(s)
            s_vals = list(s)
            r_vals = list(r)
            s_mean = sum(s_vals) / n
            r_mean = sum(r_vals) / n
            num = sum((sv - s_mean) * (rv - r_mean) for sv, rv in zip(s_vals, r_vals))
            denom_s = (sum((sv - s_mean) ** 2 for sv in s_vals)) ** 0.5
            denom_r = (sum((rv - r_mean) ** 2 for rv in r_vals)) ** 0.5
            if denom_s == 0 or denom_r == 0:
                return float("nan")
            return num / (denom_s * denom_r)
        return float(np.corrcoef(s.values, r.values)[0, 1])
    except Exception as exc:
        logger.debug(f"lag_corr failed (lag={lag}): {exc}")
        return float("nan")


def _monte_carlo_percentile(
    signal: "pd.Series",
    returns: "pd.Series",
    observed_corr: float,
    n_shuffles: int = 500,
    window: int = 20,
    rng_seed: Optional[int] = None,
) -> float:
    """
    Permutation test: shuffle the signal n_shuffles times and compute
    rolling_corr_mean for each shuffle.  Return the percentile of
    observed_corr in the shuffled distribution (0–100).
    """
    if not _HAS_PANDAS or not _HAS_NUMPY:
        return 0.0

    rng = random.Random(rng_seed)
    try:
        aligned_sig, aligned_ret = signal.align(returns, join="inner")
        sig_vals = aligned_sig.dropna().values.copy()
        if len(sig_vals) < window:
            return 0.0

        shuffled_corrs = []
        for _ in range(n_shuffles):
            rng.shuffle(sig_vals)
            shuffled = pd.Series(sig_vals, index=aligned_sig.dropna().index)
            sc = _rolling_corr_mean(shuffled, aligned_ret, window=window)
            if not (sc != sc):  # not NaN
                shuffled_corrs.append(abs(sc))

        if not shuffled_corrs:
            return 0.0

        shuffled_arr = np.array(shuffled_corrs)
        pctile = float(np.mean(shuffled_arr <= abs(observed_corr))) * 100.0
        return round(pctile, 2)

    except Exception as exc:
        logger.warning(f"Monte Carlo permutation failed: {exc}")
        return 0.0


class CorrelationDiscoverer:
    """
    Scans available frontier signals for statistically significant
    correlations with returns using a Monte Carlo permutation filter.

    Parameters
    ----------
    rolling_window : int
        Window size for rolling correlation (default 20 trading days).
    mc_seed : int or None
        Optional random seed for reproducible permutation tests.
    """

    def __init__(self, rolling_window: int = 20, mc_seed: Optional[int] = None):
        self._window = rolling_window
        self._mc_seed = mc_seed

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def discover(
        self,
        returns: "pd.Series",
        signals: Dict[str, "pd.Series"],
        min_abs_corr: float = 0.15,
        max_abs_corr: float = 0.85,
        min_obs: int = 30,
        n_shuffles: int = 500,
    ) -> List[Dict]:
        """
        For each signal in the signals dict, compute rolling correlation
        with returns and apply a Monte Carlo permutation filter.

        Parameters
        ----------
        returns        : pd.Series  — stock / index returns (daily)
        signals        : dict of {signal_name: pd.Series}
        min_abs_corr   : lower bound on abs(corr) — signals below are noise
        max_abs_corr   : upper bound on abs(corr) — above likely duplicates
        min_obs        : minimum overlapping observations required
        n_shuffles     : number of Monte Carlo permutation shuffles

        Returns
        -------
        list of dicts, each containing:
          signal_name, correlation, monte_carlo_pctile,
          lag_0_corr, lag_1_corr, lag_5_corr
        Only signals beating the 95th percentile of shuffled correlations
        are included.
        """
        if not _HAS_PANDAS:
            logger.error("pandas required for discover() — returning empty list")
            return []

        candidates = []

        for sig_name, sig_series in signals.items():
            try:
                result = self._evaluate_signal(
                    sig_name=sig_name,
                    sig_series=sig_series,
                    returns=returns,
                    min_abs_corr=min_abs_corr,
                    max_abs_corr=max_abs_corr,
                    min_obs=min_obs,
                    n_shuffles=n_shuffles,
                )
                if result is not None:
                    candidates.append(result)
            except Exception as exc:
                logger.warning(f"discover(): error evaluating '{sig_name}': {exc}")
                continue

        # Also test lag-1 and lag-5 versions of each signal
        for lag in (1, 5):
            for sig_name, sig_series in signals.items():
                lagged_name = f"{sig_name}_lag{lag}"
                try:
                    lagged_series = sig_series.shift(lag) if _HAS_PANDAS else sig_series
                    result = self._evaluate_signal(
                        sig_name=lagged_name,
                        sig_series=lagged_series,
                        returns=returns,
                        min_abs_corr=min_abs_corr,
                        max_abs_corr=max_abs_corr,
                        min_obs=min_obs,
                        n_shuffles=n_shuffles,
                    )
                    if result is not None:
                        candidates.append(result)
                except Exception as exc:
                    logger.warning(f"discover(): error evaluating lagged '{lagged_name}': {exc}")
                    continue

        candidates.sort(key=lambda c: c["monte_carlo_pctile"], reverse=True)
        logger.info(
            f"[CorrelationDiscoverer] discover(): {len(candidates)} candidates "
            f"from {len(signals)} signals (including lags)"
        )
        return candidates

    def discover_interactions(
        self,
        returns: "pd.Series",
        signals: Dict[str, "pd.Series"],
        max_combinations: int = 50,
    ) -> List[Dict]:
        """
        Generate pairwise product interaction terms from signals and run
        the same correlation + Monte Carlo filter on each product.

        Parameters
        ----------
        returns           : pd.Series — returns to correlate against
        signals           : dict of {signal_name: pd.Series}
        max_combinations  : cap on number of pairs to test (sorted alphabetically)

        Returns
        -------
        List of candidate dicts sorted by Monte Carlo percentile descending.
        Each dict includes the same fields as discover() with signal_name
        of the form "sigA_x_sigB".
        """
        if not _HAS_PANDAS:
            logger.error("pandas required for discover_interactions() — returning empty list")
            return []

        signal_names = sorted(signals.keys())
        pairs = list(combinations(signal_names, 2))[:max_combinations]

        candidates = []
        for name_a, name_b in pairs:
            interaction_name = f"{name_a}_x_{name_b}"
            try:
                aligned_a, aligned_b = signals[name_a].align(signals[name_b], join="inner")
                product = (aligned_a * aligned_b).dropna()
                if len(product) < 10:
                    continue

                result = self._evaluate_signal(
                    sig_name=interaction_name,
                    sig_series=product,
                    returns=returns,
                    min_abs_corr=0.15,
                    max_abs_corr=0.85,
                    min_obs=30,
                    n_shuffles=500,
                )
                if result is not None:
                    result["is_interaction"] = True
                    result["components"] = [name_a, name_b]
                    candidates.append(result)
            except Exception as exc:
                logger.warning(
                    f"discover_interactions(): error on pair "
                    f"({name_a}, {name_b}): {exc}"
                )
                continue

        candidates.sort(key=lambda c: c["monte_carlo_pctile"], reverse=True)
        logger.info(
            f"[CorrelationDiscoverer] discover_interactions(): "
            f"{len(candidates)} interaction candidates from {len(pairs)} pairs tested"
        )
        return candidates

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evaluate_signal(
        self,
        sig_name: str,
        sig_series: "pd.Series",
        returns: "pd.Series",
        min_abs_corr: float,
        max_abs_corr: float,
        min_obs: int,
        n_shuffles: int,
    ) -> Optional[Dict]:
        """
        Evaluate a single signal series.  Returns a candidate dict or None
        if the signal does not pass all filters.
        """
        # Overlap count
        aligned_sig, aligned_ret = sig_series.align(returns, join="inner")
        obs = int(aligned_sig.notna().sum())
        if obs < min_obs:
            logger.debug(f"'{sig_name}': insufficient obs ({obs} < {min_obs})")
            return None

        # Rolling correlation
        corr = _rolling_corr_mean(sig_series, returns, window=self._window)
        if corr != corr:  # NaN check
            return None

        abs_corr = abs(corr)
        if not (min_abs_corr <= abs_corr <= max_abs_corr):
            logger.debug(
                f"'{sig_name}': corr={corr:.4f} outside "
                f"[{min_abs_corr}, {max_abs_corr}] — skipped"
            )
            return None

        # Monte Carlo permutation test
        mc_pctile = _monte_carlo_percentile(
            sig_series, returns, corr,
            n_shuffles=n_shuffles,
            window=self._window,
            rng_seed=self._mc_seed,
        )
        if mc_pctile < 95.0:
            logger.debug(
                f"'{sig_name}': MC percentile {mc_pctile:.1f} < 95 — rejected"
            )
            return None

        # Lag correlations for additional context
        lag_0 = _lag_corr(sig_series, returns, lag=0)
        lag_1 = _lag_corr(sig_series, returns, lag=1)
        lag_5 = _lag_corr(sig_series, returns, lag=5)

        candidate = {
            "signal_name": sig_name,
            "correlation": round(corr, 4),
            "monte_carlo_pctile": mc_pctile,
            "lag_0_corr": round(lag_0, 4) if lag_0 == lag_0 else None,
            "lag_1_corr": round(lag_1, 4) if lag_1 == lag_1 else None,
            "lag_5_corr": round(lag_5, 4) if lag_5 == lag_5 else None,
            "n_obs": obs,
            "is_interaction": False,
            "components": [],
        }
        logger.info(
            f"[CorrelationDiscoverer] CANDIDATE '{sig_name}': "
            f"corr={corr:.4f}, MC_pctile={mc_pctile:.1f}, obs={obs}"
        )
        return candidate
