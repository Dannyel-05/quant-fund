import logging
from typing import Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class Normaliser:
    """
    Rolling normalisation that prevents lookahead bias.

    fit_transform() maintains an in-memory rolling history per feature
    and returns the z-score of the incoming value vs. all *prior* values
    in the window (the current value is excluded from the mean/std
    calculation to prevent lookahead).

    transform_series() applies the same principle to a full pandas Series
    using expanding windows with a one-period shift.
    """

    def __init__(self, window: int = 252):
        """
        Parameters
        ----------
        window : int
            Maximum number of historical observations to retain per feature.
            Default 252 (~1 trading year).
        """
        self.window = window
        self._history: Dict[str, List[float]] = {}

    # ------------------------------------------------------------------
    # Online (streaming) interface
    # ------------------------------------------------------------------

    def fit_transform(self, feature_name: str, value: float) -> float:
        """
        Append `value` to the rolling history for `feature_name` and return
        its z-score vs. the preceding observations.

        Returns 0.0 if fewer than 20 prior observations are available
        (insufficient history for a meaningful z-score).

        Parameters
        ----------
        feature_name : str
            Unique identifier for the feature (e.g. "AAPL_reddit_sentiment").
        value : float
            New raw observation.

        Returns
        -------
        float
            Z-score of `value` vs. prior window; 0.0 if insufficient history.
        """
        if feature_name not in self._history:
            self._history[feature_name] = []

        hist = self._history[feature_name]

        # Compute z-score *before* appending (no lookahead)
        result = 0.0
        if len(hist) >= 20:
            arr = np.array(hist, dtype=float)
            mean = arr.mean()
            std = arr.std()
            if std >= 1e-8:
                result = float((value - mean) / std)

        # Now append and trim to window
        hist.append(float(value))
        if len(hist) > self.window:
            self._history[feature_name] = hist[-self.window:]

        return result

    # ------------------------------------------------------------------
    # Batch (pandas) interface
    # ------------------------------------------------------------------

    def transform_series(self, series: pd.Series, window: int = None) -> pd.Series:
        """
        Normalise a pandas Series using an expanding (or rolling) window.
        The mean and std are computed on the *shifted* series so that
        the z-score for row t uses only rows 0..t-1 (no lookahead).

        Parameters
        ----------
        series : pd.Series
            Raw time-series of a single feature.
        window : int, optional
            Override instance window size for this call.

        Returns
        -------
        pd.Series
            Z-scored series; NaN positions are filled with 0.
        """
        min_periods = 20
        rolling_mean = series.expanding(min_periods=min_periods).mean().shift(1)
        rolling_std = series.expanding(min_periods=min_periods).std().shift(1)
        z = (series - rolling_mean) / rolling_std.clip(lower=1e-8)
        return z.fillna(0.0)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def clip_outliers(self, value: float, n_sigma: float = 3.0) -> float:
        """
        Clip value to ±n_sigma standard deviations.
        Assumes `value` is already a z-score (output of fit_transform).

        Parameters
        ----------
        value : float
            Z-scored value.
        n_sigma : float
            Clipping threshold (default 3.0).

        Returns
        -------
        float
        """
        return float(np.clip(value, -n_sigma, n_sigma))

    def clear_history(self, feature_name: str = None) -> None:
        """Clear rolling history for one feature or all features."""
        if feature_name:
            self._history.pop(feature_name, None)
        else:
            self._history.clear()
