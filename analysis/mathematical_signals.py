"""
Mathematical Signals Module
============================
Hidden Markov Models, autocorrelation analysis, spectral analysis, and
mean-reversion half-life estimation — all combined into a unified signal layer.

Classes:
  - HMMSignals               — GaussianHMM on price sequences
  - AutocorrelationSignals   — Ljung-Box, momentum, mean-reversion, spectral
  - MathematicalSignals      — Main orchestrator (cached, 1-hour TTL)

Dependencies (graceful on failure):
  - hmmlearn   (HMM fitting — skipped if not installed)
  - scipy.stats (Ljung-Box test, OLS)
  - numpy.fft  (spectral analysis)
  - statsmodels (supplementary OLS)
  - yfinance   (only used in __main__ test block)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependency: hmmlearn
# ---------------------------------------------------------------------------
try:
    from hmmlearn import hmm as _hmm_module
    HMM_AVAILABLE = True
except ImportError:
    _hmm_module = None
    HMM_AVAILABLE = False
    logger.warning("hmmlearn not installed — HMM signals will be skipped. "
                   "Install with: pip install hmmlearn")

# ---------------------------------------------------------------------------
# Optional dependency: scipy
# ---------------------------------------------------------------------------
try:
    from scipy import stats as _scipy_stats
    SCIPY_AVAILABLE = True
except ImportError:
    _scipy_stats = None
    SCIPY_AVAILABLE = False
    logger.warning("scipy not installed — statistical tests will be limited.")

# ---------------------------------------------------------------------------
# Optional dependency: statsmodels
# ---------------------------------------------------------------------------
try:
    import statsmodels.api as _sm
    STATSMODELS_AVAILABLE = True
except ImportError:
    _sm = None
    STATSMODELS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_HISTORY_DAYS = 252          # minimum bars needed to fit HMM
HMM_COMPONENTS = 3              # bull / neutral / bear
CACHE_TTL_SECONDS = 3600        # 1 hour cache


# ===========================================================================
# HMMSignals
# ===========================================================================

class HMMSignals:
    """
    Fits a GaussianHMM (3 states) on a ticker's price history and maps
    latent states to bull / neutral / bear regimes.

    Features per day (4-dimensional observation):
        1. daily_return      — log return
        2. volume_zscore     — z-score of raw volume vs 20-day rolling mean
        3. high_low_range    — (High - Low) / Close  (normalised range)
        4. close_open_gap    — (Close - Open) / Open (intra-day gap)
    """

    def __init__(self):
        self._models: Dict[str, Any] = {}           # ticker → fitted HMM
        self._state_map: Dict[str, Dict] = {}       # ticker → {bull, bear, neutral}
        self._last_obs: Dict[str, np.ndarray] = {}  # ticker → last features array

    # ------------------------------------------------------------------
    def _build_features(self, price_df: pd.DataFrame) -> Optional[np.ndarray]:
        """
        Build the (N, 4) observation matrix from a price DataFrame.
        Expected columns: Open, High, Low, Close, Volume.
        Returns None if insufficient data or missing columns.
        """
        required = {"Open", "High", "Low", "Close", "Volume"}
        missing = required - set(price_df.columns)
        if missing:
            logger.warning("HMMSignals: missing columns %s", missing)
            return None

        df = price_df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df = df.dropna()
        if len(df) < MIN_HISTORY_DAYS:
            return None

        # 1. log daily return
        log_ret = np.log(df["Close"] / df["Close"].shift(1)).fillna(0).values

        # 2. volume z-score vs 20-day rolling mean
        vol = df["Volume"].astype(float)
        vol_mean = vol.rolling(20, min_periods=5).mean()
        vol_std  = vol.rolling(20, min_periods=5).std().replace(0, np.nan)
        vol_z    = ((vol - vol_mean) / vol_std).fillna(0).values

        # 3. high-low range normalised by close
        hl_range = ((df["High"] - df["Low"]) / df["Close"].replace(0, np.nan)).fillna(0).values

        # 4. close-open intra-day gap
        co_gap = ((df["Close"] - df["Open"]) / df["Open"].replace(0, np.nan)).fillna(0).values

        features = np.column_stack([log_ret, vol_z, hl_range, co_gap])
        return features

    # ------------------------------------------------------------------
    def fit(self, ticker: str, price_df: pd.DataFrame) -> bool:
        """
        Fit a GaussianHMM for *ticker*.  Returns True on success.
        """
        if not HMM_AVAILABLE:
            return False

        try:
            features = self._build_features(price_df)
            if features is None:
                logger.info("HMMSignals.fit: insufficient data for %s", ticker)
                return False

            model = _hmm_module.GaussianHMM(
                n_components=HMM_COMPONENTS,
                covariance_type="diag",
                n_iter=100,
                random_state=42,
                verbose=False,
            )
            model.fit(features)

            # Map states to bull / neutral / bear using mean return of each state
            mean_returns = model.means_[:, 0]   # column 0 = daily_return
            sorted_states = np.argsort(mean_returns)   # ascending
            bear_state    = int(sorted_states[0])
            neutral_state = int(sorted_states[1])
            bull_state    = int(sorted_states[2])

            # Validate: bull must have mean > 0, bear must have mean < 0
            if mean_returns[bull_state] <= 0 or mean_returns[bear_state] >= 0:
                logger.info("HMMSignals.fit: state validation failed for %s "
                            "(bull_mean=%.4f, bear_mean=%.4f)",
                            ticker, mean_returns[bull_state], mean_returns[bear_state])
                return False

            self._models[ticker] = model
            self._state_map[ticker] = {
                "bull": bull_state,
                "neutral": neutral_state,
                "bear": bear_state,
                "mean_returns": mean_returns,
            }
            self._last_obs[ticker] = features   # store full sequence
            logger.info("HMMSignals.fit: fitted for %s (bull=%d bear=%d)",
                        ticker, bull_state, bear_state)
            return True

        except Exception as exc:
            logger.warning("HMMSignals.fit error for %s: %s", ticker, exc)
            return False

    # ------------------------------------------------------------------
    def get_state(self, ticker: str) -> Dict[str, Any]:
        """
        Decode the current hidden state for *ticker*.

        Returns dict:
            current_state   — 0/1/2
            state_label     — 'bull' / 'neutral' / 'bear'
            state_probs     — list[float] length 3
            likely_next_state — most probable next state index
            hmm_signal      — +1 (bull) / -1 (bear) / 0 (neutral)
        """
        empty = {
            "current_state": None,
            "state_label": "unknown",
            "state_probs": [],
            "likely_next_state": None,
            "hmm_signal": 0,
        }
        if ticker not in self._models:
            return empty

        try:
            model   = self._models[ticker]
            smap    = self._state_map[ticker]
            obs     = self._last_obs[ticker]

            # Posterior state probabilities for the full sequence
            state_probs_seq = model.predict_proba(obs)
            current_probs   = state_probs_seq[-1]          # last bar
            current_state   = int(np.argmax(current_probs))

            # Likely next state via transition matrix
            trans_row       = model.transmat_[current_state]
            likely_next     = int(np.argmax(trans_row))

            # Label
            label_map = {smap["bull"]: "bull", smap["neutral"]: "neutral",
                         smap["bear"]: "bear"}
            label = label_map.get(current_state, "unknown")

            # Signal
            if current_state == smap["bull"]:
                signal = 1
            elif current_state == smap["bear"]:
                signal = -1
            else:
                signal = 0

            return {
                "current_state": current_state,
                "state_label": label,
                "state_probs": current_probs.tolist(),
                "likely_next_state": likely_next,
                "hmm_signal": signal,
            }

        except Exception as exc:
            logger.warning("HMMSignals.get_state error for %s: %s", ticker, exc)
            return empty

    # ------------------------------------------------------------------
    def get_signal_strength(self, ticker: str) -> float:
        """
        Returns confidence of the current HMM signal in [0, 1].
        Uses the max probability of the current state.
        """
        if ticker not in self._models:
            return 0.0
        try:
            state_info = self.get_state(ticker)
            if not state_info["state_probs"]:
                return 0.0
            return float(max(state_info["state_probs"]))
        except Exception:
            return 0.0


# ===========================================================================
# AutocorrelationSignals
# ===========================================================================

class AutocorrelationSignals:
    """
    Autocorrelation-based signals:
      - Ljung-Box test for serial dependence
      - Momentum signals from significant autocorrelation
      - Ornstein-Uhlenbeck mean-reversion half-life
      - Mean-reversion z-score signal
      - Spectral dominant cycle (FFT)
    """

    LAGS_TO_TEST = [1, 2, 3, 5, 10]

    # ------------------------------------------------------------------
    def ljung_box_test(self, returns_series: pd.Series) -> List[Dict[str, Any]]:
        """
        Ljung-Box test at lags 1, 2, 3, 5, 10.

        Returns list of dicts: {lag, statistic, p_value, is_significant}
        Falls back to manual ACF-based approximation if statsmodels absent.
        """
        results = []
        arr = np.array(returns_series.dropna(), dtype=float)
        n   = len(arr)
        if n < 20:
            return results

        try:
            if STATSMODELS_AVAILABLE:
                import statsmodels.stats.diagnostic as diag
                lb = diag.acorr_ljungbox(arr, lags=self.LAGS_TO_TEST, return_df=True)
                for lag, row in zip(self.LAGS_TO_TEST, lb.itertuples()):
                    results.append({
                        "lag": lag,
                        "statistic": float(row.lb_stat),
                        "p_value": float(row.lb_pvalue),
                        "is_significant": float(row.lb_pvalue) < 0.05,
                    })
            else:
                # Manual Q-statistic approximation
                for lag in self.LAGS_TO_TEST:
                    acf_vals = [np.corrcoef(arr[k:], arr[:-k])[0, 1] if k < n else 0.0
                                for k in range(1, lag + 1)]
                    q = float(n * (n + 2) * sum(acf_vals[k] ** 2 / (n - k - 1)
                                                 for k in range(lag)))
                    if SCIPY_AVAILABLE:
                        p = float(1 - _scipy_stats.chi2.cdf(q, df=lag))
                    else:
                        p = 1.0   # cannot compute without scipy
                    results.append({
                        "lag": lag,
                        "statistic": q,
                        "p_value": p,
                        "is_significant": p < 0.05,
                    })
        except Exception as exc:
            logger.warning("ljung_box_test error: %s", exc)

        return results

    # ------------------------------------------------------------------
    def get_momentum_signal(self, ticker: str, price_df: pd.DataFrame) -> Dict[str, Any]:
        """
        If significant positive autocorrelation exists at lag k, generate a
        momentum signal aligned with the most recent k-day return.

        Returns dict: {signal (+1/-1/0), lag, acf_value, p_value, reason}
        """
        result = {"signal": 0, "lag": None, "acf_value": None,
                  "p_value": None, "reason": "no_significant_autocorrelation"}
        try:
            if "Close" not in price_df.columns or len(price_df) < 60:
                result["reason"] = "insufficient_data"
                return result

            rets = np.log(price_df["Close"] / price_df["Close"].shift(1)).dropna()
            lb_results = self.ljung_box_test(rets)

            for lb in lb_results:
                if lb["is_significant"]:
                    lag = lb["lag"]
                    arr = rets.values
                    if lag < len(arr):
                        acf = float(np.corrcoef(arr[lag:], arr[:-lag])[0, 1])
                        if abs(acf) > 0.05:
                            # Momentum direction: most recent lag-day return
                            recent = float(arr[-lag:].sum())
                            direction = 1 if recent > 0 else -1
                            signal_val = direction if acf > 0 else -direction
                            result = {
                                "signal": signal_val,
                                "lag": lag,
                                "acf_value": acf,
                                "p_value": lb["p_value"],
                                "reason": f"positive_autocorr_lag{lag}" if acf > 0
                                           else f"negative_autocorr_lag{lag}",
                            }
                            return result

        except Exception as exc:
            logger.warning("get_momentum_signal error for %s: %s", ticker, exc)

        return result

    # ------------------------------------------------------------------
    def mean_reversion_halflife(self, price_series: pd.Series) -> Optional[float]:
        """
        Estimate Ornstein-Uhlenbeck half-life in days via OLS regression:
            Δp_t = λ * (p_{t-1} - μ) + ε_t
        Half-life = -ln(2) / λ   (only meaningful when λ < 0)

        Returns half-life in days, or None if not mean-reverting.
        """
        try:
            arr = np.array(price_series.dropna(), dtype=float)
            if len(arr) < 30:
                return None

            lag   = arr[:-1]
            delta = np.diff(arr)

            # OLS: delta ~ const + lambda * lag
            X = np.column_stack([np.ones(len(lag)), lag])
            try:
                coef, *_ = np.linalg.lstsq(X, delta, rcond=None)
            except np.linalg.LinAlgError:
                return None

            lam = coef[1]
            if lam >= 0:
                return None   # not mean-reverting

            half_life = float(-np.log(2) / lam)
            if half_life <= 0 or half_life > 365:
                return None

            return half_life

        except Exception as exc:
            logger.warning("mean_reversion_halflife error: %s", exc)
            return None

    # ------------------------------------------------------------------
    def get_mean_reversion_signal(self, ticker: str, price_df: pd.DataFrame,
                                   halflife_threshold: float = 10.0) -> Dict[str, Any]:
        """
        Generate a z-score mean-reversion signal for stocks with half-life < *halflife_threshold*.

        Returns dict: {signal (+1/-1/0), zscore, halflife, reason}
        """
        result = {"signal": 0, "zscore": None, "halflife": None,
                  "reason": "not_mean_reverting"}
        try:
            if "Close" not in price_df.columns or len(price_df) < 40:
                result["reason"] = "insufficient_data"
                return result

            prices    = price_df["Close"].dropna()
            half_life = self.mean_reversion_halflife(prices)

            if half_life is None or half_life >= halflife_threshold:
                result["halflife"] = half_life
                result["reason"] = (f"halflife_too_long ({half_life:.1f}d)"
                                    if half_life is not None else "not_mean_reverting")
                return result

            # Use lookback of ~3 half-lives for z-score
            lookback = max(int(half_life * 3), 20)
            window   = prices.iloc[-lookback:]
            mu       = float(window.mean())
            sigma    = float(window.std())
            if sigma == 0:
                return result

            zscore = (float(prices.iloc[-1]) - mu) / sigma

            # Signal: revert toward mean
            if zscore > 1.0:
                signal = -1
            elif zscore < -1.0:
                signal = 1
            else:
                signal = 0

            result = {
                "signal": signal,
                "zscore": round(zscore, 3),
                "halflife": round(half_life, 2),
                "reason": f"mean_reverting_hl={half_life:.1f}d zscore={zscore:.2f}",
            }

        except Exception as exc:
            logger.warning("get_mean_reversion_signal error for %s: %s", ticker, exc)

        return result

    # ------------------------------------------------------------------
    def spectral_dominant_cycle(self, price_series: pd.Series) -> Optional[float]:
        """
        Use FFT on detrended price series to find the dominant cycle length in days.

        Returns cycle length in days, or None on failure.
        """
        try:
            arr = np.array(price_series.dropna(), dtype=float)
            if len(arr) < 60:
                return None

            # Detrend by subtracting linear fit
            x = np.arange(len(arr))
            poly = np.polyfit(x, arr, 1)
            detrended = arr - np.polyval(poly, x)

            # Hanning window to reduce spectral leakage
            windowed = detrended * np.hanning(len(detrended))

            # FFT — ensure consistent sizes
            n = len(windowed)
            fft_vals  = np.fft.rfft(windowed)
            n_fft     = len(fft_vals)                    # rfft output length
            power     = np.abs(fft_vals) ** 2
            freqs     = np.fft.rfftfreq(n)[:n_fft]      # guaranteed same length

            # Exclude DC component (freq=0) and very high frequencies (< 5 days)
            valid_mask = (freqs > 0) & (freqs < 0.2)   # > 5 day cycles
            if not np.any(valid_mask):
                return None

            dominant_freq = freqs[valid_mask][np.argmax(power[valid_mask])]
            if dominant_freq == 0:
                return None

            cycle_days = float(1.0 / dominant_freq)
            return round(cycle_days, 1)

        except Exception as exc:
            logger.warning("spectral_dominant_cycle error: %s", exc)
            return None


# ===========================================================================
# MathematicalSignals  (main orchestrator)
# ===========================================================================

class MathematicalSignals:
    """
    Main orchestrator that runs HMM + autocorrelation analysis for each ticker
    and returns a combined signal in [-1, +1].

    Results are cached for CACHE_TTL_SECONDS (1 hour) to avoid re-fitting HMM
    on every call.
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        self._config  = self._load_config(config_path)
        self._hmm     = HMMSignals()
        self._ac      = AutocorrelationSignals()
        self._cache: Dict[str, Dict[str, Any]] = {}   # ticker → results
        self._cache_ts: Dict[str, float] = {}          # ticker → timestamp

    # ------------------------------------------------------------------
    @staticmethod
    def _load_config(config_path: str) -> Dict:
        try:
            import yaml
            with open(config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    # ------------------------------------------------------------------
    def _is_cached(self, ticker: str) -> bool:
        if ticker not in self._cache:
            return False
        age = time.time() - self._cache_ts.get(ticker, 0)
        return age < CACHE_TTL_SECONDS

    # ------------------------------------------------------------------
    def _store_cache(self, ticker: str, result: Dict):
        self._cache[ticker]    = result
        self._cache_ts[ticker] = time.time()

    # ------------------------------------------------------------------
    def run_all(self, tickers: List[str],
                price_data: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Dict]:
        """
        Run HMM + autocorrelation for each ticker.

        *price_data* is a dict of ticker → OHLCV DataFrame.
        If not supplied, attempts to download via yfinance (for standalone use).

        Returns dict: ticker → {hmm, autocorr, momentum, mean_reversion,
                                 spectral, combined_signal}
        """
        if price_data is None:
            price_data = self._fetch_yfinance(tickers)

        results = {}
        for ticker in tickers:
            if self._is_cached(ticker):
                results[ticker] = self._cache[ticker]
                continue

            df = price_data.get(ticker)
            res = self._analyse_ticker(ticker, df)
            self._store_cache(ticker, res)
            results[ticker] = res

        return results

    # ------------------------------------------------------------------
    def _analyse_ticker(self, ticker: str, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "ticker": ticker,
            "hmm": {},
            "momentum": {},
            "mean_reversion": {},
            "spectral_cycle_days": None,
            "combined_signal": 0.0,
            "computed_at": datetime.utcnow().isoformat(),
        }

        if df is None or df.empty:
            out["error"] = "no_price_data"
            return out

        # --- HMM ---
        if HMM_AVAILABLE:
            fitted = self._hmm.fit(ticker, df)
            if fitted:
                out["hmm"] = self._hmm.get_state(ticker)
                out["hmm"]["signal_strength"] = self._hmm.get_signal_strength(ticker)

        # --- Autocorrelation / momentum ---
        try:
            out["momentum"] = self._ac.get_momentum_signal(ticker, df)
        except Exception as exc:
            logger.warning("momentum signal error %s: %s", ticker, exc)

        # --- Mean reversion ---
        try:
            out["mean_reversion"] = self._ac.get_mean_reversion_signal(ticker, df)
        except Exception as exc:
            logger.warning("mean_reversion signal error %s: %s", ticker, exc)

        # --- Spectral ---
        try:
            if "Close" in df.columns:
                out["spectral_cycle_days"] = self._ac.spectral_dominant_cycle(df["Close"])
        except Exception as exc:
            logger.warning("spectral error %s: %s", ticker, exc)

        # --- Combined ---
        out["combined_signal"] = self._combine_signals(out)
        return out

    # ------------------------------------------------------------------
    @staticmethod
    def _combine_signals(res: Dict[str, Any]) -> float:
        """
        Weighted combination of all sub-signals, clamped to [-1, +1].

        Weights:
          - HMM signal * strength    : 50%
          - Momentum signal          : 30%
          - Mean-reversion signal    : 20%
        """
        total_weight = 0.0
        weighted_sum = 0.0

        # HMM
        hmm = res.get("hmm", {})
        if hmm and hmm.get("hmm_signal") is not None:
            sig      = float(hmm["hmm_signal"])
            strength = float(hmm.get("signal_strength", 0.5))
            weighted_sum  += sig * strength * 0.50
            total_weight  += 0.50

        # Momentum
        mom = res.get("momentum", {})
        if mom and mom.get("signal") != 0:
            weighted_sum += float(mom["signal"]) * 0.30
            total_weight += 0.30

        # Mean reversion
        mr = res.get("mean_reversion", {})
        if mr and mr.get("signal") != 0:
            weighted_sum += float(mr["signal"]) * 0.20
            total_weight += 0.20

        if total_weight == 0:
            return 0.0

        combined = weighted_sum / total_weight
        return float(np.clip(combined, -1.0, 1.0))

    # ------------------------------------------------------------------
    def get_combined_signal(self, ticker: str) -> float:
        """
        Returns the cached combined signal for *ticker*, or 0.0 if not computed.
        Call run_all() first to populate the cache.
        """
        if ticker in self._cache:
            return float(self._cache[ticker].get("combined_signal", 0.0))
        return 0.0

    # ------------------------------------------------------------------
    # Convenience alias expected by callers
    # ------------------------------------------------------------------
    def analyse(self, tickers, price_data=None):
        """Alias for run_all() — accepts list or single ticker string."""
        if isinstance(tickers, str):
            tickers = [tickers]
        return self.run_all(tickers, price_data=price_data)

    # ------------------------------------------------------------------
    @staticmethod
    def _fetch_yfinance(tickers: List[str]) -> Dict[str, pd.DataFrame]:
        try:
            import yfinance as yf
            data = {}
            for t in tickers:
                try:
                    df = yf.download(t, period="2y", auto_adjust=True, progress=False)
                    if df is not None and not df.empty:
                        # Flatten MultiIndex columns (yfinance ≥0.2.x returns MultiIndex)
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = [c[0] for c in df.columns]
                        data[t] = df
                except Exception as exc:
                    logger.warning("yfinance download failed for %s: %s", t, exc)
            return data
        except ImportError:
            logger.warning("yfinance not installed; cannot auto-fetch price data.")
            return {}


# ===========================================================================
# KalmanSignalSmoother
# ===========================================================================

try:
    from pykalman import KalmanFilter as _PyKalmanFilter
    PYKALMAN_AVAILABLE = True
except ImportError:
    _PyKalmanFilter = None
    PYKALMAN_AVAILABLE = False
    logger.warning("pykalman not installed — Kalman smoothing unavailable")


class KalmanSignalSmoother:
    """
    Applies Kalman filtering to raw signal scores to reduce noise.

    State-space model:
        State:       θ_t = θ_{t-1} + w_t    (w_t ~ N(0, Q))
        Observation: y_t = θ_t + v_t         (v_t ~ N(0, R))

    Usage:
        smoother = KalmanSignalSmoother()
        smoothed_score = smoother.smooth_score(raw_scores_list)
    """

    def __init__(self, transition_cov: float = 1e-3, observation_cov: float = 1e-1) -> None:
        self._Q = transition_cov    # process noise — how fast state can change
        self._R = observation_cov   # observation noise — how noisy raw signals are
        self._filters: Dict[str, Any] = {}

    def smooth_score(self, scores: List[float], ticker: str = "default") -> float:
        """
        Given a list of raw signal scores (most recent last), return
        the Kalman-smoothed estimate of the current score.
        Returns the last raw score if pykalman unavailable or too few points.
        """
        if not scores:
            return 0.0
        if not PYKALMAN_AVAILABLE or len(scores) < 3:
            return float(scores[-1])
        try:
            obs = np.array(scores, dtype=float).reshape(-1, 1)
            kf = _PyKalmanFilter(
                transition_matrices=[[1]],
                observation_matrices=[[1]],
                transition_covariance=[[self._Q]],
                observation_covariance=[[self._R]],
                initial_state_mean=[obs[0, 0]],
                initial_state_covariance=[[1.0]],
                n_dim_state=1,
                n_dim_obs=1,
            )
            smoothed, _ = kf.smooth(obs)
            return float(smoothed[-1, 0])
        except Exception as exc:
            logger.debug("KalmanSignalSmoother error: %s", exc)
            return float(scores[-1])

    def smooth_series(self, series: np.ndarray) -> np.ndarray:
        """
        Smooth an entire price/score series. Returns same-length array.
        Falls back to original series if unavailable.
        """
        if not PYKALMAN_AVAILABLE or len(series) < 3:
            return series
        try:
            obs = np.array(series, dtype=float).reshape(-1, 1)
            kf = _PyKalmanFilter(
                transition_matrices=[[1]],
                observation_matrices=[[1]],
                transition_covariance=[[self._Q]],
                observation_covariance=[[self._R]],
                initial_state_mean=[obs[0, 0]],
                initial_state_covariance=[[1.0]],
                n_dim_state=1,
                n_dim_obs=1,
            )
            smoothed, _ = kf.smooth(obs)
            return smoothed[:, 0]
        except Exception as exc:
            logger.debug("KalmanSignalSmoother.smooth_series error: %s", exc)
            return series


# ===========================================================================
# KalmanPairsTrader
# ===========================================================================

class KalmanPairsTrader:
    """
    Dynamic hedge ratio estimation for pairs trading using Kalman filter.

    State: x_t = [β_t, α_t]^T  (slope=hedge_ratio, intercept)
    Observation: y_t = H_t * x_t + v_t
    where H_t = [price_x_t, 1]

    Generates spread z-score signals:
      Entry LONG spread  : z < -2.0
      Entry SHORT spread : z >  2.0
      Exit              : z crosses zero

    Usage:
        kpt = KalmanPairsTrader(delta=1e-4)
        for px, py in zip(prices_x, prices_y):
            signal = kpt.update(px, py)
    """

    def __init__(self, delta: float = 1e-4) -> None:
        self._delta    = delta
        self._Vw       = delta / (1.0 - delta)   # process noise variance
        # State: [beta, alpha] — will be initialised on first update
        self._theta    : Optional[np.ndarray] = None
        self._P        : Optional[np.ndarray] = None  # state covariance
        self._spread_history: List[float] = []
        self._e_history     : List[float] = []

    def update(self, price_x: float, price_y: float) -> Dict[str, Any]:
        """
        Process one new price pair.  Returns dict with keys:
          hedge_ratio, intercept, spread, spread_mean, spread_std, z_score, signal
        signal: +1 = long spread, -1 = short spread, 0 = hold
        """
        if not PYKALMAN_AVAILABLE:
            return {"signal": 0, "z_score": 0.0, "hedge_ratio": 1.0, "spread": price_y - price_x}

        H = np.array([[price_x, 1.0]])   # (1, 2)

        # Initialise state on first call
        if self._theta is None:
            self._theta = np.array([1.0, 0.0])   # [beta=1, alpha=0]
            self._P     = np.eye(2)

        # Prediction step
        # (state transition = identity, process noise Q = Vw * I)
        Q = self._Vw * np.eye(2)
        P_pred = self._P + Q

        # Innovation
        y_hat = float(np.squeeze(H @ self._theta))
        e     = price_y - y_hat                        # innovation (residual)
        S     = float(np.squeeze(H @ P_pred @ H.T)) + float(np.var(self._e_history[-50:]) if len(self._e_history) >= 10 else 1.0)

        # Kalman gain
        K = (P_pred @ H.T) / S    # (2, 1)

        # State update
        self._theta = self._theta + K.flatten() * e
        self._P     = (np.eye(2) - K @ H) @ P_pred

        beta  = float(self._theta[0])
        alpha = float(self._theta[1])

        # Spread
        spread = price_y - beta * price_x - alpha
        self._spread_history.append(spread)
        self._e_history.append(e)

        # Keep rolling window (250 days)
        if len(self._spread_history) > 250:
            self._spread_history = self._spread_history[-250:]
        if len(self._e_history) > 250:
            self._e_history = self._e_history[-250:]

        # Z-score from rolling window
        if len(self._spread_history) >= 20:
            mu  = float(np.mean(self._spread_history))
            std = float(np.std(self._spread_history))
        else:
            mu, std = 0.0, 1.0

        z = (spread - mu) / std if std > 1e-10 else 0.0

        # Signal
        if z < -2.0:
            signal = 1     # long spread (buy Y, sell X)
        elif z > 2.0:
            signal = -1    # short spread (sell Y, buy X)
        elif abs(z) < 0.1:
            signal = 0     # exit (mean-reverted)
        else:
            signal = 0     # hold

        return {
            "hedge_ratio": beta,
            "intercept":   alpha,
            "spread":      spread,
            "spread_mean": mu,
            "spread_std":  std,
            "z_score":     z,
            "signal":      signal,
        }

    def reset(self) -> None:
        self._theta = None
        self._P     = None
        self._spread_history.clear()
        self._e_history.clear()


# ===========================================================================
# WaveletSignal
# ===========================================================================

try:
    import pywt as _pywt
    PYWT_AVAILABLE = True
except ImportError:
    _pywt = None
    PYWT_AVAILABLE = False
    logger.warning("PyWavelets not installed — wavelet analysis unavailable")


class WaveletSignal:
    """
    Multi-scale wavelet decomposition for financial price series.

    Uses Daubechies db4 wavelet — empirically strong for financial time series.
    Decomposes into 4 detail levels + approximation:
      D1 (2-4 day)   : noise / microstructure
      D2 (4-8 day)   : short swing noise
      D3 (8-16 day)  : swing trading cycle <- primary trading signal
      D4 (16-32 day) : medium-term momentum
      A4 (32+ day)   : primary trend

    Soft thresholding for denoising:
      threshold = sigma * sqrt(2 * log(n))   (universal threshold)
      where sigma = MAD / 0.6745  (robust noise estimate from D1 coefficients)

    Returns:
      dominant_period   : most energetic cycle in days
      trend_direction   : +1 (up), -1 (down), 0 (flat) — from A4 slope
      cycle_phase       : +1 (buy), -1 (sell), 0 (neutral) — from D3 local extrema
      denoised_price    : soft-thresholded reconstruction
      trend_strength    : A4 energy / total energy (0-1)
      wavelet_score     : composite signal score (-1 to +1)
    """

    _WAVELET   = "db4"
    _LEVELS    = 4
    _D3_INDEX  = 2   # D3 is index 2 in detail coefficients (0=D1, 1=D2, 2=D3, 3=D4)

    def __init__(self) -> None:
        pass

    def analyse(self, prices: np.ndarray) -> Dict[str, Any]:
        """
        Analyse a price series.
        prices: 1-D numpy array, most recent last, minimum 32 points.
        Returns result dict.
        """
        default = {
            "dominant_period": None,
            "trend_direction": 0,
            "cycle_phase":     0,
            "denoised_price":  float(prices[-1]) if len(prices) else 0.0,
            "trend_strength":  0.5,
            "wavelet_score":   0.0,
        }

        if not PYWT_AVAILABLE:
            return default

        prices = np.array(prices, dtype=float)
        if len(prices) < 32:
            return default

        try:
            # Decompose
            coeffs = _pywt.wavedec(prices, self._WAVELET, level=self._LEVELS)
            # coeffs = [cA4, cD4, cD3, cD2, cD1]  (approximation first, then details high->low freq)
            cA4 = coeffs[0]
            details = coeffs[1:]   # [cD4, cD3, cD2, cD1]

            # ── Denoising ───────────────────────────────────────────────
            # Noise estimate from finest detail (cD1 = coeffs[-1])
            cD1 = coeffs[-1]
            sigma = float(np.median(np.abs(cD1)) / 0.6745)
            n     = len(prices)
            threshold = sigma * np.sqrt(2 * np.log(max(n, 2)))

            # Soft threshold all detail coefficients
            denoised_coeffs = [cA4]
            for d in details:
                denoised_coeffs.append(_pywt.threshold(d, threshold, mode="soft"))

            denoised_prices = _pywt.waverec(denoised_coeffs, self._WAVELET)
            # waverec may return slightly different length — trim/pad
            dn = denoised_prices[:len(prices)]
            denoised_last = float(dn[-1]) if len(dn) else float(prices[-1])

            # ── Trend from A4 ───────────────────────────────────────────
            if len(cA4) >= 3:
                trend_slope = float(cA4[-1] - cA4[-3])
                trend_direction = 1 if trend_slope > 0 else (-1 if trend_slope < 0 else 0)
            else:
                trend_direction = 0

            # Trend strength: A4 energy fraction
            a4_energy    = float(np.sum(cA4 ** 2))
            total_energy = a4_energy + sum(float(np.sum(d ** 2)) for d in details)
            trend_strength = a4_energy / max(total_energy, 1e-10)

            # ── Cycle phase from D3 (8-16 day swing) ───────────────────
            # cD3 is index 2 from high->low: [cD4, cD3, cD2, cD1]
            # Actually coeffs[1]=cD4, coeffs[2]=cD3 in wavedec with level=4
            cD3 = coeffs[2] if len(coeffs) > 2 else np.array([0.0])

            cycle_phase = 0
            if len(cD3) >= 3:
                last_d3 = float(cD3[-1])
                prev_d3 = float(cD3[-2])
                # Local minimum in D3 + positive trend -> buy
                # Local maximum in D3 + negative trend -> sell
                if last_d3 > prev_d3 and trend_direction >= 0:
                    cycle_phase = 1   # rising from trough
                elif last_d3 < prev_d3 and trend_direction <= 0:
                    cycle_phase = -1  # falling from peak

            # ── Dominant period ─────────────────────────────────────────
            # Most energetic detail level
            detail_energies = [float(np.sum(d ** 2)) for d in details]
            if detail_energies:
                strongest = int(np.argmax(detail_energies))
                # Map index to approximate period range (days)
                period_map = {0: 24, 1: 12, 2: 6, 3: 3}  # D4, D3, D2, D1 midpoints
                dominant_period = period_map.get(strongest, 12)
            else:
                dominant_period = None

            # ── Composite score ─────────────────────────────────────────
            wavelet_score = float(np.clip(
                0.6 * trend_direction + 0.4 * cycle_phase,
                -1.0, 1.0
            ))

            return {
                "dominant_period": dominant_period,
                "trend_direction": trend_direction,
                "cycle_phase":     cycle_phase,
                "denoised_price":  denoised_last,
                "trend_strength":  float(trend_strength),
                "wavelet_score":   wavelet_score,
            }

        except Exception as exc:
            logger.debug("WaveletSignal.analyse error: %s", exc)
            return default


# ===========================================================================
# Standalone test
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print("=" * 60)
    print("Mathematical Signals — standalone test on AAPL")
    print("=" * 60)

    engine = MathematicalSignals(config_path="config/settings.yaml")
    results = engine.run_all(["AAPL"])

    res = results.get("AAPL", {})
    print(f"\nTicker: AAPL")
    print(f"  HMM state      : {res.get('hmm', {}).get('state_label', 'N/A')}")
    print(f"  HMM signal     : {res.get('hmm', {}).get('hmm_signal', 'N/A')}")
    print(f"  HMM strength   : {res.get('hmm', {}).get('signal_strength', 'N/A')}")
    print(f"  Momentum       : {res.get('momentum', {}).get('signal', 0)}  "
          f"(lag={res.get('momentum', {}).get('lag')})")
    print(f"  Mean-reversion : {res.get('mean_reversion', {}).get('signal', 0)}  "
          f"(halflife={res.get('mean_reversion', {}).get('halflife')} days)")
    print(f"  Spectral cycle : {res.get('spectral_cycle_days')} days")
    print(f"  Combined signal: {res.get('combined_signal', 0.0):.3f}")
