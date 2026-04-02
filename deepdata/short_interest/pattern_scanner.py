"""Short interest pattern scanner with full statistical validation."""

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from scipy import stats as scipy_stats
    scipy_available = True
except ImportError:
    scipy_available = False

try:
    import ephem
    ephem_available = True
except ImportError:
    ephem_available = False

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata/short_interest")

N_PERMUTATIONS = 500


class ShortInterestPatternScanner:
    """Find non-obvious patterns in short interest data with rigorous statistical validation."""

    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.min_observations = config.get("min_observations", 20)
        self.p_value_threshold = config.get("p_value_threshold", 0.05)

    def scan_all(
        self,
        short_history: dict,
        price_history: dict,
        altdata_history: dict = None,
    ) -> list:
        """Run all pattern tests. Return list of pattern dicts that pass validation."""
        validated = []
        altdata = altdata_history or {}

        for ticker, sh in short_history.items():
            si_series = _to_series(sh)
            if si_series is None or len(si_series) < self.min_observations:
                continue

            ph = price_history.get(ticker)
            returns_series = _price_to_returns(ph)

            # Earnings timing
            earnings_dates = altdata.get(f"{ticker}_earnings_dates", [])
            try:
                result = self.test_earnings_timing_pattern(ticker, sh, earnings_dates)
                if result.get("valid"):
                    validated.append(result)
            except Exception as exc:
                logger.warning("earnings_timing for %s: %s", ticker, exc)

            # Day-of-week
            try:
                result = self.test_dow_pattern(sh)
                if result.get("valid"):
                    validated.append(result)
            except Exception as exc:
                logger.warning("dow_pattern for %s: %s", ticker, exc)

            # Lunar correlation
            try:
                result = self.test_lunar_correlation(sh)
                if result.get("valid"):
                    validated.append(result)
            except Exception as exc:
                logger.warning("lunar for %s: %s", ticker, exc)

            # Social lead
            reddit_hist = altdata.get(f"{ticker}_reddit", {})
            if reddit_hist:
                try:
                    result = self.test_social_lead(sh, reddit_hist)
                    if result.get("valid"):
                        validated.append(result)
                except Exception as exc:
                    logger.warning("social_lead for %s: %s", ticker, exc)

            # Weather short covering
            weather_data = altdata.get("weather_nyc", {})
            if weather_data:
                try:
                    result = self.test_weather_short_covering(sh, weather_data)
                    if result.get("valid"):
                        validated.append(result)
                except Exception as exc:
                    logger.warning("weather for %s: %s", ticker, exc)

        # Cross-ticker prediction
        tickers = list(short_history.keys())
        for i, ta in enumerate(tickers):
            for tb in tickers[i + 1:]:
                try:
                    result = self.test_cross_ticker_prediction(ta, tb, short_history)
                    if result.get("valid"):
                        validated.append(result)
                except Exception as exc:
                    logger.warning("cross_ticker(%s, %s): %s", ta, tb, exc)

        return validated

    def test_earnings_timing_pattern(
        self, ticker: str, short_history, earnings_dates: list
    ) -> dict:
        """Does short interest peak always N days before earnings for this stock?"""
        result = {
            "pattern": "earnings_timing",
            "ticker": ticker,
            "valid": False,
            "description": "Short interest peaks N days before earnings",
        }
        si_series = _to_series(short_history)
        if si_series is None or not earnings_dates:
            return result

        peak_offsets = []
        for ed in earnings_dates:
            try:
                ed_ts = pd.Timestamp(ed)
                window_start = ed_ts - pd.Timedelta(days=30)
                window = si_series[window_start:ed_ts]
                if len(window) < 5:
                    continue
                peak_date = window.idxmax()
                offset = (ed_ts - peak_date).days
                peak_offsets.append(offset)
            except Exception:
                continue

        if len(peak_offsets) < 3:
            return result

        offsets = np.array(peak_offsets)
        std = np.std(offsets)
        mean = np.mean(offsets)
        consistency = 1.0 - min(std / (abs(mean) + 1e-9), 1.0)

        validation = self.validate_pattern(
            pd.Series(offsets),
            f"earnings_timing_{ticker}",
        )

        result.update({
            "mean_days_before": float(mean),
            "std_days": float(std),
            "consistency_score": float(consistency),
            "n_events": len(offsets),
            "valid": bool(validation.get("significant", False) and consistency > 0.5),
            "validation": validation,
        })
        return result

    def test_dow_pattern(self, short_history) -> dict:
        """Day-of-week pattern in short selling."""
        result = {
            "pattern": "day_of_week",
            "valid": False,
            "description": "Short interest changes differ by day of week",
        }
        si_series = _to_series(short_history)
        if si_series is None or len(si_series) < 20:
            return result

        changes = si_series.diff().dropna()
        changes.index = pd.DatetimeIndex(changes.index)
        dow_means = changes.groupby(changes.index.dayofweek).mean()
        if len(dow_means) < 5:
            return result

        # One-way ANOVA across days
        groups = [changes[changes.index.dayofweek == d].values for d in range(5)]
        groups = [g for g in groups if len(g) >= 3]
        if len(groups) < 2:
            return result

        try:
            if scipy_available:
                f_stat, p_val = scipy_stats.f_oneway(*groups)
            else:
                f_stat, p_val = 0.0, 1.0
        except Exception:
            f_stat, p_val = 0.0, 1.0

        validation = self.validate_pattern(changes, "day_of_week")
        result.update({
            "dow_means": dow_means.to_dict(),
            "f_stat": float(f_stat),
            "p_value": float(p_val),
            "valid": bool(p_val < self.p_value_threshold and validation.get("significant", False)),
            "validation": validation,
        })
        return result

    def test_lunar_correlation(self, short_history) -> dict:
        """Does short interest change correlate with lunar phase?"""
        result = {
            "pattern": "lunar_correlation",
            "valid": False,
            "description": "Short interest correlates with lunar phase",
        }
        si_series = _to_series(short_history)
        if si_series is None or len(si_series) < 20:
            return result

        if not ephem_available:
            logger.warning("ephem not installed; cannot compute lunar phases")
            result["note"] = "ephem not available"
            return result

        try:
            lunar_phases = []
            for date in si_series.index:
                try:
                    moon = ephem.Moon(str(date))
                    lunar_phases.append(float(moon.phase))
                except Exception:
                    lunar_phases.append(np.nan)

            lunar_series = pd.Series(lunar_phases, index=si_series.index)
            combined = pd.DataFrame({"si": si_series, "lunar": lunar_series}).dropna()
            if len(combined) < 20:
                return result

            changes = combined["si"].diff().dropna()
            lunar_aligned = combined["lunar"].reindex(changes.index).dropna()
            changes_aligned = changes.reindex(lunar_aligned.index).dropna()

            if len(changes_aligned) < 10:
                return result

            if scipy_available:
                corr, p_val = scipy_stats.pearsonr(changes_aligned.values, lunar_aligned.values)
            else:
                corr = float(np.corrcoef(changes_aligned.values, lunar_aligned.values)[0, 1])
                p_val = 1.0

            validation = self.validate_pattern(changes_aligned, "lunar_correlation")
            result.update({
                "correlation": float(corr),
                "p_value": float(p_val),
                "n": len(changes_aligned),
                "valid": bool(abs(corr) > 0.2 and p_val < self.p_value_threshold and validation.get("significant", False)),
                "note": "Lunar correlation is often spurious; treat with extreme skepticism",
                "validation": validation,
            })
        except Exception as exc:
            logger.warning("lunar_correlation failed: %s", exc)

        return result

    def test_cross_ticker_prediction(
        self, ticker_a: str, ticker_b: str, short_history: dict
    ) -> dict:
        """Does short interest in A predict short interest change in B (Granger-style)?"""
        result = {
            "pattern": "cross_ticker_prediction",
            "ticker_a": ticker_a,
            "ticker_b": ticker_b,
            "valid": False,
            "description": f"Short interest in {ticker_a} predicts {ticker_b}",
        }
        si_a = _to_series(short_history.get(ticker_a))
        si_b = _to_series(short_history.get(ticker_b))
        if si_a is None or si_b is None:
            return result

        combined = pd.DataFrame({"a": si_a, "b": si_b}).dropna()
        if len(combined) < 30:
            return result

        # Simple lag-1 correlation of A's change on B's next-day change
        changes_a = combined["a"].diff().dropna()
        changes_b = combined["b"].diff().dropna()
        # Lag A by 1 relative to B
        lagged_a = changes_a.shift(1).dropna()
        aligned = pd.DataFrame({"lag_a": lagged_a, "b": changes_b}).dropna()
        if len(aligned) < 20:
            return result

        if scipy_available:
            corr, p_val = scipy_stats.pearsonr(aligned["lag_a"].values, aligned["b"].values)
        else:
            corr = float(np.corrcoef(aligned["lag_a"].values, aligned["b"].values)[0, 1])
            p_val = 1.0

        validation = self.validate_pattern(aligned["lag_a"], "cross_ticker")
        result.update({
            "lag1_correlation": float(corr),
            "p_value": float(p_val),
            "n": len(aligned),
            "valid": bool(abs(corr) > 0.25 and p_val < self.p_value_threshold and validation.get("significant", False)),
            "validation": validation,
        })
        return result

    def test_social_lead(self, short_history, reddit_history) -> dict:
        """Does Reddit mention spike precede short interest increase by N days?"""
        result = {
            "pattern": "social_lead",
            "valid": False,
            "description": "Reddit mention spikes precede short interest increases",
        }
        si_series = _to_series(short_history)
        reddit_series = _to_series(reddit_history)
        if si_series is None or reddit_series is None:
            return result

        combined = pd.DataFrame({"si": si_series, "reddit": reddit_series}).dropna()
        if len(combined) < 20:
            return result

        best_lag = 0
        best_corr = 0.0
        best_p = 1.0
        for lag in range(1, 8):
            shifted_reddit = combined["reddit"].shift(lag)
            si_changes = combined["si"].diff()
            aligned = pd.DataFrame({"reddit": shifted_reddit, "si_change": si_changes}).dropna()
            if len(aligned) < 10:
                continue
            if scipy_available:
                c, p = scipy_stats.pearsonr(aligned["reddit"].values, aligned["si_change"].values)
            else:
                c = float(np.corrcoef(aligned["reddit"].values, aligned["si_change"].values)[0, 1])
                p = 1.0
            if abs(c) > abs(best_corr):
                best_corr = c
                best_p = p
                best_lag = lag

        validation = self.validate_pattern(
            combined["reddit"].diff().dropna(), "social_lead"
        )
        result.update({
            "best_lag_days": best_lag,
            "best_correlation": float(best_corr),
            "p_value": float(best_p),
            "valid": bool(
                abs(best_corr) > 0.2
                and best_p < self.p_value_threshold
                and validation.get("significant", False)
            ),
            "validation": validation,
        })
        return result

    def test_weather_short_covering(self, short_history, weather_data) -> dict:
        """Does NYC weather predict short covering next week? (Designed to be a negative control.)"""
        result = {
            "pattern": "weather_short_covering",
            "valid": False,
            "description": "NYC weather predicts short covering (negative control / nonsense test)",
            "note": "This test is expected to fail statistical validation for most tickers",
        }
        si_series = _to_series(short_history)
        weather_series = _to_series(weather_data)
        if si_series is None or weather_series is None:
            return result

        combined = pd.DataFrame({"si": si_series, "weather": weather_series}).dropna()
        if len(combined) < 20:
            return result

        # Lag weather by 5 business days
        shifted_weather = combined["weather"].shift(5)
        si_changes = combined["si"].diff()
        aligned = pd.DataFrame({"weather": shifted_weather, "si_change": si_changes}).dropna()
        if len(aligned) < 10:
            return result

        if scipy_available:
            corr, p_val = scipy_stats.pearsonr(aligned["weather"].values, aligned["si_change"].values)
        else:
            corr = float(np.corrcoef(aligned["weather"].values, aligned["si_change"].values)[0, 1])
            p_val = 1.0

        validation = self.validate_pattern(aligned["si_change"], "weather_short_covering")
        result.update({
            "correlation": float(corr),
            "p_value": float(p_val),
            "valid": bool(
                abs(corr) > 0.3
                and p_val < self.p_value_threshold / 20  # strict threshold for nonsense test
                and validation.get("significant", False)
                and validation.get("monte_carlo_p_value", 1.0) < 0.001
            ),
            "validation": validation,
        })
        return result

    def validate_pattern(self, returns_series, pattern_name: str) -> dict:
        """
        Full statistical validation:
        - t-test (or Pearson correlation t-stat)
        - Bonferroni correction (assume 20 tests)
        - 500-shuffle Monte Carlo permutation test
        """
        validation = {
            "pattern_name": pattern_name,
            "significant": False,
            "t_stat": None,
            "p_value": None,
            "bonferroni_p": None,
            "monte_carlo_p_value": None,
            "n_permutations": N_PERMUTATIONS,
            "n_observations": 0,
        }

        if returns_series is None:
            return validation

        arr = _to_array(returns_series)
        arr = arr[~np.isnan(arr)]
        n = len(arr)
        validation["n_observations"] = n

        if n < 5:
            return validation

        n_comparisons = self.config.get("n_multiple_comparisons", 20)

        # t-test: is mean different from 0?
        try:
            if scipy_available:
                t_stat, p_val = scipy_stats.ttest_1samp(arr, popmean=0)
            else:
                mean = np.mean(arr)
                se = np.std(arr, ddof=1) / np.sqrt(n)
                t_stat = mean / (se + 1e-12)
                # Approximate p-value
                p_val = float(2 * (1 - _approx_normal_cdf(abs(t_stat))))
            bonferroni_p = min(float(p_val) * n_comparisons, 1.0)
        except Exception as exc:
            logger.warning("t-test failed for %s: %s", pattern_name, exc)
            t_stat, p_val, bonferroni_p = 0.0, 1.0, 1.0

        validation["t_stat"] = float(t_stat)
        validation["p_value"] = float(p_val)
        validation["bonferroni_p"] = float(bonferroni_p)

        # Monte Carlo permutation test
        try:
            observed_stat = abs(float(np.mean(arr)))
            shuffled_stats = np.empty(N_PERMUTATIONS)
            rng = np.random.default_rng(seed=42)
            for i in range(N_PERMUTATIONS):
                shuffled = rng.permutation(arr)
                shuffled_stats[i] = abs(np.mean(shuffled))
            mc_p = float(np.mean(shuffled_stats >= observed_stat))
            validation["monte_carlo_p_value"] = mc_p
        except Exception as exc:
            logger.warning("Monte Carlo failed for %s: %s", pattern_name, exc)
            mc_p = 1.0
            validation["monte_carlo_p_value"] = mc_p

        # Significant if both Bonferroni-corrected t-test AND Monte Carlo agree
        significant = (
            bonferroni_p < self.p_value_threshold
            and mc_p < self.p_value_threshold
        )
        validation["significant"] = bool(significant)
        return validation


# --- helpers ---

def _to_series(data) -> pd.Series:
    """Coerce various input types to a pandas Series with DatetimeIndex if possible."""
    if data is None:
        return None
    if isinstance(data, pd.Series):
        return data.astype(float)
    if isinstance(data, pd.DataFrame):
        col = next((c for c in ["value", "short_interest", "si", "count", "mentions"] if c in data.columns), None)
        if col:
            s = data[col].astype(float)
            if "date" in data.columns:
                s.index = pd.DatetimeIndex(data["date"])
            return s
        return None
    if isinstance(data, dict):
        if not data:
            return None
        try:
            s = pd.Series(data).astype(float)
            s.index = pd.DatetimeIndex(s.index)
            return s
        except Exception:
            try:
                return pd.Series(list(data.values()), dtype=float)
            except Exception:
                return None
    if isinstance(data, (list, np.ndarray)):
        try:
            return pd.Series(data, dtype=float)
        except Exception:
            return None
    return None


def _to_array(s) -> np.ndarray:
    if isinstance(s, (pd.Series, pd.DataFrame)):
        return s.values.astype(float).flatten()
    if isinstance(s, np.ndarray):
        return s.astype(float).flatten()
    return np.array(s, dtype=float).flatten()


def _price_to_returns(price_data) -> pd.Series:
    if price_data is None:
        return None
    df = price_data if isinstance(price_data, pd.DataFrame) else pd.DataFrame(price_data) if isinstance(price_data, dict) else None
    if df is None or df.empty:
        return None
    col = next((c for c in ["Close", "close"] if c in df.columns), None)
    if col is None:
        return None
    return df[col].pct_change().dropna()


def _approx_normal_cdf(z: float) -> float:
    """Approximation of normal CDF for fallback t-test p-value."""
    # Abramowitz & Stegun approximation
    t_val = 1.0 / (1.0 + 0.2316419 * abs(z))
    poly = t_val * (0.319381530 + t_val * (-0.356563782 + t_val * (1.781477937 + t_val * (-1.821255978 + t_val * 1.330274429))))
    pdf = np.exp(-0.5 * z * z) / np.sqrt(2 * np.pi)
    return float(1.0 - pdf * poly)
