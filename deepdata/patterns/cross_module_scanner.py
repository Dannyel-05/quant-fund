"""CrossModulePatternScanner — finds patterns across ALL modules simultaneously."""

import logging
import math
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

try:
    from scipy import stats as scipy_stats
    HAS_SCIPY = True
except ImportError:
    scipy_stats = None
    HAS_SCIPY = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CrossModulePatternScanner:
    """Finds patterns across ALL modules simultaneously."""

    MONTE_CARLO_N = 500
    CORRELATION_THRESHOLD = 0.3
    P_VALUE_THRESHOLD = 0.01

    def __init__(self, config: dict):
        self.config = config or {}
        self.mc_n = self.config.get("monte_carlo_n", self.MONTE_CARLO_N)
        self.corr_threshold = self.config.get("corr_threshold", self.CORRELATION_THRESHOLD)
        self.p_threshold = self.config.get("p_value_threshold", self.P_VALUE_THRESHOLD)
        self.max_auto_pairs = self.config.get("max_auto_pairs", 50)
        self.bonferroni_n = self.config.get("bonferroni_n", 100)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, all_signals: dict, price_data: dict) -> list:
        """
        all_signals: {module_name: list_of_CollectorResults}
        Returns list of validated pattern dicts.
        """
        if not all_signals:
            return []
        try:
            configured = self.test_configured_combinations(all_signals, price_data)
            auto_discovered = self.auto_discover_pairs(all_signals, price_data, self.max_auto_pairs)
            specific = self.test_specific_combos(all_signals, price_data)
            all_patterns = configured + auto_discovered + specific
            # Deduplicate by name
            seen = set()
            unique = []
            for p in all_patterns:
                name = p.get("name", "")
                if name not in seen:
                    seen.add(name)
                    unique.append(p)
            return unique
        except Exception as exc:
            logger.warning("CrossModulePatternScanner.scan failed: %s", exc)
            return []

    def test_configured_combinations(self, all_signals: dict, price_data: dict) -> list:
        """Test combinations from config patterns.test_combinations."""
        combos = self.config.get("patterns", {}).get("test_combinations", [])
        if not combos:
            return []
        results = []
        for combo in combos:
            try:
                modules = combo.get("modules", [])
                name = combo.get("name", "_".join(modules))
                signal_series_list = []
                for mod in modules:
                    series = self._extract_signal_series(all_signals.get(mod, []), price_data)
                    if series is not None and len(series) > 10:
                        signal_series_list.append(series)

                if len(signal_series_list) < 2:
                    continue

                returns_series = self._build_returns_series(price_data)
                if returns_series is None or len(returns_series) < 10:
                    continue

                validated = self.validate_combination(signal_series_list, returns_series, name)
                if validated.get("passed"):
                    results.append(validated)
            except Exception as exc:
                logger.warning("test_configured_combinations item failed: %s", exc)
        return results

    def auto_discover_pairs(
        self,
        all_signals: dict,
        price_data: dict,
        max_pairs: int = 50,
    ) -> list:
        """
        Randomly sample signal pairs. Test correlation at lags 0-60.
        Flag if |correlation| > 0.3 and p < 0.01.
        """
        if not all_signals or len(all_signals) < 2:
            return []
        try:
            module_names = list(all_signals.keys())
            returns_series = self._build_returns_series(price_data)
            if returns_series is None or len(returns_series) < 10:
                return []

            # Generate all unique pairs
            pairs = []
            for i in range(len(module_names)):
                for j in range(i + 1, len(module_names)):
                    pairs.append((module_names[i], module_names[j]))

            # Shuffle and limit
            random.shuffle(pairs)
            pairs = pairs[:max_pairs]

            results = []
            for mod_a, mod_b in pairs:
                try:
                    s_a = self._extract_signal_series(all_signals.get(mod_a, []), price_data)
                    s_b = self._extract_signal_series(all_signals.get(mod_b, []), price_data)
                    if s_a is None or s_b is None:
                        continue
                    if len(s_a) < 10 or len(s_b) < 10:
                        continue

                    # Test cross-correlation at lags 0-60
                    best_corr, best_lag, best_p = self._best_lagged_correlation(s_a, s_b)
                    if abs(best_corr) > self.corr_threshold and best_p < self.p_threshold:
                        name = f"auto_{mod_a}_{mod_b}_lag{best_lag}"
                        validated = self.validate_combination(
                            [s_a, s_b], returns_series, name
                        )
                        if validated.get("passed"):
                            results.append(validated)
                except Exception as exc:
                    logger.warning("auto_discover_pairs item failed: %s", exc)
            return results
        except Exception as exc:
            logger.warning("auto_discover_pairs failed: %s", exc)
            return []

    def test_specific_combos(self, all_signals: dict, price_data: dict) -> list:
        """
        Test the hardcoded nonsense combinations:
        1. Lunar + congressional + reddit triple
        2. Severe weather + high short interest + earnings within 14d
        3. Patent velocity + hiring momentum + transcript score > 0.7
        4. Dark pool accumulation + Wikipedia views falling
        5. Congressional sector buying + supply chain readthrough
        """
        COMBOS = [
            {
                "name": "lunar_congressional_reddit_triple",
                "modules": ["lunar", "congressional", "reddit"],
            },
            {
                "name": "weather_short_interest_earnings_proximity",
                "modules": ["weather", "short_interest", "earnings"],
            },
            {
                "name": "patent_velocity_hiring_transcript",
                "modules": ["patents", "hiring", "transcripts"],
            },
            {
                "name": "dark_pool_wikipedia_falling",
                "modules": ["options", "wikipedia"],
            },
            {
                "name": "congressional_sector_supply_chain_readthrough",
                "modules": ["congressional", "supply_chain"],
            },
        ]

        results = []
        returns_series = self._build_returns_series(price_data)
        if returns_series is None or len(returns_series) < 10:
            return results

        for combo in COMBOS:
            try:
                mods = combo["modules"]
                name = combo["name"]
                signal_series_list = []
                for mod in mods:
                    series = self._extract_signal_series(all_signals.get(mod, []), price_data)
                    if series is not None and len(series) > 5:
                        signal_series_list.append(series)

                if len(signal_series_list) < 1:
                    continue

                validated = self.validate_combination(signal_series_list, returns_series, name)
                results.append(validated)
            except Exception as exc:
                logger.warning("test_specific_combos item %s failed: %s", combo.get("name"), exc)
        return results

    def validate_combination(
        self,
        signal_series_list: list,
        returns_series: "pd.Series",
        name: str,
    ) -> dict:
        """
        Full validation pipeline.
        Returns: {name, passed, sharpe, p_value, permutation_pct, dsr}
        """
        base = {"name": name, "passed": False, "sharpe": 0.0, "p_value": 1.0,
                "permutation_pct": 0.0, "dsr": 0.0}
        if not HAS_PANDAS or not signal_series_list or returns_series is None:
            return base
        try:
            # Combine signals: average value per date
            if HAS_PANDAS:
                combined = pd.concat(
                    [s for s in signal_series_list if s is not None], axis=1
                ).mean(axis=1).dropna()
                ret = returns_series.reindex(combined.index).dropna()
                combined = combined.reindex(ret.index)

                if len(combined) < 10 or len(ret) < 10:
                    return base

                arr_sig = combined.values.astype(float)
                arr_ret = ret.values.astype(float)
            else:
                return base

            n_obs = len(arr_ret)

            # Sharpe ratio of signal-conditioned returns
            signal_returns = arr_ret * np.sign(arr_sig)
            sr_mean = float(np.mean(signal_returns))
            sr_std = float(np.std(signal_returns, ddof=1))
            if sr_std == 0:
                return base
            sharpe = sr_mean / sr_std * math.sqrt(252)

            # T-test
            if HAS_SCIPY:
                t_stat, p_value = scipy_stats.ttest_1samp(signal_returns, 0)
                p_value = float(p_value)
            else:
                # Manual t-test
                t_stat = sr_mean / (sr_std / math.sqrt(n_obs))
                # Approx p-value using normal distribution for large n
                from math import erfc
                p_value = float(erfc(abs(t_stat) / math.sqrt(2)))

            # Bonferroni correction
            bonferroni_p = min(1.0, p_value * self.bonferroni_n)

            # Monte Carlo permutation (500 shuffles)
            permutation_pct = self._permutation_test(arr_sig, arr_ret, sharpe, self.mc_n)

            # Deflated Sharpe Ratio
            n_trials = self.bonferroni_n
            dsr = self.calc_deflated_sharpe(pd.Series(signal_returns), n_trials)

            passed = (
                bonferroni_p < 0.05
                and permutation_pct >= 0.95
                and dsr > 0
            )

            return {
                "name": name,
                "passed": passed,
                "sharpe": sharpe,
                "p_value": p_value,
                "bonferroni_p": bonferroni_p,
                "permutation_pct": permutation_pct,
                "dsr": dsr,
            }
        except Exception as exc:
            logger.warning("validate_combination failed for %s: %s", name, exc)
            return base

    def calc_deflated_sharpe(self, returns: "pd.Series", n_trials: int) -> float:
        """DSR ≈ SR * (1 - sqrt(log(n_trials) / n_obs))"""
        if not HAS_PANDAS or returns is None or len(returns) < 2:
            return 0.0
        try:
            n_obs = len(returns)
            mean = float(returns.mean())
            std = float(returns.std(ddof=1))
            if std == 0:
                return 0.0
            sr = mean / std * math.sqrt(252)
            n_trials = max(n_trials, 2)
            deflation = math.sqrt(math.log(n_trials) / n_obs)
            dsr = sr * (1.0 - deflation)
            return dsr
        except Exception as exc:
            logger.warning("calc_deflated_sharpe error: %s", exc)
            return 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _permutation_test(
        self,
        signal_arr: np.ndarray,
        returns_arr: np.ndarray,
        real_stat: float,
        n_shuffles: int,
    ) -> float:
        """Shuffle returns N times, return percentile of real_stat in distribution."""
        try:
            null_stats = []
            ret_copy = returns_arr.copy()
            for _ in range(n_shuffles):
                np.random.shuffle(ret_copy)
                sim_returns = ret_copy * np.sign(signal_arr)
                sim_mean = float(np.mean(sim_returns))
                sim_std = float(np.std(sim_returns, ddof=1))
                if sim_std > 0:
                    null_stats.append(sim_mean / sim_std * math.sqrt(252))
                else:
                    null_stats.append(0.0)
            null_arr = np.array(null_stats)
            pct = float(np.mean(null_arr < real_stat))
            return pct
        except Exception as exc:
            logger.warning("_permutation_test error: %s", exc)
            return 0.0

    def _extract_signal_series(self, collector_results: list, price_data: dict):
        """Convert list of CollectorResults into a pd.Series indexed by date."""
        if not HAS_PANDAS or not collector_results:
            return None
        try:
            rows = []
            for r in collector_results:
                ts = r.get("timestamp", "")
                val = r.get("value", 0.0)
                try:
                    dt = pd.to_datetime(ts, utc=True).tz_localize(None)
                    rows.append((dt, float(val) if val is not None else 0.0))
                except Exception:
                    continue
            if not rows:
                return None
            idx, vals = zip(*rows)
            return pd.Series(vals, index=pd.DatetimeIndex(idx)).sort_index()
        except Exception as exc:
            logger.warning("_extract_signal_series failed: %s", exc)
            return None

    def _build_returns_series(self, price_data: dict):
        """Build an average returns series from price_data across all tickers."""
        if not HAS_PANDAS or not price_data:
            return None
        try:
            all_rets = []
            for ticker, data in price_data.items():
                if isinstance(data, pd.DataFrame) and "Close" in data.columns:
                    ret = data["Close"].pct_change().dropna()
                    all_rets.append(ret)
            if not all_rets:
                return None
            combined = pd.concat(all_rets, axis=1).mean(axis=1).dropna()
            return combined
        except Exception as exc:
            logger.warning("_build_returns_series failed: %s", exc)
            return None

    def _best_lagged_correlation(self, s_a, s_b, max_lag: int = 60):
        """Find lag 0-max_lag with highest absolute correlation."""
        if not HAS_PANDAS:
            return 0.0, 0, 1.0
        best_corr, best_lag, best_p = 0.0, 0, 1.0
        try:
            for lag in range(0, max_lag + 1):
                if lag == 0:
                    aligned_a = s_a.reindex(s_b.index).dropna()
                    aligned_b = s_b.reindex(aligned_a.index).dropna()
                    aligned_a = aligned_a.reindex(aligned_b.index).dropna()
                else:
                    shifted = s_a.shift(lag)
                    aligned_a = shifted.reindex(s_b.index).dropna()
                    aligned_b = s_b.reindex(aligned_a.index).dropna()
                    aligned_a = aligned_a.reindex(aligned_b.index).dropna()

                if len(aligned_a) < 10:
                    continue
                a_arr = aligned_a.values.astype(float)
                b_arr = aligned_b.values.astype(float)
                if np.std(a_arr) == 0 or np.std(b_arr) == 0:
                    continue

                corr = float(np.corrcoef(a_arr, b_arr)[0, 1])
                if HAS_SCIPY:
                    _, p_val = scipy_stats.pearsonr(a_arr, b_arr)
                    p_val = float(p_val)
                else:
                    n = len(a_arr)
                    t_stat = corr * math.sqrt(n - 2) / math.sqrt(1 - corr ** 2 + 1e-10)
                    from math import erfc
                    p_val = float(erfc(abs(t_stat) / math.sqrt(2)))

                if abs(corr) > abs(best_corr):
                    best_corr, best_lag, best_p = corr, lag, p_val
        except Exception as exc:
            logger.warning("_best_lagged_correlation failed: %s", exc)
        return best_corr, best_lag, best_p
