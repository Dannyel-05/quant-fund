"""NonsenseCorrelator — specifically designed to find and test absurd correlations."""

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


# Pre-defined hypothesis templates with economic logic scores
HYPOTHESIS_TEMPLATES = [
    {
        "id": "lunar_short_covering_sector",
        "description": "Does lunar phase predict short covering for specific sectors?",
        "modules": ["lunar", "short_interest"],
        "economic_logic_score": 0.05,
        "economic_explanation": "Lunar cycles may influence trader psychology and risk appetite, "
                                "affecting short covering decisions at margin.",
    },
    {
        "id": "london_weather_uk_options_iv",
        "description": "Does weather in London predict UK options implied volatility?",
        "modules": ["weather", "options"],
        "economic_logic_score": 0.08,
        "economic_explanation": "Adverse weather may affect trader mood and risk perception, "
                                "leading to higher option hedging demand.",
    },
    {
        "id": "congressional_filing_day_of_week",
        "description": "Does day of week of congressional filing predict signal strength?",
        "modules": ["congressional"],
        "economic_logic_score": 0.12,
        "economic_explanation": "Friday filings may receive less scrutiny; Monday filings "
                                "may indicate more deliberate timing.",
    },
    {
        "id": "bitcoin_small_cap_short_interest",
        "description": "Does Bitcoin price correlate with small-cap short interest?",
        "modules": ["crypto", "short_interest"],
        "economic_logic_score": 0.15,
        "economic_explanation": "Risk-on/risk-off sentiment drives both crypto and small-cap "
                                "short covering simultaneously.",
    },
    {
        "id": "sector_beat_count_sector_short_interest",
        "description": "Does earnings beat count in sector predict sector short interest?",
        "modules": ["earnings", "short_interest"],
        "economic_logic_score": 0.55,
        "economic_explanation": "Sector-wide beats signal healthier fundamentals, leading "
                                "short sellers to reduce exposure sector-wide.",
    },
    {
        "id": "patent_filing_weekday_momentum",
        "description": "Do patent filings on specific weekdays predict price momentum?",
        "modules": ["patents"],
        "economic_logic_score": 0.07,
        "economic_explanation": "Companies timing patent filings strategically may also time "
                                "other value-creating announcements.",
    },
    {
        "id": "wikipedia_edits_earnings_surprise",
        "description": "Do Wikipedia edit velocity spikes predict earnings surprises?",
        "modules": ["wikipedia", "earnings"],
        "economic_logic_score": 0.35,
        "economic_explanation": "Increased public interest before earnings may reflect "
                                "information leakage or analyst research activity.",
    },
    {
        "id": "reddit_sentiment_options_skew",
        "description": "Does Reddit sentiment predict options skew direction?",
        "modules": ["reddit", "options"],
        "economic_logic_score": 0.40,
        "economic_explanation": "Retail sentiment on Reddit drives options demand, directly "
                                "affecting put/call skew.",
    },
    {
        "id": "dark_pool_volume_short_squeeze_timing",
        "description": "Does dark pool accumulation precede short squeeze triggers?",
        "modules": ["options", "short_interest"],
        "economic_logic_score": 0.65,
        "economic_explanation": "Institutional accumulation in dark pools removes float "
                                "available to cover shorts, accelerating squeezes.",
    },
    {
        "id": "supply_chain_disruption_competitor_iv",
        "description": "Does supply chain disruption signal predict competitor options IV?",
        "modules": ["supply_chain", "options"],
        "economic_logic_score": 0.70,
        "economic_explanation": "Supply chain disruptions for one firm create opportunities "
                                "for competitors, increasing hedging demand.",
    },
]


class NonsenseCorrelator:
    """Finds and tests absurd correlations that may be real and durable."""

    MONTE_CARLO_N = 1000
    BONFERRONI_N = 200  # number of hypotheses tested

    def __init__(self, config: dict):
        self.config = config or {}
        self.mc_n = self.config.get("monte_carlo_n", self.MONTE_CARLO_N)
        self.bonferroni_n = self.config.get("bonferroni_n", self.BONFERRONI_N)
        self.p_threshold = self.config.get("p_value_threshold", 0.05)
        self.min_permutation_pct = self.config.get("min_permutation_pct", 0.95)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_all_nonsense(self, all_signals: dict, price_data: dict) -> list:
        """
        Generate and test ALL combinations that seem economically absurd.
        Return only those that survive rigorous statistical validation.
        """
        if not all_signals:
            return []
        try:
            available_modules = list(all_signals.keys())
            hypotheses = self.generate_nonsense_hypotheses(available_modules)
            validated = []
            for hyp in hypotheses:
                try:
                    data = self._build_hypothesis_data(hyp, all_signals, price_data)
                    if not data:
                        continue
                    result = self.test_hypothesis(hyp, data)
                    if result.get("passed"):
                        validated.append(result)
                except Exception as exc:
                    logger.warning("find_all_nonsense hyp %s failed: %s", hyp.get("id"), exc)
            # Sort by nonsense_score descending (most absurd surviving correlations first)
            validated.sort(key=lambda x: x.get("nonsense_score", 0.0), reverse=True)
            return validated
        except Exception as exc:
            logger.warning("NonsenseCorrelator.find_all_nonsense failed: %s", exc)
            return []

    def generate_nonsense_hypotheses(self, available_modules: list) -> list:
        """
        Generate hypotheses combining available modules with economic explanations.
        Returns list of hypothesis dicts.
        """
        hypotheses = []
        available_set = set(available_modules)
        for tmpl in HYPOTHESIS_TEMPLATES:
            required_mods = tmpl.get("modules", [])
            # Include if at least one required module is available, or if no modules required
            if not required_mods or any(m in available_set for m in required_mods):
                hypotheses.append(dict(tmpl))

        # Dynamic: generate cross-module hypotheses for available pairs
        mod_list = list(available_set)
        for i in range(len(mod_list)):
            for j in range(i + 1, len(mod_list)):
                ma, mb = mod_list[i], mod_list[j]
                hyp_id = f"dynamic_{ma}_{mb}_correlation"
                if not any(h["id"] == hyp_id for h in hypotheses):
                    hypotheses.append({
                        "id": hyp_id,
                        "description": f"Does {ma} signal correlate with {mb} outcomes?",
                        "modules": [ma, mb],
                        "economic_logic_score": 0.3,
                        "economic_explanation": f"Information in {ma} may lead {mb} by information diffusion.",
                    })
        return hypotheses

    def test_hypothesis(self, hypothesis: dict, data: dict) -> dict:
        """
        Full validation: t-test + Bonferroni + 1000-shuffle Monte Carlo.
        Returns: {hypothesis, passed, sharpe, economic_explanation, nonsense_score}
        """
        base = {
            "hypothesis": hypothesis,
            "passed": False,
            "sharpe": 0.0,
            "p_value": 1.0,
            "economic_explanation": hypothesis.get("economic_explanation", ""),
            "nonsense_score": 0.0,
        }
        if not HAS_PANDAS or not data:
            return base
        try:
            signal_arr = data.get("signal")
            returns_arr = data.get("returns")
            if signal_arr is None or returns_arr is None:
                return base
            if len(signal_arr) < 10 or len(returns_arr) < 10:
                return base

            signal_arr = np.array(signal_arr, dtype=float)
            returns_arr = np.array(returns_arr, dtype=float)
            n = min(len(signal_arr), len(returns_arr))
            signal_arr = signal_arr[:n]
            returns_arr = returns_arr[:n]

            # Signal-conditioned returns
            cond_returns = returns_arr * np.sign(signal_arr)
            mean_r = float(np.mean(cond_returns))
            std_r = float(np.std(cond_returns, ddof=1))
            if std_r == 0:
                return base

            sharpe = mean_r / std_r * math.sqrt(252)

            # T-test
            if HAS_SCIPY:
                t_stat, p_value = scipy_stats.ttest_1samp(cond_returns, 0)
                p_value = float(p_value)
            else:
                t_stat = mean_r / (std_r / math.sqrt(n))
                from math import erfc
                p_value = float(erfc(abs(t_stat) / math.sqrt(2)))

            # Bonferroni correction
            bonferroni_p = min(1.0, p_value * self.bonferroni_n)

            # Monte Carlo permutation (1000 shuffles)
            null_sharpes = []
            ret_copy = returns_arr.copy()
            for _ in range(self.mc_n):
                np.random.shuffle(ret_copy)
                sim = ret_copy * np.sign(signal_arr)
                sim_std = float(np.std(sim, ddof=1))
                if sim_std > 0:
                    null_sharpes.append(float(np.mean(sim)) / sim_std * math.sqrt(252))
                else:
                    null_sharpes.append(0.0)

            null_arr = np.array(null_sharpes)
            permutation_pct = float(np.mean(null_arr < sharpe))

            # Nonsense score
            nonsense_score = self.calc_nonsense_score(hypothesis, p_value)

            passed = (
                bonferroni_p < self.p_threshold
                and permutation_pct >= self.min_permutation_pct
            )

            return {
                "hypothesis": hypothesis,
                "passed": passed,
                "sharpe": sharpe,
                "p_value": p_value,
                "bonferroni_p": bonferroni_p,
                "permutation_pct": permutation_pct,
                "economic_explanation": hypothesis.get("economic_explanation", ""),
                "nonsense_score": nonsense_score,
            }
        except Exception as exc:
            logger.warning("test_hypothesis failed for %s: %s", hypothesis.get("id"), exc)
            return base

    def calc_nonsense_score(self, hypothesis: dict, p_value: float) -> float:
        """
        NonsenseScore = 1 / (economic_logic_score + 0.01)
        High nonsense_score = more durable (nobody else trading it).
        """
        try:
            els = float(hypothesis.get("economic_logic_score", 0.5))
            return 1.0 / (els + 0.01)
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_hypothesis_data(
        self,
        hypothesis: dict,
        all_signals: dict,
        price_data: dict,
    ) -> dict:
        """Build aligned signal + returns arrays for a hypothesis."""
        if not HAS_PANDAS:
            return {}
        try:
            modules = hypothesis.get("modules", [])
            signal_series_list = []
            for mod in modules:
                results = all_signals.get(mod, [])
                series = self._extract_signal_series(results)
                if series is not None and len(series) > 5:
                    signal_series_list.append(series)

            if not signal_series_list:
                # Try to use any available module
                for mod, results in all_signals.items():
                    series = self._extract_signal_series(results)
                    if series is not None and len(series) > 5:
                        signal_series_list.append(series)
                        if len(signal_series_list) >= 2:
                            break

            if not signal_series_list:
                return {}

            combined_signal = pd.concat(signal_series_list, axis=1).mean(axis=1).dropna()

            returns_series = self._build_returns_series(price_data)
            if returns_series is None or len(returns_series) < 5:
                return {}

            aligned = pd.concat([combined_signal, returns_series], axis=1).dropna()
            if len(aligned) < 10:
                return {}

            return {
                "signal": aligned.iloc[:, 0].values,
                "returns": aligned.iloc[:, 1].values,
            }
        except Exception as exc:
            logger.warning("_build_hypothesis_data failed: %s", exc)
            return {}

    def _extract_signal_series(self, collector_results: list):
        """Convert CollectorResults to pd.Series."""
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
        """Build average returns series."""
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
            return pd.concat(all_rets, axis=1).mean(axis=1).dropna()
        except Exception as exc:
            logger.warning("_build_returns_series failed: %s", exc)
            return None
