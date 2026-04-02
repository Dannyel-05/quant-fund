import logging
import itertools
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class NonsenseDetector:
    """
    RenTec-inspired engine for discovering statistically real patterns that
    have no obvious economic explanation — and are therefore harder for
    competitors to identify and arbitrage away.
    """

    HYPOTHESIS_TEMPLATES = [
        # Temporal
        {
            "name": "full_moon_3d_lag",
            "desc": "Full moon predicts return 3 days later",
            "type": "temporal",
        },
        {
            "name": "full_moon_1d_lag",
            "desc": "Full moon predicts next-day return",
            "type": "temporal",
        },
        {
            "name": "new_moon_return",
            "desc": "New moon day return premium",
            "type": "temporal",
        },
        {
            "name": "monday_wiki_friday_return",
            "desc": "Monday Wikipedia edits predict Friday close",
            "type": "temporal",
        },
        {
            "name": "shipping_90d_sector",
            "desc": "Shipping rate change 90d ago predicts sector earnings",
            "type": "temporal",
        },
        {
            "name": "weather_14d_retail",
            "desc": "Regional weather 14 days ago predicts retail stocks",
            "type": "temporal",
        },
        {
            "name": "earnings_dow_drift",
            "desc": "Day-of-week of earnings announcement predicts drift magnitude",
            "type": "temporal",
        },
        # Cross-asset
        {
            "name": "vix_spike_smallcap_lag5",
            "desc": "VIX spike predicts small-cap outperformance 5d later",
            "type": "cross_asset",
        },
        {
            "name": "oil_pharma_inverse",
            "desc": "Oil price change inversely predicts pharma performance",
            "type": "cross_asset",
        },
        # Social
        {
            "name": "reddit_morning_accuracy",
            "desc": "Pre-market Reddit posts more accurate than afternoon",
            "type": "social",
        },
        {
            "name": "reddit_awards_ratio",
            "desc": "Awards/upvote ratio predicts trade outcome better than raw sentiment",
            "type": "social",
        },
        {
            "name": "wiki_sunday_monday",
            "desc": "Sunday Wikipedia edit count predicts Monday returns",
            "type": "social",
        },
        # Compound
        {
            "name": "full_moon_high_vix_reddit",
            "desc": "Full moon + VIX>20 + Reddit spike = combined effect",
            "type": "compound",
        },
        {
            "name": "shipping_freeze_cold_wiki",
            "desc": "Shipping stress + hiring freeze + cold weather + wiki edits",
            "type": "compound",
        },
    ]

    def __init__(self, config: dict, store):
        self.config = config
        self.store = store
        self.min_obs: int = 50
        self.p_threshold: float = config.get("altdata", {}).get(
            "nonsense_threshold", 0.05
        )
        self.apply_bonferroni: bool = config.get("altdata", {}).get(
            "bonferroni_correction", True
        )
        self.results: List[Dict] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(
        self,
        returns: pd.DataFrame,
        altdata: pd.DataFrame,
    ) -> List[Dict]:
        """
        Scan for nonsense patterns across all hypothesis templates.

        Parameters
        ----------
        returns : DataFrame of ticker returns (index=date, columns=tickers)
        altdata : DataFrame of altdata features   (index=date)

        Returns list of candidate signal dicts that passed all statistical tests.
        """
        candidates: List[Dict] = []
        n_hypotheses = len(self.HYPOTHESIS_TEMPLATES)
        p_threshold = (
            self.p_threshold / n_hypotheses
            if self.apply_bonferroni
            else self.p_threshold
        )

        for hyp in self.HYPOTHESIS_TEMPLATES:
            try:
                result = self._test_hypothesis(hyp, returns, altdata, p_threshold)
                result["hypothesis"] = hyp
                self.results.append(result)
                if result.get("passed"):
                    candidates.append(result)
                    logger.info(
                        "NONSENSE CANDIDATE: %s sharpe=%.2f p=%.4f",
                        hyp["name"],
                        result.get("sharpe", 0),
                        result.get("p_value", 1),
                    )
            except Exception as e:
                logger.debug("Hypothesis %s failed: %s", hyp["name"], e)

        return candidates

    # ------------------------------------------------------------------
    # Hypothesis testing
    # ------------------------------------------------------------------

    def _test_hypothesis(
        self,
        hyp: dict,
        returns: pd.DataFrame,
        altdata: pd.DataFrame,
        p_threshold: float,
    ) -> dict:
        """Test a single hypothesis. Returns a result dict."""
        mkt_returns = returns.mean(axis=1).dropna()
        name = hyp["name"]
        signal: Optional[pd.Series] = None

        # ---- signal construction per hypothesis ----

        if name == "full_moon_3d_lag":
            if "lunar_phase_encoded" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            is_full = altdata["lunar_phase_encoded"].rolling(3).max() > 0.08
            signal = is_full.astype(float).shift(3)

        elif name == "full_moon_1d_lag":
            if "lunar_phase_encoded" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = (altdata["lunar_phase_encoded"] > 0.08).astype(float).shift(1)

        elif name == "new_moon_return":
            col = "days_from_new_moon"
            if col not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = (altdata[col] < 2).astype(float)

        elif name == "monday_wiki_friday_return":
            if "wikipedia_edit_surge_flag" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            # Monday edits (dayofweek==0) shifted 4 days forward to Friday
            monday_mask = altdata.index.dayofweek == 0
            signal = (
                altdata["wikipedia_edit_surge_flag"]
                .where(monday_mask)
                .reindex(altdata.index)
                .shift(4)
            )

        elif name == "shipping_90d_sector":
            if "shipping_pressure_score" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = altdata["shipping_pressure_score"].shift(90)

        elif name == "weather_14d_retail":
            if "weather_risk_score" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = altdata["weather_risk_score"].shift(14)

        elif name == "earnings_dow_drift":
            # Use day-of-week as a proxy signal (Monday=0 ... Friday=4)
            dow = pd.Series(altdata.index.dayofweek, index=altdata.index).astype(float)
            signal = dow / 4.0  # normalise to [0,1]

        elif name == "vix_spike_smallcap_lag5":
            if "vix_zscore" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            # Inverse: after a VIX spike we expect a relief rally
            signal = -(altdata["vix_zscore"] > 2).astype(float).shift(5)

        elif name == "oil_pharma_inverse":
            if "oil_return" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = -altdata["oil_return"]

        elif name == "reddit_morning_accuracy":
            if "reddit_sentiment_score" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = altdata["reddit_sentiment_score"]

        elif name == "reddit_awards_ratio":
            if "reddit_awards_ratio" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            signal = altdata["reddit_awards_ratio"]

        elif name == "wiki_sunday_monday":
            if "wikipedia_edit_surge_flag" not in altdata.columns:
                return {"passed": False, "reason": "missing_data"}
            sunday_mask = altdata.index.dayofweek == 6
            signal = (
                altdata["wikipedia_edit_surge_flag"]
                .where(sunday_mask)
                .reindex(altdata.index)
                .shift(1)
            )

        elif name == "full_moon_high_vix_reddit":
            needed = ["lunar_phase_encoded", "vix_zscore", "reddit_sentiment_score"]
            if not all(c in altdata.columns for c in needed):
                return {"passed": False, "reason": "missing_data"}
            signal = (
                (altdata["lunar_phase_encoded"] > 0.05)
                & (altdata["vix_zscore"] > 1.5)
                & (altdata["reddit_sentiment_score"] > 0.3)
            ).astype(float)

        elif name == "shipping_freeze_cold_wiki":
            needed = [
                "shipping_pressure_score",
                "wikipedia_edit_surge_flag",
            ]
            if not all(c in altdata.columns for c in needed):
                return {"passed": False, "reason": "missing_data"}
            shipping_stress = altdata["shipping_pressure_score"] > altdata[
                "shipping_pressure_score"
            ].quantile(0.75)
            wiki_spike = altdata["wikipedia_edit_surge_flag"] > 0
            signal = (shipping_stress & wiki_spike).astype(float)

        else:
            return {"passed": False, "reason": "not_implemented"}

        if signal is None:
            return {"passed": False, "reason": "no_signal"}

        # ---- statistical testing ----

        aligned = pd.concat(
            [signal.rename("signal"), mkt_returns.rename("return")], axis=1
        ).dropna()

        if len(aligned) < self.min_obs:
            return {
                "passed": False,
                "reason": "insufficient_obs",
                "n": len(aligned),
            }

        high = aligned[aligned["signal"] > 0]["return"]
        low = aligned[aligned["signal"] <= 0]["return"]

        if len(high) < 10 or len(low) < 10:
            return {"passed": False, "reason": "insufficient_split"}

        t_stat, p_value = stats.ttest_ind(high, low)

        signal_returns = (
            aligned["signal"].apply(lambda x: 1 if x > 0 else -1) * aligned["return"]
        )
        if signal_returns.std() > 0:
            sharpe = float(
                signal_returns.mean() / signal_returns.std() * np.sqrt(252)
            )
        else:
            sharpe = 0.0

        passed_pvalue = p_value < p_threshold
        passed_sharpe = sharpe > 0.8

        if passed_pvalue and passed_sharpe:
            passed_mc = self._permutation_test(
                aligned["signal"], aligned["return"], sharpe, n_shuffles=500
            )
        else:
            passed_mc = False

        # NonsenseScore: higher = less economically intuitive = more durable
        economic_logic_scores = {
            "temporal": 0.1,
            "cross_asset": 0.2,
            "social": 0.3,
            "compound": 0.05,
        }
        econ_score = economic_logic_scores.get(hyp["type"], 0.2)
        nonsense_score = 1.0 / (econ_score + 0.01)

        return {
            "passed": passed_pvalue and passed_sharpe and passed_mc,
            "t_statistic": float(t_stat),
            "p_value": float(p_value),
            "sharpe": float(sharpe),
            "n_observations": len(aligned),
            "n_high_signal": len(high),
            "nonsense_score": nonsense_score,
            "permutation_test_passed": passed_mc,
        }

    # ------------------------------------------------------------------
    # Monte Carlo permutation test
    # ------------------------------------------------------------------

    def _permutation_test(
        self,
        signal: pd.Series,
        returns: pd.Series,
        observed_sharpe: float,
        n_shuffles: int = 500,
    ) -> bool:
        """
        Returns True if observed_sharpe is in the top 5 % of the
        permutation null distribution.
        """
        rng = np.random.default_rng(42)
        shuffled_sharpes: List[float] = []
        ret_arr = returns.values
        sig_arr = signal.values

        for _ in range(n_shuffles):
            shuffled_sig = rng.permutation(sig_arr)
            shuffled_returns = (
                np.sign(shuffled_sig - shuffled_sig.mean()) * ret_arr
            )
            std = shuffled_returns.std()
            if std > 0:
                s = float(shuffled_returns.mean() / std * np.sqrt(252))
                shuffled_sharpes.append(s)

        if not shuffled_sharpes:
            return False

        threshold = np.percentile(shuffled_sharpes, 95)
        return bool(observed_sharpe > threshold)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def generate_report(self) -> str:
        lines = [
            "# NonsenseDetector Weekly Report",
            f"Generated: {datetime.now():%Y-%m-%d %H:%M}",
            "",
        ]
        lines.append(f"Hypotheses tested: {len(self.results)}")
        candidates = [r for r in self.results if r.get("passed")]
        lines.append(f"Candidates found: {len(candidates)}")
        lines.append("")

        for r in self.results:
            hyp = r.get("hypothesis", {})
            status = "PASS" if r.get("passed") else "FAIL"
            lines.append(
                f"[{status}] {hyp.get('name', '?')} — {hyp.get('desc', '')}"
            )
            lines.append(
                f"       p={r.get('p_value', 1):.4f} "
                f"sharpe={r.get('sharpe', 0):.2f} "
                f"n={r.get('n_observations', 0)} "
                f"nonsense={r.get('nonsense_score', 0):.1f}"
            )

        return "\n".join(lines)
