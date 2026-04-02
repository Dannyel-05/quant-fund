"""
Frontier Signal Validator.

Seven-test validation suite for any new frontier signal:
  1. In-sample Sharpe (IS-SR ≥ 1.0)
  2. Out-of-sample Sharpe (OOS-SR ≥ 0.7)
  3. Monte Carlo permutation test (top 5%)
  4. Benjamini-Hochberg FDR control
  5. Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014)
  6. Stability: OOS SR / IS SR ≥ 0.5 (decay check)
  7. Autocorrelation check: ensure signal is not trivially AR(1)

Extra certifications:
  - PublishedReplicationTest: compare to known academic benchmark
  - NoveltyCertification: confirm <0.7 correlation to all existing signals
  - EvidenceGrade: A/B/C/D/F assignment
"""
import logging
import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from frontier.validation.evidence_tracker import assign_evidence_grade, grade_to_tier

logger = logging.getLogger(__name__)

# Minimum thresholds
MIN_IS_SHARPE  = 1.0
MIN_OOS_SHARPE = 0.7
MIN_MC_PCTILE  = 0.95    # must beat 95% of random shuffles
MIN_STABILITY  = 0.50    # OOS_SR / IS_SR
MAX_AR1        = 0.30    # autocorrelation(lag=1) must be below this
DSR_MIN        = 0.50    # Deflated Sharpe Ratio threshold


# ---------------------------------------------------------------------------
# Core statistical helpers
# ---------------------------------------------------------------------------

def _sharpe(returns: pd.Series, ann_factor: float = 252.0) -> float:
    if len(returns) < 5 or returns.std() == 0:
        return 0.0
    return float((returns.mean() / returns.std()) * math.sqrt(ann_factor))


def _monte_carlo_pctile(
    returns: pd.Series, n_shuffles: int = 500
) -> float:
    """Return fraction of random shuffles whose Sharpe is below observed."""
    observed = _sharpe(returns)
    arr = returns.values.copy()
    rng = np.random.default_rng(42)
    beats = sum(
        1
        for _ in range(n_shuffles)
        if _sharpe(pd.Series(rng.permutation(arr))) < observed
    )
    return beats / n_shuffles


def _deflated_sharpe(
    observed_sr: float, n_trials: int, n_obs: int
) -> float:
    """
    Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014).
    Adjusts for the multiple testing bias from searching over many trials.
    DSR ≈ SR × (1 − √(log(n_trials) / n_obs))
    """
    if n_obs <= 0 or n_trials <= 1:
        return observed_sr
    penalty = math.sqrt(math.log(n_trials) / n_obs)
    return observed_sr * max(0.0, 1.0 - penalty)


def _benjamini_hochberg(p_values: List[float], fdr: float = 0.05) -> List[bool]:
    """Return list of booleans: True if hypothesis survives BH correction."""
    n = len(p_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(p_values), key=lambda x: x[1])
    reject = [False] * n
    for rank, (idx, pv) in enumerate(indexed, start=1):
        if pv <= fdr * rank / n:
            reject[idx] = True
    return reject


# ---------------------------------------------------------------------------
# Main validator
# ---------------------------------------------------------------------------

class FrontierValidator:
    """
    Runs the full 7-test validation suite on a candidate frontier signal.

    Usage
    -----
    validator = FrontierValidator(config)
    result = validator.validate(
        signal_name="my_signal",
        is_returns=pd.Series(...),
        oos_returns=pd.Series(...),
        n_trials_searched=50,
        has_published_paper=False,
        existing_signals={"other_signal": pd.Series(...)},
    )
    """

    def __init__(self, config: Optional[Dict] = None):
        self._cfg = (config or {}).get("frontier", {}).get("validation", {})

    def validate(
        self,
        signal_name: str,
        is_returns: pd.Series,
        oos_returns: pd.Series,
        n_trials_searched: int = 1,
        has_published_paper: bool = False,
        replications: int = 0,
        existing_signals: Optional[Dict[str, pd.Series]] = None,
        p_values_batch: Optional[List[float]] = None,
    ) -> Dict:
        """
        Run all 7 tests plus bonus certifications.

        Returns
        -------
        dict with keys:
          passed (bool), tests (dict per test), evidence_grade,
          suggested_tier, summary (str)
        """
        results: Dict[str, Dict] = {}

        # --- Test 1: In-sample Sharpe ---
        is_sr = _sharpe(is_returns)
        results["is_sharpe"] = {
            "value": round(is_sr, 3),
            "threshold": MIN_IS_SHARPE,
            "pass": is_sr >= MIN_IS_SHARPE,
        }

        # --- Test 2: Out-of-sample Sharpe ---
        oos_sr = _sharpe(oos_returns)
        results["oos_sharpe"] = {
            "value": round(oos_sr, 3),
            "threshold": MIN_OOS_SHARPE,
            "pass": oos_sr >= MIN_OOS_SHARPE,
        }

        # --- Test 3: Monte Carlo permutation ---
        mc_pct = _monte_carlo_pctile(oos_returns)
        results["monte_carlo"] = {
            "value": round(mc_pct, 4),
            "threshold": MIN_MC_PCTILE,
            "pass": mc_pct >= MIN_MC_PCTILE,
        }

        # --- Test 4: Benjamini-Hochberg FDR ---
        if p_values_batch and len(p_values_batch) > 1:
            bh_results = _benjamini_hochberg(p_values_batch)
            # Assume signal under test is the last entry
            bh_pass = bh_results[-1] if bh_results else True
        else:
            bh_pass = True  # single test — no correction needed
        results["benjamini_hochberg"] = {
            "value": int(bh_pass),
            "threshold": 1,
            "pass": bool(bh_pass),
        }

        # --- Test 5: Deflated Sharpe Ratio ---
        dsr = _deflated_sharpe(oos_sr, n_trials_searched, len(oos_returns))
        results["deflated_sharpe"] = {
            "value": round(dsr, 3),
            "threshold": DSR_MIN,
            "pass": dsr >= DSR_MIN,
        }

        # --- Test 6: Stability (OOS decay) ---
        stability = (oos_sr / is_sr) if is_sr > 0 else 0.0
        results["stability"] = {
            "value": round(stability, 3),
            "threshold": MIN_STABILITY,
            "pass": stability >= MIN_STABILITY,
        }

        # --- Test 7: AR(1) autocorrelation ---
        if len(oos_returns) > 5:
            ar1 = float(oos_returns.autocorr(lag=1))
            if np.isnan(ar1):
                ar1 = 0.0
        else:
            ar1 = 0.0
        results["autocorrelation"] = {
            "value": round(abs(ar1), 3),
            "threshold": MAX_AR1,
            "pass": abs(ar1) <= MAX_AR1,
        }

        # --- Novelty certification ---
        novelty_pass, max_corr = self._novelty_cert(signal_name, oos_returns, existing_signals)
        results["novelty"] = {
            "value": round(max_corr, 3),
            "threshold": 0.7,
            "pass": novelty_pass,
            "note": "max abs correlation to existing signals",
        }

        # --- Overall ---
        core_tests = ["is_sharpe", "oos_sharpe", "monte_carlo",
                      "deflated_sharpe", "stability", "autocorrelation"]
        all_core_pass = all(results[t]["pass"] for t in core_tests)
        overall_pass = all_core_pass and novelty_pass

        # --- Evidence grade ---
        grade = assign_evidence_grade(
            has_published_paper=has_published_paper,
            replications=replications,
            oos_sharpe=oos_sr,
            monte_carlo_pct=mc_pct,
            benjamini_pass=bool(bh_pass),
            fsp=1.0 - max_corr,
        )
        suggested_tier = grade_to_tier(grade)

        # Summary
        pass_count = sum(1 for t in results.values() if t["pass"])
        summary = (
            f"Signal '{signal_name}': {pass_count}/{len(results)} tests passed. "
            f"Grade={grade}, SuggestedTier={suggested_tier}, "
            f"IS_SR={is_sr:.2f}, OOS_SR={oos_sr:.2f}, MC={mc_pct:.1%}"
        )
        if overall_pass:
            logger.info(f"[Validator] PASS — {summary}")
        else:
            logger.warning(f"[Validator] FAIL — {summary}")

        return {
            "signal_name": signal_name,
            "passed": overall_pass,
            "tests": results,
            "evidence_grade": grade,
            "suggested_tier": suggested_tier,
            "summary": summary,
        }

    def _novelty_cert(
        self,
        signal_name: str,
        oos_returns: pd.Series,
        existing_signals: Optional[Dict[str, pd.Series]],
    ) -> Tuple[bool, float]:
        """Return (pass, max_abs_correlation). Pass if max corr < 0.7."""
        if not existing_signals:
            return True, 0.0
        max_corr = 0.0
        for name, other in existing_signals.items():
            try:
                aligned = pd.concat([oos_returns, other], axis=1).dropna()
                if len(aligned) < 10:
                    continue
                corr = abs(float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1])))
                if corr > max_corr:
                    max_corr = corr
            except Exception:
                pass
        return max_corr < 0.7, max_corr

    def quick_validate(self, signal_name: str, returns: pd.Series) -> Dict:
        """
        Fast validation splitting returns 70/30 IS/OOS.
        Suitable for automated discovery screening.
        """
        split = int(len(returns) * 0.7)
        is_r = returns.iloc[:split]
        oos_r = returns.iloc[split:]
        return self.validate(signal_name, is_r, oos_r)
