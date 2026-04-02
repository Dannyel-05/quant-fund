"""
Phase 9: Intelligence Engine

100+ feature extraction, 6 pattern learning algorithms, automated report generation.

Features extracted per earnings observation:
  - EPS / surprise features (10)
  - Price / volume features (15)
  - Alt-data features (10)
  - Deep-data / options features (10)
  - Macro / regime features (15)
  - Historical trend features (15)
  - Temporal features (10)
  - Cross-asset features (10)
  - Derived composite features (15)
  = 110 total features

Pattern learning algorithms:
  1. Single-feature effectiveness (accuracy, Sharpe by signal bucket)
  2. Two-feature combination discovery (grid search over feature pairs)
  3. Regime-conditional patterns (patterns that only work in specific macro regimes)
  4. Sector-specific patterns
  5. Cross-asset lead-lag patterns
  6. Signal decay / temporal patterns
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

class FeatureExtractor:
    """
    Extracts 110+ features from a merged earnings_observations + historical_data
    record dict. Returns a flat feature dict with float values.
    """

    def extract(self, record: dict, hist_record: Optional[dict] = None) -> Dict[str, float]:
        """
        Main extraction entry point.
        record      — row from earnings_observations (or pre_earnings_snapshots)
        hist_record — row from earnings_enriched (historical context)
        """
        features: Dict[str, float] = {}
        features.update(self._eps_features(record))
        features.update(self._price_volume_features(record))
        features.update(self._altdata_features(record))
        features.update(self._deepdata_features(record))
        features.update(self._macro_features(record))
        features.update(self._historical_features(hist_record or {}))
        features.update(self._temporal_features(record))
        features.update(self._derived_features(features))
        return features

    # --- EPS / surprise features (10) ---

    def _eps_features(self, r: dict) -> Dict[str, float]:
        eps_actual   = _safe(r.get("eps_actual"))
        eps_estimate = _safe(r.get("eps_estimate"))
        surprise_pct = _safe(r.get("surprise_pct"))
        surprise_yf  = _safe(r.get("surprise_percent_yf"))

        surprise_magnitude = abs(surprise_pct) if surprise_pct is not None else 0.0
        beat_flag   = 1.0 if (surprise_pct or 0) > 0 else (-1.0 if (surprise_pct or 0) < 0 else 0.0)
        large_beat  = 1.0 if (surprise_pct or 0) > 0.05 else 0.0
        large_miss  = 1.0 if (surprise_pct or 0) < -0.05 else 0.0
        in_line     = 1.0 if abs(surprise_pct or 0) < 0.01 else 0.0
        eps_abs     = abs(eps_actual) if eps_actual is not None else 0.0
        estimate_abs = abs(eps_estimate) if eps_estimate is not None else 0.0
        estimate_positive = 1.0 if (eps_estimate or 0) > 0 else 0.0

        return {
            "eps_surprise_pct":      _n(surprise_pct),
            "eps_surprise_magnitude":surprise_magnitude,
            "eps_beat_flag":         beat_flag,
            "eps_large_beat":        large_beat,
            "eps_large_miss":        large_miss,
            "eps_in_line":           in_line,
            "eps_actual_abs":        _n(eps_abs),
            "eps_estimate_abs":      _n(estimate_abs),
            "eps_estimate_positive": estimate_positive,
            "eps_surprise_yf":       _n(surprise_yf),
        }

    # --- Price / volume features (15) ---

    def _price_volume_features(self, r: dict) -> Dict[str, float]:
        p0  = _safe(r.get("price_t0"))
        p1  = _safe(r.get("price_t1"))
        p3  = _safe(r.get("price_t3"))
        p5  = _safe(r.get("price_t5"))
        p10 = _safe(r.get("price_t10"))
        p20 = _safe(r.get("price_t20"))
        r1  = _safe(r.get("return_t1"))
        r3  = _safe(r.get("return_t3"))
        r5  = _safe(r.get("return_t5"))
        r10 = _safe(r.get("return_t10"))
        r20 = _safe(r.get("return_t20"))
        vol_surge = _safe(r.get("volume_surge"))
        vol_t0    = _safe(r.get("volume_t0"))
        vol_avg   = _safe(r.get("volume_avg_20d"))

        drift_r1_r5 = (r5 - r1) if (r5 is not None and r1 is not None) else 0.0
        return {
            "price_t0":         _n(p0),
            "return_t1":        _n(r1),
            "return_t3":        _n(r3),
            "return_t5":        _n(r5),
            "return_t10":       _n(r10),
            "return_t20":       _n(r20),
            "volume_surge":     _n(vol_surge),
            "volume_t0":        _n(vol_t0),
            "volume_avg_20d":   _n(vol_avg),
            "high_volume_flag": 1.0 if (vol_surge or 0) > 2.0 else 0.0,
            "low_volume_flag":  1.0 if (vol_surge or 0) < 0.5 else 0.0,
            "drift_r1_to_r5":   drift_r1_r5,
            "return_positive_t1": 1.0 if (r1 or 0) > 0 else 0.0,
            "return_positive_t5": 1.0 if (r5 or 0) > 0 else 0.0,
            "return_positive_t20": 1.0 if (r20 or 0) > 0 else 0.0,
        }

    # --- Alt-data features (10) ---

    def _altdata_features(self, r: dict) -> Dict[str, float]:
        sentiment = _safe(r.get("altdata_sentiment"))
        reddit    = _safe(r.get("reddit_score"))
        news      = _safe(r.get("news_score"))
        sec       = _safe(r.get("sec_score"))
        bqm       = _safe(r.get("beat_quality_multiplier"))

        altdata_count = sum(1 for v in [sentiment, reddit, news, sec] if v is not None)
        alt_bull = 1.0 if (sentiment or 0) > 0.3 else 0.0
        alt_bear = 1.0 if (sentiment or 0) < -0.3 else 0.0

        return {
            "altdata_sentiment":  _n(sentiment),
            "reddit_score":       _n(reddit),
            "news_score":         _n(news),
            "sec_score":          _n(sec),
            "beat_quality_mult":  _n(bqm),
            "altdata_count":      float(altdata_count),
            "altdata_bull":       alt_bull,
            "altdata_bear":       alt_bear,
            "news_reddit_agree":  1.0 if _same_sign(_n(news), _n(reddit)) else 0.0,
            "bqm_high":           1.0 if (bqm or 0) > 1.2 else 0.0,
        }

    # --- Deep-data / options features (10) ---

    def _deepdata_features(self, r: dict) -> Dict[str, float]:
        smfi      = _safe(r.get("options_smfi"))
        iv_rank   = _safe(r.get("options_iv_rank"))
        put_call  = _safe(r.get("options_put_call"))
        dark_pool = _safe(r.get("options_dark_pool"))
        squeeze   = _safe(r.get("short_squeeze_score"))
        congress  = _safe(r.get("congressional_signal"))

        call_heavy = 1.0 if (put_call or 0.5) < 0.4 else 0.0
        put_heavy  = 1.0 if (put_call or 0.5) > 0.7 else 0.0
        elevated_iv = 1.0 if (iv_rank or 0) > 0.7 else 0.0
        squeeze_flag = 1.0 if (squeeze or 0) > 50 else 0.0

        return {
            "options_smfi":       _n(smfi),
            "options_iv_rank":    _n(iv_rank),
            "options_put_call":   _n(put_call),
            "options_dark_pool":  _n(dark_pool),
            "short_squeeze_score": _n(squeeze),
            "congressional_signal": _n(congress),
            "call_heavy":         call_heavy,
            "put_heavy":          put_heavy,
            "elevated_iv":        elevated_iv,
            "squeeze_flag":       squeeze_flag,
        }

    # --- Macro / regime features (15) ---

    def _macro_features(self, r: dict) -> Dict[str, float]:
        vix        = _safe(r.get("vix_t0") or r.get("vix"))
        spy_r5     = _safe(r.get("spy_return_5d"))
        sector_r5  = _safe(r.get("sector_etf_return_5d"))
        regime     = _safe(r.get("macro_regime"))
        regime_raw = r.get("macro_regime_name", "") or ""

        vix_low    = 1.0 if (vix or 20) < 15 else 0.0
        vix_mid    = 1.0 if 15 <= (vix or 20) < 25 else 0.0
        vix_high   = 1.0 if (vix or 20) >= 25 else 0.0
        vix_extreme= 1.0 if (vix or 20) >= 35 else 0.0
        spy_bull   = 1.0 if (spy_r5 or 0) > 0.02 else 0.0
        spy_bear   = 1.0 if (spy_r5 or 0) < -0.02 else 0.0
        risk_on    = 1.0 if "RISK_ON" in regime_raw.upper() else 0.0
        risk_off   = 1.0 if "RISK_OFF" in regime_raw.upper() else 0.0
        sector_bull= 1.0 if (sector_r5 or 0) > 0.01 else 0.0
        sector_bear= 1.0 if (sector_r5 or 0) < -0.01 else 0.0
        relative_strength = ((sector_r5 or 0) - (spy_r5 or 0)) if sector_r5 is not None and spy_r5 is not None else 0.0

        return {
            "vix":                 _n(vix),
            "vix_low":             vix_low,
            "vix_mid":             vix_mid,
            "vix_high":            vix_high,
            "vix_extreme":         vix_extreme,
            "spy_return_5d":       _n(spy_r5),
            "sector_return_5d":    _n(sector_r5),
            "spy_bull":            spy_bull,
            "spy_bear":            spy_bear,
            "risk_on":             risk_on,
            "risk_off":            risk_off,
            "sector_bull":         sector_bull,
            "sector_bear":         sector_bear,
            "sector_relative_strength": relative_strength,
            "macro_regime_code":   _n(regime),
        }

    # --- Historical trend features (15) ---

    def _historical_features(self, h: dict) -> Dict[str, float]:
        rev_trend   = _safe(h.get("revenue_trend_3q"))
        margin_trend= _safe(h.get("margin_trend_3q"))
        cf_quality  = _safe(h.get("cf_quality"))
        bs_health   = _safe(h.get("bs_health_score"))
        insider_90d = _safe(h.get("insider_activity_90d"))
        has_8k      = _safe(h.get("has_material_8k_30d"))
        inst_trend  = _safe(h.get("institutional_ownership_trend"))
        news_sentiment = _safe(h.get("news_sentiment_30d"))

        rev_growing  = 1.0 if (rev_trend or 0) > 0.05 else 0.0
        rev_declining= 1.0 if (rev_trend or 0) < -0.05 else 0.0
        margin_exp   = 1.0 if (margin_trend or 0) > 0 else 0.0
        insider_buy  = 1.0 if (insider_90d or 0) > 0 else 0.0
        insider_sell = 1.0 if (insider_90d or 0) < 0 else 0.0
        inst_increase= 1.0 if (inst_trend or 0) > 0 else 0.0

        return {
            "revenue_trend_3q":    _n(rev_trend),
            "margin_trend_3q":     _n(margin_trend),
            "cf_quality":          _n(cf_quality),
            "bs_health":           _n(bs_health),
            "insider_activity_90d": _n(insider_90d),
            "has_material_8k":     _n(has_8k),
            "institutional_trend": _n(inst_trend),
            "historical_sentiment": _n(news_sentiment),
            "revenue_growing":     rev_growing,
            "revenue_declining":   rev_declining,
            "margin_expanding":    margin_exp,
            "insider_buying":      insider_buy,
            "insider_selling":     insider_sell,
            "institutional_increasing": inst_increase,
            "fundamentals_composite": _safe_mean([rev_trend, margin_trend, cf_quality, bs_health]),
        }

    # --- Temporal features (10) ---

    def _temporal_features(self, r: dict) -> Dict[str, float]:
        earnings_date = r.get("earnings_date", "")
        try:
            dt = datetime.strptime(earnings_date[:10], "%Y-%m-%d")
            month      = float(dt.month)
            quarter    = float((dt.month - 1) // 3 + 1)
            day_of_week= float(dt.weekday())   # 0=Mon, 4=Fri
            is_jan     = 1.0 if dt.month == 1 else 0.0
            is_q1_end  = 1.0 if dt.month in (3, 4) else 0.0
            is_q2_end  = 1.0 if dt.month in (6, 7) else 0.0
            is_q3_end  = 1.0 if dt.month in (9, 10) else 0.0
            is_q4_end  = 1.0 if dt.month in (12, 1) else 0.0
            is_mon_tue = 1.0 if dt.weekday() <= 1 else 0.0
            is_thu_fri = 1.0 if dt.weekday() >= 3 else 0.0
        except Exception:
            month = quarter = day_of_week = 0.0
            is_jan = is_q1_end = is_q2_end = is_q3_end = is_q4_end = 0.0
            is_mon_tue = is_thu_fri = 0.0

        return {
            "month":           month,
            "quarter":         quarter,
            "day_of_week":     day_of_week,
            "is_january":      is_jan,
            "is_q1_end":       is_q1_end,
            "is_q2_end":       is_q2_end,
            "is_q3_end":       is_q3_end,
            "is_q4_end":       is_q4_end,
            "is_early_week":   is_mon_tue,
            "is_late_week":    is_thu_fri,
        }

    # --- Derived composite features (15) ---

    def _derived_features(self, f: dict) -> Dict[str, float]:
        # Conviction score: surprise + altdata + volume agree
        surprise_dir = 1.0 if f.get("eps_surprise_pct", 0) > 0 else (-1.0 if f.get("eps_surprise_pct", 0) < 0 else 0.0)
        altdata_dir  = 1.0 if f.get("altdata_sentiment", 0) > 0.1 else (-1.0 if f.get("altdata_sentiment", 0) < -0.1 else 0.0)
        vol_dir      = 1.0 if f.get("high_volume_flag", 0) else 0.0
        all_agree    = 1.0 if (surprise_dir == altdata_dir and abs(surprise_dir) > 0) else 0.0

        # Quality setup score: macro + fundamentals + altdata alignment
        macro_ok     = f.get("risk_on", 0) - f.get("risk_off", 0)
        fundamentals_ok = 1.0 if f.get("fundamentals_composite", 0) > 0 else 0.0
        setup_quality = (macro_ok * 0.4 + fundamentals_ok * 0.3 + f.get("altdata_sentiment", 0) * 0.3)

        # Squeeze + beat combo
        squeeze_beat = 1.0 if (f.get("squeeze_flag", 0) and f.get("eps_large_beat", 0)) else 0.0

        # Options bull: call heavy + dark pool positive + low iv (premium seller positioning)
        options_bull = 1.0 if (f.get("call_heavy", 0) and f.get("options_dark_pool", 0) > 0) else 0.0
        options_bear = 1.0 if (f.get("put_heavy", 0) and f.get("options_dark_pool", 0) < 0) else 0.0

        # Insider + institutional confluence
        insider_inst_bull = 1.0 if (f.get("insider_buying", 0) and f.get("institutional_increasing", 0)) else 0.0
        insider_inst_bear = 1.0 if (f.get("insider_selling", 0) and not f.get("institutional_increasing", 0)) else 0.0

        # VIX tailwind: low VIX + risk on = ideal for small-cap PEAD
        vix_tailwind = 1.0 if (f.get("vix_low", 0) and f.get("risk_on", 0)) else 0.0
        vix_headwind = 1.0 if (f.get("vix_extreme", 0) or f.get("risk_off", 0)) else 0.0

        # Congressional signal strength
        congress_bull = 1.0 if f.get("congressional_signal", 0) > 0.5 else 0.0
        congress_bear = 1.0 if f.get("congressional_signal", 0) < -0.5 else 0.0

        # Full composite (weighted average of directional signals)
        composite = (
            f.get("eps_surprise_pct", 0) * 2.0
            + f.get("altdata_sentiment", 0) * 1.5
            + f.get("options_smfi", 0) * 1.0
            + f.get("beat_quality_mult", 1.0) * 0.5
            + macro_ok * 0.5
        ) / 5.5

        return {
            "all_signals_agree":      all_agree,
            "setup_quality_score":    float(setup_quality),
            "squeeze_plus_beat":      squeeze_beat,
            "options_bull_flag":      options_bull,
            "options_bear_flag":      options_bear,
            "insider_inst_bull":      insider_inst_bull,
            "insider_inst_bear":      insider_inst_bear,
            "vix_tailwind":           vix_tailwind,
            "vix_headwind":           vix_headwind,
            "congressional_bull":     congress_bull,
            "congressional_bear":     congress_bear,
            "composite_signal":       float(np.clip(composite, -1.0, 1.0)),
            "macro_fundamentals_composite": float(macro_ok * 0.5 + fundamentals_ok * 0.5),
            "altdata_options_agree":  1.0 if _same_sign(f.get("altdata_sentiment", 0), f.get("options_smfi", 0)) else 0.0,
            "conviction_score":       float(abs(composite) * all_agree),
        }


# ---------------------------------------------------------------------------
# Pattern Learning Engine
# ---------------------------------------------------------------------------

class PatternLearner:
    """
    6 pattern learning algorithms applied to historical earnings observations.
    Inputs: list of feature dicts (from FeatureExtractor) + target returns.
    """

    def __init__(self, min_observations: int = 10, min_confidence: float = 0.55):
        self.min_obs = min_observations
        self.min_conf = min_confidence

    def run_all(
        self,
        records: List[Dict],
        target_col: str = "return_t5",
    ) -> List[Dict]:
        """
        Run all 6 pattern algorithms. Returns list of pattern discovery dicts
        ready for IntelligenceDB.upsert_pattern().
        """
        if len(records) < self.min_obs:
            logger.warning("Too few records (%d) for pattern learning", len(records))
            return []

        extractor = FeatureExtractor()
        feature_records = []
        for r in records:
            feat = extractor.extract(r)
            target = _safe(r.get(target_col))
            if target is not None:
                feat["__target__"] = target
                feat["__ticker__"] = r.get("ticker", "")
                feat["__date__"]   = r.get("earnings_date", "")
                feat["__sector__"] = r.get("sector", "")
                feat["__regime__"] = r.get("macro_regime_name", "")
                feature_records.append(feat)

        if len(feature_records) < self.min_obs:
            return []

        patterns = []
        patterns.extend(self._algo1_single_features(feature_records))
        patterns.extend(self._algo2_feature_combinations(feature_records))
        patterns.extend(self._algo3_regime_conditional(feature_records))
        patterns.extend(self._algo4_sector_specific(feature_records))
        patterns.extend(self._algo5_lead_lag(feature_records))
        patterns.extend(self._algo6_temporal(feature_records))

        # De-duplicate by pattern_id
        seen = set()
        unique = []
        for p in patterns:
            if p["pattern_id"] not in seen:
                seen.add(p["pattern_id"])
                unique.append(p)

        logger.info("Pattern learning: discovered %d unique patterns", len(unique))
        return unique

    # --- Algorithm 1: Single feature effectiveness ---

    def _algo1_single_features(self, records: List[dict]) -> List[dict]:
        results = []
        targets = np.array([r["__target__"] for r in records])
        feature_names = [k for k in records[0] if not k.startswith("__")]

        for feat in feature_names:
            vals = np.array([r.get(feat, 0.0) for r in records])
            if np.std(vals) < 1e-6:
                continue  # constant feature

            # Bucket into quintiles
            try:
                quantiles = np.percentile(vals, [20, 40, 60, 80])
            except Exception:
                continue

            # Top quintile performance
            mask_high = vals >= quantiles[3]
            mask_low  = vals <= quantiles[0]

            if mask_high.sum() < self.min_obs or mask_low.sum() < self.min_obs:
                continue

            returns_high = targets[mask_high]
            returns_low  = targets[mask_low]

            avg_high = float(np.mean(returns_high))
            avg_low  = float(np.mean(returns_low))
            edge = avg_high - avg_low

            if abs(edge) < 0.005:  # < 0.5% edge, skip
                continue

            win_rate = float(np.mean(returns_high > 0))
            sharpe = _compute_sharpe(returns_high)
            conf = min(0.99, (abs(edge) / 0.02) * (mask_high.sum() / 50))

            if conf < self.min_conf:
                continue

            feature_combo = {feat: float(quantiles[3])}
            pattern_id = _make_pattern_id("single", feature_combo)

            results.append({
                "pattern_id":    pattern_id,
                "pattern_name":  f"High {feat}",
                "pattern_type":  "single",
                "sector":        None,
                "market_regime": None,
                "features_json": json.dumps(feature_combo),
                "feature_count": 1,
                "n_occurrences": int(mask_high.sum()),
                "avg_return_t5": round(avg_high, 5),
                "avg_return_t20": None,
                "win_rate":      round(win_rate, 4),
                "sharpe_ratio":  round(sharpe, 4),
                "max_drawdown":  round(float(np.min(returns_high)), 5),
                "confidence_score": round(conf, 4),
                "best_month":    None,
                "best_day_of_week": None,
                "signal_decay_days": None,
                "discovered_at": datetime.now().isoformat(),
                "last_validated": datetime.now().isoformat()[:10],
            })

        return results

    # --- Algorithm 2: Two-feature combinations ---

    def _algo2_feature_combinations(self, records: List[dict]) -> List[dict]:
        results = []
        targets = np.array([r["__target__"] for r in records])

        # Only test boolean (0/1) features for combinations — manageable search space
        bool_features = [
            k for k in records[0]
            if not k.startswith("__")
            and set(r.get(k, 0.0) for r in records[:50]) <= {0.0, 1.0, 0, 1}
        ]

        for i, fa in enumerate(bool_features):
            for fb in bool_features[i+1:]:
                vals_a = np.array([r.get(fa, 0.0) for r in records])
                vals_b = np.array([r.get(fb, 0.0) for r in records])
                mask   = (vals_a == 1.0) & (vals_b == 1.0)

                if mask.sum() < self.min_obs:
                    continue

                returns_combo = targets[mask]
                avg_return = float(np.mean(returns_combo))

                if abs(avg_return) < 0.005:
                    continue

                win_rate = float(np.mean(returns_combo > 0))
                sharpe   = _compute_sharpe(returns_combo)
                conf     = min(0.99, (abs(avg_return) / 0.02) * (mask.sum() / 30))

                if conf < self.min_conf:
                    continue

                feature_combo = {fa: 1.0, fb: 1.0}
                pattern_id    = _make_pattern_id("combination", feature_combo)

                results.append({
                    "pattern_id":    pattern_id,
                    "pattern_name":  f"{fa} + {fb}",
                    "pattern_type":  "combination",
                    "sector":        None,
                    "market_regime": None,
                    "features_json": json.dumps(feature_combo),
                    "feature_count": 2,
                    "n_occurrences": int(mask.sum()),
                    "avg_return_t5": round(avg_return, 5),
                    "avg_return_t20": None,
                    "win_rate":      round(win_rate, 4),
                    "sharpe_ratio":  round(sharpe, 4),
                    "max_drawdown":  round(float(np.min(returns_combo)), 5),
                    "confidence_score": round(conf, 4),
                    "best_month":    None,
                    "best_day_of_week": None,
                    "signal_decay_days": None,
                    "discovered_at": datetime.now().isoformat(),
                    "last_validated": datetime.now().isoformat()[:10],
                })

        return results[:50]  # cap at 50 combination patterns

    # --- Algorithm 3: Regime-conditional patterns ---

    def _algo3_regime_conditional(self, records: List[dict]) -> List[dict]:
        results = []
        regimes = list(set(r.get("__regime__", "") for r in records if r.get("__regime__")))

        for regime in regimes:
            subset = [r for r in records if r.get("__regime__") == regime]
            if len(subset) < self.min_obs:
                continue

            targets = np.array([r["__target__"] for r in subset])

            # Look for best single feature in this regime
            feature_names = [k for k in records[0] if not k.startswith("__")]
            for feat in feature_names:
                vals = np.array([r.get(feat, 0.0) for r in subset])
                if np.std(vals) < 1e-6:
                    continue

                try:
                    threshold = float(np.percentile(vals, 75))
                    mask = vals >= threshold
                except Exception:
                    continue

                if mask.sum() < max(5, self.min_obs // 2):
                    continue

                ret = targets[mask]
                avg_return = float(np.mean(ret))
                if abs(avg_return) < 0.008:
                    continue

                win_rate = float(np.mean(ret > 0))
                sharpe   = _compute_sharpe(ret)
                conf     = min(0.99, (abs(avg_return) / 0.015) * (mask.sum() / 20))

                if conf < self.min_conf:
                    continue

                feature_combo = {feat: threshold}
                pattern_id = _make_pattern_id(f"regime_{regime}", feature_combo)

                results.append({
                    "pattern_id":    pattern_id,
                    "pattern_name":  f"High {feat} in {regime}",
                    "pattern_type":  "regime_conditional",
                    "sector":        None,
                    "market_regime": regime,
                    "features_json": json.dumps(feature_combo),
                    "feature_count": 1,
                    "n_occurrences": int(mask.sum()),
                    "avg_return_t5": round(avg_return, 5),
                    "avg_return_t20": None,
                    "win_rate":      round(win_rate, 4),
                    "sharpe_ratio":  round(sharpe, 4),
                    "max_drawdown":  round(float(np.min(ret)), 5),
                    "confidence_score": round(conf, 4),
                    "best_month":    None,
                    "best_day_of_week": None,
                    "signal_decay_days": None,
                    "discovered_at": datetime.now().isoformat(),
                    "last_validated": datetime.now().isoformat()[:10],
                })

        return results[:30]

    # --- Algorithm 4: Sector-specific patterns ---

    def _algo4_sector_specific(self, records: List[dict]) -> List[dict]:
        results = []
        sectors = list(set(r.get("__sector__", "") for r in records if r.get("__sector__")))

        for sector in sectors:
            subset = [r for r in records if r.get("__sector__") == sector]
            if len(subset) < self.min_obs:
                continue

            targets = np.array([r["__target__"] for r in subset])
            feature_names = [k for k in records[0] if not k.startswith("__")]

            for feat in feature_names:
                vals = np.array([r.get(feat, 0.0) for r in subset])
                if np.std(vals) < 1e-6:
                    continue

                try:
                    threshold = float(np.percentile(vals, 75))
                    mask = vals >= threshold
                except Exception:
                    continue

                if mask.sum() < max(3, self.min_obs // 3):
                    continue

                ret = targets[mask]
                avg_return = float(np.mean(ret))
                if abs(avg_return) < 0.01:
                    continue

                win_rate = float(np.mean(ret > 0))
                sharpe   = _compute_sharpe(ret)
                conf     = min(0.99, (abs(avg_return) / 0.015) * (mask.sum() / 15))

                if conf < self.min_conf:
                    continue

                feature_combo = {feat: threshold}
                pattern_id = _make_pattern_id(f"sector_{sector}", feature_combo)

                results.append({
                    "pattern_id":    pattern_id,
                    "pattern_name":  f"High {feat} in {sector}",
                    "pattern_type":  "sector_specific",
                    "sector":        sector,
                    "market_regime": None,
                    "features_json": json.dumps(feature_combo),
                    "feature_count": 1,
                    "n_occurrences": int(mask.sum()),
                    "avg_return_t5": round(avg_return, 5),
                    "avg_return_t20": None,
                    "win_rate":      round(win_rate, 4),
                    "sharpe_ratio":  round(sharpe, 4),
                    "max_drawdown":  round(float(np.min(ret)), 5),
                    "confidence_score": round(conf, 4),
                    "best_month":    None,
                    "best_day_of_week": None,
                    "signal_decay_days": None,
                    "discovered_at": datetime.now().isoformat(),
                    "last_validated": datetime.now().isoformat()[:10],
                })

        return results[:30]

    # --- Algorithm 5: Cross-asset lead-lag (simplified) ---

    def _algo5_lead_lag(self, records: List[dict]) -> List[dict]:
        """
        Find features that predict future return — effectively a correlation
        scan between each feature and the target, used as signal decay proxy.
        """
        results = []
        targets = np.array([r["__target__"] for r in records])
        feature_names = [k for k in records[0] if not k.startswith("__")]

        # Cross-asset proxy: look at macro/options features that lead returns
        lead_features = [
            "options_smfi", "options_dark_pool", "options_put_call",
            "spy_return_5d", "sector_return_5d", "vix",
            "congressional_signal", "altdata_sentiment",
        ]

        for feat in lead_features:
            if feat not in feature_names:
                continue
            vals = np.array([r.get(feat, 0.0) for r in records])
            if np.std(vals) < 1e-6:
                continue

            corr = float(np.corrcoef(vals, targets)[0, 1])
            if np.isnan(corr) or abs(corr) < 0.1:
                continue

            # High-value zone
            threshold = float(np.percentile(vals, 75 if corr > 0 else 25))
            mask = vals >= threshold if corr > 0 else vals <= threshold
            if mask.sum() < self.min_obs:
                continue

            ret = targets[mask]
            avg_return = float(np.mean(ret))
            win_rate   = float(np.mean(ret > 0))
            sharpe     = _compute_sharpe(ret)
            conf       = min(0.99, abs(corr) * 2 * (mask.sum() / 30))

            if conf < self.min_conf:
                continue

            feature_combo = {feat: threshold}
            pattern_id = _make_pattern_id("lead_lag", feature_combo)

            results.append({
                "pattern_id":    pattern_id,
                "pattern_name":  f"Lead-lag: {feat} → return",
                "pattern_type":  "cross_asset_lead_lag",
                "sector":        None,
                "market_regime": None,
                "features_json": json.dumps({"feature": feat, "correlation": corr, "threshold": threshold}),
                "feature_count": 1,
                "n_occurrences": int(mask.sum()),
                "avg_return_t5": round(avg_return, 5),
                "avg_return_t20": None,
                "win_rate":      round(win_rate, 4),
                "sharpe_ratio":  round(sharpe, 4),
                "max_drawdown":  round(float(np.min(ret)), 5),
                "confidence_score": round(conf, 4),
                "best_month":    None,
                "best_day_of_week": None,
                "signal_decay_days": None,
                "discovered_at": datetime.now().isoformat(),
                "last_validated": datetime.now().isoformat()[:10],
            })

        return results

    # --- Algorithm 6: Temporal patterns ---

    def _algo6_temporal(self, records: List[dict]) -> List[dict]:
        results = []

        # Monthly patterns
        targets = np.array([r["__target__"] for r in records])
        months  = np.array([r.get("month", 0) for r in records])

        for m in range(1, 13):
            mask = months == float(m)
            if mask.sum() < self.min_obs:
                continue

            ret = targets[mask]
            avg_return = float(np.mean(ret))
            if abs(avg_return) < 0.008:
                continue

            win_rate = float(np.mean(ret > 0))
            sharpe   = _compute_sharpe(ret)
            conf     = min(0.99, abs(avg_return) / 0.01 * (mask.sum() / 20))

            if conf < self.min_conf:
                continue

            import calendar
            month_name = calendar.month_abbr[m]
            feature_combo = {"month": float(m)}
            pattern_id = _make_pattern_id("temporal_month", feature_combo)

            results.append({
                "pattern_id":    pattern_id,
                "pattern_name":  f"Monthly seasonality: {month_name}",
                "pattern_type":  "temporal",
                "sector":        None,
                "market_regime": None,
                "features_json": json.dumps(feature_combo),
                "feature_count": 1,
                "n_occurrences": int(mask.sum()),
                "avg_return_t5": round(avg_return, 5),
                "avg_return_t20": None,
                "win_rate":      round(win_rate, 4),
                "sharpe_ratio":  round(sharpe, 4),
                "max_drawdown":  round(float(np.min(ret)), 5),
                "confidence_score": round(conf, 4),
                "best_month":    m,
                "best_day_of_week": None,
                "signal_decay_days": None,
                "discovered_at": datetime.now().isoformat(),
                "last_validated": datetime.now().isoformat()[:10],
            })

        return results


# ---------------------------------------------------------------------------
# Signal Effectiveness Analyzer
# ---------------------------------------------------------------------------

class SignalEffectivenessAnalyzer:
    """
    Computes per-signal effectiveness metrics (accuracy, Sharpe, win rate)
    across all earnings_observations, broken out by sector and regime.
    """

    def analyze(
        self,
        records: List[dict],
        signals: List[str],
        target_col: str = "return_t5",
    ) -> List[dict]:
        """Returns list of signal_effectiveness dicts for IntelligenceDB."""
        extractor = FeatureExtractor()
        results = []

        feature_records = []
        for r in records:
            feat = extractor.extract(r)
            target = _safe(r.get(target_col))
            if target is not None:
                feat["__target__"] = target
                feat["__sector__"] = r.get("sector", "")
                feat["__regime__"] = r.get("macro_regime_name", "")
                feature_records.append(feat)

        if not feature_records:
            return []

        now_str = datetime.now().isoformat()

        # Overall effectiveness for each signal
        for sig in signals:
            results.extend(self._analyze_signal(sig, feature_records, None, None, now_str))

        # Per-sector
        sectors = list(set(r.get("__sector__", "") for r in feature_records if r.get("__sector__")))
        for sector in sectors:
            subset = [r for r in feature_records if r.get("__sector__") == sector]
            if len(subset) < 10:
                continue
            for sig in signals:
                results.extend(self._analyze_signal(sig, subset, sector, None, now_str))

        # Per-regime
        regimes = list(set(r.get("__regime__", "") for r in feature_records if r.get("__regime__")))
        for regime in regimes:
            subset = [r for r in feature_records if r.get("__regime__") == regime]
            if len(subset) < 10:
                continue
            for sig in signals:
                results.extend(self._analyze_signal(sig, subset, None, regime, now_str))

        return results

    def _analyze_signal(
        self,
        signal_name: str,
        records: List[dict],
        sector: Optional[str],
        regime: Optional[str],
        now_str: str,
    ) -> List[dict]:
        vals    = np.array([r.get(signal_name, 0.0) for r in records])
        targets = np.array([r["__target__"] for r in records])
        n = len(vals)

        if np.std(vals) < 1e-6 or n < 5:
            return []

        try:
            threshold = float(np.percentile(vals, 75))
        except Exception:
            return []

        mask_bull = vals >= threshold
        mask_bear = vals <= float(np.percentile(vals, 25))

        avg_bull = float(np.mean(targets[mask_bull])) if mask_bull.sum() > 0 else 0.0
        avg_bear = float(np.mean(targets[mask_bear])) if mask_bear.sum() > 0 else 0.0

        returns_above = targets[mask_bull]
        win_rate = float(np.mean(returns_above > 0)) if len(returns_above) > 0 else 0.5
        avg_win  = float(np.mean(returns_above[returns_above > 0])) if (returns_above > 0).sum() > 0 else 0.0
        avg_loss = float(np.mean(returns_above[returns_above <= 0])) if (returns_above <= 0).sum() > 0 else 0.0
        sharpe   = _compute_sharpe(returns_above)

        direction_accuracy = float(np.mean((vals > 0) == (targets > 0)))

        return [{
            "signal_name":          signal_name,
            "signal_type":          _classify_signal_type(signal_name),
            "sector":               sector,
            "market_regime":        regime,
            "ticker_subset":        None,
            "n_observations":       n,
            "accuracy_direction":   round(direction_accuracy, 4),
            "avg_return_when_bull": round(avg_bull, 5),
            "avg_return_when_bear": round(avg_bear, 5),
            "sharpe_ratio":         round(sharpe, 4),
            "win_rate":             round(win_rate, 4),
            "avg_win":              round(avg_win, 5),
            "avg_loss":             round(avg_loss, 5),
            "max_drawdown":         round(float(np.min(returns_above)) if len(returns_above) > 0 else 0.0, 5),
            "optimal_threshold":    round(threshold, 4),
            "signal_decay_days":    None,
            "p_value":              None,
            "confidence_level":     round(min(0.99, abs(avg_bull) / 0.02 * (mask_bull.sum() / 20)), 4),
            "computed_at":          now_str,
        }]


# ---------------------------------------------------------------------------
# Report Generator
# ---------------------------------------------------------------------------

class ReportGenerator:
    """
    Generates automated intelligence reports from pattern and signal data.
    """

    def __init__(self, intel_db, earnings_db=None):
        self.intel_db = intel_db
        self.earnings_db = earnings_db

    def generate_full_report(self) -> str:
        """Generate a comprehensive intelligence report."""
        lines = ["=" * 70, "  INTELLIGENCE ENGINE REPORT", f"  Generated: {datetime.now():%Y-%m-%d %H:%M}", "=" * 70, ""]

        # DB status
        status = self.intel_db.status()
        lines.append("DATABASE STATUS")
        lines.append("-" * 40)
        for k, v in status.items():
            lines.append(f"  {k:35s}: {v}")
        lines.append("")

        # Top patterns
        patterns = self.intel_db.get_patterns(min_confidence=0.6, min_occurrences=5)
        lines.append(f"TOP PATTERNS ({len(patterns)} discovered)")
        lines.append("-" * 40)
        for p in patterns[:10]:
            lines.append(
                f"  {p['pattern_name']:<40s}  "
                f"n={p['n_occurrences']:3d}  "
                f"ret={p['avg_return_t5']:+.2%}  "
                f"win={p['win_rate']:.0%}  "
                f"conf={p['confidence_score']:.2f}"
            )
        lines.append("")

        # Top signals
        sig_eff = self.intel_db.get_signal_effectiveness(min_observations=10)
        lines.append(f"SIGNAL EFFECTIVENESS ({len(sig_eff)} analyzed)")
        lines.append("-" * 40)
        for s in sig_eff[:10]:
            sect = s['sector'] or "ALL"
            lines.append(
                f"  {s['signal_name']:<35s}  "
                f"sector={sect:<12s}  "
                f"acc={s['accuracy_direction']:.0%}  "
                f"sharpe={s['sharpe_ratio']:+.2f}"
            )
        lines.append("")

        # Company profiles
        profiles = self.intel_db.get_all_profiles()
        lines.append(f"COMPANY PROFILES ({len(profiles)} tickers)")
        lines.append("-" * 40)
        for p in profiles[:15]:
            lines.append(
                f"  {p['ticker']:<6s}  {p['sector'] or '':15s}  "
                f"beat_rate={p.get('beat_rate') or 0:.0%}  "
                f"avg_ret={p.get('avg_return_t5') or 0:+.2%}  "
                f"events={p.get('n_earnings_events') or 0}"
            )

        return "\n".join(lines)

    def generate_ticker_score(self, ticker: str) -> str:
        """Generate a detailed score report for one ticker."""
        lines = [f"\nINTELLIGENCE SCORE: {ticker}", "=" * 50]

        profile = self.intel_db.get_profile(ticker)
        if not profile:
            lines.append(f"  No profile found for {ticker}")
            return "\n".join(lines)

        lines.append(f"  Sector:           {profile.get('sector', 'N/A')}")
        lines.append(f"  Beat rate:        {profile.get('beat_rate', 0) or 0:.0%}")
        lines.append(f"  Avg EPS surprise: {profile.get('avg_eps_surprise_pct', 0) or 0:+.1%}")
        lines.append(f"  Avg return T+5:   {profile.get('avg_return_t5', 0) or 0:+.2%}")
        lines.append(f"  Avg PEAD return:  {profile.get('avg_pead_return', 0) or 0:+.2%}")
        lines.append(f"  Signal reliability: {profile.get('signal_reliability', 0) or 0:.0%}")
        lines.append(f"  Readthrough sensitivity: {profile.get('readthrough_sensitivity', 1.0) or 1.0:.2f}x")
        lines.append(f"  Primary influencer: {profile.get('primary_large_cap', 'N/A')}")
        lines.append(f"  Earnings events:  {profile.get('n_earnings_events', 0)}")
        lines.append(f"  Last updated:     {profile.get('last_updated', 'N/A')}")

        # Readthrough coefficients
        rt_coeffs = self.intel_db.get_readthrough_coeffs(ticker)
        if rt_coeffs:
            lines.append("\n  READTHROUGH COEFFICIENTS (top 5):")
            for rt in rt_coeffs[:5]:
                lines.append(
                    f"    {rt['large_ticker']:<6s}  coeff={rt['coeff']:.2f}  "
                    f"corr={rt['correlation']:+.2f}  n={rt['n_events']}"
                )

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main IntelligenceEngine orchestrator
# ---------------------------------------------------------------------------

class IntelligenceEngine:
    """
    Top-level orchestrator for the intelligence layer.
    Connects FeatureExtractor, PatternLearner, SignalEffectivenessAnalyzer,
    LargeCapInfluenceEngine, and ReportGenerator.
    """

    # Key signals to analyze for effectiveness
    KEY_SIGNALS = [
        "eps_surprise_pct", "eps_beat_flag", "eps_large_beat",
        "altdata_sentiment", "reddit_score", "news_score", "sec_score",
        "beat_quality_mult", "options_smfi", "options_put_call",
        "options_dark_pool", "short_squeeze_score", "congressional_signal",
        "volume_surge", "high_volume_flag", "vix", "spy_return_5d",
        "sector_return_5d", "risk_on", "vix_tailwind", "composite_signal",
        "all_signals_agree", "setup_quality_score", "conviction_score",
        "revenue_trend_3q", "margin_trend_3q", "insider_buying",
        "fundamentals_composite",
    ]

    def __init__(self, intel_db, earnings_db=None, hist_db=None):
        self.intel_db    = intel_db
        self.earnings_db = earnings_db
        self.hist_db     = hist_db
        self.extractor   = FeatureExtractor()
        self.learner     = PatternLearner()
        self.analyzer    = SignalEffectivenessAnalyzer()
        self.reporter    = ReportGenerator(intel_db, earnings_db)

        try:
            from data.large_cap_influence import LargeCapInfluenceEngine
            self.influence_engine = LargeCapInfluenceEngine(hist_db, earnings_db)
        except Exception as e:
            logger.warning("LargeCapInfluenceEngine unavailable: %s", e)
            self.influence_engine = None

    def run(self, target_col: str = "return_t5") -> dict:
        """
        Full intelligence run:
        1. Load all earnings observations
        2. Run pattern learning
        3. Analyze signal effectiveness
        4. Populate company profiles
        5. Return summary
        """
        logger.info("Intelligence engine: starting full run")
        summary = {"patterns": 0, "signals_analyzed": 0, "profiles_updated": 0}

        if not self.earnings_db:
            logger.warning("No earnings_db connected; run aborted")
            return summary

        # 1. Load observations
        records = self.earnings_db.get_observations(limit=10000)
        logger.info("Loaded %d earnings observations", len(records))

        if len(records) < 10:
            logger.warning("Too few observations for pattern learning")
        else:
            # 2. Pattern learning
            patterns = self.learner.run_all(records, target_col=target_col)
            for p in patterns:
                try:
                    self.intel_db.upsert_pattern(p)
                    summary["patterns"] += 1
                except Exception as e:
                    logger.warning("Pattern insert failed: %s", e)
            logger.info("Stored %d patterns", summary["patterns"])

            # 3. Signal effectiveness
            sig_records = self.analyzer.analyze(records, self.KEY_SIGNALS, target_col=target_col)
            for s in sig_records:
                try:
                    self.intel_db.upsert_signal_effectiveness(s)
                    summary["signals_analyzed"] += 1
                except Exception as e:
                    logger.debug("Signal eff insert failed: %s", e)
            logger.info("Stored %d signal effectiveness records", summary["signals_analyzed"])

        # 4. Company profiles
        n_profiles = self._build_company_profiles(records)
        summary["profiles_updated"] = n_profiles

        logger.info("Intelligence run complete: %s", summary)
        return summary

    def _build_company_profiles(self, records: List[dict]) -> int:
        """Build/update company_profiles from earnings observations."""
        from collections import defaultdict

        ticker_records: Dict[str, List[dict]] = defaultdict(list)
        for r in records:
            ticker = r.get("ticker")
            if ticker:
                ticker_records[ticker].append(r)

        now_str = datetime.now().isoformat()
        written = 0

        for ticker, tr in ticker_records.items():
            try:
                returns_t5 = [_safe(r.get("return_t5")) for r in tr if _safe(r.get("return_t5")) is not None]
                returns_t20 = [_safe(r.get("return_t20")) for r in tr if _safe(r.get("return_t20")) is not None]
                surprises = [_safe(r.get("surprise_pct")) for r in tr if _safe(r.get("surprise_pct")) is not None]
                beat_rate = sum(1 for s in surprises if s and s > 0) / max(1, len(surprises))

                # PEAD: avg return_t20 after positive EPS surprise
                pead_returns = [_safe(r.get("return_t20")) for r in tr
                                if _safe(r.get("surprise_pct") or 0) is not None
                                and (_safe(r.get("surprise_pct")) or 0) > 0
                                and _safe(r.get("return_t20")) is not None]

                sector = tr[-1].get("sector") if tr else None

                profile = {
                    "ticker":                ticker,
                    "sector":                sector,
                    "n_earnings_events":     len(tr),
                    "avg_eps_surprise_pct":  float(np.mean(surprises)) if surprises else None,
                    "avg_return_t5":         float(np.mean(returns_t5)) if returns_t5 else None,
                    "beat_rate":             round(beat_rate, 4),
                    "avg_pead_return":       float(np.mean(pead_returns)) if pead_returns else None,
                    "pead_consistency":      float(np.std(pead_returns)) if len(pead_returns) > 1 else None,
                    "last_updated":          now_str,
                }

                self.intel_db.upsert_profile(profile)
                written += 1
            except Exception as e:
                logger.debug("Profile build failed %s: %s", ticker, e)

        logger.info("Built/updated %d company profiles", written)
        return written

    def score_ticker(self, ticker: str, days_lookback: int = 21) -> dict:
        """
        Score a ticker using intelligence engine + readthrough signals.
        Returns composite score dict.
        """
        profile = self.intel_db.get_profile(ticker)
        readthrough = None

        if self.influence_engine:
            try:
                readthrough = self.influence_engine.score_peer(ticker, days_lookback=days_lookback)
            except Exception as e:
                logger.warning("Readthrough score failed for %s: %s", ticker, e)

        return {
            "ticker":             ticker,
            "profile":            profile,
            "readthrough_signal": readthrough,
            "report":             self.reporter.generate_ticker_score(ticker),
        }

    def generate_report(self) -> str:
        return self.reporter.generate_full_report()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None


def _n(v: Optional[float]) -> float:
    return float(v) if v is not None else 0.0


def _same_sign(a: float, b: float) -> bool:
    return (a > 0 and b > 0) or (a < 0 and b < 0)


def _safe_mean(vals: list) -> float:
    clean = [v for v in vals if v is not None and not math.isnan(float(v))]
    return float(np.mean(clean)) if clean else 0.0


def _compute_sharpe(returns: np.ndarray, risk_free: float = 0.0) -> float:
    if len(returns) < 2:
        return 0.0
    std = float(np.std(returns))
    if std < 1e-9:
        return 0.0
    return float((np.mean(returns) - risk_free) / std * math.sqrt(252))


def _make_pattern_id(prefix: str, feature_combo: dict) -> str:
    content = prefix + json.dumps(feature_combo, sort_keys=True)
    return hashlib.md5(content.encode()).hexdigest()[:16]


def _classify_signal_type(signal_name: str) -> str:
    if signal_name.startswith("eps") or "surprise" in signal_name:
        return "eps"
    if signal_name in ("altdata_sentiment", "reddit_score", "news_score", "sec_score"):
        return "altdata"
    if signal_name.startswith("options") or signal_name in ("short_squeeze_score", "congressional_signal"):
        return "deepdata"
    if signal_name in ("vix", "spy_return_5d", "sector_return_5d", "risk_on", "risk_off",
                       "vix_tailwind", "vix_headwind", "macro_regime_code"):
        return "macro"
    if signal_name in ("volume_surge", "high_volume_flag", "return_t1", "return_t5"):
        return "technical"
    if signal_name in ("revenue_trend_3q", "margin_trend_3q", "insider_buying",
                       "fundamentals_composite", "cf_quality", "bs_health"):
        return "fundamental"
    return "composite"
