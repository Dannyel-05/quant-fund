"""DeepDataSignalEngine — integrates all deepdata signals."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------
# Tier definitions
# ------------------------------------------------------------------

TIER_WEIGHTS = {1: 1.5, 2: 1.0, 3: 0.5}

TIER_1_SIGNALS = [
    "congressional_cluster",
    "options_institutional_sweep",
    "supply_chain_readthrough",
    "transcript_deflection_short_interest",
]

TIER_2_SIGNALS = [
    "squeeze_prediction_extreme",
    "revenue_driven_beat",
    "patent_hiring_surge",
    "dark_pool_options_call",
]

TIER_3_SIGNALS = [
    "factor_exposure",
    "congressional_single_low_credibility",
    "microstructure_liquidity",
    "nonsense_pattern",
]

# Source/data_type -> tier mapping for classification
TIER_1_DATA_TYPES = {
    "congressional_cluster",
    "options_sweep",
    "supply_chain_readthrough",
    "transcript_deflection",
}
TIER_2_DATA_TYPES = {
    "short_squeeze_prediction",
    "earnings_beat_quality",
    "patent_velocity",
    "dark_pool_accumulation",
}
TIER_3_DATA_TYPES = {
    "factor_mispricing",
    "congressional_single",
    "microstructure_spread",
    "nonsense_pattern",
}


class DeepDataSignalEngine:
    """Integrates all deepdata signals into ranked, tiered output."""

    def __init__(self, config: dict, store=None, notifier=None):
        self.config = config or {}
        self.store = store
        self.notifier = notifier
        self.min_quality_score = self.config.get("min_quality_score", 0.3)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, tickers: list, all_module_results: dict) -> list:
        """
        all_module_results: {module: list_of_CollectorResults}
        Returns list of signal dicts.
        """
        if not tickers or not all_module_results:
            return []
        signals = []
        try:
            # Build per-ticker buckets
            ticker_results = self._bucket_by_ticker(all_module_results, tickers)

            for ticker in tickers:
                try:
                    results = ticker_results.get(ticker, [])
                    if not results:
                        continue

                    confluence_info = self.calc_confluence(ticker, all_module_results)
                    pead_modifier = self.calc_pead_modifier(ticker, all_module_results)

                    for result in results:
                        try:
                            quality = float(result.get("quality_score", 0.0))
                            if quality < self.min_quality_score:
                                continue

                            tier = self.classify_tier(result)
                            direction = self._infer_direction(result)
                            confidence = float(result.get("quality_score", 0.5)) * confluence_info.get("deepdata_confluence", 0.5)

                            signal = {
                                "ticker": ticker,
                                "signal_type": result.get("data_type", "unknown"),
                                "direction": direction,
                                "confidence": min(1.0, confidence),
                                "tier": tier,
                                "deepdata_confluence": confluence_info.get("deepdata_confluence", 0.0),
                                "sources": [result.get("source", "unknown")],
                                "pead_modifier": pead_modifier,
                                "raw_result": result,
                                "timestamp": _now_iso(),
                            }
                            signals.append(signal)
                            self.log_signal(signal)

                            if self.should_notify_immediately(signal):
                                self._notify(signal)
                        except Exception as exc:
                            logger.warning("generate: signal build failed for %s: %s", ticker, exc)
                except Exception as exc:
                    logger.warning("generate: ticker loop failed for %s: %s", ticker, exc)

            # Sort by tier then confidence
            signals.sort(key=lambda x: (x["tier"], -x["confidence"]))
            return signals
        except Exception as exc:
            logger.warning("DeepDataSignalEngine.generate failed: %s", exc)
            return []

    def calc_confluence(self, ticker: str, all_results: dict) -> dict:
        """
        DeepDataConfluence = weighted_sum(tier1 * 1.5, tier2 * 1.0, tier3 * 0.5) / normaliser
        """
        try:
            weighted_sum = 0.0
            max_possible = 0.0
            counts = {1: 0, 2: 0, 3: 0}

            for module, results in all_results.items():
                for r in results:
                    if r.get("ticker") != ticker:
                        continue
                    tier = self.classify_tier(r)
                    weight = TIER_WEIGHTS.get(tier, 0.5)
                    quality = float(r.get("quality_score", 0.5))
                    weighted_sum += weight * quality
                    max_possible += weight
                    counts[tier] = counts.get(tier, 0) + 1

            deepdata_confluence = (weighted_sum / max_possible) if max_possible > 0 else 0.0
            deepdata_confluence = min(1.0, deepdata_confluence)

            # TotalConfluence: blend with altdata if available (placeholder 0.5 weight)
            altdata_confluence = self._get_altdata_confluence(ticker, all_results)
            total_confluence = deepdata_confluence * 0.5 + altdata_confluence * 0.5

            return {
                "deepdata_confluence": deepdata_confluence,
                "altdata_confluence": altdata_confluence,
                "total_confluence": total_confluence,
                "tier_counts": counts,
            }
        except Exception as exc:
            logger.warning("calc_confluence failed for %s: %s", ticker, exc)
            return {"deepdata_confluence": 0.0, "altdata_confluence": 0.0, "total_confluence": 0.0}

    def classify_tier(self, result: dict) -> int:
        """Classify a CollectorResult into tier 1, 2, or 3."""
        try:
            data_type = result.get("data_type", "")
            source = result.get("source", "")

            if data_type in TIER_1_DATA_TYPES or any(t in data_type for t in TIER_1_SIGNALS):
                return 1
            if data_type in TIER_2_DATA_TYPES or any(t in data_type for t in TIER_2_SIGNALS):
                return 2
            # Check signal_type field if present
            signal_type = result.get("signal_type", "")
            if signal_type in TIER_1_SIGNALS:
                return 1
            if signal_type in TIER_2_SIGNALS:
                return 2
            return 3
        except Exception:
            return 3

    def calc_pead_modifier(self, ticker: str, all_results: dict) -> float:
        """
        TotalConfluence > 0.7: max position (1.5)
        TotalConfluence > 0.5: standard position (1.0)
        TotalConfluence < 0.3: suppress PEAD (0.0)
        """
        try:
            confluence = self.calc_confluence(ticker, all_results)
            total = confluence.get("total_confluence", confluence.get("deepdata_confluence", 0.0))
            if total > 0.7:
                return 1.5
            if total > 0.5:
                return 1.0
            if total < 0.3:
                return 0.0
            return 1.0
        except Exception as exc:
            logger.warning("calc_pead_modifier failed for %s: %s", ticker, exc)
            return 1.0

    def should_notify_immediately(self, signal: dict) -> bool:
        """Always notify on Tier 1 signals regardless of other filters."""
        return signal.get("tier") == 1

    def log_signal(self, signal: dict) -> None:
        """Log to store if available."""
        if self.store is not None:
            try:
                if hasattr(self.store, "log_signal"):
                    self.store.log_signal(signal)
                elif hasattr(self.store, "save"):
                    self.store.save("signals", signal)
            except Exception as exc:
                logger.warning("log_signal store write failed: %s", exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _bucket_by_ticker(self, all_results: dict, tickers: list) -> dict:
        """Group all CollectorResults by ticker."""
        bucket = {t: [] for t in tickers}
        for module, results in all_results.items():
            for r in results:
                t = r.get("ticker")
                if t in bucket:
                    bucket[t].append(r)
        return bucket

    def _infer_direction(self, result: dict) -> int:
        """Infer trade direction from CollectorResult."""
        try:
            raw = result.get("raw_data", {})
            if isinstance(raw, dict):
                direction = raw.get("direction")
                if direction is not None:
                    return int(direction)
            value = float(result.get("value", 0.0))
            return 1 if value >= 0 else -1
        except Exception:
            return 1

    def _get_altdata_confluence(self, ticker: str, all_results: dict) -> float:
        """Extract altdata confluence score if available in results."""
        try:
            for module, results in all_results.items():
                if "altdata" not in module.lower():
                    continue
                for r in results:
                    if r.get("ticker") == ticker:
                        raw = r.get("raw_data", {})
                        if isinstance(raw, dict):
                            cs = raw.get("confluence_score") or raw.get("altdata_confluence")
                            if cs is not None:
                                return min(1.0, float(cs))
        except Exception:
            pass
        return 0.0

    def _notify(self, signal: dict) -> None:
        """Send immediate notification via notifier if available."""
        if self.notifier is not None:
            try:
                if hasattr(self.notifier, "send"):
                    msg = (
                        f"TIER 1 SIGNAL: {signal['ticker']} | "
                        f"type={signal['signal_type']} | "
                        f"confidence={signal['confidence']:.2f} | "
                        f"confluence={signal['deepdata_confluence']:.2f}"
                    )
                    self.notifier.send(msg)
            except Exception as exc:
                logger.warning("_notify failed: %s", exc)
