import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class AltDataSignalEngine:
    """
    Generates independent trade signals with full provenance, combining:
      - Online ML predictions (AdaptiveRandomForest + LogisticRegression ensemble)
      - Multi-source confluence scoring
      - Information cascade stage gating
      - Macro regime multipliers
      - PEAD position modifier logic (BOOST / REDUCE / ABORT)
    """

    SIGNAL_TYPES = [
        "ALT_LONG",
        "ALT_SHORT",
        "PEAD_BOOST",
        "PEAD_REDUCE",
        "PEAD_ABORT",
    ]

    def __init__(
        self,
        config: dict,
        store,
        feature_engineer=None,
        online_learner=None,
        custom_metrics=None,
        validator=None,
    ):
        self.config = config
        self.store = store
        self.fe = feature_engineer
        self.learner = online_learner       # optional
        self.metrics = custom_metrics or {}
        self.validator = validator          # optional

        sig_cfg = config.get("altdata", {}).get("signals", {})
        self.min_confidence: float = sig_cfg.get("min_confidence", 0.60)
        self.min_confluence: float = 0.6

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, tickers: List[str], market: str = "us") -> List[Dict]:
        """Alias for generate_signals() for backward compatibility."""
        return self.generate_signals(tickers, market)

    def generate_signals(
        self,
        tickers: List[str],
        market: str,
        active_pead_positions: Dict[str, dict] = None,
    ) -> List[Dict]:
        """
        Generate signals for all tickers.

        Parameters
        ----------
        tickers               : list of ticker strings
        market                : market identifier (e.g. "US", "UK")
        active_pead_positions : {ticker: {"direction": +1/-1, "entry_date": str}}

        Returns list of signal dicts with full provenance, one per qualifying ticker.
        Each signal is also persisted via store.log_signal().
        """
        signals: List[Dict] = []
        pead_positions = active_pead_positions or {}

        for ticker in tickers:
            try:
                signal = self._process_ticker(ticker, market, pead_positions)
                if signal:
                    signals.append(signal)
                    self.store.log_signal(signal)
            except Exception as e:
                logger.error(
                    "Signal generation failed for %s: %s", ticker, e
                )

        logger.info(
            "generate_signals: %d/%d tickers produced signals [%s]",
            len(signals),
            len(tickers),
            market,
        )
        return signals

    # ------------------------------------------------------------------
    # Per-ticker processing
    # ------------------------------------------------------------------

    def _process_ticker(
        self,
        ticker: str,
        market: str,
        pead_positions: Dict,
    ) -> Optional[Dict]:
        """Build and gate a single ticker signal. Returns None if not actionable."""

        # 1. Build feature vector ------------------------------------
        if self.fe is None:
            return None
        features = self.fe.build_feature_vector(ticker, market)

        # 2. Extract per-source signals (with freshness decay applied
        #    inside feature_engineer if available)
        source_signals = self._extract_source_signals(features, ticker)

        # 3. Online learner prediction --------------------------------
        if self.learner is None:
            return None
        direction, ml_confidence = self.learner.predict(features)
        if direction == 0:
            return None  # model not warm enough yet

        # 4. Multi-source confluence score ----------------------------
        source_values = {
            k: v for k, v in source_signals.items() if abs(v) > 0.01
        }
        if self.metrics and hasattr(self.metrics, "altdata_confluence_score"):
            confluence = self.metrics.altdata_confluence_score(source_values)
        else:
            confluence = sum(source_values.values()) / max(len(source_values), 1)

        # 5. Information cascade stage --------------------------------
        activation_times = self._get_activation_times(ticker, source_signals)
        if self.metrics and hasattr(self.metrics, "information_cascade_stage"):
            cascade_stage = self.metrics.information_cascade_stage(activation_times)
        else:
            cascade_stage = 0.0

        # 6. Macro regime multiplier ----------------------------------
        macro_regime = int(features.get("macro_regime", 2))
        # (long_mult, short_mult) per regime
        regime_multipliers = {
            0: (1.2, 0.9),
            1: (1.0, 1.0),
            2: (1.0, 1.0),
            3: (0.7, 1.2),
            4: (0.5, 0.5),
        }
        long_mult, short_mult = regime_multipliers.get(macro_regime, (1.0, 1.0))
        regime_mult = long_mult if direction > 0 else short_mult

        final_confidence = min(ml_confidence * regime_mult, 1.0)

        # 7. Gate checks ----------------------------------------------
        if final_confidence < self.min_confidence:
            logger.debug(
                "%s: confidence %.3f below threshold %.3f, skipping",
                ticker, final_confidence, self.min_confidence,
            )
            return None

        if abs(confluence) < self.min_confluence:
            logger.debug(
                "%s: confluence %.3f below threshold %.3f, skipping",
                ticker, abs(confluence), self.min_confluence,
            )
            return None

        if cascade_stage > 0.75:
            logger.debug(
                "%s: cascade too late (%.2f), skipping", ticker, cascade_stage
            )
            return None

        # 8. Determine signal type ------------------------------------
        if ticker in pead_positions:
            pead_dir = pead_positions[ticker].get("direction", 0)
            if pead_dir == direction:
                signal_type = "PEAD_BOOST"
            elif final_confidence > 0.80:
                signal_type = "PEAD_ABORT"   # strong contradiction
            else:
                signal_type = "PEAD_REDUCE"
        else:
            signal_type = "ALT_LONG" if direction > 0 else "ALT_SHORT"

        # 9. Expected holding period based on dominant source ---------
        dominant_source = (
            max(source_values, key=lambda k: abs(source_values[k]))
            if source_values
            else "news"
        )
        hold_days_by_source = {
            "sec_edgar": 15,
            "news": 3,
            "shipping": 30,
            "jobs": 20,
            "wikipedia": 5,
            "reddit": 2,
            "stocktwits": 1,
            "google": 5,
            "lunar": 7,
            "weather": 7,
        }
        expected_holding = hold_days_by_source.get(dominant_source, 5)

        return {
            "ticker": ticker,
            "market": market,
            "signal_type": signal_type,
            "direction": direction,
            "confidence": round(final_confidence, 4),
            "strength": round(abs(confluence), 4),
            "confluence_score": round(confluence, 4),
            "cascade_stage": round(cascade_stage, 3),
            "macro_regime": macro_regime,
            "sources_used": source_values,
            "dominant_source": dominant_source,
            "expected_holding_days": expected_holding,
            "model_version": "online_learner_current",
            "timestamp": datetime.now().isoformat(),
            "ml_confidence": round(ml_confidence, 4),
            "regime_multiplier": round(regime_mult, 3),
        }

    # ------------------------------------------------------------------
    # Feature helpers
    # ------------------------------------------------------------------

    def _extract_source_signals(
        self, features: Dict, ticker: str
    ) -> Dict[str, float]:
        """Map flat feature dict to per-source signal values."""
        return {
            "reddit": float(features.get("reddit_sentiment_score", 0)),
            "stocktwits": float(features.get("stocktwits_bull_ratio", 0)),
            "news": float(features.get("news_composite_score", 0)),
            "sec_edgar": float(features.get("sec_insider_sentiment", 0)),
            "shipping": float(features.get("shipping_pressure_score", 0)),
            "jobs": float(features.get("hiring_momentum_score", 0)),
            "wikipedia": float(features.get("wikipedia_momentum_score", 0)),
            "google": float(features.get("google_interest_zscore", 0)),
            "weather": float(features.get("weather_risk_score", 0)),
            "lunar": float(features.get("lunar_phase_encoded", 0)),
        }

    def _get_activation_times(
        self, ticker: str, source_signals: Dict
    ) -> Dict[str, Optional[str]]:
        """
        Return per-source activation timestamps.
        A source is considered activated if its absolute signal value > 0.1.
        """
        now = datetime.now().isoformat()
        return {
            k: now if abs(v) > 0.1 else None
            for k, v in source_signals.items()
        }
