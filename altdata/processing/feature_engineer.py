import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .normaliser import Normaliser

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Builds the complete, normalised feature vector per ticker per day
    for ML consumption.

    All raw values are sourced from an AltDataStore, normalised via a
    Normaliser (rolling z-score, no lookahead), and clipped to ±3σ.
    Interaction features are computed after normalisation.
    Missing features default to 0.0.
    """

    FEATURE_NAMES: List[str] = [
        # --- Sentiment ---
        "reddit_sentiment_score",
        "reddit_mention_velocity",
        "reddit_coordination_score",
        "stocktwits_bull_ratio",
        "stocktwits_watcher_change_pct",
        "news_composite_score",
        "news_volume_zscore",
        "leadership_change_flag",
        "sec_insider_sentiment",
        "institutional_momentum_score",
        "companies_house_risk_score",
        # --- Macro ---
        "macro_regime",
        "vix_zscore",
        "yield_curve_value",
        "oil_price_zscore",
        "usd_gbp_change_pct",
        "inflation_surprise",
        "sector_fred_sensitivity",
        # --- Alternative ---
        "shipping_pressure_score",
        "shipping_impact_forecast_90d",
        "hiring_momentum_score",
        "hiring_trend_direction",
        "wikipedia_momentum_score",
        "wikipedia_edit_surge_flag",
        "google_interest_zscore",
        "google_breakout_flag",
        "weather_risk_score",
        "weather_deviation_from_seasonal",
        "lunar_phase_encoded",
        "days_to_full_moon",
        "moon_distance_zscore",
        # --- Interaction features ---
        "reddit_x_news_sentiment",
        "insider_x_google_rising",
        "shipping_stress_x_retail_exposure",
        "weather_cold_x_energy_exposure",
        "wikipedia_surge_x_earnings_approaching",
        "lunar_full_x_retail_x_volatility",
    ]

    def __init__(self, config: dict, store, normaliser: Normaliser):
        """
        Parameters
        ----------
        config : dict
        store  : AltDataStore — must expose get_raw_data(ticker, from_ts, to_ts)
        normaliser : Normaliser
        """
        self.config = config
        self.store = store
        self.normaliser = normaliser

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_feature_vector(
        self, ticker: str, market: str, as_of_date: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Pull latest raw data from the store and construct a normalised
        feature vector for `ticker`.

        Parameters
        ----------
        ticker : str
        market : str  e.g. "us" / "uk"
        as_of_date : str, optional
            ISO timestamp acting as the evaluation point.
            Defaults to now().  All data after this point is excluded.

        Returns
        -------
        dict  {feature_name: normalised_float}  for every name in FEATURE_NAMES.
        Any feature with no available data defaults to 0.0.
        """
        as_of = as_of_date or datetime.now().isoformat()
        lookback = (
            datetime.fromisoformat(as_of) - timedelta(hours=48)
        ).isoformat()

        # Fetch raw rows from the store
        try:
            raw = (
                self.store.get_raw_data(ticker, lookback, as_of)
                if hasattr(self.store, "get_raw_data")
                else []
            )
        except Exception as exc:
            logger.warning("FeatureEngineer: store.get_raw_data failed for %s: %s", ticker, exc)
            raw = []

        # Index by data_type -> latest row (by timestamp)
        latest: Dict[str, dict] = {}
        for row in raw:
            dt = row.get("data_type", "")
            if dt not in latest or row.get("timestamp", "") > latest[dt].get("timestamp", ""):
                latest[dt] = row

        def get_val(data_type: str, default: float = 0.0) -> float:
            r = latest.get(data_type)
            if r is None:
                return default
            pv = r.get("processed_value")
            if pv is None:
                pv = r.get("value")
            try:
                return float(pv) if pv is not None else default
            except (TypeError, ValueError):
                return default

        # ------------------------------------------------------------------
        # Raw (unnormalised) feature dict
        # ------------------------------------------------------------------
        raw_features: Dict[str, float] = {
            # Sentiment
            "reddit_sentiment_score":       get_val("reddit_sentiment"),
            "reddit_mention_velocity":      get_val("reddit_velocity"),
            "reddit_coordination_score":    get_val("reddit_coordination"),
            "stocktwits_bull_ratio":        get_val("stocktwits_sentiment"),
            "stocktwits_watcher_change_pct": get_val("stocktwits_watchers"),
            "news_composite_score":         get_val("news_sentiment"),
            "news_volume_zscore":           get_val("news_volume"),
            "leadership_change_flag":       get_val("leadership_change"),
            "sec_insider_sentiment":        get_val("insider_sentiment"),
            "institutional_momentum_score": get_val("institutional_momentum"),
            "companies_house_risk_score":   get_val("companies_house_risk") if market == "uk" else 0.0,
            # Macro
            "macro_regime":                 get_val("macro_regime"),
            "vix_zscore":                   get_val("vix_zscore"),
            "yield_curve_value":            get_val("yield_curve"),
            "oil_price_zscore":             get_val("oil_zscore"),
            "usd_gbp_change_pct":           get_val("fx_usd_gbp"),
            "inflation_surprise":           get_val("inflation_surprise"),
            "sector_fred_sensitivity":      get_val("fred_sector_sensitivity"),
            # Alternative
            "shipping_pressure_score":          get_val("shipping_pressure"),
            "shipping_impact_forecast_90d":     get_val("shipping_pressure") * 0.7,
            "hiring_momentum_score":            get_val("hiring_momentum"),
            "hiring_trend_direction":           get_val("hiring_trend"),
            "wikipedia_momentum_score":         get_val("wikipedia_momentum"),
            "wikipedia_edit_surge_flag":        get_val("wikipedia_edit_surge"),
            "google_interest_zscore":           get_val("google_interest"),
            "google_breakout_flag":             get_val("google_breakout"),
            "weather_risk_score":               get_val("weather_impact"),
            "weather_deviation_from_seasonal":  get_val("weather_deviation"),
            "lunar_phase_encoded":              get_val("lunar_cycle"),
            "days_to_full_moon":                get_val("lunar_days_to_full"),
            "moon_distance_zscore":             get_val("lunar_distance"),
        }

        # ------------------------------------------------------------------
        # Interaction features (computed from raw values before normalisation)
        # ------------------------------------------------------------------
        raw_features["reddit_x_news_sentiment"] = (
            raw_features["reddit_sentiment_score"]
            * raw_features["news_composite_score"]
        )
        raw_features["insider_x_google_rising"] = (
            raw_features["sec_insider_sentiment"]
            * max(raw_features["google_interest_zscore"], 0.0)
        )
        # Shipping stress (negative shipping = stress) × retail exposure
        raw_features["shipping_stress_x_retail_exposure"] = (
            min(raw_features["shipping_pressure_score"], 0.0) * -1.0
        )
        raw_features["weather_cold_x_energy_exposure"] = (
            raw_features["weather_risk_score"]
            * raw_features["sector_fred_sensitivity"]
        )
        raw_features["wikipedia_surge_x_earnings_approaching"] = (
            raw_features["wikipedia_momentum_score"]
            * raw_features["wikipedia_edit_surge_flag"]
        )
        raw_features["lunar_full_x_retail_x_volatility"] = (
            raw_features["lunar_phase_encoded"]
            * raw_features["vix_zscore"]
            * 0.1
        )

        # ------------------------------------------------------------------
        # Normalise and clip each feature
        # ------------------------------------------------------------------
        normalised: Dict[str, float] = {}
        for fname in self.FEATURE_NAMES:
            fval = raw_features.get(fname, 0.0)
            z = self.normaliser.fit_transform(f"{ticker}_{fname}", fval)
            normalised[fname] = self.normaliser.clip_outliers(z)

        return normalised

    def build_feature_matrix(self, tickers: List[str], market: str) -> pd.DataFrame:
        """
        Build feature matrix for all tickers.

        Returns
        -------
        pd.DataFrame  with tickers as the index and FEATURE_NAMES as columns.
        """
        rows: List[Dict[str, float]] = []
        for ticker in tickers:
            try:
                vec = self.build_feature_vector(ticker, market)
            except Exception as exc:
                logger.error("build_feature_matrix: failed for %s: %s", ticker, exc)
                vec = {fname: 0.0 for fname in self.FEATURE_NAMES}
            vec["ticker"] = ticker
            rows.append(vec)

        if not rows:
            return pd.DataFrame(columns=["ticker"] + self.FEATURE_NAMES).set_index("ticker")

        return pd.DataFrame(rows).set_index("ticker")
