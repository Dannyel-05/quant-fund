"""ML-based short squeeze predictor using RandomForest."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    import joblib
    sklearn_available = True
except ImportError:
    sklearn_available = False
    RandomForestClassifier = None
    StandardScaler = None
    Pipeline = None
    joblib = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata/short_interest")

FEATURE_NAMES = [
    "short_float",
    "dtc",
    "borrow_rate",
    "si_change_1p",
    "si_change_3p",
    "momentum_5d",
    "momentum_20d",
    "volume_ratio_5d",
    "vol_surge",
    "iv_rank",
    "pc_ratio",
    "news_score",
    "reddit_sentiment",
    "days_to_earnings",
]


class SqueezePredictor:
    """ML-based short squeeze probability predictor."""

    SQUEEZE_PRICE_GAIN_THRESHOLD = 0.30  # 30% gain
    SQUEEZE_DAYS = 10
    SQUEEZE_PRE_SHORT_FLOAT = 0.20  # 20%
    MIN_TRAINING_EVENTS = 5

    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.is_trained = False
        self.model_path = CACHE_DIR / "squeeze_predictor_model.joblib"
        self._try_load_model()

    def _try_load_model(self):
        """Attempt to load a previously saved model."""
        if joblib is None or not self.model_path.exists():
            return
        try:
            self.model = joblib.load(self.model_path)
            self.is_trained = True
            logger.info("Loaded squeeze predictor model from %s", self.model_path)
        except Exception as exc:
            logger.warning("Could not load model: %s", exc)

    def train(self, historical_squeeze_events: list, feature_matrix) -> None:
        """Train RandomForest on 14-day pre-squeeze features. Require >= 5 events."""
        if not sklearn_available:
            logger.warning("scikit-learn not installed; cannot train SqueezePredictor")
            return

        if len(historical_squeeze_events) < self.MIN_TRAINING_EVENTS:
            logger.warning(
                "Only %d squeeze events; need >= %d to train",
                len(historical_squeeze_events),
                self.MIN_TRAINING_EVENTS,
            )
            return

        try:
            if isinstance(feature_matrix, pd.DataFrame):
                X = feature_matrix[FEATURE_NAMES].fillna(0).values
            else:
                X = np.array(feature_matrix, dtype=float)
                X = np.nan_to_num(X, nan=0.0)

            y = np.array([int(e.get("is_squeeze", 1)) for e in historical_squeeze_events])

            if len(X) != len(y):
                logger.warning("Feature matrix rows (%d) != labels (%d)", len(X), len(y))
                return

            pipeline = Pipeline([
                ("scaler", StandardScaler()),
                ("clf", RandomForestClassifier(
                    n_estimators=100,
                    max_depth=5,
                    min_samples_leaf=3,
                    random_state=42,
                    class_weight="balanced",
                )),
            ])
            pipeline.fit(X, y)
            self.model = pipeline
            self.is_trained = True
            logger.info("SqueezePredictor trained on %d samples", len(X))

            try:
                if joblib is not None:
                    joblib.dump(pipeline, self.model_path)
                    logger.info("Model saved to %s", self.model_path)
            except Exception as exc:
                logger.warning("Could not save model: %s", exc)

        except Exception as exc:
            logger.warning("SqueezePredictor.train failed: %s", exc)

    def predict(self, ticker: str, current_features: dict) -> float:
        """Return probability of squeeze in next 30 days. Return None if not trained."""
        if not self.is_trained or self.model is None:
            return None
        if not sklearn_available:
            return None

        try:
            feature_vec = np.array(
                [float(current_features.get(f, 0) or 0) for f in FEATURE_NAMES],
                dtype=float,
            )
            feature_vec = np.nan_to_num(feature_vec, nan=0.0)
            X = feature_vec.reshape(1, -1)
            prob = self.model.predict_proba(X)[0]
            # Return probability of squeeze class (class 1)
            classes = list(self.model.classes_) if hasattr(self.model, "classes_") else [0, 1]
            if 1 in classes:
                return float(prob[classes.index(1)])
            return float(prob[-1])
        except Exception as exc:
            logger.warning("SqueezePredictor.predict(%s): %s", ticker, exc)
            return None

    def identify_historical_squeezes(self, price_data, short_data_history: list) -> list:
        """
        Define squeeze: price up > 30% in 10 days AND short interest was > 20% before.
        Returns list of squeeze events with features.
        """
        squeezes = []
        df = _to_df(price_data)
        if df.empty or len(df) < self.SQUEEZE_DAYS + 5:
            return squeezes

        close_col = next((c for c in ["Close", "close"] if c in df.columns), None)
        if close_col is None:
            return squeezes

        closes = df[close_col].values
        dates = df.index.tolist() if hasattr(df.index, "tolist") else list(range(len(df)))

        # Build short data lookup by date
        short_lookup = {}
        for item in (short_data_history or []):
            d = item.get("date")
            if d:
                short_lookup[str(d)] = item

        for i in range(len(closes) - self.SQUEEZE_DAYS):
            start_price = closes[i]
            if start_price <= 0:
                continue
            end_price = closes[i + self.SQUEEZE_DAYS]
            gain = (end_price - start_price) / start_price

            if gain >= self.SQUEEZE_PRICE_GAIN_THRESHOLD:
                # Check pre-squeeze short interest
                start_date = dates[i]
                short_item = short_lookup.get(str(start_date), {})
                pre_short_float = short_item.get("short_float_pct", 0) or 0
                if isinstance(pre_short_float, float) and pre_short_float > 1:
                    pre_short_float /= 100

                if float(pre_short_float) >= self.SQUEEZE_PRE_SHORT_FLOAT or not short_lookup:
                    pre_features = self.build_features(
                        ticker="",
                        date=start_date,
                        short_data=short_item,
                        price_data=df.iloc[max(0, i - 30): i + 1],
                    )
                    squeezes.append({
                        "date": str(start_date),
                        "peak_gain": float(gain),
                        "days_duration": self.SQUEEZE_DAYS,
                        "pre_squeeze_features": pre_features,
                        "is_squeeze": 1,
                    })

        return squeezes

    def build_features(
        self,
        ticker: str,
        date,
        short_data: dict,
        price_data,
        options_data: dict = None,
    ) -> dict:
        """Build the 14-feature vector for a given observation."""
        features = {}
        sd = short_data or {}

        # Short float (normalize to 0-1 if in percent)
        sf = float(sd.get("short_float_pct", 0) or 0)
        if sf > 1:
            sf /= 100
        features["short_float"] = sf

        features["dtc"] = float(sd.get("days_to_cover", 0) or 0)
        features["borrow_rate"] = float(sd.get("borrow_rate", 0) or 0)
        features["si_change_1p"] = float(sd.get("change_from_prev", 0) or 0)
        features["si_change_3p"] = float(sd.get("change_3period", 0) or 0)

        df = _to_df(price_data)
        close_col = next((c for c in ["Close", "close"] if c in df.columns), None) if not df.empty else None
        vol_col = next((c for c in ["Volume", "volume"] if c in df.columns), None) if not df.empty else None

        features["momentum_5d"] = _momentum(df, close_col, 5)
        features["momentum_20d"] = _momentum(df, close_col, 20)
        features["volume_ratio_5d"] = _volume_ratio(df, vol_col, 5, 20)
        features["vol_surge"] = _volume_ratio(df, vol_col, 3, 10)

        od = options_data or {}
        features["iv_rank"] = float(od.get("iv_rank", 0) or 0)
        features["pc_ratio"] = float(od.get("put_call_ratio", 1) or 1)
        features["news_score"] = float(od.get("news_score", 0) or 0)
        features["reddit_sentiment"] = float(od.get("reddit_sentiment", 0) or 0)

        # Days to earnings
        try:
            import yfinance as yf_inner
            cal = yf_inner.Ticker(ticker).calendar if ticker else None
            if cal is not None and not cal.empty and "Earnings Date" in cal.index:
                ed = cal.loc["Earnings Date"].iloc[0]
                if hasattr(ed, "date"):
                    ed = ed.date()
                today = datetime.utcnow().date()
                features["days_to_earnings"] = max(0, (ed - today).days)
            else:
                features["days_to_earnings"] = 999
        except Exception:
            features["days_to_earnings"] = 999

        return features


# --- helpers ---

def _to_df(price_data) -> pd.DataFrame:
    if price_data is None:
        return pd.DataFrame()
    if isinstance(price_data, pd.DataFrame):
        return price_data
    try:
        return pd.DataFrame(price_data)
    except Exception:
        return pd.DataFrame()


def _momentum(df, col, n) -> float:
    if col is None or df.empty or len(df) < n:
        return 0.0
    try:
        return float((df[col].iloc[-1] / df[col].iloc[-n]) - 1)
    except Exception:
        return 0.0


def _volume_ratio(df, col, recent_n, base_n) -> float:
    if col is None or df.empty or len(df) < recent_n + base_n:
        return 1.0
    try:
        r = df[col].iloc[-recent_n:].mean()
        b = df[col].iloc[-(recent_n + base_n):-recent_n].mean()
        return float(r / b) if b > 0 else 1.0
    except Exception:
        return 1.0
