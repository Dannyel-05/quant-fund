"""Short squeeze scoring with three-layer analysis."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata/short_interest")


class SqueezeScorer:
    """Three-layer short squeeze analysis: flag, score, ML probability."""

    # Layer 1 thresholds
    SHORT_FLOAT_FLAG_PCT = 0.20       # 20%
    DTC_FLAG = 10.0                    # days
    BORROW_RATE_FLAG = 0.30           # 30%
    SI_INCREASE_FLAG = 0.20           # 20% increase

    # Layer 2 normalisation
    SHORT_FLOAT_NORM = 0.30
    DTC_NORM = 15.0
    VOLUME_SURGE_THRESHOLD = 1.5
    CATALYST_NEWS_THRESHOLD = 0.5

    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def score(
        self,
        ticker: str,
        short_data: dict,
        price_data,
        options_data: dict = None,
    ) -> dict:
        """
        Returns squeeze scoring dict with flagged, squeeze_score, layers, signal.
        """
        try:
            l1_flags = self.layer1_flag(short_data)
        except Exception as exc:
            logger.warning("layer1_flag(%s): %s", ticker, exc)
            l1_flags = []

        try:
            l2 = self.layer2_score(ticker, short_data, price_data, options_data)
        except Exception as exc:
            logger.warning("layer2_score(%s): %s", ticker, exc)
            l2 = 0.0

        try:
            l3_prob = self.layer3_predict(ticker, {
                "short_data": short_data,
                "price_data": price_data,
                "options_data": options_data,
            })
        except Exception as exc:
            logger.warning("layer3_predict(%s): %s", ticker, exc)
            l3_prob = None

        # Combine layers
        base_score = l2
        if l3_prob is not None:
            # Blend layer2 (70%) and ML (30%)
            squeeze_score = base_score * 0.7 + l3_prob * 100 * 0.3
        else:
            squeeze_score = base_score

        # Boost if layer1 flags
        flag_boost = min(len(l1_flags) * 5, 20)
        squeeze_score = min(squeeze_score + flag_boost, 100.0)

        flagged = len(l1_flags) > 0 or squeeze_score >= 50

        if squeeze_score >= 80:
            signal = "EXTREME"
        elif squeeze_score >= 60:
            signal = "HIGH"
        elif squeeze_score >= 35:
            signal = "MODERATE"
        else:
            signal = "LOW"

        return {
            "ticker": ticker,
            "flagged": bool(flagged),
            "squeeze_score": float(squeeze_score),
            "layer1_flags": l1_flags,
            "layer2_score": float(l2),
            "layer3_probability": float(l3_prob) if l3_prob is not None else None,
            "signal": signal,
            "components": _extract_components(short_data, price_data, options_data),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def layer1_flag(self, short_data: dict) -> list:
        """Flag immediate red flags for squeeze conditions."""
        flags = []
        if not short_data:
            return flags

        short_float = short_data.get("short_float_pct")
        if short_float is not None:
            if isinstance(short_float, float) and short_float > 1:
                short_float = short_float / 100  # normalize if in percent form
            if short_float is not None and float(short_float) > self.SHORT_FLOAT_FLAG_PCT:
                flags.append({
                    "flag": "HIGH_SHORT_FLOAT",
                    "value": float(short_float),
                    "threshold": self.SHORT_FLOAT_FLAG_PCT,
                })

        dtc = short_data.get("days_to_cover")
        if dtc is not None and float(dtc) > self.DTC_FLAG:
            flags.append({
                "flag": "HIGH_DAYS_TO_COVER",
                "value": float(dtc),
                "threshold": self.DTC_FLAG,
            })

        borrow_rate = short_data.get("borrow_rate")
        if borrow_rate is not None and float(borrow_rate) > self.BORROW_RATE_FLAG:
            flags.append({
                "flag": "HIGH_BORROW_RATE",
                "value": float(borrow_rate),
                "threshold": self.BORROW_RATE_FLAG,
            })

        si_change = short_data.get("change_from_prev")
        if si_change is not None and float(si_change) > self.SI_INCREASE_FLAG:
            flags.append({
                "flag": "RAPID_SI_INCREASE",
                "value": float(si_change),
                "threshold": self.SI_INCREASE_FLAG,
            })

        return flags

    def layer2_score(
        self,
        ticker: str,
        short_data: dict,
        price_data,
        options_data: dict,
    ) -> float:
        """Weighted component score 0-100."""
        components = {}

        # Short float score (max 25)
        short_float = short_data.get("short_float_pct", 0) or 0
        if isinstance(short_float, float) and short_float > 1:
            short_float = short_float / 100
        short_float_score = min(float(short_float) / self.SHORT_FLOAT_NORM, 1.0) * 25
        components["short_float_score"] = short_float_score

        # Days to cover score (max 25)
        dtc = short_data.get("days_to_cover", 0) or 0
        dtc_score = min(float(dtc) / self.DTC_NORM, 1.0) * 25
        components["dtc_score"] = dtc_score

        # Momentum score (max 20)
        momentum_20d = _calc_momentum(price_data, 20)
        momentum_score = 20.0 if momentum_20d is not None and momentum_20d > 0 else 0.0
        components["momentum_score"] = momentum_score

        # Volume surge score (max 15)
        volume_surge = _calc_volume_surge(price_data, 5)
        volume_score = 15.0 if volume_surge is not None and volume_surge > self.VOLUME_SURGE_THRESHOLD else 0.0
        components["volume_score"] = volume_score

        # Catalyst score (max 15)
        earnings_near = _check_earnings_within_30d(ticker)
        news_score = _get_news_score(options_data)
        catalyst_score = 15.0 if (earnings_near or news_score > self.CATALYST_NEWS_THRESHOLD) else 0.0
        components["catalyst_score"] = catalyst_score

        total = sum(components.values())
        return float(np.clip(total, 0.0, 100.0))

    def layer3_predict(self, ticker: str, historical_data: dict) -> float:
        """ML prediction if >= 5 historical squeezes available. Else return None."""
        try:
            from deepdata.short_interest.squeeze_predictor import SqueezePredictor
            predictor = SqueezePredictor(self.config)
            price_data = historical_data.get("price_data")
            short_data_history = [historical_data.get("short_data", {})]
            squeezes = predictor.identify_historical_squeezes(price_data, short_data_history)
            if len(squeezes) < 5:
                return None
            features = predictor.build_features(
                ticker=ticker,
                date=datetime.utcnow().date(),
                short_data=historical_data.get("short_data", {}),
                price_data=price_data,
                options_data=historical_data.get("options_data"),
            )
            return predictor.predict(ticker, features)
        except Exception as exc:
            logger.warning("layer3_predict(%s): %s", ticker, exc)
            return None


# --- helpers ---

def _extract_components(short_data, price_data, options_data) -> dict:
    return {
        "short_float_pct": short_data.get("short_float_pct") if short_data else None,
        "days_to_cover": short_data.get("days_to_cover") if short_data else None,
        "borrow_rate": short_data.get("borrow_rate") if short_data else None,
        "momentum_20d": _calc_momentum(price_data, 20),
        "volume_surge_5d": _calc_volume_surge(price_data, 5),
        "iv_rank": options_data.get("iv_rank") if options_data else None,
        "pc_ratio": options_data.get("put_call_ratio") if options_data else None,
    }


def _to_df(price_data) -> pd.DataFrame:
    """Coerce price_data to DataFrame."""
    if price_data is None:
        return pd.DataFrame()
    if isinstance(price_data, pd.DataFrame):
        return price_data
    try:
        return pd.DataFrame(price_data)
    except Exception:
        return pd.DataFrame()


def _calc_momentum(price_data, n: int = 20):
    """Return n-day return."""
    df = _to_df(price_data)
    if df.empty:
        return None
    col = next((c for c in ["Close", "close", "price"] if c in df.columns), None)
    if col is None or len(df) < n:
        return None
    try:
        return float((df[col].iloc[-1] / df[col].iloc[-n]) - 1)
    except Exception:
        return None


def _calc_volume_surge(price_data, n: int = 5):
    """Return ratio of recent avg volume to baseline avg volume."""
    df = _to_df(price_data)
    if df.empty:
        return None
    vol_col = next((c for c in ["Volume", "volume"] if c in df.columns), None)
    if vol_col is None or len(df) < n + 10:
        return None
    try:
        recent_avg = df[vol_col].iloc[-n:].mean()
        base_avg = df[vol_col].iloc[-(n + 20):-n].mean()
        if base_avg == 0:
            return None
        return float(recent_avg / base_avg)
    except Exception:
        return None


def _check_earnings_within_30d(ticker: str) -> bool:
    """Check if earnings are within 30 days using yfinance."""
    if yf is None:
        return False
    try:
        cal = yf.Ticker(ticker).calendar
        if cal is None or cal.empty:
            return False
        if "Earnings Date" in cal.index:
            earnings_date = cal.loc["Earnings Date"].iloc[0]
            if hasattr(earnings_date, "date"):
                earnings_date = earnings_date.date()
            today = datetime.utcnow().date()
            delta = (earnings_date - today).days
            return 0 <= delta <= 30
    except Exception:
        pass
    return False


def _get_news_score(options_data: dict) -> float:
    """Extract news score from options_data dict if available."""
    if not options_data:
        return 0.0
    return float(options_data.get("news_score", 0.0) or 0.0)
