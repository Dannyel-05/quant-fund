"""FactorExposureMapper — computes factor loadings for all stocks."""

import logging
import math
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
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rank_normalize(series) -> "pd.Series":
    """Cross-sectionally rank-normalize to [-1, +1]."""
    if not HAS_PANDAS:
        return series
    ranked = series.rank(pct=True)  # 0..1
    return (ranked - 0.5) * 2.0    # -1..+1


class FactorExposureMapper:
    """Computes factor loadings for all stocks."""

    FACTOR_NAMES = [
        "momentum",
        "value",
        "quality",
        "size",
        "volatility",
        "earnings_quality",
        "altdata",
        "supply_chain",
        "congressional",
    ]

    def __init__(self, config: dict):
        self.config = config or {}
        self.momentum_long = self.config.get("momentum_long_days", 252)
        self.momentum_short = self.config.get("momentum_short_days", 21)
        self.vol_window = self.config.get("vol_window_days", 60)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_all(
        self,
        tickers: list,
        price_data: dict,
        fundamental_data: dict = None,
    ) -> dict:
        """
        Returns factor exposure matrix: {ticker: {factor_name: loading}}
        """
        if not tickers:
            return {}
        fundamental_data = fundamental_data or {}
        exposures = {}
        for ticker in tickers:
            try:
                prices = price_data.get(ticker) if price_data else None
                info = fundamental_data.get(ticker, {}) or {}
                mcap = info.get("marketCap", info.get("market_cap", 0)) or 0

                exp = {}
                exp["momentum"] = self.calc_momentum_factor(ticker, prices)
                exp["value"] = self.calc_value_factor(ticker, info)
                exp["quality"] = self.calc_quality_factor(ticker, info, prices)
                exp["size"] = self.calc_size_factor(ticker, float(mcap))
                exp["volatility"] = self.calc_volatility_factor(ticker, prices)
                exp["earnings_quality"] = self.calc_earnings_quality_factor(ticker, info.get("beat_quality_scores", {}))
                exp["altdata"] = self.calc_altdata_factor(ticker, info.get("altdata_scores", {}))
                exp["supply_chain"] = self.calc_supply_chain_factor(ticker, info.get("chain_scores", {}))
                exp["congressional"] = self.calc_congressional_factor(ticker, info.get("congress_signals", {}))
                exposures[ticker] = exp
            except Exception as exc:
                logger.warning("compute_all failed for %s: %s", ticker, exc)
                exposures[ticker] = {f: 0.0 for f in self.FACTOR_NAMES}
        return exposures

    def get_exposure_matrix(
        self,
        tickers: list,
        price_data: dict,
        all_scores: dict = None,
    ) -> "pd.DataFrame":
        """Return DataFrame: rows=tickers, columns=factor names."""
        if not HAS_PANDAS:
            logger.warning("pandas not available; returning empty for exposure matrix")
            return None
        exposures = self.compute_all(tickers, price_data, all_scores)
        if not exposures:
            return pd.DataFrame(columns=self.FACTOR_NAMES)
        df = pd.DataFrame.from_dict(exposures, orient="index", columns=self.FACTOR_NAMES)
        # Cross-sectional normalize each factor
        for col in df.columns:
            df[col] = _rank_normalize(df[col])
        return df

    # ------------------------------------------------------------------
    # Individual factor calculations
    # ------------------------------------------------------------------

    def calc_momentum_factor(self, ticker: str, prices) -> float:
        """momentum_12_1 = return(t-252, t-21). Cross-sectional rank normalised -1 to +1."""
        if prices is None:
            return 0.0
        try:
            if HAS_PANDAS and isinstance(prices, pd.DataFrame):
                close = prices["Close"] if "Close" in prices.columns else prices.iloc[:, 0]
            elif HAS_PANDAS and isinstance(prices, pd.Series):
                close = prices
            else:
                return 0.0

            if len(close) < self.momentum_long:
                return 0.0

            p_long = float(close.iloc[-(self.momentum_long)])
            p_short = float(close.iloc[-(self.momentum_short)])
            if p_long <= 0:
                return 0.0
            return (p_short - p_long) / p_long
        except Exception as exc:
            logger.warning("calc_momentum_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_value_factor(self, ticker: str, info: dict) -> float:
        """rank(1/PE) + rank(1/PB) + rank(earnings_yield). Normalised composite."""
        if not info:
            return 0.0
        try:
            scores = []
            pe = info.get("trailingPE") or info.get("pe_ratio")
            if pe and float(pe) > 0:
                scores.append(1.0 / float(pe))
            pb = info.get("priceToBook") or info.get("pb_ratio")
            if pb and float(pb) > 0:
                scores.append(1.0 / float(pb))
            ey = info.get("earningsYield") or info.get("earnings_yield")
            if ey is None:
                # Approximate from PE
                if pe and float(pe) > 0:
                    ey = 1.0 / float(pe)
            if ey:
                scores.append(float(ey))
            if not scores:
                return 0.0
            return float(np.mean(scores))
        except Exception as exc:
            logger.warning("calc_value_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_quality_factor(self, ticker: str, info: dict, prices) -> float:
        """rank(ROE) + rank(gross_margin_stability) + rank(low_accruals). Normalised."""
        if not info:
            return 0.0
        try:
            scores = []
            roe = info.get("returnOnEquity") or info.get("roe")
            if roe is not None:
                scores.append(float(roe))
            gm = info.get("grossMargins") or info.get("gross_margin")
            if gm is not None:
                scores.append(float(gm))
            # Low accruals proxy: operating cash flow / total assets
            ocf = info.get("operatingCashflow") or info.get("operating_cashflow")
            ta = info.get("totalAssets") or info.get("total_assets")
            if ocf and ta and float(ta) > 0:
                scores.append(float(ocf) / float(ta))
            if not scores:
                return 0.0
            return float(np.mean(scores))
        except Exception as exc:
            logger.warning("calc_quality_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_size_factor(self, ticker: str, market_cap: float) -> float:
        """-rank(log(market_cap)). Small cap = higher loading."""
        if market_cap <= 0:
            return 0.0
        try:
            return -math.log(market_cap)
        except Exception as exc:
            logger.warning("calc_size_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_volatility_factor(self, ticker: str, prices) -> float:
        """rank(-realised_vol_60d). Lower vol = higher factor loading."""
        if prices is None:
            return 0.0
        try:
            if HAS_PANDAS and isinstance(prices, pd.DataFrame):
                close = prices["Close"] if "Close" in prices.columns else prices.iloc[:, 0]
            elif HAS_PANDAS and isinstance(prices, pd.Series):
                close = prices
            else:
                return 0.0
            if len(close) < self.vol_window + 1:
                return 0.0
            returns = close.pct_change().dropna()
            vol = float(returns.tail(self.vol_window).std() * math.sqrt(252))
            return -vol
        except Exception as exc:
            logger.warning("calc_volatility_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_earnings_quality_factor(self, ticker: str, beat_quality_scores: dict) -> float:
        """Custom: rank(beat_quality) + rank(transcript_score) + rank(guidance_accuracy)"""
        if not beat_quality_scores:
            return 0.0
        try:
            scores = []
            bq = beat_quality_scores.get("beat_quality")
            if bq is not None:
                scores.append(float(bq))
            ts = beat_quality_scores.get("transcript_score")
            if ts is not None:
                scores.append(float(ts))
            ga = beat_quality_scores.get("guidance_accuracy")
            if ga is not None:
                scores.append(float(ga))
            if not scores:
                return 0.0
            return float(np.mean(scores))
        except Exception as exc:
            logger.warning("calc_earnings_quality_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_altdata_factor(self, ticker: str, altdata_scores: dict) -> float:
        """Custom: rank(confluence_score) + rank(cascade_stage) + rank(wikipedia_momentum)"""
        if not altdata_scores:
            return 0.0
        try:
            scores = []
            cs = altdata_scores.get("confluence_score")
            if cs is not None:
                scores.append(float(cs))
            stage = altdata_scores.get("cascade_stage")
            if stage is not None:
                scores.append(float(stage) / 5.0)  # normalise stage 1-5
            wm = altdata_scores.get("wikipedia_momentum")
            if wm is not None:
                scores.append(float(wm))
            if not scores:
                return 0.0
            return float(np.mean(scores))
        except Exception as exc:
            logger.warning("calc_altdata_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_supply_chain_factor(self, ticker: str, chain_scores: dict) -> float:
        """Custom: rank(-upstream_risk) + rank(-downstream_risk)"""
        if not chain_scores:
            return 0.0
        try:
            scores = []
            ur = chain_scores.get("upstream_risk")
            if ur is not None:
                scores.append(-float(ur))
            dr = chain_scores.get("downstream_risk")
            if dr is not None:
                scores.append(-float(dr))
            if not scores:
                return 0.0
            return float(np.mean(scores))
        except Exception as exc:
            logger.warning("calc_supply_chain_factor failed for %s: %s", ticker, exc)
            return 0.0

    def calc_congressional_factor(self, ticker: str, congress_signals: dict) -> float:
        """Custom: rank(signal_strength) * rank(credibility) * rank(committee_relevance)"""
        if not congress_signals:
            return 0.0
        try:
            strength = float(congress_signals.get("signal_strength", 0.0))
            credibility = float(congress_signals.get("credibility", 0.0))
            relevance = float(congress_signals.get("committee_relevance", 0.0))
            return strength * credibility * relevance
        except Exception as exc:
            logger.warning("calc_congressional_factor failed for %s: %s", ticker, exc)
            return 0.0
