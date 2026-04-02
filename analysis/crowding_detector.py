"""
Crowding Risk Detector
=======================
Detects when PEAD or other strategies are likely crowded by many
quantitative funds, increasing the risk of simultaneous exits.

CrowdingRiskIndex = weighted composite of:
  - Short interest concentration (0.30)
  - Post-earnings move correlation (0.30)
  - Institutional ownership concentration (0.20)
  - Factor return dispersion (0.20)

Classes:
  CrowdingDetector  — computes CrowdingRiskIndex
"""
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parents[1]
_HIST_DB = _ROOT / "output" / "historical_db.db"


@dataclass
class CrowdingRiskResult:
    index: float                  # 0.0 to 1.0
    label: str                    # "LOW" / "MEDIUM" / "HIGH" / "SEVERE"
    size_multiplier: float
    short_interest_score: float
    correlation_score: float
    institutional_score: float
    dispersion_score: float
    calculated_at: str


class CrowdingDetector:
    """
    Computes a CrowdingRiskIndex for the current portfolio universe.

    Usage:
        detector = CrowdingDetector()
        result   = detector.get_current_crowding_risk()
        print(detector.summary())
    """

    def __init__(self, config=None):
        self.config = config or {}

    # ------------------------------------------------------------------
    # Component scores
    # ------------------------------------------------------------------

    def _short_interest_score(self, tickers: List[str]) -> float:
        """
        Average short-interest-as-pct-of-float across tickers.
        Score = min(1.0, avg_short_interest_pct / 30.0)  — 30 % = max crowding.
        """
        try:
            import yfinance as yf
            values = []
            for t in tickers:
                try:
                    info = yf.Ticker(t).info
                    si = info.get("shortPercentOfFloat", None)
                    if si is not None:
                        values.append(float(si) * 100)
                except Exception:
                    pass
            if not values:
                return 0.3
            avg_si = float(np.mean(values))
            return float(min(1.0, avg_si / 30.0))
        except Exception as exc:
            logger.warning("_short_interest_score failed: %s", exc)
            return 0.3

    def _correlation_score(self, tickers: List[str], lookback_days: int = 20) -> float:
        """
        Mean pairwise return correlation over lookback_days.
        Score = max(0.0, (mean_corr - 0.4) / 0.4)  — 0 at corr=0.4, 1 at corr=0.8.
        """
        if len(tickers) < 3:
            return 0.3
        try:
            import yfinance as yf
            end = datetime.utcnow()
            start = end - timedelta(days=lookback_days + 10)
            prices = yf.download(
                tickers, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False,
            )["Close"]
            if isinstance(prices, pd.Series):
                return 0.3
            prices = prices.dropna(axis=1, how="all").dropna()
            if prices.shape[1] < 3 or len(prices) < 5:
                return 0.3
            rets = prices.pct_change().dropna().tail(lookback_days)
            corr_matrix = rets.corr().values
            n = corr_matrix.shape[0]
            upper = [corr_matrix[i, j] for i in range(n) for j in range(i + 1, n)]
            if not upper:
                return 0.3
            mean_corr = float(np.mean(upper))
            score = float(max(0.0, (mean_corr - 0.4) / 0.4))
            return float(min(1.0, score))
        except Exception as exc:
            logger.warning("_correlation_score failed: %s", exc)
            return 0.3

    def _institutional_score(self, tickers: List[str]) -> float:
        """
        Mean institutional ownership fraction across tickers.
        Score = min(1.0, max(0.0, (avg_inst - 0.5) / 0.4))  — 50–90 % range.
        """
        try:
            import yfinance as yf
            values = []
            for t in tickers:
                try:
                    info = yf.Ticker(t).info
                    hi = info.get("heldPercentInstitutions", None)
                    if hi is not None:
                        values.append(float(hi))
                except Exception:
                    pass
            if not values:
                return 0.3
            avg_inst = float(np.mean(values))
            score = float(min(1.0, max(0.0, (avg_inst - 0.5) / 0.4)))
            return score
        except Exception as exc:
            logger.warning("_institutional_score failed: %s", exc)
            return 0.3

    def _dispersion_score(self, tickers: List[str], lookback_days: int = 20) -> float:
        """
        Factor return dispersion — low dispersion means stocks move together (crowded).
        Inverted: score = max(0.0, 1.0 - avg_cross_sectional_std / 0.03).
        """
        try:
            import yfinance as yf
            end = datetime.utcnow()
            start = end - timedelta(days=lookback_days + 10)
            prices = yf.download(
                tickers, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False,
            )["Close"]
            if isinstance(prices, pd.Series):
                return 0.3
            prices = prices.dropna(axis=1, how="all").dropna()
            if prices.shape[1] < 3 or len(prices) < 5:
                return 0.3
            rets = prices.pct_change().dropna().tail(lookback_days)
            daily_std = rets.std(axis=1)
            avg_dispersion = float(daily_std.mean())
            score = float(max(0.0, 1.0 - avg_dispersion / 0.03))
            return float(min(1.0, score))
        except Exception as exc:
            logger.warning("_dispersion_score failed: %s", exc)
            return 0.3

    # ------------------------------------------------------------------
    # Main method
    # ------------------------------------------------------------------

    def get_current_crowding_risk(
        self, tickers: Optional[List[str]] = None
    ) -> CrowdingRiskResult:
        """
        Compute CrowdingRiskIndex for the given tickers (or the default US Tier-1 universe).
        Stores result in historical_db.db crowding_risk table.
        """
        if tickers is None:
            universe_path = _ROOT / "data" / "universe_us_tier1.csv"
            try:
                df = pd.read_csv(universe_path, header=None)
                tickers = df.iloc[:, 0].tolist()[:50]
            except Exception as exc:
                logger.warning("Could not load universe file: %s", exc)
                tickers = []

        sample = tickers[:20]
        if not sample:
            logger.warning("CrowdingDetector: no tickers — returning neutral result")
            sample = []

        si_score   = self._short_interest_score(sample)
        corr_score = self._correlation_score(sample)
        inst_score = self._institutional_score(sample)
        disp_score = self._dispersion_score(sample)

        index = (
            si_score   * 0.30
            + corr_score * 0.30
            + inst_score * 0.20
            + disp_score * 0.20
        )
        index = float(np.clip(index, 0.0, 1.0))

        if index > 0.9:
            label = "SEVERE"
            size_multiplier = 0.50
        elif index > 0.7:
            label = "HIGH"
            size_multiplier = 0.75
        elif index > 0.4:
            label = "MEDIUM"
            size_multiplier = 1.0
        else:
            label = "LOW"
            size_multiplier = 1.0

        calculated_at = datetime.utcnow().isoformat()

        # Persist to historical DB
        try:
            _HIST_DB.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(_HIST_DB)) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crowding_risk (
                        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                        date                 TEXT,
                        crowding_index       REAL,
                        label                TEXT,
                        size_multiplier      REAL,
                        short_interest_score REAL,
                        correlation_score    REAL,
                        institutional_score  REAL,
                        dispersion_score     REAL,
                        calculated_at        TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO crowding_risk
                        (date, crowding_index, label, size_multiplier,
                         short_interest_score, correlation_score,
                         institutional_score, dispersion_score, calculated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        datetime.utcnow().strftime("%Y-%m-%d"),
                        round(index, 6),
                        label,
                        size_multiplier,
                        round(si_score, 6),
                        round(corr_score, 6),
                        round(inst_score, 6),
                        round(disp_score, 6),
                        calculated_at,
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("CrowdingDetector: DB write failed: %s", exc)

        return CrowdingRiskResult(
            index=round(index, 4),
            label=label,
            size_multiplier=size_multiplier,
            short_interest_score=round(si_score, 4),
            correlation_score=round(corr_score, 4),
            institutional_score=round(inst_score, 4),
            dispersion_score=round(disp_score, 4),
            calculated_at=calculated_at,
        )

    def summary(self) -> str:
        """Return a human-readable crowding risk summary."""
        r = self.get_current_crowding_risk()
        lines = [
            "=" * 52,
            f"  CROWDING RISK DETECTOR  —  {r.calculated_at[:10]}",
            "=" * 52,
            f"  CrowdingRiskIndex : {r.index:.4f}  [{r.label}]",
            f"  Size Multiplier   : {r.size_multiplier:.2f}x",
            "-" * 52,
            f"  Short Interest    : {r.short_interest_score:.4f}  (w=0.30)",
            f"  Correlation       : {r.correlation_score:.4f}  (w=0.30)",
            f"  Institutional     : {r.institutional_score:.4f}  (w=0.20)",
            f"  Dispersion (inv.) : {r.dispersion_score:.4f}  (w=0.20)",
            "=" * 52,
        ]
        return "\n".join(lines)
