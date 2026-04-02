"""
Sector Rotation Signal
=======================
Tracks relative strength of sector ETFs to detect rotation.
Generates modifiers for PEAD signals based on sector momentum.

ETFs tracked: XLK, XLV, XLY, XLF, XLE, XLI, XLB, XLRE, XLC, XLU, XLP
Benchmark: SPY
"""
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SECTOR_ETFS: Dict[str, str] = {
    "technology": "XLK",
    "healthcare": "XLV",
    "consumer_disc": "XLY",
    "financials": "XLF",
    "energy": "XLE",
    "industrials": "XLI",
    "materials": "XLB",
    "real_estate": "XLRE",
    "communication": "XLC",
    "utilities": "XLU",
    "consumer_staples": "XLP",
}

# Reverse map: ETF ticker → sector name
_ETF_TO_SECTOR: Dict[str, str] = {v: k for k, v in SECTOR_ETFS.items()}

TICKER_TO_SECTOR: Dict[str, str] = {
    # Technology
    "XLK": "technology",
    "NVDA": "technology",
    "AMD": "technology",
    "MSFT": "technology",
    "AAPL": "technology",
    "INTC": "technology",
    "QCOM": "technology",
    "AVGO": "technology",
    "TSM": "technology",
    "META": "technology",
    # Financials
    "XLF": "financials",
    "JPM": "financials",
    "GS": "financials",
    "BAC": "financials",
    "WFC": "financials",
    "MS": "financials",
    "BLK": "financials",
    # Energy
    "XLE": "energy",
    "XOM": "energy",
    "CVX": "energy",
    "COP": "energy",
    "SLB": "energy",
    # Healthcare
    "XLV": "healthcare",
    "JNJ": "healthcare",
    "UNH": "healthcare",
    "PFE": "healthcare",
    "MRK": "healthcare",
    "ABBV": "healthcare",
    # Consumer Discretionary
    "XLY": "consumer_disc",
    "AMZN": "consumer_disc",
    "TSLA": "consumer_disc",
    "HD": "consumer_disc",
    "MCD": "consumer_disc",
    # Industrials
    "XLI": "industrials",
    "CAT": "industrials",
    "GE": "industrials",
    "BA": "industrials",
    "HON": "industrials",
    # Communication
    "XLC": "communication",
    "GOOGL": "communication",
    "GOOG": "communication",
    "T": "communication",
    "VZ": "communication",
    # Consumer Staples
    "XLP": "consumer_staples",
    "PG": "consumer_staples",
    "KO": "consumer_staples",
    "WMT": "consumer_staples",
    # Utilities
    "XLU": "utilities",
    "NEE": "utilities",
    "DUK": "utilities",
    # Materials
    "XLB": "materials",
    "LIN": "materials",
    "APD": "materials",
    # Real Estate
    "XLRE": "real_estate",
    "AMT": "real_estate",
    "PLD": "real_estate",
}

_ALL_TICKERS = list(SECTOR_ETFS.values()) + ["SPY"]


def fetch_returns(period_days: int) -> Dict[str, float]:
    """
    Download all 11 sector ETFs + SPY and compute total return over period_days.

    Returns: dict mapping ticker → return (e.g. {"XLK": 0.05, "SPY": 0.03, ...})
    """
    try:
        import yfinance as yf

        start = (datetime.utcnow() - timedelta(days=period_days + 10)).strftime("%Y-%m-%d")
        data = yf.download(
            _ALL_TICKERS,
            start=start,
            progress=False,
            auto_adjust=True,
        )
        if data.empty:
            logger.warning("fetch_returns: empty data for period_days=%d", period_days)
            return {}

        close = data["Close"]
        if isinstance(close, pd.Series):
            close = close.to_frame()

        # Trim to period_days trading days
        close = close.tail(period_days + 1)
        if len(close) < 2:
            return {}

        results: Dict[str, float] = {}
        for ticker in _ALL_TICKERS:
            if ticker in close.columns:
                series = close[ticker].dropna()
                if len(series) >= 2:
                    ret = float(series.iloc[-1]) / float(series.iloc[0]) - 1
                    results[ticker] = ret

        return results
    except Exception as exc:
        logger.warning("fetch_returns failed: %s", exc)
        return {}


def compute_relative_strength() -> Dict[str, float]:
    """
    Compute composite relative strength for each sector vs SPY.

    Weights: 50% 4-week, 30% 12-week, 20% 26-week.

    Returns: dict mapping sector_name → rs_composite
    """
    try:
        ret_4w = fetch_returns(20)    # ~4 trading weeks
        ret_12w = fetch_returns(60)   # ~12 trading weeks
        ret_26w = fetch_returns(130)  # ~26 trading weeks

        spy_4w = ret_4w.get("SPY", 0.0)
        spy_12w = ret_12w.get("SPY", 0.0)
        spy_26w = ret_26w.get("SPY", 0.0)

        scores: Dict[str, float] = {}
        for sector, etf in SECTOR_ETFS.items():
            rs_4w = ret_4w.get(etf, 0.0) - spy_4w
            rs_12w = ret_12w.get(etf, 0.0) - spy_12w
            rs_26w = ret_26w.get(etf, 0.0) - spy_26w
            rs_composite = 0.5 * rs_4w + 0.3 * rs_12w + 0.2 * rs_26w
            scores[sector] = round(rs_composite, 6)
            logger.debug(
                "RS %s: 4w=%.3f, 12w=%.3f, 26w=%.3f → composite=%.4f",
                sector, rs_4w, rs_12w, rs_26w, rs_composite,
            )

        return scores
    except Exception as exc:
        logger.warning("compute_relative_strength failed: %s", exc)
        return {}


def get_ranking(scores: Optional[Dict[str, float]] = None) -> Dict:
    """
    Rank sectors by composite relative strength.

    Returns: {
      "top_sectors": [str, ...],    # top 3
      "bottom_sectors": [str, ...], # bottom 3
      "scores": {sector: float}
    }
    """
    try:
        if scores is None:
            scores = compute_relative_strength()

        if not scores:
            return {"top_sectors": [], "bottom_sectors": [], "scores": {}}

        sorted_sectors = sorted(scores, key=lambda s: scores[s], reverse=True)
        return {
            "top_sectors": sorted_sectors[:3],
            "bottom_sectors": sorted_sectors[-3:],
            "scores": scores,
        }
    except Exception as exc:
        logger.warning("get_ranking failed: %s", exc)
        return {"top_sectors": [], "bottom_sectors": [], "scores": {}}


def detect_rotation(
    current_scores: Optional[Dict[str, float]] = None,
) -> Dict:
    """
    Detect sector rotation by comparing current vs ~4-week-ago rankings.

    Returns: {"rotating_in": [...], "rotating_out": [...]}
    """
    try:
        import yfinance as yf

        # Current ranking
        if current_scores is None:
            current_scores = compute_relative_strength()

        current_sorted = sorted(current_scores, key=lambda s: current_scores[s], reverse=True)
        current_top5 = set(current_sorted[:5])
        current_bot5 = set(current_sorted[-5:])

        # 4-week-ago: use 4w RS only (proxy for what the ranking was then)
        ret_4w_ago = fetch_returns(40)  # 40-day window gives old state
        ret_4w_now = fetch_returns(20)
        spy_40 = ret_4w_ago.get("SPY", 0.0)
        spy_20 = ret_4w_now.get("SPY", 0.0)

        old_scores: Dict[str, float] = {}
        for sector, etf in SECTOR_ETFS.items():
            r40 = ret_4w_ago.get(etf, 0.0) - spy_40
            r20 = ret_4w_now.get(etf, 0.0) - spy_20
            # Old score proxy: 40-day return minus 20-day return
            old_scores[sector] = r40 - r20

        old_sorted = sorted(old_scores, key=lambda s: old_scores[s], reverse=True)
        old_top5 = set(old_sorted[:5])
        old_bot5 = set(old_sorted[-5:])

        rotating_in = list(current_top5 - old_top5 & old_bot5)
        rotating_out = list(current_bot5 - old_bot5 & old_top5)

        return {"rotating_in": rotating_in, "rotating_out": rotating_out}
    except Exception as exc:
        logger.warning("detect_rotation failed: %s", exc)
        return {"rotating_in": [], "rotating_out": []}


def get_pead_modifier(
    sector: str,
    top_sectors: List[str],
    bottom_sectors: List[str],
    rotating_in: List[str],
    rotating_out: List[str],
) -> float:
    """
    Compute PEAD size modifier based on sector momentum.

    Returns: float in {-0.15, 0.0, +0.15}
    """
    if sector in top_sectors or sector in rotating_in:
        return 0.15
    if sector in bottom_sectors or sector in rotating_out:
        return -0.15
    return 0.0


def get_sector_for_ticker(ticker: str) -> str:
    """
    Map a ticker to a sector name.

    Uses TICKER_TO_SECTOR first, then falls back to yfinance.
    Returns: sector name string (lowercase, underscore-separated)
    """
    sector = TICKER_TO_SECTOR.get(ticker.upper())
    if sector:
        return sector

    try:
        import yfinance as yf

        raw_sector = yf.Ticker(ticker).info.get("sector", "unknown")
        # Normalise to match SECTOR_ETFS keys
        normalised = raw_sector.lower().replace(" ", "_").replace("-", "_")
        return normalised
    except Exception:
        return "unknown"


def _get_db_path() -> str:
    """Resolve path to historical_db.db in output/."""
    base = Path(__file__).resolve().parent.parent
    return str(base / "output" / "historical_db.db")


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_rotation (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            top_sectors    TEXT,
            bottom_sectors TEXT,
            rotating_in    TEXT,
            rotating_out   TEXT,
            sector_scores  TEXT,
            calculated_at  TEXT
        )
        """
    )
    conn.commit()


def _store_rotation(state: Dict) -> None:
    """Persist sector rotation state to historical_db.db."""
    try:
        db_path = _get_db_path()
        conn = sqlite3.connect(db_path)
        _ensure_table(conn)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        conn.execute(
            """
            INSERT INTO sector_rotation
              (date, top_sectors, bottom_sectors, rotating_in, rotating_out,
               sector_scores, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                today,
                json.dumps(state.get("top_sectors", [])),
                json.dumps(state.get("bottom_sectors", [])),
                json.dumps(state.get("rotating_in", [])),
                json.dumps(state.get("rotating_out", [])),
                json.dumps(state.get("scores", {})),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        logger.info("sector_rotation: stored to DB (%s)", today)
    except Exception as exc:
        logger.warning("sector_rotation: DB store failed: %s", exc)


class SectorRotationSignal:
    """
    Tracks relative strength of SPDR sector ETFs to detect rotation.
    Provides PEAD signal modifiers based on sector momentum.
    """

    def __init__(self, config: dict):
        self.config = config
        self._state: Optional[Dict] = None

    def run(self) -> Dict:
        """
        Compute full sector rotation state and persist to DB.

        Returns: dict with top_sectors, bottom_sectors, rotating_in,
                 rotating_out, scores.
        """
        try:
            scores = compute_relative_strength()
            ranking = get_ranking(scores)
            rotation = detect_rotation(scores)

            state = {
                "top_sectors": ranking["top_sectors"],
                "bottom_sectors": ranking["bottom_sectors"],
                "scores": ranking["scores"],
                "rotating_in": rotation["rotating_in"],
                "rotating_out": rotation["rotating_out"],
                "calculated_at": datetime.utcnow().isoformat(),
            }

            self._state = state
            _store_rotation(state)

            logger.info(
                "SectorRotation: top=%s, bottom=%s, in=%s, out=%s",
                state["top_sectors"],
                state["bottom_sectors"],
                state["rotating_in"],
                state["rotating_out"],
            )
            return state
        except Exception as exc:
            logger.warning("SectorRotationSignal.run failed: %s", exc)
            return {
                "top_sectors": [],
                "bottom_sectors": [],
                "scores": {},
                "rotating_in": [],
                "rotating_out": [],
            }

    def get_modifier(self, ticker: str, sector: str = None) -> float:
        """
        Return PEAD size modifier (-0.15 to +0.15) for a given ticker.

        Runs sector rotation if state not yet computed.
        Pass `sector` directly to skip the yfinance lookup for that ticker.
        """
        try:
            if self._state is None:
                self.run()

            if sector is None:
                sector = get_sector_for_ticker(ticker)
            else:
                # Normalise caller-supplied sector to match SECTOR_ETFS keys
                sector = sector.lower().replace(" ", "_").replace("-", "_")
            if not self._state:
                return 0.0

            mod = get_pead_modifier(
                sector,
                self._state.get("top_sectors", []),
                self._state.get("bottom_sectors", []),
                self._state.get("rotating_in", []),
                self._state.get("rotating_out", []),
            )
            logger.debug(
                "%s: sector=%s → rotation_modifier=%.2f", ticker, sector, mod
            )
            return mod
        except Exception as exc:
            logger.warning("%s: SectorRotationSignal.get_modifier failed: %s", ticker, exc)
            return 0.0
