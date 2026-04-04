"""
risk_filters.py — 4 specialised risk filter classes.

  BiotechRiskFilter        — detects binary catalyst risk in biotech/healthcare stocks
  MomentumShortFilter      — blocks shorts against strong momentum
  BigWinnerReentryFilter   — validates re-entry conditions after a large winning trade
  SectorContagionDetector  — detects sector-wide selling that could affect peers

Usage
-----
from analysis.risk_filters import BiotechRiskFilter, MomentumShortFilter

bio = BiotechRiskFilter()
if bio.is_high_risk(ticker, context):
    skip_trade()
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ── BiotechRiskFilter ─────────────────────────────────────────────────────────

_BIOTECH_SECTORS = {
    "Healthcare", "Biotechnology", "Pharmaceuticals",
    "Life Sciences Tools & Services", "health_care",
}

_BINARY_CATALYST_KEYWORDS = [
    "fda", "pdufa", "phase 3", "phase iii", "nda", "bla", "advisory committee",
    "adcom", "trial results", "clinical readout", "data readout",
]


class BiotechRiskFilter:
    """
    Flags biotech/healthcare stocks with imminent binary catalyst events.
    Binary catalyst = FDA approval/rejection, Phase 3 trial results, etc.
    """

    def is_high_risk(self, ticker: str, context: dict) -> bool:
        """
        Returns True if the stock has binary catalyst risk that should block trading.
        """
        sector = context.get("sector", "")
        if not self._is_biotech_sector(sector):
            return False

        # Check explicit flag
        if context.get("binary_catalyst_pending"):
            logger.info("BiotechRiskFilter: %s has binary_catalyst_pending flag", ticker)
            return True

        # Check news for catalyst keywords
        recent_news = context.get("recent_news", [])
        for article in recent_news:
            text = str(article.get("title", "") + " " + article.get("summary", "")).lower()
            if any(kw in text for kw in _BINARY_CATALYST_KEYWORDS):
                logger.info("BiotechRiskFilter: %s has binary catalyst keyword in news", ticker)
                return True

        # Check days to earnings — PDUFA dates often coincide with earnings period
        days_to_earn = context.get("days_to_earnings")
        if days_to_earn is not None and days_to_earn <= 7:
            return True

        return False

    @staticmethod
    def _is_biotech_sector(sector: str) -> bool:
        return any(s.lower() in sector.lower() for s in _BIOTECH_SECTORS)

    def risk_multiplier(self, ticker: str, context: dict) -> float:
        """Return position size multiplier: 0.5 if high risk, 1.0 otherwise."""
        return 0.5 if self.is_high_risk(ticker, context) else 1.0


# ── MomentumShortFilter ───────────────────────────────────────────────────────

class MomentumShortFilter:
    """
    Blocks short positions against stocks with strong upward momentum.
    Strong momentum = 20-day return > 20% AND RSI > 70.
    """

    def blocks_short(self, ticker: str, context: dict, price_data=None) -> bool:
        """
        Returns True if shorting should be blocked due to strong momentum.
        """
        # Check RSI
        rsi = context.get("rsi")
        if rsi is not None and rsi > 70.0:
            # Check 20-day momentum
            momentum_20d = context.get("momentum_20d")
            if momentum_20d is not None and momentum_20d > 0.20:
                logger.info(
                    "MomentumShortFilter: %s BLOCKED — RSI=%.1f, 20d mom=%.1f%%",
                    ticker, rsi, momentum_20d * 100
                )
                return True

        # Check price data directly if context doesn't have it
        if price_data is not None and rsi is None:
            try:
                close = price_data["close"] if "close" in price_data.columns else price_data.iloc[:, 3]
                close = close.dropna()
                if len(close) >= 20:
                    mom = float(close.iloc[-1]) / float(close.iloc[-20]) - 1.0
                    if mom > 0.20:
                        logger.info(
                            "MomentumShortFilter: %s BLOCKED — 20d mom=%.1f%%",
                            ticker, mom * 100
                        )
                        return True
            except Exception:
                pass

        return False

    def confidence_penalty(self, ticker: str, context: dict) -> float:
        """Return score multiplier for a short signal given momentum context."""
        rsi = context.get("rsi", 50.0)
        if rsi > 65:
            return max(0.5, 1.0 - (rsi - 65) / 35)  # linear penalty above 65
        return 1.0


# ── BigWinnerReentryFilter ────────────────────────────────────────────────────

_MIN_REENTRY_PULLBACK_PCT = 0.05   # need at least 5% pullback from peak
_MIN_COOLING_DAYS         = 3      # wait at least 3 trading days after exit

class BigWinnerReentryFilter:
    """
    Validates re-entry conditions after a large winning trade (> 20% gain).
    Prevents chasing a stock immediately after a big win.
    """

    def __init__(self) -> None:
        # ticker → {exit_date, exit_price, peak_price, pnl_pct}
        self._big_wins: dict[str, dict] = {}

    def record_big_win(
        self,
        ticker: str,
        exit_date: date,
        exit_price: float,
        peak_price: float,
        pnl_pct: float,
    ) -> None:
        if pnl_pct >= 0.20:
            self._big_wins[ticker] = {
                "exit_date":   exit_date,
                "exit_price":  exit_price,
                "peak_price":  peak_price,
                "pnl_pct":     pnl_pct,
            }
            logger.info(
                "BigWinnerReentry: recorded %s big win (%.1f%%)", ticker, pnl_pct * 100
            )

    def allows_reentry(
        self,
        ticker: str,
        current_price: float,
        as_of: date | None = None,
    ) -> tuple[bool, str]:
        """
        Returns (allowed, reason).
        """
        win = self._big_wins.get(ticker)
        if win is None:
            return True, "no_prior_big_win"

        today = as_of or date.today()
        days_since = (today - win["exit_date"]).days

        if days_since < _MIN_COOLING_DAYS:
            return False, f"too_soon ({days_since} < {_MIN_COOLING_DAYS} days)"

        # Check pullback from peak
        if win["peak_price"] > 0:
            drawdown = (win["peak_price"] - current_price) / win["peak_price"]
            if drawdown < _MIN_REENTRY_PULLBACK_PCT:
                return False, f"insufficient_pullback ({drawdown:.1%} < {_MIN_REENTRY_PULLBACK_PCT:.0%})"

        # Passed both conditions — allow re-entry
        self._big_wins.pop(ticker, None)
        return True, "reentry_conditions_met"


# ── SectorContagionDetector ───────────────────────────────────────────────────

_CONTAGION_DROP_THRESHOLD = 0.03   # 3% sector drop in a single day = contagion risk

class SectorContagionDetector:
    """
    Detects sector-wide selling pressure that could infect peers.
    """

    def __init__(self) -> None:
        self._sector_returns: dict[str, float] = {}  # sector → today's ETF return

    def update_sector_returns(self, sector_returns: dict[str, float]) -> None:
        """Provide latest ETF returns. sector_returns = {sector_name → return_pct}."""
        self._sector_returns = sector_returns

    def is_contagion_risk(self, sector: str) -> bool:
        """
        Returns True if the sector ETF dropped > 3% today (contagion signal).
        """
        ret = self._sector_returns.get(sector)
        if ret is None:
            return False
        return ret <= -_CONTAGION_DROP_THRESHOLD

    def get_contaminated_sectors(self) -> list[str]:
        """Return list of sectors currently under contagion pressure."""
        return [s for s, r in self._sector_returns.items()
                if r <= -_CONTAGION_DROP_THRESHOLD]

    def position_size_multiplier(self, sector: str) -> float:
        """Return 0.5 if contagion detected, 1.0 otherwise."""
        return 0.5 if self.is_contagion_risk(sector) else 1.0

    def fetch_current_returns(self) -> dict[str, float]:
        """
        Convenience: fetch today's ETF sector returns from yfinance.
        Returns dict sector_name → daily return (fraction).
        """
        from analysis.sector_rotation_tracker import _ALL_SECTORS
        returns: dict[str, float] = {}
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            end   = datetime.now()
            start = end - timedelta(days=5)
            for sector, etf in _ALL_SECTORS.items():
                try:
                    df = yf.download(etf, start=str(start.date()), end=str(end.date()),
                                     progress=False, auto_adjust=True, threads=False)
                    if df is not None and len(df) >= 2:
                        closes = df["Close"].dropna()
                        ret = float(closes.iloc[-1]) / float(closes.iloc[-2]) - 1.0
                        returns[sector] = round(ret, 5)
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("SectorContagion: fetch error: %s", exc)
        self._sector_returns = returns
        return returns
