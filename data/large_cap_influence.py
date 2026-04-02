"""
Phase 7: Large-Cap Influence Engine

Tracks 100 large-cap bellwethers by sector. When they report earnings,
generates readthrough signals for small/mid-cap peers in the same sector.
Computes historical readthrough coefficients (correlation of large-cap
post-earnings returns with peer returns over the next 5 trading days).

Usage:
    from data.large_cap_influence import LargeCapInfluenceEngine
    engine = LargeCapInfluenceEngine(hist_db, earnings_db)
    signals = engine.get_readthrough_signals(["HRMY", "METC", "SHEN"])
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 100 large-cap bellwethers by sector (ticker → sector, sub-sector, weight)
# weight = relative influence strength (1.0 = standard, 2.0 = very high)
# ---------------------------------------------------------------------------

LARGE_CAP_UNIVERSE: List[Dict] = [
    # --- Technology ---
    {"ticker": "AAPL",  "sector": "technology",    "sub_sector": "consumer_electronics",  "weight": 2.0},
    {"ticker": "MSFT",  "sector": "technology",    "sub_sector": "software",               "weight": 2.0},
    {"ticker": "NVDA",  "sector": "technology",    "sub_sector": "semiconductors",         "weight": 2.0},
    {"ticker": "META",  "sector": "technology",    "sub_sector": "social_media",           "weight": 1.8},
    {"ticker": "GOOGL", "sector": "technology",    "sub_sector": "internet_services",      "weight": 1.8},
    {"ticker": "AVGO",  "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.5},
    {"ticker": "AMD",   "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.5},
    {"ticker": "INTC",  "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.3},
    {"ticker": "CRM",   "sector": "technology",    "sub_sector": "software",               "weight": 1.2},
    {"ticker": "ADBE",  "sector": "technology",    "sub_sector": "software",               "weight": 1.2},
    {"ticker": "ORCL",  "sector": "technology",    "sub_sector": "software",               "weight": 1.2},
    {"ticker": "QCOM",  "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.2},
    {"ticker": "TXN",   "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.2},
    {"ticker": "MU",    "sector": "technology",    "sub_sector": "semiconductors",         "weight": 1.1},
    {"ticker": "AMAT",  "sector": "technology",    "sub_sector": "semiconductor_equipment","weight": 1.1},
    {"ticker": "LRCX",  "sector": "technology",    "sub_sector": "semiconductor_equipment","weight": 1.1},

    # --- Healthcare / Biotech ---
    {"ticker": "JNJ",   "sector": "healthcare",    "sub_sector": "pharma",                 "weight": 1.8},
    {"ticker": "UNH",   "sector": "healthcare",    "sub_sector": "managed_care",           "weight": 1.8},
    {"ticker": "LLY",   "sector": "healthcare",    "sub_sector": "pharma",                 "weight": 1.8},
    {"ticker": "ABBV",  "sector": "healthcare",    "sub_sector": "pharma",                 "weight": 1.5},
    {"ticker": "MRK",   "sector": "healthcare",    "sub_sector": "pharma",                 "weight": 1.5},
    {"ticker": "PFE",   "sector": "healthcare",    "sub_sector": "pharma",                 "weight": 1.3},
    {"ticker": "AMGN",  "sector": "healthcare",    "sub_sector": "biotech",                "weight": 1.3},
    {"ticker": "GILD",  "sector": "healthcare",    "sub_sector": "biotech",                "weight": 1.2},
    {"ticker": "BIIB",  "sector": "healthcare",    "sub_sector": "biotech",                "weight": 1.1},
    {"ticker": "REGN",  "sector": "healthcare",    "sub_sector": "biotech",                "weight": 1.2},
    {"ticker": "VRTX",  "sector": "healthcare",    "sub_sector": "biotech",                "weight": 1.2},
    {"ticker": "CI",    "sector": "healthcare",    "sub_sector": "managed_care",           "weight": 1.2},
    {"ticker": "HUM",   "sector": "healthcare",    "sub_sector": "managed_care",           "weight": 1.2},
    {"ticker": "CVS",   "sector": "healthcare",    "sub_sector": "pharmacy",               "weight": 1.1},

    # --- Financials ---
    {"ticker": "JPM",   "sector": "financials",    "sub_sector": "banks",                  "weight": 2.0},
    {"ticker": "BAC",   "sector": "financials",    "sub_sector": "banks",                  "weight": 1.8},
    {"ticker": "WFC",   "sector": "financials",    "sub_sector": "banks",                  "weight": 1.5},
    {"ticker": "GS",    "sector": "financials",    "sub_sector": "investment_banking",     "weight": 1.5},
    {"ticker": "MS",    "sector": "financials",    "sub_sector": "investment_banking",     "weight": 1.3},
    {"ticker": "BLK",   "sector": "financials",    "sub_sector": "asset_management",       "weight": 1.3},
    {"ticker": "AXP",   "sector": "financials",    "sub_sector": "credit_cards",           "weight": 1.2},
    {"ticker": "V",     "sector": "financials",    "sub_sector": "payments",               "weight": 1.5},
    {"ticker": "MA",    "sector": "financials",    "sub_sector": "payments",               "weight": 1.5},
    {"ticker": "SCHW",  "sector": "financials",    "sub_sector": "brokerage",              "weight": 1.2},

    # --- Energy ---
    {"ticker": "XOM",   "sector": "energy",        "sub_sector": "integrated_oil",         "weight": 2.0},
    {"ticker": "CVX",   "sector": "energy",        "sub_sector": "integrated_oil",         "weight": 1.8},
    {"ticker": "COP",   "sector": "energy",        "sub_sector": "e_and_p",                "weight": 1.5},
    {"ticker": "SLB",   "sector": "energy",        "sub_sector": "oilfield_services",      "weight": 1.3},
    {"ticker": "HAL",   "sector": "energy",        "sub_sector": "oilfield_services",      "weight": 1.2},
    {"ticker": "EOG",   "sector": "energy",        "sub_sector": "e_and_p",                "weight": 1.2},
    {"ticker": "PXD",   "sector": "energy",        "sub_sector": "e_and_p",                "weight": 1.1},
    {"ticker": "MPC",   "sector": "energy",        "sub_sector": "refining",               "weight": 1.1},
    {"ticker": "VLO",   "sector": "energy",        "sub_sector": "refining",               "weight": 1.1},

    # --- Consumer Discretionary ---
    {"ticker": "AMZN",  "sector": "consumer_disc", "sub_sector": "ecommerce",              "weight": 2.0},
    {"ticker": "TSLA",  "sector": "consumer_disc", "sub_sector": "autos",                  "weight": 1.8},
    {"ticker": "HD",    "sector": "consumer_disc", "sub_sector": "home_improvement",       "weight": 1.5},
    {"ticker": "LOW",   "sector": "consumer_disc", "sub_sector": "home_improvement",       "weight": 1.3},
    {"ticker": "NKE",   "sector": "consumer_disc", "sub_sector": "apparel",                "weight": 1.3},
    {"ticker": "MCD",   "sector": "consumer_disc", "sub_sector": "restaurants",            "weight": 1.3},
    {"ticker": "SBUX",  "sector": "consumer_disc", "sub_sector": "restaurants",            "weight": 1.2},
    {"ticker": "TGT",   "sector": "consumer_disc", "sub_sector": "retail",                 "weight": 1.2},
    {"ticker": "WMT",   "sector": "consumer_disc", "sub_sector": "retail",                 "weight": 1.5},
    {"ticker": "COST",  "sector": "consumer_disc", "sub_sector": "retail",                 "weight": 1.3},

    # --- Consumer Staples ---
    {"ticker": "PG",    "sector": "consumer_stpl", "sub_sector": "household_products",     "weight": 1.5},
    {"ticker": "KO",    "sector": "consumer_stpl", "sub_sector": "beverages",              "weight": 1.3},
    {"ticker": "PEP",   "sector": "consumer_stpl", "sub_sector": "beverages",              "weight": 1.3},
    {"ticker": "PM",    "sector": "consumer_stpl", "sub_sector": "tobacco",                "weight": 1.2},
    {"ticker": "MO",    "sector": "consumer_stpl", "sub_sector": "tobacco",                "weight": 1.1},
    {"ticker": "CL",    "sector": "consumer_stpl", "sub_sector": "household_products",     "weight": 1.1},

    # --- Industrials ---
    {"ticker": "BA",    "sector": "industrials",   "sub_sector": "aerospace",              "weight": 1.8},
    {"ticker": "GE",    "sector": "industrials",   "sub_sector": "conglomerate",           "weight": 1.5},
    {"ticker": "HON",   "sector": "industrials",   "sub_sector": "conglomerate",           "weight": 1.3},
    {"ticker": "CAT",   "sector": "industrials",   "sub_sector": "heavy_equipment",        "weight": 1.3},
    {"ticker": "DE",    "sector": "industrials",   "sub_sector": "heavy_equipment",        "weight": 1.2},
    {"ticker": "RTX",   "sector": "industrials",   "sub_sector": "defense",                "weight": 1.2},
    {"ticker": "LMT",   "sector": "industrials",   "sub_sector": "defense",                "weight": 1.2},
    {"ticker": "UPS",   "sector": "industrials",   "sub_sector": "logistics",              "weight": 1.2},
    {"ticker": "FDX",   "sector": "industrials",   "sub_sector": "logistics",              "weight": 1.2},
    {"ticker": "EMR",   "sector": "industrials",   "sub_sector": "automation",             "weight": 1.1},

    # --- Real Estate ---
    {"ticker": "AMT",   "sector": "real_estate",   "sub_sector": "cell_towers",            "weight": 1.5},
    {"ticker": "PLD",   "sector": "real_estate",   "sub_sector": "industrial_reit",        "weight": 1.3},
    {"ticker": "EQIX",  "sector": "real_estate",   "sub_sector": "data_centers",           "weight": 1.3},
    {"ticker": "SPG",   "sector": "real_estate",   "sub_sector": "retail_reit",            "weight": 1.2},
    {"ticker": "O",     "sector": "real_estate",   "sub_sector": "net_lease",              "weight": 1.1},
    {"ticker": "VICI",  "sector": "real_estate",   "sub_sector": "gaming_reit",            "weight": 1.1},

    # --- Utilities ---
    {"ticker": "NEE",   "sector": "utilities",     "sub_sector": "electric",               "weight": 1.5},
    {"ticker": "DUK",   "sector": "utilities",     "sub_sector": "electric",               "weight": 1.2},
    {"ticker": "SO",    "sector": "utilities",     "sub_sector": "electric",               "weight": 1.2},
    {"ticker": "AEP",   "sector": "utilities",     "sub_sector": "electric",               "weight": 1.1},
    {"ticker": "EXC",   "sector": "utilities",     "sub_sector": "electric",               "weight": 1.1},

    # --- Materials ---
    {"ticker": "LIN",   "sector": "materials",     "sub_sector": "chemicals",              "weight": 1.5},
    {"ticker": "APD",   "sector": "materials",     "sub_sector": "chemicals",              "weight": 1.2},
    {"ticker": "FCX",   "sector": "materials",     "sub_sector": "metals_mining",          "weight": 1.3},
    {"ticker": "NEM",   "sector": "materials",     "sub_sector": "gold_mining",            "weight": 1.2},
    {"ticker": "NUE",   "sector": "materials",     "sub_sector": "steel",                  "weight": 1.1},
    {"ticker": "X",     "sector": "materials",     "sub_sector": "steel",                  "weight": 1.0},

    # --- Communication Services ---
    {"ticker": "NFLX",  "sector": "comm_services", "sub_sector": "streaming",              "weight": 1.5},
    {"ticker": "DIS",   "sector": "comm_services", "sub_sector": "media",                  "weight": 1.5},
    {"ticker": "CMCSA", "sector": "comm_services", "sub_sector": "cable",                  "weight": 1.3},
    {"ticker": "T",     "sector": "comm_services", "sub_sector": "telecom",                "weight": 1.3},
    {"ticker": "VZ",    "sector": "comm_services", "sub_sector": "telecom",                "weight": 1.3},
    {"ticker": "TMUS",  "sector": "comm_services", "sub_sector": "telecom",                "weight": 1.2},
    {"ticker": "CHTR",  "sector": "comm_services", "sub_sector": "cable",                  "weight": 1.1},
    {"ticker": "EA",    "sector": "comm_services", "sub_sector": "gaming",                 "weight": 1.1},
    {"ticker": "TTWO",  "sector": "comm_services", "sub_sector": "gaming",                 "weight": 1.0},
]

# Sector ETF mapping — used for market context
SECTOR_ETFS: Dict[str, str] = {
    "technology":     "XLK",
    "healthcare":     "XLV",
    "financials":     "XLF",
    "energy":         "XLE",
    "consumer_disc":  "XLY",
    "consumer_stpl":  "XLP",
    "industrials":    "XLI",
    "real_estate":    "XLRE",
    "utilities":      "XLU",
    "materials":      "XLB",
    "comm_services":  "XLC",
}

# Small/mid-cap universe sector mapping (ticker → sector)
# Populated from universe_us.csv / universe.py at runtime
_PEER_SECTOR_CACHE: Dict[str, str] = {}


def _build_large_cap_index() -> Dict[str, Dict]:
    """Return ticker → record dict for fast lookup."""
    return {r["ticker"]: r for r in LARGE_CAP_UNIVERSE}


LARGE_CAP_INDEX: Dict[str, Dict] = _build_large_cap_index()


def get_large_caps_by_sector(sector: str) -> List[Dict]:
    """Return all large-cap records for a given sector."""
    return [r for r in LARGE_CAP_UNIVERSE if r["sector"] == sector]


def get_sector_for_ticker(ticker: str, universe_path: str = "data/universe_us.csv") -> Optional[str]:
    """
    Look up sector for a small/mid-cap ticker from the universe CSV.
    Falls back to yfinance info if not in CSV.
    """
    if ticker in _PEER_SECTOR_CACHE:
        return _PEER_SECTOR_CACHE[ticker]

    # Try CSV first
    try:
        import csv
        with open(universe_path, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("ticker", "").upper() == ticker.upper():
                    sector = row.get("sector", "").lower().replace(" ", "_")
                    _PEER_SECTOR_CACHE[ticker] = sector
                    return sector
    except FileNotFoundError:
        pass

    # Fallback: yfinance
    try:
        info = yf.Ticker(ticker).info
        sector_raw = info.get("sector", "")
        sector_map = {
            "Technology": "technology",
            "Healthcare": "healthcare",
            "Financial Services": "financials",
            "Energy": "energy",
            "Consumer Cyclical": "consumer_disc",
            "Consumer Defensive": "consumer_stpl",
            "Industrials": "industrials",
            "Real Estate": "real_estate",
            "Utilities": "utilities",
            "Basic Materials": "materials",
            "Communication Services": "comm_services",
        }
        sector = sector_map.get(sector_raw, "unknown")
        _PEER_SECTOR_CACHE[ticker] = sector
        return sector
    except Exception:
        return None


class LargeCapInfluenceEngine:
    """
    Computes readthrough signals from large-cap earnings events to
    small/mid-cap peers in the same sector.

    Key methods
    -----------
    get_readthrough_signals(peer_tickers)  → list of current readthrough signals
    compute_historical_coefficients(...)  → historical correlation table
    get_recent_large_cap_events(days=14)  → recent large-cap earnings events
    score_peer(peer_ticker)               → composite readthrough score [-1, +1]
    """

    def __init__(self, hist_db=None, earnings_db=None):
        self.hist_db = hist_db          # HistoricalDB instance (optional)
        self.earnings_db = earnings_db  # EarningsDB instance (optional)
        self._coeff_cache: Dict[str, Dict] = {}  # (large_ticker, peer_ticker) → coeff dict

    # ------------------------------------------------------------------
    # Recent large-cap earnings events
    # ------------------------------------------------------------------

    def get_recent_large_cap_events(self, days: int = 14) -> List[Dict]:
        """
        Fetch recent earnings events for all 100 large-caps using yfinance.
        Returns list of dicts: ticker, earnings_date, eps_actual, eps_estimate,
        surprise_pct, return_t1, sector.
        """
        results = []
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")

        tickers_batch = [r["ticker"] for r in LARGE_CAP_UNIVERSE]
        logger.info("Checking %d large-caps for recent earnings events (last %d days)", len(tickers_batch), days)

        for record in LARGE_CAP_UNIVERSE:
            ticker = record["ticker"]
            try:
                t = yf.Ticker(ticker)
                cal = t.calendar
                earnings_date = None

                if cal is not None and not cal.empty:
                    if hasattr(cal, "columns"):
                        # Different yfinance versions return different structures
                        if "Earnings Date" in cal.columns:
                            ed_val = cal["Earnings Date"].iloc[0] if len(cal) > 0 else None
                        elif "Earnings Date" in cal.index:
                            ed_val = cal.loc["Earnings Date"].iloc[0] if not cal.empty else None
                        else:
                            ed_val = None
                        if ed_val is not None:
                            try:
                                earnings_date = str(ed_val)[:10]
                            except Exception:
                                pass

                # Get earnings history for recent events
                eh = t.earnings_history
                if eh is not None and not eh.empty:
                    eh = eh.copy()
                    eh.index = eh.index.astype(str).str[:10]
                    recent = eh[(eh.index >= cutoff) & (eh.index <= today)]
                    for date_str, row in recent.iterrows():
                        eps_actual   = float(row.get("epsActual",   0) or 0)
                        eps_estimate = float(row.get("epsEstimate", 0) or 0)
                        surprise_pct = float(row.get("surprisePercent", 0) or 0) / 100.0

                        # Get post-event return (t+1)
                        return_t1 = self._get_price_return(ticker, date_str, days_after=1)

                        results.append({
                            "ticker":       ticker,
                            "sector":       record["sector"],
                            "sub_sector":   record["sub_sector"],
                            "weight":       record["weight"],
                            "earnings_date": date_str,
                            "eps_actual":   eps_actual,
                            "eps_estimate": eps_estimate,
                            "surprise_pct": surprise_pct,
                            "return_t1":    return_t1,
                            "signal":       self._classify_signal(surprise_pct, return_t1),
                        })
                time.sleep(0.15)  # gentle rate limiting
            except Exception as e:
                logger.debug("Error fetching %s earnings: %s", ticker, e)

        results.sort(key=lambda x: x["earnings_date"], reverse=True)
        logger.info("Found %d recent large-cap earnings events", len(results))
        return results

    def _get_price_return(self, ticker: str, date_str: str, days_after: int = 1) -> Optional[float]:
        """Return price return from date_str to date_str + days_after trading days."""
        try:
            start = datetime.strptime(date_str, "%Y-%m-%d")
            end   = start + timedelta(days=days_after + 5)  # buffer for weekends
            hist  = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                                end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
            if hist is None or len(hist) < 2:
                return None
            closes = hist["Close"].dropna().values
            if len(closes) < days_after + 1:
                return float(closes[-1] / closes[0] - 1)
            return float(closes[days_after] / closes[0] - 1)
        except Exception:
            return None

    def _classify_signal(self, surprise_pct: Optional[float], return_t1: Optional[float]) -> str:
        """Classify a large-cap event as STRONG_BULL / BULL / NEUTRAL / BEAR / STRONG_BEAR."""
        if surprise_pct is None:
            surprise_pct = 0.0
        if return_t1 is None:
            return_t1 = 0.0
        score = (surprise_pct * 0.5) + (return_t1 * 0.5)
        if score > 0.05:
            return "STRONG_BULL"
        elif score > 0.02:
            return "BULL"
        elif score < -0.05:
            return "STRONG_BEAR"
        elif score < -0.02:
            return "BEAR"
        return "NEUTRAL"

    # ------------------------------------------------------------------
    # Readthrough signal generation
    # ------------------------------------------------------------------

    def get_readthrough_signals(
        self,
        peer_tickers: List[str],
        days_lookback: int = 14,
    ) -> List[Dict]:
        """
        For each peer ticker, find all large-cap earnings events in the last
        `days_lookback` days in the same sector. Combine into a weighted
        readthrough signal.

        Returns list of dicts per peer ticker with composite signal and details.
        """
        recent_events = self.get_recent_large_cap_events(days=days_lookback)
        if not recent_events:
            logger.info("No recent large-cap earnings events found")
            return []

        # Group events by sector
        events_by_sector: Dict[str, List[Dict]] = {}
        for ev in recent_events:
            events_by_sector.setdefault(ev["sector"], []).append(ev)

        signals = []
        for peer in peer_tickers:
            peer_sector = get_sector_for_ticker(peer)
            if not peer_sector or peer_sector not in events_by_sector:
                signals.append({
                    "peer_ticker":        peer,
                    "sector":             peer_sector,
                    "readthrough_score":  0.0,
                    "signal":             "NO_DATA",
                    "n_events":           0,
                    "events":             [],
                })
                continue

            sector_events = events_by_sector[peer_sector]
            weighted_sum  = 0.0
            total_weight  = 0.0

            for ev in sector_events:
                large_ticker = ev["ticker"]
                base_weight  = ev["weight"]

                # Historical coefficient adjustment
                coeff = self._get_readthrough_coeff(large_ticker, peer)
                adjusted_weight = base_weight * max(0.1, coeff)

                # Signal value: surprise_pct drives direction, return_t1 confirms
                sp  = ev.get("surprise_pct",  0.0) or 0.0
                r1  = ev.get("return_t1",     0.0) or 0.0
                raw = (sp * 0.6) + (r1 * 0.4)
                raw = max(-1.0, min(1.0, raw * 10))  # scale to [-1, +1]

                weighted_sum  += raw * adjusted_weight
                total_weight  += adjusted_weight

            composite = weighted_sum / total_weight if total_weight > 0 else 0.0
            composite = max(-1.0, min(1.0, composite))

            signals.append({
                "peer_ticker":        peer,
                "sector":             peer_sector,
                "readthrough_score":  round(composite, 4),
                "signal":             self._classify_signal(composite, 0),
                "n_events":           len(sector_events),
                "large_cap_events":   sector_events[:5],  # top 5 most recent
            })

        return signals

    # ------------------------------------------------------------------
    # Historical readthrough coefficients
    # ------------------------------------------------------------------

    def _get_readthrough_coeff(self, large_ticker: str, peer_ticker: str) -> float:
        """
        Return the historical readthrough coefficient (0 to 2.0) for how much
        large_ticker earnings events influence peer_ticker returns.
        Defaults to 1.0 (no adjustment) if not cached.
        """
        cache_key = f"{large_ticker}:{peer_ticker}"
        if cache_key in self._coeff_cache:
            return self._coeff_cache[cache_key].get("coeff", 1.0)
        return 1.0

    def compute_historical_coefficients(
        self,
        large_ticker: str,
        peer_tickers: List[str],
        start_date: str = "2010-01-01",
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Compute historical readthrough coefficients between large_ticker and
        each peer. Coefficient = correlation between large-cap post-earnings
        return (t+1) and peer return over next 5 days, back to start_date.

        Algorithm:
        1. Fetch large-cap earnings history
        2. For each event: get large-cap t+1 return and peer t+1..t+5 returns
        3. Compute Pearson correlation across all events
        4. Cache result

        Returns list of dicts: {peer_ticker, coeff, n_events, p_value}
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        results = []

        # Get large-cap earnings history
        large_cap_events = self._fetch_earnings_history(large_ticker, start_date, end_date)
        if len(large_cap_events) < 5:
            logger.warning("%s: insufficient earnings history (%d events)", large_ticker, len(large_cap_events))
            return []

        for peer in peer_tickers:
            try:
                large_returns = []
                peer_returns  = []

                for event_date in large_cap_events:
                    lr = self._get_price_return(large_ticker, event_date, days_after=1)
                    pr = self._get_price_return(peer, event_date, days_after=5)
                    if lr is not None and pr is not None:
                        large_returns.append(lr)
                        peer_returns.append(pr)

                if len(large_returns) < 5:
                    continue

                la = np.array(large_returns)
                pa = np.array(peer_returns)
                corr = float(np.corrcoef(la, pa)[0, 1])
                if np.isnan(corr):
                    corr = 0.0

                # Scale correlation to coefficient (0.1 to 2.0)
                coeff = 1.0 + corr  # corr in [-1, 1] → coeff in [0, 2]
                coeff = max(0.1, min(2.0, coeff))

                cache_key = f"{large_ticker}:{peer}"
                self._coeff_cache[cache_key] = {
                    "coeff":    coeff,
                    "corr":     corr,
                    "n_events": len(large_returns),
                }

                results.append({
                    "large_ticker": large_ticker,
                    "peer_ticker":  peer,
                    "coeff":        round(coeff, 4),
                    "correlation":  round(corr, 4),
                    "n_events":     len(large_returns),
                })

                time.sleep(0.1)
            except Exception as e:
                logger.debug("Coeff error %s→%s: %s", large_ticker, peer, e)

        return results

    def _fetch_earnings_history(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """Fetch list of earnings date strings for ticker between start and end."""
        try:
            t = yf.Ticker(ticker)
            eh = t.earnings_history
            if eh is None or eh.empty:
                return []
            dates = [str(d)[:10] for d in eh.index]
            dates = [d for d in dates if start_date <= d <= end_date]
            return sorted(dates)
        except Exception as e:
            logger.debug("Error fetching earnings history for %s: %s", ticker, e)
            return []

    # ------------------------------------------------------------------
    # Per-peer composite score
    # ------------------------------------------------------------------

    def score_peer(
        self,
        peer_ticker: str,
        days_lookback: int = 21,
    ) -> Dict:
        """
        Compute a composite readthrough influence score for one peer ticker.
        Returns score in [-1, +1] with signal classification and top drivers.
        """
        signals = self.get_readthrough_signals([peer_ticker], days_lookback=days_lookback)
        if not signals:
            return {"peer_ticker": peer_ticker, "score": 0.0, "signal": "NO_DATA", "drivers": []}

        sig = signals[0]
        drivers = []
        for ev in sig.get("large_cap_events", []):
            drivers.append({
                "large_cap":     ev["ticker"],
                "date":          ev["earnings_date"],
                "surprise_pct":  ev.get("surprise_pct"),
                "return_t1":     ev.get("return_t1"),
                "signal":        ev.get("signal"),
                "weight":        ev.get("weight"),
            })

        return {
            "peer_ticker":       peer_ticker,
            "sector":            sig.get("sector"),
            "score":             sig["readthrough_score"],
            "signal":            sig["signal"],
            "n_large_cap_events": sig["n_events"],
            "drivers":           drivers,
        }

    # ------------------------------------------------------------------
    # Batch update all coefficients
    # ------------------------------------------------------------------

    def update_all_coefficients(
        self,
        peer_tickers: List[str],
        start_date: str = "2010-01-01",
    ) -> int:
        """
        Compute and cache historical coefficients for all large-caps × all peers.
        Throttled to avoid API rate limits. Returns total number of coefficients computed.
        """
        total = 0
        for record in LARGE_CAP_UNIVERSE:
            large_ticker = record["ticker"]
            peers_same_sector = [
                p for p in peer_tickers
                if get_sector_for_ticker(p) == record["sector"]
            ]
            if not peers_same_sector:
                continue

            logger.info("Computing coefficients: %s → %d peers", large_ticker, len(peers_same_sector))
            results = self.compute_historical_coefficients(
                large_ticker, peers_same_sector, start_date=start_date
            )
            total += len(results)
            time.sleep(0.5)

        logger.info("Computed %d readthrough coefficients", total)
        return total

    def summary(self) -> Dict:
        """Return summary statistics about the influence engine."""
        sector_counts: Dict[str, int] = {}
        for r in LARGE_CAP_UNIVERSE:
            sector_counts[r["sector"]] = sector_counts.get(r["sector"], 0) + 1
        return {
            "total_large_caps":          len(LARGE_CAP_UNIVERSE),
            "sectors":                   len(sector_counts),
            "sector_breakdown":          sector_counts,
            "cached_coefficients":       len(self._coeff_cache),
        }
