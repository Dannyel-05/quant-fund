"""
FRED (Federal Reserve Economic Data) macro collector.

Fetches configured economic series from the St. Louis Fed free API,
computes z-scores and trend direction, then classifies the macro regime
and emits per-series z-score signals plus an overall regime signal.
"""

import logging
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    _PANDAS_AVAILABLE = False
    logger.warning("pandas not installed — fred_collector will return empty results")

_FRED_BASE = "https://api.stlouisfed.org/fred"

# ── Macro regime constants ─────────────────────────────────────────────────────

RISK_ON        = 0
GOLDILOCKS     = 1
STAGFLATION    = 2
RISK_OFF       = 3
RECESSION_RISK = 4

_REGIME_NAMES = {
    RISK_ON:        "RISK_ON",
    GOLDILOCKS:     "GOLDILOCKS",
    STAGFLATION:    "STAGFLATION",
    RISK_OFF:       "RISK_OFF",
    RECESSION_RISK: "RECESSION_RISK",
}

_REGIME_MULTIPLIERS = {
    RISK_ON:        {"long_multiplier": 1.2, "short_multiplier": 0.9},
    RISK_OFF:       {"long_multiplier": 0.7, "short_multiplier": 1.2},
    RECESSION_RISK: {"long_multiplier": 0.5, "short_multiplier": 0.5},
    GOLDILOCKS:     {"long_multiplier": 1.0, "short_multiplier": 1.0},
    STAGFLATION:    {"long_multiplier": 1.0, "short_multiplier": 1.0},
}

# Series IDs we rely on for regime classification
_REGIME_SERIES = {
    "vix":         "VIXCLS",
    "yield_curve": "T10Y2Y",
    "unemployment":"UNRATE",
    "cpi":         "CPIAUCSL",
}

# Default series if config doesn't specify any
_DEFAULT_SERIES = list(_REGIME_SERIES.values())

# ── helpers ───────────────────────────────────────────────────────────────────

def _fetch_series(session: requests.Session, series_id: str, api_key: str) -> "pd.Series | None":
    """
    Fetch up to 520 observations for a FRED series (newest first).
    Returns a pandas Series indexed by observation date, or None on failure.
    """
    if not _PANDAS_AVAILABLE:
        return None

    url = f"{_FRED_BASE}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "limit": 520,
        "sort_order": "desc",
        "file_type": "json",
    }

    try:
        resp = session.get(url, params=params, timeout=15)
    except requests.RequestException as exc:
        logger.warning("fred: network error fetching %s: %s", series_id, exc)
        return None

    if resp.status_code == 429:
        logger.warning("fred: rate limited fetching %s", series_id)
        return None
    if resp.status_code != 200:
        logger.warning("fred: HTTP %s fetching %s", resp.status_code, series_id)
        return None

    try:
        data = resp.json()
    except Exception as exc:
        logger.warning("fred: JSON parse error for %s: %s", series_id, exc)
        return None

    observations = data.get("observations", [])
    if not observations:
        return None

    dates: list = []
    values: list = []
    for obs in observations:
        date_str = obs.get("date", "")
        val_str = obs.get("value", ".")
        if val_str in (".", "", None):
            continue
        try:
            values.append(float(val_str))
            dates.append(date_str)
        except (ValueError, TypeError):
            continue

    if not dates:
        return None

    s = pd.Series(values, index=pd.to_datetime(dates), name=series_id)
    return s.sort_index()  # ascending chronological order


def _compute_stats(s: "pd.Series") -> dict:
    """
    Given a pandas Series, compute:
      current, previous, pct_change, z_score_2yr, trend_6m
    """
    if s is None or len(s) < 2:
        return {
            "current": None,
            "previous": None,
            "pct_change": None,
            "z_score_2yr": None,
            "trend_6m": None,
        }

    current = float(s.iloc[-1])
    previous = float(s.iloc[-2])
    pct_change = (current - previous) / abs(previous) if previous != 0 else 0.0

    # 2-year z-score (~504 trading days; for monthly data ~24 obs)
    two_year_window = s.last("730D")  # approx 2 years
    if len(two_year_window) >= 2:
        mean = float(two_year_window.mean())
        std = float(two_year_window.std())
        z_score = (current - mean) / std if std > 0 else 0.0
    else:
        z_score = 0.0

    # 6-month trend direction (+1 = rising, -1 = falling, 0 = flat)
    six_month = s.last("180D")
    if len(six_month) >= 2:
        trend_slope = float(six_month.iloc[-1]) - float(six_month.iloc[0])
        trend_6m = 1 if trend_slope > 0 else (-1 if trend_slope < 0 else 0)
    else:
        trend_6m = 0

    return {
        "current": current,
        "previous": previous,
        "pct_change": round(pct_change, 6),
        "z_score_2yr": round(z_score, 4),
        "trend_6m": trend_6m,
    }


# ── regime classification ─────────────────────────────────────────────────────

def _classify_regime(stats_by_id: dict) -> int:
    """
    Classify macro regime from series stats.

    Parameters
    ----------
    stats_by_id : dict mapping series_id -> stats dict (from _compute_stats)

    Returns
    -------
    int : one of RISK_ON, GOLDILOCKS, STAGFLATION, RISK_OFF, RECESSION_RISK
    """
    def _get(series_id: str, field: str, default=None):
        s = stats_by_id.get(series_id)
        if s is None:
            return default
        return s.get(field, default)

    vix_current     = _get("VIXCLS",   "current",   20.0)
    yc_current      = _get("T10Y2Y",   "current",    0.5)
    yc_trend        = _get("T10Y2Y",   "trend_6m",    0)
    cpi_zscore      = _get("CPIAUCSL", "z_score_2yr", 0.0)
    unrate_trend    = _get("UNRATE",   "trend_6m",    0)

    # Use safe defaults if values are None
    if vix_current  is None: vix_current  = 20.0
    if yc_current   is None: yc_current   = 0.5
    if cpi_zscore   is None: cpi_zscore   = 0.0

    # Priority order matters — first match wins
    if yc_current < 0 and vix_current > 25:
        return RECESSION_RISK

    if vix_current > 20 and yc_trend < 0:
        return RISK_OFF

    if cpi_zscore > 1.5 and unrate_trend > 0:
        return STAGFLATION

    if vix_current < 15 and cpi_zscore < 0.5 and unrate_trend < 0:
        return GOLDILOCKS

    return RISK_ON


# ── yfinance fallback (no FRED API key) ──────────────────────────────────────

def _collect_yfinance_fallback(market: str) -> list:
    """
    Classify macro regime using yfinance data when no FRED API key is available.
    Uses ^VIX (volatility), ^TNX (10Y yield), ^IRX (3-month T-bill) to approximate
    yield curve and compute a regime without the FRED API.
    """
    if not _PANDAS_AVAILABLE:
        return []

    try:
        import yfinance as yf
    except ImportError:
        logger.warning("fred_collector: yfinance not installed — cannot fallback")
        return []

    now_iso = datetime.now().isoformat()
    results: list = []

    try:
        # Fetch 6 months of data for trend / z-score computation
        raw = yf.download(["^VIX", "^TNX", "^IRX"], period="6mo", auto_adjust=True, progress=False)
        if raw.empty:
            logger.warning("fred_collector: yfinance fallback returned empty data")
            return []

        # Normalise MultiIndex columns → {ticker: Series}
        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw.xs("Close", axis=1, level=0)
        else:
            close = raw

        def _series(sym: str) -> "pd.Series | None":
            if sym in close.columns:
                s = close[sym].dropna()
                return s if len(s) >= 2 else None
            return None

        vix_s   = _series("^VIX")
        tnx_s   = _series("^TNX")   # 10Y
        irx_s   = _series("^IRX")   # 3M

        # Build stats_by_id compatible dict for _classify_regime
        stats: dict = {}

        if vix_s is not None:
            stats["VIXCLS"] = _compute_stats(vix_s)

        if tnx_s is not None and irx_s is not None:
            # Approximate T10Y2Y as 10Y minus 3M (close enough for regime classification)
            yc_s = tnx_s.reindex(irx_s.index, method="nearest") - irx_s
            yc_s = yc_s.dropna()
            if len(yc_s) >= 2:
                stats["T10Y2Y"] = _compute_stats(yc_s)
        elif tnx_s is not None:
            stats["T10Y2Y"] = _compute_stats(tnx_s)

        regime = _classify_regime(stats)
        regime_name = _REGIME_NAMES[regime]
        multipliers = _REGIME_MULTIPLIERS[regime]
        regime_value = (regime / 2.0) - 1.0

        # Emit per-series results
        for series_id, s_stats in stats.items():
            z = s_stats.get("z_score_2yr") or 0.0
            import math
            results.append({
                "source": "fred",
                "ticker": "MACRO",
                "market": market,
                "data_type": "macro_series",
                "value": round(math.tanh(z), 6),
                "raw_data": {"series_id": series_id, **s_stats},
                "timestamp": now_iso,
                "quality_score": 0.7,  # lower quality — yfinance approximation
            })

        results.append({
            "source": "fred",
            "ticker": "MACRO",
            "market": market,
            "data_type": "macro_regime",
            "value": round(regime_value, 6),
            "raw_data": {
                "regime_code": regime,
                "regime_name": regime_name,
                "long_multiplier": multipliers["long_multiplier"],
                "short_multiplier": multipliers["short_multiplier"],
                "input_series": stats,
                "fallback": "yfinance",
            },
            "timestamp": now_iso,
            "quality_score": 0.75,
        })

        logger.info("fred_collector (yfinance fallback): regime=%s", regime_name)

    except Exception as exc:
        logger.warning("fred_collector yfinance fallback failed: %s", exc)

    return results


# ── main collector ─────────────────────────────────────────────────────────────

def collect(tickers: list, market: str, config: dict = None) -> list:
    """
    Collect FRED macro data and classify macro regime.

    Parameters
    ----------
    tickers : list of str  (not directly used — FRED is market-wide)
    market  : str
    config  : dict

    Returns
    -------
    list of result dicts with data_type in {"macro_series", "macro_regime"}
    """
    if config is None:
        config = {}

    if not _PANDAS_AVAILABLE:
        logger.warning("fred_collector: pandas not available, returning []")
        return []

    fred_cfg = (
        config
        .get("altdata", {})
        .get("collectors", {})
        .get("fred", {})
    )

    # Read FRED key from top-level api_keys first, fall back to legacy collector config
    api_key: str = (config.get("api_keys") or {}).get("fred", "") \
        or fred_cfg.get("api_key", "")
    if not api_key:
        logger.warning(
            "[fred_collector] No FRED API key — falling back to yfinance for regime classification."
        )
        return _collect_yfinance_fallback(market)

    # Series to collect — defaults to regime-relevant series if not configured
    series_ids: list = fred_cfg.get("series", _DEFAULT_SERIES)

    # Ensure regime-critical series are always included
    for sid in _REGIME_SERIES.values():
        if sid not in series_ids:
            series_ids = list(series_ids) + [sid]

    # Additional series from api_keys update
    _EXTRA_SERIES = ["DGS10", "DGS2", "FEDFUNDS", "T10Y2Y", "BAMLH0A0HYM2", "VIXCLS", "USREC", "UMCSENT"]
    for sid in _EXTRA_SERIES:
        if sid not in series_ids:
            series_ids = list(series_ids) + [sid]

    session = requests.Session()
    session.headers.update({"User-Agent": "quant-fund/1.0"})

    stats_by_id: dict = {}
    results: list = []
    now_iso = datetime.now().isoformat()

    for series_id in series_ids:
        time.sleep(0.3)  # gentle rate limiting
        try:
            s = _fetch_series(session, series_id, api_key)
            if s is None:
                logger.info("fred: no data for series %s", series_id)
                continue

            stats = _compute_stats(s)
            stats_by_id[series_id] = stats

            # z-score as signal: clamp tanh to [-1, 1]
            z = stats.get("z_score_2yr") or 0.0
            import math
            signal_value = math.tanh(z)

            results.append({
                "source": "fred",
                "ticker": "MACRO",
                "market": market,
                "data_type": "macro_series",
                "value": round(signal_value, 6),
                "raw_data": {
                    "series_id": series_id,
                    **stats,
                },
                "timestamp": now_iso,
                "quality_score": 0.9,  # FRED data is high quality
            })

        except Exception as exc:
            logger.warning("fred: failed to process series %s: %s", series_id, exc)
            continue

    # ── macro regime classification ───────────────────────────────────────────
    try:
        regime = _classify_regime(stats_by_id)
        regime_name = _REGIME_NAMES[regime]
        multipliers = _REGIME_MULTIPLIERS[regime]

        # Regime integer as normalised value: map 0..4 → [-1, +1] via (r/2 - 1)
        # RISK_ON=0 → -1.0 (most bullish), RECESSION_RISK=4 → +1.0 (most bearish)
        # Caller can interpret sign conventions as preferred.
        regime_value = (regime / 2.0) - 1.0

        # Collect input stats for the regime result
        regime_inputs = {
            sid: stats_by_id.get(sid, {})
            for sid in _REGIME_SERIES.values()
        }

        results.append({
            "source": "fred",
            "ticker": "MACRO",
            "market": market,
            "data_type": "macro_regime",
            "value": round(regime_value, 6),
            "raw_data": {
                "regime_code": regime,
                "regime_name": regime_name,
                "long_multiplier": multipliers["long_multiplier"],
                "short_multiplier": multipliers["short_multiplier"],
                "input_series": regime_inputs,
            },
            "timestamp": now_iso,
            "quality_score": 0.95,
        })

        logger.info("fred_collector: macro regime classified as %s", regime_name)

    except Exception as exc:
        logger.warning("fred_collector: regime classification failed: %s", exc)

    logger.info("fred_collector: returned %d signals", len(results))
    return results


class FredCollector:
    """Class wrapper around the module-level collect() function."""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def collect(self, tickers: list, market: str = 'US') -> list:
        return collect(tickers, market, self.config)
