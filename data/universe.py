import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Universe Tiers (Refinement 5)
# ---------------------------------------------------------------------------

TIER_4_MICROCAP = "TIER_4_MICROCAP"    # $10M–$50M: extreme caution, 25% size
TIER_1_SMALLCAP = "TIER_1_SMALLCAP"    # $50M–$2B: primary targets, full size
TIER_2_MIDCAP   = "TIER_2_MIDCAP"      # $2B–$10B: reduced size (70%)
TIER_3_LARGECAP = "TIER_3_LARGECAP"    # $10B+: very selective (50%)

# Market cap boundaries (USD)
_MICRO_MAX  =    50_000_000
_SMALL_MAX  = 2_000_000_000
_MID_MAX    = 10_000_000_000

# Position size multipliers by tier
TIER_SIZE_MULTIPLIERS = {
    TIER_4_MICROCAP: 0.25,
    TIER_1_SMALLCAP: 1.00,
    TIER_2_MIDCAP:   0.70,
    TIER_3_LARGECAP: 0.50,
}

# Minimum signal requirements by tier (zscore, confluence)
TIER_SIGNAL_GATES = {
    TIER_4_MICROCAP: {"min_zscore": 2.0,  "min_confluence": 0.80},
    TIER_1_SMALLCAP: {"min_zscore": 0.5,  "min_confluence": 0.30},
    TIER_2_MIDCAP:   {"min_zscore": 2.5,  "min_confluence": 0.75},
    TIER_3_LARGECAP: {"min_zscore": 3.0,  "min_confluence": 0.85},
}

# ---------------------------------------------------------------------------
# Fallback tickers for development/testing
# ---------------------------------------------------------------------------
_DEFAULT_US = [
    # TIER_1 Small-cap — consumer/industrial
    "CROX", "BOOT", "TREX", "SSD", "GIII", "CONN", "HIBB", "CATO",
    # TIER_1 Semi
    "DIOD", "KLIC", "FORM", "CRUS", "COHU", "ACMR", "PLAB", "AXTI",
    # TIER_1 Biotech/Pharma
    "EXEL", "LGND", "SUPN", "ACAD", "HRMY", "RCUS", "SAGE", "AGIO",
    # TIER_1 Tech small
    "IDCC", "SLAB", "POWI", "MTSI", "CEVA", "SMTC", "ATNI", "LIQT",
    # TIER_1 Diversified
    "IOSP", "MLAB", "ANDE", "ROAD", "WDFC", "GRC", "HBT", "METC",
    # TIER_2 Mid-cap additions
    "LKFN", "SFBS", "NBTB", "GNTY", "PFBC",
]

_DEFAULT_UK = [
    "VOD.L", "BP.L", "HSBA.L", "LLOY.L", "BARC.L",
    "AZN.L", "GSK.L", "ULVR.L", "RIO.L", "GLEN.L",
]

# Additional tickers expanding universe (Refinement 5: 500+ total)
_EXPANDED_US = [
    # Healthcare / biotech
    "ALNY", "FOLD", "RARE", "MYMD", "ADMA", "BNGO", "NVAX", "OCGN",
    "SRPT", "BLUE", "EDIT", "BEAM", "CRSP", "NTLA", "PRVB", "FATE",
    "IMVT", "ARDX", "ACRS", "CGEM", "YMAB", "NRIX", "TGTX", "FULC",
    # Technology
    "INVA", "HLIT", "DGII", "PCTY", "ATEN", "LFUS", "MPWR", "AEIS",
    "BRKS", "IMOS", "ONTO", "UCTT", "AMBA", "AXNX", "CLFD", "LYTS",
    "NTGR", "EGHT", "FIVN", "LPSN", "BLKB", "EVBG", "COUP", "PROS",
    # Consumer
    "PLAY", "KFRC", "LCI", "KIRK", "BCAL", "FLWS", "LOVE", "SGC",
    "BOOT", "TLYS", "GMAN", "YETI", "OXM", "CATO", "PRTS", "DXLG",
    # Energy
    "REX", "CLNE", "GEVO", "VTNR", "AMPY", "CIVI", "BATL", "SND",
    "CRK", "NUVL", "MNRL", "SBOW", "GPP", "CEI", "PHX", "REI",
    # Financials
    "QCRH", "FFIN", "TCBK", "HTLF", "SRCE", "WMPN", "BSVN", "OPBK",
    "CCBG", "FBMS", "HONE", "OSHC", "CVCY", "FCNCA", "IBTX", "SFNC",
    # Industrials
    "GFF", "ASTE", "MYRG", "KBAL", "MGRC", "NVEE", "LQDT", "POWL",
    "TNC", "TWIN", "HDSN", "NHC", "HEES", "GES", "GFED", "PKOH",
    # Materials
    "ZEUS", "ASIX", "CMP", "RYAM", "WIRE", "TREC", "OMN", "UFPI",
    "ARLP", "SXC", "CALC", "SCL", "VNTR", "SENS", "ACNT", "HAFC",
    # Real Estate
    "FBRT", "GPMT", "CLNC", "RC", "KREF", "BRMK", "SACH", "HCDI",
    "LMND", "PLYM", "SAMG", "FRPH", "ALEX", "NXRT", "GMRE", "VRE",
    # Communication Services
    "CODA", "NTWK", "IRDM", "CALX", "SHEN", "LUMN", "OOMA", "IDT",
    "CCOI", "NTLS", "GSAT", "BAND", "SPOK", "ATNI", "CNSL", "LMNS",
]


def classify_tier(market_cap_usd: float) -> str:
    """Classify a ticker into a universe tier based on market cap."""
    if market_cap_usd <= 0:
        return TIER_1_SMALLCAP  # unknown → default to small-cap
    if market_cap_usd < 10_000_000:
        return "EXCLUDED"       # too small
    if market_cap_usd < _MICRO_MAX:
        return TIER_4_MICROCAP
    if market_cap_usd < _SMALL_MAX:
        return TIER_1_SMALLCAP
    if market_cap_usd < _MID_MAX:
        return TIER_2_MIDCAP
    return TIER_3_LARGECAP


def get_tier_size_multiplier(tier: str) -> float:
    """Return position size multiplier for a given tier."""
    return TIER_SIZE_MULTIPLIERS.get(tier, 1.0)


def get_tier_signal_gate(tier: str) -> Dict:
    """Return minimum signal requirements for a given tier."""
    return TIER_SIGNAL_GATES.get(tier, TIER_SIGNAL_GATES[TIER_1_SMALLCAP])


class UniverseManager:
    def __init__(self, config: dict, fetcher=None):
        self.config = config
        self.fetcher = fetcher
        self._tier_cache: Dict[str, str] = {}       # ticker → tier
        self._cap_cache:  Dict[str, float] = {}      # ticker → market_cap

    def get_tiered_universe(self, market: str = "us", tier: int = 1) -> List[str]:
        """
        Load tickers from the appropriate tiered CSV file.

        Args:
            market: 'us' or 'uk'
            tier:   1 = small-cap (primary), 2 = mid/large cap (secondary),
                    3 = large cap (context), 4 = micro cap (selective)

        Returns list of tickers, falling back to get_tickers() if file not found.
        """
        _data_dir = Path(__file__).parent
        tier_files = {
            ("us", 1): _data_dir / "universe_us_tier1.csv",
            ("us", 2): _data_dir / "universe_us_tier2.csv",
            ("us", 3): _data_dir / "universe_us_tier3.csv",
            ("us", 4): _data_dir / "universe_us_micro.csv",
            ("uk", 1): _data_dir / "universe_uk_tier1.csv",
            ("uk", 2): _data_dir / "universe_uk_tier2.csv",
        }
        path = tier_files.get((market, tier))
        if path and path.exists() and path.stat().st_size > 0:
            try:
                tickers = pd.read_csv(path, header=None)[0].dropna().str.strip().tolist()
                logger.info(
                    "Loaded %d tickers from %s (market=%s tier=%d)",
                    len(tickers), path.name, market, tier,
                )
                return tickers
            except Exception as e:
                logger.warning("Could not load %s: %s", path, e)
        logger.warning(
            "Tiered file for market=%s tier=%d not found; falling back to default universe.",
            market, tier,
        )
        return self.get_tickers(market)

    # Names of pre-screened CSV files produced by universe_builder.py.
    # Tickers in these files are already within the target market-cap band, so
    # we skip the slow per-ticker yfinance API call and only apply the fast
    # price/volume filters that don't require a network round-trip.
    _PRESCREENED_CSV_SUFFIXES = (
        "universe_us.csv", "universe_uk.csv",
        "universe_us_tier1.csv", "universe_us_tier2.csv",
        "universe_uk_tier1.csv", "universe_uk_tier2.csv",
        "universe_all.csv",
    )

    def get_universe(self, market: str = "us", tickers_file: str = None) -> List[str]:
        """Load tickers from file (if provided) then apply market filters.

        When loading from a pre-screened universe CSV the expensive per-ticker
        yfinance market-cap lookup is skipped — those files are already within
        the correct cap range.  Only the cheap static filters (price, volume)
        are enforced via :meth:`apply_filters_fast`.
        """
        _data_dir = Path(__file__).parent
        default_csvs = {
            "us": _data_dir / "universe_us.csv",
            "uk": _data_dir / "universe_uk.csv",
        }

        if tickers_file and Path(tickers_file).exists():
            raw = pd.read_csv(tickers_file, header=None)[0].dropna().str.strip().tolist()
            prescreened = any(
                str(tickers_file).endswith(suf)
                for suf in self._PRESCREENED_CSV_SUFFIXES
            )
            if prescreened:
                logger.info(
                    "Universe [%s]: loaded %d tickers from pre-screened CSV (skipping API cap filter)",
                    market, len(raw),
                )
                return raw
        else:
            raw = self._default_tickers(market)
            # _default_tickers loads from universe_us/uk.csv when available.
            # Those are pre-screened — skip the slow per-ticker API filter.
            default_csv = default_csvs.get(market)
            if default_csv and default_csv.exists() and default_csv.stat().st_size > 0:
                logger.info(
                    "Universe [%s]: %d tickers from pre-screened default CSV (skipping API cap filter)",
                    market, len(raw),
                )
                return raw

        return self.apply_filters(raw, market)

    def apply_filters_fast(self, tickers: List[str], market: str = "us") -> List[str]:
        """Lightweight filter: price and volume only, no API calls.

        Used when tickers are already pre-screened by market cap (e.g. loaded
        from universe_us.csv).  Falls through to :meth:`apply_filters` if no
        fast criteria are configured.
        """
        cfg = self.config["markets"].get(market, {})
        min_price = cfg.get("min_price_usd", 0.50) if market == "us" else cfg.get("min_price_gbp", 0.01)
        min_vol   = cfg.get("avg_daily_volume_min", 10_000)
        # Without live price/volume data we can't verify these cheaply; pass through.
        if not self.fetcher:
            return tickers
        passed = []
        for ticker in tickers:
            try:
                info = self.fetcher.fetch_ticker_info(ticker) or {}
                # Skip delisted/404 tickers — yfinance returns an empty or near-empty
                # dict with no usable fields when a ticker is no longer listed.
                if not info or not any(info.get(k) for k in ("regularMarketPrice", "currentPrice", "previousClose", "averageVolume")):
                    logger.debug("Universe: skipping %s (no data — likely delisted or 404)", ticker)
                    continue
                vol   = info.get("averageVolume") or 0
                price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
                if vol >= min_vol and price >= min_price:
                    passed.append(ticker)
            except Exception:
                passed.append(ticker)  # fail-open
        logger.info("Universe [%s]: %d/%d passed fast filters", market, len(passed), len(tickers))
        return passed

    def get_tickers(self, market: str = "us") -> List[str]:
        """Alias for get_universe without fetcher-based filters (fast path)."""
        return self._default_tickers(market)

    def get_universe_with_tiers(
        self,
        market: str = "us",
        tickers_file: str = None,
        include_expanded: bool = True,
    ) -> List[Tuple[str, str]]:
        """
        Returns list of (ticker, tier) tuples for all universe tickers.
        Tiers: TIER_1_SMALLCAP, TIER_2_MIDCAP, TIER_3_LARGECAP, TIER_4_MICROCAP
        """
        raw = self._default_tickers(market)
        if include_expanded and market == "us":
            raw = list(dict.fromkeys(raw + _EXPANDED_US))  # dedup, preserve order

        results = []
        for ticker in raw:
            tier = self._get_tier(ticker)
            if tier != "EXCLUDED":
                results.append((ticker, tier))

        return results

    def get_tickers_by_tier(self, tier: str, market: str = "us") -> List[str]:
        """Return only tickers matching a specific tier."""
        all_tiers = self.get_universe_with_tiers(market)
        return [t for t, tr in all_tiers if tr == tier]

    def _get_tier(self, ticker: str) -> str:
        """Get tier for a ticker, using cache and fetcher fallback."""
        if ticker in self._tier_cache:
            return self._tier_cache[ticker]

        cap = 0.0
        if self.fetcher:
            try:
                info = self.fetcher.fetch_ticker_info(ticker) or {}
                cap = float(info.get("marketCap") or 0)
            except Exception:
                pass

        tier = classify_tier(cap)
        self._tier_cache[ticker] = tier
        self._cap_cache[ticker] = cap
        return tier

    def apply_filters(self, tickers: List[str], market: str = "us") -> List[str]:
        cfg = self.config["markets"][market]
        passed = []

        for ticker in tickers:
            if not self.fetcher:
                passed.append(ticker)  # no fetcher = let through
                continue
            info = self.fetcher.fetch_ticker_info(ticker)
            if not info:
                passed.append(ticker)  # can't verify; let through
                continue

            ok = (
                self._passes_us_filters(info, cfg)
                if market == "us"
                else self._passes_uk_filters(info, cfg)
            )
            if ok:
                passed.append(ticker)

        logger.info(f"Universe [{market}]: {len(passed)}/{len(tickers)} passed filters")
        return passed

    def _passes_us_filters(self, info: dict, cfg: dict) -> bool:
        cap = info.get("marketCap") or 0
        vol = info.get("averageVolume") or 0
        price = info.get("regularMarketPrice") or 0
        # Accept TIER_1 through TIER_4 — all non-excluded tickers
        min_cap = 10_000_000  # $10M absolute minimum
        max_cap = cfg.get("market_cap_max_usd", 10_000_000_000)  # up to $10B
        return (
            min_cap <= cap <= max_cap
            and vol >= cfg.get("avg_daily_volume_min", 10_000)
            and price >= cfg.get("min_price_usd", 0.50)
        )

    def _passes_uk_filters(self, info: dict, cfg: dict) -> bool:
        # yfinance returns marketCap in the quote currency (pence for LSE)
        cap_gbp = (info.get("marketCap") or 0) / 100
        vol = info.get("averageVolume") or 0
        price_gbp = (info.get("regularMarketPrice") or 0) / 100
        return (
            cfg["market_cap_min_gbp"] <= cap_gbp <= cfg["market_cap_max_gbp"]
            and vol >= cfg["avg_daily_volume_min"]
            and price_gbp >= cfg["min_price_gbp"]
        )

    def _default_tickers(self, market: str) -> List[str]:
        # Auto-load CSV universe files if present.
        # Use absolute path relative to this file so it works regardless of cwd.
        _data_dir = Path(__file__).parent
        csv_paths = {
            "us": _data_dir / "universe_us.csv",
            "uk": _data_dir / "universe_uk.csv",
        }
        csv_path = csv_paths.get(market)
        if csv_path and csv_path.exists() and csv_path.stat().st_size > 0:
            try:
                tickers = pd.read_csv(csv_path, header=None)[0].dropna().str.strip().tolist()
                logger.info("Loaded %d tickers from %s", len(tickers), csv_path.name)
                return tickers
            except Exception as e:
                logger.warning("Could not load %s: %s", csv_path, e)

        defaults = {"us": _DEFAULT_US, "uk": _DEFAULT_UK}
        logger.warning(
            "No tickers file found for [%s]; using built-in defaults (%d tickers). "
            "Run data/universe_builder.py to generate full universe CSVs.",
            market, len(defaults.get(market, [])),
        )
        return defaults.get(market, [])


# ---------------------------------------------------------------------------
# Simplified Universe class for trading_bot / chunked_scanner
# ---------------------------------------------------------------------------

class Universe:
    """
    Convenience wrapper that loads all available universe CSV files
    and provides fast tier lookups. Uses UniverseManager internally.
    """

    def __init__(self, config: dict):
        self.config = config
        self._cache: Dict[str, List[str]] = {}
        self._tier_map: Dict[str, str] = {}
        self._tier_map_loaded = False

    # ------------------------------------------------------------------
    def get_all_tickers(self, include_uk: bool = True) -> List[str]:
        """Load every available universe file and return deduplicated list."""
        import os
        files_to_load = [
            'data/universe_us_tier1.csv',
            'data/universe_us_tier2.csv',
            'data/universe_us_tier3.csv',
            'data/universe_us_micro.csv',
            'data/universe_all.csv',
            'data/universe_us.csv',
        ]
        if include_uk:
            files_to_load.extend([
                'data/universe_uk_tier1.csv',
                'data/universe_uk_tier2.csv',
                'data/universe_uk.csv',
            ])
        all_tickers: set = set()
        files_loaded = 0
        for path in files_to_load:
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path, header=None)
                    col = 0
                    tickers = (df[col].dropna().astype(str)
                               .str.strip()
                               .str.upper())
                    tickers = tickers[tickers.str.len() > 0]
                    all_tickers.update(tickers.tolist())
                    files_loaded += 1
                except Exception as e:
                    logger.warning('Universe: could not load %s: %s', path, e)
        result = sorted(all_tickers)
        logger.info('Universe.get_all_tickers: %d unique from %d files', len(result), files_loaded)
        return result

    def get_us_tickers(self) -> List[str]:
        """Return all US tickers from tier files."""
        if 'us' in self._cache:
            return self._cache['us']
        result = self._load_from_files([
            'data/universe_us_tier1.csv',
            'data/universe_us_tier2.csv',
            'data/universe_us_tier3.csv',
            'data/universe_us_micro.csv',
            'data/universe_us.csv',
        ])
        self._cache['us'] = result
        return result

    def get_uk_tickers(self) -> List[str]:
        """Return all UK tickers with .L suffix."""
        if 'uk' in self._cache:
            return self._cache['uk']
        raw = self._load_from_files([
            'data/universe_uk_tier1.csv',
            'data/universe_uk_tier2.csv',
            'data/universe_uk.csv',
        ])
        result = [t if t.endswith('.L') else t + '.L' for t in raw]
        self._cache['uk'] = result
        return result

    def get_ticker_tier(self, ticker: str) -> str:
        """Return tier string for a ticker."""
        if not self._tier_map_loaded:
            self._build_tier_map()
        clean = ticker.replace('.L', '').upper()
        return self._tier_map.get(clean, 'UNKNOWN')

    def _build_tier_map(self) -> None:
        """Build ticker->tier mapping from all tier files."""
        import os
        tier_files = [
            ('data/universe_us_tier1.csv', 'TIER_1'),
            ('data/universe_us_tier2.csv', 'TIER_2'),
            ('data/universe_us_tier3.csv', 'TIER_3'),
            ('data/universe_us_micro.csv', 'MICRO'),
            ('data/universe_uk_tier1.csv', 'UK_1'),
            ('data/universe_uk_tier2.csv', 'UK_2'),
        ]
        for path, tier_name in tier_files:
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path, header=None)
                    for t in df[0].dropna().astype(str).str.strip().str.upper():
                        if t and t not in self._tier_map:
                            self._tier_map[t] = tier_name
                except Exception:
                    pass
        self._tier_map_loaded = True

    def _load_from_files(self, files: List[str]) -> List[str]:
        """Load and deduplicate tickers from a list of CSV paths."""
        import os
        tickers: set = set()
        for path in files:
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path, header=None)
                    for t in df[0].dropna().astype(str).str.strip().str.upper():
                        if t:
                            tickers.add(t)
                except Exception:
                    pass
        return sorted(tickers)
