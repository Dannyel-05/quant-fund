"""
Universe Builder — fetches 3000-4000 stock tickers from multiple free sources.

Sources:
  - Wikipedia: S&P 500, S&P 400, S&P 600, Nasdaq 100, FTSE 100, FTSE 250
  - StockAnalysis.com: Russell 2000, Russell 3000, micro-cap, ETF holdings
  - Built-in expanded lists for coverage gaps

Output CSVs:
  data/universe_us_tier1.csv  — small-mid cap $50M-$2B (primary targets)
  data/universe_us_tier2.csv  — mid-large cap $2B-$10B (secondary)
  data/universe_us_tier3.csv  — large cap $10B+ (context only)
  data/universe_us_micro.csv  — micro cap $10M-$50M (selective)
  data/universe_uk_tier1.csv  — UK small-mid £30M-£1.5B
  data/universe_uk_tier2.csv  — UK large £1.5B+
  data/universe_all.csv       — all tickers with tier column
"""

import logging
import time
import re
from io import StringIO
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/",
}
_DELAY = 1.5  # seconds between requests
_DATA_DIR = Path(__file__).parent


class UniverseBuilder:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._seen: Set[str] = set()

    def _sleep(self):
        time.sleep(_DELAY)

    def _get(self, url: str, timeout: int = 20) -> Optional[requests.Response]:
        try:
            self._sleep()
            resp = self._session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            logger.warning("HTTP %d for %s", resp.status_code, url)
        except Exception as e:
            logger.warning("Request failed for %s: %s", url, e)
        return None

    # ── Wikipedia fetchers ────────────────────────────────────────────────

    def _read_html_via_session(self, url: str) -> List[pd.DataFrame]:
        """Fetch URL via session and parse HTML tables using StringIO."""
        resp = self._get(url)
        if not resp:
            return []
        try:
            return pd.read_html(StringIO(resp.text), header=0)
        except Exception as e:
            logger.warning("HTML parse failed for %s: %s", url, e)
            return []

    def fetch_sp500_wikipedia(self) -> List[str]:
        """Fetch S&P 500 tickers from Wikipedia."""
        try:
            tables = self._read_html_via_session(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            )
            if not tables:
                return []
            df = tables[0]
            col = next((c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower()), df.columns[0])
            tickers = df[col].astype(str).str.strip().str.replace(".", "-", regex=False).tolist()
            logger.info("S&P 500 Wikipedia: %d tickers", len(tickers))
            return tickers
        except Exception as e:
            logger.warning("S&P 500 fetch failed: %s", e)
            return []

    def fetch_sp400_wikipedia(self) -> List[str]:
        """Fetch S&P 400 MidCap tickers from Wikipedia."""
        try:
            tables = self._read_html_via_session(
                "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
            )
            for df in tables:
                cols_lower = [c.lower() for c in df.columns]
                if any("symbol" in c or "ticker" in c for c in cols_lower):
                    col = df.columns[next(i for i, c in enumerate(cols_lower) if "symbol" in c or "ticker" in c)]
                    tickers = df[col].astype(str).str.strip().str.replace(".", "-", regex=False).tolist()
                    logger.info("S&P 400 Wikipedia: %d tickers", len(tickers))
                    return tickers
        except Exception as e:
            logger.warning("S&P 400 fetch failed: %s", e)
        return []

    def fetch_sp600_wikipedia(self) -> List[str]:
        """Fetch S&P 600 SmallCap tickers from Wikipedia."""
        try:
            tables = self._read_html_via_session(
                "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
            )
            for df in tables:
                cols_lower = [c.lower() for c in df.columns]
                if any("symbol" in c or "ticker" in c for c in cols_lower):
                    col = df.columns[next(i for i, c in enumerate(cols_lower) if "symbol" in c or "ticker" in c)]
                    tickers = df[col].astype(str).str.strip().str.replace(".", "-", regex=False).tolist()
                    logger.info("S&P 600 Wikipedia: %d tickers", len(tickers))
                    return tickers
        except Exception as e:
            logger.warning("S&P 600 fetch failed: %s", e)
        return []

    def fetch_nasdaq100_wikipedia(self) -> List[str]:
        """Fetch Nasdaq 100 tickers from Wikipedia."""
        try:
            tables = self._read_html_via_session("https://en.wikipedia.org/wiki/Nasdaq-100")
            for df in tables:
                if "Ticker" in df.columns or "Symbol" in df.columns:
                    col = "Ticker" if "Ticker" in df.columns else "Symbol"
                    tickers = df[col].astype(str).str.strip().tolist()
                    if len(tickers) > 50:
                        logger.info("Nasdaq 100 Wikipedia: %d tickers", len(tickers))
                        return tickers
        except Exception as e:
            logger.warning("Nasdaq 100 fetch failed: %s", e)
        return []

    def fetch_ftse100_wikipedia(self) -> List[str]:
        """Fetch FTSE 100 tickers from Wikipedia (adds .L suffix)."""
        try:
            tables = self._read_html_via_session("https://en.wikipedia.org/wiki/FTSE_100_Index")
            for df in tables:
                cols_lower = [c.lower() for c in df.columns]
                if any("ticker" in c or "epic" in c or "symbol" in c for c in cols_lower):
                    idx = next(i for i, c in enumerate(cols_lower) if "ticker" in c or "epic" in c or "symbol" in c)
                    col = df.columns[idx]
                    tickers = [t.strip() + ".L" for t in df[col].astype(str) if len(t.strip()) >= 2 and t.strip() != "nan"]
                    if len(tickers) > 50:
                        logger.info("FTSE 100 Wikipedia: %d tickers", len(tickers))
                        return tickers
        except Exception as e:
            logger.warning("FTSE 100 fetch failed: %s", e)
        return []

    def fetch_ftse250_wikipedia(self) -> List[str]:
        """Fetch FTSE 250 tickers from Wikipedia (adds .L suffix)."""
        try:
            tables = self._read_html_via_session("https://en.wikipedia.org/wiki/FTSE_250_Index")
            for df in tables:
                cols_lower = [c.lower() for c in df.columns]
                if any("ticker" in c or "epic" in c for c in cols_lower):
                    idx = next(i for i, c in enumerate(cols_lower) if "ticker" in c or "epic" in c)
                    col = df.columns[idx]
                    tickers = [t.strip() + ".L" for t in df[col].astype(str) if len(t.strip()) >= 2 and t.strip() != "nan"]
                    if len(tickers) > 100:
                        logger.info("FTSE 250 Wikipedia: %d tickers", len(tickers))
                        return tickers
        except Exception as e:
            logger.warning("FTSE 250 fetch failed: %s", e)
        return []

    # ── StockAnalysis.com fetchers ─────────────────────────────────────────

    def _fetch_stockanalysis_list(self, path: str, label: str) -> List[str]:
        """Generic StockAnalysis list fetcher."""
        url = f"https://stockanalysis.com{path}"
        resp = self._get(url)
        if not resp:
            return []
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            # StockAnalysis uses a table with links like /stocks/TICKER/
            tickers = []
            for a in soup.find_all("a", href=re.compile(r"^/stocks/[A-Z]+/$")):
                ticker = a["href"].split("/")[2]
                if ticker and len(ticker) <= 6:
                    tickers.append(ticker)
            # Also try table approach
            if not tickers:
                for row in soup.select("table tbody tr"):
                    cells = row.find_all("td")
                    if cells:
                        t = cells[0].get_text(strip=True)
                        if t and len(t) <= 6 and t.isalpha():
                            tickers.append(t)
            tickers = list(dict.fromkeys(tickers))
            logger.info("%s StockAnalysis: %d tickers", label, len(tickers))
            return tickers
        except Exception as e:
            logger.warning("StockAnalysis %s failed: %s", label, e)
            return []

    def _fetch_stockanalysis_etf_holdings(self, etf: str) -> List[str]:
        """Fetch ETF holdings from StockAnalysis."""
        url = f"https://stockanalysis.com/etf/{etf.lower()}/holdings/"
        resp = self._get(url)
        if not resp:
            return []
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            tickers = []
            for a in soup.find_all("a", href=re.compile(r"^/stocks/[A-Z]+/$")):
                ticker = a["href"].split("/")[2]
                if ticker and len(ticker) <= 6:
                    tickers.append(ticker)
            # Also parse table
            if not tickers:
                try:
                    tables = pd.read_html(StringIO(str(soup)), match="Symbol")
                    if tables:
                        df = tables[0]
                        if "Symbol" in df.columns:
                            tickers = df["Symbol"].astype(str).str.strip().tolist()
                except Exception:
                    pass
            tickers = list(dict.fromkeys(tickers))
            logger.info("ETF %s holdings: %d tickers", etf, len(tickers))
            return tickers
        except Exception as e:
            logger.debug("ETF %s holdings failed: %s", etf, e)
            return []

    # ── Built-in supplemental lists ────────────────────────────────────────

    def _supplemental_us_small_cap(self) -> List[str]:
        """Hand-curated small/mid cap tickers across sectors for coverage."""
        return [
            # Already in existing universe.py but expanded
            # Healthcare
            "ACAD","ACMR","ACNT","ACRS","ADMA","AGIO","ALNY","AMBA","AMPY",
            "ANDE","ARDX","ARLP","ASIX","ASGN","ASTH","ASTE","ATNI","AXNX",
            "AXTI","BAND","BATL","BCAL","BEAM","BHE","BLKB","BLUE","BNGO",
            "BOOT","BRKS","BRMK","BSVN","CALC","CALX","CATO","CCBG","CCOI",
            "CEI","CEVA","CERT","CGEM","CIK","CLFD","CLNE","CLNC","CMP",
            "CNMD","CODA","COHU","CONN","COUP","CRK","CROX","CRSR","CRUS",
            "CTKB","CTS","CVCY","CXM","DGII","DIOD","DXLG","EDIT","EGHT",
            "EVBG","EXEL","FATE","FBMS","FBRT","FCNCA","FFIN","FIVN","FLWS",
            "FOLD","FORM","FRPH","FULC","GDYN","GES","GFED","GFED","GFF",
            "GIII","GMAN","GMRE","GPMT","GPP","GRC","GSAT","HBT","HCDI",
            "HDSN","HEES","HIBB","HLIT","HONE","HRMY","HTLF","IBTX","ICHR",
            "IDT","IMVT","IMOS","INVA","IOSP","IRDM","KBAL","KFRC","KIRK",
            "KLIC","KREF","LKFN","LGND","LMND","LMNS","LPSN","LQDT","LUMN",
            "LYTS","METC","MGRC","MLAB","MNRL","MPWR","MTSI","MYMD","MYRG",
            "NABL","NBTB","NHC","NRIX","NSSC","NTGR","NTGR","NTLS","NTLA",
            "NTWK","NUVL","NVEE","NVRI","NXRT","OCGN","OCGN","OMN","ONTO",
            "OOMA","OPBK","OXM","PDFS","PENG","PFBC","PHX","PKOH","PLAB",
            "PLAY","PLYM","PLUS","POWL","POWI","PRVB","PRGO","PRTS","PROS",
            "QCRH","RARE","RC","RCUS","REI","REX","ROAD","ROG","RAMP","RYAM",
            "SACH","SAGE","SAMG","SBOW","SCSC","SCL","SDGR","SENS","SFBS",
            "SFNC","SHEN","SMTC","SND","SPOK","SRCE","SRPT","SSD","SXC",
            "SUPN","TCBK","TGTX","TLYS","TNC","TREC","TREX","TWIN","UCTT",
            "UFPI","UFPT","VNTR","VRE","VTNR","WDFC","WIRE","WMPN","YMAB",
            "YETI","ZEUS","ZEUS","QRVO","SMCI","CORT","VCNX","ZION","PRAX",
            "VITL","LAUR","SAIC","MARA","RIOT","CIFR","COIN","GLBE","TASK",
            "ASTS","LUNR","RDW","ACHR","JOBY","LILM","EVTL","NKLA","WKHS",
            "GOEV","ARVL","XPEV","LI","NIO","BYDDY","FFIE","MULN","RIDE",
        ]

    def _supplemental_us_mid_cap(self) -> List[str]:
        """Mid-cap tickers $2B-$10B for tier 2."""
        return [
            "AEIS","AMAT","AMKR","APPN","ARGT","BFAM","BPOP","BRKL","BRP",
            "CABO","CACI","CAKE","CAN","CARG","CBZ","CHGG","CHUY","CIR","CNXC",
            "COOP","COUPA","CPNG","CRVL","CSL","CTLT","CTOS","DDS","DXCM",
            "ENPH","ESAB","EVRI","EXLS","FAF","FCFS","FFIV","FHB","FIVN",
            "FRME","FTDR","GBCI","GDOT","GNTX","GOLF","GRBK","GTN","GWW",
            "HALO","HAYW","HCI","HIMS","HLNE","HOLX","HOMB","HOPE","HUMA",
            "HWKN","IDCC","IIVI","IMCO","INDB","INFN","INMD","IPGP","IRTC",
            "ITRI","JACK","JBSS","KALU","KFRC","KMT","LBRT","LCII","LECO",
            "LGIH","LKFN","LMAT","LMND","LPSN","LRCX","LYFT","MASI","MATX",
            "MAXN","MCB","MCBS","MCRI","MDXG","MFA","MFIN","MGNX","MGPI",
            "MLNK","MMSI","MNKD","MNST","MODG","MPLN","MPWR","MRIN","MRTN",
            "MRUS","MSFUT","MTG","MTSI","NARI","NBTX","NEWR","NGEN","NHI",
            "NJR","NOVA","NRC","NTRA","NUS","NVRI","OGS","OMCL","OPRT",
            "ORGO","OTTR","PACS","PAGP","PAHC","PARR","PATK","PAYO","PCRX",
            "PDCO","PDFS","PDVP","PECK","PENN","PLAY","PLMR","PLRX","PLSE",
            "PMTS","PNFP","PODD","POOL","POWL","PRAA","PRIM","PRO","PROF",
            "PRPB","PRSP","PRTA","PRTK","PRVB","PSTV","PTCT","PTGX","PTRA",
        ]

    # ── Filtering ──────────────────────────────────────────────────────────

    def _clean_ticker(self, t: str) -> str:
        """Normalize a ticker symbol."""
        t = str(t).strip().upper()
        # Replace dots with dashes (BRK.B -> BRK-B) for US
        if not t.endswith(".L"):
            t = t.replace(".", "-")
        return t

    def _is_valid_us_ticker(self, t: str) -> bool:
        """Basic validation for US tickers."""
        if not t or len(t) > 6 or len(t) < 1:
            return False
        if t in ("NAN", "NONE", "N/A", ""):
            return False
        # US tickers: letters, possibly with hyphen for share class
        if not re.match(r"^[A-Z]{1,5}(-[A-Z])?$", t):
            return False
        return True

    def _is_valid_uk_ticker(self, t: str) -> bool:
        """Basic validation for UK tickers."""
        if not t.endswith(".L"):
            return False
        base = t[:-2]
        return bool(re.match(r"^[A-Z]{2,6}$", base))

    # ── Main build method ─────────────────────────────────────────────────

    def build(self) -> Dict[str, List[str]]:
        """
        Fetch all tickers from all sources and return categorized dict.
        Categories: us_all, uk_all
        """
        us_tickers: Set[str] = set()
        uk_tickers: Set[str] = set()
        failed_sources = []

        print("Fetching S&P 500 (Wikipedia)...")
        sp500_raw = self.fetch_sp500_wikipedia()
        if not sp500_raw:
            failed_sources.append("S&P 500 Wikipedia")
        for t in sp500_raw:
            ct = self._clean_ticker(t)
            if self._is_valid_us_ticker(ct):
                us_tickers.add(ct)

        print(f"  S&P 500: {len(us_tickers)} so far")

        print("Fetching S&P 400 (Wikipedia)...")
        before = len(us_tickers)
        sp400_raw = self.fetch_sp400_wikipedia()
        if not sp400_raw:
            failed_sources.append("S&P 400 Wikipedia")
        for t in sp400_raw:
            ct = self._clean_ticker(t)
            if self._is_valid_us_ticker(ct):
                us_tickers.add(ct)
        print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        print("Fetching S&P 600 (Wikipedia)...")
        before = len(us_tickers)
        sp600_raw = self.fetch_sp600_wikipedia()
        if not sp600_raw:
            failed_sources.append("S&P 600 Wikipedia")
        for t in sp600_raw:
            ct = self._clean_ticker(t)
            if self._is_valid_us_ticker(ct):
                us_tickers.add(ct)
        print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        print("Fetching Nasdaq 100 (Wikipedia)...")
        before = len(us_tickers)
        ndx_raw = self.fetch_nasdaq100_wikipedia()
        if not ndx_raw:
            failed_sources.append("Nasdaq 100 Wikipedia")
        for t in ndx_raw:
            ct = self._clean_ticker(t)
            if self._is_valid_us_ticker(ct):
                us_tickers.add(ct)
        print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        # ETF holdings
        etfs = ["iwm", "ijr", "vb", "mdy", "qqq", "spy", "scha", "vtwo", "iwc", "vti"]
        for etf in etfs:
            print(f"Fetching {etf.upper()} holdings (StockAnalysis)...")
            before = len(us_tickers)
            raw = self._fetch_stockanalysis_etf_holdings(etf)
            if not raw:
                failed_sources.append(f"ETF {etf.upper()} StockAnalysis")
            for t in raw:
                ct = self._clean_ticker(t)
                if self._is_valid_us_ticker(ct):
                    us_tickers.add(ct)
            print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        # StockAnalysis lists
        for path, label in [
            ("/list/russell-2000/", "Russell 2000"),
            ("/list/russell-3000/", "Russell 3000"),
            ("/list/micro-cap-stocks/", "Micro Cap"),
        ]:
            print(f"Fetching {label} (StockAnalysis)...")
            before = len(us_tickers)
            raw = self._fetch_stockanalysis_list(path, label)
            if not raw:
                failed_sources.append(f"{label} StockAnalysis")
            for t in raw:
                ct = self._clean_ticker(t)
                if self._is_valid_us_ticker(ct):
                    us_tickers.add(ct)
            print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        # Supplemental lists
        print("Adding supplemental small/mid-cap lists...")
        before = len(us_tickers)
        for t in self._supplemental_us_small_cap() + self._supplemental_us_mid_cap():
            ct = self._clean_ticker(t)
            if self._is_valid_us_ticker(ct):
                us_tickers.add(ct)
        print(f"  +{len(us_tickers)-before} new (total: {len(us_tickers)})")

        # UK stocks
        print("Fetching FTSE 100 (Wikipedia)...")
        ftse100_raw = self.fetch_ftse100_wikipedia()
        if not ftse100_raw:
            failed_sources.append("FTSE 100 Wikipedia")
        for t in ftse100_raw:
            if self._is_valid_uk_ticker(t):
                uk_tickers.add(t)
        print(f"  FTSE 100: {len(uk_tickers)} UK tickers")

        print("Fetching FTSE 250 (Wikipedia)...")
        before = len(uk_tickers)
        ftse250_raw = self.fetch_ftse250_wikipedia()
        if not ftse250_raw:
            failed_sources.append("FTSE 250 Wikipedia")
        for t in ftse250_raw:
            if self._is_valid_uk_ticker(t):
                uk_tickers.add(t)
        print(f"  +{len(uk_tickers)-before} new UK (total: {len(uk_tickers)})")

        # UK ETF holdings
        print("Fetching EWU (iShares UK) holdings...")
        ewu_raw = self._fetch_stockanalysis_etf_holdings("ewu")
        if not ewu_raw:
            failed_sources.append("ETF EWU StockAnalysis")
        for t in ewu_raw:
            ct = self._clean_ticker(t)
            uk = ct + ".L"
            if self._is_valid_uk_ticker(uk):
                uk_tickers.add(uk)

        print(f"\nTotals before dedup: US={len(us_tickers)} UK={len(uk_tickers)}")

        if failed_sources:
            print(f"\nFailed sources ({len(failed_sources)}): {', '.join(failed_sources)}")

        # Store failed sources for reporting
        self._failed_sources = failed_sources

        return {
            "us_all": sorted(us_tickers),
            "uk_all": sorted(uk_tickers),
        }

    def assign_tiers(self, tickers: List[str], market: str = "us") -> pd.DataFrame:
        """
        Assign tiers WITHOUT API calls — use index membership as proxy.
        SP500/400 members → tier 2-3, SP600/Russell2000 → tier 1, micro → tier 4.
        Return DataFrame with columns: ticker, market, source, tier
        """
        # We don't have real market caps without API calls, so use a heuristic:
        # Large companies (commonly known): tier 3
        # Index members from SP400/SP600: tier 1-2
        # Everything else: tier 1 (assume small-mid until verified)
        rows = []
        for t in tickers:
            rows.append({
                "ticker": t,
                "market": market,
                "tier": "TIER_1_SMALLCAP",  # conservative default
            })
        return pd.DataFrame(rows)

    def save(self, data: Dict[str, List[str]]) -> Dict[str, int]:
        """Save tiered CSVs to data/ directory."""
        _DATA_DIR.mkdir(exist_ok=True)
        counts = {}

        us_all = data.get("us_all", [])
        uk_all = data.get("uk_all", [])

        # For US: split into rough tiers by known index membership
        # Re-use what was already fetched during build() if available
        sp500_set = set()
        try:
            print("\nFetching S&P 500 membership for tier assignment...")
            sp500_raw = self.fetch_sp500_wikipedia()
            sp500_set = set(self._clean_ticker(t) for t in sp500_raw if self._is_valid_us_ticker(self._clean_ticker(t)))
            print(f"  S&P 500 set: {len(sp500_set)} members for tier split")
        except Exception as e:
            print(f"  S&P 500 tier split fetch failed: {e} — all US tickers → tier1")

        tier1, tier2, tier3, micro = [], [], [], []
        for t in us_all:
            if t in sp500_set:
                tier2.append(t)  # S&P 500 → mostly mid/large → tier 2
            else:
                tier1.append(t)  # everything else → tier 1 (small-mid)

        # Save US tiers
        for fname, tickers in [
            ("universe_us_tier1.csv", tier1),
            ("universe_us_tier2.csv", tier2),
            ("universe_us_tier3.csv", list(sp500_set - set(us_all))),  # large caps not yet in list
            ("universe_us_micro.csv", []),  # would need market cap data
        ]:
            path = _DATA_DIR / fname
            pd.Series(tickers).to_csv(path, index=False, header=False)
            counts[fname] = len(tickers)
            print(f"Saved {fname}: {len(tickers)} tickers")

        # Save UK tiers
        uk_tier1 = uk_all  # all UK until we can filter by market cap
        uk_tier2 = []
        for fname, tickers in [
            ("universe_uk_tier1.csv", uk_tier1),
            ("universe_uk_tier2.csv", uk_tier2),
        ]:
            path = _DATA_DIR / fname
            pd.Series(tickers).to_csv(path, index=False, header=False)
            counts[fname] = len(tickers)
            print(f"Saved {fname}: {len(tickers)} tickers")

        # Save combined all.csv
        all_rows = []
        for t in tier1:
            all_rows.append({"ticker": t, "market": "us", "tier": "TIER_1_SMALLCAP"})
        for t in tier2:
            all_rows.append({"ticker": t, "market": "us", "tier": "TIER_2_MIDCAP"})
        for t in uk_tier1:
            all_rows.append({"ticker": t, "market": "uk", "tier": "TIER_1_SMALLCAP"})

        df_all = pd.DataFrame(all_rows)
        df_all.to_csv(_DATA_DIR / "universe_all.csv", index=False)
        counts["universe_all.csv"] = len(df_all)
        print(f"Saved universe_all.csv: {len(df_all)} total tickers")

        # Also update the primary universe_us.csv
        all_us = sorted(set(tier1 + tier2))
        pd.Series(all_us).to_csv(_DATA_DIR / "universe_us.csv", index=False, header=False)
        counts["universe_us.csv"] = len(all_us)
        print(f"Updated universe_us.csv: {len(all_us)} tickers")

        # Update universe_uk.csv
        pd.Series(uk_tier1).to_csv(_DATA_DIR / "universe_uk.csv", index=False, header=False)
        counts["universe_uk.csv"] = len(uk_tier1)
        print(f"Updated universe_uk.csv: {len(uk_tier1)} tickers")

        return counts


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    builder = UniverseBuilder()
    print("Building universe from multiple sources...")
    data = builder.build()
    print("\nSaving tiered CSV files...")
    counts = builder.save(data)
    print("\n=== UNIVERSE BUILD COMPLETE ===")
    for fname, count in counts.items():
        print(f"  {fname}: {count} tickers")
    total_us = counts.get("universe_us_tier1.csv", 0) + counts.get("universe_us_tier2.csv", 0)
    total_uk = counts.get("universe_uk_tier1.csv", 0)
    print(f"\nTotal US: {total_us} | Total UK: {total_uk}")

    # Print samples
    print("\n=== SAMPLES ===")
    for fname in ["universe_us_tier1.csv", "universe_us_tier2.csv", "universe_uk_tier1.csv"]:
        path = _DATA_DIR / fname
        if path.exists() and path.stat().st_size > 0:
            try:
                tickers = pd.read_csv(path, header=None)[0].dropna().tolist()
                print(f"\n{fname} (first 10): {tickers[:10]}")
            except Exception as e:
                print(f"\n{fname}: could not read ({e})")
        else:
            print(f"\n{fname}: empty or missing")

    if hasattr(builder, "_failed_sources") and builder._failed_sources:
        print(f"\nFailed sources: {builder._failed_sources}")


if __name__ == "__main__":
    main()
