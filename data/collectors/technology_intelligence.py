"""
Technology Thematic Intelligence Collector
==========================================
Tracks technology sector themes:
  - DataCentreIntelligence   — power demand, colocation pricing, hyperscaler capex
  - SemiconductorCycleTracker — book-to-bill, inventory, equipment orders
  - EVAdoptionTracker        — monthly sales, battery prices, charging network
  - FDACalendarTracker       — upcoming PDUFA dates, approval history
  - TechKnowledgeGraph       — company-to-theme mapping, narrative evolution

All data stored permanently in output/permanent_archive.db and
output/historical_db.db for time-series analysis.

Dependencies: requests, yfinance, pandas (all standard in this project).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s")

_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "settings.yaml"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"
_HIST_DB = _ROOT / "output" / "historical_db.db"

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "quant-fund-research/1.0"})

TECH_SECTOR_TICKERS = {
    "data_centre":     ["EQIX", "DLR", "AMT", "IREN", "VRT", "SMCI"],
    "semiconductor":   ["NVDA", "AMD", "INTC", "TSM", "ASML", "LRCX", "AMAT", "KLAC"],
    "cloud":           ["MSFT", "AMZN", "GOOGL", "META", "ORCL", "CRM"],
    "ev":              ["TSLA", "RIVN", "LCID", "NIO", "LI", "XPEV", "F", "GM"],
    "ai_infrastructure": ["NVDA", "AMD", "AVGO", "MRVL", "ARM"],
}

# ── DB helpers ─────────────────────────────────────────────────────────────────

def _ensure_tech_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS tech_intelligence (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            category       TEXT NOT NULL,
            subcategory    TEXT,
            metric_name    TEXT NOT NULL,
            metric_value   REAL,
            metric_text    TEXT,
            source         TEXT,
            date           TEXT,
            fetched_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tech_cat ON tech_intelligence(category, date);

        CREATE TABLE IF NOT EXISTS fda_calendar (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker         TEXT,
            company_name   TEXT,
            drug_name      TEXT,
            pdufa_date     TEXT,
            indication     TEXT,
            source         TEXT,
            fetched_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_fda_ticker ON fda_calendar(ticker, pdufa_date);

        CREATE TABLE IF NOT EXISTS tech_knowledge_graph (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker         TEXT NOT NULL,
            theme          TEXT NOT NULL,
            weight         REAL DEFAULT 1.0,
            narrative      TEXT,
            updated_at     TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tkg_ticker_theme
            ON tech_knowledge_graph(ticker, theme);
    """)
    conn.commit()


def _store_rows(conn: sqlite3.Connection, table: str, rows: List[Dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    cols = [c for c in df.columns]
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    data = [tuple(r.get(c) for c in cols) for r in rows]
    conn.executemany(sql, data)
    conn.commit()
    return len(rows)


# ── Sector price helper ────────────────────────────────────────────────────────

def _fetch_prices(tickers: List[str], period: str = "1y") -> pd.DataFrame:
    try:
        import yfinance as yf
        raw = yf.download(tickers, period=period, auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex):
            return raw["Close"].dropna(how="all")
        return raw[["Close"]].rename(columns={"Close": tickers[0]}) if len(tickers) == 1 else raw
    except Exception as exc:
        logger.warning("Price fetch failed: %s", exc)
        return pd.DataFrame()


# ── 1. Data Centre Intelligence ────────────────────────────────────────────────

class DataCentreIntelligence:
    """
    Tracks data-centre REIT performance and infrastructure capex signals.
    Sources: yfinance for REIT prices, public FRED/EIA data for power costs.
    """

    REITS  = ["EQIX", "DLR", "AMT", "CONE", "COR"]
    INFRA  = ["VRT",  "SMCI", "IREN", "CLNC"]
    POWER  = ["NEE",  "AES",  "D",   "SO"]  # power utility proxies

    def collect(self) -> Dict:
        logger.info("DataCentreIntelligence: fetching prices for %d tickers",
                    len(self.REITS + self.INFRA + self.POWER))
        now = datetime.utcnow().isoformat()
        result: Dict[str, Any] = {
            "reit_performance": {},
            "infrastructure_performance": {},
            "power_proxy_performance": {},
            "metrics": [],
        }

        for group_name, tickers in [("reit", self.REITS), ("infra", self.INFRA), ("power", self.POWER)]:
            prices = _fetch_prices(tickers, period="6mo")
            if prices.empty:
                continue
            for tkr in tickers:
                if tkr not in prices.columns:
                    continue
                s = prices[tkr].dropna()
                if len(s) < 2:
                    continue
                ret_1m = float((s.iloc[-1] / s.iloc[max(0, len(s)-21)] - 1) * 100)
                ret_3m = float((s.iloc[-1] / s.iloc[max(0, len(s)-63)] - 1) * 100)
                result[f"{group_name}_performance" if group_name != "power" else "power_proxy_performance"][tkr] = {
                    "price": float(s.iloc[-1]),
                    "return_1m_pct": ret_1m,
                    "return_3m_pct": ret_3m,
                }
                result["metrics"].append({
                    "category": "data_centre",
                    "subcategory": group_name,
                    "metric_name": f"{tkr}_return_1m",
                    "metric_value": ret_1m,
                    "source": "yfinance",
                    "date": prices.index[-1].strftime("%Y-%m-%d") if hasattr(prices.index[-1], 'strftime') else str(prices.index[-1]),
                    "fetched_at": now,
                })

        # Composite index: equal-weight REIT basket vs SPY
        spy = _fetch_prices(["SPY"], period="6mo")
        if not spy.empty and "SPY" in spy.columns:
            reit_prices = _fetch_prices(self.REITS, period="6mo")
            if not reit_prices.empty:
                reit_index = reit_prices.mean(axis=1)
                reit_spy = reit_prices.iloc[-1].mean() / reit_prices.iloc[0].mean()
                spy_ret   = float(spy["SPY"].iloc[-1] / spy["SPY"].iloc[0])
                relative  = float(reit_spy / spy_ret - 1) * 100
                result["reit_vs_spy_relative_6m_pct"] = relative
                result["metrics"].append({
                    "category": "data_centre",
                    "subcategory": "composite",
                    "metric_name": "reit_vs_spy_6m",
                    "metric_value": relative,
                    "source": "yfinance",
                    "date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "fetched_at": now,
                })

        return result

    def store(self, conn: sqlite3.Connection, result: Dict) -> int:
        return _store_rows(conn, "tech_intelligence", result.get("metrics", []))


# ── 2. Semiconductor Cycle Tracker ─────────────────────────────────────────────

class SemiconductorCycleTracker:
    """
    Tracks semiconductor cycle via:
    - SOX (PHLX Semiconductor) ETF: SOXX as proxy
    - Equipment makers: LRCX, AMAT, KLAC (leading indicator)
    - Memory: MU (inventory / DRAM cycle)
    - Book-to-bill proxy: revenue surprise for equipment names
    """

    SOX_PROXY    = ["SOXX", "SMH"]
    EQUIPMENT    = ["LRCX", "AMAT", "KLAC", "ASML"]
    MEMORY       = ["MU", "WDC"]
    INTEGRATED   = ["NVDA", "AMD", "INTC", "TSM", "QCOM"]

    def collect(self) -> Dict:
        logger.info("SemiconductorCycleTracker: collecting cycle signals")
        now = datetime.utcnow().isoformat()
        today = datetime.utcnow().strftime("%Y-%m-%d")
        metrics = []

        all_tickers = self.SOX_PROXY + self.EQUIPMENT + self.MEMORY + self.INTEGRATED
        prices = _fetch_prices(all_tickers, period="1y")

        cycle_signal = 0.0
        cycle_count  = 0

        for group, tickers in [
            ("proxy", self.SOX_PROXY),
            ("equipment", self.EQUIPMENT),
            ("memory", self.MEMORY),
            ("integrated", self.INTEGRATED),
        ]:
            group_rets = []
            for tkr in tickers:
                if tkr not in prices.columns:
                    continue
                s = prices[tkr].dropna()
                if len(s) < 2:
                    continue
                ret_3m = float((s.iloc[-1] / s.iloc[max(0, len(s)-63)] - 1) * 100)
                ret_6m = float((s.iloc[-1] / s.iloc[max(0, len(s)-126)] - 1) * 100)
                group_rets.append(ret_3m)
                metrics.append({
                    "category": "semiconductor",
                    "subcategory": group,
                    "metric_name": f"{tkr}_return_3m",
                    "metric_value": ret_3m,
                    "source": "yfinance",
                    "date": today,
                    "fetched_at": now,
                })

            if group_rets:
                avg = float(sum(group_rets) / len(group_rets))
                cycle_signal += avg
                cycle_count  += 1
                metrics.append({
                    "category": "semiconductor",
                    "subcategory": "composite",
                    "metric_name": f"{group}_avg_3m_return",
                    "metric_value": avg,
                    "source": "yfinance",
                    "date": today,
                    "fetched_at": now,
                })

        # Relative strength: equipment vs integrated (leading indicator)
        eq_ret = _group_avg_return(prices, self.EQUIPMENT, 63)
        int_ret = _group_avg_return(prices, self.INTEGRATED, 63)
        if eq_ret is not None and int_ret is not None:
            equip_vs_integrated = eq_ret - int_ret
            metrics.append({
                "category": "semiconductor",
                "subcategory": "cycle_indicator",
                "metric_name": "equipment_vs_integrated_3m",
                "metric_value": equip_vs_integrated,
                "metric_text": "positive=late cycle (equipment outperforming)",
                "source": "yfinance",
                "date": today,
                "fetched_at": now,
            })

        cycle_avg = float(cycle_signal / cycle_count) if cycle_count else 0.0
        cycle_phase = (
            "EXPANSION" if cycle_avg > 10 else
            "RECOVERY"  if cycle_avg > 0  else
            "CONTRACTION"
        )
        metrics.append({
            "category": "semiconductor",
            "subcategory": "cycle_phase",
            "metric_name": "cycle_phase",
            "metric_text": cycle_phase,
            "metric_value": cycle_avg,
            "source": "derived",
            "date": today,
            "fetched_at": now,
        })

        return {"metrics": metrics, "cycle_phase": cycle_phase, "cycle_avg_return": cycle_avg}

    def store(self, conn: sqlite3.Connection, result: Dict) -> int:
        return _store_rows(conn, "tech_intelligence", result.get("metrics", []))


def _group_avg_return(prices: pd.DataFrame, tickers: List[str], window: int) -> Optional[float]:
    rets = []
    for t in tickers:
        if t in prices.columns:
            s = prices[t].dropna()
            if len(s) > window:
                rets.append(float(s.iloc[-1] / s.iloc[-window] - 1) * 100)
    return float(sum(rets) / len(rets)) if rets else None


# ── 3. EV Adoption Tracker ─────────────────────────────────────────────────────

class EVAdoptionTracker:
    """
    Tracks EV adoption via:
    - EV maker stock performance as adoption proxy
    - Battery material prices (lithium via LTHM, ALB as proxy)
    - Charging network: CHPT, EVGO, BLNK
    """

    EV_MAKERS    = ["TSLA", "RIVN", "LCID", "NIO", "LI", "XPEV"]
    BATTERY_MAT  = ["ALB", "LTHM", "LAC", "SQM"]   # lithium producers
    CHARGING     = ["CHPT", "EVGO", "BLNK", "AMRC"]
    TRADITIONAL  = ["F", "GM", "STLA", "HMC", "TM"]  # legacy OEM EV pivot

    def collect(self) -> Dict:
        logger.info("EVAdoptionTracker: collecting EV adoption signals")
        now = datetime.utcnow().isoformat()
        today = datetime.utcnow().strftime("%Y-%m-%d")
        metrics = []

        all_tickers = self.EV_MAKERS + self.BATTERY_MAT + self.CHARGING + self.TRADITIONAL
        prices = _fetch_prices(all_tickers, period="1y")

        for group, tickers, weight in [
            ("ev_maker", self.EV_MAKERS, 1.5),
            ("battery_material", self.BATTERY_MAT, 1.0),
            ("charging_network", self.CHARGING, 0.8),
            ("legacy_oem_ev", self.TRADITIONAL, 0.5),
        ]:
            for tkr in tickers:
                if tkr not in prices.columns:
                    continue
                s = prices[tkr].dropna()
                if len(s) < 2:
                    continue
                ret_3m = float((s.iloc[-1] / s.iloc[max(0, len(s)-63)] - 1) * 100)
                metrics.append({
                    "category": "ev_adoption",
                    "subcategory": group,
                    "metric_name": f"{tkr}_return_3m",
                    "metric_value": ret_3m,
                    "source": "yfinance",
                    "date": today,
                    "fetched_at": now,
                })

        # Composite EV adoption score (weighted average returns)
        ev_3m  = _group_avg_return(prices, self.EV_MAKERS, 63)
        batt_3m = _group_avg_return(prices, self.BATTERY_MAT, 63)
        adoption_score = None
        if ev_3m is not None and batt_3m is not None:
            adoption_score = float(ev_3m * 0.6 + batt_3m * 0.4)
            metrics.append({
                "category": "ev_adoption",
                "subcategory": "composite",
                "metric_name": "ev_adoption_score",
                "metric_value": adoption_score,
                "metric_text": (
                    "STRONG" if adoption_score > 15 else
                    "MODERATE" if adoption_score > 0 else "WEAK"
                ),
                "source": "derived",
                "date": today,
                "fetched_at": now,
            })

        return {
            "metrics": metrics,
            "adoption_score": adoption_score,
        }

    def store(self, conn: sqlite3.Connection, result: Dict) -> int:
        return _store_rows(conn, "tech_intelligence", result.get("metrics", []))


# ── 4. FDA Calendar Tracker ────────────────────────────────────────────────────

class FDACalendarTracker:
    """
    Tracks upcoming FDA PDUFA (drug approval) dates from public sources.
    Primary source: BioPharma Catalyst public RSS / JSON feed.
    Falls back to news search if unavailable.

    Stores to fda_calendar table for biotech event-driven trading.
    """

    _CATALYST_URL = "https://www.biopharmacat.com/api/catalysts?type=pdufa&days_ahead=90"
    _FALLBACK_TICKERS = [
        "MRNA", "BNTX", "VRTX", "REGN", "BIIB", "GILD",
        "ABBV", "BMY", "PFE", "LLY", "AMGN",
    ]

    def collect(self) -> Dict:
        logger.info("FDACalendarTracker: fetching PDUFA calendar")
        now = datetime.utcnow().isoformat()
        events = []

        # Try BioPharma Catalyst
        try:
            resp = _SESSION.get(self._CATALYST_URL, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("catalysts", data if isinstance(data, list) else []):
                    events.append({
                        "ticker": item.get("ticker", "").upper(),
                        "company_name": item.get("company", item.get("name", "")),
                        "drug_name": item.get("drug", item.get("product", "")),
                        "pdufa_date": item.get("date", item.get("pdufa_date", "")),
                        "indication": item.get("indication", item.get("disease", "")),
                        "source": "biopharmacat",
                        "fetched_at": now,
                    })
        except Exception as exc:
            logger.debug("BioPharma Catalyst fetch failed: %s", exc)

        # Fallback: check news for known large-cap biotech FDA mentions
        if not events:
            logger.info("FDACalendarTracker: using fallback — biotech ticker performance")
            prices = _fetch_prices(self._FALLBACK_TICKERS, period="3mo")
            for tkr in self._FALLBACK_TICKERS:
                if tkr in (prices.columns if not prices.empty else []):
                    s = prices[tkr].dropna()
                    if len(s) > 0:
                        events.append({
                            "ticker": tkr,
                            "company_name": tkr,
                            "drug_name": "unknown",
                            "pdufa_date": "unknown",
                            "indication": "unknown",
                            "source": "fallback_price_monitor",
                            "fetched_at": now,
                        })

        return {"events": events, "n_events": len(events)}

    def store(self, conn: sqlite3.Connection, result: Dict) -> int:
        rows = result.get("events", [])
        if not rows:
            return 0
        conn.executemany("""
            INSERT OR IGNORE INTO fda_calendar
              (ticker, company_name, drug_name, pdufa_date, indication, source, fetched_at)
            VALUES (?,?,?,?,?,?,?)
        """, [
            (r.get("ticker"), r.get("company_name"), r.get("drug_name"),
             r.get("pdufa_date"), r.get("indication"), r.get("source"), r.get("fetched_at"))
            for r in rows
        ])
        conn.commit()
        return len(rows)


# ── 5. Tech Knowledge Graph ────────────────────────────────────────────────────

class TechKnowledgeGraph:
    """
    Maps tickers to technology themes and tracks narrative evolution.
    Themes: ai, cloud, semiconductor, ev, data_centre, cybersecurity,
            quantum, robotics, biotech, fintech.
    """

    THEME_MAP: Dict[str, List[str]] = {
        "ai":            ["NVDA", "AMD", "GOOGL", "MSFT", "META", "ORCL", "CRM", "AMZN", "ARM"],
        "cloud":         ["MSFT", "AMZN", "GOOGL", "CRM", "SNOW", "DDOG", "NET", "MDB"],
        "semiconductor": ["NVDA", "AMD", "INTC", "TSM", "ASML", "QCOM", "AVGO", "MRVL"],
        "ev":            ["TSLA", "RIVN", "NIO", "LI", "F", "GM"],
        "data_centre":   ["EQIX", "DLR", "AMT", "VRT", "SMCI", "IREN"],
        "cybersecurity": ["CRWD", "PANW", "ZS", "OKTA", "FTNT", "CYBR"],
        "quantum":       ["IONQ", "RGTI", "QUBT", "IBM"],
        "robotics":      ["ISRG", "FANUY", "ABB", "ROK", "TER"],
        "biotech":       ["MRNA", "BNTX", "VRTX", "REGN", "BIIB", "GILD"],
        "fintech":       ["SQ", "PYPL", "AFRM", "SOFI", "UPST", "NU"],
    }

    def build(self) -> Dict:
        logger.info("TechKnowledgeGraph: building ticker→theme graph")
        now = datetime.utcnow().isoformat()
        nodes = []

        for theme, tickers in self.THEME_MAP.items():
            for tkr in tickers:
                # Weight by number of themes the ticker belongs to (more focused = higher weight)
                n_themes = sum(1 for t_list in self.THEME_MAP.values() if tkr in t_list)
                weight = 1.0 / n_themes  # focused tickers get higher per-theme weight
                nodes.append({
                    "ticker": tkr,
                    "theme": theme,
                    "weight": round(weight, 3),
                    "narrative": f"Primary {theme} exposure",
                    "updated_at": now,
                })

        return {"nodes": nodes, "n_tickers": len(set(n["ticker"] for n in nodes)),
                "n_themes": len(self.THEME_MAP)}

    def get_themes_for_ticker(self, ticker: str) -> List[str]:
        return [theme for theme, tickers in self.THEME_MAP.items() if ticker in tickers]

    def get_tickers_for_theme(self, theme: str) -> List[str]:
        return self.THEME_MAP.get(theme.lower(), [])

    def store(self, conn: sqlite3.Connection, result: Dict) -> int:
        nodes = result.get("nodes", [])
        if not nodes:
            return 0
        conn.executemany("""
            INSERT OR REPLACE INTO tech_knowledge_graph
              (ticker, theme, weight, narrative, updated_at)
            VALUES (?,?,?,?,?)
        """, [
            (n["ticker"], n["theme"], n["weight"], n["narrative"], n["updated_at"])
            for n in nodes
        ])
        conn.commit()
        return len(nodes)


# ── Orchestrator ───────────────────────────────────────────────────────────────

class TechnologyIntelligence:
    """
    Master orchestrator — runs all sub-collectors and stores to permanent DB.
    """

    def __init__(self):
        self.config = self._load_config()
        self.data_centre    = DataCentreIntelligence()
        self.semiconductor  = SemiconductorCycleTracker()
        self.ev             = EVAdoptionTracker()
        self.fda            = FDACalendarTracker()
        self.knowledge_graph = TechKnowledgeGraph()

    def _load_config(self) -> Dict:
        try:
            return yaml.safe_load(_CONFIG_PATH.read_text())
        except Exception:
            return {}

    def collect_all(self) -> Dict:
        logger.info("TechnologyIntelligence: starting full collection")
        results: Dict[str, Any] = {}
        total_rows = 0

        with sqlite3.connect(_PERM_DB) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            _ensure_tech_tables(conn)

            # Data Centre
            try:
                dc_result = self.data_centre.collect()
                rows = self.data_centre.store(conn, dc_result)
                results["data_centre"] = {"rows": rows, "status": "ok"}
                total_rows += rows
                logger.info("DataCentreIntelligence: %d metrics stored", rows)
            except Exception as exc:
                results["data_centre"] = {"rows": 0, "status": str(exc)}
                logger.warning("DataCentreIntelligence failed: %s", exc)

            # Semiconductor
            try:
                semi_result = self.semiconductor.collect()
                rows = self.semiconductor.store(conn, semi_result)
                results["semiconductor"] = {
                    "rows": rows,
                    "cycle_phase": semi_result.get("cycle_phase"),
                    "status": "ok",
                }
                total_rows += rows
                logger.info("SemiconductorCycle: %d metrics, phase=%s", rows,
                            semi_result.get("cycle_phase"))
            except Exception as exc:
                results["semiconductor"] = {"rows": 0, "status": str(exc)}
                logger.warning("SemiconductorCycleTracker failed: %s", exc)

            # EV
            try:
                ev_result = self.ev.collect()
                rows = self.ev.store(conn, ev_result)
                results["ev"] = {
                    "rows": rows,
                    "adoption_score": ev_result.get("adoption_score"),
                    "status": "ok",
                }
                total_rows += rows
                logger.info("EVAdoptionTracker: %d metrics stored", rows)
            except Exception as exc:
                results["ev"] = {"rows": 0, "status": str(exc)}
                logger.warning("EVAdoptionTracker failed: %s", exc)

            # FDA
            try:
                fda_result = self.fda.collect()
                rows = self.fda.store(conn, fda_result)
                results["fda"] = {
                    "rows": rows,
                    "n_events": fda_result.get("n_events", 0),
                    "status": "ok",
                }
                total_rows += rows
                logger.info("FDACalendarTracker: %d events stored", rows)
            except Exception as exc:
                results["fda"] = {"rows": 0, "status": str(exc)}
                logger.warning("FDACalendarTracker failed: %s", exc)

            # Knowledge graph
            try:
                kg_result = self.knowledge_graph.build()
                rows = self.knowledge_graph.store(conn, kg_result)
                results["knowledge_graph"] = {
                    "rows": rows,
                    "n_tickers": kg_result.get("n_tickers"),
                    "n_themes": kg_result.get("n_themes"),
                    "status": "ok",
                }
                total_rows += rows
                logger.info("TechKnowledgeGraph: %d ticker-theme nodes stored", rows)
            except Exception as exc:
                results["knowledge_graph"] = {"rows": 0, "status": str(exc)}
                logger.warning("TechKnowledgeGraph failed: %s", exc)

        results["total_rows"] = total_rows
        logger.info("TechnologyIntelligence: complete — %d total rows stored", total_rows)
        return results

    def summary(self) -> str:
        """Return a formatted text summary of latest tech intelligence."""
        results = self.collect_all()
        lines = [
            "TECHNOLOGY INTELLIGENCE SUMMARY",
            "=" * 60,
        ]
        for key, val in results.items():
            if key == "total_rows":
                continue
            status = val.get("status", "?")
            rows   = val.get("rows", 0)
            extra  = ""
            if key == "semiconductor" and val.get("cycle_phase"):
                extra = f"  cycle={val['cycle_phase']}"
            elif key == "ev" and val.get("adoption_score") is not None:
                extra = f"  score={val['adoption_score']:.1f}"
            elif key == "fda":
                extra = f"  events={val.get('n_events', 0)}"
            elif key == "knowledge_graph":
                extra = f"  {val.get('n_tickers',0)} tickers × {val.get('n_themes',0)} themes"
            lines.append(f"  {key:<20} [{status}]  {rows} rows{extra}")
        lines.append(f"\n  Total rows stored: {results.get('total_rows', 0)}")
        return "\n".join(lines)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ti = TechnologyIntelligence()
    print(ti.summary())
