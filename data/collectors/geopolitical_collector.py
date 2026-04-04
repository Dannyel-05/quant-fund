"""
STEP 5 — Geopolitical Collector

Collects geopolitical risk signals from:
  - GDELT Project API (global event data)
  - NewsAPI (high-impact keyword monitoring)
  - USGS Earthquake feed (infrastructure risk)
  - Hardcoded historical crisis database (1990–present)

Outputs GeopoliticalAlert objects and stores raw events in SQLite.
"""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "settings.yaml"
_PERM_DB = _ROOT / "output" / "permanent_archive.db"

# ── Config ────────────────────────────────────────────────────────────────────

def _load_config() -> Dict[str, Any]:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as exc:
        logger.warning("Could not load settings.yaml: %s", exc)
        return {}


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    _PERM_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_PERM_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    # raw_geopolitical_events: table may already exist with a different schema from
    # previous versions. CREATE TABLE IF NOT EXISTS is a no-op if it already exists,
    # so we use ALTER TABLE ADD COLUMN to add any missing columns.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_geopolitical_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT NOT NULL,
            event_date      TEXT NOT NULL,
            title           TEXT,
            description     TEXT,
            url             TEXT,
            goldstein_scale REAL,
            magnitude       REAL,
            latitude        REAL,
            longitude       REAL,
            location        TEXT,
            severity        TEXT,
            affected_sectors TEXT,
            affected_regions TEXT,
            raw_json        TEXT,
            collected_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    # Migrate: add raw_json column if the table pre-dates it
    existing = {r[1] for r in conn.execute("PRAGMA table_info(raw_geopolitical_events)").fetchall()}
    for col, typedef in [
        ("raw_json",     "TEXT"),
        ("goldstein_scale", "REAL"),
        ("affected_sectors", "TEXT"),
        ("affected_regions", "TEXT"),
        ("severity",     "TEXT"),
        ("location",     "TEXT"),
        ("title",        "TEXT"),
        ("url",          "TEXT"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE raw_geopolitical_events ADD COLUMN {col} {typedef}")
    # raw_articles is created by setup_permanent_archive with publication_date column
    # (not published_at). Do NOT re-create it here; just ensure it exists with the
    # correct schema so our INSERTs below work against whatever schema is present.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            url             TEXT,
            fetch_date      TEXT,
            source          TEXT,
            ticker_context  TEXT,
            full_text       TEXT,
            word_count      INTEGER,
            title           TEXT,
            author          TEXT,
            publication_date TEXT,
            is_paywalled    INTEGER DEFAULT 0,
            fetch_method    TEXT DEFAULT 'newsapi',
            all_tickers_mentioned   TEXT,
            all_companies_mentioned TEXT,
            sentiment_score REAL,
            article_type    TEXT
        );
    """)
    conn.commit()


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class GeopoliticalAlert:
    severity: str                        # LOW / MEDIUM / HIGH / CRITICAL
    description: str
    affected_sectors: List[str] = field(default_factory=list)
    affected_regions: List[str] = field(default_factory=list)
    signal_modifier: float = 1.0         # multiplier for PEAD signals
    source: str = ""
    event_date: str = ""
    url: str = ""

    # Convenience: convert to plain dict for JSON storage
    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity,
            "description": self.description,
            "affected_sectors": self.affected_sectors,
            "affected_regions": self.affected_regions,
            "signal_modifier": self.signal_modifier,
            "source": self.source,
            "event_date": self.event_date,
            "url": self.url,
        }


# ── Severity helpers ──────────────────────────────────────────────────────────

_SEVERITY_MODIFIER: Dict[str, float] = {
    "LOW": 0.95,
    "MEDIUM": 0.85,
    "HIGH": 0.70,
    "CRITICAL": 0.50,
}

_HIGH_IMPACT_KEYWORDS: List[str] = [
    "bank failure",
    "credit crunch",
    "liquidity crisis",
    "sovereign default",
    "sanctions",
    "tariffs announced",
    "trade war",
    "military escalation",
    "invasion",
    "missile strike",
    "ceasefire",
    "recession confirmed",
    "factory shutdown",
    "port closure",
    "supply shortage",
]

_KEYWORD_SEVERITY: Dict[str, str] = {
    "bank failure": "CRITICAL",
    "credit crunch": "HIGH",
    "liquidity crisis": "CRITICAL",
    "sovereign default": "CRITICAL",
    "sanctions": "HIGH",
    "tariffs announced": "MEDIUM",
    "trade war": "HIGH",
    "military escalation": "HIGH",
    "invasion": "CRITICAL",
    "missile strike": "CRITICAL",
    "ceasefire": "MEDIUM",
    "recession confirmed": "HIGH",
    "factory shutdown": "MEDIUM",
    "port closure": "HIGH",
    "supply shortage": "MEDIUM",
}

_KEYWORD_SECTORS: Dict[str, List[str]] = {
    "bank failure": ["financials", "real_estate", "insurance"],
    "credit crunch": ["financials", "real_estate", "consumer_discretionary"],
    "liquidity crisis": ["financials", "all"],
    "sovereign default": ["financials", "emerging_markets"],
    "sanctions": ["energy", "materials", "industrials", "financials"],
    "tariffs announced": ["industrials", "consumer_staples", "technology"],
    "trade war": ["industrials", "technology", "agriculture", "materials"],
    "military escalation": ["defense", "energy", "materials"],
    "invasion": ["defense", "energy", "materials", "financials"],
    "missile strike": ["defense", "energy"],
    "ceasefire": ["defense", "energy"],
    "recession confirmed": ["consumer_discretionary", "industrials", "financials"],
    "factory shutdown": ["industrials", "consumer_staples", "technology"],
    "port closure": ["industrials", "consumer_staples", "materials"],
    "supply shortage": ["consumer_staples", "industrials", "technology"],
}


def _classify_severity(text: str) -> str:
    text_lower = text.lower()
    worst = "LOW"
    order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    for kw, sev in _KEYWORD_SEVERITY.items():
        if kw in text_lower:
            if order.index(sev) > order.index(worst):
                worst = sev
    return worst


def _identify_sectors(text: str) -> List[str]:
    text_lower = text.lower()
    sectors: List[str] = []
    for kw, secs in _KEYWORD_SECTORS.items():
        if kw in text_lower:
            for s in secs:
                if s not in sectors:
                    sectors.append(s)
    return sectors


# ── Historical crisis database ────────────────────────────────────────────────

HISTORICAL_CRISES: List[Dict[str, Any]] = [
    {
        "name": "Asian Financial Crisis",
        "date": "1997-07-02",
        "type": "currency_crisis",
        "peak_drawdown": -0.60,
        "affected_sectors": ["financials", "emerging_markets", "real_estate"],
        "affected_regions": ["Asia", "Southeast Asia"],
        "recovery_time_days": 730,
        "description": "Currency collapse across Southeast Asia, starting with Thailand baht devaluation.",
    },
    {
        "name": "Dot-com Crash",
        "date": "2000-03-10",
        "type": "equity_bubble_burst",
        "peak_drawdown": -0.78,
        "affected_sectors": ["technology", "telecom", "consumer_discretionary"],
        "affected_regions": ["United States", "Global"],
        "recovery_time_days": 1440,
        "description": "Collapse of internet stock valuations; NASDAQ lost ~78% peak to trough.",
    },
    {
        "name": "September 11 Attacks",
        "date": "2001-09-11",
        "type": "geopolitical_shock",
        "peak_drawdown": -0.14,
        "affected_sectors": ["airlines", "defense", "insurance", "tourism"],
        "affected_regions": ["United States"],
        "recovery_time_days": 30,
        "description": "Terrorist attacks on US; markets closed 4 days, sharp short-term drawdown.",
    },
    {
        "name": "Iraq War",
        "date": "2003-03-20",
        "type": "military_conflict",
        "peak_drawdown": -0.15,
        "affected_sectors": ["energy", "defense", "airlines"],
        "affected_regions": ["Middle East", "Global"],
        "recovery_time_days": 180,
        "description": "US-led invasion of Iraq; oil price spike, short-term market uncertainty.",
    },
    {
        "name": "Global Financial Crisis",
        "date": "2008-09-15",
        "type": "financial_crisis",
        "peak_drawdown": -0.57,
        "affected_sectors": ["financials", "real_estate", "consumer_discretionary", "industrials"],
        "affected_regions": ["Global"],
        "recovery_time_days": 1200,
        "description": "Lehman Brothers collapse triggered global banking crisis and deep recession.",
    },
    {
        "name": "European Sovereign Debt Crisis",
        "date": "2011-07-01",
        "type": "sovereign_debt_crisis",
        "peak_drawdown": -0.22,
        "affected_sectors": ["financials", "real_estate", "consumer_discretionary"],
        "affected_regions": ["Europe", "Eurozone"],
        "recovery_time_days": 548,
        "description": "Greece, Portugal, Ireland, Spain debt crises threatened Eurozone integrity.",
    },
    {
        "name": "COVID-19 Pandemic",
        "date": "2020-02-20",
        "type": "pandemic",
        "peak_drawdown": -0.34,
        "affected_sectors": ["airlines", "tourism", "retail", "restaurants", "energy"],
        "affected_regions": ["Global"],
        "recovery_time_days": 180,
        "description": "Global pandemic caused fastest bear market in history; record stimulus response.",
    },
    {
        "name": "Russia-Ukraine War",
        "date": "2022-02-24",
        "type": "military_conflict",
        "peak_drawdown": -0.25,
        "affected_sectors": ["energy", "agriculture", "materials", "defense"],
        "affected_regions": ["Europe", "Russia", "Ukraine", "Global"],
        "recovery_time_days": 365,
        "description": "Russian invasion of Ukraine; energy and food price shocks, European security crisis.",
    },
    {
        "name": "Silicon Valley Bank Failure",
        "date": "2023-03-10",
        "type": "bank_failure",
        "peak_drawdown": -0.10,
        "affected_sectors": ["financials", "technology", "venture_capital"],
        "affected_regions": ["United States"],
        "recovery_time_days": 90,
        "description": "SVB collapse triggered regional bank contagion fears; Fed emergency backstop.",
    },
]


# ── GDELT collector ───────────────────────────────────────────────────────────

_GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
_GDELT_QUERIES = [
    "sanctions",
    "tariffs",
    "war",
    "invasion",
    "ceasefire",
    "embargo",
    "default",
    "pandemic",
]


def _fetch_gdelt(conn: sqlite3.Connection) -> List[GeopoliticalAlert]:
    alerts: List[GeopoliticalAlert] = []
    for query in _GDELT_QUERIES:
        try:
            params = {
                "query": query,
                "mode": "artlist",
                "maxrecords": 250,
                "format": "json",
            }
            # Retry with exponential backoff to handle GDELT 429 rate limits
            resp = None
            for attempt in range(4):
                try:
                    resp = requests.get(_GDELT_BASE, params=params, timeout=20)
                    if resp.status_code == 429:
                        wait = 2 ** attempt * 5  # 5s, 10s, 20s, 40s
                        logger.warning("GDELT 429 for '%s', retrying in %ds", query, wait)
                        time.sleep(wait)
                        continue
                    break
                except requests.exceptions.RequestException as _re:
                    if attempt < 3:
                        time.sleep(2 ** attempt * 3)
                    else:
                        raise
            if resp is None or resp.status_code == 429:
                logger.error("GDELT query '%s' rate-limited after retries; skipping", query)
                time.sleep(5)
                continue
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            logger.info("GDELT query '%s': %d articles", query, len(articles))
            for art in articles:
                title = art.get("title", "")
                url = art.get("url", "")
                seendate = art.get("seendate", "")
                domain = art.get("domain", "")

                # Try to get full text (best-effort)
                full_text = ""
                if url:
                    try:
                        tr = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                        if tr.ok:
                            # Store raw HTML; NLP/parsing is downstream responsibility
                            full_text = tr.text[:4000]
                    except Exception:
                        pass

                severity = _classify_severity(title)
                sectors = _identify_sectors(title)
                modifier = _SEVERITY_MODIFIER.get(severity, 1.0)

                conn.execute(
                    """INSERT OR IGNORE INTO raw_geopolitical_events
                       (source, event_date, title, url, severity, affected_sectors,
                        affected_regions, raw_json, goldstein_scale)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        "gdelt",
                        seendate,
                        title,
                        url,
                        severity,
                        json.dumps(sectors),
                        json.dumps([domain]),
                        json.dumps(art),
                        None,
                    ),
                )

                if severity in ("HIGH", "CRITICAL"):
                    alerts.append(
                        GeopoliticalAlert(
                            severity=severity,
                            description=title,
                            affected_sectors=sectors,
                            affected_regions=[domain],
                            signal_modifier=modifier,
                            source="gdelt",
                            event_date=seendate,
                            url=url,
                        )
                    )
            conn.commit()
            time.sleep(1)  # Be polite to GDELT
        except Exception as exc:
            logger.error("GDELT query '%s' failed: %s", query, exc)
    return alerts


# ── NewsAPI collector ─────────────────────────────────────────────────────────

_NEWSAPI_URL = "https://newsapi.org/v2/everything"


def _fetch_newsapi(conn: sqlite3.Connection, api_key: str) -> List[GeopoliticalAlert]:
    if not api_key:
        logger.warning("NewsAPI key not configured; skipping.")
        return []

    alerts: List[GeopoliticalAlert] = []
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    for keyword in _HIGH_IMPACT_KEYWORDS:
        try:
            params = {
                "q": f'"{keyword}"',
                "from": from_date,
                "sortBy": "publishedAt",
                "pageSize": 100,
                "language": "en",
                "apiKey": api_key,
            }
            resp = requests.get(_NEWSAPI_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            logger.info("NewsAPI keyword '%s': %d articles", keyword, len(articles))

            for art in articles:
                title = art.get("title", "") or ""
                description = art.get("description", "") or ""
                content = art.get("content", "") or ""
                url = art.get("url", "") or ""
                published_at = art.get("publishedAt", "")
                author = art.get("author", "")
                combined_text = f"{title} {description} {content}"

                severity = _classify_severity(combined_text)
                sectors = _identify_sectors(combined_text)
                modifier = _SEVERITY_MODIFIER.get(severity, 1.0)

                try:
                    # Use permanent_archive schema (publication_date, not published_at).
                    # Combine description + content into full_text; store severity in
                    # article_type so geopolitical context is not lost.
                    full_text = f"{description}\n{content}".strip()
                    conn.execute(
                        """INSERT OR IGNORE INTO raw_articles
                           (source, publication_date, title, full_text, url,
                            author, article_type, fetch_date, fetch_method)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            "newsapi_geopolitical",
                            published_at,
                            title,
                            full_text,
                            url,
                            author,
                            f"geopolitical|{severity}|{','.join(sectors[:3])}",
                            datetime.utcnow().strftime("%Y-%m-%d"),
                            "newsapi",
                        ),
                    )
                except sqlite3.IntegrityError:
                    pass  # Duplicate URL

                if severity in ("HIGH", "CRITICAL"):
                    alerts.append(
                        GeopoliticalAlert(
                            severity=severity,
                            description=title,
                            affected_sectors=sectors,
                            affected_regions=[],
                            signal_modifier=modifier,
                            source="newsapi",
                            event_date=published_at,
                            url=url,
                        )
                    )
            conn.commit()
            time.sleep(0.5)  # Stay within rate limits
        except Exception as exc:
            logger.error("NewsAPI keyword '%s' failed: %s", keyword, exc)

    return alerts


# ── USGS Earthquake collector ─────────────────────────────────────────────────

_USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

# Rough mapping of regions near major populated/industrial areas
_POPULATED_REGIONS = {
    "Japan": {"lat": (30, 46), "lon": (129, 146), "sectors": ["technology", "industrials", "auto"]},
    "California": {"lat": (32, 42), "lon": (-125, -114), "sectors": ["technology", "agriculture"]},
    "Indonesia": {"lat": (-10, 6), "lon": (95, 141), "sectors": ["materials", "agriculture", "energy"]},
    "Turkey": {"lat": (36, 42), "lon": (26, 45), "sectors": ["industrials", "tourism", "financials"]},
    "Italy": {"lat": (36, 47), "lon": (7, 19), "sectors": ["industrials", "tourism", "consumer_staples"]},
    "Chile": {"lat": (-56, -17), "lon": (-76, -66), "sectors": ["materials", "agriculture", "energy"]},
    "Mexico": {"lat": (14, 33), "lon": (-118, -86), "sectors": ["industrials", "agriculture", "energy"]},
}


def _magnitude_to_severity(mag: float) -> str:
    if mag >= 8.0:
        return "CRITICAL"
    if mag >= 7.0:
        return "HIGH"
    if mag >= 6.0:
        return "MEDIUM"
    return "LOW"


def _check_populated(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    for region, bounds in _POPULATED_REGIONS.items():
        lat_ok = bounds["lat"][0] <= lat <= bounds["lat"][1]
        lon_ok = bounds["lon"][0] <= lon <= bounds["lon"][1]
        if lat_ok and lon_ok:
            return {"region": region, "sectors": bounds["sectors"]}
    return None


def _fetch_usgs(conn: sqlite3.Connection) -> List[GeopoliticalAlert]:
    alerts: List[GeopoliticalAlert] = []
    try:
        resp = requests.get(_USGS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        logger.info("USGS: %d earthquake features", len(features))

        for feat in features:
            props = feat.get("properties", {})
            mag = props.get("mag") or 0.0
            if mag < 3.0:
                continue

            place = props.get("place", "")
            time_ms = props.get("time") or 0
            event_dt = datetime.utcfromtimestamp(time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if time_ms else ""
            url = props.get("url", "")

            coords = feat.get("geometry", {}).get("coordinates", [None, None, None])
            lon_val = coords[0]
            lat_val = coords[1]

            severity = _magnitude_to_severity(mag)
            populated = _check_populated(lat_val or 0, lon_val or 0)
            sectors = populated["sectors"] if populated else []
            regions = [populated["region"]] if populated else []

            if mag >= 6.0:
                modifier = _SEVERITY_MODIFIER.get(severity, 1.0)
                desc = f"M{mag:.1f} earthquake near {place}"
                conn.execute(
                    """INSERT OR IGNORE INTO raw_geopolitical_events
                       (source, event_date, title, url, magnitude, latitude, longitude,
                        location, severity, affected_sectors, affected_regions, raw_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        "usgs",
                        event_dt,
                        desc,
                        url,
                        mag,
                        lat_val,
                        lon_val,
                        place,
                        severity,
                        json.dumps(sectors),
                        json.dumps(regions),
                        json.dumps(props),
                    ),
                )
                conn.commit()

                if populated:
                    alerts.append(
                        GeopoliticalAlert(
                            severity=severity,
                            description=desc,
                            affected_sectors=sectors,
                            affected_regions=regions,
                            signal_modifier=modifier,
                            source="usgs",
                            event_date=event_dt,
                            url=url,
                        )
                    )
    except Exception as exc:
        logger.error("USGS fetch failed: %s", exc)

    return alerts


# ── Main class ────────────────────────────────────────────────────────────────

class GeopoliticalCollector:
    """
    Collects and aggregates geopolitical risk data from multiple sources.
    """

    def __init__(self, config=None):
        self._config = config if isinstance(config, dict) else _load_config()
        self._api_keys = self._config.get("api_keys", {})
        self._alerts: List[GeopoliticalAlert] = []

    # ── Public API ─────────────────────────────────────────────────────────

    def collect(self, market=None, **kwargs) -> List[GeopoliticalAlert]:
        """
        Run all collections. Returns list of GeopoliticalAlert objects.
        Failures in any source are logged and skipped.
        """
        conn = _get_conn()
        try:
            _init_db(conn)
            all_alerts: List[GeopoliticalAlert] = []

            # 1. GDELT
            logger.info("=== Collecting GDELT data ===")
            try:
                gdelt_alerts = _fetch_gdelt(conn)
                all_alerts.extend(gdelt_alerts)
                logger.info("GDELT: %d HIGH/CRITICAL alerts", len(gdelt_alerts))
            except Exception as exc:
                logger.error("GDELT collection error: %s", exc)

            # 2. NewsAPI
            logger.info("=== Collecting NewsAPI data ===")
            try:
                news_key = self._api_keys.get("news_api", "")
                news_alerts = _fetch_newsapi(conn, news_key)
                all_alerts.extend(news_alerts)
                logger.info("NewsAPI: %d HIGH/CRITICAL alerts", len(news_alerts))
            except Exception as exc:
                logger.error("NewsAPI collection error: %s", exc)

            # 3. USGS
            logger.info("=== Collecting USGS earthquake data ===")
            try:
                usgs_alerts = _fetch_usgs(conn)
                all_alerts.extend(usgs_alerts)
                logger.info("USGS: %d HIGH/CRITICAL alerts", len(usgs_alerts))
            except Exception as exc:
                logger.error("USGS collection error: %s", exc)

            self._alerts = all_alerts
            logger.info("Total geopolitical alerts collected: %d", len(all_alerts))
            return all_alerts

        finally:
            conn.close()

    def get_current_risk_level(self) -> str:
        """
        Returns overall risk level based on active alerts.
        CRITICAL > HIGH > MEDIUM > LOW
        """
        if not self._alerts:
            return "LOW"
        order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
        worst = max(self._alerts, key=lambda a: order.get(a.severity, 0))
        return worst.severity

    def get_sector_modifiers(self) -> Dict[str, float]:
        """
        Returns dict of sector -> combined signal_modifier.
        Multiple alerts affecting the same sector compound multiplicatively.
        """
        modifiers: Dict[str, float] = {}
        for alert in self._alerts:
            for sector in alert.affected_sectors:
                current = modifiers.get(sector, 1.0)
                modifiers[sector] = current * alert.signal_modifier
        return modifiers

    def get_alerts(self, severity: Optional[str] = None) -> List[GeopoliticalAlert]:
        """
        Returns active alerts, optionally filtered by severity level.
        severity: None (all), 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
        """
        if severity is None:
            return list(self._alerts)
        return [a for a in self._alerts if a.severity == severity]

    def get_historical_crisis(self, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Returns historical crisis records, optionally filtered by event_type.
        event_types: financial_crisis, currency_crisis, military_conflict,
                     pandemic, bank_failure, sovereign_debt_crisis, geopolitical_shock,
                     equity_bubble_burst
        """
        if event_type is None:
            return list(HISTORICAL_CRISES)
        return [c for c in HISTORICAL_CRISES if c.get("type") == event_type]

    def get_summary(self) -> Dict[str, Any]:
        """Returns a summary dict of the current geopolitical risk state."""
        return {
            "overall_risk_level": self.get_current_risk_level(),
            "total_alerts": len(self._alerts),
            "critical_alerts": len(self.get_alerts("CRITICAL")),
            "high_alerts": len(self.get_alerts("HIGH")),
            "medium_alerts": len(self.get_alerts("MEDIUM")),
            "low_alerts": len(self.get_alerts("LOW")),
            "sector_modifiers": self.get_sector_modifiers(),
        }


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint

    collector = GeopoliticalCollector()

    print("\n" + "=" * 70)
    print("STEP 5 — GEOPOLITICAL COLLECTOR")
    print("=" * 70)

    alerts = collector.collect()

    summary = collector.get_summary()
    print("\n--- Summary ---")
    pprint.pprint(summary)

    print("\n--- CRITICAL Alerts ---")
    for a in collector.get_alerts("CRITICAL")[:5]:
        print(f"  [{a.source.upper()}] {a.description[:100]}")
        print(f"    Sectors: {a.affected_sectors}  Modifier: {a.signal_modifier:.2f}")

    print("\n--- HIGH Alerts ---")
    for a in collector.get_alerts("HIGH")[:5]:
        print(f"  [{a.source.upper()}] {a.description[:100]}")

    print("\n--- Sector Modifiers ---")
    pprint.pprint(collector.get_sector_modifiers())

    print("\n--- Historical Crises (Financial) ---")
    for c in collector.get_historical_crisis("financial_crisis"):
        print(f"  {c['name']} ({c['date']}): peak drawdown {c['peak_drawdown']:.0%}, "
              f"recovery {c['recovery_time_days']} days")

    print(f"\nData stored in: {_PERM_DB}")
    print("=" * 70)
