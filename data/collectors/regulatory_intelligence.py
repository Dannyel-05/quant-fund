"""
Regulatory Filing Intelligence
================================
Monitors SEC comment letters, FDA warning letters, EPA violations,
and OSHA violations for companies in our universe.

Classes:
  SECCommentLetterMonitor  — EDGAR comment letters
  FDAWarningLetterMonitor  — FDA warning letters RSS
  EPAViolationMonitor      — ECHO database violations
  OSHAViolationMonitor     — OSHA violations
  RegulatoryIntelligence   — orchestrator
"""

import csv
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from xml.etree import ElementTree

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
_UNIVERSE_CSV = _ROOT / "data" / "universe_us_tier1.csv"

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
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS regulatory_alerts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            company          TEXT,
            ticker           TEXT,
            alert_type       TEXT,
            alert_date       TEXT,
            alert_severity   TEXT,
            description      TEXT,
            source_url       TEXT,
            signal_generated TEXT,
            fetched_at       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ra_ticker
            ON regulatory_alerts(ticker, alert_date);
    """)
    conn.commit()


# ── Universe loader ───────────────────────────────────────────────────────────

def _load_universe_tickers() -> List[str]:
    """Load tickers from universe_us_tier1.csv (one ticker per line, no header)."""
    tickers: List[str] = []
    if not _UNIVERSE_CSV.exists():
        logger.warning("Universe CSV not found: %s", _UNIVERSE_CSV)
        return tickers
    try:
        with open(_UNIVERSE_CSV) as f:
            reader = csv.reader(f)
            for row in reader:
                if row:
                    tickers.append(row[0].strip())
    except Exception as exc:
        logger.warning("Could not load universe CSV: %s", exc)
    return tickers


# ── Fuzzy company-to-ticker matching ─────────────────────────────────────────

def _match_company_to_ticker(
    company_name: str,
    tickers: List[str],
) -> Optional[str]:
    """
    Simple fuzzy match:
      1. Check if any ticker is contained in the company name (case-insensitive).
      2. Check if first 4 chars of company name match a ticker.
    Returns the matched ticker or None.
    """
    name_upper = company_name.upper().strip()
    for ticker in tickers:
        t = ticker.upper()
        if t in name_upper:
            return ticker
    # first-4-char heuristic
    prefix = name_upper[:4]
    for ticker in tickers:
        if ticker.upper().startswith(prefix):
            return ticker
    return None


# ── Signal generation ─────────────────────────────────────────────────────────

def _determine_signal(alert_type: str, alert_count: int = 1) -> str:
    if alert_count > 1:
        return "STRONG_NEGATIVE"
    mapping = {
        "SEC_COMMENT":   "WARNING",
        "FDA_WARNING":   "SUPPRESS_LONG",
        "EPA_VIOLATION": "WARNING",
        "OSHA_VIOLATION":"WARNING",
    }
    return mapping.get(alert_type, "WARNING")


def _determine_severity(alert_type: str, context: str = "") -> str:
    if alert_type == "FDA_WARNING":
        return "HIGH"
    if alert_type == "SEC_COMMENT":
        repeat_keywords = ["going concern", "internal control", "related party"]
        if any(kw in context.lower() for kw in repeat_keywords):
            return "HIGH"
        return "MEDIUM"
    if alert_type == "EPA_VIOLATION":
        return "HIGH" if "multiple" in context.lower() else "MEDIUM"
    return "MEDIUM"


# ═══════════════════════════════════════════════════════════════════════════════
# SEC Comment Letter Monitor
# ═══════════════════════════════════════════════════════════════════════════════

class SECCommentLetterMonitor:
    """Fetches recent SEC EDGAR comment letters and matches to our universe."""

    _EDGAR_URL = (
        "https://efts.sec.gov/LATEST/search-index"
        "?q=%22comment+letter%22"
        "&dateRange=custom&startdt={start}&enddt={end}"
        "&forms=UPLOAD"
    )
    _FLAG_TOPICS = [
        "revenue recognition",
        "going concern",
        "related party",
        "internal control",
    ]

    def __init__(self, conn: sqlite3.Connection, tickers: List[str]) -> None:
        self.conn = conn
        self.tickers = tickers

    def collect(self) -> List[Dict[str, Any]]:
        today = datetime.utcnow().date()
        start = (today - timedelta(days=30)).isoformat()
        end = today.isoformat()
        url = self._EDGAR_URL.format(start=start, end=end)

        results: List[Dict[str, Any]] = []
        try:
            resp = requests.get(url, timeout=20)
            data = resp.json()
        except Exception as exc:
            logger.warning("SEC EDGAR fetch error: %s", exc)
            return results

        hits = data.get("hits", {}).get("hits", [])
        logger.info("SEC EDGAR returned %d hits", len(hits))
        now = datetime.utcnow().isoformat()

        for hit in hits:
            src = hit.get("_source", {})
            company_name = src.get("entity_name", src.get("display_names", [""])[0] if src.get("display_names") else "")
            filing_date = src.get("period_of_report", src.get("file_date", ""))
            description = src.get("form_type", "") + " " + src.get("file_num", "")
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={src.get('entity_id','')}"

            ticker = _match_company_to_ticker(company_name, self.tickers)
            if not ticker:
                continue

            # Detect flagged topics in description
            flag_found = any(t in description.lower() for t in self._FLAG_TOPICS)
            severity = "HIGH" if flag_found else "MEDIUM"
            signal = _determine_signal("SEC_COMMENT")

            try:
                self.conn.execute(
                    """INSERT INTO regulatory_alerts
                           (company, ticker, alert_type, alert_date, alert_severity,
                            description, source_url, signal_generated, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (company_name, ticker, "SEC_COMMENT", filing_date, severity,
                     description, source_url, signal, now),
                )
                results.append({
                    "company": company_name, "ticker": ticker, "type": "SEC_COMMENT",
                    "date": filing_date, "severity": severity, "signal": signal,
                })
            except Exception as exc:
                logger.debug("DB insert error: %s", exc)

        self.conn.commit()
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# FDA Warning Letter Monitor
# ═══════════════════════════════════════════════════════════════════════════════

class FDAWarningLetterMonitor:
    """Parses FDA warning letters RSS feed."""

    _RSS_URL = "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/warning-letters/rss.xml"

    def __init__(self, conn: sqlite3.Connection, tickers: List[str]) -> None:
        self.conn = conn
        self.tickers = tickers

    def collect(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()

        try:
            resp = requests.get(self._RSS_URL, timeout=20)
            root = ElementTree.fromstring(resp.content)
        except Exception as exc:
            logger.warning("FDA RSS fetch error: %s", exc)
            return results

        # RSS structure: rss > channel > item*
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        channel = root.find("channel")
        if channel is None:
            return results

        items = channel.findall("item")
        logger.info("FDA RSS returned %d items", len(items))

        for item in items:
            title_el = item.find("title")
            date_el = item.find("pubDate")
            link_el = item.find("link")
            desc_el = item.find("description")

            company_name = title_el.text.strip() if title_el is not None and title_el.text else ""
            alert_date = date_el.text.strip() if date_el is not None and date_el.text else ""
            source_url = link_el.text.strip() if link_el is not None and link_el.text else ""
            description = desc_el.text.strip() if desc_el is not None and desc_el.text else company_name

            ticker = _match_company_to_ticker(company_name, self.tickers)
            if not ticker:
                continue

            try:
                self.conn.execute(
                    """INSERT INTO regulatory_alerts
                           (company, ticker, alert_type, alert_date, alert_severity,
                            description, source_url, signal_generated, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (company_name, ticker, "FDA_WARNING", alert_date, "HIGH",
                     description, source_url, "SUPPRESS_LONG", now),
                )
                results.append({
                    "company": company_name, "ticker": ticker, "type": "FDA_WARNING",
                    "date": alert_date, "severity": "HIGH", "signal": "SUPPRESS_LONG",
                })
            except Exception as exc:
                logger.debug("DB insert error: %s", exc)

        self.conn.commit()
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# EPA Violation Monitor
# ═══════════════════════════════════════════════════════════════════════════════

class EPAViolationMonitor:
    """Fetches EPA ECHO database violations."""

    _ECHO_URL = (
        "https://echo.epa.gov/api/echo/cwa_rest_services"
        "?action=get_facilities&p_st=US&p_vio_flag=Y"
        "&p_date_last_vio_min={cutoff}&output=JSON"
    )

    def __init__(self, conn: sqlite3.Connection, tickers: List[str]) -> None:
        self.conn = conn
        self.tickers = tickers

    def collect(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()
        cutoff = (datetime.utcnow() - timedelta(days=90)).strftime("%m/%d/%Y")
        url = self._ECHO_URL.format(cutoff=cutoff)

        try:
            resp = requests.get(url, timeout=30)
            data = resp.json()
        except Exception as exc:
            logger.warning("EPA ECHO fetch error: %s", exc)
            return results

        facilities = (
            data.get("Results", {}).get("Facilities", [])
            or data.get("Results", {}).get("facilities", [])
            or []
        )
        logger.info("EPA ECHO returned %d facilities", len(facilities))

        for fac in facilities:
            company_name = fac.get("FacilityName", fac.get("FAC_NAME", ""))
            viol_cnt = fac.get("p_viol_cnt", fac.get("VIOL_CNT", 1))
            description = fac.get("ViolationDescription", fac.get("FAC_QTRS_WITH_NC", "EPA violation"))

            try:
                viol_cnt = int(viol_cnt)
            except (TypeError, ValueError):
                viol_cnt = 1

            ticker = _match_company_to_ticker(company_name, self.tickers)
            if not ticker:
                continue

            severity = "HIGH" if viol_cnt > 1 else "MEDIUM"
            signal = _determine_signal("EPA_VIOLATION", viol_cnt)

            try:
                self.conn.execute(
                    """INSERT INTO regulatory_alerts
                           (company, ticker, alert_type, alert_date, alert_severity,
                            description, source_url, signal_generated, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (company_name, ticker, "EPA_VIOLATION",
                     datetime.utcnow().date().isoformat(),
                     severity, str(description), self._ECHO_URL.split("?")[0],
                     signal, now),
                )
                results.append({
                    "company": company_name, "ticker": ticker, "type": "EPA_VIOLATION",
                    "date": datetime.utcnow().date().isoformat(),
                    "severity": severity, "signal": signal,
                })
            except Exception as exc:
                logger.debug("DB insert error: %s", exc)

        self.conn.commit()
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# OSHA Violation Monitor
# ═══════════════════════════════════════════════════════════════════════════════

class OSHAViolationMonitor:
    """
    Checks OSHA violations via News API search (primary) as the OSHA IMIS
    enforcement database does not expose a convenient public JSON API.
    Falls back gracefully if no API key.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        tickers: List[str],
        news_api_key: Optional[str] = None,
    ) -> None:
        self.conn = conn
        self.tickers = tickers
        self.news_api_key = news_api_key

    def _search_news(self, ticker: str) -> List[Dict[str, Any]]:
        if not self.news_api_key:
            return []
        try:
            url = (
                f"https://newsapi.org/v2/everything"
                f"?q={ticker}+OSHA+violation"
                f"&sortBy=publishedAt&pageSize=5&apiKey={self.news_api_key}"
            )
            resp = requests.get(url, timeout=15)
            return resp.json().get("articles", [])
        except Exception as exc:
            logger.debug("OSHA news search error for %s: %s", ticker, exc)
            return []

    def collect(self, tickers_to_check: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()
        check_list = tickers_to_check or self.tickers[:50]  # cap to avoid rate limits

        for ticker in check_list:
            articles = self._search_news(ticker)
            for art in articles:
                title = art.get("title", "")
                if "osha" not in title.lower() and "osha" not in art.get("description", "").lower():
                    continue
                alert_date = art.get("publishedAt", datetime.utcnow().date().isoformat())[:10]
                source_url = art.get("url", "")
                description = title

                try:
                    self.conn.execute(
                        """INSERT INTO regulatory_alerts
                               (company, ticker, alert_type, alert_date, alert_severity,
                                description, source_url, signal_generated, fetched_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (ticker, ticker, "OSHA_VIOLATION", alert_date, "MEDIUM",
                         description, source_url, "WARNING", now),
                    )
                    results.append({
                        "company": ticker, "ticker": ticker, "type": "OSHA_VIOLATION",
                        "date": alert_date, "severity": "MEDIUM", "signal": "WARNING",
                    })
                except Exception as exc:
                    logger.debug("DB insert error: %s", exc)

            time.sleep(0.2)  # polite rate limiting

        self.conn.commit()
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# RegulatoryIntelligence — Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

class RegulatoryIntelligence:
    """Master orchestrator for regulatory alert collection and signal generation."""

    def __init__(self) -> None:
        self.config = _load_config()
        self.conn = _get_conn()
        _init_db(self.conn)

        self.tickers = _load_universe_tickers()
        api_keys = self.config.get("api_keys", {})
        self.news_api_key: Optional[str] = api_keys.get("news_api") or None

        self.sec_monitor   = SECCommentLetterMonitor(self.conn, self.tickers)
        self.fda_monitor   = FDAWarningLetterMonitor(self.conn, self.tickers)
        self.epa_monitor   = EPAViolationMonitor(self.conn, self.tickers)
        self.osha_monitor  = OSHAViolationMonitor(self.conn, self.tickers, self.news_api_key)

    # ── collect_all ───────────────────────────────────────────────────────────

    def collect_all(self, tickers: Optional[List[str]] = None) -> Dict[str, Any]:
        if tickers:
            # Temporarily override monitors with restricted ticker list
            self.sec_monitor.tickers  = tickers
            self.fda_monitor.tickers  = tickers
            self.epa_monitor.tickers  = tickers
            self.osha_monitor.tickers = tickers

        summary: Dict[str, Any] = {
            "sec_comment_letters": [],
            "fda_warning_letters": [],
            "epa_violations":      [],
            "osha_violations":     [],
            "total_alerts":        0,
        }

        logger.info("Collecting SEC comment letters...")
        try:
            summary["sec_comment_letters"] = self.sec_monitor.collect()
        except Exception as exc:
            logger.warning("SEC monitor error: %s", exc)

        logger.info("Collecting FDA warning letters...")
        try:
            summary["fda_warning_letters"] = self.fda_monitor.collect()
        except Exception as exc:
            logger.warning("FDA monitor error: %s", exc)

        logger.info("Collecting EPA violations...")
        try:
            summary["epa_violations"] = self.epa_monitor.collect()
        except Exception as exc:
            logger.warning("EPA monitor error: %s", exc)

        logger.info("Collecting OSHA violations...")
        try:
            summary["osha_violations"] = self.osha_monitor.collect()
        except Exception as exc:
            logger.warning("OSHA monitor error: %s", exc)

        summary["total_alerts"] = (
            len(summary["sec_comment_letters"])
            + len(summary["fda_warning_letters"])
            + len(summary["epa_violations"])
            + len(summary["osha_violations"])
        )

        logger.info(
            "collect_all complete — total_alerts=%d (SEC=%d FDA=%d EPA=%d OSHA=%d)",
            summary["total_alerts"],
            len(summary["sec_comment_letters"]),
            len(summary["fda_warning_letters"]),
            len(summary["epa_violations"]),
            len(summary["osha_violations"]),
        )
        return summary

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_alerts_for_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
        rows = self.conn.execute(
            """SELECT id, company, ticker, alert_type, alert_date, alert_severity,
                      description, source_url, signal_generated, fetched_at
               FROM regulatory_alerts
               WHERE ticker = ? AND alert_date >= ?
               ORDER BY alert_date DESC""",
            (ticker, cutoff),
        ).fetchall()
        cols = ["id", "company", "ticker", "alert_type", "alert_date",
                "alert_severity", "description", "source_url", "signal_generated", "fetched_at"]
        return [dict(zip(cols, row)) for row in rows]

    def get_signal_modifier(self, ticker: str) -> float:
        """
        Returns a multiplier for signal strength:
          0.0 — suppress (FDA warning letter present)
          0.7 — reduce (SEC comment / EPA / OSHA warning)
          1.0 — clean (no alerts in last 90 days)
        """
        alerts = self.get_alerts_for_ticker(ticker)
        if not alerts:
            return 1.0

        signals = {a["signal_generated"] for a in alerts}
        if "SUPPRESS_LONG" in signals:
            return 0.0
        if "STRONG_NEGATIVE" in signals:
            return 0.0
        if "WARNING" in signals:
            return 0.7
        return 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(RegulatoryIntelligence().collect_all())
