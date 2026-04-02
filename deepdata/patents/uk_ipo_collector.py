"""
uk_ipo_collector.py — Collects patent data for UK-listed companies from UK IPO and USPTO.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

CACHE_DIR = Path("data/cache/deepdata")

# UK IPO open data portal endpoints (best-effort; availability varies)
UK_IPO_API_URL = "https://api.ipo.gov.uk/patents"
UK_IPO_IPSUM_URL = "https://www.ipo.gov.uk/p-ipsum.htm"
COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk"
PATENTS_VIEW_URL = "https://api.patentsview.org/patents/query"

UK_IPO_SLEEP = 1.5
USPTO_SLEEP = 1.0

COLLECTOR_SOURCE = "UK_IPO"


class UKIPOCollector:
    def __init__(self, config: dict):
        self.config = config
        self.companies_house_key = config.get("companies_house_api_key", "")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._company_cache: dict = self._load_cache("uk_company_names.json")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, tickers: list) -> list:
        """Collect for UK .L tickers only. Return CollectorResult list."""
        results = []
        uk_tickers = [t for t in tickers if t.endswith(".L") or t.endswith(".l")]

        for ticker in uk_tickers:
            try:
                company_name = self._get_company_name(ticker)
                if not company_name:
                    logger.warning("No company name for UK ticker %s, skipping", ticker)
                    continue

                uk_patents = self.fetch_uk_patents(company_name)
                us_filing = self.detect_us_patent_filing(ticker, company_name)

                # Build summary metrics
                patent_count = len(uk_patents)

                results.append(_make_collector_result(
                    source=COLLECTOR_SOURCE,
                    ticker=ticker,
                    market="uk",
                    data_type="uk_patent_activity",
                    value=float(patent_count),
                    raw_data={
                        "uk_patent_count": patent_count,
                        "us_filing_detected": us_filing,
                        "company_name": company_name,
                        "patents": uk_patents[:20],  # cap stored sample
                    },
                ))

            except Exception as exc:
                logger.warning("UK IPO collect failed for %s: %s", ticker, exc)

        return results

    # ------------------------------------------------------------------
    # UK patent fetch
    # ------------------------------------------------------------------

    def fetch_uk_patents(self, company_name: str) -> list:
        """
        Attempt to fetch UK patent data from:
        1. UK IPO API (https://api.ipo.gov.uk/patents) if available
        2. Companies House for R&D disclosures as fallback
        Returns list of {patent_number, date, title, status} dicts.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping UK IPO fetch")
            return []

        results = self._fetch_from_uk_ipo_api(company_name)
        if results:
            return results

        # Fallback: Companies House R&D disclosures
        results = self._fetch_from_companies_house(company_name)
        return results

    def _fetch_from_uk_ipo_api(self, company_name: str) -> list:
        """Try the UK IPO REST API endpoint."""
        results = []
        try:
            time.sleep(UK_IPO_SLEEP)
            params = {
                "applicant": company_name,
                "format": "json",
            }
            resp = requests.get(UK_IPO_API_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            patents = data.get("patents") or data.get("results") or []
            for p in patents:
                results.append({
                    "patent_number": p.get("applicationNumber") or p.get("patent_number", ""),
                    "date": p.get("filingDate") or p.get("date", ""),
                    "title": p.get("title") or p.get("patent_title", ""),
                    "status": p.get("status", ""),
                    "source": "UK_IPO_API",
                })
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (404, 501, 503):
                logger.debug("UK IPO API not available: %s", exc)
            else:
                logger.warning("UK IPO API error for '%s': %s", company_name, exc)
        except Exception as exc:
            logger.debug("UK IPO API unreachable for '%s': %s", company_name, exc)

        return results

    def _fetch_from_companies_house(self, company_name: str) -> list:
        """Fallback: search Companies House for R&D and patent disclosures."""
        results = []
        if not self.companies_house_key:
            logger.debug("No Companies House API key; skipping CH fallback")
            return results

        try:
            time.sleep(UK_IPO_SLEEP)
            search_url = f"{COMPANIES_HOUSE_API}/search/companies"
            resp = requests.get(
                search_url,
                params={"q": company_name},
                auth=(self.companies_house_key, ""),
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])

            for item in items[:3]:
                company_number = item.get("company_number", "")
                if not company_number:
                    continue

                # Fetch filing history
                time.sleep(UK_IPO_SLEEP)
                filing_url = f"{COMPANIES_HOUSE_API}/company/{company_number}/filing-history"
                filing_resp = requests.get(
                    filing_url,
                    auth=(self.companies_house_key, ""),
                    timeout=15,
                )
                filing_resp.raise_for_status()
                filings = filing_resp.json().get("items", [])

                for filing in filings:
                    description = filing.get("description", "").lower()
                    if "patent" in description or "intellectual property" in description:
                        results.append({
                            "patent_number": filing.get("transaction_id", ""),
                            "date": filing.get("date", ""),
                            "title": filing.get("description", ""),
                            "status": filing.get("type", ""),
                            "source": "COMPANIES_HOUSE",
                        })
        except Exception as exc:
            logger.warning("Companies House fallback failed for '%s': %s", company_name, exc)

        return results

    # ------------------------------------------------------------------
    # US patent detection (international expansion signal)
    # ------------------------------------------------------------------

    def detect_us_patent_filing(self, ticker: str, company_name: str) -> bool:
        """
        Check if UK company is filing US patents = international expansion signal.
        Uses USPTO PatentsView API.
        """
        if not HAS_REQUESTS:
            return False

        try:
            time.sleep(USPTO_SLEEP)
            payload = {
                "q": {"_contains": {"assignee_organization": company_name}},
                "f": ["patent_number", "patent_date"],
                "o": {"per_page": 5},
            }
            resp = requests.post(
                PATENTS_VIEW_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            patents = data.get("patents") or []
            return len(patents) > 0

        except Exception as exc:
            logger.warning("US patent detection failed for '%s': %s", company_name, exc)
            return False

    # ------------------------------------------------------------------
    # Cross-reference hiring
    # ------------------------------------------------------------------

    def cross_reference_hiring(
        self, ticker: str, patent_velocity: float, hiring_momentum: float
    ) -> dict:
        """
        UK companies: filing patents + hiring engineers = R&D phase signal.
        patent_velocity: ratio of recent vs prior patent filings.
        hiring_momentum: normalised hiring signal (e.g. from LinkedIn scrape or Indeed data).
        Returns {rd_phase: bool, signal_strength: float}
        """
        # Both signals elevated = R&D acceleration phase
        rd_phase = patent_velocity > 1.2 and hiring_momentum > 0.5

        # Signal strength: geometric mean scaled to [0, 1]
        if patent_velocity > 0 and hiring_momentum > 0:
            combined = (patent_velocity * hiring_momentum) ** 0.5
            signal_strength = min(combined / 2.0, 1.0)
        else:
            signal_strength = 0.0

        return {
            "rd_phase": rd_phase,
            "signal_strength": round(signal_strength, 4),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_company_name(self, ticker: str) -> str:
        """Get company name, using cache then yfinance."""
        ticker_upper = ticker.upper()
        if ticker_upper in self._company_cache:
            return self._company_cache[ticker_upper]

        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            name = info.get("longName") or info.get("shortName") or ""
            if name:
                self._company_cache[ticker_upper] = name
                self._save_cache("uk_company_names.json", self._company_cache)
            return name
        except ImportError:
            logger.warning("yfinance not available; cannot look up company name for %s", ticker)
        except Exception as exc:
            logger.warning("Company name lookup failed for %s: %s", ticker, exc)
        return ""

    def _load_cache(self, filename: str) -> dict:
        path = CACHE_DIR / filename
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception:
                pass
        return {}

    def _save_cache(self, filename: str, data: dict) -> None:
        try:
            (CACHE_DIR / filename).write_text(json.dumps(data, indent=2))
        except Exception as exc:
            logger.warning("Could not save cache %s: %s", filename, exc)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_collector_result(
    source: str,
    ticker: str,
    market: str,
    data_type: str,
    value: float,
    raw_data: dict,
    quality_score: float = 0.7,
) -> dict:
    return {
        "source": source,
        "ticker": ticker,
        "market": market,
        "data_type": data_type,
        "value": value,
        "raw_data": raw_data,
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
    }
