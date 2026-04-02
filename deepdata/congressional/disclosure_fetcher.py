"""
CongressionalDisclosureFetcher — fetches congressional trading disclosures.
Primary: Capitol Trades API / scrape. Backup: House Financial Disclosures.
"""
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

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

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

AMOUNT_RANGES = {
    "$1,001 - $15,000": (1001, 15000),
    "$15,001 - $50,000": (15001, 50000),
    "$50,001 - $100,000": (50001, 100000),
    "$100,001 - $250,000": (100001, 250000),
    "$250,001 - $500,000": (250001, 500000),
    "$500,001 - $1,000,000": (500001, 1000000),
    "$1,000,001 - $5,000,000": (1000001, 5000000),
    "Over $5,000,000": (5000001, 10000000),
}

BUY_TYPES = {"purchase", "buy", "exercise", "received", "exchange"}
SELL_TYPES = {"sale (full)", "sale (partial)", "sale", "sell"}

CAPITOL_TRADES_BASE = "https://www.capitoltrades.com"
HOUSE_DISCLOSURES_BASE = "https://disclosures.house.gov"

REQUEST_DELAY = 2.0  # seconds between requests


class CongressionalDisclosureFetcher:
    """
    Fetches and parses congressional trading disclosures from multiple sources.
    """

    def __init__(self, config: dict):
        self.config = config
        cd_config = config.get("deepdata", {}).get("congressional", {})
        self.request_delay = cd_config.get("request_delay", REQUEST_DELAY)
        self.timeout = cd_config.get("timeout", 20)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; QuantFund/1.0; research@quantfund.example.com)"
            )
        }
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def fetch_recent(self, days_back: int = 30) -> list:
        """
        Fetch recent congressional disclosures.
        Primary: Capitol Trades. Backup: House Financial Disclosures.

        Returns list of disclosure dicts:
        {member, chamber, committee, ticker, transaction_type,
         amount_min, amount_max, transaction_date, filing_date, delay_days}
        """
        disclosures = []

        try:
            ct_disclosures = self.fetch_capitol_trades(days_back=days_back)
            if ct_disclosures:
                logger.info("Capitol Trades returned %d disclosures", len(ct_disclosures))
                disclosures.extend(ct_disclosures)
        except Exception as exc:
            logger.warning("Capitol Trades fetch failed: %s", exc)

        if not disclosures:
            try:
                house_disclosures = self.fetch_house_disclosures()
                if house_disclosures:
                    logger.info("House disclosures returned %d records", len(house_disclosures))
                    disclosures.extend(house_disclosures)
            except Exception as exc:
                logger.warning("House disclosures fetch failed: %s", exc)

        # Filter by recency
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
        recent = [
            d for d in disclosures
            if d.get("transaction_date", "9999") >= cutoff
            or d.get("filing_date", "9999") >= cutoff
        ]

        return recent

    def fetch_capitol_trades(self, days_back: int = 30) -> list:
        """
        Scrape Capitol Trades public trades page.
        Rate limit: 2s between requests.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping Capitol Trades")
            return []

        disclosures = []

        try:
            url = f"{CAPITOL_TRADES_BASE}/trades"
            time.sleep(self.request_delay)
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            resp.raise_for_status()
            disclosures.extend(self._parse_capitol_trades_html(resp.text))
        except Exception as exc:
            logger.warning("Capitol Trades scrape failed: %s", exc)

        # Try JSON API endpoint if available
        try:
            api_url = f"{CAPITOL_TRADES_BASE}/api/trades?limit=200"
            time.sleep(self.request_delay)
            resp = requests.get(api_url, headers=self.headers, timeout=self.timeout)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, list):
                        disclosures.extend(self._normalise_ct_json(data))
                    elif isinstance(data, dict):
                        items = data.get("trades", data.get("data", data.get("results", [])))
                        disclosures.extend(self._normalise_ct_json(items))
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Capitol Trades API failed: %s", exc)

        return disclosures

    def _parse_capitol_trades_html(self, html: str) -> list:
        """Parse Capitol Trades HTML page for trade data."""
        disclosures = []
        if not HAS_BS4:
            # Fallback regex parse
            return self._regex_parse_ct(html)

        try:
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.select("table tbody tr") or soup.select(".trade-row")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue
                try:
                    disclosure = self._parse_ct_row(cells)
                    if disclosure:
                        disclosures.append(disclosure)
                except Exception:
                    continue
        except Exception as exc:
            logger.warning("BeautifulSoup CT parse failed: %s", exc)

        return disclosures

    def _parse_ct_row(self, cells) -> dict:
        """Parse a single Capitol Trades table row."""
        try:
            texts = [c.get_text(strip=True) for c in cells]
            if len(texts) < 6:
                return {}
            member = texts[0] if len(texts) > 0 else ""
            ticker_raw = texts[1] if len(texts) > 1 else ""
            ticker = self.normalize_ticker(ticker_raw)
            tx_type = texts[2].lower() if len(texts) > 2 else ""
            amount_str = texts[3] if len(texts) > 3 else ""
            tx_date = texts[4] if len(texts) > 4 else ""
            filing_date = texts[5] if len(texts) > 5 else ""

            if not ticker or not member:
                return {}

            amount_parsed = self.parse_amount_range(amount_str)
            delay_days = self._calc_delay(tx_date, filing_date)

            return {
                "member": member,
                "chamber": "House",
                "committee": "",
                "ticker": ticker,
                "transaction_type": tx_type,
                "amount_min": amount_parsed.get("min", 0),
                "amount_max": amount_parsed.get("max", 0),
                "transaction_date": tx_date,
                "filing_date": filing_date,
                "delay_days": delay_days,
                "source": "capitol_trades",
            }
        except Exception:
            return {}

    def _regex_parse_ct(self, html: str) -> list:
        """Basic regex fallback for Capitol Trades HTML."""
        disclosures = []
        ticker_pattern = re.compile(r'\b([A-Z]{1,5})\b')
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        amount_pattern = re.compile(r'\$[\d,]+\s*-\s*\$[\d,]+|Over\s+\$[\d,]+')

        tickers = ticker_pattern.findall(html)
        dates = date_pattern.findall(html)
        amounts = amount_pattern.findall(html)

        for i, ticker in enumerate(tickers[:100]):
            if len(ticker) < 1 or len(ticker) > 5:
                continue
            tx_date = dates[i] if i < len(dates) else ""
            filing_date = dates[i + 1] if i + 1 < len(dates) else tx_date
            amount_str = amounts[i] if i < len(amounts) else ""
            amount_parsed = self.parse_amount_range(amount_str)
            disclosures.append({
                "member": "Unknown",
                "chamber": "Unknown",
                "committee": "",
                "ticker": ticker,
                "transaction_type": "unknown",
                "amount_min": amount_parsed.get("min", 0),
                "amount_max": amount_parsed.get("max", 0),
                "transaction_date": tx_date,
                "filing_date": filing_date,
                "delay_days": 0,
                "source": "capitol_trades_regex",
            })

        return disclosures

    def _normalise_ct_json(self, items: list) -> list:
        """Normalise Capitol Trades JSON response items."""
        disclosures = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                ticker_raw = (
                    item.get("ticker", "")
                    or item.get("asset_ticker", "")
                    or item.get("symbol", "")
                )
                ticker = self.normalize_ticker(str(ticker_raw))
                if not ticker:
                    continue

                member = (
                    item.get("politician", {}).get("name", "")
                    if isinstance(item.get("politician"), dict)
                    else item.get("politician", item.get("member", "Unknown"))
                )
                tx_type = (item.get("type", "") or item.get("transaction_type", "")).lower()
                amount_str = item.get("amount", item.get("amount_range", ""))
                tx_date = item.get("date", item.get("transaction_date", ""))
                filing_date = item.get("published", item.get("filing_date", tx_date))

                amount_parsed = self.parse_amount_range(str(amount_str))
                delay_days = self._calc_delay(str(tx_date), str(filing_date))

                disclosures.append({
                    "member": str(member),
                    "chamber": item.get("chamber", "Unknown"),
                    "committee": "",
                    "ticker": ticker,
                    "transaction_type": tx_type,
                    "amount_min": amount_parsed.get("min", 0),
                    "amount_max": amount_parsed.get("max", 0),
                    "transaction_date": str(tx_date),
                    "filing_date": str(filing_date),
                    "delay_days": delay_days,
                    "source": "capitol_trades_api",
                })
            except Exception as exc:
                logger.warning("Error normalising CT item: %s", exc)
                continue

        return disclosures

    def fetch_house_disclosures(self) -> list:
        """
        Fetch House Financial Disclosures from disclosures.house.gov.
        Returns list of disclosure dicts.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping House disclosures")
            return []

        disclosures = []
        try:
            url = f"{HOUSE_DISCLOSURES_BASE}/FinancialDisclosure/ViewMemberSearchResult"
            time.sleep(self.request_delay)
            resp = requests.post(
                url,
                data={"LastName": "", "State": "", "District": "", "FilingYear": ""},
                headers=self.headers,
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                logger.warning("House disclosures returned status %d", resp.status_code)
                return []

            disclosures.extend(self._parse_house_html(resp.text))
        except Exception as exc:
            logger.warning("House disclosures fetch failed: %s", exc)

        return disclosures

    def _parse_house_html(self, html: str) -> list:
        """Parse House Financial Disclosure HTML response."""
        disclosures = []
        if not HAS_BS4:
            return disclosures

        try:
            soup = BeautifulSoup(html, "html.parser")
            links = soup.select("a[href*='FilingID']")
            for link in links[:50]:
                href = link.get("href", "")
                member_name = link.get_text(strip=True)
                row = link.find_parent("tr")
                if row:
                    cells = row.find_all("td")
                    texts = [c.get_text(strip=True) for c in cells]
                    if len(texts) >= 3:
                        disclosures.append({
                            "member": member_name or texts[0],
                            "chamber": "House",
                            "committee": "",
                            "ticker": "",
                            "transaction_type": "filing",
                            "amount_min": 0,
                            "amount_max": 0,
                            "transaction_date": texts[2] if len(texts) > 2 else "",
                            "filing_date": texts[2] if len(texts) > 2 else "",
                            "delay_days": 0,
                            "source": "house_disclosures",
                            "filing_url": f"{HOUSE_DISCLOSURES_BASE}{href}",
                        })
        except Exception as exc:
            logger.warning("House HTML parse failed: %s", exc)

        return disclosures

    def filter_universe(self, disclosures: list, universe_tickers: list) -> list:
        """Return only disclosures where ticker is in universe."""
        universe_set = set(t.upper() for t in universe_tickers)
        return [
            d for d in disclosures
            if d.get("ticker", "").upper() in universe_set
        ]

    def parse_amount_range(self, amount_str: str) -> dict:
        """
        Parse ranges like '$15,001 - $50,000' -> {min: 15001, max: 50000, midpoint: 32500}
        """
        if not amount_str:
            return {"min": 0, "max": 0, "midpoint": 0}

        # Check known ranges
        for key, (mn, mx) in AMOUNT_RANGES.items():
            if key.lower() in amount_str.lower():
                return {"min": mn, "max": mx, "midpoint": (mn + mx) // 2}

        # Parse numerically
        numbers = re.findall(r"[\d,]+", amount_str.replace("$", ""))
        nums = []
        for n in numbers:
            try:
                nums.append(int(n.replace(",", "")))
            except ValueError:
                pass

        if len(nums) >= 2:
            mn, mx = nums[0], nums[1]
            return {"min": mn, "max": mx, "midpoint": (mn + mx) // 2}
        elif len(nums) == 1:
            return {"min": nums[0], "max": nums[0], "midpoint": nums[0]}

        # "Over $X"
        over_match = re.search(r"over\s*\$?([\d,]+)", amount_str, re.IGNORECASE)
        if over_match:
            val = int(over_match.group(1).replace(",", ""))
            return {"min": val, "max": val * 2, "midpoint": int(val * 1.5)}

        return {"min": 0, "max": 0, "midpoint": 0}

    def normalize_ticker(self, ticker_str: str) -> str:
        """
        Clean ticker: remove $, convert to uppercase, handle class shares (BRK.A -> BRK-A).
        """
        if not ticker_str:
            return ""
        ticker = ticker_str.strip().upper()
        ticker = ticker.lstrip("$")
        ticker = ticker.replace(".", "-")
        # Remove any non-alphanumeric characters except dash
        ticker = re.sub(r"[^A-Z0-9\-]", "", ticker)
        return ticker

    def _calc_delay(self, tx_date: str, filing_date: str) -> int:
        """Calculate delay in days between transaction and filing."""
        try:
            fmt = "%Y-%m-%d"
            tx = datetime.strptime(tx_date[:10], fmt)
            fi = datetime.strptime(filing_date[:10], fmt)
            delta = (fi - tx).days
            return max(0, delta)
        except Exception:
            return 0
