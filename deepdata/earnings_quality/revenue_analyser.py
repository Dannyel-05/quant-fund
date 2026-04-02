"""
RevenueAnalyser — distinguishes HOW a company beat earnings.
Classifies beat quality to determine PEAD signal multiplier.
"""
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

BEAT_QUALITY_SCORES = {
    "REVENUE_DRIVEN": 1.0,
    "MARGIN_DRIVEN": 0.75,
    "COST_CUT": 0.5,
    "TAX_DRIVEN": 0.25,
    "ONE_OFF": 0.0,
}

EDGAR_BASE = "https://www.sec.gov"
REQUEST_DELAY = 1.5


class RevenueAnalyser:
    """
    Classifies earnings beat quality to adjust PEAD signal strength.
    Revenue-driven beats are most persistent; one-off items should not trigger PEAD.
    """

    def __init__(self, config: dict):
        self.config = config
        eq_config = config.get("deepdata", {}).get("earnings_quality", {})
        self.tax_anomaly_threshold = eq_config.get("tax_anomaly_threshold_pp", 5.0)
        self.nonrecurring_threshold = eq_config.get("nonrecurring_threshold_pct", 0.20)
        self.min_revenue_beat_pct = eq_config.get("min_revenue_beat_pct", 0.5)
        self.request_delay = eq_config.get("request_delay", REQUEST_DELAY)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def analyse(self, ticker: str, earnings_data: dict) -> dict:
        """
        Classify beat quality from earnings data.

        Returns:
        {
          beat_quality, quality_score, revenue_beat_pct, eps_beat_pct,
          margin_expansion, non_recurring_contribution, tax_contribution,
          pead_multiplier
        }
        """
        revenue = earnings_data.get("revenue", 0.0) or 0.0
        revenue_estimate = earnings_data.get("revenue_estimate", 0.0) or 0.0
        eps = earnings_data.get("eps", 0.0) or 0.0
        eps_estimate = earnings_data.get("eps_estimate", 0.0) or 0.0
        gross_profit = earnings_data.get("gross_profit", 0.0) or 0.0
        operating_income = earnings_data.get("operating_income", 0.0) or 0.0
        net_income = earnings_data.get("net_income", 0.0) or 0.0
        non_recurring_items = earnings_data.get("non_recurring_items", 0.0) or 0.0
        effective_tax_rate = earnings_data.get("effective_tax_rate", 0.25) or 0.25
        shares_outstanding = earnings_data.get("shares_outstanding", 1.0) or 1.0
        historical_tax_rates = earnings_data.get("historical_tax_rates", [])
        prev_gross_margin = earnings_data.get("prev_gross_margin", None)

        # Revenue beat %
        revenue_beat_pct = 0.0
        if revenue_estimate and revenue_estimate != 0:
            revenue_beat_pct = ((revenue - revenue_estimate) / abs(revenue_estimate)) * 100.0

        # EPS beat %
        eps_beat_pct = 0.0
        if eps_estimate and eps_estimate != 0:
            eps_beat_pct = ((eps - eps_estimate) / abs(eps_estimate)) * 100.0

        # Margin expansion (gross margin vs prior quarter)
        current_gross_margin = gross_profit / revenue if revenue != 0 else 0.0
        margin_expansion = 0.0
        if prev_gross_margin is not None:
            margin_expansion = current_gross_margin - prev_gross_margin

        # Non-recurring contribution to EPS
        non_recurring_contribution = 0.0
        if shares_outstanding > 0 and net_income != 0:
            nonrecurring_eps_contribution = non_recurring_items / shares_outstanding
            non_recurring_contribution = abs(nonrecurring_eps_contribution / max(abs(eps), 1e-9))

        # Tax contribution to EPS
        tax_anomaly = self.detect_tax_anomaly(effective_tax_rate, historical_tax_rates)
        tax_contribution = 0.0
        if tax_anomaly and historical_tax_rates:
            avg_tax = sum(historical_tax_rates) / len(historical_tax_rates)
            tax_contribution = avg_tax - effective_tax_rate  # positive = tax benefit

        beat_quality, quality_score = self.classify_beat(
            revenue_beat=revenue_beat_pct,
            eps_beat=eps_beat_pct,
            margin_change=margin_expansion,
            tax_anomaly=tax_anomaly,
            nonrecurring_pct=non_recurring_contribution,
        )

        pead_multiplier = self.calc_pead_multiplier(quality_score)

        return {
            "ticker": ticker,
            "beat_quality": beat_quality,
            "quality_score": round(quality_score, 4),
            "revenue_beat_pct": round(revenue_beat_pct, 4),
            "eps_beat_pct": round(eps_beat_pct, 4),
            "margin_expansion": round(margin_expansion, 6),
            "non_recurring_contribution": round(non_recurring_contribution, 4),
            "tax_contribution": round(tax_contribution, 4),
            "pead_multiplier": round(pead_multiplier, 4),
            "tax_anomaly_detected": tax_anomaly,
            "analysed_at": datetime.now(timezone.utc).isoformat(),
        }

    def classify_beat(
        self,
        revenue_beat: float,
        eps_beat: float,
        margin_change: float,
        tax_anomaly: bool,
        nonrecurring_pct: float,
    ) -> tuple:
        """
        Classify earnings beat into quality buckets.
        Returns (beat_quality: str, quality_score: float)
        """
        # ONE_OFF: large non-recurring contribution
        if nonrecurring_pct > self.nonrecurring_threshold:
            return ("ONE_OFF", 0.0)

        # TAX_DRIVEN: beat driven by unusual tax rate
        if tax_anomaly and eps_beat > 0 and revenue_beat < self.min_revenue_beat_pct:
            return ("TAX_DRIVEN", 0.25)

        # REVENUE_DRIVEN: beat on both revenue and EPS, with revenue leading
        if revenue_beat >= self.min_revenue_beat_pct and eps_beat > 0:
            if revenue_beat >= eps_beat * 0.5:
                return ("REVENUE_DRIVEN", 1.0)

        # MARGIN_DRIVEN: strong EPS beat driven by margin expansion, less revenue
        if eps_beat > 0 and margin_change > 0 and revenue_beat >= 0:
            if margin_change > 0.01:  # > 1pp margin expansion
                return ("MARGIN_DRIVEN", 0.75)

        # COST_CUT: EPS beat with flat/declining revenue = cost-cutting
        if eps_beat > 0 and revenue_beat < 0:
            return ("COST_CUT", 0.5)

        # Default: if beat but unclear driver
        if eps_beat > 0:
            return ("MARGIN_DRIVEN", 0.5)

        # Miss or no beat
        return ("ONE_OFF", 0.0)

    def extract_from_edgar(self, ticker: str) -> dict:
        """
        Fetch 8-K Item 2.02 (Results of Operations) from EDGAR.
        Parse financial tables. Returns earnings_data dict.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; cannot fetch EDGAR 8-K for %s", ticker)
            return {}

        try:
            headers = {"User-Agent": "QuantFund research@quantfund.example.com"}
            # Search for 8-K filings
            search_url = (
                f"https://efts.sec.gov/LATEST/search-index"
                f"?q=%22{ticker}%22&forms=8-K&dateRange=custom"
                f"&startdt=2020-01-01"
            )
            time.sleep(self.request_delay)
            resp = requests.get(search_url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("EDGAR 8-K search failed for %s: %s", ticker, exc)
            return {}

        try:
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                return {}

            # Find most recent 8-K with Item 2.02
            for hit in hits[:10]:
                source = hit.get("_source", {})
                # Check if it contains Item 2.02
                description = source.get("file_description", "").lower()
                if "2.02" not in description and "results of operations" not in description:
                    continue

                entity_id = source.get("entity_id", "")
                acc_no = hit.get("_id", "")
                text = self._fetch_8k_text(acc_no, entity_id, headers)
                if text:
                    return self._parse_8k_financials(text)
        except Exception as exc:
            logger.warning("EDGAR 8-K parse failed for %s: %s", ticker, exc)

        return {}

    def _fetch_8k_text(self, acc_no: str, entity_id: str, headers: dict) -> str:
        """Fetch 8-K document text from EDGAR."""
        try:
            clean_acc = acc_no.replace("-", "")
            index_url = (
                f"{EDGAR_BASE}/Archives/edgar/data/{entity_id}/{clean_acc}/{acc_no}-index.htm"
            )
            time.sleep(self.request_delay)
            resp = requests.get(index_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return ""

            doc_links = re.findall(r'href="([^"]+\.htm)"', resp.text, re.IGNORECASE)
            if not doc_links:
                return ""

            doc_url = f"{EDGAR_BASE}{doc_links[0]}"
            time.sleep(self.request_delay)
            doc_resp = requests.get(doc_url, headers=headers, timeout=30)
            doc_resp.raise_for_status()
            return doc_resp.text
        except Exception as exc:
            logger.warning("8-K text fetch failed: %s", exc)
            return ""

    def _parse_8k_financials(self, html_text: str) -> dict:
        """Parse financial data from 8-K HTML text using regex."""
        result = {}

        # Strip HTML tags
        clean_text = re.sub(r"<[^>]+>", " ", html_text)
        clean_text = re.sub(r"\s+", " ", clean_text)

        # Revenue patterns
        rev_pattern = re.compile(
            r'(?:net\s+)?(?:revenue|sales)[^\d]*\$?([\d,]+(?:\.\d+)?)\s*(?:million|billion)?',
            re.IGNORECASE
        )
        rev_match = rev_pattern.search(clean_text)
        if rev_match:
            val = float(rev_match.group(1).replace(",", ""))
            # Assume millions unless 'billion' mentioned
            context = clean_text[rev_match.start():rev_match.end() + 20]
            if "billion" in context.lower():
                val *= 1000
            result["revenue"] = val * 1_000_000

        # EPS patterns
        eps_pattern = re.compile(
            r'(?:diluted\s+)?(?:earnings|eps|e\.p\.s\.)\s+per\s+(?:diluted\s+)?share[^\d\-]*'
            r'(\-?[\d]+\.[\d]+)',
            re.IGNORECASE
        )
        eps_match = eps_pattern.search(clean_text)
        if eps_match:
            result["eps"] = float(eps_match.group(1))

        # Gross profit
        gp_pattern = re.compile(
            r'gross\s+(?:profit|margin)[^\d]*\$?([\d,]+(?:\.\d+)?)\s*(?:million|billion)?',
            re.IGNORECASE
        )
        gp_match = gp_pattern.search(clean_text)
        if gp_match:
            val = float(gp_match.group(1).replace(",", ""))
            context = clean_text[gp_match.start():gp_match.end() + 20]
            if "billion" in context.lower():
                val *= 1000
            result["gross_profit"] = val * 1_000_000

        # Tax rate
        tax_pattern = re.compile(
            r'(?:effective\s+)?tax\s+rate[^\d]*(\d{1,3}(?:\.\d+)?)\s*%',
            re.IGNORECASE
        )
        tax_match = tax_pattern.search(clean_text)
        if tax_match:
            result["effective_tax_rate"] = float(tax_match.group(1)) / 100.0

        return result

    def detect_tax_anomaly(
        self, effective_tax_rate: float, historical_tax_rates: list
    ) -> bool:
        """
        True if effective_tax_rate is more than 5pp below historical average.
        """
        if not historical_tax_rates:
            return False
        avg_rate = sum(historical_tax_rates) / len(historical_tax_rates)
        diff = avg_rate - effective_tax_rate
        return diff > (self.tax_anomaly_threshold / 100.0)

    def calc_pead_multiplier(self, quality_score: float) -> float:
        """
        ONE_OFF (0.0): return 0.0 (suppress PEAD)
        REVENUE_DRIVEN (1.0): return 1.3 (amplify)
        Others: return quality_score as-is
        """
        if quality_score == 0.0:
            return 0.0
        elif quality_score == 1.0:
            return 1.3
        else:
            return round(quality_score, 4)
