"""
transcript_fetcher.py — Fetches earnings call transcripts from free sources (SEC EDGAR, Motley Fool).
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta
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
CIK_CACHE_FILE = CACHE_DIR / "cik_cache.json"

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar"
EDGAR_CIK_LOOKUP = "https://www.sec.gov/cgi-bin/browse-edgar"
MOTLEY_FOOL_SEARCH = "https://www.fool.com/search/solr.aspx"

SEC_SLEEP = 0.15   # 10 req/s max per SEC guidelines
FOOL_SLEEP = 10.0  # 1 req per 10s


class TranscriptFetcher:
    def __init__(self, config: dict):
        self.config = config
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cik_cache: dict = self._load_cik_cache()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self, ticker: str, max_transcripts: int = 4) -> list:
        """Return list of transcript dicts: {ticker, date, source, raw_text, sections}."""
        results = []
        try:
            edgar_transcripts = self.fetch_from_edgar(ticker)
            results.extend(edgar_transcripts)
        except Exception as exc:
            logger.warning("EDGAR fetch failed for %s: %s", ticker, exc)

        if len(results) < max_transcripts:
            try:
                fool_transcripts = self.fetch_from_motley_fool(ticker)
                results.extend(fool_transcripts)
            except Exception as exc:
                logger.warning("Motley Fool fetch failed for %s: %s", ticker, exc)

        # Deduplicate by date+source, limit count
        seen = set()
        unique = []
        for t in results:
            key = (t.get("date", ""), t.get("source", ""))
            if key not in seen:
                seen.add(key)
                unique.append(t)
        return unique[:max_transcripts]

    # ------------------------------------------------------------------
    # EDGAR
    # ------------------------------------------------------------------

    def fetch_from_edgar(self, ticker: str) -> list:
        """
        Fetch 8-K filings (Item 2.02 and 7.01) from SEC EDGAR full-text search.
        Rate limit: 10 requests/second max, sleep 0.15s between calls.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping EDGAR fetch")
            return []

        results = []
        try:
            cik = self.get_cik(ticker)
            if not cik:
                logger.warning("No CIK found for ticker %s", ticker)
                return []

            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=365 * 2)
            start_str = start_dt.strftime("%Y-%m-%d")
            end_str = end_dt.strftime("%Y-%m-%d")

            params = {
                "q": f'"{ticker}"',
                "dateRange": "custom",
                "startdt": start_str,
                "enddt": end_str,
                "forms": "8-K",
                "entity": cik,
            }
            headers = {
                "User-Agent": "QuantFund research@example.com",
                "Accept-Encoding": "gzip, deflate",
            }

            time.sleep(SEC_SLEEP)
            resp = requests.get(EDGAR_SEARCH_URL, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            hits = data.get("hits", {}).get("hits", [])
            for hit in hits[:8]:
                source = hit.get("_source", {})
                accession = source.get("file_date", "")
                filing_url = source.get("file_url", "")
                filed_at = source.get("file_date", "")

                # Only fetch earnings-related 8-Ks (Items 2.02, 7.01)
                items = source.get("items", "")
                if not any(x in str(items) for x in ["2.02", "7.01"]):
                    continue

                if not filing_url:
                    continue

                time.sleep(SEC_SLEEP)
                try:
                    doc_resp = requests.get(filing_url, headers=headers, timeout=20)
                    doc_resp.raise_for_status()
                    raw_text = doc_resp.text
                except Exception as exc:
                    logger.warning("Failed to fetch EDGAR filing %s: %s", filing_url, exc)
                    continue

                sections = self.parse_transcript_structure(raw_text)
                results.append({
                    "ticker": ticker,
                    "date": filed_at,
                    "source": "SEC_EDGAR",
                    "raw_text": raw_text,
                    "sections": sections,
                    "accession": accession,
                    "url": filing_url,
                })

        except Exception as exc:
            logger.warning("EDGAR search error for %s: %s", ticker, exc)

        return results

    # ------------------------------------------------------------------
    # Motley Fool
    # ------------------------------------------------------------------

    def fetch_from_motley_fool(self, ticker: str) -> list:
        """
        Scrape fool.com/earnings-call-transcripts/ with BeautifulSoup.
        Rate limit: 1 request per 10 seconds. Respects robots.txt spirit.
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; skipping Motley Fool fetch")
            return []
        if not HAS_BS4:
            logger.warning("BeautifulSoup not available; skipping Motley Fool fetch")
            return []

        results = []
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; QuantFundResearch/1.0)",
        }

        try:
            time.sleep(FOOL_SLEEP)
            search_url = f"{MOTLEY_FOOL_SEARCH}?q={ticker}+earnings+transcript"
            resp = requests.get(search_url, headers=headers, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Collect article links from search results
            links = []
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "earnings-call-transcript" in href.lower():
                    if href.startswith("/"):
                        href = "https://www.fool.com" + href
                    if ticker.upper() in href.upper() or ticker.lower() in href.lower():
                        links.append(href)

            links = list(dict.fromkeys(links))[:4]  # deduplicate, cap at 4

            for link in links:
                time.sleep(FOOL_SLEEP)
                try:
                    page_resp = requests.get(link, headers=headers, timeout=20)
                    page_resp.raise_for_status()
                    page_soup = BeautifulSoup(page_resp.text, "html.parser")

                    # Extract article body
                    article = page_soup.find("article") or page_soup.find("div", class_=re.compile(r"article|content|transcript", re.I))
                    if not article:
                        continue

                    raw_text = article.get_text(separator="\n")

                    # Try to extract date
                    date_str = ""
                    time_tag = page_soup.find("time")
                    if time_tag:
                        date_str = time_tag.get("datetime", time_tag.get_text(strip=True))

                    sections = self.parse_transcript_structure(raw_text)
                    results.append({
                        "ticker": ticker,
                        "date": date_str,
                        "source": "MOTLEY_FOOL",
                        "raw_text": raw_text,
                        "sections": sections,
                        "url": link,
                    })
                except Exception as exc:
                    logger.warning("Failed to fetch Motley Fool article %s: %s", link, exc)

        except Exception as exc:
            logger.warning("Motley Fool search error for %s: %s", ticker, exc)

        return results

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def parse_transcript_structure(self, raw_text: str) -> dict:
        """
        Parse transcript into sections: prepared_remarks, qa_section, speakers, operator_lines.
        Uses heuristics to detect Q&A start and speaker boundaries.
        """
        lines = raw_text.splitlines()

        # Detect Q&A boundary
        qa_start_idx = None
        qa_patterns = [
            re.compile(r"question.and.answer", re.I),
            re.compile(r"\bQ&A\b", re.I),
            re.compile(r"Q&A Session", re.I),
            re.compile(r"Operator:\s*(Please|We.ll|We will|At this time)", re.I),
            re.compile(r"questions?\s+from\s+(analyst|investor|participant)", re.I),
        ]
        for i, line in enumerate(lines):
            for pat in qa_patterns:
                if pat.search(line):
                    qa_start_idx = i
                    break
            if qa_start_idx is not None:
                break

        if qa_start_idx is not None:
            prepared_lines = lines[:qa_start_idx]
            qa_lines = lines[qa_start_idx:]
        else:
            # Rough split: first 60% is prepared remarks
            split = int(len(lines) * 0.6)
            prepared_lines = lines[:split]
            qa_lines = lines[split:]

        prepared_remarks = "\n".join(prepared_lines).strip()
        qa_section = "\n".join(qa_lines).strip()

        # Speaker detection
        # Pattern 1: "Name (Role):" or "Name:" at line start
        # Pattern 2: ALL CAPS NAME followed by content
        speaker_pattern = re.compile(
            r"^([A-Z][a-zA-Z\-']+(?:\s[A-Z][a-zA-Z\-']+){0,3})\s*(?:\([^)]*\))?\s*:",
            re.MULTILINE
        )
        all_caps_pattern = re.compile(r"^([A-Z][A-Z\s]{3,30})\s*$", re.MULTILINE)

        speakers: dict = {}
        operator_lines = []

        current_speaker = None
        current_paragraphs: list = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Operator lines
            if re.match(r"^Operator\s*:", stripped, re.I):
                operator_lines.append(stripped)
                if current_speaker:
                    _append_paragraph(speakers, current_speaker, current_paragraphs)
                current_speaker = "Operator"
                current_paragraphs = []
                continue

            # Speaker label
            m = speaker_pattern.match(stripped)
            if m:
                if current_speaker:
                    _append_paragraph(speakers, current_speaker, current_paragraphs)
                current_speaker = m.group(1).strip()
                remainder = stripped[m.end():].strip()
                current_paragraphs = [remainder] if remainder else []
                continue

            # All-caps speaker name on its own line
            if all_caps_pattern.match(stripped) and len(stripped.split()) <= 5:
                if current_speaker:
                    _append_paragraph(speakers, current_speaker, current_paragraphs)
                current_speaker = stripped.title()
                current_paragraphs = []
                continue

            # Regular content line
            if current_speaker:
                current_paragraphs.append(stripped)

        if current_speaker:
            _append_paragraph(speakers, current_speaker, current_paragraphs)

        # Build speakers list
        speakers_list = [
            {"name": name, "role": _infer_role(name), "paragraphs": paras}
            for name, paras in speakers.items()
            if name != "Operator"
        ]

        return {
            "prepared_remarks": prepared_remarks,
            "qa_section": qa_section,
            "speakers": speakers_list,
            "operator_lines": operator_lines,
        }

    # ------------------------------------------------------------------
    # CIK lookup
    # ------------------------------------------------------------------

    def get_cik(self, ticker: str) -> str:
        """Look up SEC CIK number for a ticker. Cache in data/cache/deepdata/cik_cache.json."""
        ticker = ticker.upper()
        if ticker in self._cik_cache:
            return self._cik_cache[ticker]

        if not HAS_REQUESTS:
            return ""

        try:
            time.sleep(SEC_SLEEP)
            headers = {"User-Agent": "QuantFund research@example.com"}
            # Try the EDGAR company search JSON endpoint
            url = "https://www.sec.gov/cgi-bin/browse-edgar"
            params = {"company": "", "CIK": ticker, "action": "getcompany", "output": "atom"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()

            # Parse CIK from atom feed
            cik_match = re.search(r"CIK=(\d+)", resp.url)
            if not cik_match:
                cik_match = re.search(r"<cik>(\d+)</cik>", resp.text, re.I)
            if not cik_match:
                cik_match = re.search(r"/(\d{10})/", resp.text)

            if cik_match:
                cik = cik_match.group(1).lstrip("0")
                self._cik_cache[ticker] = cik
                self._save_cik_cache()
                return cik

            # Fallback: company_tickers.json
            time.sleep(SEC_SLEEP)
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            resp2 = requests.get(tickers_url, headers=headers, timeout=15)
            resp2.raise_for_status()
            tickers_data = resp2.json()
            for entry in tickers_data.values():
                if entry.get("ticker", "").upper() == ticker:
                    cik = str(entry["cik_str"])
                    self._cik_cache[ticker] = cik
                    self._save_cik_cache()
                    return cik

        except Exception as exc:
            logger.warning("CIK lookup failed for %s: %s", ticker, exc)

        return ""

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _load_cik_cache(self) -> dict:
        if CIK_CACHE_FILE.exists():
            try:
                return json.loads(CIK_CACHE_FILE.read_text())
            except Exception:
                pass
        return {}

    def _save_cik_cache(self) -> None:
        try:
            CIK_CACHE_FILE.write_text(json.dumps(self._cik_cache, indent=2))
        except Exception as exc:
            logger.warning("Could not save CIK cache: %s", exc)


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _append_paragraph(speakers: dict, name: str, paragraphs: list) -> None:
    text = " ".join(paragraphs).strip()
    if text:
        if name not in speakers:
            speakers[name] = []
        speakers[name].append(text)


def _infer_role(name: str) -> str:
    """Crude role inference from common title keywords embedded in name strings."""
    name_lower = name.lower()
    if any(kw in name_lower for kw in ["ceo", "chief executive"]):
        return "CEO"
    if any(kw in name_lower for kw in ["cfo", "chief financial"]):
        return "CFO"
    if any(kw in name_lower for kw in ["coo", "chief operating"]):
        return "COO"
    if any(kw in name_lower for kw in ["analyst", "research"]):
        return "Analyst"
    if "operator" in name_lower:
        return "Operator"
    return "Unknown"
