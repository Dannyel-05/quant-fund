"""
Supply chain relationship mapper using SEC 10-K filings and NetworkX.
"""
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False

try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
    HAS_SPACY = True
except (ImportError, OSError):
    HAS_SPACY = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from difflib import SequenceMatcher
    HAS_DIFFLIB = True
except ImportError:
    HAS_DIFFLIB = False

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")
CACHE_FILE = CACHE_DIR / "supply_chain_graph.json"

# Regex patterns for supplier/customer extraction when spaCy unavailable
CUSTOMER_REVENUE_PATTERN = re.compile(
    r'([A-Z][A-Za-z\s&,\.]+(?:Inc\.?|Corp\.?|Ltd\.?|LLC\.?|Co\.?|Group|Holdings)?)'
    r'[^\d]*?(\d{1,3}(?:\.\d+)?)\s*%\s*(?:of\s+)?(?:(?:net\s+)?revenue|sales)',
    re.IGNORECASE
)
SUPPLIER_PATTERN = re.compile(
    r'(?:sole\s+source|primary|key|critical|sole)\s+supplier[s]?\s+(?:of|for|include[s]?)?\s+'
    r'([A-Z][A-Za-z\s&,\.]+(?:Inc\.?|Corp\.?|Ltd\.?|LLC\.?|Co\.?|Group|Holdings)?)',
    re.IGNORECASE
)
CONCENTRATION_PATTERN = re.compile(
    r'([A-Z][A-Za-z\s&,\.]+(?:Inc\.?|Corp\.?|Ltd\.?|LLC\.?|Co\.?|Group|Holdings)?)'
    r'[^\d]*?accounted\s+for\s+(?:approximately\s+)?(\d{1,3}(?:\.\d+)?)\s*%',
    re.IGNORECASE
)


class SupplyChainRelationshipMapper:
    """
    Builds supply chain relationship graphs from SEC 10-K filings.
    Nodes = companies, edges = supplier -> customer with revenue dependency weights.
    """

    def __init__(self, config: dict):
        self.config = config
        sc_config = config.get("deepdata", {}).get("supply_chain", {})
        self.relationship_depth = sc_config.get("relationship_depth", 3)
        self.min_revenue_pct = sc_config.get("min_revenue_pct", 10.0)
        self.request_delay = sc_config.get("request_delay", 2.0)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        # Internal graph representation
        if HAS_NETWORKX:
            self._graph = nx.DiGraph()
        else:
            # Fallback: dict-based adjacency
            self._graph = {"nodes": {}, "edges": {}}

    def collect(self, tickers: list, market: str = "us") -> list:
        """Dispatch entry-point called by the deepdata runner. Returns edge list."""
        return self.build_graph(tickers) or []

    def build_graph(self, tickers: list):
        """
        Build supply chain graph. Nodes=companies, edges=supplier->customer.
        Weight = revenue dependency fraction.
        Depth up to config relationship_depth.
        Caches to data/cache/deepdata/supply_chain_graph.json.
        """
        logger.info("Building supply chain graph for %d tickers", len(tickers))

        if HAS_NETWORKX:
            graph = nx.DiGraph()
        else:
            graph = {"nodes": {}, "edges": {}}

        processed = set()
        queue = list(tickers)
        depth_map = {t: 0 for t in tickers}

        while queue:
            ticker = queue.pop(0)
            current_depth = depth_map.get(ticker, 0)

            if ticker in processed or current_depth > self.relationship_depth:
                continue
            processed.add(ticker)

            relationships = self.fetch_10k_relationships(ticker)
            if not relationships:
                continue

            if HAS_NETWORKX:
                if ticker not in graph:
                    graph.add_node(ticker, label=ticker)
            else:
                if ticker not in graph["nodes"]:
                    graph["nodes"][ticker] = {"label": ticker}

            # Process customers: ticker is supplier_to customer
            for customer in relationships.get("customers", []):
                cname = customer.get("name", "")
                rev_pct = customer.get("revenue_pct", 0.0)
                matched = self.match_to_universe(cname, tickers)
                if matched:
                    weight = rev_pct / 100.0
                    if HAS_NETWORKX:
                        graph.add_node(matched, label=matched)
                        graph.add_edge(ticker, matched, weight=weight,
                                       relationship="supplier_to",
                                       revenue_pct=rev_pct)
                    else:
                        graph["nodes"].setdefault(matched, {"label": matched})
                        graph["edges"].setdefault(ticker, {})[matched] = {
                            "weight": weight,
                            "relationship": "supplier_to",
                            "revenue_pct": rev_pct,
                        }
                    if matched not in processed and current_depth + 1 <= self.relationship_depth:
                        queue.append(matched)
                        depth_map[matched] = current_depth + 1

            # Process suppliers: supplier is supplier_to ticker
            for supplier in relationships.get("suppliers", []):
                sname = supplier.get("name", "")
                dep = supplier.get("dependency", 0.5)
                matched = self.match_to_universe(sname, tickers)
                if matched:
                    if HAS_NETWORKX:
                        graph.add_node(matched, label=matched)
                        graph.add_edge(matched, ticker, weight=dep,
                                       relationship="supplier_to",
                                       dependency=dep)
                    else:
                        graph["nodes"].setdefault(matched, {"label": matched})
                        graph["edges"].setdefault(matched, {})[ticker] = {
                            "weight": dep,
                            "relationship": "supplier_to",
                            "dependency": dep,
                        }
                    if matched not in processed and current_depth + 1 <= self.relationship_depth:
                        queue.append(matched)
                        depth_map[matched] = current_depth + 1

        self._graph = graph
        self._save_graph(graph)
        return graph

    def _save_graph(self, graph) -> None:
        """Serialize graph to JSON cache."""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            if HAS_NETWORKX:
                data = {
                    "nodes": list(graph.nodes(data=True)),
                    "edges": [
                        {"src": u, "dst": v, **d}
                        for u, v, d in graph.edges(data=True)
                    ],
                    "cached_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                data = {**graph, "cached_at": datetime.now(timezone.utc).isoformat()}
            with open(CACHE_FILE, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to cache supply chain graph: %s", exc)

    def fetch_10k_relationships(self, ticker: str) -> dict:
        """
        Fetch latest 10-K from EDGAR full-text search.
        Parse Item 1 and Item 1A for customer/supplier mentions.
        Returns: {customers: [{name, revenue_pct}], suppliers: [{name, dependency}]}
        """
        if not HAS_REQUESTS:
            logger.warning("requests not available; cannot fetch 10-K for %s", ticker)
            return {"customers": [], "suppliers": []}

        try:
            url = (
                f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=10-K"
            )
            headers = {"User-Agent": "QuantFund research@quantfund.example.com"}
            time.sleep(self.request_delay)
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("EDGAR search failed for %s: %s", ticker, exc)
            return {"customers": [], "suppliers": []}

        # Try to get the accession number for latest 10-K
        try:
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                return {"customers": [], "suppliers": []}
            latest = hits[0]
            accession = latest.get("_source", {}).get("file_date", "")
            filing_url = latest.get("_source", {}).get("period_of_report", "")
            # Try fetching actual filing text
            entity_id = latest.get("_source", {}).get("entity_id", "")
            acc_no = latest.get("_id", "")
        except Exception as exc:
            logger.warning("Could not parse EDGAR response for %s: %s", ticker, exc)
            return {"customers": [], "suppliers": []}

        # Attempt to fetch the filing document text
        text = self._fetch_filing_text(acc_no, entity_id)
        if not text:
            return {"customers": [], "suppliers": []}

        return self._parse_relationships(text)

    def _fetch_filing_text(self, acc_no: str, entity_id: str) -> str:
        """Attempt to retrieve 10-K filing text from EDGAR."""
        if not HAS_REQUESTS or not acc_no:
            return ""
        try:
            clean_acc = acc_no.replace("-", "")
            headers = {"User-Agent": "QuantFund research@quantfund.example.com"}
            base = "https://www.sec.gov/Archives/edgar/data"
            index_url = f"{base}/{entity_id}/{clean_acc}/{acc_no}-index.htm"
            time.sleep(self.request_delay)
            resp = requests.get(index_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return ""
            # Find the main document link
            doc_links = re.findall(r'href="([^"]+\.htm)"', resp.text, re.IGNORECASE)
            if not doc_links:
                return ""
            doc_url = f"https://www.sec.gov{doc_links[0]}"
            time.sleep(self.request_delay)
            doc_resp = requests.get(doc_url, headers=headers, timeout=30)
            doc_resp.raise_for_status()
            return doc_resp.text
        except Exception as exc:
            logger.warning("Failed to fetch filing text: %s", exc)
            return ""

    def _parse_relationships(self, text: str) -> dict:
        """Extract customer/supplier relationships from filing text."""
        customers = []
        suppliers = []

        # Extract relevant sections
        item1_match = re.search(
            r'(?:Item\s+1[\.\s]+Business)(.*?)(?:Item\s+1A|Item\s+2)',
            text, re.IGNORECASE | re.DOTALL
        )
        item1a_match = re.search(
            r'(?:Item\s+1A[\.\s]+Risk\s+Factors)(.*?)(?:Item\s+1B|Item\s+2)',
            text, re.IGNORECASE | re.DOTALL
        )

        sections = []
        if item1_match:
            sections.append(item1_match.group(1))
        if item1a_match:
            sections.append(item1a_match.group(1))
        combined = "\n".join(sections) if sections else text[:50000]

        if HAS_SPACY:
            customers, suppliers = self._parse_with_spacy(combined)
        else:
            customers, suppliers = self._parse_with_regex(combined)

        return {"customers": customers, "suppliers": suppliers}

    def _parse_with_spacy(self, text: str) -> tuple:
        """Use spaCy NER to identify ORG entities near revenue mentions."""
        customers = []
        suppliers = []
        try:
            doc = nlp(text[:100000])  # limit to 100k chars
            for ent in doc.ents:
                if ent.label_ != "ORG":
                    continue
                context = text[max(0, ent.start_char - 200):ent.end_char + 200]
                rev_match = re.search(r'(\d{1,3}(?:\.\d+)?)\s*%', context)
                if rev_match:
                    pct = float(rev_match.group(1))
                    if pct >= self.min_revenue_pct:
                        if re.search(r'revenue|sales|net\s+sales', context, re.IGNORECASE):
                            customers.append({"name": ent.text.strip(), "revenue_pct": pct})
                if re.search(r'supplier|vendor|sole\s+source', context, re.IGNORECASE):
                    dep = 0.5
                    dep_match = re.search(r'(\d{1,3}(?:\.\d+)?)\s*%', context)
                    if dep_match:
                        dep = float(dep_match.group(1)) / 100.0
                    suppliers.append({"name": ent.text.strip(), "dependency": dep})
        except Exception as exc:
            logger.warning("spaCy parsing failed: %s", exc)
        return customers, suppliers

    def _parse_with_regex(self, text: str) -> tuple:
        """Fallback regex-based extraction."""
        customers = []
        suppliers = []

        for match in CUSTOMER_REVENUE_PATTERN.finditer(text):
            name = match.group(1).strip()
            pct = float(match.group(2))
            if pct >= self.min_revenue_pct and len(name) > 2:
                customers.append({"name": name, "revenue_pct": pct})

        for match in CONCENTRATION_PATTERN.finditer(text):
            name = match.group(1).strip()
            pct = float(match.group(2))
            if pct >= self.min_revenue_pct and len(name) > 2:
                context = text[max(0, match.start() - 100):match.end() + 100]
                if re.search(r'revenue|sales', context, re.IGNORECASE):
                    customers.append({"name": name, "revenue_pct": pct})

        for match in SUPPLIER_PATTERN.finditer(text):
            name = match.group(1).strip()
            if len(name) > 2:
                suppliers.append({"name": name, "dependency": 0.5})

        # Deduplicate
        seen = set()
        unique_customers = []
        for c in customers:
            if c["name"] not in seen:
                seen.add(c["name"])
                unique_customers.append(c)

        seen = set()
        unique_suppliers = []
        for s in suppliers:
            if s["name"] not in seen:
                seen.add(s["name"])
                unique_suppliers.append(s)

        return unique_customers, unique_suppliers

    def match_to_universe(self, company_name: str, universe_tickers: list) -> str:
        """
        Match a mentioned company name to a ticker in the universe using fuzzy matching.
        Returns ticker string or empty string if no good match.
        """
        if not company_name or not universe_tickers:
            return ""

        company_name_clean = company_name.upper().strip()

        # Exact match on ticker
        if company_name_clean in universe_tickers:
            return company_name_clean

        if not HAS_DIFFLIB:
            return ""

        best_ratio = 0.0
        best_ticker = ""

        for ticker in universe_tickers:
            # Compare against ticker directly
            ratio = SequenceMatcher(None, company_name_clean, ticker.upper()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_ticker = ticker

        # Only return if sufficiently similar
        if best_ratio >= 0.7:
            return best_ticker
        return ""

    def get_upstream_companies(self, ticker: str, depth: int = 1) -> list:
        """Return all supplier companies up to 'depth' levels upstream."""
        if HAS_NETWORKX and hasattr(self._graph, "predecessors"):
            return self._bfs_neighbors(ticker, direction="upstream", depth=depth)
        else:
            return self._dict_bfs(ticker, direction="upstream", depth=depth)

    def get_downstream_companies(self, ticker: str, depth: int = 1) -> list:
        """Return all customer companies up to 'depth' levels downstream."""
        if HAS_NETWORKX and hasattr(self._graph, "successors"):
            return self._bfs_neighbors(ticker, direction="downstream", depth=depth)
        else:
            return self._dict_bfs(ticker, direction="downstream", depth=depth)

    def _bfs_neighbors(self, ticker: str, direction: str, depth: int) -> list:
        """BFS traversal using NetworkX graph."""
        visited = set()
        queue = [(ticker, 0)]
        results = []
        while queue:
            node, d = queue.pop(0)
            if node in visited or d > depth:
                continue
            visited.add(node)
            if node != ticker:
                results.append(node)
            if d < depth:
                try:
                    if direction == "upstream":
                        neighbors = list(self._graph.predecessors(node))
                    else:
                        neighbors = list(self._graph.successors(node))
                    for n in neighbors:
                        if n not in visited:
                            queue.append((n, d + 1))
                except Exception:
                    pass
        return results

    def _dict_bfs(self, ticker: str, direction: str, depth: int) -> list:
        """BFS traversal using dict-based graph."""
        visited = set()
        queue = [(ticker, 0)]
        results = []
        edges = self._graph.get("edges", {})
        while queue:
            node, d = queue.pop(0)
            if node in visited or d > depth:
                continue
            visited.add(node)
            if node != ticker:
                results.append(node)
            if d < depth:
                if direction == "upstream":
                    # Find nodes that point TO node
                    neighbors = [src for src, dsts in edges.items() if node in dsts]
                else:
                    # Find nodes that node points TO
                    neighbors = list(edges.get(node, {}).keys())
                for n in neighbors:
                    if n not in visited:
                        queue.append((n, d + 1))
        return results

    def calc_relationship_strength(self, ticker_a: str, ticker_b: str) -> float:
        """Return dependency weight on edge A->B. 0 if no relationship."""
        try:
            if HAS_NETWORKX and hasattr(self._graph, "has_edge"):
                if self._graph.has_edge(ticker_a, ticker_b):
                    return self._graph[ticker_a][ticker_b].get("weight", 0.0)
            else:
                return self._graph.get("edges", {}).get(ticker_a, {}).get(
                    ticker_b, {}
                ).get("weight", 0.0)
        except Exception as exc:
            logger.warning("calc_relationship_strength error: %s", exc)
        return 0.0
