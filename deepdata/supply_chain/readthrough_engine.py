"""
ReadThroughEngine — generates pre-emptive signals from supply chain relationships.
When a company reports earnings, propagates signals to supply chain neighbors.
"""
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

# CollectorResult format: {source, ticker, market, data_type, value, raw_data,
#                          timestamp, quality_score}


class ReadThroughEngine:
    """
    Generates pre-emptive signals from supply chain relationships.
    Uses supply chain graph edges to propagate earnings results to neighbors.
    """

    def __init__(self, config: dict, graph=None):
        self.config = config
        sc_config = config.get("deepdata", {}).get("supply_chain", {})
        self.readthrough_decay = sc_config.get("readthrough_decay", 0.7)
        self.min_signal_threshold = sc_config.get("min_signal_threshold", 0.05)
        self.graph = graph
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def set_graph(self, graph) -> None:
        """Set the supply chain graph to use for readthrough calculations."""
        self.graph = graph

    def generate_readthroughs(
        self,
        reporting_ticker: str,
        earnings_result: dict,
        all_price_data: dict,
    ) -> list:
        """
        When a company reports earnings, generate signals for supply chain neighbors.

        earnings_result: {revenue_surprise_pct, eps_surprise_pct, beat_quality, direction}

        Returns list of CollectorResult dicts with:
        - source="supply_chain_readthrough"
        - ticker=neighbor_ticker
        - value=signal_strength (signed float)
        - raw_data={reporting_ticker, relationship_type, depth, dependency_weight}
        """
        results = []
        if self.graph is None:
            logger.warning("No supply chain graph set; cannot generate readthroughs")
            return results

        revenue_surprise = earnings_result.get("revenue_surprise_pct", 0.0)
        eps_surprise = earnings_result.get("eps_surprise_pct", 0.0)

        # Primary signal driver is revenue surprise
        primary_surprise = revenue_surprise if revenue_surprise != 0.0 else eps_surprise * 0.5

        # Walk the graph for upstream and downstream neighbors
        neighbors = self._get_all_neighbors(reporting_ticker)

        for neighbor_ticker, relationship_type, depth, dependency_weight in neighbors:
            signal = self.calc_readthrough_signal(
                revenue_surprise=primary_surprise,
                dependency_weight=dependency_weight,
                depth=depth,
                relationship_type=relationship_type,
            )

            if abs(signal) < self.min_signal_threshold:
                continue

            results.append({
                "source": "supply_chain_readthrough",
                "ticker": neighbor_ticker,
                "market": all_price_data.get(neighbor_ticker, {}).get("market", "US"),
                "data_type": "supply_chain_readthrough",
                "value": signal,
                "raw_data": {
                    "reporting_ticker": reporting_ticker,
                    "relationship_type": relationship_type,
                    "depth": depth,
                    "dependency_weight": dependency_weight,
                    "revenue_surprise_pct": revenue_surprise,
                    "eps_surprise_pct": eps_surprise,
                    "beat_quality": earnings_result.get("beat_quality", "UNKNOWN"),
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "quality_score": max(0.0, min(1.0, abs(signal))),
            })

        return results

    def _get_all_neighbors(
        self, ticker: str, max_depth: int = 3
    ) -> list:
        """
        Return list of (neighbor_ticker, relationship_type, depth, dependency_weight).
        Traverses both upstream (suppliers) and downstream (customers).
        """
        neighbors = []

        try:
            import networkx as nx
            if isinstance(self.graph, nx.DiGraph):
                # Downstream: ticker's successors = customers
                for depth in range(1, max_depth + 1):
                    for path in self._nx_paths(self.graph, ticker, direction="out", depth=depth):
                        if len(path) < 2:
                            continue
                        end_node = path[-1]
                        weight = self._path_weight(self.graph, path)
                        neighbors.append((end_node, "supplier_to", depth, weight))
                # Upstream: ticker's predecessors = suppliers
                for depth in range(1, max_depth + 1):
                    for path in self._nx_paths(self.graph, ticker, direction="in", depth=depth):
                        if len(path) < 2:
                            continue
                        end_node = path[-1]
                        weight = self._path_weight(self.graph, path)
                        neighbors.append((end_node, "customer_of", depth, weight))
                return neighbors
        except ImportError:
            pass

        # Fallback: dict-based graph
        if isinstance(self.graph, dict):
            edges = self.graph.get("edges", {})
            # Downstream
            visited_down = {ticker}
            queue_down = [(ticker, 0)]
            while queue_down:
                node, d = queue_down.pop(0)
                if d >= max_depth:
                    continue
                for succ, edge_data in edges.get(node, {}).items():
                    w = edge_data.get("weight", 0.5) if isinstance(edge_data, dict) else 0.5
                    if succ not in visited_down:
                        visited_down.add(succ)
                        neighbors.append((succ, "supplier_to", d + 1, w))
                        queue_down.append((succ, d + 1))
            # Upstream
            visited_up = {ticker}
            queue_up = [(ticker, 0)]
            while queue_up:
                node, d = queue_up.pop(0)
                if d >= max_depth:
                    continue
                for src, dsts in edges.items():
                    if node in dsts and src not in visited_up:
                        w = dsts[node].get("weight", 0.5) if isinstance(dsts[node], dict) else 0.5
                        visited_up.add(src)
                        neighbors.append((src, "customer_of", d + 1, w))
                        queue_up.append((src, d + 1))

        return neighbors

    def _nx_paths(self, graph, source: str, direction: str, depth: int) -> list:
        """Return simple paths of exactly 'depth' hops from source."""
        paths = []
        try:
            if direction == "out":
                for node in graph.nodes():
                    if node == source:
                        continue
                    try:
                        import networkx as nx
                        for path in nx.all_simple_paths(graph, source, node, cutoff=depth):
                            if len(path) - 1 == depth:
                                paths.append(path)
                    except Exception:
                        pass
            else:
                for node in graph.nodes():
                    if node == source:
                        continue
                    try:
                        import networkx as nx
                        for path in nx.all_simple_paths(graph, node, source, cutoff=depth):
                            if len(path) - 1 == depth:
                                paths.append(path)
                    except Exception:
                        pass
        except Exception as exc:
            logger.warning("Path finding error: %s", exc)
        return paths

    def _path_weight(self, graph, path: list) -> float:
        """Compute product of edge weights along path."""
        weight = 1.0
        try:
            for i in range(len(path) - 1):
                edge_data = graph[path[i]][path[i + 1]]
                weight *= edge_data.get("weight", 0.5)
        except Exception:
            weight = 0.5
        return weight

    def calc_readthrough_signal(
        self,
        revenue_surprise: float,
        dependency_weight: float,
        depth: int,
        relationship_type: str,
    ) -> float:
        """
        Calculate readthrough signal strength.

        If Company A (customer) reports strong revenue growth:
          signal = revenue_surprise * dependency_weight * readthrough_decay^depth

        relationship_type:
          'supplier_to': ticker is a supplier to reporting company (customer impact)
          'customer_of': ticker is a customer of reporting company (upstream impact)

        Customers propagate immediately; suppliers have reduced weight (1-quarter lag proxy).
        """
        decay = self.readthrough_decay ** depth
        base_signal = revenue_surprise / 100.0 * dependency_weight * decay

        if relationship_type == "customer_of":
            # Upstream supplier; impact is muted (lag proxy factor 0.5)
            base_signal *= 0.5
        # supplier_to: full propagation (downstream customer feels it directly)

        return round(base_signal, 6)

    def backtest_readthroughs(
        self, historical_earnings: dict, price_data: dict
    ) -> dict:
        """
        Backtest accuracy of readthroughs historically.
        For each historical earnings report: generate readthroughs, check forward return.

        historical_earnings: {date_str: {ticker: earnings_result_dict}}
        price_data: {ticker: {date_str: price_float}}

        Returns: {accuracy, mean_signal_strength, calibration_by_industry}
        """
        if self.graph is None:
            logger.warning("No graph available for backtest")
            return {"accuracy": 0.0, "mean_signal_strength": 0.0, "calibration_by_industry": {}}

        correct = 0
        total = 0
        signal_strengths = []
        industry_results: dict = {}

        for date_str, ticker_earnings in historical_earnings.items():
            for ticker, earnings_result in ticker_earnings.items():
                readthroughs = self.generate_readthroughs(ticker, earnings_result, price_data)
                for rt in readthroughs:
                    neighbor = rt["ticker"]
                    signal = rt["value"]
                    # Check 30-day forward return as proxy for quarterly
                    fwd_return = self._calc_forward_return(neighbor, date_str, price_data, days=30)
                    if fwd_return is None:
                        continue
                    total += 1
                    signal_strengths.append(abs(signal))
                    # Signal correct if sign matches forward return direction
                    if (signal > 0 and fwd_return > 0) or (signal < 0 and fwd_return < 0):
                        correct += 1
                    industry = earnings_result.get("industry", "UNKNOWN")
                    if industry not in industry_results:
                        industry_results[industry] = {"correct": 0, "total": 0}
                    industry_results[industry]["total"] += 1
                    if (signal > 0 and fwd_return > 0) or (signal < 0 and fwd_return < 0):
                        industry_results[industry]["correct"] += 1

        accuracy = correct / total if total > 0 else 0.0
        mean_strength = sum(signal_strengths) / len(signal_strengths) if signal_strengths else 0.0

        calibration = {}
        for ind, res in industry_results.items():
            calibration[ind] = res["correct"] / res["total"] if res["total"] > 0 else 0.0

        return {
            "accuracy": round(accuracy, 4),
            "mean_signal_strength": round(mean_strength, 6),
            "calibration_by_industry": calibration,
            "total_observations": total,
        }

    def _calc_forward_return(
        self, ticker: str, date_str: str, price_data: dict, days: int = 30
    ) -> Optional[float]:
        """Calculate forward return for ticker from date_str over 'days' trading days."""
        try:
            prices = price_data.get(ticker, {})
            if not prices:
                return None
            sorted_dates = sorted(prices.keys())
            if date_str not in sorted_dates:
                # Find closest date
                idx = next(
                    (i for i, d in enumerate(sorted_dates) if d >= date_str), None
                )
                if idx is None:
                    return None
                start_date = sorted_dates[idx]
            else:
                idx = sorted_dates.index(date_str)
                start_date = date_str

            end_idx = min(idx + days, len(sorted_dates) - 1)
            start_price = prices[start_date]
            end_price = prices[sorted_dates[end_idx]]
            if start_price == 0:
                return None
            return (end_price - start_price) / start_price
        except Exception as exc:
            logger.warning("Forward return calc failed for %s: %s", ticker, exc)
            return None

    def calc_upstream_risk(self, ticker: str, altdata_scores: dict) -> float:
        """
        UpstreamRisk = sum(supplier_stress_score * dependency_weight)
        supplier_stress_score from altdata signals (sentiment, short interest).
        """
        upstream = self._get_direct_neighbors(ticker, direction="upstream")
        if not upstream:
            return 0.0

        total_risk = 0.0
        for supplier, weight in upstream:
            stress_score = altdata_scores.get(supplier, {}).get("stress_score", 0.0)
            if stress_score == 0.0:
                # Derive from available signals
                sentiment = altdata_scores.get(supplier, {}).get("sentiment", 0.0)
                short_interest = altdata_scores.get(supplier, {}).get("short_interest", 0.0)
                # Negative sentiment and high short interest = stress
                stress_score = max(0.0, -sentiment * 0.5 + short_interest * 0.5)
            total_risk += stress_score * weight

        return round(min(1.0, total_risk), 4)

    def calc_downstream_risk(self, ticker: str, altdata_scores: dict) -> float:
        """DownstreamRisk = sum(customer_weakness_score * revenue_dependency)"""
        downstream = self._get_direct_neighbors(ticker, direction="downstream")
        if not downstream:
            return 0.0

        total_risk = 0.0
        for customer, weight in downstream:
            weakness_score = altdata_scores.get(customer, {}).get("weakness_score", 0.0)
            if weakness_score == 0.0:
                sentiment = altdata_scores.get(customer, {}).get("sentiment", 0.0)
                rev_growth = altdata_scores.get(customer, {}).get("revenue_growth", 0.0)
                weakness_score = max(0.0, -sentiment * 0.4 + max(0.0, -rev_growth) * 0.6)
            total_risk += weakness_score * weight

        return round(min(1.0, total_risk), 4)

    def _get_direct_neighbors(self, ticker: str, direction: str) -> list:
        """Return [(neighbor_ticker, weight)] for direct (depth=1) neighbors."""
        neighbors = self._get_all_neighbors(ticker, max_depth=1)
        if direction == "upstream":
            return [(n, w) for n, rel, d, w in neighbors if rel == "customer_of" and d == 1]
        else:
            return [(n, w) for n, rel, d, w in neighbors if rel == "supplier_to" and d == 1]
