"""
ChainRiskScorer — overall supply chain risk per company.
Combines upstream/downstream risk, concentration, and network centrality.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

# Risk level thresholds
RISK_THRESHOLDS = {
    "CRITICAL": 0.75,
    "HIGH": 0.5,
    "MODERATE": 0.25,
}


class ChainRiskScorer:
    """
    Scores overall supply chain risk per company by combining:
    - Upstream supplier stress
    - Downstream customer weakness
    - Revenue concentration
    - Network centrality (systemic risk)
    """

    def __init__(self, config: dict):
        self.config = config
        sc_config = config.get("deepdata", {}).get("supply_chain", {})
        self.concentration_threshold = sc_config.get("concentration_threshold", 0.50)
        self.weights = sc_config.get("risk_weights", {
            "upstream": 0.3,
            "downstream": 0.3,
            "concentration": 0.2,
            "centrality": 0.2,
        })
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def score(
        self,
        ticker: str,
        upstream_risk: float,
        downstream_risk: float,
        graph=None,
    ) -> dict:
        """
        Returns:
        {
          ticker, total_risk: float,
          upstream_risk, downstream_risk,
          concentration_risk: float,
          network_centrality: float,
          risk_signal: float,  # -1 (low risk, bullish) to +1 (high risk, bearish)
          risk_level: 'LOW'|'MODERATE'|'HIGH'|'CRITICAL'
        }
        """
        concentration_risk = 0.0
        network_centrality = 0.0

        if graph is not None:
            try:
                concentration_risk = self.calc_concentration_risk(ticker, graph)
            except Exception as exc:
                logger.warning("Concentration risk calc failed for %s: %s", ticker, exc)

            try:
                network_centrality = self.calc_network_centrality(ticker, graph)
            except Exception as exc:
                logger.warning("Network centrality calc failed for %s: %s", ticker, exc)

        w = self.weights
        total_risk = (
            upstream_risk * w.get("upstream", 0.3)
            + downstream_risk * w.get("downstream", 0.3)
            + concentration_risk * w.get("concentration", 0.2)
            + network_centrality * w.get("centrality", 0.2)
        )
        total_risk = round(min(1.0, max(0.0, total_risk)), 4)

        # risk_signal: positive = bearish (high risk), negative = bullish (low risk)
        # Map [0,1] total_risk -> [-1, +1] signal
        risk_signal = round((total_risk - 0.5) * 2.0, 4)

        risk_level = self._classify_risk_level(total_risk)

        return {
            "ticker": ticker,
            "total_risk": total_risk,
            "upstream_risk": round(upstream_risk, 4),
            "downstream_risk": round(downstream_risk, 4),
            "concentration_risk": round(concentration_risk, 4),
            "network_centrality": round(network_centrality, 4),
            "risk_signal": risk_signal,
            "risk_level": risk_level,
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

    def _classify_risk_level(self, total_risk: float) -> str:
        """Map total_risk score to categorical risk level."""
        if total_risk >= RISK_THRESHOLDS["CRITICAL"]:
            return "CRITICAL"
        elif total_risk >= RISK_THRESHOLDS["HIGH"]:
            return "HIGH"
        elif total_risk >= RISK_THRESHOLDS["MODERATE"]:
            return "MODERATE"
        else:
            return "LOW"

    def calc_concentration_risk(self, ticker: str, graph) -> float:
        """
        Single customer > 50% revenue = high concentration risk.
        Returns 0.0 to 1.0 based on dependency concentration (Herfindahl-style).
        """
        try:
            import networkx as nx
            if isinstance(graph, nx.DiGraph):
                # Downstream edges: ticker -> customer
                edges = list(graph.out_edges(ticker, data=True))
                if not edges:
                    return 0.0
                weights = [d.get("weight", 0.0) for _, _, d in edges]
            else:
                raise ImportError
        except (ImportError, Exception):
            # Dict-based fallback
            if isinstance(graph, dict):
                out_edges = graph.get("edges", {}).get(ticker, {})
                weights = [
                    v.get("weight", 0.0) if isinstance(v, dict) else 0.0
                    for v in out_edges.values()
                ]
            else:
                return 0.0

        if not weights:
            return 0.0

        total_weight = sum(weights)
        if total_weight == 0:
            return 0.0

        # Check if any single customer exceeds threshold
        max_weight = max(weights)
        if max_weight >= self.concentration_threshold:
            # Very high concentration - map to high risk
            concentration_risk = min(1.0, max_weight)
        else:
            # Herfindahl-Hirschman Index (HHI) normalised
            # HHI = sum(market_share^2); max = 1.0 (monopoly)
            shares = [w / total_weight for w in weights]
            hhi = sum(s ** 2 for s in shares)
            concentration_risk = hhi  # Already in [0,1]

        return round(concentration_risk, 4)

    def calc_network_centrality(self, ticker: str, graph) -> float:
        """
        Use networkx betweenness_centrality. High centrality = systemic risk.
        Returns normalised centrality score 0.0 to 1.0.
        """
        try:
            import networkx as nx
            if isinstance(graph, nx.DiGraph) and ticker in graph:
                centrality = nx.betweenness_centrality(graph, normalized=True)
                raw = centrality.get(ticker, 0.0)
                return round(min(1.0, raw), 4)
        except ImportError:
            pass
        except Exception as exc:
            logger.warning("Betweenness centrality failed for %s: %s", ticker, exc)

        # Dict-based fallback: approximate by degree centrality
        if isinstance(graph, dict):
            edges = graph.get("edges", {})
            nodes = graph.get("nodes", {})
            n_nodes = len(nodes)
            if n_nodes <= 1:
                return 0.0
            # Out-degree + in-degree normalised
            out_deg = len(edges.get(ticker, {}))
            in_deg = sum(1 for dsts in edges.values() if ticker in dsts)
            degree = out_deg + in_deg
            return round(min(1.0, degree / (n_nodes - 1)), 4)

        return 0.0

    def generate_collector_results(self, ticker: str, score: dict) -> list:
        """Convert ChainRiskScorer output to CollectorResult format."""
        ts = datetime.now(timezone.utc).isoformat()
        results = []

        # Main risk signal
        results.append({
            "source": "supply_chain_risk",
            "ticker": ticker,
            "market": "US",
            "data_type": "chain_risk",
            "value": score.get("risk_signal", 0.0),
            "raw_data": score,
            "timestamp": ts,
            "quality_score": 1.0 - score.get("total_risk", 0.5),
        })

        # Concentration risk sub-signal
        if score.get("concentration_risk", 0.0) > 0:
            results.append({
                "source": "supply_chain_risk",
                "ticker": ticker,
                "market": "US",
                "data_type": "concentration_risk",
                "value": score.get("concentration_risk", 0.0),
                "raw_data": {
                    "concentration_risk": score.get("concentration_risk"),
                    "risk_level": score.get("risk_level"),
                },
                "timestamp": ts,
                "quality_score": 0.8,
            })

        # Network centrality sub-signal
        if score.get("network_centrality", 0.0) > 0:
            results.append({
                "source": "supply_chain_risk",
                "ticker": ticker,
                "market": "US",
                "data_type": "network_centrality",
                "value": score.get("network_centrality", 0.0),
                "raw_data": {
                    "network_centrality": score.get("network_centrality"),
                    "risk_level": score.get("risk_level"),
                },
                "timestamp": ts,
                "quality_score": 0.7,
            })

        return results
