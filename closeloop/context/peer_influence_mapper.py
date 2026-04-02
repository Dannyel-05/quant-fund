"""
PeerInfluenceMapper — maps influence between related stocks when one company
triggers a significant event (earnings, upgrade, M&A, index rebalancing).

Four influence types:
  TYPE1 — Direct competitors (same sector, similar market cap, correlated price)
  TYPE2 — Analyst coverage contagion (upgrades cluster in correlated groups)
  TYPE3 — Index membership effects (delegated to IndexRebalancingDetector)
  TYPE4 — M&A spillover (delegated to MergerSpilloverDetector)
"""
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False
    logger.warning("yfinance not available — PeerInfluenceMapper will use fallback paths")

try:
    import numpy as np
    _NP_AVAILABLE = True
except ImportError:
    _NP_AVAILABLE = False
    logger.warning("numpy not available — correlation computed with fallback")

try:
    from closeloop.context.index_rebalancing_detector import IndexRebalancingDetector
    _IRD_AVAILABLE = True
except Exception:
    _IRD_AVAILABLE = False
    logger.warning("IndexRebalancingDetector not importable — TYPE3 signals disabled")

try:
    from closeloop.context.merger_spillover_detector import MergerSpilloverDetector
    _MSD_AVAILABLE = True
except Exception:
    _MSD_AVAILABLE = False
    logger.warning("MergerSpilloverDetector not importable — TYPE4 signals disabled")

# Influence type weights
_TYPE_WEIGHTS = {
    "TYPE1": 0.5,
    "TYPE2": 0.3,
    "TYPE3": 0.4,
    "TYPE4": 0.3,
}

# Minimum correlation to consider as a peer
_MIN_CORRELATION = 0.5
# Market-cap ratio window (within N× of trigger company)
_MAX_CAP_RATIO = 3.0
# Days of price history for correlation
_CORR_WINDOW = 252


def _safe_float(val, default: float = 0.0) -> float:
    """Return float or default if conversion fails."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _pearson_corr(xs: list, ys: list) -> float:
    """Pure-Python Pearson correlation; returns 0.0 on any failure."""
    try:
        n = min(len(xs), len(ys))
        if n < 20:
            return 0.0
        xs, ys = xs[:n], ys[:n]
        if _NP_AVAILABLE:
            arr = np.corrcoef(xs, ys)
            v = float(arr[0, 1])
            return 0.0 if math.isnan(v) else v
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
        den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
        den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
        if den_x == 0 or den_y == 0:
            return 0.0
        return num / (den_x * den_y)
    except Exception:
        return 0.0


def _fetch_ticker_info(ticker: str) -> dict:
    """Return yfinance info dict or empty dict on failure."""
    if not _YF_AVAILABLE:
        return {}
    try:
        return yf.Ticker(ticker).info or {}
    except Exception as exc:
        logger.debug("yfinance info failed for %s: %s", ticker, exc)
        return {}


def _fetch_price_history(ticker: str, period_days: int = _CORR_WINDOW) -> list:
    """Return list of adjusted close prices (oldest first). Empty list on failure."""
    if not _YF_AVAILABLE:
        return []
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=f"{period_days}d", auto_adjust=True)
        if hist is None or hist.empty:
            return []
        closes = hist["Close"].dropna().tolist()
        return closes
    except Exception as exc:
        logger.debug("yfinance history failed for %s: %s", ticker, exc)
        return []


class PeerInfluenceMapper:
    """
    Identifies peer companies likely to be influenced when a trigger event
    occurs on a source company (earnings surprise, analyst upgrade, M&A, index
    rebalancing), then generates directional signals for those peers.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        cfg = (config or {}).get("closeloop", {}).get("context", {}).get("peer_influence", {})
        self.max_peers: int = cfg.get("max_peers", 10)
        self.influence_decay: float = cfg.get("influence_decay", 0.6)
        self.store = store

        self._ird: Optional[object] = None
        self._msd: Optional[object] = None
        if _IRD_AVAILABLE:
            try:
                self._ird = IndexRebalancingDetector(store=store, config=config)
            except Exception as exc:
                logger.warning("Could not instantiate IndexRebalancingDetector: %s", exc)
        if _MSD_AVAILABLE:
            try:
                self._msd = MergerSpilloverDetector(store=store, config=config)
            except Exception as exc:
                logger.warning("Could not instantiate MergerSpilloverDetector: %s", exc)

        # Cache to avoid repeated yfinance calls within one session
        self._info_cache: Dict[str, dict] = {}
        self._price_cache: Dict[str, list] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_info(self, ticker: str) -> dict:
        if ticker not in self._info_cache:
            self._info_cache[ticker] = _fetch_ticker_info(ticker)
        return self._info_cache[ticker]

    def _get_prices(self, ticker: str) -> list:
        if ticker not in self._price_cache:
            self._price_cache[ticker] = _fetch_price_history(ticker)
        return self._price_cache[ticker]

    def _market_cap(self, ticker: str) -> float:
        info = self._get_info(ticker)
        return _safe_float(info.get("marketCap") or info.get("market_cap"), 0.0)

    def _sector(self, ticker: str) -> str:
        info = self._get_info(ticker)
        return info.get("sector", "") or ""

    def _correlation_with_trigger(self, trigger_ticker: str, peer_ticker: str) -> float:
        """Pearson correlation of daily returns over _CORR_WINDOW days."""
        t_prices = self._get_prices(trigger_ticker)
        p_prices = self._get_prices(peer_ticker)
        n = min(len(t_prices), len(p_prices))
        if n < 20:
            return 0.0
        t_ret = [t_prices[i] / t_prices[i - 1] - 1 for i in range(1, n)]
        p_ret = [p_prices[i] / p_prices[i - 1] - 1 for i in range(1, n)]
        return _pearson_corr(t_ret, p_ret)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_peers(self, ticker: str, universe: Optional[List[str]] = None) -> List[Dict]:
        """
        Return list of peer dicts: {ticker, correlation, market_cap_ratio,
        sector_match, influence_type}.

        Peers are identified by:
          - TYPE1: same sector + cap within 3× + correlation > 0.5
          - TYPE2: any ticker with correlation > 0.5 (analyst contagion)
        TYPE3/TYPE4 delegate to sub-detectors when available.

        Graceful fallback to empty list on any failure.
        """
        peers: List[Dict] = []
        if universe is None:
            universe = []
        if not universe:
            logger.debug("find_peers: empty universe for %s", ticker)
            return peers

        try:
            trigger_cap = self._market_cap(ticker)
            trigger_sector = self._sector(ticker)

            for candidate in universe:
                if candidate == ticker:
                    continue
                try:
                    corr = self._correlation_with_trigger(ticker, candidate)
                    if corr < _MIN_CORRELATION:
                        continue

                    cand_cap = self._market_cap(candidate)
                    cand_sector = self._sector(candidate)
                    sector_match = int(
                        bool(trigger_sector) and trigger_sector == cand_sector
                    )

                    # Market cap ratio (symmetric)
                    if trigger_cap > 0 and cand_cap > 0:
                        cap_ratio = max(trigger_cap, cand_cap) / min(trigger_cap, cand_cap)
                    else:
                        cap_ratio = float("inf")

                    # Determine influence type
                    if sector_match and cap_ratio <= _MAX_CAP_RATIO:
                        inf_type = "TYPE1"
                    else:
                        inf_type = "TYPE2"  # analyst contagion by correlation alone

                    peers.append({
                        "ticker": candidate,
                        "correlation": round(corr, 4),
                        "market_cap_ratio": round(cap_ratio, 2) if cap_ratio != float("inf") else None,
                        "sector_match": bool(sector_match),
                        "influence_type": inf_type,
                    })
                except Exception as exc:
                    logger.debug("Peer candidate %s failed: %s", candidate, exc)

            # Sort by correlation descending, cap to max_peers
            peers.sort(key=lambda p: p["correlation"], reverse=True)
            peers = peers[: self.max_peers]
            logger.debug("find_peers(%s): found %d peers", ticker, len(peers))
        except Exception as exc:
            logger.error("find_peers failed for %s: %s", ticker, exc)

        return peers

    def calc_peer_influence_score(
        self,
        trigger_ticker: str,
        trigger_event: str,
        trigger_magnitude: float,
        influenced_ticker: str,
    ) -> float:
        """
        Compute 0-1 influence score:
          proximity_score = market_cap_similarity * correlation * sector_match_factor
          score = trigger_magnitude * influence_type_weight * proximity_score

        Returns 0.0 on any failure.
        """
        try:
            corr = self._correlation_with_trigger(trigger_ticker, influenced_ticker)
            if corr < 0:
                corr = 0.0  # only positive correlation matters for halo

            trigger_cap = self._market_cap(trigger_ticker)
            inf_cap = self._market_cap(influenced_ticker)
            if trigger_cap > 0 and inf_cap > 0:
                cap_ratio = max(trigger_cap, inf_cap) / min(trigger_cap, inf_cap)
                cap_similarity = max(0.0, 1.0 - math.log(cap_ratio) / math.log(_MAX_CAP_RATIO + 1))
            else:
                cap_similarity = 0.5  # neutral when unknown

            trigger_sector = self._sector(trigger_ticker)
            inf_sector = self._sector(influenced_ticker)
            sector_match_factor = 1.0 if (trigger_sector and trigger_sector == inf_sector) else 0.5

            # Determine primary influence type for weight lookup
            cap_ok = (trigger_cap > 0 and inf_cap > 0
                      and max(trigger_cap, inf_cap) / min(trigger_cap, inf_cap) <= _MAX_CAP_RATIO)
            if sector_match_factor == 1.0 and cap_ok:
                inf_type = "TYPE1"
            else:
                inf_type = "TYPE2"

            type_weight = _TYPE_WEIGHTS.get(inf_type, 0.3)
            proximity_score = cap_similarity * corr * sector_match_factor
            raw_score = abs(trigger_magnitude) * type_weight * proximity_score

            # Clamp to [0, 1]
            score = max(0.0, min(1.0, raw_score))
            return round(score, 4)
        except Exception as exc:
            logger.error("calc_peer_influence_score failed: %s", exc)
            return 0.0

    def generate_signals(
        self,
        trigger_ticker: str,
        trigger_event: str,
        trigger_magnitude: float,
        universe: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        For each peer scoring above 0.4, emit a PEER_LONG or PEER_SHORT signal
        sized at 50% of normal.

        Returns list of signal dicts: {ticker, direction, confidence,
        signal_type, peer_score, source_ticker}.
        """
        signals: List[Dict] = []
        try:
            peers = self.find_peers(trigger_ticker, universe=universe)
            for peer in peers:
                peer_ticker = peer["ticker"]
                try:
                    score = self.calc_peer_influence_score(
                        trigger_ticker, trigger_event, trigger_magnitude, peer_ticker
                    )
                    if score < 0.4:
                        continue

                    # Halo effect: positive trigger_magnitude → long peers,
                    # negative → short (sector contagion)
                    direction = "PEER_LONG" if trigger_magnitude >= 0 else "PEER_SHORT"
                    signals.append({
                        "ticker": peer_ticker,
                        "direction": direction,
                        "confidence": score,
                        "signal_type": peer["influence_type"],
                        "peer_score": score,
                        "source_ticker": trigger_ticker,
                        "trigger_event": trigger_event,
                        "size_fraction": 0.5,
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as exc:
                    logger.debug("Signal generation failed for peer %s: %s", peer_ticker, exc)

            # Append TYPE3 / TYPE4 signals if sub-detectors available
            if self._ird is not None and universe:
                try:
                    ird_signals = self._ird.generate_signals(universe) or []
                    for s in ird_signals:
                        s["signal_type"] = "TYPE3"
                        s["source_ticker"] = trigger_ticker
                        signals.append(s)
                except Exception as exc:
                    logger.debug("IRD signal generation failed: %s", exc)

            if self._msd is not None and universe:
                try:
                    msd_signals = self._msd.generate_signals(universe) or []
                    for s in msd_signals:
                        s["signal_type"] = "TYPE4"
                        s["source_ticker"] = trigger_ticker
                        signals.append(s)
                except Exception as exc:
                    logger.debug("MSD signal generation failed: %s", exc)

            logger.info(
                "generate_signals(%s, %s): %d signals emitted",
                trigger_ticker, trigger_event, len(signals),
            )
        except Exception as exc:
            logger.error("generate_signals failed: %s", exc)

        return signals

    def record_outcome(self, trade_id: int, closed_trade: dict) -> None:
        """
        Record actual outcome vs prediction in store.peer_influence_outcomes.
        Silently no-ops when store is unavailable.
        """
        if self.store is None:
            return
        try:
            outcome = {
                "trigger_ticker": closed_trade.get("source_ticker", ""),
                "trigger_event": closed_trade.get("trigger_event", ""),
                "influenced_ticker": closed_trade.get("ticker", ""),
                "influence_type": closed_trade.get("signal_type", ""),
                "predicted_direction": 1 if closed_trade.get("direction") == "PEER_LONG" else -1,
                "actual_direction": closed_trade.get("actual_direction"),
                "predicted_magnitude": closed_trade.get("peer_score"),
                "actual_magnitude": closed_trade.get("actual_magnitude"),
                "lag_days": closed_trade.get("holding_days"),
                "was_correct": closed_trade.get("was_correct"),
                "pnl": closed_trade.get("net_pnl"),
            }
            self.store.record_peer_influence(outcome)
            logger.debug("record_outcome: trade_id=%s stored", trade_id)
        except Exception as exc:
            logger.error("record_outcome failed: %s", exc)

    def render_accuracy_summary(self) -> str:
        """
        Text summary of influence type accuracy and mean PnL from store.
        Returns placeholder string when store is unavailable or no data.
        """
        if self.store is None:
            return "PeerInfluenceMapper: no store configured."
        try:
            conn = self.store._conn()
            rows = conn.execute("""
                SELECT influence_type,
                       COUNT(*) as n,
                       AVG(was_correct) as accuracy,
                       AVG(pnl) as mean_pnl
                FROM peer_influence_outcomes
                GROUP BY influence_type
                ORDER BY influence_type
            """).fetchall()
            if not rows:
                return "PeerInfluenceMapper: no recorded outcomes yet."

            lines = ["PeerInfluenceMapper Accuracy Summary",
                     "-" * 50,
                     f"{'Type':<10} {'N':>6} {'Accuracy':>10} {'Mean PnL':>12}"]
            for r in rows:
                lines.append(
                    f"{r['influence_type'] or 'N/A':<10} "
                    f"{r['n']:>6} "
                    f"{(r['accuracy'] or 0) * 100:>9.1f}% "
                    f"{(r['mean_pnl'] or 0):>12.4f}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.error("render_accuracy_summary failed: %s", exc)
            return f"PeerInfluenceMapper: summary unavailable ({exc})"
