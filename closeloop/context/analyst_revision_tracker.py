"""
AnalystRevisionTracker — tracks analyst estimate revisions and computes
EstimateRevisionMomentum (ERM) to adjust PEAD holding periods and signal
strength, and to generate standalone revision momentum signals.
"""
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False
    logger.warning("yfinance not available — AnalystRevisionTracker will use fallbacks")

try:
    import pandas as pd
    _PD_AVAILABLE = True
except ImportError:
    _PD_AVAILABLE = False
    logger.warning("pandas not available — some revision logic degraded")


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


class AnalystRevisionTracker:
    """
    Tracks analyst EPS and price target revisions via yfinance and computes
    EstimateRevisionMomentum (ERM) for use as a PEAD modifier and standalone
    signal generator.
    """

    def __init__(self, store=None, config: Optional[Dict] = None):
        cfg = (config or {}).get("closeloop", {}).get("context", {}).get("analyst_revisions", {})
        self.lookback_days: int = cfg.get("lookback_days", 30)
        self.store = store
        # Rolling ERM history per ticker for acceleration detection: {ticker: [erm, erm, ...]}
        self._erm_history: Dict[str, List[float]] = {}

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_revisions(self, ticker: str) -> Dict:
        """
        Fetch analyst data via yfinance:
          - analyst_price_targets: current/mean/low/high targets
          - earnings_estimate: EPS consensus and revisions
          - revenue_estimate: revenue consensus

        Returns raw dict. Graceful empty dict on any failure.
        """
        if not _YF_AVAILABLE:
            return {}
        try:
            t = yf.Ticker(ticker)
            result: Dict = {}

            # Price targets
            try:
                apt = t.analyst_price_targets
                if apt is not None:
                    # Can be a dict or DataFrame; normalise to dict
                    if _PD_AVAILABLE and hasattr(apt, "to_dict"):
                        result["analyst_price_targets"] = apt.to_dict()
                    elif isinstance(apt, dict):
                        result["analyst_price_targets"] = apt
            except Exception as exc:
                logger.debug("analyst_price_targets failed for %s: %s", ticker, exc)

            # Earnings estimate
            try:
                ee = t.earnings_estimate
                if ee is not None:
                    if _PD_AVAILABLE and hasattr(ee, "to_dict"):
                        result["earnings_estimate"] = ee.to_dict()
                    elif isinstance(ee, dict):
                        result["earnings_estimate"] = ee
            except Exception as exc:
                logger.debug("earnings_estimate failed for %s: %s", ticker, exc)

            # Revenue estimate
            try:
                re = t.revenue_estimate
                if re is not None:
                    if _PD_AVAILABLE and hasattr(re, "to_dict"):
                        result["revenue_estimate"] = re.to_dict()
                    elif isinstance(re, dict):
                        result["revenue_estimate"] = re
            except Exception as exc:
                logger.debug("revenue_estimate failed for %s: %s", ticker, exc)

            # Info for supplementary context
            try:
                info = t.info or {}
                result["info"] = {
                    k: info.get(k)
                    for k in (
                        "targetMeanPrice", "targetLowPrice", "targetHighPrice",
                        "targetMedianPrice", "currentPrice", "sector",
                    )
                }
            except Exception as exc:
                logger.debug("info fetch failed for %s: %s", ticker, exc)

            return result
        except Exception as exc:
            logger.error("fetch_revisions failed for %s: %s", ticker, exc)
            return {}

    # ------------------------------------------------------------------
    # ERM computation
    # ------------------------------------------------------------------

    def calc_erm(self, ticker: str) -> float:
        """
        EstimateRevisionMomentum (ERM):
          ERM = (n_up - n_down) / (n_up + n_down + 1)

        Approximated from yfinance data by comparing current EPS consensus
        to the 4-week-ago value.  When the direction is upward we infer
        n_up = 1, n_down = 0 (and vice-versa for downward), scaled by the
        magnitude of the revision relative to price.

        Returns float in [-1, +1]. Returns 0.0 on insufficient data.
        """
        try:
            data = self.fetch_revisions(ticker)
            if not data:
                return 0.0

            # --- Try earnings_estimate for 0q (current quarter) ---
            ee = data.get("earnings_estimate")
            eps_current: Optional[float] = None
            eps_4w_ago: Optional[float] = None

            if ee and _PD_AVAILABLE:
                try:
                    import pandas as pd  # local re-import in case of deferred load
                    df = pd.DataFrame(ee)
                    if "avg" in df.columns and "0q" in df.index:
                        eps_current = _safe_float(df.loc["0q", "avg"], None)
                    if "4WeeksAgo" in df.columns and "0q" in df.index:
                        eps_4w_ago = _safe_float(df.loc["0q", "4WeeksAgo"], None)
                    elif "7daysAgo" in df.columns and "0q" in df.index:
                        eps_4w_ago = _safe_float(df.loc["0q", "7daysAgo"], None)
                except Exception as exc:
                    logger.debug("ERM earnings_estimate parse failed: %s", exc)

            # Fallback: price target direction as proxy
            if eps_current is None or eps_4w_ago is None:
                info = data.get("info", {})
                target_mean = _safe_float(info.get("targetMeanPrice"), 0.0)
                current_price = _safe_float(info.get("currentPrice"), 0.0)
                if target_mean > 0 and current_price > 0:
                    upside = (target_mean - current_price) / current_price
                    erm = _clamp(upside * 2, -1.0, 1.0)
                    self._update_erm_history(ticker, erm)
                    return round(erm, 4)
                return 0.0

            if eps_4w_ago == 0:
                return 0.0

            revision_pct = (eps_current - eps_4w_ago) / abs(eps_4w_ago)
            # Map revision percentage to n_up/n_down ratio
            if revision_pct > 0:
                n_up = min(10, max(1, round(revision_pct * 20)))
                n_down = 0
            elif revision_pct < 0:
                n_up = 0
                n_down = min(10, max(1, round(abs(revision_pct) * 20)))
            else:
                n_up = n_down = 0

            erm = (n_up - n_down) / (n_up + n_down + 1)
            erm = _clamp(erm, -1.0, 1.0)
            self._update_erm_history(ticker, erm)
            return round(erm, 4)

        except Exception as exc:
            logger.error("calc_erm failed for %s: %s", ticker, exc)
            return 0.0

    def _update_erm_history(self, ticker: str, erm: float) -> None:
        """Append erm to rolling history (max 10 observations)."""
        hist = self._erm_history.get(ticker, [])
        hist.append(erm)
        self._erm_history[ticker] = hist[-10:]

    # ------------------------------------------------------------------
    # PEAD modifier
    # ------------------------------------------------------------------

    def pead_modifier(
        self,
        ticker: str,
        base_holding_days: int,
        base_signal_strength: float,
    ) -> Tuple[int, float]:
        """
        Adjust PEAD holding period and signal strength based on ERM.

          holding_period = base * (1 + ERM * 0.5)  clamped [base*0.5, base*2]
          signal_strength = base * (1 + ERM * 0.3)  clamped [base*0.5, base*1.5]

        Returns (adjusted_holding_days, adjusted_strength).
        """
        try:
            erm = self.calc_erm(ticker)
            adj_days = base_holding_days * (1.0 + erm * 0.5)
            adj_days = _clamp(adj_days, base_holding_days * 0.5, base_holding_days * 2.0)

            adj_strength = base_signal_strength * (1.0 + erm * 0.3)
            adj_strength = _clamp(
                adj_strength, base_signal_strength * 0.5, base_signal_strength * 1.5
            )

            return int(round(adj_days)), round(adj_strength, 4)
        except Exception as exc:
            logger.error("pead_modifier failed for %s: %s", ticker, exc)
            return base_holding_days, base_signal_strength

    # ------------------------------------------------------------------
    # Standalone signal
    # ------------------------------------------------------------------

    def standalone_signal(self, ticker: str) -> Optional[Dict]:
        """
        Generate a standalone revision-momentum signal:
          ERM > 0.6  → weak LONG, size 25% normal
          ERM < -0.6 → weak SHORT, size 25% normal
          ERM improving for 3 consecutive observations → REVISION_MOMENTUM_BUILDING

        Returns signal dict or None.
        """
        try:
            erm = self.calc_erm(ticker)
            signal: Optional[Dict] = None

            if erm > 0.6:
                signal = {
                    "ticker": ticker,
                    "direction": "LONG",
                    "signal_type": "ANALYST_REVISION_MOMENTUM",
                    "erm": erm,
                    "confidence": round(erm * 0.6, 4),
                    "size_fraction": 0.25,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            elif erm < -0.6:
                signal = {
                    "ticker": ticker,
                    "direction": "SHORT",
                    "signal_type": "ANALYST_REVISION_MOMENTUM",
                    "erm": erm,
                    "confidence": round(abs(erm) * 0.6, 4),
                    "size_fraction": 0.25,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }

            # Acceleration: 3 consecutive improvements
            hist = self._erm_history.get(ticker, [])
            if len(hist) >= 3:
                last3 = hist[-3:]
                if last3[0] < last3[1] < last3[2]:
                    acc_signal: Dict = {
                        "ticker": ticker,
                        "direction": "LONG",
                        "signal_type": "REVISION_MOMENTUM_BUILDING",
                        "erm": erm,
                        "erm_history": last3,
                        "confidence": round(min(erm * 0.8, 0.9), 4),
                        "size_fraction": 0.25,
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    # If we already have a directional signal, enrich it
                    if signal is not None:
                        signal["acceleration"] = True
                        signal["signal_type"] = "REVISION_MOMENTUM_BUILDING"
                    else:
                        signal = acc_signal

            return signal
        except Exception as exc:
            logger.error("standalone_signal failed for %s: %s", ticker, exc)
            return None

    # ------------------------------------------------------------------
    # Sector sensitivity
    # ------------------------------------------------------------------

    def sector_sensitivity_map(self) -> Dict[str, float]:
        """
        Compute per-sector ERM sensitivity from stored outcomes:
          {sector: correlation(ERM, 20d_return)}

        Returns empty dict when store is unavailable or data is insufficient.
        """
        if self.store is None:
            return {}
        try:
            conn = self.store._conn()
            rows = conn.execute("""
                SELECT a.ticker, a.revision_magnitude, a.forward_return_20d,
                       t.sector
                FROM analyst_revision_outcomes a
                LEFT JOIN trade_ledger t ON t.ticker = a.ticker
                WHERE a.revision_magnitude IS NOT NULL
                  AND a.forward_return_20d IS NOT NULL
                  AND t.sector IS NOT NULL
                ORDER BY a.created_at DESC
                LIMIT 2000
            """).fetchall()

            if not rows:
                return {}

            # Group by sector
            sector_data: Dict[str, Tuple[List[float], List[float]]] = {}
            for r in rows:
                sec = r["sector"] or "Unknown"
                if sec not in sector_data:
                    sector_data[sec] = ([], [])
                sector_data[sec][0].append(_safe_float(r["revision_magnitude"]))
                sector_data[sec][1].append(_safe_float(r["forward_return_20d"]))

            result: Dict[str, float] = {}
            for sec, (erms, rets) in sector_data.items():
                if len(erms) < 5:
                    continue
                n = len(erms)
                mean_e = sum(erms) / n
                mean_r = sum(rets) / n
                num = sum((erms[i] - mean_e) * (rets[i] - mean_r) for i in range(n))
                den_e = math.sqrt(sum((x - mean_e) ** 2 for x in erms))
                den_r = math.sqrt(sum((x - mean_r) ** 2 for x in rets))
                if den_e == 0 or den_r == 0:
                    result[sec] = 0.0
                else:
                    corr = num / (den_e * den_r)
                    result[sec] = round(_clamp(corr, -1.0, 1.0), 4)

            return result
        except Exception as exc:
            logger.error("sector_sensitivity_map failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Outcome recording
    # ------------------------------------------------------------------

    def record_outcome(
        self,
        ticker: str,
        revision_type: str,
        revision_magnitude: float,
        forward_return_5d: float,
        forward_return_20d: float,
        combined_with_pead: bool = False,
    ) -> None:
        """
        Store revision outcome in store.analyst_revision_outcomes.
        Silently no-ops when store is unavailable.
        """
        if self.store is None:
            return
        try:
            conn = self.store._conn()
            conn.execute("""
                INSERT INTO analyst_revision_outcomes
                    (ticker, revision_type, revision_magnitude, revision_date,
                     forward_return_5d, forward_return_20d, combined_with_pead)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                revision_type,
                revision_magnitude,
                datetime.now(timezone.utc).date().isoformat(),
                forward_return_5d,
                forward_return_20d,
                int(combined_with_pead),
            ))
            conn.commit()
            logger.debug("record_outcome: analyst revision stored for %s", ticker)
        except Exception as exc:
            logger.error("record_outcome failed for %s: %s", ticker, exc)
