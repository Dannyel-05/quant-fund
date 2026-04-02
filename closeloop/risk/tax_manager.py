"""
UK Capital Gains Tax manager.

Rules implemented:
  - Section 104 pool: average cost basis across acquisitions
  - Same-day rule: match disposal with same-day acquisitions
  - 30-day bed & breakfast rule: match disposal with acquisitions in next 30 days
  - Annual CGT allowance: £3,000 (2024/25)
  - Basic rate: 10%, Higher rate: 20% (investments)
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_CGT_ALLOWANCE_GBP = 3_000.0
_BASIC_RATE = 0.10
_HIGHER_RATE = 0.20


class TaxManager:
    """
    Maintains Section 104 pools per ticker and computes UK CGT liability.

    All values in GBP.
    """

    def __init__(self, store=None, config=None, tax_year_start: str = "04-06"):
        self.store = store
        self.config = config or {}
        self.tax_year_start = tax_year_start
        # Section 104 pools: ticker -> {"shares": float, "cost": float}
        self._pools: Dict[str, Dict] = defaultdict(lambda: {"shares": 0.0, "cost": 0.0})
        # Pending acquisitions for B&B rule: ticker -> [(date, shares, cost)]
        self._pending: Dict[str, List] = defaultdict(list)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def record_acquisition(
        self,
        ticker: str,
        shares: float,
        cost_gbp: float,
        date: datetime,
        market: str = "uk",
    ) -> None:
        """Add shares to Section 104 pool."""
        if market != "uk":
            return
        pool = self._pools[ticker]
        pool["shares"] += shares
        pool["cost"] += cost_gbp
        self._pending[ticker].append((date, shares, cost_gbp))
        if self.store:
            try:
                self.store.record_tax_disposal(
                    ticker=ticker,
                    disposal_date=date.isoformat(),
                    disposal_proceeds=0.0,
                    allowable_cost=cost_gbp,
                    gain=0.0,
                    disposal_type="acquisition",
                    pool_shares_after=pool["shares"],
                    pool_cost_after=pool["cost"],
                )
            except Exception as exc:
                logger.warning("TaxManager.record_acquisition store error: %s", exc)

    def record_disposal(
        self,
        ticker,
        shares=None,
        proceeds_gbp: float = 0.0,
        date: datetime = None,
        market: str = "uk",
    ) -> Dict:
        """
        Process a disposal under UK CGT rules.

        Accepts two calling conventions:
          1. record_disposal(ticker, shares, proceeds_gbp, date, market)
          2. record_disposal(trade_id, closed_trade_dict)  — autopsy pipeline shortcut

        Returns:
            gain (float): chargeable gain (negative = loss)
            disposal_type (str): same_day | bed_and_breakfast | section_104
            allowable_cost (float)
        """
        # Autopsy pipeline passes (trade_id, closed_trade_dict)
        if isinstance(ticker, (int, str)) and isinstance(shares, dict):
            closed_trade = shares
            ticker = str(closed_trade.get("ticker", ""))
            shares = float(closed_trade.get("position_size", 0) or 0)
            proceeds_gbp = float(closed_trade.get("net_pnl", 0) or 0)
            exit_date = closed_trade.get("exit_date")
            date = datetime.fromisoformat(exit_date) if exit_date else datetime.utcnow()
            market = closed_trade.get("market", "us")

        if market != "uk":
            return {"gain": 0.0, "disposal_type": "non_uk", "allowable_cost": 0.0}

        # 1. Same-day rule
        same_day = self._match_same_day(ticker, shares, date)
        if same_day:
            cost = same_day["cost"]
            gain = proceeds_gbp - cost
            self._update_store(ticker, date, proceeds_gbp, cost, gain, "same_day")
            return {"gain": gain, "disposal_type": "same_day", "allowable_cost": cost}

        # 2. 30-day B&B rule (forward matching — matched after the fact)
        bb = self._match_bed_and_breakfast(ticker, shares, date)
        if bb:
            cost = bb["cost"]
            gain = proceeds_gbp - cost
            self._update_store(ticker, date, proceeds_gbp, cost, gain, "bed_and_breakfast")
            return {"gain": gain, "disposal_type": "bed_and_breakfast", "allowable_cost": cost}

        # 3. Section 104 pool
        pool = self._pools[ticker]
        if pool["shares"] <= 0:
            logger.warning("TaxManager: no pool for %s; disposal cost basis = 0", ticker)
            gain = proceeds_gbp
            self._update_store(ticker, date, proceeds_gbp, 0.0, gain, "section_104_empty")
            return {"gain": gain, "disposal_type": "section_104_empty", "allowable_cost": 0.0}

        fraction = min(shares / pool["shares"], 1.0)
        cost = pool["cost"] * fraction
        pool["shares"] -= shares
        pool["cost"] -= cost
        if pool["shares"] < 0:
            pool["shares"] = 0.0
            pool["cost"] = 0.0

        gain = proceeds_gbp - cost
        self._update_store(ticker, date, proceeds_gbp, cost, gain, "section_104")
        return {"gain": gain, "disposal_type": "section_104", "allowable_cost": cost}

    def compute_annual_liability(self, tax_year: Optional[str] = None) -> Dict:
        """
        Compute CGT liability for the current or specified tax year.

        Uses store.get_ytd_gains() if store is available.
        Returns:
            total_gains, net_gain, allowance_used, taxable_gain,
            estimated_tax_basic, estimated_tax_higher
        """
        try:
            if self.store:
                ytd = self.store.get_ytd_gains()
                total_gains = ytd.get("total_gains", 0.0)
                total_losses = ytd.get("total_losses", 0.0)
            else:
                total_gains = 0.0
                total_losses = 0.0

            net_gain = total_gains - total_losses
            taxable = max(0.0, net_gain - _CGT_ALLOWANCE_GBP)
            return {
                "total_gains": total_gains,
                "total_losses": total_losses,
                "net_gain": net_gain,
                "annual_allowance": _CGT_ALLOWANCE_GBP,
                "taxable_gain": taxable,
                "estimated_tax_basic_rate": round(taxable * _BASIC_RATE, 2),
                "estimated_tax_higher_rate": round(taxable * _HIGHER_RATE, 2),
            }
        except Exception as exc:
            logger.warning("TaxManager.compute_annual_liability: %s", exc)
            return {"taxable_gain": 0.0, "estimated_tax_higher_rate": 0.0}

    def pool_summary(self) -> Dict:
        """Return current pool state for all tickers."""
        return {
            t: {"shares": p["shares"], "cost": p["cost"],
                "avg_cost": p["cost"] / p["shares"] if p["shares"] > 0 else 0.0}
            for t, p in self._pools.items()
        }

    # ------------------------------------------------------------------
    # Internal matching
    # ------------------------------------------------------------------

    def _match_same_day(self, ticker: str, shares: float, date: datetime) -> Optional[Dict]:
        pending = self._pending.get(ticker, [])
        same = [(d, s, c) for d, s, c in pending if d.date() == date.date()]
        if not same:
            return None
        total_shares = sum(s for _, s, _ in same)
        total_cost = sum(c for _, _, c in same)
        if total_shares <= 0:
            return None
        fraction = min(shares / total_shares, 1.0)
        return {"cost": total_cost * fraction}

    def _match_bed_and_breakfast(
        self, ticker: str, shares: float, date: datetime
    ) -> Optional[Dict]:
        """Match disposal against acquisitions within 30 days after disposal date."""
        cutoff = date + timedelta(days=30)
        pending = self._pending.get(ticker, [])
        future = [(d, s, c) for d, s, c in pending
                  if date < d <= cutoff]
        if not future:
            return None
        total_shares = sum(s for _, s, _ in future)
        total_cost = sum(c for _, _, c in future)
        if total_shares <= 0:
            return None
        fraction = min(shares / total_shares, 1.0)
        return {"cost": total_cost * fraction}

    def _update_store(
        self,
        ticker: str,
        date: datetime,
        proceeds: float,
        cost: float,
        gain: float,
        disposal_type: str,
    ) -> None:
        if not self.store:
            return
        pool = self._pools[ticker]
        try:
            self.store.record_tax_disposal(
                ticker=ticker,
                disposal_date=date.isoformat(),
                disposal_proceeds=proceeds,
                allowable_cost=cost,
                gain=gain,
                disposal_type=disposal_type,
                pool_shares_after=pool["shares"],
                pool_cost_after=pool["cost"],
            )
        except Exception as exc:
            logger.warning("TaxManager store error: %s", exc)
