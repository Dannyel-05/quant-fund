"""
MemberTracker — tracks every congressman's trading history and performance.
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")
HISTORY_FILE = CACHE_DIR / "congress_history.json"
COMMITTEES_FILE = CACHE_DIR / "congress_committees.json"

FORWARD_RETURN_DAYS = 90  # Standard look-forward window

# Hardcoded committee assignments as fallback
HARDCODED_COMMITTEES = {
    "Financial Services": {
        "relevance": {
            "Finance": 1.0, "Banking": 1.0, "Insurance": 1.0,
            "Real Estate": 0.8, "Technology": 0.5,
        }
    },
    "Armed Services": {
        "relevance": {
            "Defense": 1.0, "Aerospace": 1.0, "Technology": 0.7,
            "Cybersecurity": 0.8,
        }
    },
    "Energy and Commerce": {
        "relevance": {
            "Energy": 1.0, "Utilities": 1.0, "Healthcare": 0.8,
            "Telecommunications": 0.8, "Consumer": 0.6,
        }
    },
    "Agriculture": {
        "relevance": {
            "Agriculture": 1.0, "Food": 0.9, "Commodities": 0.8,
            "Chemicals": 0.5,
        }
    },
    "Science and Technology": {
        "relevance": {
            "Technology": 1.0, "Biotechnology": 0.9, "Space": 1.0,
            "Research": 0.8,
        }
    },
}


class MemberTracker:
    """
    Tracks and scores each congressional member's trading history.
    Persists data to data/cache/deepdata/congress_history.json.
    """

    def __init__(self, config: dict):
        self.config = config
        cd_config = config.get("deepdata", {}).get("congressional", {})
        self.forward_days = cd_config.get("forward_return_days", FORWARD_RETURN_DAYS)
        self.min_trades = cd_config.get("min_track_record_trades", 5)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._history: dict = self._load_history()
        self._committees: dict = self.get_committee_assignments()

    def _load_history(self) -> dict:
        """Load trade history from cache file."""
        if HISTORY_FILE.exists():
            try:
                with open(HISTORY_FILE, "r") as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning("Failed to load congress history: %s", exc)
        return {}

    def _save_history(self) -> None:
        """Persist trade history to cache file."""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(HISTORY_FILE, "w") as f:
                json.dump(self._history, f, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to save congress history: %s", exc)

    def update(self, disclosures: list, price_data: dict) -> None:
        """
        Process new disclosures. Calculate 90-day forward returns for past trades.
        Store to data/cache/deepdata/congress_history.json.
        """
        for disclosure in disclosures:
            member = disclosure.get("member", "")
            if not member:
                continue

            if member not in self._history:
                self._history[member] = {
                    "member": member,
                    "chamber": disclosure.get("chamber", ""),
                    "trades": [],
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                }

            # Check if this trade already recorded (dedup by date+ticker+type)
            trade_key = (
                f"{disclosure.get('transaction_date', '')}|"
                f"{disclosure.get('ticker', '')}|"
                f"{disclosure.get('transaction_type', '')}"
            )
            existing_keys = {
                f"{t.get('transaction_date', '')}|{t.get('ticker', '')}|{t.get('transaction_type', '')}"
                for t in self._history[member].get("trades", [])
            }
            if trade_key in existing_keys:
                continue

            trade = dict(disclosure)
            # Add direction
            tx_type = disclosure.get("transaction_type", "").lower()
            if any(b in tx_type for b in ("purchase", "buy", "exercise", "received")):
                trade["direction"] = 1
            elif any(s in tx_type for s in ("sale", "sell")):
                trade["direction"] = -1
            else:
                trade["direction"] = 0

            # Calculate forward return if price data available
            fwd_return = self._calc_forward_return(
                disclosure.get("ticker", ""),
                disclosure.get("transaction_date", ""),
                price_data,
            )
            trade["forward_return_90d"] = fwd_return
            trade["recorded_at"] = datetime.now(timezone.utc).isoformat()

            self._history[member]["trades"].append(trade)
            self._history[member]["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Backfill forward returns for trades that didn't have them
        self._backfill_returns(price_data)
        self._save_history()

    def _backfill_returns(self, price_data: dict) -> None:
        """Fill in forward returns for trades missing them."""
        for member, record in self._history.items():
            for trade in record.get("trades", []):
                if trade.get("forward_return_90d") is None:
                    ticker = trade.get("ticker", "")
                    date = trade.get("transaction_date", "")
                    fwd = self._calc_forward_return(ticker, date, price_data)
                    trade["forward_return_90d"] = fwd

    def _calc_forward_return(
        self, ticker: str, date_str: str, price_data: dict
    ) -> Optional[float]:
        """Calculate 90-day forward return from transaction date."""
        try:
            prices = price_data.get(ticker, {})
            if not prices:
                return None
            sorted_dates = sorted(prices.keys())
            # Find start date
            start_idx = next(
                (i for i, d in enumerate(sorted_dates) if d >= date_str[:10]), None
            )
            if start_idx is None:
                return None
            end_idx = min(start_idx + self.forward_days, len(sorted_dates) - 1)
            start_price = prices[sorted_dates[start_idx]]
            end_price = prices[sorted_dates[end_idx]]
            if not start_price or start_price == 0:
                return None
            return round((end_price - start_price) / start_price, 6)
        except Exception as exc:
            logger.warning("Forward return calc error %s %s: %s", ticker, date_str, exc)
            return None

    def get_member_record(self, member_name: str) -> dict:
        """
        Return member trading record with statistics.
        {member, trades, accuracy, excess_return, information_ratio,
         committee_memberships, total_trades, profitable_trades, top_sectors}
        """
        record = self._history.get(member_name, {})
        trades = record.get("trades", [])

        accuracy = 0.0
        excess_return = 0.0
        info_ratio = 0.0
        profitable_trades = 0
        top_sectors: list = []

        if trades:
            # Only use trades with known direction and return
            eval_trades = [
                t for t in trades
                if t.get("direction", 0) != 0 and t.get("forward_return_90d") is not None
            ]
            if eval_trades:
                accurate = [
                    t for t in eval_trades
                    if (t["direction"] > 0 and t["forward_return_90d"] > 0)
                    or (t["direction"] < 0 and t["forward_return_90d"] < 0)
                ]
                profitable_trades = len(accurate)
                accuracy = len(accurate) / len(eval_trades)

            sector_counts: dict = defaultdict(int)
            for t in trades:
                sector = t.get("sector", "")
                if sector:
                    sector_counts[sector] += 1
            top_sectors = sorted(sector_counts, key=sector_counts.get, reverse=True)[:5]  # type: ignore[arg-type]

        committees = self._get_member_committees(member_name)

        return {
            "member": member_name,
            "trades": trades,
            "accuracy": round(accuracy, 4),
            "excess_return": round(excess_return, 4),
            "information_ratio": round(info_ratio, 4),
            "committee_memberships": committees,
            "total_trades": len(trades),
            "profitable_trades": profitable_trades,
            "top_sectors": top_sectors,
        }

    def _get_member_committees(self, member_name: str) -> list:
        """Look up committee memberships for a member."""
        committees = []
        for committee, data in self._committees.items():
            members = data.get("members", [])
            if member_name in members or any(
                member_name.lower() in m.lower() for m in members
            ):
                committees.append(committee)
        return committees

    def calc_accuracy(self, trades: list, price_data: dict) -> float:
        """profitable_trades / total_trades using 90-day forward return."""
        eval_trades = []
        for t in trades:
            direction = t.get("direction", 0)
            if direction == 0:
                continue
            fwd = t.get("forward_return_90d") or self._calc_forward_return(
                t.get("ticker", ""), t.get("transaction_date", ""), price_data
            )
            if fwd is None:
                continue
            eval_trades.append((direction, fwd))

        if not eval_trades:
            return 0.0

        profitable = sum(
            1 for direction, fwd in eval_trades
            if (direction > 0 and fwd > 0) or (direction < 0 and fwd < 0)
        )
        return round(profitable / len(eval_trades), 4)

    def calc_excess_return(
        self, trades: list, price_data: dict, benchmark: dict
    ) -> float:
        """mean(trade_90d_return) - mean(market_90d_return_same_period)"""
        trade_returns = []
        market_returns = []

        for t in trades:
            direction = t.get("direction", 0)
            if direction == 0:
                continue
            fwd = t.get("forward_return_90d") or self._calc_forward_return(
                t.get("ticker", ""), t.get("transaction_date", ""), price_data
            )
            if fwd is None:
                continue
            # Signed return: positive if correct direction
            signed_return = fwd * direction
            trade_returns.append(signed_return)

            # Benchmark return for same period
            date_str = t.get("transaction_date", "")
            bench_return = self._calc_forward_return(
                "SPY", date_str, benchmark
            )
            if bench_return is not None:
                market_returns.append(bench_return)

        if not trade_returns:
            return 0.0

        mean_trade = sum(trade_returns) / len(trade_returns)
        mean_market = sum(market_returns) / len(market_returns) if market_returns else 0.0
        return round(mean_trade - mean_market, 4)

    def get_committee_power_score(
        self, member_name: str, ticker: str, sector: str
    ) -> float:
        """
        CommitteePowerScore = base * committee_relevance * seniority_multiplier
        Relevance: 1.0 direct oversight, 0.5 indirect, 0.1 none.
        Seniority: Chair=2.0, Ranking Member=1.5, Member=1.0.
        """
        base = 1.0
        best_score = 0.1  # Default: no oversight

        for committee, data in self._committees.items():
            members_data = data.get("members_with_roles", {})
            role = members_data.get(member_name, "")

            if not role:
                # Check plain members list
                plain_members = data.get("members", [])
                if member_name not in plain_members and not any(
                    member_name.lower() in m.lower() for m in plain_members
                ):
                    continue
                role = "Member"

            # Seniority multiplier
            role_lower = role.lower()
            if "chair" in role_lower and "rank" not in role_lower:
                seniority = 2.0
            elif "ranking" in role_lower or "vice" in role_lower:
                seniority = 1.5
            else:
                seniority = 1.0

            # Committee relevance to this sector/ticker
            relevance_map = HARDCODED_COMMITTEES.get(committee, {}).get("relevance", {})
            sector_relevance = relevance_map.get(sector, 0.1)

            committee_score = base * sector_relevance * seniority
            if committee_score > best_score:
                best_score = committee_score

        return round(best_score, 4)

    def identify_contra_indicators(self) -> list:
        """
        Members with accuracy < 40% consistently = trade AGAINST them.
        Returns list of {member, accuracy, total_trades, recommendation}.
        """
        contra = []
        for member_name, record in self._history.items():
            trades = record.get("trades", [])
            eval_trades = [
                t for t in trades
                if t.get("direction", 0) != 0 and t.get("forward_return_90d") is not None
            ]
            if len(eval_trades) < self.config.get("deepdata", {}).get(
                "congressional", {}
            ).get("min_track_record_trades", 5):
                continue

            accurate = sum(
                1 for t in eval_trades
                if (t["direction"] > 0 and t["forward_return_90d"] > 0)
                or (t["direction"] < 0 and t["forward_return_90d"] < 0)
            )
            accuracy = accurate / len(eval_trades)

            if accuracy < 0.40:
                contra.append({
                    "member": member_name,
                    "accuracy": round(accuracy, 4),
                    "total_trades": len(eval_trades),
                    "recommendation": "TRADE_AGAINST",
                })

        return sorted(contra, key=lambda x: x["accuracy"])

    def get_committee_assignments(self) -> dict:
        """
        Load committee assignments from cache.
        Falls back to hardcoded mapping if unavailable.
        """
        if COMMITTEES_FILE.exists():
            try:
                with open(COMMITTEES_FILE, "r") as f:
                    data = json.load(f)
                if data:
                    return data
            except Exception as exc:
                logger.warning("Failed to load committees file: %s", exc)

        # Return hardcoded fallback structure
        return {
            committee: {
                "members": [],
                "members_with_roles": {},
                **data,
            }
            for committee, data in HARDCODED_COMMITTEES.items()
        }
