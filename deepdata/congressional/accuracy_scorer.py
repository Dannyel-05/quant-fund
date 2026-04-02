"""
AccuracyScorer — weights congressional signals by member track record.
"""
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

CREDIBILITY_THRESHOLDS = {
    "HIGH": {"min_accuracy": 0.65, "min_excess_return": 5.0, "min_trades": 10},
    "MEDIUM_ACCURACY": {"min_accuracy": 0.55},
    "MEDIUM_RETURN": {"min_excess_return": 2.0},
    "CONTRA": {"max_accuracy": 0.40},
}


class AccuracyScorer:
    """
    Scores congressional members by their historical trading performance.
    Assigns credibility tiers used to weight trading signals.
    """

    def __init__(self, config: dict):
        self.config = config
        cd_config = config.get("deepdata", {}).get("congressional", {})
        self.min_track_record_trades = cd_config.get("min_track_record_trades", 5)
        self.high_accuracy_threshold = cd_config.get("high_accuracy_threshold", 0.65)
        self.high_excess_return_threshold = cd_config.get("high_excess_return_threshold", 5.0)
        self.medium_accuracy_threshold = cd_config.get("medium_accuracy_threshold", 0.55)
        self.medium_excess_return_threshold = cd_config.get("medium_excess_return_threshold", 2.0)
        self.contra_accuracy_threshold = cd_config.get("contra_accuracy_threshold", 0.40)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def score_member(self, member_record: dict) -> dict:
        """
        Score a congressional member by credibility.

        Returns:
        {
          member, credibility: 'HIGH'|'MEDIUM'|'LOW'|'CONTRA',
          credibility_score: float,  # 0 to 1 (or -0.5 for contra)
          accuracy, excess_return, information_ratio, n_trades,
          recommendation: str
        }
        """
        member = member_record.get("member", "Unknown")
        accuracy = member_record.get("accuracy", 0.0)
        excess_return = member_record.get("excess_return", 0.0)
        info_ratio = member_record.get("information_ratio", 0.0)
        n_trades = member_record.get("total_trades", 0)

        credibility = self.classify_credibility(accuracy, excess_return, n_trades)
        credibility_score = self._calc_credibility_score(
            credibility, accuracy, excess_return, info_ratio, n_trades
        )
        recommendation = self._make_recommendation(credibility, credibility_score)

        return {
            "member": member,
            "credibility": credibility,
            "credibility_score": round(credibility_score, 4),
            "accuracy": round(accuracy, 4),
            "excess_return": round(excess_return, 4),
            "information_ratio": round(info_ratio, 4),
            "n_trades": n_trades,
            "recommendation": recommendation,
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

    def classify_credibility(
        self, accuracy: float, excess_return: float, n_trades: int
    ) -> str:
        """
        HIGH: accuracy > 65% AND excess_return > 5% AND n_trades >= 10
        MEDIUM: accuracy > 55% OR excess_return > 2%
        LOW: accuracy < 50%
        CONTRA: accuracy < 40% consistently (negative score)
        """
        if n_trades < self.min_track_record_trades:
            return "LOW"

        if (
            accuracy > self.high_accuracy_threshold
            and excess_return > self.high_excess_return_threshold
            and n_trades >= 10
        ):
            return "HIGH"

        if (
            accuracy > self.medium_accuracy_threshold
            or excess_return > self.medium_excess_return_threshold
        ):
            return "MEDIUM"

        if accuracy < self.contra_accuracy_threshold and n_trades >= 10:
            return "CONTRA"

        return "LOW"

    def _calc_credibility_score(
        self,
        credibility: str,
        accuracy: float,
        excess_return: float,
        info_ratio: float,
        n_trades: int,
    ) -> float:
        """
        Compute a continuous credibility score.
        HIGH -> 0.7 to 1.0
        MEDIUM -> 0.4 to 0.7
        LOW -> 0.1 to 0.4
        CONTRA -> -0.5 to 0.0
        """
        if credibility == "CONTRA":
            # More negative = lower accuracy
            base = -0.5 + max(0.0, accuracy - 0.20) * 0.5 / 0.20
            return round(max(-0.5, min(0.0, base)), 4)

        if credibility == "LOW":
            base = 0.1 + min(accuracy, 0.5) * 0.6
            return round(max(0.0, min(0.4, base)), 4)

        if credibility == "MEDIUM":
            # Scale 0.4 - 0.7 based on accuracy and excess return
            acc_score = min(1.0, accuracy / self.high_accuracy_threshold)
            ret_score = min(1.0, excess_return / self.high_excess_return_threshold)
            base = 0.4 + (acc_score * 0.15 + ret_score * 0.15)
            return round(min(0.7, base), 4)

        if credibility == "HIGH":
            # Scale 0.7 - 1.0
            acc_score = min(1.0, (accuracy - self.high_accuracy_threshold) / 0.2)
            ret_score = min(1.0, (excess_return - self.high_excess_return_threshold) / 10.0)
            ir_score = min(1.0, max(0.0, info_ratio) / 2.0)
            trade_score = min(1.0, (n_trades - 10) / 40.0)
            base = 0.7 + (acc_score * 0.1 + ret_score * 0.1 + ir_score * 0.05 + trade_score * 0.05)
            return round(min(1.0, base), 4)

        return 0.0

    def _make_recommendation(self, credibility: str, score: float) -> str:
        """Generate human-readable recommendation."""
        if credibility == "HIGH":
            return "FOLLOW: Strong track record of informed trading"
        elif credibility == "MEDIUM":
            return "MODERATE_FOLLOW: Some evidence of alpha generation"
        elif credibility == "CONTRA":
            return "TRADE_AGAINST: Consistently poor performer; fade their trades"
        else:
            return "IGNORE: Insufficient or poor track record"

    def calc_information_ratio(
        self, trade_returns: list, market_returns: list
    ) -> float:
        """
        IR = mean(trade - market) / std(trade - market)
        Returns 0.0 if insufficient data.
        """
        if not trade_returns or not market_returns:
            return 0.0

        n = min(len(trade_returns), len(market_returns))
        if n < 2:
            return 0.0

        excess = [t - m for t, m in zip(trade_returns[:n], market_returns[:n])]
        mean_excess = sum(excess) / n
        variance = sum((x - mean_excess) ** 2 for x in excess) / (n - 1)
        std_excess = math.sqrt(variance) if variance > 0 else 0.0

        if std_excess == 0.0:
            return 0.0

        ir = mean_excess / std_excess
        return round(ir, 4)

    def get_all_scored_members(self, tracker) -> list:
        """
        Score all members with >= min_track_record_trades trades.
        tracker: MemberTracker instance
        """
        scored = []
        history = tracker._history if hasattr(tracker, "_history") else {}

        for member_name in history:
            try:
                record = tracker.get_member_record(member_name)
                if record.get("total_trades", 0) < self.min_track_record_trades:
                    continue
                scored_record = self.score_member(record)
                scored.append(scored_record)
            except Exception as exc:
                logger.warning("Error scoring member %s: %s", member_name, exc)

        return sorted(scored, key=lambda x: x.get("credibility_score", 0.0), reverse=True)

    def leaderboard(self, n: int = 10) -> list:
        """
        Top N members by credibility_score.
        Note: requires get_all_scored_members to have been called and results passed.
        Returns empty list if called standalone without data.
        """
        logger.warning(
            "leaderboard() called without scored members; use get_all_scored_members() first"
        )
        return []

    def leaderboard_from_scored(self, scored_members: list, n: int = 10) -> list:
        """Return top N members from a pre-scored list."""
        sorted_members = sorted(
            scored_members,
            key=lambda x: x.get("credibility_score", 0.0),
            reverse=True,
        )
        return sorted_members[:n]
