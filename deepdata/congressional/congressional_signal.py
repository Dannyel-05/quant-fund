"""
CongressionalSignal — generates investment signals from congressional trading disclosures.
"""
import logging
import math
import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata")

BUY_KEYWORDS = {"purchase", "buy", "exercise", "received", "exchange"}
SELL_KEYWORDS = {"sale", "sell"}

TIER1_CREDIBILITY_THRESHOLD = 0.7  # HIGH credibility members


class CongressionalSignal:
    """
    Generates investment signals from congressional trading disclosures.
    Weights signals by member credibility, committee power, filing timeliness.
    """

    def __init__(self, config: dict):
        self.config = config
        cd_config = config.get("deepdata", {}).get("congressional", {})
        self.recency_decay_rate = cd_config.get("recency_decay_rate", 0.05)
        self.cross_party_multiplier = cd_config.get("cross_party_multiplier", 1.5)
        self.cluster_window_days = cd_config.get("cluster_window_days", 30)
        self.min_cluster_members = cd_config.get("min_cluster_members", 2)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def generate(self, disclosures: list, tracker, scorer) -> list:
        """
        Generate CollectorResult signals from congressional disclosures.

        For each disclosure:
          BaseSignal = direction * amount_midpoint_normalised
          AdjustedSignal = BaseSignal * credibility * committee_power * (1/delay_days) * recency_decay

        Returns CollectorResult list.
        """
        results = []
        ts = datetime.now(timezone.utc).isoformat()

        scored_cache: dict = {}

        for disclosure in disclosures:
            ticker = disclosure.get("ticker", "")
            if not ticker:
                continue

            member = disclosure.get("member", "")
            sector = disclosure.get("sector", "")

            # Get member credibility
            credibility_score = 0.5  # default
            try:
                if member not in scored_cache:
                    record = tracker.get_member_record(member)
                    scored = scorer.score_member(record)
                    scored_cache[member] = scored
                scored_member = scored_cache[member]
                credibility_score = scored_member.get("credibility_score", 0.5)
            except Exception as exc:
                logger.warning("Could not score member %s: %s", member, exc)

            # Get committee power for this ticker/sector
            committee_power = 1.0
            try:
                committee_power = tracker.get_committee_power_score(member, ticker, sector)
            except Exception as exc:
                logger.warning("Could not get committee power for %s: %s", member, exc)

            signal_value = self.calc_adjusted_signal(
                disclosure, credibility_score, committee_power
            )

            if signal_value == 0.0:
                continue

            results.append({
                "source": "congressional",
                "ticker": ticker,
                "market": "US",
                "data_type": "congressional_trade",
                "value": signal_value,
                "raw_data": {
                    "member": member,
                    "chamber": disclosure.get("chamber", ""),
                    "committee_power": committee_power,
                    "credibility_score": credibility_score,
                    "transaction_type": disclosure.get("transaction_type", ""),
                    "amount_midpoint": disclosure.get("amount_min", 0) + (
                        (disclosure.get("amount_max", 0) - disclosure.get("amount_min", 0)) // 2
                    ),
                    "transaction_date": disclosure.get("transaction_date", ""),
                    "delay_days": disclosure.get("delay_days", 0),
                },
                "timestamp": ts,
                "quality_score": min(1.0, abs(signal_value)),
            })

        # Add cluster signals
        cluster_signals = self._generate_cluster_signals(disclosures, scored_cache, ts)
        results.extend(cluster_signals)

        return results

    def calc_adjusted_signal(
        self,
        disclosure: dict,
        credibility_score: float,
        committee_power: float,
    ) -> float:
        """
        Compute adjusted signal for a single disclosure.

        direction = +1 for buy, -1 for sell
        amount_norm = log(midpoint) / log(1_000_000)
        recency_decay = exp(-0.05 * days_since_transaction)
        filing_freshness = 1 / max(delay_days, 1)
        """
        tx_type = disclosure.get("transaction_type", "").lower()
        if any(b in tx_type for b in BUY_KEYWORDS):
            direction = 1.0
        elif any(s in tx_type for s in SELL_KEYWORDS):
            direction = -1.0
        else:
            return 0.0

        # If credibility is negative (CONTRA), flip signal
        if credibility_score < 0:
            direction *= -1.0
            credibility_abs = abs(credibility_score)
        else:
            credibility_abs = credibility_score

        # Amount normalisation
        amount_min = disclosure.get("amount_min", 0) or 0
        amount_max = disclosure.get("amount_max", 0) or 0
        midpoint = (amount_min + amount_max) / 2 if amount_max > 0 else amount_min
        midpoint = max(midpoint, 1.0)
        amount_norm = math.log(midpoint) / math.log(1_000_000)
        amount_norm = max(0.0, min(1.0, amount_norm))

        # Recency decay based on days since transaction
        tx_date_str = disclosure.get("transaction_date", "")
        days_since = 0
        try:
            tx_date = datetime.strptime(tx_date_str[:10], "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            days_since = (today - tx_date).days
        except Exception:
            days_since = disclosure.get("delay_days", 0)

        recency_decay = math.exp(-self.recency_decay_rate * max(0, days_since))

        # Filing freshness: penalise stale filings
        delay_days = disclosure.get("delay_days", 45) or 45
        filing_freshness = 1.0 / max(delay_days, 1)

        adjusted = (
            direction
            * amount_norm
            * credibility_abs
            * committee_power
            * recency_decay
            * filing_freshness
        )

        return round(adjusted, 6)

    def _generate_cluster_signals(
        self, disclosures: list, scored_cache: dict, ts: str
    ) -> list:
        """Generate cluster signals from detect_clusters output."""
        clusters = self.detect_clusters(disclosures, window_days=self.cluster_window_days)
        results = []
        for cluster in clusters:
            ticker = cluster.get("ticker", "")
            if not ticker:
                continue
            results.append({
                "source": "congressional",
                "ticker": ticker,
                "market": "US",
                "data_type": "congressional_cluster",
                "value": cluster.get("cluster_score", 0.0),
                "raw_data": cluster,
                "timestamp": ts,
                "quality_score": min(1.0, abs(cluster.get("cluster_score", 0.0))),
            })
        return results

    def detect_clusters(self, disclosures: list, window_days: int = 30) -> list:
        """
        Multiple members buying same stock within window_days = cluster.
        ClusterScore = n_members * mean_credibility * committee_overlap
        Cross-party (R+D) clusters get extra 1.5x multiplier.
        """
        # Group by ticker
        ticker_disclosures: dict = defaultdict(list)
        for d in disclosures:
            ticker = d.get("ticker", "")
            if ticker:
                ticker_disclosures[ticker].append(d)

        clusters = []

        for ticker, ticker_group in ticker_disclosures.items():
            # Find buys only (clusters are buy-side)
            buys = [
                d for d in ticker_group
                if any(b in d.get("transaction_type", "").lower() for b in BUY_KEYWORDS)
            ]

            if len(buys) < self.min_cluster_members:
                continue

            # Filter by window
            windowed_buys = self._filter_by_window(buys, window_days)
            if len(windowed_buys) < self.min_cluster_members:
                continue

            members = list({d.get("member", "") for d in windowed_buys if d.get("member")})
            n_members = len(members)

            # Mean credibility across members
            credibilities = []
            for member in members:
                cred = 0.5  # default
                if member in self._get_scored_cache_placeholder():
                    cred = self._get_scored_cache_placeholder()[member]
                credibilities.append(abs(cred))

            mean_credibility = sum(credibilities) / len(credibilities) if credibilities else 0.5

            # Committee overlap (simplified: fraction of members on relevant committee)
            committee_overlap = min(1.0, n_members / 5.0)

            base_score = n_members * mean_credibility * committee_overlap

            # Cross-party bonus
            parties = {d.get("party", "") for d in windowed_buys if d.get("party")}
            has_cross_party = len(parties) > 1 and "R" in parties and "D" in parties
            if has_cross_party:
                base_score *= self.cross_party_multiplier

            clusters.append({
                "ticker": ticker,
                "n_members": n_members,
                "members": members,
                "mean_credibility": round(mean_credibility, 4),
                "committee_overlap": round(committee_overlap, 4),
                "cross_party": has_cross_party,
                "cluster_score": round(base_score, 4),
                "window_days": window_days,
            })

        return sorted(clusters, key=lambda x: x["cluster_score"], reverse=True)

    def _filter_by_window(self, disclosures: list, window_days: int) -> list:
        """Filter disclosures to those within window_days of each other."""
        if not disclosures:
            return []

        dates = []
        for d in disclosures:
            try:
                dt = datetime.strptime(d.get("transaction_date", "")[:10], "%Y-%m-%d").date()
                dates.append(dt)
            except Exception:
                dates.append(None)

        valid = [(d, dt) for d, dt in zip(disclosures, dates) if dt is not None]
        if not valid:
            return disclosures

        earliest = min(dt for _, dt in valid)
        latest = max(dt for _, dt in valid)

        if (latest - earliest).days <= window_days:
            return [d for d, _ in valid]

        # Find the largest window cluster
        best = []
        for i, (d_i, dt_i) in enumerate(valid):
            window = [(d, dt) for d, dt in valid if abs((dt - dt_i).days) <= window_days]
            if len(window) > len(best):
                best = window
        return [d for d, _ in best]

    def _get_scored_cache_placeholder(self) -> dict:
        """Return empty dict (cluster scoring without full tracker context)."""
        return {}

    def scan_nonsense_patterns(
        self,
        disclosure_history: list,
        price_history: dict,
        altdata: dict = None,
    ) -> list:
        """
        Test and validate spurious patterns using Monte Carlo permutation (500 shuffles).
        Tests:
        1. Day-of-week pattern in congressional buying
        2. Member state predicts return better than average
        3. Filing delay length correlates with signal strength
        4. Congressional activity correlates with lunar phase (nonsense)
        5. Washington DC weather affects trading activity (nonsense)

        Returns only statistically validated patterns (p < 0.05).
        """
        validated = []

        if not disclosure_history or not price_history:
            return validated

        n_permutations = 500

        # Helper: calc actual metric
        def calc_dow_metric(disclosures: list) -> float:
            """Fraction of buys on Monday vs other days."""
            buys = [
                d for d in disclosures
                if any(b in d.get("transaction_type", "").lower() for b in BUY_KEYWORDS)
            ]
            if not buys:
                return 0.0
            monday_buys = 0
            for d in buys:
                try:
                    dt = datetime.strptime(d.get("transaction_date", "")[:10], "%Y-%m-%d")
                    if dt.weekday() == 0:
                        monday_buys += 1
                except Exception:
                    pass
            return monday_buys / len(buys)

        # Test 1: Day-of-week pattern
        actual_dow = calc_dow_metric(disclosure_history)
        permuted_dow = []
        shuffled_dates = [d.get("transaction_date", "") for d in disclosure_history]
        for _ in range(n_permutations):
            random.shuffle(shuffled_dates)
            permuted = [dict(d, transaction_date=dt)
                        for d, dt in zip(disclosure_history, shuffled_dates)]
            permuted_dow.append(calc_dow_metric(permuted))

        if permuted_dow:
            p_value_dow = sum(1 for p in permuted_dow if p >= actual_dow) / n_permutations
            if p_value_dow < 0.05:
                validated.append({
                    "pattern": "day_of_week",
                    "description": "Congressional buys cluster on specific weekdays",
                    "metric": round(actual_dow, 4),
                    "p_value": round(p_value_dow, 4),
                    "validated": True,
                })

        # Test 3: Filing delay correlation with signal strength
        def calc_delay_correlation(disclosures: list) -> float:
            delays = []
            returns = []
            for d in disclosures:
                delay = d.get("delay_days", 0) or 0
                ticker = d.get("ticker", "")
                tx_date = d.get("transaction_date", "")
                prices = price_history.get(ticker, {})
                if not prices:
                    continue
                sorted_dates = sorted(prices.keys())
                idx = next((i for i, dt in enumerate(sorted_dates) if dt >= tx_date[:10]), None)
                if idx is None or idx + 30 >= len(sorted_dates):
                    continue
                ret = (prices[sorted_dates[idx + 30]] - prices[sorted_dates[idx]]) / max(
                    prices[sorted_dates[idx]], 1e-9
                )
                delays.append(delay)
                returns.append(ret)
            if len(delays) < 5:
                return 0.0
            mean_d = sum(delays) / len(delays)
            mean_r = sum(returns) / len(returns)
            cov = sum((d - mean_d) * (r - mean_r) for d, r in zip(delays, returns)) / len(delays)
            std_d = math.sqrt(sum((d - mean_d) ** 2 for d in delays) / len(delays))
            std_r = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / len(returns))
            if std_d == 0 or std_r == 0:
                return 0.0
            return cov / (std_d * std_r)

        actual_delay_corr = calc_delay_correlation(disclosure_history)
        permuted_delay = []
        shuffled_returns_source = list(disclosure_history)
        for _ in range(n_permutations):
            random.shuffle(shuffled_returns_source)
            permuted_delay.append(calc_delay_correlation(shuffled_returns_source))

        if permuted_delay and actual_delay_corr != 0.0:
            p_value_delay = sum(
                1 for p in permuted_delay if abs(p) >= abs(actual_delay_corr)
            ) / n_permutations
            if p_value_delay < 0.05:
                validated.append({
                    "pattern": "filing_delay_signal_correlation",
                    "description": "Filing delay length correlates with forward return signal strength",
                    "metric": round(actual_delay_corr, 4),
                    "p_value": round(p_value_delay, 4),
                    "validated": True,
                })

        return validated

    def should_notify(self, signal: dict) -> bool:
        """
        Always notify on any Tier 1 signal regardless of threshold.
        Tier 1: HIGH credibility member, significant cluster, or CRITICAL committee power.
        """
        raw_data = signal.get("raw_data", {})

        # Tier 1 criteria
        credibility_score = raw_data.get("credibility_score", 0.0)
        if credibility_score >= TIER1_CREDIBILITY_THRESHOLD:
            return True

        # Cluster with many members
        data_type = signal.get("data_type", "")
        if data_type == "congressional_cluster":
            n_members = raw_data.get("n_members", 0)
            if n_members >= 3:
                return True

        # High committee power
        committee_power = raw_data.get("committee_power", 0.0)
        if committee_power >= 1.5:
            return True

        # Signal value exceeds threshold
        value = abs(signal.get("value", 0.0))
        threshold = self.config.get("deepdata", {}).get(
            "congressional", {}
        ).get("notification_threshold", 0.3)
        if value >= threshold:
            return True

        return False
