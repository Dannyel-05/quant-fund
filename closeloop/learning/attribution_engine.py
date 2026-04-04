"""
AttributionEngine — decomposes trade P&L into contributing signal factors.

For each closed trade, queries the trade_detail_log table and computes
the marginal attribution of each signal type to the final P&L.

Usage
-----
ae = AttributionEngine(config)
report = ae.generate_report(lookback_days=90)
# report["top_factors"]    — list of (factor, avg_attribution) sorted desc
# report["worst_factors"]  — list of (factor, avg_attribution) sorted asc
# report["full_table"]     — dict factor → stats
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Columns we extract from trade_detail_log that represent signal contributions
_SIGNAL_FACTORS = [
    "pead_surprise_pct",
    "pead_signal_strength",
    "altdata_confluence_score",
    "transcript_score",
    "congressional_signal",
    "short_squeeze_score",
    "options_smfi",
    "hiring_momentum",
    "wikipedia_surge",
    "shipping_pressure",
    "weather_risk",
    "peer_influence_score",
    "analyst_revision_momentum",
    "academic_tailwind_score",
    "news_financial_context",
    "index_rebalancing_pressure",
    "signal_contradiction_score",
    "sector_rotation_score",
    "multi_tf_confirmation",
    "rsi_at_entry",
    "macd_at_entry",
    "bb_position_at_entry",
    "atr_at_entry",
]


class AttributionEngine:
    """
    Analyses closed trades to attribute P&L to individual signal factors.
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._db_path = config.get("closeloop", {}).get(
            "storage_path", "closeloop/storage/closeloop.db"
        )

    def generate_report(self, lookback_days: int = 90) -> dict[str, Any]:
        """
        Generate a factor attribution report.
        Returns dict with top_factors, worst_factors, full_table.
        """
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        trades = self._load_trades(cutoff)
        if not trades:
            logger.info("AttributionEngine: no trades found in lookback window")
            return {"top_factors": [], "worst_factors": [], "full_table": {}, "trade_count": 0}

        # For each factor, compute weighted correlation with pnl_pct
        factor_stats: dict[str, dict] = {}

        for factor in _SIGNAL_FACTORS:
            pairs = []
            for t in trades:
                factor_val = t.get(factor)
                pnl        = t.get("pnl_pct")
                if factor_val is not None and pnl is not None:
                    try:
                        pairs.append((float(factor_val), float(pnl)))
                    except (ValueError, TypeError):
                        pass

            if len(pairs) < 3:
                continue

            vals = [p[0] for p in pairs]
            pnls = [p[1] for p in pairs]
            n    = len(pairs)

            mean_v = sum(vals) / n
            mean_p = sum(pnls) / n

            cov   = sum((v - mean_v) * (p - mean_p) for v, p in zip(vals, pnls)) / n
            std_v = (sum((v - mean_v) ** 2 for v in vals) / n) ** 0.5
            std_p = (sum((p - mean_p) ** 2 for p in pnls) / n) ** 0.5

            corr = (cov / (std_v * std_p)) if (std_v > 1e-9 and std_p > 1e-9) else 0.0
            avg_factor = mean_v
            avg_pnl    = mean_p

            # Attribution = corr * std of pnl — how much of pnl variance is explained
            attribution = corr * std_p

            factor_stats[factor] = {
                "n":           n,
                "correlation": round(corr, 4),
                "attribution": round(attribution, 4),
                "avg_factor":  round(avg_factor, 4),
                "avg_pnl":     round(avg_pnl, 4),
            }

        # Sort
        sorted_factors = sorted(factor_stats.items(), key=lambda x: -x[1]["attribution"])
        top_factors    = [(f, s["attribution"]) for f, s in sorted_factors[:5]]
        worst_factors  = [(f, s["attribution"]) for f, s in sorted_factors[-5:]]

        report = {
            "trade_count":   len(trades),
            "lookback_days": lookback_days,
            "top_factors":   top_factors,
            "worst_factors": worst_factors,
            "full_table":    factor_stats,
            "generated_at":  datetime.now().isoformat(),
        }

        logger.info(
            "AttributionEngine: analysed %d trades, %d factors. Top: %s",
            len(trades), len(factor_stats),
            [f for f, _ in top_factors[:3]]
        )
        return report

    def _load_trades(self, cutoff_iso: str) -> list[dict]:
        """Load closed trades from trade_detail_log."""
        try:
            con = sqlite3.connect(self._db_path, timeout=30)
            con.row_factory = sqlite3.Row
            cur = con.cursor()

            # Check table exists
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_detail_log'"
            )
            if not cur.fetchone():
                logger.warning("AttributionEngine: trade_detail_log table not found")
                con.close()
                return []

            cur.execute(
                "SELECT * FROM trade_detail_log WHERE exit_date >= ? AND pnl_pct IS NOT NULL",
                (cutoff_iso,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            con.close()
            return rows
        except Exception as exc:
            logger.warning("AttributionEngine: DB load error: %s", exc)
            return []

    def top_factor_signals(self, n: int = 3) -> list[str]:
        """Return names of top-N attribution factors."""
        report = self.generate_report()
        return [f for f, _ in report.get("top_factors", [])[:n]]

    def save_report_to_db(self, report: dict) -> None:
        """Persist the report JSON to closeloop.db for historical tracking."""
        try:
            import json
            con = sqlite3.connect(self._db_path, timeout=30)
            cur = con.cursor()
            cur.execute(
                """CREATE TABLE IF NOT EXISTS attribution_reports (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    generated_at TEXT,
                    lookback_days INTEGER,
                    trade_count INTEGER,
                    report_json TEXT
                )"""
            )
            cur.execute(
                "INSERT INTO attribution_reports (generated_at, lookback_days, trade_count, report_json) "
                "VALUES (?, ?, ?, ?)",
                (
                    report.get("generated_at", datetime.now().isoformat()),
                    report.get("lookback_days", 90),
                    report.get("trade_count", 0),
                    __import__("json").dumps(report),
                ),
            )
            con.commit()
            con.close()
            logger.info("AttributionEngine: report saved to DB")
        except Exception as exc:
            logger.warning("AttributionEngine: save error: %s", exc)
