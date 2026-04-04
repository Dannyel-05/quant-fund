"""
FrontierSignalValidator — statistically validates frontier signals against SPY returns.

For each frontier signal type:
  1. Query historical observations from database
  2. Match with SPY next-day returns from historical_db.db
  3. Calculate correlation, t-statistic, p-value (Pearson)
  4. Calculate strategy Sharpe ratio (long when signal positive, flat otherwise)
  5. Promotion criteria: Sharpe > 0.5 AND p < 0.05 AND n >= 50 observations
  6. Status: PROMOTED / FAILED_VALIDATION / INSUFFICIENT_DATA

Results written to:
  logs/frontier_signal_validation.log
  closeloop.db frontier_signal_validation table
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

_MIN_OBSERVATIONS  = 50
_MIN_SHARPE        = 0.5
_MAX_P_VALUE       = 0.05
_PROMOTED_WEIGHT   = 0.02

_LOG_FILE = "logs/frontier_signal_validation.log"

# Known frontier signal names (from FrontierSignalEngine)
_KNOWN_SIGNALS = [
    "grai", "schumann_deviation", "pollen_stress_index",
    "electricity_anomaly", "canal_congestion_index",
    "satellite_activity_drop", "asi", "social_contagion_r0",
    "obituary_impact_score", "church_attendance_signal",
    "divorce_anomaly", "hq_traffic_index",
    "academic_citation_velocity", "amr_urgency",
    "soil_health_degradation", "qtpi", "food_safety_risk",
    "gamma_overhang_score", "building_permit_velocity",
    "lpas_mean",
    # Classic frontier signals
    "geomagnetic", "lunar_phase", "schumann",
]


class FrontierSignalValidator:
    """
    Validates frontier signals against SPY returns using statistical testing.
    """

    CLOSELOOP_DB   = "closeloop/storage/closeloop.db"
    HISTORICAL_DB  = "output/historical_db.db"
    PERMANENT_DB   = "output/permanent_archive.db"

    def __init__(
        self,
        closeloop_db: Optional[str] = None,
        historical_db: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> None:
        self._closeloop_db  = closeloop_db  or self.CLOSELOOP_DB
        self._historical_db = historical_db or self.HISTORICAL_DB
        self._config = config or {}
        self._ensure_table()

    # ── DB setup ──────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            con = sqlite3.connect(self._closeloop_db, timeout=10)
            con.execute("""
                CREATE TABLE IF NOT EXISTS frontier_signal_validation (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_name  TEXT    NOT NULL,
                    run_date     TEXT    NOT NULL,
                    n_obs        INTEGER,
                    correlation  REAL,
                    t_stat       REAL,
                    p_value      REAL,
                    sharpe       REAL,
                    status       TEXT,
                    weight       REAL    DEFAULT 0.0,
                    UNIQUE(signal_name, run_date)
                )
            """)
            con.commit()
            con.close()
        except Exception as exc:
            logger.warning("FrontierSignalValidator._ensure_table: %s", exc)

    # ── data loading ──────────────────────────────────────────────────────

    def _load_spy_returns(self) -> Dict[str, float]:
        """Load SPY daily returns keyed by date string."""
        spy: Dict[str, float] = {}
        try:
            con = sqlite3.connect(self._historical_db, timeout=10)
            rows = con.execute(
                "SELECT date, close FROM price_history WHERE ticker='SPY' ORDER BY date"
            ).fetchall()
            con.close()
            closes = [(r[0], r[1]) for r in rows if r[1]]
            for i in range(1, len(closes)):
                date_str  = closes[i][0]
                ret = (closes[i][1] - closes[i - 1][1]) / closes[i - 1][1]
                spy[date_str] = ret
        except Exception as exc:
            logger.debug("_load_spy_returns: %s", exc)
        return spy

    def _load_signal_observations(self, signal_name: str) -> Dict[str, float]:
        """
        Query databases for historical signal observations.
        Returns {date_str: signal_value}.
        """
        obs: Dict[str, float] = {}

        # Try closeloop.db signals_log
        try:
            con = sqlite3.connect(self._closeloop_db, timeout=10)
            rows = con.execute("""
                SELECT date(timestamp), AVG(score)
                FROM signals_log
                WHERE signal_type LIKE ?
                GROUP BY date(timestamp)
            """, (f"%{signal_name}%",)).fetchall()
            con.close()
            for date_str, val in rows:
                if val is not None and date_str:
                    obs[date_str] = float(val)
        except Exception:
            pass

        # Try permanent_archive.db predictions_log
        try:
            con = sqlite3.connect(self.PERMANENT_DB, timeout=10)
            rows = con.execute("""
                SELECT date(created_at), AVG(prediction_value)
                FROM predictions_log
                WHERE signal_type LIKE ?
                GROUP BY date(created_at)
            """, (f"%{signal_name}%",)).fetchall()
            con.close()
            for date_str, val in rows:
                if val is not None and date_str and date_str not in obs:
                    obs[date_str] = float(val)
        except Exception:
            pass

        return obs

    # ── statistics ────────────────────────────────────────────────────────

    def _compute_stats(
        self, signal_vals: List[float], returns: List[float]
    ) -> Dict[str, Any]:
        """Compute correlation, t-stat, p-value, and Sharpe for aligned series."""
        try:
            from scipy import stats as sp_stats
        except ImportError:
            sp_stats = None  # type: ignore

        n = len(signal_vals)
        sig = np.array(signal_vals, dtype=float)
        ret = np.array(returns,     dtype=float)

        # Pearson correlation
        if sp_stats is not None:
            corr_r, p_value = sp_stats.pearsonr(sig, ret)
        else:
            corr_r = float(np.corrcoef(sig, ret)[0, 1])
            # Approximate p-value via t distribution
            t_val  = corr_r * np.sqrt((n - 2) / max(1 - corr_r ** 2, 1e-12))
            # two-tailed p via normal approximation for large n
            p_value = float(2 * (1 - 0.5 * (1 + np.sign(abs(t_val)) *
                            (1 - np.exp(-0.717 * abs(t_val) - 0.416 * t_val ** 2)))))

        t_stat = float(corr_r * np.sqrt((n - 2) / max(1 - corr_r ** 2, 1e-12)))

        # Simple strategy: long next day when signal > 0, flat otherwise
        strategy_returns = np.where(sig > 0, ret, 0.0)
        mean_ret = strategy_returns.mean()
        std_ret  = strategy_returns.std()
        sharpe   = float(mean_ret / std_ret * np.sqrt(252)) if std_ret > 1e-10 else 0.0

        return {
            "n_obs":       n,
            "correlation": float(corr_r),
            "t_stat":      t_stat,
            "p_value":     float(p_value),
            "sharpe":      sharpe,
        }

    # ── main validation ───────────────────────────────────────────────────

    def validate_signal(self, signal_name: str) -> Dict[str, Any]:
        """Validate one frontier signal. Returns result dict."""
        spy_returns = self._load_spy_returns()
        obs = self._load_signal_observations(signal_name)

        # Align: dates where we have both signal and SPY next-day return
        aligned_sig: List[float] = []
        aligned_ret: List[float] = []
        for date_str, sig_val in sorted(obs.items()):
            if date_str in spy_returns:
                aligned_sig.append(sig_val)
                aligned_ret.append(spy_returns[date_str])

        n = len(aligned_sig)
        today = datetime.utcnow().date().isoformat()

        if n < _MIN_OBSERVATIONS:
            result = {
                "signal_name": signal_name,
                "run_date":    today,
                "n_obs":       n,
                "correlation": None,
                "t_stat":      None,
                "p_value":     None,
                "sharpe":      None,
                "status":      "INSUFFICIENT_DATA",
                "weight":      0.0,
            }
        else:
            stats = self._compute_stats(aligned_sig, aligned_ret)
            promoted = (
                stats["sharpe"]  >= _MIN_SHARPE and
                stats["p_value"] <= _MAX_P_VALUE
            )
            status = "PROMOTED" if promoted else "FAILED_VALIDATION"
            weight = _PROMOTED_WEIGHT if promoted else 0.0
            result = {
                "signal_name": signal_name,
                "run_date":    today,
                "status":      status,
                "weight":      weight,
                **stats,
            }

        # Persist
        try:
            con = sqlite3.connect(self._closeloop_db, timeout=10)
            con.execute("""
                INSERT OR REPLACE INTO frontier_signal_validation
                (signal_name, run_date, n_obs, correlation, t_stat, p_value, sharpe, status, weight)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                result["signal_name"], result["run_date"], result.get("n_obs"),
                result.get("correlation"), result.get("t_stat"), result.get("p_value"),
                result.get("sharpe"), result["status"], result["weight"],
            ))
            con.commit()
            con.close()
        except Exception as exc:
            logger.debug("frontier validation store %s: %s", signal_name, exc)

        return result

    def run_all(self) -> List[Dict[str, Any]]:
        """Validate all known frontier signals. Write log. Return results list."""
        results = [self.validate_signal(s) for s in _KNOWN_SIGNALS]
        self._write_log(results)
        return results

    # ── log writing ───────────────────────────────────────────────────────

    def _write_log(self, results: List[Dict[str, Any]]) -> None:
        os.makedirs("logs", exist_ok=True)
        promoted   = [r for r in results if r["status"] == "PROMOTED"]
        failed     = [r for r in results if r["status"] == "FAILED_VALIDATION"]
        insuff     = [r for r in results if r["status"] == "INSUFFICIENT_DATA"]

        lines = [
            "=" * 72,
            "FRONTIER SIGNAL VALIDATION REPORT",
            f"Date: {datetime.utcnow().isoformat()}",
            f"Signals tested: {len(results)}",
            f"PROMOTED: {len(promoted)} | FAILED: {len(failed)} | INSUFFICIENT DATA: {len(insuff)}",
            "=" * 72,
            "",
        ]

        if promoted:
            lines += ["── PROMOTED (Sharpe > 0.5, p < 0.05) ──"]
            for r in promoted:
                lines.append(
                    f"  {r['signal_name']:30s} Sharpe={r['sharpe']:.3f} "
                    f"p={r['p_value']:.4f} corr={r['correlation']:.3f} n={r['n_obs']} weight={r['weight']}"
                )
            lines.append("")

        if failed:
            lines += ["── FAILED VALIDATION ──"]
            for r in failed:
                lines.append(
                    f"  {r['signal_name']:30s} Sharpe={r['sharpe']:.3f} "
                    f"p={r['p_value']:.4f} n={r['n_obs']}"
                )
            lines.append("")

        lines += [f"── INSUFFICIENT DATA (n < {_MIN_OBSERVATIONS}) ──"]
        for r in insuff:
            lines.append(f"  {r['signal_name']:30s} n={r['n_obs']}")
        lines += ["", "=" * 72]

        log_text = "\n".join(lines)
        with open(_LOG_FILE, "w") as fh:
            fh.write(log_text + "\n")
        logger.info("Frontier validation report written to %s", _LOG_FILE)

    # ── Telegram ──────────────────────────────────────────────────────────

    def send_telegram_summary(self) -> None:
        results = self.run_all()
        promoted = [r for r in results if r["status"] == "PROMOTED"]
        failed   = [r for r in results if r["status"] == "FAILED_VALIDATION"]
        insuff   = [r for r in results if r["status"] == "INSUFFICIENT_DATA"]

        lines = [
            "[FrontierValidator] Validation complete",
            f"Tested: {len(results)} signals",
            f"PROMOTED: {len(promoted)} | FAILED: {len(failed)} | INSUFF DATA: {len(insuff)}",
        ]
        for r in promoted:
            lines.append(f"✅ PROMOTED: {r['signal_name']} (Sharpe={r['sharpe']:.3f})")
        for r in failed:
            lines.append(f"❌ FAILED: {r['signal_name']}")
        if not promoted:
            lines.append("No frontier signals yet meet promotion criteria.")
            lines.append(f"All {len(insuff)} have < {_MIN_OBSERVATIONS} observations.")
            lines.append("Will re-validate as data accumulates.")

        text = "\n".join(lines)
        try:
            import requests
            tg = self._config.get("notifications", {}).get("telegram", {})
            token   = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
            if token and chat_id:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=10,
                )
        except Exception:
            pass

    def status(self) -> Dict[str, Any]:
        try:
            con = sqlite3.connect(self._closeloop_db, timeout=10)
            rows = con.execute(
                "SELECT status, COUNT(*) FROM frontier_signal_validation GROUP BY status"
            ).fetchall()
            con.close()
            return {r[0]: r[1] for r in rows}
        except Exception:
            return {}
