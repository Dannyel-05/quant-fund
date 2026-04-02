"""
Discovery Registry — stores and manages newly discovered frontier patterns.

All patterns that pass the correlation threshold enter the watchlist.
Patterns that subsequently pass full validation are promoted to live.
Patterns that fail validation are archived (never deleted — the failure
itself is evidence against data-mining bias).
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

STATUS_WATCHLIST  = "watchlist"
STATUS_VALIDATING = "validating"
STATUS_LIVE       = "live"
STATUS_FAILED     = "failed"
STATUS_RETIRED    = "retired"


class DiscoveryRegistry:
    """
    Persistent JSON-backed registry of all frontier signal candidates.

    Uses a flat JSONL file so every state transition is auditable.
    Active records are kept in memory; append-only log for history.
    """

    def __init__(self, registry_path: str = "logs/discovery_registry.jsonl"):
        self._path = Path(registry_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._active: Dict[str, Dict] = {}
        self._load()

    # ------------------------------------------------------------------
    def _load(self) -> None:
        if not self._path.exists():
            return
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    name = rec.get("signal_name")
                    if name:
                        self._active[name] = rec
                except Exception:
                    pass

    def _append(self, record: Dict) -> None:
        with open(self._path, "a") as f:
            f.write(json.dumps(record) + "\n")

    # ------------------------------------------------------------------
    def register(
        self,
        signal_name: str,
        description: str,
        source_signals: List[str],
        initial_correlation: float,
        discovery_method: str = "auto",
        nonsense_score: float = 0.0,
        has_economic_story: bool = False,
        config: Optional[Dict] = None,
    ) -> Dict:
        """
        Register a new discovered pattern on the watchlist.

        Parameters
        ----------
        signal_name         : unique identifier
        description         : human-readable explanation
        source_signals      : list of raw signals involved
        initial_correlation : IS correlation with returns (for logging)
        discovery_method    : 'auto', 'manual', 'cross_module'
        nonsense_score      : 0–1; 1 = completely economically implausible
        has_economic_story  : True if a coherent mechanism exists
        """
        ts = datetime.now(timezone.utc).isoformat()
        record = {
            "signal_name": signal_name,
            "description": description,
            "source_signals": source_signals,
            "initial_correlation": round(initial_correlation, 4),
            "discovery_method": discovery_method,
            "nonsense_score": round(nonsense_score, 3),
            "has_economic_story": has_economic_story,
            "status": STATUS_WATCHLIST,
            "evidence_tier": 4,
            "evidence_grade": "D",
            "live_days": 0,
            "oos_sharpe": None,
            "registered_at": ts,
            "last_updated": ts,
            "validation_attempts": 0,
            "notes": [],
        }
        self._active[signal_name] = record
        self._append(record)
        logger.info(
            f"[DiscoveryRegistry] Registered '{signal_name}' "
            f"(nonsense={nonsense_score:.2f}, story={has_economic_story})"
        )
        return record

    def update_status(
        self,
        signal_name: str,
        new_status: str,
        evidence_grade: Optional[str] = None,
        suggested_tier: Optional[int] = None,
        oos_sharpe: Optional[float] = None,
        note: Optional[str] = None,
    ) -> Optional[Dict]:
        rec = self._active.get(signal_name)
        if not rec:
            logger.warning(f"[DiscoveryRegistry] Unknown signal: '{signal_name}'")
            return None

        rec["status"] = new_status
        rec["last_updated"] = datetime.now(timezone.utc).isoformat()
        if evidence_grade:
            rec["evidence_grade"] = evidence_grade
        if suggested_tier is not None:
            rec["evidence_tier"] = suggested_tier
        if oos_sharpe is not None:
            rec["oos_sharpe"] = round(oos_sharpe, 3)
        if note:
            rec["notes"].append({"ts": rec["last_updated"], "note": note})

        self._append(rec)
        logger.info(f"[DiscoveryRegistry] '{signal_name}' → {new_status}")
        return rec

    def get(self, signal_name: str) -> Optional[Dict]:
        return self._active.get(signal_name)

    def get_all(self, status: Optional[str] = None) -> List[Dict]:
        records = list(self._active.values())
        if status:
            records = [r for r in records if r["status"] == status]
        return sorted(records, key=lambda r: r["registered_at"], reverse=True)

    def get_watchlist(self, limit: int = 20) -> List[Dict]:
        return self.get_all(STATUS_WATCHLIST)[:limit]

    def get_live(self) -> List[Dict]:
        return self.get_all(STATUS_LIVE)

    def summary(self) -> Dict:
        all_rec = list(self._active.values())
        by_status = {}
        for r in all_rec:
            s = r["status"]
            by_status[s] = by_status.get(s, 0) + 1
        return {
            "total": len(all_rec),
            "by_status": by_status,
            "live_signals": [r["signal_name"] for r in all_rec if r["status"] == STATUS_LIVE],
            "watchlist_count": by_status.get(STATUS_WATCHLIST, 0),
        }
