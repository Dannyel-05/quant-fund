"""
Persistent registry of candidate and live signals.
Stored as JSON so it survives process restarts.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SignalRegistry:
    def __init__(self, config: dict, path: str = "output/signal_registry.json"):
        self.config = config
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict = self._load()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def register(
        self, name: str, signal_type: str, params: dict, metadata: dict
    ) -> None:
        entry = {
            "name": name,
            "type": signal_type,
            "params": params,
            "metadata": metadata,
            "status": "candidate",
            "registered_at": datetime.now().isoformat(),
            "promoted_at": None,
            "validation": None,
        }
        if name in self._data:
            logger.warning("Signal %s already exists — overwriting", name)
        self._data[name] = entry
        self._save()
        logger.info("Registered signal: %s (%s)", name, signal_type)

    def promote(self, name: str, validation: dict) -> bool:
        """
        If auto_promote is False (default), moves to 'validated' and waits for
        human approval.  Returns True only if actually promoted to 'live'.
        """
        if name not in self._data:
            logger.error("Signal %s not found", name)
            return False

        self._data[name]["validation"] = validation
        auto = self.config["signal"]["anomaly"].get("auto_promote", False)

        if auto:
            self._data[name]["status"] = "live"
            self._data[name]["promoted_at"] = datetime.now().isoformat()
            logger.info("Auto-promoted signal to live: %s", name)
            self._save()
            return True
        else:
            self._data[name]["status"] = "validated"
            logger.info(
                "Signal %s is validated — awaiting manual promotion to live", name
            )
            self._save()
            return False

    def set_live(self, name: str) -> None:
        """Manual promotion after human review."""
        if name in self._data:
            self._data[name]["status"] = "live"
            self._data[name]["promoted_at"] = datetime.now().isoformat()
            self._save()
            logger.info("Manually promoted to live: %s", name)

    def retire(self, name: str, reason: str = "") -> None:
        if name in self._data:
            self._data[name]["status"] = "retired"
            self._data[name]["retired_reason"] = reason
            self._save()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_live(self) -> List[str]:
        return [k for k, v in self._data.items() if v["status"] == "live"]

    def get_candidates(self) -> List[str]:
        return [k for k, v in self._data.items() if v["status"] == "candidate"]

    def get_validated(self) -> List[str]:
        return [k for k, v in self._data.items() if v["status"] == "validated"]

    def get(self, name: str) -> Optional[Dict]:
        return self._data.get(name)

    def list_all(self) -> pd.DataFrame:
        if not self._data:
            return pd.DataFrame()
        rows = []
        for v in self._data.values():
            row = {k: val for k, val in v.items() if k not in ("metadata", "validation")}
            row["sharpe"] = (v.get("metadata") or {}).get("sharpe")
            row["n_obs"] = (v.get("metadata") or {}).get("n_obs")
            rows.append(row)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._data, indent=2, default=str))
