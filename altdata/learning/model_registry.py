from pathlib import Path
import json, joblib, logging, shutil
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ModelRegistry:
    def __init__(self, base_path: str = "altdata/models"):
        self.base_path = Path(base_path)
        self.history_path = self.base_path / "model_history.json"
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._history: Dict = self._load_history()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_model(
        self,
        model_name: str,
        model_obj,
        metrics: dict,
        hyperparams: dict = None,
    ) -> str:
        """Save model, create version string, update history, create 'current' symlink.

        Returns version string.
        """
        existing_versions = self._history.get(model_name, [])
        n = len(existing_versions) + 1
        sharpe = metrics.get("sharpe", 0)
        version = (
            f"v{n:03d}_{datetime.now().strftime('%Y-%m-%d')}_sharpe{sharpe:.2f}"
        )

        # Directory for this version
        version_dir = self.base_path / model_name / version
        version_dir.mkdir(parents=True, exist_ok=True)

        model_path = version_dir / "model.pkl"
        joblib.dump(model_obj, model_path)

        # Persist metadata alongside the model
        meta = {
            "version": version,
            "trained_at": datetime.now().isoformat(),
            "metrics": metrics,
            "hyperparams": hyperparams or {},
            "is_active": True,
            "rolled_back": False,
            "live_sharpe": None,
            "live_accuracy": None,
        }
        (version_dir / "metadata.json").write_text(json.dumps(meta, indent=2, default=str))

        # Mark all previous versions inactive
        for entry in existing_versions:
            entry["is_active"] = False

        existing_versions.append(meta)
        self._history[model_name] = existing_versions
        self._save_history()

        # Update 'current' symlink
        current_link = self.base_path / model_name / "current"
        if current_link.is_symlink() or current_link.exists():
            current_link.unlink()
        current_link.symlink_to(version_dir.resolve())

        logger.info(
            "Saved model %s version %s (sharpe=%.2f)", model_name, version, sharpe
        )
        return version

    def load_model(self, model_name: str, version: str = "current"):
        """Load model from disk. version='current' loads active model."""
        if version == "current":
            model_path = self.base_path / model_name / "current" / "model.pkl"
        else:
            model_path = self.base_path / model_name / version / "model.pkl"

        if not model_path.exists():
            raise FileNotFoundError(
                f"Model file not found: {model_path}"
            )

        logger.debug("Loading model %s version %s from %s", model_name, version, model_path)
        return joblib.load(model_path)

    def get_history(self, model_name: str) -> List[dict]:
        """Return all versions for a model, sorted by trained_at."""
        entries = self._history.get(model_name, [])
        try:
            return sorted(entries, key=lambda e: e.get("trained_at", ""))
        except Exception:
            return entries

    def mark_rolled_back(self, model_name: str, version: str, reason: str):
        """Mark a version as rolled back."""
        for entry in self._history.get(model_name, []):
            if entry["version"] == version:
                entry["rolled_back"] = True
                entry["is_active"] = False
                entry["rollback_reason"] = reason
                entry["rolled_back_at"] = datetime.now().isoformat()
                logger.warning(
                    "Marked %s version %s as rolled back. Reason: %s",
                    model_name, version, reason,
                )
                break
        self._save_history()

    def update_live_performance(
        self,
        model_name: str,
        version: str,
        live_sharpe: float,
        live_accuracy: float,
    ):
        """Update the live performance stats for a deployed model."""
        for entry in self._history.get(model_name, []):
            if entry["version"] == version:
                entry["live_sharpe"] = live_sharpe
                entry["live_accuracy"] = live_accuracy
                entry["live_updated_at"] = datetime.now().isoformat()
                logger.debug(
                    "Updated live perf for %s %s: sharpe=%.2f acc=%.3f",
                    model_name, version, live_sharpe, live_accuracy,
                )
                break
        self._save_history()

    def get_previous_version(self, model_name: str) -> Optional[str]:
        """Return the version before the current active one."""
        history = self.get_history(model_name)
        if len(history) < 2:
            return None

        # Find index of current active version
        active_idx = None
        for i, entry in enumerate(history):
            if entry.get("is_active") and not entry.get("rolled_back"):
                active_idx = i
                break

        if active_idx is None or active_idx == 0:
            return None

        # Walk backwards to find previous non-rolled-back version
        for i in range(active_idx - 1, -1, -1):
            if not history[i].get("rolled_back"):
                return history[i]["version"]

        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_history(self) -> Dict:
        if self.history_path.exists():
            try:
                return json.loads(self.history_path.read_text())
            except Exception:
                return {}
        return {}

    def _save_history(self):
        self.history_path.write_text(
            json.dumps(self._history, indent=2, default=str)
        )
