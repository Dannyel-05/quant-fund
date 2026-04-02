import numpy as np
import pandas as pd
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class ModelValidator:
    def __init__(self, config: dict):
        self.min_sharpe = 0.5
        self.min_win_rate = 0.50
        self.min_samples = 30

    def validate_predictions(
        self,
        predictions: List[int],
        actuals: List[int],
        returns: List[float],
    ) -> Dict:
        """
        Validate model predictions against actual outcomes.

        predictions : list of +1/-1 direction predictions
        actuals     : list of +1/-1 actual outcomes
        returns     : list of trade returns (signed)

        Returns full validation report dict.
        """
        if len(predictions) < self.min_samples:
            return {
                "valid": False,
                "reason": "insufficient_samples",
                "n": len(predictions),
            }

        preds = np.array(predictions)
        acts = np.array(actuals)
        rets = np.array(returns)

        accuracy = float((preds == acts).mean())

        # Sharpe of signal-weighted returns
        signed_returns = preds * rets
        if signed_returns.std() > 0:
            sharpe = float(
                signed_returns.mean() / signed_returns.std() * np.sqrt(252)
            )
        else:
            sharpe = 0.0

        win_rate = float((signed_returns > 0).mean())

        is_valid = sharpe >= self.min_sharpe and win_rate >= self.min_win_rate

        report = {
            "valid": is_valid,
            "accuracy": accuracy,
            "sharpe": sharpe,
            "win_rate": win_rate,
            "n_samples": len(predictions),
            "mean_return": float(signed_returns.mean()),
        }

        logger.debug(
            "Validation result: valid=%s sharpe=%.2f win_rate=%.3f acc=%.3f n=%d",
            is_valid, sharpe, win_rate, accuracy, len(predictions),
        )
        return report

    def compare_models(
        self,
        current_metrics: Dict,
        new_metrics: Dict,
        threshold: float = 0.9,
    ) -> bool:
        """
        Returns True if new model should replace current.

        Condition: new_sharpe >= current_sharpe * threshold
        """
        curr_sharpe = current_metrics.get("sharpe", 0)
        new_sharpe = new_metrics.get("sharpe", 0)
        should_replace = new_sharpe >= curr_sharpe * threshold

        logger.info(
            "compare_models: curr_sharpe=%.2f new_sharpe=%.2f threshold=%.2f -> replace=%s",
            curr_sharpe, new_sharpe, threshold, should_replace,
        )
        return should_replace
