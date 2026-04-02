"""
Frontier Position Sizer.

Tiered sizing based on evidence quality and UMCI regime:
  Tier 1 (published replicated)  : 1.00× base Kelly
  Tier 2 (novel, strong OOS)     : 0.50× base Kelly
  Tier 3 (novel, moderate OOS)   : 0.25× base Kelly
  Tier 4 (watchlist, early data) : 0.10× base Kelly
  Tier 5 (speculative / banned)  : 0.00× — no sizing

UMCI then applies a multiplier on top.
"""
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Base fraction of Kelly allocated per tier
TIER_KELLY_FRACTIONS = {1: 1.00, 2: 0.50, 3: 0.25, 4: 0.10, 5: 0.00}

# Minimum live days before a tier can be promoted
TIER_MIN_LIVE_DAYS = {1: 365, 2: 180, 3: 90, 4: 30, 5: 0}

# Minimum out-of-sample Sharpe to remain at tier
TIER_MIN_OOS_SHARPE = {1: 1.5, 2: 1.0, 3: 0.7, 4: 0.3, 5: 0.0}


def calculate_frontier_position_size(
    signal_name: str,
    direction: int,               # +1 / -1
    confidence: float,            # 0–1
    evidence_tier: int,           # 1–5
    base_kelly_fraction: float,   # from core risk module
    portfolio_value: float,
    current_price: float,
    umci_multiplier: float = 1.0, # from UMCI level
    config: Optional[Dict] = None,
) -> Dict:
    """
    Calculate shares / notional for a frontier signal position.

    Returns
    -------
    dict with keys: shares, notional, tier_fraction, final_kelly,
                    blocked, block_reason
    """
    cfg = (config or {}).get("frontier", {}).get("sizing", {})
    max_single_position_pct = cfg.get("max_single_position_pct", 0.02)

    tier_fraction = TIER_KELLY_FRACTIONS.get(evidence_tier, 0.0)

    if tier_fraction == 0.0:
        return {
            "shares": 0,
            "notional": 0.0,
            "tier_fraction": 0.0,
            "final_kelly": 0.0,
            "blocked": True,
            "block_reason": f"Tier {evidence_tier} — zero allocation",
        }

    if umci_multiplier == 0.0:
        return {
            "shares": 0,
            "notional": 0.0,
            "tier_fraction": tier_fraction,
            "final_kelly": 0.0,
            "blocked": True,
            "block_reason": "UMCI UNPRECEDENTED — new positions halted",
        }

    final_kelly = base_kelly_fraction * tier_fraction * umci_multiplier * confidence
    cap = max_single_position_pct * portfolio_value
    notional = min(final_kelly * portfolio_value, cap)
    shares = int(notional / current_price) * direction if current_price > 0 else 0

    logger.debug(
        f"[FrontierSizer] {signal_name} tier={evidence_tier} "
        f"kelly={final_kelly:.4f} notional=${notional:,.0f} shares={shares}"
    )

    return {
        "shares": shares,
        "notional": round(notional * direction, 2),
        "tier_fraction": tier_fraction,
        "final_kelly": round(final_kelly, 6),
        "blocked": False,
        "block_reason": None,
    }


def check_tier_promotion(
    signal_name: str,
    current_tier: int,
    live_days: int,
    oos_sharpe: float,
    fsp: float,
    validated_replications: int,
) -> Dict:
    """
    Check whether a signal qualifies for automatic tier promotion.

    Promotion criteria (all must pass):
      - Minimum live days for the next tier
      - OOS Sharpe above next-tier minimum
      - FSP > 0.5 (signal still has independent information)
      - For tier 2→1: must have >= 2 independent replications

    Returns
    -------
    dict: {promoted: bool, new_tier: int, reason: str}
    """
    if current_tier <= 1:
        return {"promoted": False, "new_tier": current_tier, "reason": "Already at Tier 1"}

    next_tier = current_tier - 1
    min_days = TIER_MIN_LIVE_DAYS[next_tier]
    min_sharpe = TIER_MIN_OOS_SHARPE[next_tier]

    if live_days < min_days:
        return {
            "promoted": False,
            "new_tier": current_tier,
            "reason": f"Insufficient live days: {live_days} < {min_days}",
        }
    if oos_sharpe < min_sharpe:
        return {
            "promoted": False,
            "new_tier": current_tier,
            "reason": f"OOS Sharpe too low: {oos_sharpe:.2f} < {min_sharpe}",
        }
    if fsp < 0.5:
        return {
            "promoted": False,
            "new_tier": current_tier,
            "reason": f"FSP too low ({fsp:.2f}): signal purity declining",
        }
    if next_tier == 1 and validated_replications < 2:
        return {
            "promoted": False,
            "new_tier": current_tier,
            "reason": f"Tier 1 requires ≥2 replications, have {validated_replications}",
        }

    logger.info(
        f"[TierPromotion] '{signal_name}' promoted: Tier {current_tier} → Tier {next_tier} "
        f"(live_days={live_days}, OOS_Sharpe={oos_sharpe:.2f}, FSP={fsp:.2f})"
    )
    return {
        "promoted": True,
        "new_tier": next_tier,
        "reason": f"All criteria met: {live_days}d live, Sharpe={oos_sharpe:.2f}, FSP={fsp:.2f}",
    }
