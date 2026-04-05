"""
Apollo Reasoning Engine — Two-layer architecture:
  Layer 1: Deterministic Core — pure Python, no API calls, always runs first
  Layer 2: LLM Interpretation — wraps Layer 1 output only, optional

The LLM never touches raw trade data or signal weights directly.
It only receives the structured context dict from Layer 1.
"""
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]

# ── Signal interpretation thresholds ──────────────────────────────────────────

_SIGNAL_THRESHOLDS = {
    "momentum": [
        (0.70, "strong", "Strong upward trend"),
        (0.40, "moderate", "Moderate positive momentum"),
        (0.0,  "weak",   "Weak signal — insufficient momentum"),
    ],
    "pead": [
        (0.75, "strong", "Strong post-earnings drift expected"),
        (0.45, "moderate", "Moderate earnings surprise — drift likely"),
        (0.0,  "weak",   "Weak earnings signal"),
    ],
    "mean_reversion": [
        (0.70, "strong", "Strong mean reversion setup — significant deviation"),
        (0.40, "moderate", "Moderate mean reversion opportunity"),
        (0.0,  "weak",   "Weak reversion signal — price near mean"),
    ],
    "gap": [
        (0.65, "strong", "Large gap — high follow-through probability"),
        (0.35, "moderate", "Moderate gap — conditional entry"),
        (0.0,  "weak",   "Weak gap signal"),
    ],
    "pairs": [
        (2.0, "strong", "High z-score — strong pairs spread divergence"),
        (1.5, "moderate", "Moderate z-score — pairs spread widening"),
        (0.0, "weak",   "Low z-score — spread near equilibrium"),
    ],
    "options_flow": [
        (0.70, "strong", "Strong unusual options activity detected"),
        (0.40, "moderate", "Moderate options flow signal"),
        (0.0,  "weak",   "Weak options flow — limited unusual activity"),
    ],
    "insider": [
        (0.75, "strong", "Significant insider buying — strong conviction"),
        (0.45, "moderate", "Moderate insider activity"),
        (0.0,  "weak",   "Weak insider signal"),
    ],
    "wavelet": [
        (0.65, "strong", "Strong cycle signal — clear dominant period"),
        (0.35, "moderate", "Moderate wavelet signal"),
        (0.0,  "weak",   "Weak wavelet signal — noisy cycle"),
    ],
    "kalman": [
        (0.60, "strong", "Kalman trend confirmed — smooth directional bias"),
        (0.35, "moderate", "Moderate Kalman signal"),
        (0.0,  "weak",   "Weak Kalman signal — trend uncertain"),
    ],
}

_REGIME_PROFILES = {
    "BULL": {
        "plain_english": "Bull market — equities trending upward with broad participation",
        "sizing_impact": "Full sizing active — standard position limits apply",
        "active_signals": ["momentum", "pead", "gap", "options_flow", "insider", "wavelet", "kalman"],
        "suppressed_signals": [],
    },
    "NEUTRAL": {
        "plain_english": "Neutral market — no clear directional bias, range-bound",
        "sizing_impact": "Standard sizing with moderate caution",
        "active_signals": ["pead", "mean_reversion", "pairs", "options_flow", "insider"],
        "suppressed_signals": ["momentum", "gap"],
    },
    "BEAR": {
        "plain_english": "Bear market — equities declining, defensive posture required",
        "sizing_impact": "Reduced long sizing (50%), short signals prioritised",
        "active_signals": ["mean_reversion", "pairs", "options_flow"],
        "suppressed_signals": ["momentum", "gap", "pead"],
    },
    "CRISIS": {
        "plain_english": "Crisis regime — extreme volatility, capital preservation mode",
        "sizing_impact": "Minimal or zero new positions — drawdown protection active",
        "active_signals": ["pairs"],
        "suppressed_signals": ["momentum", "gap", "pead", "insider", "options_flow", "wavelet"],
    },
    "EUPHORIA": {
        "plain_english": "Euphoria regime — overbought conditions, mean reversion risk elevated",
        "sizing_impact": "Reduced long sizing, mean reversion signals weighted higher",
        "active_signals": ["mean_reversion", "pairs", "kalman"],
        "suppressed_signals": ["momentum", "gap"],
    },
}


class ReasoningEngine:
    """
    Two-layer reasoning engine for Apollo.

    Layer 1: Deterministic — always runs, pure Python logic.
    Layer 2: LLM — wraps Layer 1 output, optional, gracefully degrades.
    """

    def __init__(self):
        self._cache: dict[str, str] = {}  # hash -> LLM response
        self._cache_ts: dict[str, float] = {}  # hash -> timestamp
        self._cache_ttl = 600  # 10 minutes
        self._api_key = self._load_api_key()

    def _load_api_key(self) -> Optional[str]:
        """Load Anthropic API key from env or config."""
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if key:
            return key
        try:
            cfg = yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())
            return cfg.get("api_keys", {}).get("anthropic", "")
        except Exception:
            return ""

    # ── Layer 1: Deterministic ─────────────────────────────────────────────────

    def interpret_signal(self, signal_name: str, score: float) -> dict:
        """
        Deterministically interpret a signal score.
        Returns: {"label": str, "direction": str, "strength": str, "raw_score": float}
        """
        name = signal_name.lower().replace(" ", "_")
        thresholds = _SIGNAL_THRESHOLDS.get(name, _SIGNAL_THRESHOLDS.get("momentum"))

        label = thresholds[-1][2]  # default: weakest
        strength = thresholds[-1][1]

        for threshold, strength_label, description in thresholds:
            if score >= threshold:
                label = description
                strength = strength_label
                break

        direction = "bullish" if score > 0 else ("bearish" if score < 0 else "neutral")

        return {
            "label": label,
            "direction": direction,
            "strength": strength,
            "raw_score": round(score, 4),
        }

    def interpret_regime(self, regime: str, confidence: float) -> dict:
        """
        Deterministically interpret a regime string.
        Returns: {"plain_english": str, "sizing_impact": str, "active_signals": list, "suppressed_signals": list}
        """
        profile = _REGIME_PROFILES.get(regime.upper(), _REGIME_PROFILES["NEUTRAL"])
        result = dict(profile)
        result["regime"] = regime
        result["confidence"] = round(confidence, 3)
        result["confidence_label"] = (
            "high" if confidence >= 0.70
            else "moderate" if confidence >= 0.45
            else "low"
        )
        return result

    def detect_conflicts(self, signals_dict: dict) -> list[str]:
        """
        Detect contradicting signals.
        Returns list of conflict description strings.
        """
        conflicts = []
        momentum = signals_dict.get("momentum", 0.0) or 0.0
        mean_rev = signals_dict.get("mean_reversion", 0.0) or 0.0
        gap = signals_dict.get("gap", 0.0) or 0.0
        wavelet = signals_dict.get("wavelet", 0.0) or 0.0
        kalman = signals_dict.get("kalman", 0.0) or 0.0
        pairs = signals_dict.get("pairs", 0.0) or 0.0

        if momentum > 0.60 and mean_rev > 0.50:
            conflicts.append(
                f"Momentum ({momentum:.2f}) and mean reversion ({mean_rev:.2f}) conflict — "
                "trend-following vs reversal signal active simultaneously"
            )

        if gap > 0.60 and mean_rev > 0.55:
            conflicts.append(
                f"Gap signal ({gap:.2f}) conflicts with mean reversion ({mean_rev:.2f}) — "
                "directional gap vs regression-to-mean"
            )

        if abs(kalman) > 0.50 and pairs > 1.8:
            conflicts.append(
                f"Kalman directional bias ({kalman:.2f}) conflicts with pairs z-score ({pairs:.2f}) — "
                "consider which holds more weight in current regime"
            )

        if wavelet > 0.60 and mean_rev > 0.60:
            conflicts.append(
                f"Wavelet cycle ({wavelet:.2f}) and mean reversion ({mean_rev:.2f}) both elevated — "
                "verify cycle phase direction matches reversion signal"
            )

        return conflicts

    def build_trade_context(
        self,
        ticker: str,
        trade_data: dict,
        signals: dict,
        regime: str,
        news: list,
    ) -> dict:
        """
        Build a fully structured deterministic context dict for a trade.
        No LLM involved at this stage.
        """
        # Interpret each signal
        signal_interpretations = {}
        for sig_name, score in signals.items():
            signal_interpretations[sig_name] = self.interpret_signal(sig_name, score or 0.0)

        # Build signal stack summary
        signal_stack_summary = []
        for sig_name, interp in signal_interpretations.items():
            signal_stack_summary.append({
                "signal": sig_name,
                "score": interp["raw_score"],
                "strength": interp["strength"],
                "label": interp["label"],
            })

        # Sort by score descending
        signal_stack_summary.sort(key=lambda x: x["score"], reverse=True)

        # Detect conflicts
        conflicts = self.detect_conflicts(signals)

        # Regime interpretation
        regime_interp = self.interpret_regime(regime, trade_data.get("regime_confidence", 0.5))

        # Entry rationale — top signals
        top_signals = [s for s in signal_stack_summary if s["strength"] in ("strong", "moderate")]
        if top_signals:
            entry_rationale = (
                f"Entry driven by {', '.join(s['signal'] for s in top_signals[:3])} signal(s). "
                f"Regime: {regime} ({regime_interp['plain_english']})"
            )
        else:
            entry_rationale = f"Weak signal confluence — entry conditional. Regime: {regime}"

        # Filter pass reasons
        filter_reasons = []
        if trade_data.get("portfolio_correlation") is not None:
            corr = trade_data["portfolio_correlation"]
            filter_reasons.append({
                "filter": "Portfolio correlation",
                "value": round(corr, 3),
                "limit": 0.65,
                "passed": corr <= 0.65,
            })
        if trade_data.get("sector_exposure_pct") is not None:
            exp = trade_data["sector_exposure_pct"]
            filter_reasons.append({
                "filter": "Sector exposure",
                "value": round(exp, 1),
                "limit": 15.0,
                "passed": exp <= 15.0,
            })

        # Risk metrics
        risk_metrics = {
            "atr_stop": trade_data.get("atr_at_entry"),
            "trailing_stop_tier": trade_data.get("trailing_stop_tier"),
            "entry_timing_score": trade_data.get("entry_timing_score"),
            "vix_at_entry": trade_data.get("vix_level"),
            "signal_contradiction_score": trade_data.get("signal_contradiction_score"),
        }

        # News summary (max 3 headlines)
        news_summary = [
            {"headline": n.get("headline", ""), "sentiment": n.get("sentiment_raw", 0.0)}
            for n in (news or [])[:3]
        ]

        return {
            "ticker": ticker,
            "entry_rationale": entry_rationale,
            "signal_stack": signal_stack_summary,
            "conflicts_detected": conflicts,
            "filter_pass_reasons": filter_reasons,
            "regime": {
                "state": regime,
                "interpretation": regime_interp,
            },
            "risk_metrics_at_entry": risk_metrics,
            "news_context": news_summary,
            "trade_data": {
                "entry_date": trade_data.get("entry_date"),
                "entry_price": trade_data.get("entry_price"),
                "current_price": trade_data.get("current_price"),
                "pnl_pct": trade_data.get("pnl_pct"),
                "holding_days": trade_data.get("holding_days"),
                "sector": trade_data.get("sector"),
                "direction": trade_data.get("direction"),
            },
        }

    def explain_why_no_trade(self, scan_results: dict) -> str:
        """
        Pure deterministic logic: explain which filter blocked which ticker and why.
        scan_results: {ticker: {"filter": str, "value": any, "limit": any}, ...}
        """
        if not scan_results:
            return "No scan results available — filters may not have logged rejections."

        lines = ["**Why No Trade — Filter Analysis:**\n"]
        for ticker, result in scan_results.items():
            filter_name = result.get("filter", "unknown filter")
            value = result.get("value", "?")
            limit = result.get("limit", "?")
            reason = result.get("reason", "")
            lines.append(
                f"❌ **{ticker}**: blocked by {filter_name} "
                f"(value={value}, limit={limit})"
                + (f" — {reason}" if reason else "")
            )

        return "\n".join(lines)

    def build_deterministic_summary(self, context: dict) -> str:
        """
        Build a plain-English summary from Layer 1 context dict.
        Used as LLM fallback.
        """
        ticker = context.get("ticker", "?")
        entry_rationale = context.get("entry_rationale", "No rationale available")
        signals = context.get("signal_stack", [])
        conflicts = context.get("conflicts_detected", [])
        regime = context.get("regime", {})

        lines = [
            f"**{ticker} — Deterministic Analysis**\n",
            f"Entry rationale: {entry_rationale}\n",
        ]

        if signals:
            lines.append("Signal stack:")
            for s in signals[:5]:
                lines.append(
                    f"  — {s['signal'].upper()}: {s['score']:.2f} ({s['strength']}) — {s['label']}"
                )

        if conflicts:
            lines.append("\nConflicts detected:")
            for c in conflicts:
                lines.append(f"  ⚠️ {c}")

        regime_state = regime.get("state", "?")
        regime_interp = regime.get("interpretation", {})
        lines.append(f"\nRegime: {regime_state} — {regime_interp.get('plain_english', '')}")
        lines.append(f"Sizing: {regime_interp.get('sizing_impact', '')}")

        return "\n".join(lines)

    # ── Layer 2: LLM interpretation ────────────────────────────────────────────

    async def llm_explain(self, context: dict, question: Optional[str] = None) -> str:
        """
        Layer 2: Send structured context to Claude for plain-English explanation.
        The LLM only receives the context dict produced by Layer 1.
        Falls back to deterministic summary if API fails.
        """
        import time

        context_str = json.dumps(context, indent=2, default=str)
        cache_key = hashlib.sha256((context_str + (question or "")).encode()).hexdigest()[:32]

        # Check TTL cache
        now = __import__("time").monotonic()
        if cache_key in self._cache:
            if now - self._cache_ts.get(cache_key, 0) < self._cache_ttl:
                return self._cache[cache_key]

        if not self._api_key:
            logger.warning("ReasoningEngine: no Anthropic API key — using deterministic fallback")
            return self.build_deterministic_summary(context)

        system_prompt = (
            "You are Apollo's communication layer. You receive structured trade data produced by "
            "a deterministic quantitative system. Your only job is to translate numbers and labels "
            "into clear, plain English for the portfolio manager. You must never suggest trades, "
            "modify any values, question signal weights, or express opinions on whether a trade "
            "is good or bad. Output exactly what the data says, in plain English only."
        )

        user_prompt = f"Explain this trade context in plain English:\n{context_str}"
        if question:
            user_prompt += f"\n\nThe user also asks: {question}"

        try:
            import anthropic

            async def _call():
                client = anthropic.AsyncAnthropic(api_key=self._api_key)
                msg = await client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=600,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                return msg.content[0].text if msg.content else ""

            from monitoring.rate_limiter import RateLimiter
            rl = RateLimiter()
            result = await rl.call_with_retry(
                "anthropic",
                _call,
                cached_value=self.build_deterministic_summary(context),
                endpoint="messages.create",
            )

            if result:
                import time as _t
                self._cache[cache_key] = result
                self._cache_ts[cache_key] = _t.monotonic()
                return result
            else:
                return self.build_deterministic_summary(context)

        except ImportError:
            logger.warning("ReasoningEngine: anthropic package not installed — using fallback")
            return self.build_deterministic_summary(context)
        except Exception as e:
            logger.error(f"ReasoningEngine: LLM call failed — {e}")
            return self.build_deterministic_summary(context)
