"""
Post-Earnings Announcement Drift (PEAD) signal.

Long (short) after positive (negative) earnings surprise with volume confirmation.
Holding period and thresholds are driven by config.

Signal quality rules:
  1. Data quality gate   — only use events where BOTH epsActual AND epsEstimate
                           are present; epsDifference-only events are discarded.
  2. Day1 direction check — enter on day +2 only if day +1 close moved in the
                            signal direction vs earnings-day close (any positive
                            move for longs, any negative move for shorts).
  3. Dynamic holding period — 30/20/12 days based on surprise magnitude:
                              >20% → 30d, 10-20% → 20d, 8-10% → 12d.
  4. Combined threshold gate — |surprise| ≥ threshold (config, default 8%) AND
                               (zscore > 0.5 OR no zscore yet).
"""
import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Dynamic holding thresholds by absolute surprise magnitude
_HOLD_LONG_ABS  = 0.20     # > 20% → 30 days
_HOLD_MID_ABS   = 0.10     # 10-20% → 20 days
# 8-10% → 12 days (default; below this the threshold gate already filtered)

# Combined threshold gate
_SURPRISE_THRESHOLD = 0.08  # 8% minimum (config overrides this)
_ZSCORE_GATE        = 0.5   # zscore must exceed this (or be NaN for early history)


class PEADSignal:
    def __init__(self, config: dict):
        self.config = config
        cfg = config["signal"]["pead"]
        self.surprise_threshold = cfg.get("earnings_surprise_threshold", _SURPRISE_THRESHOLD)
        self.volume_multiplier  = cfg.get("volume_surge_multiplier", 1.3)
        self.holding_period     = cfg.get("holding_period_days", 20)
        self.zscore_window      = cfg.get("zscore_window", 60)

    def generate(
        self,
        ticker: str,
        price_data: pd.DataFrame,
        earnings_history: pd.DataFrame = None,
        earnings_data: list = None,
    ) -> pd.DataFrame:
        """
        earnings_data: optional pre-fetched list of dicts (from EarningsCache).
            Each dict must have keys: date, epsActual, epsEstimate.
            If supplied, earnings_history is built from this list and the
            per-ticker yfinance/Finnhub call is skipped entirely.
        """
        # ── Convert earnings_data list → DataFrame (bypasses API call) ─
        if earnings_data is not None and earnings_history is None:
            if earnings_data:
                try:
                    rows = []
                    for rec in earnings_data:
                        date_val = rec.get("date", "")
                        if not date_val:
                            continue
                        rows.append({
                            "date": pd.to_datetime(date_val),
                            "epsActual": rec.get("epsActual"),
                            "epsEstimate": rec.get("epsEstimate"),
                            "epsDifference": (
                                float(rec.get("epsActual", 0) or 0) -
                                float(rec.get("epsEstimate", 0) or 0)
                            ) if rec.get("epsActual") is not None else None,
                            "surprisePercent": None,
                        })
                    if rows:
                        df_built = pd.DataFrame(rows).set_index("date").sort_index()
                        earnings_history = df_built
                    else:
                        earnings_history = pd.DataFrame()
                except Exception as _e:
                    logger.debug("earnings_data conversion failed for %s: %s", ticker, _e)
                    earnings_history = pd.DataFrame()
            else:
                earnings_history = pd.DataFrame()

        if earnings_history is None:
            earnings_history = pd.DataFrame()
        """
        Returns DataFrame with columns:
          [ticker, signal, entry_date, exit_date, earnings_date,
           surprise_pct, surprise_zscore, holding_days, data_quality]
        signal: +1 long, -1 short
        """
        if price_data.empty or earnings_history.empty:
            return pd.DataFrame()

        n_total = n_quality = n_threshold = n_volume = n_momentum = 0

        records = []
        for date, row in earnings_history.iterrows():
            n_total += 1

            # ── 1. Data quality gate ─────────────────────────────────
            surprise, quality = self._calc_surprise(row, ticker=ticker)
            if surprise is None:
                continue
            if quality == "low":
                # epsDifference-only: no analyst estimate available — skip
                logger.debug(
                    "%s @ %s: dropped — low quality (epsDiff only, no estimate)",
                    ticker, date.date(),
                )
                continue
            n_quality += 1

            # ── 4. Combined threshold + zscore gate (zscore added after loop) ─
            if abs(surprise) < self.surprise_threshold:
                continue
            n_threshold += 1

            # ── Volume confirmation ──────────────────────────────────
            if not self._volume_surge(price_data, date):
                continue
            n_volume += 1

            direction = 1 if surprise > 0 else -1

            # ── 2. Momentum confirmation (day +3) ────────────────────
            entry = self._momentum_entry(price_data, date, direction)
            if entry is None:
                continue
            n_momentum += 1

            records.append(
                {
                    "ticker":        ticker,
                    "signal":        direction,
                    "entry_date":    entry,
                    "exit_date":     None,   # filled after z-score
                    "earnings_date": date,
                    "surprise_pct":  surprise,
                    "data_quality":  quality,
                }
            )

        logger.info(
            "%s: %d events → quality=%d threshold=%d volume=%d momentum=%d signals",
            ticker, n_total, n_quality, n_threshold, n_volume, n_momentum,
        )

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records).sort_values("earnings_date").reset_index(drop=True)

        # ── Z-score (needs sorted history) ──────────────────────────
        df = self._add_zscore(df)

        # ── 4b. Z-score gate now that scores are available ───────────
        df = df[
            df["surprise_zscore"].isna()
            | (df["surprise_zscore"].abs() > _ZSCORE_GATE)
        ].reset_index(drop=True)

        if df.empty:
            return pd.DataFrame()

        # ── 3. Dynamic holding period + exit date ────────────────────
        df["holding_days"] = df.apply(
            lambda r: self._dynamic_hold(r["surprise_zscore"], r["surprise_pct"]), axis=1
        )
        df["exit_date"] = df.apply(
            lambda r: self._nth_trading_day(
                price_data.index, r["entry_date"], int(r["holding_days"])
            ),
            axis=1,
        )

        # Apply insider multipliers if enabled
        if self.config.get("signal", {}).get("pead", {}).get("use_insider_signals", True):
            df = self.apply_insider_multipliers(df, ticker)

        return df

    # ------------------------------------------------------------------
    # Insider Multipliers
    # ------------------------------------------------------------------

    def apply_insider_multipliers(
        self, signals_df: "pd.DataFrame", ticker: str
    ) -> "pd.DataFrame":
        """Apply insider signal multipliers and generate cluster signals."""
        # Lazy import to avoid circular deps
        try:
            from analysis.insider_analyser import InsiderAnalyser
            ia = InsiderAnalyser()
        except Exception:
            return signals_df

        if signals_df.empty:
            return signals_df

        enhanced_rows = []
        for _, row in signals_df.iterrows():
            earnings_date = str(row.get("earnings_date", ""))[:10]
            direction = int(row.get("signal", 1))

            # Only apply to long signals (direction > 0)
            if direction <= 0:
                enhanced_rows.append(row.to_dict())
                continue

            try:
                multiplier, reason, extra_signals = ia.get_pead_multiplier(
                    ticker, earnings_date=earnings_date, price_change_30d=0.0
                )
            except Exception:
                enhanced_rows.append(row.to_dict())
                continue

            if multiplier == 0.0:
                # Suppressed by cluster selling
                logger.info("%s: PEAD long suppressed by %s", ticker, reason)
                continue  # don't add this signal

            new_row = row.to_dict()
            new_row["surprise_pct"]       = float(row.get("surprise_pct", 0)) * multiplier
            new_row["insider_multiplier"] = multiplier
            new_row["insider_reason"]     = reason

            if multiplier >= 1.8:
                new_row["conviction"] = "HIGH_CONVICTION"
                logger.info(
                    "%s: HIGH_CONVICTION insider+PEAD signal (%.1fx)", ticker, multiplier
                )
            elif multiplier >= 1.6:
                new_row["conviction"] = "CLUSTER_BUY"

            enhanced_rows.append(new_row)

        if not enhanced_rows:
            return pd.DataFrame()
        return pd.DataFrame(enhanced_rows)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _calc_surprise(self, row, ticker: str = ""):
        """
        Return (surprise_fraction, quality) where quality is:
          'high'  — both epsActual and epsEstimate present
          'low'   — estimate missing; epsDifference-only (should be skipped for backtesting)
          None    — all fields absent; returns (None, None)
        """
        def _f(key):
            v = row.get(key)
            try:
                f = float(v)
                return None if np.isnan(f) else f
            except (TypeError, ValueError):
                return None

        actual   = _f("epsActual")
        estimate = _f("epsEstimate")

        # 1. Pre-computed surprisePercent — only trust it when we also have
        #    both actual and estimate (otherwise it may be a stale/cached value).
        #    EarningsCalendar.get_earnings_surprise() already normalises to a
        #    fraction (e.g. 0.1676 for a 16.76% beat), so no further division needed.
        pct = _f("surprisePercent")
        if pct is not None and actual is not None and estimate is not None:
            logger.debug("%s path=1 (surprisePercent+verify) → %.4f", ticker, pct)
            return pct, "high"   # already a fraction from EarningsCalendar

        # 2. Direct calculation — requires BOTH fields, non-zero estimate
        if actual is not None and estimate is not None and estimate != 0:
            result = (actual - estimate) / abs(estimate)
            logger.debug("%s path=2 (actual/estimate) → %.4f", ticker, result)
            return result, "high"

        # No clean actual+estimate pair — skip entirely.
        # epsDifference-only events have no analyst consensus and are discarded.
        logger.debug("%s path=None: no clean actual+estimate EPS pair", ticker)
        return None, None

    def _volume_surge(self, price_data: pd.DataFrame, earnings_date) -> bool:
        try:
            window = price_data.loc[:earnings_date]["volume"]
            if len(window) < 6:
                return False
            avg = window.iloc[:-1].tail(20).mean()
            return window.iloc[-1] >= self.volume_multiplier * avg
        except Exception:
            return False

    def _momentum_entry(
        self,
        price_data: pd.DataFrame,
        earnings_date,
        direction: int,
    ) -> Optional[pd.Timestamp]:
        """
        Day1 direction check: enter on day +2 only if day +1 close moved in
        the signal direction vs the earnings-day close (any positive move for
        longs, any negative move for shorts — no minimum threshold).

        Falls back to day +1 entry when insufficient post-earnings data.
        """
        try:
            t0_dates = price_data.index[price_data.index <= earnings_date]
            post     = price_data.index[price_data.index > earnings_date]

            if len(post) == 0:
                return None
            if len(post) < 2 or len(t0_dates) == 0:
                # Not enough history — fall back to day +1 entry
                return post[0]

            t0_close   = float(price_data.loc[t0_dates[-1], "close"])
            day1_close = float(price_data.loc[post[0], "close"])

            confirmed = (
                (direction == 1  and day1_close > t0_close) or
                (direction == -1 and day1_close < t0_close)
            )

            if confirmed:
                return post[1]   # enter on day +2

            logger.debug(
                "Day1 check failed: direction=%+d t0=%.2f day1=%.2f",
                direction, t0_close, day1_close,
            )
            return None
        except Exception:
            return None

    def _dynamic_hold(
        self, zscore: Optional[float], surprise_pct: float
    ) -> int:
        """Return holding period in days based on absolute surprise magnitude."""
        ab = abs(surprise_pct)
        if ab > _HOLD_LONG_ABS:
            return 30
        if ab > _HOLD_MID_ABS:
            return 20
        return 12

    def _add_zscore(self, df: pd.DataFrame) -> pd.DataFrame:
        sp = df["surprise_pct"]
        roll_mean = sp.rolling(self.zscore_window, min_periods=5).mean()
        roll_std  = sp.rolling(self.zscore_window, min_periods=5).std()
        df["surprise_zscore"] = (sp - roll_mean) / (roll_std + 1e-8)
        return df

    def _next_trading_day(
        self, index: pd.DatetimeIndex, date
    ) -> Optional[pd.Timestamp]:
        future = index[index > date]
        return future[0] if len(future) > 0 else None

    def _nth_trading_day(
        self, index: pd.DatetimeIndex, start: pd.Timestamp, n: int
    ) -> Optional[pd.Timestamp]:
        future = index[index > start]
        if len(future) == 0:
            return None
        return future[n - 1] if len(future) >= n else future[-1]

    # ------------------------------------------------------------------
    # Context multipliers (macro + altdata + shipping + analyst)
    # ------------------------------------------------------------------

    def apply_context_multipliers(
        self,
        signals: pd.DataFrame,
        ticker: str,
        sector: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply context-aware size multipliers to a signals DataFrame.
        Modifies (or adds) a 'size_multiplier' column in-place.

        Multiplier sources (all gracefully degraded):
          1. EarningsContextScore from macro_signal_engine
             ≥ 0.70 → ×1.20 | ≤ 0.40 → ×0.70
          2. Altdata confluence score from closeloop store
             ≥ 0.80 → ×1.30 | ≤ 0.30 → suppress (×0.0)
          3. Shipping stress index (retailer sectors only)
             SSI > 1.5 AND sector in retail/consumer → ×0.70
          4. Analyst revision tracker
             positive revision → ×1.20 | negative → ×0.80
        """
        if signals.empty:
            return signals

        if "size_multiplier" not in signals.columns:
            signals = signals.copy()
            signals["size_multiplier"] = 1.0

        mult = 1.0

        # ── 1. EarningsContextScore ────────────────────────────────────
        try:
            from analysis.macro_signal_engine import MacroSignalEngine
            mse = MacroSignalEngine()
            ctx = mse.get_earnings_context(ticker=ticker, sector=sector or "unknown")
            score = getattr(ctx, "composite_score", None)
            if score is not None:
                if score >= 0.70:
                    mult *= 1.20
                elif score <= 0.40:
                    mult *= 0.70
        except Exception as exc:
            logger.debug("apply_context_multipliers: EarningsContextScore failed: %s", exc)

        # ── 2. Altdata confluence score ────────────────────────────────
        try:
            from closeloop.storage.closeloop_store import CloseloopStore
            store = CloseloopStore()
            confluence = store.get_altdata_confluence(ticker)
            if confluence is not None:
                if confluence >= 0.80:
                    mult *= 1.30
                elif confluence <= 0.30:
                    mult = 0.0   # suppress signal
        except Exception as exc:
            logger.debug("apply_context_multipliers: altdata confluence failed: %s", exc)

        # ── 3. Shipping stress index (consumer/retail sectors) ─────────
        _retail_sectors = {"consumer", "retail", "consumer discretionary",
                           "consumer staples", "e-commerce"}
        if sector and sector.lower() in _retail_sectors:
            try:
                import sqlite3
                from pathlib import Path
                hist_db = Path(__file__).resolve().parents[1] / "output" / "historical_db.db"
                with sqlite3.connect(hist_db) as conn:
                    row = conn.execute("""
                        SELECT value FROM shipping_data
                        WHERE metric='ShippingStressIndex'
                        ORDER BY date DESC LIMIT 1
                    """).fetchone()
                    if row and float(row[0]) > 1.5:
                        mult *= 0.70
            except Exception as exc:
                logger.debug("apply_context_multipliers: SSI check failed: %s", exc)

        # ── 4. Analyst revision tracker ────────────────────────────────
        try:
            from closeloop.context.analyst_revision_tracker import AnalystRevisionTracker
            tracker = getattr(self, "_analyst_tracker", None) or AnalystRevisionTracker()
            revision = tracker.get_revision_direction(ticker)
            if revision == "positive":
                mult *= 1.20
            elif revision == "negative":
                mult *= 0.80
        except Exception as exc:
            logger.debug("apply_context_multipliers: analyst revision failed: %s", exc)

        # ── Apply ──────────────────────────────────────────────────────
        if mult != 1.0:
            signals["size_multiplier"] = signals["size_multiplier"] * mult
            logger.info(
                "apply_context_multipliers: %s composite_mult=%.2f (sector=%s)",
                ticker, mult, sector,
            )

        return signals
