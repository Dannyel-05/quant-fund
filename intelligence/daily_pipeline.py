"""
Phase 11: Intelligence Daily Pipeline

Three scheduled jobs:
  Morning prep   — 7:00 AM ET (weekdays): refresh large-cap events,
                   readthrough signals, pre-market snapshot
  Market close   — 5:00 PM ET (weekdays): collect outcomes, run intelligence
                   engine update, update company profiles
  Weekly deep    — Sunday 3:00 AM: full pattern learning, historical
                   coefficient recompute, generate full report

Also provides CLI-callable run functions for manual execution.

Usage:
    from intelligence.daily_pipeline import DailyPipeline
    pipeline = DailyPipeline(config)
    pipeline.run_morning()
    pipeline.run_close()
    pipeline.run_weekly()
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class DailyPipeline:
    """
    Orchestrates all intelligence pipeline jobs.
    Each method is independently callable for manual runs or cron scheduling.
    """

    def __init__(self, config: dict):
        self.config = config
        self._earnings_db   = None
        self._intel_db      = None
        self._hist_db       = None
        self._engine        = None
        self._influence     = None

    # ------------------------------------------------------------------
    # Lazy initialization
    # ------------------------------------------------------------------

    def _get_earnings_db(self):
        if self._earnings_db is None:
            from data.earnings_db import EarningsDB
            self._earnings_db = EarningsDB(
                self.config.get("earnings_db_path", "output/earnings.db")
            )
        return self._earnings_db

    def _get_intel_db(self):
        if self._intel_db is None:
            from analysis.intelligence_db import IntelligenceDB
            self._intel_db = IntelligenceDB(
                self.config.get("intelligence_db_path", "output/intelligence_db.db")
            )
        return self._intel_db

    def _get_hist_db(self):
        if self._hist_db is None:
            try:
                from data.historical_db import HistoricalDB
                self._hist_db = HistoricalDB(
                    self.config.get("historical_db_path", "output/historical_db.db")
                )
            except Exception as e:
                logger.warning("HistoricalDB unavailable: %s", e)
        return self._hist_db

    def _get_engine(self):
        if self._engine is None:
            from analysis.intelligence_engine import IntelligenceEngine
            self._engine = IntelligenceEngine(
                self._get_intel_db(),
                self._get_earnings_db(),
                self._get_hist_db(),
            )
        return self._engine

    def _get_influence_engine(self):
        if self._influence is None:
            try:
                from data.large_cap_influence import LargeCapInfluenceEngine
                self._influence = LargeCapInfluenceEngine(
                    self._get_hist_db(),
                    self._get_earnings_db(),
                )
            except Exception as e:
                logger.warning("LargeCapInfluenceEngine unavailable: %s", e)
        return self._influence

    def _get_universe(self) -> List[str]:
        """Load universe tickers from config or CSV."""
        from pathlib import Path
        _root = Path(__file__).resolve().parents[1]
        candidates = [
            self.config.get("universe_path", ""),
            "data/universe_us_tier1.csv",
            "data/universe_us.csv",
        ]
        for rel in candidates:
            if not rel:
                continue
            path = _root / rel
            if not path.exists():
                continue
            try:
                lines = path.read_text().splitlines()
                tickers = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Handle CSV with or without header
                    col = line.split(",")[0].strip().upper()
                    if col in ("TICKER", "SYMBOL", ""):
                        continue  # skip header row
                    tickers.append(col)
                if tickers:
                    logger.info("Universe: %d tickers from %s", len(tickers), rel)
                    return tickers
            except Exception as e:
                logger.warning("Could not load universe from %s: %s", rel, e)
        logger.warning("No universe file loaded — morning prep will be limited")
        return []

    # ------------------------------------------------------------------
    # Morning prep (7 AM ET, weekdays)
    # ------------------------------------------------------------------

    def run_morning(self) -> dict:
        """
        Morning preparation:
        1. Fetch recent large-cap earnings events (last 3 days)
        2. Compute readthrough signals for universe tickers
        3. Snapshot pre-earnings setups for tickers reporting today/tomorrow
        4. Print morning briefing
        5. Run macro briefing
        """
        logger.info("=== MORNING PREP: %s ===", datetime.now().strftime("%Y-%m-%d %H:%M"))
        summary = {"readthrough_signals": 0, "snapshots": 0, "errors": 0}

        universe = self._get_universe()
        if not universe:
            logger.warning("Empty universe — morning prep aborted")
            return summary

        # 1. Readthrough signals
        influence = self._get_influence_engine()
        readthrough_signals = []
        if influence:
            try:
                logger.info("Computing readthrough signals for %d tickers", len(universe))
                readthrough_signals = influence.get_readthrough_signals(universe, days_lookback=3)
                summary["readthrough_signals"] = len(readthrough_signals)
                logger.info("Readthrough: %d signals generated", len(readthrough_signals))

                # Store in intelligence_db cross_asset_correlations
                intel_db = self._get_intel_db()
                now_str = datetime.now(timezone.utc).isoformat()
                for rs in readthrough_signals:
                    if not rs.get("large_cap_events"):
                        continue
                    for ev in rs.get("large_cap_events", [])[:3]:
                        try:
                            intel_db.upsert_correlation({
                                "asset_a":          ev["ticker"],
                                "asset_b":          rs["peer_ticker"],
                                "relationship_type": "readthrough",
                                "correlation":      rs.get("readthrough_score"),
                                "lead_lag_days":    1,
                                "n_events":         rs.get("n_events", 0),
                                "p_value":          None,
                                "sector":           rs.get("sector"),
                                "sub_sector":       None,
                                "computed_at":      now_str,
                            })
                        except Exception:
                            pass
            except Exception as e:
                logger.error("Readthrough computation failed: %s", e)
                summary["errors"] += 1

        # 2. Pre-earnings snapshots for upcoming reporters
        try:
            earnings_db = self._get_earnings_db()
            upcoming = earnings_db.get_upcoming_calendar(days_ahead=2)
            logger.info("Upcoming earnings (next 2 days): %d tickers", len(upcoming))
            for entry in upcoming:
                ticker = entry["ticker"]
                try:
                    self._take_pre_earnings_snapshot(ticker, entry["earnings_date"])
                    summary["snapshots"] += 1
                except Exception as e:
                    logger.warning("Snapshot failed %s: %s", ticker, e)
                    summary["errors"] += 1
        except Exception as e:
            logger.error("Pre-earnings snapshot phase failed: %s", e)
            summary["errors"] += 1

        # 3. Print morning briefing
        self._print_morning_briefing(readthrough_signals)

        # 4. Run macro briefing
        try:
            self.run_macro_briefing()
        except Exception as e:
            logger.error("Macro briefing failed in run_morning: %s", e)
            summary["errors"] += 1

        logger.info("Morning prep complete: %s", summary)
        return summary

    def _take_pre_earnings_snapshot(self, ticker: str, earnings_date: str) -> None:
        """Take a full pre-earnings snapshot for a ticker."""
        try:
            import main as m
            config = self.config
            m.cmd_pead_snapshot(config, ticker, earnings_date)
        except Exception as e:
            logger.warning("cmd_pead_snapshot failed for %s: %s", ticker, e)

    def _print_morning_briefing(self, readthrough_signals: list) -> None:
        """Print formatted morning briefing to stdout."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"\n{'='*60}")
        print(f"  MORNING INTELLIGENCE BRIEFING — {now}")
        print(f"{'='*60}")

        bull_signals = [s for s in readthrough_signals if s.get("readthrough_score", 0) > 0.2]
        bear_signals = [s for s in readthrough_signals if s.get("readthrough_score", 0) < -0.2]

        if bull_signals:
            print(f"\n  BULLISH READTHROUGH ({len(bull_signals)} tickers):")
            for s in sorted(bull_signals, key=lambda x: x["readthrough_score"], reverse=True)[:5]:
                print(f"    {s['peer_ticker']:<6s}  score={s['readthrough_score']:+.3f}  "
                      f"n_events={s['n_events']}  sector={s.get('sector','?')}")

        if bear_signals:
            print(f"\n  BEARISH READTHROUGH ({len(bear_signals)} tickers):")
            for s in sorted(bear_signals, key=lambda x: x["readthrough_score"])[:5]:
                print(f"    {s['peer_ticker']:<6s}  score={s['readthrough_score']:+.3f}  "
                      f"n_events={s['n_events']}  sector={s.get('sector','?')}")

        if not bull_signals and not bear_signals:
            print("\n  No significant readthrough signals today.")
        print()

    # ------------------------------------------------------------------
    # Market close (5 PM ET, weekdays)
    # ------------------------------------------------------------------

    def run_close(self) -> dict:
        """
        Market close update:
        1. Capture post-earnings outcomes for snapshots > 1 day old
        2. Run intelligence engine update (patterns, signals, profiles)
        3. Enrich historical earnings observations
        4. Update signal effectiveness records
        """
        logger.info("=== MARKET CLOSE: %s ===", datetime.now().strftime("%Y-%m-%d %H:%M"))
        summary = {"outcomes_captured": 0, "intelligence_patterns": 0, "profiles": 0, "errors": 0}

        # 1. Capture outcomes for past snapshots
        try:
            outcomes = self._capture_pending_outcomes()
            summary["outcomes_captured"] = outcomes
        except Exception as e:
            logger.error("Outcome capture failed: %s", e)
            summary["errors"] += 1

        # 2. Intelligence engine run
        try:
            engine = self._get_engine()
            run_summary = engine.run()
            summary["intelligence_patterns"] = run_summary.get("patterns", 0)
            summary["profiles"] = run_summary.get("profiles_updated", 0)
            logger.info("Intelligence engine: %s", run_summary)
        except Exception as e:
            logger.error("Intelligence engine run failed: %s", e)
            summary["errors"] += 1

        logger.info("Market close complete: %s", summary)
        return summary

    def _capture_pending_outcomes(self) -> int:
        """
        For each pre-earnings snapshot where earnings have passed but outcomes
        are not yet captured, compute and store returns.
        """
        import yfinance as yf
        from datetime import timedelta

        earnings_db = self._get_earnings_db()
        today = datetime.now().strftime("%Y-%m-%d")
        captured = 0

        # Get snapshots from past 30 days that need outcomes
        snapshots = earnings_db.get_all_snapshots(days_ahead=0)  # returns empty (future-only)
        # Also check earnings_observations for missing returns
        recent_obs = earnings_db.get_observations(since=(
            datetime.now() - __import__("datetime").timedelta(days=30)
        ).strftime("%Y-%m-%d"), limit=200)

        for obs in recent_obs:
            if obs.get("return_t5") is not None:
                continue  # already have it
            ticker = obs.get("ticker")
            edate  = obs.get("earnings_date")
            if not ticker or not edate:
                continue
            try:
                from datetime import timedelta as _td
                ed = datetime.strptime(edate, "%Y-%m-%d")
                if (datetime.now() - ed).days < 5:
                    continue  # too early for t+5
                end_date = (ed + _td(days=30)).strftime("%Y-%m-%d")
                hist = yf.download(ticker, start=edate, end=end_date,
                                   auto_adjust=True, progress=False)
                if hist is None or len(hist) < 2:
                    continue
                closes = hist["Close"].dropna().values.flatten()
                p0 = float(closes[0]) if len(closes) > 0 else None
                if p0 is None or p0 == 0:
                    continue
                updates = {}
                for offset, col in [(1, "return_t1"), (3, "return_t3"),
                                    (5, "return_t5"), (10, "return_t10"), (20, "return_t20")]:
                    if len(closes) > offset:
                        updates[col] = round(float(closes[offset]) / p0 - 1, 6)
                if updates:
                    from data.earnings_db import EarningsDB
                    # Direct update via upsert_observation
                    record = {"ticker": ticker, "earnings_date": edate,
                              "collected_at": datetime.now().isoformat()}
                    record.update(updates)
                    earnings_db.upsert_observation(record)
                    captured += 1
            except Exception as e:
                logger.debug("Outcome capture failed %s@%s: %s", ticker, edate, e)

        logger.info("Captured %d pending outcomes", captured)
        return captured

    # ------------------------------------------------------------------
    # Weekly deep analysis (Sunday 3 AM)
    # ------------------------------------------------------------------

    def run_weekly(self) -> dict:
        """
        Weekly deep analysis:
        1. Recompute all historical readthrough coefficients
        2. Full pattern learning with extended dataset
        3. Generate comprehensive report
        4. Save report to output/reports/
        """
        logger.info("=== WEEKLY DEEP ANALYSIS: %s ===", datetime.now().strftime("%Y-%m-%d %H:%M"))
        summary = {"coefficients": 0, "patterns": 0, "report_path": None, "errors": 0}

        universe = self._get_universe()

        # 1. Recompute readthrough coefficients
        influence = self._get_influence_engine()
        if influence and universe:
            try:
                logger.info("Recomputing readthrough coefficients...")
                n_coeffs = influence.update_all_coefficients(universe, start_date="2010-01-01")
                summary["coefficients"] = n_coeffs

                # Persist to intelligence_db
                intel_db = self._get_intel_db()
                now_str = datetime.now(timezone.utc).isoformat()
                for cache_key, val in influence._coeff_cache.items():
                    large, peer = cache_key.split(":", 1)
                    try:
                        intel_db.upsert_readthrough_coeff({
                            "large_ticker": large,
                            "peer_ticker":  peer,
                            "coeff":        val.get("coeff"),
                            "correlation":  val.get("corr"),
                            "n_events":     val.get("n_events"),
                            "start_date":   "2010-01-01",
                            "end_date":     datetime.now().strftime("%Y-%m-%d"),
                            "computed_at":  now_str,
                        })
                    except Exception:
                        pass
            except Exception as e:
                logger.error("Coefficient recompute failed: %s", e)
                summary["errors"] += 1

        # 2. Full intelligence run
        try:
            engine = self._get_engine()
            run_summary = engine.run()
            summary["patterns"] = run_summary.get("patterns", 0)
        except Exception as e:
            logger.error("Weekly intelligence run failed: %s", e)
            summary["errors"] += 1

        # 3. Generate and save report
        try:
            engine = self._get_engine()
            report = engine.generate_report()
            report_dir = Path("output/reports")
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / f"intelligence_{datetime.now():%Y%m%d}.txt"
            report_path.write_text(report)
            summary["report_path"] = str(report_path)
            logger.info("Report saved to %s", report_path)
            print(report)
        except Exception as e:
            logger.error("Report generation failed: %s", e)
            summary["errors"] += 1

        logger.info("Weekly deep analysis complete: %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return current pipeline status and DB stats."""
        result = {"pipeline": "ok", "timestamp": datetime.now().isoformat()}

        try:
            result["earnings_db"] = self._get_earnings_db().status()
        except Exception as e:
            result["earnings_db"] = {"error": str(e)}

        try:
            result["intelligence_db"] = self._get_intel_db().status()
        except Exception as e:
            result["intelligence_db"] = {"error": str(e)}

        try:
            influence = self._get_influence_engine()
            if influence:
                result["influence_engine"] = influence.summary()
        except Exception as e:
            result["influence_engine"] = {"error": str(e)}

        return result

    # ------------------------------------------------------------------
    # Macro state helper (used by run_macro_briefing)
    # ------------------------------------------------------------------

    def get_macro_state(self) -> dict:
        """
        Returns a dict with current macro state from all available collectors.
        Keys: regime, pead_multiplier, shipping_stress, consumer_health,
              yield_curve_slope, is_inverted, geopolitical_risk.
        All values default to None if unavailable.
        """
        state = {
            "regime":             None,
            "pead_multiplier":    None,
            "shipping_stress":    None,
            "consumer_health":    None,
            "yield_curve_slope":  None,
            "is_inverted":        None,
            "geopolitical_risk":  None,
        }

        # Macro signal engine — regime + PEAD multiplier
        try:
            from analysis.macro_signal_engine import MacroSignalEngine
            mse = MacroSignalEngine()  # loads config internally
            ms = mse.run_full_analysis()
            state["regime"]           = ms.regime
            state["pead_multiplier"]  = ms.pead_multiplier
            state["yield_curve_slope"] = ms.yield_curve_slope
            state["is_inverted"]      = ms.is_inverted
            state["shipping_stress"]  = ms.shipping_stress
            state["consumer_health"]  = ms.consumer_health
            state["geopolitical_risk"] = ms.geopolitical_risk_level
        except Exception as e:
            logger.debug("MacroSignalEngine unavailable: %s", e)

        # Shipping stress fallback
        if state["shipping_stress"] is None:
            try:
                from data.collectors.shipping_intelligence import ShippingIntelligence
                si = ShippingIntelligence()
                state["shipping_stress"] = si.get_current_stress()
            except Exception as e:
                logger.debug("ShippingIntelligence unavailable: %s", e)

        # Rates / credit collector fallback
        if state["yield_curve_slope"] is None:
            try:
                from data.collectors.rates_credit_collector import RatesCreditCollector
                rc = RatesCreditCollector()
                yc = rc.get_yield_curve_status()
                if yc:
                    state["yield_curve_slope"] = yc.get("slope")
                    state["is_inverted"]       = yc.get("is_inverted")
            except Exception as e:
                logger.debug("RatesCreditCollector unavailable: %s", e)

        # Consumer intelligence fallback
        if state["consumer_health"] is None:
            try:
                from data.collectors.consumer_intelligence import ConsumerIntelligence
                ci = ConsumerIntelligence()
                state["consumer_health"] = ci.get_consumer_health_index()
            except Exception as e:
                logger.debug("ConsumerIntelligence unavailable: %s", e)

        # Geopolitical risk fallback
        if state["geopolitical_risk"] is None:
            try:
                from data.collectors.geopolitical_collector import GeopoliticalCollector
                gc = GeopoliticalCollector()
                state["geopolitical_risk"] = gc.get_current_risk_level()
            except Exception as e:
                logger.debug("GeopoliticalCollector unavailable: %s", e)

        return state

    # ------------------------------------------------------------------
    # Macro briefing
    # ------------------------------------------------------------------

    def run_macro_briefing(self, save_to_file: bool = True) -> str:
        """
        Generates and optionally saves a complete macro briefing.
        Returns the briefing string. Never crashes — all errors are logged.
        """
        now_dt   = datetime.now()
        date_str = now_dt.strftime("%Y-%m-%d")
        time_str = now_dt.strftime("%H:%M")
        date_tag = now_dt.strftime("%Y%m%d")

        # ── Collect data from all available modules ────────────────────
        macro_state = {}
        try:
            macro_state = self.get_macro_state()
        except Exception as e:
            logger.error("get_macro_state failed: %s", e)

        regime          = macro_state.get("regime") or "UNKNOWN"
        pead_mult       = macro_state.get("pead_multiplier")
        shipping_stress = macro_state.get("shipping_stress")
        consumer_health = macro_state.get("consumer_health")
        yc_slope        = macro_state.get("yield_curve_slope")
        is_inverted     = macro_state.get("is_inverted")
        geo_risk        = macro_state.get("geopolitical_risk")

        # ── Shipping details ──────────────────────────────────────────
        bdi_value      = "N/A"
        bdi_change     = "N/A"
        shipping_level = "UNKNOWN"
        shipping_sector_impact = "No shipping data available"
        try:
            import sqlite3 as _sql
            hconn = _sql.connect("output/historical_db.db")
            rows = hconn.execute(
                "SELECT date, bdi_value, bdi_roc_1w, shipping_stress_index, stress_regime FROM shipping_data ORDER BY date DESC LIMIT 8"
            ).fetchall()
            hconn.close()
            if rows:
                latest = rows[0]
                bdi_value = f"{latest[1]:.1f} (PROXY)"
                roc_1w = latest[2]
                bdi_change = f"{roc_1w*100:+.1f}%" if roc_1w is not None else "N/A"
                ssi_val = latest[3]
                shipping_level = latest[4] or ("HIGH" if ssi_val > 1.5 else ("LOW" if ssi_val < -1.5 else "NEUTRAL"))
                shipping_sector_impact = f"SSI={ssi_val:.2f} — Importers/retailers at risk if SSI>1.5"
        except Exception as e:
            logger.debug("Shipping DB read failed: %s", e)
        try:
            from data.collectors.shipping_intelligence import ShippingIntelligence
            si = ShippingIntelligence()
            ssi = si.get_current_stress()
            if ssi is not None and shipping_level == "UNKNOWN":
                shipping_level = "HIGH" if ssi > 1.5 else ("LOW" if ssi < -1.5 else "NEUTRAL")
                impacts = si.get_sector_impacts(ssi)
                if impacts:
                    top = sorted(impacts.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                    shipping_sector_impact = "; ".join(f"{k}: {v:+.1f}x" for k, v in top)
        except Exception as e:
            logger.debug("ShippingIntelligence live failed: %s", e)

        # ── Weather extreme events ────────────────────────────────────
        weather_lines = []
        try:
            from data.collectors.geographic_intelligence import GeographicIntelligence
            gi = GeographicIntelligence()
            extremes = gi.get_extreme_events(threshold=2.0)
            for ev in extremes[:5]:
                anoms = ev.get("anomalies", {})
                anom_str = ", ".join(
                    f"{k.replace('_anomaly','').replace('_zscore','')}: {v:+.1f}σ"
                    for k, v in anoms.items()
                )
                weather_lines.append(
                    f"  {ev['location'].upper()} ({ev['date']}): {anom_str}"
                )
        except Exception as e:
            logger.debug("GeographicIntelligence weather alerts failed: %s", e)
        weather_block = "\n".join(weather_lines) if weather_lines else "  NONE"

        # ── Interest rates details ────────────────────────────────────
        rate_10y       = "N/A"
        rate_2y        = "N/A"
        yc_label       = "UNKNOWN"
        yc_bps_str     = "N/A"
        hy_spreads_str = "N/A"
        fed_meeting    = "N/A"
        try:
            from data.collectors.rates_credit_collector import RatesCreditCollector
            rc = RatesCreditCollector()
            yc_status = rc.get_yield_curve_status()
            if yc_status:
                slope = yc_status.get("slope")
                if slope is not None:
                    yc_bps_str = f"{slope*100:+.0f}bps"
                    yc_label = "INVERTED" if yc_status.get("is_inverted") else (
                        "FLAT" if abs(slope) < 0.25 else "NORMAL")
                # Get actual yield levels from historical rates_data table
                try:
                    import sqlite3 as _sql
                    hconn = _sql.connect("output/historical_db.db")
                    r10_row = hconn.execute("SELECT value FROM rates_data WHERE series_id='DGS10' ORDER BY obs_date DESC LIMIT 1").fetchone()
                    r2_row  = hconn.execute("SELECT value FROM rates_data WHERE series_id='DGS2' ORDER BY obs_date DESC LIMIT 1").fetchone()
                    hconn.close()
                    rate_10y = f"{r10_row[0]:.2f}%" if r10_row else "N/A"
                    rate_2y  = f"{r2_row[0]:.2f}%" if r2_row else "N/A"
                except Exception:
                    pass
            credit = rc.get_credit_conditions()
            if credit:
                hy = credit.get("hy_spread")
                if hy is not None:
                    # hy_spread from FRED BAMLH0A0HYM2 is in percent; convert to bps
                    hy_bps = hy * 100
                    hy_level = "TIGHT" if hy_bps < 300 else ("WIDE" if hy_bps > 500 else "NORMAL")
                    hy_spreads_str = f"{hy_bps:.0f}bps ({hy_level})"
            days_to_fed = rc.days_to_next_fed_meeting()
            if days_to_fed is not None:
                from datetime import date, timedelta
                fed_dt = date.today() + timedelta(days=days_to_fed)
                fed_meeting = f"{fed_dt.strftime('%Y-%m-%d')} — {days_to_fed} days away"
            # Supplement with macro state data if available
            if yc_slope is not None and yc_bps_str == "N/A":
                yc_bps_str = f"{yc_slope*100:+.0f}bps"
                yc_label = "INVERTED" if is_inverted else "NORMAL"
        except Exception as e:
            logger.debug("RatesCreditCollector details failed: %s", e)

        # ── Consumer health details ───────────────────────────────────
        conf_val   = "N/A"
        conf_prior = "N/A"
        conf_trend = "N/A"
        claims_val = "N/A"
        claims_prior = "N/A"
        consumer_label = "UNKNOWN"
        try:
            from data.collectors.consumer_intelligence import ConsumerIntelligence
            ci = ConsumerIntelligence()
            latest = ci.get_latest_values()
            # Direct DB fallback for consumer data
            try:
                import sqlite3 as _sql
                hconn2 = _sql.connect("output/historical_db.db")
                def _get_series(sid):
                    r = hconn2.execute(
                        "SELECT value FROM macro_series WHERE series_id=? ORDER BY date DESC LIMIT 1", (sid,)
                    ).fetchone()
                    return r[0] if r else None
                c_conf   = _get_series("UMCSENT") or latest.get("UMCSENT")
                c_claims = _get_series("ICSA") or latest.get("ICSA")
                hconn2.close()
            except Exception:
                c_conf   = latest.get("UMCSENT") if latest else None
                c_claims = latest.get("ICSA") if latest else None
            if c_conf   is not None: conf_val   = f"{c_conf:.1f}"
            if c_claims is not None: claims_val = f"{c_claims:,.0f}"
            if latest: conf_trend = ci.get_trend("UMCSENT")
            health_idx = ci.get_consumer_health_index()
            if health_idx is not None:
                consumer_label = (
                    "STRONG" if health_idx > 0.5 else
                    "MODERATE" if health_idx > 0.1 else
                    "WEAK" if health_idx > -0.3 else "DETERIORATING"
                )
            elif isinstance(consumer_health, (int, float)):
                consumer_label = (
                    "STRONG" if consumer_health > 0.5 else
                    "MODERATE" if consumer_health > 0.1 else
                    "WEAK" if consumer_health > -0.3 else "DETERIORATING"
                )
        except Exception as e:
            logger.debug("ConsumerIntelligence details failed: %s", e)

        # ── Geopolitical details ──────────────────────────────────────
        geo_alerts_lines = []
        geo_level = "UNKNOWN"
        try:
            from data.collectors.geopolitical_collector import GeopoliticalCollector
            gc = GeopoliticalCollector()
            alerts = gc.get_alerts()  # returns List[GeopoliticalAlert]
            for al in (alerts or [])[:5]:
                if hasattr(al, 'description'):
                    geo_alerts_lines.append(f"  - [{al.severity}] {al.description[:100]}")
                else:
                    geo_alerts_lines.append(f"  - {str(al)[:100]}")
            geo_level = gc.get_current_risk_level()
        except Exception as e:
            logger.debug("GeopoliticalCollector details failed: %s", e)
        if not geo_level and isinstance(geo_risk, str):
            geo_level = geo_risk
        geo_block = "\n".join(geo_alerts_lines) if geo_alerts_lines else "  NONE"

        # ── Overnight news ────────────────────────────────────────────
        overnight_news = "No data available"
        try:
            api_keys = self.config.get("api_keys", {})
            news_key = api_keys.get("news_api", "")
            if news_key:
                import requests as _req
                resp = _req.get(
                    "https://newsapi.org/v2/top-headlines",
                    params={
                        "apiKey":   news_key,
                        "category": "business",
                        "language": "en",
                        "pageSize": 5,
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    articles = resp.json().get("articles", [])
                    headlines = [a.get("title", "") for a in articles if a.get("title")]
                    if headlines:
                        overnight_news = "\n  ".join(f"- {h}" for h in headlines[:5])
        except Exception as e:
            logger.debug("Overnight news fetch failed: %s", e)

        # ── Trading guidance based on regime + conditions ─────────────
        favour_sectors = []
        avoid_sectors  = []
        position_pct   = 100

        if regime == "RISK_OFF" or (is_inverted and is_inverted is True):
            favour_sectors = ["Healthcare", "Utilities", "Consumer Staples"]
            avoid_sectors  = ["Small Cap Growth", "High Beta Tech", "Cyclicals"]
            position_pct   = 65
        elif regime == "RISK_ON":
            favour_sectors = ["Technology", "Consumer Discretionary", "Industrials"]
            avoid_sectors  = ["Defensive Utilities", "Long Duration Bonds"]
            position_pct   = 100
        elif regime in ("STAGFLATION", "HIGH_INFLATION"):
            favour_sectors = ["Energy", "Materials", "Real Estate (REIT)"]
            avoid_sectors  = ["Consumer Discretionary", "High P/E Growth"]
            position_pct   = 75
        else:
            favour_sectors = ["Quality Factor", "Dividend Growers"]
            avoid_sectors  = ["Highly Leveraged Names"]
            position_pct   = 85

        if pead_mult is not None:
            position_pct = int(round(position_pct * float(pead_mult)))

        confidence_display = "N/A"
        try:
            from analysis.macro_signal_engine import MacroSignalEngine
            mse = MacroSignalEngine(config_path='config/settings.yaml')
            rdata = mse.get_current_regime()
            if rdata and rdata.get("confidence") is not None:
                confidence_display = f"{rdata['confidence']*100:.0f}"
        except Exception:
            pass

        mult_display = f"{pead_mult:.2f}" if pead_mult is not None else "1.00"

        favour_str = ", ".join(favour_sectors) if favour_sectors else "No strong preference"
        avoid_str  = ", ".join(avoid_sectors)  if avoid_sectors  else "None identified"

        # ── Assemble briefing ─────────────────────────────────────────
        briefing = f"""================================
MACRO BRIEFING — {date_str} {time_str}
================================

MARKET REGIME: {regime} ({confidence_display}% confidence)
Strategy adjustment: {mult_display}x position sizes

OVERNIGHT DEVELOPMENTS:
  {overnight_news}

SHIPPING STATUS:
  Baltic Dry Index: {bdi_value} ({bdi_change} 1-week change)
  Shipping Stress Index: {shipping_level}
  Sector impact: {shipping_sector_impact}

WEATHER ALERTS:
{weather_block}

INTEREST RATES:
  10yr yield: {rate_10y}
  2yr yield:  {rate_2y}
  Yield curve: {yc_bps_str} — {yc_label}
  Credit spreads HY: {hy_spreads_str}
  Fed next meeting: {fed_meeting}

CONSUMER HEALTH:
  Consumer confidence: {conf_val} vs {conf_prior} ({conf_trend})
  Jobless claims: {claims_val} vs {claims_prior}
  Overall: {consumer_label}

GEOPOLITICAL:
  Active alerts: {geo_block}
  Conflict level: {geo_level}

TODAY'S TRADING GUIDANCE:
  Sectors to FAVOUR: {favour_str}
  Sectors to AVOID:  {avoid_str}
  Position sizing: {position_pct}% of normal (regime adjustment)

================================
"""

        # ── Save to file ──────────────────────────────────────────────
        if save_to_file:
            try:
                out_dir = Path("output")
                out_dir.mkdir(parents=True, exist_ok=True)

                dated_path  = out_dir / f"macro_briefing_{date_tag}.txt"
                latest_path = out_dir / "macro_briefing_latest.txt"

                dated_path.write_text(briefing)
                latest_path.write_text(briefing)
                logger.info("Macro briefing saved to %s", dated_path)
            except Exception as e:
                logger.error("Failed to save macro briefing: %s", e)

        print(briefing)
        return briefing
