"""
FactorModelAnalyser — Fama-French 6-factor model (FF5 + MOM).

Data: Ken French's free data library (CSV zip files).
Factors: MKT_RF, SMB, HML, RMW, CMA, MOM.

Usage:
    fma = FactorModelAnalyser()
    loadings = fma.compute_factor_loadings("AAPL", returns_series)
    exposure = fma.portfolio_factor_exposure(["AAPL", "MSFT"], weights)
    recs = fma.neutralise_recommendation(exposure, regime="BEAR")
    fma.run_weekly()   # full portfolio analysis + Telegram summary
"""
from __future__ import annotations

import io
import logging
import os
import pickle
import sqlite3
import zipfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# FF5 daily — try daily first, fall back to all-frequency zip
_FF5_URLS = [
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip",
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip",
]
# MOM daily
_MOM_URLS = [
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip",
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip",
]
# Keep old names as aliases (backward compat)
_FF5_URL = _FF5_URLS[0]
_MOM_URL = _MOM_URLS[0]
_CACHE    = "data/cache/ff_factors.pkl"
_MAX_CACHE_AGE_DAYS = 7

_FACTORS = ["MKT_RF", "SMB", "HML", "RMW", "CMA", "MOM"]


def _parse_ff_csv(content: str) -> pd.DataFrame:
    """Parse Ken French's whitespace/comma CSV format."""
    lines = content.splitlines()
    # Drop copyright lines and find header
    data_lines = []
    in_data = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Copyright"):
            continue
        # Header line contains 'Mkt-RF' or 'Mom'
        if any(k in stripped for k in ("Mkt-RF", "Mom", "SMB", "HML")):
            in_data = True
        if in_data:
            data_lines.append(stripped)

    if not data_lines:
        return pd.DataFrame()

    try:
        df = pd.read_csv(io.StringIO("\n".join(data_lines)), sep=r"\s*,\s*", engine="python")
        # First column is date YYYYMMDD
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "Date"})
        df["Date"] = pd.to_datetime(df["Date"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date")
        df = df.apply(pd.to_numeric, errors="coerce") / 100.0  # percentages → decimals
        return df
    except Exception as exc:
        logger.warning("_parse_ff_csv error: %s", exc)
        return pd.DataFrame()


class FactorModelAnalyser:
    """
    Fama-French 6-factor model analyser with weekly portfolio reports.
    """

    HISTORICAL_DB  = "output/historical_db.db"
    CLOSELOOP_DB   = "closeloop/storage/closeloop.db"

    def __init__(
        self,
        historical_db: Optional[str] = None,
        closeloop_db: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> None:
        self._hist_db    = historical_db or self.HISTORICAL_DB
        self._closeloop  = closeloop_db  or self.CLOSELOOP_DB
        self._config     = config or {}
        self._factors_df: Optional[pd.DataFrame] = None
        self._ensure_table()

    # ── DB ────────────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            con = sqlite3.connect(self._closeloop, timeout=10)
            con.execute("""
                CREATE TABLE IF NOT EXISTS factor_exposures (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker    TEXT NOT NULL,
                    run_date  TEXT NOT NULL,
                    alpha     REAL,
                    beta_mkt  REAL,
                    beta_smb  REAL,
                    beta_hml  REAL,
                    beta_rmw  REAL,
                    beta_cma  REAL,
                    beta_mom  REAL,
                    r_squared REAL,
                    UNIQUE(ticker, run_date)
                )
            """)
            con.commit()
            con.close()
        except Exception as exc:
            logger.warning("FactorModelAnalyser._ensure_table: %s", exc)

    # ── factor data ───────────────────────────────────────────────────────

    def download_factors(self) -> pd.DataFrame:
        """Download and cache Fama-French daily factors. Returns DataFrame."""
        # Check cache
        if os.path.exists(_CACHE):
            age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(_CACHE))
            if age.days < _MAX_CACHE_AGE_DAYS:
                try:
                    with open(_CACHE, "rb") as fh:
                        df = pickle.load(fh)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        self._factors_df = df
                        return df
                except Exception:
                    pass

        try:
            import requests
            sess = requests.Session()

            def _fetch_zip_csv(urls: list) -> str:
                """Try each URL; return decoded CSV text from the first CSV file inside the zip."""
                last_exc = None
                for url in urls:
                    try:
                        resp = sess.get(url, timeout=30)
                        resp.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                            # Case-insensitive search for CSV file
                            csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
                            if not csv_names:
                                continue
                            return z.read(csv_names[0]).decode("utf-8", errors="ignore")
                    except Exception as exc:
                        last_exc = exc
                        logger.debug("download_factors URL %s failed: %s", url, exc)
                raise RuntimeError(f"All FF URLs failed. Last error: {last_exc}")

            # FF5 daily
            ff5_text = _fetch_zip_csv(_FF5_URLS)
            ff5 = _parse_ff_csv(ff5_text)
            # All-frequency zip has 5 cols; daily has 6 (RF included)
            if len(ff5.columns) >= 6:
                ff5.columns = ["MKT_RF", "SMB", "HML", "RMW", "CMA", "RF"]
            elif len(ff5.columns) == 5:
                ff5.columns = ["MKT_RF", "SMB", "HML", "RMW", "CMA"]
            elif len(ff5.columns) == 4:
                # 3-factor fallback (F-F_Research_Data_Factors): MKT_RF SMB HML RF
                ff5.columns = ["MKT_RF", "SMB", "HML", "RF"]
            else:
                raise ValueError(f"Unexpected FF5 column count: {len(ff5.columns)}")

            # MOM daily
            mom_text = _fetch_zip_csv(_MOM_URLS)
            mom = _parse_ff_csv(mom_text)
            mom.columns = ["MOM"]

            df = ff5.join(mom, how="inner").dropna()
            os.makedirs(os.path.dirname(_CACHE), exist_ok=True)
            with open(_CACHE, "wb") as fh:
                pickle.dump(df, fh)
            self._factors_df = df
            logger.info("FactorModelAnalyser: downloaded %d factor rows", len(df))
            return df

        except Exception as exc:
            logger.warning("FactorModelAnalyser.download_factors error: %s", exc)
            # Return empty — graceful degradation
            return pd.DataFrame(columns=_FACTORS)

    def _get_factors(self) -> pd.DataFrame:
        if self._factors_df is None or self._factors_df.empty:
            self.download_factors()
        if self._factors_df is None or self._factors_df.empty:
            return pd.DataFrame(columns=_FACTORS)
        return self._factors_df

    # ── regression ────────────────────────────────────────────────────────

    def compute_factor_loadings(
        self, ticker: str, returns: pd.Series, min_obs: int = 60
    ) -> Dict[str, Any]:
        """
        OLS regression of excess returns on 6 factors.

        Parameters
        ----------
        ticker  : symbol (for logging / storage)
        returns : pd.Series of daily returns, DatetimeIndex
        min_obs : minimum aligned observations required

        Returns
        -------
        dict with alpha, beta_mkt … beta_mom, r_squared
        """
        empty = {f: 0.0 for f in ["alpha"] + [f"beta_{f.lower()}" for f in _FACTORS]}
        empty["r_squared"] = 0.0
        empty["ticker"] = ticker
        empty["n_obs"]  = 0

        factors = self._get_factors()
        if factors.empty:
            return empty

        try:
            # Align returns with factors on date
            ret_df = returns.to_frame("ret")
            ret_df.index = pd.to_datetime(ret_df.index)
            merged = ret_df.join(factors, how="inner").dropna()

            if len(merged) < min_obs:
                logger.debug("FactorModelAnalyser %s: only %d aligned obs", ticker, len(merged))
                return {**empty, "n_obs": len(merged)}

            y = merged["ret"].values - merged.get("RF", pd.Series(0, index=merged.index)).values
            X_cols = [c for c in _FACTORS if c in merged.columns]
            X_raw = merged[X_cols].values
            # Add intercept
            X = np.column_stack([np.ones(len(X_raw)), X_raw])

            # OLS: β = (X'X)^{-1} X'y
            try:
                coeffs, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)
            except np.linalg.LinAlgError:
                return empty

            alpha = float(coeffs[0])
            betas = coeffs[1:]

            # R²
            y_pred = X @ coeffs
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - y.mean()) ** 2)
            r2 = float(1 - ss_res / ss_tot) if ss_tot > 1e-12 else 0.0

            result = {
                "ticker":    ticker,
                "alpha":     alpha,
                "r_squared": r2,
                "n_obs":     len(merged),
            }
            for i, factor in enumerate(X_cols):
                result[f"beta_{factor.lower()}"] = float(betas[i]) if i < len(betas) else 0.0

            # Store in DB
            self._store_loadings(result)
            return result

        except Exception as exc:
            logger.warning("compute_factor_loadings %s: %s", ticker, exc)
            return empty

    def _store_loadings(self, result: Dict[str, Any]) -> None:
        today = datetime.utcnow().date().isoformat()
        try:
            con = sqlite3.connect(self._closeloop, timeout=10)
            con.execute("""
                INSERT OR REPLACE INTO factor_exposures
                (ticker, run_date, alpha, beta_mkt, beta_smb, beta_hml, beta_rmw, beta_cma, beta_mom, r_squared)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                result.get("ticker"), today,
                result.get("alpha"),
                result.get("beta_mkt_rf", result.get("beta_mkt", 0.0)),
                result.get("beta_smb", 0.0),
                result.get("beta_hml", 0.0),
                result.get("beta_rmw", 0.0),
                result.get("beta_cma", 0.0),
                result.get("beta_mom", 0.0),
                result.get("r_squared", 0.0),
            ))
            con.commit()
            con.close()
        except Exception as exc:
            logger.debug("_store_loadings %s: %s", result.get("ticker"), exc)

    # ── portfolio exposure ─────────────────────────────────────────────────

    def portfolio_factor_exposure(
        self,
        tickers_returns: Dict[str, pd.Series],
        weights: Optional[Dict[str, float]] = None,
    ) -> Dict[str, float]:
        """
        Compute weighted-average factor loadings across a portfolio.

        Parameters
        ----------
        tickers_returns : {ticker: returns_series}
        weights         : {ticker: weight} — equal-weight if None

        Returns
        -------
        dict: {factor → weighted_loading}
        """
        if not tickers_returns:
            return {}

        n = len(tickers_returns)
        if weights is None:
            weights = {t: 1.0 / n for t in tickers_returns}

        exposure: Dict[str, float] = {f: 0.0 for f in ["alpha"] + [f"beta_{f.lower()}" for f in _FACTORS]}

        total_w = 0.0
        for ticker, returns in tickers_returns.items():
            w = weights.get(ticker, 1.0 / n)
            loadings = self.compute_factor_loadings(ticker, returns)
            for key in exposure:
                exposure[key] += w * loadings.get(key, 0.0)
            total_w += w

        if total_w > 0:
            for key in exposure:
                exposure[key] /= total_w

        return exposure

    # ── regime recommendations ────────────────────────────────────────────

    def neutralise_recommendation(
        self, exposures: Dict[str, float], regime: str = "NEUTRAL"
    ) -> List[str]:
        """
        Generate text recommendations to reduce factor risk.

        Rules (regime-specific):
          - BEAR: SMB > 0.5 → reduce small-cap; HML < -0.3 → add value
          - BULL: MOM < -0.3 → add momentum; SMB > 0.4 → fine (small-cap in BULL is OK)
          - Any:  alpha < -0.03 → review under-performing positions
        """
        recs: List[str] = []
        regime = (regime or "NEUTRAL").upper()

        smb = exposures.get("beta_smb", 0.0)
        hml = exposures.get("beta_hml", 0.0)
        mom = exposures.get("beta_mom", 0.0)
        alpha = exposures.get("alpha", 0.0)
        mkt  = exposures.get("beta_mkt_rf", exposures.get("beta_mkt", 0.0))

        if regime in ("BEAR", "CRISIS"):
            if smb > 0.5:
                recs.append(f"Reduce small-cap exposure (SMB={smb:.2f} in {regime})")
            if hml < -0.3:
                recs.append(f"Rotate toward value (HML={hml:.2f})")
            if mkt > 1.2:
                recs.append(f"High market beta {mkt:.2f} — reduce net long in {regime}")

        if regime == "BULL":
            if mom < -0.3:
                recs.append(f"Low momentum exposure (MOM={mom:.2f}) — add momentum stocks in BULL")
            if mkt < 0.6:
                recs.append(f"Low market beta {mkt:.2f} — may be under-investing in BULL")

        if alpha < -0.03:
            recs.append(f"Portfolio alpha negative ({alpha:.3f}) — review under-performers")

        if not recs:
            recs.append(f"Factor exposure acceptable for {regime} regime")

        return recs

    # ── weekly run ────────────────────────────────────────────────────────

    def run_weekly(
        self,
        positions: Optional[List[str]] = None,
        regime: str = "NEUTRAL",
    ) -> Dict[str, Any]:
        """
        Run full factor analysis for current positions.
        Sends Telegram summary.
        Returns analysis dict.
        """
        # Load open positions from DB if not provided
        if positions is None:
            try:
                con = sqlite3.connect(self._closeloop, timeout=10)
                rows = con.execute(
                    "SELECT DISTINCT ticker FROM trade_ledger WHERE exit_date IS NULL LIMIT 30"
                ).fetchall()
                con.close()
                positions = [r[0] for r in rows]
            except Exception:
                positions = []

        # Load returns from historical DB
        tickers_returns: Dict[str, pd.Series] = {}
        try:
            con = sqlite3.connect(self._hist_db, timeout=10)
            for ticker in positions[:20]:  # cap at 20 for speed
                rows = con.execute(
                    "SELECT date, close FROM price_history WHERE ticker=? ORDER BY date DESC LIMIT 252",
                    (ticker,),
                ).fetchall()
                if len(rows) >= 30:
                    closes = pd.Series(
                        {r[0]: r[1] for r in reversed(rows)},
                        dtype=float,
                    )
                    closes.index = pd.to_datetime(closes.index)
                    tickers_returns[ticker] = closes.pct_change().dropna()
            con.close()
        except Exception as exc:
            logger.warning("run_weekly load returns: %s", exc)

        if not tickers_returns:
            return {"positions": 0, "exposure": {}, "recommendations": ["No position data available"]}

        exposure = self.portfolio_factor_exposure(tickers_returns)
        recs = self.neutralise_recommendation(exposure, regime)

        result = {
            "positions":       len(tickers_returns),
            "regime":          regime,
            "exposure":        exposure,
            "recommendations": recs,
        }

        self._send_telegram_summary(result)
        return result

    def _send_telegram_summary(self, result: Dict[str, Any]) -> None:
        try:
            tg = self._config.get("notifications", {}).get("telegram", {})
            token   = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
            if not token or not chat_id:
                return
            exp = result.get("exposure", {})
            lines = [
                "[FactorModel] Weekly Factor Exposure",
                f"Positions analysed: {result.get('positions', 0)}",
                f"Regime: {result.get('regime', 'UNKNOWN')}",
                f"Alpha:  {exp.get('alpha', 0):.4f}",
                f"MKT_RF: {exp.get('beta_mkt_rf', exp.get('beta_mkt', 0)):.3f}",
                f"SMB:    {exp.get('beta_smb', 0):.3f}",
                f"HML:    {exp.get('beta_hml', 0):.3f}",
                f"MOM:    {exp.get('beta_mom', 0):.3f}",
                "",
                "Recommendations:",
            ] + [f"  • {r}" for r in result.get("recommendations", [])]
            import requests
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": "\n".join(lines)},
                timeout=10,
            )
        except Exception:
            pass

    def status(self) -> Dict[str, Any]:
        try:
            con = sqlite3.connect(self._closeloop, timeout=10)
            n = con.execute("SELECT COUNT(*) FROM factor_exposures").fetchone()[0]
            t = con.execute("SELECT COUNT(DISTINCT ticker) FROM factor_exposures").fetchone()[0]
            con.close()
            return {"rows": n, "tickers": t}
        except Exception:
            return {"rows": 0, "tickers": 0}
