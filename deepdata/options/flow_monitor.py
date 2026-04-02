"""Options flow monitoring using yfinance (free, no API key required)."""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/cache/deepdata/options")


def _make_result(
    source: str,
    ticker: str,
    market: str,
    data_type: str,
    value,
    raw_data=None,
    quality_score: float = 1.0,
) -> dict:
    return {
        "source": source,
        "ticker": ticker,
        "market": market,
        "data_type": data_type,
        "value": value,
        "raw_data": raw_data or {},
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
    }


class OptionsFlowMonitor:
    """Monitor options flow for smart money activity signals."""

    def __init__(self, config: dict):
        self.config = config
        self.uk_confidence_weight = config.get("uk_confidence_weight", 0.6)
        self.block_threshold = config.get("block_threshold", 500)
        self.rate_limit_sleep = config.get("rate_limit_sleep", 0.5)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def collect(self, tickers: list, market: str = "us") -> list:
        """Alias for scan() — called by cmd_deepdata_collect dispatch."""
        return self.scan(tickers, market)

    def scan(self, tickers: list, market: str = "us") -> list:
        """Return list of CollectorResult dicts with options flow data."""
        results = []
        if yf is None:
            logger.warning("yfinance not installed; cannot scan options flow")
            return results

        for ticker in tickers:
            yf_ticker = ticker + ".L" if market == "uk" and not ticker.endswith(".L") else ticker
            confidence = self.uk_confidence_weight if market == "uk" else 1.0

            try:
                chain = self.get_options_chain(yf_ticker)
                if not chain or chain.get("calls") is None:
                    logger.warning("No options chain for %s", yf_ticker)
                    continue

                # Get spot price
                try:
                    info = yf.Ticker(yf_ticker).fast_info
                    spot = float(getattr(info, "last_price", 0) or 0)
                except Exception:
                    spot = 0.0

                # SMFI
                try:
                    smfi = self.calc_smart_money_flow_index(chain)
                    results.append(_make_result(
                        source="options_flow",
                        ticker=ticker,
                        market=market,
                        data_type="smfi",
                        value=smfi,
                        raw_data={"smfi": smfi},
                        quality_score=confidence,
                    ))
                except Exception as exc:
                    logger.warning("SMFI calc failed for %s: %s", ticker, exc)

                # Gamma exposure
                if spot > 0:
                    try:
                        gamma = self.calc_gamma_exposure(chain, spot)
                        results.append(_make_result(
                            source="options_flow",
                            ticker=ticker,
                            market=market,
                            data_type="gamma_exposure",
                            value=gamma.get("net_gamma", 0.0),
                            raw_data=gamma,
                            quality_score=confidence,
                        ))
                    except Exception as exc:
                        logger.warning("Gamma calc failed for %s: %s", ticker, exc)

                # IV rank
                try:
                    calls = chain.get("calls", pd.DataFrame())
                    current_iv = float(calls["impliedVolatility"].mean()) if "impliedVolatility" in calls.columns and not calls.empty else 0.0
                    iv_rank = self.calc_iv_rank(yf_ticker, current_iv)
                    results.append(_make_result(
                        source="options_flow",
                        ticker=ticker,
                        market=market,
                        data_type="iv_rank",
                        value=iv_rank,
                        raw_data={"current_iv": current_iv, "iv_rank": iv_rank},
                        quality_score=confidence,
                    ))
                except Exception as exc:
                    logger.warning("IV rank failed for %s: %s", ticker, exc)

                # Put/call ratio
                try:
                    pc = self.calc_put_call_ratio(chain)
                    results.append(_make_result(
                        source="options_flow",
                        ticker=ticker,
                        market=market,
                        data_type="put_call_ratio",
                        value=pc,
                        raw_data={"put_call_ratio": pc},
                        quality_score=confidence,
                    ))
                except Exception as exc:
                    logger.warning("PC ratio failed for %s: %s", ticker, exc)

                # Dark pool score
                try:
                    dp = self.dark_pool_score(yf_ticker)
                    results.append(_make_result(
                        source="options_flow",
                        ticker=ticker,
                        market=market,
                        data_type="dark_pool_score",
                        value=dp,
                        raw_data={"dark_pool_zscore": dp},
                        quality_score=confidence * 0.7,
                    ))
                except Exception as exc:
                    logger.warning("Dark pool score failed for %s: %s", ticker, exc)

                time.sleep(self.rate_limit_sleep)

            except Exception as exc:
                logger.warning("scan failed for %s: %s", ticker, exc)

        return results

    def get_options_chain(self, ticker: str) -> dict:
        """Fetch full options chain via yfinance. Return {calls: df, puts: df}."""
        if yf is None:
            return {}
        try:
            t = yf.Ticker(ticker)
            expiries = t.options
            if not expiries:
                return {}

            all_calls = []
            all_puts = []
            for expiry in expiries[:4]:  # Limit to near-term expiries
                try:
                    chain = t.option_chain(expiry)
                    calls = chain.calls.copy()
                    puts = chain.puts.copy()
                    calls["expiry"] = expiry
                    puts["expiry"] = expiry
                    all_calls.append(calls)
                    all_puts.append(puts)
                    time.sleep(self.rate_limit_sleep)
                except Exception as exc:
                    logger.warning("option_chain(%s, %s) failed: %s", ticker, expiry, exc)

            calls_df = pd.concat(all_calls, ignore_index=True) if all_calls else pd.DataFrame()
            puts_df = pd.concat(all_puts, ignore_index=True) if all_puts else pd.DataFrame()
            return {"calls": calls_df, "puts": puts_df}
        except Exception as exc:
            logger.warning("get_options_chain(%s) failed: %s", ticker, exc)
            return {}

    def calc_smart_money_flow_index(self, chain: dict) -> float:
        """SMFI = (call_sweep_vol*1.5 + block_call*1.3 + unusual_call) / (put_sweep*1.5 + block_put*1.3 + unusual_put + 0.001)"""
        calls = chain.get("calls", pd.DataFrame())
        puts = chain.get("puts", pd.DataFrame())

        def _get_vol(df):
            if df.empty or "volume" not in df.columns:
                return 0.0
            return float(df["volume"].fillna(0).sum())

        def _get_sweep_vol(df):
            if df.empty or "volume" not in df.columns or "openInterest" not in df.columns:
                return 0.0
            mask = df["volume"].fillna(0) > df["openInterest"].fillna(1) * 2
            return float(df.loc[mask, "volume"].fillna(0).sum())

        def _get_block_vol(df):
            if df.empty or "volume" not in df.columns:
                return 0.0
            mask = df["volume"].fillna(0) > self.block_threshold
            return float(df.loc[mask, "volume"].fillna(0).sum())

        def _get_unusual_vol(df):
            unusual = self.detect_unusual_volume({"calls": df, "puts": pd.DataFrame()}) if not df.empty else []
            return float(sum(e.get("volume", 0) for e in unusual))

        call_sweep = _get_sweep_vol(calls)
        block_call = _get_block_vol(calls)
        unusual_call = _get_unusual_vol(calls)

        put_sweep = _get_sweep_vol(puts)
        block_put = _get_block_vol(puts)
        unusual_put = _get_unusual_vol(puts)

        numerator = call_sweep * 1.5 + block_call * 1.3 + unusual_call
        denominator = put_sweep * 1.5 + block_put * 1.3 + unusual_put + 0.001

        return float(numerator / denominator)

    def calc_gamma_exposure(self, chain: dict, spot: float) -> dict:
        """Net gamma exposure. Return {net_gamma, gamma_flip_level, regime}"""
        calls = chain.get("calls", pd.DataFrame())
        puts = chain.get("puts", pd.DataFrame())

        def _net_gamma_for(df, is_call: bool):
            if df.empty:
                return 0.0, {}
            vol_col = "impliedVolatility"
            gamma_col = "gamma"
            oi_col = "openInterest"
            strike_col = "strike"

            required = [strike_col, oi_col]
            for c in required:
                if c not in df.columns:
                    return 0.0, {}

            df = df.copy().fillna(0)

            # Use gamma column if available, else estimate
            if gamma_col in df.columns and df[gamma_col].sum() > 0:
                gamma_vals = df[gamma_col].values
            else:
                # Black-Scholes approximation: gamma ~ 1 / (spot * IV * sqrt(T))
                if vol_col in df.columns and df[vol_col].sum() > 0:
                    iv = df[vol_col].replace(0, np.nan).fillna(0.3)
                    T = 30 / 252  # assume 30-day expiry proxy
                    gamma_vals = np.where(
                        spot > 0,
                        1.0 / (spot * iv * np.sqrt(T) + 1e-9),
                        0.0,
                    )
                else:
                    gamma_vals = np.zeros(len(df))

            oi = df[oi_col].values
            strikes = df[strike_col].values
            gex_per_strike = gamma_vals * oi * 100 * (spot ** 2) / 100
            total_gex = float(gex_per_strike.sum())
            gex_by_strike = dict(zip(strikes.tolist(), gex_per_strike.tolist()))
            return total_gex, gex_by_strike

        call_gex, call_by_strike = _net_gamma_for(calls, True)
        put_gex, put_by_strike = _net_gamma_for(puts, False)
        net_gamma = call_gex - put_gex

        # Find gamma flip level (strike where net GEX crosses zero)
        flip_level = spot
        try:
            all_strikes = sorted(set(list(call_by_strike.keys()) + list(put_by_strike.keys())))
            if all_strikes:
                net_by_strike = {s: call_by_strike.get(s, 0) - put_by_strike.get(s, 0) for s in all_strikes}
                # Find sign change
                prev_sign = None
                for s in sorted(all_strikes):
                    sign = np.sign(net_by_strike[s])
                    if prev_sign is not None and sign != prev_sign and sign != 0:
                        flip_level = s
                        break
                    if sign != 0:
                        prev_sign = sign
        except Exception:
            pass

        regime = "positive" if net_gamma >= 0 else "negative"

        return {
            "net_gamma": net_gamma,
            "gamma_flip_level": flip_level,
            "regime": regime,
            "call_gex": call_gex,
            "put_gex": put_gex,
        }

    def calc_iv_rank(self, ticker: str, current_iv: float) -> float:
        """IV_rank = (current_IV - 52wk_low) / (52wk_high - 52wk_low)"""
        if yf is None or current_iv <= 0:
            return 0.0
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1y", interval="1wk")
            if hist.empty:
                return 0.0
            # Use weekly close returns as IV proxy (realized vol)
            returns = hist["Close"].pct_change().dropna()
            weekly_vols = returns.rolling(4).std() * np.sqrt(52)
            weekly_vols = weekly_vols.dropna()
            if weekly_vols.empty:
                return 0.0
            low_52 = float(weekly_vols.min())
            high_52 = float(weekly_vols.max())
            if high_52 - low_52 < 1e-9:
                return 0.5
            rank = (current_iv - low_52) / (high_52 - low_52)
            return float(np.clip(rank, 0.0, 1.0))
        except Exception as exc:
            logger.warning("calc_iv_rank(%s) failed: %s", ticker, exc)
            return 0.0

    def detect_unusual_volume(self, chain: dict) -> list:
        """Flag strikes where volume > 3x average OI for that strike."""
        events = []
        for side, df in [("call", chain.get("calls", pd.DataFrame())), ("put", chain.get("puts", pd.DataFrame()))]:
            if df.empty or "volume" not in df.columns or "openInterest" not in df.columns:
                continue
            df = df.copy().fillna(0)
            mean_oi = df["openInterest"].replace(0, np.nan).mean()
            if pd.isna(mean_oi) or mean_oi == 0:
                continue
            threshold = mean_oi * 3
            unusual = df[df["volume"] > threshold]
            for _, row in unusual.iterrows():
                events.append({
                    "side": side,
                    "strike": row.get("strike", None),
                    "expiry": row.get("expiry", None),
                    "volume": float(row.get("volume", 0)),
                    "openInterest": float(row.get("openInterest", 0)),
                    "volume_oi_ratio": float(row["volume"] / max(row.get("openInterest", 1), 1)),
                })
        return events

    def calc_put_call_ratio(self, chain: dict) -> float:
        """Put/call volume ratio."""
        calls = chain.get("calls", pd.DataFrame())
        puts = chain.get("puts", pd.DataFrame())
        call_vol = float(calls["volume"].fillna(0).sum()) if not calls.empty and "volume" in calls.columns else 0.0
        put_vol = float(puts["volume"].fillna(0).sum()) if not puts.empty and "volume" in puts.columns else 0.0
        if call_vol < 1:
            return float("nan")
        return put_vol / call_vol

    def dark_pool_score(self, ticker: str) -> float:
        """Approximate dark pool accumulation from volume/price divergence. Returns z-score."""
        if yf is None:
            return 0.0
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="30d", interval="1d")
            if hist.empty or len(hist) < 5:
                return 0.0
            price_changes = hist["Close"].pct_change().abs()
            volumes = hist["Volume"].fillna(0)
            avg_vol = volumes.rolling(20).mean()
            std_vol = volumes.rolling(20).std()

            # Dark pool proxy: high volume + low price movement
            low_price_move = price_changes < 0.005  # < 0.5%
            vol_diff = volumes - avg_vol
            recent = hist.tail(5)
            recent_low_move = price_changes.tail(5) < 0.005
            recent_vol_diff = (volumes - avg_vol).tail(5)
            std_last = std_vol.iloc[-1] if not std_vol.empty else 1.0
            if pd.isna(std_last) or std_last == 0:
                std_last = 1.0
            dark_pool_signal = float(recent_vol_diff[recent_low_move].sum() / std_last) if recent_low_move.any() else 0.0
            return float(dark_pool_signal)
        except Exception as exc:
            logger.warning("dark_pool_score(%s) failed: %s", ticker, exc)
            return 0.0
