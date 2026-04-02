import numpy as np
import pandas as pd


class DataCleaner:
    def clean_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df = df.dropna(subset=["close"])
        df = df.ffill(limit=5)
        df = df[df["close"] > 0]
        df = df[df["volume"] >= 0]

        # Remove likely data errors: single-day moves > 50%
        if len(df) > 1:
            returns = df["close"].pct_change()
            bad = returns.abs() > 0.50
            bad.iloc[0] = False
            df.loc[bad, "close"] = np.nan
            df["close"] = df["close"].ffill()

        # Re-enforce OHLC consistency
        df["high"] = df[["open", "high", "low", "close"]].max(axis=1)
        df["low"] = df[["open", "high", "low", "close"]].min(axis=1)

        return df.dropna(subset=["close"])

    def compute_returns(self, df: pd.DataFrame, col: str = "close") -> pd.Series:
        return df[col].pct_change().dropna()

    def compute_log_returns(self, df: pd.DataFrame, col: str = "close") -> pd.Series:
        return np.log(df[col] / df[col].shift(1)).dropna()
