import pandas as pd
import numpy as np


class Features:

    def features_of(self, candles: pd.DataFrame, period: int, freq: str, n: int)->pd.DataFrame:
        """
        Feature engineering
        """
        return self.time_features(candles).join(self.low_high_past(candles, period, freq, n))

    def low_high_past(self, candles: pd.DataFrame, period: int, freq: str, n: int) -> pd.DataFrame:
        """
        Candles features: low/high values of past n candles
        """
        windowspec = f"{period} {freq}"
        rolling = candles.rolling(windowspec, min_periods=0).agg(
            {'high': 'max', 'low': 'min'}, closed='right')
        # Add previous intervals lows/highs
        df2 = pd.DataFrame(index=candles.index)
        for shift in range(1, n + 1):
            df2[[f"-{shift}*{period}{freq}_high", f"-{shift}*{period}{freq}_low"]] = \
                rolling.shift(shift - 1, freq)[['high', 'low']] \
                    .reindex(df2.index, method='nearest', tolerance=windowspec)
        return df2.sort_index()

    def time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add day, week to features
        :param df: dataframe with datetime column
        :return: dataframe with time features
        """
        df2 = pd.DataFrame(index=df.index)
        df2["dayofweek"] = df['close_time'].dt.dayofweek
        df2["month"] = df['close_time'].dt.month
        df2["year"] = df['close_time'].dt.year
        df2["hour"] = df['close_time'].dt.hour
        df2["minute"] = df['close_time'].dt.minute
        df2["sec"] = df['close_time'].dt.second
        return df2
