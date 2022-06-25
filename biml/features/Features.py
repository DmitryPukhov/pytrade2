import pandas as pd
import numpy as np


class Features:

    def features_of(self, candles: pd.DataFrame, period: int, freq: str, n: int) -> pd.DataFrame:
        """
        Feature engineering
        """
        return self.time_features(candles).join(self.low_high_past(candles, period, freq, n))

    def low_high_past(self, candles: pd.DataFrame, period: int, freq: str, window_size: int) -> pd.DataFrame:
        """
        Candles features: low/high values of past n candles
        """
        windowspec = f"{period} {freq}"
        rolling = candles.rolling(windowspec, min_periods=0).agg(
            {'open': lambda x: list(x)[0], 'high': 'max', 'low': 'min', 'close': lambda x: list(x)[-1], },
            closed='right')
        df2 = pd.DataFrame(index=candles.index)
        for shift in range(1, window_size + 1):
            src_cols = ['open', 'high', 'low', 'close']
            target_cols = [f"-{shift}*{period}{freq}_{src_col}" for src_col in src_cols]
            df2[target_cols] = rolling.shift(shift - 1, freq)[src_cols] \
                .reindex(df2.index, method='nearest', tolerance=windowspec)
        return df2.sort_index()

    def time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add day, week to features
        :param df: dataframe with datetime column
        :return: dataframe with time features
        """
        df2 = pd.DataFrame(index=df.index)
        df2['close_time_abs'] = df['close_time'].astype(np.int64)
        # df2["dayofweek"] = df['close_time'].dt.dayofweek
        # df2["month"] = df['close_time'].dt.month
        # df2["year"] = df['close_time'].dt.year
        # df2["hour"] = df['close_time'].dt.hour
        # df2["minute"] = df['close_time'].dt.minute
        # df2["sec"] = df['close_time'].dt.second
        return df2
