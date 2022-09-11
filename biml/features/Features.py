from functools import reduce

import pandas as pd
import numpy as np


class Features:
    # @staticmethod
    # def features_and_targets_balanced(candles, period, window_size, predict_window_size):
    #     features, targets = Features.features_and_targets(candles, period, window_size, predict_window_size, "min")
    #     return Features.balanced(features, targets)
    #
    # @staticmethod
    # def balanced(X: pd.DataFrame, y: pd.DataFrame):
    #     """
    #     Make X, y balanced by buy/sell/offmarket signal count
    #     """
    #     vc = y.value_counts()
    #     mincount = min(vc.values)
    #     y_bal = reduce(lambda df1, df2: df1.sort_index().append(df2).sort_index(),
    #                    [y[y[col]==1].sample(n=mincount) for col in y.columns])
    #     X_bal = X[X.index.isin(y_bal.index)].sort_index()
    #     return X_bal, y_bal
    period=1
    freq="min"

    @staticmethod
    def features_and_targets(candles: pd.DataFrame, window_size: int, predict_window_size: int) -> (
            pd.DataFrame, pd.DataFrame):
        """
        :param candles: ohlc candles dataframe
        :param period: candles period
        :param window_size: how many previous candles to include into the features
        :param predict_window_size: how many future candles to include into the target prediction
        :param freq:  m for minute, h for hour
        :return: not scaled (features dataframe, targets dataframe)
        """

        # Prepare features and targets columns
        features = Features.features_of(candles, window_size).dropna()
        targets = Features.targets_of(features, predict_window_size).dropna()

        # Some features and targets has na and not matched, drop them
        targets = targets[targets.index.isin(features.index)]
        features = features[features.index.isin(targets.index)]

        return features, targets

    @staticmethod
    def targets_of(features: pd.DataFrame, window_size: int) -> pd.DataFrame:
        """
        Add target features: low and high price during future window
        :param features: pandas dataframe with candles
        :param freq : time unit for future window
        :param window_size: duration of future window in given time units
        """
        # Trick to implement forward rolling window for timeseries with unequal intervals:
        # reverse, apply rolling, then reverse back
        windowspec = f'{window_size} {Features.freq}'
        # df2 = df.reset_index(level='ticker', drop=True)
        df2: pd.DataFrame = features[['low', 'high']].sort_index(
            ascending=False).rolling(windowspec, min_periods=0).agg(
            {'low': 'min','high': 'max'}, closed='right')
        df2.rename({'high': 'fut_high', 'low': 'fut_low'}, inplace=True)
        #df2[['fut_low', 'fut_high']] = df2[['low', 'high']]
        #df2.drop(['high', 'low'], axis=1, inplace=True)
        return df2.sort_index()

    @staticmethod
    def features_of(candles: pd.DataFrame, window_size: int) -> pd.DataFrame:
        """
        Feature engineering
        """
        transformed = Features.low_high_diff(candles)
        windowed = Features.low_high_past(transformed, window_size)
        withtime = Features.time_features(windowed)
        return withtime

    @staticmethod
    def low_high_diff(candles: pd.DataFrame):
        close_prev = candles["close"].shift(1)
        features = pd.DataFrame(index=candles.index)

        features["open"] = candles["open"] - close_prev
        features["high"] = candles["high"] - close_prev
        features["low"] = candles["low"] - close_prev
        features["close"] = candles["close"] - close_prev
        features['close_time_abs'] = candles.index.astype(np.int64)
        return features

    @staticmethod
    def low_high_past(candles: pd.DataFrame, window_size: int) -> pd.DataFrame:
        """
        Candles features: low/high values of past n candles
        """
        if not candles.index.is_monotonic:
            print("here")

        windowspec = f"{Features.period} {Features.freq}"
        rolling = candles.rolling(windowspec, min_periods=0).agg(
            {'open': lambda x: list(x)[0], 'high': 'max', 'low': 'min', 'close': lambda x: list(x)[-1], },
            closed='right')
        src_cols = ['open', 'high', 'low', 'close']
        df2 = candles[src_cols].copy()
        for shift in range(1, window_size + 1):
            target_cols = [f"-{shift}*{Features.period}{Features.freq}_{src_col}" for src_col in src_cols]
            df2[target_cols] = rolling.shift(shift - 1, Features.freq)[src_cols] \
                .reindex(df2.index, method='nearest', tolerance=windowspec)
        return df2.sort_index()

    @staticmethod
    def time_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add day, week to features
        :param df: dataframe with datetime column
        :return: dataframe with time features
        """
        df['close_time_abs'] = df.index.astype(np.int64)
        return df
        # df2["dayofweek"] = df['close_time'].dt.dayofweek
        # df2["month"] = df['close_time'].dt.month
        # df2["year"] = df['close_time'].dt.year
        # df2["hour"] = df['close_time'].dt.hour
        # df2["minute"] = df['close_time'].dt.minute
        # df2["sec"] = df['close_time'].dt.second
