from typing import Dict

import numpy as np
import pandas as pd


class CandlesFeatures:

    @staticmethod
    def rolling_candles_by_periods(candles_1min: pd.DataFrame, periods: list[str]) -> dict[str, pd.DataFrame]:
        out = {}
        for period in periods:
            out[period] = CandlesFeatures.rolling_ohlc(candles_1min, period)
        return out

    @staticmethod
    def rolling_ohlc(candles_1min, window):
        """ Unlike resample, rolling window does not work with first, last aggregations and with datetime there.
            Some hacks needed """
        df = candles_1min.copy()
        # Convert datetime to numeric because rolling window does not work with times for getting first in aggregation
        df['open_time'] = pd.to_datetime(df['open_time']).astype('int64')
        df['close_time'] = pd.to_datetime(df['close_time']).astype('int64')
        # Aggregate
        df = df.rolling(window, closed = 'both').agg({
            'open_time': 'min',
            'close_time': 'max',      # Equivalent to 'last' for time
            'open': lambda x: x.iloc[0],    # First value
            'high': 'max',
            'low': 'min',
            'close': lambda x: x.iloc[-1],   # Last value
            'vol': 'sum'})
        df['open_time'] = pd.to_datetime(df['open_time'], unit = 'ns')
        df['close_time'] = pd.to_datetime(df['close_time'], unit = 'ns')

        return df

    @staticmethod
    def rolling_candles_of_bid_ask(bid_ask: pd.DataFrame, period: str = "1min"):
        """ Calculate candles from bidask data. Consider price as (bid+ask)/2 """

        df = pd.DataFrame(bid_ask)
        df["price"] = (df["bid"] + df["ask"])/2
        df["vol"] = df["bid_vol"] + df["ask_vol"]

        # datetime hack to avooid numeric type error when rolling agg
        df["datetime"] = df["datetime"].astype('int64')

        df = df[[ "datetime", "price", "vol"]].rolling(period, closed = "right").agg({
            'datetime': [lambda x: x.iloc[0], lambda x: x.iloc[-1]],     # Open/close times
            'price': [lambda x: x.iloc[0], 'max', 'min', lambda x: x.iloc[-1]],
            'vol': 'sum',
        })
        df.columns = [
            'open_time',   # First datetime
            'close_time',  # Last datetime
            'open',        # First price
            'high',        # Max price
            'low',         # Min price
            'close',       # Last price
            'vol',         # Summed volume
        ]
        # Set back to datetime
        df["open_time"] = pd.to_datetime(df["open_time"])
        df["close_time"] = pd.to_datetime(df["close_time"])
        df.set_index("close_time", inplace=True, drop=False)
        return df

    @staticmethod
    def candles_last_combined_features_of(candles_by_periods: Dict[str, pd.DataFrame],
                                          cnt_by_period: Dict[str, int]) -> pd.DataFrame:
        # todo: optimize
        return CandlesFeatures.candles_combined_features_of(candles_by_periods, cnt_by_period).tail(1)

    @staticmethod
    def candles_combined_features_of(candles_by_periods: Dict[str, pd.DataFrame],
                                     cnt_by_period: Dict[str, int]) -> pd.DataFrame:
        """ Combine feature columns from fast and slow candles to single dataframe """
        if not candles_by_periods:
            return pd.DataFrame()

        merged = None
        for period, candles in candles_by_periods.items():
            features = CandlesFeatures.candles_features_of(candles_by_periods[period], period, cnt_by_period[period])
            merged = pd.merge_asof(merged, features, left_index=True, right_index=True) \
                if merged is not None else features
        return merged.dropna()

    @staticmethod
    def candles_features_of(candles: pd.DataFrame, interval: str, window_size: int):
        cols = ["open", "high", "low", "close", "vol"]
        features = candles.copy().reset_index(drop=True)[cols + ["close_time"]]

        # Add previous window candles to columns
        concat_features = [features]
        for i in range(1, window_size):
            prefix = f"{interval}_-{i}_"
            prev_cols_map = {col: prefix + col for col in cols}
            prev_features = features[cols].shift(i).rename(prev_cols_map, axis=1)
            concat_features.append(prev_features)
        features = pd.concat(concat_features, axis=1).set_index("close_time", drop=True)

        # Add prefix to ohlcv columns
        for col in cols:
            features.rename(columns={col: f"{interval}_{col}"}, inplace=True)

        return features.diff().dropna()

    @staticmethod
    def time_features_of(df: pd.DataFrame):
        #dt = df.index.to_frame()["close_time"].dt
        dt = df.index
        df["time_hour"] = dt.hour
        df["time_minute"] = dt.minute
        df["time_second"] = dt.second
        return df
