from typing import Dict

import pandas as pd


class CandlesFeatures:

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
        return merged
        #
        # fast_features = CandlesFeatures.candles_features_of(fast_candles, fast_interval, fast_window_size)
        # slow_features = CandlesFeatures.candles_features_of(slow_candles, slow_interval, slow_window_size)
        # return pd.merge_asof(fast_features, slow_features, left_index=True, right_index=True)

    # @staticmethod
    # def candles_combined_features_of(fast_candles: pd.DataFrame, fast_window_size, slow_candles: pd.DataFrame,
    #                                  slow_window_size) -> pd.DataFrame:
    #     """ Combine feature columns from fast and slow candles to single dataframe """
    #     if fast_candles.empty or slow_candles.empty:
    #         return pd.DataFrame()
    #     fast_interval = fast_candles["interval"][-1]
    #     slow_interval = slow_candles["interval"][-1]
    #
    #     fast_features = CandlesFeatures.candles_features_of(fast_candles, fast_interval, fast_window_size)
    #     slow_features = CandlesFeatures.candles_features_of(slow_candles, slow_interval, slow_window_size)
    #     return pd.merge_asof(fast_features, slow_features, left_index=True, right_index=True)

    @staticmethod
    def candles_features_of(candles: pd.DataFrame, interval: str, window_size: int):
        cols = ["open", "high", "low", "close", "vol"]
        #features = candles.copy().reset_index(drop=False)[cols + ["close_time"]]
        features = candles.copy().reset_index(drop=True)[cols + ["close_time"]]

        # Add previous window candles to columns
        for i in range(1, window_size):
            prefix = f"{interval}_-{i}_"
            for col in cols:
                features[prefix + col] = features.shift(i)[col]
        features.set_index("close_time", inplace=True, drop=True)

        # Add prefix to ohlcv columns
        for col in cols:
            features.rename(columns={col: f"{interval}_{col}"}, inplace=True)

        return features.diff().dropna()
