from typing import Dict

import numpy as np
import pandas as pd


class CandlesFeatures:

    @staticmethod
    def features_targets_of(candles_by_periods: Dict[str, pd.DataFrame],
                            cnt_by_period: Dict[str, int], target_period: str) -> (pd.DataFrame, pd.DataFrame):

        # Candles features -
        features = CandlesFeatures.candles_combined_features_of(candles_by_periods, cnt_by_period)

        # Get targets - movements
        targets_src = candles_by_periods[target_period]
        targets = CandlesFeatures.targets_of(targets_src)

        common_index = features.index.intersection(targets.index)

        return features.loc[common_index], targets.loc[common_index]

    @staticmethod
    def targets_of(candles: pd.DataFrame):
        """ One hot encoded signal: buy signal if next 2 candles moves up, sell if down, none if not buy and not sell"""

        # Next 2 candles
        next1 = candles.shift(-1)
        next2 = candles.shift(-2)

        targets = pd.DataFrame(index=candles.index)
        # Up move
        next1up = next1["low"] > (candles["low"] + (candles["high"] - candles["low"]) / 2)
        next2up = next2["low"] > (next1["low"] + (next1["high"] - next1["low"]) / 2)
        targets["buy"] = next1up & next2up

        # Down move
        next1down = next1["high"] < (candles["high"] - (candles["high"] - candles["low"]) / 2)
        next2down = next2["high"] < (next1["high"] - (next1["high"] - next1["low"]) / 2)
        targets["sell"] = next1down & next2down

        #targets["none"] = ~ targets["buy"] & ~ targets["sell"]
        targets["signal"] = targets["buy"].astype(int) - targets["sell"].astype(int)
        return targets[["signal"]].iloc[:-2]

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
        return merged

    @staticmethod
    def candles_features_of(candles: pd.DataFrame, interval: str, window_size: int):
        cols = ["open", "high", "low", "close", "vol"]
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
