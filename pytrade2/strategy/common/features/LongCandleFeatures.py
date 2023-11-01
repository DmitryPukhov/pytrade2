from typing import Dict

import numpy as np
import pandas as pd

from strategy.common.features.CandlesFeatures import CandlesFeatures


class LongCandleFeatures:

    @staticmethod
    def features_targets_of(candles_by_periods: Dict[str, pd.DataFrame],
                            cnt_by_period: Dict[str, int],
                            target_period: str,
                            profit_min_coeff: float) -> (pd.DataFrame, pd.DataFrame):
        # Candles features -
        features = CandlesFeatures.candles_combined_features_of(candles_by_periods, cnt_by_period).dropna()
        features = LongCandleFeatures.time_features_of(features)
        # Get targets - movements
        targets_src = candles_by_periods[target_period]
        targets = LongCandleFeatures.targets_of(targets_src, profit_min_coeff).dropna()

        common_index = features.index.intersection(targets.index)
        features_wo_targets = features[features.index > common_index.max()]
        features_with_targets, targets = features.loc[common_index], targets.loc[common_index]

        return features_with_targets, targets, features_wo_targets

    @staticmethod
    def time_features_of(df: pd.DataFrame):
        dt = df.index.to_frame()["close_time"].dt
        df["time_hour"] = dt.hour
        df["time_minute"] = dt.minute
        df["time_second"] = dt.second
        return df

    @staticmethod
    def targets_of(candles: pd.DataFrame, profit_min_coeff: float):
        """ One hot encoded signal: buy signal if next 2 candles moves up, sell if down, none if not buy and not sell"""

        # Next 2 candles
        next1 = candles.shift(-1)
        next2 = candles.shift(-2)

        targets = pd.DataFrame(index=candles.index)
        # Up move
        next1up = next1["low"] > (candles["low"] + (candles["high"] - candles["low"]) / 2)
        next2up = next2["low"] > (next1["low"] + (next1["high"] - next1["low"]) / 2)
        profitup = next2["high"] >= candles["close"] * (1 + profit_min_coeff)
        targets["buy"] = next1up & next2up & profitup

        # Down move
        next1down = next1["high"] < (candles["high"] - (candles["high"] - candles["low"]) / 2)
        next2down = next2["high"] < (next1["high"] - (next1["high"] - next1["low"]) / 2)
        profitdown = next2["low"] <= candles["close"] * (1 - profit_min_coeff)
        targets["sell"] = next1down & next2down & profitdown

        # targets["none"] = ~ targets["buy"] & ~ targets["sell"]
        targets["signal"] = targets["buy"].astype(int) - targets["sell"].astype(int)
        return targets[["signal"]].iloc[:-2]
