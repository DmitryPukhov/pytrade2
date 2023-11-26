from typing import Dict
import pandas as pd
from strategy.features.CandlesFeatures import CandlesFeatures
from strategy.features.Level2Features import Level2Features


class LongCandleFeatures:

    @staticmethod
    def features_targets_of(
                            candles_by_periods: Dict[str, pd.DataFrame],
                            cnt_by_period: Dict[str, int],
                            level2: pd.DataFrame,
                            level2_past_window: str,
                            target_period: str,
                            loss_min_coeff: float,
                            profit_min_coeff: float) -> (pd.DataFrame, pd.DataFrame):

        # Candles + level2 features
        features = CandlesFeatures.candles_combined_features_of(candles_by_periods, cnt_by_period).dropna()
        features = CandlesFeatures.time_features_of(features)
        l2features = Level2Features().level2_buckets(level2, level2_past_window)
        # Merge backward
        features = pd.merge_asof(features, l2features, left_index=True, right_index=True).dropna()

        # Get targets - movements
        targets_src = candles_by_periods[target_period]
        targets = LongCandleFeatures.targets_of(targets_src, loss_min_coeff, profit_min_coeff).dropna()

        common_index = features.index.intersection(targets.index)
        features_wo_targets = features[common_index.empty | (features.index > common_index.max())]
        features_with_targets, targets = features.loc[common_index], targets.loc[common_index]

        return features_with_targets, targets, features_wo_targets

    @staticmethod
    def targets_of(candles: pd.DataFrame, loss_min_coeff: float, profit_min_coeff: float):
        """ One hot encoded signal: buy signal if next candle moves up, sell if down, none if not buy and not sell"""

        # Next candle
        next_ = candles.shift(-1)

        targets = pd.DataFrame(index=candles.index)
        # Up move
        is_sl_up = next_["low"] > candles["close"] * (1 - loss_min_coeff)
        is_tp_up = next_["high"] >= candles["close"] * (1 + profit_min_coeff)
        targets["buy"] = is_sl_up & is_tp_up

        # Down move
        is_sl_down = next_["high"] < candles["close"] * (1 + loss_min_coeff)
        is_tp_down = next_["low"] <= candles["close"] * (1 - profit_min_coeff)
        targets["sell"] = is_sl_down & is_tp_down

        # targets["none"] = ~ targets["buy"] & ~ targets["sell"]
        targets["signal"] = targets["buy"].astype(int) - targets["sell"].astype(int)
        return targets[["signal"]].iloc[:-1]
