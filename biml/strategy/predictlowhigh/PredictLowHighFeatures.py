import numpy as np
import pandas as pd

from strategy.predictlowhigh.BidAskFeatures import BidAskFeatures
from strategy.predictlowhigh.Level2Features import Level2Features


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """
    default_predict_window = "10s"

    @staticmethod
    def last_data_of(bid_ask: pd.DataFrame, level2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        """ @:return (last bid ask value, last level2 values just before last bid ask)"""
        last_bid_ask = bid_ask.tail(1)
        last_level2 = level2[level2.index <= last_bid_ask.index.max()]
        return last_bid_ask, last_level2

    @staticmethod
    def last_features_of(bid_ask: pd.DataFrame, level2: pd.DataFrame) -> pd.DataFrame:
        # Need 2 last records because features contain diff.
        last_bid_ask = bid_ask.tail(2)
        last_level2 = level2[level2.index <= last_bid_ask.index.max()]
        return PredictLowHighFeatures.features_of(last_bid_ask, last_level2)

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame, level2: pd.DataFrame, predict_window=default_predict_window) \
            -> (pd.DataFrame, pd.DataFrame):
        features = PredictLowHighFeatures.features_of(bid_ask, level2)
        targets = PredictLowHighFeatures.targets_of(bid_ask, predict_window)
        merged = pd.merge_asof(features, targets, left_index=True, right_index=True, direction="forward") \
            .dropna()
        features = merged[features.columns]
        targets = merged[targets.columns]
        return features, targets

    @staticmethod
    def features_of(bid_ask: pd.DataFrame, level2: pd.DataFrame):
        if bid_ask.empty or level2.empty:
            return pd.DataFrame()
        l2_features = Level2Features().level2_buckets(level2)
        bid_ask_features = pd.merge(BidAskFeatures.time_features_of(bid_ask),
                                    BidAskFeatures.bid_ask_features_of(bid_ask),
                                    left_index=True, right_index=True)
        features = pd.merge_asof(bid_ask_features, l2_features, left_index=True, right_index=True, direction="backward")
        # features.drop(["symbol", "datetime"], axis=1, inplace=True)
        return features.dropna()

    @staticmethod
    def targets_of(bid_ask: pd.DataFrame, predict_window: str = default_predict_window) -> pd.DataFrame:
        # Calculate <bid or ask>_<min or max>_fut
        rolling_max = bid_ask[["bid", "ask"]][::-1].rolling(predict_window, closed='right').max()[::-1] \
            .add_suffix("_max_fut")
        rolling_min = bid_ask[["bid", "ask"]][::-1].rolling(predict_window, closed='right').min()[::-1] \
            .add_suffix("_min_fut")
        merged = pd.concat([rolling_min, rolling_max], axis=1)
        prediction_bound = bid_ask.index.max() - pd.to_timedelta(predict_window)
        merged.loc[merged.index > prediction_bound] = [np.nan] * len(merged.columns)

        # To avoid predictions when min_fut > max_fut, targets are: bid_max_fut, bid_spread_fut, ask_min_fut, ask_spread_fut
        return pd.DataFrame({
            # Max future bid, min future ask
            "bid_max_fut_diff": merged["bid_max_fut"] - bid_ask["bid"],
            "bid_spread_fut": merged["bid_max_fut"] - merged["bid_min_fut"],

            "ask_min_fut_diff": merged["ask_min_fut"] - bid_ask["ask"],
            "ask_spread_fut": merged["ask_max_fut"] - merged["ask_min_fut"]}) \
            .dropna()
