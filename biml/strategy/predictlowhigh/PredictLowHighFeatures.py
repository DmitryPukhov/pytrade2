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
        future: pd.DataFrame = bid_ask.rolling(predict_window, closed='left') \
            .agg({"bid": "min", "ask": "max"}) \
            .rename(columns={'bid': 'bid_fut', 'ask': 'ask_fut'}) \
            .shift(-1, predict_window)
        merged = pd.merge_asof(bid_ask, future, left_index=True, right_index=True, direction='backward')
        # completed_bound=bid_ask.index.max()-pd.to_timedelta(predict_window)
        # todo: remove tail when prediction window is not completed yet
        # pd.to_timedelta(predict_window)
        prediction_bound = bid_ask.index.max() - pd.to_timedelta(predict_window)

        predicted_columns = list(filter(lambda col: col.endswith('_fut'), merged.columns))
        merged.loc[merged.index > prediction_bound, predicted_columns] = [np.nan] * len(predicted_columns)
        return merged[["bid_fut", "ask_fut"]].diff().rename(
            columns={"bid_fut": "bid_diff_fut", "ask_fut": "ask_diff_fut"}).dropna()
