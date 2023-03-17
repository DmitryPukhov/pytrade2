import numpy as np
import pandas as pd
import datetime as dt
from strategy.predictlowhigh.Level2Features import Level2Features


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame, level2: pd.DataFrame):
        # todo: merge them to have the same datetime index
        features = PredictLowHighFeatures.features_of(bid_ask, level2)
        targets = PredictLowHighFeatures.targets_of(bid_ask).dropna()
        merged = pd.merge_asof(features, targets, left_index=True, right_index=True, direction="forward")
        features = merged[features.columns]
        targets = merged[targets.columns]
        return features, targets

    @staticmethod
    def features_of(bid_ask: pd.DataFrame, level2: pd.DataFrame):
        l2_features = Level2Features().level2_buckets(level2)
        features = pd.merge_asof(bid_ask, l2_features, left_index=True, right_index=True, direction="backward")
        features.drop(["symbol", "datetime"], axis=1, inplace=True)
        return features

    @staticmethod
    def targets_of(bid_ask: pd.DataFrame, predict_window="10s") -> pd.DataFrame:
        future: pd.DataFrame = bid_ask.rolling(predict_window) \
            .agg({"bid": "max", "ask": "min", "bid_vol": "sum", "ask_vol": "sum"}) \
            .rename(columns={'bid': 'bid_fut', 'ask': 'ask_fut', 'bid_vol': 'bid_vol_fut', 'ask_vol': 'ask_vol_fut'}) \
            .shift(-1, predict_window)
        merged = pd.merge_asof(bid_ask, future, left_index=True, right_index=True, direction='backward')
        # completed_bound=bid_ask.index.max()-pd.to_timedelta(predict_window)
        # todo: remove tail when prediction window is not completed yet
        # pd.to_timedelta(predict_window)
        prediction_bound = bid_ask.index.max() - pd.to_timedelta(predict_window)

        predicted_columns = list(filter(lambda col: col.endswith('_fut'), merged.columns))
        merged.loc[merged.index > prediction_bound, predicted_columns] = [np.nan] * len(predicted_columns)
        return merged[["bid_fut", "ask_fut"]]
