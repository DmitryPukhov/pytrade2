import numpy as np
import pandas as pd
import datetime as dt
from strategy.predictlowhigh.Level2Features import Level2Features


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame):
        # Features is aggregated order book
        features = Level2Features().level2_buckets(bid_ask)
        features[bid_ask.columns] = bid_ask

        targets = PredictLowHighFeatures.targets_of(bid_ask)
        return features, targets

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
        merged.loc[merged.index > prediction_bound,predicted_columns] = [np.nan]*len(predicted_columns)
        return merged
