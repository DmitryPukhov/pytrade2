import numpy as np
import pandas as pd

from strategy.predictlowhigh.Level2Features import Level2Features


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame):
        # Features is aggregated order book
        features = Level2Features().level2_buckets(bid_ask)
        targets = PredictLowHighFeatures.targets_of(bid_ask)

        # Targets are future min/max
        agg = bid_ask.resample("10s") \
            .agg({"bid": "max", "ask": "min", "bid_vol": "sum", "ask_vol": "sum"}) \
            .ffill()

        return agg, None

    @staticmethod
    def targets_of(bid_ask: pd.DataFrame, predict_window="10s"):

        agg = bid_ask.resample(predict_window) \
            .agg({"bid": "max", "ask": "min", "bid_vol": "sum", "ask_vol": "sum"}) \
            .ffill()
        return agg
