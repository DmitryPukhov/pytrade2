import numpy as np
import pandas as pd


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame):

        agg = bid_ask.resample("10s") \
            .agg({"bid": "min", "ask": "max", "bid_qty": "sum", "ask_qty": "sum"}) \
            .ffill()

        return agg, None
