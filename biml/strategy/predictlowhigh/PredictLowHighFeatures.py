import pandas as pd


class PredictLowHighFeatures:
    """
    Feature engineering for PredictLowHighStrategy
    """

    @staticmethod
    def features_of(bid_ask: pd.DataFrame, size):
        agg = bid_ask.resample("10s").agg({"bid": "min", "ask": "max", "bid_qty": "sum", "ask_qty": "sum"}, min_count=1)
        return agg.tail(size), None
