import numpy as np
import pandas as pd


class BidAskFeatures:

    @staticmethod
    def bid_ask_features_of(bid_ask: pd.DataFrame, past_window: str) -> pd.DataFrame:
        df = bid_ask[[]].copy()  # df without columns, just bidask index
        agg = bid_ask.rolling(past_window).agg({"bid": "mean", "bid_vol": "sum", "ask": "mean", "ask_vol": "sum"})
        # Differences instead of absolute values
        bidask_cols = ["bid", "bid_vol", "ask", "ask_vol"]
        df[[f"{c}_diff" for c in bidask_cols]] = agg[bidask_cols].diff()
        df["spread"] = agg["ask"] - bid_ask["bid"]
        return df

    @staticmethod
    def time_features_of(bid_ask: pd.DataFrame):
        dt = bid_ask.index.to_frame()["datetime"].dt
        df = bid_ask[[]].copy()
        df["time_hour"] = dt.hour
        df["time_minute"] = dt.minute
        df["time_second"] = dt.second
        df["time_day_of_week"] = dt.dayofweek
        df["time_diff"] = bid_ask.index.to_frame().diff().dropna().astype(np.int64)
        return df
