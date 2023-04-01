import pandas as pd


class BidAskFeatures:

    @staticmethod
    def bid_ask_features_of(bid_ask: pd.DataFrame) -> pd.DataFrame:
        df = bid_ask[["ask"]]
        df["spread"] = bid_ask["ask"] - bid_ask["bid"]
        # Differences instead of absolute values
        diff_cols = ["bid", "bid_vol", "ask", "ask_vol"]
        df[[f"{c}_diff" for c in diff_cols]] = bid_ask[diff_cols].diff()
        return df

    @staticmethod
    def time_features_of(bid_ask: pd.DataFrame):
        dt = bid_ask.index.to_frame()["datetime"].dt
        df = bid_ask[[]]
        df["time_hour"] = dt.hour
        df["time_minute"] = dt.minute
        df["time_second"] = dt.second
        df["time_day_of_week"] = dt.dayofweek
        df["time_diff"] = bid_ask.index.to_frame().diff()
        return df
