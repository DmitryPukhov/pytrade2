import pandas as pd


class Level2Indicators:
    def volume(self, level2_df: pd.DataFrame) -> int:
        """ Total volume per order book"""
        bid_ask_volumes = level2_df \
                .groupby("datetime") \
                .agg({"ask_vol": "sum", "bid_vol":"sum"})
        bid_ask_volumes["level2_vol"] = bid_ask_volumes["ask_vol"] + bid_ask_volumes["bid_vol"]

        return bid_ask_volumes[["bid_vol", "ask_vol", "level2_vol"]]

