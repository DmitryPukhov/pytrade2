import pandas as pd


class Level2Indicators:
    def expectation(self, level2_df: pd.DataFrame):
        """ Price expectation - sum(price * vol)/sum(vol)"""
        # bid+ask volumes
        level2_df = level2_df.set_index("datetime", drop = False)
        df = pd.DataFrame(index = level2_df["datetime"])
        df["price"] = level2_df["bid"].fillna(level2_df["ask"])
        df["vol"] = level2_df["bid_vol"].fillna(level2_df["ask_vol"])
        df["price_vol"] = df["price"] * df["vol"]
        df_grouped = df.groupby("datetime").agg({"price_vol": "sum", "vol": "sum"})
        df_grouped["l2_expectation"] = df_grouped["price_vol"] / df_grouped["vol"]
        return df_grouped[["l2_expectation"]]

    def volume(self, level2_df: pd.DataFrame) -> int:
        """ Total volume per order book"""
        bid_ask_volumes = level2_df \
                .groupby("datetime") \
                .agg({"ask_vol": "sum", "bid_vol":"sum"})
        bid_ask_volumes["level2_vol"] = bid_ask_volumes["ask_vol"] + bid_ask_volumes["bid_vol"]

        return bid_ask_volumes[["bid_vol", "ask_vol", "level2_vol"]]

