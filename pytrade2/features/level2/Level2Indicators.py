import pandas as pd


class Level2Indicators:
    def expectation(self, level2_df: pd.DataFrame):
        """ Expectations with volumes"""
        # temp columns to calc expectations
        df = level2_df.set_index("datetime")
        df["bid_vol_mult"] = df["bid"] * df["bid_vol"]
        df["ask_vol_mult"] = df["ask"] * df["ask_vol"]

        df["bidask"] = df["bid"].fillna(df["ask"])
        df["bidask_vol"] = df["bid_vol"].fillna(df["ask_vol"])
        df["bidask_vol_mult"] = df["bidask"] * df["bidask_vol"]

        # Group by time to get order books, aggregate order books
        df = df.groupby("datetime").agg(
            bid = ("bid", "sum"),
            bid_vol = ("bid_vol", "sum"),
            bid_vol_mult = ("bid_vol_mult", "sum"),
            bid_max=("bid", "max"),

            ask=("ask", "sum"),
            ask_vol=("ask_vol", "sum"),
            ask_vol_mult = ("ask_vol_mult", "sum"),
            ask_min=("ask", "min"),

            bidask_vol_mult = ("bidask_vol_mult", "sum")

        )

        # calc expectations
        df["bid_expect"] = df["bid_vol_mult"]/df["bid_vol"]
        df["ask_expect"] = df["ask_vol_mult"]/df["ask_vol"]
        df["bid_ask_expect"] = df["bidask_vol_mult"] / (df["bid_vol"] + df["ask_vol"])

        df.columns = [f"l2_{col}" for col in df.columns]
        # Return without temp columns
        return df[["l2_bid_max", "l2_bid_vol",  "l2_bid_expect",
                   "l2_ask_min","l2_ask_vol","l2_ask_expect", "l2_bid_ask_expect"]]


