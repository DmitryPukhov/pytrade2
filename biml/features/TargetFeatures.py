import pandas as pd
import numpy as np


class TargetFeatures:
    """
    Target features engineering
    """

    def signal(self, df: pd.DataFrame, loss: int, trailing: int, ratio: int) -> pd.DataFrame:
        """
        Add target feature: signal to buy, sell or off market
        :param df: pandas dataframe with candles and future low/high features.
        """
        # Default signal = off market
        df["signal"] = 0

        # Set buy signal
        buy_profit = (df["fut_high"] - df["close"]) - trailing
        buy_loss = (df["close"] - df["fut_low"]).abs()
        buy_ratio = buy_profit / buy_loss
        buy_loss_is_small = (buy_loss <= loss)
        is_buy = (buy_ratio >= ratio) & buy_loss_is_small
        df["signal"] = np.where(is_buy, 1, df["signal"])

        # Set sell signal
        sell_profit = (df["close"] - df["fut_low"]) - trailing
        sell_loss = (df["fut_high"] - df["close"]).abs()
        sell_ratio = sell_profit / sell_loss
        sell_loss_is_small = (sell_loss <= loss)
        is_sell = (sell_ratio >= ratio) & sell_loss_is_small
        df["signal"] = np.where(is_sell, -1, df["signal"])
        return df

    def future_low_high(self, df: pd.DataFrame, periods: int, freq: str) -> pd.DataFrame:
        """
        Add target features: low and high price during future window
        :param df: pandas dataframe with candles
        :param freq : time unit for future window
        :param periods: duration of future window in given time units
        """
        # Trick to implement forward rolling window for timeseries with unequal intervals:
        # reverse, apply rolling, then reverse back
        windowspec = f'{periods} {freq}'
        # df2 = df.reset_index(level='ticker', drop=True)
        df2: pd.DataFrame = df[['high', 'low']].sort_index(
            ascending=False).rolling(windowspec, min_periods=0).agg(
            {'high': 'max', 'low': 'min'}, closed='right')
        df2.rename({'high': 'fut_high', 'low': 'fut_low'}, inplace=True)
        df2[['fut_high', 'fut_low']] = df2[['high', 'low']]
        df2.drop(['high', 'low'], axis=1, inplace=True)
        return df2.sort_index()
