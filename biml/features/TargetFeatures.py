import pandas as pd


class TargetFeatures:
    """
    Target features engineering
    """

    def low_high_future(self, df: pd.DataFrame, periods: int, freq: str) -> pd.DataFrame:
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
        df2.rename({'high': 'fut_high_max', 'low': 'fut_low_min'}, inplace=True)
        df2[['fut_high_max', 'fut_low_min']] = df2[['high', 'low']]
        df2.drop(['high', 'low'], axis=1, inplace=True)
        return df2.sort_index()
