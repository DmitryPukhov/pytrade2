import pandas as pd
from ta import trend, momentum

from pytrade2.features.CandlesMultiIndiFeatures import CandlesMultiIndiFeatures


class Level2MultiIndiFeatures:

    @staticmethod
    def level2_ichimoku_of(df_level2: pd.DataFrame, period: str = '', window1=9,
                           window2=26, window3=52):
        df = pd.DataFrame()
        ichimoku = trend.IchimokuIndicator(df_level2['l2_ask_expect'],
                                           df_level2['l2_bid_expect'],
                                           window1=window1, window2=window2,
                                           window3=window3, fillna=False)
        df[
            f'l2_ichimoku_base_line_{period}_diff'] = ichimoku.ichimoku_base_line().diff()
        df[
            f'l2_ichimoku_conversion_line_{period}_diff'] = ichimoku.ichimoku_conversion_line().diff()
        df[f'l2_ichimoku_a_{period}_diff'] = ichimoku.ichimoku_a().diff()
        df[f'l2_ichimoku_b_{period}_diff'] = ichimoku.ichimoku_b().diff()
        # df[f'l2_ichimoku_base_line_{period}'] = ichimoku.ichimoku_base_line()
        # df[f'l2_ichimoku_conversion_line_{period}'] = ichimoku.ichimoku_conversion_line()
        # df[f'l2_ichimoku_a_{period}'] = ichimoku.ichimoku_a()
        # df[f'l2_ichimoku_b_{period}'] = ichimoku.ichimoku_b()
        return df

    @staticmethod
    def rolling_level2(df_level2, window):
        """ Unlike resample, rolling window does not work with first, last aggregations and with datetime there.
            Some hacks needed """
        df = df_level2.copy()
        # Convert datetime to numeric because rolling window does not work with times for getting first in aggregation
        df['datetime'] = pd.to_datetime(df['datetime']).astype('int64')
        # Aggregate
        df = df.rolling(window, closed='right').mean()
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ns')

        return df

    @staticmethod
    def level2_indicators_of(df_level2, period, params: dict = CandlesMultiIndiFeatures.default_params):
        """ Single period indicators"""

        resampled = df_level2.resample("1min", closed="right", label="right").agg("mean")
        # resampled = rolling_level2(df_level2, period)
        resampled["l2_price"] = (resampled["l2_bid_max"] + resampled[
            "l2_ask_min"]) / 2

        df = Level2MultiIndiFeatures.level2_ichimoku_of(resampled,
                                                        period,
                                                        params["ichimoku"][
                                                            "window1"],
                                                        params["ichimoku"][
                                                            "window2"],
                                                        params["ichimoku"][
                                                            "window3"], )
        df[f'l2_cci_{period}_diff'] = trend.cci(resampled['l2_ask_expect'],
                                                resampled['l2_bid_expect'],
                                                resampled['l2_price'],
                                                window=params["cca"]["window"],
                                                fillna=False).diff()
        df[f'l2_adx_{period}_diff'] = trend.adx(resampled['l2_ask_expect'],
                                                resampled['l2_bid_expect'],
                                                resampled['l2_price'],
                                                window=params["adx"]["window"],
                                                fillna=False).diff()
        df[f'l2_rsi_{period}_diff'] = momentum.rsi(resampled['l2_price'],
                                                   window=params["rsi"]["window"],
                                                   fillna=False).diff()
        df[f'l2_stoch_{period}_diff'] = momentum.stoch(resampled['l2_ask_expect'],
                                                       resampled['l2_bid_expect'],
                                                       resampled['l2_price'],
                                                       window=params["stoch"][
                                                           "window"],
                                                       smooth_window=
                                                       params["stoch"][
                                                           "smooth_window"],
                                                       fillna=False).diff()
        df[f'l2_macd_{period}_diff'] = trend.macd(resampled['l2_price'],
                                                  window_slow=params["macd"][
                                                      "slow"],
                                                  window_fast=params["macd"][
                                                      "fast"], fillna=False).diff()

        # df = df.dropna()
        return df

    @staticmethod
    def level2_features_of(df_level2, periods, params):
        # df = df_level2.copy()
        # del df["datetime"]

        level2_features = None
        for period in periods:
            period_indicators = Level2MultiIndiFeatures.level2_indicators_of(
                df_level2, period, params.get(period, CandlesMultiIndiFeatures.default_params))
            if level2_features is None:
                level2_features = period_indicators
            else:
                level2_features = pd.merge(
                    level2_features,
                    period_indicators,
                    left_index=True,
                    right_index=True,
                    how='outer'
                )
        return level2_features.dropna().sort_index()
