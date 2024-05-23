import logging
from typing import Dict

import pandas as pd
from ta import trend, momentum

from strategy.features.CandlesFeatures import CandlesFeatures


class MultiIndiFeatures:
    """ Multiple TA indicators on multiple periods"""
    _log = logging.getLogger("MultiIndiFeatures")
    default_params = {"cca": {"window": 20},
                      "ichimoku": {"window1": 9, "window2": 26, "window3": 52},
                      "adx": {"window": 14},
                      "rsi": {"window": 14},
                      "stoch": {"window": 14, "smooth_window": 3},
                      "macd": {"slow": 26, "fast": 12}
                      }

    @staticmethod
    def multi_indi_features_last(candles_by_periods: Dict[str, pd.DataFrame], n=1, params=None):
        max_candles = 52  # Ichimoku period is last todo: remove 2
        last_candles_by_periods = {period: candles.tail(max_candles) for period, candles in candles_by_periods.items()}
        return MultiIndiFeatures.multi_indi_features(last_candles_by_periods, params=params).tail(n)

    @staticmethod
    def multi_indi_features(candles_by_periods: Dict[str, pd.DataFrame], params=None):
        # Create time features
        if params is None:
            params = dict()
        min_period = min(candles_by_periods.keys(), key=pd.Timedelta)

        min_candles = candles_by_periods[min_period]

        # time
        time_features = CandlesFeatures.time_features_of(min_candles)[['time_hour', 'time_minute']]
        MultiIndiFeatures._log.debug(f"time features:\n{time_features.tail()}")

        # Indicators
        indicators_features = []
        for period, candles in candles_by_periods.items():
            period_indicators = MultiIndiFeatures.indicators_of(candles, period,
                                                                params.get(period, MultiIndiFeatures.default_params))
            indicators_features.append(period_indicators)
            MultiIndiFeatures._log.debug(f"Indicators of period: {period}\n{period_indicators.tail()}")

        # Concat time and indicators columns
        features = pd.concat([time_features] + indicators_features, axis=1).sort_index().ffill()
        MultiIndiFeatures._log.debug(f"Resulted features with nans:\n{features.tail()}")
        features = features.dropna()
        MultiIndiFeatures._log.debug(f"Resulted features dropna:\n{features.tail()}")
        return features

    @staticmethod
    def ichimoku_of(candles: pd.DataFrame, period: str = '', window1=9, window2=26, window3=52):
        df = pd.DataFrame()
        ichimoku = trend.IchimokuIndicator(candles['high'], candles['low'],
                                           window1=window1, window2=window2, window3=window3, fillna=False)
        df[f'ichimoku_base_line_{period}_diff'] = ichimoku.ichimoku_base_line().diff()
        df[f'ichimoku_conversion_line_{period}_diff'] = ichimoku.ichimoku_conversion_line().diff()
        df[f'ichimoku_a_{period}_diff'] = ichimoku.ichimoku_a().diff()
        df[f'ichimoku_b_{period}_diff'] = ichimoku.ichimoku_b().diff()
        return df

    @staticmethod
    def indicators_of(candles, period, params: dict):
        """ Single period indicators"""

        if not params:
            params = MultiIndiFeatures.default_params

        resampled = candles.resample(period).agg(
            {'high': 'max', 'low': 'min', 'open': 'first', 'close': 'last', 'vol': 'sum'})
        df = MultiIndiFeatures.ichimoku_of(resampled,
                                           period,
                                           params["ichimoku"]["window1"],
                                           params["ichimoku"]["window2"],
                                           params["ichimoku"]["window3"], )
        df[f'cci_{period}_diff'] = trend.cci(resampled['high'], resampled['low'], resampled['close'],
                                             window=params["cca"]["window"], fillna=False).diff()
        df[f'adx_{period}_diff'] = trend.adx(resampled['high'], resampled['low'], resampled['close'],
                                             window=params["adx"]["window"], fillna=False).diff()
        df[f'rsi_{period}_diff'] = momentum.rsi(resampled['close'], window=params["rsi"]["window"], fillna=False).diff()
        df[f'stoch_{period}_diff'] = momentum.stoch(resampled['high'], resampled['low'], resampled['close'],
                                                    window=params["stoch"]["window"],
                                                    smooth_window=params["stoch"]["smooth_window"], fillna=False).diff()
        df[f'macd_{period}_diff'] = trend.macd(resampled['close'], window_slow=params["macd"]["slow"],
                                               window_fast=params["macd"]["fast"], fillna=False).diff()
        df = df.dropna()
        return df
