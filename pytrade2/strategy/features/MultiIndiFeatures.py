from typing import List, Dict, re

import pandas as pd
from ta import trend, momentum, volume, others, volatility

from strategy.features.CandlesFeatures import CandlesFeatures


class MultiIndiFeatures:
    """ Multiple TA indicators on multiple periods"""

    @staticmethod
    def multi_indi_features_last(candles_by_periods: Dict[str, pd.DataFrame], n=1):
        max_candles = 52  # Ichimoku period is last
        last_candles_by_periods = {period: candles.tail(max_candles) for period, candles in candles_by_periods.items()}
        return MultiIndiFeatures.multi_indi_features(last_candles_by_periods).tail(n)

    @staticmethod
    def multi_indi_features(candles_by_periods: Dict[str, pd.DataFrame]):
        # Create time features
        min_period = min(candles_by_periods.keys(), key=pd.Timedelta)

        min_candles = candles_by_periods[min_period]
        time_features = CandlesFeatures.time_features_of(min_candles)[['time_hour', 'time_minute']]
        # indi_features = pd.concat([indi_features] + [indicators_of(candles,period) for period in ['1min', '5min', '15min', '30min', '60min']], axis=1).ffill()
        # Enrich with columns from each period
        features = pd.concat(
            [time_features] + [MultiIndiFeatures.indicators_of(candles, period) for period, candles in
                               candles_by_periods.items()],
            axis=1).sort_index().ffill().dropna()
        return features

    @staticmethod
    def ichimoku_of(candles: pd.DataFrame, period: str = ''):
        df = pd.DataFrame()
        ichimoku = trend.IchimokuIndicator(candles['high'], candles['low'])
        df[f'ichimoku_base_line_{period}'] = ichimoku.ichimoku_base_line()
        df[f'ichimoku_conversion_line_{period}'] = ichimoku.ichimoku_conversion_line()
        df[f'ichimoku_a_{period}'] = ichimoku.ichimoku_a()
        df[f'ichimoku_b_{period}'] = ichimoku.ichimoku_b()
        return df

    @staticmethod
    def indicators_of(candles, period):
        """ Single period indicators"""
        resampled = candles.resample(period).agg(
            {'high': 'max', 'low': 'min', 'open': 'first', 'close': 'last', 'vol': 'sum'})
        df = MultiIndiFeatures.ichimoku_of(resampled, period)
        df[f'cci_{period}'] = trend.cci(resampled['high'], resampled['low'], resampled['close'])
        df[f'adx_{period}'] = trend.adx(resampled['high'], resampled['low'], resampled['close'])
        df[f'rsi_{period}'] = momentum.rsi(resampled['close'])
        df[f'stoch_{period}'] = momentum.stoch(resampled['high'], resampled['low'], resampled['close'])
        df[f'macd_{period}'] = trend.macd(resampled['close'])
        # df = features.dropna()
        return df
