import logging
from typing import Dict

import pandas as pd
from ta import trend, momentum

from strategy.features.CandlesFeatures import CandlesFeatures
from strategy.features.LowHighTargets import LowHighTargets


class MultiIndiFeatures:
    """ Multiple TA indicators on multiple periods"""
    _log = logging.getLogger("MultiIndiFeatures")

    @staticmethod
    def multi_indi_features_last(candles_by_periods: Dict[str, pd.DataFrame], n=1):
        max_candles = 52  # Ichimoku period is last todo: remove 2
        last_candles_by_periods = {period: candles.tail(max_candles) for period, candles in candles_by_periods.items()}
        return MultiIndiFeatures.multi_indi_features(last_candles_by_periods).tail(n)

    @staticmethod
    def multi_indi_features_targets(candles_by_periods: Dict[str, pd.DataFrame], target_period,
                                    drop_features_wo_targets=True):
        features = MultiIndiFeatures.multi_indi_features(candles_by_periods)
        targets = LowHighTargets.fut_lohi(candles_by_periods[target_period], target_period)
        if drop_features_wo_targets:
            common_index = features.index.intersection(targets.index)
            features = features.loc[common_index]
            targets = targets.loc[common_index]

        return features, targets

    @staticmethod
    def multi_indi_features(candles_by_periods: Dict[str, pd.DataFrame]):
        # Create time features
        min_period = min(candles_by_periods.keys(), key=pd.Timedelta)

        min_candles = candles_by_periods[min_period]

        # time
        time_features = CandlesFeatures.time_features_of(min_candles)[['time_hour', 'time_minute']]
        MultiIndiFeatures._log.debug(f"time features:\n{time_features.tail()}")

        # Indicators
        indicators_features = []
        for period, candles in candles_by_periods.items():
            period_indicators = MultiIndiFeatures.indicators_of(candles, period)
            indicators_features.append(period_indicators)
            MultiIndiFeatures._log.debug(f"Indicators of period: {period}\n{period_indicators.tail()}")

        # Concat time and indicators columns
        features = pd.concat([time_features] + indicators_features, axis=1).sort_index().ffill()
        MultiIndiFeatures._log.debug(f"Resulted features with nans:\n{features.tail()}")
        features = features.dropna()
        MultiIndiFeatures._log.debug(f"Resulted features dropna:\n{features.tail()}")

        # indi_features = pd.concat([indi_features] + [indicators_of(candles,period) for period in ['1min', '5min', '15min', '30min', '60min']], axis=1).ffill()
        # Enrich with columns from each period
        # features = pd.concat(
        #     [time_features] + [MultiIndiFeatures.indicators_of(candles, period) for period, candles in
        #                        candles_by_periods.items()],
        #     axis=1).sort_index().ffill().dropna()
        return features

    @staticmethod
    def ichimoku_of(candles: pd.DataFrame, period: str = ''):
        df = pd.DataFrame()
        ichimoku = trend.IchimokuIndicator(candles['high'], candles['low'], fillna=False)
        df[f'ichimoku_base_line_{period}_diff'] = ichimoku.ichimoku_base_line().diff()
        df[f'ichimoku_conversion_line_{period}_diff'] = ichimoku.ichimoku_conversion_line().diff()
        df[f'ichimoku_a_{period}_diff'] = ichimoku.ichimoku_a().diff()
        df[f'ichimoku_b_{period}_diff'] = ichimoku.ichimoku_b().diff()
        return df

    @staticmethod
    def indicators_of(candles, period):
        """ Single period indicators"""
        resampled = candles.resample(period).agg(
            {'high': 'max', 'low': 'min', 'open': 'first', 'close': 'last', 'vol': 'sum'})
        df = MultiIndiFeatures.ichimoku_of(resampled, period)
        df[f'cci_{period}_diff'] = trend.cci(resampled['high'], resampled['low'], resampled['close'],
                                             fillna=False).diff()
        df[f'adx_{period}_diff'] = trend.adx(resampled['high'], resampled['low'], resampled['close'],
                                             fillna=False).diff()
        df[f'rsi_{period}_diff'] = momentum.rsi(resampled['close'], fillna=False).diff()
        df[f'stoch_{period}_diff'] = momentum.stoch(resampled['high'], resampled['low'], resampled['close'],
                                                    fillna=False).diff()
        df[f'macd_{period}_diff'] = trend.macd(resampled['close'], fillna=False).diff()
        df = df.dropna()
        return df
