import pandas as pd
import ta.wrapper
from keras import Sequential, Input
from keras.layers import Dense, Dropout
from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase
from strategy.features.CandlesFeatures import CandlesFeatures
from strategy.features.LongCandleFeatures import LongCandleFeatures
from ta import trend, momentum, volume, others, volatility


class LongCandleDenseStrategy(LongCandleStrategyBase):
    """ Predict long candle, NN with dense layers mainly """

    # def features_targets(self):
    #     # Data - 1 minute or other minimal period candles
    #     candles = self.candles_by_interval[min(self.candles_by_interval)].copy()
    #     candles_cols = candles.columns
    #
    #     # Targets
    #     targets = LongCandleFeatures.targets_of(candles, self.stop_loss_min_coeff, self.profit_min_coeff)
    #
    #     # Time features
    #     features = CandlesFeatures.time_features_of(candles)
    #
    #     # Ichimoku indicator
    #     ichimoku = trend.IchimokuIndicator(candles['high'], candles['low'])
    #     features['ichimoku_base_line'] = ichimoku.ichimoku_base_line()
    #     features['ichimoku_conversion_line'] = ichimoku.ichimoku_conversion_line()
    #     features['ichimoku_a'] = ichimoku.ichimoku_a()
    #     features['ichimoku_b'] = ichimoku.ichimoku_b()
    #
    #     # CCI indicator
    #     features['cci'] = trend.cci(candles['high'], candles['low'], candles['close'])
    #     features['adx'] = trend.adx(candles['high'], candles['low'], candles['close'])
    #     features['rsi'] = momentum.rsi(candles['close'])
    #     features['stoch'] = momentum.stoch(candles['high'], candles['low'], candles['close'])
    #     features['macd'] = ta.trend.macd(candles['close'])
    #
    #     features.dropna(inplace=True)
    #     features = features.drop(candles_cols, axis=1)
    #
    #     # Split to features, targets, features wo targets
    #     common_index = features.index.intersection(targets.index)
    #     features_wo_targets = features[features.index > common_index.max()]
    #     features_with_targets, targets = features.loc[common_index], targets.loc[common_index]
    #     return features_with_targets, targets, features_wo_targets

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(Input(shape=(X_size,)))
        model.add(Dense(64, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(128, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(32, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['categorical_accuracy'])

        # Load weights
        self.load_last_model(model)
        model.summary()
        return model
