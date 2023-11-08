import pandas as pd
from keras import Sequential, Input
from keras.layers import Dense, Dropout
from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase
from strategy.common.features.CandlesFeatures import CandlesFeatures
from strategy.common.features.LongCandleFeatures import LongCandleFeatures
from ta import trend


class LongCandleDenseStrategy(LongCandleStrategyBase):
    """ Predict long candle, NN with dense layers mainly """

    def features_targets(self):
        # Data
        candles = self.candles_by_interval[min(self.candles_by_interval)]  # 1 minute

        # Targets
        targets = LongCandleFeatures.targets_of(candles, self.profit_min_coeff)

        # Features
        features = CandlesFeatures.time_features_of(candles.copy())

        ichimoku = trend.IchimokuIndicator(candles['high'], candles['low'])
        features['ichimoku_base_line'] = ichimoku.ichimoku_base_line()
        features['ichimoku_conversion_line'] = ichimoku.ichimoku_conversion_line()
        features['ichimoku_a'] = ichimoku.ichimoku_a()
        features['ichimoku_b'] = ichimoku.ichimoku_b()

        features.dropna(inplace=True)
        features = features.drop(candles.columns, axis=1)

        # Split to features, targets, features wo targets
        common_index = features.index.intersection(targets.index)
        features_wo_targets = features[features.index > common_index.max()]
        features_with_targets, targets = features.loc[common_index], targets.loc[common_index]
        return features_with_targets, targets, features_wo_targets

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
