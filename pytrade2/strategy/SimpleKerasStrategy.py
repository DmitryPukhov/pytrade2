from typing import Dict

from keras import Sequential, Input
from keras.layers import Dense, Dropout

from strategy.common.PredictLowHighStrategyBase import PredictLowHighStrategyBase


class SimpleKerasStrategy(PredictLowHighStrategyBase):
    """
    Keras simple NN
    """
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
        # model.add(Dropout(0.1))
        model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])

        # Load weights
        self.load_last_model(model)
        model.summary()
        return model
