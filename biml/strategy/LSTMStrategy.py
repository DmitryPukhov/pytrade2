from typing import Dict

import numpy as np
import pandas as pd
from keras import Sequential
from keras.layers import Dense, Dropout, LSTM
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray

from strategy.common.features.PredictLowHighFeatures import PredictLowHighFeatures
from strategy.common.PredictLowHighStrategyBase import PredictLowHighStrategyBase


class LSTMStrategy(PredictLowHighStrategyBase):
    """
    LSTM Neural network predicts future interval
    """

    def __init__(self, broker, config: Dict):
        PredictLowHighStrategyBase.__init__(self, broker, config)
        # lstm window
        self.lstm_window_size = config["biml.strategy.lstm.window.size"]
        self.min_xy_len = self.lstm_window_size + 1  # ??? ValueError: `start_index+length=10 > end_index=9` is disallowed, as no part of the sequence would be left to be used as current step.
        self._log.info(f"LSTM window size: {self.lstm_window_size}")

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(LSTM(128, return_sequences=True, input_shape=(self.lstm_window_size, X_size)))
        model.add(Dropout(0.2))
        model.add(LSTM(32))
        model.add(Dropout(0.2))
        model.add(Dense(20, activation='relu'))
        model.add(Dense(y_size, activation='linear'))
        # model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])
        # Load weights
        self.load_last_model(model)
        model.summary()
        return model

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Reshape last features to lstm window"""
        X = PredictLowHighFeatures.last_features_of(self.bid_ask, self.lstm_window_size, self.level2)
        X_trans = self.X_pipe.transform(X)
        X_reshaped = np.reshape(X_trans, (-1, self.lstm_window_size, X_trans.shape[1]))
        return X, X_reshaped

    def generator_of(self, train_X, train_y):
        """ Learning data generator. Override here to return shapes with lstm window"""
        return TimeseriesGenerator(train_X, train_y, length=self.lstm_window_size)
