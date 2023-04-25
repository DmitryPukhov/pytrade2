from typing import Dict

import pandas as pd

from strategy.common.predictlowhigh.PredictLowHighStrategyBase import PredictLowHighStrategyBase
from keras import Sequential, Input
from keras.layers import Dense, Dropout, LSTM
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, FunctionTransformer


class LSTMStrategy(PredictLowHighStrategyBase):
    """
    LSTM Neural network predicts future interval
    """

    def __init__(self, broker, config: Dict):
        PredictLowHighStrategyBase.__init__(self, broker, config)
        # todo: change to lstm window
        self.window_size = 1

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(LSTM(128, return_sequences=True, input_shape=(self.window_size, X_size)))
        model.add(Dropout(0.2))
        model.add(LSTM(32))
        model.add(Dropout(0.2))
        model.add(Dense(20, activation='relu'))
        model.add(Dense(y_size, activation='linear'))
        # model.add(Dense(y_train.shape[1], activation='softmax'))
        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])
        # Load weights
        self.load_last_model(model)
        # model.summary()
        return model

    def reshape(self, data):
        return data.reshape(data.shape[0], self.window_size, data.shape[1])

    def create_pipe(self, X: pd.DataFrame, y: pd.DataFrame, epochs: int, batch_size: int) -> TransformedTargetRegressor:
        model = self.create_model(X_size=X.values.shape[1], y_size=y.values.shape[1])
        regressor = KerasRegressor(model=model, epochs=epochs, batch_size=self.window_size * batch_size, verbose=1)

        xpipe = Pipeline([('xscaler', StandardScaler()),
                          ('reshape', FunctionTransformer(self.reshape, validate=False)),
                          ('model', regressor)])
        ypipe = Pipeline([
            ('yscaler', StandardScaler())
            # ('reshape', FunctionTransformer(reshape, validate=False))
        ])

        # Add y transformer
        wrapped = TransformedTargetRegressor(regressor=xpipe, transformer=ypipe)
        return wrapped
