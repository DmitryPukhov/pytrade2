import logging
from functools import reduce
from typing import Dict

import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential
from keras.utils.np_utils import to_categorical
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OneHotEncoder

from features.FeatureEngineering import FeatureEngineering


class FutureLowHigh:
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def __init__(self):
        self.model = None
        self.window_size = 15
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()
        self.fe = FeatureEngineering()
        self.pipe = None

    def on_candles(self, ticker: str, interval: str, new_candles: pd.DataFrame):
        """
        Received new candles from feed
        """
        # Append new candles to current candles
        # This strategy is a single ticker and interval and only these candles can come
        self.candles = self.candles.append(new_candles).tail(self.window_size + self.predict_sindow_size)
        if len(self.candles) < self.window_size + self.predict_sindow_size:
            return
        self.candles = self.candles.tail(self.window_size + self.predict_sindow_size)

        train_X, train_y = self.fe.features_and_targets(self.candles, self.window_size,
                                                        self.predict_sindow_size)
        self.pipe = self.create_pipe(train_X, train_y) if not self.pipe else self.pipe
        history = self.pipe.fit(train_X, train_y)

        X = self.fe.features(self.candles, self.window_size).tail(self.window_size)
        # todo: convert to predicted value
        y = self.pipe.predict(X.tail(self.window_size))

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(Input(shape=(X_size,)))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(1024, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(128, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(64, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])
        # model.summary()
        return model

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values()).sort_index()

        # Feature engineering.
        X, y = self.fe.features_and_targets_balanced(data, self.window_size, self.predict_sindow_size)
        logging.info(f"Learn set size: {len(X)}")

        self.pipe = self.create_pipe(X, y) if not self.pipe else self.pipe
        tscv = TimeSeriesSplit(n_splits=20)
        cv = cross_val_score(self.pipe, X=X, y=y, cv=tscv, error_score="raise")
        print(cv)

    def create_pipe(self, X, y):
        # Fit the model
        estimator = KerasClassifier(build_fn=self.create_model, X_size=len(X.columns), y_size=len(y.columns),
                                    epochs=100, batch_size=100, verbose=1)
        column_transformer = self.fe.column_transformer(X, y)

        return Pipeline([("column_transformer", column_transformer), ('model', estimator)])
