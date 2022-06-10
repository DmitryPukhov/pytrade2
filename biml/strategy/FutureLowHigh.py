import logging
from functools import reduce
from typing import Dict

import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler

from features.FeatureEngineering import FeatureEngineering


class FutureLowHigh:
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def __init__(self):
        self.model = None
        self.window_size = 15

    def on_candles(ticker: str, interval: str, candles: pd.DataFrame):
        """
        Received new candles from feed
        """
        logging.info("Got new candles")

    def create_model(self):
        model = Sequential()
        model.add(Input(shape=(self.X_size,)))
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
        model.add(Dense(self.y_size, activation='softmax'))
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
        fe = FeatureEngineering()
        X, y = fe.features_and_targets_balanced(data, self.window_size)
        logging.info(f"Learn set size: {len(X)}")

        # ax = sns.countplot(y_train)
        # ax.bar_label(ax.containers[0])
        # ax.set_title("Signal distribution balanced")
        # plt.show()

        # Fit the model
        self.X_size, self.y_size = len(X.columns), len(y.columns)
        estimator = KerasClassifier(build_fn=self.create_model, epochs=100, batch_size=100, verbose=1)
        pipe = Pipeline([("scaler", StandardScaler()), ('model', estimator)])
        tscv = TimeSeriesSplit(n_splits=20)
        cv = cross_val_score(pipe, X=X, y=y, cv=tscv, error_score="raise")
        print(cv)
