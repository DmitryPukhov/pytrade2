import logging
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from keras import Sequential, Input
from keras.layers import Dense, Dropout
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from feed.BaseFeed import BaseFeed
from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from strategy.PeriodicalLearnStrategy import PeriodicalLearnStrategy
from strategy.PersistableModelStrategy import PersistableModelStrategy
from strategy.StrategyBase import StrategyBase
from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class PredictLowHighStrategy(StrategyBase, PeriodicalLearnStrategy, PersistableModelStrategy):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, broker, config: Dict):
        self.config = config
        StrategyBase.__init__(self, broker, config)
        PeriodicalLearnStrategy.__init__(self, config)
        PersistableModelStrategy.__init__(self, config)

        self.tickers = self.config["biml.tickers"].split(",")
        self.min_history_interval = pd.Timedelta("2 seconds")

        #self.ticker = pd.DataFrame(columns=BaseFeed.bid_ask_columns).set_index("datetime")
        self.ticker = self.tickers[-1]

        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.level2: pd.DataFrame = pd.DataFrame()
        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)

        self.is_learning = False

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceWebsocketFeed(tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        new_df = pd.DataFrame(level2, columns=BaseFeed.bid_ask_columns).set_index("datetime", drop=False)
        self.level2 = self.level2.append(new_df)
        # self.learn_or_skip()
        # self.process_new_data()

    def on_ticker(self, ticker: dict):
        new_df = pd.DataFrame([ticker], columns=ticker.keys()).set_index("datetime", drop=False)
        self.bid_ask = self.bid_ask.append(new_df)
        self.learn_or_skip()
        self.process_new_data()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model:
            # Preduct next low/high based on last data
            X, y = self.predict_low_high()
            self.fut_low_high[y.columns] = y
            self.save_lastXy(X, y, self.bid_ask.tail(-1))

    def predict_low_high(self) -> (pd.DataFrame, pd.DataFrame):
        X = PredictLowHighFeatures.last_features_of(self.bid_ask, self.level2)
        y = self.model.predict(X, verbose=0) if not X.empty else [[np.nan, np.nan]]
        (low, high) = (y[-1][0], y[-1][1]) if y.shape[0] < 2 else (y[0], y[1])
        y_df = pd.DataFrame(index=X.index, data={"fut_low": low, "fut_high": high})
        return X, y_df

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""
        # Check learn conditions
        if self.is_learning or self.bid_ask.empty or self.level2.empty:
            return False
        # Check If we have enough data to learn
        interval = self.bid_ask.index.max() - self.bid_ask.index.min()
        if interval < self.min_history_interval:
            return False
        return True

    def learn(self):
        if self.is_learning:
            return
        self._log.info("Learning")
        self.is_learning = True
        try:

            bid_ask_since_last_learn = self.bid_ask[self.bid_ask.index > self.last_learn_bidask_time]
            train_X, train_y = PredictLowHighFeatures.features_targets_of(
                bid_ask_since_last_learn, self.level2)
            self._log.info(
                f"Train data len: {train_X.shape[0]}, bid_ask since last learn: {bid_ask_since_last_learn.shape[0]}")
            model = self.create_pipe(train_X, train_y, 1, 1) if not self.model else self.model
            if not train_X.empty:
                model.fit(train_X, train_y)
                self.model = model
                self.last_learn_bidask_time = pd.to_datetime(train_X.index.max())
                self.save_model()
                # # Clear the data, already used for learning
                # self.bid_ask = self.bid_ask[self.bid_ask.index > train_X.index.max()]
                # self.level2 = self.level2[self.level2.index > train_y.index.max()]

        finally:
            self.is_learning = False
            self._log.info("Learning completed")

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
        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])

        # Load weights
        self.load_last_model(model)
        # model.summary()
        return model

    def create_pipe(self, X: pd.DataFrame, y: pd.DataFrame, epochs: int, batch_size: int) -> TransformedTargetRegressor:
        # Fit the model
        regressor = KerasRegressor(model=self.create_model(X_size=len(X.columns), y_size=len(y.columns)),
                                   epochs=epochs, batch_size=batch_size, verbose=1)
        column_transformer = ColumnTransformer(
            [
                ('xscaler', StandardScaler(), X.columns)
                # ('yscaler', StandardScaler(), y.columns)
                # ('cat_encoder', OneHotEncoder(handle_unknown="ignore"), y.columns)
            ]
        )

        pipe = Pipeline([("column_transformer", column_transformer), ('model', regressor)])
        wrapped = TransformedTargetRegressor(regressor=pipe, transformer=StandardScaler())
        return wrapped
