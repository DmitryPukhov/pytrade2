import logging
from functools import reduce
from typing import Dict
from binance.spot import Spot as Client
import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from features.FeatureEngineering import FeatureEngineering
from strategy.StrategyBase import StrategyBase


class FutureLowHigh(StrategyBase):
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def __init__(self, client: Client, ticker: str):
        super().__init__(client)
        self.ticker = ticker
        self.order_quantity = 0.001
        self.stop_loss_ratio = 0.02
        self.model = None
        self.window_size = 15
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()
        self.fe = FeatureEngineering()
        self.pipe = None

        # Raise exception if we are in trade for this ticker
        if self.client and not self.is_out_of_market(ticker):
            raise AssertionError(f"Fatal: cannot trade. Opened positions detected for {ticker}.")

    def on_candles(self, ticker: str, interval: str, new_candles: pd.DataFrame):
        """
        Received new candles from feed
        """
        if ticker != self.ticker:
            return
        # Append new candles to current candles
        # This strategy is a single ticker and interval and only these candles can come
        new_candles["signal"] = 0
        self.candles = self.candles.append(new_candles).tail(self.window_size + self.predict_sindow_size)
        if len(self.candles) < (self.window_size + self.predict_sindow_size):
            return
        # Fit on last
        self.learn_on_last()

        # Get last predicted signal
        signal = {-1: "SELL", 0: None, 1: "BUY"}[self.candles.signal[-1]]
        logging.debug(f"Last signal: {signal}")
        if signal:
            if self.is_out_of_market(self.ticker):
                # Buy or sell
                close_price = self.candles.close[-1]
                self.create_order(symbol=self.ticker, side=signal, price=close_price, quantity=self.order_quantity,
                                  stop_loss_ratio=self.stop_loss_ratio, )
            else:
                logging.info(f"Do not create an order because we already are in trade for {self.ticker}")

    def learn_on_last(self):
        """
        Fit the model on last data window with new candle
        """
        # Fit
        train_X, train_y = self.fe.features_and_targets(self.candles, self.window_size, self.predict_sindow_size)
        self.pipe = self.create_pipe(train_X, train_y, 1, 1) if not self.pipe else self.pipe
        self.pipe.fit(train_X, train_y)

        # Predict
        X_last = self.fe.features(self.candles, self.window_size).tail(1)
        y_pred = self.pipe.predict(X_last)

        signal = int(train_y.columns[y_pred[0]].lstrip("signal_"))
        self.candles.loc[self.candles.index[-1], "signal"] = signal
        return signal

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

        self.pipe = self.create_pipe(X, y, 100,100) if not self.pipe else self.pipe
        tscv = TimeSeriesSplit(n_splits=20)
        cv = cross_val_score(self.pipe, X=X, y=y, cv=tscv, error_score="raise")
        print(cv)

    def create_pipe(self, X: pd.DataFrame, y: pd.DataFrame, epochs: int, batch_size: int) -> Pipeline:
        # Fit the model
        estimator = KerasClassifier(build_fn=self.create_model, X_size=len(X.columns), y_size=len(y.columns),
                                    epochs=epochs, batch_size=batch_size, verbose=1)
        column_transformer = self.fe.column_transformer(X, y)

        return Pipeline([("column_transformer", column_transformer), ('model', estimator)])
