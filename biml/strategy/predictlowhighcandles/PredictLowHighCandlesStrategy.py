import glob
import logging
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Dict
import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential, Model
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from AppTools import AppTools
from feed.BinanceCandlesFeed import BinanceCandlesFeed
from strategy.PersistableModelStrategy import PersistableModelStrategy
from strategy.StrategyBase import StrategyBase
from strategy.predictlowhighcandles.LowHighCandlesFeatures import LowHighCandlesFeatures


class PredictLowHighCandlesStrategy(StrategyBase, PersistableModelStrategy):
    """
    Candles based. Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def __init__(self, broker, config: Dict):
        super().__init__(broker, config)
        self.tickers = AppTools.read_candles_tickers(config)
        self.ticker: str = self.tickers[-1].ticker

        self.window_size = 15
        self.candles_size = self.window_size * 100
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()

        # Minimum stop loss ratio = (price-stop_loss)/price
        # self.min_stop_loss_ratio = 0.005
        self.min_stop_loss_ratio = 0.001
        # Minimum profit/loss
        # For test only. Should be > 4
        self.profit_loss_ratio = 1.5

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceCandlesFeed(spot_client=client, tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_candles(self, ticker: str, interval: str, new_candles: pd.DataFrame):
        """
        Received new candles from feed
        """

        if ticker != self.ticker or interval != "1m":
            return
        # Append new candles to current candles
        # This strategy is a single ticker and interval and only these candles can come
        new_candles["signal"] = 0
        self.candles = self.candles.append(new_candles).tail(self.candles_size + self.predict_sindow_size)
        if len(self.candles) < (self.window_size + self.predict_sindow_size):
            return
        # Fit on last
        self.learn_on_last()

        # Open/close trade
        self.process_new_data()

    def close_signal(self) -> int:

        """ Buy or sell or no signal to close current opened order"""
        self._log.debug(f"Calculating close signal for trade {self.broker.cur_trade}")
        if not self.broker.cur_trade:
            # No opened trade, nothing to close
            return 0

        # Calculate variables of current state
        close, fut_high, fut_low = self.candles["close"].iloc[-1], self.candles["fut_high"].iloc[-1], \
        self.candles["fut_low"].iloc[-1]
        signal, cur_trade_signal = 0, 0
        predicted_loss, cur_trade_stop_loss = 0, abs(
            self.broker.cur_trade.open_price - self.broker.cur_trade.stop_loss_price)
        if self.broker.cur_trade.side == self.broker.order_side_names.get(1):
            # Calc variables for opened buy order
            predicted_loss, cur_trade_signal = close - fut_low, 1
        elif self.broker.cur_trade.side == self.broker.order_side_names.get(-1):
            # Calc vars for opened sell order
            predicted_loss, cur_trade_signal = fut_high - close, -1

        # If predicted loss is too much, signal to close opened order
        signal = -cur_trade_signal if predicted_loss > cur_trade_stop_loss else 0
        self._log.debug(
            f"Calculated close signal: {signal}, close price: {close}, fut_high: {fut_high}, fut_low: {fut_low},  current trade: {self.broker.cur_trade}")
        return signal

    def open_signal(self) -> (int, int, int):
        """
        Return buy or sell or no signal using predicted prices
        :param df: candles dataframe
        :return: (<signal:1 for buy, -1 sell, 0 no signal>, <price>, <stop loss adjusted>)
        """
        signal, price, stop_loss, stop_loss_adj, take_profit = 0, None, None, None, None
        if self.candles.empty:
            self._log.debug("Candles are empty")
            return signal, price, stop_loss
        close, fut_high, fut_low = self.candles["close"].iloc[-1], self.candles["fut_high"].iloc[-1], \
        self.candles["fut_low"].iloc[-1]
        delta_high, delta_low = (fut_high - close), (close - fut_low)
        ratio = abs(delta_high / delta_low)

        self._log.debug(
            f"Calculating signal. close: {close}, fut_high:{fut_high}, fut_low:{fut_low},"
            f" delta_high:{delta_high}, delta_low: {delta_low}, "
            f"ratio: {max(ratio, 1 / ratio)}, profit_loss_ratio:{self.profit_loss_ratio}")
        if ratio >= self.profit_loss_ratio:
            # Buy signal
            signal, price, stop_loss, take_profit = 1, close, fut_low, fut_high
            # Adjust stop loss if predicted is too small
            stop_loss_adj = min(stop_loss, price * (1 - self.min_stop_loss_ratio))
        elif ratio <= 1 / self.profit_loss_ratio:
            # Sell signal
            signal, price, stop_loss, take_profit = -1, close, fut_high, fut_low
            # Adjust stop loss if predicted is too small
            stop_loss_adj = max(stop_loss, price * (1 + self.min_stop_loss_ratio))

        if signal:
            self._log.debug(
                f"Calculated signal: {signal}, price:{price}, "
                f"stop_loss: {stop_loss} ({stop_loss - price}),"
                f"stop_loss adjusted: {stop_loss} ({stop_loss_adj - price})  for min ratio {self.min_stop_loss_ratio},"
                f"take_profit: {take_profit} ({take_profit - price}).")

        if signal and abs(take_profit - price) < abs(price - stop_loss_adj) * self.profit_loss_ratio:
            self._log.debug(
                f"Expected profit {abs(take_profit - price)} is too small "
                f"for loss {abs(price - stop_loss)}, adjusted loss {abs(price - stop_loss_adj)}"
                f" and profit loss ratio {self.profit_loss_ratio}. set signal to 0")
            signal, price, stop_loss_adj = 0, None, None

        self._log.debug(
            f"Calculated adjusted signal: {signal}, price:{price}, stop_loss: {stop_loss_adj}, "
            f"take_profit: {take_profit}.")

        return signal, price, stop_loss_adj

    def learn_on_last(self):
        """
        Fit the model on last data window with new candle
        """
        # Fit
        train_X, train_y = LowHighCandlesFeatures.features_and_targets(self.candles, self.window_size,
                                                                       self.predict_sindow_size)
        self.model = self.create_pipe(train_X, train_y, 1, 1) if not self.model else self.model
        self.model.fit(train_X, train_y)

        # Predict
        X_last = LowHighCandlesFeatures.features_of(self.candles, self.window_size).tail(1)
        y_pred = self.model.predict(X_last)
        LowHighCandlesFeatures.set_predicted_fields(self.candles, y_pred)
        self._log.debug(f"Predicted fut_low-close: {y_pred[0][0]}, fut_high-fut_low:{y_pred[0][1]}")

        # Save model
        self.save_model()
        self.save_lastXy(X_last, y_pred, self.candles.tail(1))

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

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values()).sort_index()

        # Feature engineering.

        X, y = LowHighCandlesFeatures.features_and_targets(data, self.window_size, self.predict_sindow_size)

        self._log.info(f"Learn set size: {len(X)}")

        # self.pipe = self.create_pipe(X, y,epochs= 100, batch_size=100) if not self.pipe else self.pipe
        # tscv = TimeSeriesSplit(n_splits=20)
        self.model = self.create_pipe(X, y, epochs=50, batch_size=100) if not self.model else self.model
        tscv = TimeSeriesSplit(n_splits=7)
        cv = cross_val_score(self.model, X=X, y=y, cv=tscv, error_score="raise")
        print(cv)
        # Save weights
        self.save_model()

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
