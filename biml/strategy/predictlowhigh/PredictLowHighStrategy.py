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
        self.min_history_interval = pd.Timedelta(config['biml.strategy.learn.interval.sec'])

        # self.ticker = pd.DataFrame(columns=BaseFeed.bid_ask_columns).set_index("datetime")
        self.ticker = self.tickers[-1]

        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.level2: pd.DataFrame = pd.DataFrame()
        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)

        self.is_learning = False
        self.is_processing = False

        self.profit_loss_ratio = 4
        self.close_profit_loss_ratio = 2
        self.predict_window = config["biml.strategy.predict.window"]

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
        new_df = pd.DataFrame([ticker], columns=ticker.keys()).set_index("datetime")
        self.bid_ask = self.bid_ask.append(new_df)
        self.learn_or_skip()
        self.process_new_data()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model and not self.is_processing:
            try:
                self.is_processing = True
                # Predict
                X, y = self.predict_low_high()
                self.fut_low_high[y.columns] = y
                cur_trade_direction = self.broker.cur_trade.direction() if self.broker.cur_trade else 0

                # Open or close or do nothing
                open_signal, close_signal = self.process_new_prediction()
                y[["predict_window", "open_signal", "close_signal", "cur_trade"]] = \
                    [self.predict_window, open_signal, close_signal, cur_trade_direction]

                # Save to historical data
                self.save_lastXy(X, y, self.bid_ask.tail(1))

            except Exception as e:
                self._log.error(e)
            finally:
                self.is_processing = False

    def process_new_prediction(self) -> (int, int):
        """ Try to open or close based on last prediction data
        @:return (open_signal, close_signal), where signals can be 0,-1,1 """

        bid = self.bid_ask.loc[self.bid_ask.index[-1], "bid"]
        ask = self.bid_ask.loc[self.bid_ask.index[-1], "ask"]
        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = self.fut_low_high.loc[self.fut_low_high.index[-1], \
            ["bid_min_fut", "bid_max_fut", "ask_min_fut", "ask_max_fut"]]
        open_signal, close_signal = 0, 0
        if not self.broker.cur_trade:
            # Maybe open a new order
            open_signal, stop_loss, take_profit = self.get_open_signal(bid, ask, bid_min_fut, bid_max_fut, ask_min_fut,
                                                                       ask_max_fut)
            if open_signal:
                self.broker.create_cur_trade(symbol=self.ticker, direction=open_signal, quantity=self.order_quantity,
                                             price=None, stop_loss=stop_loss)
        else:
            # We do have an opened trade, maybe close the trade
            close_signal, _, _ = self.get_close_signal(bid, ask, bid_min_fut, bid_max_fut, ask_min_fut,
                                                       ask_max_fut)
            if close_signal and self.broker.cur_trade.direction() == -close_signal:
                self.broker.end_cur_trade()
        return open_signal, close_signal

    def get_open_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                        ask_max_fut: float) -> (int, float, float):
        """ Get open signal based on current price and prediction """
        return self.get_signal(bid, ask, bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut, self.profit_loss_ratio)

    def get_close_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                         ask_max_fut: float) -> (int, float, float):
        """ Get close signal based on current price and prediction """
        return self.get_signal(bid, ask, bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut,
                               self.close_profit_loss_ratio)

    def get_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                   ask_max_fut: float, ratio: float) -> (int, float, float):
        """ Calculate buy, sell or nothing signal based on predictions and profit/loss ratio
        :return (<-1 for sell, 0 for none, 1 for buy>, stop loss, take profit)"""
        buy_profit = bid_max_fut - ask
        buy_loss = ask - bid_min_fut
        sell_profit = bid - ask_min_fut
        sell_loss = ask_max_fut - bid

        if buy_profit / buy_loss >= ratio:
            # Buy
            return 1, bid_min_fut, bid_max_fut
        elif sell_profit / sell_loss >= ratio:
            # Sell
            return -1, ask_max_fut, ask_min_fut
        else:
            # No action
            return 0, None, None

    def predict_low_high(self) -> (pd.DataFrame, pd.DataFrame):

        X = PredictLowHighFeatures.last_features_of(self.bid_ask, self.level2)
        # todo: model predicts bid_diff_fut, ask_diff_fut
        y = self.model.predict(X, verbose=0) if not X.empty else [[np.nan, np.nan]]
        (bid_max_fut_diff, bid_spread_fut, ask_min_fut_diff, ask_spread_fut) = y[-1] if y.shape[0] < 2 else y
        y_df = self.bid_ask.loc[X.index][["bid", "ask"]]
        y_df["bid_max_fut"] = y_df["bid"] + bid_max_fut_diff
        y_df["bid_min_fut"] = y_df["bid_max_fut"] - bid_spread_fut
        y_df["ask_min_fut"] = y_df["ask"] + ask_min_fut_diff
        y_df["ask_max_fut"] = y_df["ask_min_fut"] + ask_spread_fut
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
                bid_ask_since_last_learn, self.level2, self.predict_window)
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
