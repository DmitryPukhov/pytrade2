import logging
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler

from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from strategy.common.DataPurger import DataPurger
from strategy.common.PeriodicalLearnStrategy import PeriodicalLearnStrategy
from strategy.common.PersistableStateStrategy import PersistableStateStrategy
from strategy.common.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class PredictLowHighStrategyBase(PeriodicalLearnStrategy, PersistableStateStrategy, DataPurger):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, broker, config: Dict):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.order_quantity = config["biml.order.quantity"]
        self._log.info(f"Order quantity: {self.order_quantity}")
        self.broker = broker
        self.model = None

        PeriodicalLearnStrategy.__init__(self, config)
        PersistableStateStrategy.__init__(self, config)
        DataPurger.__init__(self, config)

        self.tickers = self.config["biml.tickers"].split(",")
        self.min_history_interval = pd.Timedelta(config['biml.strategy.learn.interval.sec'])

        self.ticker = self.tickers[-1]

        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.level2: pd.DataFrame = pd.DataFrame()
        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)
        self.min_xy_len = 1

        self.is_learning = False
        self.is_processing = False

        self.profit_loss_ratio = 2
        # stop loss should be above price * min_stop_loss_coeff
        self.min_stop_loss_coeff = 0.00005  # 0.00005 for BTCUSDT 30000 means 1,5
        self.max_stop_loss_coeff = 0.005  # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.trade_check_interval = timedelta(seconds=10)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval
        self.predict_window = config["biml.strategy.predict.window"]
        self.min_xy_len = 1
        self.X_pipe, self.y_pipe = self.create_pipe()
        self._log.info(
            f"predict window: {self.predict_window}, profit loss ratio: {self.profit_loss_ratio}, "
            f"min stop loss coeff: {self.min_stop_loss_coeff}, max stop loss coeff: {self.max_stop_loss_coeff}")

    def create_pipe(self) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """
        x_pipe = Pipeline(
            [("xrs", RobustScaler()),  # Remove outliers
             ("xmms", MinMaxScaler())])  # Equal scales for the features
        y_pipe = Pipeline(
            [("yrs", RobustScaler()),  # Remove outliers
             ("ymms", MinMaxScaler())])  # Eaual scales for the features
        return x_pipe, y_pipe

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
        new_df = pd.DataFrame(level2, columns=BinanceWebsocketFeed.bid_ask_columns).set_index("datetime", drop=False)
        self.level2 = pd.concat([self.level2, new_df])  # self.level2.append(new_df)
        # self.learn_or_skip()
        # self.process_new_data()

    def on_ticker(self, ticker: dict):
        new_df = pd.DataFrame([ticker], columns=ticker.keys()).set_index("datetime")
        self.bid_ask = pd.concat([self.bid_ask, new_df])

        # self.bid_ask = self.bid_ask.append(new_df)
        self.learn_or_skip()
        self.process_new_data()

        # Purge
        self.purge_or_skip(self.bid_ask, self.level2, self.fut_low_high)

    def purge_all(self):
        """ Purge old data to reduce memory usage"""
        self.bid_ask = self.purged(self.bid_ask)
        self.level2 = self.purged(self.level2)
        self.fut_low_high = self.purged(self.fut_low_high)

    def check_cur_trade(self, bid: float, ask: float):
        """ Update cur trade if sl or tp reached """
        if not self.broker.cur_trade:
            return
        is_closable = False
        sl, tp = self.broker.cur_trade.stop_loss_price, self.broker.cur_trade.take_profit_price
        if self.broker.cur_trade.direction() == 1:
            is_closable = not sl or sl >= bid or (tp and tp <= bid)
        elif self.broker.cur_trade.direction() == -1:
            is_closable = not sl or sl <= ask or (tp and tp >= ask)

        # Timeout from last check passed
        interval_flag = datetime.utcnow() - self.last_trade_check_time >= self.trade_check_interval

        # if interval_flag or (interval_flag and (buy_close_flag or sell_close_flag)):
        if interval_flag and is_closable:
            self.broker.update_trade_status(self.broker.cur_trade)
            self.last_trade_check_time = datetime.utcnow()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model and not self.is_processing:
            try:
                self.is_processing = True
                # Predict
                X, y = self.predict_low_high()
                self.fut_low_high = y

                # Open or close or do nothing
                open_signal = self.process_new_prediction()
                cur_trade_direction = self.broker.cur_trade.direction() if self.broker.cur_trade else 0
                y[["predict_window", "open_signal", "cur_trade"]] = \
                    [self.predict_window, open_signal, cur_trade_direction]

                # Save to historical data
                self.save_lastXy(X.tail(1), y, self.bid_ask.tail(1))

            except Exception as e:
                self._log.error(e)
            finally:
                self.is_processing = False

    def process_new_prediction(self) -> int:
        """ Process last prediction, open a new order, save history if needed
        @:return open signal where signal can be 0,-1,1 """

        bid = self.bid_ask.loc[self.bid_ask.index[-1], "bid"]
        ask = self.bid_ask.loc[self.bid_ask.index[-1], "ask"]

        # Update current trade status
        self.check_cur_trade(bid, ask)

        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = self.fut_low_high.loc[self.fut_low_high.index[-1], \
            ["bid_min_fut", "bid_max_fut", "ask_min_fut", "ask_max_fut"]]
        open_signal = 0
        if not self.broker.cur_trade:
            # Maybe open a new order
            open_signal, open_price, stop_loss, take_profit = self.get_signal(bid, ask, bid_min_fut, bid_max_fut,
                                                                              ask_min_fut,
                                                                              ask_max_fut)
            if open_signal:
                self.broker.create_cur_trade(symbol=self.ticker, direction=open_signal, quantity=self.order_quantity,
                                             price=open_price,
                                             stop_loss_price=stop_loss,
                                             take_profit_price=take_profit)
        return open_signal

    def get_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                   ask_max_fut: float) -> (int, float, float, float):
        """ Calculate buy, sell or nothing signal based on predictions and profit/loss ratio
        :return (<-1 for sell, 0 for none, 1 for buy>, stop loss, take profit)"""

        buy_profit = bid_max_fut - ask
        buy_loss = ask - bid_min_fut
        sell_profit = bid - ask_min_fut
        sell_loss = ask_max_fut - bid

        if buy_profit / buy_loss >= self.profit_loss_ratio \
                and self.max_stop_loss_coeff * ask >= abs(buy_loss) >= self.min_stop_loss_coeff * ask:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            return 1, ask, stop_loss_adj, ask + buy_profit
        elif sell_profit / sell_loss >= self.profit_loss_ratio \
                and self.max_stop_loss_coeff * bid >= abs(sell_loss) >= self.min_stop_loss_coeff * bid:
            # Sell and possibly fix the loss
            stop_loss_adj = bid + abs(sell_loss) * 1.25
            return -1, bid, stop_loss_adj, bid - sell_profit
        else:
            # No action
            return 0, None, None, None

    def predict_low_high(self) -> (pd.DataFrame, pd.DataFrame):
        # X - features with absolute values, x_prepared - nd array fith final scaling and normalization
        X, X_prepared = self.prepare_last_X()
        # Predict
        y = self.model.predict(X_prepared, verbose=0) if not X.empty else np.ndarray[np.nan, np.nan, np.nan, np.nan]
        y = y.reshape((-1, 4))

        # Get prediction result
        y = self.y_pipe.inverse_transform(y)
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

    def generator_of(self, train_X, train_y):
        """ Data generator for learning """
        return TimeseriesGenerator(train_X, train_y, length=1)

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Get last X for prediction"""
        X = PredictLowHighFeatures.last_features_of(self.bid_ask, self.level2, 1)
        return X, self.X_pipe.transform(X)

    def learn(self):
        if self.is_learning:
            return
        self._log.debug("Learning")
        self.is_learning = True
        try:
            bid_ask_since_last_learn = self.bid_ask[self.bid_ask.index > self.last_learn_bidask_time]
            train_X, train_y = PredictLowHighFeatures.features_targets_of(
                bid_ask_since_last_learn, self.level2, self.predict_window)

            self._log.info(
                f"Learning on last data. Train data len: {train_X.shape[0]}, bid_ask since last learn: {bid_ask_since_last_learn.shape[0]}, last bid_ask at: {self.bid_ask.index[-1]}")
            if len(train_X.index) >= self.min_xy_len:
                if not self.model:
                    self.model = self.create_model(train_X.values.shape[1], train_y.values.shape[1])
                self.last_learn_bidask_time = pd.to_datetime(train_X.index.max())
                # Final scaling and normalization
                self.X_pipe.fit(train_X)
                self.y_pipe.fit(train_y)
                gen = self.generator_of(self.X_pipe.transform(train_X), self.y_pipe.transform(train_y))
                # Train
                self.model.fit(gen)

                # Save weights
                self.save_model()
                # # Clear the data, already used for learning
                # self.bid_ask = self.bid_ask[self.bid_ask.index > train_X.index.max()]
                # self.level2 = self.level2[self.level2.index > train_y.index.max()]
            else:
                self._log.info(f"Not enough train data to learn should be >= {self.min_xy_len}")

        finally:
            self.is_learning = False
