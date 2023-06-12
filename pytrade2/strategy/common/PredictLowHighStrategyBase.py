import logging
import multiprocessing
import time
from datetime import datetime, timedelta
from threading import Thread, Event
from typing import Dict, List

import numpy as np
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray
from pandas import Timedelta
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler

from exch.Exchange import Exchange
from strategy.common.CandlesStrategy import CandlesStrategy
from strategy.common.DataPurger import DataPurger
from strategy.common.PeriodicalLearnStrategy import PeriodicalLearnStrategy
from strategy.common.PersistableStateStrategy import PersistableStateStrategy
from strategy.common.features.PredictLowHighFeatures import PredictLowHighFeatures


class PredictLowHighStrategyBase(CandlesStrategy, PeriodicalLearnStrategy, PersistableStateStrategy, DataPurger):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.tickers = self.config["pytrade2.tickers"].split(",")
        self.ticker = self.tickers[-1]
        self.order_quantity = config["pytrade2.order.quantity"]
        self._log.info(f"Order quantity: {self.order_quantity}")
        self.exchange_provider = exchange_provider
        self.exchange = None
        self.websocket_feed = None
        self.candles_feed = None
        self.model = None
        self.broker = None
        self.new_data_event: Event = Event()
        self.data_lock = multiprocessing.RLock()

        CandlesStrategy.__init__(self, ticker=self.ticker, candles_feed=self.candles_feed)
        PeriodicalLearnStrategy.__init__(self, config)
        PersistableStateStrategy.__init__(self, config)
        DataPurger.__init__(self, config)

        self.min_history_interval = pd.Timedelta(config['pytrade2.strategy.learn.interval.sec'])

        # Price, level2 dataframes and their buffers
        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.bid_ask_buf: pd.DataFrame = pd.DataFrame()  # Buffer
        self.level2: pd.DataFrame = pd.DataFrame()
        self.level2_buf: pd.DataFrame = pd.DataFrame()  # Buffer

        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)
        self.min_xy_len = 2

        self.is_learning = False
        self.is_processing = False

        # Expected profit/loss >= ratio means signal to trade
        self.profit_loss_ratio = config.get("pytrade2.strategy.profitloss.ratio", 1)

        # stop loss should be above price * min_stop_loss_coeff
        # 0.00005 for BTCUSDT 30000 means 1,5
        self.stop_loss_min_coeff = config.get("pytrade2.strategy.stoploss.min.coeff", 0)

        # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.stop_loss_max_coeff = config.get("pytrade2.strategy.stoploss.max.coeff",
                                              float('inf'))
        self.trade_check_interval = timedelta(seconds=10)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.min_xy_len = 2
        self.X_pipe, self.y_pipe = None, None
        self._log.info(
            f"predict window: {self.predict_window}, profit loss ratio: {self.profit_loss_ratio}, "
            f"min stop loss coeff: {self.stop_loss_min_coeff}, max stop loss coeff: {self.stop_loss_max_coeff}")

    def get_report(self):
        """ Short info for report """

        broker_report = self.broker.get_report() if hasattr(self.broker, "get_report") else "Not provided"
        last_bid_ask = self.bid_ask.index.max() if not self.bid_ask.empty else None
        last_level2 = self.level2.index.max() if not self.level2.empty else None
        last_candle = self.last_candles_read_time

        broker_report += f"\nLast bid ask: {last_bid_ask}" \
                         f"\nLast level2: {last_level2}" \
                         f"\nLast candle: {last_candle}"
        return broker_report

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", RobustScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MinMaxScaler())])
        x_pipe.fit(X)

        y_pipe = Pipeline(
            [("yrs", RobustScaler()),
             ("ymms", MinMaxScaler())])
        y_pipe.fit(y)
        return x_pipe, y_pipe

    def run(self):
        """
        Attach to the feed and listen
        """
        exchange_name = self.config["pytrade2.exchange"]

        # Create feed and broker
        self.websocket_feed = self.exchange_provider.websocket_feed(exchange_name)
        self.websocket_feed.consumers.append(self)
        self.candles_feed = self.exchange_provider.candles_feed(exchange_name)
        self.broker = self.exchange_provider.broker(exchange_name)

        # Start main processing loop
        Thread(target=self.processing_loop).start()

        # Run the feed, listen events
        self.websocket_feed.run()

    def processing_loop(self):
        self._log.info("Starting processing loop")

        # If alive is None, not started, so continue loop
        is_alive = self.is_alive()
        while is_alive or is_alive is None:

            # Wait for new data received
            while not self.new_data_event.is_set():
                self.new_data_event.wait()
            self.new_data_event.clear()

            # Copy new data from buffers to main data frames
            with self.data_lock:
                self.bid_ask = pd.concat([self.bid_ask, self.bid_ask_buf]).sort_index()
                self.bid_ask_buf = pd.DataFrame()
                self.level2 = pd.concat([self.level2, self.level2_buf]).sort_index()
                self.level2_buf = pd.DataFrame()

            # Learn and predict only if no gap between level2 and bidask
            self.read_candles_or_skip()
            self.learn_or_skip()
            self.process_new_data()

            # Purge
            self.purge_or_skip(self.bid_ask, self.level2, self.fut_low_high)

            # Refresh live status
            is_alive = self.is_alive()

        self._log.info("End main processing loop")

    def is_alive(self):
        maxdelta = pd.Timedelta("90s")

        # Last received data
        last_bid_ask = self.bid_ask.index.max() if not self.bid_ask.empty else None
        last_level2 = self.level2.index.max() if not self.level2.empty else None
        last_candle = self.last_candles_read_time
        dt = datetime.utcnow()
        delta = max([dt - last_bid_ask, dt - last_level2, dt - last_candle]) \
            if last_bid_ask and last_level2 and last_candle else None

        is_alive = delta and (delta < maxdelta)
        # self._log.info(
        #     f"Strategy is_alive:{is_alive}. Time since last full data: {delta}, max allowed inactivity: {maxdelta}.")
        return is_alive

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

        # Add new data to df
        new_df = pd.DataFrame(level2, columns=bid_ask_columns).set_index("datetime", drop=False)
        with self.data_lock:
            self.level2_buf = pd.concat([self.level2_buf, new_df])

        self.new_data_event.set()

    def on_ticker(self, ticker: dict):
        # Add new data to df

        new_df = pd.DataFrame([ticker], columns=list(ticker.keys())).set_index("datetime")
        with self.data_lock:
            self.bid_ask_buf = pd.concat([self.bid_ask_buf, new_df])

        self.new_data_event.set()

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
            self.broker.update_cur_trade_status()
            self.last_trade_check_time = datetime.utcnow()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model and not self.is_processing \
                and self.X_pipe and self.y_pipe:
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
                self.save_lastXy(X.tail(1), y.tail(1), self.bid_ask.tail(1))

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
            ["bid_min_fut", "bid_max_fut",
             "ask_min_fut", "ask_max_fut"]]
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
                and self.stop_loss_max_coeff * ask >= abs(buy_loss) >= self.stop_loss_min_coeff * ask:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            return 1, ask, stop_loss_adj, ask + buy_profit
        elif sell_profit / sell_loss >= self.profit_loss_ratio \
                and self.stop_loss_max_coeff * bid >= abs(sell_loss) >= self.stop_loss_min_coeff * bid:
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
        if self.is_learning or self.bid_ask.empty or self.level2.empty or self.candles_features.empty:
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
        X = PredictLowHighFeatures.last_features_of(self.bid_ask, 1, self.level2, self.candles_features)
        return X, self.X_pipe.transform(X)

    def learn(self):
        if self.is_learning:
            return
        self._log.debug("Learning")
        self.is_learning = True
        try:
            new_bid_ask = self.bid_ask[self.bid_ask.index > self.last_learn_bidask_time]
            new_level2 = self.level2[self.level2.index > self.last_learn_bidask_time]
            train_X, train_y = PredictLowHighFeatures.features_targets_of(
                new_bid_ask, self.level2, self.candles_features, self.predict_window)

            self._log.info(
                f"Learning on last data. Train data len: {train_X.shape[0]}, "
                f"new bid_ask: {new_bid_ask.shape[0]}, new level2: {new_level2.shape[0]}, "
                f"last bid_ask at: {self.bid_ask.index[-1]}, last level2 at: {self.level2.index[-1]}, "
                f"last candle at: {self.candles_features.index[-1]}")
            if len(train_X.index) >= self.min_xy_len:
                if not self.model:
                    self.model = self.create_model(train_X.values.shape[1], train_y.values.shape[1])
                self.last_learn_bidask_time = pd.to_datetime(train_X.index.max())
                if not (self.X_pipe and self.y_pipe):
                    self.X_pipe, self.y_pipe = self.create_pipe(train_X, train_y)
                # Final scaling and normalization
                self.X_pipe.fit(train_X)
                self.y_pipe.fit(train_y)
                gen = self.generator_of(self.X_pipe.transform(train_X), self.y_pipe.transform(train_y))
                # Train
                self.model.fit(gen)

                # Save weights
                self.save_model()
            else:
                self._log.info(f"Not enough train data to learn should be >= {self.min_xy_len}")

        finally:
            self.is_learning = False
