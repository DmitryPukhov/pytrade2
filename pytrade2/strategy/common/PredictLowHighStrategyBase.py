import gc
import logging
import multiprocessing
from datetime import datetime, timedelta, timezone
from threading import Thread, Event, Timer
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import tensorflow.python.keras.backend
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler

from exch.Exchange import Exchange
from strategy.common.CandlesStrategy import CandlesStrategy
from strategy.common.PersistableStateStrategy import PersistableStateStrategy
from strategy.common.features.CandlesFeatures import CandlesFeatures
from strategy.common.features.PredictLowHighFeatures import PredictLowHighFeatures


class PredictLowHighStrategyBase(CandlesStrategy, PersistableStateStrategy):
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

        self.price_precision = config["pytrade2.price.precision"]
        self.amount_precision = config["pytrade2.amount.precision"]

        CandlesStrategy.__init__(self, config=config, ticker=self.ticker, candles_feed=self.candles_feed)
        PersistableStateStrategy.__init__(self, config)
        self.new_data_event: Event = Event()
        self.data_lock = multiprocessing.RLock()

        # Learn params
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.past_window = config["pytrade2.strategy.past.window"]
        self.history_min_window = pd.Timedelta(config["pytrade2.strategy.history.min.window"])
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])
        self.learn_interval = pd.Timedelta(config['pytrade2.strategy.learn.interval']) \
            if 'pytrade2.strategy.learn.interval' in config else None
        # Purge params
        self.purge_interval = pd.Timedelta(config['pytrade2.strategy.purge.interval']) \
            if 'pytrade2.strategy.purge.interval' in config else None

        # Price, level2 dataframes and their buffers
        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.bid_ask_buf: pd.DataFrame = pd.DataFrame()  # Buffer
        self.level2: pd.DataFrame = pd.DataFrame()
        self.level2_buf: pd.DataFrame = pd.DataFrame()  # Buffer

        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)
        self.min_xy_len = 2

        self.is_processing = False

        # Expected profit/loss >= ratio means signal to trade
        self.profit_loss_ratio = config.get("pytrade2.strategy.profitloss.ratio", 1)

        # stop loss should be above price * min_stop_loss_coeff
        # 0.00005 for BTCUSDT 30000 means 1,5
        self.stop_loss_min_coeff = config.get("pytrade2.strategy.stoploss.min.coeff", 0)

        # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.stop_loss_max_coeff = config.get("pytrade2.strategy.stoploss.max.coeff",
                                              float('inf'))
        # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
        self.profit_min_coeff = config.get("pytrade2.strategy.profit.min.coeff", 0)

        self.trade_check_interval = timedelta(seconds=10)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval
        self.min_xy_len = 2
        self.X_pipe, self.y_pipe = None, None

        self._log.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

    def get_report(self):
        """ Short info for report """

        broker_report = self.broker.get_report() if hasattr(self.broker, "get_report") else "Not provided"
        last_bid_ask = self.bid_ask.index.max() if not self.bid_ask.empty else None
        last_level2 = self.level2.index.max() if not self.level2.empty else None
        last_candle = self.last_candle_min_time()

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
        self.candles_feed.consumers.append(self)
        self.broker = self.exchange_provider.broker(exchange_name)

        # Start main processing loop
        Thread(target=self.processing_loop).start()

        # Start periodical jobs
        if self.learn_interval:
            self._log.info(f"Starting periodical learning, interval: {self.learn_interval}")
            Timer(self.learn_interval.seconds, self.learn).start()
        if self.purge_interval:
            self._log.info(f"Starting periodical purging, interval: {self.purge_interval}")
            Timer(self.purge_interval.seconds, self.purge_all).start()

        # Run the feed, listen events
        self.websocket_feed.run()
        self.candles_feed.run()
        self.broker.run()

    def processing_loop(self):
        self._log.info("Starting processing loop")

        # If alive is None, not started, so continue loop
        is_alive = self.is_alive()
        while is_alive or is_alive is None:

            # Wait for new data received
            while not self.new_data_event.is_set():
                self.new_data_event.wait()
            self.new_data_event.clear()

            # Append new data from buffers to main data frames
            with self.data_lock:
                self.bid_ask = pd.concat([self.bid_ask, self.bid_ask_buf]).sort_index()
                self.bid_ask_buf = pd.DataFrame()
                self.level2 = pd.concat([self.level2, self.level2_buf]).sort_index()
                self.level2_buf = pd.DataFrame()

            # Learn and predict only if no gap between level2 and bidask
            self.process_new_data()

            # Refresh live status
            is_alive = self.is_alive()

        self._log.info("End main processing loop")

    def is_alive(self):
        maxdelta = self.history_min_window + pd.Timedelta("60s")

        # Last received data
        last_bid_ask = self.bid_ask.index.max() if not self.bid_ask.empty else None
        last_level2 = self.level2.index.max() if not self.level2.empty else None
        last_candle = self.last_candle_min_time()
        dt = datetime.utcnow()
        if last_bid_ask:
            dbidask = dt - last_bid_ask
        if last_level2:
            dl2 = dt - last_level2
        if last_candle:
            dc = dt - last_candle
        delta = max([dt - last_bid_ask, dt - last_level2, dt - last_candle]) \
            if last_bid_ask and last_level2 and last_candle else None
        is_alive = (delta < maxdelta) if delta else None
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
        try:
            with self.data_lock:
                self.bid_ask = self.purged(self.bid_ask, "bid_ask")
                self.level2 = self.purged(self.level2, "level2")
                self.fut_low_high = self.purged(self.fut_low_high, "fut_low_high")
        finally:
            if self.purge_interval:
                Timer(self.purge_interval.seconds, self.purge_all).start()

    def purged(self, df: Optional[pd.DataFrame], tag: str) -> Optional[pd.DataFrame]:
        """
        Purge data frame
        """
        purge_window = self.history_max_window
        self._log.debug(f"Purging old {tag} data using window {purge_window}")
        if df is None or df.empty:
            return df
        left_bound = df.index.max() - pd.Timedelta(purge_window)
        return df[df.index >= left_bound]

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

        if interval_flag and is_closable:
            self.broker.update_cur_trade_status()
            self.last_trade_check_time = datetime.utcnow()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model \
                and not self.is_processing and self.X_pipe and self.y_pipe:
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

        # Buy signal
        # Not zeroes and ratio is ok and max/min are ok
        is_buy_ratio = buy_profit > 0 and (buy_loss <= 0 or buy_profit / buy_loss >= self.profit_loss_ratio)
        is_buy_loss = abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_profit = abs(buy_profit) >= self.profit_min_coeff * ask
        is_buy = is_buy_ratio and is_buy_loss and is_buy_profit

        # Sell signal
        # Not zeroes and ratio is ok and max/min are ok
        is_sell_ratio = sell_profit > 0 and (sell_loss <= 0 or sell_profit / sell_loss >= self.profit_loss_ratio)
        is_sell_loss = abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_profit = abs(sell_profit) >= self.profit_min_coeff * bid
        is_sell = is_sell_ratio and is_sell_loss and is_sell_profit

        # This should not happen, but let's handle it and clear the flags
        if is_buy and is_sell:
            is_buy = is_sell = False

        if is_buy:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            stop_loss_adj = min(stop_loss_adj, round(ask * (1 - self.stop_loss_min_coeff), self.price_precision))
            return 1, ask, stop_loss_adj, ask + buy_profit
        elif is_sell:
            # Sell and possibly fix the loss
            stop_loss_adj = bid + abs(sell_loss) * 1.25
            stop_loss_adj = max(stop_loss_adj, round(bid * (1 + self.stop_loss_min_coeff), self.price_precision))
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
        is_enough_candles = self.has_all_candles()
        # Check learn conditions
        if self.bid_ask.empty or self.level2.empty or not is_enough_candles:
            self._log.debug(f"Can not learn because some datasets are empty. "
                            f"bid_ask.empty: {self.bid_ask.empty}, "
                            f"level2.empty: {self.level2.empty}, "
                            f"not all candles: {not is_enough_candles}")
            return False
        # Check If we have enough data to learn
        interval = self.bid_ask.index.max() - self.bid_ask.index.min()
        if interval < self.history_min_window:
            self._log.debug(
                f"Can not learn because not enough history. We have {interval}, but we need {self.history_min_window}")
            return False
        return True

    def generator_of(self, train_X, train_y):
        """ Data generator for learning """
        return TimeseriesGenerator(train_X, train_y, length=1)

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Get last X for prediction"""
        X = PredictLowHighFeatures.last_features_of(self.bid_ask, 1, self.level2, self.candles_features,
                                                    past_window=self.past_window)
        return X, self.X_pipe.transform(X)

    def learn(self):
        try:
            self._log.debug("Learning")
            if not self.can_learn():
                return
            with self.data_lock:
                # Copy data for this thread only
                bid_ask = self.bid_ask.copy()
                level2 = self.level2.copy()
                candles_features = CandlesFeatures.candles_combined_features_of(self.candles_by_period,
                                                                                self.candles_cnt_by_period)
            train_X, train_y = PredictLowHighFeatures.features_targets_of(
                bid_ask, level2, candles_features, self.predict_window, self.past_window)

            self._log.info(
                f"Learning on last data. Train data len: {train_X.shape[0]}, "
                f"bid_ask len: {bid_ask.shape[0]}, level2 len: {level2.shape[0]}, "
                f"last bid_ask at: {bid_ask.index[-1]}, last level2 at: {level2.index[-1]}, "
                f"last candle at: {candles_features.index[-1]}")
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
                # to avoid OOM
                tensorflow.keras.backend.clear_session()
                gc.collect()

            else:
                self._log.info(f"Not enough train data to learn should be >= {self.min_xy_len}")
        finally:
            if self.learn_interval:
                Timer(self.learn_interval.seconds, self.learn).start()
