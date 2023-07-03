import gc
import logging
import multiprocessing
from datetime import datetime, timedelta
from threading import Thread, Event, Timer
from typing import Dict, Optional

import pandas as pd
import tensorflow.python.keras.backend
from keras.preprocessing.sequence import TimeseriesGenerator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler

from exch.Exchange import Exchange
from strategy.common.PersistableStateStrategy import PersistableStateStrategy


class StrategyBase(PersistableStateStrategy):
    """ Any strategy """
    def __init__(self, config: Dict, exchange_provider: Exchange):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.tickers = self.config["pytrade2.tickers"].split(",")
        self.ticker = self.tickers[-1]
        self.order_quantity = config["pytrade2.order.quantity"]
        self._log.info(f"Order quantity: {self.order_quantity}")
        self.price_precision = config["pytrade2.price.precision"]
        self.amount_precision = config["pytrade2.amount.precision"]
        self.learn_interval = pd.Timedelta(config['pytrade2.strategy.learn.interval']) \
            if 'pytrade2.strategy.learn.interval' in config else None
        # Purge params
        self.purge_interval = pd.Timedelta(config['pytrade2.strategy.purge.interval']) \
            if 'pytrade2.strategy.purge.interval' in config else None
        self.exchange_provider = exchange_provider
        self.model = None
        self.broker = None
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

        self.trade_check_interval = timedelta(seconds=30)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval
        self.min_xy_len = 2
        self.X_pipe, self.y_pipe = None, None

        self.data_lock: multiprocessing.RLock() = None
        self.new_data_event: Optional[Event] = None

        PersistableStateStrategy.__init__(self, config)


    def check_cur_trade(self):
        """ Update cur trade if sl or tp reached """
        if not self.broker.cur_trade:
            return

        # Timeout from last check passed
        if datetime.utcnow() - self.last_trade_check_time >= self.trade_check_interval:
            self.broker.update_cur_trade_status()
            self.last_trade_check_time = datetime.utcnow()

    def generator_of(self, train_X, train_y):
        """ Data generator for learning """
        return TimeseriesGenerator(train_X, train_y, length=1)

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

    def can_learn(self):
        raise NotImplementedError

    def prepare_Xy(self):
        raise NotImplementedError

    def learn(self):
        try:
            self._log.debug("Learning")
            if not self.can_learn():
                return

            train_X, train_y = self.prepare_Xy()

            self._log.info(
                f"Learning on last data. Train data len: {train_X.shape[0]}")
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

    def processing_loop(self):
        self._log.info("Starting processing loop")

        # If alive is None, not started, so continue loop
        is_alive = self.is_alive()
        while is_alive or is_alive is None:

            # Wait for new data received
            #while not self.new_data_event.is_set():
            self.new_data_event.wait()
            self.new_data_event.clear()

            # Learn and predict only if no gap between level2 and bidask
            self.process_new_data()

            # Refresh live status
            is_alive = self.is_alive()

        self._log.info("End main processing loop")

    def run(self):
        # Start main processing loop
        Thread(target=self.processing_loop).start()

        # Start periodical jobs
        if self.learn_interval:
            self._log.info(f"Starting periodical learning, interval: {self.learn_interval}")
            Timer(self.learn_interval.seconds, self.learn).start()

    def create_model(self, param, param1):
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()

    def process_new_data(self):
        raise  NotImplementedError()