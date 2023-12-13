import gc
import logging
import multiprocessing
import sys
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
from strategy.common.LearnDataBalancer import LearnDataBalancer
from strategy.common.PersistableStateStrategy import PersistableStateStrategy
from strategy.common.RiskManager import RiskManager


class StrategyBase(PersistableStateStrategy):
    """ Any strategy """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.risk_manager = None
        self.config = config
        self.learn_data_balancer = LearnDataBalancer()
        self.tickers = self.config["pytrade2.tickers"].split(",")
        self.ticker = self.tickers[-1]
        self.order_quantity = config["pytrade2.order.quantity"]
        logging.info(f"Order quantity: {self.order_quantity}")
        self.price_precision = config["pytrade2.price.precision"]
        self.amount_precision = config["pytrade2.amount.precision"]
        self.learn_interval = pd.Timedelta(config['pytrade2.strategy.learn.interval']) \
            if 'pytrade2.strategy.learn.interval' in config else None
        # Purge params
        self.purge_interval = pd.Timedelta(config['pytrade2.strategy.purge.interval']) \
            if 'pytrade2.strategy.purge.interval' in config else None
        self._wait_after_loss = pd.Timedelta(config["pytrade2.strategy.riskmanager.wait_after_loss"])
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
        self.stop_loss_max_coeff = config.get("pytrade2.strategy.stoploss.max.coeff", float('inf'))
        # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
        self.profit_min_coeff = config.get("pytrade2.strategy.profit.min.coeff", 0)
        self.profit_max_coeff = config.get("pytrade2.strategy.profit.max.coeff", float('inf'))

        self.trade_check_interval = timedelta(seconds=30)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval
        self.min_xy_len = 2
        self.X_pipe, self.y_pipe = None, None

        self.data_lock: multiprocessing.RLock() = None
        self.new_data_event: Optional[Event] = None

        PersistableStateStrategy.__init__(self, config)

        logging.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

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
        raise NotImplementedError("prepare_Xy")

    def learn(self):
        try:
            self.apply_buffers()
            logging.debug("Learning")
            if not self.can_learn():
                return

            train_X, train_y = self.prepare_Xy()

            logging.info(
                f"Learning on last data. Train data len: {train_X.shape[0]}")
            if len(train_X.index) >= self.min_xy_len:
                if not (self.X_pipe and self.y_pipe):
                    self.X_pipe, self.y_pipe = self.create_pipe(train_X, train_y)

                # Final scaling and normalization
                self.X_pipe.fit(train_X)
                self.y_pipe.fit(train_y)

                X_trans, y_trans = self.X_pipe.transform(train_X), self.y_pipe.transform(train_y)

                # Train
                if not self.model:
                    self.model = self.create_model(X_trans.shape[1], y_trans.shape[1])
                # Generator produces error
                # gen = self.generator_of(X_trans, y_trans)
                # self.model.fit(gen)
                self.model.fit(X_trans, y_trans)

                # Save weights and xy new delta
                self.save_model()
                self.save_learn_xy_new(self.ticker, train_X, train_y)

                # to avoid OOM
                tensorflow.keras.backend.clear_session()
                gc.collect()

            else:
                logging.info(f"Not enough train data to learn should be >= {self.min_xy_len}")
        finally:
            if self.learn_interval:
                Timer(self.learn_interval.seconds, self.learn).start()

    def processing_loop(self):
        logging.info("Starting processing loop")

        # If alive is None, not started, so continue loop
        is_alive = self.is_alive()
        while is_alive or is_alive is None:
            try:
                # Wait for new data received
                # while not self.new_data_event.is_set():
                if self.new_data_event:
                    self.new_data_event.wait()
                    self.new_data_event.clear()

                # Learn and predict only if no gap between level2 and bidask
                self.process_new_data()

                # Refresh live status
                is_alive = self.is_alive()
            except Exception as e:
                logging.error(e)
                sys.exit(1)

        logging.info("End main processing loop")

    def run(self):
        self.risk_manager = RiskManager(self.broker, self._wait_after_loss)

        # Start main processing loop
        Thread(target=self.processing_loop).start()

        # Start periodical jobs
        if self.learn_interval:
            logging.info(f"Starting periodical learning, interval: {self.learn_interval}")
            Timer(self.learn_interval.seconds, self.learn).start()

        if self.purge_interval:
            logging.info(f"Starting periodical purging, interval: {self.purge_interval}")
            Timer(self.purge_interval.seconds, self.purge_all).start()

    def create_model(self, x_size, y_size):
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()

    def process_new_data(self):
        raise NotImplementedError()

    def purge_all(self):
        # If no data purging in child strategy, just not override this method
        pass

    def apply_buffers(self):
        pass
