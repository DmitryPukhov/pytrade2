import gc
import logging
import multiprocessing
import time
import traceback
from datetime import datetime, timedelta
from threading import Thread, Event, Timer
from typing import Dict

import pandas as pd
import tensorflow.python.keras.backend
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MaxAbsScaler

from exch.Exchange import Exchange
from metrics.MetricServer import MetricServer
from strategy.common.RiskManager import RiskManager
from feed.BidAskFeed import BidAskFeed
from feed.CandlesFeed import CandlesFeed
from feed.Level2Feed import Level2Feed
from strategy.persist.DataPersister import DataPersister
from strategy.persist.ModelPersister import ModelPersister


class StrategyBase:
    """ Any strategy """

    def __init__(self, config: Dict, exchange_provider: Exchange, is_candles_feed: bool, is_bid_ask_feed: bool,
                 is_level2_feed: bool):
        self._logger = logging.getLogger(self.__class__.__name__)

        strategy_name = self.__class__.__name__
        self.data_persister = DataPersister(config, strategy_name)
        self.model_name = strategy_name.rstrip("Strategy")
        self.model_persister = ModelPersister(config, strategy_name)
        self.model_source = config.get("pytrade2.model.source", "local")  # or "all"
        self.app_params: dict = {}
        self.model_version = {}

        self.config = config
        self.tickers = self.config["pytrade2.tickers"].split(",")
        self.ticker = self.tickers[-1]

        self.risk_manager = None
        self.data_lock = multiprocessing.RLock()

        self.is_periodical = False
        self.new_data_event: Event = Event()
        self.candles_feed = self.bid_ask_feed = self.level2_feed = None
        if is_candles_feed:
            self.candles_feed = CandlesFeed(config, self.ticker, exchange_provider, self.data_lock, self.new_data_event,
                                            strategy_name)
        if is_level2_feed:
            self.level2_feed = Level2Feed(config, exchange_provider, self.data_lock, self.new_data_event)
        if is_bid_ask_feed:
            self.bid_ask_feed = BidAskFeed(config, exchange_provider, self.data_lock, self.new_data_event)
        # self.learn_data_balancer = LearnDataBalancer()
        self.order_quantity = config["pytrade2.order.quantity"]
        self.is_trailing_stop = config.get("pytrade2.order.is_trailingstop", "false").lower() == "true"
        self._logger.info(f"Order quantity: {self.order_quantity}")
        self.price_precision = config["pytrade2.price.precision"]
        self.amount_precision = config["pytrade2.amount.precision"]
        self.learn_interval = pd.Timedelta(config['pytrade2.strategy.learn.interval']) \
            if 'pytrade2.strategy.learn.interval' in config else None
        self._wait_after_loss = pd.Timedelta(config["pytrade2.strategy.riskmanager.wait_after_loss"])
        self.exchange_provider = exchange_provider
        self.model = None
        self.broker = None
        self.is_processing = False
        self.is_learn_enabled = config.get("pytrade2.strategy.learn.enabled", "true").lower() == "true"

        # Expected profit/loss >= ratio means signal to trade
        self.profit_loss_ratio = float(config.get("pytrade2.strategy.profitloss.ratio", 1.0))

        # stop loss should be above price * min_stop_loss_coeff
        # 0.00005 for BTCUSDT 30000 means 1,5
        self.stop_loss_min_coeff = config.get("pytrade2.strategy.stoploss.min.coeff", 0.0)

        # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.stop_loss_max_coeff = config.get("pytrade2.strategy.stoploss.max.coeff", float('inf'))

        self.stop_loss_add_ratio = config.get("pytrade2.strategy.stoploss.add.ratio", 0.0)

        # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
        self.take_profit_min_coeff = config.get("pytrade2.strategy.takeprofit.min.coeff", 0.0)
        self.take_profit_max_coeff = config.get("pytrade2.strategy.takeprofit.max.coeff", float('inf'))

        self.history_min_window = pd.Timedelta(config["pytrade2.strategy.history.min.window"])
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])

        self.trade_check_interval = timedelta(seconds=30)
        self.last_trade_check_time = datetime.utcnow() - self.trade_check_interval

        self.model_check_interval = timedelta(seconds=30)
        self.last_model_check_time = datetime.utcnow() - self.model_check_interval

        self.min_xy_len = 2
        self.X_pipe, self.y_pipe = None, None

        self.processing_interval = pd.Timedelta(config.get('pytrade2.strategy.processing.interval', '30 seconds'))

        self._logger.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

    def run(self):
        exchange_name = self.config["pytrade2.exchange"]
        self.broker = self.exchange_provider.broker(exchange_name)
        if self.candles_feed:
            self.candles_feed.read_candles()
            if not self.is_periodical:
                self.candles_feed.run()
        if self.level2_feed and not self.is_periodical:
            self.level2_feed.run()
        self.risk_manager = RiskManager(self.broker, self._wait_after_loss)

        with self.data_lock:
            # Create pipe and model
            self.update_model()
            train_X, train_y = self.prepare_xy()
            self.X_pipe, self.y_pipe = self.create_pipe(train_X, train_y)
            # Learn the model
            if self.is_learn_enabled:
                self.learn()

        # Start main processing loop
        Thread(target=self.processing_loop).start()
        self.broker.run()

    def processing_loop(self):
        self._logger.info("Starting processing loop")

        # If alive is None, not started, so continue loop
        is_alive = self.is_alive()
        while is_alive or is_alive is None:
            try:
                # Wait for new data received
                # while not self.new_data_event.is_set():
                if self.new_data_event and not self.is_periodical:
                    self.new_data_event.wait()
                    self.new_data_event.clear()

                # Learn and predict only if no gap between level2 and bidask
                self.update_model(is_periodical=True)
                self.process_new_data()
                gc.collect()

                if self.processing_interval.total_seconds() > 0:
                    # Delay before next processing cycle
                    time.sleep(self.processing_interval.total_seconds())
                # Refresh live status
                is_alive = self.is_alive()
            except Exception as e:
                logging.exception(e)
                self._logger.error("Exiting")
                exit(1)

        self._logger.info("End main processing loop")

    def is_alive(self):
        maxdelta = self.history_min_window + pd.Timedelta("60s")
        feeds = filter(lambda f: f, [self.candles_feed, self.bid_ask_feed, self.level2_feed])
        is_alive = all([feed.is_alive(maxdelta) for feed in feeds])
        if not is_alive:
            self._logger.info(self.get_report())
            self._logger.error(f"Strategy is not alive for {maxdelta}")
        return is_alive

    def get_info(self) -> dict:
        """ Short info for """
        ...

    def get_report(self) -> dict:
        """ Short info for report """

        report = {}
        # Broker report
        if hasattr(self.broker, "get_report"):
            report.update(self.broker.get_report())
        # Feeds reports
        for feed in filter(lambda f: hasattr(f, "get_report"),
                           [self.candles_feed, self.bid_ask_feed, self.level2_feed]):
            report.update(feed.get_report())
            # msg.write("\n")
        return report

    def check_cur_trade(self):
        """ Update cur trade if sl or tp reached """
        if not self.broker.cur_trade:
            return

        # Timeout from last check passed
        if datetime.utcnow() - self.last_trade_check_time >= self.trade_check_interval:
            self.broker.update_cur_trade_status()
            self.last_trade_check_time = datetime.utcnow()

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""
        feeds = filter(lambda f: f, [self.candles_feed, self.bid_ask_feed, self.level2_feed])
        status = {feed.__class__.__name__: feed.has_min_history() for feed in feeds}
        has_min_history = all([f for f in status.values()])
        if not has_min_history:
            self._logger.info(f"Can not learn because some datasets have not enough data. Filled status {status}")
        return has_min_history

    def update_model(self, is_periodical=False):
        """ Read last trade ready model from mlflow"""
        if is_periodical and (datetime.utcnow() - self.last_model_check_time) < self.model_check_interval:
            # For periodical update, skip if check interval is not elapsed
            return

        if self.model_source == "local":
            # Get model from local file, usually this is for dev
            self.model = self.model_persister.load_last_model()
        else:
            # Get model from mlflow server
            model, model_version, params = self.model_persister.get_last_trade_ready_model(self.model_name)
            is_model_changed = model and model_version and (model_version != self.model_version)
            is_params_changed = params and (params != self.app_params)
            # Set model if changaed
            if is_model_changed:
                self._logger.info(f"Updating model {self.model_name} from v{self.model_version} to {model_version}")
                self.model, self.model_version = model, model_version

            # Set params i
            if is_params_changed:
                self._logger.info(f"Updating model params from {self.app_params} to {params}")
                self.apply_params(params)
                self.app_params = params

                MetricServer.app_params["model"] = f"{self.model_version.name} v{self.model_version.version}"
        self.last_model_check_time = datetime.utcnow()

    def create_model(self, x_size, y_size):
        raise NotImplementedError()

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", StandardScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MaxAbsScaler())])
        x_pipe.fit(X)

        y_pipe = Pipeline(
            [("yrs", StandardScaler()),
             ("ymms", MaxAbsScaler())])
        y_pipe.fit(y)
        return x_pipe, y_pipe

    def prepare_xy(self):
        raise NotImplementedError("prepare_Xy")

    def learn(self):
        if not self.is_learn_enabled:
            self._logger.info("Learning is disabled")
            return
        try:
            # Update model and clear buffers
            self.apply_buffers()
            self.update_model()

            self._logger.debug("Learning")
            if not self.can_learn():
                return

            train_X, train_y = self.prepare_xy()

            # Metrics
            train_period_sec = (train_X.index.max() - train_X.index.min()).total_seconds()
            MetricServer.metrics.strategy.learn.train_period_sec.set(train_period_sec)

            self._logger.info(
                f"Learning on last data. Train data len: {train_X.shape[0]} from {min(train_X.index)} to {max(train_X.index)}")
            if len(train_X.index) >= self.min_xy_len:
                start_time = datetime.utcnow()
                if not (self.X_pipe and self.y_pipe):
                    self.X_pipe, self.y_pipe = self.create_pipe(train_X, train_y)
                # Final scaling and normalization
                self.X_pipe.fit(train_X)
                self.y_pipe.fit(train_y)

                X_trans, y_trans = self.X_pipe.transform(train_X), self.y_pipe.transform(train_y)
                # If x window transformation applied, x size reduced => adjust y
                y_trans = y_trans[-X_trans.shape[0]:]

                # Get or create model, parameters
                if not self.model:
                    self.model = self.create_model(X_trans.shape[-1], y_trans.shape[-1])

                # Train
                self.model.fit(X_trans, y_trans)

                # Save weights and xy new delta
                # todo: uncomment
                self.model_persister.save_model(self.model)

                # to avoid OOM
                tensorflow.keras.backend.clear_session()
                gc.collect()
                self._logger.info("Learning completed")
                learn_duration = datetime.utcnow() - start_time
                MetricServer.metrics.strategy.learn.train_exec_duration_sec.set(learn_duration.total_seconds())

            else:
                self._logger.info(f"Not enough train data to learn should be >= {self.min_xy_len}")
        finally:
            if self.learn_interval and self.is_learn_enabled:
                Timer(self.learn_interval.seconds, self.learn).start()

    def apply_params(self, params: dict) -> None:
        """ After last model and params read from mlflow, apply params to strategy"""
        for name, val in {name: val for name, val in params.items() if hasattr(self, name)}.items():
            val_type = type(getattr(self, name))
            if val_type == bool:
                typed_val = not (str(val).lower() == "false")
            else:
                typed_val = type(getattr(self, name))(val)
            setattr(self, name, typed_val)

        # Update metrics server with params from strategy
        MetricServer.app_params = {name: getattr(self, name) if hasattr(self, name) else None
                                   for name, val in params.items()}

    def prepare_last_x(self):
        raise NotImplementedError("prepare_Xy")

    def predict(self, x: pd.DataFrame):
        raise NotImplementedError()

    def apply_buffers(self):
        """ Append new data from buffers to main data frames """
        with (self.data_lock):
            [feed.apply_buf() for feed in [self.bid_ask_feed, self.candles_feed, self.level2_feed] if feed]

    def process_new_data(self):

        if self.model and not self.is_processing:
            start_time = datetime.utcnow()
            try:
                self.is_processing = True
                self.apply_buffers()
                x = self.prepare_last_x()
                # x can be dataframe or np array, check is it empty
                if (hasattr(x, 'empty') and x.empty) or (hasattr(x, 'shape') and x.shape[0] == 0):
                    self._logger.info('Cannot process new data: features are empty. ')
                    return
                # Predict
                y_pred = self.predict(x)

                # Update current trade status
                self.check_cur_trade()

                # Open or close or do nothing
                self.process_prediction(y_pred)

                # Save to disk for analysis
                #y_pred["datetime"] = dt
                self.data_persister.save_last_data(self.ticker, {'y_pred': y_pred})
            except Exception as e:
                self._logger.error(f"{e}. Traceback: {traceback.format_exc()}")
            finally:
                # Set metrics
                process_duration = datetime.utcnow() - start_time
                MetricServer.metrics.strategy.process.process_duration_sec.set(process_duration.total_seconds())
                self.is_processing = False

    def process_prediction(self, y_pred):
        raise NotImplementedError()
