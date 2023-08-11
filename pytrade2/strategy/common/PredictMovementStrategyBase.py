import logging
import multiprocessing
import traceback
from datetime import datetime
from io import StringIO
from threading import Event, Timer
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder, LabelEncoder, FunctionTransformer

from exch.Exchange import Exchange
from strategy.common.CandlesStrategy import CandlesStrategy
from strategy.common.StrategyBase import StrategyBase
from strategy.common.features.CandlesFeatures import CandlesFeatures
from strategy.common.features.PredictBidAskFeatures import PredictBidAskFeatures
from strategy.common.features.SignalLabelTransformer import SignalLabelTransformer


class PredictMovementStrategyBase(StrategyBase, CandlesStrategy):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None
        self.candles_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        CandlesStrategy.__init__(self, config=config, ticker=self.ticker, candles_feed=self.candles_feed)
        self.data_lock = multiprocessing.RLock()
        self.new_data_event: Event = Event()

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        # Broker report
        if hasattr(self.broker, "get_report"):
            msg.write(self.broker.get_report())

        # Candles report
        for i, t in self.last_candles_info().items():
            msg.write(f"\nLast {i} candle: {t}")
        return msg.getvalue()

    def run(self):
        """
        Attach to the feed and listen
        """
        exchange_name = self.config["pytrade2.exchange"]

        # Create feed and broker
        self.websocket_feed = self.exchange_provider.websocket_feed(exchange_name)
        self.websocket_feed.consumers.add(self)
        self.candles_feed = self.exchange_provider.candles_feed(exchange_name)
        self.candles_feed.consumers.add(self)

        self.broker = self.exchange_provider.broker(exchange_name)

        self.read_initial_candles()

        StrategyBase.run(self)

        if self.purge_interval:
            self._log.info(f"Starting periodical purging, interval: {self.purge_interval}")
            Timer(self.purge_interval.seconds, self.purge_all).start()
        # Run the feed, listen events
        self.candles_feed.run()
        self.broker.run()

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""

        if not self.has_all_candles():
            self._log.info(f"Can not learn because not enough candles.")
            return False

        # Check If we have enough data to learn
        return True

    def process_new_data(self):
        if self.model:
            with self.data_lock:
                x = CandlesFeatures.candles_last_combined_features_of(self.candles_by_interval,
                                                                      self.candles_cnt_by_interval)
                x_trans = self.X_pipe.transform(x)
                y = self.model.predict(x_trans)
                y_trans = self.y_pipe.inverse_transform(y)
                signal = y_trans[0][0] if y_trans else 0
                print(f"Signal: {signal}")

    def is_alive(self):
        return CandlesStrategy.is_alive(self)

    def purge_all(self):
        pass

    def prepare_Xy(self):
        return CandlesFeatures.features_targets_of(self.candles_by_interval, self.candles_cnt_by_interval,
                                                   min(self.candles_by_interval.keys()))

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", RobustScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MinMaxScaler())])
        x_pipe.fit(X)
        y_pipe = Pipeline([
            ('adjust_labels', SignalLabelTransformer())  # Adjust labels
        ])

        return x_pipe, y_pipe
