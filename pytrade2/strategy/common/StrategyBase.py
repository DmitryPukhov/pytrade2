import gc
import logging
import multiprocessing
import traceback
from datetime import datetime, timedelta
from io import StringIO
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

from strategy.common.features.PredictBidAskFeatures import PredictBidAskFeatures


class StrategyBase:
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
        self.exchange_provider = exchange_provider

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
