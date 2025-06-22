import logging
import multiprocessing
from datetime import datetime

import pandas as pd

from pytrade2.exch.Exchange import Exchange


class LastCandle1MinFeed:
    """ Decorator for strategies. Reads candles from exchange """

    kind = "candles"

    def __init__(self, config, ticker: str, exchange_provider: Exchange, data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_lock = data_lock
        self.exchange_candles_feed = exchange_provider.candles_feed(config["pytrade2.exchange"])
        self.exchange_candles_feed.consumers.add(self)
        self.ticker = ticker
        self.new_data_event = new_data_event
        self.period = "1min"

        self.last_candle = None

    def apply_buf(self):
        """ Combine candles with buffers"""
        ...

    def on_candle(self, candle: {}):
        # self._logger.debug(f"Got candle {candle}")
        period = str(candle["interval"])
        if period == self.period and not self.last_candle or candle["close_time"] > self.last_candle["close_time"]:
            self.last_candle = candle
            self.new_data_event.set()

    def has_min_history(self):
        """ If gathered required history """
        return self.last_candle is not None

    def is_alive(self, _):
        dt = datetime.now()

        max_lag = pd.Timedelta(self.period) * 2
        lag = datetime.now() - self.last_candle["close_time"] if self.last_candle else pd.Timedelta(0)
        is_alive = self.last_candle is not None and lag < max_lag
        if not is_alive:
            self._logger.warning(
                f"{self.__class__.__name__} {self.ticker}:{i} is dead. lag: {lag}, max lag allowed: {max_lag}")
            return False
        return True
