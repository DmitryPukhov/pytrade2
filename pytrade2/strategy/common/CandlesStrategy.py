import logging
from datetime import datetime

import pandas as pd

from strategy.common.features.CandlesFeatures import CandlesFeatures


class CandlesStrategy:
    """ Decorator for strategies. Reads candles from Binance """

    def __init__(self, config, ticker: str, candles_feed):
        self.last_candles_read_time = datetime.min
        self._log = logging.getLogger(self.__class__.__name__)
        self.candles_feed = candles_feed
        self.ticker = ticker
        self.candles_features = pd.DataFrame()
        # Candles intervals
        self.candles_fast_interval = config["pytrade2.strategy.candles.fast.interval"]
        self.candles_fast_window = config["pytrade2.strategy.candles.fast.window"]
        self.candles_slow_interval = config["pytrade2.strategy.candles.slow.interval"]
        self.candles_slow_window = config["pytrade2.strategy.candles.slow.window"]
        self._log.info(f"Candles fast interval:{self.candles_fast_interval}, window: {self.candles_fast_window}")
        self._log.info(f"Candles slow interval:{self.candles_slow_interval}, window: {self.candles_slow_window}")

    def read_candles_or_skip(self):
        """ If time elapsed, read candles from binance."""

        read_interval = pd.Timedelta(self.candles_fast_interval)
        elapsed = datetime.utcnow() - self.last_candles_read_time
        if elapsed >= read_interval:
            self._log.debug(f"Reading last {self.ticker} candles from binance")
            # Read fast, clow candles from binance +1 for last candle in progress, +1 for diff, +1 for prediction window
            candles_fast = self.candles_feed.read_candles(self.ticker, self.candles_fast_interval, self.candles_fast_window*2+3)
            candles_slow = self.candles_feed.read_candles(self.ticker, self.candles_slow_interval, self.candles_slow_window*2+3)
            # Prepare candles features
            self.candles_features = CandlesFeatures.candles_combined_features_of(candles_fast, self.candles_fast_window,
                                                                                 candles_slow, self.candles_slow_window)

            self.last_candles_read_time = datetime.utcnow()
