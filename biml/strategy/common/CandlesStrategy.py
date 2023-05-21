import logging
from datetime import datetime

import pandas as pd
from binance.spot import Spot as Client

from feed.BinanceCandlesFeed import BinanceCandlesFeed
from strategy.common.features.CandlesFeatures import CandlesFeatures


class CandlesStrategy:
    """ Decorator for strategies. Reads candles from Binance """

    def __init__(self, ticker: str, spot_client: Client):
        self.last_candles_read_time = datetime.min
        self._log = logging.getLogger(self.__class__.__name__)
        self.feed = BinanceCandlesFeed(spot_client)
        self.ticker = ticker
        self.candles_features = pd.DataFrame()
        # Candles intervals
        self.candles_fast_interval, self.candles_slow_interval = "1m", "5m"
        self.candles_fast_limit, self.candles_slow_limit = 5, 5
        self._log.info(f"Candles fast interval:{self.candles_fast_interval}, limit: {self.candles_fast_limit}")
        self._log.info(f"Candles slow interval:{self.candles_slow_interval}, limit: {self.candles_slow_limit}")

    def read_candles_or_skip(self):
        """ If time elapsed, read candles from binance."""

        read_interval = pd.Timedelta(self.candles_fast_interval)
        elapsed = datetime.utcnow() - self.last_candles_read_time
        if elapsed >= read_interval:
            self._log.debug(f"Reading last {self.ticker} candles from binance")
            # Read fast, clow candles from binance
            candles_fast = self.feed.read_candles(self.ticker, self.candles_fast_interval, self.candles_fast_limit)
            candles_slow = self.feed.read_candles(self.ticker, self.candles_slow_interval, self.candles_slow_limit)
            # Prepare candles features
            self.candles_features = CandlesFeatures.candles_combined_features_of(candles_fast, self.candles_fast_limit,
                                                                                 candles_slow, self.candles_slow_limit)

            self.last_candles_read_time = datetime.utcnow()
