import logging.config
from typing import Optional

from binance.spot import Spot as Client
from datetime import datetime, timedelta
import pandas as pd
from biml.feed.BaseFeed import BaseFeed


class BinanceFeed(BaseFeed):
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def __init__(self, spot_client: Client, ticker: str, read_interval: str,
                 candle_fast_interval: str, candle_fast_limit: int,
                 candle_medium_interval: str, candle_medium_limit: int):
        super().__init__(ticker, candle_fast_interval, candle_fast_limit, candle_medium_interval, candle_medium_limit)
        self.spot_client: Client = spot_client
        self.read_interval = pd.Timedelta(read_interval)
        self.last_candle_time_ms = None
        self.last_read_time = None

    def run(self):
        """
        Read data periodically
        """
        self.read()

    def read_raw_candles(self, fast: list, medium: list):
        """
        Read from raw data buffer to pandas
        """
        self.candles_fast = self.candles_fast.append(other=pd.DataFrame(data=fast, columns=self.candle_columns))
        self.candles_medium = self.candles_medium.append(other=pd.DataFrame(data=medium, columns=self.candle_columns))
        self.last_candle_time_ms = self.candles_fast["close_time"].max()

    def read(self):
        """
        Read data from binance to pandas
        """
        if self.last_read_time and (datetime.now() - self.last_read_time) < self.read_interval:
            # If read interval is not elapsed yet
            return

        # Call binance for the data, read only new candles
        start_candle_time = self.last_candle_time_ms + 1 if self.last_candle_time_ms else None
        fast = self.spot_client.klines(symbol=self.ticker,
                                       interval=self.candle_fast_interval,
                                       limit=self.candle_fast_limit if not start_candle_time else None,
                                       startTime=start_candle_time)
        medium = self.spot_client.klines(symbol=self.ticker,
                                         interval=self.candle_medium_interval,
                                         limit=self.candle_medium_limit if not start_candle_time else None,
                                         startTime=start_candle_time)
        # Load raw data to pandas dataframes
        self.read_raw_candles(fast=fast, medium=medium)

        self.last_read_time = datetime.now()
