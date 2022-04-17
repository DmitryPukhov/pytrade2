import logging.config
from binance.spot import Spot as Client
from datetime import datetime, timedelta
import pandas as pd
from biml.feed.BaseFeed import BaseFeed


class BinanceFeed(BaseFeed):
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def read_raw(self, fast: list, medium: list):
        """
        Read from raw data buffer to pandas
        """
        self.candles_fast = self.candles_fast.append(other=pd.DataFrame(data=fast, columns=self.candle_columns))
        self.candles_medium = self.candles_medium.append(other=pd.DataFrame(data=medium, columns=self.candle_columns))

    def read(self):
        """
        Read data from binance to pandas
        """
        # Call binance for the data
        fast = self.spot_client.klines(symbol=self.ticker,
                                       interval=self.candle_fast_interval,
                                       limit=self.candle_fast_limit)
        medium = self.spot_client.klines(symbol=self.ticker,
                                         interval=self.candle_medium_interval,
                                         limit=self.candle_medium_limit)
        # Load raw data to pandas dataframes
        self.read_raw(fast=fast, medium=medium)
