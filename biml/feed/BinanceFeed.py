import logging
import time
from typing import List, Dict

import pandas as pd
from binance.spot import Spot as Client

from biml.feed.BaseFeed import BaseFeed


class BinanceFeed(BaseFeed):
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def __init__(self, spot_client: Client, ticker: str, read_interval: str, limits: Dict[str, int]):
        super().__init__(ticker, limits)
        self.spot_client: Client = spot_client
        self.read_interval = pd.Timedelta(read_interval)

    def run(self):
        """
        Read data periodically
        """
        while True:
            self.read()
            time.sleep(self.read_interval.total_seconds())

    def read(self):
        """
        Read data from binance to pandas
        """
        # Call binance for the data, read only new candles
        start_candle_time = self.last_candle_time_ms + 1 if self.last_candle_time_ms else None
        prev_last_candle_time_ms = self.last_candle_time_ms
        for interval in self.candles:
            limit = self.limits[interval] if not start_candle_time else None
            logging.debug(f"Read data from binance. ticker={self.ticker}, interval={interval}, "
                          f"startTime={start_candle_time}, limit={limit}")
            # Read from binance
            new_binance_candles = self.spot_client.klines(symbol=self.ticker,
                                                          interval=interval,
                                                          limit=limit,
                                                          startTime=start_candle_time)
            # Append new data
            new_candles = pd.DataFrame(data=new_binance_candles, columns=self.candle_columns)
            self.candles[interval] = self.candles[interval].append(other=new_candles)
            # Update last candle time if new candles are later
            self.last_candle_time_ms = max(self.last_candle_time_ms, self.candles[interval]["close_time"].max())
        # Produce on_candles event if present
        if self.last_candle_time_ms > prev_last_candle_time_ms:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_candles')]:
                consumer.on_candles(src=self, candles=self.candles, new_data_start_time_ms=prev_last_candle_time_ms + 1)
