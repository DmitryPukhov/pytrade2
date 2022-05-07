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
            logging.info(f"Sleeping for {self.read_interval}")
            time.sleep(self.read_interval.total_seconds())

    def preprocess(self, df: pd.DataFrame)->pd.DataFrame:
        df["open_time"]=pd.to_datetime(df["open_time"], unit='ms')
        df["close_time"]=pd.to_datetime(df["open_time"], unit='ms')
        return df

    def read(self):
        """
        Read data from binance to pandas
        """
        # Call binance for the data, read only new candles
        start_candle_time_ms = self.last_candle_time_ms + 1 if self.last_candle_time_ms else None

        prev_last_candle_time_ms = self.last_candle_time_ms
        new_candles: Dict[str, pd.DataFrame] = dict()
        for interval in self.candles:
            limit = self.limits[interval] if not start_candle_time_ms else None
            logging.debug(f"Read data from binance. ticker={self.ticker}, interval={interval}, "
                          f"startTime={pd.to_datetime(start_candle_time_ms, unit='ms')}, limit={limit}")
            # Read from binance
            # new_binance_candles = self.spot_client.klines(symbol=self.ticker,
            #                                               interval=interval,
            #                                               limit=limit,
            #                                               startTime=start_candle_time_ms)
            new_binance_candles = self.spot_client.klines(symbol=self.ticker,
                                                          interval=interval,
                                                          limit=limit)
            # Append new data
            new_candles_df = pd.DataFrame(data=new_binance_candles, columns=self.candle_columns)
            self.last_candle_time_ms = max(self.last_candle_time_ms, new_candles_df["close_time"].max())
            logging.info(f"Last candle time={pd.to_datetime(self.last_candle_time_ms, unit='ms')}")
            new_candles_df = self.preprocess(new_candles_df)
            self.candles[interval] = self.candles[interval].append(other=new_candles_df)
            new_candles[interval] = new_candles_df
            # Update last candle time if new candles are later

        # Produce on_candles event if present
        new_candles_dict = {self.ticker: new_candles}
        if self.last_candle_time_ms > prev_last_candle_time_ms:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_candles')]:
                consumer.on_candles(candles_dict=new_candles_dict)
