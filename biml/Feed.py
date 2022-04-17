import logging.config
from binance.spot import Spot as Client
from datetime import datetime, timedelta
import pandas as pd


class Feed:
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def __init__(self, spot_client: Client, ticker: str):
        self.spot_client: Client = spot_client
        self.ticker = ticker

        # Fast and medium candles
        self.candle_fast_interval = "1m"
        self.candle_medium_interval = "15m"
        self.candle_fast_limit = 60
        self.candle_medium_limit = 20
        # Column names of binance candle data
        self.candle_columns = ["open_time", "open", "high", "low", "close", "vol", "close_time", "quote_asset_volume",
                               "number_of_trades", " taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                               "ignore"]
        # Pandas dataframes to hold data
        self.candles_fast = pd.DataFrame(columns=self.candle_columns)
        self.candles_medium = pd.DataFrame(columns=self.candle_columns)

        logging.info(
            f"Feed initialized. candle_fast_interval: {self.candle_fast_interval}, candle_fast_limit:{self.candle_fast_limit}"
            f"candle_medium_interval: {self.candle_medium_interval}, candle_medium_limit:{self.candle_medium_limit},\n"
            f"candle_columns: {self.candle_columns},\n")

    def read_raw(self, fast: list, medium: list):
        """
        Read from raw data buffer to pandas
        """
        self.candles_fast = self.candles_fast.append(other=pd.DataFrame(data=fast, columns=self.candle_columns))
        self.candles_medium = self.candles_medium.append(other=pd.DataFrame(data=medium, columns=self.candle_columns))

    def read_binance(self):
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
