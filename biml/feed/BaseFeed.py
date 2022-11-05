import logging.config
from typing import List

import pandas
import pandas as pd
from binance.spot import Spot as Client


from feed.TickerInfo import TickerInfo


class BaseFeed:
    """
    Base class for price data feed. Read data, provide pandas dataframes with that data
    """

    candle_columns = ["open_time", "open", "high", "low", "close", "vol", "close_time", "quote_asset_volume",
                      "number_of_trades", " taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                      "ignore"]

    def __init__(self, tickers: List[TickerInfo], spot_client: Client):
        self.tickers = tickers
        self.spot_client: Client = spot_client

        logging.info(
            f"Feed initialized. candle_columns: {BaseFeed.candle_columns}\n, tickers: {self.tickers}")

        self.consumers = []

    def read(self):
        """
        Read data to pandas
        """
        pass

