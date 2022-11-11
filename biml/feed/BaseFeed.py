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
    bid_ask_columns = ["datetime","symbol","bid","bid_qty","ask","ask_qty"]

    def __init__(self):
        self.consumers = []

    def read(self):
        """
        Read data to pandas
        """
        pass

