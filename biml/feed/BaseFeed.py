import logging.config
from datetime import datetime
from typing import Dict, List

import pandas as pd

from biml.feed.TickerInfo import TickerInfo


class BaseFeed:
    """
    Base class for price data feed. Read data, provide pandas dataframes with that data
    :param ticker: asset code like BTCUSDT
    :param intervals: {"M1":"1m10s"}
    """

    candle_columns = ["open_time", "open", "high", "low", "close", "vol", "close_time", "quote_asset_volume",
                      "number_of_trades", " taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                      "ignore"]

    def __init__(self, tickers: List[TickerInfo]):
        self.tickers = tickers

        # Dictionary interval:limit to read from binance
        # Pandas dataframes to hold data
        # for ti in self.tickers:
        #     ti.candles = [pd.DataFrame(columns=BaseFeed.candle_columns) for _ in ti.candle_intervals]
        logging.info(
            f"Feed initialized. {self.tickers}"
            f"candle_columns: {BaseFeed.candle_columns},\n")
        self.consumers = []

    def read(self):
        """
        Read data to pandas
        """
        pass
