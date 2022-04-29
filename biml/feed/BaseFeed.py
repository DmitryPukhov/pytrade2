import logging.config
from typing import Dict

import pandas as pd


class BaseFeed:
    """
    Base class for price data feed. Read data, provide pandas dataframes with that data
    :param ticker: asset code like BTCUSDT
    :param intervals: {"M1":"1m10s"}
    """

    def __init__(self, ticker: str, limits: Dict[str, int]):
        self.ticker = ticker
        self.candle_columns = ["open_time", "open", "high", "low", "close", "vol", "close_time", "quote_asset_volume",
                               "number_of_trades", " taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                               "ignore"]
        # Dictionary interval:limit to read from binance
        self.limits = limits
        # Pandas dataframes to hold data
        self.candles: Dict[str, pd.DataFrame] = dict(
            [(interval, pd.DataFrame(columns=self.candle_columns)) for interval in limits])
        logging.info(
            f"Feed initialized. "
            f"intervals with read limits: {self.limits}"
            f"candle_columns: {self.candle_columns},\n")
        self.consumers = []
        self.last_candle_time_ms = 0

    def read(self):
        """
        Read data to pandas
        """
        pass
