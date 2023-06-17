import logging
from datetime import datetime

import pandas as pd
from huobi.client.market import MarketClient
from huobi.model.market.candlestick import Candlestick

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient


class HuobiCandlesFeedHbdm:
    """
    Huobi candles feed on Huobi derivatives market.
    """

    def __init__(self, config: dict, rest_client: HuobiRestClient):
        self._log = logging.getLogger(self.__class__.__name__)
        self.rest_client = rest_client

    def read_candles(self, ticker, interval, limit):
        """ Read candles from Huobi """

        return pd.DataFrame()

