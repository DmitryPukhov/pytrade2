import logging
import traceback
from datetime import datetime
from typing import Dict

import pandas as pd
from huobi.client.market import MarketClient
from huobi.model.market import *
from huobi.utils import PrintBasic

from exch.huobi.HuobiTools import HuobiTools
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient


class HuobiWebSocketFeedHbdm:
    """
    Huobi derivatives market web socket.
    """

    def __init__(self, config: dict, client: HuobiWebSocketClient):
        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
        self.tickers = config["pytrade2.tickers"].lower().split(",")
        self._client = client
        self._client.consumers.append(self)

    def run(self):
        """
        Read data from web socket
        """
        # Subscribe bid/ask
        self._client.open()
        for ticker in self.tickers:
            self._log.info(f"Subscribing to {ticker} feed")
            sub_params = {"sub": f"market.{ticker}.bbo"}
            self._client.sub(sub_params)

    def on_socket_data(self, msg):
        print(f"Got msg {msg}")
