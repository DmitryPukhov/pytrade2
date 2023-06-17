import logging
import re
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
        """ Got subscribed data from socket"""
        try:
            # Channel like "market.BTC-USDT.bbo"
            ch = msg.get("ch")

            # If bidask received
            if ch and re.fullmatch("market\\..*\\.bbo", ch):
                bidask = self.rawticker2model(msg["tick"])
                for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                    consumer.on_ticker(bidask)
        except Exception as e:
            self._log.error(e)

    @staticmethod
    def rawticker2model(tick: dict) -> Dict:
        dt = datetime.utcfromtimestamp(tick["ts"] / 1000)
        ticker = re.match("market\\.(.*)\\.bbo", tick["ch"]).group(1)
        return {"datetime": dt,
                "symbol": ticker,
                "bid": tick["bid"][0],
                "bid_vol": tick["bid"][1],
                "ask": tick["ask"][0],
                "ask_vol": tick["ask"][1]
                }
