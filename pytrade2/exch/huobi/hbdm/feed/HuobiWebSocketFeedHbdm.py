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
from exch.huobi.hbdm.feed.HuobiFeedBase import HuobiFeedBase


class HuobiWebSocketFeedHbdm(HuobiFeedBase):
    """
    Huobi derivatives market web socket.
    """

    def __init__(self, config: dict, client: HuobiWebSocketClient):
        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
        self.tickers = config["pytrade2.tickers"].lower().split(",")
        self._client = client
        self._client.consumers.append(self)

    @staticmethod
    def is_bidask(ch):
        """ If channel name is bidask like market.BTC-USDT.bbo """
        return re.fullmatch("market\\..*\\.bbo", ch)

    @staticmethod
    def is_level2(ch):
        """ Is channel name is level2 like market.BTC-USDT.depth.step1"""
        return re.fullmatch("market\\..*\\.depth\\.step\\d+", ch)

    def run(self):
        """
        Read data from web socket
        """
        # Subscribe bid/ask
        self._client.open()
        self.sub_events()

    def sub_events(self):
        for ticker in self.tickers:
            self._log.info(f"Subscribing to {ticker} feed")

            # Sub bid ask, level2
            for sub_params in [{"sub": f"market.{ticker}.bbo"},
                               {"sub": f"market.{ticker}.depth.step0"}]:
                self._log.info(f"Subscribing to {sub_params}")
                self._client.sub(sub_params)
    def on_socket_data(self, msg):
        """ Got subscribed data from socket"""
        try:
            # Channel like "market.BTC-USDT.bbo"
            ch = msg.get("ch")
            if not ch:
                return
            # If bidask received

            if self.is_bidask(ch):
                bidask = self.rawticker2model(msg["tick"])
                for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                    consumer.on_ticker(bidask)
            elif self.is_level2(ch):
                l2 = self.rawlevel2model(msg["tick"])
                for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                    consumer.on_level2(l2)
        except Exception as e:
            self._log.error(e)

    @staticmethod
    def rawlevel2model(tick: dict) -> Dict:

        # dt = datetime.utcfromtimestamp(tick["ts"] / 1000)
        dt = datetime.utcnow()
        ticker = HuobiWebSocketFeedHbdm.ticker_of_ch(tick["ch"])
        out = [{"datetime": dt, "symbol": ticker,
                "bid": float(price), "bid_vol": float(vol)} for price, vol in tick["bids"]] + \
              [{"datetime": dt, "symbol": ticker,
                "ask": float(price), "ask_vol": float(vol)} for price, vol in tick["asks"]]
        return out

    @staticmethod
    def rawticker2model(tick: dict) -> Dict:
        # dt = datetime.utcfromtimestamp(tick["ts"] / 1000)
        dt = datetime.utcnow()
        ticker = HuobiWebSocketFeedHbdm.ticker_of_ch(tick["ch"])
        return {"datetime": dt,
                "symbol": ticker,
                "bid": tick["bid"][0],
                "bid_vol": tick["bid"][1],
                "ask": tick["ask"][0],
                "ask_vol": tick["ask"][1]
                }
