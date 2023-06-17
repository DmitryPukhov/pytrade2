import logging
from typing import Optional

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from exch.huobi.hbdm.feed.HuobiCandlesFeedHbdm import HuobiCandlesFeedHbdm
from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm


class HuobiExchangeHbdm:
    """ Huobi derivatives market exchange: futures, swaps """

    def __init__(self, config: dict):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.__rest_client: Optional[HuobiRestClient] = None
        self.__websocket_client: Optional[HuobiWebSocketClient] = None


        self.__websocket_feed: Optional[HuobiWebSocketFeedHbdm] = None
        self.__candles_feed: Optional[HuobiCandlesFeedHbdm] = None

        self.__broker: Optional[HuobiBrokerHbdm] = None

    def _key_secret(self):
        key = self.config["pytrade2.exchange.huobi.connector.key"]
        secret = self.config["pytrade2.exchange.huobi.connector.secret"]
        return key, secret

    def broker(self):
        if not self.__broker:
            self.__broker = HuobiBrokerHbdm(self.config, rest_client= self._rest_client(), ws_client= self._websocket_client())
        return self.__broker

    def candles_feed(self):
        if not self.__candles_feed:
            self.__candles_feed = HuobiCandlesFeedHbdm(self.config, self._rest_client())
        return self.__candles_feed

    def websocket_feed(self) -> HuobiWebSocketFeedHbdm:
        """ Binance websocket feed lazy creation """
        if not self.__websocket_feed:
            self.__websocket_feed = HuobiWebSocketFeedHbdm(config=self.config, client=self._websocket_client())
        return self.__websocket_feed

    def _websocket_client(self) -> HuobiWebSocketClient:
        if not self.__websocket_client:
            # wss://api.hbdm.com/swap-ws
            key, secret = self._key_secret()
            self.__websocket_client = HuobiWebSocketClient(host="api.hbdm.com",
                                                           path="/linear-swap-ws",
                                                           access_key=key,
                                                           secret_key=secret,
                                                           be_spot=False)
        return self.__websocket_client

    def _rest_client(self):
        if not self.__rest_client:
            self.__rest_client = HuobiRestClient(*self._key_secret())
        return self.__rest_client
