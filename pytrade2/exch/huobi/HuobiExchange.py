import logging

from exch.huobi.broker.HuobiBroker import HuobiBroker
from huobi.client.market import MarketClient

from exch.huobi.feed.HuobiCandlesFeed import HuobiCandlesFeed
from exch.huobi.feed.HuobiWebsocketFeed import HuobiWebsocketFeed


class HuobiExchange:
    def __init__(self, config: dict):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config

        # Attrs for lazy initialization
        self.__market_client: MarketClient = None
        self.__broker: HuobiBroker = None
        self.__websocket_feed: HuobiWebsocketFeed = None
        self.__candles_feed: HuobiCandlesFeed = None
        # Supress rubbish logging
        logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)

    def websocket_feed(self) -> HuobiWebsocketFeed:
        """ Binance websocket feed lazy creation """
        if not self.__websocket_feed:
            self.__websocket_feed = HuobiWebsocketFeed(config=self.config, market_client=self._websocket_client())
        return self.__websocket_feed

    def candles_feed(self) -> HuobiCandlesFeed:
        """ Binance candles feed lazy creation """
        if not self.__candles_feed:
            self.__candles_feed = HuobiCandlesFeed(self._websocket_client())
        return self.__candles_feed

    def broker(self) -> HuobiBroker:
        """ Binance broker lazy creation """
        if not self.__broker:
            self.__broker = HuobiBroker(self.config)
        return self.__broker

    def _websocket_client(self) -> MarketClient:
        """ Huobi spot client creation. Each strategy can be configured at it's own account"""
        if not self.__market_client:
            key, secret = self.config["pytrade2.connector.key"], self.config["pytrade2.connector.secret"]
            url = self.config["pytrade2.exchange.huobi.client.websocket.url"]
            self._log.info(
                f"Init Huobi client, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            #self.__market_client: MarketClient = MarketClient(api_key=key, secret_key=secret, url=url, init_log=True)
            # todo: add key and secret
            #self.__market_client: MarketClient = MarketClient()
            self.__market_client: MarketClient = MarketClient(url=url, init_log=True)
        return self.__market_client

    def __market_client(self, url) -> MarketClient:
        """ Huobi spot client creation. Each strategy can be configured at it's own account"""
        if not self.__market_client:
            key, secret = self.config["pytrade2.connector.key"], self.config["pytrade2.connector.secret"]
            url = self.config["pytrade2.connector.url"]
            self._log.info(
                f"Init Huobi client, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            #self.__market_client: MarketClient = MarketClient(api_key=key, secret_key=secret, url=url, init_log=True)
            # todo: add key and secret
            #self.__market_client: MarketClient = MarketClient()
            self.__market_client: MarketClient = MarketClient(url=url, init_log=True)
        return self.__market_client