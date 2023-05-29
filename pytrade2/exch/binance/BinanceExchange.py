import logging

from binance.spot import Spot

from exch.binance.broker.BinanceBroker import BinanceBroker
from exch.binance.feed.BinanceCandlesFeed import BinanceCandlesFeed
from exch.binance.feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from binance.websocket.spot.websocket_client import SpotWebsocketClient


class BinanceExchange:
    def __init__(self, config: dict):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config

        # Attrs for lazy initialization
        self.__spot_client: Spot = None
        self.__websocket_client: SpotWebsocketClient = None
        self.__broker: BinanceBroker = None
        self.__websocket_feed: BinanceWebsocketFeed = None
        self.__candles_feed: BinanceCandlesFeed = None

    def websocket_feed(self) -> BinanceWebsocketFeed:
        """ Binance websocket feed lazy creation """
        if not self.__websocket_feed:
            self.__websocket_feed = BinanceWebsocketFeed(config=self.config, websocket_client=self._websocket_client())
        return self.__websocket_feed

    def candles_feed(self) -> BinanceCandlesFeed:
        """ Binance candles feed lazy creation """
        if not self.__candles_feed:
            self.__candles_feed = BinanceCandlesFeed(self._spot_client())
        return self.__candles_feed

    def broker(self) -> BinanceBroker:
        """ Binance broker lazy creation """
        if not self.__broker:
            self.__broker = BinanceBroker(self._spot_client(), self.config)
        return self.__broker

    def _websocket_client(self) -> SpotWebsocketClient:
        """ Binance websocket client lazy creation"""
        if not self.__websocket_client:
            self.__websocket_client = SpotWebsocketClient()
        return self.__websocket_client

    def _spot_client(self) -> Spot:
        """ Binance spot client creation. Each strategy can be configured at it's own account"""
        if not self.__spot_client:
            key, secret = self.config["pytrade2.exchange.binance.connector.key"], self.config["pytrade2.exchange.binance.connector.secret"]
            url = self.config["pytrade2.exchange.binance.spot.url"]
            self._log.info(
                f"Init binance client, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            self.__spot_client: Spot = Spot(key=key, secret=secret, base_url=url, timeout=10)
        return self.__spot_client
