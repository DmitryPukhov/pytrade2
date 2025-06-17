import logging
from typing import Optional

from huobi.client.account import AccountClient
from huobi.client.algo import AlgoClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.connection.impl.websocket_manage import WebsocketManage
from huobi.utils import PrintBasic

from pytrade2.exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from pytrade2.exch.huobi.spot.broker.HuobiBrokerSpot import HuobiBrokerSpot
from pytrade2.exch.huobi.spot.feed.HuobiCandlesFeedSpot import HuobiCandlesFeedSpot
from pytrade2.exch.huobi.spot.feed.HuobiWebsocketFeedSpot import HuobiWebsocketFeedSpot


class HuobiExchangeSpot:
    def __init__(self, config: dict):
        self._logger = logging.getLogger(self.__class__.__name__)
        # Apply fixes to reduce rubbish in logs
        PrintBasic.print_basic = lambda data, name: None  # Supress hiobi api print response ts for each response
        WebsocketManage.on_close = lambda msg: self._logger.info(f"WebsocketManage on_close called: {msg}")

        
        self.config = config

        # Attrs for lazy initialization
        self.__rest_client: Optional[HuobiRestClient] = None
        self.__market_client: Optional[MarketClient] = None
        self.__trade_client: Optional[TradeClient] = None
        self.__algo_client: Optional[AlgoClient] = None
        self.__account_client: Optional[AccountClient] = None
        self.__broker: Optional[HuobiBrokerSpot] = None
        self.__websocket_feed: Optional[HuobiWebsocketFeedSpot] = None
        self.__candles_feed: Optional[HuobiCandlesFeedSpot] = None
        # Supress rubbish logging
        logging.getLogger("huobi-client").setLevel("CRITICAL")
        logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)

    def websocket_feed(self) -> HuobiWebsocketFeedSpot:
        """ Binance websocket feed lazy creation """
        if not self.__websocket_feed:
            self.__websocket_feed = HuobiWebsocketFeedSpot(config=self.config, market_client=self._market_client())
        return self.__websocket_feed

    def candles_feed(self) -> HuobiCandlesFeedSpot:
        """ Binance candles feed lazy creation """
        if not self.__candles_feed:
            self.__candles_feed = HuobiCandlesFeedSpot(self._market_client())
        return self.__candles_feed

    def broker(self) -> HuobiBrokerSpot:
        """ Binance broker lazy creation """
        if not self.__broker:
            self.__broker = HuobiBrokerSpot(config=self.config,
                                            account_client=self._account_client(),
                                            trade_client=self._trade_client(),
                                            market_client=self._market_client(),
                                            algo_client=self._algo_client())
        return self.__broker

    def _key_secret(self):
        key = self.config["pytrade2.exchange.huobi.connector.key"]
        secret = self.config["pytrade2.exchange.huobi.connector.secret"]
        return key, secret

    def _market_client(self):
        if not self.__market_client:
            key, secret = self._key_secret()
            url = self.config.get("pytrade2.exchange.huobi.market.client.url")
            self._logger.info(f"Creating huobi market client, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            self.__market_client = MarketClient(url=url, api_key=key, secret_key=secret, init_log=False)
        return self.__market_client

    def _trade_client(self):
        """ Huobi trade client creation."""
        if not self.__trade_client:
            key, secret = self._key_secret()
            url = self.config.get("pytrade2.exchange.huobi.trade.client.url")
            self._logger.info(f"Creating huobi trade client, url:{url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            self.__trade_client = TradeClient(url=url, api_key=key, secret_key=secret, init_log=False)
        return self.__trade_client

    def _algo_client(self):
        """ Huobi algo client creation."""
        if not self.__algo_client:
            key, secret = self._key_secret()
            url = self.config.get("pytrade2.exchange.huobi.trade.client.url")
            self._logger.info(f"Creating huobi algo trade client, url:{url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            self.__algo_client = AlgoClient(url=url, api_key=key, secret_key=secret, init_log=False)
        return self.__algo_client

    def _account_client(self):
        if not self.__account_client:
            key, secret = self._key_secret()
            url = self.config.get("pytrade2.exchange.huobi.account.client.url")
            self._logger.info(f"Creating huobi account client, url:{url} key: ***{key[-3:]}, secret: ***{secret[-3:]}")
            self.__account_client = AccountClient(url=url, api_key=key, secret_key=secret, init_log=False)
        return self.__account_client
