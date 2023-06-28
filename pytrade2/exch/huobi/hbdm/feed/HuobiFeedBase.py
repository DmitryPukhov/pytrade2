import logging
import re

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient


class HuobiFeedBase:

    def __init__(self, config: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
        self.tickers = config["pytrade2.tickers"].lower().split(",")
        self._client = ws_client
        self.rest_client = rest_client
        self.config = config

    def run(self):
        """
        Read data from web socket
        """
        self._client.open()

    @staticmethod
    def ticker_of_ch(ch):
        return re.match("market\\.([\\w\\-]*)\\..*", ch).group(1)

    @staticmethod
    def period_of_ch(ch):
        return re.match("market\\.[\\w\\-]*\\.kline\\.(.*)", ch).group(1)

    def sub_events(self):
        raise NotImplementedError()
