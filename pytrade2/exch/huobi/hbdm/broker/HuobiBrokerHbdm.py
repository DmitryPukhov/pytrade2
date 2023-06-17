import logging

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient


class HuobiBrokerHbdm(Broker):
    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        super().__init__(conf)
        self._log = logging.getLogger(self.__class__.__name__)
        self.ws_client, self.rest_client = ws_client, rest_client
        self.ws_client.consumers.append(self)

    def on_socket_open(self):
        """ When socket opened, subscribe to events """
        self.sub_events()

    def sub_events(self):
        for ticker in self.config["pytrade2.tickers"].split(","):
            # Subscribe to order events
            params = {"sub": f"orders.*"}
            self._log.info(f"Subscribing to {params}")
            self.ws_client.sub(params)

    def on_socket_data(self, msg):
        """ Got subscribed data from socket"""
        try:
            topic = msg.get("topic")
            trade = msg.get("trade")
            if not topic or not trade:
                return
        except Exception as e:
            self._log.error(e)