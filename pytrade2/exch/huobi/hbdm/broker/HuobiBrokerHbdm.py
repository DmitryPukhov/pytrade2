import logging
import time
from typing import Optional

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from model.Trade import Trade


class HuobiBrokerHbdm(Broker):
    huobi_order_status_filled = 6

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        super().__init__(conf)
        self._log = logging.getLogger(self.__class__.__name__)
        self.ws_client, self.rest_client = ws_client, rest_client
        self.ws_client.consumers.append(self)

    def run(self):
        """ Open socket and subscribe to events """

        # Open and wait until opened
        if not self.ws_client.is_opened:
            self.ws_client.open()
        while not self.ws_client.is_opened:
            time.sleep(1)
        # Subscribe
        self.sub_events()

    def sub_events(self):
        """ Subscribe account and order events """

        for ticker in self.config["pytrade2.tickers"].split(","):
            # Subscribe to order events
            # params = [{"op": "sub", "topic": "orders.*"}, {"op": "sub", "topic": "accounts.*"}]
            params = [{"op": "sub", "topic": f"orders.{ticker}"}, {"op": "sub", "topic": f"accounts.*"}]
            for param in params:
                self._log.info(f"Subscribing to {param}")
                self.ws_client.sub(param)

    def on_socket_data(self, msg):
        """ Got subscribed data from socket"""
        try:
            topic = msg.get("topic")
            status = msg.get("status")

            if not topic or not status == self.huobi_order_status_filled or not topic.startswith("orders."):
                return
            self._log.info(f"Got order event: {msg}")
        except Exception as e:
            self._log.error(e)

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        path = "/linear-swap-api/v1/swap_cross_order"
        data = {"contract_code": symbol,
                "contract_type": "swap",
                "volume": quantity,
                "direction": Trade.order_side_names[direction],
                "price": price,
                "order_price_type": "fok",
                "tp_trigger_price": take_profit_price,
                "tp_order_price_type": "market",
                "sl_trigger_price": stop_loss_price,
                "sl_order_price_type": "market"
                }
        res = self.rest_client.post(path=path, data=data)
        if res["status"] == "ok":
            order_id = res["order_id"]
            self._log.info(f"Created {symbol} order, direction: {direction}, order_id: {order_id}")
        else:
            self._log.error(f"Error creating order: {res}")
