import datetime
from datetime import datetime
import logging
import time
from typing import Optional

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from model.Trade import Trade
from model.TradeStatus import TradeStatus


class HuobiBrokerHbdm(Broker):
    huobi_order_status_filled = 6

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        super().__init__(conf)
        self._log = logging.getLogger(self.__class__.__name__)
        self.ws_client, self.rest_client = ws_client, rest_client
        self.ws_client.consumers.append(self)
        self.set_one_way_mode()

    def set_one_way_mode(self):
        self._log.info(f"Setting one way trading mode (opposite trade will close current one)")
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_switch_position_mode",
                                    {"margin_account": "USDT", "position_mode": "single_side"})
        self._log.debug(f"responce: f{res}")

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
        if self.cur_trade:
            self._log.info(f"Can not create current trade because another exists:{self.cur_trade}")

        # Prepare create order command
        path = "/linear-swap-api/v1/swap_cross_order"
        client_order_id = int(datetime.utcnow().timestamp())
        limit_ratio = 0.01  # 15030 for bt
        data = {"contract_code": symbol,
                "client_order_id": client_order_id,
                # "contract_type": "swap",
                "volume": quantity,
                "direction": Trade.order_side_names[direction],
                "price": price,
                "lever_rate": 1,
                "order_price_type": "limit",
                "tp_trigger_price": take_profit_price,
                "tp_order_price": round(take_profit_price * (1 + direction * limit_ratio), self.price_precision),
                "reduce_only": 0,
                "sl_trigger_price": stop_loss_price,
                "sl_order_price": round(stop_loss_price * (1 - direction * limit_ratio), self.price_precision),
                "tp_order_price_type": "limit",
                "sl_order_price_type": "limit"
                }
        # Request to create a new order
        res = self.rest_client.post(path=path, data=data)

        # Process result
        print(f"Create order response: {res}")
        if res["status"] == "ok":
            self._log.debug(f"Create order response: {res}")

            # Get order details, fill current trade
            info = self.get_order_info(client_order_id, ticker=symbol)
            self.cur_trade = self.res2trade(info)
            self._log.info(f"Created order: {self.cur_trade}")
        else:
            self._log.error(f"Error creating order: {res}")

        return self.cur_trade

    @staticmethod
    def res2trade(res: dict):
        """ Convert get order response to trade model"""
        data = res["data"][0]
        dt = datetime.utcfromtimestamp(data["created_at"] / 1000)

        trade = Trade()
        trade.ticker = data["contract_code"]
        trade.side = data["direction"].upper()
        trade.quantity = data["volume"]
        trade.open_order_id = str(data["order_id"])
        trade.open_time = dt
        trade.open_price = data["trade_avg_price"]
        # ??? Process status better
        trade.status = TradeStatus.opened if data["status"] == "filled" else TradeStatus.opened
        return trade

    def get_order_info(self, client_order_id: int, ticker: str):
        """ Request order from exchange"""

        path = "/linear-swap-api/v1/swap_cross_order_info"
        data = {"client_order_id": client_order_id, "contract_code": ticker}
        res = self.rest_client.post(path, data)
        self._log.info(f"Got order {res}")
        return res
