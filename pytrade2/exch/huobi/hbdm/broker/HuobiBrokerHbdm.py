import logging
from io import StringIO

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.OrderFollower import OrderFollower
from exch.huobi.hbdm.broker.OrderCreator import OrderCreator
from exch.huobi.hbdm.broker.TrailingStopSupport import TrailingStopSupport
from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm
from model.Trade import Trade


class HuobiBrokerHbdm(OrderCreator, TrailingStopSupport, OrderFollower, Broker):
    """ Huobi derivatives market broker"""

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient, ws_feed: HuobiWebSocketFeedHbdm):
        self._logger = logging.getLogger(self.__class__.__name__)

        OrderCreator.__init__(self, conf)
        OrderFollower.__init__(self)
        TrailingStopSupport.__init__(self, conf=conf, ws_feed=ws_feed, rest_client=rest_client)
        Broker.__init__(self, conf)

        self.tickers = conf["pytrade2.tickers"].lower().split(",")
        self.fee = float(conf["pytrade2.exchange.huobi.hbdm.fee"])
        
        self.ws_client, self.ws_feed, self.rest_client = ws_client, ws_feed, rest_client

        # This mode means that sell closes previous buy and vice versa
        self.set_one_way_mode()
        self.account_manager = AccountManagerHbdm(conf, rest_client, ws_client)

    def set_one_way_mode(self):
        """ Set up exchange to set one way (no buy and sell opened simultaneously) """
        self._logger.info(f"Setting one way trading mode (opposite trade will close current one)")
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_switch_position_mode",
                                    {"margin_account": "USDT", "position_mode": "single_side"})
        self._logger.debug(f"responce: f{res}")

    def run(self):
        """ Open socket and subscribe to events """
        # Subscribe (add self to subscriber list, events will come later when socket is opened)
        self.sub_events()

        # Open and wait until opened
        if not self.ws_client.is_opened:
            self.ws_client.open()

        self.ws_feed.run()

        self.account_manager.refresh_balance()

        self.update_cur_trade_status()

    def sub_events(self):
        """ Subscribe account and order events """

        # Subscribe to account events
        self.account_manager.sub_events()

        # Subscibe to order events
        for ticker in self.tickers:
            # Subscribe to order events
            topic = f"orders_cross.{ticker}"
            params = {"op": "sub", "topic": topic}
            self.ws_client.add_consumer(topic, params, self)

        self._logger.info("Broker subscribed to all events needed.")

    def on_socket_data(self, topic, msg):
        """ Got subscribed data from socket"""
        try:
            status = msg.get("status")
            self._logger.info(f"Got order event: {msg}")

            if not self.cur_trade \
                    or not status \
                    or not status == self.HuobiOrderStatus.filled \
                    or not topic \
                    or not topic.startswith("orders_cross."):
                return
            with self.trade_lock:
                if self.cur_trade:
                    order_direction = Trade.order_side_codes[msg["direction"].upper()]
                    if order_direction == self.cur_trade.direction():
                        # Current trade is opened
                        self.update_trade_opened_event(msg, self.cur_trade)
                        self.db_session.commit()
                        self._logger.info(f"Current trade is opened")

                    elif order_direction == - self.cur_trade.direction():
                        # Current trade is closed
                        t = self.update_trade_closed_event(msg, self.cur_trade)
                        self.finalize_closed_trade()
                        self._logger.info(f"Current trade is closed: {t}")

        except Exception as e:
            self._logger.error(f"Socket message processing error: {e}")

    def get_report(self):
        """ Short info for report """

        # Form message string
        msg = StringIO()
        msg.write(super().get_report())
        try:
            # Report balance
            res = self.rest_client.post("/linear-swap-api/v1/swap_balance_valuation", {"valuation_asset": "USDT"})
            msg.writelines([f'Balance {b["valuation_asset"]}: {b["balance"]}\n' for b in res["data"]])

            # Report positions
            res = self.rest_client.post("/linear-swap-api/v1/swap_cross_account_info", {"margin_account": "USDT"})
            data = res["data"]
            for dataitem in data:
                for c in [c for c in dataitem["futures_contract_detail"] if c["margin_position"] != 0]:
                    currency = c['trade_partition']
                    msg.write(f"Position {c['contract_code']}: {c['margin_position']} {currency}, "
                              f"frozen:{c['margin_frozen']} {currency}, "
                              f"available: {c['margin_available']} {currency}\n")
        except Exception as e:
            self._logger.error(f"Error reporting broker info: {e}")

        return msg.getvalue()
