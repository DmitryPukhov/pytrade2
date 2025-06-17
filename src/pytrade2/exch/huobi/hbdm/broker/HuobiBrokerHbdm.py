import logging

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.OrderFollower import OrderFollower
from exch.huobi.hbdm.broker.OrderCreator import OrderCreator
from exch.huobi.hbdm.broker.TrailingStopSupport import TrailingStopSupport
from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm
from datamodel.Trade import Trade
from metrics.MetricServer import MetricServer


class HuobiBrokerHbdm(OrderCreator, TrailingStopSupport, OrderFollower, Broker):
    """ Huobi derivatives market broker"""

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient,
                 ws_feed: HuobiWebSocketFeedHbdm):
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
        self._logger.info("Setting one way trading mode (opposite trade will close current one)")
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_switch_position_mode",
                                    {"margin_account": "USDT", "position_mode": "single_side"})
        self._logger.debug(f"response: f{res}")

    def run(self):
        """ Open socket and subscribe to events """
        # Subscribe (add self to subscriber list, events will come later when socket is opened)
        self.sub_events()

        # Open and wait until opened
        if not self.ws_client.is_running:
            self.ws_client.open()

        #self.ws_feed.run()

        self.account_manager.refresh_balance()

        # Initial set current trade if it is opened
        self.update_cur_trade_status()
        MetricServer.metrics.broker.trade.set_metrics(self.cur_trade)

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
                        self._logger.info("Current trade is opened")

                    elif order_direction == - self.cur_trade.direction():
                        # Current trade is closed
                        t = self.update_trade_closed_event(msg, self.cur_trade)
                        self.finalize_closed_trade()
                        self._logger.info(f"Current trade is closed: {t}")

        except Exception as e:
            self._logger.error(f"Socket message processing error: {e}")

    def on_ticker(self, ticker: dict):
        """On price changed, set metric and move trailing stop"""

        with self.trade_lock:
            if self.cur_trade and self.cur_trade.ticker == ticker["symbol"]:
                # Move trailing stop
                TrailingStopSupport.on_ticker(self, ticker)

                # Calculate current profit metric
                direction = self.cur_trade.direction()
                if direction == 1:
                    profit = ticker["bid"] - self.cur_trade.open_price
                    MetricServer.metrics.broker.trade.trade_profit.set(profit)
                elif direction == -1:
                    profit = self.cur_trade.open_price - ticker["ask"]
                    MetricServer.metrics.broker.trade.trade_profit.set(profit)
