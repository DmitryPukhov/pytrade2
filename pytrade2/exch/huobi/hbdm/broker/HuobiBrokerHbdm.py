import datetime
import logging
import time
from datetime import datetime
from typing import Optional

from exch.Broker import Broker
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from model.Trade import Trade
from model.TradeStatus import TradeStatus


class HuobiBrokerHbdm(Broker):
    """ Huobi derivatives market broker"""

    class HuobiOrderStatus:
        """
        Huobi rests response constants
        https://www.huobi.com/en-us/opend/newApiPages/?id=8cb85ba1-77b5-11ed-9966-0242ac110003
        """
        filled = 6

    class HuobiTradeType:
        """
        Huobi rests response constants
        https://www.huobi.com/en-us/opend/newApiPages/?id=8cb85ba1-77b5-11ed-9966-0242ac110003
        """
        buy = 17
        sell = 18

    class HuobiOrderType:
        """
        Huobi rests response constants
        https://www.huobi.com/en-us/opend/newApiPages/?id=8cb85ba1-77b5-11ed-9966-0242ac110003
        """
        all = 1
        finished = 2

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        super().__init__(conf)
        self._log = logging.getLogger(self.__class__.__name__)
        self.ws_client, self.rest_client = ws_client, rest_client
        self.ws_client.consumers.append(self)
        self.set_one_way_mode()

    def set_one_way_mode(self):
        """ Set up exchange to set one way (no buy and sell opened simultaneously) """
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

        self.update_cur_trade_status()
        # Subscribe
        self.sub_events()

    def sub_events(self):
        """ Subscribe account and order events """

        for ticker in self.config["pytrade2.tickers"].split(","):
            # Subscribe to order events
            # params = [{"op": "sub", "topic": "orders.*"}, {"op": "sub", "topic": "accounts.*"}]
            params = [{"op": "sub", "topic": f"orders_cross.{ticker}"}, {"op": "sub", "topic": f"accounts_cross.*"}]
            for param in params:
                self._log.info(f"Subscribing to {param}")
                self.ws_client.sub(param)
        self._log.info("Broker subscribed to all events needed.")

    def on_socket_data(self, msg):
        """ Got subscribed data from socket"""
        try:
            topic = msg.get("topic")
            status = msg.get("status")

            if not self.cur_trade \
                    or not status \
                    or not status == self.HuobiOrderStatus.filled \
                    or not topic \
                    or not topic.startswith("orders_cross."):
                return
            with self.trade_lock:
                if self.cur_trade:
                    self._log.info(f"Got order event: {msg}")
                    order_direction = Trade.order_side_codes(msg["direction"].upper())
                    if order_direction == - self.cur_trade.direction():
                        # Close current trade
                        self.update_trade_closed_event(msg, self.cur_trade)
                        self.db_session.commit()
                        self.cur_trade = None

        except Exception as e:
            self._log.error(f"Socket message processing error: {e}")

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        if not self.allow_trade:
            return None

        with self.trade_lock:
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
                    "tp_order_price": round(take_profit_price * (1 - direction * limit_ratio), self.price_precision),
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

                # Get order details, fill in current trade
                info = self.get_order_info(client_order_id, ticker=symbol)
                self.cur_trade = self.res2trade(info)
                # Fill in sltp info
                sltp_info = self.get_sltp_orders_info(self.cur_trade.open_order_id)
                self.update_trade_sltp(sltp_info, self.cur_trade)

                # Save current trade to db
                self.db_session.add(self.cur_trade)
                self.db_session.commit()
                self._log.info(f"Created order: {self.cur_trade}")
            else:
                self._log.error(f"Error creating order: {res}")

            return self.cur_trade

    @staticmethod
    def huobi_history_close_order_query_params(trade: Trade):
        # Closing trade type - opposite for main order
        close_trade_type = [HuobiBrokerHbdm.HuobiTradeType.buy, None, HuobiBrokerHbdm.HuobiTradeType.sell][
            trade.direction() + 1]
        return {"contract": "BTC-USDT", "trade_type": close_trade_type,
                "type": HuobiBrokerHbdm.HuobiOrderType.finished, "status": HuobiBrokerHbdm.HuobiOrderStatus.filled,
                "start_time": trade.open_time_epoch_millis()}

    def update_cur_trade_status(self):
        with self.trade_lock:
            if not self.cur_trade:
                return
            # Get close order example:
            # {'code': 200, 'msg': 'ok', 'data': [
            #     {'direction': 'sell', 'offset': 'both', 'volume': 1.0, 'price': 26583.0, 'profit': 0.05, 'pair': 'BTC-USDT',
            #      'query_id': 69592538249, 'order_id': 1120016247351635968, 'contract_code': 'BTC-USDT', 'symbol': 'BTC',
            #      'lever_rate': 1, 'create_date': 1687074282253, 'order_source': 'web', 'canceled_source': '',
            #      'order_price_type': 4, 'order_type': 1, 'margin_frozen': 0.0, 'trade_volume': 1.0,
            #      'trade_turnover': 26.592, 'fee': -0.0106368, 'trade_avg_price': 26592.0, 'status': 6,
            #      'order_id_str': '1120016247351635968', 'fee_asset': 'USDT', 'fee_amount': 0, 'fee_quote_amount': 0.0106368,
            #      'liquidation_type': '0', 'margin_asset': 'USDT', 'margin_mode': 'cross', 'margin_account': 'USDT',
            #      'update_time': 1687074282782, 'is_tpsl': 0, 'real_profit': 0.05, 'trade_partition': 'USDT',
            #      'reduce_only': 1, 'contract_type': 'swap', 'business_type': 'swap'}], 'ts': 1687077630615}

            # Call history
            params = self.huobi_history_close_order_query_params(self.cur_trade)
            res = self.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders",
                                        self.huobi_history_close_order_query_params(self.cur_trade))

            if len(res["data"] > 1):
                raise RuntimeError(
                    f"Error: got multiple closed orders from exchange. Cur trade: {self.cur_trade}, "
                    f"history query params: {params},  exchange response: {res} ")
            if len(res["data"] == 1):
                # Got closing order
                self.update_trade_closed(res, self.cur_trade)
                self._log.info(f"Current trade found closed, probably by sl or tp: {self.cur_trade}")
                self.db_session.commit()
                self.cur_trade = None

    @staticmethod
    def update_trade_closed(res, trade):
        # Response example:
        # {'code': 200, 'msg': 'ok', 'data': [
        #     {'direction': 'sell', 'offset': 'both', 'volume': 1.0, 'price': 26583.0, 'profit': 0.05, 'pair': 'BTC-USDT',
        #      'query_id': 69592538249, 'order_id': 1120016247351635968, 'contract_code': 'BTC-USDT', 'symbol': 'BTC',
        #      'lever_rate': 1, 'create_date': 1687074282253, 'order_source': 'web', 'canceled_source': '',
        #      'order_price_type': 4, 'order_type': 1, 'margin_frozen': 0.0, 'trade_volume': 1.0,
        #      'trade_turnover': 26.592, 'fee': -0.0106368, 'trade_avg_price': 26592.0, 'status': 6,
        #      'order_id_str': '1120016247351635968', 'fee_asset': 'USDT', 'fee_amount': 0, 'fee_quote_amount': 0.0106368,
        #      'liquidation_type': '0', 'margin_asset': 'USDT', 'margin_mode': 'cross', 'margin_account': 'USDT',
        #      'update_time': 1687074282782, 'is_tpsl': 0, 'real_profit': 0.05, 'trade_partition': 'USDT',
        #      'reduce_only': 1, 'contract_type': 'swap', 'business_type': 'swap'}], 'ts': 1687077630615}
        if len(res["data"]) == 1:
            raw = res["data"][-1]
            # raw param is the last order in response["data"]
            trade.close_price = raw["trade_avg_price"]
            trade.close_order_id = str(raw["order_id"])
            trade.close_time = datetime.utcfromtimestamp(raw["update_time"] / 1000)
            trade.status = TradeStatus.closed

    def get_sltp_orders_info(self, main_order_id):
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
                                    {"contract_code": "BTC-USDT", "order_id": main_order_id})
        self._log.debug(f"Got sltp order info: f{res}")
        return res

    @staticmethod
    def update_trade_closed_event(raw, trade):
        """ When close message came from socket"""
        trade.close_order_id = str(raw["order_id"])
        trade.close_price = float(raw["trade_avg_price"])
        trade.close_time = datetime.utcfromtimestamp(raw["created_at"] / 1000)
        trade.status = TradeStatus.closed
        return trade

    @staticmethod
    def update_trade_sltp(sltp_res, trade):
        """ Update trade from sltp"""
        # sl/tp response example
        # {'status': 'ok', 'data':
        # {'contract_type': 'swap', 'business_type': 'swap', 'pair': 'BTC-USDT', 'symbol': 'BTC',
        #  'contract_code': 'BTC-USDT', 'margin_mode': 'cross', 'margin_account': 'USDT', 'volume': 1, 'price': 26720,
        #  'order_price_type': 'limit', 'direction': 'buy', 'offset': 'both', 'lever_rate': 1,
        #  'order_id': 1119997217854570496, 'order_id_str': '1119997217854570496', 'client_order_id': 1687058944,
        #  'created_at': 1687069745272, 'trade_volume': 1, 'trade_turnover': 26.542, 'fee': -0.0106168,
        #  'trade_avg_price': 26542.0, 'margin_frozen': 0, 'profit': 0, 'status': 6, 'order_type': 1,
        #  'order_source': 'api', 'fee_asset': 'USDT', 'canceled_at': 0, 'tpsl_order_info': [
        #     {'volume': 1.0, 'direction': 'sell', 'tpsl_order_type': 'tp', 'order_id': 1119997217904902144,
        #      'order_id_str': '1119997217904902144', 'trigger_type': 'ge', 'trigger_price': 27000.0,
        #      'order_price': 27270.0, 'created_at': 1687069745290, 'order_price_type': 'limit',
        #      'relation_tpsl_order_id': '1119997217909096448', 'status': 2, 'canceled_at': 0, 'fail_code': None,
        #      'fail_reason': None, 'triggered_price': None, 'relation_order_id': '-1'},
        #     {'volume': 1.0, 'direction': 'sell', 'tpsl_order_type': 'sl', 'order_id': 1119997217909096448,
        #      'order_id_str': '1119997217909096448', 'trigger_type': 'le', 'trigger_price': 26000.0,
        #      'order_price': 25740.0, 'created_at': 1687069745291, 'order_price_type': 'limit',
        #      'relation_tpsl_order_id': '1119997217904902144', 'status': 2, 'canceled_at': 0, 'fail_code': None,
        #      'fail_reason': None, 'triggered_price': None, 'relation_order_id': '-1'}],
        #  'trade_partition': 'USDT'}, 'ts': 1687071763277}

        if "status" in sltp_res and sltp_res["status"] == "ok":
            if len(sltp_res["data"]["tpsl_order_info"]) != 2:
                raise RuntimeError(f"Error: sl/tp order count != 2: {sltp_res}")
            (sl_order, tp_order) = sorted(sltp_res["data"]["tpsl_order_info"], key=lambda item: item["order_price"],
                                          reverse=trade.direction() == -1)
            trade.stop_loss_order_id = ",".join([sl_order["order_id_str"], tp_order["order_id_str"]])
            trade.stop_loss_price = sl_order["order_price"]
            trade.take_profit_price = tp_order["order_price"]

        return trade

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
        self._log.debug(f"Got order {res}")
        return res
