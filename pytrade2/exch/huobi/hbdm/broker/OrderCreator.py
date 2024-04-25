import datetime
import logging
import time
from datetime import datetime
from logging import Logger
from multiprocessing import RLock
from typing import Optional

from sqlalchemy.orm.session import Session

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from datamodel.Trade import Trade
from datamodel.TradeStatus import TradeStatus


class OrderCreator:
    """ Creation of main order with sl/tp """

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

    price_precision = 0
    limit_ratio = 0.01

    def __init__(self, conf: dict):
        # All variables will be redefined in child classes
        self._logger = logging.getLogger(self.__class__.__name__)

        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.account_manager: Optional[AccountManagerHbdm] = None
        self.db_session: Optional[Session] = None
        self.rest_client: Optional[HuobiRestClient] = None
        self.trade_lock: Optional[RLock] = None
        self.allow_trade = False

    @staticmethod
    def sl_trade_params(symbol: str,
                        side: str,
                        quantity: float,
                        sl_trigger_price,
                        sl_order_price):

        params = {"contract_code": symbol,
                  # "client_order_id": client_order_id,
                  # "contract_type": "swap",
                  "volume": int(quantity),
                  "direction": side,
                  "lever_rate": 1,
                  # "order_price_type": "optimal_5_fok",
                  # "reduce_only": 0,  # 0 for opening order
                  "sl_trigger_price": round(sl_trigger_price, OrderCreator.price_precision),
                  "sl_order_price": round(sl_order_price, OrderCreator.price_precision),
                  "sl_order_price_type": "limit"
                  }
        return params

    @staticmethod
    def sl_order_price(main_direction: int, sl_trigger_price: float):
        return float(
            round(sl_trigger_price * (1 - main_direction * OrderCreator.limit_ratio), OrderCreator.price_precision))

    @staticmethod
    def cur_trade_params(symbol: str,
                         client_order_id: int,
                         side: str,
                         price: float,
                         quantity: float,
                         sl_trigger_price,
                         sl_order_price,
                         tp_trigger_price=None):
        # Works, but order types are limit => risk that price would fly out
        # data = {"contract_code": symbol,
        #         "client_order_id": client_order_id,
        #         # "contract_type": "swap",
        #         "volume": quantity,
        #         "direction": side,
        #         "price": price,
        #         "lever_rate": 1,
        #         "order_price_type": "limit",
        #         "tp_trigger_price": tp_trigger_price,
        #         "tp_order_price": tp_order_price,
        #         "reduce_only": 0,  # 0 for opening order
        #         "sl_trigger_price": sl_trigger_price,
        #         "sl_order_price": sl_order_price,
        #         "tp_order_price_type": "limit",
        #         "sl_order_price_type": "limit"
        #         }
        # Works, but tp is limit => bad exec price, no profit
        # data = {"contract_code": symbol,
        #         "client_order_id": client_order_id,
        #         # "contract_type": "swap",
        #         "volume": quantity,
        #         "direction": side,
        #         "price": price,
        #         "lever_rate": 1,
        #         "order_price_type": "optimal_5_fok",
        #         "tp_trigger_price": tp_trigger_price,
        #         "tp_order_price": tp_order_price,
        #         "reduce_only": 0,  # 0 for opening order
        #         "sl_trigger_price": sl_trigger_price,
        #         "sl_order_price": sl_order_price,
        #         "tp_order_price_type": "limit",
        #         "sl_order_price_type": "limit"
        #         }
        # Works, with fixed price tp
        params = {"contract_code": symbol,
                  "client_order_id": client_order_id,
                  # "contract_type": "swap",
                  "volume": quantity,
                  "direction": side,
                  "price": price,
                  "lever_rate": 1,
                  "order_price_type": "optimal_5_fok",
                  "reduce_only": 0,  # 0 for opening order
                  "sl_trigger_price": sl_trigger_price,
                  "sl_order_price": sl_order_price,
                  "sl_order_price_type": "limit"
                  }
        if price:
            params.update({"order_price_type": "fok", "price": price})
        if tp_trigger_price:
            params.update({
                "tp_trigger_price": tp_trigger_price,
                "tp_order_price_type": "optimal_5",
            })
        return params

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float],
                         trailing_delta: Optional[float]) -> Optional[Trade]:
        if not self.allow_trade:
            return None

        with self.trade_lock:
            if self.cur_trade:
                self._logger.info(f"Can not create current trade because another exists:{self.cur_trade}")
                return None
            side = Trade.order_side_names[direction]
            # Adjust prices to precision

            price, sl_trigger_price, sl_order_price, tp_trigger_price, tp_order_price = self.adjust_prices(
                direction, price, stop_loss_price, take_profit_price, self.price_precision, self.limit_ratio)
            self._logger.info(
                f"Creating current {symbol} {side} trade. price: {price}, "
                f"sl trigger: {sl_trigger_price}, sl order: {sl_order_price}, "
                f"tp trigger: {tp_trigger_price}, tp order: {tp_order_price}, trailing delta: {trailing_delta},"
                f"price precision: {self.price_precision}, limit ratio: {self.limit_ratio}")
            # Prepare create order command
            client_order_id = round(time.time() * 1000)
            # client_order_id = int(datetime.utcnow().timestamp())
            params = self.cur_trade_params(symbol=symbol,
                                           client_order_id=client_order_id,
                                           side=side,
                                           price=price,
                                           quantity=quantity,
                                           sl_trigger_price=sl_trigger_price,
                                           sl_order_price=sl_order_price,
                                           # If trailing delta is set, we'll take profit manually, not here
                                           tp_trigger_price=tp_trigger_price if not trailing_delta else None
                                           )
            self._logger.debug(f"Create order params: {params}")
            # Request to create a new order
            path = "/linear-swap-api/v1/swap_cross_order"
            res = self.rest_client.post(path=path, data=params)

            # Process result
            self._logger.info(f"Create order response: {res}")
            if res["status"] == "ok":
                # Get order details, fill in current trade
                info = self.get_order_info(client_order_id, ticker=symbol)
                trade = self.res2trade(info)

                if trade.status != TradeStatus.opened:
                    self._logger.info(f"Order not filled, that's ok.")
                    return None

                self.cur_trade = trade
                # Fill in sltp info with executed prices
                sltp_info = self.get_sltp_orders_info(self.cur_trade.open_order_id)
                self.update_trade_sltp(sltp_info, self.cur_trade)

                # Take profit order not set => take profit is manual => set it's price here
                if not self.cur_trade.take_profit_price:
                    self.cur_trade.take_profit_price = tp_trigger_price
                    self.cur_trade.trailing_delta = trailing_delta

                # Save current trade to db
                self.db_session.add(self.cur_trade)
                self.db_session.commit()
                self._logger.info(f"Opened trade: {self.cur_trade}")
            else:
                self._logger.error(f"Error creating order: {res}")

            return self.cur_trade

    @staticmethod
    def adjust_prices(direction, price: Optional[float], stop_loss_price: float, take_profit_price: float, price_precision: int,
                      limit_ratio: float) -> \
            (float, float, float, float, float):
        """ Calc trigger and order prices, adjust precision """
        price = float(round(price, price_precision)) if price else None
        tp_trigger_price = float(round(take_profit_price, price_precision)) if take_profit_price else None
        tp_order_price = float(round(take_profit_price * (1 - direction * limit_ratio), price_precision)) if take_profit_price else None
        sl_trigger_price = float(round(stop_loss_price, price_precision))
        sl_order_price = float(round(stop_loss_price * (1 - direction * limit_ratio), price_precision))
        return price, sl_trigger_price, sl_order_price, tp_trigger_price, tp_order_price

    def get_sltp_orders_info(self, main_order_id):
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
                                    {"contract_code": "BTC-USDT", "order_id": main_order_id})
        self._logger.debug(f"Got sltp order info: f{res}")
        return res

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
            if not 1 <= len(sltp_res["data"]["tpsl_order_info"]) <= 2:
                raise RuntimeError(f"Error: sl/tp order count is not 1 or 2: {sltp_res}")
            sltp_orders = sorted(sltp_res["data"]["tpsl_order_info"], key=lambda item: item["trigger_price"],
                                 reverse=trade.direction() == -1)
            # Stop loss order is always exists
            sl_order = sltp_orders[0]
            trade.stop_loss_order_id = sl_order["order_id_str"]
            trade.stop_loss_price = sl_order["trigger_price"] \
                if sl_order["trigger_price"] else sl_order["order_price"]
            if len(sltp_orders) > 1:
                # If tp order
                tp_order = sltp_orders[1]
                # sl order id format: stoploss id, take profit id
                trade.stop_loss_order_id += f",{tp_order['order_id_str']}"
                trade.take_profit_price = tp_order["trigger_price"] \
                    if tp_order["trigger_price"] else tp_order["order_price"]

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
        trade.status = TradeStatus.opened if data["status"] == OrderCreator.HuobiOrderStatus.filled \
            else str(data["status"])
        return trade

    def get_order_info(self, client_order_id: int, ticker: str):
        """ Request order from exchange"""

        path = "/linear-swap-api/v1/swap_cross_order_info"
        data = {"client_order_id": client_order_id, "contract_code": ticker}
        res = self.rest_client.post(path, data)
        self._logger.debug(f"Got order {res}")
        return res
