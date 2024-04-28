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
from exch.huobi.hbdm.broker.OrderCreator import OrderCreator
from datamodel.Trade import Trade
from datamodel.TradeStatus import TradeStatus
from metrics.MetricServer import MetricServer


class OrderFollower:
    """ Following order events from exchange"""

    def __init__(self):
        # All variables will be redefined in child classes
        self._logger = logging.getLogger(self.__class__.__name__)
        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.account_manager: Optional[AccountManagerHbdm] = None
        self.db_session: Optional[Session] = None
        self.rest_client: Optional[HuobiRestClient] = None
        self.trade_lock: Optional[RLock] = None
        self.allow_trade = False
        self.price_precision = 2

    @staticmethod
    def update_trade_closed(raw, trade):
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

        # raw param is the last order in response["data"]
        trade.close_price = raw["trade_avg_price"]
        trade.close_order_id = str(raw["order_id"])
        trade.close_time = datetime.utcfromtimestamp(raw["update_time"] / 1000)
        trade.status = TradeStatus.closed
        MetricServer.metrics.broker.trade.trade_close_price.set(trade.close_price)

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
            self._logger.debug(f"Updating current trade status:: {self.cur_trade}")
            params = self.huobi_history_close_order_query_params(self.cur_trade)
            self._logger.debug(
                f"Order history query start_time: {datetime.utcfromtimestamp(params['start_time'] / 1000)}, tz:{time.tzname}, params: {params}")
            res = self.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders", params)

            # Handle situation when server time zone is not UTC and it can return several previous orders
            if len(res["data"]) >= 1:
                # Get last order
                raw = sorted(res["data"], key=lambda o: o["update_time"])[-1]
                raw_update_time = datetime.utcfromtimestamp(raw["update_time"] / 1000)
                if raw_update_time > self.cur_trade.open_time:
                    # Got closing order - after cur trade
                    self.update_trade_closed(raw, self.cur_trade)
                    self._logger.info(f"Current trade found closed, probably by sl or tp: {self.cur_trade}")
                    self.finalize_closed_trade()
                else:
                    self._logger.debug(
                        f"Current trade is still opened. open time: {self.cur_trade.open_time},"
                        f" last order in history: {raw_update_time}")
            else:
                self._logger.debug(
                    f"Current trade is still opened. open time: {self.cur_trade.open_time}, last orders are empty.")

    @staticmethod
    def update_trade_opened_event(raw, trade):
        trade.open_price = float(raw["trade_avg_price"])
        trade.status = TradeStatus.opened

    @staticmethod
    def update_trade_closed_event(raw, trade):
        """ When close message came from socket"""
        trade.close_order_id = str(raw["order_id"])
        trade.close_price = float(raw["trade_avg_price"])
        trade.close_time = datetime.utcfromtimestamp(raw["created_at"] / 1000)
        trade.status = TradeStatus.closed
        return trade

    def finalize_closed_trade(self):
        """ When current trade was closed, do final routine and clear current trade"""
        # Save and clear current trade
        self.db_session.commit()
        self.cur_trade, self.prev_trade = None, self.cur_trade
        MetricServer.metrics.broker.trade.is_in_trade.set(0)
        # Ask account manager to read changed balance from the server
        self.account_manager.refresh_balance()

    @staticmethod
    def huobi_history_close_order_query_params(trade: Trade):
        # Closing trade type - opposite for main order
        close_trade_type = [OrderCreator.HuobiTradeType.buy, None, OrderCreator.HuobiTradeType.sell][
            trade.direction() + 1]
        # Temporary hack, search from an hour before start time to get orders, not executed immediately
        start_ts = trade.open_time_epoch_millis() - 1000 * 60 * 60
        # return {"contract": "BTC-USDT", "trade_type": close_trade_type,
        #         "type": HuobiBrokerHbdm.HuobiOrderType.finished, "status": HuobiBrokerHbdm.HuobiOrderStatus.filled}

        return {"contract": "BTC-USDT", "trade_type": close_trade_type,
                "type": OrderCreator.HuobiOrderType.finished, "status": OrderCreator.HuobiOrderStatus.filled,
                "start_time": start_ts}
