import logging
from datetime import datetime
from typing import Optional

from exch.AccountManagerBase import AccountManagerBase
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from metrics.MetricNames import MetricNames
from metrics.Metrics import Metrics


class AccountManagerHbdm(AccountManagerBase):
    """ Huobi derivatives market account manager"""

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        
        super().__init__(conf)
        self._rest_client = rest_client
        self._ws_client = ws_client
        self.cur_balance: Optional[float] = None

    def sub_events(self):
        """ Subscribe to web socket balance events """
        topic = f"accounts_cross.usdt"
        params = {"op": "sub", "topic": f"accounts_cross.usdt"}
        # For accounts_cross.usdt topic will be just accounts_cross
        self._ws_client.add_consumer("accounts_cross", params, self)
        self._logger.info(f"Account manager subscribed to all events needed")

    def on_socket_data(self, topic, msg: {}):
        """ Got subscribed data from socket"""
        try:
            with self.account_lock:

                if self.cur_balance != msg["data"][-1]["margin_static"]:
                    self._logger.debug(f"Got websocket message, topic: {topic}, msg: {msg}")
                    # If balance changed, append to the buffer
                    balance_data = list(self.event_to_list(msg))
                    self._buffer.extend(balance_data)
                    self.cur_balance = balance_data[-1]["balance"]
                    Metrics.gauge(self, MetricNames.Broker.Account.balance).set(self.cur_balance)
        except Exception as e:
            self._logger.error(e)

    def event_to_list(self, msg: {}) -> [{}]:
        """ Convert huobi model to dictionary for pd dataframe"""
        time = datetime.utcnow()
        for item in msg["data"]:
            yield {"time": time, "asset": item["margin_asset"], "balance": item["margin_static"]}

    def refresh_balance(self):
        """ Read new balance from huobi, write to output directory """
        try:
            res = self._rest_client.post("/linear-swap-api/v1/swap_balance_valuation", {"valuation_asset": "USDT"})
            self._logger.debug(f"Got new balance: {res}")
            self._buffer.extend(self.response_to_list(res))
            # Write and clean the buffer
            self.write()
        except Exception as e:
            self._logger.error(e)

    @staticmethod
    def response_to_list(response: dict) -> [{}]:
        """ Huobi balance response to model dictionary """
        dt = datetime.utcnow()
        for item in response.get("data", {}):
            yield {"time": dt, "asset": item["valuation_asset"], "balance": float(item["balance"])}
