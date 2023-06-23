import logging
from datetime import datetime

from exch.AccountManagerBase import AccountManagerBase
from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient


class AccountManagerHbdm(AccountManagerBase):
    """ Huobi derivatives market account manager"""

    def __init__(self, conf: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        self._log = logging.getLogger(self.__class__.__name__)
        super().__init__(conf)
        self._rest_client = rest_client
        self._ws_client = ws_client

    def sub_events(self):
        """ Subscribe to web socket balance events """
        topic = f"accounts_cross.usdt"
        params = {"op": "sub", "topic": f"accounts_cross.usdt"}
        self._ws_client.add_consumer(topic, params, self)
        self._log.info(f"Account manager subscribed to all events needed")

    def on_socket_data(self, topic, msg: {}):
        """ Got subscribed data from socket"""
        try:
            with self.account_lock:
                self._log.debug(f"Got websocket message, topic: {topic}, msg: {msg}")
                self._buffer.append(self.event_to_dict(msg))
        except Exception as e:
            self._log.error(e)

    def event_to_dict(self, msg: {}):
        """ Convert huobi model to dictionary for pd dataframe"""
        time = datetime.utcnow()
        data = msg["data"]
        return {"time": time, "account_id": msg["uid"], "asset": data["margin_asset"], "account_type": "cross",
                "change_type": None, "balance": data["margin_balance"], "available": "margin_static"}

    def refresh_balance(self):
        """ Read new balance from huobi, write to output directory """
        try:
            res = self._rest_client.post("/linear-swap-api/v1/swap_balance_valuation", {"valuation_asset": "USDT"})
            self._log.debug(f"Got new balance: {res}")
            self._buffer.extend(self.response_to_list(res))
            # Write and clean the buffer
            self.write()
        except Exception as e:
            self._log.error(e)

    @staticmethod
    def response_to_list(response: dict) -> [{}]:
        """ Huobi balance response to model dictionary """
        dt = datetime.utcnow()
        for item in response.get("data", {}):
            yield {"time": dt, "asset": item["valuation_asset"], "balance": float(item["balance"])}
