import logging
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
from huobi.client.account import AccountClient
from huobi.constant import AccountBalanceMode
from huobi.model.account import AccountUpdateEvent, AccountUpdate

from exch.AccountManagerBase import AccountManagerBase
from exch.huobi.HuobiTools import HuobiTools


class AccountManagerSpot(AccountManagerBase):
    """ Account monitoring, subscribe to balance changes, save balance history"""

    def __init__(self, config: {}, account_client: AccountClient):
        super().__init__()
        self.config = config

        self._account_client = account_client

        self._logger.info(
            f"Initialized. Account history dir: {self.data_dir}, write interval sec: {self._write_interval_sec}")

    def sub_events(self):
        self._logger.debug("Subscribing to account events")
        self._account_client.sub_account_update(mode=AccountBalanceMode.TOTAL, callback=self.on_account_update,
                                                error_handler=self.account_error_handler)

    def on_account_update(self, event: AccountUpdateEvent):
        self._logger.debug(f"Got account update event")
        with self.account_lock:
            try:
                record = self.event_to_dict(event.data)
                self._logger.debug(f"Converted account update event: {record}")
                self._buffer.append(record)
            except Exception as e:
                self._logger.error(f"on_account_update error: {e}")

    def event_to_dict(self, au: AccountUpdate):
        """ Convert huobi model to dictionary for pd dataframe"""
        time = datetime.utcnow()
        return {"time": time, "account_id": au.accountId, "asset": au.currency, "account_type": au.accountType,
                "change_type": au.changeType, "balance": au.balance, "available": au.available}

    def account_error_handler(self, ex):
        self._logger.error(HuobiTools.format_exception("Account client", ex))

