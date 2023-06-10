import logging
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
from huobi.client.account import AccountClient
from huobi.constant import AccountBalanceMode
from huobi.model.account import AccountUpdateEvent, AccountUpdate

from exch.huobi.HuobiTools import HuobiTools


class AccountManager:
    """ Account monitoring, subscribe to balance changes, save balance history"""

    def __init__(self, config: {}, account_client: AccountClient):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config

        # Ensure data dire
        self.data_dir = Path(config["pytrade2.data.dir"], config["pytrade2.strategy"], "account")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self._account_client = account_client
        self.account_lock = threading.RLock()

        # Buffer for periodical writing to history
        self._buffer = []
        self._write_interval_sec = 10
        threading.Timer(self._write_interval_sec, self.write).start()
        self._log.info(
            f"Initialized. Account history dir: {self.data_dir}, write interval sec: {self._write_interval_sec}")

    def sub_events(self):
        self._log.debug("Subscribing to account events")
        self._account_client.sub_account_update(mode=AccountBalanceMode.TOTAL, callback=self.on_account_update,
                                                error_handler=self.account_error_handler)

    def on_account_update(self, event: AccountUpdateEvent):
        self._log.debug(f"Got account update event")
        with self.account_lock:
            try:
                record = self.event_to_dict(event.data)
                self._log.debug(f"Converted account update event: {record}")
                self._buffer.append(record)
            except Exception as e:
                self._log.error(f"on_account_update error: {e}")

    def event_to_dict(self, au: AccountUpdate):
        """ Convert huobi model to dictionary for pd dataframe"""
        time = datetime.utcnow()
        return {"time": time, "account_id": au.accountId, "asset": au.currency, "account_type": au.accountType,
                "balance": au.balance, "available": au.available}

    def account_error_handler(self, ex):
        self._log.error(HuobiTools.format_exception("Account client", ex))

    def write(self):
        """ Write buffer to history """
        if self._buffer:
            self._log.debug(f"Writing account updates to dir {self.data_dir}")
            with self.account_lock:
                # Convert buffer to dataframe
                df = pd.DataFrame(data=self._buffer).set_index("time")
                self._buffer = []

            # Calc file name for current date
            time = df.index[-1]
            file_name = f"{pd.to_datetime(time).date()}_balance.csv"
            file_path = Path(self.data_dir, file_name)

            # Append to csv
            self._log.debug(f"Writing account updates to file {file_path}")
            df.to_csv(file_path, header=not file_path.exists(), mode='a')

        else:
            self._log.debug("No new account updates to write ")

        # Schedule next write
        threading.Timer(self._write_interval_sec, self.write).start()
