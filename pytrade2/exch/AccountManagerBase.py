import logging
import threading
from pathlib import Path

import pandas as pd


class AccountManagerBase:
    def __init__(self, config: {}):
        
        self.account_lock = threading.RLock()

        # Buffer for periodical writing to history
        self._buffer = []
        self._write_interval_sec = 10
        # Ensure data dire
        self.data_dir = Path(config["pytrade2.data.dir"], config["pytrade2.strategy"], "account")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        threading.Timer(self._write_interval_sec, self.write).start()

    def write(self):
        """ Write buffer to history """
        if self._buffer:
            logging.debug(f"Writing account updates to dir {self.data_dir}")
            with self.account_lock:
                # Convert buffer to dataframe
                df = pd.DataFrame(data=self._buffer).set_index("time")
                self._buffer = []

            # Calc file name for current date
            time = df.index[-1]
            file_name = f"{pd.to_datetime(time).date()}_balance.csv"
            file_path = Path(self.data_dir, file_name)

            # Append to csv
            logging.debug(f"Writing account updates to file {file_path}")
            df.to_csv(file_path, header=not file_path.exists(), mode='a')

        else:
            logging.debug("No new account updates to write ")

        # Schedule next write
        threading.Timer(self._write_interval_sec, self.write).start()
