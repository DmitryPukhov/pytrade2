import logging
import multiprocessing
from datetime import datetime
from typing import Dict, List
import pandas as pd

from exch.Exchange import Exchange


class Level2Feed:
    def __init__(self, cfg: Dict[str, str], exchange_provider: Exchange, data_lock: multiprocessing.RLock, new_data_event: multiprocessing.Event):
        self.websocket_feed = exchange_provider.websocket_feed(cfg["pytrade2.exchange"])
        self.websocket_feed.consumers.add(self)
        self.level2: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])
        self.level2_buf: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])  # Buffer
        self.level2_history_period = (pd.Timedelta(cfg.get("pytrade2.strategy.history.max.window"))
                                      + pd.Timedelta(cfg.get("pytrade2.strategy.predict.window", "0s")))
        self.data_lock = data_lock
        self.new_data_event = new_data_event

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

        # Add new data to df
        new_df = pd.DataFrame(level2, columns=bid_ask_columns).set_index("datetime", drop=False)
        with self.data_lock:
            self.level2_buf = pd.concat([df for df in [self.level2_buf, new_df] if not df.empty])

        self.new_data_event.set()

    def apply_buf(self):
        """ Add level2 buf to level2 and purge old level2 """
        if self.level2_buf.empty:
            return

        with self.data_lock:
            self.level2 = pd.concat([df for df in [self.level2, self.level2_buf] if not df.empty])
            self.level2_buf = pd.DataFrame()
            # Purge old level2
            min_time = self.level2["datetime"].max() - self.level2_history_period
            self.level2 = self.level2[self.level2["datetime"] > min_time]
        return self.level2

    def get_report(self):
        """ Short info for report """
        time_format = '%Y-%m-%d %H:%M:%S'

        return (f"Level2 cnt:{len(self.level2)}, "
                f"first: {self.level2.index.min().strftime(time_format)}, "
                f"last: {self.level2.index.max().strftime(time_format)}") \
            if not self.level2.empty \
            else "Level2 is empty"

    def is_alive(self, maxdelta: pd.Timedelta):
        return self.level2.empty or (datetime.utcnow() - self.level2.index.max() <= maxdelta)
