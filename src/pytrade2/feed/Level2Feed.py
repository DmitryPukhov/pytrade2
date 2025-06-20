import logging
import multiprocessing
from datetime import datetime
from typing import Dict, List

import pandas as pd

from pytrade2.exch.Exchange import Exchange


class Level2Feed:
    kind = "level2"

    def __init__(self, cfg: Dict[str, str], exchange_provider: Exchange, data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.websocket_feed = exchange_provider.websocket_feed(cfg["pytrade2.exchange"])
        self.websocket_feed.consumers.add(self)
        self.level2: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])
        self.level2_buf: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])  # Buffer
        self.history_min_window = (pd.Timedelta(cfg.get("pytrade2.strategy.history.min.window"))
                                   + pd.Timedelta(cfg.get("pytrade2.strategy.predict.window", "0s")))
        self.history_max_window = (pd.Timedelta(cfg.get("pytrade2.strategy.history.max.window"))
                                   + pd.Timedelta(cfg.get("pytrade2.strategy.predict.window", "0s")))
        self.data_lock = data_lock
        self.new_data_event = new_data_event

    def run(self):
        self.websocket_feed.run()

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        # self._logger.debug("Got new level2 items: %s", level2)
        bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

        # Add new data to df
        new_df = pd.DataFrame(level2, columns=bid_ask_columns).set_index("datetime", drop=False)
        with self.data_lock:
            self.level2_buf = pd.concat([df for df in [self.level2_buf, new_df] if not df.empty])

        self.new_data_event.set()

    def apply_buf(self):
        """ Add level2 buf to level2 and purge old level2 """
        if self.level2_buf.empty:
            return self.level2

        with self.data_lock:
            self.level2 = pd.concat([df for df in [self.level2, self.level2_buf] if not df.empty])
            self.level2_buf = pd.DataFrame()
            # Purge old level2
            min_time = self.level2["datetime"].max() - self.history_max_window
            self.level2 = self.level2[self.level2["datetime"] > min_time]
        return self.level2

    def is_alive(self, maxdelta: pd.Timedelta):
        last_data_time = self.level2.index.max()
        lag = datetime.utcnow() - last_data_time
        is_alive = self.level2.empty or lag <= maxdelta
        if not is_alive:
            self._logger.warning(
                f"{self.__class__.__name__} : is dead. Last {self.kind} data: {last_data_time}, lag: {lag}, max lag allowed: {maxdelta}")
        return is_alive

    def has_min_history(self):
        interval = self.level2.index.max() - self.level2.index.min() if not self.level2.empty else pd.Timedelta(0)
        return interval >= self.history_min_window
