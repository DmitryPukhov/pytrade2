import logging
from datetime import timedelta, datetime
from typing import Dict

import pandas as pd


class DataPurger:
    """ Purge old data"""

    def __init__(self, config: Dict):
        self._log = logging.getLogger(self.__class__.__name__)
        self.purge_window = config.get("pytrade2.strategy.purge.window", None)
        self.purge_interval: timedelta = timedelta(seconds=config.get('pytrade2.strategy.purge.sec', 60))
        self.last_purge_time: datetime = datetime.min
        self._log.info(f"Data purge window: {self.purge_window}, purge interval: {self.purge_interval}")

    def purge_or_skip(self, *dfs: [pd.DataFrame]):
        time1 = datetime.utcnow()
        if time1 - self.last_purge_time >= self.purge_interval:
            self._log.debug(f"{self.purge_interval} elapsed from last learn time: {self.last_purge_time}")
            self.purge_all()
            self.last_purge_time = time1

    def purge_all(self):
        raise NotImplemented

    def purged(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        """
        self._log.debug(f"Purge old data using window {self.purge_window}")
        if not self.purge_window or df.empty:
            return df
        left_bound = df.index.max() - pd.Timedelta(self.purge_window)
        return df[df.index >= left_bound]

