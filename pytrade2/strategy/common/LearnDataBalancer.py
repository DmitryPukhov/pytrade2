from datetime import datetime
from multiprocessing import RLock

import pandas as pd


class LearnDataBalancer:
    """ Balance data to have equal amoount of 1,0,-1 signals """

    def __init__(self, max_len=100):
        self.x_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
        self.y_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
        self.max_len = max_len
        self.data_lock: RLock = RLock()

    def add_balanced(self,
                     new_dt_index: datetime,
                     new_x: {},
                     new_y: {}):
        """
        Add new xy to learn data and purge old learn data keeping balance
        """
        signal = new_y["signal"]
        # Insert or update
        with self.data_lock:
            self.x_dict[signal].loc[new_dt_index] = new_x
            self.y_dict[signal].loc[new_dt_index] = new_y
            self.balance()

    def balance(self):
        with self.data_lock:
            min_len = min([len(df) for df in self.x_dict.values()])
            min_len = min(self.max_len, min_len)
            for signal in (-1, 0, 1):
                self.x_dict[signal] = self.x_dict[signal].tail(min_len)
                self.y_dict[signal] = self.y_dict[signal].tail(min_len)
