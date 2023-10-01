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

    def add(self,
            new_dt_index: datetime,
            new_x: {},
            new_y: {}):
        """
        Add new xy to learn data and purge old
        """
        signal = new_y["signal"]
        # Insert or update
        with (self.data_lock):
            self.x_dict[signal] = pd \
                .concat([self.x_dict[signal], pd.DataFrame(data=[new_x], index=[new_dt_index])]) \
                .sort_index() \
                .tail(self.max_len)
            self.y_dict[signal] = pd \
                .concat([self.y_dict[signal], pd.DataFrame(data=[new_y], index=[new_dt_index])]) \
                .sort_index() \
                .tail(self.max_len)

    def get_balanced_xy(self) -> (pd.DataFrame, pd.DataFrame):
        with self.data_lock:
            min_len = min([len(df) for df in self.x_dict.values()])
            min_len = min(self.max_len, min_len)
            out_x = pd.concat([self.x_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()
            out_y = pd.concat([self.y_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()
            return out_x, out_y
