from io import StringIO

import pandas as pd


class LearnDataBalancer:
    """ Balance data to have equal amoount of 1,0,-1 signals """

    def __init__(self, max_len=1000):
        self.x_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
        self.y_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
        self.max_len = max_len

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        signal_names = {-1: "sell", 0: "oom", 1: "buy"}
        for signal in self.x_dict:
            msg.write(f"learn balanced {signal_names[signal]}: {len(self.x_dict[signal])} ")
            if not self.x_dict[signal].empty:
                msg.write(f"from {self.x_dict[signal].index.min().floor('s')}"
                          f" to {self.x_dict[signal].index.max().floor('s')}")
            msg.write("\n")

        return msg.getvalue()

    def add(self, x: pd.DataFrame, y: pd.DataFrame):
        for signal in y['signal'].unique():
            new_x = x[y['signal'] == signal]
            self.x_dict[signal] = pd.concat([self.x_dict[signal], new_x]).sort_index().tail(self.max_len)
            new_y = y[y['signal'] == signal]
            self.y_dict[signal] = pd.concat([self.y_dict[signal], new_y]).sort_index().tail(self.max_len)

    def pop_balanced_xy(self) -> (pd.DataFrame, pd.DataFrame):
        # Remove and return balanced data from accumulated data
        min_len = min([len(df) for df in self.x_dict.values()])
        min_len = min(self.max_len, min_len)
        out_x = pd.concat([self.x_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()
        out_y = pd.concat([self.y_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()

        # Remove extracted x,y from stored data
        for signal in (-1, 0, 1):
            self.x_dict[signal] = self.x_dict[signal].loc[~self.x_dict[signal].index.isin(out_x.index)]
            self.y_dict[signal] = self.y_dict[signal].loc[~self.y_dict[signal].index.isin(out_y.index)]

        return out_x, out_y
