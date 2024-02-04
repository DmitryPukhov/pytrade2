import logging
from io import StringIO

import pandas as pd


class LearnDataBalancer:
    """ Balance data to have equal amoount of 1,0,-1 signals """

    @staticmethod
    def balanced(x, y):
        if y.empty or x.empty:
            return x, y
        signal_counts = y['signal'].value_counts()
        balanced_cnt = min(signal_counts)
        if len(signal_counts) < 3:
            # Not all 3 signals are present
            balanced_cnt = 0

        balanced_y = pd.concat([y[y['signal'] == signal].tail(balanced_cnt) for signal in [-1,0,1]]).sort_index()
        balanced_x = x[x.index.isin(balanced_y.index)]

        # Log each signal count
        msgs_bal = ["Prepared balanced xy for learning. Balanced counts "]
        msgs_unbal = ["Unbalanced unused counts "]
        for signal in [-1, 0, 1]:
            cnt_bal = len(balanced_y[balanced_y['signal'] == signal]) if not balanced_y.empty else 0
            msgs_bal.append(f"signal{signal}:{cnt_bal}")
            cnt_unbal = len(y)
            msgs_unbal.append(f"signal{signal}:{cnt_unbal}")

        logging.info(' '.join(msgs_bal))
        logging.info(' '.join(msgs_unbal))
        return balanced_x, balanced_y
    #
    # def __init__(self, max_len=1000):
    #     self.x_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
    #     self.y_dict = {-1: pd.DataFrame(), 0: pd.DataFrame(), 1: pd.DataFrame()}
    #     self.max_len = max_len
    #
    # def get_report(self):
    #     """ Short info for report """
    #
    #     msg = StringIO()
    #     signal_names = {-1: "sell", 0: "oom", 1: "buy"}
    #     for signal in self.x_dict:
    #         msg.write(f"learn balanced {signal_names[signal]}: {len(self.x_dict[signal])} ")
    #         if not self.x_dict[signal].empty:
    #             msg.write(f"from {self.x_dict[signal].index.min().floor('s')}"
    #                       f" to {self.x_dict[signal].index.max().floor('s')}")
    #         msg.write("\n")
    #
    #     return msg.getvalue()
    #
    # def add(self, x: pd.DataFrame, y: pd.DataFrame):
    #     for signal in y['signal'].unique():
    #         new_x = x[y['signal'] == signal]
    #         self.x_dict[signal] = pd.concat([self.x_dict[signal], new_x]).sort_index().tail(self.max_len)
    #         new_y = y[y['signal'] == signal]
    #         self.y_dict[signal] = pd.concat([self.y_dict[signal], new_y]).sort_index().tail(self.max_len)
    #
    # def pop_balanced_xy(self) -> (pd.DataFrame, pd.DataFrame):
    #     # Remove and return balanced data from accumulated data
    #     min_len = min([len(df) for df in self.x_dict.values()])
    #     min_len = min(self.max_len, min_len)
    #     out_x = pd.concat([self.x_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()
    #     out_y = pd.concat([self.y_dict[signal].tail(min_len) for signal in (-1, 0, 1)]).sort_index()
    #
    #     # Remove extracted x,y from stored data
    #     for signal in (-1, 0, 1):
    #         self.x_dict[signal] = self.x_dict[signal].loc[~self.x_dict[signal].index.isin(out_x.index)]
    #         self.y_dict[signal] = self.y_dict[signal].loc[~self.y_dict[signal].index.isin(out_y.index)]
    #
    #     return out_x, out_y
