import logging

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
