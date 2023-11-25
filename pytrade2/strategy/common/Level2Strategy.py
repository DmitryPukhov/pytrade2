from typing import Dict, List
import pandas as pd


class Level2Strategy:
    def __init__(self):
        self.level2: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])
        self.level2_buf: pd.DataFrame = pd.DataFrame(columns=["datetime", "bid", "bid_vol", "ask", "ask_vol"])  # Buffer
        self.data_lock = None
        self.new_data_event = None

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

        # Add new data to df
        new_df = pd.DataFrame(level2, columns=bid_ask_columns).set_index("datetime", drop=False)
        with self.data_lock:
            self.level2_buf = pd.concat([self.level2_buf, new_df])

        self.new_data_event.set()

    def get_report(self):
        """ Short info for report """
        time_format = '%Y-%m-%d %H:%M:%S'

        return (f"Level2 cnt:{self.level2.index.size}, "
                f"first: {self.level2.index.min().strftime(time_format)}, "
                f"last: {self.level2.index.max().strftime(time_format)}") \
            if not self.level2.empty \
            else "Level2 is empty"
