from io import StringIO
from typing import Dict, List
import pandas as pd


class BidAskFeed:
    def __init__(self, cfg: Dict[str, str]):
        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.bid_ask_buf: pd.DataFrame = pd.DataFrame()  # Buffer

        self.bid_ask_history_period = (pd.Timedelta(cfg.get("pytrade2.strategy.history.max.window"))
                                      + pd.Timedelta(cfg.get("pytrade2.strategy.predict.window", "0s")))
        self.data_lock = None
        self.new_data_event = None

    def on_ticker(self, ticker: dict):
        # Add new data to df
        new_df = pd.DataFrame([ticker], columns=list(ticker.keys())).set_index("datetime", drop=False)
        with self.data_lock:
            self.bid_ask_buf = pd.concat([self.bid_ask_buf, new_df])

        self.new_data_event.set()

    def update_bid_ask(self):
        """ Add buf to the data and purge old buf """

        with self.data_lock:
            self.bid_ask = pd.concat([df for df in [self.bid_ask, self.bid_ask_buf] if not df.empty]).sort_index()
            self.bid_ask_buf = pd.DataFrame()
            # Purge old data
            min_time = self.bid_ask.index.max() - self.bid_ask_history_period
            self.bid_ask = self.bid_ask[self.bid_ask.index > min_time]
        return self.bid_ask

    def get_report(self):
        """ Short info for report """
        time_format = '%Y-%m-%d %H:%M:%S'
        msg = StringIO()
        msg.write(f"\nBid ask ")
        msg.write(
            f"cnt:{self.bid_ask.index.size}, "
            f"first:{self.bid_ask.index.min().strftime(time_format)}, "
            f"last: {self.bid_ask.index.max().strftime(time_format)}"
            if not self.bid_ask.empty else "is empty")
        return msg.getvalue()
