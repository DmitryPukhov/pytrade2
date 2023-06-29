import logging
import multiprocessing
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd


class CandlesStrategy:
    """ Decorator for strategies. Reads candles from Binance """

    def __init__(self, config, ticker: str, candles_feed):
        # self.last_candles_read_time = datetime.min
        self._log = logging.getLogger(self.__class__.__name__)

        self.data_lock: multiprocessing.RLock() = None
        self.candles_feed = candles_feed
        self.ticker = ticker
        self.candles_by_interval: Dict[str, pd.DataFrame] = dict()
        self.data_lock: multiprocessing.RLock() = None

        periods = [s.strip() for s in str(config["pytrade2.feed.candles.periods"]).split(",")]
        counts = [int(s) for s in config["pytrade2.feed.candles.counts"].split(",")]

        self.candles_cnt_by_interval = dict(zip(periods, counts))

        history_window = config["pytrade2.strategy.history.max.window"]
        predict_window = config["pytrade2.strategy.predict.window"]
        self.candles_history_cnt_by_interval = self.candles_history_cnts(periods, counts, history_window,
                                                                         predict_window)

    @staticmethod
    def candles_history_cnts(intervals, counts, history_window, predict_window) -> dict:
        """ Calc how much candles of each interval to keep in history """

        out = {}
        for interval, count in zip(intervals, counts):
            period_time = pd.Timedelta(interval) * count + pd.Timedelta(history_window) + pd.Timedelta(predict_window)
            cnt = period_time // pd.Timedelta(interval) + 1
            out[interval] = cnt
        return out

    def read_initial_candles(self):
        # Produce initial candles
        for period, cnt in self.candles_history_cnt_by_interval.items():
            candles = pd.DataFrame(self.candles_feed.read_candles(self.ticker, period, cnt)).set_index("close_time",
                                                                                                       drop=False)
            self.candles_by_interval[period] = candles

    def on_candle(self, candle: {}):
        with self.data_lock:
            # candles_buf = self.candles_buf_all.get(candle["period"])
            period = str(candle["interval"])
            if period in self.candles_by_interval:
                candles_buf = self.candles_by_interval.get(period)
                last_open_time = candles_buf.iloc[-1]["open_time"]
                new_candle_time = candle["close_time"]
                if new_candle_time - last_open_time < pd.Timedelta(period):
                    # New candle is update of last candle in buffer
                    candle["open_time"] = last_open_time
                    candles_buf.iloc[-1] = candle
                else:
                    candle["open_time"] = candles_buf.iloc[-1]["close_time"]
                    # New candle is a new candle because period is passed
                    candles_buf.loc[candle["close_time"]] = candle
                    self.candles_by_interval[period] = candles_buf.tail(self.candles_history_cnt_by_interval[period])
            else:
                candle["open_time"] = candle["close_time"]
                # self.candles_buf_all[candle["period"]] = [candle]
                self.candles_by_interval[period] = pd.DataFrame([candle]).set_index("close_time", drop=False)

    def has_all_candles(self):
        """ If gathered required history """
        for period, min_count in self.candles_cnt_by_interval.items():
            if period not in self.candles_by_interval or len(self.candles_by_interval[period]) < min_count:
                return False
        return True

    def last_candles_info(self) -> dict:
        """ interval: last candle time"""

        return dict([(i, c.index[-1]) for i, c in self.candles_by_interval.items()])

    def is_alive(self):
        dt = datetime.now()
        for i, c in self.candles_by_interval.items():
            candle_max_delta = pd.Timedelta(i)
            if dt - c.index.max() > candle_max_delta * 2:
                # If double candle interval passed and we did not get a new candle, we are dead
                return False
        return True
