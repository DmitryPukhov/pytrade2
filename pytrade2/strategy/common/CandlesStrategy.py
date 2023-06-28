import logging
import multiprocessing
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd


class CandlesStrategy:
    """ Decorator for strategies. Reads candles from Binance """

    def __init__(self, config, ticker: str, candles_feed):
        #self.last_candles_read_time = datetime.min
        self._log = logging.getLogger(self.__class__.__name__)

        self.data_lock: multiprocessing.RLock() = None
        self.candles_feed = candles_feed
        self.ticker = ticker
        self.candles_by_period: Dict[str, pd.DataFrame] = dict()
        self.data_lock: multiprocessing.RLock() = None

        periods = [s.strip() for s in str(config["pytrade2.feed.candles.periods"]).split(",")]
        counts = [int(s) for s in config["pytrade2.feed.candles.counts"].split(",")]
        self.candles_cnt_by_period = dict(zip(periods, counts))

    def on_candle(self, candle: {}):
        with self.data_lock:
            # candles_buf = self.candles_buf_all.get(candle["period"])
            period = candle["interval"]
            if period in self.candles_by_period:
                candles_buf = self.candles_by_period.get(candle["interval"])
                last_open_time = candles_buf.iloc[-1]["open_time"]
                new_candle_time = candle["close_time"]
                period = pd.Timedelta(candle["interval"])
                if new_candle_time - last_open_time < period:
                    # New candle is update of last candle in buffer
                    candle["open_time"] = last_open_time
                    candles_buf.iloc[-1] = candle
                else:
                    candle["open_time"] = candles_buf.iloc[-1]["close_time"]
                    # New candle is a new candle because period is passed
                    candles_buf.loc[candle["close_time"]] = candle
            else:
                candle["open_time"] = candle["close_time"]
                # self.candles_buf_all[candle["period"]] = [candle]
                self.candles_by_period[period] = pd.DataFrame([candle]).set_index("close_time", drop=False)

    def has_all_candles(self):
        """ If gathered required history """
        for period, min_count in self.candles_cnt_by_period.items():
            if period not in self.candles_by_period or len(self.candles_by_period[period]) < min_count:
                return False
        return True

    def last_candle_min_time(self)->Optional[datetime]:
        """ Minimal time of all last time of all candles. """
        return min([candles.index[-1] for candles in self.candles_by_period.values()]) if self.candles_by_period else None
