import logging
import multiprocessing
from datetime import datetime
from io import StringIO
from typing import Dict

import pandas as pd


class CandlesStrategy:
    """ Decorator for strategies. Reads candles from exchange """

    def __init__(self, config, ticker: str, candles_feed):

        self.data_lock: multiprocessing.RLock() = None
        self.candles_feed = candles_feed
        self.ticker = ticker
        self.candles_by_interval: Dict[str, pd.DataFrame] = dict()
        self.data_lock: multiprocessing.RLock() = None
        self.new_data_event: multiprocessing.Event = None

        periods = [s.strip() for s in str(config["pytrade2.feed.candles.periods"]).split(",")]
        counts = [int(s) for s in str(config["pytrade2.feed.candles.counts"]).split(",")]

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
            cnt = period_time // pd.Timedelta(interval) + 1  # 1 - for diff
            out[interval] = cnt
        return out

    def read_candles(self):
        # Produce initial candles
        for period, cnt in self.candles_history_cnt_by_interval.items():
            # Read cnt + 1 extra for diff candles
            candles = pd.DataFrame(self.candles_feed.read_candles(self.ticker, period, cnt)) \
                .set_index("close_time", drop=False)
            logging.debug(f"Got {len(candles.index)} initial {self.ticker} {period} candles")
            self.candles_by_interval[period] = candles

    def get_report(self):
        if not self.candles_by_interval:
            return "Candles are empty"

        msg = StringIO()
        n = 5
        time_format = '%Y-%m-%d %H:%M:%S'
        for interval, candles in self.candles_by_interval.items():
            times = candles.tail(n).apply(lambda row: f"{row['close_time'].strftime(time_format)}", axis=1)[::-1]
            msg.write(f"{interval} candles: {', '.join(times)} ...\n")

        return msg.getvalue()

    def on_candle(self, candle: {}):
        with (self.data_lock):
            period = str(candle["interval"])
            logging.debug(f"Got {period} candle: {candle}")
            if period in self.candles_by_interval:
                candles = self.candles_by_interval.get(period)
                last_candle = candles.iloc[-1]
                last_open_time = last_candle["open_time"]

                if candle["close_time"] - last_open_time < pd.Timedelta(period):
                    if candle["close_time"] > last_candle["close_time"]:
                        candle["open_time"] = last_open_time
                        # Replace last candle
                        candles.drop(candles.index[-1], inplace=True)
                        candles.loc[candle["close_time"]] = candle
                else:
                    # Add new candle
                    candle["open_time"] = max(candle["open_time"], last_candle["close_time"] + pd.Timedelta(seconds=1))
                    candles.loc[candle["close_time"]] = candle
                    self.candles_by_interval[period] = candles.tail(self.candles_history_cnt_by_interval[period])
            else:
                # Create new dataframe for candles of this period
                self.candles_by_interval[period] = pd.DataFrame([candle]).set_index("close_time", drop=False)
        self.new_data_event.set()

    def has_all_candles(self):
        """ If gathered required history """
        for period, min_count in self.candles_cnt_by_interval.items():
            if period not in self.candles_by_interval or len(self.candles_by_interval[period]) < min_count:
                return False
        return True

    def is_alive(self):
        dt = datetime.now()
        for i, c in self.candles_by_interval.items():
            candle_max_delta = pd.Timedelta(i)
            if dt - c.index.max() > candle_max_delta * 2:
                # If double candle interval passed, and we did not get a new candle, we are dead
                return False
        return True
