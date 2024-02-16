import logging
import multiprocessing
from datetime import datetime, date, time, timedelta, timezone
from io import StringIO
from typing import Dict
import pytz
import pandas as pd

from exch.Exchange import Exchange


class CandlesFeed:
    """ Decorator for strategies. Reads candles from exchange """

    def __init__(self, config, ticker: str, exchange_provider: Exchange, data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):

        self.data_lock = data_lock
        self.candles_feed = exchange_provider.candles_feed(config["pytrade2.exchange"])
        self.candles_feed.consumers.add(self)
        self.ticker = ticker
        self.candles_by_interval: Dict[str, pd.DataFrame] = dict()
        self.candles_by_interval_buf: Dict[str, pd.DataFrame] = dict()
        self.new_data_event = new_data_event

        periods = [s.strip() for s in str(config["pytrade2.feed.candles.periods"]).split(",")]
        counts = [int(s) for s in str(config["pytrade2.feed.candles.counts"]).split(",")]
        history_counts = [int(s) for s in str(config["pytrade2.feed.candles.history.counts"]).split(",")]
        self.candles_cnt_by_interval = dict(zip(periods, counts))
        self.candles_history_cnt_by_interval = dict(zip(periods, history_counts))

    def read_candles(self):
        # Produce initial candles
        for period, cnt in self.candles_cnt_by_interval.items():
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
        for interval, candles in self.candles_by_interval.items():
            times = candles.tail(n).apply(lambda row: f"{row['close_time'].floor('s')}", axis=1)[::-1]
            msg.write(f"{interval} candles: {', '.join(times)} ...\n")

        return msg.getvalue()

    def apply_buf(self):
        """ Combine candles with buffers"""

        with self.data_lock:
            for period, buf in self.candles_by_interval_buf.items():
                if buf.empty:
                    continue
                # candles + buf
                candles = self.candles_by_interval.get(period, pd.DataFrame())
                candles = pd.concat([df for df in [candles, buf] if not df.empty]).set_index("close_time", drop=False)
                candles = candles.resample(period).agg(
                    {'open_time': 'first', 'close_time': 'last', 'open': 'first', 'high': 'max', 'low': 'min',
                     'close': 'last', 'vol': 'max'}).sort_index().tail(
                    self.candles_history_cnt_by_interval[period])

                self.candles_by_interval[period] = candles
                self.candles_by_interval_buf[period] = pd.DataFrame()

    def on_candle(self, candle: {}):
        candle_df = pd.DataFrame([candle]).set_index("close_time", drop=False)
        with (self.data_lock):
            period = str(candle["interval"])
            logging.debug(f"Got {period} candle: {candle}")
            prev_buf = self.candles_by_interval_buf.get(period, pd.DataFrame())
            # Add to buffer
            self.candles_by_interval_buf[period] = pd.concat([prev_buf, candle_df])
        self.new_data_event.set()

    def has_min_history(self):
        """ If gathered required history """
        for period, min_count in self.candles_cnt_by_interval.items():
            if period not in self.candles_by_interval or len(self.candles_by_interval[period]) < min_count:
                return False
        return True

    def is_alive(self, _):
        dt = datetime.now()
        for i, c in self.candles_by_interval.items():
            candle_max_delta = pd.Timedelta(i)
            if dt - c.index.max() > candle_max_delta * 2:
                # If double candle interval passed, and we did not get a new candle, we are dead
                return False
        return True


