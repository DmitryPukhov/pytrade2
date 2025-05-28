import logging
import math
from datetime import datetime

import pandas as pd

from feed.CandlesFeed import CandlesFeed
from feed.history.HistoryS3Downloader import HistoryS3Downloader
from feed.history.Preprocessor import Preprocessor


class StreamWithHistoryPreprocFeed(object):
    """ Get old data from history and new data from stream"""

    def __init__(self, config: dict, stream_feed):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_dir = config.get("pytrade2.data.dir")
        self.ticker = config.get("pytrade2.tickers")
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])
        self._history_downloader = HistoryS3Downloader(config, data_dir=self.data_dir)

        self.stream_feed = stream_feed
        self.data_lock = stream_feed.data_lock

        if isinstance(self.stream_feed, CandlesFeed):
            # Candles feed has it's owm history window mechanism, set it to our history window
            history_days = math.ceil(pd.Timedelta(self.history_max_window) / pd.Timedelta("1d"))
            self._logger.debug(
                f"Adjust candles stream feed to {self.history_max_window} window as {history_days} days of candles")
            self.stream_feed.apply_periods("1min", history_days=history_days, load_history=False)

        self.kind = self.stream_feed.kind
        self._preprocessor = Preprocessor(data_dir=self.data_dir)
        self.is_good_history = False
        self._last_history_datetime = pd.Timestamp.min
        self._reload_history_interval = pd.Timedelta(
            config.get("pytrade2.strategy.history.initial.reload.interval", "1min"))
        self._next_reload_history_datetime = datetime.now() + self._reload_history_interval
        self.preproc_data_df = pd.DataFrame()

    def run(self):
        self.stream_feed.run()

    def is_alive(self, max_delta):
        # After history is loaded, stream data should come regularly
        return not self.is_good_history or self.stream_feed.is_alive(max_delta)

    def reload_initial_history(self, history_start=None, history_end=None):
        """ Initial download history from s3 to local raw data.
        Preprocess and put to local preprocessed data"""

        # Download history to raw data
        if not history_start:
            history_start = pd.Timestamp.now().date() - self.history_max_window
        if not history_end:
            history_end = pd.Timestamp.now().date()
        self._logger.info(
            f"Load s3 initial {self.ticker} {self.kind} history {self.ticker}, from {history_start} to {history_end}")
        self._history_downloader.update_local_history(self.ticker, history_start, history_end,
                                                      kinds=[self.kind])
        # Preprocess local raw data, put to local preprocessed data
        self._last_history_datetime = max(self._last_history_datetime,
                                          self._preprocessor.preprocess_last_raw_data(self.ticker, self.kind))
        self._next_reload_history_datetime = pd.Timestamp.now() + self._reload_history_interval
        return self._last_history_datetime

    def apply_buf(self):
        """ Update data from stream and history. Return if no gap"""

        # Get last stream data raw
        stream_raw_df = self.stream_feed.apply_buf()

        if isinstance(self.stream_feed, CandlesFeed):
            # apply_buf() above does not return 1 minute dataframe, get it from candles_by_interval
            stream_raw_df = self.stream_feed.candles_by_interval.get("1min", pd.DataFrame())
        if stream_raw_df is None or stream_raw_df.empty:
            self._logger.debug(f"Buffer {self.kind} {self.ticker} did not contain any data")
            return

        stream_start_datetime = stream_raw_df.index.min()

        # Initial download history from s3. Maybe skip this time if there is no enough history in s3 yet
        if not self.is_good_history:
            if datetime.now() >= self._next_reload_history_datetime:
                # Reload timeout passed, download history from s3
                history_start_datetime = stream_start_datetime - self.history_max_window if not stream_raw_df.empty else pd.Timestamp.now()
                history_end_datetime = self.reload_initial_history(history_start=history_start_datetime.date(),
                                                                   history_end=stream_start_datetime.date())
                self.is_good_history = history_end_datetime >= stream_start_datetime

                # After reloaded, if there is still a gap between stream and history, exit now to try later
                if not self.is_good_history:
                    gap = pd.Timedelta(stream_start_datetime.to_numpy() - history_end_datetime.to_numpy())
                    self._logger.info(
                        f"Still not enough {self.kind} {self.ticker} history data, gap {gap} between history and stream. "
                        f"last preproc history end:{history_end_datetime}, stream start date: {stream_start_datetime}")
                    return pd.DataFrame()
                else:
                    # History is good
                    self._logger.info(f"History of {self.kind} {self.ticker} is good and ready")
            else:
                # Timeout to reload is not elapsed yet
                self._logger.debug(
                    f"Too early to reload {self.kind} {self.ticker} history, wait until {self._next_reload_history_datetime}")
                # Don't reload too often, try after self._reload_history_interval
                return pd.DataFrame()

        # Initial get all local history window for learning
        if self.preproc_data_df.empty:
            self.preproc_data_df = self._preprocessor.read_last_preproc_data(
                self.ticker, self.kind, days=self.history_max_window.days)
        return self.preproc_incremental(stream_raw_df)

    def preproc_incremental(self, stream_raw_df) -> pd.DataFrame:
        """
        Preprocess and append new stream data to old previous data
        Updates self.preproc_data_df and returns it as well
        """
        if self.preproc_data_df.empty or stream_raw_df.empty:
            self._logger.debug(f"Nothing to preprocess. History is empty:{self.preproc_data_df.empty}, stream data is empty: {stream_raw_df.empty}")
            return self.preproc_data_df

        self._logger.debug(
            f"Combine new and previous {self.kind} {self.ticker} data. Previous data is from: {self.preproc_data_df.index[0]}, to:{self.preproc_data_df.index[-1]}, new data is from: {stream_raw_df.index[0]} to:{stream_raw_df.index[-1]}")

        # Cut old data from the stream df
        stream_low_bound = self.preproc_data_df.index[-1] - pd.Timedelta("1min")  # 1 minute before last preproc data
        stream_raw_df = stream_raw_df[stream_raw_df.index > stream_low_bound]
        if stream_raw_df.empty or stream_raw_df.index[-1] < self.preproc_data_df.index[0]:
            self._logger.debug(f"Not enough {self.kind} {self.ticker} new data. Will try next time")
            return self.preproc_data_df

        stream_preproc_df = self._preprocessor.transform(stream_raw_df, self.kind)
        stream_preproc_df = stream_preproc_df[stream_preproc_df.index > self.preproc_data_df.index[-1]]
        if stream_preproc_df.empty:
            self._logger.debug(f"Not enough {self.kind} {self.ticker} new data after preprocessing. Will try next time")
            return self.preproc_data_df
        else:
            self._logger.debug(
                f"Stream adjusted preproc {self.kind} {self.ticker} data is from: {stream_preproc_df.index[0]}, to: {stream_preproc_df.index[-1]}")

        # Append new stream data to old previous data
        with self.data_lock:
            self.preproc_data_df = pd.concat([df for df in [self.preproc_data_df, stream_preproc_df] if not df.empty])
        self._logger.debug(
            f"Final preprocessed {self.kind} {self.ticker} data starts at {self.preproc_data_df.index[0]}, ends at {self.preproc_data_df.index[-1]}")
        return self.preproc_data_df