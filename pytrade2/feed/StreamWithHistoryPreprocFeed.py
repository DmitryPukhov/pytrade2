import logging
from datetime import datetime

import pandas as pd

from feed.history.HistoryS3Downloader import HistoryS3Downloader
from feed.history.Preprocessor import Preprocessor


class StreamWithHistoryPreprocFeed(object):
    """ Get old data from history and new data from stream"""

    def __init__(self, config: dict, stream_feed):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_dir = config.get("pytrade2.data.dir")
        self.ticker = config.get("pytrade2.tickers")
        self.stream_feed = stream_feed
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])
        self._history_downloader = HistoryS3Downloader(config, data_dir=self.data_dir)
        self._preprocessor = Preprocessor(data_dir=self.data_dir)
        self.is_good_history = False
        self._last_initial_history_datetime = datetime.min
        self._last_reload_initial_history_datetime = datetime.min
        self._reload_history_interval = pd.Timedelta(config.get("pytrade2.strategy.history.initial.reload.interval", "5min"))
        self._history_raw_today_df = pd.DataFrame()
        self._history_before_today_df = pd.DataFrame()

    def reload_initial_history(self, history_start = None, history_end = None):
        """ Initial download history from s3 to local raw data. Preprocess and put to local preprocessed data"""

        # Download history to raw data
        if not history_start:
            history_start = datetime.utcnow().date() - self.history_max_window
        if not history_end:
            history_end = datetime.utcnow().date()
        self._logger.info(
            f"Load s3 initial {self.ticker} {self.stream_feed.kind} history {self.ticker}, from {history_start} to {history_end}")
        self._history_downloader.update_local_history(self.ticker, history_start, history_end,
                                                      kinds=[self.stream_feed.kind])
        # Preprocess local raw data, put to local preprocessed data
        self._last_initial_history_datetime = max(self._last_initial_history_datetime, self._preprocessor.preprocess_last_raw_data(self.ticker, self.stream_feed.kind))
        self._last_reload_initial_history_datetime = datetime.utcnow()
        return self._last_initial_history_datetime

    def apply_buf(self):
        """ Update data from stream and history. Return if no gap"""

        # Get last stream data raw
        stream_raw_df = self.stream_feed.apply_buf()
        if stream_raw_df.empty:
            return

        stream_start_datetime = stream_raw_df.index.min()

        if not self.is_good_history:
            if (datetime.utcnow() - self._last_initial_history_datetime) < self._reload_history_interval:
                # Don't reload too often, try after self._reload_history_interval
                return pd.DataFrame()
            # Download history from s3
            history_start_datetime = stream_start_datetime - self.history_max_window if not stream_raw_df.empty else datetime.utcnow()
            history_end_datetime = self.reload_initial_history(history_start=history_start_datetime.date(), history_end=stream_start_datetime.date())
            self.is_good_history = history_end_datetime >= stream_start_datetime
            # If gap between stream and history, will try later, when history updated. Exiting now.
            if not self.is_good_history:
                gap = stream_start_datetime - history_end_datetime
                self._logger.warning(
                    f"Not enough history data, gap {gap} between history and stream. last preproc history end:{history_end_datetime}, stream start date: {stream_start_datetime}")
                return pd.DataFrame()

        # History is good

        # Get all local history except today
        if self._history_before_today_df.empty:
            self._history_before_today_df = self._preprocessor.read_last_preproc_data(self.ticker, self.stream_feed.kind,
                                                                                     days=self.history_max_window.days)

        # Get today preproc data from  history and stream
        if self._history_raw_today_df.empty:
            self._history_raw_today_df = self._history_downloader.read_local_history(self.ticker, self.stream_feed.kind,
                                                                           stream_start_datetime.date(),
                                                                           stream_start_datetime.date())
            self._history_raw_today_df = self._history_raw_today_df[self._history_raw_today_df.index < stream_start_datetime]

        raw_today_df = pd.concat([self._history_raw_today_df, stream_raw_df]).sort_index()
        preproc_today_df = self._preprocessor.transform(raw_today_df, self.stream_feed.kind)

        # Concatenate previous and today
        all_history_window = pd.concat([self._history_before_today_df, preproc_today_df]).sort_index()
        return all_history_window


