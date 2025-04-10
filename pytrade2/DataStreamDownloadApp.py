import logging
import multiprocessing
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from App import App
from exch.Exchange import Exchange
from feed.BidAskFeed import BidAskFeed
from feed.CandlesFeed import CandlesFeed
from feed.Level2Feed import Level2Feed
from strategy.persist.DataPersister import DataPersister


class DataStreamDownloadApp(App):
    """
    Download raw data from data stream
    """

    def __init__(self):
        super().__init__()

        # History window to keep in memory is the same as save interval, we need not saved data
        save_interval_sec = self.config.get("pytrade2.stream.save.interval.sec.local", 60)
        self.config["pytrade2.strategy.history.max.window"] = f"{save_interval_sec}s"

        self.save_interval_local = timedelta(seconds=float(save_interval_sec))
        self.save_interval_s3 = timedelta(seconds=float(self.config.get("pytrade2.stream.save.interval.sec.s3", 60.0)))


        # Will capture 1 min candles only
        self.config["pytrade2.feed.candles.periods"] = "1min"
        self.config["pytrade2.feed.candles.counts"] = "1"

        # Ensure download dir
        data_dir = Path(self.config["pytrade2.data.dir"])
        self.download_dir = Path(data_dir, "raw")
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Set up exchange
        self.ticker = self.config["pytrade2.tickers"].split(",")[-1]
        exchange_name = self.config["pytrade2.exchange"]
        logging.info(f"Exchange: {exchange_name}")
        exchange_provider = Exchange(self.config)
        tag = self.__class__.__name__
        # Set up feeds
        self.candles_feed = CandlesFeed(self.config, self.ticker, exchange_provider, multiprocessing.RLock(),
                                        multiprocessing.Event(), tag)
        self.level2_feed = Level2Feed(self.config, exchange_provider, multiprocessing.RLock(), multiprocessing.Event())
        self.bid_ask_lock = multiprocessing.RLock()
        self.bid_ask_feed = BidAskFeed(self.config, exchange_provider, multiprocessing.RLock(), multiprocessing.Event())

        # Set up persister to accumulate the data locally then upload to s3
        self.data_persister = DataPersister(self.config, "raw")
        #self.data_persister._logger.setLevel(logging.DEBUG)


    def run(self):
        self._logger.info(f"Start downloading stream data to {self.download_dir}")

        # Run feeds
        [feed.run() for feed in [self.candles_feed, self.bid_ask_feed, self.level2_feed]]
        last_save_time_local = datetime.now()
        last_save_time_s3 = datetime.now()
        # processing loop
        saved_s3_flag = False
        while True:
            # Get from buffers
            new_candles, new_bid_ask, new_level2 = self.get_accumulated_data()

            # Save
            for tag, df in {"candles": new_candles, "bid_ask": new_bid_ask, "level2": new_level2}.items():
                if not df.empty:
                    # Save locally
                    file_path = self.data_persister.persist_df(df, str(Path(self.download_dir, tag)), tag, self.ticker)

                    # Copy to s3
                    if (datetime.now() - last_save_time_s3) > self.save_interval_s3:
                        self.data_persister.copy2s3(file_path)
                        saved_s3_flag = True # Set the flag to update last s3 save time later

            # If saving to s3 happened, update the last s3 save time
            if saved_s3_flag: last_save_time_s3 = datetime.now()

            # Remove previous days data
            for subdir in ["candles", "bid_ask", "level2"]:
                path = Path(self.download_dir, subdir)
                if path.exists():
                    self.data_persister.purge_data_files(path)

            # Wait for next save time
            time.sleep(self.save_interval_local.seconds)

    def get_accumulated_data(self):
        """ Get accumulated data from feed buffers. Clean feed buffers then. """
        new_candles, new_bid_ask, new_level2 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        # New candles
        if self.candles_feed.new_data_event.is_set():
            # Get new candles buffer
            for period in self.candles_feed.candles_by_interval_buf:
                with self.candles_feed.data_lock:
                    new_candles = self.candles_feed.candles_by_interval_buf[period]
                    self.candles_feed.candles_by_interval_buf[period] = pd.DataFrame(columns=new_candles.columns)
        # New bid ask
        if self.bid_ask_feed.new_data_event.is_set():
            with self.bid_ask_feed.data_lock:
                new_bid_ask = self.bid_ask_feed.bid_ask_buf
                self.bid_ask_feed.bid_ask_buf = pd.DataFrame(columns=self.bid_ask_feed.bid_ask_buf.columns)
        # Mew level2
        if self.level2_feed.new_data_event.is_set():
            with self.level2_feed.data_lock:
                new_level2 = self.level2_feed.level2_buf
                self.level2_feed.level2_buf = pd.DataFrame(columns=self.level2_feed.level2_buf.columns)

        logging.info(
            f"Got new {len(new_candles)} candles, {len(new_bid_ask)} bid ask items, {len(new_level2)} level2 items")
        return new_candles, new_bid_ask, new_level2


if __name__ == "__main__":
    DataStreamDownloadApp().run()
