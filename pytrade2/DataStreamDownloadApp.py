import logging
import multiprocessing
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from App import App
from exch.Exchange import Exchange
from strategy.feed.BidAskFeed import BidAskFeed
from strategy.feed.CandlesFeed import CandlesFeed
from strategy.feed.Level2Feed import Level2Feed
from strategy.persist.DataPersister import DataPersister


class DataStreamDownloadApp(App):
    """
    Download raw data from data stream
    """

    def __init__(self):
        super().__init__()

        # Will capture 1 min candles only
        self.config["pytrade2.feed.candles.periods"] = "1min"
        self.config["pytrade2.feed.candles.counts"] = "1"

        # Ensure download dir
        data_dir = Path(self.config["pytrade2.data.dir"])
        self.download_dir = Path(data_dir, "stream/raw")
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
        # self.data_persister.save_interval = pd.Timedelta(0)

    def run(self):
        self._logger.info(f"Start downloading stream data to {self.download_dir}")

        # Run feeds
        [feed.run() for feed in [self.candles_feed, self.bid_ask_feed, self.level2_feed]]
        last_save_time = datetime.now()
        last_s3_time = datetime.now()
        # processing loop
        while True:
            # Get from buffers
            new_candles, new_bid_ask, new_level2 = self.get_accumulated_data()

            # Save
            self.data_persister.s3_enabled = False
            for tag, df in {"candles": new_candles, "bid_ask": new_bid_ask, "level2": new_level2}.items():
                if not df.empty:
                    # Save locally then copy to s3
                    file_path = self.data_persister.persist_df(df, str(Path(self.download_dir, tag)), tag, self.ticker)
                    self.data_persister.copy2s3(file_path)
            # Remove previous days data

            for subdir in ["candles", "bid_ask", "level2"]:
                self.data_persister.purge_data_files(Path(self.download_dir, subdir))

            time.sleep(5)

    def purge(self):
        os.listdir(self.download_dir)

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
