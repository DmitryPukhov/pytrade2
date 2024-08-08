import logging
import multiprocessing
import time
from pathlib import Path

from App import App
from exch.Exchange import Exchange
from strategy.feed.BidAskFeed import BidAskFeed
from strategy.feed.CandlesFeed import CandlesFeed
from strategy.feed.Level2Feed import Level2Feed


class DataStreamDownloadApp(App):
    """
    Download raw data from data stream
    """

    def __init__(self):
        super().__init__()

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
        self.bid_ask_feed = BidAskFeed(self.config, exchange_provider, multiprocessing.RLock(), multiprocessing.Event())

    def run(self):
        self._logger.info(f"Start downloading stream data to {self.download_dir}")
        #self.candles_feed.run()

        # processing loop
        while True:
            if self.candles_feed.new_data_event.is_set():
                new_candles = self.candles_feed.apply_buf()
            if self.bid_ask_feed.new_data_event.is_set():
                new_bid_ask = self.bid_ask_feed.apply_buf()
            if self.level2_feed.new_data_event.is_set():
                new_level2 = self.level2_feed.apply_buf()

            time.sleep(5)


if __name__ == "__main__":
    DataStreamDownloadApp().run()
