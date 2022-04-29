import logging
from typing import Any, Dict

import pandas as pd

from biml.App import App


class Downloader(App):
    """
    Download prices from binance to data folder
    """

    def __init__(self):
        super().__init__()
        self.data_dir = self.config["biml.data.dir"]

    # def run(self, binance_feed: BinanceFeed, local_feed:LocalFeed,  ticker: str):
    def run(self):
        logging.info("Run downloader")
        self.feed.consumers.append(self)
        self.feed.run()

    def on_candles(self, src: Any, candles: Dict[str, pd.DataFrame], new_data_start_time_ms: int):
        for interval in candles:
            c = candles[interval]
            new_candles = c[c["close_time"] >= new_data_start_time_ms]
            new_candles.to_csv()


if __name__ == "__main__":
    Downloader().run()
