import argparse
import importlib
import logging.config
import os
import signal
import sys
import threading
from collections import defaultdict
import time
from datetime import datetime, date
from pathlib import Path
from pprint import pprint
from typing import Dict

import pandas as pd
import yaml

from App import App
from exch.Exchange import Exchange
from strategy.feed.CandlesFeed import CandlesFeed


class DataDownloadApp(App):
    """
    Download candles to history
    """

    def __init__(self):
        super().__init__()
        data_dir = Path(self.config["pytrade2.data.dir"])
        self.download_dir = Path(data_dir, "common", "candles")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        exchange_name = self.config["pytrade2.exchange"]
        self.exchange_provider = Exchange(self.config).exchange(exchange_name)
        self.ticker = self.config["pytrade2.tickers"].split(",")[-1]

        self.days = int(self.config.get("days", 1))
        # self.from_ = datetime.fromisoformat(self.config["from"]) if "from" in self.config \
        #     else datetime.combine(datetime.today(), datetime.min.time())
        # self.to = datetime.fromisoformat(self.config["to"]) if "to" in self.config \
        #     else datetime.now()
        # #self.limit = self.config.get("limit", 24 * 60)
        # self.limit = self.config.get("limit", None)

    def _parse_args(self) -> Dict[str, str]:
        """ Parse command line arguments"""

        parser = argparse.ArgumentParser()
        parser.add_argument('--days',
                            help='How much days to download example: --days 2')
        return vars(parser.parse_args())

    def run(self):
        logging.info(f"Start downloading data to {self.download_dir}")
        feed = self.exchange_provider.candles_feed()
        period = "1min"
        intervals = CandlesFeed.last_days(datetime.utcnow(), self.days, period)
        for start, end in intervals:
            # Get candles for the day from the service
            candles_raw = feed.read_candles(ticker=self.ticker,
                                            interval=period,
                                            limit=None,
                                            from_=start,
                                            to=end)

            candles = pd.DataFrame(candles_raw).set_index("close_time")
            # Save to file system
            file_name = f"{end.date()}_{self.ticker}_candles_{period}.csv"
            file_path = Path(self.download_dir, f"{file_name}")
            candles.to_csv(str(file_path),
                           header=True,
                           mode='w')
            logging.info(f"{period} {len(candles)} candles from {candles.index.min()} to {candles.index.max()} for {end.date()} downloaded to {file_path}")
        logging.info(f"Downloading of {self.days} days completed")


if __name__ == "__main__":
    DataDownloadApp().run()
