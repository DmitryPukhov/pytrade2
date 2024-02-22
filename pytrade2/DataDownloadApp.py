import argparse
import logging.config
from datetime import datetime
from pathlib import Path
from typing import Dict

from App import App
from exch.Exchange import Exchange
from strategy.feed.CandlesFeed import CandlesFeed
from strategy.feed.CandlesDownloader import CandlesDownloader


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

        to = self.config.get("to")
        self.to = datetime.fromisoformat(to) if to else datetime.now()



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
        parser.add_argument('--to',
                            help='last date',
                            default=None)

        return vars(parser.parse_args())

    def run(self):
        self._logger.info(f"Start downloading data to {self.download_dir}")
        period = "1min"
        feed = self.exchange_provider.candles_feed()
        candles_downloader = CandlesDownloader(self.config, feed, "common")
        intervals = CandlesDownloader.last_days(self.to, self.days, period)

        candles_downloader.download_intervals(intervals)


if __name__ == "__main__":
    DataDownloadApp().run()
