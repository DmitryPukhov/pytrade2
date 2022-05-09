import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import List
from feed.BaseFeed import BaseFeed
from feed.TickerInfo import TickerInfo


class LocalFeed(BaseFeed):
    """
    Read data from local folder, provide pandas dataframes with that data
    """

    def __init__(self, data_dir: str, tickers: List[TickerInfo]):
        super().__init__(tickers=tickers)
        self.data_dir = data_dir

    def read_interval(self, start_time: datetime, end_time: datetime):
        """
        Read data from local folder to pandas
        :param start_time: inclusive start time
        :param end_time: exclusive end time
        """
        for ticker in self.tickers:
            for interval in ticker.candle_intervals:
                pattern = f"{self.data_dir}/{ticker}/*_{ticker}_{interval}.csv"
                logging.info(f"Reading files, ticker: {ticker}, interval: {interval}, search pattern: {pattern}")
                files = [file for file in glob.glob(pattern) if LocalFeed.is_between(start_time)]

    @staticmethod
    def is_between(file: str, start_time: datetime, end_time: datetime) -> bool:
        """
        Does this file in data dir contain the data in interval
        :param file: file name with pattern f"{self.data_dir}/{ticker}/{datetime or date}_{ticker}_{interval}.csv"
        :param start_time: inclusive start time
        :param end_time: exclusive end time
        :return: True if between, False otherwise
        """
        date_str = Path(file).name.split("_")[0]
        if len(date_str) == 4:
            # If yearly file
            return start_time.year <= int(date_str) < end_time.year
        else:
            # If daily file
            file_date = datetime.fromisoformat(date_str).date()
            return start_time.date() <= file_date < end_time.date()
