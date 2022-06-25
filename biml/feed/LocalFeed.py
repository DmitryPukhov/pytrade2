import glob
import logging
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import List, Dict

import pandas as pd

from feed.BaseFeed import BaseFeed
from feed.TickerInfo import TickerInfo


class LocalFeed(BaseFeed):
    """
    Read data from local folder, provide pandas dataframes with that data
    """

    def __init__(self, data_dir: str, tickers: List[TickerInfo]):
        super().__init__(tickers=tickers)
        self.data_dir = data_dir

    def run(self):
        # Emulate feed for the last ticker
        self.emulate_feed(self.tickers[-1].ticker, self.tickers[-1].candle_intervals[-1], datetime.min, datetime.max)

    def emulate_feed(self, ticker: str, interval: str, start_time: datetime, end_time: datetime):
        df = self.read_ticker_interval(ticker, interval, start_time, end_time)
        for i in df.index:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_candles')]:
                # Imitate that new candle has come
                new_candles = df[df.index == i]
                consumer.on_candles(ticker=ticker, interval=interval, new_candles=new_candles)

    def read_ticker_interval(self, ticker: str, interval: str, start_time: datetime,
                             end_time: datetime) -> pd.DataFrame:
        """
        Read single ticker for interval like BTCUSDT M1 from start_time to end_time
        """
        pattern = f"{self.data_dir}/{ticker}/*_{ticker}_{interval}.csv"
        logging.info(f"Reading files, ticker: {ticker}, interval: {interval}, search pattern: {pattern}")
        files = [file for file in glob.glob(pattern) if LocalFeed.is_between(file, start_time, end_time)]
        df = pd.DataFrame()
        for file in files:
            new_df = pd.read_csv(file, parse_dates=["close_time"]).set_index("close_time", drop=False)
            df = df.append(new_df)
        return df

    def read_intervals(self, start_time: datetime, end_time: datetime) -> Dict:
        """
        Read data from local folder to pandas
        :param start_time: inclusive start time
        :param end_time: exclusive end time
        """
        return dict(
            [((ti.ticker, i),  # Key - ticker, interval
              self.read_ticker_interval(ti.ticker, i, start_time, end_time))  # Value - pandas df
             for ti in self.tickers for i in ti.candle_intervals])

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
        if not start_time: start_time = datetime.min
        if not end_time: end_time = datetime.max
        if len(date_str) == 4:
            # If yearly file
            return start_time.year <= int(date_str) < end_time.year
        else:
            # If daily file
            file_date = datetime.fromisoformat(date_str).date()
            return start_time.date() <= file_date < end_time.date()
