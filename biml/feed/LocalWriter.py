import glob
import logging.config
from datetime import datetime
from pathlib import Path
from typing import Dict
import pandas as pd


class LocalWriter:
    """
    Write feed to local dir
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        # {ticker:{interval: last existing date}}
        self.last_time_dict: Dict[str, Dict[str, datetime]] = {}
        logging.info(f"Initialized. data_dir={self.data_dir}")

    def on_candles(self, ticker: str, interval: str, candles: pd.DataFrame):
        """
        New candles have come, write them to local data folder. Files split by date.
        """
        logging.debug(f"Got {len(candles)}candles for {ticker}, {interval}")
        logging.debug(f"last_time_dict: {self.last_time_dict}")

        # Get last candle time
        if ticker not in self.last_time_dict:
            self.last_time_dict[ticker] = {interval: self.get_last_data_time(ticker, interval)}
        elif interval not in self.last_time_dict[ticker]:
            self.last_time_dict[ticker][interval] = self.get_last_data_time(ticker, interval)
        # Skip already received candles
        candles = candles[candles["open_time"] > self.last_time_dict[ticker][interval]]
        logging.debug(f"Selected {len(candles)} candles after last processed time {self.last_time_dict[ticker][interval]}")

        # If candles are for diferent days, write each day to it's file
        dates = candles["close_time"].dt.date.unique()
        for date in dates:
            file_path = self.get_file_name(ticker, interval, date)
            logging.debug(f"Writing candles for ticker {ticker}, interval {interval}, date {date} to {file_path}")
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            candles2write = candles[candles["close_time"].dt.date == date]
            # Write
            candles2write.to_csv(file_path, header=not Path(file_path).exists(), mode='a', index=False)
        # Remember last candle time to skip already received
        self.last_time_dict[ticker][interval] = max(self.last_time_dict[ticker][interval],
                                                    candles["close_time"].max())
        logging.debug(f"Processed candles. New last times: {self.last_time_dict}")

    def get_file_name(self, ticker, interval: str, time: datetime):
        """
        Create file name for file data: folder/ticker/ticker_interval_time.csv
        """
        # For minutes inside  day keep each day in it's file
        date = pd.to_datetime(time).date()
        # For days and months keep each year in it's file
        if interval[-1] in {'d', 'M'}:
            date = date.year
        return f"{self.data_dir}/{ticker}/{date}_{ticker}_{interval}.csv"

    def get_last_data_time(self, ticker: str, interval: str):
        """
        Get last time in the data, stored in data dir
        """

        # Go through data dir, get the last file of given interval
        pattern = f"{self.data_dir}/{ticker}/*_{ticker}_{interval}.csv"
        files = glob.glob(pattern, )
        if not files:
            return pd.Timestamp.min
        last_day_path = max(files)
        # Get last close time in last day csv file
        return pd.read_csv(last_day_path, parse_dates=["close_time"])["close_time"].max()
