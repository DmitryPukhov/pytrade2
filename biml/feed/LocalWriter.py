import logging.config
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from pathlib import Path
import glob


class LocalWriter:
    """
    Write feed to local dir
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        # {ticker:{interval: last existing date}}
        self.last_time_dict: Dict[str, Dict[str, datetime]] = {}

    def on_candles(self, candles_dict: Dict[str, Dict[str, pd.DataFrame]]):
        """
        New candles have come, write them to local data folder. Files split by date.
        """
        for ticker in candles_dict:
            for interval in candles_dict[ticker]:
                # Get candles of interval, i.e. M15
                candles = candles_dict[ticker][interval]

                # Filter candles after last time only
                if ticker not in self.last_time_dict:
                    self.last_time_dict[ticker] = {interval: self.get_last_data_time(ticker, interval)}
                elif interval not in self.last_time_dict[ticker]:
                    self.last_time_dict[ticker][interval] = self.get_last_data_time(ticker, interval)
                candles = candles[candles["open_time"] > self.last_time_dict[ticker][interval]]

                # If candles are for diferent days, write each day to it's file
                dates = candles["close_time"].dt.date.unique()
                for date in dates:
                    file_path = self.get_file_name(ticker, interval, date)
                    logging.info(f"Writing to {file_path}")
                    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
                    # Write
                    candles.to_csv(file_path, header=not Path(file_path).exists(), mode='a')
                self.last_time_dict[ticker][interval] = max(self.last_time_dict[ticker][interval], candles["close_time"].max())

    def get_file_name(self, ticker, interval: str, time: datetime):
        """
        Create file name for file data: folder/ticker/ticker_interval_time.csv
        """
        date = pd.to_datetime(time).date()
        return f"{self.data_dir}/{ticker}/{date}_{ticker}_{interval}.csv"

    def get_last_data_time(self, ticker: str, interval: str):
        """
        Get last time in the data, stored in data dir
        """
        # Go through data dir, get the last file of given interval
        pattern = f"{self.data_dir}/{ticker}/*_{ticker}_{interval}.csv"
        files = glob.glob(pattern)
        if not files:
            return pd.Timestamp.min
        last_day_path = max(files)
        # Get last close time in last day csv file
        return pd.read_csv(last_day_path, parse_dates=["close_time"])["close_time"].max()
