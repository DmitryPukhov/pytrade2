import glob
import logging.config
from pathlib import Path

import pandas as pd

from biml.feed.BaseFeed import BaseFeed


class LocalFeed(BaseFeed):
    """
    Read data from local folder, provide pandas dataframes with that data
    """

    def __init__(self, ticker: str, data_dir: str, candle_fast_interval: str, candle_fast_limit: int,
                 candle_medium_interval: str, candle_medium_limit: int):
        super().__init__(ticker, candle_fast_interval, candle_fast_limit, candle_medium_interval, candle_medium_limit)
        self.data_dir = data_dir

    @staticmethod
    def extract_time(file_name: str):
        """
        Extract time information from data file name, formatted like ticker_time.csv
        """
        # todo: implement
        return file_name.split('_')[-1].split(".")[0] if file_name else None

    def get_last_existing_time(self, interval: str):
        """
        Find last time of existing local data
        """
        pattern = f"{self.data_dir}/{self.ticker}/{self.ticker}_{interval}_*.csv"
        files = glob.glob(pattern)
        last_file = max(files) if files else None
        last_time = self.extract_time(str(last_file)) if last_file else None
        return last_time

    def get_file_name(self, interval: str, time):
        """
        Create file name for file data: folder/ticker/ticker_interval_time.csv
        """
        return f"{self.data_dir}/{self.ticker}/{self.ticker}_{interval}_{time}.csv"

    def write_new(self, interval, all_data: pd.DataFrame):
        """
        Write only new data to local folder for the ticker
        """
        # Get last time of existing data for the ticker
        last_existing_time = self.get_last_existing_time(interval)

        # Get only new data from input dataframe
        new_data = all_data[all_data["open_time"] > int(last_existing_time)] if last_existing_time else all_data

        # Write new data
        file_name = self.get_file_name(interval, all_data["close_time"].max())
        logging.info(f"Writing new data to {file_name}")
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
        new_data.to_csv(file_name)

    def read(self):
        """
        Read data from local folder to pandas
        """
        pass
        # todo: implement reading from local folder
