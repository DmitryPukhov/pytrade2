import glob
import logging.config
from pathlib import Path
from typing import List, Dict
import pandas as pd
from biml.feed.BaseFeed import BaseFeed


class LocalFeed(BaseFeed):
    """
    Read data from local folder, provide pandas dataframes with that data
    """

    def __init__(self, ticker: str, data_dir: str, limits: Dict[str, int]):
        super().__init__(ticker, limits)
        self.data_dir = data_dir

    #
    # @staticmethod
    # def extract_time(file_name: str):
    #     """
    #     Extract time information from data file name, formatted like ticker_time.csv
    #     """
    #     return file_name.split('_')[-1].split(".")[0] if file_name else None
    #
    # def get_last_existing_time(self, interval: str):
    #     """
    #     Find last time of existing local data
    #     """
    #     pattern = f"{self.data_dir}/{self.ticker}/{self.ticker}_{interval}_*.csv"
    #     files = glob.glob(pattern)
    #     last_file = max(files) if files else None
    #     last_time = self.extract_time(str(last_file)) if last_file else None
    #     return last_time
    #
    # @staticmethod
    # def get_file_name(data_dir, ticker, interval: str, time):
    #     """
    #     Create file name for file data: folder/ticker/ticker_interval_time.csv
    #     """
    #     return f"{data_dir}/{ticker}/{ticker}_{interval}_{time}.csv"

    def read(self):
        """
        Read data from local folder to pandas
        """
        pass
        # todo: implement reading from local folder
