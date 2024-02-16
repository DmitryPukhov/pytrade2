import logging.config
import logging.config
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


class CandlesDownloader:
    """
    Download 1min candles to history to data/common
    """

    def __init__(self, config: Dict, candles_feed):
        self.config = config
        self.feed = candles_feed
        data_dir = Path(self.config["pytrade2.data.dir"])
        self.download_dir = Path(data_dir, "common", "candles")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.ticker = self.config["pytrade2.tickers"].split(",")[-1]
        self.period = "1min"
        self.days = config.get("pytrade2.feed.candles.history.download.days", 10)

    def get_start_date(self):
        """ In candles data folder find last file and parse date from it''s name"""

        files = os.listdir(self.download_dir)
        if not files:
            return datetime.today() - timedelta(self.days)
        last_file = Path(max(files)).name
        last_date = datetime.fromisoformat(last_file[:10])
        return last_date + timedelta(days=1)

    def download_candles_inc(self):
        start = self.get_start_date()
        end = datetime.today()
        intervals = self.date_intervals(start, end)
        self.download_intervals(intervals)

    def download_intervals(self, intervals: List[Tuple[datetime, datetime]]):
        """ @:param intervals: [<from>, <to>] """

        logging.info(f"Start downloading data to {self.download_dir}")
        period = "1min"

        for start, end in intervals:
            # Get candles for the day from the service
            candles_raw = self.feed.read_candles(ticker=self.ticker,
                                                 interval=period,
                                                 limit=None,
                                                 from_=start,
                                                 to=end)

            candles = pd.DataFrame(candles_raw).set_index("close_time")
            # Save to file system
            file_name = f"{start.date()}_{self.ticker}_candles_{period}.csv"
            file_path = Path(self.download_dir, f"{file_name}")
            candles.to_csv(str(file_path),
                           header=True,
                           mode='w')
            logging.info(
                f"{period} {len(candles)} candles from {candles.index.min()} to {candles.index.max()} for {end.date()} downloaded to {file_path}")
        logging.info(f"Downloading of {self.days} days completed")

    @staticmethod
    def date_intervals(from_: datetime, to: datetime, period="1d"):
        # Create a DatetimeIndex with the intervals
        intervals = pd.date_range(start=from_, end=to, freq=period)

        # Pair each interval with the next one
        intervals_pairs = list(zip(intervals[:-1].to_pydatetime(), intervals[1:].to_pydatetime()))
        intervals_pairs = [(t1.replace(hour=0, minute=1), t2) for (t1, t2) in intervals_pairs]
        return intervals_pairs

    @staticmethod
    def last_days(to: datetime, days, period="1min") -> [(datetime, datetime)]:
        period_delta = timedelta(seconds=pd.Timedelta(period).total_seconds())
        for i in list(range(days)):
            start = to.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i)
            end = start + timedelta(days=1)
            start_close_time = start + period_delta
            yield start_close_time, end
