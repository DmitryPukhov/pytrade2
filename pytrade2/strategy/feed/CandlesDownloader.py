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

    def __init__(self, config: Dict, exchange_candles_feed, tag: str):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.exchange_candles_feed = exchange_candles_feed
        data_dir = Path(self.config["pytrade2.data.dir"])
        self.download_dir = Path(data_dir, tag, "candles")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.ticker = self.config["pytrade2.tickers"].split(",")[-1]
        self.period = "1min"
        self.days = config.get("pytrade2.feed.candles.history.download.days", 2)

    def get_start_date(self):
        """ In candles data folder find last file and parse date from it''s name"""

        files = os.listdir(self.download_dir)
        if files:
            last_file = Path(max(files)).name
            last_date = datetime.fromisoformat(last_file[:10])
        else:
            last_date = datetime.now() - timedelta(self.days)
        # Force to start of the day
        last_date = datetime.combine(last_date.date(), datetime.min.time())
        return last_date

    def download_candles_inc(self):
        # Start date is parsed from last existing file. Last day file can be uncompleted, so download it again.
        start = self.get_start_date()
        end = datetime.today() + timedelta(days=1)

        self._logger.info(f"Downloading new candles from {start} to {end}. Total days: {self.days}")
        intervals = self.date_intervals(start, end)
        self._logger.info(f"{len(intervals)} days will be downloaded")
        self.download_intervals(intervals)

    def download_intervals(self, intervals: List[Tuple[datetime, datetime]]):
        """ Download 1 minite candles to history data. Other periods should be resampled from 1min if needed by strategy
         @:param intervals: [<from>, <to>] one interval - one day (2024-02-17 00:01, 2024-02-18 00:00)
         """
        self._logger.info(f"Start downloading candles to {self.download_dir}")
        period = "1min"

        for start, end in intervals:
            # Get candles for the day from the service
            candles_raw = self.exchange_candles_feed.read_candles(ticker=self.ticker,
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
            self._logger.info(
                f"{period} {len(candles)} candles from {candles.index.min()} to {candles.index.max()} for {end.date()} downloaded to {file_path}")
        self._logger.info(f"Downloading of {len(intervals)} intervals completed")

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
