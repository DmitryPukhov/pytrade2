from collections import defaultdict
from datetime import datetime, timedelta
from multiprocessing import RLock, Event
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd

from pytrade2.feed.CandlesFeed import CandlesFeed
from pytrade2.feed.LastCandle1MinFeed import LastCandle1MinFeed


class TestLastCandle1MinFeed(TestCase):

    @staticmethod
    def new_last_candle_1min_feed():
        final_config = defaultdict(str)
        final_config["pytrade2.feed.candles.periods"] = "1min"
        feed = LastCandle1MinFeed(final_config, "ticker1", MagicMock(), RLock(), Event())
        return feed

    def test_should_refresh_last_candle(self):
        feed = self.new_last_candle_1min_feed()
        candle1 = {"interval": "1min", "open_time": pd.Timestamp("2025-06-22 16:50"),
                   "close_time": pd.Timestamp("2025-06-22 16:51"), "open": 100, "high": 110, "low": 90,
                   "close": 105, "vol": 100}
        candle2 = {"interval": "1min", "open_time": pd.Timestamp("2025-06-22 16:51"),
                   "close_time": pd.Timestamp("2025-06-22 16:52"), "open": 100, "high": 110, "low": 90,
                   "close": 105, "vol": 100}
        candle3 = {"interval": "1min", "open_time": pd.Timestamp("2025-06-22 16:52"),
                   "close_time": pd.Timestamp("2025-06-22 16:53"), "open": 100, "high": 110, "low": 90,
                   "close": 105, "vol": 100}

        # Receive candle 1, 3
        feed.on_candle(candle1)
        self.assertEqual(feed.last_candle, candle1)
        feed.on_candle(candle3)
        self.assertEqual(feed.last_candle, candle3)
        # Receive candle 2 from the past, should be ignored
        feed.on_candle(candle2)
        self.assertEqual(feed.last_candle, candle3)