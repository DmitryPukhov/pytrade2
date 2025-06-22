from collections import defaultdict
from datetime import datetime, timedelta
from multiprocessing import RLock, Event
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd

from pytrade2.feed.CandlesFeed import CandlesFeed


class TestCandlesFeed(TestCase):
    @staticmethod
    def new_candles_feed(config={}):
        final_config = defaultdict(str)
        final_config["pytrade2.feed.candles.periods"] = config.get("pytrade2.feed.candles.periods", "1min,5min")
        final_config["pytrade2.feed.candles.counts"] = config.get("pytrade2.feed.candles.counts", "1,1")
        final_config["pytrade2.feed.candles.history.days"] = config.get("pytrade2.feed.candles.history.days", "1")
        feed = CandlesFeed(final_config, "ticker1", MagicMock(), RLock(), Event(), "test")
        feed.read_candles = MagicMock()
        return feed

    def test_apply_periods_empty(self):
        feed = self.new_candles_feed()
        days = 2
        feed.apply_periods("", days)

        # CandlesFeed should have new periods counts
        self.assertEqual({}, feed.candles_cnt_by_interval)

    def test_apply_periods(self):
        feed = self.new_candles_feed()
        days = 2
        expected = {"10min": 6 * 24 * days, "15min": 60 / 15 * 24 * days}

        # Call, compare with expected
        feed.apply_periods("[ 10min , 15min ]", days)
        self.assertEqual(expected, feed.candles_cnt_by_interval)

        feed.apply_periods("[ '10min' , '15min' ]", days)
        self.assertEqual(expected, feed.candles_cnt_by_interval)

    def test_candles_counts_in_days(self):
        days = 2
        actual = CandlesFeed.candles_counts_in_days({"1min", "5min"}, days)
        # Check candles counts in given number of days
        self.assertEqual({"1min": 60 * 24 * days, "5min": 60 / 5 * 24 * days}, actual)
        # Counts should be int, not floats
        for cnt in actual.values():
            self.assertEqual(int, type(cnt))

    def test_candles_counts_in_days_empty_intervals(self):
        actual = CandlesFeed.candles_counts_in_days(set(), 2)
        # Empty input - empty output
        self.assertEqual({}, actual)
        actual = CandlesFeed.candles_counts_in_days(set(), 0)
        # Empty input - empty output
        self.assertEqual({}, actual)

    def test_candles_counts_in_days_zero_days(self):
        actual = CandlesFeed.candles_counts_in_days({"1min", "5min"}, 0)
        # zero days - zero candles of each period
        self.assertEqual({"1min": 0, "5min": 0}, actual)

    def test_update_candles(self):
        candles_feed = self.new_candles_feed()
        # strategy.candles_feed.candles_by_interval = {}

        # Candle 1 received
        dt1 = datetime(year=2023, month=6, day=28, hour=9, minute=52)
        candle1 = {"close_time": dt1, "open_time": dt1, "interval": "1min", "open": 1, "high": 1, "low": 1, "close": 1,
                   "vol": 1}
        # Call
        candles_feed.on_candle(candle1)
        candles_feed.apply_buf()
        candles = candles_feed.candles_by_interval["1min"]
        # Assert candle1
        self.assertEqual(1, len(candles))
        self.assertEqual([dt1], candles.index.tolist())
        self.assertEqual([pd.Timestamp(dt1)],
                         [pd.Timestamp(dt) for dt in candles.index.values.tolist()])
        self.assertEqual([1], candles["close"].values.tolist())

        # Candle2 received in 59 sec - update of candle1
        dt2 = dt1 + timedelta(seconds=59)
        candle2 = {"open_time": dt1, "close_time": dt2, "interval": "1min", "close": 2}
        # Call
        candles_feed.on_candle(candle2)
        candles_feed.apply_buf()
        candles = candles_feed.candles_by_interval["1min"]
        # Assert candle 2
        self.assertEqual(2, len(candles))
        self.assertEqual([pd.Timestamp(dt1), pd.Timestamp(dt1)],
                         [pd.Timestamp(dt) for dt in candles["open_time"].values.tolist()])
        self.assertEqual([pd.Timestamp(dt1), pd.Timestamp(dt2)],
                         [pd.Timestamp(dt) for dt in
                          candles["close_time"].values.tolist()])
        self.assertEqual([1, 2], candles["close"].values.tolist())

        # Candle3 received in 1 min - new candle
        dt3 = dt1 + timedelta(minutes=1)
        candle3 = {"open_time": dt2, "close_time": dt3, "interval": "1min", "close": 3}
        # Call candle3
        candles_feed.on_candle(candle3)
        candles_feed.apply_buf()
        candles = candles_feed.candles_by_interval["1min"]
        # Assert candle3
        self.assertEqual(2, len(candles))
        # Second candle is forming, open time is fixed prev candle + 1s
        self.assertEqual([pd.Timestamp(dt1), pd.Timestamp(dt1)],
                         [pd.Timestamp(dt) for dt in candles["open_time"].values.tolist()])
        self.assertEqual([pd.Timestamp(dt1), pd.Timestamp(dt3)],
                         [pd.Timestamp(dt) for dt in candles["close_time"].values.tolist()])
        self.assertEqual([1, 3], candles["close"].values.tolist())

    def test_has_history_empty(self):
        candles_feed = self.new_candles_feed()
        candles_feed.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        candles_feed.candles_by_interval = {}

        self.assertFalse(candles_feed.has_min_history())

    def test_has_all_candles_enough_history(self):
        candles_feed = self.new_candles_feed()
        candles_feed.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        candles_feed.candles_by_interval = {"1min": [{}, {}],
                                            "5min": [{}, {}, {}]}

        self.assertTrue(candles_feed.has_min_history())

    def test_has_all_candles_not_enough_history(self):
        candles_feed = self.new_candles_feed()
        candles_feed.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        candles_feed.candles_by_interval = {"1min": [{}, {}],
                                            "5min": [{}, {}]}

        self.assertFalse(candles_feed.has_min_history())

    def test_has_all_candles_not_all_periods(self):
        candles_feed = self.new_candles_feed()
        candles_feed.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        candles_feed.candles_by_interval = {"1min": [{}, {}]}

        self.assertFalse(candles_feed.has_min_history())

    def test_is_alive_alive(self):
        candles_feed = self.new_candles_feed()
        # Small lag
        candles = pd.DataFrame(index=[datetime.now()], data={"close": 1})
        candles_feed.candles_by_interval = {"1min": candles}

        self.assertTrue(candles_feed.is_alive(None))

    def test_is_alive_not_alive(self):
        candles_feed = self.new_candles_feed()
        # Large lag
        candles = pd.DataFrame(index=[datetime.now() - timedelta(minutes=10)], data={"close": 1})
        candles_feed.candles_by_interval = {"1min": candles}

        self.assertFalse(candles_feed.is_alive(None))

    def test_last_1min_candle(self):
        candles_feed = self.new_candles_feed(
            {"pytrade2.feed.candles.periods": "1min", "pytrade2.feed.candles.counts": "1"})
        self.assertEqual(candles_feed.candles_cnt_by_interval, {"1min": 1})
        self.assertEqual(candles_feed.candles_by_interval.keys(), {"1min"})

        # Received 2 candles, should go to buffer
        candles_feed.on_candle({"interval": "1min", "open_time": pd.Timestamp("2025-06-22 16:50"),
                                "close_time": pd.Timestamp("2025-06-22 16:51"), "open": 100, "high": 110, "low": 90,
                                "close": 105, "vol": 100})
        candles_feed.on_candle({"interval": "1min", "open_time": pd.Timestamp("2025-06-22 16:51"),
                                "close_time": pd.Timestamp("2025-06-22 16:52"), "open": 100, "high": 110, "low": 90,
                                "close": 105, "vol": 100})

        self.assertEqual(len(candles_feed.candles_by_interval_buf["1min"]), 2)
        self.assertTrue(candles_feed.candles_by_interval["1min"].empty)

        # apply_buf should move candles to main candles_by_interval and clean buffer
        candles_feed.apply_buf()
        self.assertTrue(candles_feed.candles_by_interval_buf["1min"].empty)
        self.assertEqual(len(candles_feed.candles_by_interval["1min"].index.tolist()), 2)
