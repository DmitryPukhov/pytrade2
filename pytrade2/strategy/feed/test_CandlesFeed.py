from datetime import datetime, timedelta
import os
import sys
from unittest import TestCase

import pandas as pd

from strategy.common.test_StrategyStub import StrategyStub
from strategy.feed.CandlesFeed import CandlesFeed


class TestCandlesFeed(TestCase):
    def test_update_candles(self):
        strategy = StrategyStub()
        strategy.candles_by_interval = {}

        # Candle 1 received
        dt1 = datetime(year=2023, month=6, day=28, hour=9, minute=52)
        candle1 = {"close_time": dt1, "open_time": dt1, "interval": "1min", "close": 1}
        # Call
        strategy.on_candle(candle1)
        strategy.update_candles()
        candles = strategy.candles_by_interval["1min"]
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
        strategy.on_candle(candle2)
        strategy.update_candles()
        candles = strategy.candles_by_interval["1min"]
        # Assert candle 2
        self.assertEqual(1, len(candles))
        self.assertEqual([pd.Timestamp(dt1)],
                         [pd.Timestamp(dt) for dt in candles["open_time"].values.tolist()])
        self.assertEqual([pd.Timestamp(dt2)],
                         [pd.Timestamp(dt) for dt in
                          candles["close_time"].values.tolist()])
        self.assertEqual([2], candles["close"].values.tolist())

        # Candle3 received in 1 min - new candle
        dt3 = dt1 + timedelta(minutes=1)
        candle3 = {"open_time": dt2, "close_time": dt3, "interval": "1min", "close": 3}
        # Call candle3
        strategy.on_candle(candle3)
        strategy.update_candles()
        candles = strategy.candles_by_interval["1min"]
        # Assert candle3
        self.assertEqual(2, len(candles))
        # Second candle is forming, open time is fixed prev candle + 1s
        self.assertEqual([pd.Timestamp(dt1), pd.Timestamp(dt1) + pd.Timedelta("59s")],
                         [pd.Timestamp(dt) for dt in candles["open_time"].values.tolist()])
        self.assertEqual([pd.Timestamp(dt1) + pd.Timedelta("59s"), pd.Timestamp(dt3)],
                         [pd.Timestamp(dt) for dt in candles["close_time"].values.tolist()])
        self.assertEqual([2, 3], candles["close"].values.tolist())

    def test_has_history_empty(self):
        strategy = StrategyStub()
        strategy.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        strategy.candles_by_interval = {}

        self.assertFalse(strategy.has_all_candles())

    def test_has_all_candles_enough_history(self):
        strategy = StrategyStub()
        strategy.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        strategy.candles_by_interval = {"1min": [{}, {}],
                                        "5min": [{}, {}, {}]}

        self.assertTrue(strategy.has_all_candles())

    def test_has_all_candles_not_enough_history(self):
        strategy = StrategyStub()
        strategy.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        strategy.candles_by_interval = {"1min": [{}, {}],
                                        "5min": [{}, {}]}

        self.assertFalse(strategy.has_all_candles())

    def test_has_all_candles_not_all_periods(self):
        strategy = StrategyStub()
        strategy.candles_cnt_by_interval = {"1min": 2, "5min": 3}
        strategy.candles_by_interval = {"1min": [{}, {}]}

        self.assertFalse(strategy.has_all_candles())

    def test_candles_history_counts(self):
        periods = ["1min", "5min"]
        counts = [2, 2]
        history_window = "10min"
        predict_window = "1min"
        actual = CandlesFeed.candles_history_cnts(periods, counts, history_window, predict_window)
        self.assertEqual({"1min": 14, "5min": 5}, actual)

    def test_candles_history_counts__small_history(self):
        periods = ["1min", "5min"]
        counts = [2, 2]
        history_window = "10s"
        predict_window = "1s"
        actual = CandlesFeed.candles_history_cnts(periods, counts, history_window, predict_window)
        self.assertEqual({"1min": 3, "5min": 3}, actual)

    def test_days_of_2(self):
        actual = list(CandlesFeed.last_days(datetime.fromisoformat("2023-12-18"), 2, '1min'))

        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00")),
             (datetime.fromisoformat("2023-12-17 00:01:00"), datetime.fromisoformat("2023-12-18 00:00:00")),
             ], actual)

    def test_days_of_1(self):
        actual = list(CandlesFeed.last_days(datetime.fromisoformat("2023-12-18"), 1, '1min'))
        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00"))], actual)

    def test_days_of_0(self):
        actual = list(CandlesFeed.last_days(datetime.fromisoformat("2023-12-18"), 0, '1min'))
        self.assertListEqual(
            [], actual)
