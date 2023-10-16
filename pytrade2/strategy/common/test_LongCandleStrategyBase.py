import datetime
import multiprocessing
import threading
import time
from threading import Timer
from typing import Dict
from unittest import TestCase, skip
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase
from strategy.common.features.CandlesFeatures import CandlesFeatures
from strategy.common.features.LongCandleFeatures import LongCandleFeatures


class LongCandleStrategyBaseTest(TestCase):

    def new_strategy(self):
        conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                "pytrade2.data.dir": None,
                "pytrade2.price.precision": 2,
                "pytrade2.amount.precision": 2,
                "pytrade2.strategy.predict.window": "10s",
                "pytrade2.strategy.past.window": "1s",
                "pytrade2.strategy.history.min.window": "10s",
                "pytrade2.strategy.history.max.window": "10s",
                "pytrade2.strategy.riskmanager.wait_after_loss": "0s",

                "pytrade2.feed.candles.periods": "1min,5min",
                "pytrade2.feed.candles.counts": "1,1",
                "pytrade2.order.quantity": 0.001}

        # LongCandleStrategyBase.__init__ = MagicMock(return_value=None)
        threading.Timer = MagicMock()
        strategy = LongCandleStrategyBase(conf, None)
        # strategy.x_unchecked = pd.DataFrame()
        # strategy.y_unchecked = pd.DataFrame()
        strategy.model = MagicMock()
        strategy.X_pipe = MagicMock()
        strategy.y_pipe = MagicMock()
        strategy.save_last_data = MagicMock()
        # strategy.data_lock = multiprocessing.RLock()
        # strategy.new_data_event = multiprocessing.Event()
        # strategy.candles_cnt_by_interval = {'1min': 1, '5min': 1}
        # strategy.candles_by_interval = dict()
        # strategy.learn_interval = pd.Timedelta.min
        strategy.candles_feed = MagicMock()
        return strategy

    def test_get_sl_tp_trdelta_buy(self):
        strategy = self.new_strategy()
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame([{"high": 3, "low": 1}])}
        sl, tp, trd = strategy.get_sl_tp_trdelta(1)

        self.assertEqual(1, sl)
        self.assertEqual(3, tp)
        self.assertEqual(2, trd)

    def test_get_sl_tp_trdelta_sell(self):
        strategy = self.new_strategy()
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame([{"high": 3, "low": 1}])}
        sl, tp, trd = strategy.get_sl_tp_trdelta(-1)

        self.assertEqual(3, sl)
        self.assertEqual(1, tp)
        self.assertEqual(2, trd)

    def test_update_unchecked_dups(self):
        # Prepare strategy
        strategy = self.new_strategy()

        # Single candle, cannot calculate targets
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame(
            data=[{'low': 1, 'high': 1}],
            index=[1])}
        # First call
        checked_x, checked_y = strategy.update_unchecked(pd.DataFrame([{'low': 1, 'high': 1}], index=[1]),
                                                         pd.DataFrame(data=[1], index=[1]))
        self.assertTrue(checked_x.empty)
        self.assertTrue(checked_y.empty)
        self.assertListEqual([1], strategy.x_unchecked.index.tolist())
        self.assertListEqual([1], strategy.y_unchecked.index.tolist())

        checked_x, checked_y = strategy.update_unchecked(pd.DataFrame([{'low': 1, 'high': 1}], index=[1]),
                                                         pd.DataFrame(data=[1], index=[1]))
        self.assertTrue(checked_x.empty)
        self.assertTrue(checked_y.empty)
        self.assertListEqual([1], strategy.x_unchecked.index.tolist())  # No duplicates
        self.assertListEqual([1], strategy.y_unchecked.index.tolist())  # No duplicates

    def test_update_unchecked(self):
        # Prepare strategy
        strategy = self.new_strategy()
        # CandlesFeatures.targets_of = MagicMock(return_value=pd.DataFrame(data=[1], index=[1]))
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame(
            data=[{'low': 1, 'high': 1}, {'low': 2, 'high': 2}, {'low': 3, 'high': 3}],
            index=[1, 2, 3])}

        # Add candle 1
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame(
            data=[{'low': 1, 'high': 1}],
            index=[1])}
        checked_x, checked_y = strategy.update_unchecked(pd.DataFrame([{'low': 1, 'high': 1}], index=[1]),
                                                         pd.DataFrame(data=[{"signal": 1}], index=[1]))

        self.assertTrue(checked_x.empty)
        self.assertTrue(checked_y.empty)

        # Add candle 2
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame(
            data=[{'low': 1, 'high': 1}, {'low': 2, 'high': 2}],
            index=[1, 2])}
        checked_x, checked_y = strategy.update_unchecked(pd.DataFrame([{'low': 2, 'high': 2}], index=[2]),
                                                         pd.DataFrame(data=[{'signal': -1}], index=[2]))

        self.assertTrue(checked_x.empty)
        self.assertTrue(checked_y.empty)

        # Add candle3
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame(
            data=[{'low': 1, 'high': 1}, {'low': 2, 'high': 2}, {'low': 3, 'high': 3}],
            index=[1, 2, 3])}
        checked_x, checked_y = strategy.update_unchecked(pd.DataFrame([{'low': 3, 'high': 3}], index=[3]),
                                                         pd.DataFrame(data=[{'signal': 0}], index=[3]))

        # Candle1 is old, has targets
        self.assertListEqual([1], checked_x.index.tolist())
        self.assertListEqual([1], checked_y.index.tolist())

        # Candle 2,3 are still without targets
        self.assertListEqual([2, 3], strategy.x_unchecked.index.tolist())
        self.assertListEqual([2, 3], strategy.y_unchecked.index.tolist())

    # @skip
    def test_process_new_data(self):
        # Build strategy instance
        base_dt = datetime.datetime.fromisoformat('2023-10-15T10:00:00')
        strategy = self.new_strategy()
        strategy.candles_cnt_by_interval = {'1min': 2, '5min': 2}
        time.sleep = MagicMock()

        # Input data
        candles_1min = ([
            # before prev to make diff() for prev not none
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:57:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T09:58:00'),
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # prev
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:58:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T09:59:00'),
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},

            # Current
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:59:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T10:00'),
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # next1
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T10:00:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T10:01:00'),
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # next2
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T10:01:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T10:02:00'),
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0}])

        candles_5min = ([
            # before prev to make diff() for prev not none
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:45:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T09:50:00'),
             'interval': '5min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # prev
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:50:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T09:55:00'),
             'interval': '5min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # Current
            {'open_time': datetime.datetime.fromisoformat('2023-10-15T09:55:00'),
             'close_time': datetime.datetime.fromisoformat('2023-10-15T10:00'),
             'interval': '5min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0},
            # next1
            # {'open_time': datetime.datetime.fromisoformat('2023-10-15T10:00:00'),
            #  'close_time': datetime.datetime.fromisoformat('2023-10-15T10:01:00'),
            #  'interval': '1min', 'open': 10, 'high': 11, 'low': 9, 'close': 10, 'vol': 1},
            # # next2
            # {'open_time': datetime.datetime.fromisoformat('2023-10-15T10:01:00'),
            #  'close_time': datetime.datetime.fromisoformat('2023-10-15T10:02:00'),
            #  'interval': '1min', 'open': 10, 'high': 11, 'low': 9, 'close': 10, 'vol': 1}
        ])

        # Mock input
        strategy.candles_feed.read_candles = lambda ticker, interval: \
            {'1min': candles_1min, '5min': candles_5min}[interval]
        # Mock signal
        strategy.y_pipe.inverse_transform = MagicMock(return_value=[[0]])

        # Call
        strategy.process_new_data()

        # self.assertListEqual([pd.Timestamp(base_dt - datetime.timedelta(minutes=5))],
        #                      strategy.candles_by_interval['5min']['close_time'].tolist())
