import datetime
import multiprocessing
import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase


class LongCandleStrategyBaseTest(TestCase):

    def new_strategy(self):
        conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                "pytrade2.exchange": None,
                "pytrade2.data.dir": None,
                "pytrade2.price.precision": 2,
                "pytrade2.amount.precision": 2,
                #"pytrade2.strategy.predict.window": "10s",
                "pytrade2.strategy.predict.window": "1min",
                "pytrade2.strategy.past.window": "1s",
                "pytrade2.strategy.history.min.window": "10s",
                "pytrade2.strategy.history.max.window": "10s",
                "pytrade2.strategy.riskmanager.wait_after_loss": "0s",

                "pytrade2.feed.candles.periods": "1min,5min",
                "pytrade2.feed.candles.counts": "1,1",
                "pytrade2.feed.candles.history.counts": "1,1",
                "pytrade2.order.quantity": 0.001}

        # LongCandleStrategyBase.__init__ = MagicMock(return_value=None)
        threading.Timer = MagicMock()
        strategy = LongCandleStrategyBase(conf, MagicMock())
        strategy.candles_feed = MagicMock()
        strategy.level2_feed = MagicMock()
        strategy.model = MagicMock()
        strategy.X_pipe = MagicMock()
        strategy.y_pipe = MagicMock()
        strategy.data_persister = MagicMock()
        strategy.data_lock = multiprocessing.RLock()
        # strategy.new_data_event = multiprocessing.Event()
        # strategy.candles_cnt_by_interval = {'1min': 1, '5min': 1}
        # strategy.candles_feed.candles_by_interval = dict()
        # strategy.learn_interval = pd.Timedelta.min
        strategy.processing_interval = pd.Timedelta('0 seconds')
        strategy.broker = MagicMock()
        return strategy

    # @skip
    def test_process_new_data(self):
        # Build strategy instance
        base_dt = datetime.datetime.fromisoformat('2023-10-15T10:00:00')
        strategy = self.new_strategy()
        strategy.candles_feed.candles_cnt_by_interval = {'1min': 2, '5min': 2}
        time.sleep = MagicMock()

        # Input data
        candles_1min = pd.DataFrame([
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
             'interval': '1min', 'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0, 'vol': 1.0}
        ]).set_index('close_time', drop=False)

        candles_5min = pd.DataFrame([
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
        ]).set_index('close_time', drop=False)
        level2 = pd.DataFrame(
            [
                {'datetime': datetime.datetime.fromisoformat('2023-05-21 07:05:00'),
                 'ask': 0.9, 'ask_vol': 1},
                {'datetime': datetime.datetime.fromisoformat('2023-05-21 07:05:00'),
                 'bid': 0.8, 'bid_vol': 1},

            ])

        # Mock input
        # strategy.candles_feed.candles_feed.read_candles = lambda ticker, interval, limit: \
        #     {'1min': candles_1min, '5min': candles_5min}[interval]
        # strategy.candles_feed.read_candles()
        strategy.candles_feed.candles_by_interval = {'1min': candles_1min, '5min': candles_5min}
        strategy.level2_feed.level2 = level2

        # Flat signal received
        strategy.y_pipe.inverse_transform = MagicMock(return_value=np.array([[0]]))
        strategy.process_new_data()

        # New candles should be added to candles_by_interval
        self.assertListEqual(candles_1min.index.tolist(),
                             strategy.candles_feed.candles_by_interval['1min'].index.tolist())
        # Data with targets should be added to learn data
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.x_dict[0].index.tolist())
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.y_dict[0].index.tolist())
        # self.assertTrue(strategy.learn_data_balancer.x_dict[1].empty)
        # self.assertTrue(strategy.learn_data_balancer.x_dict[-1].empty)

        # # Bull signal received
        # strategy.y_pipe.inverse_transform = MagicMock(return_value=[[1]])
        # strategy.process_new_data()
        #
        # # New candles should be added to candles_by_interval
        # self.assertListEqual([c['close_time'] for c in candles_1min],
        #                      strategy.candles_feed.candles_by_interval['1min'].index.tolist())
        # # Data with targets should be added to learn data
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.x_dict[0].index.tolist())
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.y_dict[0].index.tolist())
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.x_dict[1].index.tolist())
        # self.assertListEqual([datetime.datetime.fromisoformat('2023-10-15T10:00')],
        #                      strategy.learn_data_balancer.y_dict[1].index.tolist())
        #
        # self.assertTrue(strategy.learn_data_balancer.x_dict[-1].empty)

    def test_create_pipe_y_encode_decode(self):
        x = pd.DataFrame([1, 2, 3], columns=['col1'])
        y = pd.DataFrame([-1, 0, 1], columns=['signal'])

        x_pipe, y_pipe = self.new_strategy().create_pipe(x, y)

        y_encoded = y_pipe.transform(y)
        y_actual = y_pipe.inverse_transform(y_encoded)
        self.assertListEqual([[-1], [0], [1]], y_actual.tolist())
