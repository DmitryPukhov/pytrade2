import multiprocessing
import threading
from unittest import TestCase
from unittest.mock import MagicMock
import pandas as pd
from strategy.common.SignalStrategyBase import SignalStrategyBase


class SignalStrategyBaseTest(TestCase):

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
        strategy = SignalStrategyBase(conf, MagicMock(), False, False, False)
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

    def test_create_pipe_y_encode_decode(self):
        x = pd.DataFrame([1, 2, 3], columns=['col1'])
        y = pd.DataFrame([-1, 0, 1], columns=['signal'])

        x_pipe, y_pipe = self.new_strategy().create_pipe(x, y)

        y_encoded = y_pipe.transform(y)
        y_actual = y_pipe.inverse_transform(y_encoded)
        self.assertListEqual([[-1], [0], [1]], y_actual.tolist())
