from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import MagicMock

import pandas as pd

from pytrade2.metrics.MetricServer import MetricServer
from pytrade2.strategy.LgbLowHighRegressionStrategy import LgbLowHighRegressionStrategy
from pytrade2.feed.CandlesFeed import CandlesFeed


class TestLgbLowHighRegressionStrategy(TestCase):

    def setUp(self):
        MetricServer.metrics = MagicMock()
        self.candles_init_patch = mock.patch.object(CandlesFeed, '__init__', return_value=None)
        self.candles_init_patch.start()

    def tearDown(self):
        self.candles_init_patch.stop()

    @classmethod
    def new_strategy(cls):
        conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                "pytrade2.exchange": None,
                "pytrade2.data.dir": None,
                "pytrade2.price.precision": 2,
                "pytrade2.amount.precision": 2,
                "pytrade2.strategy.predict.window": "1min",
                # "pytrade2.strategy.past.window": "1s",
                "pytrade2.strategy.history.min.window": "10s",
                "pytrade2.strategy.history.max.window": "10s",
                "pytrade2.strategy.riskmanager.wait_after_loss": "0s",

                "pytrade2.feed.candles.periods": "1min,5min",
                "pytrade2.feed.candles.counts": "1,1",
                # "pytrade2.feed.candles.history.days": "1",
                "pytrade2.order.quantity": 0.001,
                "pytrade2.broker.comissionpct": 0
                }
        strategy = LgbLowHighRegressionStrategy(config=conf, exchange_provider=MagicMock())
        strategy.candles_feed = MagicMock()
        strategy.risk_manager = MagicMock()
        strategy.risk_manager.can_trade = lambda: True
        strategy.broker = MagicMock()
        strategy.broker.cur_trade = None
        return strategy

    def test_apply_params_is_trailing_stop(self):
        strategy = self.new_strategy()

        strategy.apply_params({"features_candles_periods": "1min", "is_trailing_stop": True})
        self.assertTrue(strategy.is_trailing_stop)

        strategy.apply_params({"features_candles_periods": "1min", "is_trailing_stop": False})
        self.assertFalse(strategy.is_trailing_stop)

        y_pred = pd.DataFrame(data=[{"fut_low_diff": -1, "fut_high_diff": 1}])

        candles = pd.DataFrame(data=[{"close_time": None,
                                      'open': 10, "high": 20,
                                      "low": 5, "close": 10}])

        # Mock the strategy with current test behavior
        strategy.candles_feed.candles_by_interval = {strategy.target_period: candles}
        signal_ext_data = {"datetime": datetime.utcnow(), "signal": -1, "price": 100, "sl": 110, "tp": 90}

        # Call with trailing stop on
        strategy.apply_params({"features_candles_periods": "1min", "is_trailing_stop": True})
        strategy.signal_calc.calc_signal_ext = lambda close, fut_low, fut_high: signal_ext_data
        strategy.process_prediction(y_pred)
        self.assertIsNotNone(strategy.broker.create_cur_trade.call_args.kwargs["trailing_delta"])

        # Call with trailing stop off
        strategy.apply_params({"features_candles_periods": "1min", "is_trailing_stop": False})
        strategy.signal_calc.calc_signal_ext = lambda close, fut_low, fut_high: signal_ext_data
        strategy.process_prediction(y_pred)
        self.assertIsNone(strategy.broker.create_cur_trade.call_args.kwargs["trailing_delta"])
