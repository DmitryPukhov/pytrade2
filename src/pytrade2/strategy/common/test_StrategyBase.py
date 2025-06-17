from unittest import TestCase
from unittest.mock import MagicMock

from strategy.common.StrategyBase import StrategyBase


class TestStrategyBase(TestCase):
    class MyStrategy(StrategyBase):
        """ Derived strategy to check how do we use parameters from both"""

        def __init__(self, config: {}):
            super().__init__(config, MagicMock(), False, False, False)
            self.floatParam = 1.0
            self.intParam = 1
            self.strParam = "strParam1"
            self.boolParam = False

    @staticmethod
    def new_strategy():
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
                "pytrade2.order.quantity": 0.001}

        return TestStrategyBase.MyStrategy(conf)

    def test_apply_params_strings(self):
        strategy = self.new_strategy()
        strategy.apply_params({"floatParam": "2.0", "intParam": "3", "strParam": "strParam4"})

        # Strategy should get param type from previous value and set new param
        self.assertEqual(2.0, strategy.floatParam)
        self.assertEqual(3, strategy.intParam)
        self.assertEqual("strParam4", strategy.strParam)

    def test_apply_params_bools(self):
        strategy = self.new_strategy()
        strategy.apply_params({"boolParam": False})
        self.assertFalse(strategy.boolParam)

        strategy.apply_params({"boolParam": "False"})
        self.assertFalse(strategy.boolParam)

        strategy.apply_params({"boolParam": "True"})
        self.assertTrue(strategy.boolParam)

        strategy.apply_params({"boolParam": True})
        self.assertTrue(strategy.boolParam)

    def test_apply_params_floats(self):
        strategy = self.new_strategy()
        strategy.apply_params({"floatParam": 2.0, "intParam": 3.0, "strParam": "strParam4"})

        # Strategy should get param type from previous value and set new param
        self.assertEqual(2.0, strategy.floatParam)
        self.assertEqual(3, strategy.intParam)
        self.assertEqual("strParam4", strategy.strParam)

    def test_apply_params_is_trailing_stop(self):
        strategy = self.new_strategy()
        strategy.is_trailing_stop = True
        strategy.apply_params({"is_trailing_stop": "False"})
        self.assertFalse(strategy.is_trailing_stop)

        strategy.is_trailing_stop = True
        strategy.apply_params({"is_trailing_stop": False})
        self.assertFalse(strategy.is_trailing_stop)

        strategy.apply_params({"is_trailing_stop": True})
        self.assertTrue(strategy.is_trailing_stop)
