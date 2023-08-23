from unittest import TestCase
from unittest.mock import MagicMock, Mock

import pandas as pd

from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase


class LongCandleStrategyBaseTest(TestCase):

    def new_strategy(self):
        conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                "pytrade2.data.dir": "tmp",
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

        LongCandleStrategyBase.__init__ = MagicMock(return_value=None)
        strategy = LongCandleStrategyBase(conf, None)
        strategy.target_period = "1min"
        return strategy

    def test_get_sl_tp_trdelta_buy(self):
        strategy = self.new_strategy()
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame([{"high": 3, "low": 1}])}
        sl,tp,trd = strategy.get_sl_tp_trdelta(1)

        self.assertEqual(1, sl)
        self.assertEqual(3, tp)
        self.assertEqual(2, trd)

    def test_get_sl_tp_trdelta_sell(self):
        strategy = self.new_strategy()
        strategy.candles_by_interval = {strategy.target_period: pd.DataFrame([{"high": 3, "low": 1}])}
        sl,tp,trd = strategy.get_sl_tp_trdelta(-1)

        self.assertEqual(3, sl)
        self.assertEqual(1, tp)
        self.assertEqual(2, trd)

