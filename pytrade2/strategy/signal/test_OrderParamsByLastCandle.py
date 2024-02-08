from unittest import TestCase
import pandas as pd
from strategy.signal.OrderParamsByLastCandle import OrderParamsByLastCandle


class TestOrderParamsByLastCandle(TestCase):
    @classmethod
    def new_signal_calc(cls):
        return OrderParamsByLastCandle(profit_loss_ratio=1, stop_loss_min_coeff=0, stop_loss_max_coeff=float('inf'),
                                       take_profit_min_coeff=0, take_profit_max_coeff=float('inf'), price_precision=0.2)

    def test_get_sl_tp_trdelta_buy(self):
        signal_calc = self.new_signal_calc()
        last_candle = pd.DataFrame([{"high": 3, "close": 2, "low": 1}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(1, last_candle)

        self.assertEqual(1, sl)
        self.assertEqual(3, tp)
        self.assertEqual(2, trd)

    def test_get_sl_tp_trdelta_buy_with_min_sltp(self):
        signal_calc = self.new_signal_calc()
        signal_calc.stop_loss_min_coeff = 0.6
        signal_calc.take_profit_min_coeff = 0.8

        last_candle = pd.DataFrame([{"high": 21, "close": 20, "low": 19}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(1, last_candle)

        self.assertEqual(8, sl)
        self.assertEqual(36, tp)
        self.assertEqual(12, trd)

    def test_get_sl_tp_trdelta_buy_with_max_sltp(self):
        signal_calc = self.new_signal_calc()
        signal_calc.stop_loss_max_coeff = 0.1
        signal_calc.take_profit_max_coeff = 0.2

        last_candle = pd.DataFrame([{"high": 30, "close": 20, "low": 10}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(1, last_candle)

        self.assertEqual(18, sl)
        self.assertEqual(24, tp)
        self.assertEqual(2, trd)

    def test_get_sl_tp_trdelta_buy_no_min_sltp(self):
        signal_calc = self.new_signal_calc()
        # default strategy.stop_loss_min_coeff = strategy.profit_min_coeff = 0
        last_candle = pd.DataFrame([{"high": 30, "close": 20, "low": 10}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(1, last_candle)

        self.assertEqual(10, sl)
        self.assertEqual(30, tp)
        self.assertEqual(20, trd)

    def test_get_sl_tp_trdelta_sell_with_min_sltp(self):
        signal_calc = self.new_signal_calc()
        signal_calc.stop_loss_min_coeff = 0.6
        signal_calc.take_profit_min_coeff = 0.8

        # default strategy.stop_loss_min_coeff = strategy.profit_min_coeff = 0
        last_candle = pd.DataFrame([{"high": 21, "close": 20, "low": 19}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(-1, last_candle)

        self.assertEqual(32, sl)
        self.assertEqual(4, tp)
        self.assertEqual(12, trd)

    def test_get_sl_tp_trdelta_sell_with_max_sltp(self):
        signal_calc = self.new_signal_calc()
        signal_calc.stop_loss_max_coeff = 0.1
        signal_calc.take_profit_max_coeff = 0.2

        # default strategy.stop_loss_min_coeff = strategy.profit_min_coeff = 0
        last_candle = pd.DataFrame([{"high": 30, "close": 20, "low": 10}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(-1, last_candle)

        self.assertEqual(22, sl)
        self.assertEqual(16, tp)
        self.assertEqual(2, trd)

    def test_get_sl_tp_trdelta_sell_no_min_sltp(self):
        signal_calc = self.new_signal_calc()
        # default strategy.stop_loss_min_coeff = strategy.profit_min_coeff = 0
        last_candle = pd.DataFrame([{"high": 3, "close": 2, "low": 1}]).iloc[-1]
        sl, tp, trd = signal_calc.get_sl_tp_trdelta(-1, last_candle)

        self.assertEqual(3, sl)
        self.assertEqual(1, tp)
        self.assertEqual(2, trd)
