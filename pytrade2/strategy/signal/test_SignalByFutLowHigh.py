from unittest import TestCase

from strategy.signal.SignalByFutBidAsk import SignalByFutBidAsk
from strategy.signal.SignalByFutLowHigh import SignalByFutLowHigh


class TestSignalByFutLowHigh(TestCase):
    @classmethod
    def signal_stub(cls):
        signal = SignalByFutLowHigh(profit_loss_ratio=4,
                                    stop_loss_min_coeff=0,
                                    stop_loss_max_coeff=float('inf'),
                                    take_profit_min_coeff=0,
                                    take_profit_max_coeff=float('inf'),
                                    comission_pct=0,
                                    price_presision=2)
        return signal

    def test_get_signal_buy(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        signal, sl, tp = calc.calc_signal(100, 99, 104)
        self.assertEqual(1, signal)
        self.assertEqual(99, sl)
        self.assertEqual(104, tp)

    def test_get_signal_buy_sl_min(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()
        calc.stop_loss_min_coeff = 0.02

        signal, sl, tp = calc.calc_signal(100, 99, 104)
        self.assertEqual(1, signal)
        self.assertEqual(98, sl)  # sl =  close * (1-stop_loss_min_coeff)
        self.assertEqual(104, tp)

    def test_get_signal_buy_tp_max(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()
        calc.take_profit_max_coeff = 0.02

        signal, sl, tp = calc.calc_signal(100, 99, 104)
        self.assertEqual(1, signal)
        self.assertEqual(99, sl)  # sl =  close * (1-stop_loss_min_coeff)
        self.assertEqual(102, tp)

    def test_get_signal_buy_gap_up(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        signal, sl, tp = calc.calc_signal(100, 101, 102)
        self.assertEqual(1, signal)
        self.assertEqual(100, sl)
        self.assertEqual(102, tp)

    def test_get_signal_sell(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        signal, sl, tp = calc.calc_signal(100, 96, 101)
        self.assertEqual(-1, signal)
        self.assertEqual(101, sl)
        self.assertEqual(96, tp)

    def test_get_signal_sell_sl_min(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()
        calc.stop_loss_min_coeff = 0.02

        signal, sl, tp = calc.calc_signal(100, 96, 101)
        self.assertEqual(-1, signal)
        self.assertEqual(102, sl)  # sl =  close * (1+stop_loss_min_coeff)
        self.assertEqual(96, tp)

    def test_get_signal_sell_tp_max(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()
        calc.take_profit_max_coeff = 0.02

        signal, sl, tp = calc.calc_signal(100, 96, 101)
        self.assertEqual(-1, signal)
        self.assertEqual(101, sl)  # sl =  close * (1-stop_loss_min_coeff)
        self.assertEqual(98, tp)

    def test_get_signal_sell_gap_down(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        signal, sl, tp = calc.calc_signal(100, 99, 98)
        self.assertEqual(-1, signal)
        self.assertEqual(100, sl)
        self.assertEqual(99, tp)