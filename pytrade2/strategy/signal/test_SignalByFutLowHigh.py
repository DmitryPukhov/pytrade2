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

        signal, sl, tp = calc.calc_signal(100, 90, 110, 100, 200)
        self.assertEqual(1, signal)
        self.assertEqual(100, sl)
        self.assertEqual(200, tp)

    def test_get_signal_buy_tp_sl_ratio(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        # pessimistic  buy loss = high-fut_low = 101-98 = 3, buy profit = fut_high-high=113-101=12, ratio=12/3=4 - buy
        signal, sl, tp = calc.calc_signal(100, 99, 101, 98, 113)
        self.assertEqual(1, signal)
        self.assertEqual(98, sl)
        self.assertEqual(113, tp)

        # fut_high decreased below tp/sl ratio
        signal, sl, tp = calc.calc_signal(100, 99, 101, 98, 112)
        self.assertEqual(0, signal)

    def test_get_signal_sell_tp_sl_ratio(self):
        # Strategy with profit/loss ratio = 4
        calc = self.signal_stub()

        # pessimistic  sell loss = fut_high-low = 102-99=3, sell profit = low-fut_low = 99-86=12, tp/sl = 4 ok
        signal, sl, tp = calc.calc_signal(100, 99, 101, 87, 102)
        self.assertEqual(-1, signal)
        self.assertEqual(102, sl)
        self.assertEqual(87, tp)

        # fut_low increased, tp/sl ratio is bad for sell
        signal, sl, tp = calc.calc_signal(100, 99, 101, 88, 102)
        self.assertEqual(0, signal)
