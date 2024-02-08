from unittest import TestCase

from strategy.signal.SignalByFutBidAsk import SignalByFutBidAsk


class TestSignalByFutBidAsk(TestCase):
    @classmethod
    def signal_stub(cls):
        signal = SignalByFutBidAsk(profit_loss_ratio=1, stop_loss_min_coeff=0, stop_loss_max_coeff=float('inf'),
                                   take_profit_min_coeff=0, take_profit_max_coeff=float('inf'), price_precision=2)
        signal.profit_loss_ratio = 4
        return signal

    def test_get_signal_buy(self):
        # Strategy with profit/loss ratio = 4
        signal = self.signal_stub()

        actual_signal, price, actual_loss, actual_profit, tr_delta = signal.get_signal_sl_tp_trdelta(bid=10, ask=11,
                                                                                                     bid_max_fut=19,
                                                                                                     bid_min_fut=9,
                                                                                                     ask_min_fut=11,
                                                                                                     ask_max_fut=11)
        self.assertEqual(1, actual_signal)
        self.assertEqual(11, price)
        self.assertEqual(8.5, actual_loss)  # price - sl*1.25
        self.assertEqual(19, actual_profit)
        self.assertEqual(actual_loss, tr_delta)

    def test_open_signal_should_not_buy_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal_sl_tp_trdelta(bid=0, ask=100,
                                                                                                bid_min_fut=100,
                                                                                                bid_max_fut=149,
                                                                                                ask_min_fut=100,
                                                                                                ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_buy_gte_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0

        actual_signal, price, actual_loss, actual_profit, tp_delta = strategy.get_signal_sl_tp_trdelta(bid=0, ask=100,
                                                                                                       bid_min_fut=100,
                                                                                                       bid_max_fut=150,
                                                                                                       ask_min_fut=100,
                                                                                                       ask_max_fut=100)
        self.assertEqual(1, actual_signal)

    def test_open_signal_buy_should_not_buy_min_loss(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0.1

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal_sl_tp_trdelta(bid=0, ask=100,
                                                                                                bid_min_fut=100,
                                                                                                bid_max_fut=150,
                                                                                                ask_min_fut=100,
                                                                                                ask_max_fut=100)
        self.assertEqual(0, actual_signal)

    def test_open_signal_sell(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()

        actual_signal, price, actual_loss, actual_profit, tr_delta = strategy.get_signal_sl_tp_trdelta(bid=10, ask=11,
                                                                                                       bid_min_fut=0,
                                                                                                       bid_max_fut=0,
                                                                                                       ask_min_fut=2,
                                                                                                       ask_max_fut=12)

        self.assertEqual(-1, actual_signal)
        self.assertEqual(10, price)
        self.assertEqual(12.5, actual_loss)  # adjusted sl*1.25
        self.assertEqual(2, actual_profit)
        self.assertEqual(actual_loss, tr_delta)

    def test_open_signal_not_sell_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal_sl_tp_trdelta(bid=10, ask=11,
                                                                                                bid_min_fut=0,
                                                                                                bid_max_fut=0,
                                                                                                ask_max_fut=12,
                                                                                                ask_min_fut=2.1)

        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_not_sell_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal_sl_tp_trdelta(bid=100, ask=100,
                                                                                                bid_min_fut=100,
                                                                                                bid_max_fut=149,
                                                                                                ask_min_fut=51,
                                                                                                ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_sell_le_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, tp_delta = strategy.get_signal_sl_tp_trdelta(bid=100, ask=100,
                                                                                                       bid_min_fut=100,
                                                                                                       bid_max_fut=100,
                                                                                                       ask_min_fut=50,
                                                                                                       ask_max_fut=100)
        self.assertEqual(-1, actual_signal)

    def test_open_signal_sell_should_not_sell_min_loss(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.signal_stub()
        strategy.take_profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0.1

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal_sl_tp_trdelta(bid=100, ask=100,
                                                                                                bid_min_fut=100,
                                                                                                bid_max_fut=100,
                                                                                                ask_min_fut=50,
                                                                                                ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        # self.assertEqual(110, actual_loss)
