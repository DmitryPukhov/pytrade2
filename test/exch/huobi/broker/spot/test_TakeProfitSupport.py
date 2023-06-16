import unittest
from threading import RLock
from unittest.mock import Mock
from huobi.model.market import TradeDetailEvent, TradeDetail
from exch.huobi.broker.spot.TakeProfitSupport import TakeProfitSupport
from model.Trade import Trade


class TestTakeProfitSupport(unittest.TestCase):

    @staticmethod
    def event_of(price):
        """ Event from exchange, containing given price"""
        tde = TradeDetailEvent()
        td = TradeDetail()
        td.price = price
        # Append twice to check that called once only
        tde.data.append(td)
        tde.data.append(td)
        return tde

    @staticmethod
    def tps_of(direction, open_price, tp_price):
        """ Prepared TakeProfitSupport object with current trade"""
        tss = TakeProfitSupport()
        tss.trade_lock = RLock()
        tss.cur_trade = Trade()
        side = Trade.order_side_names[direction]
        tss.cur_trade.side, tss.cur_trade.open_price, tss.cur_trade.take_profit_price = side, open_price, tp_price
        tss.cur_trade.status = "opened"
        tss.close_cur_trade = Mock()
        tss.create_closing_order = Mock()
        tss.update_cur_trade_status = Mock()
        return tss

    def test_tp_buy_not_reached(self):
        tss = self.tps_of(1, 10, 11)
        event = self.event_of(10.9)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_not_called()

    def test_tp_buy_reached(self):
        tss = self.tps_of(1, 10, 11)
        event = self.event_of(11)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_called_once()

    def test_tp_buy_overcome(self):
        tss = self.tps_of(1, 10, 11)
        event = self.event_of(11.01)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_called_once()

    def test_tp_sell_not_reached(self):
        tss = self.tps_of(-1, 10, 9)
        event = self.event_of(9.1)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_not_called()

    def test_tp_sell_reached(self):
        tss = self.tps_of(-1, 10, 9)
        event = self.event_of(9)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_called_once()

    def test_tp_sell_overcome(self):
        tss = self.tps_of(-1, 10, 9)
        event = self.event_of(8.9)
        tss.on_price_changed(event)

        tss.create_closing_order.assert_called_once()


if __name__ == '__main__':
    unittest.main()
