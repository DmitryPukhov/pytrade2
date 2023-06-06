import unittest
from threading import RLock
from unittest.mock import Mock

from huobi.constant import TradeDirection
from huobi.model.market import TradeDetailEvent, TradeDetail

from exch.huobi.broker.TrailingStopSupport import TrailingStopSupport
from model.Trade import Trade


class TestTrailingStopSupport(unittest.TestCase):

    @staticmethod
    def event_of(direction, price):
        tde = TradeDetailEvent()
        td = TradeDetail()
        td.direction, td.price = direction, price
        # Append twice to check that called once only
        tde.data.append(td)
        tde.data.append(td)
        return tde

    @staticmethod
    def tss_of(open_price, tp_price):
        tss = TrailingStopSupport()
        tss.trade_lock = RLock()
        tss.cur_trade = Trade()
        tss.cur_trade.open_price, tss.cur_trade.take_profit_price = open_price, tp_price
        tss.cur_trade.status = "opened"
        tss.close_cur_trade = Mock()
        tss.update_trade_status = Mock()
        return tss

    def test_tp_buy_not_reached(self):
        tss = self.tss_of(10, 11)
        event = self.event_of(TradeDirection.BUY, 10.9)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_not_called()

    def test_tp_buy_reached(self):
        tss = self.tss_of(10, 11)
        event = self.event_of(TradeDirection.BUY, 11)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_called_once()

    def test_tp_buy_overcome(self):
        tss = self.tss_of(10, 11)
        event = self.event_of(TradeDirection.BUY, 11.01)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_called_once()

    def test_tp_sell_not_reached(self):
        tss = self.tss_of(10, 9)
        event = self.event_of(TradeDirection.SELL, 9.1)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_not_called()

    def test_tp_sell_reached(self):
        tss = self.tss_of(10, 9)
        event = self.event_of(TradeDirection.SELL, 9)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_called_once()

    def test_tp_sell_overcome(self):
        tss = self.tss_of(10, 9)
        event = self.event_of(TradeDirection.SELL, 8.9)
        tss.on_price_changed(event)

        tss.close_cur_trade.assert_called_once()


if __name__ == '__main__':
    unittest.main()
