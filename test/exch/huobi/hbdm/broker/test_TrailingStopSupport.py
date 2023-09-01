import logging
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from exch.huobi.hbdm.broker.TrailingStopSupport import TrailingStopSupport
from model.Trade import Trade


class TestTrailingStopSupport(TestCase):

    @staticmethod
    def tss_mock():
        """ Trailing stop suport mocked"""
        tss = TrailingStopSupport({}, MagicMock(), MagicMock())
        tss.ticker = "btcusdt"
        tss._log = logging.Logger(TrailingStopSupport.__class__.__name__)
        tss.rest_client = MagicMock()
        tss.rest_client.post.return_value = {"status": "ok"}
        tss.db_session = MagicMock()

        return tss

    @staticmethod
    def new_trade():
        trade = Trade()
        trade.quantity = 1
        trade.take_profit_price = 10
        trade.trailing_delta = 2
        return trade

    def test_move_up(self):
        trade = self.new_trade() # take profit 10, trailing 2
        trade.side = "BUY"
        tss = self.tss_mock()
        tss.cur_trade = trade

        # Call
        tss.on_ticker({"ask": 13, "bid": 9})
        self.assertEqual(tss.rest_client.post.call_count, 2)
        self.assertEqual(tss.cur_trade.take_profit_price, 13)

        tss.rest_client.post.reset_mock()
        tss.last_ts_move_time = datetime.min
        # Call
        tss.on_ticker({"ask": 12, "bid": 9})
        self.assertEqual(tss.rest_client.post.call_count, 0)
        self.assertEqual(tss.cur_trade.take_profit_price, 13)

        tss.rest_client.post.reset_mock()
        tss.last_ts_move_time = datetime.min
        # Call
        tss.on_ticker({"ask": 14, "bid": 9})
        self.assertEqual(tss.rest_client.post.call_count, 2)
        self.assertEqual(tss.cur_trade.take_profit_price, 14)

    def test_move_down(self):
        trade = self.new_trade()  # Take profit 10, trailing 2
        trade.side = "SELL"
        tss = self.tss_mock()
        tss.cur_trade = trade

        # Call
        tss.on_ticker({"ask": 11, "bid": 8})
        self.assertEqual(tss.rest_client.post.call_count, 2)
        self.assertEqual(tss.cur_trade.take_profit_price, 8)

        tss.rest_client.post.reset_mock()
        tss.last_ts_move_time = datetime.min
        # Call
        tss.on_ticker({"ask": 11, "bid": 9})
        self.assertEqual(tss.rest_client.post.call_count, 0)
        self.assertEqual(tss.cur_trade.take_profit_price, 8)

        tss.rest_client.post.reset_mock()
        tss.last_ts_move_time = datetime.min
        # Call
        tss.on_ticker({"ask": 11, "bid": 7})
        self.assertEqual(tss.rest_client.post.call_count, 2)
        self.assertEqual(tss.cur_trade.take_profit_price, 7)

        self.assertNotEqual(tss.last_ts_move_time, datetime.min)