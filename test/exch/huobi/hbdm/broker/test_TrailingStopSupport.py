import logging
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
        trade.take_profit_price = 10
        trade.trailing_delta = 2
        return trade

    def test_move_up(self):
        trade = self.new_trade()
        trade.side = "BUY"
        tss = self.tss_mock()
        tss.cur_trade = trade

        # Call
        tss.on_ticker({"ask": 13, "bid": 11})
        mm: MagicMock
        # mm.a
        self.assertEqual(tss.rest_client.post.call_count, 2)
        # tss.rest_client.post.assert_called_with("/linear-swap-api/v1/swap_cross_tpsl_cancelall",
        #                                              {"pair": tss.ticker})
