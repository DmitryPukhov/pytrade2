from unittest import TestCase
from datetime import datetime

from sqlalchemy import DateTime

from model.Trade import Trade


class TestTrade(TestCase):
    def test_open_time_epoch_millis(self):
        # Set original trade
        original_dt = datetime.utcnow()
        original_millis = int(original_dt.timestamp()*1000)
        trade = Trade()
        trade.open_time = original_dt

        # Call get method
        actual_millis = trade.open_time_epoch_millis()

        # Assert millis
        self.assertEqual(original_millis, actual_millis)

    def test_direction_buy(self):
        trade = Trade()
        trade.side = "BUY"
        self.assertEqual(1, trade.direction())

    def test_direction_sell(self):
        trade = Trade()
        trade.side = "SELL"
        self.assertEqual(-1, trade.direction())

    def test_direction_bad(self):
        trade = Trade()
        self.assertIsNone(trade.direction())
