import datetime
import unittest
from typing import Dict
from unittest import mock
from unittest.mock import Mock, MagicMock

from broker.BinanceBroker import BinanceBroker


class TestBinanceBroker(unittest.TestCase):
    config: Dict[str, any] = {"biml.broker.trade.allow": True}

    @staticmethod
    def __broker__init_db__(self, cfg):
        self.db_session = MagicMock()

    def setUp(self) -> None:
        mock.patch.object(BinanceBroker, '__init_db__', self.__broker__init_db__).start()
        mock.patch.object(BinanceBroker, "read_last_opened_trade", lambda self: None).start()

    def test_create_cur_trade__not_filled(self):
        # Binance client mock
        client = MagicMock()
        client.new_order = Mock(return_value={"status": "bad"})
        client.new_oco_order = Mock()

        # Class under test
        broker = BinanceBroker(client, TestBinanceBroker.config)

        # Test call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Assert
        client.new_order.assert_called_once()  # Main order attempt
        client.new_oco_order.assert_not_called()  # No sl/tp call if main order failed

    def test_create_cur_trade__sl_tp_from_filled_price(self):
        # Binance client mock
        client = Mock()
        client.new_order = Mock(return_value={"status": "FILLED",
                                              "orderId": 1,
                                              "fills": [{"price": 20}],
                                              "transactTime": 123})
        client.new_oco_order = Mock(return_value={"status": "FILLED", "orderListId": 2})
        client.get_oco_order = Mock(return_value={"status": "FILLED", "orderListId": 2,
                                                  "orders": [{"orderId": 3}, {"orderId": 4}]})

        # Class under test
        broker = BinanceBroker(client, TestBinanceBroker.config)

        # Test call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Asserts
        client.new_order.assert_called()  # Main order
        client.new_oco_order.assert_called()  # Sl/tp order

        self.assertEqual("BUY", broker.cur_trade.side)
        self.assertEqual(1.0, broker.cur_trade.quantity)
        self.assertEqual(20, broker.cur_trade.open_price)
        self.assertEqual(19, broker.cur_trade.stop_loss_price)
        self.assertEqual(21, broker.cur_trade.take_profit_price)


if __name__ == '__main__':
    unittest.main()
