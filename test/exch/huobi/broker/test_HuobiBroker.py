from datetime import datetime
import unittest
from typing import Dict
from unittest import mock
from unittest.mock import Mock, MagicMock

from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderState
from huobi.model.trade import Order
from exch.huobi.broker.HuobiBroker import HuobiBroker
from exch.BrokerBase import BrokerBase


class TestHuobiBroker(unittest.TestCase):
    config: Dict[str, any] = {"pytrade2.broker.trade.allow": True,
                              "pytrade2.exchange.huobi.connector.key": "123",
                              "pytrade2.exchange.huobi.connector.secret": "456",
                              "pytrade2.exchange.huobi.connector.url": None,
                              "pytrade2.broker.huobi.account.id": 1,
                              "pytrade2.tickers": "btcusdt"
                              }

    @staticmethod
    def __broker__init_db__(self, cfg):
        self.db_session = MagicMock()

    def setUp(self) -> None:
        mock.patch.object(BrokerBase, '__init_db__', self.__broker__init_db__).start()
        mock.patch.object(BrokerBase, "read_last_opened_trade", lambda self: None).start()
        mock.patch.object(TradeClient, "sub_order_update", Mock()).start()
        mock.patch.object(MarketClient, "sub_trade_detail", Mock()).start()

    def test_create_cur_trade__not_filled(self):
        # Empty orders mocks
        mock.patch.object(TradeClient, "create_order", Mock()).start()
        mock.patch.object(TradeClient, "get_order", Mock()).start()

        # Class under test
        broker = HuobiBroker(TestHuobiBroker.config)

        # Test call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Assert
        broker.trade_client.create_order.assert_called_once()  # Main order attempt

    def test_create_cur_trade__success(self):
        # Binance client mock
        mock.patch.object(TradeClient, "create_order", Mock(return_value=1)).start()
        order = Order()
        order.orderId = 1
        order.created_at = datetime.utcnow().timestamp()*1000
        order.price = 10
        order.amount = 1
        order.state = OrderState.FILLED

        mock.patch.object(TradeClient, "get_order", Mock(return_value=order)).start()

        # Class under test
        broker = HuobiBroker(TestHuobiBroker.config)
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)


        self.assertEqual("BUY", broker.cur_trade.side)
        self.assertEqual(1, broker.cur_trade.open_order_id)
        self.assertEqual(1.0, broker.cur_trade.quantity)
        self.assertEqual(10, broker.cur_trade.open_price)

        self.assertEqual(1, broker.cur_trade.stop_loss_order_id)
        self.assertEqual(9, broker.cur_trade.stop_loss_price)
        self.assertEqual(11, broker.cur_trade.take_profit_price)


if __name__ == '__main__':
    unittest.main()
