from datetime import datetime
import unittest
from typing import Dict
from unittest import mock
from unittest.mock import Mock, MagicMock

from huobi.client.account import AccountClient
from huobi.client.algo import AlgoClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderState, OrderType
from huobi.model.trade import Order, OrderUpdateEvent, OrderUpdate

from exch.huobi.spot.broker.AccountManagerSpot import AccountManagerSpot
from exch.huobi.spot.broker.HuobiBrokerSpot import HuobiBrokerSpot
from exch.BrokerSpotBase import BrokerSpotBase


class TestHuobiBrokerSpot(unittest.TestCase):
    config: Dict[str, any] = {"pytrade2.broker.trade.allow": True,
                              "pytrade2.exchange.huobi.connector.key": "123",
                              "pytrade2.exchange.huobi.connector.secret": "456",
                              "pytrade2.exchange.huobi.connector.url": None,
                              "pytrade2.broker.huobi.account.id": 1,
                              "pytrade2.tickers": "btcusdt",
                              "pytrade2.price.precision": 2,
                              "pytrade2.amount.precision": 2,
                              "pytrade2.data.dir": ""
                              }

    @staticmethod
    def __broker__init_db__(self, cfg):
        self.db_session = MagicMock()

    def setUp(self) -> None:
        mock.patch.object(BrokerSpotBase, '__init_db__', self.__broker__init_db__).start()
        mock.patch.object(BrokerSpotBase, "read_last_opened_trade", lambda self: None).start()
        mock.patch.object(HuobiBrokerSpot, "_sub_events", lambda self, ticker: None).start()
        mock.patch.object(TradeClient, "sub_order_update", Mock()).start()
        mock.patch.object(MarketClient, "sub_trade_detail", Mock()).start()

        mock.patch.object(AccountManagerSpot, "__init__", Mock(return_value=None)).start()


    def test_create_cur_trade__main_order_not_filled(self):

        broker = HuobiBrokerSpot(config=TestHuobiBrokerSpot.config, market_client=MagicMock(),
                                 account_client=MagicMock(), trade_client=MagicMock(), algo_client=MagicMock())

        # Test call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Assert
        broker.trade_client.create_order.assert_called_once()  # Single attempt and nothing to cancel
        self.assertIsNone(broker.cur_trade)

    def test_create_cur_trade__main_order_error(self):
        # Prepare mocks
        def create_order_crash(selfsymbol: 'str', account_id: 'int', order_type: 'OrderType', amount: 'float',
                               price: 'float', source: 'str', client_order_id=None, stop_price=None,
                               operator=None) -> int:
            """ Main order not created on Huobi exchange"""
            raise Exception()

        trade_client = MagicMock()
        mock.patch.object(trade_client, "create_order", Mock(create_order_crash)).start()

        broker = HuobiBrokerSpot(config=TestHuobiBrokerSpot.config, market_client=MagicMock(),
                                 account_client=MagicMock(), trade_client=trade_client, algo_client=MagicMock())

        # Test call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Assert
        broker.trade_client.create_order.assert_called_once()  # Single attempt and nothing to cancel
        self.assertIsNone(broker.cur_trade)

    def test_create_cur_trade__sl_tp_error(self):
        # Mocks
        def create_order_wrap(symbol,
                              account_id,
                              order_type,
                              amount,
                              price,
                              source,
                              stop_price=None,
                              operator=None
                              ):

            if order_type == OrderType.BUY_LIMIT_FOK:
                # Main order
                return 1
            elif order_type == OrderType.SELL_STOP_LIMIT:
                # Sl order, problems happened
                raise Exception("Sl/tp not created")
            elif order_type == OrderType.SELL_LIMIT:
                # Order to close main
                return 3

        def get_order_wrap(order_id: int) -> Order:
            order = Order()
            order.finished_at = datetime.utcnow().timestamp() * 1000
            order.orderId = order_id
            order.created_at = datetime.utcnow().timestamp() * 1000
            order.price = 10
            order.amount = 1
            order.state = OrderState.FILLED
            return order

        mock.patch.object(TradeClient, "create_order", Mock(wraps=create_order_wrap)).start()
        mock.patch.object(TradeClient, "cancel_order", Mock()).start()
        mock.patch.object(TradeClient, "get_order", Mock(wraps=get_order_wrap)).start()
        mock.patch.object(AlgoClient, "create_order", Mock(wraps=create_order_wrap)).start()
        mock.patch.object(AlgoClient, "get_order", Mock(wraps=get_order_wrap)).start()

        # Class under test
        broker = HuobiBrokerSpot(config=TestHuobiBrokerSpot.config, market_client=MagicMock(),
                                 account_client=MagicMock(), trade_client=TradeClient(), algo_client=AlgoClient())

        # Call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Receive closing order filled event
        oa = OrderUpdate()
        oa.orderId = broker.cur_trade.close_order_id
        oa.orderStatus = OrderState.FILLED
        oa.tradePrice = broker.cur_trade.open_price
        oa.tradeTime = datetime(2023, 6, 9, 11, 44).timestamp() / 1000.0
        oae = OrderUpdateEvent()
        oae.data = oa
        broker.on_order_update(oae)

        # Asserts
        self.assertIsNone(broker.cur_trade)
        broker.trade_client.create_order.assert_called()  # Main order attempt
        broker.trade_client.get_order.assert_called()  # Main order attempt

    def test_create_cur_trade__sl_tp_close_error(self):
        # Mocks
        def create_order_wrap(symbol,
                              account_id,
                              order_type,
                              amount,
                              price,
                              source,
                              stop_price=None,
                              operator=None
                              ):

            if order_type == OrderType.BUY_LIMIT_FOK:
                # Main order
                return 1
            elif order_type == OrderType.SELL_STOP_LIMIT:
                # Sl order, problems happened
                raise Exception("Sl/tp not created")
            elif order_type == OrderType.SELL_MARKET:
                # Close error, main trade left unclosed
                raise Exception("Close order error")

        def get_order_wrap(order_id: int) -> Order:
            order = Order()
            order.finished_at = datetime.utcnow().timestamp() * 1000
            order.orderId = order_id
            order.created_at = datetime.utcnow().timestamp() * 1000
            order.price = 10
            order.amount = 1
            order.state = OrderState.FILLED
            return order

        mock.patch.object(TradeClient, "create_order", Mock(wraps=create_order_wrap)).start()
        mock.patch.object(TradeClient, "cancel_order", Mock()).start()
        mock.patch.object(TradeClient, "get_order", Mock(wraps=get_order_wrap)).start()
        mock.patch.object(AlgoClient, "create_order", Mock(wraps=create_order_wrap)).start()
        mock.patch.object(AlgoClient, "get_order", Mock(wraps=get_order_wrap)).start()

        # Class under test
        broker = HuobiBrokerSpot(config=TestHuobiBrokerSpot.config, market_client=MagicMock(),
                                 account_client=MagicMock(), trade_client=TradeClient(), algo_client=AlgoClient())

        # Call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)
        self.assertIsNotNone(broker.cur_trade)
        broker.trade_client.create_order.assert_called()  # Main order attempt
        broker.trade_client.get_order.assert_called()  # Main order attempt

    def test_create_cur_trade__success(self):
        # Mocks
        def create_order_wrap(symbol,
                              account_id,
                              order_type,
                              amount,
                              price,
                              source,
                              stop_price=None,
                              operator=None
                              ):
            # Main order id =1, sl order id = 2
            return 1 if order_type in {OrderType.BUY_LIMIT_FOK, OrderType.BUY_LIMIT_FOK} else 2

        def get_order_wrap(order_id: int) -> Order:
            order = Order()
            order.orderId = str(order_id)
            order.created_at = datetime.utcnow().timestamp() * 1000
            order.price = 10
            order.amount = 1
            order.state = OrderState.FILLED
            return order

        mock.patch.object(TradeClient, "create_order", Mock(wraps=create_order_wrap)).start()
        mock.patch.object(TradeClient, "get_order", Mock(wraps=get_order_wrap)).start()

        # Class under test
        broker = HuobiBrokerSpot(config=TestHuobiBrokerSpot.config, market_client=MarketClient(),
                                 account_client=AccountClient(), trade_client=TradeClient(), algo_client=AlgoClient())

        # Call
        broker.create_cur_trade(symbol="BTCUSDT",
                                direction=1,
                                quantity=1.0,
                                price=10,
                                stop_loss_price=9,
                                take_profit_price=11)

        # Assert
        self.assertEqual("BUY", broker.cur_trade.side)
        self.assertEqual('1', broker.cur_trade.open_order_id)
        self.assertEqual(1.0, broker.cur_trade.quantity)
        self.assertEqual(10, broker.cur_trade.open_price)

        self.assertEqual('2', broker.cur_trade.stop_loss_order_id)
        self.assertEqual(9, broker.cur_trade.stop_loss_price)
        self.assertEqual(11, broker.cur_trade.take_profit_price)


if __name__ == '__main__':
    unittest.main()
