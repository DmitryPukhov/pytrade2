from unittest import TestCase
from unittest.mock import MagicMock

from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm


class TestHuobiWebSocketFeedHbdm(TestCase):

    def test_rawticker2model(self):
        msg = {'mrid': 100010776952278, 'id': 1686966700, 'bid': [26216.3, 5633], 'ask': [26216.4, 2],
               'ts': 1686966700177, 'version': 100010776952278, 'ch': 'market.BTC-USDT.bbo'}
        actual = HuobiWebSocketFeedHbdm.rawticker2model(msg)

        self.assertEqual("BTC-USDT", actual["symbol"])
        self.assertEqual(26216.3, actual["bid"])
        self.assertEqual(5633, actual["bid_vol"])
        self.assertEqual(26216.4, actual["ask"])
        self.assertEqual(2, actual["ask_vol"])

    def test_on_socket_data_ticker(self):
        # Prepare
        msg = {'ch': 'market.BTC-USDT.bbo',
               'tick': {'mrid': 100010776952278, 'id': 1686966700, 'bid': [26216.3, 5633], 'ask': [26216.4, 2],
                        'ts': 1686966700177, 'version': 100010776952278, 'ch': 'market.BTC-USDT.bbo'}}

        feed = HuobiWebSocketFeedHbdm(config={"pytrade2.tickers": "BTC-USDT"}, client=MagicMock())
        consumer = MagicMock()
        feed.consumers.append(consumer)

        # Call
        feed.on_socket_data(msg)
        consumer.on_ticker.assert_called_once()
        actual = consumer.on_ticker.call_args[0][0]
        self.assertEqual("BTC-USDT", actual["symbol"])
        self.assertEqual(26216.3, actual["bid"])
        self.assertEqual(5633, actual["bid_vol"])
        self.assertEqual(26216.4, actual["ask"])
        self.assertEqual(2, actual["ask_vol"])

    def test_on_socket_data_no_ticker(self):
        # Prepare
        msg = {'ch': 'notmarket.BTC-USDT.bbo',
               'tick': {'mrid': 100010776952278, 'id': 1686966700, 'bid': [26216.3, 5633], 'ask': [26216.4, 2],
                        'ts': 1686966700177, 'version': 100010776952278, 'ch': 'market.BTC-USDT.bbo'}}

        feed = HuobiWebSocketFeedHbdm(config={"pytrade2.tickers": "BTC-USDT"}, client=MagicMock())
        consumer = MagicMock()
        feed.consumers.append(consumer)

        # Call
        feed.on_socket_data(msg)
        consumer.on_ticker.assert_not_called()
