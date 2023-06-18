from unittest import TestCase
from unittest.mock import MagicMock

from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm


class TestHuobiWebSocketFeedHbdm(TestCase):

    def test_is_level2(self):
        self.assertTrue(HuobiWebSocketFeedHbdm.is_level2("market.BTC-USDT.depth.step0"))
        self.assertTrue(HuobiWebSocketFeedHbdm.is_level2("market.BTC-USDT.depth.step15"))
        self.assertFalse(HuobiWebSocketFeedHbdm.is_level2("market.BTC-USDT.bbo"))

    def test_is_bidask(self):
        self.assertTrue(HuobiWebSocketFeedHbdm.is_bidask("market.BTC-USDT.bbo"))
        self.assertFalse(HuobiWebSocketFeedHbdm.is_bidask("market.BTC-USDT.depth.step0"))

    def test_rawlevel2model(self):
        msg = {'ch': 'market.BTC-USDT.depth.step0', 'ts': 1686980108772, 'version': 1,
               'mrid': 100010780581381, 'id': 1686980108,
               'bids': [['1.01', '1.1'], ['1.02', '1.2']],
               'asks': [['2.01', '2.1'], ['2.02', '2.2']]}

        actual = HuobiWebSocketFeedHbdm.rawlevel2model(msg)

        self.assertListEqual([1.01, 1.02], sorted([a["bid"] for a in actual if "bid" in a]))
        self.assertListEqual([1.1, 1.2], sorted([a["bid_vol"] for a in actual if "bid_vol" in a]))
        self.assertListEqual([2.01, 2.02], sorted([a["ask"] for a in actual if "ask" in a]))
        self.assertListEqual([2.1, 2.2], sorted([a["ask_vol"] for a in actual if "ask_vol" in a]))
        self.assertListEqual(["BTC-USDT"]*4, [a["symbol"] for a in actual])

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
        feed.on_socket_data('market.BTC-USDT.bbo', msg)
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
        feed.on_socket_data('notmarket.BTC-USDT.bbo', msg)
        consumer.on_ticker.assert_not_called()
