from unittest import TestCase

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
