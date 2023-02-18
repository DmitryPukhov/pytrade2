from unittest import TestCase

from feed.BinanceWebsocketFeed import BinanceWebsocketFeed


class TestBinanceWebsocketFeed(TestCase):

    def test_raw2model(self):

        raw = {'u': 26909985931, 's': 'BTCUSDT', 'b': '21401.86000000', 'B': '0.08000000', 'a': '21401.99000000', 'A': '0.00940000'}
        converted = BinanceWebsocketFeed(["BTCUSDT"]).rawbidask2model(raw)
        expected_bid = list(filter(lambda i: "bid" in i, converted))[0]
        expected_ask = list(filter(lambda i: "ask" in i, converted))[0]

        self.assertEqual(expected_bid["symbol"], raw["s"])
        self.assertEqual(expected_bid["bid"], raw["b"])
        self.assertEqual(expected_bid["bid_vol"], raw["B"])
        self.assertIsNotNone(expected_bid["datetime"])
        self.assertEqual(expected_bid["symbol"], raw["s"])
        self.assertEqual(expected_ask["ask"], raw["a"])
        self.assertEqual(expected_ask["ask_vol"], raw["A"])
        self.assertIsNotNone(expected_ask["datetime"])

