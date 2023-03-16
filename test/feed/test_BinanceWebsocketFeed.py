from unittest import TestCase

import pandas as pd

from feed.BinanceWebsocketFeed import BinanceWebsocketFeed


class TestBinanceWebsocketFeed(TestCase):

    def test_rawlevel2model(self):

        msg = {'e': 'depthUpdate', 'E': 1678981150789, 's': 'BTCUSDT', 'U': 35239161237, 'u': 35239162829,
               'b': [['1.01', '1.1'], ['1.02', '1.2']],
               'a': [['2.01', '2.1'], ['2.02', '2.2']]}

        actual = BinanceWebsocketFeed(["BTCUSDT"]).rawlevel2model(msg=msg)
        self.assertListEqual([1.01, 1.02], sorted([a["bid"] for a in actual if "bid" in a]))
        self.assertListEqual([1.1, 1.2], sorted([a["bid_vol"] for a in actual if "bid_vol" in a]))
        self.assertListEqual([2.01, 2.02], sorted([a["ask"] for a in actual if "ask" in a]))
        self.assertListEqual([2.1, 2.2], sorted([a["ask_vol"] for a in actual if "ask_vol" in a]))
        self.assertListEqual(["BTCUSDT"]*4, [a["symbol"] for a in actual])



    def test_rawbidask2model(self):
        raw = {'u': 26909985931, 's': 'BTCUSDT', 'b': 21401.86000000, 'B': 0.08000000, 'a': 21401.99000000,
               'A': 0.00940000}
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
