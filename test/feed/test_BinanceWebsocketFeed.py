from collections import defaultdict
from datetime import datetime
from unittest import TestCase
from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
import pandas as pd


class TestBinanceWebsocketFeed(TestCase):

    def test_raw2model(self):
        raw = {'u': 26909985931, 's': 'BTCUSDT', 'b': '21401.86000000', 'B': '0.08000000', 'a': '21401.99000000', 'A': '0.00940000'}
        expected = BinanceWebsocketFeed([]).raw2model(raw)

        self.assertEqual(expected["symbol"], raw["s"])
        self.assertEqual(expected["bid"], raw["b"])
        self.assertEqual(expected["bid_qty"], raw["B"])
        self.assertEqual(expected["ask"], raw["a"])
        self.assertEqual(expected["ask_qty"], raw["A"])
        self.assertIsNotNone(expected["datetime"])
