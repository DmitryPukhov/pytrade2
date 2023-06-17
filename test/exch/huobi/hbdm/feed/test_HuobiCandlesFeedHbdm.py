import datetime
from unittest import TestCase

import pandas as pd

from exch.huobi.hbdm.feed.HuobiCandlesFeedHbdm import HuobiCandlesFeedHbdm


class TestHuobiCandlesFeedHbdm(TestCase):
    def test_rawcandles2df(self):

        # Prepare 2 candles
        ts = pd.Timestamp(year=2023, month=6, day=17, hour=10, minute=19)
        raw = {'ch': 'market.BTC-USDT.kline.1min', 'ts': ts.timestamp() * 1000, 'status': 'ok',
               "data": [
                   {'id': 1686981240, 'open': 10, 'close': 40, 'high': 20, 'low': 30, 'amount': 33.826,
                    'vol': 50, 'trade_turnover': 902606.0032, 'count': 228},
                   {'id': 1686981240, 'open': 1, 'close': 4, 'high': 2, 'low': 3, 'amount': 33.826,
                    'vol': 5, 'trade_turnover': 902606.0032, 'count': 228}
               ]}
        # Call
        actual = HuobiCandlesFeedHbdm.rawcandles2df(raw)

        # Assert
        self.assertListEqual([ts - pd.Timedelta(minutes=1), ts], actual.index.tolist())
        self.assertListEqual([1, 10], actual["open"].to_list())
        self.assertListEqual([2, 20], actual["high"].to_list())
        self.assertListEqual([3, 30], actual["low"].to_list())
        self.assertListEqual([4, 40], actual["close"].to_list())
        self.assertListEqual([5, 50], actual["vol"].to_list())


    def test_rawcandle2model(self):
        # Prepare the data
        raw = {'id': 1686981240, 'open': 1, 'close': 4, 'high': 2, 'low': 3, 'amount': 33.826,
               'vol': 5, 'trade_turnover': 902606.0032, 'count': 228}
        dt = datetime.datetime(year=2023, month=6, day=17, hour=9, minute=31)

        # call
        actual = HuobiCandlesFeedHbdm.rawcandle2model(time=dt, ticker="ticker1", interval="1min", raw_candle=raw)

        # Assert
        self.assertEqual("ticker1", actual["ticker"])
        self.assertEqual("1min", actual["interval"])
        self.assertEqual(dt, actual["close_time"])
        self.assertEqual(1, actual["open"])
        self.assertEqual(2, actual["high"])
        self.assertEqual(3, actual["low"])
        self.assertEqual(4, actual["close"])
        self.assertEqual(5, actual["vol"])
