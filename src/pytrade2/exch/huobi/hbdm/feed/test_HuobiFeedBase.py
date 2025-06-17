from unittest import TestCase

from pytrade2.exch.huobi.hbdm.feed.HuobiFeedBase import HuobiFeedBase


class TestHuobiFeedBase(TestCase):
    def test_ticker_of_ch(self):
        self.assertEqual("BTC-USDT", HuobiFeedBase.ticker_of_ch("market.BTC-USDT.bbo"))
        self.assertEqual("BTC-USDT", HuobiFeedBase.ticker_of_ch("market.BTC-USDT.depth.step0"))

    def test_interval_of_ch(self):
        self.assertEqual("1min", HuobiFeedBase.period_of_ch("market.BTC-USDT.kline.1min"))
        self.assertEqual("15min", HuobiFeedBase.period_of_ch("market.BTC-USDT.kline.15min"))
