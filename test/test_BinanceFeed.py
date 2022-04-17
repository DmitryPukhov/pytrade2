from unittest import TestCase

from biml.feed.BinanceFeed import BinanceFeed


class TestBinanceFeed(TestCase):
    def test_read(self):
        # Raw data from binance
        fast = [
            [1650189960000, '38633.76000000', '38806.49000000', '38593.09000000', '38806.49000000', '0.15386700',
             1650190019999, '5956.46023199', 86, '0.11014100', '4264.84945024', '0'],
            [1650190080000, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000', '0.10214000',
             1650190139999, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']]
        medium = [
            [1650189960000, '38633.76000000', '38806.49000000', '38593.09000000', '38806.49000000', '0.15386700',
             1650190019999, '5956.46023199', 86, '0.11014100', '4264.84945024', '0']]

        # Read raw data to feed
        feed = BinanceFeed(spot_client=None, ticker=None)
        feed.read_raw(fast, medium)

        # Assert fast candles have been read
        assert len(feed.candles_fast) == len(fast)
        assert feed.candles_fast.columns.tolist() == feed.candle_columns

        # Assert medium candles have been read
        assert len(feed.candles_medium) == len(medium)
        assert feed.candles_medium.columns.tolist() == feed.candle_columns
