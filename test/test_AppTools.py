from unittest import TestCase
from AppTools import AppTools


class TestApp(TestCase):

    def test__read_candle_tickers(self):
        # Input
        conf = {"biml.tickers": "ticker1,ticker2",
                "biml.feed.ticker1.candle.intervals": "interval1,interval2",
                "biml.feed.ticker1.candle.limits": "1,2",
                "biml.feed.ticker2.candle.intervals": "interval3,interval4",
                "biml.feed.ticker2.candle.limits": "3,4"
                }
        # Call
        tickers = list(AppTools.read_candles_tickers(conf))

        # Asserts
        self.assertEqual([t.ticker for t in tickers], ["ticker1", "ticker2"])
        self.assertEqual([t.candle_intervals for t in tickers],
                         [["interval1", "interval2"], ["interval3", "interval4"]])
        self.assertEqual([t.candle_limits for t in tickers],
                         [{"interval1": 1, "interval2": 2}, {"interval3": 3, "interval4": 4}])
