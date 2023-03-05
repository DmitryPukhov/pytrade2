from collections import defaultdict
from datetime import datetime
from unittest import TestCase

import pandas as pd
from strategy.predictlowhighcandles.PredictLowHighCandlesStrategy import PredictLowHighCandlesStrategy


class TestPredictLowHighCandlesStrategy(TestCase):

    @staticmethod
    def new_strategy(candles: pd.DataFrame):
        conf = defaultdict(str, {"biml.tickers": "BTCUSDT",
                                 "biml.feed.BTCUSDT.candle.intervals": "1m,15m,1h,1d",
                                 "biml.feed.BTCUSDT.candle.limits": "1440,96,24,30"})
        strategy = PredictLowHighCandlesStrategy(broker=None, config=conf)
        strategy.profit_loss_ratio = 4
        strategy.min_stop_loss_ratio = 0.01
        strategy.candles = candles
        return strategy

    def test_last_signal_none(self):
        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 139}])) \
            .open_signal()
        self.assertEqual(0, signal, "expected 0 signal for profit/loss ratio < 4")
        self.assertEqual(None, price)
        self.assertEqual(None, stop_loss)

        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 61, "fut_high": 110}])) \
            .open_signal()
        self.assertEqual(0, signal, "expected 0 signal for profit/loss ratio < 4")
        self.assertEqual(None, price)
        self.assertEqual(None, stop_loss)

    def test_multi_candles_last_signal_sell(self):
        candles = pd.DataFrame([
            # Last candle - sell signal
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:02'),
             "close": 100,
             "fut_low": 60,
             "fut_high": 110},
            # Prev candle - 0 signal
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:01'),
             "close": 100,
             "fut_low": 100,
             "fut_high": 100},
        ]) \
            .set_index("close_time")
        candles.sort_index(inplace=True)

        signal, price, stop_loss = TestPredictLowHighCandlesStrategy.new_strategy(candles).open_signal()
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)

    def test_last_signal_buy(self):
        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 140}])) \
            .open_signal()
        self.assertEqual(1, signal, "expected 1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(90, stop_loss)

        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 141}])) \
            .open_signal()
        self.assertEqual(1, signal, "expected 1 signal for profit/loss ratio >= 4")
        self.assertEqual(100, price)
        self.assertEqual(90, stop_loss)

    def test_multi_candles_last_signal_buy(self):
        candles = pd.DataFrame([
            # Last candle - buy signal
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:02'),
             "close": 100,
             "fut_low": 60,
             "fut_high": 110},
            # Prev candle - 0 signal
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:01'),
             "close": 100,
             "fut_low": 100,
             "fut_high": 100},
        ]) \
            .set_index("close_time")
        candles.sort_index(inplace=True)

        signal, price, stop_loss = TestPredictLowHighCandlesStrategy.new_strategy(candles).open_signal()
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)

    def test_last_signal_sell(self):
        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 60, "fut_high": 110}])) \
            .open_signal()
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)

        signal, price, stop_loss = TestPredictLowHighCandlesStrategy \
            .new_strategy(pd.DataFrame([{"close": 100, "fut_low": 59, "fut_high": 110}])).open_signal()
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio >= 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)
