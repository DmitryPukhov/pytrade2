from collections import defaultdict
from datetime import datetime
from unittest import TestCase

import pandas as pd
from strategy.predictlowhighcandles.PredictLowHighCandlesStrategy import PredictLowHighCandlesStrategy


class TestPredictLowHighStrategy(TestCase):

    @staticmethod
    def new_strategy():
        conf = defaultdict(str, {"biml.tickers": "BTCUSDT",
                                 "biml.feed.BTCUSDT.candle.intervals": "1m,15m,1h,1d",
                                 "biml.feed.BTCUSDT.candle.limits": "1440,96,24,30"})
        strategy = PredictLowHighCandlesStrategy(broker=None, config=conf)
        strategy.profit_loss_ratio = 4
        strategy.min_stop_loss_ratio = 0.01
        return strategy

    def test_last_signal_none(self):
        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 139}]))
        self.assertEqual(0, signal, "expected 0 signal for profit/loss ratio < 4")
        self.assertEqual(None, price)
        self.assertEqual(None, stop_loss)
        self.assertEqual(None, take_profit)

        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 61, "fut_high": 110}]))
        self.assertEqual(0, signal, "expected 0 signal for profit/loss ratio < 4")
        self.assertEqual(None, price)
        self.assertEqual(None, stop_loss)
        self.assertEqual(None, take_profit)

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

        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(candles)
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)
        self.assertEqual(60, take_profit)

    def test_last_signal_buy(self):
        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 140}]))
        self.assertEqual(1, signal, "expected 1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(90, stop_loss)
        self.assertEqual(140, take_profit)

        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 90, "fut_high": 141}]))
        self.assertEqual(1, signal, "expected 1 signal for profit/loss ratio >= 4")
        self.assertEqual(100, price)
        self.assertEqual(90, stop_loss)
        self.assertEqual(141, take_profit)

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

        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(candles)
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)
        self.assertEqual(60, take_profit)

    def test_last_signal_sell(self):
        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 60, "fut_high": 110}]))
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio == 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)
        self.assertEqual(60, take_profit)

        signal, price, stop_loss, take_profit = TestPredictLowHighStrategy.new_strategy().last_signal(
            pd.DataFrame([{"close": 100, "fut_low": 59, "fut_high": 110}]))
        self.assertEqual(-1, signal, "expected -1 signal for profit/loss ratio >= 4")
        self.assertEqual(100, price)
        self.assertEqual(110, stop_loss)
        self.assertEqual(59, take_profit)
