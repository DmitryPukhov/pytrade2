from datetime import datetime
from unittest import TestCase

import pandas as pd

from features.LowHighTargets import LowHighTargets


class TestLowHighTargets(TestCase):
    def test_fut_lohi_signal_target_buy(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 99.1, 'high': 104}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([1,0],actual["signal"].tolist())

    def test_fut_lohi_signal_small_profit_not_buy(self):
        # Profit is too small
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 99.1, 'high': 103.9}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([0,0],actual["signal"].tolist())

    def test_fut_lohi_signal_target_big_loss_not_buy(self):
        # Los is too big
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 99, 'high': 104}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([0,0],actual["signal"].tolist())

    def test_fut_lohi_signal_target_sell(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 96, 'high': 100.9}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([-1,0],actual["signal"].tolist())

    def test_fut_lohi_signal_target_small_profit_not_sell(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 96.1, 'high': 100.9}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([0,0],actual["signal"].tolist())


    def test_fut_lohi_signal_target_big_loss_not_sell(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 100, 'low': 1000, 'high': -1000},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 100, 'low': 96, 'high': 101}
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi_signal(candles, '1min', 0.01, 4)
        self.assertSequenceEqual(candles.index.tolist(), actual.index.tolist())
        self.assertSequenceEqual([0,0],actual["signal"].tolist())

    def test_fut_lohi(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 0, 'low': -1, 'high': 1},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 0, 'low': -2, 'high': 2},
            {'datetime': datetime.fromisoformat('2024-02-07 20:03'), 'close': 0, 'low': -4, 'high': 4},
            {'datetime': datetime.fromisoformat('2024-02-07 20:04'), 'close': 0, 'low': -7, 'high': 7},
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi(candles, '1min')
        self.assertSequenceEqual(candles.index[:-1].tolist(), actual.index.tolist())
        self.assertSequenceEqual([-2, -4, -7], actual['fut_low_delta'].tolist())
        self.assertSequenceEqual([2, 4, 7], actual['fut_high_delta'].tolist())
