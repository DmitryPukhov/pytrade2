from datetime import datetime
from unittest import TestCase

import pandas as pd

from strategy.features.LowHighTargets import LowHighTargets


class TestLowHighTargets(TestCase):
    def test_fut_lohi(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'low': -1, 'high': 1},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'low': -2, 'high': 2},
            {'datetime': datetime.fromisoformat('2024-02-07 20:03'), 'low': -3, 'high': 3},
            {'datetime': datetime.fromisoformat('2024-02-07 20:04'), 'low': -4, 'high': 4},
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi(candles, '1min')
        self.assertSequenceEqual(candles.index[:-1].tolist(), actual.index.tolist())
        self.assertSequenceEqual([-2, -3, -4], actual['fut_low'].tolist() )
        self.assertSequenceEqual([2, 3, 4], actual['fut_high'].tolist() )
