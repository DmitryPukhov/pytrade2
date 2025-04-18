from datetime import datetime
from unittest import TestCase

import pandas as pd

from features.LowHighTargets import LowHighTargets


class TestLowHighTargets(TestCase):
    def test_fut_lohi(self):
        candles = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2024-02-07 20:01'), 'close': 0, 'low': -1, 'high': 1},
            {'datetime': datetime.fromisoformat('2024-02-07 20:02'), 'close': 0, 'low': -2, 'high': 2},
            {'datetime': datetime.fromisoformat('2024-02-07 20:03'), 'close': 0, 'low': -4, 'high': 4},
            {'datetime': datetime.fromisoformat('2024-02-07 20:04'), 'close': 0, 'low': -7, 'high': 7},
        ]).set_index('datetime')

        actual = LowHighTargets.fut_lohi(candles, '1min')
        self.assertSequenceEqual(candles.index[:-1].tolist(), actual.index.tolist())
        self.assertSequenceEqual([-2, -4, -7], actual['fut_low_diff'].tolist())
        self.assertSequenceEqual([2, 4, 7], actual['fut_high_diff'].tolist())
