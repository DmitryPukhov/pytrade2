from datetime import datetime
from unittest import TestCase

import pandas as pd

from features.TargetFeatures import TargetFeatures


class TestTargetFeatures(TestCase):

    def test_target_features__window_should_include_current(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame([{'close_time': datetime.fromisoformat('2021-12-08 07:00:00'),  'low': 1,
                                'high': 10},
                               {'close_time': datetime.fromisoformat('2021-12-08 07:01:01'),  'low': 4,
                                'high': 6}
                               ]).set_index('close_time')

        withminmax = TargetFeatures().low_high_future(candles, 1, 'min')
        self.assertEqual([1, 4], withminmax['fut_low_min'].values.tolist())
        self.assertEqual([10, 6], withminmax['fut_high_max'].values.tolist())

    def test_target_features__window_should_include_right_bound(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame([{'close_time': datetime.fromisoformat('2021-12-08 07:00:00'),  'low': 4,
                                'high': 6},
                               {'close_time': datetime.fromisoformat('2021-12-08 07:00:59'),  'low': 1,
                                'high': 10}
                               ]).set_index('close_time')

        withminmax = TargetFeatures().low_high_future(candles, 1, 'min')
        self.assertEqual(withminmax['fut_low_min'].values.tolist(), [1, 1] )
        self.assertEqual(withminmax['fut_high_max'].values.tolist(),[10,10])

    def test_target_features__(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame([
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:00'),  'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:30'),  'low': 1, 'high': 10},
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:59'),  'low': 3, 'high': 7},
            {'close_time': datetime.fromisoformat('2021-11-26 17:01:58'),  'low': 2, 'high': 8},

            {'close_time': datetime.fromisoformat('2021-11-26 17:03:00'),  'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:04:00'),  'low': 3, 'high': 7},
            {'close_time': datetime.fromisoformat('2021-11-26 17:05:00'),  'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:06:00'),  'low': 3, 'high': 7}
        ]).set_index('close_time')

        withminmax = TargetFeatures().low_high_future(candles, 1, 'min')

        self.assertEqual([1, 1, 2, 2, 4, 3, 4, 3], withminmax['fut_low_min'].values.tolist())
        self.assertEqual([10, 10, 8, 8, 6, 7, 6, 7], withminmax['fut_high_max'].values.tolist())
