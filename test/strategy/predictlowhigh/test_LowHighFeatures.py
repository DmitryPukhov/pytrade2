from datetime import datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from strategy.predictlowhigh.LowHighFeatures import LowHighFeatures


class TestLowHighFeatures(TestCase):

    def test_low_high_transformed(self):
        # prepare the data
        candles = pd.DataFrame([
            # 7:00-7:05
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:01'), 'ticker': 'asset1', 'open': 2, 'low': 1,
             'high': 11, 'close': 5},
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:10'), 'ticker': 'asset1', 'open': 6, 'low': 4,
             'high': 8, 'close': 7}
        ]).set_index('close_time')

        # Call
        features = LowHighFeatures().low_high_diff(candles)
        np.testing.assert_equal([np.nan, 1.0], features['open'].values.tolist())
        np.testing.assert_equal([np.nan, -1.0], features['low'].values.tolist())
        np.testing.assert_equal([np.nan, 3.0], features['high'].values.tolist())
        np.testing.assert_equal([np.nan, 2.0], features['close'].values.tolist())

    def test_low_high_past__(self):
        candles = pd.DataFrame([
            # 7:00-7:05
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:01'), 'ticker': 'asset1', 'open': 2, 'low': 1,
             'high': 11, 'close': 3},
            {'close_time': datetime.fromisoformat('2021-12-08 07:00:10'), 'ticker': 'asset1', 'open': 2, 'low': 1,
             'high': 11, 'close': 3},
            {'close_time': datetime.fromisoformat('2021-12-08 07:01:00'), 'ticker': 'asset1', 'open': 2, 'low': 2,
             'high': 22, 'close': 3},
            # 7:05-7:10
            {'close_time': datetime.fromisoformat('2021-12-08 07:01:59'), 'ticker': 'asset1', 'open': 2, 'low': 3,
             'high': 33, 'close': 3},
            {'close_time': datetime.fromisoformat('2021-12-08 07:02:05'), 'ticker': 'asset1', 'open': 2, 'low': 2,
             'high': 22, 'close': 3},
            {'close_time': datetime.fromisoformat('2021-12-08 07:03:00'), 'ticker': 'asset1', 'open': 2, 'low': 1,
             'high': 11, 'close': 3},
        ]).set_index('close_time')

        # Call
        features = LowHighFeatures.low_high_past(candles, 3)

        # Assert
        # Aggregated -1 minute min/max
        self.assertEqual([11.0, 11.0, 22.0, 33.0, 33.0, 22.0], features['-1*1min_high'].values.tolist())
        self.assertEqual([1.0, 1.0, 1.0, 2.0, 2.0, 1.0], features['-1*1min_low'].values.tolist())
        # Aggregated -2
        self.assertEqual([11.0, 11.0, 11.0, 22.0, 22.0, 33.0], features['-2*1min_high'].values.tolist())
        self.assertEqual([1.0, 1.0, 1.0, 1.0, 1.0, 2.0], features['-2*1min_low'].values.tolist())
        # Aggregated -3
        self.assertEqual([-1, -1, -1, 11.0, 11.0, 22.0], features['-3*1min_high'].fillna(-1).tolist())
        self.assertEqual([-1, -1, -1, 1, 1, 1], features['-3*1min_low'].fillna(-1).tolist())
