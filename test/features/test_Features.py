from datetime import datetime
from unittest import TestCase
import pandas as pd

from features.Features import Features


class TestFeatures(TestCase):

    def test_features__(self):
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
        features = Features().low_high_past(candles, 1, 'min', 3)

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
