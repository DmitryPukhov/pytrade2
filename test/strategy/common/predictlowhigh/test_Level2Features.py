from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.common.predictlowhigh.Level2Features import Level2Features


class TestLevel2Features(TestCase):

    def test_level2_features__equal_buckets(self):
        # datetime, price, ask_vol, bid_vol
        # Each bucket has one item
        # todo: set index as in feed
        asks = [{'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'ask': i, 'ask_vol': 1,
                 'bid_vol': None} for i in range(10, 20)]
        bids = [
            {'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'bid': i, 'ask_vol': None,
             'bid_vol': 1} for i in range(0, 10)]
        data = pd.DataFrame(asks+bids)
        # Call
        buckets = Level2Features().level2_buckets(data)
        features = buckets.values.tolist()

        # Assert all features should be 1.0
        self.assertEqual([[1.0] * 20], features)

    def test_level2_absent_levels(self):
        # datetime, price, ask_vol, bid_vol
        # Not all level buckets are present
        data = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
            {'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1}
        ])

        features = Level2Features().level2_buckets(data, l2size=20, buckets=20)
        lst = features.values.tolist()

        # All features should be 1.0
        self.assertEqual([[0, 0, 0, 0, 0, 0, 0, 0, 0, 2.0, 2.0, 0, 0, 0, 0, 0, 0, 0, 0, 0]], lst)
        self.assertSequenceEqual(
            ['l2_bucket_-10', 'l2_bucket_-9', 'l2_bucket_-8', 'l2_bucket_-7',
             'l2_bucket_-6', 'l2_bucket_-5', 'l2_bucket_-4', 'l2_bucket_-3',
             'l2_bucket_-2', 'l2_bucket_-1', 'l2_bucket_0', 'l2_bucket_1',
             'l2_bucket_2', 'l2_bucket_3', 'l2_bucket_4', 'l2_bucket_5',
             'l2_bucket_6', 'l2_bucket_7', 'l2_bucket_8', 'l2_bucket_9'],
            features.columns.tolist())

