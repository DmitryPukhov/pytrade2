from datetime import datetime
from unittest import TestCase

import pandas as pd

from features.level2.Level2Indicators import Level2Indicators


class TestLevel2Indicators(TestCase):
    def test_expectation(self):
        # Math expectation = sum(price * vol) / sum(vol)
        dt = datetime.fromisoformat('2021-11-26 17:39:00')
        level2_data = pd.DataFrame([
            {"datetime": dt, "ask": 4, "ask_vol": 2},
            {"datetime": dt, "bid": 2, "bid_vol": 2}
        ])
        expectations = Level2Indicators().expectation(level2_data)

        self.assertEqual([3], expectations["l2_expectation"].tolist())

    def test_volume(self):
        # Order book1 for time1 bids and acks
        asks1 = [{'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'ask': i, 'ask_vol': 1,
                  'bid_vol': None} for i in range(10, 20)]
        bids1 = [
            {'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:39:00'), 'bid': i, 'ask_vol': None,
             'bid_vol': 10} for i in range(0, 10)]

        # Order book2 for time1 bids and acks
        asks2 = [{'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:40:00'), 'ask': i, 'ask_vol': 2,
                  'bid_vol': None} for i in range(10, 20)]
        bids2 = [
            {'asset': 'asset1', 'datetime': datetime.fromisoformat('2021-11-26 17:40:00'), 'bid': i, 'ask_vol': None,
             'bid_vol': 20} for i in range(0, 10)]

        # Order book 1 and 2 for  time1 and time2
        level2_data = pd.DataFrame(asks1 + bids1 + asks2 + bids2)

        # Call
        volumes_df = Level2Indicators().volume(level2_data)

        # datetime should be the same as source
        self.assertEqual(level2_data["datetime"].unique().tolist(), volumes_df.index.tolist())

        # bid+ask check
        volumes = volumes_df["level2_vol"].values.tolist()
        self.assertListEqual([110, 220], volumes)

        # bid check
        bid_volumes = volumes_df["bid_vol"].values.tolist()
        self.assertListEqual([100, 200], bid_volumes)

        # ask check
        ask_volumes = volumes_df["ask_vol"].values.tolist()
        self.assertListEqual([10, 20], ask_volumes)
