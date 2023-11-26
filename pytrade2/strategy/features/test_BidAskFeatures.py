from datetime import datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from strategy.features.BidAskFeatures import BidAskFeatures


class TestBidAskFeatures(TestCase):
    def test_bid_ask_features_of(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "symbol": "asset1",
             "bid": 2, "bid_vol": 4, "ask": 6, "ask_vol": 8}
        ]).set_index("datetime", drop=False)
        # Call
        actual_features = BidAskFeatures.bid_ask_features_of(bid_ask, "1s").fillna(-1)
        #self.assertEqual([3, 6], actual_features["ask"].values.tolist())
        self.assertEqual([-1, 1.0], actual_features["bid_diff"].values.tolist())
        self.assertEqual([-1, 2.0], actual_features["bid_vol_diff"].values.tolist())
        self.assertEqual([-1, 3.0], actual_features["ask_diff"].values.tolist())
        self.assertEqual([-1, 4.0], actual_features["ask_vol_diff"].values.tolist())
        self.assertEqual([2, 4], actual_features["spread"].values.tolist())

    def test_time_features_of(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-04-01 01:02:03")},
            {"datetime": datetime.fromisoformat("2023-04-02 02:03:06")}
        ]).set_index("datetime", drop=False)
        actual_features = BidAskFeatures.time_features_of(bid_ask)
        self.assertEqual([1,2], actual_features["time_hour"].values.tolist())
        self.assertEqual([2,3], actual_features["time_minute"].values.tolist())
        self.assertEqual([5,6], actual_features["time_day_of_week"].values.tolist())
        self.assertEqual(bid_ask["datetime"].diff().dropna().values.tolist(), actual_features["time_diff"].dropna().values.tolist())
