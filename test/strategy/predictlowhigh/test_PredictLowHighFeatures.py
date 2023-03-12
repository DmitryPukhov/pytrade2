from datetime import datetime
import pandas as pd
from unittest import TestCase

from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class TestPredictLowHighFeatures(TestCase):
    def test_targets_of(self):
        df = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:00"), "symbol": "asset1",
             "bid": 0, "bid_vol": 2, "ask": 200, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 201, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:02"), "symbol": "asset1",
             "bid": 2, "bid_vol": 6, "ask": 202, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:03"), "symbol": "asset1",
             "bid": 3, "bid_vol": 6, "ask": 203, "ask_vol": 8},

            {"datetime": datetime.fromisoformat("2021-12-08 07:00:11"), "symbol": "asset1",
             "bid": 11, "bid_vol": 10, "ask": 211, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:12"), "symbol": "asset1",
             "bid": 12, "bid_vol": 10, "ask": 212, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:13"), "symbol": "asset1",
             "bid": 13, "bid_vol": 10, "ask": 213, "ask_vol": 12}

        ]).set_index("datetime")
        actual = PredictLowHighFeatures().targets_of(df, predict_window="10s")

        # Future values should be predicted only if future window completed
        self.assertListEqual(actual["bid_fut"].values.tolist(), [3,11,12,13,None,None,None])

    def test_features_of(self):
        df = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10, "ask": 11, "ask_vol": 12}
        ]).set_index("datetime", drop=False)

        # Call
        actual, _ = PredictLowHighFeatures.features_targets_of(df)

        self.assertListEqual([1,5,9], actual["bid"].values.tolist())
        self.assertListEqual([2,6,10], actual["bid_vol"].values.tolist())
        self.assertListEqual([3,7,11], actual["ask"].values.tolist())
        self.assertListEqual([4,8,12], actual["ask_vol"].values.tolist())
        pass
