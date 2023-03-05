from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class TestPredictLowHighFeatures(TestCase):
    def test_targets_of(self):
        df = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_qty": 2, "ask": 3, "ask_qty": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:02"), "symbol": "asset1",
             "bid": 5, "bid_qty": 6, "ask": 7, "ask_qty": 8},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "bid": 9, "bid_qty": 10, "ask": 11, "ask_qty": 12}
        ]).set_index("datetime")
        actual = PredictLowHighFeatures().targets_of(df)
        self.assertEqual(actual[["bid_fut","ask_fut"]].values(), [5,3])

    def test_features_of(self):
        df = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_qty": 2, "ask": 3, "ask_qty": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:02"), "symbol": "asset1",
             "bid": 5, "bid_qty": 6, "ask": 7, "ask_qty": 8},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "bid": 9, "bid_qty": 10, "ask": 11, "ask_qty": 12}
        ]).set_index("datetime", drop=False)

        # Call
        actual, _ = PredictLowHighFeatures.features_targets_of(df)

        self.assertListEqual([1, 1, 1, 1, 1, 1, 9], actual["bid"].values.tolist())
        self.assertListEqual([8, 0, 0, 0, 0, 0, 10], actual["bid_qty"].values.tolist())
        self.assertListEqual([7, 7, 7, 7, 7, 7, 11], actual["ask"].values.tolist())
        self.assertListEqual([12, 0, 0, 0, 0, 0, 12], actual["ask_qty"].values.tolist())
        pass
