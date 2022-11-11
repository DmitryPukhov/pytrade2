from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class TestPredictLowHighFeatures(TestCase):
    def test_features_of(self):
        df = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 2, "bid_qty": 3, "ask": 4, "ask_qty": 5},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:02"), "symbol": "asset1",
             "bid": 2, "bid_qty": 3, "ask": 4, "ask_qty": 5},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "bid": 2, "bid_qty": 3, "ask": 4, "ask_qty": 5}
        ]).set_index("datetime")
        actual = PredictLowHighFeatures.features_of(df, 3)
        pass
