from unittest import TestCase
from datetime import datetime, timedelta
import pandas as pd

from strategy.features.MultiIndiFeatures import MultiIndiFeatures


class TestMultiIndiFeatures(TestCase):
    def test_multi_indi_features_last_same_candle_should_not_produce_empty_features(self):
        candles = map(
            lambda x: {"datetime": datetime(year=2024, month=9, day=10) + timedelta(minutes=x), "open": 0, "high": 0, "low": 0,
                       "close": 0, "vol":0}, range(60))
        candles = pd.DataFrame(candles).set_index("datetime")

        actual =  MultiIndiFeatures.multi_indi_features({"1min": candles})
        self.assertFalse(actual.empty)
