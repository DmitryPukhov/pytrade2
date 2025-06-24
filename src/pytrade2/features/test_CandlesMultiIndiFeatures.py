from datetime import datetime
from unittest import TestCase

import pandas as pd

from pytrade2.features.CandlesMultiIndiFeatures import CandlesMultiIndiFeatures


class TestCandlesMultiIndiFeatures(TestCase):
    def params(self):
        return {"1min": {"cca": {"window": 2},
                         "ichimoku": {"window1": 1, "window2": 1, "window3": 1},
                         "adx": {"window": 1},
                         "rsi": {"window": 1},
                         "stoch": {"window": 1, "smooth_window": 1},
                         "macd": {"slow": 1, "fast": 1}
                         }}

    @staticmethod
    def candles_1m_unordered():
        return {"1min": pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
             "open": 1, "high": 10, "low": 100, "close": 1000, "vol": 10000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 2, "high": 20, "low": 200, "close": 2000, "vol": 20000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 3, "high": 30, "low": 300, "close": 3000, "vol": 30000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:04:00"), "symbol": "asset1", "interval": "1m",
             "open": 4, "high": 40, "low": 400, "close": 4000, "vol": 40000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:05:00"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000},
            {"close_time": datetime.fromisoformat("2023-05-21 07:00:00"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000}]) \
            .set_index("close_time", drop=False)}

    @staticmethod
    def candles_1m_duplicates():
        return {"1min": pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 07:00:00"), "symbol": "asset1", "interval": "1m",
             "open": 1, "high": 10, "low": 100, "close": 1000, "vol": 10000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
             "open": 2, "high": 20, "low": 200, "close": 2000, "vol": 20000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 3, "high": 30, "low": 300, "close": 3000, "vol": 30000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:00:00"), "symbol": "asset1", "interval": "1m",
             "open": 4, "high": 40, "low": 400, "close": 4000, "vol": 40000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000},
            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000}]) \
            .set_index("close_time", drop=False)}
    @staticmethod
    def candles_1m_empty():
        return {"1min": pd.DataFrame(data = [], columns = ["open_time", "close_time", "open", "high", "low", "close", "vol"], index = pd.DatetimeIndex([]))}

    def test_multi_indi_features_should_handle_empty_data(self):
        features = CandlesMultiIndiFeatures.multi_indi_features(self.candles_1m_empty(), self.params())
        self.assertTrue(features.empty)


    def test_multi_indi_features_should_handle_duplicated_indices(self):
        features = CandlesMultiIndiFeatures.multi_indi_features(self.candles_1m_duplicates(), self.params())
        self.assertFalse(features.empty)

    def test_multi_indi_features_should_handle_unordered_indices(self):
        candles_by_periods = {"1min": TestCandlesMultiIndiFeatures.candles_1m_unordered()}
        features = CandlesMultiIndiFeatures.multi_indi_features(self.candles_1m_unordered(), self.params())
        self.assertFalse(features.empty)
