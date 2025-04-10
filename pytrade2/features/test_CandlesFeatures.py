from datetime import datetime
from unittest import TestCase
import pandas as pd
from features.CandlesFeatures import CandlesFeatures


class TestCandlesFeatures(TestCase):

    @staticmethod
    def candles_1m_5():
        return pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
             "open": 1, "high": 10, "low": 100, "close": 1000, "vol": 10000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 2, "high": 20, "low": 200, "close": 2000, "vol": 20000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:03:00"), "symbol": "asset1", "interval": "1m",
             "open": 3, "high": 30, "low": 300, "close": 3000, "vol": 30000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:04:00"), "symbol": "asset1", "interval": "1m",
             "open": 4, "high": 40, "low": 400, "close": 4000, "vol": 40000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:05:00"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000}]) \
            .set_index("close_time", drop=False)

    @staticmethod
    def candles_5m_5():
        return pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 06:40:00"), "symbol": "asset1", "interval": "5m",
             "open": 6, "high": 6.40, "low": 6.40, "close": 6.40, "vol": 6.40},

            {"close_time": datetime.fromisoformat("2023-05-21 06:45:00"), "symbol": "asset1", "interval": "5m",
             "open": 7, "high": 6.450, "low": 6.4500, "close": 6.45000, "vol": 6.450000},

            {"close_time": datetime.fromisoformat("2023-05-21 06:50:00"), "symbol": "asset1", "interval": "5m",
             "open": 9, "high": 6.500, "low": 6.5000, "close": 6.50000, "vol": 6.500000},

            {"close_time": datetime.fromisoformat("2023-05-21 06:55:00"), "symbol": "asset1", "interval": "5m",
             "open": 12, "high": 6.550, "low": 6.5500, "close": 6.55000, "vol": 6.550000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:05:00"), "symbol": "asset1", "interval": "5m",
             "open": 16, "high": 7.0000, "low": 7.0000, "close": 7000, "vol": 70000}]) \
            .set_index("close_time", drop=False)

    def test_candles_combined_features_of_empty(self):
        features = CandlesFeatures.candles_combined_features_of({}, {"1min": 2, "5min": 2})
        self.assertEqual(True, features.empty)

    def test_candles_combined_features_of(self):
        features = CandlesFeatures.candles_combined_features_of(
            {"1min": self.candles_1m_5(), "5min": self.candles_5m_5()},
            {"1min": 2, "5min": 2})

        self.assertSequenceEqual([
            "1min_open", "1min_high", "1min_low", "1min_close", "1min_vol",
            "1min_-1_open", "1min_-1_high", "1min_-1_low", "1min_-1_close", "1min_-1_vol",
            "5min_open", "5min_high", "5min_low", "5min_close", "5min_vol",
            "5min_-1_open", "5min_-1_high", "5min_-1_low", "5min_-1_close", "5min_-1_vol"],
            features.columns.tolist())
        self.assertSequenceEqual([1, 1, 1], features["1min_open"].tolist())
        self.assertSequenceEqual([10, 10, 10], features["1min_high"].tolist())
        self.assertSequenceEqual([100, 100, 100], features["1min_low"].tolist())
        self.assertSequenceEqual([1000, 1000, 1000], features["1min_close"].tolist())
        self.assertSequenceEqual([10000, 10000, 10000], features["1min_vol"].tolist())

        self.assertSequenceEqual([1, 1, 1], features["1min_-1_open"].tolist())
        self.assertSequenceEqual([10, 10, 10], features["1min_-1_high"].tolist())
        self.assertSequenceEqual([100, 100, 100], features["1min_-1_low"].tolist())
        self.assertSequenceEqual([1000, 1000, 1000], features["1min_-1_close"].tolist())
        self.assertSequenceEqual([10000, 10000, 10000], features["1min_-1_vol"].tolist())

        # Slow should be merged backward
        self.assertSequenceEqual([3, 3, 4], features["5min_open"].tolist())

    def test_candles_features_of(self):
        candles = self.candles_1m_5()
        features = CandlesFeatures.candles_features_of(candles, interval="1m", window_size=4)

        # 5 candles window should present
        self.assertSequenceEqual(
            ["1m_open", "1m_high", "1m_low", "1m_close", "1m_vol",
             "1m_-1_open", "1m_-1_high", "1m_-1_low", "1m_-1_close", "1m_-1_vol",
             "1m_-2_open", "1m_-2_high", "1m_-2_low", "1m_-2_close", "1m_-2_vol",
             "1m_-3_open", "1m_-3_high", "1m_-3_low", "1m_-3_close", "1m_-3_vol"
             ],
            features.columns.tolist())
        # Previous window filled only for last candle
        self.assertSequenceEqual([datetime.fromisoformat("2023-05-21 07:05:00")], features.index.tolist())

        self.assertSequenceEqual([1], features["1m_open"].tolist())
        self.assertSequenceEqual([1], features["1m_-1_open"].tolist())
        self.assertSequenceEqual([1], features["1m_-2_open"].tolist())
        self.assertSequenceEqual([1], features["1m_-3_open"].tolist())

        self.assertSequenceEqual([10], features["1m_high"].tolist())
        self.assertSequenceEqual([10], features["1m_-1_high"].tolist())
        self.assertSequenceEqual([10], features["1m_-2_high"].tolist())
        self.assertSequenceEqual([10], features["1m_-3_high"].tolist())

        self.assertSequenceEqual([100], features["1m_low"].tolist())
        self.assertSequenceEqual([100], features["1m_-1_low"].tolist())
        self.assertSequenceEqual([100], features["1m_-2_low"].tolist())
        self.assertSequenceEqual([100], features["1m_-3_low"].tolist())

        self.assertSequenceEqual([1000], features["1m_close"].tolist())
        self.assertSequenceEqual([1000], features["1m_-1_close"].tolist())
        self.assertSequenceEqual([1000], features["1m_-2_close"].tolist())
        self.assertSequenceEqual([1000], features["1m_-3_close"].tolist())

        self.assertSequenceEqual([10000], features["1m_vol"].tolist())
        self.assertSequenceEqual([10000], features["1m_-1_vol"].tolist())
        self.assertSequenceEqual([10000], features["1m_-2_vol"].tolist())
        self.assertSequenceEqual([10000], features["1m_-3_vol"].tolist())


