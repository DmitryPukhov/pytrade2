from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.common.features.CandlesFeatures import CandlesFeatures


class TestCandlesFeatures(TestCase):

    def candles_1m_5(self):
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
            .set_index("close_time")

    def candles_5m_5(self):
        return pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 06:40:00"), "symbol": "asset1", "interval": "5m",
             "open": 6.40, "high": 6.40, "low": 6.40, "close": 6.40, "vol": 6.40},

            {"close_time": datetime.fromisoformat("2023-05-21 06:45:00"), "symbol": "asset1", "interval": "5m",
             "open": 6.45, "high": 6.450, "low": 6.4500, "close": 6.45000, "vol": 6.450000},

            {"close_time": datetime.fromisoformat("2023-05-21 06:50:00"), "symbol": "asset1", "interval": "5m",
             "open": 6.50, "high": 6.500, "low": 6.5000, "close": 6.50000, "vol": 6.500000},

            {"close_time": datetime.fromisoformat("2023-05-21 06:55:00"), "symbol": "asset1", "interval": "5m",
             "open": 6.55, "high": 6.550, "low": 6.5500, "close": 6.55000, "vol": 6.550000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:05:00"), "symbol": "asset1", "interval": "5m",
             "open": 7.00, "high": 7.0000, "low": 7.0000, "close": 7000, "vol": 70000}]) \
            .set_index("close_time")

    def test_candles_combined_features_of(self):
        features = CandlesFeatures.candles_combined_features_of(self.candles_1m_5(), 2, self.candles_5m_5(), 2)

        self.assertSequenceEqual([
            "1m_open", "1m_high", "1m_low", "1m_close", "1m_vol",
            "1m_-1_open", "1m_-1_high", "1m_-1_low", "1m_-1_close", "1m_-1_vol",
            "5m_open", "5m_high", "5m_low", "5m_close", "5m_vol",
            "5m_-1_open", "5m_-1_high", "5m_-1_low", "5m_-1_close", "5m_-1_vol"],
            features.columns.tolist())
        self.assertSequenceEqual([2, 3, 4, 5], features["1m_open"].tolist())
        self.assertSequenceEqual([20, 30, 40, 50], features["1m_high"].tolist())
        self.assertSequenceEqual([200, 300, 400, 500], features["1m_low"].tolist())
        self.assertSequenceEqual([2000, 3000, 4000, 5000], features["1m_close"].tolist())
        self.assertSequenceEqual([20000, 30000, 40000, 50000], features["1m_vol"].tolist())

        self.assertSequenceEqual([1, 2, 3, 4], features["1m_-1_open"].tolist())
        self.assertSequenceEqual([10, 20, 30, 40], features["1m_-1_high"].tolist())
        self.assertSequenceEqual([100, 200, 300, 400], features["1m_-1_low"].tolist())
        self.assertSequenceEqual([1000, 2000, 3000, 4000], features["1m_-1_close"].tolist())
        self.assertSequenceEqual([10000, 20000, 30000, 40000], features["1m_-1_vol"].tolist())

        # Slow should be merged backward
        self.assertSequenceEqual([6.55, 6.55, 6.55, 7], features["5m_open"].tolist())


    def test_candles_features_of(self):
        candles = self.candles_1m_5()
        features = CandlesFeatures.candles_features_of(candles, interval="1m", window_size=5)

        # 5 candles window should present
        self.assertSequenceEqual(
            ["1m_open", "1m_high", "1m_low", "1m_close", "1m_vol",
             "1m_-1_open", "1m_-1_high", "1m_-1_low", "1m_-1_close", "1m_-1_vol",
             "1m_-2_open", "1m_-2_high", "1m_-2_low", "1m_-2_close", "1m_-2_vol",
             "1m_-3_open", "1m_-3_high", "1m_-3_low", "1m_-3_close", "1m_-3_vol",
             "1m_-4_open", "1m_-4_high", "1m_-4_low", "1m_-4_close", "1m_-4_vol",
             ],
            features.columns.tolist())
        # Previous window filled only for last candle
        self.assertSequenceEqual([datetime.fromisoformat("2023-05-21 07:05:00")], features.index.tolist())

        self.assertSequenceEqual([5], features["1m_open"].tolist())
        self.assertSequenceEqual([4], features["1m_-1_open"].tolist())
        self.assertSequenceEqual([3], features["1m_-2_open"].tolist())
        self.assertSequenceEqual([2], features["1m_-3_open"].tolist())
        self.assertSequenceEqual([1], features["1m_-4_open"].tolist())

        self.assertSequenceEqual([50], features["1m_high"].tolist())
        self.assertSequenceEqual([40], features["1m_-1_high"].tolist())
        self.assertSequenceEqual([30], features["1m_-2_high"].tolist())
        self.assertSequenceEqual([20], features["1m_-3_high"].tolist())
        self.assertSequenceEqual([10], features["1m_-4_high"].tolist())

        self.assertSequenceEqual([500], features["1m_low"].tolist())
        self.assertSequenceEqual([400], features["1m_-1_low"].tolist())
        self.assertSequenceEqual([300], features["1m_-2_low"].tolist())
        self.assertSequenceEqual([200], features["1m_-3_low"].tolist())
        self.assertSequenceEqual([100], features["1m_-4_low"].tolist())

        self.assertSequenceEqual([5000], features["1m_close"].tolist())
        self.assertSequenceEqual([4000], features["1m_-1_close"].tolist())
        self.assertSequenceEqual([3000], features["1m_-2_close"].tolist())
        self.assertSequenceEqual([2000], features["1m_-3_close"].tolist())
        self.assertSequenceEqual([1000], features["1m_-4_close"].tolist())

        self.assertSequenceEqual([50000], features["1m_vol"].tolist())
        self.assertSequenceEqual([40000], features["1m_-1_vol"].tolist())
        self.assertSequenceEqual([30000], features["1m_-2_vol"].tolist())
        self.assertSequenceEqual([20000], features["1m_-3_vol"].tolist())
        self.assertSequenceEqual([10000], features["1m_-4_vol"].tolist())
