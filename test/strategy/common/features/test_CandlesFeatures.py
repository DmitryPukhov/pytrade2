from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.common.features.CandlesFeatures import CandlesFeatures


class TestCandlesFeatures(TestCase):

    def candles_1m_5(self):
        return pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 07:00:01"), "symbol": "asset1", "interval": "1m",
             "open": 1, "high": 10, "low": 100, "close": 1000, "vol": 10000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:00:02"), "symbol": "asset1", "interval": "1m",
             "open": 2, "high": 20, "low": 200, "close": 2000, "vol": 20000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:00:03"), "symbol": "asset1", "interval": "1m",
             "open": 3, "high": 30, "low": 300, "close": 3000, "vol": 30000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:00:04"), "symbol": "asset1", "interval": "1m",
             "open": 4, "high": 40, "low": 400, "close": 4000, "vol": 40000},

            {"close_time": datetime.fromisoformat("2023-05-21 07:00:05"), "symbol": "asset1", "interval": "1m",
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000}]).set_index("close_time")

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
        self.assertSequenceEqual([datetime.fromisoformat("2023-05-21 07:00:05")], features.index.tolist())

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
