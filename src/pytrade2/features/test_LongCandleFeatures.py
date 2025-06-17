from datetime import datetime
from unittest import TestCase

import pandas as pd

from pytrade2.features.LongCandleFeatures import LongCandleFeatures


class TestLongCandleFeatures(TestCase):
    @staticmethod
    def level2():
        return pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-05-21 07:04:00'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-05-21 07:04:00'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-05-21 07:04:00'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
            {'datetime': datetime.fromisoformat('2023-05-21 07:04:00'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1}
        ])

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
             "open": 5, "high": 50, "low": 500, "close": 5000, "vol": 50000}
        ]).set_index("close_time", drop=False)

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
             "open": 16, "high": 7.0000, "low": 7.0000, "close": 7000, "vol": 70000}
        ]).set_index("close_time", drop=False)

    def test_targets_of_low_profit_no_buy(self):
        candles = pd.DataFrame(
            [{"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
             {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 10.9, "low": 10.1, "close": 10, "vol": 1}
             ]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0, 0.1)
        self.assertSequenceEqual([0], actual["signal"].tolist())
        self.assertSequenceEqual([datetime.fromisoformat("2023-05-21 07:01:00")], actual.index)

    def test_targets_of_buy(self):
        candles = pd.DataFrame(
            [{"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
             {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11.1, "low": 10.1, "close": 10, "vol": 1}
             ]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0, 0.1)
        self.assertSequenceEqual([1], actual["signal"].tolist())
        self.assertSequenceEqual([datetime.fromisoformat("2023-05-21 07:01:00")], actual.index)

    def test_targets_of_sell(self):
        candles = pd.DataFrame(
            [{"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
             {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 9.9, "low": 9, "close": 14, "vol": 1
              }]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0, 0.1)
        self.assertSequenceEqual([-1], actual["signal"].tolist())

    def test_targets_of_low_profit_no_sell(self):
        candles = pd.DataFrame(
            [{"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
             {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 9.9, "low": 9.1, "close": 14, "vol": 1
              }]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0, 0.1)
        self.assertSequenceEqual([0], actual["signal"].tolist())

    def test_targets_of_big_loss_no_sell(self):
        candles = pd.DataFrame(
            [{"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
             {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
              "open": 10, "high": 11, "low": 9, "close": 14, "vol": 1
              }]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0.1, 0)
        self.assertSequenceEqual([0], actual["signal"].tolist())

    def test_targets_of_flat(self):
        candles = pd.DataFrame([
            {"close_time": datetime.fromisoformat("2023-05-21 07:01:00"), "symbol": "asset1", "interval": "1m",
             "open": 16, "high": 18, "low": 15, "close": 17, "vol": 1},
            {"close_time": datetime.fromisoformat("2023-05-21 07:02:00"), "symbol": "asset1", "interval": "1m",
             "open": 16, "high": 18, "low": 15, "close": 17, "vol": 1}
        ]).set_index("close_time", drop=False)

        actual = LongCandleFeatures.targets_of(candles, 0, 0)
        self.assertSequenceEqual([0], actual["signal"].tolist())

    def test_features_targets__same_index(self):
        actual_features, actual_targets = LongCandleFeatures.features_targets_of(
            candles_by_periods={"1min": self.candles_1m_5(), "5min": self.candles_5m_5()},
            cnt_by_period={"1min": 2, "5min": 2},
            #level2=self.level2(),
            #level2_past_window="1min",
            target_period="1min",
            loss_min_coeff=0,
            profit_min_coeff=0)

        self.assertSequenceEqual(actual_features.index.tolist(), actual_targets.index.tolist())
        self.assertEqual(2, len(actual_features))


    # def test_features_targets__should_get_last_level2(self):
    #     actual_features, actual_targets, actual_features_wo_targets = LongCandleFeatures.features_targets_of(
    #         # Last feature candle is at 7:04:00, target candle is at 7:05:00
    #         candles_by_periods={"1min": self.candles_1m_5(), "5min": self.candles_5m_5()},
    #         cnt_by_period={"1min": 2, "5min": 2},
    #         # Level2 is at 7:04:00, should be included in last feature candle at 7:04 only
    #         level2=self.level2(),
    #         level2_past_window="1min",
    #         target_period="1min",
    #         loss_min_coeff=0,
    #         profit_min_coeff=0)
    #
    #     self.assertSequenceEqual(actual_features.index.tolist(), actual_targets.index.tolist())
    #     self.assertEqual(1, len(actual_features))
    #     self.assertEqual(1, len(actual_features_wo_targets))
    #
    #     # L2 should be filled in the features after l2
    #     l2cols = [c for c in actual_features.columns if c.startswith("l2_bucket")]
    #     l2index = actual_features[l2cols].dropna().index.tolist()
    #     self.assertEqual([datetime.fromisoformat('2023-05-21 07:04:00')], l2index)
    #
    # def test_features_targets__should_get_prev_level2(self):
    #     level2 = self.level2()
    #     level2["datetime"] = datetime.fromisoformat('2023-05-21 07:03:59')
    #     actual_features, actual_targets, actual_features_wo_targets = LongCandleFeatures.features_targets_of(
    #         # Last feature candle is at 7:04:00, target candle is at 7:05:00
    #         candles_by_periods={"1min": self.candles_1m_5(), "5min": self.candles_5m_5()},
    #         cnt_by_period={"1min": 2, "5min": 2},
    #         # Level2 is at 7:04:00, should be included in last feature candle at 7:04 only
    #         level2=level2,
    #         level2_past_window="1min",
    #         target_period="1min",
    #         loss_min_coeff=0,
    #         profit_min_coeff=0)
    #
    #     self.assertSequenceEqual(actual_features.index.tolist(), actual_targets.index.tolist())
    #     self.assertEqual(1, len(actual_features))
    #     self.assertEqual(1, len(actual_features_wo_targets))
    #
    #     # L2 should be filled in the features after l2
    #     l2cols = [c for c in actual_features.columns if c.startswith("l2_bucket")]
    #     l2index = actual_features[l2cols].index.tolist()
    #     self.assertEqual([datetime.fromisoformat('2023-05-21 07:04:00')], l2index)
