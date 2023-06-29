from datetime import datetime

import pandas as pd
from unittest import TestCase

from strategy.common.features.PredictLowHighFeatures import PredictLowHighFeatures


class TestPredictLowHighFeatures(TestCase):
    past_window = "10s"
    candles_by_interval = {
        "1min": pd.DataFrame([
            {"close_time": datetime.fromisoformat("2021-12-08 07:00"),
             "symbol": "asset1", "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},
            {"close_time": datetime.fromisoformat("2021-12-08 07:01"),
             "symbol": "asset1", "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},
            {"close_time": datetime.fromisoformat("2021-12-08 07:02"),
             "symbol": "asset1", "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},

            {"close_time": datetime.fromisoformat("2023-03-17 15:55:00"),
             "symbol": "asset1", "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},

            {"close_time": datetime.fromisoformat("2023-03-17 15:56:00"),
             "symbol": "asset1", "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},
            {"close_time": datetime.fromisoformat("2023-03-17 15:56:01"),
             "symbol": "asset1", "open": 1.1, "high": 2.2, "low": 3.3, "close": 4.4, "vol": 5.5},
            {"close_time": datetime.fromisoformat("2023-03-17 15:56:02"),
             "symbol": "asset1", "open": 1.1, "high": 2.2, "low": 3.3, "close": 4.4, "vol": 5.5},
        ]).set_index("close_time", drop=False)}
    candles_cnt_by_interval = {"1min": 1}

    def test_last_features_of(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10, "ask": 11, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:15"), "symbol": "asset1",
             "bid": 13, "bid_vol": 14, "ask": 15, "ask_vol": 16}
        ]).set_index("datetime", drop=False)
        level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'ask': 0.9, 'ask_vol': 1},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'bid': -0.9, 'bid_vol': 1},

            {'datetime': datetime.fromisoformat('2023-03-17 15:56:16'), 'ask': 0.9, 'ask_vol': 1},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:16'), 'bid': -0.9, 'bid_vol': 1}
        ]).set_index("datetime", drop=False)
        # candles_features = pd.DataFrame([
        #     {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "doesntmatter": 1}
        # ]).set_index("datetime", drop=False)

        # Call
        actual = PredictLowHighFeatures.last_features_of(bid_ask, 1, level2, self.candles_by_interval,
                                                         self.candles_cnt_by_interval, past_window="10s")
        # Assert
        self.assertListEqual(actual.index.to_pydatetime().tolist(),
                             [datetime.fromisoformat("2023-03-17 15:56:15")])

    def test_features_targets_of__features_and_targets_should_have_same_index(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10, "ask": 11, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:15"), "symbol": "asset1",
             "bid": 13, "bid_vol": 14, "ask": 15, "ask_vol": 16}
        ]).set_index("datetime", drop=False)
        level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
        ])
        # candles_features = pd.DataFrame([
        #     {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "doesntmatter": 1}
        # ]).set_index("datetime", drop=False)

        # Call
        actual_features, actual_targets = PredictLowHighFeatures.features_targets_of(
            bid_ask, level2, self.candles_by_interval, self.candles_cnt_by_interval, "10s", "10s")
        self.assertListEqual(actual_features.index.values.tolist(), actual_targets.index.values.tolist())

    def test_features_of__merge_bidask_level2(self):
        """ Bidask should be enriched with latest level2 before bidask"""

        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10, "ask": 11, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:15"), "symbol": "asset1",
             "bid": 13, "bid_vol": 14, "ask": 15, "ask_vol": 16}
        ]).set_index("datetime", drop=False)
        level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:03'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
        ])
        # candles_features = pd.DataFrame([
        #     {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "doesntmatter": 1}
        # ]).set_index("datetime")

        # Call.
        actual_features = PredictLowHighFeatures.features_of(
            bid_ask, level2, self.candles_by_interval, self.candles_cnt_by_interval, "1s")

        # First level2 is at 15:56:03, so features with level2 will start from 15:56:03
        self.assertListEqual(actual_features.index.to_pydatetime().tolist(),
                             [datetime.fromisoformat("2023-03-17 15:56:03"),
                              datetime.fromisoformat("2023-03-17 15:56:15")])
        # bid_ask should present
        self.assertListEqual([4, 4], actual_features.bid_diff.values.tolist())
        self.assertListEqual([4, 4], actual_features.bid_vol_diff.values.tolist())
        self.assertListEqual([4, 4], actual_features.ask_diff.values.tolist())
        self.assertListEqual([4, 4], actual_features.ask_vol_diff.values.tolist())
        self.assertListEqual([2, 2], actual_features.spread.values.tolist())
        # level2 features are checked in others level2 tests

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

        ]).set_index("datetime", drop=False)
        actual = PredictLowHighFeatures().targets_of(df, predict_window="10s")

        # Future values should be predicted only if future window completed
        self.assertListEqual(actual["bid_max_fut_diff"].values.tolist(), [3, 2, 9, 9])
        self.assertListEqual(actual["bid_spread_fut"].values.tolist(), [3, 2, 9, 9])
        self.assertListEqual(actual["ask_min_fut_diff"].values.tolist(), [0, 0, 0, 0])
        self.assertListEqual(actual["ask_spread_fut"].values.tolist(), [3, 2, 9, 9])

    def test_features_of__bid_ask_columns(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:02"), "symbol": "asset1",
             "bid": 2, "bid_vol": 5, "ask": 8, "ask_vol": 11},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 4, "bid_vol": 9, "ask": 14, "ask_vol": 19},
        ]).set_index("datetime", drop=False)

        level2 = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2},

            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:01"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6},

            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "ask": 11, "ask_vol": 12},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10}
        ])
        # candles_features = pd.DataFrame([
        #     {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "doesntmatter": 1}
        # ]).set_index("datetime")

        # Call
        actual = PredictLowHighFeatures.features_of(
            bid_ask, level2, self.candles_by_interval, self.candles_cnt_by_interval, past_window=self.past_window)

        self.assertListEqual([6, 10], actual["spread"].dropna().values.tolist())
        self.assertListEqual([1, 2], actual["bid_diff"].dropna().values.tolist())
        self.assertListEqual([3, 4], actual["bid_vol_diff"].dropna().values.tolist())
        self.assertListEqual([5, 6], actual["ask_diff"].dropna().values.tolist())
        self.assertListEqual([7, 8], actual["ask_vol_diff"].dropna().values.tolist())

    def test_features_of__empty_features_of_empty_level2_or_bidask(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-18 09:20:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4}
        ]).set_index("datetime", drop=False)
        level2 = bid_ask  # columns does not matter here
        # Empty level2 => empty features
        self.assertTrue(PredictLowHighFeatures.features_of(
            bid_ask, pd.DataFrame(), self.candles_by_interval, self.candles_cnt_by_interval,
            past_window=self.past_window).empty)
        # Empty bidask => empty features
        self.assertTrue(PredictLowHighFeatures.features_of(
            pd.DataFrame(), level2, self.candles_by_interval, self.candles_cnt_by_interval,
            past_window=self.past_window).empty)
        # Empty candles => empty features
        self.assertTrue(
            PredictLowHighFeatures.features_of(
                bid_ask, level2, self.candles_by_interval, self.candles_cnt_by_interval,
                past_window=self.past_window).empty)

    def test_features_of__should_be_empty_if_bidask_hasnot_prev_level2(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-18 09:20:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4}
        ]).set_index("datetime", drop=False)
        level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-18 09:20:02'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-03-18 09:20:02'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
        ])
        # candles_features = pd.DataFrame(
        #     [{"datetime": datetime.fromisoformat("2023-03-18 09:20:01"), "doesntmatter": 1}])
        actual = PredictLowHighFeatures.features_of(
            bid_ask, level2, self.candles_by_interval, self.candles_cnt_by_interval, past_window=self.past_window)
        self.assertTrue(actual.empty)
