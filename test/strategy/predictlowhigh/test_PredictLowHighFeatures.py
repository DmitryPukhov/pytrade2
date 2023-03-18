from datetime import datetime

import numpy as np
import pandas as pd
from unittest import TestCase

from feed.BaseFeed import BaseFeed
from strategy.StrategyBase import StrategyBase
from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class TestPredictLowHighFeatures(TestCase):

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
        # Call
        actual_features, actual_targets = PredictLowHighFeatures.features_targets_of(bid_ask, level2)
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
        # Call
        actual_features = PredictLowHighFeatures.features_of(bid_ask, level2).dropna()

        # First level2 is at 15:56:03, so features with level2 will start from 15:56:03
        self.assertListEqual(actual_features.index.to_pydatetime().tolist(),
                             [datetime.fromisoformat("2023-03-17 15:56:03"),
                              datetime.fromisoformat("2023-03-17 15:56:15")])
        # bid_ask should present
        self.assertListEqual(actual_features.bid.values.tolist(), [9, 13])
        self.assertListEqual(actual_features.bid_vol.values.tolist(), [10, 14])
        self.assertListEqual(actual_features.ask.values.tolist(), [11, 15])
        self.assertListEqual(actual_features.ask_vol.values.tolist(), [12, 16])
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

        ]).set_index("datetime")
        actual = PredictLowHighFeatures().targets_of(df, predict_window="10s")

        # Future values should be predicted only if future window completed
        self.assertListEqual(actual["bid_fut"].dropna().values.tolist(), [3.0, 11.0, 12.0, 13.0])

    def test_features_of__bid_ask_columns_should_come_from_bidask(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2021-12-08 07:00:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4},
            {"datetime": datetime.fromisoformat("2021-12-08 07:01:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:03"), "symbol": "asset1",
             "bid": 9, "bid_vol": 10, "ask": 11, "ask_vol": 12},
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

        # Call
        actual = PredictLowHighFeatures.features_of(bid_ask, level2)

        self.assertListEqual([1, 5, 9], actual["bid"].dropna().values.tolist())
        self.assertListEqual([2, 6, 10], actual["bid_vol"].dropna().values.tolist())
        self.assertListEqual([3, 7, 11], actual["ask"].dropna().values.tolist())
        self.assertListEqual([4, 8, 12], actual["ask_vol"].dropna().values.tolist())

    def test_features_of__empty_features_of_empty_level2_or_bidask(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-18 09:20:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4}
        ]).set_index("datetime", drop=False)
        level2 = bid_ask
        # Empty level2 => empty features
        self.assertTrue(PredictLowHighFeatures.features_of(bid_ask, pd.DataFrame()).empty)
        # Empty bidask => empty features
        self.assertTrue(PredictLowHighFeatures.features_of(pd.DataFrame(), level2).empty)

    def test_features_of__should_be_empty_if_bidask_hasnot_prev_level2(self):
        bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-18 09:20:01"), "symbol": "asset1",
             "bid": 1, "bid_vol": 2, "ask": 3, "ask_vol": 4}
        ]).set_index("datetime", drop=False)
        level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-18 09:20:02'), 'ask': 0.9, 'ask_vol': 1, 'bid_vol': None},
            {'datetime': datetime.fromisoformat('2023-03-18 09:20:02'), 'bid': -0.9, 'ask_vol': None, 'bid_vol': 1},
        ])
        actual = PredictLowHighFeatures.features_of(bid_ask, level2)
        self.assertTrue(actual.empty)
